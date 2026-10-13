//! 职责：NameServer 地址管理、Topic 路由获取与缓存、Broker 地址解析、
//! 消息发送（SEND_MESSAGE / SEND_MESSAGE_V2）、拉取（PULL_MESSAGE）、POP 弹取
//! （POP_MESSAGE / ACK_MESSAGE / CHANGE_MESSAGE_INVISIBLETIME）、offset 查询/更新、
//! 心跳、队列锁、管理类 API（创建/删除 Topic、集群信息等）、broker 主动请求
//! （220/221/307/309/326）分派。

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::client::consumer_stats::ConsumerStatsManager;
use crate::client::latency::{LatencyFaultToleranceImpl, PublishInfo, QueueFilter};
use crate::client::request_reply::{is_reply_message, request_future_holder};
use crate::client::result::{
    ChangeInvisibleTimeResult, PopResult, PopStatus, PullResult, PullStatus, SendResult, SendStatus,
};
use crate::client::top_addressing::DefaultTopAddressing;
use crate::common::boundary_type::BoundaryType;
use crate::common::compression::decompress_body;
use crate::common::message::{Message, MessageBatch, MessageExt, MessageQueue};
use crate::common::message_client_id_setter::{get_uniq_id, set_uniq_id};
use crate::common::message_const::{
    KEY_SEPARATOR, PROPERTY_CORRELATION_ID, PROPERTY_FIRST_POP_TIME, PROPERTY_MAX_RECONSUME_TIMES,
    PROPERTY_MSG_REGION, PROPERTY_POP_CK, PROPERTY_RECONSUME_TIME,
    PROPERTY_REPLY_MESSAGE_ARRIVE_TIME, PROPERTY_TRACE_SWITCH,
};
use crate::common::message_decoder::{
    decode_message, decode_messages, message_properties_2_string, string_2_message_properties,
};
use crate::common::mix_all::MixAll;
use crate::common::sysflag::{MessageSysFlag, PullSysFlag};
use crate::common::topic_config::TopicFilterType;
use crate::common::util_all::current_time_millis;
use crate::error::{client_error_code, Error, Result};
use crate::remoting::client::{
    RemotingClient, RemotingClientConfig, RequestProcessor, ResponseSink,
};
use crate::remoting::protocol::admin_body::MessageQueueKey;
use crate::remoting::protocol::body::{
    CheckClientRequestBody, ClusterInfo, ConsumeMessageDirectlyResult, ConsumerRunningInfo,
    GetConsumerListByGroupResponseBody, GetConsumerStatusBody, LockBatchRequestBody,
    LockBatchResponseBody, ResetOffsetBody, ResetOffsetBodyForC, TopicList, UnlockBatchRequestBody,
};
use crate::remoting::protocol::codes::{request_code, response_code};
use crate::remoting::protocol::ext_fields::CustomHeader;
use crate::remoting::protocol::extra_info;
use crate::remoting::protocol::headers::{
    AckMessageRequestHeader, ChangeInvisibleTimeRequestHeader, ChangeInvisibleTimeResponseHeader,
    ConsumeMessageDirectlyResultRequestHeader, ConsumerSendMsgBackRequestHeader,
    CreateTopicRequestHeader, GetConsumerListByGroupRequestHeader,
    GetConsumerRunningInfoRequestHeader, GetConsumerStatusRequestHeader,
    GetEarliestMsgStoretimeRequestHeader, GetEarliestMsgStoretimeResponseHeader,
    GetMaxOffsetRequestHeader, GetMaxOffsetResponseHeader, GetMinOffsetRequestHeader,
    GetMinOffsetResponseHeader, LockBatchMqRequestHeader, NotifyConsumerIdsChangedRequestHeader,
    PopMessageRequestHeader, PopMessageResponseHeader, PullMessageRequestHeader,
    PullMessageResponseHeader, QueryConsumerOffsetRequestHeader, QueryConsumerOffsetResponseHeader,
    QueryMessageRequestHeader, RecallMessageRequestHeader, RecallMessageResponseHeader,
    ReplyMessageRequestHeader, ResetOffsetRequestHeader, SearchOffsetRequestHeader,
    SearchOffsetResponseHeader, SendMessageRequestHeaderV2, SendMessageResponseHeader,
    UnlockBatchMqRequestHeader, UnregisterClientRequestHeader, UpdateConsumerOffsetRequestHeader,
};
use crate::remoting::protocol::heartbeat::{
    ConsumerData, ExpressionType, HeartbeatData, SubscriptionData,
};
use crate::remoting::protocol::query_assignment_body::{
    MessageQueueAssignment, QueryAssignmentRequestBody, QueryAssignmentResponseBody,
};
use crate::remoting::protocol::remoting_command::RemotingCommand;
use crate::remoting::protocol::route::{pseudo_random_index, TopicRouteData};
use crate::remoting::rpchook::{NamespaceRpcHook, StreamTypeRPCHook};
use crate::remoting::socks::SocksProxyConfig;
use crate::{bail, rmq_debug, rmq_info, rmq_warn};

// ================================================================ seams

pub(crate) const MQ_CLIENT_API_TIMEOUT_MILLIS: i64 = 3000;

/// 对象安全：异步行为方法用 `self: Arc<Self>` 接收者 + 装箱 future，
/// 实例里以 `Arc<dyn RegisteredConsumer>` 持有。
pub trait RegisteredConsumer: Send + Sync {
    /// 消费者自己的 clientId（进心跳的 ConsumerData）。
    fn client_id(&self) -> String;

    /// `ConsumerData.groupName`。
    fn consumer_group(&self) -> String;

    fn consume_type(&self) -> String;

    /// `ConsumerData.messageModel`（`CLUSTERING` / `BROADCASTING`）。
    fn message_model(&self) -> String;

    /// `ConsumerData.consumeFromWhere`。
    fn consume_from_where(&self) -> String;

    fn is_unit_mode(&self) -> bool;

    fn subscription(&self) -> Vec<String>;

    /// `topic -> SubscriptionData` 表，
    /// 进心跳的 `ConsumerData.subscriptionDataSet`。
    fn subscriptions(&self) -> Vec<SubscriptionData>;

    fn adjust_thread_pool(&self) {}

    fn rebalance_immediately(&self) {}

    /// 220 处理：broker 下发位点重置。必须**后台**执行（内部会做 rebalance /
    /// lock / batch 等 `invoke_sync`，不能卡在 remoting 读线程上）。
    fn reset_offset(
        self: Arc<Self>,
        topic: String,
        offset_table: Vec<(MessageQueue, i64)>,
    ) -> ConsumerFuture<()>;

    /// 221 处理：返回已消费位点表。
    /// `None` = 请求头里没有 topic，与"显式传空串"区分开（只有非空 topic 才过滤）。
    fn get_consumer_status(&self, topic: Option<&str>) -> Vec<(MessageQueue, i64)>;

    /// 307 处理：运行信息。
    fn consumer_running_info(&self) -> ConsumerRunningInfo;

    /// 309 处理：本地真实消费一条消息。
    fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<String>,
    ) -> Result<ConsumeMessageDirectlyResult>;

    fn persist_consumer_offset(self: Arc<Self>) -> ConsumerFuture<()>;
}

/// `RegisteredConsumer` 异步行为的返回类型（无 async-trait 依赖的装箱 future）。
pub type ConsumerFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>;

pub trait TraceDispatcher: Send + Sync {
    /// 启动轨迹分发器（带上 namesrv 地址）。
    fn start(&self, name_server_addr: &str) -> Result<()>;

    /// 关闭轨迹分发器。
    fn shutdown(&self);
}

// ================================================================ TopicPublishInfo

/// 一把锁覆盖队列列表 + 游标 +
/// 路由字段（更新路由时整批替换，读侧一次性快照）。
#[derive(Debug, Default)]
pub struct TopicPublishInfo {
    state: Mutex<PublishState>,
}

#[derive(Debug, Default)]
struct PublishState {
    order_topic: bool,
    msg_queue_list: Vec<MessageQueue>,
    topic_route_data: Option<TopicRouteData>,
    /// 轮询游标（int，可为负无所谓，取模前用 rem_euclid 保证非负）。
    index: i64,
}

/// 轮询游标 → 环内下标（负数/溢出都映射到 `[0, len)`）。
fn ring_index(index: i64, len: usize) -> usize {
    index.rem_euclid(len as i64) as usize
}

impl TopicPublishInfo {
    pub fn new() -> TopicPublishInfo {
        TopicPublishInfo::default()
    }

    /// `ok()`：有队列才算可用。
    pub fn ok(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        !state.msg_queue_list.is_empty()
    }

    /// 把轮询游标清零。
    pub fn reset_index(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).index = 0;
    }

    pub fn select_one_message_queue(
        &self,
        filters: &[&QueueFilter<'_>],
    ) -> Result<Option<MessageQueue>> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let n = state.msg_queue_list.len();
        if n == 0 {
            bail!("no message queue for publish info");
        }
        if filters.is_empty() {
            let mq = state.msg_queue_list[ring_index(state.index, n)].clone();
            state.index += 1;
            return Ok(Some(mq));
        }
        for _ in 0..n {
            let mq = state.msg_queue_list[ring_index(state.index, n)].clone();
            state.index += 1;
            if filters.iter().all(|f| f(&mq)) {
                return Ok(Some(mq));
            }
        }
        Ok(None)
    }

    /// `order_topic` 读（直接读缓存字段）。
    pub fn order_topic(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .order_topic
    }

    /// `msg_queue_list` 快照。
    pub fn msg_queue_list(&self) -> Vec<MessageQueue> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .msg_queue_list
            .clone()
    }

    /// `topic_route_data` 快照。
    pub fn topic_route_data(&self) -> Option<TopicRouteData> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .topic_route_data
            .clone()
    }

    /// 序列化：`{"orderTopic":..., "messageQueueList":[{topic,brokerName,queueId}]}`。
    /// 注意 `messageQueueList` **不含路由本身**，只列队列。
    pub fn to_dict(&self) -> serde_json::Value {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        serde_json::Value::Object(serde_json::Map::from_iter(vec![
            (
                "orderTopic".to_string(),
                serde_json::Value::Bool(state.order_topic),
            ),
            (
                "messageQueueList".to_string(),
                serde_json::Value::Array(
                    state
                        .msg_queue_list
                        .iter()
                        .map(|q| {
                            serde_json::Value::Object(serde_json::Map::from_iter(vec![
                                (
                                    "topic".to_string(),
                                    serde_json::Value::String(q.topic.clone()),
                                ),
                                (
                                    "brokerName".to_string(),
                                    serde_json::Value::String(q.broker_name.clone()),
                                ),
                                ("queueId".to_string(), serde_json::Value::from(q.queue_id)),
                            ]))
                        })
                        .collect(),
                ),
            ),
        ]))
    }

    /// 路由落库时整批刷新：`order_topic / topic_route_data / msg_queue_list`
    /// 三连赋值。
    fn update_from_route(&self, route: &TopicRouteData, topic: &str) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.order_topic = route.order_topic_conf.is_some();
        state.topic_route_data = Some(route.clone());
        state.msg_queue_list = route_queue_lists(route, topic);
    }
}

impl Clone for TopicPublishInfo {
    fn clone(&self) -> TopicPublishInfo {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        TopicPublishInfo {
            state: Mutex::new(PublishState {
                order_topic: state.order_topic,
                msg_queue_list: state.msg_queue_list.clone(),
                topic_route_data: state.topic_route_data.clone(),
                index: state.index,
            }),
        }
    }
}

/// 实现 [`crate::client::latency::PublishInfo`]：`MQFaultStrategy` 拿它做故障延迟选队。
impl PublishInfo for TopicPublishInfo {
    fn reset_index(&self) {
        TopicPublishInfo::reset_index(self);
    }

    fn select_one_message_queue(
        &self,
        filters: &[&QueueFilter<'_>],
    ) -> Result<Option<MessageQueue>> {
        TopicPublishInfo::select_one_message_queue(self, filters)
    }
}

/// 按 queueDatas × writeQueueNums 组装 topic 的全部队列。
/// 与 `TopicRouteData::get_all_message_queue` 同源，这里独立走读以便
/// `TopicPublishInfo` 与实例都复用。
fn route_queue_lists(route: &TopicRouteData, topic: &str) -> Vec<MessageQueue> {
    route
        .get_all_message_queue(topic)
        .into_iter()
        .map(|q| MessageQueue::new(&q.topic, &q.broker_name, q.queue_id))
        .collect()
}

// ================================================================ 发布消息包装

/// 发送入口的消息包装：发送内核既收
/// `Message` 也收 `MessageBatch`；batch 标志决定编码路径与是否补 UNIQ_KEY。
#[derive(Debug)]
pub enum PublishMessage<'a> {
    /// 普通消息。
    Single(&'a mut Message),
    /// 批量消息（body 已在组批时编码好）。
    Batch(&'a mut MessageBatch),
}

impl<'a> PublishMessage<'a> {
    /// 外层 `Message` 视图：批量消息把外层字段收在
    /// `batch.message` 里，语义等价。
    pub fn as_message(&self) -> &Message {
        match self {
            PublishMessage::Single(m) => m,
            PublishMessage::Batch(b) => &b.message,
        }
    }

    /// 外层 `Message` 的可变视图：发送内核里的 traceparent 注入要就地改属性。
    pub fn as_message_mut(&mut self) -> &mut Message {
        match self {
            PublishMessage::Single(m) => m,
            PublishMessage::Batch(b) => &mut b.message,
        }
    }

    /// 是否批量消息。
    pub fn is_batch(&self) -> bool {
        matches!(self, PublishMessage::Batch(_))
    }

    /// 重新借出同一份消息，借期缩短到本次调用 —— 发送**重试链**每轮都要新建一个
    /// `PublishMessage`（每轮按选中的 broker 重建请求），但底层消息只有一份。
    pub fn reborrow(&mut self) -> PublishMessage<'_> {
        match self {
            PublishMessage::Single(m) => PublishMessage::Single(m),
            PublishMessage::Batch(b) => PublishMessage::Batch(b),
        }
    }
}

// ================================================================ 配置

/// `MQClientInstance` 构造参数（全量显式注入 + 模块内部常量）。
#[derive(Clone)]
pub struct MQClientInstanceConfig {
    /// 连接超时（默认 3000）。
    pub connect_timeout_millis: i64,
    /// RPC 调用超时（默认 15000）。
    pub invoke_timeout_millis: i32,
    /// TLS 开关：`None` 走环境变量 `ROCKETMQ_TLS_ENABLE`（与
    /// [`RemotingClientConfig::default`] 同口径），`Some(v)` 强制。
    pub tls_enable: Option<bool>,
    pub socks_proxy: Option<SocksProxyConfig>,
    pub route_refresh_interval_millis: u64,
    /// 动态 namesrv 首延迟 / 周期（10s / 120s）。
    pub namesrv_refresh_initial_delay_millis: u64,
    pub namesrv_refresh_interval_millis: u64,
    /// 线程弹性巡检（60s / 60s）。
    pub adjust_pool_initial_delay_millis: u64,
    pub adjust_pool_interval_millis: u64,
    pub heartbeat_initial_delay_millis: u64,
    pub heartbeat_interval_millis: u64,
    pub persist_offset_initial_delay_millis: u64,
    pub persist_offset_interval_millis: u64,
    /// 实例级共享统计器（注入用；
    /// `None` = 默认新建）。
    pub consumer_stats_manager: Option<Arc<ConsumerStatsManager>>,
    pub latency_fault_tolerance: Option<Arc<LatencyFaultToleranceImpl>>,
    /// 见 [`TraceDispatcher`]：本模块只存不调。
    pub trace_dispatcher: Option<Arc<dyn TraceDispatcher>>,
    /// 动态 name server（注入用，
    /// `None` = `DefaultTopAddressing::default()`，读环境变量）。
    pub top_addressing: Option<DefaultTopAddressing>,
    pub unit_name: Option<String>,
    pub namespace_v2: Option<String>,
    pub enable_stream_request_type: bool,
}

impl Default for MQClientInstanceConfig {
    fn default() -> Self {
        MQClientInstanceConfig {
            connect_timeout_millis: 3000,
            invoke_timeout_millis: 15000,
            tls_enable: None,
            socks_proxy: None,
            route_refresh_interval_millis: 30_000,
            namesrv_refresh_initial_delay_millis: 10_000,
            namesrv_refresh_interval_millis: 120_000,
            adjust_pool_initial_delay_millis: 60_000,
            adjust_pool_interval_millis: 60_000,
            heartbeat_initial_delay_millis: 1_000,
            heartbeat_interval_millis: 30_000,
            persist_offset_initial_delay_millis: 10_000,
            persist_offset_interval_millis: 5_000,
            consumer_stats_manager: None,
            latency_fault_tolerance: None,
            trace_dispatcher: None,
            top_addressing: None,
            unit_name: None,
            namespace_v2: None,
            enable_stream_request_type: false,
        }
    }
}

/// 手写 `Debug`：注入的 seam（`Arc<dyn TraceDispatcher>` 等）本身不要求 `Debug`，
/// 这里只打印「是否注入」。
impl std::fmt::Debug for MQClientInstanceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let injected = |on: bool| if on { "injected" } else { "default" };
        f.debug_struct("MQClientInstanceConfig")
            .field("connect_timeout_millis", &self.connect_timeout_millis)
            .field("invoke_timeout_millis", &self.invoke_timeout_millis)
            .field("tls_enable", &self.tls_enable)
            .field("socks_proxy", &self.socks_proxy)
            .field(
                "route_refresh_interval_millis",
                &self.route_refresh_interval_millis,
            )
            .field(
                "namesrv_refresh_initial_delay_millis",
                &self.namesrv_refresh_initial_delay_millis,
            )
            .field(
                "namesrv_refresh_interval_millis",
                &self.namesrv_refresh_interval_millis,
            )
            .field(
                "adjust_pool_initial_delay_millis",
                &self.adjust_pool_initial_delay_millis,
            )
            .field(
                "adjust_pool_interval_millis",
                &self.adjust_pool_interval_millis,
            )
            .field(
                "heartbeat_initial_delay_millis",
                &self.heartbeat_initial_delay_millis,
            )
            .field("heartbeat_interval_millis", &self.heartbeat_interval_millis)
            .field(
                "persist_offset_initial_delay_millis",
                &self.persist_offset_initial_delay_millis,
            )
            .field(
                "persist_offset_interval_millis",
                &self.persist_offset_interval_millis,
            )
            .field(
                "consumer_stats_manager",
                &injected(self.consumer_stats_manager.is_some()),
            )
            .field(
                "latency_fault_tolerance",
                &injected(self.latency_fault_tolerance.is_some()),
            )
            .field(
                "trace_dispatcher",
                &injected(self.trace_dispatcher.is_some()),
            )
            .field("top_addressing", &injected(self.top_addressing.is_some()))
            .field("unit_name", &self.unit_name)
            .field("namespace_v2", &self.namespace_v2)
            .field(
                "enable_stream_request_type",
                &self.enable_stream_request_type,
            )
            .finish()
    }
}

// ================================================================ MQClientInstance

/// 路由三张表：`topic_route_table` + `topic_publish_info_table` +
/// `broker_addr_table`，共用一把路由锁。
#[derive(Default)]
struct RouteTables {
    topic_route_table: HashMap<String, TopicRouteData>,
    topic_publish_info_table: HashMap<String, Arc<TopicPublishInfo>>,
    broker_addr_table: HashMap<String, HashMap<i64, String>>,
    topic_end_points_table: HashMap<String, HashMap<MessageQueue, String>>,
}

struct Inner {
    client_id: String,
    name_server_addrs: Mutex<Vec<String>>,
    remoting_client: RemotingClient,
    tables: Mutex<RouteTables>,
    /// 已登记使用的 topic 集合。
    topics_in_use: Mutex<HashSet<String>>,
    started: AtomicBool,
    consumer_table: Mutex<HashMap<String, Arc<dyn RegisteredConsumer>>>,
    producer_table: Mutex<HashSet<String>>,
    consumer_group_table: Mutex<HashSet<String>>,
    /// `fetch_and_apply` 要 `&mut`，放进 `tokio::sync::Mutex`。
    top_addressing: Arc<tokio::sync::Mutex<DefaultTopAddressing>>,
    consumer_stats_manager: Arc<ConsumerStatsManager>,
    latency_fault_tolerance: Option<Arc<LatencyFaultToleranceImpl>>,
    trace_dispatcher: Option<Arc<dyn TraceDispatcher>>,
    config: MQClientInstanceConfig,
    /// 后台任务的停止信号（true = 停）。
    stop: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    consumer_ids_changed_count: AtomicUsize,
}

fn parse_reset_offset_table(data: &[u8]) -> Result<Vec<(MessageQueue, i64)>> {
    match ResetOffsetBody::decode(data) {
        Ok(body) => Ok(body
            .offset_table
            .into_iter()
            .map(|(k, v)| (MessageQueue::new(&k.topic, &k.broker_name, k.queue_id), v))
            .collect()),
        Err(map_err) => {
            let for_c = ResetOffsetBodyForC::decode(data).map_err(|_| map_err)?;
            Ok(for_c
                .offset_table
                .into_iter()
                .map(|e| {
                    (
                        MessageQueue::new(&e.topic, &e.broker_name, e.queue_id),
                        e.offset,
                    )
                })
                .collect())
        }
    }
}

/// 客户端实例本体（类级 `INSTANCE_MAP` 是
/// [`MQClientInstance::INSTANCE_MAP`]，存弱引用）。
#[derive(Clone)]
pub struct MQClientInstance {
    inner: Arc<Inner>,
}

impl MQClientInstance {
    fn instance_map() -> &'static Mutex<HashMap<String, Weak<Inner>>> {
        static MAP: std::sync::OnceLock<Mutex<HashMap<String, Weak<Inner>>>> =
            std::sync::OnceLock::new();
        MAP.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// 按 client_id + namesrv 地址列表构造（其余参数取默认）。
    pub fn new(client_id: &str, name_server_addrs: Vec<String>) -> MQClientInstance {
        MQClientInstance::with_config(
            client_id,
            name_server_addrs,
            MQClientInstanceConfig::default(),
        )
    }

    /// 全参数注入版本。
    pub fn with_config(
        client_id: &str,
        name_server_addrs: Vec<String>,
        config: MQClientInstanceConfig,
    ) -> MQClientInstance {
        let remoting_config = RemotingClientConfig {
            connect_timeout_millis: config.connect_timeout_millis,
            invoke_timeout_millis: config.invoke_timeout_millis,
            tls_enable: config
                .tls_enable
                .unwrap_or_else(default_tls_enable_from_env),
            socks_proxy: config.socks_proxy.clone(),
            ..RemotingClientConfig::default()
        };
        let inner = Arc::new(Inner {
            client_id: client_id.to_string(),
            name_server_addrs: Mutex::new(name_server_addrs),
            remoting_client: RemotingClient::with_config(remoting_config),
            tables: Mutex::new(RouteTables::default()),
            topics_in_use: Mutex::new(HashSet::new()),
            started: AtomicBool::new(false),
            consumer_table: Mutex::new(HashMap::new()),
            producer_table: Mutex::new(HashSet::new()),
            consumer_group_table: Mutex::new(HashSet::new()),
            top_addressing: Arc::new(tokio::sync::Mutex::new(
                config
                    .top_addressing
                    .clone()
                    .unwrap_or_default()
                    .with_unit_name(config.unit_name.clone().unwrap_or_default()),
            )),
            consumer_stats_manager: config
                .consumer_stats_manager
                .clone()
                .unwrap_or_else(|| Arc::new(ConsumerStatsManager::new())),
            latency_fault_tolerance: config.latency_fault_tolerance.clone(),
            trace_dispatcher: config.trace_dispatcher.clone(),
            config,
            stop: watch::channel(false).0,
            tasks: Mutex::new(Vec::new()),
            consumer_ids_changed_count: AtomicUsize::new(0),
        });
        let this = MQClientInstance {
            inner: inner.clone(),
        };
        this.inner
            .remoting_client
            .register_rpc_hook(Arc::new(NamespaceRpcHook::new(
                inner.config.namespace_v2.as_deref(),
            )));
        if inner.config.enable_stream_request_type {
            this.inner
                .remoting_client
                .register_rpc_hook(Arc::new(StreamTypeRPCHook::new()));
        }
        let processor = Arc::new(ClientRemotingProcessor {
            instance: Arc::downgrade(&inner),
        });
        for code in [
            request_code::PUSH_REPLY_MESSAGE_TO_CLIENT,
            request_code::RESET_CONSUMER_CLIENT_OFFSET,
            request_code::GET_CONSUMER_STATUS_FROM_CLIENT,
            request_code::GET_CONSUMER_RUNNING_INFO,
            request_code::CONSUME_MESSAGE_DIRECTLY,
            request_code::NOTIFY_CONSUMER_IDS_CHANGED,
        ] {
            this.inner
                .remoting_client
                .register_processor(code, processor.clone());
        }
        MQClientInstance::instance_map()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(client_id.to_string(), Arc::downgrade(&inner));
        this
    }

    pub fn create_mq_client_instance(
        client_id: &str,
        name_server_addrs: Vec<String>,
        config: MQClientInstanceConfig,
    ) -> MQClientInstance {
        if let Some(existing) = MQClientInstance::find_instance(client_id) {
            return existing;
        }
        MQClientInstance::with_config(client_id, name_server_addrs, config)
    }

    /// `INSTANCE_MAP` 读侧（供 admin/测试按 clientId 找回实例）。
    pub fn find_instance(client_id: &str) -> Option<MQClientInstance> {
        let weak = MQClientInstance::instance_map()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(client_id)
            .cloned()?;
        weak.upgrade().map(|inner| MQClientInstance { inner })
    }

    pub fn client_id(&self) -> &str {
        &self.inner.client_id
    }

    pub fn name_server_addrs(&self) -> Vec<String> {
        self.inner
            .name_server_addrs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn config(&self) -> &MQClientInstanceConfig {
        &self.inner.config
    }

    pub fn remoting_client(&self) -> &RemotingClient {
        &self.inner.remoting_client
    }

    pub fn consumer_stats_manager(&self) -> &Arc<ConsumerStatsManager> {
        &self.inner.consumer_stats_manager
    }

    pub fn latency_fault_tolerance(&self) -> Option<&Arc<LatencyFaultToleranceImpl>> {
        self.inner.latency_fault_tolerance.as_ref()
    }

    pub fn trace_dispatcher(&self) -> Option<&Arc<dyn TraceDispatcher>> {
        self.inner.trace_dispatcher.as_ref()
    }

    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::Acquire)
    }

    // ---------------- 消费者注册（broker 主动请求按 group 分派） ----------------

    pub fn register_consumer(&self, group: &str, consumer: Arc<dyn RegisteredConsumer>) {
        self.inner
            .consumer_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(group.to_string(), consumer);
    }

    pub fn unregister_consumer(&self, group: &str) {
        self.inner
            .consumer_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(group);
    }

    pub fn find_consumer(&self, group: &str) -> Option<Arc<dyn RegisteredConsumer>> {
        self.inner
            .consumer_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(group)
            .cloned()
    }

    fn consumers_snapshot(&self) -> Vec<Arc<dyn RegisteredConsumer>> {
        self.inner
            .consumer_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }

    // ---------------- 生产者 / 拉模式消费者登记（只服务于关闭守卫） ----------------

    pub fn register_producer(&self, group: &str) {
        self.inner
            .producer_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(group.to_string());
    }

    pub fn unregister_producer(&self, group: &str) {
        self.inner
            .producer_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(group);
    }

    pub fn has_producer(&self, group: &str) -> bool {
        self.inner
            .producer_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(group)
    }

    pub fn register_consumer_group(&self, group: &str) {
        self.inner
            .consumer_group_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(group.to_string());
    }

    pub fn unregister_consumer_group(&self, group: &str) {
        self.inner
            .consumer_group_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(group);
    }

    pub fn has_consumer_group(&self, group: &str) -> bool {
        self.inner
            .consumer_group_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(group)
    }

    fn tenant_count(&self) -> usize {
        let consumers = self
            .inner
            .consumer_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len();
        let groups = self
            .inner
            .consumer_group_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len();
        let producers = self
            .inner
            .producer_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len();
        consumers + groups + producers
    }

    pub fn rebalance_immediately(&self) {
        for consumer in self.consumers_snapshot() {
            consumer.rebalance_immediately();
        }
    }

    /// 收到过多少次 broker 的 `NOTIFY_CONSUMER_IDS_CHANGED(40)`。
    /// 见 [`Inner::consumer_ids_changed_count`]：反向推送只有 broker 发得出来，
    /// 真机用例需要一个可断言的落点。
    pub fn consumer_ids_changed_count(&self) -> usize {
        self.inner.consumer_ids_changed_count.load(Ordering::SeqCst)
    }

    // ---------------- broker 主动请求处理（ClientRemotingProcessor） ----------------

    pub(crate) fn process_reset_offset(&self, cmd: &RemotingCommand) {
        let mut header = ResetOffsetRequestHeader::default();
        header.from_ext_fields(cmd.ext_fields());
        let group = header.group.clone();
        let consumer = group.as_ref().and_then(|g| self.find_consumer(g));
        let Some(consumer) = consumer else {
            rmq_warn!("RESET_CONSUMER_CLIENT_OFFSET: no consumer for group={group:?}");
            return;
        };
        let offset_table = match cmd.body() {
            Some(bytes) if !bytes.is_empty() => match parse_reset_offset_table(bytes) {
                Ok(table) => table,
                Err(e) => {
                    rmq_warn!("RESET_CONSUMER_CLIENT_OFFSET: bad body: {e}");
                    return;
                }
            },
            _ => Vec::new(),
        };
        let topic = header.topic.unwrap_or_default();
        // 220 的 rebalance 会 invokeSync，不能卡在 remoting 读线程：丢后台任务，
        // 立即回 None（oneway）。
        let task = consumer.reset_offset(topic.clone(), offset_table);
        // 220 只可能在 remoting 读线程里到达，那里必有运行时；真取不到时
        // 宁愿丢任务也不能 panic。
        let Some(handle) = self.inner.remoting_client.runtime_handle() else {
            rmq_warn!(
                "RESET_CONSUMER_CLIENT_OFFSET: no tokio runtime, reset skipped \
                 (group={group:?} topic={topic})"
            );
            return;
        };
        handle.spawn(async move {
            if let Err(e) = task.await {
                rmq_warn!("reset offset failed (group={group:?} topic={topic}): {e}");
            }
        });
    }

    pub(crate) fn process_notify_consumer_ids_changed(&self, cmd: &RemotingCommand, addr: &str) {
        let mut header = NotifyConsumerIdsChangedRequestHeader::default();
        header.from_ext_fields(cmd.ext_fields());
        let group = header.consumer_group.clone().unwrap_or_default();
        self.inner
            .consumer_ids_changed_count
            .fetch_add(1, Ordering::SeqCst);
        rmq_info!(
            "receive broker's notification[{addr}], the consumer group: {group} changed, \
             rebalance immediately"
        );
        self.rebalance_immediately();
    }

    pub(crate) fn process_get_consumer_status(&self, cmd: &RemotingCommand) -> RemotingCommand {
        let mut header = GetConsumerStatusRequestHeader::default();
        header.from_ext_fields(cmd.ext_fields());
        let group = header.group.clone().unwrap_or_default();
        let Some(consumer) = self.find_consumer(&group) else {
            return RemotingCommand::create_response(
                response_code::SYSTEM_ERROR,
                Some(format!("no consumer for group={group}")),
            );
        };
        let status = consumer.get_consumer_status(header.topic.as_deref());
        let body = GetConsumerStatusBody {
            message_queue_table: status
                .into_iter()
                .map(|(mq, off)| {
                    (
                        MessageQueueKey::new(&mq.topic, &mq.broker_name, mq.queue_id),
                        off,
                    )
                })
                .collect(),
            consumer_table: Vec::new(),
        };
        let mut resp = RemotingCommand::create_response(response_code::SUCCESS, None);
        resp.set_body(Some(body.encode()));
        resp
    }

    pub(crate) fn process_get_consumer_running_info(
        &self,
        cmd: &RemotingCommand,
    ) -> RemotingCommand {
        let mut header = GetConsumerRunningInfoRequestHeader::default();
        header.from_ext_fields(cmd.ext_fields());
        let group = header.consumer_group.clone().unwrap_or_default();
        let Some(consumer) = self.find_consumer(&group) else {
            return RemotingCommand::create_response(
                response_code::SYSTEM_ERROR,
                Some(format!("no consumer for group={group}")),
            );
        };
        let info = consumer.consumer_running_info();
        let mut resp = RemotingCommand::create_response(response_code::SUCCESS, None);
        resp.set_body(Some(info.encode()));
        resp
    }

    pub(crate) fn process_consume_message_directly(
        &self,
        cmd: &RemotingCommand,
    ) -> RemotingCommand {
        let mut header = ConsumeMessageDirectlyResultRequestHeader::default();
        header.from_ext_fields(cmd.ext_fields());
        let group = header.consumer_group.clone().unwrap_or_default();
        let Some(consumer) = self.find_consumer(&group) else {
            return RemotingCommand::create_response(
                response_code::SYSTEM_ERROR,
                Some(format!("no consumer for group={group}")),
            );
        };
        let Some(raw) = cmd.body().filter(|b| !b.is_empty()) else {
            return RemotingCommand::create_response(
                response_code::SYSTEM_ERROR,
                Some("empty message body".to_string()),
            );
        };
        // 解码不校验 CRC（默认 options 的
        // check_crc 本来就是 false，见 message_decoder::DecodeOptions::default）。
        let msg = match decode_message(raw) {
            Ok(msg) => msg,
            Err(_) => {
                return RemotingCommand::create_response(
                    response_code::SYSTEM_ERROR,
                    Some("decode message failed".to_string()),
                )
            }
        };
        match consumer.consume_message_directly(msg, header.broker_name.clone()) {
            Ok(result) => {
                let mut resp = RemotingCommand::create_response(response_code::SUCCESS, None);
                resp.set_body(Some(result.encode()));
                resp
            }
            Err(e) => RemotingCommand::create_response(
                response_code::SYSTEM_ERROR,
                Some(format!("consume message directly failed: {e}")),
            ),
        }
    }

    /// 326 处理：把应答投给等待中的 request()。
    pub(crate) fn process_reply_message(&self, cmd: &RemotingCommand) -> RemotingCommand {
        let header: ReplyMessageRequestHeader = match cmd.decode_command_custom_header() {
            Ok(h) => h,
            Err(e) => {
                return RemotingCommand::create_response(
                    response_code::SYSTEM_ERROR,
                    Some(format!("process reply message fail: {e}")),
                )
            }
        };
        match build_reply_message_ext(&header, cmd.body()) {
            Ok(msg) => {
                let correlation_id = msg
                    .get_property(PROPERTY_CORRELATION_ID)
                    .map(|s| s.to_string());
                if request_future_holder()
                    .put_response(correlation_id.as_deref(), msg)
                    .is_none()
                {
                    rmq_warn!(
                        "receive reply message, but not matched any request, CorrelationId: {:?}, reply from host: {:?}",
                        correlation_id,
                        header.born_host
                    );
                }
                RemotingCommand::create_response(response_code::SUCCESS, None)
            }
            Err(e) => {
                rmq_warn!("unknown err when receiveReplyMsg: {e}");
                RemotingCommand::create_response(
                    response_code::SYSTEM_ERROR,
                    Some(format!("process reply message fail: {e}")),
                )
            }
        }
    }

    // ---------------- 生命周期 ----------------

    /// 置位 started、启动统计采样、（可选）动态 namesrv 刷新，
    /// 然后拉起 4 个后台任务。动态取址拿不到地址时返回
    /// `Err`。
    pub async fn start(&self) -> Result<()> {
        if self
            .inner
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let _ = self.inner.stop.send(false);
        self.inner.remoting_client.start();
        if let Err(e) = self.inner.consumer_stats_manager.start() {
            self.shutdown_factory();
            return Err(e);
        }
        let dynamic_ns = {
            let addrs = self.name_server_addrs();
            let ta = self.inner.top_addressing.lock().await;
            addrs.is_empty() && !ta.ws_addr().is_empty()
        };
        if dynamic_ns {
            if let Err(e) = self.fetch_name_server_addr().await {
                self.shutdown_factory();
                return Err(e);
            }
            if self.name_server_addrs().is_empty() {
                let ws_addr = self.inner.top_addressing.lock().await.ws_addr().to_string();
                self.shutdown_factory();
                return Err(Error::client_with_code(
                    client_error_code::NO_NAME_SERVER_EXCEPTION,
                    format!("name server address is not set and address server ({ws_addr}) returned none"),
                ));
            }
            self.spawn_periodic(
                "namesrv-refresh",
                |me| async move {
                    if let Err(e) = me.fetch_name_server_addr().await {
                        rmq_debug!("fetchNameServerAddr exception: {e}");
                    }
                },
                self.inner.config.namesrv_refresh_initial_delay_millis,
                self.inner.config.namesrv_refresh_interval_millis,
            );
        }
        // 路由刷新循环：首个 10ms、周期 route_refresh_interval
        let me = self.clone();
        let handle = tokio::spawn(async move {
            me.route_refresh_loop().await;
        });
        self.push_task(handle);
        self.spawn_periodic(
            "adjust-pool",
            |me| async move { me.adjust_thread_pool() },
            self.inner.config.adjust_pool_initial_delay_millis,
            self.inner.config.adjust_pool_interval_millis,
        );
        self.spawn_periodic(
            "heartbeat",
            |me| async move {
                me.send_heartbeat_to_all_broker(5000).await;
            },
            self.inner.config.heartbeat_initial_delay_millis,
            self.inner.config.heartbeat_interval_millis,
        );
        self.spawn_periodic(
            "persist-offset",
            |me| async move {
                me.persist_consumer_offsets().await;
            },
            self.inner.config.persist_offset_initial_delay_millis,
            self.inner.config.persist_offset_interval_millis,
        );
        Ok(())
    }

    pub fn shutdown(&self) {
        let tenants = self.tenant_count();
        if tenants > 0 {
            rmq_debug!(
                "client factory [{}] still has {tenants} client(s) registered, skip shutdown",
                self.inner.client_id
            );
            return;
        }
        self.shutdown_factory();
    }

    pub fn detach_from_registry_if_last_tenant(&self) {
        if self.tenant_count() == 0 {
            self.remove_from_instance_map();
        }
    }

    fn shutdown_factory(&self) {
        self.inner.started.store(false, Ordering::Release);
        let _ = self.inner.stop.send(true);
        let handles: Vec<JoinHandle<()>> = {
            let mut tasks = self.inner.tasks.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *tasks)
        };
        for handle in handles {
            handle.abort();
        }
        self.inner.consumer_stats_manager.shutdown();
        self.inner.remoting_client.shutdown();
        self.remove_from_instance_map();
    }

    fn remove_from_instance_map(&self) {
        let mut map = MQClientInstance::instance_map()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let alive = map
            .get(self.inner.client_id.as_str())
            .and_then(|weak| weak.upgrade())
            .is_some_and(|strong| Arc::ptr_eq(&strong, &self.inner));
        if alive {
            map.remove(self.inner.client_id.as_str());
        }
    }

    fn push_task(&self, handle: JoinHandle<()>) {
        self.inner
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(handle);
    }

    /// 通用周期任务：initial 延迟 → 循环（work → sleep ∥ stop 信号），每轮先查 started。
    fn spawn_periodic<F, Fut>(
        &self,
        _name: &str,
        work: F,
        initial_delay_millis: u64,
        period_millis: u64,
    ) where
        F: Fn(MQClientInstance) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let me = self.clone();
        let handle = tokio::spawn(async move {
            let mut next = tokio::time::Instant::now()
                + std::time::Duration::from_millis(initial_delay_millis);
            loop {
                if wait_until_or_stop(&me.inner.stop, next).await {
                    return;
                }
                next += std::time::Duration::from_millis(period_millis);
                if !me.is_started() {
                    return;
                }
                work(me.clone()).await;
            }
        });
        self.push_task(handle);
    }

    async fn route_refresh_loop(&self) {
        let period = self.inner.config.route_refresh_interval_millis;
        let mut next = tokio::time::Instant::now() + std::time::Duration::from_millis(10);
        loop {
            if wait_until_or_stop(&self.inner.stop, next).await {
                return;
            }
            next += std::time::Duration::from_millis(period);
            if !self.is_started() {
                return;
            }
            let topics: Vec<String> = self
                .inner
                .topics_in_use
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .cloned()
                .collect();
            for topic in topics {
                if let Err(e) = self
                    .update_topic_route_info_from_name_server(&topic, 5000, false)
                    .await
                {
                    rmq_debug!("route refresh failed for {topic}: {e}");
                }
            }
        }
    }

    // ---------------- 动态 namesrv / 弹性巡检 ----------------

    pub async fn fetch_name_server_addr(&self) -> Result<Option<String>> {
        let mut ta = self.inner.top_addressing.lock().await;
        if ta.ws_addr().is_empty() {
            return Ok(None);
        }
        let changed = ta.fetch_and_apply().await;
        drop(ta);
        if let Some(text) = &changed {
            self.apply_namesrv_change(text);
        }
        Ok(changed)
    }

    fn apply_namesrv_change(&self, text: &str) {
        let addrs: Vec<String> = text
            .split(';')
            .map(|a| a.trim().to_string())
            .filter(|a| !a.is_empty())
            .collect();
        self.update_name_server_address_list(&addrs);
    }

    /// 更新 namesrv 地址列表：空列表不改。
    pub fn update_name_server_address_list(&self, addrs: &[String]) {
        if addrs.is_empty() {
            return;
        }
        *self
            .inner
            .name_server_addrs
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = addrs.to_vec();
    }

    pub fn adjust_thread_pool(&self) {
        for consumer in self.consumers_snapshot() {
            consumer.adjust_thread_pool();
        }
    }

    /// 登记需要后台周期刷新路由的 topic。
    pub fn register_topic_in_use(&self, topic: &str) {
        if !topic.is_empty() {
            self.inner
                .topics_in_use
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(topic.to_string());
        }
    }
}

// ================================================================ 实例级 RPC

impl MQClientInstance {
    // ---------------- 底层 invoke ----------------

    /// `remoting_client.invoke_sync` 的薄封装。
    pub(crate) async fn invoke_sync(
        &self,
        addr: &str,
        request: &mut RemotingCommand,
        timeout_millis: i64,
    ) -> Result<RemotingCommand> {
        self.inner
            .remoting_client
            .invoke_sync(addr, request, Some(timeout_millis))
            .await
    }

    /// 应答检查：非 SUCCESS 一律抛 `MQBrokerException`
    /// （remark 为空时用 `""`）。
    pub(crate) fn check_response(response: &RemotingCommand) -> Result<&RemotingCommand> {
        if response.code == response_code::SUCCESS {
            return Ok(response);
        }
        Err(Error::Broker {
            response_code: response.code,
            message: response.remark.clone().unwrap_or_default(),
        })
    }

    // ---------------- 路由管理 ----------------

    /// 拉取并落库 topic 路由。
    pub async fn update_topic_route_info_from_name_server(
        &self,
        topic: &str,
        timeout_millis: i64,
        is_default: bool,
    ) -> Result<bool> {
        let addrs = self.name_server_addrs();
        if addrs.is_empty() {
            return Err(Error::client_with_code(
                client_error_code::NO_NAME_SERVER_EXCEPTION,
                "name server address list is empty",
            ));
        }
        let mut route = self
            .fetch_topic_route_from_namesrv(topic, timeout_millis, &addrs)
            .await?;
        if route.is_none() && is_default && topic != MixAll::DEFAULT_TOPIC {
            route = self
                .fetch_topic_route_from_namesrv(MixAll::DEFAULT_TOPIC, timeout_millis, &addrs)
                .await?;
            if let Some(data) = route.as_mut() {
                // 新 topic 由 broker 用 default_topic_queue_nums 创建队列，而默认 topic 自身
                // 可能配置了更多队列，这里按 broker 实际创建数裁剪，避免选中非法 queueId。
                let cap = MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
                for qd in data.queue_datas.iter_mut() {
                    qd.write_queue_nums = qd.write_queue_nums.min(cap);
                    qd.read_queue_nums = qd.read_queue_nums.min(cap);
                }
            }
        }
        let Some(route) = route else {
            return Ok(false);
        };
        let publish = {
            let mut tables = self.inner.tables.lock().unwrap_or_else(|e| e.into_inner());
            tables
                .topic_route_table
                .insert(topic.to_string(), route.clone());
            for bd in route.get_broker_datas() {
                tables.broker_addr_table.insert(
                    bd.broker_name.clone(),
                    bd.broker_addrs.iter().cloned().collect(),
                );
            }
            match crate::client::static_topic::topic_route_data_2_endpoints_for_static_topic(
                topic, &route,
            ) {
                Some(endpoints) => {
                    tables.topic_end_points_table.insert(topic.to_string(), endpoints);
                }
                None => {
                    tables.topic_end_points_table.remove(topic);
                }
            }
            tables
                .topic_publish_info_table
                .entry(topic.to_string())
                .or_insert_with(|| Arc::new(TopicPublishInfo::new()))
                .clone()
        };
        // publish 的三个字段要整批刷新：
        // `TopicPublishInfo` 自带细粒度锁，先出表锁再整体刷新（避免嵌套锁）。
        publish.update_from_route(&route, topic);
        Ok(true)
    }

    /// 逐个 namesrv 试；**拿到响应但不可用就 break**
    /// （不再试后续 namesrv），只有传输/解码异常才继续下一个；
    /// 循环结束后「最后一个异常」若不是 broker 业务错就原样抛出。
    ///
    /// remoting 层按 `opaque` 配对响应，故每次尝试新建请求
    /// （每次 `opaque` 递增，语义不变）。
    async fn fetch_topic_route_from_namesrv(
        &self,
        topic: &str,
        timeout_millis: i64,
        addrs: &[String],
    ) -> Result<Option<TopicRouteData>> {
        let mut last_exc: Option<Error> = None;
        for ns_addr in addrs {
            let mut request =
                RemotingCommand::create_request_command(request_code::GET_ROUTEINFO_BY_TOPIC, None);
            request.add_ext_field("topic", topic);
            match self
                .invoke_sync(ns_addr, &mut request, timeout_millis)
                .await
            {
                Ok(response) => {
                    if response.code != response_code::SUCCESS {
                        break;
                    }
                    match response.body() {
                        // 解码失败算 last_exc 并继续下一个 namesrv
                        Some(body) if !body.is_empty() => match TopicRouteData::decode(body) {
                            Ok(data) => return Ok(Some(data)),
                            Err(e) => last_exc = Some(e),
                        },
                        // SUCCESS 但没 body：这里同样 break（不再试后续 namesrv）
                        _ => break,
                    }
                }
                Err(e) => last_exc = Some(e),
            }
        }
        if let Some(e) = last_exc {
            // 最后一个异常不是 broker 业务错才原样抛出
            if !matches!(e, Error::Broker { .. }) {
                return Err(e);
            }
        }
        Ok(None)
    }

    /// 缓存可用直接返回，否则拉一次路由；
    /// 仍拿不到队列 ⇒ `Can not find Message Queue for topic: <topic>`。
    pub async fn get_topic_publish_info(
        &self,
        topic: &str,
        is_default: bool,
    ) -> Result<Arc<TopicPublishInfo>> {
        if let Some(info) = self.publish_info_of(topic) {
            if info.ok() {
                return Ok(info);
            }
        }
        self.update_topic_route_info_from_name_server(topic, 5000, is_default)
            .await?;
        match self.publish_info_of(topic) {
            Some(info) if info.ok() => Ok(info),
            _ => bail!("Can not find Message Queue for topic: {topic}"),
        }
    }

    /// 命中缓存即返回；否则拉一次路由（**吞掉异常**）后再读。
    pub async fn get_topic_route_data(&self, topic: &str) -> Option<TopicRouteData> {
        if let Some(route) = self.route_of(topic) {
            return Some(route);
        }
        if let Err(e) = self
            .update_topic_route_info_from_name_server(topic, 5000, false)
            .await
        {
            // 刷新失败吞掉异常，继续读缓存
            rmq_debug!("get_topic_route_data refresh failed for {topic}: {e}");
        }
        self.route_of(topic)
    }

    pub async fn get_topic_subscribe_info(&self, topic: &str) -> Vec<MessageQueue> {
        let Some(route) = self.get_topic_route_data(topic).await else {
            return Vec::new();
        };
        route
            .get_all_subscribe_message_queue(topic)
            .into_iter()
            .map(|q| MessageQueue::new(&q.topic, &q.broker_name, q.queue_id))
            .collect()
    }

    /// 只读缓存路由（不触发 RPC），未命中返回 `None`。
    pub(crate) fn route_of(&self, topic: &str) -> Option<TopicRouteData> {
        self.inner
            .tables
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .topic_route_table
            .get(topic)
            .cloned()
    }

    /// 只读缓存发布信息（不触发 RPC）。
    fn publish_info_of(&self, topic: &str) -> Option<Arc<TopicPublishInfo>> {
        self.inner
            .tables
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .topic_publish_info_table
            .get(topic)
            .cloned()
    }

    /// 在路由里找该 brokerName 的地址。
    pub fn find_broker_addr_in_route(route: &TopicRouteData, broker_name: &str) -> Option<String> {
        route
            .get_broker_datas()
            .iter()
            .find(|bd| bd.broker_name == broker_name)
            .and_then(|bd| bd.select_broker_addr())
    }

    // ---------------- 消息发送 ----------------

    /// 按路由解析 broker 地址后同步发送。
    #[allow(clippy::too_many_arguments)]
    pub async fn send_message(
        &self,
        producer_group: &str,
        msg: &mut PublishMessage<'_>,
        mq: &MessageQueue,
        timeout_millis: i64,
        sys_flag: i32,
        unit_mode: bool,
        create_topic_key: &str,
        default_topic_queue_nums: i32,
    ) -> Result<SendResult> {
        // 查发布地址（只认 master）
        // → 查不到刷一次路由 → 仍查不到照 `sendKernelImpl:1100` 报
        // 「The broker[X] not exist」。**不是** `broker_addr_in_route`（那个允许落到从节点，
        // 主从切换期间会白挨一轮 SYSTEM_BUSY(2)）。
        let addr = self.publish_addr_for(&mq.broker_name, &mq.topic).await?;
        self.send_message_to_addr(
            producer_group,
            msg,
            mq,
            &addr,
            timeout_millis,
            sys_flag,
            unit_mode,
            create_topic_key,
            default_topic_queue_nums,
        )
        .await
    }

    /// 向指定地址直发。
    #[allow(clippy::too_many_arguments)]
    pub async fn send_message_to_addr(
        &self,
        producer_group: &str,
        msg: &mut PublishMessage<'_>,
        mq: &MessageQueue,
        addr: &str,
        timeout_millis: i64,
        sys_flag: i32,
        unit_mode: bool,
        create_topic_key: &str,
        default_topic_queue_nums: i32,
    ) -> Result<SendResult> {
        let mut request = Self::build_send_request(
            producer_group,
            msg,
            mq,
            sys_flag,
            unit_mode,
            create_topic_key,
            default_topic_queue_nums,
        );
        let response = self.invoke_sync(addr, &mut request, timeout_millis).await?;
        Self::parse_send_response(&response, msg.as_message(), mq)
    }

    /// `mark_oneway_rpc` 后 fire-and-forget。
    ///
    /// `timeout_millis` 在 oneway 里没有语义（不等响应），
    /// 本方法签名省略该参数。
    #[allow(clippy::too_many_arguments)]
    pub async fn send_message_oneway(
        &self,
        producer_group: &str,
        msg: &mut PublishMessage<'_>,
        mq: &MessageQueue,
        addr: &str,
        sys_flag: i32,
        unit_mode: bool,
        create_topic_key: &str,
        default_topic_queue_nums: i32,
    ) -> Result<()> {
        let mut request = Self::build_send_request(
            producer_group,
            msg,
            mq,
            sys_flag,
            unit_mode,
            create_topic_key,
            default_topic_queue_nums,
        );
        request.mark_oneway_rpc();
        self.inner
            .remoting_client
            .invoke_oneway(addr, &mut request)
            .await
    }

    /// `on_complete` 恰好回调一次（由 remoting 层保证）。`msg` 只被
    /// [`parse_send_response`](Self::parse_send_response) 用来取 UNIQ_KEY，
    /// body 已经在 request 里编好了，调用方可以传一份**去掉 body** 的副本。
    pub(crate) fn send_message_async(
        &self,
        addr: &str,
        request: RemotingCommand,
        msg: Arc<Message>,
        mq: Arc<MessageQueue>,
        timeout_millis: i64,
        on_complete: Box<dyn FnOnce(Result<SendResult>) + Send + 'static>,
    ) {
        self.inner.remoting_client.invoke_async(
            addr,
            request,
            Box::new(move |response| match response {
                Ok(response) => on_complete(Self::parse_send_response(&response, &msg, &mq)),
                Err(e) => on_complete(Err(e)),
            }),
            Some(timeout_millis),
        );
    }

    /// 组装 SEND_MESSAGE_V2 请求（V2 短字段头，`sendSmartMsg` 恒 true）。
    pub(crate) fn build_send_request(
        producer_group: &str,
        msg: &mut PublishMessage<'_>,
        mq: &MessageQueue,
        sys_flag: i32,
        unit_mode: bool,
        create_topic_key: &str,
        default_topic_queue_nums: i32,
    ) -> RemotingCommand {
        if !msg.is_batch() {
            set_uniq_id(msg.as_message_mut());
        }
        let outer = msg.as_message();
        let (reconsume_times, max_reconsume_times) = if MixAll::is_retry_topic(Some(&outer.topic)) {
            (
                outer
                    .properties
                    .get(PROPERTY_RECONSUME_TIME)
                    .and_then(|v| v.parse::<i32>().ok())
                    .unwrap_or(0),
                outer
                    .properties
                    .get(PROPERTY_MAX_RECONSUME_TIMES)
                    .and_then(|v| v.parse::<i32>().ok()),
            )
        } else {
            (0, None)
        };
        let header = SendMessageRequestHeaderV2 {
            producer_group: Some(producer_group.to_string()),
            topic: Some(outer.topic.clone()),
            default_topic: Some(if create_topic_key.is_empty() {
                MixAll::DEFAULT_TOPIC.to_string()
            } else {
                create_topic_key.to_string()
            }),
            default_topic_queue_nums: Some(default_topic_queue_nums),
            queue_id: Some(mq.queue_id),
            sys_flag: Some(sys_flag),
            born_timestamp: Some(current_time_millis()),
            flag: Some(outer.flag),
            properties: Some(message_properties_2_string(&outer.properties)),
            reconsume_times: Some(reconsume_times),
            unit_mode: Some(unit_mode),
            max_reconsume_times,
            batch: Some(msg.is_batch()),
            broker_name: if mq.broker_name.is_empty() {
                None
            } else {
                Some(mq.broker_name.clone())
            },
        };
        let code = if is_reply_message(outer) {
            request_code::SEND_REPLY_MESSAGE_V2
        } else if msg.is_batch() {
            request_code::SEND_BATCH_MESSAGE
        } else {
            request_code::SEND_MESSAGE_V2
        };
        let mut request = RemotingCommand::create_request_command(code, Some(Box::new(header)));
        request.set_body(Some(Self::encode_body(outer)));
        request
    }

    /// body 编码：普通消息是原始 body；
    /// 批量消息的 body 在构造时已由 `encode_messages` 编好。
    fn encode_body(msg: &Message) -> Vec<u8> {
        msg.body.clone().unwrap_or_default()
    }

    pub(crate) fn parse_send_response(
        response: &RemotingCommand,
        msg: &Message,
        mq: &MessageQueue,
    ) -> Result<SendResult> {
        let status = match response.code {
            response_code::SUCCESS => SendStatus::SendOk,
            response_code::FLUSH_DISK_TIMEOUT => SendStatus::FlushDiskTimeout,
            response_code::FLUSH_SLAVE_TIMEOUT => SendStatus::FlushSlaveTimeout,
            response_code::SLAVE_NOT_AVAILABLE => SendStatus::SlaveNotAvailable,
            _ => {
                return Err(Error::Broker {
                    response_code: response.code,
                    message: response.remark.clone().unwrap_or_default(),
                })
            }
        };
        let mut header = SendMessageResponseHeader::default();
        header.from_ext_fields(response.ext_fields());
        let uniq_id = get_uniq_id(msg)
            .filter(|s| !s.is_empty())
            .or_else(|| header.batch_uniq_id.clone().filter(|s| !s.is_empty()));
        Ok(SendResult {
            status,
            msg_id: uniq_id.or_else(|| header.msg_id.clone()),
            message_queue: Some(MessageQueue::new(
                &mq.topic,
                &mq.broker_name,
                header.queue_id.unwrap_or(mq.queue_id),
            )),
            queue_offset: header.queue_offset.unwrap_or(0),
            transaction_id: header.transaction_id.clone(),
            offset_msg_id: header.msg_id.clone(),
            recall_handle: header.recall_handle.clone(),
            region_id: Some(
                response
                    .get_ext_field(PROPERTY_MSG_REGION)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(MixAll::DEFAULT_TRACE_REGION_ID)
                    .to_string(),
            ),
            // 只有字面量 "false" 算关闭：缺字段/空串都算开启
            trace_on: match response.get_ext_field(PROPERTY_TRACE_SWITCH) {
                Some(value) => value != "false",
                None => true,
            },
        })
    }

    // ---------------- 定时消息撤回（370） ----------------

    pub async fn recall_message(
        &self,
        addr: &str,
        header: RecallMessageRequestHeader,
        timeout_millis: i64,
    ) -> Result<String> {
        let mut request = RemotingCommand::create_request_command(
            request_code::RECALL_MESSAGE,
            Some(Box::new(header)),
        );
        let response = self.invoke_sync(addr, &mut request, timeout_millis).await?;
        Self::check_response(&response)?;
        let mut resp_header = RecallMessageResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        resp_header
            .msg_id
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                Error::client(format!("recall message response has no msgId, addr {addr}"))
            })
    }

    // ---------------- 拉取 ----------------

    /// 拉取请求（PULL_MESSAGE），参数逐一落进请求头字段。
    #[allow(clippy::too_many_arguments)]
    pub async fn pull_message(
        &self,
        consumer_group: &str,
        mq: &MessageQueue,
        queue_offset: i64,
        max_msg_nums: i32,
        sys_flag: i32,
        commit_offset: i64,
        subscription: &str,
        sub_version: i64,
        expression_type: &str,
        timeout_millis: i64,
        max_msg_bytes: i32,
        suspend_timeout_millis: i64,
        addr: Option<&str>,
        request_source: i32,
        broker_id: Option<i64>,
    ) -> Result<PullResult> {
        let mut sys_flag = sys_flag;
        let addr = match addr {
            Some(a) => a.to_string(),
            None => {
                let route = self.get_topic_route_data(&mq.topic).await.ok_or_else(|| {
                    Error::client(format!("No route info of this topic: {}", mq.topic))
                })?;
                match broker_id {
                    Some(id) => {
                        let broker_data = route
                            .get_broker_datas()
                            .iter()
                            .find(|bd| bd.broker_name == mq.broker_name)
                            .ok_or_else(|| {
                                Error::client(format!("Broker {} not exist", mq.broker_name))
                            })?;
                        let (chosen, slave) = Self::find_broker_addr_in_subscribe(
                            &broker_data.broker_addrs,
                            id,
                            false,
                        )
                        .ok_or_else(|| {
                            Error::client(format!("Broker {} not exist", mq.broker_name))
                        })?;
                        if slave {
                            sys_flag = PullSysFlag::clear_commit_offset_flag(sys_flag);
                        }
                        chosen
                    }
                    None => Self::find_broker_addr_in_route(&route, &mq.broker_name).ok_or_else(
                        || {
                            Error::client(format!(
                                "Broker {} not found in route of topic {}",
                                mq.broker_name, mq.topic
                            ))
                        },
                    )?,
                }
            }
        };
        let header = PullMessageRequestHeader {
            consumer_group: Some(consumer_group.to_string()),
            topic: Some(mq.topic.clone()),
            lite_topic: None,
            queue_id: Some(mq.queue_id),
            queue_offset: Some(queue_offset),
            max_msg_nums: Some(max_msg_nums),
            sys_flag: Some(sys_flag),
            commit_offset: Some(commit_offset),
            suspend_timeout_millis: Some(suspend_timeout_millis),
            subscription: if PullSysFlag::has_subscription_flag(sys_flag) {
                Some(subscription.to_string())
            } else {
                None
            },
            sub_version: Some(sub_version),
            expression_type: Some(expression_type.to_string()),
            max_msg_bytes: Some(max_msg_bytes),
            request_source: Some(request_source),
            proxy_froward_client_id: None,
        };
        let code = if PullSysFlag::has_lite_pull_flag(sys_flag) {
            request_code::LITE_PULL_MESSAGE
        } else {
            request_code::PULL_MESSAGE
        };
        let mut request = RemotingCommand::create_request_command(code, Some(Box::new(header)));
        let response = self
            .invoke_sync(&addr, &mut request, timeout_millis)
            .await?;
        let status = match response.code {
            response_code::SUCCESS => PullStatus::Found,
            response_code::PULL_NOT_FOUND => PullStatus::NoNewMsg,
            response_code::PULL_OFFSET_MOVED => PullStatus::OffsetIllegal,
            response_code::PULL_RETRY_IMMEDIATELY => PullStatus::NoMatchedMsg,
            _ => {
                return Err(Error::Broker {
                    response_code: response.code,
                    message: response.remark.clone().unwrap_or_default(),
                })
            }
        };
        let mut resp_header = PullMessageResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        let mut found = response.body().map(decode_messages).unwrap_or_default();
        // 无条件盖 brokerName/queueId
        for m in found.iter_mut() {
            m.broker_name = Some(mq.broker_name.clone());
            m.queue_id = mq.queue_id;
        }
        Ok(PullResult {
            status,
            next_begin_offset: resp_header.next_begin_offset.unwrap_or(0),
            min_offset: resp_header.min_offset.unwrap_or(0),
            max_offset: resp_header.max_offset.unwrap_or(0),
            msg_found_list: found,
            suggest_which_broker_id: resp_header.suggest_which_broker_id,
        })
    }

    pub(crate) fn find_broker_addr_in_subscribe(
        broker_addrs: &[(i64, String)],
        broker_id: i64,
        only_this_broker: bool,
    ) -> Option<(String, bool)> {
        if broker_addrs.is_empty() {
            return None;
        }
        let master_id = MixAll::MASTER_ID as i64;
        if let Some((_, addr)) = broker_addrs.iter().find(|(id, _)| *id == broker_id) {
            return Some((addr.clone(), broker_id != master_id));
        }
        if broker_id != master_id {
            if let Some((_, addr)) = broker_addrs.iter().find(|(id, _)| *id == broker_id + 1) {
                return Some((addr.clone(), true));
            }
        }
        if !only_this_broker {
            let min = broker_addrs.iter().min_by_key(|(id, _)| *id)?;
            return Some((min.1.clone(), min.0 != master_id));
        }
        None
    }
}

// ================================================================ POP / ACK / 不可见时间

impl MQClientInstance {
    /// `pop_message` 的 addr / brokerName 解析段。
    async fn pop_target(
        &self,
        topic: &str,
        broker_name: Option<&str>,
        addr: Option<&str>,
    ) -> Result<(String, String)> {
        let broker_name = broker_name.filter(|s| !s.is_empty());
        let broker_name = match broker_name {
            Some(b) => b.to_string(),
            None => {
                let route = self.get_topic_route_data(topic).await.ok_or_else(|| {
                    Error::client(format!("No route info of this topic: {topic}"))
                })?;
                let Some(bd) = route.get_broker_datas().first() else {
                    bail!("No broker in route of topic: {topic}");
                };
                // 先按路由挑一台，挑完照样只认它的 master
                bd.broker_name.clone()
            }
        };
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.publish_addr_for(&broker_name, topic).await?,
        };
        Ok((addr, broker_name))
    }

    /// POP 弹取（POP_MESSAGE = 200050）。
    ///
    /// 与 pull 的语义差别：**不需要提交位点**，消费完成用
    /// [`ack_message`] 确认；不 ack 的消息在 `invisible_time` 后被 broker 复活重投到
    /// `%RETRY%<group>_<topic>`（V1），下次 POP 再弹回来 —— 至少一次语义；
    /// `queue_id = -1` 表示弹该 topic 的所有队列。
    ///
    /// `born_time` 必须是当前毫秒时间戳：broker 用 `now - bornTime - pollTime > 500`
    /// 判定「超时太久」并直接回 POLLING_TIMEOUT(210)，填 0 会必然失败。
    #[allow(clippy::too_many_arguments)]
    pub async fn pop_message(
        &self,
        consumer_group: &str,
        topic: &str,
        queue_id: i32,
        max_msg_nums: i32,
        invisible_time: i64,
        poll_time: i64,
        init_mode: i32,
        exp: Option<&str>,
        exp_type: Option<&str>,
        order: bool,
        broker_name: Option<&str>,
        timeout_millis: i64,
        addr: Option<&str>,
    ) -> Result<PopResult> {
        let (addr, broker_name) = self.pop_target(topic, broker_name, addr).await?;
        let header = PopMessageRequestHeader {
            consumer_group: Some(consumer_group.to_string()),
            topic: Some(topic.to_string()),
            queue_id: Some(queue_id),
            max_msg_nums: Some(max_msg_nums),
            invisible_time: Some(invisible_time),
            poll_time: Some(poll_time),
            born_time: Some(current_time_millis()),
            init_mode: Some(init_mode),
            exp_type: exp_type.map(|s| s.to_string()),
            exp: exp.map(|s| s.to_string()),
            order,
            attempt_id: None,
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::POP_MESSAGE,
            Some(Box::new(header)),
        );
        let response = self
            .invoke_sync(&addr, &mut request, timeout_millis)
            .await?;
        let status = match response.code {
            response_code::SUCCESS => PopStatus::Found,
            response_code::POLLING_FULL => PopStatus::PollingFull,
            response_code::POLLING_TIMEOUT | response_code::PULL_NOT_FOUND => {
                PopStatus::PollingNotFound
            }
            _ => {
                return Err(Error::Broker {
                    response_code: response.code,
                    message: response.remark.clone().unwrap_or_default(),
                })
            }
        };
        let mut resp_header = PopMessageResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        let mut found = if status == PopStatus::Found {
            response.body().map(decode_messages).unwrap_or_default()
        } else {
            Vec::new()
        };
        if !found.is_empty() {
            Self::stamp_pop_ck(&mut found, &broker_name, &resp_header)?;
        }
        for m in found.iter_mut() {
            m.broker_name = Some(broker_name.clone());
            m.topic = topic.to_string();
        }
        Ok(PopResult {
            status,
            msg_found_list: found,
            rest_num: resp_header.rest_num.unwrap_or(0),
            pop_time: resp_header.pop_time.unwrap_or(0),
            invisible_time: resp_header.invisible_time.unwrap_or(0),
            revive_qid: resp_header.revive_qid.unwrap_or(0),
            start_offset_info: resp_header.start_offset_info.clone(),
            msg_offset_info: resp_header.msg_offset_info.clone(),
            order_count_info: resp_header.order_count_info.clone(),
        })
    }

    /// 从消息的 `POP_CK` 构造队列映射键（模块路径直接用，无需额外参数）。
    pub fn pop_queue_map_key(msg: &MessageExt) -> Result<String> {
        let ck = msg
            .properties
            .get(PROPERTY_POP_CK)
            .filter(|ck| !ck.is_empty());
        match ck {
            Some(ck) => {
                let segments = extra_info::split(ck)?;
                Ok(format!(
                    "{}@{}",
                    extra_info::get_retry(&segments)?,
                    msg.queue_id
                ))
            }
            None => Ok(extra_info::get_start_offset_info_map_key(
                &msg.topic,
                i64::from(msg.queue_id),
            )),
        }
    }

    /// 给 POP 出来的消息反构 `POP_CK` 与 `1ST_POP_TIME`（关联函数，无实例状态）。
    pub(crate) fn stamp_pop_ck(
        found: &mut [MessageExt],
        broker_name: &str,
        resp_header: &PopMessageResponseHeader,
    ) -> Result<()> {
        let pop_time = resp_header.pop_time.unwrap_or(0);
        let invisible_time = resp_header.invisible_time.unwrap_or(0);
        let revive_qid = resp_header.revive_qid.unwrap_or(0);
        let start_offset_info = resp_header
            .start_offset_info
            .as_deref()
            .filter(|s| !s.is_empty());
        let Some(start_offset_info) = start_offset_info else {
            let mut per_queue: HashMap<String, String> = HashMap::new();
            for msg in found.iter_mut() {
                let key = format!("{}{}", msg.topic, msg.queue_id);
                let base = match per_queue.get(&key) {
                    Some(v) => v.clone(),
                    None => {
                        let v = extra_info::build_extra_info(
                            msg.queue_offset,
                            pop_time,
                            invisible_time,
                            revive_qid,
                            &msg.topic,
                            broker_name,
                            msg.queue_id,
                            None,
                        );
                        per_queue.insert(key, v.clone());
                        v
                    }
                };
                msg.properties.insert(
                    PROPERTY_POP_CK,
                    format!("{base}{KEY_SEPARATOR}{}", msg.queue_offset),
                );
            }
            return Self::stamp_first_pop_time(found, pop_time);
        };
        let start_map = extra_info::parse_start_offset_info(Some(start_offset_info))?;
        let msg_map = extra_info::parse_msg_offset_info(resp_header.msg_offset_info.as_deref())?;
        let mut sorted_offsets: HashMap<String, Vec<i64>> = HashMap::new();
        for msg in found.iter() {
            let key = Self::pop_queue_map_key(msg)?;
            sorted_offsets
                .entry(key)
                .or_default()
                .push(msg.queue_offset);
        }
        for offsets in sorted_offsets.values_mut() {
            offsets.sort_unstable();
        }
        for msg in found.iter_mut() {
            // retry topic 弹回来的消息 broker 已经写好 POP_CK，不能覆盖。
            if msg.properties.get(PROPERTY_POP_CK).is_some() {
                continue;
            }
            let key = Self::pop_queue_map_key(msg)?;
            let start_offset = start_map
                .as_ref()
                .and_then(|table| table.iter().find(|(k, _)| *k == key))
                .map(|(_, v)| *v);
            let offsets = msg_map
                .as_ref()
                .and_then(|table| table.iter().find(|(k, _)| *k == key))
                .map(|(_, v)| v);
            let (Some(start_offset), Some(offsets)) = (start_offset, offsets) else {
                continue;
            };
            if offsets.is_empty() {
                continue;
            }
            let index = match sorted_offsets
                .get(&key)
                .and_then(|list| list.iter().position(|o| *o == msg.queue_offset))
            {
                Some(index) => index,
                None => continue,
            };
            if index >= offsets.len() {
                continue;
            }
            msg.properties.insert(
                PROPERTY_POP_CK,
                extra_info::build_extra_info(
                    start_offset,
                    pop_time,
                    invisible_time,
                    revive_qid,
                    &msg.topic,
                    broker_name,
                    msg.queue_id,
                    Some(offsets[index]),
                ),
            );
        }
        Self::stamp_first_pop_time(found, pop_time)
    }

    fn stamp_first_pop_time(found: &mut [MessageExt], pop_time: i64) -> Result<()> {
        for msg in found.iter_mut() {
            if !msg.properties.contains_key(PROPERTY_FIRST_POP_TIME) {
                msg.properties
                    .insert(PROPERTY_FIRST_POP_TIME, pop_time.to_string());
            }
        }
        Ok(())
    }

    /// 确认一条 POP 消息（ACK_MESSAGE = 200051）。
    ///
    /// `extra_info` 用消息上的 `POP_CK` 属性；`offset` 必须是 **consumeQueue offset**
    /// （即 CK 串第 8 段 / msgQueueOffset），不是 commitlog offset —— 传错 broker 会回
    /// `NO_MESSAGE`。返回 broker 响应码，`SUCCESS` 即确认成功。
    #[allow(clippy::too_many_arguments)]
    pub async fn ack_message(
        &self,
        consumer_group: &str,
        topic: &str,
        queue_id: i32,
        extra_info: &str,
        offset: i64,
        broker_name: Option<&str>,
        timeout_millis: i64,
        addr: Option<&str>,
    ) -> Result<i32> {
        // 形参名与模块同名，局部起别名
        use crate::remoting::protocol::extra_info as extra;
        let broker_name = match broker_name {
            Some(name) => name.to_string(),
            None => extra::get_broker_name(&extra::split(extra_info)?)?,
        };
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.pop_ack_addr(&broker_name, topic, queue_id).await?,
        };
        let header = AckMessageRequestHeader {
            consumer_group: Some(consumer_group.to_string()),
            topic: Some(topic.to_string()),
            queue_id: Some(queue_id),
            extra_info: Some(extra_info.to_string()),
            offset: Some(offset),
            lite_topic: None,
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::ACK_MESSAGE,
            Some(Box::new(header)),
        );
        let response = self
            .invoke_sync(&addr, &mut request, timeout_millis)
            .await?;
        Ok(response.code)
    }

    /// 修改不可见时间（CHANGE_MESSAGE_INVISIBLETIME = 200053）。
    ///
    /// 用于「还在处理、别让 broker 复活重投」的场景。响应返回的是**新的**
    /// popTime / invisibleTime / reviveQid，客户端据此重建 8 段 extraInfo
    /// （放在结果的 `extra_info` 字段里）供后续 ACK 使用 —— 不是原来那个旧串。
    #[allow(clippy::too_many_arguments)]
    pub async fn change_invisible_time(
        &self,
        consumer_group: &str,
        topic: &str,
        queue_id: i32,
        extra_info: &str,
        offset: i64,
        invisible_time: i64,
        broker_name: Option<&str>,
        timeout_millis: i64,
        addr: Option<&str>,
    ) -> Result<ChangeInvisibleTimeResult> {
        use crate::remoting::protocol::extra_info as extra;
        let broker_name = match broker_name {
            Some(name) => name.to_string(),
            None => extra::get_broker_name(&extra::split(extra_info)?)?,
        };
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.pop_ack_addr(&broker_name, topic, queue_id).await?,
        };
        let header = ChangeInvisibleTimeRequestHeader {
            consumer_group: Some(consumer_group.to_string()),
            topic: Some(topic.to_string()),
            queue_id: Some(queue_id),
            extra_info: Some(extra_info.to_string()),
            offset: Some(offset),
            invisible_time: Some(invisible_time),
            lite_topic: None,
            suspend: false,
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::CHANGE_MESSAGE_INVISIBLETIME,
            Some(Box::new(header)),
        );
        let response = self
            .invoke_sync(&addr, &mut request, timeout_millis)
            .await?;
        let mut resp_header = ChangeInvisibleTimeResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        let pop_time = resp_header.pop_time.unwrap_or(0);
        let new_invisible_time = resp_header.invisible_time.unwrap_or(0);
        let revive_qid = resp_header.revive_qid.unwrap_or(0);
        let new_extra = if response.code == response_code::SUCCESS {
            Some(extra::build_extra_info(
                offset,
                pop_time,
                new_invisible_time,
                revive_qid,
                topic,
                &broker_name,
                queue_id,
                Some(offset),
            ))
        } else {
            None
        };
        Ok(ChangeInvisibleTimeResult {
            response_code: response.code,
            pop_time,
            invisible_time: new_invisible_time,
            revive_qid,
            extra_info: new_extra,
            success: response.code == response_code::SUCCESS,
        })
    }
}

// ================================================================ Offset / 查询 / 队列锁 / 心跳 / 管理

impl MQClientInstance {
    // ---------------- 地址工具 ----------------

    /// 按 topic（可选再按 brokerName）解析 broker 地址。
    /// brokerName 为空串时退化为「路由里第一台 broker」。
    pub(crate) async fn addr_for(&self, topic: &str, broker_name: Option<&str>) -> Result<String> {
        let route = self
            .get_topic_route_data(topic)
            .await
            .ok_or_else(|| Error::client(format!("No route info of this topic: {topic}")))?;
        if let Some(broker_name) = broker_name.filter(|name| !name.is_empty()) {
            return Self::find_broker_addr_in_route(&route, broker_name).ok_or_else(|| {
                Error::client(format!(
                    "Broker {broker_name} not found in route of topic {topic}"
                ))
            });
        }
        let Some(bd) = route.get_broker_datas().first() else {
            bail!("No broker in route of topic: {topic}");
        };
        bd.select_broker_addr()
            .ok_or_else(|| Error::client(format!("No available broker addr for topic: {topic}")))
    }

    /// 按 `mq.broker_name` 在该 topic 路由里查地址。
    pub(crate) async fn broker_addr(&self, mq: &MessageQueue) -> Result<String> {
        let route = self
            .get_topic_route_data(&mq.topic)
            .await
            .ok_or_else(|| Error::client(format!("No route info of this topic: {}", mq.topic)))?;
        Self::find_broker_addr_in_route(&route, &mq.broker_name).ok_or_else(|| {
            Error::client(format!(
                "Broker {} not found in route of topic {}",
                mq.broker_name, mq.topic
            ))
        })
    }

    /// 等价于 `addr_for(topic)`（两处代码相同）。
    pub(crate) async fn broker_addr_for_topic(&self, topic: &str) -> Result<String> {
        self.addr_for(topic, None).await
    }

    /// 在**所有**已缓存路由里找该 brokerName 的地址。
    pub fn broker_addr_of(&self, broker_name: &str) -> Option<String> {
        self.cached_routes()
            .into_iter()
            .find_map(|route| Self::find_broker_addr_in_route(&route, broker_name))
    }

    /// 返回 `None` 是正常结果（master 掉线就是这个形状），报什么错由调用方决定
    /// （`sendKernelImpl` 报 MQClientException、`endTransaction` 什么都不报）。
    pub fn find_broker_address_in_publish(&self, broker_name: &str) -> Option<String> {
        let tables = self.inner.tables.lock().unwrap_or_else(|e| e.into_inner());
        tables
            .broker_addr_table
            .get(broker_name)
            .and_then(|addrs| addrs.get(&(MixAll::MASTER_ID as i64)))
            .cloned()
    }

    pub(crate) fn pick_topic_for_broker(&self, broker_name: &str) -> Option<String> {
        let tables = self.inner.tables.lock().unwrap_or_else(|e| e.into_inner());
        tables
            .topic_route_table
            .iter()
            .find(|(_, route)| {
                route
                    .get_broker_datas()
                    .iter()
                    .any(|bd| bd.broker_name == broker_name)
            })
            .map(|(topic, _)| topic.clone())
    }

    /// 测试专用：把发布地址直接写进平表（等价于路由落库那一处做的一次刷新）。
    /// 离线用例没有名字服务，种不上这张表的话地址解析会在钩子之前就失败。
    #[cfg(test)]
    pub(crate) fn seed_publish_addr_for_test(&self, broker_name: &str, addr: &str) {
        let mut tables = self.inner.tables.lock().unwrap_or_else(|e| e.into_inner());
        tables.broker_addr_table.insert(
            broker_name.to_string(),
            HashMap::from([(MixAll::MASTER_ID as i64, addr.to_string())]),
        );
    }

    /// 对应 `DefaultMQProducerImpl.sendKernelImpl:919-924`（发送）、
    /// `DefaultMQPushConsumerImpl.changePopInvisibleTimeAsync:869-876` /
    /// `ackAsync`（POP 的 ack 与延长不可见时间走
    /// `findBrokerAddressInSubscribe(brokerName, MASTER_ID, true)`，只要主），
    /// 以及 `MQAdminImpl` 的 offset 查询（见 [`Self::publish_addr_in_admin`]）。
    /// 形状一致：查发布地址（**只认 brokerId=0**）→ 查不到按 topic 刷一次路由 →
    /// 重查 → 仍查不到照 `sendKernelImpl:1100` 抛
    /// `MQClientException("The broker[X] not exist", null)`。
    ///
    /// 定点发送不会在 `sendDefaultImpl` 里取发布信息，这里是它唯一的路由来源；
    /// 主从切换期间这也是「本端立刻报错」与「把请求打到从节点上白挨一轮
    /// SYSTEM_BUSY(2)」的分水岭 —— 从节点不接 SEND_MESSAGE / CONSUMER_SEND_MSG_BACK /
    /// CHANGE_INVISIBLE_TIME 这些写请求。
    pub async fn publish_addr_for(&self, broker_name: &str, topic: &str) -> Result<String> {
        let mut addr = self.find_broker_address_in_publish(broker_name);
        if addr.is_none() {
            self.update_topic_route_info_from_name_server(topic, 5000, false)
                .await?;
            addr = self.find_broker_address_in_publish(broker_name);
        }
        addr.ok_or_else(|| Error::client(format!("The broker[{broker_name}] not exist")))
    }

    pub fn get_broker_name_from_message_queue(&self, mq: &MessageQueue) -> String {
        let tables = self.inner.tables.lock().unwrap_or_else(|e| e.into_inner());
        match tables.topic_end_points_table.get(&mq.topic) {
            Some(endpoints) if !endpoints.is_empty() => endpoints
                .get(mq)
                .cloned()
                .unwrap_or_else(|| mq.broker_name.clone()),
            _ => mq.broker_name.clone(),
        }
    }

    /// POP ack / changeInvisibleTime 的地址解析。
    pub async fn pop_ack_addr(&self, broker_name: &str, topic: &str, queue_id: i32) -> Result<String> {
        let resolve_des = |inst: &Self| -> String {
            if crate::client::static_topic::is_logical_queue_mock_broker_name(broker_name) {
                inst.get_broker_name_from_message_queue(&MessageQueue::new(
                    topic, broker_name, queue_id,
                ))
            } else {
                broker_name.to_string()
            }
        };
        let mut des = resolve_des(self);
        let mut addr = self.find_broker_address_in_publish(&des);
        if addr.is_none() {
            self.update_topic_route_info_from_name_server(topic, 5000, false)
                .await?;
            des = resolve_des(self);
            addr = self.find_broker_address_in_publish(&des);
        }
        addr.ok_or_else(|| Error::client(format!("The broker[{des}] master node does not exist")))
    }

    /// 测试专用：直接把端点表写进 RouteTables（等价于一次静态主题路由落库）。
    #[cfg(test)]
    pub(crate) fn seed_end_points_for_test(
        &self,
        topic: &str,
        endpoints: HashMap<MessageQueue, String>,
    ) {
        let mut tables = self.inner.tables.lock().unwrap_or_else(|e| e.into_inner());
        tables
            .topic_end_points_table
            .insert(topic.to_string(), endpoints);
    }

    async fn publish_addr_in_admin(&self, mq: &MessageQueue) -> Result<String> {
        let mut addr = self.find_broker_address_in_publish(&mq.broker_name);
        if addr.is_none() {
            self.update_topic_route_info_from_name_server(&mq.topic, 5000, false)
                .await?;
            addr = self.find_broker_address_in_publish(&mq.broker_name);
        }
        addr.ok_or_else(|| Error::client(format!("The broker[{}] not exist", mq.broker_name)))
    }

    /// 所有已缓存路由里的 broker 地址（去重）。
    ///
    /// HashMap 无序，这里按地址字典序排序，
    /// 让心跳/注销的发送顺序确定（集合内容完全一致）。
    pub fn get_route_of_all_brokers(&self) -> Vec<String> {
        let mut addrs: Vec<String> = Vec::new();
        for route in self.cached_routes() {
            for bd in route.get_broker_datas() {
                if let Some(addr) = bd.select_broker_addr() {
                    if !addrs.contains(&addr) {
                        addrs.push(addr);
                    }
                }
            }
        }
        addrs.sort();
        addrs
    }

    /// 路由里出现过的**每一台** broker（主 + 从）。
    pub fn get_all_broker_addrs(&self) -> Vec<String> {
        let mut addrs: Vec<String> = Vec::new();
        for route in self.cached_routes() {
            for bd in route.get_broker_datas() {
                for (_, addr) in &bd.broker_addrs {
                    if !addr.is_empty() && !addrs.contains(addr) {
                        addrs.push(addr.clone());
                    }
                }
            }
        }
        addrs.sort();
        addrs
    }

    /// 已缓存路由快照。
    fn cached_routes(&self) -> Vec<TopicRouteData> {
        self.inner
            .tables
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .topic_route_table
            .values()
            .cloned()
            .collect()
    }

    /// 与 [`Self::publish_addr_in_admin`] 的差别只在最后那一步：管理侧的 offset 查询
    /// 一律打主、主没了就报错；位点读取允许退到从节点。
    async fn consumer_offset_addr(&self, mq: &MessageQueue) -> Result<String> {
        let mut addr = self.find_broker_address_in_publish(&mq.broker_name);
        if addr.is_none() {
            self.update_topic_route_info_from_name_server(&mq.topic, 5000, false)
                .await?;
            addr = self.broker_addr_of(&mq.broker_name);
        }
        addr.ok_or_else(|| Error::client(format!("The broker[{}] not exist", mq.broker_name)))
    }

    // ---------------- Offset 查询 / 更新 ----------------

    /// `QUERY_NOT_FOUND(22)` ⇒ `None`（直接返回，不抛），
    /// 不抛），其余非 SUCCESS 由 `_check_response` 抛 `MQBrokerException`。
    pub async fn query_consumer_offset(
        &self,
        consumer_group: &str,
        mq: &MessageQueue,
        timeout_millis: i64,
        addr: Option<&str>,
        set_zero_if_not_found: bool,
    ) -> Result<Option<i64>> {
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.consumer_offset_addr(mq).await?,
        };
        let header = QueryConsumerOffsetRequestHeader {
            consumer_group: Some(consumer_group.to_string()),
            topic: Some(mq.topic.clone()),
            queue_id: Some(mq.queue_id),
            set_zero_if_not_found: Some(set_zero_if_not_found),
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::QUERY_CONSUMER_OFFSET,
            Some(Box::new(header)),
        );
        let response = self
            .invoke_sync(&addr, &mut request, timeout_millis)
            .await?;
        if response.code == response_code::QUERY_NOT_FOUND {
            return Ok(None);
        }
        Self::check_response(&response)?;
        let mut resp_header = QueryConsumerOffsetResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        Ok(resp_header.offset)
    }

    /// 上报消费位点。
    pub async fn update_consumer_offset(
        &self,
        consumer_group: &str,
        mq: &MessageQueue,
        commit_offset: i64,
        timeout_millis: i64,
        addr: Option<&str>,
    ) -> Result<()> {
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.broker_addr(mq).await?,
        };
        let header = UpdateConsumerOffsetRequestHeader {
            consumer_group: Some(consumer_group.to_string()),
            topic: Some(mq.topic.clone()),
            queue_id: Some(mq.queue_id),
            commit_offset: Some(commit_offset),
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::UPDATE_CONSUMER_OFFSET,
            Some(Box::new(header)),
        );
        let response = self
            .invoke_sync(&addr, &mut request, timeout_millis)
            .await?;
        Self::check_response(&response)?;
        Ok(())
    }

    pub async fn get_max_offset(
        &self,
        mq: &MessageQueue,
        timeout_millis: i64,
        addr: Option<&str>,
    ) -> Result<i64> {
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.publish_addr_in_admin(mq).await?,
        };
        let header = GetMaxOffsetRequestHeader {
            topic: Some(mq.topic.clone()),
            queue_id: Some(mq.queue_id),
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::GET_MAX_OFFSET,
            Some(Box::new(header)),
        );
        let response = self
            .invoke_sync(&addr, &mut request, timeout_millis)
            .await?;
        Self::check_response(&response)?;
        let mut resp_header = GetMaxOffsetResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        Ok(resp_header.offset.unwrap_or(0))
    }

    pub async fn get_min_offset(
        &self,
        mq: &MessageQueue,
        timeout_millis: i64,
        addr: Option<&str>,
    ) -> Result<i64> {
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.publish_addr_in_admin(mq).await?,
        };
        let header = GetMinOffsetRequestHeader {
            topic: Some(mq.topic.clone()),
            queue_id: Some(mq.queue_id),
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::GET_MIN_OFFSET,
            Some(Box::new(header)),
        );
        let response = self
            .invoke_sync(&addr, &mut request, timeout_millis)
            .await?;
        Self::check_response(&response)?;
        let mut resp_header = GetMinOffsetResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        Ok(resp_header.offset.unwrap_or(0))
    }

    /// 按时间戳查 offset（不带边界参数的重载）。
    pub async fn search_offset_by_timestamp(
        &self,
        mq: &MessageQueue,
        timestamp: i64,
        timeout_millis: i64,
        addr: Option<&str>,
    ) -> Result<i64> {
        self.search_offset_by_boundary(
            mq,
            timestamp,
            Some(BoundaryType::Lower),
            timeout_millis,
            addr,
        )
        .await
    }

    /// 地址口径同 `MQAdminImpl.searchOffset:195`：只认 master（见
    /// [`Self::publish_addr_in_admin`]）。
    pub async fn search_offset_by_boundary(
        &self,
        mq: &MessageQueue,
        timestamp: i64,
        boundary: Option<BoundaryType>,
        timeout_millis: i64,
        addr: Option<&str>,
    ) -> Result<i64> {
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.publish_addr_in_admin(mq).await?,
        };
        let header = SearchOffsetRequestHeader {
            topic: Some(mq.topic.clone()),
            queue_id: Some(mq.queue_id),
            timestamp: Some(timestamp),
            boundary_type: boundary,
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::SEARCH_OFFSET_BY_TIMESTAMP,
            Some(Box::new(header)),
        );
        let response = self
            .invoke_sync(&addr, &mut request, timeout_millis)
            .await?;
        Self::check_response(&response)?;
        let mut resp_header = SearchOffsetResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        Ok(resp_header.offset.unwrap_or(0))
    }

    pub async fn get_earliest_msg_store_time(
        &self,
        mq: &MessageQueue,
        timeout_millis: i64,
        addr: Option<&str>,
    ) -> Result<i64> {
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.publish_addr_in_admin(mq).await?,
        };
        let header = GetEarliestMsgStoretimeRequestHeader {
            topic: Some(mq.topic.clone()),
            queue_id: Some(mq.queue_id),
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::GET_EARLIEST_MSG_STORETIME,
            Some(Box::new(header)),
        );
        let response = self
            .invoke_sync(&addr, &mut request, timeout_millis)
            .await?;
        Self::check_response(&response)?;
        let mut resp_header = GetEarliestMsgStoretimeResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        Ok(resp_header.timestamp.unwrap_or(0))
    }

    /// 与 push 消费者内部那次回投的差别（`consumer.rs` 的 `send_message_back`）：
    /// 那条是**发射后不管**（`tokio::spawn` + 只记日志），且把
    /// `maxReconsumeTimes == -1` 映射成 16；这里必须把失败返回给调用方。
    #[allow(clippy::too_many_arguments)]
    pub async fn consumer_send_msg_back(
        &self,
        consumer_group: &str,
        msg: &MessageExt,
        delay_level: i32,
        max_reconsume_times: Option<i32>,
        timeout_millis: i64,
        addr: &str,
        unit_mode: bool,
    ) -> Result<()> {
        let header = ConsumerSendMsgBackRequestHeader {
            offset: Some(msg.commit_log_offset),
            group: Some(consumer_group.to_string()),
            delay_level: Some(delay_level),
            origin_msg_id: msg.msg_id.clone(),
            origin_topic: Some(msg.topic.clone()),
            unit_mode: Some(unit_mode),
            max_reconsume_times,
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::CONSUMER_SEND_MSG_BACK,
            Some(Box::new(header)),
        );
        let response = self.invoke_sync(addr, &mut request, timeout_millis).await?;
        Self::check_response(&response)?;
        Ok(())
    }

    // ---------------- 消息查询 ----------------

    /// `index_type` 取 `MessageConst::INDEX_KEY_TYPE`("K") / `INDEX_UNIQUE_TYPE`("U")；
    /// `uniq_key = true` 时额外下发 `extFields["_UNIQUE_KEY_QUERY"]="true"`，broker 端据此
    /// 强制走 uniqKey 索引（并把 maxNum 覆盖为默认查询条数）。
    /// `QUERY_NOT_FOUND(22)` ⇒ `None`。
    #[allow(clippy::too_many_arguments)]
    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
        timeout_millis: i64,
        addr: Option<&str>,
        index_type: Option<&str>,
        uniq_key: bool,
    ) -> Result<Option<Vec<u8>>> {
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.broker_addr_for_topic(topic).await?,
        };
        let header = QueryMessageRequestHeader {
            topic: Some(topic.to_string()),
            key: Some(key.to_string()),
            max_num: Some(max_num),
            begin_timestamp: Some(begin_timestamp),
            end_timestamp: Some(end_timestamp),
            index_type: index_type.map(str::to_string),
            last_key: None,
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::QUERY_MESSAGE,
            Some(Box::new(header)),
        );
        if uniq_key {
            request.add_ext_field(MixAll::UNIQUE_MSG_QUERY_FLAG, "true");
        }
        let response = self
            .invoke_sync(&addr, &mut request, timeout_millis)
            .await?;
        if response.code == response_code::QUERY_NOT_FOUND {
            return Ok(None);
        }
        Self::check_response(&response)?;
        Ok(response.body)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn query_message_all_brokers(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
        index_type: Option<&str>,
        uniq_key: bool,
        timeout_millis: i64,
    ) -> Vec<MessageExt> {
        let mut messages: Vec<MessageExt> = Vec::new();
        let Some(route) = self.get_topic_route_data(topic).await else {
            return messages;
        };
        for broker_data in route.get_broker_datas() {
            let Some(addr) = broker_data.select_broker_addr().filter(|a| !a.is_empty()) else {
                continue;
            };
            let body = match self
                .query_message(
                    topic,
                    key,
                    max_num,
                    begin_timestamp,
                    end_timestamp,
                    timeout_millis,
                    Some(&addr),
                    index_type,
                    uniq_key,
                )
                .await
            {
                Ok(Some(body)) if !body.is_empty() => body,
                // 异常与空 body 都是 continue
                _ => continue,
            };
            for mut msg in decode_messages(&body) {
                msg.broker_name = Some(broker_data.broker_name.clone());
                if uniq_key {
                    if msg.msg_id.as_deref() == Some(key) {
                        messages.push(msg);
                    }
                    continue;
                }
                let keys = msg.get_keys().map(str::to_string).unwrap_or_default();
                if keys.is_empty() {
                    continue;
                }
                let hit = keys
                    .split(KEY_SEPARATOR)
                    .any(|k| k == key && msg.topic == topic);
                if hit {
                    messages.push(msg);
                }
            }
        }
        messages.sort_by_key(|m| m.queue_offset);
        if max_num > 0 {
            messages.truncate(max_num as usize);
        }
        messages
    }

    // ---------------- 队列锁（顺序消费） ----------------

    /// 按 brokerName 分组，**保持首次出现顺序**（插入序）。
    fn group_by_broker(mqs: &[MessageQueue]) -> Vec<(String, Vec<MessageQueue>)> {
        let mut out: Vec<(String, Vec<MessageQueue>)> = Vec::new();
        for mq in mqs {
            match out.iter_mut().find(|(name, _)| *name == mq.broker_name) {
                Some((_, list)) => list.push(mq.clone()),
                None => out.push((mq.broker_name.clone(), vec![mq.clone()])),
            }
        }
        out
    }

    /// 按 broker 分组发送；返回 broker 确认锁定成功的队列集（`lockOKMQSet`）。
    /// 单台失败只 warn 并继续。
    pub async fn lock_batch_mq(
        &self,
        consumer_group: &str,
        client_id: &str,
        mqs: &[MessageQueue],
        timeout_millis: i64,
    ) -> Result<Vec<MessageQueue>> {
        let mut lock_ok: Vec<MessageQueue> = Vec::new();
        for (broker_name, broker_mqs) in Self::group_by_broker(mqs) {
            let Some(addr) = self.find_broker_address_in_publish(&broker_name) else {
                continue;
            };
            let body = LockBatchRequestBody {
                consumer_group: Some(consumer_group.to_string()),
                client_id: Some(client_id.to_string()),
                mq_set: broker_mqs
                    .iter()
                    .map(|m| MessageQueueKey::new(&m.topic, &m.broker_name, m.queue_id))
                    .collect(),
            };
            let mut request = RemotingCommand::create_request_command(
                request_code::LOCK_BATCH_MQ,
                Some(Box::new(LockBatchMqRequestHeader::default())),
            );
            request.set_body(Some(body.encode()));
            let result = self
                .invoke_sync(&addr, &mut request, timeout_millis)
                .await
                .and_then(|response| {
                    Self::check_response(&response)?;
                    LockBatchResponseBody::decode(response.body.as_deref().unwrap_or_default())
                });
            match result {
                Ok(rb) => {
                    for d in rb.lock_ok_mq_set {
                        lock_ok.push(MessageQueue::new(&d.topic, &d.broker_name, d.queue_id));
                    }
                }
                Err(e) => rmq_warn!("lock_batch_mq failed for broker {broker_name}: {e}"),
            }
        }
        Ok(lock_ok)
    }

    pub async fn unlock_batch_mq(
        &self,
        consumer_group: &str,
        client_id: &str,
        mqs: &[MessageQueue],
        timeout_millis: i64,
    ) -> Result<()> {
        for (broker_name, broker_mqs) in Self::group_by_broker(mqs) {
            let Some(addr) = self.find_broker_address_in_publish(&broker_name) else {
                continue;
            };
            let body = UnlockBatchRequestBody {
                consumer_group: Some(consumer_group.to_string()),
                client_id: Some(client_id.to_string()),
                mq_set: broker_mqs
                    .iter()
                    .map(|m| MessageQueueKey::new(&m.topic, &m.broker_name, m.queue_id))
                    .collect(),
            };
            let mut request = RemotingCommand::create_request_command(
                request_code::UNLOCK_BATCH_MQ,
                Some(Box::new(UnlockBatchMqRequestHeader::default())),
            );
            request.set_body(Some(body.encode()));
            let result = self
                .invoke_sync(&addr, &mut request, timeout_millis)
                .await
                .and_then(|response| Self::check_response(&response).map(|_| ()));
            if let Err(e) = result {
                rmq_warn!("unlock_batch_mq failed for broker {broker_name}: {e}");
            }
        }
        Ok(())
    }

    // ---------------- 心跳 / 注销 ----------------

    /// 发心跳（HEART_BEAT = 34）。
    pub async fn send_heartbeat(
        &self,
        addr: &str,
        heartbeat_data: &HeartbeatData,
        timeout_millis: i64,
    ) -> Result<()> {
        let mut request = RemotingCommand::create_request_command(request_code::HEART_BEAT, None);
        request.set_body(Some(heartbeat_data.encode()));
        let response = self.invoke_sync(addr, &mut request, timeout_millis).await?;
        Self::check_response(&response)?;
        Ok(())
    }

    pub fn prepare_heartbeat_data(&self) -> HeartbeatData {
        let mut hb = HeartbeatData::new(self.inner.client_id.clone());
        for (group, consumer) in self.consumers_entries() {
            let mut cd = ConsumerData::new(
                group,
                consumer.consume_type(),
                consumer.message_model(),
                consumer.consume_from_where(),
            );
            cd.unit_mode = consumer.is_unit_mode();
            for sub in consumer.subscriptions() {
                cd.add_subscription_data(sub);
            }
            hb.add_consumer_data(cd);
        }
        hb
    }

    /// 向所有已知 broker 发心跳（**含从节点**），返回成功的台数。
    pub async fn send_heartbeat_to_all_broker(&self, timeout_millis: i64) -> usize {
        if self
            .inner
            .consumer_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
        {
            return 0;
        }
        let heartbeat_data = self.prepare_heartbeat_data();
        let mut ok = 0usize;
        for addr in self.get_all_broker_addrs() {
            match self
                .send_heartbeat(&addr, &heartbeat_data, timeout_millis)
                .await
            {
                Ok(()) => ok += 1,
                Err(e) => rmq_debug!("heartbeat to {addr} failed: {e}"),
            }
        }
        ok
    }

    pub async fn persist_consumer_offsets(&self) {
        for (group, consumer) in self.consumers_entries() {
            if let Err(e) = consumer.persist_consumer_offset().await {
                rmq_warn!("persist consumer offset failed for group {group}: {e}");
            }
        }
    }

    /// `consumer_table` 快照（按 group 排序，保证遍历顺序确定）。
    fn consumers_entries(&self) -> Vec<(String, Arc<dyn RegisteredConsumer>)> {
        let mut entries: Vec<(String, Arc<dyn RegisteredConsumer>)> = self
            .inner
            .consumer_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(group, consumer)| (group.clone(), consumer.clone()))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    // ---------------- CHECK_CLIENT_CONFIG(46)：订阅表达式向 broker 求证 ----------------

    /// 只读缓存路由、随机挑其中一个 broker（优先 master 地址）；没有缓存返回 `None`。
    pub fn find_broker_addr_by_topic(&self, topic: &str) -> Option<String> {
        let route = self.route_of(topic)?;
        let brokers = route.get_broker_datas();
        brokers
            .get(pseudo_random_index(brokers.len()))?
            .select_broker_addr()
    }

    // ---------------- QUERY_ASSIGNMENT(400)：broker 侧队列分配 ----------------

    pub async fn query_assignment(
        &self,
        topic: &str,
        consumer_group: &str,
        strategy_name: &str,
        message_model: &str,
        timeout_millis: i64,
    ) -> Result<Option<Vec<MessageQueueAssignment>>> {
        let client_id = self.client_id();
        let addr = match self.find_broker_addr_by_topic(topic) {
            Some(addr) => addr,
            None => {
                if let Err(e) =
                    self.update_topic_route_info_from_name_server(topic, 5000, false)
                        .await
                {
                    rmq_debug!("queryAssignment: route refresh for {topic} failed: {e}");
                }
                match self.find_broker_addr_by_topic(topic) {
                    Some(addr) => addr,
                    None => return Ok(None),
                }
            }
        };
        self.query_assignment_from_broker(
            &addr,
            topic,
            consumer_group,
            client_id,
            strategy_name,
            message_model,
            timeout_millis,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn query_assignment_from_broker(
        &self,
        addr: &str,
        topic: &str,
        consumer_group: &str,
        client_id: &str,
        strategy_name: &str,
        message_model: &str,
        timeout_millis: i64,
    ) -> Result<Option<Vec<MessageQueueAssignment>>> {
        let mut request =
            RemotingCommand::create_request_command(request_code::QUERY_ASSIGNMENT, None);
        request.set_body(Some(
            QueryAssignmentRequestBody::new(
                topic,
                consumer_group,
                client_id,
                strategy_name,
                message_model,
            )
            .encode(),
        ));
        let response = self.invoke_sync(addr, &mut request, timeout_millis).await?;
        if response.code != response_code::SUCCESS {
            return Err(Error::Broker {
                response_code: response.code,
                message: response.remark.clone().unwrap_or_default(),
            });
        }
        match response.body() {
            None | Some([]) => Ok(None),
            Some(body) => QueryAssignmentResponseBody::decode(body),
        }
    }

    pub async fn check_client_config(
        &self,
        broker_addr: &str,
        consumer_group: &str,
        client_id: &str,
        subscription_data: &SubscriptionData,
        timeout_millis: i64,
    ) -> Result<()> {
        let mut request =
            RemotingCommand::create_request_command(request_code::CHECK_CLIENT_CONFIG, None);
        let body = CheckClientRequestBody {
            client_id: Some(client_id.to_string()),
            group: Some(consumer_group.to_string()),
            subscription_data: Some(subscription_data.clone()),
            namespace: None,
        };
        request.set_body(Some(body.encode()));
        let response = self
            .invoke_sync(broker_addr, &mut request, timeout_millis)
            .await?;
        if response.code != response_code::SUCCESS {
            return Err(Error::client_with_code(
                response.code,
                response.remark.clone().unwrap_or_default(),
            ));
        }
        Ok(())
    }

    /// 遍历本实例登记的每个消费者的订阅，只把**非 TAG**（SQL92 / CLASS_FILTER）的表达式
    /// 发给 broker 校验。为什么必须发：SQL92 表达式写错时 broker 的
    /// `ExpressionMessageFilter` 在拿不到编译好的过滤数据时**直接放行全部消息**
    /// （返回 true），也就是静默变成「订阅全部消息」，消费者启动照样成功、一条错误都不报。
    /// 这一调用把「写错的表达式」变成启动期的一次显式失败。
    pub async fn check_client_in_broker(&self) -> Result<()> {
        for (group, consumer) in self.consumers_entries() {
            let subs = consumer.subscriptions();
            if subs.is_empty() {
                return Ok(());
            }
            self.check_subscriptions_in_broker(&group, &subs).await?;
        }
        Ok(())
    }

    /// `check_client_in_broker` 的内层循环，单独暴露给未登记进 consumerTable 的调用方。
    pub async fn check_subscriptions_in_broker(
        &self,
        group: &str,
        subs: &[SubscriptionData],
    ) -> Result<()> {
        let timeout_millis = MQ_CLIENT_API_TIMEOUT_MILLIS;
        for sub in subs {
            if sub.expression_type.is_empty() || sub.expression_type == ExpressionType::TAG {
                continue;
            }
            let Some(addr) = self.find_broker_addr_by_topic(&sub.topic) else {
                continue;
            };
            let result = self
                .check_client_config(&addr, group, &self.inner.client_id, sub, timeout_millis)
                .await;
            if let Err(e) = result {
                if matches!(e, Error::Client { .. }) {
                    return Err(e);
                }
                return Err(Error::client(format!(
                    "Check client in broker error, maybe because you use {} to filter message, \
                     but server has not been upgraded to support!This error would not affect the \
                     launch of consumer, but may has impact on message receiving if you have use \
                     the new features which are not supported by server, please check the log!",
                    sub.expression_type
                )));
            }
        }
        Ok(())
    }

    /// 向 namesrv 注销客户端（UNREGISTER_CLIENT = 35）。
    pub async fn unregister_client(
        &self,
        addr: &str,
        client_id: &str,
        producer_group: &str,
        consumer_group: &str,
        timeout_millis: i64,
    ) -> Result<()> {
        let header = UnregisterClientRequestHeader {
            client_id: Some(client_id.to_string()),
            producer_group: blank_group_to_none(producer_group),
            consumer_group: blank_group_to_none(consumer_group),
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::UNREGISTER_CLIENT,
            Some(Box::new(header)),
        );
        let response = self.invoke_sync(addr, &mut request, timeout_millis).await?;
        Self::check_response(&response)?;
        Ok(())
    }

    // ---------------- 管理类 API ----------------

    /// 拉取 broker 集群信息（GET_BROKER_CLUSTER_INFO = 106）。
    pub async fn get_broker_cluster_info(&self, timeout_millis: i64) -> Result<ClusterInfo> {
        let mut request =
            RemotingCommand::create_request_command(request_code::GET_BROKER_CLUSTER_INFO, None);
        for ns_addr in self.name_server_addrs() {
            let response = match self
                .invoke_sync(&ns_addr, &mut request, timeout_millis)
                .await
            {
                Ok(response) => response,
                Err(e) => {
                    rmq_debug!("get broker cluster info from {ns_addr} failed: {e}");
                    continue;
                }
            };
            let decoded = if response.code == response_code::SUCCESS {
                response
                    .body
                    .as_deref()
                    .filter(|b| !b.is_empty())
                    .map(ClusterInfo::decode)
            } else {
                None
            };
            if let Some(result) = decoded {
                match result {
                    Ok(info) => return Ok(info),
                    Err(e) => rmq_debug!("decode cluster info failed: {e}"),
                }
            }
        }
        bail!("Failed to get broker cluster info from name server")
    }

    /// 从 namesrv 拉全量 topic 列表（GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206）。
    pub async fn get_all_topic_list_from_name_server(
        &self,
        timeout_millis: i64,
    ) -> Result<TopicList> {
        let mut request = RemotingCommand::create_request_command(
            request_code::GET_ALL_TOPIC_LIST_FROM_NAMESERVER,
            None,
        );
        for ns_addr in self.name_server_addrs() {
            let response = match self
                .invoke_sync(&ns_addr, &mut request, timeout_millis)
                .await
            {
                Ok(response) => response,
                Err(e) => {
                    rmq_debug!("get all topic list from {ns_addr} failed: {e}");
                    continue;
                }
            };
            if response.code == response_code::SUCCESS {
                if let Some(body) = response.body.as_deref().filter(|b| !b.is_empty()) {
                    match TopicList::decode(body) {
                        Ok(list) => return Ok(list),
                        Err(e) => rmq_debug!("decode topic list failed: {e}"),
                    }
                }
            }
        }
        bail!("Failed to get all topic list from name server")
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_topic_in_broker(
        &self,
        broker_addr: &str,
        default_topic: &str,
        topic: &str,
        read_queue_nums: i32,
        write_queue_nums: i32,
        perm: i32,
        topic_sys_flag: i32,
        topic_filter_type: &str,
        order: bool,
        attributes: Option<&str>,
        timeout_millis: i64,
        retry_times: i32,
    ) -> Result<()> {
        let header = CreateTopicRequestHeader {
            topic: Some(topic.to_string()),
            default_topic: Some(default_topic.to_string()),
            read_queue_nums: Some(read_queue_nums),
            write_queue_nums: Some(write_queue_nums),
            perm: Some(perm),
            topic_filter_type: Some(topic_filter_type.to_string()),
            topic_sys_flag: Some(topic_sys_flag),
            order: Some(order),
            attributes: Some(attributes.unwrap_or("").to_string()),
            force: Some(false),
        };
        let mut last_exc: Option<Error> = None;
        for attempt in 0..retry_times.max(1) {
            let mut request = RemotingCommand::create_request_command(
                request_code::UPDATE_AND_CREATE_TOPIC,
                Some(Box::new(header.clone())),
            );
            match self
                .invoke_sync(broker_addr, &mut request, timeout_millis)
                .await
            {
                Ok(response) => {
                    // 业务错不重试，直接上抛
                    Self::check_response(&response)?;
                    return Ok(());
                }
                Err(e) => {
                    if matches!(e, Error::Broker { .. }) {
                        return Err(e);
                    }
                    if attempt == retry_times - 1 {
                        return Err(e);
                    }
                    rmq_debug!(
                        "create topic {topic} on {broker_addr} failed (attempt {attempt}): {e}"
                    );
                    last_exc = Some(e);
                }
            }
        }
        match last_exc {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_topic_in_route(
        &self,
        topic: &str,
        read_queue_nums: i32,
        write_queue_nums: i32,
        perm: i32,
        topic_sys_flag: i32,
        attributes: Option<&str>,
        timeout_millis: i64,
    ) -> Result<()> {
        let route = self
            .get_topic_route_data(MixAll::DEFAULT_TOPIC)
            .await
            .ok_or_else(|| {
                Error::client(format!(
                    "No route info of default topic {}",
                    MixAll::DEFAULT_TOPIC
                ))
            })?;
        let mut created_at_least_once = false;
        let mut last_exc: Option<String> = None;
        for broker_data in route.get_broker_datas() {
            let Some(addr) = broker_data.select_broker_addr().filter(|a| !a.is_empty()) else {
                continue;
            };
            // 这里用 create_topic_in_broker 的默认值：SINGLE_TAG / order=false / 重试 5 次
            let result = self
                .create_topic_in_broker(
                    &addr,
                    MixAll::DEFAULT_TOPIC,
                    topic,
                    read_queue_nums,
                    write_queue_nums,
                    perm,
                    topic_sys_flag,
                    TopicFilterType::SINGLE_TAG,
                    false,
                    attributes,
                    timeout_millis,
                    5,
                )
                .await;
            match result {
                Ok(()) => created_at_least_once = true,
                Err(e) => last_exc = Some(e.to_string()),
            }
        }
        if !created_at_least_once {
            if let Some(cause) = last_exc {
                // 一台都没建成：报 "create new topic failed"，带上最后一个异常
                return Err(Error::client(format!("create new topic failed: {cause}")));
            }
        }
        Ok(())
    }

    /// 在 broker 上删 topic（DELETE_TOPIC_IN_BROKER = 215）。
    pub async fn delete_topic_in_broker(
        &self,
        broker_addr: &str,
        topic: &str,
        timeout_millis: i64,
    ) -> Result<()> {
        let mut request =
            RemotingCommand::create_request_command(request_code::DELETE_TOPIC_IN_BROKER, None);
        request.add_ext_field("topic", topic);
        let response = self
            .invoke_sync(broker_addr, &mut request, timeout_millis)
            .await?;
        Self::check_response(&response)?;
        Ok(())
    }

    /// 在 namesrv 上删 topic 配置（DELETE_TOPIC_IN_NAMESRV = 216）：
    /// 任取一台成功的 namesrv，全失败才抛。
    pub async fn delete_topic_in_namesrv(&self, topic: &str, timeout_millis: i64) -> Result<()> {
        let mut request =
            RemotingCommand::create_request_command(request_code::DELETE_TOPIC_IN_NAMESRV, None);
        request.add_ext_field("topic", topic);
        for ns_addr in self.name_server_addrs() {
            let result = self
                .invoke_sync(&ns_addr, &mut request, timeout_millis)
                .await
                .and_then(|response| Self::check_response(&response).map(|_| ()));
            if result.is_ok() {
                return Ok(());
            }
            if let Err(e) = result {
                rmq_debug!("delete topic {topic} in {ns_addr} failed: {e}");
            }
        }
        bail!("Failed to delete topic {topic} in name server")
    }

    pub async fn get_consumer_list_by_group(
        &self,
        consumer_group: &str,
        timeout_millis: i64,
        addr: Option<&str>,
    ) -> Result<GetConsumerListByGroupResponseBody> {
        let Some(addr) = addr else {
            bail!("broker addr required for get consumer list");
        };
        let header = GetConsumerListByGroupRequestHeader {
            consumer_group: Some(consumer_group.to_string()),
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::GET_CONSUMER_LIST_BY_GROUP,
            Some(Box::new(header)),
        );
        let response = self.invoke_sync(addr, &mut request, timeout_millis).await?;
        Self::check_response(&response)?;
        match response.body.as_deref() {
            Some(body) if !body.is_empty() => Ok(GetConsumerListByGroupResponseBody::decode(body)?),
            _ => Ok(GetConsumerListByGroupResponseBody::default()),
        }
    }

    pub async fn get_consumer_id_list_by_group(
        &self,
        topic: &str,
        consumer_group: &str,
        timeout_millis: i64,
    ) -> Option<Vec<String>> {
        let addr = match self.broker_addr_for_topic(topic).await {
            Ok(addr) => addr,
            Err(e) => {
                rmq_debug!("get_consumer_id_list_by_group: no broker for topic {topic}: {e}");
                return None;
            }
        };
        let header = GetConsumerListByGroupRequestHeader {
            consumer_group: Some(consumer_group.to_string()),
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::GET_CONSUMER_LIST_BY_GROUP,
            Some(Box::new(header)),
        );
        let response = match self.invoke_sync(&addr, &mut request, timeout_millis).await {
            Ok(response) => response,
            Err(e) => {
                rmq_debug!("get_consumer_id_list_by_group failed, {addr} {consumer_group}: {e}");
                return None;
            }
        };
        if response.code != response_code::SUCCESS {
            return None;
        }
        let body = response.body.as_deref()?;
        match GetConsumerListByGroupResponseBody::decode(body) {
            Ok(body) => Some(body.consumer_id_list),
            Err(e) => {
                rmq_debug!("get_consumer_id_list_by_group decode failed: {e}");
                None
            }
        }
    }

    pub async fn unregister_client_all_brokers(
        &self,
        client_id: &str,
        producer_group: &str,
        consumer_group: &str,
        timeout_millis: i64,
    ) {
        for addr in self.get_all_broker_addrs() {
            if let Err(e) = self
                .unregister_client(
                    &addr,
                    client_id,
                    producer_group,
                    consumer_group,
                    timeout_millis,
                )
                .await
            {
                rmq_debug!("unregister_client failed, addr={addr}: {e}");
            }
        }
    }
}

/// `UNREGISTER_CLIENT`(35) 的组名槽位：空白 ⇒ 不上线。
fn blank_group_to_none(group: &str) -> Option<String> {
    (!group.trim().is_empty()).then(|| group.to_string())
}

/// 睡到绝对时刻 `deadline`（固定速率锚点，误差不累积：每轮都对着同一个时间轴算）；
/// stop 置位时立刻返回 true，调用方收尾。
async fn wait_until_or_stop(stop: &watch::Sender<bool>, deadline: tokio::time::Instant) -> bool {
    let mut rx = stop.subscribe();
    if *rx.borrow_and_update() {
        return true;
    }
    if tokio::time::Instant::now() >= deadline {
        return false;
    }
    tokio::select! {
        _ = tokio::time::sleep_until(deadline) => false,
        _ = rx.changed() => true,
    }
}

/// `tls_enable=None` 的行为：读环境变量。
fn default_tls_enable_from_env() -> bool {
    matches!(
        std::env::var("ROCKETMQ_TLS_ENABLE")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// 326 的报文 → MessageExt（应答还原段，单独拆出以便单测）。
fn build_reply_message_ext(
    header: &ReplyMessageRequestHeader,
    body: Option<&[u8]>,
) -> Result<MessageExt> {
    let mut bytes = body.unwrap_or_default().to_vec();
    let sys_flag = header.sys_flag.unwrap_or(0);
    if MessageSysFlag::is_compressed(sys_flag) {
        bytes = decompress_body(&bytes, MessageSysFlag::get_compression_type(sys_flag))?;
    }
    let mut msg = MessageExt::new();
    msg.topic = header.topic.clone().unwrap_or_default();
    msg.body = Some(bytes);
    msg.queue_id = header.queue_id.unwrap_or(0);
    msg.store_timestamp = header.store_timestamp.unwrap_or(0);
    msg.flag = header.flag.unwrap_or(0);
    msg.born_timestamp = header.born_timestamp.unwrap_or(0);
    msg.reconsume_times = header.reconsume_times.unwrap_or(0);
    // 空串视为未提供，不能存成 Some("")。
    msg.born_host = header
        .born_host
        .clone()
        .filter(|h| !h.is_empty())
        .or(msg.born_host);
    msg.store_host = header
        .store_host
        .clone()
        .filter(|h| !h.is_empty())
        .or(msg.store_host);
    let mut properties = string_2_message_properties(header.properties.as_deref().unwrap_or(""));
    properties.insert(
        PROPERTY_REPLY_MESSAGE_ARRIVE_TIME,
        current_time_millis().to_string(),
    );
    msg.properties = properties;
    Ok(msg)
}

/// 六个实例级 broker 主动请求码共用一个处理器（一个 `RequestProcessor`
/// 对象注册到多个 code，等价于按方法逐个注册）。
struct ClientRemotingProcessor {
    instance: Weak<Inner>,
}

impl RequestProcessor for ClientRemotingProcessor {
    fn process(&self, request: RemotingCommand, addr: String, sink: ResponseSink) {
        let Some(inner) = self.instance.upgrade() else {
            sink.respond(RemotingCommand::create_response(
                response_code::SYSTEM_ERROR,
                Some("client instance already released".to_string()),
            ));
            return;
        };
        let instance = MQClientInstance { inner };
        let response = match request.code {
            request_code::PUSH_REPLY_MESSAGE_TO_CLIENT => instance.process_reply_message(&request),
            request_code::RESET_CONSUMER_CLIENT_OFFSET => {
                // oneway：ResponseSink 按 wants_reply 丢弃，
                // 这里干脆不回。
                instance.process_reset_offset(&request);
                return;
            }
            request_code::NOTIFY_CONSUMER_IDS_CHANGED => {
                instance.process_notify_consumer_ids_changed(&request, &addr);
                return;
            }
            request_code::GET_CONSUMER_STATUS_FROM_CLIENT => {
                instance.process_get_consumer_status(&request)
            }
            request_code::GET_CONSUMER_RUNNING_INFO => {
                instance.process_get_consumer_running_info(&request)
            }
            request_code::CONSUME_MESSAGE_DIRECTLY => {
                instance.process_consume_message_directly(&request)
            }
            other => RemotingCommand::create_response(
                response_code::SYSTEM_ERROR,
                Some(format!("unsupported request code {other}")),
            ),
        };
        sink.respond(response);
    }
}

// ================================================================ tests
//
// broker 主动请求（220/221/307/309/326）只能由 broker 沿已建立的连接反向打进来，
// 在线样例（`examples/live_mq_client.rs`）无法注入，所以这里离线把协议 + 分派
// 逻辑锁死 —— 协议对齐用例（消费者侧的对应用例
// 要等 `consumer.rs` 落地后再补）。

// 夹具统一走「default + 只设被读的字段」：请求头有十几个字段，逐字段字面量会把
// 用例意图淹掉，所以该模块整体关掉 field_reassign_with_default。
#[cfg(test)]
mod tests {
    #![allow(clippy::field_reassign_with_default)]

    use super::*;
    use crate::common::message_decoder::encode_message_ext;
    use crate::common::sysflag::PermName;
    use crate::remoting::protocol::body::MessageQueueForC;
    use crate::remoting::protocol::route::{BrokerData, QueueData};
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;
    use std::time::{Duration, Instant};

    const GROUP: &str = "GID_MqClientUnit";
    const TOPIC: &str = "MqClientUnitTopic";
    const BROKER: &str = "broker-a";
    const NAMESRV: &str = "127.0.0.1:9876";

    static SEQ: AtomicUsize = AtomicUsize::new(0);

    /// 每个用例用独立 clientId：`INSTANCE_MAP` 是进程级共享表。
    fn new_instance() -> MQClientInstance {
        let id = format!("{GROUP}@{}", SEQ.fetch_add(1, Ordering::Relaxed));
        MQClientInstance::new(&id, vec![NAMESRV.to_string()])
    }

    fn guard<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        m.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// 轮询等待后台任务推进（最多 2s），避免把断言挂在固定 sleep 上。
    async fn wait_until(mut pred: impl FnMut() -> bool) -> bool {
        for _ in 0..200 {
            if pred() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        pred()
    }

    fn request<T: CustomHeader + 'static>(code: i32, header: T) -> RemotingCommand {
        let mut cmd = RemotingCommand::create_request_command(code, Some(Box::new(header)));
        // 真链路上 broker 收到的是 extFields，这里同口径（处理器只读 extFields）。
        cmd.make_custom_header_to_net();
        cmd
    }

    fn sample_message() -> MessageExt {
        let mut msg = MessageExt::new();
        msg.topic = TOPIC.to_string();
        msg.body = Some(b"directly-consume-me".to_vec());
        msg.properties
            .insert(PROPERTY_CORRELATION_ID.to_string(), "corr-1".to_string());
        msg
    }

    /// 每次 220 的入参原样留档（topic + 位点表），两种 body 形状都靠它断言。
    type ResetCall = (String, Vec<(MessageQueue, i64)>);

    /// 只记录调用、不做任何 RPC 的消费者替身。
    struct StubConsumer {
        group: String,
        /// 220 落位点前的耗时，用来证明「处理没有卡在调用线程上」。
        reset_delay: Duration,
        resets: Arc<Mutex<Vec<ResetCall>>>,
        status_topics: Arc<Mutex<Vec<Option<String>>>>,
        persisted: Arc<AtomicUsize>,
        /// 40 通知叫醒了几次重平衡。
        rebalance_wakeups: Arc<AtomicUsize>,
        subs: Vec<SubscriptionData>,
    }

    impl StubConsumer {
        fn new() -> Arc<StubConsumer> {
            Arc::new(StubConsumer {
                ..StubConsumer::bare()
            })
        }

        /// 只带订阅表的替身：46 号检查只需要 `subscriptions()`。
        fn with_subs(group: &str, subs: Vec<SubscriptionData>) -> Arc<StubConsumer> {
            Arc::new(StubConsumer {
                group: group.to_string(),
                subs,
                ..StubConsumer::bare()
            })
        }

        fn bare() -> StubConsumer {
            StubConsumer {
                group: GROUP.to_string(),
                reset_delay: Duration::from_millis(40),
                resets: Arc::new(Mutex::new(Vec::new())),
                status_topics: Arc::new(Mutex::new(Vec::new())),
                persisted: Arc::new(AtomicUsize::new(0)),
                rebalance_wakeups: Arc::new(AtomicUsize::new(0)),
                subs: Vec::new(),
            }
        }
    }

    impl RegisteredConsumer for StubConsumer {
        fn client_id(&self) -> String {
            format!("{}@stub", self.group)
        }

        fn consumer_group(&self) -> String {
            self.group.clone()
        }

        fn consume_type(&self) -> String {
            "CONSUME_PASSIVELY".to_string()
        }

        fn message_model(&self) -> String {
            "CLUSTERING".to_string()
        }

        fn consume_from_where(&self) -> String {
            "CONSUME_FROM_LAST_OFFSET".to_string()
        }

        fn is_unit_mode(&self) -> bool {
            false
        }

        fn subscription(&self) -> Vec<String> {
            vec![TOPIC.to_string()]
        }

        fn subscriptions(&self) -> Vec<SubscriptionData> {
            self.subs.clone()
        }

        fn rebalance_immediately(&self) {
            self.rebalance_wakeups.fetch_add(1, Ordering::SeqCst);
        }

        fn reset_offset(
            self: Arc<Self>,
            topic: String,
            offset_table: Vec<(MessageQueue, i64)>,
        ) -> ConsumerFuture<()> {
            Box::pin(async move {
                // 真实实现在这里面会做 rebalance / lock / batch（全是 invokeSync），
                // 所以整段必须在后台跑。
                tokio::time::sleep(self.reset_delay).await;
                guard(&self.resets).push((topic, offset_table));
                Ok(())
            })
        }

        fn get_consumer_status(&self, topic: Option<&str>) -> Vec<(MessageQueue, i64)> {
            guard(&self.status_topics).push(topic.map(|t| t.to_string()));
            vec![(MessageQueue::new(TOPIC, BROKER, 0), 42)]
        }

        fn consumer_running_info(&self) -> ConsumerRunningInfo {
            let mut info = ConsumerRunningInfo::default();
            info.properties.insert(
                ConsumerRunningInfo::PROP_NAMESERVER_ADDR.to_string(),
                format!("{NAMESRV};"),
            );
            info.mq_table.push((
                MessageQueueKey::new(TOPIC, BROKER, 0),
                serde_json::json!({ "commitOffset": 42i64 }),
            ));
            info
        }

        fn consume_message_directly(
            &self,
            msg: MessageExt,
            broker_name: Option<String>,
        ) -> Result<ConsumeMessageDirectlyResult> {
            Ok(ConsumeMessageDirectlyResult {
                consume_result: Some(format!(
                    "{}|{}|{}",
                    msg.topic,
                    broker_name.unwrap_or_else(|| "<none>".to_string()),
                    msg.body.clone().unwrap_or_default().len()
                )),
                ..Default::default()
            })
        }

        fn persist_consumer_offset(self: Arc<Self>) -> ConsumerFuture<()> {
            Box::pin(async move {
                self.persisted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    // ---------------- 220 RESET_CONSUMER_CLIENT_OFFSET ----------------

    #[tokio::test]
    async fn reset_offset_is_handled_off_the_calling_thread() {
        let instance = new_instance();
        let consumer = StubConsumer::new();
        instance.register_consumer(GROUP, consumer.clone());

        let mut header = ResetOffsetRequestHeader::default();
        header.group = Some(GROUP.to_string());
        header.topic = Some(TOPIC.to_string());
        let mut cmd = request(request_code::RESET_CONSUMER_CLIENT_OFFSET, header);
        cmd.set_body(Some(
            ResetOffsetBody {
                offset_table: vec![
                    (MessageQueueKey::new(TOPIC, BROKER, 0), 11i64),
                    (MessageQueueKey::new(TOPIC, BROKER, 1), 22i64),
                ],
            }
            .encode(),
        ));

        instance.process_reset_offset(&cmd);
        // 调用线程立刻返回：此刻替身还在「RPC」里，位点表必须为空。
        assert!(
            guard(&consumer.resets).is_empty(),
            "220 ran inline on the calling thread"
        );

        assert!(wait_until(|| !guard(&consumer.resets).is_empty()).await);
        assert_eq!(
            guard(&consumer.resets).clone(),
            vec![(
                TOPIC.to_string(),
                vec![
                    (MessageQueue::new(TOPIC, BROKER, 0), 11i64),
                    (MessageQueue::new(TOPIC, BROKER, 1), 22i64),
                ],
            )]
        );
        instance.shutdown();
    }

    /// 222 发起方 language=CPP 时 broker 推的是**数组**体（`ResetOffsetBodyForC`）；
    /// map 形状解析器对它只会报错，若不做兜底整笔重置会被静默丢掉。
    #[tokio::test]
    async fn reset_offset_array_body_for_c_is_not_dropped() {
        let instance = new_instance();
        let consumer = StubConsumer::new();
        instance.register_consumer(GROUP, consumer.clone());

        let mut header = ResetOffsetRequestHeader::default();
        header.group = Some(GROUP.to_string());
        header.topic = Some(TOPIC.to_string());
        let mut cmd = request(request_code::RESET_CONSUMER_CLIENT_OFFSET, header);
        // broker 真机报文形状：offsetTable 是数组，每条自带 offset
        cmd.set_body(Some(
            ResetOffsetBodyForC {
                offset_table: vec![
                    MessageQueueForC {
                        topic: TOPIC.to_string(),
                        broker_name: BROKER.to_string(),
                        queue_id: 1,
                        offset: 9,
                    },
                    MessageQueueForC {
                        topic: TOPIC.to_string(),
                        broker_name: BROKER.to_string(),
                        queue_id: 3,
                        offset: 77,
                    },
                ],
            }
            .encode(),
        ));

        instance.process_reset_offset(&cmd);
        assert!(wait_until(|| !guard(&consumer.resets).is_empty()).await);
        assert_eq!(
            guard(&consumer.resets).clone(),
            vec![(
                TOPIC.to_string(),
                vec![
                    (MessageQueue::new(TOPIC, BROKER, 1), 9i64),
                    (MessageQueue::new(TOPIC, BROKER, 3), 77i64),
                ],
            )]
        );
        instance.shutdown();
    }

    /// map 形状解析成功后不得再叠一次 ForC 解析（两条路各自建表），
    /// 否则同一队列会被推两次。表里两条键不同，数量断言足够区分。
    #[tokio::test]
    async fn reset_offset_map_body_is_not_double_counted() {
        let instance = new_instance();
        let consumer = StubConsumer::new();
        instance.register_consumer(GROUP, consumer.clone());

        let mut header = ResetOffsetRequestHeader::default();
        header.group = Some(GROUP.to_string());
        header.topic = Some(TOPIC.to_string());
        let mut cmd = request(request_code::RESET_CONSUMER_CLIENT_OFFSET, header);
        cmd.set_body(Some(
            ResetOffsetBody {
                offset_table: vec![(MessageQueueKey::new(TOPIC, BROKER, 0), 11i64)],
            }
            .encode(),
        ));

        instance.process_reset_offset(&cmd);
        assert!(wait_until(|| !guard(&consumer.resets).is_empty()).await);
        assert_eq!(guard(&consumer.resets).len(), 1);
        assert_eq!(guard(&consumer.resets)[0].1.len(), 1);
        instance.shutdown();
    }

    #[tokio::test]
    async fn reset_offset_without_a_consumer_or_group_is_dropped_quietly() {
        let instance = new_instance();
        // group 不存在：只 warn，不回包、不 panic（oneway）。
        let mut header = ResetOffsetRequestHeader::default();
        header.group = Some("GID_Nobody".to_string());
        header.topic = Some(TOPIC.to_string());
        instance.process_reset_offset(&request(request_code::RESET_CONSUMER_CLIENT_OFFSET, header));
        // 连 group 都没有：同样直接返回。
        instance.process_reset_offset(&request(
            request_code::RESET_CONSUMER_CLIENT_OFFSET,
            ResetOffsetRequestHeader::default(),
        ));
        // 坏 body：解码失败也不该波及调用方。
        let consumer = StubConsumer::new();
        instance.register_consumer(GROUP, consumer.clone());
        // 三种里两种形状都解不出：非法 JSON、offsetTable 是标量（map 侧非对象、
        // ForC 侧非数组）、顶层就是数组 —— 一支手指都不许碰。
        for body in [&b"{not a reset body"[..], br#"{"offsetTable":5}"#, b"[1,2]"] {
            let mut header = ResetOffsetRequestHeader::default();
            header.group = Some(GROUP.to_string());
            let mut cmd = request(request_code::RESET_CONSUMER_CLIENT_OFFSET, header);
            cmd.set_body(Some(body.to_vec()));
            instance.process_reset_offset(&cmd);
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(guard(&consumer.resets).is_empty());
        instance.shutdown();
    }

    // ---------------- 221 GET_CONSUMER_STATUS_FROM_CLIENT ----------------

    #[tokio::test]
    async fn consumer_status_keeps_absent_and_empty_topic_distinct() {
        let instance = new_instance();
        let consumer = StubConsumer::new();
        instance.register_consumer(GROUP, consumer.clone());

        // `header.topic` 原样交给消费者（None 与 "" 语义不同：只有非空
        // topic 才过滤），所以这里必须能区分三种入参。
        let cases: Vec<Option<String>> = vec![None, Some(String::new()), Some(TOPIC.to_string())];
        for topic in cases.clone() {
            let mut header = GetConsumerStatusRequestHeader::default();
            header.group = Some(GROUP.to_string());
            header.topic = topic;
            let resp = instance.process_get_consumer_status(&request(
                request_code::GET_CONSUMER_STATUS_FROM_CLIENT,
                header,
            ));
            assert_eq!(resp.code, response_code::SUCCESS);
            let body = GetConsumerStatusBody::decode(resp.body().unwrap_or_default()).unwrap();
            assert_eq!(
                body.message_queue_table,
                vec![(MessageQueueKey::new(TOPIC, BROKER, 0), 42i64)],
                "messageQueueTable must round-trip with inline MessageQueue keys"
            );
        }
        assert_eq!(
            guard(&consumer.status_topics).clone(),
            vec![None, Some(String::new()), Some(TOPIC.to_string())]
        );
        instance.shutdown();
    }

    #[tokio::test]
    async fn consumer_status_reports_system_error_for_an_unknown_group() {
        let instance = new_instance();
        let mut header = GetConsumerStatusRequestHeader::default();
        header.group = Some("GID_Nobody".to_string());
        let resp = instance.process_get_consumer_status(&request(
            request_code::GET_CONSUMER_STATUS_FROM_CLIENT,
            header,
        ));
        assert_eq!(resp.code, response_code::SYSTEM_ERROR);
        assert_eq!(
            resp.remark.as_deref(),
            Some("no consumer for group=GID_Nobody")
        );
        instance.shutdown();
    }

    // ---------------- 307 GET_CONSUMER_RUNNING_INFO ----------------

    #[tokio::test]
    async fn running_info_body_keeps_inline_queue_keys() {
        let instance = new_instance();
        instance.register_consumer(GROUP, StubConsumer::new());
        let mut header = GetConsumerRunningInfoRequestHeader::default();
        header.consumer_group = Some(GROUP.to_string());
        let resp = instance.process_get_consumer_running_info(&request(
            request_code::GET_CONSUMER_RUNNING_INFO,
            header,
        ));
        assert_eq!(resp.code, response_code::SUCCESS);
        let info = ConsumerRunningInfo::decode(resp.body().unwrap_or_default()).unwrap();
        assert_eq!(
            info.properties
                .get(ConsumerRunningInfo::PROP_NAMESERVER_ADDR),
            Some("127.0.0.1:9876;")
        );
        assert_eq!(info.mq_table.len(), 1);
        assert_eq!(info.mq_table[0].0, MessageQueueKey::new(TOPIC, BROKER, 0));
        assert_eq!(info.mq_table[0].1["commitOffset"], serde_json::json!(42i64));

        let mut missing = GetConsumerRunningInfoRequestHeader::default();
        missing.consumer_group = Some("GID_Nobody".to_string());
        let resp = instance.process_get_consumer_running_info(&request(
            request_code::GET_CONSUMER_RUNNING_INFO,
            missing,
        ));
        assert_eq!(resp.code, response_code::SYSTEM_ERROR);
        instance.shutdown();
    }

    // ---------------- 309 CONSUME_MESSAGE_DIRECTLY ----------------

    #[tokio::test]
    async fn consume_message_directly_encodes_the_result() {
        let instance = new_instance();
        let consumer = StubConsumer::new();
        instance.register_consumer(GROUP, consumer.clone());

        let mut header = ConsumeMessageDirectlyResultRequestHeader::default();
        header.consumer_group = Some(GROUP.to_string());
        header.broker_name = Some(BROKER.to_string());
        let mut cmd = request(request_code::CONSUME_MESSAGE_DIRECTLY, header);
        cmd.set_body(Some(encode_message_ext(&sample_message(), false).unwrap()));

        let resp = instance.process_consume_message_directly(&cmd);
        assert_eq!(resp.code, response_code::SUCCESS);
        let result = ConsumeMessageDirectlyResult::decode(resp.body().unwrap_or_default()).unwrap();
        assert_eq!(
            result.consume_result.as_deref(),
            Some("MqClientUnitTopic|broker-a|19"),
            "topic / brokerName / body length must reach the consumer verbatim"
        );
        instance.shutdown();
    }

    #[tokio::test]
    async fn consume_message_directly_rejects_bad_bodies() {
        let instance = new_instance();
        instance.register_consumer(GROUP, StubConsumer::new());
        let mut header = ConsumeMessageDirectlyResultRequestHeader::default();
        header.consumer_group = Some(GROUP.to_string());

        // 空 body / 缺 body（两处都是 SYSTEM_ERROR + 固定 remark）。
        let mut empty = request(request_code::CONSUME_MESSAGE_DIRECTLY, header.clone());
        empty.set_body(Some(Vec::new()));
        let resp = instance.process_consume_message_directly(&empty);
        assert_eq!(resp.code, response_code::SYSTEM_ERROR);
        assert_eq!(resp.remark.as_deref(), Some("empty message body"));
        let resp = instance.process_consume_message_directly(&request(
            request_code::CONSUME_MESSAGE_DIRECTLY,
            header.clone(),
        ));
        assert_eq!(resp.remark.as_deref(), Some("empty message body"));

        let mut garbage = request(request_code::CONSUME_MESSAGE_DIRECTLY, header);
        garbage.set_body(Some(vec![0u8; 64]));
        let resp = instance.process_consume_message_directly(&garbage);
        assert_eq!(resp.code, response_code::SYSTEM_ERROR);
        assert_eq!(resp.remark.as_deref(), Some("decode message failed"));
        instance.shutdown();
    }

    // ---------------- 40 NOTIFY_CONSUMER_IDS_CHANGED ----------------

    #[tokio::test]
    async fn consumer_ids_changed_notification_wakes_every_consumer() {
        let instance = new_instance();
        let first = StubConsumer::new();
        let second = StubConsumer::new();
        instance.register_consumer(GROUP, first.clone());
        instance.register_consumer("GID_Other", second.clone());

        let mut header = NotifyConsumerIdsChangedRequestHeader::default();
        header.consumer_group = Some(GROUP.to_string());
        instance.process_notify_consumer_ids_changed(
            &request(request_code::NOTIFY_CONSUMER_IDS_CHANGED, header),
            "127.0.0.1:10911",
        );

        assert_eq!(instance.consumer_ids_changed_count(), 1);
        assert_eq!(first.rebalance_wakeups.load(Ordering::SeqCst), 1);
        assert_eq!(second.rebalance_wakeups.load(Ordering::SeqCst), 1);

        instance.process_notify_consumer_ids_changed(
            &request(
                request_code::NOTIFY_CONSUMER_IDS_CHANGED,
                NotifyConsumerIdsChangedRequestHeader::default(),
            ),
            "127.0.0.1:10911",
        );
        assert_eq!(instance.consumer_ids_changed_count(), 2);
        assert_eq!(first.rebalance_wakeups.load(Ordering::SeqCst), 2);

        // 注销后不再被叫醒，但通知本身仍然被处理。
        instance.unregister_consumer(GROUP);
        instance.unregister_consumer("GID_Other");
        instance.rebalance_immediately();
        assert_eq!(first.rebalance_wakeups.load(Ordering::SeqCst), 2);
        assert_eq!(second.rebalance_wakeups.load(Ordering::SeqCst), 2);
        instance.shutdown();
    }

    // ---------------- 326 PUSH_REPLY_MESSAGE_TO_CLIENT ----------------

    /// properties 上线格式由 message_properties_2_string 决定（kv 与条目各有分隔
    /// 符），不能手拼 "k=v"。
    fn wire_properties(key: &str, value: &str) -> String {
        let mut props = crate::remoting::protocol::ext_fields::ExtFields::new();
        props.insert(key.to_string(), value.to_string());
        message_properties_2_string(&props)
    }

    #[test]
    fn reply_message_treats_blank_hosts_as_absent() {
        // 空串不能变成 Some("")。
        let mut header = ReplyMessageRequestHeader::default();
        header.topic = Some(TOPIC.to_string());
        header.born_host = Some(String::new());
        header.store_host = Some("127.0.0.1:10911".to_string());
        header.properties = Some(wire_properties(PROPERTY_CORRELATION_ID, "corr-1"));
        let msg = build_reply_message_ext(&header, Some(b"reply-payload".as_slice())).unwrap();
        assert_eq!(msg.born_host, None, "blank bornHost must stay absent");
        assert_eq!(msg.store_host.as_deref(), Some("127.0.0.1:10911"));
        assert_eq!(msg.topic, TOPIC);
        assert_eq!(msg.body.as_deref(), Some(b"reply-payload".as_slice()));
        assert_eq!(msg.get_property(PROPERTY_CORRELATION_ID), Some("corr-1"));
        assert!(
            msg.get_property(PROPERTY_REPLY_MESSAGE_ARRIVE_TIME)
                .is_some(),
            "arrive time must be stamped"
        );
    }

    #[tokio::test]
    async fn unmatched_reply_message_still_answers_success() {
        // 没有等待中的 request()：只 warn，回 SUCCESS（broker 侧不需要重试）。
        let instance = new_instance();
        let mut header = ReplyMessageRequestHeader::default();
        header.topic = Some(TOPIC.to_string());
        header.properties = Some(wire_properties(PROPERTY_CORRELATION_ID, "corr-unknown"));
        let resp = instance
            .process_reply_message(&request(request_code::PUSH_REPLY_MESSAGE_TO_CLIENT, header));
        assert_eq!(resp.code, response_code::SUCCESS);
        instance.shutdown();
    }

    // ---------------- 生命周期 ----------------

    /// 只让 persist-offset 循环快跑，其余周期任务推到 1 小时之后（离线用例不该联网）。
    fn fast_persist_config() -> MQClientInstanceConfig {
        MQClientInstanceConfig {
            persist_offset_initial_delay_millis: 1,
            persist_offset_interval_millis: 5,
            heartbeat_initial_delay_millis: 3_600_000,
            adjust_pool_initial_delay_millis: 3_600_000,
            route_refresh_interval_millis: 3_600_000,
            ..MQClientInstanceConfig::default()
        }
    }

    fn task_count(instance: &MQClientInstance) -> usize {
        guard(&instance.inner.tasks).len()
    }

    #[tokio::test]
    async fn spawn_periodic_first_tick_lands_at_initial_delay() {
        let id = format!("{GROUP}@periodic-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        let instance = MQClientInstance::with_config(
            &id,
            vec![NAMESRV.to_string()],
            MQClientInstanceConfig {
                // 把内建循环全推到 1 小时之后，只留探测器在跑。
                namesrv_refresh_initial_delay_millis: 3_600_000,
                namesrv_refresh_interval_millis: 3_600_000,
                adjust_pool_initial_delay_millis: 3_600_000,
                adjust_pool_interval_millis: 3_600_000,
                route_refresh_interval_millis: 3_600_000,
                heartbeat_initial_delay_millis: 3_600_000,
                heartbeat_interval_millis: 3_600_000,
                persist_offset_initial_delay_millis: 3_600_000,
                persist_offset_interval_millis: 3_600_000,
                ..MQClientInstanceConfig::default()
            },
        );
        instance.start().await.unwrap();

        let ticks = Arc::new(Mutex::new(Vec::<Instant>::new()));
        let probe = ticks.clone();
        instance.spawn_periodic(
            "probe",
            move |_me| {
                let probe = probe.clone();
                async move {
                    guard(&probe).push(Instant::now());
                }
            },
            30,
            300,
        );

        let t0 = Instant::now();
        assert!(
            wait_until(|| !guard(&ticks).is_empty()).await,
            "首跳压根没发生"
        );
        let first = guard(&ticks)[0].duration_since(t0);
        assert!(
            first < Duration::from_millis(250),
            "首跳晚了 {first:?}：initialDelay=30ms 时首跳就该发生（旧写法要等到 330ms）"
        );

        assert!(
            wait_until(|| guard(&ticks).len() >= 2).await,
            "第二跳没发生"
        );
        let second = guard(&ticks)[1].duration_since(t0);
        assert!(
            second >= Duration::from_millis(300) && second < Duration::from_millis(600),
            "第二跳应落在 initialDelay+period（~330ms），实测 {second:?}"
        );

        instance.shutdown();
    }

    #[tokio::test]
    async fn start_is_idempotent_and_restartable() {
        let id = format!("{GROUP}@life-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        let instance =
            MQClientInstance::with_config(&id, vec![NAMESRV.to_string()], fast_persist_config());
        let consumer = StubConsumer::new();
        instance.register_consumer(GROUP, consumer.clone());

        instance.start().await.unwrap();
        let started = task_count(&instance);
        assert_eq!(started, 4, "route/adjust/heartbeat/persist 各一份");
        assert!(instance.is_started());

        // 同一 clientId 的第二个 producer/consumer 也会调 start()：必须不再拉起第二份循环。
        instance.start().await.unwrap();
        assert_eq!(
            task_count(&instance),
            started,
            "second start spawned duplicate loops"
        );

        assert!(wait_until(|| consumer.persisted.load(Ordering::SeqCst) > 0).await);

        // 守卫读的就是 consumer_table：先摘掉自己，shutdown 才真能拆（见
        // `shutdown_keeps_the_instance_alive_while_tenants_remain`）。
        instance.unregister_consumer(GROUP);
        instance.shutdown();
        assert!(!instance.is_started());
        assert_eq!(
            task_count(&instance),
            0,
            "shutdown left task handles behind"
        );
        let after_stop = consumer.persisted.load(Ordering::SeqCst);

        // 重启必须真能跑：靠 stop 信号复位。
        instance.start().await.unwrap();
        instance.register_consumer(GROUP, consumer.clone());
        assert_eq!(task_count(&instance), started);
        assert!(instance.is_started());
        assert!(
            wait_until(|| consumer.persisted.load(Ordering::SeqCst) > after_stop + 1).await,
            "loops stayed dead after restart"
        );
        instance.unregister_consumer(GROUP);
        instance.shutdown();
    }

    #[tokio::test]
    async fn shutdown_keeps_the_instance_alive_while_tenants_remain() {
        let id = format!("{GROUP}@guard-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        let instance =
            MQClientInstance::with_config(&id, vec![NAMESRV.to_string()], fast_persist_config());
        let consumer = StubConsumer::new();
        instance.register_consumer(GROUP, consumer.clone());
        instance.register_consumer_group("GID_Lite");
        instance.register_producer("PG_First");
        instance.register_producer("PG_Second");
        assert!(instance.has_producer("PG_First"));
        assert!(instance.has_producer("PG_Second"));

        instance.start().await.unwrap();
        let tasks = task_count(&instance);
        assert_eq!(tasks, 4);

        // 一个生产者退出：消费者和另一个生产者还在用，实例必须照常跑。
        instance.unregister_producer("PG_First");
        assert!(!instance.has_producer("PG_First"));
        instance.shutdown();
        assert!(
            instance.is_started(),
            "shutdown tore down a shared instance"
        );
        assert_eq!(task_count(&instance), tasks);
        assert!(
            MQClientInstance::find_instance(&id).is_some(),
            "a guarded shutdown must not unregister the factory"
        );

        // 逐个退完之前的每一次都还是 no-op。
        instance.unregister_producer("PG_Second");
        instance.shutdown();
        assert!(instance.is_started());
        instance.unregister_consumer_group("GID_Lite");
        instance.shutdown();
        assert!(instance.is_started());

        instance.unregister_consumer(GROUP);
        instance.shutdown();
        assert!(!instance.is_started());
        assert_eq!(
            task_count(&instance),
            0,
            "shutdown left task handles behind"
        );
        assert!(
            MQClientInstance::find_instance(&id).is_none(),
            "the factory stayed in INSTANCE_MAP after real shutdown"
        );
        // `JoinHandle::abort` 要到下一个让出点才生效，先留一拍；之后 persist 间隔 5ms，
        // 再等 80ms 计数必须纹丝不动，说明循环真的停了。
        tokio::time::sleep(Duration::from_millis(30)).await;
        let settled = consumer.persisted.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(
            consumer.persisted.load(Ordering::SeqCst),
            settled,
            "the background loop survived the last tenant's shutdown"
        );
    }

    #[tokio::test]
    async fn start_failure_cleans_up_and_unregisters_the_instance() {
        let id = format!("{GROUP}@startfail-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        let instance = MQClientInstance::with_config(
            &id,
            Vec::new(),
            MQClientInstanceConfig {
                // 没配静态地址 ⇒ 走动态取址；地址服务器不可达 ⇒ start 必失败
                top_addressing: Some(DefaultTopAddressing::from_domain("127.0.0.1:1")),
                ..Default::default()
            },
        );
        instance
            .start()
            .await
            .expect_err("an unreachable address server must fail start()");
        assert!(!instance.is_started());
        assert_eq!(
            task_count(&instance),
            0,
            "failed start left background loops behind"
        );
        assert!(
            MQClientInstance::find_instance(&id).is_none(),
            "a factory that never reached RUNNING stayed in INSTANCE_MAP"
        );
        let rebuilt = MQClientInstance::create_mq_client_instance(
            &id,
            vec![NAMESRV.to_string()],
            MQClientInstanceConfig::default(),
        );
        assert_eq!(rebuilt.name_server_addrs(), vec![NAMESRV.to_string()]);
        rebuilt.shutdown();
    }

    /// 守卫通过后拆的是自己那份：同 clientId 已被更晚的实例覆盖时，
    /// 不能把别人的登记一起摘掉（`with_config` 是无条件 insert）。
    #[tokio::test]
    async fn shutdown_does_not_unregister_a_newer_instance_with_the_same_client_id() {
        let id = format!("{GROUP}@overwrite-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        let older = MQClientInstance::with_config(
            &id,
            vec!["127.0.0.1:1".to_string()],
            MQClientInstanceConfig::default(),
        );
        let newer = MQClientInstance::with_config(
            &id,
            vec![NAMESRV.to_string()],
            MQClientInstanceConfig::default(),
        );
        assert_eq!(
            MQClientInstance::find_instance(&id)
                .expect("newest instance must own the registration")
                .name_server_addrs(),
            newer.name_server_addrs(),
            "the newer instance must win the same clientId slot"
        );

        older.shutdown();
        let still_there = MQClientInstance::find_instance(&id).expect("newer entry must survive");
        assert_eq!(still_there.name_server_addrs(), newer.name_server_addrs());
        newer.shutdown();
        assert!(MQClientInstance::find_instance(&id).is_none());
    }

    #[tokio::test]
    async fn instance_map_and_broker_push_processor_hold_only_weak_refs() {
        let id = format!("{GROUP}@weak-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        let marker = "202.1.2.3:9876".to_string();
        {
            let instance = MQClientInstance::create_mq_client_instance(
                &id,
                vec![NAMESRV.to_string()],
                MQClientInstanceConfig::default(),
            );
            instance.update_name_server_address_list(std::slice::from_ref(&marker));
            let same = MQClientInstance::find_instance(&id).expect("registered on creation");
            assert_eq!(
                same.name_server_addrs(),
                vec![marker.clone()],
                "create_mq_client_instance must hand out the shared instance"
            );
        }
        assert!(
            MQClientInstance::find_instance(&id).is_none(),
            "a leaked instance kept the weak entry alive"
        );
        // 登记表失效后，同 clientId 必须拿到全新实例（而不是带着旧地址的死实例）。
        let fresh = MQClientInstance::create_mq_client_instance(
            &id,
            vec![NAMESRV.to_string()],
            MQClientInstanceConfig::default(),
        );
        assert_eq!(fresh.name_server_addrs(), vec![NAMESRV.to_string()]);
        assert_eq!(fresh.client_id(), id);
    }

    #[tokio::test]
    async fn rpc_hook_chain_is_namespace_then_stream_then_user_acl() {
        use crate::remoting::rpchook::{AclClientRPCHook, SessionCredentials};
        fn cmd() -> RemotingCommand {
            let mut cmd =
                RemotingCommand::create_request_command(request_code::SEND_MESSAGE_V2, None);
            cmd.add_ext_field("topic", "T");
            cmd
        }
        let id = format!("{GROUP}@hook-order-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        let instance = MQClientInstance::with_config(
            &id,
            vec![NAMESRV.to_string()],
            MQClientInstanceConfig {
                namespace_v2: Some("NS_V2".to_string()),
                enable_stream_request_type: true,
                ..Default::default()
            },
        );
        // 门面的用户钩子（ACL）是**实例创建之后**注册的，与五个门面的 start() 同构。
        instance
            .remoting_client()
            .register_rpc_hook(Arc::new(AclClientRPCHook::new(SessionCredentials::new(
                "AK", "SK",
            ))));
        let hooks = instance.remoting_client().rpc_hooks();
        assert_eq!(
            hooks.len(),
            3,
            "链上必须恰好是 Namespace → Stream → 用户 ACL"
        );

        // hooks[0] = Namespace：只写 nsd/ns，不碰 ReqT / AccessKey
        let mut c0 = cmd();
        hooks[0].do_before_request(NAMESRV, &mut c0);
        assert_eq!(
            c0.get_ext_field(MixAll::RPC_REQUEST_HEADER_NAMESPACED_FIELD),
            Some("true")
        );
        assert_eq!(
            c0.get_ext_field(MixAll::RPC_REQUEST_HEADER_NAMESPACE_FIELD),
            Some("NS_V2")
        );
        assert_eq!(
            c0.get_ext_field(MixAll::REQ_T),
            None,
            "第一位必须是 Namespace"
        );
        assert_eq!(c0.get_ext_field(SessionCredentials::ACCESS_KEY), None);

        // hooks[1] = Stream：写 ReqT，仍轮不到 ACL
        let mut c1 = cmd();
        hooks[0].do_before_request(NAMESRV, &mut c1);
        hooks[1].do_before_request(NAMESRV, &mut c1);
        assert_eq!(c1.get_ext_field(MixAll::REQ_T), Some("0"));
        assert_eq!(
            c1.get_ext_field(SessionCredentials::ACCESS_KEY),
            None,
            "第二位必须是 Stream：ACL 还没跑"
        );

        // 全链跑完：Signature 逐字节等于「nsd/ns/ReqT 都在签名内容里」的期望值
        let mut full = cmd();
        for hook in hooks.iter() {
            hook.do_before_request(NAMESRV, &mut full);
        }
        assert_eq!(
            full.get_ext_field(SessionCredentials::ACCESS_KEY),
            Some("AK")
        );
        let mut expected = cmd();
        expected.add_ext_field(MixAll::RPC_REQUEST_HEADER_NAMESPACED_FIELD, "true");
        expected.add_ext_field(MixAll::RPC_REQUEST_HEADER_NAMESPACE_FIELD, "NS_V2");
        expected.add_ext_field(MixAll::REQ_T, "0");
        expected.add_ext_field(SessionCredentials::ACCESS_KEY, "AK");
        assert_eq!(
            full.get_ext_field(SessionCredentials::SIGNATURE),
            Some(AclClientRPCHook::calc_signature("SK", &mut expected).as_str()),
            "签名内容必须覆盖 nsd/ns/ReqT"
        );

        // 对照组：只跑用户 ACL（没有 Namespace/Stream 垫前）⇒ 签名不同，
        // 即 namespace_v2 的配置会改变最终上线的签名。
        let mut plain = cmd();
        hooks[2].do_before_request(NAMESRV, &mut plain);
        assert_ne!(
            plain.get_ext_field(SessionCredentials::SIGNATURE),
            full.get_ext_field(SessionCredentials::SIGNATURE)
        );
        instance.shutdown();
    }

    #[tokio::test]
    async fn rpc_hook_chain_without_namespace_v2_keeps_request_untouched() {
        let id = format!("{GROUP}@hook-none-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        let instance = MQClientInstance::with_config(
            &id,
            vec![NAMESRV.to_string()],
            MQClientInstanceConfig::default(),
        );
        let hooks = instance.remoting_client().rpc_hooks();
        assert_eq!(
            hooks.len(),
            1,
            "默认配置只有 Namespace 一个钩子（无 stream）"
        );
        let mut cmd = RemotingCommand::create_request_command(request_code::SEND_MESSAGE_V2, None);
        cmd.add_ext_field("topic", "T");
        let before = cmd.ext_fields().sorted().len();
        hooks[0].do_before_request(NAMESRV, &mut cmd);
        assert_eq!(cmd.get_ext_field("nsd"), None);
        assert_eq!(cmd.get_ext_field("ns"), None);
        assert_eq!(
            cmd.ext_fields().sorted().len(),
            before,
            "no-op 钩子不许加头"
        );
        assert_eq!(cmd.get_ext_field(MixAll::REQ_T), None);
        instance.shutdown();
    }

    // ---------------- 46 CHECK_CLIENT_CONFIG ----------------

    /// 假 broker 收到的一笔请求（线上原样，未经任何解码便捷封装）。
    #[derive(Debug, Clone)]
    struct Recorded {
        code: i32,
        ext_fields: Vec<(String, String)>,
        body: Vec<u8>,
    }

    /// 脚本里排队的应答 extFields 表。
    type ExtFieldsQueue = Arc<Mutex<VecDeque<Vec<(String, String)>>>>;

    /// 进程内假 broker：只说 remoting 协议，逐笔按脚本应答。
    struct MockBroker {
        addr: String,
        state: Arc<Mutex<Vec<Recorded>>>,
        /// 脚本：下一笔请求的应答码（用完一律回 SUCCESS）。
        codes: Arc<Mutex<VecDeque<i32>>>,
        remarks: Arc<Mutex<VecDeque<String>>>,
        /// 脚本：下一笔应答的 extFields（用完为空表）。PULL_MESSAGE 的
        /// `suggestWhichBrokerId` / `nextBeginOffset` 之类都靠它上线。
        resp_ext: ExtFieldsQueue,
        /// 脚本：下一笔应答的 body（用完不回 body）。锁集之类的回包靠它上线。
        resp_body: Arc<Mutex<VecDeque<Vec<u8>>>>,
    }

    impl MockBroker {
        async fn start() -> Arc<MockBroker> {
            let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
                Ok(l) => l,
                Err(e) => panic!("bind mock broker: {e}"),
            };
            let addr = match listener.local_addr() {
                Ok(a) => a.to_string(),
                Err(e) => panic!("mock broker addr: {e}"),
            };
            let broker = Arc::new(MockBroker {
                addr,
                state: Arc::new(Mutex::new(Vec::new())),
                codes: Arc::new(Mutex::new(VecDeque::new())),
                remarks: Arc::new(Mutex::new(VecDeque::new())),
                resp_ext: Arc::new(Mutex::new(VecDeque::new())),
                resp_body: Arc::new(Mutex::new(VecDeque::new())),
            });
            let inner = Arc::clone(&broker);
            tokio::spawn(async move {
                loop {
                    let accepted = match listener.accept().await {
                        Ok(pair) => pair,
                        Err(_) => return,
                    };
                    let broker = Arc::clone(&inner);
                    tokio::spawn(async move {
                        let mut stream = accepted.0;
                        while let Some(request) = read_request(&mut stream).await {
                            let code = guard(&broker.codes).pop_front();
                            let remark = guard(&broker.remarks).pop_front();
                            let ext = guard(&broker.resp_ext).pop_front().unwrap_or_default();
                            let body = guard(&broker.resp_body).pop_front();
                            guard(&broker.state).push(Recorded {
                                code: request.code,
                                ext_fields: request
                                    .ext_fields()
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect(),
                                body: request.body().unwrap_or_default().to_vec(),
                            });
                            let _ = write_response(
                                &mut stream,
                                &request,
                                code.unwrap_or(response_code::SUCCESS),
                                remark,
                                ext,
                                body,
                            )
                            .await;
                        }
                    });
                }
            });
            broker
        }

        fn script(&self, codes: Vec<i32>, remarks: Vec<String>) {
            *guard(&self.codes) = codes.into();
            *guard(&self.remarks) = remarks.into();
        }

        /// 脚本化后续应答的 extFields（缺省空表，与改造前行为一致）。
        fn script_resp_ext(&self, exts: Vec<Vec<(String, String)>>) {
            *guard(&self.resp_ext) = exts.into();
        }

        /// 脚本化后续应答的 body（缺省不回 body）。
        fn script_resp_body(&self, bodies: Vec<Vec<u8>>) {
            *guard(&self.resp_body) = bodies.into();
        }

        /// 只保留 46 号请求：路由刷新等副作用不该混进断言。
        fn checks(&self) -> Vec<Recorded> {
            self.recorded(request_code::CHECK_CLIENT_CONFIG)
        }

        fn recorded(&self, code: i32) -> Vec<Recorded> {
            guard(&self.state)
                .iter()
                .filter(|r| r.code == code)
                .cloned()
                .collect()
        }

        /// 第 `n` 笔 46 号请求的 body 解析成 JSON。
        fn body_of(&self, n: usize) -> serde_json::Value {
            let checks = self.checks();
            match checks.get(n) {
                Some(r) => serde_json::from_slice(&r.body).unwrap_or(serde_json::Value::Null),
                None => serde_json::Value::Null,
            }
        }
    }

    /// 读一整帧并解码（`decode` 从 totalLength 开始，所以帧要连长度前缀一起给）。
    async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<RemotingCommand> {
        use tokio::io::AsyncReadExt as _;
        let mut len_buf = [0_u8; 4];
        stream.read_exact(&mut len_buf).await.ok()?;
        let total = i32::from_be_bytes(len_buf);
        if total <= 4 || total > 20 * 1024 * 1024 {
            return None;
        }
        let usize_len = usize::try_from(total).ok()?;
        let mut frame = vec![0_u8; usize_len + 4];
        frame[..4].copy_from_slice(&len_buf);
        stream.read_exact(&mut frame[4..]).await.ok()?;
        RemotingCommand::decode(&frame).ok()
    }

    async fn write_response(
        stream: &mut tokio::net::TcpStream,
        request: &RemotingCommand,
        code: i32,
        remark: Option<String>,
        ext_fields: Vec<(String, String)>,
        body: Option<Vec<u8>>,
    ) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt as _;
        let mut response = RemotingCommand::create_response(code, remark);
        // 客户端按 opaque 配对响应，串了就当噪声丢掉，所以必须回原值。
        response.opaque = request.opaque;
        response.serialize_type_current_rpc = request.serialize_type_current_rpc;
        for (k, v) in ext_fields {
            response.ext_fields_mut().insert(&k, v);
        }
        response.set_body(body);
        let bytes = response.encode();
        stream.write_all(&bytes).await?;
        stream.flush().await
    }

    fn sql92_sub(topic: &str, expression: &str) -> SubscriptionData {
        SubscriptionData {
            topic: topic.to_string(),
            sub_string: expression.to_string(),
            expression_type: ExpressionType::SQL92.to_string(),
            ..SubscriptionData::default()
        }
    }

    fn tag_sub(topic: &str, expression: &str, expression_type: &str) -> SubscriptionData {
        SubscriptionData {
            topic: topic.to_string(),
            sub_string: expression.to_string(),
            expression_type: expression_type.to_string(),
            ..SubscriptionData::default()
        }
    }

    /// 把路由塞进实例缓存，broker 地址指向 `addr`（`route_of` 只读缓存，不会补拉）。
    fn seed_route(instance: &MQClientInstance, topic: &str, broker_name: &str, addr: &str) {
        let route = TopicRouteData {
            queue_datas: vec![QueueData::new(
                broker_name,
                1,
                1,
                PermName::PERM_READ | PermName::PERM_WRITE,
                0,
            )],
            broker_datas: vec![BrokerData::new(
                "DefaultCluster",
                broker_name,
                vec![(i64::from(MixAll::MASTER_ID), addr.to_string())],
                "",
            )],
            ..Default::default()
        };
        guard(&instance.inner.tables)
            .topic_route_table
            .insert(topic.to_string(), route);
    }

    /// 订阅 + 缓存路由都就位，返回可以直接跑 `check_client_in_broker` 的实例。
    fn instance_with_sub(broker: &MockBroker, subs: Vec<SubscriptionData>) -> MQClientInstance {
        let instance = new_instance();
        for sub in &subs {
            seed_route(&instance, &sub.topic, BROKER, &broker.addr);
        }
        instance.register_consumer(GROUP, StubConsumer::with_subs(GROUP, subs));
        instance
    }

    #[tokio::test]
    async fn tag_only_subscriptions_send_no_check_request() {
        let broker = MockBroker::start().await;
        let instance = instance_with_sub(
            &broker,
            vec![
                tag_sub(TOPIC, "tagA || tagB", ExpressionType::TAG),
                tag_sub("NullTypeTopic", "*", ""),
            ],
        );
        instance
            .check_client_in_broker()
            .await
            .expect("TAG-only must not fail");
        assert!(
            broker.checks().is_empty(),
            "TAG subscriptions must not reach the broker"
        );
        instance.shutdown();
    }

    #[tokio::test]
    async fn sql92_request_shape_matches_broker_contract() {
        let broker = MockBroker::start().await;
        let instance = instance_with_sub(&broker, vec![sql92_sub(TOPIC, "a > 10")]);
        instance
            .check_client_in_broker()
            .await
            .expect("SUCCESS reply");
        let checks = broker.checks();
        assert_eq!(checks.len(), 1, "one SQL92 sub ⇒ exactly one 46");
        // 请求头是 null ⇒ 线上没有 extFields。
        assert!(
            checks[0].ext_fields.is_empty(),
            "header must be null: {:?}",
            checks[0].ext_fields
        );

        let body = broker.body_of(0);
        assert_eq!(body["clientId"], instance.client_id());
        assert_eq!(body["group"], GROUP);
        let sd = &body["subscriptionData"];
        assert_eq!(sd["topic"], TOPIC);
        assert_eq!(sd["subString"], "a > 10");
        assert_eq!(sd["expressionType"], ExpressionType::SQL92);
        let mut keys: Vec<&str> = sd
            .as_object()
            .expect("subscriptionData is an object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "classFilterMode",
                "codeSet",
                "expressionType",
                "subString",
                "subVersion",
                "tagsSet",
                "topic"
            ]
        );
        instance.shutdown();
    }

    #[tokio::test]
    async fn broker_reject_code_becomes_client_error_with_that_code() {
        // SUBSCRIPTION_PARSE_FAILED(23) 要原样带上响应码 —— 这是启动失败的判据。
        let broker = MockBroker::start().await;
        broker.script(
            vec![response_code::SUBSCRIPTION_PARSE_FAILED],
            vec!["bad sql92".to_string()],
        );
        let instance = instance_with_sub(&broker, vec![sql92_sub(TOPIC, "a >")]);
        let err = instance
            .check_client_in_broker()
            .await
            .expect_err("23 must fail the check");
        assert_eq!(
            err.response_code(),
            Some(response_code::SUBSCRIPTION_PARSE_FAILED)
        );
        assert!(err.to_string().contains("bad sql92"), "remark lost: {err}");
        instance.shutdown();
    }

    #[tokio::test]
    async fn broker_without_property_filter_reports_system_error() {
        let broker = MockBroker::start().await;
        broker.script(vec![response_code::SYSTEM_ERROR], vec![]);
        let instance = instance_with_sub(&broker, vec![sql92_sub(TOPIC, "a > 10")]);
        let err = instance
            .check_client_in_broker()
            .await
            .expect_err("SYSTEM_ERROR must fail");
        assert_eq!(err.response_code(), Some(response_code::SYSTEM_ERROR));
        instance.shutdown();
    }

    #[tokio::test]
    async fn missing_route_skips_the_subscription() {
        let instance = new_instance();
        instance.register_consumer(
            GROUP,
            StubConsumer::with_subs(GROUP, vec![sql92_sub(TOPIC, "a > 10")]),
        );
        instance
            .check_client_in_broker()
            .await
            .expect("no route must be skipped, not fatal");
        instance.shutdown();
    }

    #[tokio::test]
    async fn transport_error_is_wrapped_with_canonical_message() {
        let idle = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind an idle port");
        let dead_addr = match idle.local_addr() {
            Ok(a) => a.to_string(),
            Err(e) => panic!("idle addr: {e}"),
        };
        drop(idle);
        let instance = new_instance();
        seed_route(&instance, TOPIC, BROKER, &dead_addr);
        instance.register_consumer(
            GROUP,
            StubConsumer::with_subs(GROUP, vec![sql92_sub(TOPIC, "a > 10")]),
        );
        let err = instance
            .check_client_in_broker()
            .await
            .expect_err("dead broker must fail");
        let text = err.to_string();
        assert!(
            text.contains(ExpressionType::SQL92),
            "expression type lost: {text}"
        );
        assert!(
            text.contains("server has not been upgraded to support"),
            "wrap message missing: {text}"
        );
        instance.shutdown();
    }

    #[tokio::test]
    async fn consumer_without_subscriptions_ends_the_whole_check() {
        let broker = MockBroker::start().await;
        let empty = StubConsumer::with_subs("A_empty_group", Vec::new());
        let sql = StubConsumer::with_subs("B_sql_group", vec![sql92_sub(TOPIC, "a > 10")]);
        let instance = new_instance();
        seed_route(&instance, TOPIC, BROKER, &broker.addr);
        instance.register_consumer("A_empty_group", empty);
        instance.register_consumer("B_sql_group", sql);
        instance
            .check_client_in_broker()
            .await
            .expect("the quirk is silent, not fatal");
        assert!(
            broker.checks().is_empty(),
            "the check must stop at the first empty consumer"
        );
        instance.shutdown();
    }

    #[tokio::test]
    async fn inner_helper_covers_unregistered_callers() {
        // consumerTable 只收推模式消费者；lite 拉 / 拉模式消费者用内层循环拿同样语义。
        let broker = MockBroker::start().await;
        let instance = new_instance();
        seed_route(&instance, TOPIC, BROKER, &broker.addr);
        instance
            .check_subscriptions_in_broker("GID_Unregistered", &[sql92_sub(TOPIC, "a > 10")])
            .await
            .expect("SUCCESS reply");
        let checks = broker.checks();
        assert_eq!(checks.len(), 1);
        let body = broker.body_of(0);
        assert_eq!(body["group"], "GID_Unregistered");
        instance.shutdown();
    }

    #[tokio::test]
    async fn master_broker_is_preferred_and_only_one_addr_is_used() {
        // `selectBrokerAddr`：MASTER_ID(0) 在表里就用 master，不会挑到 slave。
        let broker = MockBroker::start().await;
        let instance = new_instance();
        let route = TopicRouteData {
            queue_datas: vec![QueueData::new(
                BROKER,
                1,
                1,
                PermName::PERM_READ | PermName::PERM_WRITE,
                0,
            )],
            broker_datas: vec![BrokerData::new(
                "DefaultCluster",
                BROKER,
                vec![
                    (i64::from(MixAll::MASTER_ID) + 1, "127.0.0.1:1".to_string()),
                    (i64::from(MixAll::MASTER_ID), broker.addr.clone()),
                ],
                "",
            )],
            ..Default::default()
        };
        guard(&instance.inner.tables)
            .topic_route_table
            .insert(TOPIC.to_string(), route);
        assert_eq!(
            instance.find_broker_addr_by_topic(TOPIC).as_deref(),
            Some(broker.addr.as_str()),
            "master must win over the acting slave"
        );
        assert_eq!(instance.find_broker_addr_by_topic("NoSuchTopic"), None);
        instance.shutdown();
    }

    #[tokio::test]
    async fn unregister_reaches_slaves_too() {
        let master = MockBroker::start().await;
        let slave = MockBroker::start().await;
        let instance = new_instance();
        let route = TopicRouteData {
            queue_datas: vec![QueueData::new(
                BROKER,
                1,
                1,
                PermName::PERM_READ | PermName::PERM_WRITE,
                0,
            )],
            broker_datas: vec![BrokerData::new(
                "DefaultCluster",
                BROKER,
                vec![
                    (i64::from(MixAll::MASTER_ID), master.addr.clone()),
                    (i64::from(MixAll::MASTER_ID) + 1, slave.addr.clone()),
                ],
                "",
            )],
            ..Default::default()
        };
        guard(&instance.inner.tables)
            .topic_route_table
            .insert(TOPIC.to_string(), route);
        // 两个 helper 的分工：`get_route_of_all_brokers` 每个 brokerName 只挑一台
        // （生产者心跳用它），`get_all_broker_addrs` 每台都算（注销、消费者心跳用它）
        assert_eq!(
            instance.get_route_of_all_brokers(),
            vec![master.addr.clone()]
        );
        let mut want = vec![master.addr.clone(), slave.addr.clone()];
        want.sort();
        assert_eq!(instance.get_all_broker_addrs(), want);

        instance
            .unregister_client_all_brokers("cid-1", "GID_p", "", 3000)
            .await;
        let unregs = |b: &Arc<MockBroker>| b.recorded(request_code::UNREGISTER_CLIENT);
        assert_eq!(unregs(&master).len(), 1, "master 收到一发 35");
        assert_eq!(unregs(&slave).len(), 1, "slave 也收到一发 35");
        for rec in unregs(&master).into_iter().chain(unregs(&slave)) {
            let keys: Vec<&str> = rec.ext_fields.iter().map(|(k, _)| k.as_str()).collect();
            assert_eq!(keys, vec!["clientID", "producerGroup"], "空白槽位不上线");
        }
        instance.shutdown();
    }

    /// 消费者心跳必须覆盖**每一台** broker（主 + 从），只带生产数据的心跳只打主。
    #[tokio::test]
    async fn consumer_heartbeat_reaches_slaves_too() {
        let master = MockBroker::start().await;
        let slave = MockBroker::start().await;
        let instance = new_instance();
        let route = TopicRouteData {
            queue_datas: vec![QueueData::new(
                BROKER,
                1,
                1,
                PermName::PERM_READ | PermName::PERM_WRITE,
                0,
            )],
            broker_datas: vec![BrokerData::new(
                "DefaultCluster",
                BROKER,
                vec![
                    (i64::from(MixAll::MASTER_ID), master.addr.clone()),
                    (i64::from(MixAll::MASTER_ID) + 1, slave.addr.clone()),
                ],
                "",
            )],
            ..Default::default()
        };
        guard(&instance.inner.tables)
            .topic_route_table
            .insert(TOPIC.to_string(), route);

        assert_eq!(instance.send_heartbeat_to_all_broker(3000).await, 0);
        assert!(master.recorded(request_code::HEART_BEAT).is_empty());
        assert!(slave.recorded(request_code::HEART_BEAT).is_empty());

        instance.register_consumer(GROUP, StubConsumer::new());
        assert_eq!(
            instance.send_heartbeat_to_all_broker(3000).await,
            2,
            "主 + 从各一发"
        );
        assert_eq!(master.recorded(request_code::HEART_BEAT).len(), 1);
        let on_slave = slave.recorded(request_code::HEART_BEAT);
        assert_eq!(on_slave.len(), 1, "从节点也要收到心跳");
        let hb =
            HeartbeatData::decode(&on_slave[0].body).expect("heartbeat body is a HeartbeatData");
        assert_eq!(hb.client_id, instance.client_id());
        let groups: Vec<&str> = hb
            .consumer_data_set
            .iter()
            .map(|cd| cd.group_name.as_str())
            .collect();
        assert_eq!(groups, vec![GROUP]);
        instance.shutdown();
    }

    #[test]
    fn blank_unregister_groups_stay_off_the_wire() {
        assert_eq!(blank_group_to_none(""), None);
        assert_eq!(blank_group_to_none("   "), None);
        assert_eq!(blank_group_to_none("GID_x").as_deref(), Some("GID_x"));

        let header_ext = |producer: &str, consumer: &str| -> Vec<String> {
            crate::remoting::protocol::ext_fields::ExtFields::from_header(
                &UnregisterClientRequestHeader {
                    client_id: Some("cid".to_string()),
                    producer_group: blank_group_to_none(producer),
                    consumer_group: blank_group_to_none(consumer),
                },
            )
            .iter()
            .map(|(k, _)| k.clone())
            .collect()
        };
        // 生产者退出：只有 clientID + producerGroup
        assert_eq!(
            header_ext("GID_p", ""),
            vec!["clientID".to_string(), "producerGroup".to_string()]
        );
        // 消费者退出：只有 clientID + consumerGroup
        assert_eq!(
            header_ext("", "GID_c"),
            vec!["clientID".to_string(), "consumerGroup".to_string()]
        );
    }

    // ---------------- P5: pullFromWhichNode / findBrokerAddressInSubscribe ----------------

    #[test]
    fn find_broker_addr_in_subscribe_branches() {
        let addrs = vec![
            (0_i64, "127.0.0.1:10911".to_string()),
            (1, "127.0.0.1:10921".to_string()),
            (2, "127.0.0.1:10931".to_string()),
        ];
        // 命中：id 直接用，slave = id != 0
        assert_eq!(
            MQClientInstance::find_broker_addr_in_subscribe(&addrs, 0, false),
            Some(("127.0.0.1:10911".to_string(), false))
        );
        assert_eq!(
            MQClientInstance::find_broker_addr_in_subscribe(&addrs, 1, false),
            Some(("127.0.0.1:10921".to_string(), true))
        );
        let sparse = vec![
            (0_i64, "127.0.0.1:10911".to_string()),
            (2, "127.0.0.1:10931".to_string()),
        ];
        assert_eq!(
            MQClientInstance::find_broker_addr_in_subscribe(&sparse, 1, false),
            Some(("127.0.0.1:10931".to_string(), true))
        );
        assert_eq!(
            MQClientInstance::find_broker_addr_in_subscribe(&addrs, 9, false),
            Some(("127.0.0.1:10911".to_string(), false))
        );
        assert_eq!(
            MQClientInstance::find_broker_addr_in_subscribe(
                &[(1_i64, "127.0.0.1:10921".to_string())],
                3,
                false
            ),
            Some(("127.0.0.1:10921".to_string(), true))
        );
        // 只认这台（onlyThisBroker）时宁缺毋滥
        assert_eq!(
            MQClientInstance::find_broker_addr_in_subscribe(&addrs, 9, true),
            None
        );
        assert_eq!(
            MQClientInstance::find_broker_addr_in_subscribe(&[], 0, false),
            None
        );
    }

    /// 建好「主 slave 各一台假 broker」的实例：返回 (instance, master, slave)。
    async fn instance_with_master_slave() -> (MQClientInstance, Arc<MockBroker>, Arc<MockBroker>) {
        let master = MockBroker::start().await;
        let slave = MockBroker::start().await;
        let instance = new_instance();
        let route = TopicRouteData {
            queue_datas: vec![QueueData::new(
                BROKER,
                1,
                1,
                PermName::PERM_READ | PermName::PERM_WRITE,
                0,
            )],
            broker_datas: vec![BrokerData::new(
                "DefaultCluster",
                BROKER,
                vec![
                    (i64::from(MixAll::MASTER_ID), master.addr.clone()),
                    (i64::from(MixAll::MASTER_ID) + 1, slave.addr.clone()),
                ],
                "",
            )],
            ..Default::default()
        };
        guard(&instance.inner.tables)
            .topic_route_table
            .insert(TOPIC.to_string(), route);
        (instance, master, slave)
    }

    /// 拉取请求的 extFields 里挑后缀匹配的键（断言报文形状用）。
    fn ext_of(rec: &Recorded, key: &str) -> Option<String> {
        rec.ext_fields
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    }

    #[tokio::test]
    async fn pull_message_routes_to_slave_and_clears_commit_offset() {
        let (instance, master, slave) = instance_with_master_slave().await;
        slave.script_resp_ext(vec![vec![
            ("nextBeginOffset".to_string(), "1".to_string()),
            ("minOffset".to_string(), "0".to_string()),
            ("maxOffset".to_string(), "10".to_string()),
            ("suggestWhichBrokerId".to_string(), "0".to_string()),
        ]]);

        let mq = MessageQueue::new(TOPIC, BROKER, 0);
        let sys_flag = PullSysFlag::build_sys_flag_basic(true, true, false, false);
        assert!(PullSysFlag::has_commit_offset_flag(sys_flag));

        let result = instance
            .pull_message(
                GROUP,
                &mq,
                0,
                32,
                sys_flag,
                0,
                "TagA",
                0,
                "TAG",
                3000,
                -1,
                15000,
                None,
                0,
                Some(i64::from(MixAll::MASTER_ID) + 1),
            )
            .await
            .expect("pull to slave");

        // 打到了 slave，不是 master
        assert_eq!(slave.recorded(request_code::PULL_MESSAGE).len(), 1);
        assert!(master.recorded(request_code::PULL_MESSAGE).is_empty());
        let rec = &slave.recorded(request_code::PULL_MESSAGE)[0];
        let sent_flag: i32 = ext_of(rec, "sysFlag").expect("sysFlag").parse().unwrap();
        assert!(
            !PullSysFlag::has_commit_offset_flag(sent_flag),
            "slave 上一次 COMMIT_OFFSET 都没有意义"
        );
        assert!(PullSysFlag::has_suspend_flag(sent_flag), "suspend 位保持");
        assert!(
            ext_of(rec, "subscription").is_none(),
            "SUBSCRIPTION 位关闭时该字段根本不进 extFields（null → 丢字段）"
        );
        // 应答头 suggestWhichBrokerId 透传给调用方，供回写 pullFromWhichNodeTable
        assert_eq!(result.suggest_which_broker_id, Some(0));
        instance.shutdown();
    }

    /// brokerId=0（主节点）时：请求打到 master、`COMMIT_OFFSET` 位保留；SUBSCRIPTION
    /// 位置位时 `subscription` 才上线，内容是调用方给的表达式。
    #[tokio::test]
    async fn pull_message_keeps_commit_offset_on_master_and_posts_subscription() {
        let (instance, master, _slave) = instance_with_master_slave().await;
        master.script_resp_ext(vec![vec![
            ("nextBeginOffset".to_string(), "7".to_string()),
            ("minOffset".to_string(), "0".to_string()),
            ("maxOffset".to_string(), "10".to_string()),
        ]]);

        let mq = MessageQueue::new(TOPIC, BROKER, 0);
        let sys_flag = PullSysFlag::build_sys_flag_basic(true, true, true, false);
        let result = instance
            .pull_message(
                GROUP,
                &mq,
                3,
                32,
                sys_flag,
                0,
                "TagA||TagB",
                0,
                "TAG",
                3000,
                -1,
                15000,
                None,
                0,
                Some(i64::from(MixAll::MASTER_ID)),
            )
            .await
            .expect("pull to master");

        let recs = master.recorded(request_code::PULL_MESSAGE);
        assert_eq!(recs.len(), 1);
        let sent_flag: i32 = ext_of(&recs[0], "sysFlag").unwrap().parse().unwrap();
        assert!(
            PullSysFlag::has_commit_offset_flag(sent_flag),
            "master 上位点照提交"
        );
        assert!(PullSysFlag::has_subscription_flag(sent_flag));
        assert_eq!(
            ext_of(&recs[0], "subscription").as_deref(),
            Some("TagA||TagB")
        );
        assert_eq!(ext_of(&recs[0], "queueOffset").as_deref(), Some("3"));
        // 老 broker 不带 suggestWhichBrokerId：调用方按 master=0 记账（None 传出去）
        assert_eq!(result.suggest_which_broker_id, None);
        assert_eq!(result.next_begin_offset, 7);
        instance.shutdown();
    }


    /// 假 namesrv：`GET_ROUTEINFO_BY_TOPIC` 回 `holder` 里那份路由 body
    /// （`None` ⇒ TOPIC_NOT_EXIST），其余一律 SUCCESS；把收到的请求码记下来。
    ///
    /// 路由一律走**真的** `update_topic_route_info_from_name_server` 落库：发布地址
    /// 平表的写入点就在它里面，绕过去直接往 `broker_addr_table` 塞值等于这条路径没测。
    struct RouteNamesrv {
        addr: String,
        holder: Arc<Mutex<Option<Vec<u8>>>>,
        codes: Arc<Mutex<Vec<i32>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl RouteNamesrv {
        async fn start() -> RouteNamesrv {
            let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
                Ok(l) => l,
                Err(e) => panic!("bind 假 namesrv: {e}"),
            };
            let addr = match listener.local_addr() {
                Ok(a) => a.to_string(),
                Err(e) => panic!("假 namesrv 地址: {e}"),
            };
            let holder: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
            let codes: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
            let served = Arc::clone(&holder);
            let seen = Arc::clone(&codes);
            let task = tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        return;
                    };
                    let served = Arc::clone(&served);
                    let seen = Arc::clone(&seen);
                    tokio::spawn(async move {
                        use tokio::io::AsyncWriteExt as _;
                        while let Some(request) = read_request(&mut stream).await {
                            guard(&seen).push(request.code);
                            let body = guard(&served).clone();
                            let (code, body) = match body {
                                Some(body) => (response_code::SUCCESS, Some(body)),
                                None => (response_code::TOPIC_NOT_EXIST, None),
                            };
                            let mut response = RemotingCommand::create_response(code, None);
                            // 客户端按 opaque 配对响应，串了就当噪声丢掉。
                            response.opaque = request.opaque;
                            response.serialize_type_current_rpc =
                                request.serialize_type_current_rpc;
                            response.set_body(body);
                            let bytes = response.encode();
                            if stream.write_all(&bytes).await.is_err() {
                                return;
                            }
                            let _ = stream.flush().await;
                        }
                    });
                }
            });
            RouteNamesrv {
                addr,
                holder,
                codes,
                task,
            }
        }

        /// 换一份路由 body（`None` = namesrv 说这个 topic 不存在）。
        fn serve(&self, body: Option<Vec<u8>>) {
            *guard(&self.holder) = body;
        }

        fn codes(&self) -> Vec<i32> {
            guard(&self.codes).clone()
        }

        /// 刷了几次路由（发布地址解析里那次「查不到就刷一遍」）。
        fn route_requests(&self) -> usize {
            self.codes()
                .iter()
                .filter(|c| **c == request_code::GET_ROUTEINFO_BY_TOPIC)
                .count()
        }
    }

    impl Drop for RouteNamesrv {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    /// 一份 topic 路由的 body：`broker_name` 一台机器，`addrs` 是它的 brokerId → 地址。
    fn route_body(broker_name: &str, addrs: &[(i64, &str)]) -> Vec<u8> {
        let route = TopicRouteData {
            queue_datas: vec![QueueData::new(
                broker_name,
                1,
                1,
                PermName::PERM_READ | PermName::PERM_WRITE,
                0,
            )],
            broker_datas: vec![BrokerData::new(
                "DefaultCluster",
                broker_name,
                addrs
                    .iter()
                    .map(|(id, a)| (*id, (*a).to_string()))
                    .collect(),
                "",
            )],
            ..Default::default()
        };
        serde_json::to_vec(&route.to_json_value()).expect("路由可序列化")
    }

    /// 指向给定 namesrv 的实例（`new_instance` 固定用 NAMESRV 常量，这里要假服务器地址）。
    fn instance_against(namesrv: &str) -> MQClientInstance {
        let id = format!("{GROUP}@rs{}", SEQ.fetch_add(1, Ordering::Relaxed));
        MQClientInstance::new(&id, vec![namesrv.to_string()])
    }

    /// `find_broker_address_in_publish` 只认 brokerId=0；同一份路由上管理口径要退到从节点。
    #[tokio::test]
    async fn publish_lookup_takes_only_the_master_while_the_admin_lookup_falls_back() {
        let namesrv = RouteNamesrv::start().await;
        // 主掉线：路由里只剩 brokerId=1（从节点自己也会注册进 namesrv）
        namesrv.serve(Some(route_body(
            BROKER,
            &[(MixAll::MASTER_ID as i64 + 1, "127.0.0.1:10931")],
        )));
        let instance = instance_against(&namesrv.addr);
        assert!(instance
            .update_topic_route_info_from_name_server(TOPIC, 5000, false)
            .await
            .expect("假 namesrv 答得好好的"));

        assert_eq!(instance.find_broker_address_in_publish(BROKER), None);
        // 负控：退让口径（心跳/拉取/位点查询用）在**同一张路由**上必须拿得到从节点地址
        assert_eq!(
            instance.broker_addr_of(BROKER).as_deref(),
            Some("127.0.0.1:10931")
        );

        // 主重新注册：同一个名字立刻解析得出（跳的是「没有 master」，不是 broker-a）
        namesrv.serve(Some(route_body(
            BROKER,
            &[
                (MixAll::MASTER_ID as i64, "127.0.0.1:10911"),
                (MixAll::MASTER_ID as i64 + 1, "127.0.0.1:10931"),
            ],
        )));
        assert!(instance
            .update_topic_route_info_from_name_server(TOPIC, 5000, false)
            .await
            .expect("假 namesrv 答得好好的"));
        assert_eq!(
            instance.find_broker_address_in_publish(BROKER).as_deref(),
            Some("127.0.0.1:10911")
        );
        instance.shutdown();
    }

    #[tokio::test]
    async fn publish_addr_for_refreshes_the_route_then_rechecks() {
        let namesrv = RouteNamesrv::start().await;
        namesrv.serve(Some(route_body(
            BROKER,
            &[(MixAll::MASTER_ID as i64, "127.0.0.1:10911")],
        )));
        let instance = instance_against(&namesrv.addr);
        assert_eq!(
            instance
                .publish_addr_for(BROKER, TOPIC)
                .await
                .expect("刷路由之后解析得出"),
            "127.0.0.1:10911"
        );
        assert_eq!(namesrv.route_requests(), 1, "查不到才刷，且只刷一次");
        instance.shutdown();
    }

    fn assert_broker_not_exist(err: &Error) {
        match err {
            Error::Client {
                response_code,
                message,
            } => {
                assert_eq!(message, &format!("The broker[{BROKER}] not exist"));
                assert_eq!(*response_code, None);
            }
            other => panic!("应当是本端的 MQClientException，得到 {other:?}"),
        }
    }

    /// 主掉线（路由里只剩 brokerId=1）：本端报「broker 不存在」，一次请求都不发。
    #[tokio::test]
    async fn publish_addr_for_reports_not_exist_when_the_master_is_gone() {
        let namesrv = RouteNamesrv::start().await;
        namesrv.serve(Some(route_body(
            BROKER,
            &[(MixAll::MASTER_ID as i64 + 1, "127.0.0.1:10931")],
        )));
        let instance = instance_against(&namesrv.addr);
        let err = instance
            .publish_addr_for(BROKER, TOPIC)
            .await
            .expect_err("没有主就没有发布地址");
        assert_broker_not_exist(&err);
        assert_eq!(namesrv.route_requests(), 1);

        // 负控：主一注册立刻解析得出（刷的是「没有 master」，不是这个 brokerName）
        namesrv.serve(Some(route_body(
            BROKER,
            &[(MixAll::MASTER_ID as i64, "127.0.0.1:10911")],
        )));
        assert_eq!(
            instance
                .publish_addr_for(BROKER, TOPIC)
                .await
                .expect("主回来了"),
            "127.0.0.1:10911"
        );
        instance.shutdown();
    }

    /// 路由里压根没有这个 brokerName（拼错/已下线）：同样报 not exist，不退到别的 broker。
    #[tokio::test]
    async fn publish_addr_for_reports_not_exist_for_an_unknown_broker() {
        let namesrv = RouteNamesrv::start().await;
        namesrv.serve(Some(route_body(
            "broker-b",
            &[(MixAll::MASTER_ID as i64, "127.0.0.1:10912")],
        )));
        let instance = instance_against(&namesrv.addr);
        let err = instance
            .publish_addr_for(BROKER, TOPIC)
            .await
            .expect_err("路由里没有这个名字");
        assert_broker_not_exist(&err);
        instance.shutdown();
    }

    /// `MQAdminImpl:195-264` 的四个 offset 查询同一口径：只打主，主没了就报 not exist。
    #[tokio::test]
    async fn admin_offset_queries_are_master_only_too() {
        let namesrv = RouteNamesrv::start().await;
        let master = MockBroker::start().await;
        let slave = MockBroker::start().await;
        // 只登记从节点 ⇒ 管理口径也不该拿它凑合
        namesrv.serve(Some(route_body(
            BROKER,
            &[(MixAll::MASTER_ID as i64 + 1, &slave.addr)],
        )));
        let instance = instance_against(&namesrv.addr);
        let mq = MessageQueue::new(TOPIC, BROKER, 0);
        let err = instance
            .get_max_offset(&mq, 5000, None)
            .await
            .expect_err("没有主就没有可查的 store");
        assert_broker_not_exist(&err);
        assert!(
            slave.recorded(request_code::GET_MAX_OFFSET).is_empty(),
            "报错前一次请求都不发"
        );

        // 负控：主回来之后查得到，且请求落在**主**地址上（不是路由里那台从节点）
        namesrv.serve(Some(route_body(
            BROKER,
            &[
                (MixAll::MASTER_ID as i64, &master.addr),
                (MixAll::MASTER_ID as i64 + 1, &slave.addr),
            ],
        )));
        master.script_resp_ext(vec![vec![("offset".to_string(), "7".to_string())]]);
        assert_eq!(
            instance
                .get_max_offset(&mq, 5000, None)
                .await
                .expect("主回来了"),
            7
        );
        assert_eq!(master.recorded(request_code::GET_MAX_OFFSET).len(), 1);
        assert!(slave.recorded(request_code::GET_MAX_OFFSET).is_empty());
        instance.shutdown();
    }


    /// 路由里只剩从节点时 LOCK/UNLOCK 一条都不该上线：从节点上锁等于锁在它自己的锁管理器里，
    /// master 不知情，顺序消费的互斥保证静默失效。
    #[tokio::test]
    async fn orderly_locks_skip_the_broker_when_the_master_is_gone() {
        let namesrv = RouteNamesrv::start().await;
        let master = MockBroker::start().await;
        let slave = MockBroker::start().await;
        namesrv.serve(Some(route_body(
            BROKER,
            &[(MixAll::MASTER_ID as i64 + 1, &slave.addr)],
        )));
        let instance = instance_against(&namesrv.addr);
        assert!(instance
            .update_topic_route_info_from_name_server(TOPIC, 5000, false)
            .await
            .expect("假 namesrv 答得好好的"));
        let before = namesrv.route_requests();
        let mqs = vec![
            MessageQueue::new(TOPIC, BROKER, 0),
            MessageQueue::new(TOPIC, BROKER, 1),
        ];

        assert!(instance
            .lock_batch_mq(GROUP, "cid", &mqs, 3000)
            .await
            .expect("整台跳过不算错")
            .is_empty());
        instance
            .unlock_batch_mq(GROUP, "cid", &mqs, 3000)
            .await
            .expect("整台跳过不算错");
        assert!(slave.recorded(request_code::LOCK_BATCH_MQ).is_empty());
        assert!(slave.recorded(request_code::UNLOCK_BATCH_MQ).is_empty());
        assert_eq!(namesrv.route_requests(), before);

        // 负控：主回来之后锁/解锁都落在主地址上、返回的锁集就是请求的那两个队列
        namesrv.serve(Some(route_body(
            BROKER,
            &[
                (MixAll::MASTER_ID as i64, &master.addr),
                (MixAll::MASTER_ID as i64 + 1, &slave.addr),
            ],
        )));
        assert!(instance
            .update_topic_route_info_from_name_server(TOPIC, 5000, false)
            .await
            .expect("假 namesrv 答得好好的"));
        master.script_resp_body(vec![LockBatchResponseBody {
            lock_ok_mq_set: vec![
                MessageQueueKey::new(TOPIC, BROKER, 0),
                MessageQueueKey::new(TOPIC, BROKER, 1),
            ],
        }
        .encode()]);
        let locked = instance
            .lock_batch_mq(GROUP, "cid", &mqs, 3000)
            .await
            .expect("主回来就锁得上");
        assert_eq!(
            locked
                .iter()
                .map(|m| (m.topic.as_str(), m.broker_name.as_str(), m.queue_id))
                .collect::<Vec<_>>(),
            vec![(TOPIC, BROKER, 0), (TOPIC, BROKER, 1)]
        );
        instance
            .unlock_batch_mq(GROUP, "cid", &mqs, 3000)
            .await
            .expect("主回来就解得开");
        assert_eq!(master.recorded(request_code::LOCK_BATCH_MQ).len(), 1);
        assert_eq!(master.recorded(request_code::UNLOCK_BATCH_MQ).len(), 1);
        assert!(slave.recorded(request_code::LOCK_BATCH_MQ).is_empty());
        assert!(slave.recorded(request_code::UNLOCK_BATCH_MQ).is_empty());
        instance.shutdown();
    }

    #[tokio::test]
    async fn pop_message_is_master_only_and_reports_not_exist() {
        let namesrv = RouteNamesrv::start().await;
        let master = MockBroker::start().await;
        let slave = MockBroker::start().await;
        namesrv.serve(Some(route_body(
            BROKER,
            &[(MixAll::MASTER_ID as i64 + 1, &slave.addr)],
        )));
        let instance = instance_against(&namesrv.addr);
        assert!(instance
            .update_topic_route_info_from_name_server(TOPIC, 5000, false)
            .await
            .expect("假 namesrv 答得好好的"));
        let before = namesrv.route_requests();

        let err = instance
            .pop_message(
                GROUP,
                TOPIC,
                0,
                1,
                30000,
                1000,
                0,
                None,
                None,
                false,
                Some(BROKER),
                3000,
                None,
            )
            .await
            .expect_err("没有主就没有可弹的 broker");
        assert_broker_not_exist(&err);
        // 报错前必须刷过一次路由（popAsync 的 findBrokerAddressInSubscribe + 重查）
        assert_eq!(namesrv.route_requests(), before + 1);
        assert!(slave.recorded(request_code::POP_MESSAGE).is_empty());

        // 负控：主注册后（这一发顺带触发一次路由刷新）请求落在主地址上，从节点一条没有
        namesrv.serve(Some(route_body(
            BROKER,
            &[
                (MixAll::MASTER_ID as i64, &master.addr),
                (MixAll::MASTER_ID as i64 + 1, &slave.addr),
            ],
        )));
        master.script(vec![response_code::POLLING_TIMEOUT], vec![String::new()]);
        let result = instance
            .pop_message(
                GROUP,
                TOPIC,
                0,
                1,
                30000,
                1000,
                0,
                None,
                None,
                false,
                Some(BROKER),
                3000,
                None,
            )
            .await
            .expect("主回来就弹得动");
        assert_eq!(result.status, PopStatus::PollingNotFound);
        assert_eq!(master.recorded(request_code::POP_MESSAGE).len(), 1);
        assert!(slave.recorded(request_code::POP_MESSAGE).is_empty());
        instance.shutdown();
    }

    #[tokio::test]
    async fn consumer_offset_falls_back_to_the_slave_after_a_refresh() {
        let namesrv = RouteNamesrv::start().await;
        let master = MockBroker::start().await;
        let slave = MockBroker::start().await;
        namesrv.serve(Some(route_body(
            BROKER,
            &[(MixAll::MASTER_ID as i64 + 1, &slave.addr)],
        )));
        let instance = instance_against(&namesrv.addr);
        assert!(instance
            .update_topic_route_info_from_name_server(TOPIC, 5000, false)
            .await
            .expect("假 namesrv 答得好好的"));
        let mq = MessageQueue::new(TOPIC, BROKER, 0);
        let before = namesrv.route_requests();

        // 主没了：退到从节点上把查询做完，取回的是**从节点**那份值
        slave.script_resp_ext(vec![vec![("offset".to_string(), "424242".to_string())]]);
        assert_eq!(
            instance
                .query_consumer_offset(GROUP, &mq, 5000, None, false)
                .await
                .expect("从节点上查得到"),
            Some(424242)
        );
        assert_eq!(slave.recorded(request_code::QUERY_CONSUMER_OFFSET).len(), 1);
        assert_eq!(namesrv.route_requests(), before + 1);

        // 负控：主回来之后只打主（从节点计数不再增长）
        namesrv.serve(Some(route_body(
            BROKER,
            &[
                (MixAll::MASTER_ID as i64, &master.addr),
                (MixAll::MASTER_ID as i64 + 1, &slave.addr),
            ],
        )));
        assert!(instance
            .update_topic_route_info_from_name_server(TOPIC, 5000, false)
            .await
            .expect("假 namesrv 答得好好的"));
        master.script_resp_ext(vec![vec![("offset".to_string(), "111".to_string())]]);
        assert_eq!(
            instance
                .query_consumer_offset(GROUP, &mq, 5000, None, false)
                .await
                .expect("主上有位点"),
            Some(111)
        );
        assert_eq!(
            master.recorded(request_code::QUERY_CONSUMER_OFFSET).len(),
            1
        );
        assert_eq!(slave.recorded(request_code::QUERY_CONSUMER_OFFSET).len(), 1);
        instance.shutdown();

        // 路由里压根没有 broker-a（连从节点都没有）：刷一次路由后仍查不到 ⇒ 本端报 not exist
        let namesrv2 = RouteNamesrv::start().await;
        let other = MockBroker::start().await;
        namesrv2.serve(Some(route_body(
            "broker-b",
            &[(MixAll::MASTER_ID as i64, &other.addr)],
        )));
        let instance2 = instance_against(&namesrv2.addr);
        let err = instance2
            .query_consumer_offset(GROUP, &mq, 5000, None, false)
            .await
            .expect_err("路由里没有这个名字");
        assert_broker_not_exist(&err);
        assert!(other
            .recorded(request_code::QUERY_CONSUMER_OFFSET)
            .is_empty());
        instance2.shutdown();
    }
}
