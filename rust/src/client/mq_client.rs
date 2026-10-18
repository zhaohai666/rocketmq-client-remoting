//! Responsibilities: name server address management, topic route retrieval and caching,
//! broker address resolution, message sending (SEND_MESSAGE / SEND_MESSAGE_V2), pull
//! (PULL_MESSAGE), POP retrieval (POP_MESSAGE / ACK_MESSAGE / CHANGE_MESSAGE_INVISIBLETIME),
//! offset query/update, heartbeat, queue locking, admin APIs (create/delete topic, cluster
//! info, etc.) and broker-initiated request (220/221/307/309/326) dispatch.

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
    ConsumerData, ExpressionType, HeartbeatData, ProducerData, SubscriptionData,
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

/// Object-safe: async behavioral methods take a `self: Arc<Self>` receiver plus a boxed
/// future; instances hold them as `Arc<dyn RegisteredConsumer>`.
pub trait RegisteredConsumer: Send + Sync {
    /// The consumer's own clientId (goes into the heartbeat's ConsumerData).
    fn client_id(&self) -> String;

    /// `ConsumerData.groupName`.
    fn consumer_group(&self) -> String;

    fn consume_type(&self) -> String;

    /// `ConsumerData.messageModel` (`CLUSTERING` / `BROADCASTING`).
    fn message_model(&self) -> String;

    /// `ConsumerData.consumeFromWhere`.
    fn consume_from_where(&self) -> String;

    fn is_unit_mode(&self) -> bool;

    fn subscription(&self) -> Vec<String>;

    /// The `topic -> SubscriptionData` table, going into the heartbeat's
    /// `ConsumerData.subscriptionDataSet`.
    fn subscriptions(&self) -> Vec<SubscriptionData>;

    fn adjust_thread_pool(&self) {}

    fn rebalance_immediately(&self) {}

    /// 220 handling: the broker pushes an offset reset. Must run **in the background**
    /// (it performs rebalance / lock / batch `invoke_sync` calls, which must not block the remoting read thread).
    fn reset_offset(
        self: Arc<Self>,
        topic: String,
        offset_table: Vec<(MessageQueue, i64)>,
    ) -> ConsumerFuture<()>;

    /// 221 handling: return the consumed-offset table.
    /// `None` = no topic in the request header, distinct from "explicit empty string" (only a non-empty topic filters).
    fn get_consumer_status(&self, topic: Option<&str>) -> Vec<(MessageQueue, i64)>;

    /// 307 handling: running info.
    fn consumer_running_info(&self) -> ConsumerRunningInfo;

    /// 309 handling: consume one message locally for real.
    fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<String>,
    ) -> Result<ConsumeMessageDirectlyResult>;

    fn persist_consumer_offset(self: Arc<Self>) -> ConsumerFuture<()>;
}

/// Return type for `RegisteredConsumer` async behavior (a boxed future, no async-trait dependency).
pub type ConsumerFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>;

pub trait TraceDispatcher: Send + Sync {
    /// Start the trace dispatcher (with the name server address).
    fn start(&self, name_server_addr: &str) -> Result<()>;

    /// Shut down the trace dispatcher.
    fn shutdown(&self);
}

// ================================================================ TopicPublishInfo

/// One lock covers the queue list + the round-robin cursor +
/// the route fields (the whole batch is replaced on a route update; readers take one snapshot).
#[derive(Debug, Default)]
pub struct TopicPublishInfo {
    state: Mutex<PublishState>,
}

#[derive(Debug, Default)]
struct PublishState {
    order_topic: bool,
    msg_queue_list: Vec<MessageQueue>,
    topic_route_data: Option<TopicRouteData>,
    /// Round-robin cursor (int, may be negative; rem_euclid before the modulo keeps it non-negative).
    index: i64,
}

/// Round-robin cursor -> ring index (negatives and overflow all map into `[0, len)`).
fn ring_index(index: i64, len: usize) -> usize {
    index.rem_euclid(len as i64) as usize
}

impl TopicPublishInfo {
    pub fn new() -> TopicPublishInfo {
        TopicPublishInfo::default()
    }

    /// `ok()`: usable only when queues exist.
    pub fn ok(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        !state.msg_queue_list.is_empty()
    }

    /// Reset the round-robin cursor to zero.
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

    /// `order_topic` read (reads the cached field directly).
    pub fn order_topic(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .order_topic
    }

    /// Snapshot of `msg_queue_list`.
    pub fn msg_queue_list(&self) -> Vec<MessageQueue> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .msg_queue_list
            .clone()
    }

    /// Snapshot of `topic_route_data`.
    pub fn topic_route_data(&self) -> Option<TopicRouteData> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .topic_route_data
            .clone()
    }

    /// Serialization: `{"orderTopic":..., "messageQueueList":[{topic,brokerName,queueId}]}`.
    /// Note `messageQueueList` **excludes the route itself**, it only lists queues.
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

    /// Batch refresh when a route lands: the `order_topic / topic_route_data / msg_queue_list`
    /// triple is assigned together.
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

/// Implements [`crate::client::latency::PublishInfo`]: `MQFaultStrategy` uses it for latency-fault-tolerant queue selection.
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

/// Assembles all queues of a topic from queueDatas × writeQueueNums.
/// Same source as `TopicRouteData::get_all_message_queue`; the read is kept independent
/// so `TopicPublishInfo` and the instance can both reuse it.
fn route_queue_lists(route: &TopicRouteData, topic: &str) -> Vec<MessageQueue> {
    route
        .get_all_message_queue(topic)
        .into_iter()
        .map(|q| MessageQueue::new(&q.topic, &q.broker_name, q.queue_id))
        .collect()
}

// ================================================================ publish message wrapper

/// Message wrapper at the send entry: the send kernel takes both a
/// `Message` and a `MessageBatch`; the batch flag decides the encoding path and whether UNIQ_KEY is filled in.
#[derive(Debug)]
pub enum PublishMessage<'a> {
    /// A normal message.
    Single(&'a mut Message),
    /// A batch message (body already encoded when the batch was assembled).
    Batch(&'a mut MessageBatch),
}

impl<'a> PublishMessage<'a> {
    /// Outer `Message` view: a batch message keeps its outer fields in
    /// `batch.message`; semantically equivalent.
    pub fn as_message(&self) -> &Message {
        match self {
            PublishMessage::Single(m) => m,
            PublishMessage::Batch(b) => &b.message,
        }
    }

    /// Mutable view of the outer `Message`: traceparent injection in the send kernel mutates properties in place.
    pub fn as_message_mut(&mut self) -> &mut Message {
        match self {
            PublishMessage::Single(m) => m,
            PublishMessage::Batch(b) => &mut b.message,
        }
    }

    /// Whether this is a batch message.
    pub fn is_batch(&self) -> bool {
        matches!(self, PublishMessage::Batch(_))
    }

    /// Re-borrow the same message with the lifetime shortened to this call -- the send **retry chain**
    /// creates a new `PublishMessage` each round (the request is rebuilt per selected broker), but the underlying message exists only once.
    pub fn reborrow(&mut self) -> PublishMessage<'_> {
        match self {
            PublishMessage::Single(m) => PublishMessage::Single(m),
            PublishMessage::Batch(b) => PublishMessage::Batch(b),
        }
    }
}

// ================================================================ config

/// `MQClientInstance` constructor parameters (fully explicit injection + module-internal constants).
#[derive(Clone)]
pub struct MQClientInstanceConfig {
    /// Connect timeout (default 3000).
    pub connect_timeout_millis: i64,
    /// RPC call timeout (default 15000).
    pub invoke_timeout_millis: i32,
    /// TLS switch: `None` follows the env var `ROCKETMQ_TLS_ENABLE` (same convention as
    /// [`RemotingClientConfig::default`]), `Some(v)` forces the value.
    pub tls_enable: Option<bool>,
    pub socks_proxy: Option<SocksProxyConfig>,
    pub route_refresh_interval_millis: u64,
    /// Dynamic name server initial delay / period (10s / 120s).
    pub namesrv_refresh_initial_delay_millis: u64,
    pub namesrv_refresh_interval_millis: u64,
    /// Thread-pool adjust sweep (60s / 60s).
    pub adjust_pool_initial_delay_millis: u64,
    pub adjust_pool_interval_millis: u64,
    pub heartbeat_initial_delay_millis: u64,
    pub heartbeat_interval_millis: u64,
    pub persist_offset_initial_delay_millis: u64,
    pub persist_offset_interval_millis: u64,
    /// Instance-level shared stats manager (for injection;
    /// `None` = a fresh default one).
    pub consumer_stats_manager: Option<Arc<ConsumerStatsManager>>,
    pub latency_fault_tolerance: Option<Arc<LatencyFaultToleranceImpl>>,
    /// See [`TraceDispatcher`]: this module only stores it and never calls it.
    pub trace_dispatcher: Option<Arc<dyn TraceDispatcher>>,
    /// Dynamic name server addressing (for injection;
    /// `None` = `DefaultTopAddressing::default()`, which reads env vars).
    pub top_addressing: Option<DefaultTopAddressing>,
    pub unit_name: Option<String>,
    pub namespace_v2: Option<String>,
    pub enable_stream_request_type: bool,
    /// `ClientConfig.vipChannelEnabled` (default false; the producer facade calls it
    /// `sendMessageWithVIPChannel`). When on, every broker-facing RPC of this instance is sent
    /// to `port - 2` of the route address (`MixAll.brokerVIPChannel`), the dedicated fast port a
    /// broker opens besides its main listening port. Name-server requests never shift.
    pub vip_channel_enabled: bool,
    /// `ClientConfig.useHeartbeatV2` (default false; the
    /// `com.rocketmq.heartbeat.v2` switch). When on, heartbeat rounds take the fingerprint path:
    /// `HEART_BEAT(34)` with a non-zero `heartbeatFingerprint`, and -- once the broker says
    /// `IS_SUPPORT_HEART_BEAT_V2` and the fingerprint is unchanged -- a `withoutSub` body.
    pub use_heartbeat_v2: bool,
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
            vip_channel_enabled: false,
            use_heartbeat_v2: false,
        }
    }
}

/// Hand-written `Debug`: the injected seams (`Arc<dyn TraceDispatcher>` etc.) do not require
/// `Debug` themselves, so this only prints "injected or not".
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
            .field("vip_channel_enabled", &self.vip_channel_enabled)
            .field("use_heartbeat_v2", &self.use_heartbeat_v2)
            .finish()
    }
}

// ================================================================ MQClientInstance

/// The three route tables: `topic_route_table` + `topic_publish_info_table` +
/// `broker_addr_table`, sharing one route lock.
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
    /// Topics registered as in use.
    topics_in_use: Mutex<HashSet<String>>,
    started: AtomicBool,
    consumer_table: Mutex<HashMap<String, Arc<dyn RegisteredConsumer>>>,
    producer_table: Mutex<HashSet<String>>,
    consumer_group_table: Mutex<HashSet<String>>,
    /// Broker addresses the current registration was last beaten to, sorted.
    /// A route refresh only changes this after a beat actually reached the new set, so a
    /// broker that joined in between keeps being retried until the registration lands there.
    brokers_beaten: Mutex<Vec<String>>,
    /// `fetch_and_apply` needs `&mut`, so this sits in a `tokio::sync::Mutex`.
    top_addressing: Arc<tokio::sync::Mutex<DefaultTopAddressing>>,
    consumer_stats_manager: Arc<ConsumerStatsManager>,
    latency_fault_tolerance: Option<Arc<LatencyFaultToleranceImpl>>,
    trace_dispatcher: Option<Arc<dyn TraceDispatcher>>,
    config: MQClientInstanceConfig,
    /// Stop signal for background tasks (true = stop).
    stop: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    consumer_ids_changed_count: AtomicUsize,
    /// Live copy of `MQClientInstanceConfig::vip_channel_enabled`: unlike the other config fields
    /// this one is readable *and* writable after `start()` (a facade's
    /// `set_send_message_with_vip_channel` flips it), because the address rewrite happens per
    /// request.
    vip_channel_enabled: AtomicBool,
    /// `MQClientInstance.brokerSupportV2HeartbeatSet`: addresses that answered
    /// `IS_SUPPORT_HEART_BEAT_V2` at least once. An address outside the set is beaten with the
    /// full body and fingerprint 0 semantics do not apply to it.
    broker_support_v2_heartbeat_set: Mutex<HashSet<String>>,
    /// `MQClientInstance.brokerAddrHeartbeatFingerprintTable`: the fingerprint the last accepted
    /// full-body beat left at each V2-capable address. While a row matches the current
    /// subscriptions, beats to that address may drop the subscription set.
    broker_addr_heartbeat_fingerprint_table: Mutex<HashMap<String, i32>>,
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

/// The client instance itself (the class-level `INSTANCE_MAP` lives at
/// [`MQClientInstance::INSTANCE_MAP`], holding weak references).
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

    /// Construct from a client_id + name server address list (remaining parameters use defaults).
    pub fn new(client_id: &str, name_server_addrs: Vec<String>) -> MQClientInstance {
        MQClientInstance::with_config(
            client_id,
            name_server_addrs,
            MQClientInstanceConfig::default(),
        )
    }

    /// The full-parameter injection variant.
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
        let vip_channel_enabled = config.vip_channel_enabled;
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
            brokers_beaten: Mutex::new(Vec::new()),
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
            vip_channel_enabled: AtomicBool::new(vip_channel_enabled),
            broker_support_v2_heartbeat_set: Mutex::new(HashSet::new()),
            broker_addr_heartbeat_fingerprint_table: Mutex::new(HashMap::new()),
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

    /// `INSTANCE_MAP` reader side (lets admin/tests find an instance by clientId).
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

    // ---------------- consumer registration (broker-initiated requests dispatch by group) ----------------

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

    // ---------------- producer / pull-mode consumer registration (serves only the shutdown guard) ----------------

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

    /// How many broker `NOTIFY_CONSUMER_IDS_CHANGED(40)` notifications have been received.
    /// See [`Inner::consumer_ids_changed_count`]: the reverse push only ever comes from the
    /// broker, so live-cluster cases need an assertable landing point.
    pub fn consumer_ids_changed_count(&self) -> usize {
        self.inner.consumer_ids_changed_count.load(Ordering::SeqCst)
    }

    // ---------------- broker-initiated request handling (ClientRemotingProcessor) ----------------

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
        // The 220 rebalance calls invokeSync, so it must not block the remoting read thread:
        // hand it to a background task and reply None immediately (oneway).
        let task = consumer.reset_offset(topic.clone(), offset_table);
        // A 220 can only arrive on the remoting read thread, where a runtime always exists; if it
        // genuinely cannot be had, drop the task rather than panic.
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
        // Decoding does not verify CRC (check_crc in the default options is already
        // false, see message_decoder::DecodeOptions::default).
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

    /// 326 handling: hand the reply to the awaiting request().
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

    // ---------------- lifecycle ----------------

    /// Marks started, starts stats sampling and (optionally) the dynamic name server refresh,
    /// then spawns 4 background tasks. Returns `Err` when dynamic
    /// addressing cannot obtain any address.
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
        // A previous session of this instance may have beaten the very same brokers already,
        // but a restart means a new channel and the broker forgot the registration: force the
        // next route refresh (and the beat loop) to treat the current set as never beaten.
        self.inner
            .brokers_beaten
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
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
        // Route refresh loop: first tick at 10ms, period route_refresh_interval
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

    /// Generic periodic task: initial delay → loop (work → sleep ∥ stop signal), checking started first each round.
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

    // ---------------- dynamic name server / thread-pool adjust ----------------

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

    /// Update the name server address list: an empty list changes nothing.
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

    /// Register a topic that needs background periodic route refresh.
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

// ================================================================ instance-level RPC

impl MQClientInstance {
    // ---------------- low-level invoke ----------------

    /// Thin wrapper around `remoting_client.invoke_sync`.
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

    /// Broker-facing sync call that follows Java's `MQClientAPIImpl` methods with
    /// `MixAll.brokerVIPChannel(clientConfig.isVipChannelEnabled(), addr)` in front of the
    /// `invokeSync`: same address rewrite as [`send_message_to_addr`], applied per request.
    ///
    /// [`send_message_to_addr`]: Self::send_message_to_addr
    pub(crate) async fn invoke_broker_sync(
        &self,
        addr: &str,
        request: &mut RemotingCommand,
        timeout_millis: i64,
    ) -> Result<RemotingCommand> {
        self.invoke_sync(&self.vip_addr(addr), request, timeout_millis)
            .await
    }

    /// Response check: any non-SUCCESS raises `MQBrokerException`; an empty
    /// remark becomes `""`.
    pub(crate) fn check_response(response: &RemotingCommand) -> Result<&RemotingCommand> {
        if response.code == response_code::SUCCESS {
            return Ok(response);
        }
        Err(Error::Broker {
            response_code: response.code,
            message: response.remark.clone().unwrap_or_default(),
        })
    }

    // ---------------- route management ----------------

    /// Fetch a topic route and store it.
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
                // The broker creates a new topic with default_topic_queue_nums queues, while the
                // default topic itself may advertise more; cap to the broker's actual count so no illegal queueId is picked.
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
        // The publish info's three fields must be refreshed as one batch:
        // `TopicPublishInfo` has its own fine-grained lock, so the table lock is released before the refresh (no nested locks).
        publish.update_from_route(&route, topic);
        self.send_heartbeat_if_broker_set_changed().await;
        Ok(true)
    }

    /// Try name servers one by one; **a response that is unusable breaks the loop**
    /// (later servers are not retried); only transport/decode errors move on to the next one;
    /// after the loop, the "last error" is rethrown as-is when it is not a broker business error.
    ///
    /// The remoting layer pairs responses by `opaque`, so every attempt builds a fresh
    /// request (`opaque` increments each time; semantics unchanged).
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
                        // A decode failure becomes last_exc and moves on to the next name server
                        Some(body) if !body.is_empty() => match TopicRouteData::decode(body) {
                            Ok(data) => return Ok(Some(data)),
                            Err(e) => last_exc = Some(e),
                        },
                        // SUCCESS but no body: break here too (later name servers are not retried)
                        _ => break,
                    }
                }
                Err(e) => last_exc = Some(e),
            }
        }
        if let Some(e) = last_exc {
            // Rethrow the last error as-is only when it is not a broker business error
            if !matches!(e, Error::Broker { .. }) {
                return Err(e);
            }
        }
        Ok(None)
    }

    /// Return the cached info when usable, otherwise fetch the route once;
    /// still no queues ⇒ `Can not find Message Queue for topic: <topic>`.
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

    /// Return on cache hit; otherwise fetch the route once (**swallowing exceptions**) and read again.
    pub async fn get_topic_route_data(&self, topic: &str) -> Option<TopicRouteData> {
        if let Some(route) = self.route_of(topic) {
            return Some(route);
        }
        if let Err(e) = self
            .update_topic_route_info_from_name_server(topic, 5000, false)
            .await
        {
            // A refresh failure swallows the exception and falls through to the cache read
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

    /// Cached route only (no RPC triggered), returns `None` on a miss.
    pub(crate) fn route_of(&self, topic: &str) -> Option<TopicRouteData> {
        self.inner
            .tables
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .topic_route_table
            .get(topic)
            .cloned()
    }

    /// Cached publish info only (no RPC triggered).
    fn publish_info_of(&self, topic: &str) -> Option<Arc<TopicPublishInfo>> {
        self.inner
            .tables
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .topic_publish_info_table
            .get(topic)
            .cloned()
    }

    /// Find this brokerName's address in the route.
    pub fn find_broker_addr_in_route(route: &TopicRouteData, broker_name: &str) -> Option<String> {
        route
            .get_broker_datas()
            .iter()
            .find(|bd| bd.broker_name == broker_name)
            .and_then(|bd| bd.select_broker_addr())
    }

    // ---------------- VIP channel ----------------

    /// `ClientConfig.setVipChannelEnabled`: flip the address rewrite for this live instance.
    /// Every broker-facing RPC of this instance that the Java client rewrites through
    /// `MixAll.brokerVIPChannel` follows the switch immediately, including requests already
    /// queued on an existing connection (the address is resolved per request).
    pub fn set_vip_channel_enabled(&self, enable: bool) {
        self.inner
            .vip_channel_enabled
            .store(enable, Ordering::Release);
    }

    pub fn is_vip_channel_enabled(&self) -> bool {
        self.inner.vip_channel_enabled.load(Ordering::Acquire)
    }

    /// `MixAll.brokerVIPChannel(clientConfig.isVipChannelEnabled(), addr)`: `ip:port` becomes
    /// `ip:(port - 2)` (10911 -> 10909). Off (the default) the address is returned untouched,
    /// and a name-server address must never be passed through here.
    fn vip_addr(&self, addr: &str) -> String {
        if !self.is_vip_channel_enabled() {
            return addr.to_string();
        }
        MixAll::broker_vip_channel(true, addr)
    }

    /// The address this instance's live VIP decision would send a publish request to, or `None`
    /// when the route holds no master for that broker name. The rewrite itself happens inside
    /// the send RPCs, so this only exists for callers that must log or compare the address.
    pub(crate) fn vip_addr_of_publish(&self, broker_name: &str) -> Option<String> {
        self.find_broker_address_in_publish(broker_name)
            .map(|addr| self.vip_addr(&addr))
    }

    // ---------------- message sending ----------------

    /// Resolve the broker address from the route, then send synchronously.
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
        // Look up the publish address (master only): on a miss refresh the route once,
        // and if still missing, report "The broker[X] not exist" the way the send kernel does.
        // **Not** `broker_addr_in_route` (that one may fall back to a slave, so during a
        // master/slave switch the send would pointlessly eat one SYSTEM_BUSY(2) round).
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

    /// Send directly to the given address.
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
        let response = self
            .invoke_sync(&self.vip_addr(addr), &mut request, timeout_millis)
            .await?;
        Self::parse_send_response(&response, msg.as_message(), mq)
    }

    /// Mark with `mark_oneway_rpc` then fire-and-forget.
    ///
    /// `timeout_millis` carries no meaning in a oneway call (no response is awaited), so
    /// this signature omits the parameter.
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
        let addr = self.vip_addr(addr);
        self.inner.remoting_client.invoke_oneway(&addr, &mut request).await
    }

    /// `on_complete` fires exactly once (guaranteed by the remoting layer). `msg` is only read by
    /// [`parse_send_response`](Self::parse_send_response) for the UNIQ_KEY, and the body is
    /// already encoded in the request, so callers may pass a copy with the body stripped.
    pub(crate) fn send_message_async(
        &self,
        addr: &str,
        request: RemotingCommand,
        msg: Arc<Message>,
        mq: Arc<MessageQueue>,
        timeout_millis: i64,
        on_complete: Box<dyn FnOnce(Result<SendResult>) + Send + 'static>,
    ) {
        let addr = self.vip_addr(addr);
        self.inner.remoting_client.invoke_async(
            &addr,
            request,
            Box::new(move |response| match response {
                Ok(response) => on_complete(Self::parse_send_response(&response, &msg, &mq)),
                Err(e) => on_complete(Err(e)),
            }),
            Some(timeout_millis),
        );
    }

    /// Builds the SEND_MESSAGE_V2 request (V2 short-field header, `sendSmartMsg` always true).
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

    /// Body encoding: a normal message uses its raw body; a batch message's body was
    /// already encoded by `encode_messages` when the batch was built.
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
            // Only the literal "false" means off: a missing field or empty string both mean on
            trace_on: match response.get_ext_field(PROPERTY_TRACE_SWITCH) {
                Some(value) => value != "false",
                None => true,
            },
        })
    }

    // ---------------- scheduled message recall (370) ----------------

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

    // ---------------- pull ----------------

    /// Pull request (PULL_MESSAGE); each parameter lands in its own request header field.
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
        // Stamp brokerName/queueId unconditionally
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

// ================================================================ POP / ACK / invisible time

impl MQClientInstance {
    /// The addr / brokerName resolution part of `pop_message`.
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
                // Pick any broker from the route first, then still resolve only its master
                bd.broker_name.clone()
            }
        };
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.publish_addr_for(&broker_name, topic).await?,
        };
        Ok((addr, broker_name))
    }

    /// POP retrieval (POP_MESSAGE = 200050).
    ///
    /// Semantic difference from pull: **no commit offset is needed**; consumption is confirmed
    /// with [`ack_message`]; un-acked messages are revived by the broker after `invisible_time` and
    /// redelivered to `%RETRY%<group>_<topic>` (V1), then pop up again on the next POP -- at-least-once semantics;
    /// `queue_id = -1` means pop from all queues of the topic.
    ///
    /// `born_time` must be the current millisecond timestamp: the broker treats a request as
    /// timed out when `now - bornTime - pollTime > 500` and answers POLLING_TIMEOUT(210) directly; 0 always fails.
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

    /// Build the queue-map key from the message's `POP_CK` (module path used directly, no extra params).
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

    /// Stamps `POP_CK` and `1ST_POP_TIME` onto popped messages (associated function, no instance state).
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
            // Messages redelivered from a retry topic already carry a broker-written POP_CK; it must not be overwritten.
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

    /// Ack one POP message (ACK_MESSAGE = 200051).
    ///
    /// `extra_info` comes from the message's `POP_CK` property; `offset` must be the **consume-queue offset**
    /// (the 8th segment of the CK string / msgQueueOffset), not the commitlog offset -- a wrong one makes the
    /// broker answer `NO_MESSAGE`. Returns the broker response code; `SUCCESS` means the ack succeeded.
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
        // The parameter shadows the module name, so alias it locally
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

    /// Change the invisible time (CHANGE_MESSAGE_INVISIBLETIME = 200053).
    ///
    /// For the "still processing, don't let the broker revive and redeliver" case. The response carries the **new**
    /// popTime / invisibleTime / reviveQid, from which the client rebuilds the 8-segment extraInfo
    /// (returned in the result's `extra_info` field) for later ACKs -- not the old string.
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

// ================================================================ offset / query / queue lock / heartbeat / admin

impl MQClientInstance {
    // ---------------- address helpers ----------------

    /// Resolve a broker address by topic (optionally narrowed by brokerName).
    /// An empty brokerName degrades to "the first broker in the route".
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

    /// Look up the address for `mq.broker_name` in that topic's route.
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

    /// Equivalent to `addr_for(topic)` (identical code in both places).
    pub(crate) async fn broker_addr_for_topic(&self, topic: &str) -> Result<String> {
        self.addr_for(topic, None).await
    }

    /// Find this brokerName's address across **all** cached routes.
    pub fn broker_addr_of(&self, broker_name: &str) -> Option<String> {
        self.cached_routes()
            .into_iter()
            .find_map(|route| Self::find_broker_addr_in_route(&route, broker_name))
    }

    /// A `None` return is a normal outcome (that is the shape of a downed master); error reporting is
    /// the caller's choice (`sendKernelImpl` raises MQClientException, `endTransaction` stays silent).
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

    /// Test-only: seed the publish address into the flat table (equivalent to the refresh route storage does).
    /// Offline cases have no name server; without a seeded table, address resolution fails before the hook.
    #[cfg(test)]
    pub(crate) fn seed_publish_addr_for_test(&self, broker_name: &str, addr: &str) {
        let mut tables = self.inner.tables.lock().unwrap_or_else(|e| e.into_inner());
        tables.broker_addr_table.insert(
            broker_name.to_string(),
            HashMap::from([(MixAll::MASTER_ID as i64, addr.to_string())]),
        );
    }

    /// This is the address policy of the send path, of the POP ack / invisible-time extension
    /// (both resolve with `findBrokerAddressInSubscribe(brokerName, MASTER_ID, true)`,
    /// i.e. the master only), and of the admin-side offset queries
    /// (see [`Self::publish_addr_in_admin`]).
    /// Same shape: look up the publish address (**brokerId=0 only**) → on a miss refresh the
    /// route once by topic → recheck → still missing, raise the client-side error
    /// `MQClientException("The broker[X] not exist", null)`.
    ///
    /// A send to a chosen queue never reads publish info in the send-default path, so this is
    /// its only route source; during a master/slave switch this is also the dividing line between
    /// failing fast locally and sending the request to a slave to pointlessly eat
    /// one SYSTEM_BUSY(2) round -- slaves reject the writes
    /// SEND_MESSAGE / CONSUMER_SEND_MSG_BACK / CHANGE_INVISIBLE_TIME.
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

    /// Address resolution for POP ack / changeInvisibleTime.
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

    /// Test-only: seed the end-point table into RouteTables (equivalent to one static-topic route storage).
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

    /// Address used by the offset-family RPCs (`QUERY_CONSUMER_OFFSET` / `UPDATE_CONSUMER_OFFSET`
    /// / `GET_MAX_OFFSET` / `GET_MIN_OFFSET` / `SEARCH_OFFSET_BY_TIMESTAMP` /
    /// `GET_EARLIEST_MSG_STORETIME`).
    ///
    /// Returns the **plain** route address: the VIP rewrite happens at invoke time inside those
    /// RPCs (Java does it in `MQClientAPIImpl`, so a caller-supplied address is rewritten too).
    async fn publish_addr_in_admin(&self, mq: &MessageQueue) -> Result<String> {
        let mut addr = self.find_broker_address_in_publish(&mq.broker_name);
        if addr.is_none() {
            self.update_topic_route_info_from_name_server(&mq.topic, 5000, false)
                .await?;
            addr = self.find_broker_address_in_publish(&mq.broker_name);
        }
        addr.ok_or_else(|| Error::client(format!("The broker[{}] not exist", mq.broker_name)))
    }

    /// Broker addresses across **all** cached routes (deduplicated).
    ///
    /// HashMap iteration is unordered, so addresses are sorted lexicographically here to make
    /// the heartbeat/unregister send order deterministic (identical set contents).
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

    /// **Every** broker seen in the routes (master + slave).
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

    /// Snapshot of the cached routes.
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

    /// Differs from [`Self::publish_addr_in_admin`] only in the last step: admin offset queries
    /// always hit the master and fail if it is gone; consumer offset reads may fall back to a slave.
    /// Like it, this returns the plain route address; the VIP rewrite happens at invoke time.
    async fn consumer_offset_addr(&self, mq: &MessageQueue) -> Result<String> {
        let mut addr = self.find_broker_address_in_publish(&mq.broker_name);
        if addr.is_none() {
            self.update_topic_route_info_from_name_server(&mq.topic, 5000, false)
                .await?;
            addr = self.broker_addr_of(&mq.broker_name);
        }
        addr.ok_or_else(|| Error::client(format!("The broker[{}] not exist", mq.broker_name)))
    }

    // ---------------- consumer offset query / update ----------------

    /// `QUERY_NOT_FOUND(22)` ⇒ `None` (returned directly without raising),
    /// any other non-SUCCESS raises `MQBrokerException` via the response check.
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
            .invoke_broker_sync(&addr, &mut request, timeout_millis)
            .await?;
        if response.code == response_code::QUERY_NOT_FOUND {
            return Ok(None);
        }
        Self::check_response(&response)?;
        let mut resp_header = QueryConsumerOffsetResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        Ok(resp_header.offset)
    }

    /// Commit the consumer offset.
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
            .invoke_broker_sync(&addr, &mut request, timeout_millis)
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
            .invoke_broker_sync(&addr, &mut request, timeout_millis)
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
            .invoke_broker_sync(&addr, &mut request, timeout_millis)
            .await?;
        Self::check_response(&response)?;
        let mut resp_header = GetMinOffsetResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        Ok(resp_header.offset.unwrap_or(0))
    }

    /// Query an offset by timestamp (overload without the boundary parameter).
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

    /// Address policy: master only, matching the admin-side search-offset path (see
    /// [`Self::publish_addr_in_admin`]).
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
            .invoke_broker_sync(&addr, &mut request, timeout_millis)
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
            .invoke_broker_sync(&addr, &mut request, timeout_millis)
            .await?;
        Self::check_response(&response)?;
        let mut resp_header = GetEarliestMsgStoretimeResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        Ok(resp_header.timestamp.unwrap_or(0))
    }

    /// Unlike the push consumer's internal redelivery (`send_message_back` in `consumer.rs`):
    /// that one is **fire-and-forget** (`tokio::spawn` + log only) and maps
    /// `maxReconsumeTimes == -1` to 16; this call must return the failure to the caller.
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
        let response = self.invoke_broker_sync(addr, &mut request, timeout_millis).await?;
        Self::check_response(&response)?;
        Ok(())
    }

    // ---------------- message query ----------------

    /// `index_type` takes `MessageConst::INDEX_KEY_TYPE`("K") / `INDEX_UNIQUE_TYPE`("U");
    /// with `uniq_key = true` the request additionally carries `extFields["_UNIQUE_KEY_QUERY"]="true"`, making the broker
    /// force the uniqKey index (and overwrite maxNum with the default query count).
    /// `QUERY_NOT_FOUND(22)` ⇒ `None`.
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
            .invoke_broker_sync(&addr, &mut request, timeout_millis)
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
                // Both errors and empty bodies just continue
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

    // ---------------- queue locking (orderly consumption) ----------------

    /// Group by brokerName, **keeping first-occurrence order** (insertion order).
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

    /// Send one batch per broker group; returns the queues the brokers confirmed locked (`lockOKMQSet`).
    /// A failure against one broker only warns and the loop continues.
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
                .invoke_broker_sync(&addr, &mut request, timeout_millis)
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
                .invoke_broker_sync(&addr, &mut request, timeout_millis)
                .await
                .and_then(|response| Self::check_response(&response).map(|_| ()));
            if let Err(e) = result {
                rmq_warn!("unlock_batch_mq failed for broker {broker_name}: {e}");
            }
        }
        Ok(())
    }

    // ---------------- heartbeat / unregister ----------------

    /// Send a heartbeat (HEART_BEAT = 34).
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
        for group in self.producer_groups() {
            hb.add_producer_data(ProducerData::new(group));
        }
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

    /// Both registries of this clientId, so one beat keeps producer **and** consumer
    /// registrations alive. Dropping either half leaves that kind registered only by its
    /// own facade loop; when that loop is not running, the broker holds no channel for the
    /// group and can never route CHECK_TRANSACTION_STATE or a reply back to this client.
    fn has_registration(&self) -> bool {
        !self
            .inner
            .producer_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
            || !self
                .inner
                .consumer_table
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
    }

    /// `producer_table` snapshot (sorted by group for deterministic traversal order).
    fn producer_groups(&self) -> Vec<String> {
        let mut groups: Vec<String> = self
            .inner
            .producer_table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .cloned()
            .collect();
        groups.sort();
        groups
    }

    /// Send heartbeats to every known broker (**slaves included**); returns the success count.
    pub async fn send_heartbeat_to_all_broker(&self, timeout_millis: i64) -> usize {
        if !self.has_registration() {
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

    /// Beat as soon as the route cache shows a broker set the registration has not reached
    /// yet. Registration only goes to brokers already cached, so the first beat of a client
    /// that starts without a route lands nowhere; without this the group waits for the next
    /// 30s beat to register with a broker that joined in between.
    ///
    /// Returns the number of brokers that accepted the beat (0 when the set is unchanged or
    /// nothing is registered). A beat that reaches no broker leaves the set unrecorded, so
    /// the next route refresh retries it.
    pub async fn send_heartbeat_if_broker_set_changed(&self) -> usize {
        let addrs = self.get_all_broker_addrs();
        if addrs.is_empty() {
            return 0;
        }
        if *self
            .inner
            .brokers_beaten
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            == addrs
        {
            return 0;
        }
        let ok = self.send_heartbeat_to_all_broker(5000).await;
        if ok > 0 {
            *self
                .inner
                .brokers_beaten
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = addrs;
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

    /// `consumer_table` snapshot (sorted by group for deterministic traversal order).
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

    // ---------------- CHECK_CLIENT_CONFIG(46): validate subscription expressions against the broker ----------------

    /// Reads only cached routes and picks a random broker from them (master address preferred); returns `None` without a cache hit.
    pub fn find_broker_addr_by_topic(&self, topic: &str) -> Option<String> {
        let route = self.route_of(topic)?;
        let brokers = route.get_broker_datas();
        brokers
            .get(pseudo_random_index(brokers.len()))?
            .select_broker_addr()
    }

    // ---------------- QUERY_ASSIGNMENT(400): broker-side queue assignment ----------------

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
        let response = self.invoke_broker_sync(addr, &mut request, timeout_millis).await?;
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

    /// Walk every registered consumer's subscriptions and send only the **non-TAG** (SQL92 / CLASS_FILTER)
    /// expressions to the broker for validation. Why this must be sent: with a malformed SQL92 expression, the
    /// broker's `ExpressionMessageFilter`, unable to obtain compiled filter data, **admits every message**
    /// (returns true), silently degrading to "subscribe to everything"; the consumer still starts without a single error.
    /// This call turns a broken expression into an explicit startup failure.
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

    /// Inner loop of `check_client_in_broker`, exposed for callers that are not registered in the consumerTable.
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

    /// Unregister the client from the name server (UNREGISTER_CLIENT = 35).
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

    // ---------------- admin APIs ----------------

    /// Fetch the broker cluster info (GET_BROKER_CLUSTER_INFO = 106).
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

    /// Pull the full topic list from the name server (GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206).
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
                .invoke_broker_sync(broker_addr, &mut request, timeout_millis)
                .await
            {
                Ok(response) => {
                    // Business errors are not retried; they propagate immediately
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
            // Uses create_topic_in_broker's defaults here: SINGLE_TAG / order=false / 5 retries
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
                // No broker created it: report "create new topic failed" with the last error
                return Err(Error::client(format!("create new topic failed: {cause}")));
            }
        }
        Ok(())
    }

    /// Delete a topic on a broker (DELETE_TOPIC_IN_BROKER = 215).
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
            .invoke_broker_sync(broker_addr, &mut request, timeout_millis)
            .await?;
        Self::check_response(&response)?;
        Ok(())
    }

    /// Delete the topic config on the name server (DELETE_TOPIC_IN_NAMESRV = 216):
    /// succeed on any name server; raise only when all of them fail.
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
        let response = match self.invoke_broker_sync(&addr, &mut request, timeout_millis).await {
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

/// Group-name slots of `UNREGISTER_CLIENT`(35): blank ⇒ kept off the wire.
fn blank_group_to_none(group: &str) -> Option<String> {
    (!group.trim().is_empty()).then(|| group.to_string())
}

/// Sleep until the absolute `deadline` (fixed-rate anchor, errors never accumulate: every round
/// is computed against the same timeline); returns true immediately once stop is set.
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

/// `tls_enable=None` behavior: read the environment variable.
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

/// The 326 payload → MessageExt (reply-restore step split out so it can be unit-tested).
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
    // An empty string means absent; it must not be stored as Some("").
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

/// The six instance-level broker-initiated request codes share one processor (a single
/// `RequestProcessor` registered under several codes, equivalent to per-method registration).
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
                // Oneway: the ResponseSink drops the reply per wants_reply,
                // so nothing is sent back here.
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
// Broker-initiated requests (220/221/307/309/326) can only be pushed in by the broker over an
// already-established connection, so live examples (`examples/live_mq_client.rs`) cannot inject
// them; the protocol + dispatch logic is pinned down offline here (consumer-side counterparts
// land once `consumer.rs` is done).

// Fixtures use "default + set only the fields that are read": a request header has a dozen-plus
// fields, per-field literals would bury the test intent, so field_reassign_with_default is off module-wide.
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

    /// Each test uses its own clientId: `INSTANCE_MAP` is a process-wide shared table.
    fn new_instance() -> MQClientInstance {
        let id = format!("{GROUP}@{}", SEQ.fetch_add(1, Ordering::Relaxed));
        MQClientInstance::new(&id, vec![NAMESRV.to_string()])
    }

    fn guard<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        m.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Poll for background progress (up to 2s) instead of hanging assertions on a fixed sleep.
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
        // On the real wire the broker receives extFields; same convention here (the processor reads only extFields).
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

    /// Records each 220's arguments verbatim (topic + offset table); both body shapes assert through it.
    type ResetCall = (String, Vec<(MessageQueue, i64)>);

    /// Consumer stand-in that only records calls and performs no RPC.
    struct StubConsumer {
        group: String,
        /// Delay before the 220 writes offsets, proving handling did not block the calling thread.
        reset_delay: Duration,
        resets: Arc<Mutex<Vec<ResetCall>>>,
        status_topics: Arc<Mutex<Vec<Option<String>>>>,
        persisted: Arc<AtomicUsize>,
        /// How many rebalance wake-ups the 40 notification caused.
        rebalance_wakeups: Arc<AtomicUsize>,
        subs: Vec<SubscriptionData>,
    }

    impl StubConsumer {
        fn new() -> Arc<StubConsumer> {
            Arc::new(StubConsumer {
                ..StubConsumer::bare()
            })
        }

        /// Stand-in carrying only a subscription table: the 46 check needs just `subscriptions()`.
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
                // A real implementation would run rebalance / lock / batch here (all invokeSync),
                // so the whole block must execute in the background.
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
        // The calling thread returns at once: the stub is still "in an RPC" here, so the offset table must be empty.
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

    /// When a 222 was initiated with language=CPP, the broker pushes the **array** body (`ResetOffsetBodyForC`);
    /// the map-shape parser only fails on it, and without the fallback the whole reset would vanish silently.
    #[tokio::test]
    async fn reset_offset_array_body_for_c_is_not_dropped() {
        let instance = new_instance();
        let consumer = StubConsumer::new();
        instance.register_consumer(GROUP, consumer.clone());

        let mut header = ResetOffsetRequestHeader::default();
        header.group = Some(GROUP.to_string());
        header.topic = Some(TOPIC.to_string());
        let mut cmd = request(request_code::RESET_CONSUMER_CLIENT_OFFSET, header);
        // Real broker wire shape: offsetTable is an array, each entry carrying its own offset
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

    /// Once the map shape parses, the ForC parse must not be layered on top (each path builds its own table);
    /// otherwise the same queue would be pushed twice. The table holds two distinct keys, so count assertions suffice to tell them apart.
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
        // Nonexistent group: warn only, no reply, no panic (oneway).
        let mut header = ResetOffsetRequestHeader::default();
        header.group = Some("GID_Nobody".to_string());
        header.topic = Some(TOPIC.to_string());
        instance.process_reset_offset(&request(request_code::RESET_CONSUMER_CLIENT_OFFSET, header));
        // Not even a group: returns directly the same way.
        instance.process_reset_offset(&request(
            request_code::RESET_CONSUMER_CLIENT_OFFSET,
            ResetOffsetRequestHeader::default(),
        ));
        // Bad body: a decode failure must not affect the caller.
        let consumer = StubConsumer::new();
        instance.register_consumer(GROUP, consumer.clone());
        // Both shapes fail to decode in all three cases: invalid JSON, offsetTable is a scalar (not an object on the
        // map side, not an array on the ForC side), top level is an array -- not a single finger may be touched.
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

        // `header.topic` is handed to the consumer as-is (None and "" differ in meaning: only a non-empty
        // topic filters), so the three kinds of input must be distinguishable here.
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

        // Empty body / missing body (both yield SYSTEM_ERROR + a fixed remark).
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

        // After unregistration they are no longer woken, but the notification itself is still processed.
        instance.unregister_consumer(GROUP);
        instance.unregister_consumer("GID_Other");
        instance.rebalance_immediately();
        assert_eq!(first.rebalance_wakeups.load(Ordering::SeqCst), 2);
        assert_eq!(second.rebalance_wakeups.load(Ordering::SeqCst), 2);
        instance.shutdown();
    }

    // ---------------- 326 PUSH_REPLY_MESSAGE_TO_CLIENT ----------------

    /// The properties wire format is decided by message_properties_2_string (k/v and entries each have their own
    /// separators); "k=v" must not be hand-assembled.
    fn wire_properties(key: &str, value: &str) -> String {
        let mut props = crate::remoting::protocol::ext_fields::ExtFields::new();
        props.insert(key.to_string(), value.to_string());
        message_properties_2_string(&props)
    }

    #[test]
    fn reply_message_treats_blank_hosts_as_absent() {
        // An empty string must not become Some("").
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
        // No pending request(): only warn and reply SUCCESS (no retry needed on the broker side).
        let instance = new_instance();
        let mut header = ReplyMessageRequestHeader::default();
        header.topic = Some(TOPIC.to_string());
        header.properties = Some(wire_properties(PROPERTY_CORRELATION_ID, "corr-unknown"));
        let resp = instance
            .process_reply_message(&request(request_code::PUSH_REPLY_MESSAGE_TO_CLIENT, header));
        assert_eq!(resp.code, response_code::SUCCESS);
        instance.shutdown();
    }

    // ---------------- Lifecycle ----------------

    /// Only the persist-offset loop runs fast; other periodic tasks are pushed past 1 hour (offline cases must not touch the network).
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
                // Push all built-in loops past 1 hour, leaving only the probe running.
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
            "the first tick never happened"
        );
        let first = guard(&ticks)[0].duration_since(t0);
        assert!(
            first < Duration::from_millis(250),
            "first tick late {first:?}: with initialDelay=30ms the first tick must fire (the old form waited until 330ms)"
        );

        assert!(
            wait_until(|| guard(&ticks).len() >= 2).await,
            "the second tick never happened"
        );
        let second = guard(&ticks)[1].duration_since(t0);
        assert!(
            second >= Duration::from_millis(300) && second < Duration::from_millis(600),
            "second tick should land at initialDelay+period (~330ms), measured {second:?}"
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
        assert_eq!(started, 4, "one each of route/adjust/heartbeat/persist");
        assert!(instance.is_started());

        // A second producer/consumer with the same clientId also calls start(): it must not spin up a second copy of the loops.
        instance.start().await.unwrap();
        assert_eq!(
            task_count(&instance),
            started,
            "second start spawned duplicate loops"
        );

        assert!(wait_until(|| consumer.persisted.load(Ordering::SeqCst) > 0).await);

        // The guard reads consumer_table itself: unregister self first so shutdown can truly tear down
        // (see `shutdown_keeps_the_instance_alive_while_tenants_remain`).
        instance.unregister_consumer(GROUP);
        instance.shutdown();
        assert!(!instance.is_started());
        assert_eq!(
            task_count(&instance),
            0,
            "shutdown left task handles behind"
        );
        let after_stop = consumer.persisted.load(Ordering::SeqCst);

        // Restart must truly run: relies on the stop signal being reset.
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

        // One producer leaves: the consumer and the other producer still use it, the instance must keep running.
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

        // Every shutdown before the last tenant has left is still a no-op.
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
        // `JoinHandle::abort` only takes effect at the next yield point, so allow one beat; afterwards the persist interval is 5ms,
        // and after another 80ms the counter must not budge, proving the loop truly stopped.
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
                // No static address configured ⇒ dynamic addressing; unreachable address server ⇒ start() must fail
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

    /// After the guard passes, teardown removes its own registration: when the same clientId was overwritten
    /// by a later instance, it must not unregister someone else's entry (`with_config` inserts unconditionally).
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
        // Once the registry entry is dead, the same clientId must get a brand-new instance (not a stale one carrying old addresses).
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
        // The facade's user hook (ACL) is registered **after instance creation**, mirroring the five facades' start().
        instance
            .remoting_client()
            .register_rpc_hook(Arc::new(AclClientRPCHook::new(SessionCredentials::new(
                "AK", "SK",
            ))));
        let hooks = instance.remoting_client().rpc_hooks();
        assert_eq!(
            hooks.len(),
            3,
            "the chain must be exactly Namespace → Stream → user ACL"
        );

        // hooks[0] = Namespace: only writes nsd/ns, never touches ReqT / AccessKey
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
            "the first hook must be Namespace"
        );
        assert_eq!(c0.get_ext_field(SessionCredentials::ACCESS_KEY), None);

        // hooks[1] = Stream: writes ReqT, ACL still has not run
        let mut c1 = cmd();
        hooks[0].do_before_request(NAMESRV, &mut c1);
        hooks[1].do_before_request(NAMESRV, &mut c1);
        assert_eq!(c1.get_ext_field(MixAll::REQ_T), Some("0"));
        assert_eq!(
            c1.get_ext_field(SessionCredentials::ACCESS_KEY),
            None,
            "the second hook must be Stream: ACL has not run yet"
        );

        // Full chain run: Signature equals, byte for byte, the expected value built with nsd/ns/ReqT all in the signing content
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
            "the signature content must cover nsd/ns/ReqT"
        );

        // Control group: run only the user ACL (no Namespace/Stream in front) ⇒ a different signature,
        // i.e. the namespace_v2 config changes the signature that finally goes on the wire.
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
            "the default config has only the Namespace hook (no stream)"
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
            "a no-op hook must not add headers"
        );
        assert_eq!(cmd.get_ext_field(MixAll::REQ_T), None);
        instance.shutdown();
    }

    // ---------------- 46 CHECK_CLIENT_CONFIG ----------------

    /// One request received by the fake broker (verbatim off the wire, without any decoding convenience wrapper).
    #[derive(Debug, Clone)]
    struct Recorded {
        code: i32,
        ext_fields: Vec<(String, String)>,
        body: Vec<u8>,
    }

    /// Queued reply extFields tables from the script.
    type ExtFieldsQueue = Arc<Mutex<VecDeque<Vec<(String, String)>>>>;

    /// In-process fake broker: speaks only the remoting protocol, answering each request per script.
    struct MockBroker {
        addr: String,
        state: Arc<Mutex<Vec<Recorded>>>,
        /// Script: response code for the next request (answers SUCCESS once exhausted).
        codes: Arc<Mutex<VecDeque<i32>>>,
        remarks: Arc<Mutex<VecDeque<String>>>,
        /// Script: extFields of the next response (empty table once exhausted). Things like PULL_MESSAGE's
        /// `suggestWhichBrokerId` / `nextBeginOffset` go on the wire through it.
        resp_ext: ExtFieldsQueue,
        /// Script: body of the next response (no body once exhausted). Replies such as the lock set go on the wire through it.
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

        /// Script the extFields of subsequent responses (default empty table, matching pre-change behavior).
        fn script_resp_ext(&self, exts: Vec<Vec<(String, String)>>) {
            *guard(&self.resp_ext) = exts.into();
        }

        /// Script the body of subsequent responses (default: send no body).
        fn script_resp_body(&self, bodies: Vec<Vec<u8>>) {
            *guard(&self.resp_body) = bodies.into();
        }

        /// Keep only request code 46: side effects like route refreshes must not leak into the assertions.
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

        /// Body of the `n`-th 46 request parsed as JSON.
        fn body_of(&self, n: usize) -> serde_json::Value {
            let checks = self.checks();
            match checks.get(n) {
                Some(r) => serde_json::from_slice(&r.body).unwrap_or(serde_json::Value::Null),
                None => serde_json::Value::Null,
            }
        }
    }

    /// Read one full frame and decode it (`decode` starts at totalLength, so the frame must be handed over with its length prefix).
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
        // The client pairs responses by opaque and drops a crossed one as noise, so the original value must be echoed back.
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

    /// Seeds the route into the instance cache with the broker address pointing at `addr` (`route_of` only reads the cache, it never fetches).
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

    /// Subscriptions and the cached route both in place: returns an instance ready to run `check_client_in_broker` directly.
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
        // The request header is null ⇒ no extFields on the wire.
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
        // SUBSCRIPTION_PARSE_FAILED(23) must carry the response code as-is -- that is the criterion for the startup failure.
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
        // consumerTable only takes push-mode consumers; lite pull / pull-mode consumers get the same semantics via the inner loop.
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
        // `selectBrokerAddr`: when MASTER_ID(0) is in the table the master is used, never a slave.
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
        // Division of labor between the two helpers: `get_route_of_all_brokers` picks one addr per brokerName
        // (used by the producer heartbeat), `get_all_broker_addrs` covers every one (used by unregister and the consumer heartbeat)
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
        assert_eq!(unregs(&master).len(), 1, "master received one 35");
        assert_eq!(unregs(&slave).len(), 1, "slave also received one 35");
        for rec in unregs(&master).into_iter().chain(unregs(&slave)) {
            let keys: Vec<&str> = rec.ext_fields.iter().map(|(k, _)| k.as_str()).collect();
            assert_eq!(keys, vec!["clientID", "producerGroup"], "blank slots stay off the wire");
        }
        instance.shutdown();
    }

    /// The consumer heartbeat must reach **every** broker (master + slave); a heartbeat carrying only producer data hits the master only.
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
            "master + slave, one each"
        );
        assert_eq!(master.recorded(request_code::HEART_BEAT).len(), 1);
        let on_slave = slave.recorded(request_code::HEART_BEAT);
        assert_eq!(on_slave.len(), 1, "the slave must receive a heartbeat too");
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

    /// The instance heartbeat is the registration path of **both** kinds: with only a
    /// producer registered it must still beat, or the broker holds no channel of this client
    /// and can never route a transaction check-back or a reply back to it.
    #[tokio::test]
    async fn instance_heartbeat_carries_producer_and_consumer_registrations() {
        const PG: &str = "GID_producer_half";
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

        // Nothing registered: the round trip is not spent on an empty payload.
        assert_eq!(instance.send_heartbeat_to_all_broker(3000).await, 0);
        assert!(master.recorded(request_code::HEART_BEAT).is_empty());

        instance.register_producer(PG);
        assert_eq!(
            instance.send_heartbeat_to_all_broker(3000).await,
            2,
            "a producer-only instance beats to master + slave"
        );
        let on_master = master.recorded(request_code::HEART_BEAT);
        let hb = HeartbeatData::decode(&on_master[0].body).expect("heartbeat body is a HeartbeatData");
        let groups: Vec<&str> = hb
            .producer_data_set
            .iter()
            .map(|pd| pd.group_name.as_str())
            .collect();
        assert_eq!(groups, vec![PG]);
        assert!(
            hb.consumer_data_set.is_empty(),
            "no consumer is registered on this instance"
        );

        // Once a consumer joins, one beat refreshes both halves.
        instance.register_consumer(GROUP, StubConsumer::new());
        assert_eq!(instance.send_heartbeat_to_all_broker(3000).await, 2);
        let on_master = master.recorded(request_code::HEART_BEAT);
        let hb = HeartbeatData::decode(&on_master[1].body).expect("heartbeat body is a HeartbeatData");
        assert_eq!(hb.producer_data_set.len(), 1, "the producer stays registered");
        assert_eq!(hb.consumer_data_set.len(), 1, "the consumer joins the same beat");

        instance.unregister_producer(PG);
        assert_eq!(instance.prepare_heartbeat_data().producer_data_set.len(), 0);
        instance.shutdown();
    }

    /// Registration only goes to brokers already in the route cache, so the beat has to
    /// follow the route that first lands (and every broker that joins later) instead of
    /// waiting for the next 30s round.
    #[tokio::test]
    async fn route_refresh_re_registers_brokers_that_just_joined() {
        const PG: &str = "GID_route_join";
        let first = MockBroker::start().await;
        let namesrv = RouteNamesrv::start().await;
        namesrv.serve(Some(route_body(BROKER, &[(0, first.addr.as_str())])));
        let instance = instance_against(namesrv.addr.as_str());
        instance.register_producer(PG);

        assert!(instance
            .update_topic_route_info_from_name_server(TOPIC, 3000, false)
            .await
            .unwrap());
        let beats = first.recorded(request_code::HEART_BEAT);
        assert_eq!(
            beats.len(),
            1,
            "the broker that just entered the route is registered at once"
        );
        let hb = HeartbeatData::decode(&beats[0].body).expect("heartbeat body is a HeartbeatData");
        assert_eq!(hb.producer_data_set.len(), 1, "the beat carries the producer group");

        // Same route again: every known broker is already registered, so nothing goes out.
        assert!(instance
            .update_topic_route_info_from_name_server(TOPIC, 3000, false)
            .await
            .unwrap());
        assert_eq!(
            first.recorded(request_code::HEART_BEAT).len(),
            1,
            "an unchanged broker set is not beaten again"
        );

        // A slave joins the route: it is registered by the very next refresh, and the broker
        // that was already registered is not beaten a second time.
        let second = MockBroker::start().await;
        namesrv.serve(Some(route_body(
            BROKER,
            &[(0, first.addr.as_str()), (1, second.addr.as_str())],
        )));
        assert!(instance
            .update_topic_route_info_from_name_server(TOPIC, 3000, false)
            .await
            .unwrap());
        assert_eq!(
            second.recorded(request_code::HEART_BEAT).len(),
            1,
            "the new broker is registered without waiting for the next beat"
        );
        assert_eq!(
            first.recorded(request_code::HEART_BEAT).len(),
            2,
            "one beat registers the whole known set, not only the broker that joined"
        );

        // And with the set unchanged again, the refresh spends no round trip at all.
        assert!(instance
            .update_topic_route_info_from_name_server(TOPIC, 3000, false)
            .await
            .unwrap());
        assert_eq!(
            first.recorded(request_code::HEART_BEAT).len(),
            2,
            "an unchanged broker set is not re-beaten"
        );
        assert_eq!(second.recorded(request_code::HEART_BEAT).len(), 1);
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
        // Producer exit: only clientID + producerGroup
        assert_eq!(
            header_ext("GID_p", ""),
            vec!["clientID".to_string(), "producerGroup".to_string()]
        );
        // Consumer exit: only clientID + consumerGroup
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
        // Hit: the id is used as-is, slave = id != 0
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
        // Only this broker (onlyThisBroker) is accepted: none is preferred over a wrong one
        assert_eq!(
            MQClientInstance::find_broker_addr_in_subscribe(&addrs, 9, true),
            None
        );
        assert_eq!(
            MQClientInstance::find_broker_addr_in_subscribe(&[], 0, false),
            None
        );
    }

    /// Builds an instance with one fake master broker and one fake slave broker: returns (instance, master, slave).
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

    /// Picks the suffix-matched key from a pull request's extFields (used for asserting the wire shape).
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

        // Hit the slave, not the master
        assert_eq!(slave.recorded(request_code::PULL_MESSAGE).len(), 1);
        assert!(master.recorded(request_code::PULL_MESSAGE).is_empty());
        let rec = &slave.recorded(request_code::PULL_MESSAGE)[0];
        let sent_flag: i32 = ext_of(rec, "sysFlag").expect("sysFlag").parse().unwrap();
        assert!(
            !PullSysFlag::has_commit_offset_flag(sent_flag),
            "a COMMIT_OFFSET on the slave would be meaningless"
        );
        assert!(PullSysFlag::has_suspend_flag(sent_flag), "suspend bit stays set");
        assert!(
            ext_of(rec, "subscription").is_none(),
            "with the SUBSCRIPTION bit off the field never enters extFields (null → field dropped)"
        );
        // The response header's suggestWhichBrokerId is passed through to the caller, for writing back pullFromWhichNodeTable
        assert_eq!(result.suggest_which_broker_id, Some(0));
        instance.shutdown();
    }

    /// With brokerId=0 (the master): the request goes to the master and the `COMMIT_OFFSET` bit is kept; the
    /// `subscription` field only goes on the wire with the SUBSCRIPTION bit set, carrying the caller-supplied expression.
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
            "offset still committed on master"
        );
        assert!(PullSysFlag::has_subscription_flag(sent_flag));
        assert_eq!(
            ext_of(&recs[0], "subscription").as_deref(),
            Some("TagA||TagB")
        );
        assert_eq!(ext_of(&recs[0], "queueOffset").as_deref(), Some("3"));
        // Old brokers omit suggestWhichBrokerId: the caller books master=0 (None is passed out)
        assert_eq!(result.suggest_which_broker_id, None);
        assert_eq!(result.next_begin_offset, 7);
        instance.shutdown();
    }


    /// Fake name server: answers `GET_ROUTEINFO_BY_TOPIC` with the route body held in `holder`
    /// (`None` ⇒ TOPIC_NOT_EXIST), SUCCESS for everything else; records every request code it receives.
    ///
    /// Route updates always go through the **real** `update_topic_route_info_from_name_server` to reach the tables: the
    /// publish-address table's write point lives inside it, and bypassing it to stuff `broker_addr_table` directly would leave this path untested.
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
                Err(e) => panic!("bind fake name server: {e}"),
            };
            let addr = match listener.local_addr() {
                Ok(a) => a.to_string(),
                Err(e) => panic!("fake name server addr: {e}"),
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
                            // The client pairs responses by opaque; a mismatched one is dropped as noise.
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

        /// Swap in a different route body (`None` = the name server says this topic does not exist).
        fn serve(&self, body: Option<Vec<u8>>) {
            *guard(&self.holder) = body;
        }

        fn codes(&self) -> Vec<i32> {
            guard(&self.codes).clone()
        }

        /// How many route refreshes ran (the "refresh once on lookup miss" pass inside publish-address resolution).
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

    /// A topic route body: one machine under `broker_name`, with `addrs` mapping its brokerId → addr.
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
        serde_json::to_vec(&route.to_json_value()).expect("route is serializable")
    }

    /// An instance pointed at the given name server (`new_instance` hardcodes the NAMESRV constant, so the fake server address is needed here).
    fn instance_against(namesrv: &str) -> MQClientInstance {
        let id = format!("{GROUP}@rs{}", SEQ.fetch_add(1, Ordering::Relaxed));
        MQClientInstance::new(&id, vec![namesrv.to_string()])
    }

    /// `find_broker_address_in_publish` only accepts brokerId=0; on the same route the admin lookup must fall back to the slave.
    #[tokio::test]
    async fn publish_lookup_takes_only_the_master_while_the_admin_lookup_falls_back() {
        let namesrv = RouteNamesrv::start().await;
        // Master is down: only brokerId=1 is left in the route (the slave registers itself with the name server too)
        namesrv.serve(Some(route_body(
            BROKER,
            &[(MixAll::MASTER_ID as i64 + 1, "127.0.0.1:10931")],
        )));
        let instance = instance_against(&namesrv.addr);
        assert!(instance
            .update_topic_route_info_from_name_server(TOPIC, 5000, false)
            .await
            .expect("the fake name server answers just fine"));

        assert_eq!(instance.find_broker_address_in_publish(BROKER), None);
        // Negative control: the fallback lookup (used by heartbeat/pull/offset queries) must get the slave address from **the same route**
        assert_eq!(
            instance.broker_addr_of(BROKER).as_deref(),
            Some("127.0.0.1:10931")
        );

        // Master re-registers: the same name resolves immediately (what was skipped is "no master", not broker-a)
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
            .expect("the fake name server answers just fine"));
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
                .expect("resolves after the route refresh"),
            "127.0.0.1:10911"
        );
        assert_eq!(namesrv.route_requests(), 1, "refresh only on lookup miss, and only once");
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
            other => panic!("expected a local MQClientException, got {other:?}"),
        }
    }

    /// Master down (only brokerId=1 left in the route): this side reports "broker not exist" without sending a single request.
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
            .expect_err("no master means no publish address");
        assert_broker_not_exist(&err);
        assert_eq!(namesrv.route_requests(), 1);

        // Negative control: as soon as the master registers the name resolves (what was refreshed was "no master", not this brokerName)
        namesrv.serve(Some(route_body(
            BROKER,
            &[(MixAll::MASTER_ID as i64, "127.0.0.1:10911")],
        )));
        assert_eq!(
            instance
                .publish_addr_for(BROKER, TOPIC)
                .await
                .expect("the master is back"),
            "127.0.0.1:10911"
        );
        instance.shutdown();
    }

    /// The brokerName is not in the route at all (typo / already offline): also reports not exist, never falls back to another broker.
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
            .expect_err("this name is not in the route");
        assert_broker_not_exist(&err);
        instance.shutdown();
    }

    /// All four admin offset queries share one policy: master only, and "broker not exist" once the master is gone.
    #[tokio::test]
    async fn admin_offset_queries_are_master_only_too() {
        let namesrv = RouteNamesrv::start().await;
        let master = MockBroker::start().await;
        let slave = MockBroker::start().await;
        // Only the slave is registered ⇒ the admin path must not settle for it either
        namesrv.serve(Some(route_body(
            BROKER,
            &[(MixAll::MASTER_ID as i64 + 1, &slave.addr)],
        )));
        let instance = instance_against(&namesrv.addr);
        let mq = MessageQueue::new(TOPIC, BROKER, 0);
        let err = instance
            .get_max_offset(&mq, 5000, None)
            .await
            .expect_err("no master means no store to query");
        assert_broker_not_exist(&err);
        assert!(
            slave.recorded(request_code::GET_MAX_OFFSET).is_empty(),
            "not a single request is sent before the error"
        );

        // Negative control: once the master is back the query succeeds, and the request lands on the **master** address (not the slave from the route)
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
                .expect("the master is back"),
            7
        );
        assert_eq!(master.recorded(request_code::GET_MAX_OFFSET).len(), 1);
        assert!(slave.recorded(request_code::GET_MAX_OFFSET).is_empty());
        instance.shutdown();
    }


    /// When only a slave is left in the route, not a single LOCK/UNLOCK may go on the wire: a lock taken on the slave
    /// lives only in that slave's own lock manager, the master never learns of it, and orderly consumption's mutual exclusion silently breaks.
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
            .expect("the fake name server answers just fine"));
        let before = namesrv.route_requests();
        let mqs = vec![
            MessageQueue::new(TOPIC, BROKER, 0),
            MessageQueue::new(TOPIC, BROKER, 1),
        ];

        assert!(instance
            .lock_batch_mq(GROUP, "cid", &mqs, 3000)
            .await
            .expect("skipping the whole broker is not an error")
            .is_empty());
        instance
            .unlock_batch_mq(GROUP, "cid", &mqs, 3000)
            .await
            .expect("skipping the whole broker is not an error");
        assert!(slave.recorded(request_code::LOCK_BATCH_MQ).is_empty());
        assert!(slave.recorded(request_code::UNLOCK_BATCH_MQ).is_empty());
        assert_eq!(namesrv.route_requests(), before);

        // Negative control: once the master is back lock/unlock land on the master address and the returned lock set is exactly the two requested queues
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
            .expect("the fake name server answers just fine"));
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
            .expect("locks once the master is back");
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
            .expect("unlocks once the master is back");
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
            .expect("the fake name server answers just fine"));
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
            .expect_err("no master means no broker to pop from");
        assert_broker_not_exist(&err);
        // A route refresh must have happened before the error (popAsync's findBrokerAddressInSubscribe + re-lookup)
        assert_eq!(namesrv.route_requests(), before + 1);
        assert!(slave.recorded(request_code::POP_MESSAGE).is_empty());

        // Negative control: after the master registers (this call triggers a route refresh along the way) the request lands on the master address and the slave gets none
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
            .expect("pops once the master is back");
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
            .expect("the fake name server answers just fine"));
        let mq = MessageQueue::new(TOPIC, BROKER, 0);
        let before = namesrv.route_requests();

        // Master gone: the query finishes on the slave and returns the **slave's** value
        slave.script_resp_ext(vec![vec![("offset".to_string(), "424242".to_string())]]);
        assert_eq!(
            instance
                .query_consumer_offset(GROUP, &mq, 5000, None, false)
                .await
                .expect("queryable on the slave"),
            Some(424242)
        );
        assert_eq!(slave.recorded(request_code::QUERY_CONSUMER_OFFSET).len(), 1);
        assert_eq!(namesrv.route_requests(), before + 1);

        // Negative control: once the master is back only the master is hit (the slave's count stops growing)
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
            .expect("the fake name server answers just fine"));
        master.script_resp_ext(vec![vec![("offset".to_string(), "111".to_string())]]);
        assert_eq!(
            instance
                .query_consumer_offset(GROUP, &mq, 5000, None, false)
                .await
                .expect("the offset exists on the master"),
            Some(111)
        );
        assert_eq!(
            master.recorded(request_code::QUERY_CONSUMER_OFFSET).len(),
            1
        );
        assert_eq!(slave.recorded(request_code::QUERY_CONSUMER_OFFSET).len(), 1);
        instance.shutdown();

        // broker-a is not in the route at all (not even a slave): after one route refresh the lookup still misses ⇒ this side reports not exist
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
            .expect_err("this name is not in the route");
        assert_broker_not_exist(&err);
        assert!(other
            .recorded(request_code::QUERY_CONSUMER_OFFSET)
            .is_empty());
        instance2.shutdown();
    }
}
