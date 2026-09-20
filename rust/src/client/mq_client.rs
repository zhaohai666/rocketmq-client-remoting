//! `MQClientInstance`：RocketMQ 客户端核心编排（对应
//! `org.apache.rocketmq.client.impl.factory.MQClientInstance` 与
//! `MQClientAPIImpl` 的核心调用面，逐条移植
//! `python/rocketmq/client/mq_client.py`）。
//!
//! 职责：NameServer 地址管理、Topic 路由获取与缓存、Broker 地址解析、
//! 消息发送（SEND_MESSAGE / SEND_MESSAGE_V2）、拉取（PULL_MESSAGE）、POP 弹取
//! （POP_MESSAGE / ACK_MESSAGE / CHANGE_MESSAGE_INVISIBLETIME）、offset 查询/更新、
//! 心跳、队列锁、管理类 API（创建/删除 Topic、集群信息等）、broker 主动请求
//! （220/221/307/309/326）分派。
//!
//! ## 与 Python 参考实现的有意差异（均在对应条目 doc 上再次标注）
//!
//! 1. **实例即句柄**：Python 的 `MQClientInstance` 对象被 producer/consumer 共享；
//!    Rust 里 [`MQClientInstance`] 是 `Clone` 的 `Arc<Inner>` 句柄（同 `RemotingClient`），
//!    注册进 `INSTANCE_MAP` 的是弱引用。
//! 2. **依赖注入**：Python 在 `__init__` 里直接构造 `ConsumerStatsManager()`；
//!    这里改为调用方通过 [`MQClientInstanceConfig`] 传 `Arc<...>`
//!    （`consumer_stats_manager` / `latency_fault_tolerance` / `trace_dispatcher` /
//!    `top_addressing`），None 时回落默认构造，保持与 Python 相同的默认行为，
//!    让 producer / consumer / admin 各移植层可以共享同一份实例。
//! 3. **Python 把「实例级 RPC 包装」与「工厂」合并在同一个 `MQClientInstance` 类里**
//!    （Java 是拆成 `MQClientAPIImpl` + `MQClientInstance` 两个类），这里照抄 Python：
//!    不再单独造 `MQClientAPIImpl`。
//! 4. **`send_heartbeat_to_all_broker` / `persist_consumer_offsets` 及对应周期任务**
//!    在 Python 里挂在 consumer（`consumer.py` `_build_heartbeat` 等）与实例的
//!    consumerTable 之间，本模块按 Java `MQClientInstance` 语义收敛到实例上，
//!    通过 [`RegisteredConsumer`] seam 读取消费者数据；间隔参数在
//!    [`MQClientInstanceConfig`] 中可配（测试用）。
//! 5. 所有 RPC 方法是 `async fn`（Python 是同步阻塞调用），后台线程换成
//!    `tokio::spawn` + `JoinHandle`，shutdown 用 watch 信号 + abort，确定可测。
//! 6. `time.time()*1000` 一律换成 [`current_time_millis`]；`threading.Lock`
//!    换成 `Mutex`（统一 `lock().unwrap_or_else(|e| e.into_inner())`）。

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
use crate::common::compression::decompress_body;
use crate::common::message::{Message, MessageBatch, MessageExt, MessageQueue};
use crate::common::message_client_id_setter::{get_uniq_id, set_uniq_id};
use crate::common::message_const::{
    KEY_SEPARATOR, PROPERTY_CORRELATION_ID, PROPERTY_FIRST_POP_TIME, PROPERTY_MSG_REGION,
    PROPERTY_POP_CK, PROPERTY_REPLY_MESSAGE_ARRIVE_TIME, PROPERTY_TRACE_SWITCH,
};
use crate::common::message_decoder::{
    decode_message, decode_messages, message_properties_2_string, string_2_message_properties,
};
use crate::common::mix_all::MixAll;
use crate::common::sysflag::MessageSysFlag;
use crate::common::topic_config::TopicFilterType;
use crate::common::util_all::current_time_millis;
use crate::error::{Error, Result};
use crate::remoting::client::{RemotingClient, RemotingClientConfig, RequestProcessor, ResponseSink};
use crate::remoting::protocol::admin_body::MessageQueueKey;
use crate::remoting::protocol::body::{
    ClusterInfo, ConsumeMessageDirectlyResult, ConsumerRunningInfo,
    GetConsumerListByGroupResponseBody, GetConsumerStatusBody, LockBatchRequestBody,
    LockBatchResponseBody, ResetOffsetBody, TopicList, UnlockBatchRequestBody,
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
    GetMaxOffsetRequestHeader, GetMaxOffsetResponseHeader,
    GetMinOffsetRequestHeader, GetMinOffsetResponseHeader, LockBatchMqRequestHeader,
    NotifyConsumerIdsChangedRequestHeader,
    PopMessageRequestHeader, PopMessageResponseHeader, PullMessageRequestHeader,
    PullMessageResponseHeader, QueryConsumerOffsetRequestHeader,
    QueryConsumerOffsetResponseHeader, QueryMessageRequestHeader, RecallMessageRequestHeader,
    RecallMessageResponseHeader, ReplyMessageRequestHeader,
    ResetOffsetRequestHeader, SearchOffsetRequestHeader, SearchOffsetResponseHeader,
    SendMessageRequestHeaderV2, SendMessageResponseHeader, UnregisterClientRequestHeader,
    UpdateConsumerOffsetRequestHeader, UnlockBatchMqRequestHeader,
};
use crate::remoting::protocol::heartbeat::{ConsumerData, HeartbeatData, SubscriptionData};
use crate::remoting::protocol::remoting_command::RemotingCommand;
use crate::remoting::protocol::route::TopicRouteData;
use crate::{bail, rmq_debug, rmq_info, rmq_warn};

// ================================================================ seams

/// 消费者对实例暴露的能力面（对应 Java `MQConsumerInner` 中
/// `MQClientInstance` 心跳 / 位点持久化 / broker 主动请求分派真正读到的部分）。
///
/// 方法集合按 Python 实际读取逐条枚举：
/// - `consumer.py::_build_heartbeat` 读 `client_id`/`consumer_group`/
///   `ConsumeType.CONSUME_PASSIVELY`/`message_model`/`consume_from_where` +
///   `topic -> SubscriptionData` 表（`subscription_data`），`unit_mode` 恒 `False`；
/// - `mq_client.py::_process_reset_offset`（220）调 `reset_offset(topic, offset_table)`，
///   且必须离开 remoting 读线程后台执行；
/// - `_process_get_consumer_status`（221）调 `get_consumer_status(topic)`；
/// - `_process_get_consumer_running_info`（307）调 `consumer_running_info()`；
/// - `_process_consume_message_directly`（309）调 `consume_message_directly(msg, broker_name)`；
/// - `adjust_thread_pool`（Java `MQClientInstance#adjustThreadPool`，5.5.1 里是 no-op）；
/// - Java `updateTopicRouteInfoFromNameServer()` / `sendHeartbeatToAllBroker` 需要
///   `subscription()`（topic 集合）与 `persistConsumerOffset()`。
///
/// 对象安全：异步行为方法用 `self: Arc<Self>` 接收者 + 装箱 future，
/// 实例里以 `Arc<dyn RegisteredConsumer>` 持有。
pub trait RegisteredConsumer: Send + Sync {
    /// 消费者自己的 clientId（Python `HeartbeatData(self.client_id)`）。
    fn client_id(&self) -> String;

    /// `ConsumerData.groupName`（Python `self.consumer_group`）。
    fn consumer_group(&self) -> String;

    /// `ConsumerData.consumeType`，Python 恒为 `CONSUME_PASSIVELY`
    /// （Java `DefaultMQPushConsumerImpl#consumeType`）。
    fn consume_type(&self) -> String;

    /// `ConsumerData.messageModel`（`CLUSTERING` / `BROADCASTING`）。
    fn message_model(&self) -> String;

    /// `ConsumerData.consumeFromWhere`。
    fn consume_from_where(&self) -> String;

    /// `ConsumerData.unitMode`。Python 没有 unit mode，恒 `false`
    /// （见 `consumer.py:122` 注释，对齐 Java `isUnitMode()`）。
    fn is_unit_mode(&self) -> bool;

    /// 订阅的 topic 集合（Java `MQConsumerInner#subscription`）。
    /// 路由周期刷新与心跳都会读。
    fn subscription(&self) -> Vec<String>;

    /// `topic -> SubscriptionData` 表（Python `self.subscription_data.values()`），
    /// 进心跳的 `ConsumerData.subscriptionDataSet`。
    fn subscriptions(&self) -> Vec<SubscriptionData>;

    /// 线程弹性巡检（Java `MQClientInstance#adjustThreadPool` 调用的
    /// `consumer.adjustThreadPool()`；Python/Java 均 no-op）。
    fn adjust_thread_pool(&self) {}

    /// broker 推 `NOTIFY_CONSUMER_IDS_CHANGED(40)` 时，实例对每个已注册消费者的扇出
    /// （Java `MQClientInstance#rebalanceImmediately` → `RebalanceService#wakeup` →
    /// `doRebalance` 逐个调 `tryRebalance`）。
    ///
    /// 默认 no-op：拉模式的消费组没有后台重平衡循环，Java 那边唤醒它也是空转。
    fn rebalance_immediately(&self) {}

    /// 220 处理：broker 下发位点重置。必须**后台**执行（内部会做 rebalance /
    /// lock / batch 等 `invoke_sync`，不能卡在 remoting 读线程上）。
    fn reset_offset(
        self: Arc<Self>,
        topic: String,
        offset_table: Vec<(MessageQueue, i64)>,
    ) -> ConsumerFuture<()>;

    /// 221 处理：返回已消费位点表（Python `consumer.get_consumer_status(topic)`）。
    /// `None` = 请求头里没有 topic，与"显式传空串"区分开（Python 用 truthy 判断，
    /// 只有非空 topic 才过滤）。
    fn get_consumer_status(&self, topic: Option<&str>) -> Vec<(MessageQueue, i64)>;

    /// 307 处理：运行信息（Python `consumer.consumer_running_info()`）。
    fn consumer_running_info(&self) -> ConsumerRunningInfo;

    /// 309 处理：本地真实消费一条消息（Python
    /// `consumer.consume_message_directly(msg, broker_name)`）。
    fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<String>,
    ) -> Result<ConsumeMessageDirectlyResult>;

    /// Java `MQConsumerInner#persistConsumerOffset`：把缓冲位点刷到 broker。
    /// （Python 的位点持久化循环在 `consumer.py` 自己内部；实例侧收敛见模块头差异 4。）
    fn persist_consumer_offset(self: Arc<Self>) -> ConsumerFuture<()>;
}

/// `RegisteredConsumer` 异步行为的返回类型（无 async-trait 依赖的装箱 future）。
pub type ConsumerFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>;

/// 消息轨迹派发器的实例侧接缝（对应 Java producer/consumer 持有的
/// `TraceDispatcher` / Python `client/trace_dispatcher.py`）。
///
/// ⚠ **`mq_client.py` 全文没有任何 `self.trace_dispatcher` 调用点**（已逐行核对，
/// grep 无命中），Python 把 dispatcher 直接挂在 producer/consumer 上。这里保留
/// 该 trait 只为对齐 Java 的注入形状：由调用方 `Arc` 传入、实例只存不叫，
/// 供后续 producer/consumer 移植层通过 [`MQClientInstance::trace_dispatcher`] 取用。
/// 方法集只镜像 Python dispatcher 真正存在的生命周期入口
/// （`trace_dispatcher.py` 的 `start` / `shutdown`；append/flush 属于上层，不走这里）。
pub trait TraceDispatcher: Send + Sync {
    /// Python `TraceDispatcher.start(name_srv_addr)`。
    fn start(&self, name_server_addr: &str) -> Result<()>;

    /// Python `TraceDispatcher.shutdown()`。
    fn shutdown(&self);
}

// ================================================================ TopicPublishInfo

/// 发布路由的派生视图（对应 Java
/// `org.apache.rocketmq.client.impl.producer.TopicPublishInfo`、
/// Python `mq_client.TopicPublishInfo`）。
///
/// Python 用 `threading.Lock` 保护轮询游标 `_index`；这里锁覆盖队列列表 + 游标 +
/// 路由字段（更新路由时整批替换，读侧一次性快照，语义等价）。
#[derive(Debug, Default)]
pub struct TopicPublishInfo {
    state: Mutex<PublishState>,
}

#[derive(Debug, Default)]
struct PublishState {
    order_topic: bool,
    msg_queue_list: Vec<MessageQueue>,
    topic_route_data: Option<TopicRouteData>,
    /// Python `_index`（int，可为负无所谓，取模前用 rem_euclid 保证非负）。
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

    /// Python `reset_index`。
    pub fn reset_index(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).index = 0;
    }

    /// 轮询选队列（对应 Java `TopicPublishInfo.selectOneMessageQueue`、
    /// Python `select_one_message_queue(*filters)`）。
    ///
    /// `filters` 为空 ⇒ 无条件轮询，**永不返回 `Ok(None)`**；非空 ⇒ 从当前游标起
    /// 最多试 `n` 个队列，全被过滤掉才 `Ok(None)`（游标照样每步推进，Java 同）。
    /// 队列为空 ⇒ `Err`（Python `MQClientException("no message queue for publish info")`）。
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

    /// `order_topic` 读（Python 直接读属性）。
    pub fn order_topic(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).order_topic
    }

    /// `msg_queue_list` 快照。
    pub fn msg_queue_list(&self) -> Vec<MessageQueue> {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).msg_queue_list.clone()
    }

    /// `topic_route_data` 快照。
    pub fn topic_route_data(&self) -> Option<TopicRouteData> {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).topic_route_data.clone()
    }

    /// Python `to_dict`：`{"orderTopic":..., "messageQueueList":[{topic,brokerName,queueId}]}`。
    /// 注意 Python 的 `messageQueueList` **不含路由本身**，只列队列。
    pub fn to_dict(&self) -> serde_json::Value {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        serde_json::Value::Object(serde_json::Map::from_iter(vec![
            ("orderTopic".to_string(), serde_json::Value::Bool(state.order_topic)),
            (
                "messageQueueList".to_string(),
                serde_json::Value::Array(
                    state
                        .msg_queue_list
                        .iter()
                        .map(|q| {
                            serde_json::Value::Object(serde_json::Map::from_iter(vec![
                                ("topic".to_string(), serde_json::Value::String(q.topic.clone())),
                                (
                                    "brokerName".to_string(),
                                    serde_json::Value::String(q.broker_name.clone()),
                                ),
                                (
                                    "queueId".to_string(),
                                    serde_json::Value::from(q.queue_id),
                                ),
                            ]))
                        })
                        .collect(),
                ),
            ),
        ]))
    }

    /// 路由落库时整批刷新（对应 Python `update_topic_route_info_from_name_server`
    /// 末尾对 `publish.order_topic / topic_route_data / msg_queue_list` 的三连赋值）。
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

/// Python `route.get_all_message_queue(topic)`：按 queueDatas × writeQueueNums 组装。
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

/// 发送入口的消息包装（对应 Python 的鸭子类型：`send_message(msg)` 既收
/// `Message` 也收 `MessageBatch`；`MessageBatch` 继承 `Message`，
/// `isinstance(msg, MessageBatch)` 决定 batch 标志 / 是否补 UNIQ_KEY）。
#[derive(Debug)]
pub enum PublishMessage<'a> {
    /// 普通消息（Python `Message`）。
    Single(&'a mut Message),
    /// 批量消息（Python `MessageBatch`；body 已在 `generate_from_list` 时编码好）。
    Batch(&'a mut MessageBatch),
}

impl<'a> PublishMessage<'a> {
    /// 外层 `Message` 视图：Python 里 `MessageBatch` 本身就是一个 `Message`，
    /// Rust 的批量消息把外层字段收在 `batch.message` 里，语义一致。
    pub fn as_message(&self) -> &Message {
        match self {
            PublishMessage::Single(m) => m,
            PublishMessage::Batch(b) => &b.message,
        }
    }

    /// 外层 `Message` 的可变视图：发送内核里的 traceparent 注入
    /// （Python `_send_with_hooks` 的 `inject_trace_context(msg)`）要就地改属性。
    pub fn as_message_mut(&mut self) -> &mut Message {
        match self {
            PublishMessage::Single(m) => m,
            PublishMessage::Batch(b) => &mut b.message,
        }
    }

    /// Python `isinstance(msg, MessageBatch)`。
    pub fn is_batch(&self) -> bool {
        matches!(self, PublishMessage::Batch(_))
    }
}

// ================================================================ 配置

/// `MQClientInstance` 构造参数（Python `__init__` 的关键字参数 + 模块内部常量）。
#[derive(Clone)]
pub struct MQClientInstanceConfig {
    /// Python `connect_timeout_millis`（默认 3000）。
    pub connect_timeout_millis: i64,
    /// Python `invoke_timeout_millis`（默认 15000）。
    pub invoke_timeout_millis: i32,
    /// Python `tls_enable`：`None` 走环境变量 `ROCKETMQ_TLS_ENABLE`（与
    /// [`RemotingClientConfig::default`] 同口径），`Some(v)` 强制。
    pub tls_enable: Option<bool>,
    /// 路由周期刷新（Python `_route_refresh_loop` 的 30s）。
    pub route_refresh_interval_millis: u64,
    /// 动态 namesrv 首延迟 / 周期（Python `_namesrv_refresh_loop`：10s / 120s）。
    pub namesrv_refresh_initial_delay_millis: u64,
    pub namesrv_refresh_interval_millis: u64,
    /// 线程弹性巡检（Python `_adjust_thread_pool_loop`：60s / 60s）。
    pub adjust_pool_initial_delay_millis: u64,
    pub adjust_pool_interval_millis: u64,
    /// Java `sendHeartbeatToAllBroker` 周期任务（initialDelay 2s，
    /// `heartbeatBrokerInterval` 默认 30s）。Python 无实例级心跳循环（见模块头差异 4）。
    pub heartbeat_initial_delay_millis: u64,
    pub heartbeat_interval_millis: u64,
    /// Java `persistAllConsumerOffset` 周期任务（10s / 30s）。
    pub persist_offset_initial_delay_millis: u64,
    pub persist_offset_interval_millis: u64,
    /// 实例级共享统计器（Python 内部构造 `ConsumerStatsManager()`；这里注入，
    /// `None` = 默认新建，行为与 Python 相同）。
    pub consumer_stats_manager: Option<Arc<ConsumerStatsManager>>,
    /// Java `MQClientAPIImpl#latencyFaultTolerance`：Python 实例层没有该字段
    /// （在 producer 的 `MQFaultStrategy` 里）。注入仅为共享 seam，本模块不读它。
    pub latency_fault_tolerance: Option<Arc<LatencyFaultToleranceImpl>>,
    /// 见 [`TraceDispatcher`]：本模块只存不调。
    pub trace_dispatcher: Option<Arc<dyn TraceDispatcher>>,
    /// 动态 name server（Python 内部 `DefaultTopAddressing()`；注入用，
    /// `None` = `DefaultTopAddressing::default()`，读环境变量）。
    pub top_addressing: Option<DefaultTopAddressing>,
}

impl Default for MQClientInstanceConfig {
    fn default() -> Self {
        MQClientInstanceConfig {
            connect_timeout_millis: 3000,
            invoke_timeout_millis: 15000,
            tls_enable: None,
            route_refresh_interval_millis: 30_000,
            namesrv_refresh_initial_delay_millis: 10_000,
            namesrv_refresh_interval_millis: 120_000,
            adjust_pool_initial_delay_millis: 60_000,
            adjust_pool_interval_millis: 60_000,
            heartbeat_initial_delay_millis: 2_000,
            heartbeat_interval_millis: 30_000,
            persist_offset_initial_delay_millis: 10_000,
            persist_offset_interval_millis: 30_000,
            consumer_stats_manager: None,
            latency_fault_tolerance: None,
            trace_dispatcher: None,
            top_addressing: None,
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
            .field("route_refresh_interval_millis", &self.route_refresh_interval_millis)
            .field(
                "namesrv_refresh_initial_delay_millis",
                &self.namesrv_refresh_initial_delay_millis,
            )
            .field("namesrv_refresh_interval_millis", &self.namesrv_refresh_interval_millis)
            .field(
                "adjust_pool_initial_delay_millis",
                &self.adjust_pool_initial_delay_millis,
            )
            .field("adjust_pool_interval_millis", &self.adjust_pool_interval_millis)
            .field("heartbeat_initial_delay_millis", &self.heartbeat_initial_delay_millis)
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
            .field("trace_dispatcher", &injected(self.trace_dispatcher.is_some()))
            .field("top_addressing", &injected(self.top_addressing.is_some()))
            .finish()
    }
}

// ================================================================ MQClientInstance

/// 路由两张表（Python `topic_route_table` + `topic_publish_info_table`，
/// 共用一把 `topic_route_lock`）。
#[derive(Default)]
struct RouteTables {
    topic_route_table: HashMap<String, TopicRouteData>,
    topic_publish_info_table: HashMap<String, Arc<TopicPublishInfo>>,
}

struct Inner {
    client_id: String,
    name_server_addrs: Mutex<Vec<String>>,
    remoting_client: RemotingClient,
    tables: Mutex<RouteTables>,
    /// Python `_topics_in_use`。
    topics_in_use: Mutex<HashSet<String>>,
    started: AtomicBool,
    /// Python `MQClientInstance._consumer_table`（Java `consumerTable`）。
    consumer_table: Mutex<HashMap<String, Arc<dyn RegisteredConsumer>>>,
    /// `fetch_and_apply` 要 `&mut`，放进 `tokio::sync::Mutex`。
    top_addressing: Arc<tokio::sync::Mutex<DefaultTopAddressing>>,
    consumer_stats_manager: Arc<ConsumerStatsManager>,
    latency_fault_tolerance: Option<Arc<LatencyFaultToleranceImpl>>,
    trace_dispatcher: Option<Arc<dyn TraceDispatcher>>,
    config: MQClientInstanceConfig,
    /// 后台任务的停止信号（true = 停）。
    stop: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// 收到过多少次 broker 的 `NOTIFY_CONSUMER_IDS_CHANGED(40)`。
    /// Java 只打一行 INFO（`ClientRemotingProcessor#notifyConsumerIdsChanged`），
    /// 这里额外计数是为了让"通知到底有没有被处理"在真机用例里可断言 ——
    /// 反向推送只有 broker 能发，测试没法从外部注入。
    consumer_ids_changed_count: AtomicUsize,
}

/// Python `MQClientInstance`（类级 `INSTANCE_MAP` 在 Rust 里是
/// [`MQClientInstance::INSTANCE_MAP`]，存弱引用）。
#[derive(Clone)]
pub struct MQClientInstance {
    inner: Arc<Inner>,
}

impl MQClientInstance {
    /// 进程级实例表（对应 Python `MQClientInstance.INSTANCE_MAP` /
    /// Java `MQClientInstance.instanceMap`）。构造即登记（Python `__init__` 最后
    /// 一行 `MQClientInstance.INSTANCE_MAP[client_id] = self`，同 key 直接覆盖）。
    fn instance_map() -> &'static Mutex<HashMap<String, Weak<Inner>>> {
        static MAP: std::sync::OnceLock<Mutex<HashMap<String, Weak<Inner>>>> =
            std::sync::OnceLock::new();
        MAP.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// Python `MQClientInstance(client_id, name_server_addrs)`（其余参数取默认）。
    pub fn new(client_id: &str, name_server_addrs: Vec<String>) -> MQClientInstance {
        MQClientInstance::with_config(client_id, name_server_addrs, MQClientInstanceConfig::default())
    }

    /// Python `__init__` 的完整注入版本（差异见模块头第 2 条）。
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
            top_addressing: Arc::new(tokio::sync::Mutex::new(
                config.top_addressing.clone().unwrap_or_default(),
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
        let this = MQClientInstance { inner: inner.clone() };
        // Python __init__：注册 326/220/221/307/309/40 六个实例级处理器
        // （Java MQClientAPIImpl 构造函数里的 clientRemotingProcessor）。
        // 40 也在这里：Java 是 `registerProcessor(NOTIFY_CONSUMER_IDS_CHANGED,
        // clientRemotingProcessor, null)`，属于**实例**而不是某个消费者 ——
        // 同 clientId 上的 lite / 拉模式 / 生产者连接都会收到 broker 的这条反向推送，
        // 交给消费者自己注册就会留下"没人处理"的告警。
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
            this.inner.remoting_client.register_processor(code, processor.clone());
        }
        MQClientInstance::instance_map()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(client_id.to_string(), Arc::downgrade(&inner));
        this
    }

    /// Java `MQClientInstance.createMQClientInstance`：同 clientId 已有存活实例时
    /// **复用**（Python 只写 INSTANCE_MAP 不读——复用工厂语义对齐 Java，比 Python 多）。
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

    /// `INSTANCE_MAP` 读侧（Python 没有该函数；供 admin/测试按 clientId 找回实例）。
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
        self.inner.name_server_addrs.lock().unwrap_or_else(|e| e.into_inner()).clone()
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

    /// Python `register_consumer`（Java `MQClientInstance#registerConsumer`；
    /// 不校验冲突，与 Python 的裸 dict 赋值一致）。
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

    /// Java `MQClientInstance#rebalanceImmediately`（一行 `rebalanceService.wakeup()`，
    /// 由共享的重平衡线程逐个 `tryRebalance`）。Rust 没有实例级重平衡线程，
    /// 改成对每个已注册消费者点一次名，让它叫醒自己那份循环。
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
        let body = match cmd.body() {
            Some(bytes) if !bytes.is_empty() => match ResetOffsetBody::decode(bytes) {
                Ok(body) => body,
                Err(e) => {
                    rmq_warn!("RESET_CONSUMER_CLIENT_OFFSET: bad body: {e}");
                    return;
                }
            },
            _ => ResetOffsetBody::default(),
        };
        let topic = header.topic.unwrap_or_default();
        let offset_table: Vec<(MessageQueue, i64)> = body
            .offset_table
            .into_iter()
            .map(|(k, v)| (MessageQueue::new(&k.topic, &k.broker_name, k.queue_id), v))
            .collect();
        // Python 丢后台线程（220 的 rebalance 会 invokeSync，不能卡在 remoting 读线程）；
        // 这里丢 tokio 任务，立即回 None（oneway）。
        let task = consumer.reset_offset(topic.clone(), offset_table);
        // 220 只可能在 remoting 读线程里到达，那里必有运行时；真取不到时
        // 宁愿丢任务也不能 panic（Python 此时只是起线程失败）。
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

    /// 对应 Java `ClientRemotingProcessor#notifyConsumerIdsChanged`：记一行 INFO
    /// 文案照抄（`receive broker's notification[<addr>], the consumer group: <g>
    /// changed, rebalance immediately`），然后 `rebalanceImmediately()`。
    /// Java 整段包在 `try/catch` 里且**返回 null**（不回包）—— 通知处理失败不能让
    /// broker 侧连接报错，这里同理：不抛、也不回响应。
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
                .map(|(mq, off)| (MessageQueueKey::new(&mq.topic, &mq.broker_name, mq.queue_id), off))
                .collect(),
            consumer_table: Vec::new(),
        };
        let mut resp = RemotingCommand::create_response(response_code::SUCCESS, None);
        resp.set_body(Some(body.encode()));
        resp
    }

    pub(crate) fn process_get_consumer_running_info(&self, cmd: &RemotingCommand) -> RemotingCommand {
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

    pub(crate) fn process_consume_message_directly(&self, cmd: &RemotingCommand) -> RemotingCommand {
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
        // Python `decode_message(cmd.body, check_crc=False)`（Rust 默认 options 的
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

    /// 326 处理：把应答投给等待中的 request()（Python `_process_reply_message`）。
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
                let correlation_id = msg.get_property(PROPERTY_CORRELATION_ID).map(|s| s.to_string());
                if request_future_holder()
                    .put_response(correlation_id.as_deref(), msg)
                    .is_none()
                {
                    // 查不到是正常情况（请求已超时 / 应答重复），Java 此处也是 warn
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

    /// Python `start()`：置位 started、启动统计采样、（可选）动态 namesrv 刷新，
    /// 然后拉起 4 个后台任务。Python 在动态取址拿不到地址时抛
    /// `MQClientException`，这里等价返回 `Err`。
    pub async fn start(&self) -> Result<()> {
        // Java `MQClientInstance#start` 的 `started.compareAndSet(false, true)`：同一
        // clientId 的多个 producer/consumer 都会调 start()，后台循环只能有一份。
        if self
            .inner
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        // Python 每次 start 前 `clear()` 那些 stop 事件：shutdown 之后再 start 要真能跑；
        // 传输同理（Java 在 `MQClientInstance#start` 里调 `mQClientAPIExt.start()`）。
        let _ = self.inner.stop.send(false);
        self.inner.remoting_client.start();
        if let Err(e) = self.inner.consumer_stats_manager.start() {
            self.inner.started.store(false, Ordering::Release);
            return Err(e);
        }
        // 动态 name server（Java MQClientInstance.start:344-348）：**当且仅当**没配置
        // 地址且 top_addressing.ws_addr 非空时，先 fetch 一次；取不到就直接报错。
        let dynamic_ns = {
            let addrs = self.name_server_addrs();
            let ta = self.inner.top_addressing.lock().await;
            addrs.is_empty() && !ta.ws_addr().is_empty()
        };
        if dynamic_ns {
            self.fetch_name_server_addr().await?;
            if self.name_server_addrs().is_empty() {
                let ws_addr = self.inner.top_addressing.lock().await.ws_addr().to_string();
                bail!(
                    "name server address is not set and address server ({ws_addr}) returned none"
                );
            }
            // 周期刷新（Java scheduleAtFixedRate(fetchNameServerAddr, 10s, 2min)）
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
        // Python `_route_refresh_loop`：首个 10ms、周期 route_refresh_interval
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
        // Java sendHeartbeatToAllBrokerWithLock / persistAllConsumerOffset
        // （Python 这两个循环不在实例里，见模块头差异 4）
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

    /// Python `shutdown()`：先置 `started=false` 再 set 三个 stop 事件，
    /// 然后停统计、关 remoting。Rust 版额外 abort 后台任务并清句柄，
    /// 保证无 broker 场景下确定收敛（模块头差异 5）。
    pub fn shutdown(&self) {
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
    }

    fn push_task(&self, handle: JoinHandle<()>) {
        self.inner.tasks.lock().unwrap_or_else(|e| e.into_inner()).push(handle);
    }

    /// 通用周期任务：initial 延迟 → 循环（sleep ∥ stop 信号），每轮先查 started。
    fn spawn_periodic<F, Fut>(&self, _name: &str, work: F, initial_delay_millis: u64, period_millis: u64)
    where
        F: Fn(MQClientInstance) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let me = self.clone();
        let handle = tokio::spawn(async move {
            if wait_or_stop(&me.inner.stop, initial_delay_millis).await {
                return;
            }
            loop {
                if wait_or_stop(&me.inner.stop, period_millis).await {
                    return;
                }
                if !me.is_started() {
                    return;
                }
                work(me.clone()).await;
            }
        });
        self.push_task(handle);
    }

    async fn route_refresh_loop(&self) {
        if wait_or_stop(&self.inner.stop, 10).await {
            return;
        }
        loop {
            if wait_or_stop(&self.inner.stop, self.inner.config.route_refresh_interval_millis)
                .await
            {
                return;
            }
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
                if let Err(e) =
                    self.update_topic_route_info_from_name_server(&topic, 5000, false).await
                {
                    rmq_debug!("route refresh failed for {topic}: {e}");
                }
            }
        }
    }

    // ---------------- 动态 namesrv / 弹性巡检 ----------------

    /// 对应 Java `MQClientAPIImpl.fetchNameServerAddr`：地址**变化才应用**。
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
        let addrs: Vec<String> = text.split(';').map(|a| a.trim().to_string()).filter(|a| !a.is_empty()).collect();
        self.update_name_server_address_list(&addrs);
    }

    /// Python `update_name_server_address_list`：空列表不改。
    pub fn update_name_server_address_list(&self, addrs: &[String]) {
        if addrs.is_empty() {
            return;
        }
        *self.inner.name_server_addrs.lock().unwrap_or_else(|e| e.into_inner()) = addrs.to_vec();
    }

    /// Python `adjust_thread_pool`（Java `MQClientInstance#adjustThreadPool`）：
    /// 逐实例吞异常（catch (Exception ignored)），一个消费者失败不影响其它。
    pub fn adjust_thread_pool(&self) {
        for consumer in self.consumers_snapshot() {
            consumer.adjust_thread_pool();
        }
    }

    /// Python `register_topic_in_use`：登记需要后台周期刷新路由的 topic。
    pub fn register_topic_in_use(&self, topic: &str) {
        if !topic.is_empty() {
            self.inner.topics_in_use.lock().unwrap_or_else(|e| e.into_inner()).insert(topic.to_string());
        }
    }
}

// ================================================================ 实例级 RPC
//
// 下面三个 `impl MQClientInstance` 块对应 Java `MQClientAPIImpl` 的调用面
// （Python 把它合并进了 `MQClientInstance`，见模块头差异 3）。

impl MQClientInstance {
    // ---------------- 底层 invoke ----------------

    /// Python `_invoke_sync`：`remoting_client.invoke_sync` 的薄封装。
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

    /// Python `_check_response`：非 SUCCESS 一律抛 `MQBrokerException`
    /// （remark 为空时 Python 用 `""`）。
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

    /// Python `update_topic_route_info_from_name_server`：拉取并落库 topic 路由。
    ///
    /// `is_default` 对应 Java `MQClientInstance#updateTopicRouteInfoFromNameServer(topic,
    /// isDefault, defaultMQProducer)`：**只有生产者**在真实路由拉不到时才回退到默认 topic
    /// （TBW102）来为新 topic 合成发布信息（见 Java `DefaultMQProducerImpl:905`）；
    /// 消费者路径**绝不允许**兜底——否则 `%RETRY%group` 这类尚未由 broker 创建的主题会被
    /// 合成出一组假队列，rebalance 视图不一致。
    pub async fn update_topic_route_info_from_name_server(
        &self,
        topic: &str,
        timeout_millis: i64,
        is_default: bool,
    ) -> Result<bool> {
        let addrs = self.name_server_addrs();
        if addrs.is_empty() {
            bail!("name server address list is empty");
        }
        let mut route = self
            .fetch_topic_route_from_namesrv(topic, timeout_millis, &addrs)
            .await?;
        if route.is_none() && is_default && topic != MixAll::DEFAULT_TOPIC {
            // RocketMQ 5.x nameServer 不为未知 topic 合成默认路由（回 TOPIC_NOT_EXIST），
            // **生产者**要像 Java 客户端那样回退到默认 topic 构造发布信息。
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
            tables.topic_route_table.insert(topic.to_string(), route.clone());
            tables
                .topic_publish_info_table
                .entry(topic.to_string())
                .or_insert_with(|| Arc::new(TopicPublishInfo::new()))
                .clone()
        };
        // Python 在 `topic_route_lock` 内直接改 publish 的三个字段；Rust 里
        // `TopicPublishInfo` 自带细粒度锁，先出表锁再整体刷新（顺序等价，且避免嵌套锁）。
        publish.update_from_route(&route, topic);
        Ok(true)
    }

    /// Python 内部闭包 `_fetch`：逐个 namesrv 试；**拿到响应但不可用就 break**
    /// （不再试后续 namesrv），只有传输/解码异常才继续下一个；
    /// 循环结束后「最后一个异常」若不是 broker 业务错就原样抛出（Python 逐字如此）。
    ///
    /// 与 Python 的唯一差别：Python 复用一个 `request` 对象，Rust 的 remoting 层按
    /// `opaque` 配对响应，故每次尝试新建请求（每次 `opaque` 递增，语义不变）。
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
            match self.invoke_sync(ns_addr, &mut request, timeout_millis).await {
                Ok(response) => {
                    if response.code != response_code::SUCCESS {
                        break;
                    }
                    match response.body() {
                        // Python 的 decode 在 try 里：解码失败算 last_exc 并继续下一个 namesrv
                        Some(body) if !body.is_empty() => match TopicRouteData::decode(body) {
                            Ok(data) => return Ok(Some(data)),
                            Err(e) => last_exc = Some(e),
                        },
                        // SUCCESS 但没 body：Python 同样 break（不再试后续 namesrv）
                        _ => break,
                    }
                }
                Err(e) => last_exc = Some(e),
            }
        }
        if let Some(e) = last_exc {
            // Python: `if last_exc is not None and not isinstance(last_exc, MQBrokerException)`
            if !matches!(e, Error::Broker { .. }) {
                return Err(e);
            }
        }
        Ok(None)
    }

    /// Python `get_topic_publish_info`：缓存可用直接返回，否则拉一次路由；
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

    /// Python `get_topic_route_data`：命中缓存即返回；否则拉一次路由（**吞掉异常**）后再读。
    pub async fn get_topic_route_data(&self, topic: &str) -> Option<TopicRouteData> {
        if let Some(route) = self.route_of(topic) {
            return Some(route);
        }
        if let Err(e) = self.update_topic_route_info_from_name_server(topic, 5000, false).await {
            // Python `except Exception: pass`
            rmq_debug!("get_topic_route_data refresh failed for {topic}: {e}");
        }
        self.route_of(topic)
    }

    /// 只读缓存路由（不触发 RPC）；Python 直接读 `self.topic_route_table`。
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

    /// Python `find_broker_addr_in_route`（staticmethod）。
    pub fn find_broker_addr_in_route(route: &TopicRouteData, broker_name: &str) -> Option<String> {
        route
            .get_broker_datas()
            .iter()
            .find(|bd| bd.broker_name == broker_name)
            .and_then(|bd| bd.select_broker_addr())
    }

    // ---------------- 消息发送 ----------------

    /// Python `send_message`：按路由解析 broker 地址后同步发送。
    pub async fn send_message(
        &self,
        producer_group: &str,
        msg: &mut PublishMessage<'_>,
        mq: &MessageQueue,
        timeout_millis: i64,
        sys_flag: i32,
    ) -> Result<SendResult> {
        // Python 先 `... if self.get_topic_route_data(mq.topic) else None` 再取一次，
        // 两次调用读的是同一份缓存（第二次必然命中），语义等价于「取一次路由」。
        let route = self
            .get_topic_route_data(&mq.topic)
            .await
            .ok_or_else(|| Error::client(format!("No route info of this topic: {}", mq.topic)))?;
        let addr = Self::find_broker_addr_in_route(&route, &mq.broker_name).ok_or_else(|| {
            Error::client(format!(
                "Broker {} not found in route of topic {}",
                mq.broker_name, mq.topic
            ))
        })?;
        self.send_message_to_addr(producer_group, msg, mq, &addr, timeout_millis, sys_flag)
            .await
    }

    /// Python `send_message_to_addr`。
    pub async fn send_message_to_addr(
        &self,
        producer_group: &str,
        msg: &mut PublishMessage<'_>,
        mq: &MessageQueue,
        addr: &str,
        timeout_millis: i64,
        sys_flag: i32,
    ) -> Result<SendResult> {
        let mut request = Self::build_send_request(producer_group, msg, mq, sys_flag);
        let response = self.invoke_sync(addr, &mut request, timeout_millis).await?;
        Self::parse_send_response(&response, msg.as_message(), mq)
    }

    /// Python `send_message_oneway`：`mark_oneway_rpc` 后 fire-and-forget。
    ///
    /// Python 的 `timeout_millis` 形参在本方法里从未被使用（oneway 不等响应），
    /// 这里省略该形参（唯一一处签名收窄）。
    pub async fn send_message_oneway(
        &self,
        producer_group: &str,
        msg: &mut PublishMessage<'_>,
        mq: &MessageQueue,
        addr: &str,
        sys_flag: i32,
    ) -> Result<()> {
        let mut request = Self::build_send_request(producer_group, msg, mq, sys_flag);
        request.mark_oneway_rpc();
        self.inner.remoting_client.invoke_oneway(addr, &mut request).await
    }

    /// Python `_build_send_request`（V2 短字段头，`sendSmartMsg` 恒 true）。
    ///
    /// 对齐 Java `DefaultMQProducerImpl#sendKernelImpl:932-935`：非批量消息在
    /// **发请求之前**补客户端唯一 ID（UNIQ_KEY）；批量消息的 ID 在
    /// `MessageBatch.generateFromList` 时已逐条写好，不覆盖。
    pub(crate) fn build_send_request(
        producer_group: &str,
        msg: &mut PublishMessage<'_>,
        mq: &MessageQueue,
        sys_flag: i32,
    ) -> RemotingCommand {
        if !msg.is_batch() {
            set_uniq_id(msg.as_message_mut());
        }
        let outer = msg.as_message();
        let header = SendMessageRequestHeaderV2 {
            producer_group: Some(producer_group.to_string()),
            topic: Some(outer.topic.clone()),
            default_topic: Some(MixAll::DEFAULT_TOPIC.to_string()),
            default_topic_queue_nums: Some(MixAll::DEFAULT_TOPIC_QUEUE_NUMS),
            queue_id: Some(mq.queue_id),
            sys_flag: Some(sys_flag),
            born_timestamp: Some(current_time_millis()),
            flag: Some(outer.flag),
            properties: Some(message_properties_2_string(&outer.properties)),
            reconsume_times: Some(0),
            unit_mode: Some(false),
            // Java `sendKernelImpl:1007-1018` 只在「发往 %RETRY% 且消息带
            // MAX_RECONSUME_TIMES 属性」时才设这个字段，平时留 null。这里必须留
            // `None`：broker 在 `version >= V3_4_9` 后无条件采纳请求里的值
            // （`SendMessageProcessor:196-199`），固定发 0 会让重试消息直接进 `%DLQ%`。
            max_reconsume_times: None,
            batch: Some(msg.is_batch()),
            broker_name: None,
        };
        // Request-Reply：`msgType == "reply"` 的应答消息走 SEND_REPLY_MESSAGE_V2(325)：
        // broker 只在 324/325 上注册了 ReplyMessageProcessor（它负责按 REPLY_TO_CLIENT
        // 把应答推回请求方）。对齐 Java `MQClientAPIImpl#sendMessage:550-558`。
        let code = if is_reply_message(outer) {
            request_code::SEND_REPLY_MESSAGE_V2
        } else {
            request_code::SEND_MESSAGE_V2
        };
        let mut request = RemotingCommand::create_request_command(
            code,
            Some(Box::new(header)),
        );
        request.set_body(Some(Self::encode_body(outer)));
        request
    }

    /// Python `_encode_body`（staticmethod）：普通消息是原始 body；
    /// 批量消息的 body 在构造时已由 `encode_messages` 编好。
    fn encode_body(msg: &Message) -> Vec<u8> {
        msg.body.clone().unwrap_or_default()
    }

    /// Python `_parse_send_response`：状态码映射 + Java `processSendResponse:794-806` 的
    /// msgId / offsetMsgId / regionId / traceOn 口径。
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
        // msgId = 客户端唯一 ID（UNIQ_KEY），缺省回落响应头的 msgId
        let uniq_id = get_uniq_id(msg).filter(|s| !s.is_empty());
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
            // Python: `str(ext.get(TRACE_ON)) != "false"` —— 缺字段/空串都算开启
            trace_on: match response.get_ext_field(PROPERTY_TRACE_SWITCH) {
                Some(value) => value != "false",
                None => true,
            },
        })
    }

    // ---------------- 定时消息撤回（370） ----------------

    /// Java `MQClientAPIImpl#recallMessage`：`RECALL_MESSAGE`(370) 同步往返，
    /// 只有 SUCCESS 才取响应头的 `msgId`，其余码抛 broker 异常。
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
            .ok_or_else(|| Error::client(format!("recall message response has no msgId, addr {addr}")))
    }

    // ---------------- 拉取 ----------------

    /// Python `pull_message`：14 个位置参数逐一对应。
    ///
    /// 响应码映射照 Java `MQClientAPIImpl#pullMessage`：
    /// SUCCESS⇒FOUND / PULL_NOT_FOUND⇒NO_NEW_MSG / PULL_OFFSET_MOVED⇒OFFSET_ILLEGAL /
    /// PULL_RETRY_IMMEDIATELY⇒NO_MATCHED_MSG / 其余抛 `MQBrokerException`。
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
    ) -> Result<PullResult> {
        let addr = match addr {
            Some(a) => a.to_string(),
            None => {
                let route = self
                    .get_topic_route_data(&mq.topic)
                    .await
                    .ok_or_else(|| Error::client(format!("No route info of this topic: {}", mq.topic)))?;
                Self::find_broker_addr_in_route(&route, &mq.broker_name).ok_or_else(|| {
                    Error::client(format!(
                        "Broker {} not found in route of topic {}",
                        mq.broker_name, mq.topic
                    ))
                })?
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
            subscription: Some(subscription.to_string()),
            sub_version: Some(sub_version),
            expression_type: Some(expression_type.to_string()),
            max_msg_bytes: Some(max_msg_bytes),
            request_source: Some(request_source),
            proxy_froward_client_id: None,
        };
        let mut request =
            RemotingCommand::create_request_command(request_code::PULL_MESSAGE, Some(Box::new(header)));
        let response = self.invoke_sync(&addr, &mut request, timeout_millis).await?;
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
        // Python 无条件盖 brokerName/queueId
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
        })
    }
}

// ================================================================ POP / ACK / 不可见时间

impl MQClientInstance {
    /// Python `pop_message` 的 addr / brokerName 解析段：任一缺失就查路由，
    /// 取路由里的**第一台** broker。
    async fn pop_target(
        &self,
        topic: &str,
        broker_name: Option<&str>,
        addr: Option<&str>,
    ) -> Result<(String, String)> {
        let broker_name = broker_name.filter(|s| !s.is_empty());
        if let (Some(a), Some(b)) = (addr, broker_name) {
            return Ok((a.to_string(), b.to_string()));
        }
        let route = self
            .get_topic_route_data(topic)
            .await
            .ok_or_else(|| Error::client(format!("No route info of this topic: {topic}")))?;
        let brokers = route.get_broker_datas();
        let Some(bd) = brokers.first() else {
            bail!("No broker in route of topic: {topic}");
        };
        let broker_name = broker_name.unwrap_or(&bd.broker_name).to_string();
        let addr = match addr {
            Some(a) => a.to_string(),
            None => bd
                .select_broker_addr()
                .ok_or_else(|| Error::client(format!("No available broker addr for topic: {topic}")))?,
        };
        Ok((addr, broker_name))
    }

    /// Python `pop_message`（POP_MESSAGE = 200050）。
    ///
    /// 与 pull 的语义差别（Python docstring）：**不需要提交位点**，消费完成用
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
        let mut request =
            RemotingCommand::create_request_command(request_code::POP_MESSAGE, Some(Box::new(header)));
        let response = self.invoke_sync(&addr, &mut request, timeout_millis).await?;
        // 响应码映射照 Java `MQClientAPIImpl#processPopResponse`
        let status = match response.code {
            response_code::SUCCESS => PopStatus::Found,
            response_code::POLLING_FULL => PopStatus::PollingFull,
            response_code::POLLING_TIMEOUT | response_code::PULL_NOT_FOUND => PopStatus::PollingNotFound,
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
            // **必须在改写 topic 之前**反构 POP_CK —— retryFlag 是从消息**原始** topic
            // 推出来的（broker 可能改写 topic，见 Java buildQueueOffsetSortedMap 注释）
            Self::stamp_pop_ck(&mut found, &broker_name, &resp_header)?;
        }
        // Java processPopResponse 收尾：统一盖 brokerName，并把 topic 还原成请求的 topic
        // （不带命名空间），这样消费方不用关心 retry topic。
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

    /// Python `_pop_queue_map_key`（staticmethod，Python 多传一个 `extra` 模块参数，
    /// Rust 直接用模块路径，故省掉）。
    ///
    /// Java `getStartOffsetInfoMapKey(topic, popCk, key)` 的规则：消息**已带** `POP_CK`
    /// （说明是从 retry topic 弹回来的）时 retryFlag 取自 POP_CK 第 5 段，
    /// 否则由消息 topic 推断 —— 因为 broker 可能改写 topic。
    pub fn pop_queue_map_key(msg: &MessageExt) -> Result<String> {
        let ck = msg.properties.get(PROPERTY_POP_CK).filter(|ck| !ck.is_empty());
        match ck {
            Some(ck) => {
                let segments = extra_info::split(ck)?;
                Ok(format!("{}@{}", extra_info::get_retry(&segments)?, msg.queue_id))
            }
            None => Ok(extra_info::get_start_offset_info_map_key(
                &msg.topic,
                i64::from(msg.queue_id),
            )),
        }
    }

    /// Python `_stamp_pop_ck`：给 POP 出来的消息反构 `POP_CK` 与 `1ST_POP_TIME`。
    ///
    /// **这是 POP 最容易踩的坑**：普通 topic 直连 POP 时 broker **不写** `POP_CK`
    /// （只在 retry topic 重编码路径才写），必须由客户端用响应头的
    /// `startOffsetInfo` / `msgOffsetInfo` 反构 —— 没有它就无法发 ACK。
    /// 逐条对齐 Java `MQClientAPIImpl#processPopResponse`。
    ///
    /// Python 形参里的 `self` 未被使用（它只调 staticmethod），这里是关联函数。
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
            // Java 的 startOffsetInfo == null 分支：用消息自身 queueOffset 当
            // ckQueueOffset 拼 7 段，再手工补一段凑成 8 段。
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
        // Java 先按队列收集 queueOffset 并排序，再用 indexOf 求下标去取
        // msgOffsetInfo 里对应的 msgQueueOffset。
        let mut sorted_offsets: HashMap<String, Vec<i64>> = HashMap::new();
        for msg in found.iter() {
            let key = Self::pop_queue_map_key(msg)?;
            sorted_offsets.entry(key).or_default().push(msg.queue_offset);
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

    /// Java 用 `computeIfAbsent`：只在缺失时补 `1ST_POP_TIME`。
    fn stamp_first_pop_time(found: &mut [MessageExt], pop_time: i64) -> Result<()> {
        for msg in found.iter_mut() {
            if !msg.properties.contains_key(PROPERTY_FIRST_POP_TIME) {
                msg.properties
                    .insert(PROPERTY_FIRST_POP_TIME, pop_time.to_string());
            }
        }
        Ok(())
    }

    /// Python `ack_message`（ACK_MESSAGE = 200051）：确认一条 POP 消息。
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
        // 形参名与模块同名（Python 也这么写：`import extra_info as extra`），局部起别名
        use crate::remoting::protocol::extra_info as extra;
        let broker_name = match broker_name {
            Some(name) => name.to_string(),
            None => extra::get_broker_name(&extra::split(extra_info)?)?,
        };
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.addr_for(topic, Some(&broker_name)).await?,
        };
        let header = AckMessageRequestHeader {
            consumer_group: Some(consumer_group.to_string()),
            topic: Some(topic.to_string()),
            queue_id: Some(queue_id),
            extra_info: Some(extra_info.to_string()),
            offset: Some(offset),
            lite_topic: None,
        };
        let mut request =
            RemotingCommand::create_request_command(request_code::ACK_MESSAGE, Some(Box::new(header)));
        let response = self.invoke_sync(&addr, &mut request, timeout_millis).await?;
        Ok(response.code)
    }

    /// Python `change_invisible_time`（CHANGE_MESSAGE_INVISIBLETIME = 200053）。
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
            None => self.addr_for(topic, Some(&broker_name)).await?,
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
        let response = self.invoke_sync(&addr, &mut request, timeout_millis).await?;
        let mut resp_header = ChangeInvisibleTimeResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        let pop_time = resp_header.pop_time.unwrap_or(0);
        let new_invisible_time = resp_header.invisible_time.unwrap_or(0);
        let revive_qid = resp_header.revive_qid.unwrap_or(0);
        let new_extra = if response.code == response_code::SUCCESS {
            // 与 Java `changeInvisibleTimeAsync` 一致：7 段 build（ckQueueOffset 用本次的
            // offset）再补一段凑成 8 段。
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

    /// Python `_addr_for`：按 topic（可选再按 brokerName）解析 broker 地址。
    /// brokerName 为空串时与 Python 一样退化为「路由里第一台 broker」。
    pub(crate) async fn addr_for(&self, topic: &str, broker_name: Option<&str>) -> Result<String> {
        let route = self
            .get_topic_route_data(topic)
            .await
            .ok_or_else(|| Error::client(format!("No route info of this topic: {topic}")))?;
        if let Some(broker_name) = broker_name.filter(|name| !name.is_empty()) {
            return Self::find_broker_addr_in_route(&route, broker_name).ok_or_else(|| {
                Error::client(format!("Broker {broker_name} not found in route of topic {topic}"))
            });
        }
        let Some(bd) = route.get_broker_datas().first() else {
            bail!("No broker in route of topic: {topic}");
        };
        bd.select_broker_addr()
            .ok_or_else(|| Error::client(format!("No available broker addr for topic: {topic}")))
    }

    /// Python `_broker_addr(mq)`：按 `mq.broker_name` 在该 topic 路由里查地址。
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

    /// Python `_broker_addr_for_topic`：等价于 `_addr_for(topic)`（Python 里两处代码相同）。
    pub(crate) async fn broker_addr_for_topic(&self, topic: &str) -> Result<String> {
        self.addr_for(topic, None).await
    }

    /// Python `broker_addr_of`：在**所有**已缓存路由里找该 brokerName 的地址。
    pub fn broker_addr_of(&self, broker_name: &str) -> Option<String> {
        self.cached_routes()
            .into_iter()
            .find_map(|route| Self::find_broker_addr_in_route(&route, broker_name))
    }

    /// Python `get_route_of_all_brokers`：所有已缓存路由里的 broker 地址（去重）。
    ///
    /// Python 按 dict 插入序返回；Rust 的 HashMap 无序，这里按地址字典序排序，
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

    /// 已缓存路由快照（Python 直接遍历 `topic_route_table.values()`）。
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

    // ---------------- Offset 查询 / 更新 ----------------

    /// Python `query_consumer_offset`：`QUERY_NOT_FOUND(22)` ⇒ `None`（Python 直接 return，
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
            None => self.broker_addr(mq).await?,
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
        let response = self.invoke_sync(&addr, &mut request, timeout_millis).await?;
        if response.code == response_code::QUERY_NOT_FOUND {
            return Ok(None);
        }
        Self::check_response(&response)?;
        let mut resp_header = QueryConsumerOffsetResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        Ok(resp_header.offset)
    }

    /// Python `update_consumer_offset`。
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
        let response = self.invoke_sync(&addr, &mut request, timeout_millis).await?;
        Self::check_response(&response)?;
        Ok(())
    }

    /// Python `get_max_offset`。
    pub async fn get_max_offset(
        &self,
        mq: &MessageQueue,
        timeout_millis: i64,
        addr: Option<&str>,
    ) -> Result<i64> {
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.broker_addr(mq).await?,
        };
        let header = GetMaxOffsetRequestHeader {
            topic: Some(mq.topic.clone()),
            queue_id: Some(mq.queue_id),
        };
        let mut request =
            RemotingCommand::create_request_command(request_code::GET_MAX_OFFSET, Some(Box::new(header)));
        let response = self.invoke_sync(&addr, &mut request, timeout_millis).await?;
        Self::check_response(&response)?;
        let mut resp_header = GetMaxOffsetResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        Ok(resp_header.offset.unwrap_or(0))
    }

    /// Python `get_min_offset`。
    pub async fn get_min_offset(
        &self,
        mq: &MessageQueue,
        timeout_millis: i64,
        addr: Option<&str>,
    ) -> Result<i64> {
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.broker_addr(mq).await?,
        };
        let header = GetMinOffsetRequestHeader {
            topic: Some(mq.topic.clone()),
            queue_id: Some(mq.queue_id),
        };
        let mut request =
            RemotingCommand::create_request_command(request_code::GET_MIN_OFFSET, Some(Box::new(header)));
        let response = self.invoke_sync(&addr, &mut request, timeout_millis).await?;
        Self::check_response(&response)?;
        let mut resp_header = GetMinOffsetResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        Ok(resp_header.offset.unwrap_or(0))
    }

    /// Python `search_offset_by_timestamp`。
    pub async fn search_offset_by_timestamp(
        &self,
        mq: &MessageQueue,
        timestamp: i64,
        timeout_millis: i64,
        addr: Option<&str>,
    ) -> Result<i64> {
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.broker_addr(mq).await?,
        };
        let header = SearchOffsetRequestHeader {
            topic: Some(mq.topic.clone()),
            queue_id: Some(mq.queue_id),
            timestamp: Some(timestamp),
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::SEARCH_OFFSET_BY_TIMESTAMP,
            Some(Box::new(header)),
        );
        let response = self.invoke_sync(&addr, &mut request, timeout_millis).await?;
        Self::check_response(&response)?;
        let mut resp_header = SearchOffsetResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        Ok(resp_header.offset.unwrap_or(0))
    }

    /// Python 把这条 RPC 内联在 `DefaultMQPullConsumer.earliest_msg_store_time`
    /// （`consumer.py:2217`）里，Rust 与其它 offset RPC 一并收在实例层
    /// （Java `MQClientAPIImpl#getEarliestMsgStoretime`）。
    /// 响应头缺 `timestamp` ⇒ `0`（Python `resp_header.timestamp or 0`）。
    pub async fn get_earliest_msg_store_time(
        &self,
        mq: &MessageQueue,
        timeout_millis: i64,
        addr: Option<&str>,
    ) -> Result<i64> {
        let addr = match addr {
            Some(a) => a.to_string(),
            None => self.broker_addr(mq).await?,
        };
        let header = GetEarliestMsgStoretimeRequestHeader {
            topic: Some(mq.topic.clone()),
            queue_id: Some(mq.queue_id),
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::GET_EARLIEST_MSG_STORETIME,
            Some(Box::new(header)),
        );
        let response = self.invoke_sync(&addr, &mut request, timeout_millis).await?;
        Self::check_response(&response)?;
        let mut resp_header = GetEarliestMsgStoretimeResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        Ok(resp_header.timestamp.unwrap_or(0))
    }

    /// 消息回投（Java `MQClientAPIImpl#sendMessageBack`，同步等待响应）。
    ///
    /// 与 push 消费者内部那次回投的差别（`consumer.rs` 的 `send_message_back`）：
    /// 那条是**发射后不管**（`tokio::spawn` + 只记日志），且把
    /// `maxReconsumeTimes == -1` 映射成 16；这里必须把失败返回给调用方。
    ///
    /// `max_reconsume_times = None` 表示「不下发这个字段」，broker 会用订阅组自己的
    /// `retryMaxTimes` 判定是否转 `%DLQ%`。⚠ 有意偏离 Java：Java 的
    /// `DefaultMQPullConsumerImpl#sendMessageBack`（已 `@Deprecated`）把
    /// `getMaxReconsumeTimes()`（默认 -1）原样带上，而 broker 在
    /// `version >= V3_4_9` 时无条件采纳它（`AbstractSendMessageProcessor:172-179`），
    /// 于是 `reconsumeTimes(0) >= -1` 恒成立 ⇒ 拉模式回投的消息**不进 `%RETRY%` 而是
    /// 直接进 `%DLQ%`。Python 传 -1 时注释写的是「交给 broker 决定」，这里按那个语义走。
    #[allow(clippy::too_many_arguments)]
    pub async fn consumer_send_msg_back(
        &self,
        consumer_group: &str,
        msg: &MessageExt,
        delay_level: i32,
        max_reconsume_times: Option<i32>,
        timeout_millis: i64,
        addr: &str,
    ) -> Result<()> {
        let header = ConsumerSendMsgBackRequestHeader {
            offset: Some(msg.commit_log_offset),
            group: Some(consumer_group.to_string()),
            delay_level: Some(delay_level),
            origin_msg_id: msg.msg_id.clone(),
            origin_topic: Some(msg.topic.clone()),
            unit_mode: Some(false),
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

    /// Python `query_message`（Java `MQClientAPIImpl.queryMessage`）。
    ///
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
        let mut request =
            RemotingCommand::create_request_command(request_code::QUERY_MESSAGE, Some(Box::new(header)));
        if uniq_key {
            request.add_ext_field(MixAll::UNIQUE_MSG_QUERY_FLAG, "true");
        }
        let response = self.invoke_sync(&addr, &mut request, timeout_millis).await?;
        if response.code == response_code::QUERY_NOT_FOUND {
            return Ok(None);
        }
        Self::check_response(&response)?;
        Ok(response.body)
    }

    /// Python `query_message_all_brokers`（Java `MQAdminImpl#queryMessage`）：
    /// 查该 topic **所有** broker 并合并的消息；单台失败只跳过。
    ///
    /// Java 还会做客户端侧二次校验（uniqKey 命中要求 msgId == key；普通 key 命中要求
    /// `message.keys` 拆分后含 key 且 topic 相同），这里保持一致。
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
                // Python：异常与空 body 都是 continue
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
                let hit = keys.split(KEY_SEPARATOR).any(|k| k == key && msg.topic == topic);
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

    /// 按 brokerName 分组，**保持首次出现顺序**（Python 的 dict 就是插入序）。
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

    /// Python `lock_batch_mq`（Java `MQClientAPIImpl#lockBatchMQ`，LOCK_BATCH_MQ）。
    ///
    /// 按 broker 分组发送；返回 broker 确认锁定成功的队列集（`lockOKMQSet`）。
    /// 单台失败只 warn 并继续（Python 同）。
    pub async fn lock_batch_mq(
        &self,
        consumer_group: &str,
        client_id: &str,
        mqs: &[MessageQueue],
        timeout_millis: i64,
    ) -> Result<Vec<MessageQueue>> {
        let mut lock_ok: Vec<MessageQueue> = Vec::new();
        for (broker_name, broker_mqs) in Self::group_by_broker(mqs) {
            let Some(addr) = self.broker_addr_of(&broker_name) else {
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

    /// Python `unlock_batch_mq`（Java `MQClientAPIImpl#unlockBatchMQ`，UNLOCK_BATCH_MQ）。
    pub async fn unlock_batch_mq(
        &self,
        consumer_group: &str,
        client_id: &str,
        mqs: &[MessageQueue],
        timeout_millis: i64,
    ) -> Result<()> {
        for (broker_name, broker_mqs) in Self::group_by_broker(mqs) {
            let Some(addr) = self.broker_addr_of(&broker_name) else {
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

    /// Python `send_heartbeat`（HEART_BEAT = 34）。
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

    /// 汇总本实例所有已注册消费者的一份心跳报文
    /// （对应 Java `MQClientInstance#prepareHeartbeatData`；`clientId` 用实例的，
    /// 与 Java 一致，Python 是用 consumer 自己的 client_id —— 两者同源）。
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

    /// 向所有已知 broker 发心跳，返回成功的台数。
    ///
    /// Java `MQClientInstance#sendHeartbeatToAllBrokerWithLock` 的周期任务
    /// （Python 的心跳循环在 `consumer.py::_send_heartbeat_to_all_broker`，见模块头差异 4）；
    /// 单台失败只记 debug，不抛（Python 同）。
    pub async fn send_heartbeat_to_all_broker(&self, timeout_millis: i64) -> usize {
        if self.inner.consumer_table.lock().unwrap_or_else(|e| e.into_inner()).is_empty() {
            return 0;
        }
        let heartbeat_data = self.prepare_heartbeat_data();
        let mut ok = 0usize;
        for addr in self.get_route_of_all_brokers() {
            match self.send_heartbeat(&addr, &heartbeat_data, timeout_millis).await {
                Ok(()) => ok += 1,
                Err(e) => rmq_debug!("heartbeat to {addr} failed: {e}"),
            }
        }
        ok
    }

    /// Java `MQClientInstance#persistAllConsumerOffset` 的周期任务体
    /// （Python 的位点持久化循环在 consumer 内部，见模块头差异 4）；单消费者失败不影响其它。
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

    /// Python `unregister_client`（UNREGISTER_CLIENT = 35）。
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
            producer_group: Some(producer_group.to_string()),
            consumer_group: Some(consumer_group.to_string()),
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

    /// Python `get_broker_cluster_info`（GET_BROKER_CLUSTER_INFO = 106）。
    pub async fn get_broker_cluster_info(&self, timeout_millis: i64) -> Result<ClusterInfo> {
        let mut request =
            RemotingCommand::create_request_command(request_code::GET_BROKER_CLUSTER_INFO, None);
        for ns_addr in self.name_server_addrs() {
            let response = match self.invoke_sync(&ns_addr, &mut request, timeout_millis).await {
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

    /// Python `get_all_topic_list_from_name_server`（GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206）。
    pub async fn get_all_topic_list_from_name_server(
        &self,
        timeout_millis: i64,
    ) -> Result<TopicList> {
        let mut request = RemotingCommand::create_request_command(
            request_code::GET_ALL_TOPIC_LIST_FROM_NAMESERVER,
            None,
        );
        for ns_addr in self.name_server_addrs() {
            let response = match self.invoke_sync(&ns_addr, &mut request, timeout_millis).await {
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

    /// Python `create_topic_in_broker`（Java `MQClientAPIImpl#createTopic`）。
    ///
    /// ⚠ 必须下发 `topic_filter_type`：broker 的 `CreateTopicRequestHeader#checkFields()`
    /// 会把它转成枚举，为空直接报 `topicFilterType = [null] value invalid`。
    /// Java 的 `MQAdminImpl#createTopic` 对每个 broker 还会重试 5 次：
    /// broker 业务错**立即抛**，其它错（连接/超时）重试到最后一次才抛。
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
            // Java: AttributeParser.parseToString(map) —— 空 map 输出 ""，不是 null
            attributes: Some(attributes.unwrap_or("").to_string()),
            force: Some(false),
        };
        let mut last_exc: Option<Error> = None;
        for attempt in 0..retry_times.max(1) {
            let mut request = RemotingCommand::create_request_command(
                request_code::UPDATE_AND_CREATE_TOPIC,
                Some(Box::new(header.clone())),
            );
            match self.invoke_sync(broker_addr, &mut request, timeout_millis).await {
                Ok(response) => {
                    // Python: `except MQBrokerException: raise` —— 业务错不重试
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
                    rmq_debug!("create topic {topic} on {broker_addr} failed (attempt {attempt}): {e}");
                    last_exc = Some(e);
                }
            }
        }
        match last_exc {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Python `create_topic_in_route`（Java `MQAdminImpl#createTopic`）：
    /// 只对默认 topic 路由里的 broker 下发，一台成功即可，全失败才抛
    /// `create new topic failed`。
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
        let route = self.get_topic_route_data(MixAll::DEFAULT_TOPIC).await.ok_or_else(|| {
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
            // Python 这里用 create_topic_in_broker 的默认值：SINGLE_TAG / order=false / 重试 5 次
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
                // Python: MQClientException("create new topic failed", cause=last_exc)
                return Err(Error::client(format!("create new topic failed: {cause}")));
            }
        }
        Ok(())
    }

    /// Python `delete_topic_in_broker`（DELETE_TOPIC_IN_BROKER = 215）。
    pub async fn delete_topic_in_broker(
        &self,
        broker_addr: &str,
        topic: &str,
        timeout_millis: i64,
    ) -> Result<()> {
        let mut request =
            RemotingCommand::create_request_command(request_code::DELETE_TOPIC_IN_BROKER, None);
        request.add_ext_field("topic", topic);
        let response = self.invoke_sync(broker_addr, &mut request, timeout_millis).await?;
        Self::check_response(&response)?;
        Ok(())
    }

    /// Python `delete_topic_in_namesrv`（DELETE_TOPIC_IN_NAMESRV = 216）：
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

    /// Python `get_consumer_list_by_group`（GET_CONSUMER_LIST_BY_GROUP = 38）。
    /// 与 Java 不同，Python 要求显式给 addr（不给就报 `broker addr required ...`）。
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

    /// Python `get_consumer_id_list_by_group`（Java `MQClientInstance#findConsumerIdList`）。
    ///
    /// Java 取该 topic 路由里的 master broker 发 GET_CONSUMER_LIST_BY_GROUP(38)：
    /// 所有客户端都会向集群内每台 broker 心跳注册，故任取一台即持有**完整**消费者列表。
    /// 查不到（无路由 / 非 SUCCESS / 异常 / 解码失败）返回 `None`；调用方按 Java 语义
    /// 「保留当前分配」，不要回退成「自己独占全部队列」（那会让多实例互相重复消费）。
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

    /// Python `unregister_client_all_brokers`（Java `MQClientInstance#unregisterClient`）。
    ///
    /// Java 在生产者/消费者 shutdown 时会逐台 broker 发 UNREGISTER_CLIENT(35)。
    /// 不发的话 broker 端 Producer/ConsumerManager 只能等心跳超时（默认 ~120s）清理，
    /// 期间事务回查、消费者变更通知仍可能发往已退出的实例。
    /// 单台失败只记 debug —— shutdown 路径不应因网络抖动抛异常。
    pub async fn unregister_client_all_brokers(
        &self,
        client_id: &str,
        producer_group: &str,
        consumer_group: &str,
        timeout_millis: i64,
    ) {
        for addr in self.get_route_of_all_brokers() {
            if let Err(e) = self
                .unregister_client(&addr, client_id, producer_group, consumer_group, timeout_millis)
                .await
            {
                rmq_debug!("unregister_client failed, addr={addr}: {e}");
            }
        }
    }
}

/// `wait_or_stop`：sleep 与 stop 信号赛跑；`true` 表示该停了。
async fn wait_or_stop(stop: &watch::Sender<bool>, millis: u64) -> bool {
    let mut rx = stop.subscribe();
    if *rx.borrow_and_update() {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_millis(millis)) => false,
        _ = rx.changed() => true,
    }
}

/// Python 的 `tls_enable=None` 行为：读环境变量。
fn default_tls_enable_from_env() -> bool {
    matches!(
        std::env::var("ROCKETMQ_TLS_ENABLE").unwrap_or_default().trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// 326 的报文 → MessageExt（Python `_process_reply_message` 的还原段，单独拆出以便单测）。
fn build_reply_message_ext(header: &ReplyMessageRequestHeader, body: Option<&[u8]>) -> Result<MessageExt> {
    let mut bytes = body.unwrap_or_default().to_vec();
    let sys_flag = header.sys_flag.unwrap_or(0);
    // sysFlag 里带压缩标志时要先解压：326 推的是**裸包**，不走消息解码路径
    // （对齐 Java 同处的 Compressor 分支）。
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
    // Python `if header.born_host:` —— 空串视为未提供，不能存成 Some("")。
    msg.born_host = header.born_host.clone().filter(|h| !h.is_empty()).or(msg.born_host);
    msg.store_host = header.store_host.clone().filter(|h| !h.is_empty()).or(msg.store_host);
    let mut properties = string_2_message_properties(header.properties.as_deref().unwrap_or(""));
    properties.insert(PROPERTY_REPLY_MESSAGE_ARRIVE_TIME, current_time_millis().to_string());
    msg.properties = properties;
    Ok(msg)
}

/// 六个实例级 broker 主动请求码共用一个处理器（Python 注册的是同一个实例上的
/// 不同方法，Rust 的 `RequestProcessor` 一个对象可注册到多个 code，语义一致）。
///
/// 对应 Java `ClientRemotingProcessor`。
struct ClientRemotingProcessor {
    /// ⚠ 必须是弱引用：实例 → 传输 → 处理器 → 实例 若全为强引用则永远回收不了，
    /// `INSTANCE_MAP` 里的 `Weak` 也永远能升级成功 —— Java 靠 GC 断掉这条边。
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
                // oneway：Python 返回 None；Rust 由 ResponseSink 按 wants_reply 丢弃，
                // 这里干脆不回。
                instance.process_reset_offset(&request);
                return;
            }
            request_code::NOTIFY_CONSUMER_IDS_CHANGED => {
                // 同 220：broker 发的是通知，Java 返回 null ⇒ 不回包。
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
// 逻辑锁死 —— 对齐 `python/tests/test_broker_requests.py`（该文件的消费者侧用例
// 要等 `consumer.rs` 落地后再补）。

// 夹具统一走「default + 只设被读的字段」：请求头有十几个字段，逐字段字面量会把
// 用例意图淹掉，所以该模块整体关掉 field_reassign_with_default。
#[cfg(test)]
mod tests {
    #![allow(clippy::field_reassign_with_default)]

    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;
    use crate::common::message_decoder::encode_message_ext;

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

    /// 只记录调用、不做任何 RPC 的消费者替身。
    struct StubConsumer {
        group: String,
        /// 220 落位点前的耗时，用来证明「处理没有卡在调用线程上」。
        reset_delay: Duration,
        resets: Arc<Mutex<Vec<(String, usize)>>>,
        status_topics: Arc<Mutex<Vec<Option<String>>>>,
        persisted: Arc<AtomicUsize>,
        /// 40 通知叫醒了几次重平衡。
        rebalance_wakeups: Arc<AtomicUsize>,
    }

    impl StubConsumer {
        fn new() -> Arc<StubConsumer> {
            Arc::new(StubConsumer {
                group: GROUP.to_string(),
                reset_delay: Duration::from_millis(40),
                resets: Arc::new(Mutex::new(Vec::new())),
                status_topics: Arc::new(Mutex::new(Vec::new())),
                persisted: Arc::new(AtomicUsize::new(0)),
                rebalance_wakeups: Arc::new(AtomicUsize::new(0)),
            })
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
            Vec::new()
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
                guard(&self.resets).push((topic, offset_table.len()));
                Ok(())
            })
        }

        fn get_consumer_status(&self, topic: Option<&str>) -> Vec<(MessageQueue, i64)> {
            guard(&self.status_topics).push(topic.map(|t| t.to_string()));
            vec![(MessageQueue::new(TOPIC, BROKER, 0), 42)]
        }

        fn consumer_running_info(&self) -> ConsumerRunningInfo {
            let mut info = ConsumerRunningInfo::default();
            info.properties
                .insert(ConsumerRunningInfo::PROP_NAMESERVER_ADDR.to_string(), format!("{NAMESRV};"));
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
        assert!(guard(&consumer.resets).is_empty(), "220 ran inline on the calling thread");

        assert!(wait_until(|| !guard(&consumer.resets).is_empty()).await);
        assert_eq!(guard(&consumer.resets).clone(), vec![(TOPIC.to_string(), 2)]);
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
        instance.process_reset_offset(&request(request_code::RESET_CONSUMER_CLIENT_OFFSET, ResetOffsetRequestHeader::default()));
        // 坏 body：解码失败也不该波及调用方。
        let consumer = StubConsumer::new();
        instance.register_consumer(GROUP, consumer.clone());
        let mut header = ResetOffsetRequestHeader::default();
        header.group = Some(GROUP.to_string());
        let mut cmd = request(request_code::RESET_CONSUMER_CLIENT_OFFSET, header);
        cmd.set_body(Some(b"{not a reset body".to_vec()));
        instance.process_reset_offset(&cmd);
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

        // Python 把 `header.topic` 原样交给消费者（None 与 "" 语义不同：只有非空
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
        assert_eq!(resp.remark.as_deref(), Some("no consumer for group=GID_Nobody"));
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

        // 空 body / 缺 body（Python 两处都是 SYSTEM_ERROR + 固定 remark）。
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

    /// Java 把 40 注册在 `MQClientAPIImpl` 构造器里（实例级），处理器只做
    /// `rebalanceImmediately()` 且**不回包**；Rust 同口径：扇出到 consumerTable
    /// 里每个消费者，没有注册者也不报错。
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
        // 扇出是整组唤醒，不是只叫醒 header 里那一个组（Java 同样不读 group）。
        assert_eq!(first.rebalance_wakeups.load(Ordering::SeqCst), 1);
        assert_eq!(second.rebalance_wakeups.load(Ordering::SeqCst), 1);

        // 缺 consumerGroup 也照样计数：Java 只用它拼日志。
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
        // Python `if header.born_host:` —— 空串不能变成 Some("")。
        let mut header = ReplyMessageRequestHeader::default();
        header.topic = Some(TOPIC.to_string());
        header.born_host = Some(String::new());
        header.store_host = Some("127.0.0.1:10911".to_string());
        header.properties = Some(wire_properties(PROPERTY_CORRELATION_ID, "corr-1"));
        let msg = build_reply_message_ext(
            &header,
            Some(b"reply-payload".as_slice()),
        )
        .unwrap();
        assert_eq!(msg.born_host, None, "blank bornHost must stay absent");
        assert_eq!(msg.store_host.as_deref(), Some("127.0.0.1:10911"));
        assert_eq!(msg.topic, TOPIC);
        assert_eq!(msg.body.as_deref(), Some(b"reply-payload".as_slice()));
        assert_eq!(msg.get_property(PROPERTY_CORRELATION_ID), Some("corr-1"));
        assert!(
            msg.get_property(PROPERTY_REPLY_MESSAGE_ARRIVE_TIME).is_some(),
            "arrive time must be stamped like Python"
        );
    }

    #[tokio::test]
    async fn unmatched_reply_message_still_answers_success() {
        // 没有等待中的 request()：Python 只 warn，回 SUCCESS（broker 侧不需要重试）。
        let instance = new_instance();
        let mut header = ReplyMessageRequestHeader::default();
        header.topic = Some(TOPIC.to_string());
        header.properties = Some(wire_properties(PROPERTY_CORRELATION_ID, "corr-unknown"));
        let resp = instance.process_reply_message(&request(
            request_code::PUSH_REPLY_MESSAGE_TO_CLIENT,
            header,
        ));
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
        assert_eq!(task_count(&instance), started, "second start spawned duplicate loops");

        assert!(wait_until(|| consumer.persisted.load(Ordering::SeqCst) > 0).await);

        instance.shutdown();
        assert!(!instance.is_started());
        assert_eq!(task_count(&instance), 0, "shutdown left task handles behind");
        let after_stop = consumer.persisted.load(Ordering::SeqCst);

        // 重启必须真能跑：Python 靠把线程句柄置 None 重来，这里靠 stop 信号复位。
        instance.start().await.unwrap();
        assert_eq!(task_count(&instance), started);
        assert!(instance.is_started());
        assert!(
            wait_until(|| consumer.persisted.load(Ordering::SeqCst) > after_stop + 1).await,
            "loops stayed dead after restart"
        );
        instance.shutdown();
    }

    #[tokio::test]
    async fn instance_map_and_broker_push_processor_hold_only_weak_refs() {
        // 实例 → 传输 → ClientRemotingProcessor → 实例 若为强引用就永远回收不了，
        // 之后同 clientId 会一直拿到一个已关掉的死实例（Java 靠 GC 断这条边）。
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
}
