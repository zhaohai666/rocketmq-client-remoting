//! `DefaultMQProducer`: the message-sending entry point, implementing producer semantics of the classic remoting protocol.
//!
//! Protocol fields, behavioural constraints and error semantics all follow the
//! classic remoting wire protocol as served by the RocketMQ broker / NameServer.
//!
//! Responsibilities: producer configuration, lifecycle (shared instance with [`MQClientInstance`] + producer heartbeat task),
//! sync / async / oneway / pinned / selector / batch sending, send retries and latency fault avoidance, before/after send hooks and
//! check-forbidden hooks, W3C traceparent injection, message tracing, Request-Reply, transactional messages
//! (half message + local transaction + END_TRANSACTION + broker check-back) and several admin convenience methods.
//!
//! ## Implementation notes (restated on the doc of each relevant item)
//!
//! 1. **The handle is clonable**: producer handles are commonly shared with callbacks/processors; here
//!    [`DefaultMQProducer`] is a `Clone` `Arc<Inner>` whose configuration lives behind an `RwLock` for interior mutability,
//!    so setters are "callable at any time, partially rejected after start".
//! 2. **Callbacks/listeners are trait objects**: [`SendCallback`],
//!    [`MessageQueueSelector`], [`TransactionListener`].
//! 3. **`send_async` uses tokio tasks**: the async model goes through tokio uniformly, with no dedicated thread pool:
//!    the runtime handle used for dispatching is bound lazily and cached at [`DefaultMQProducer::start`] (construction may happen
//!    outside a runtime), so neither `send_async` nor the heartbeat requires the call site to be inside a runtime context.
//!    The bounded `AsyncSenderExecutor` keeps the classic pool shape (one queue of capacity
//!    `async_sender_queue_capacity` + `available_parallelism()` units of concurrency, see
//!    [`AsyncSenderExecutor`]); the "threads" here are tasks: unnamed and not pinned to cores.
//! 4. **Trace dispatcher: injected or auto-bridged** ([`DefaultMQProducer::set_trace_dispatcher`]):
//!    the usual approach is to create the `AsyncTraceDispatcher` directly inside `start()` (including a real internal producer).
//!    This crate lets callers inject a ready-made dispatcher (the `crate::client::mq_client::TraceDispatcher` seam), but when
//!    `enable_trace=true` and nothing was injected, `start()` builds one automatically through [`auto_bridged_dispatcher`]
//!    (including a real internal producer), so tracing works out of the box.
//!    Whether injected or bridged, `start()` always registers `SendMessageTraceHook` /
//!    `EndTransactionTraceHook` first and only then starts the dispatcher.
//! 5. Millisecond timestamps go through [`current_time_millis`] uniformly; mutual exclusion uses `Mutex`.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::Duration;

use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinHandle;

use crate::client::backpressure::{
    back_pressure_permits, FairSemaphore, MIN_ASYNC_SEND_NUM, MIN_ASYNC_SEND_SIZE,
};
use crate::client::hook::{
    AnyHolder, CheckForbiddenContext, CheckForbiddenHook, CheckForbiddenHookList,
    CommunicationMode, EndTransactionContext, EndTransactionHook, EndTransactionHookList,
    SendMessageContext, SendMessageHook, SendMessageHookList,
};
use crate::client::latency::{MQFaultStrategy, Resolver, ServiceDetector};
use crate::client::metrics::ClientMetrics;
use crate::client::mq_client::{
    default_concurrent_heartbeat_pool_size, MQClientInstance, MQClientInstanceConfig,
    PublishMessage, TopicPublishInfo, TraceDispatcher, MQ_CLIENT_API_TIMEOUT_MILLIS,
};
use crate::client::produce_accumulator::{
    get_or_create_produce_accumulator, AccumulatorSender, ProduceAccumulator, TIMER_DELAY_KEYS,
};
use crate::client::request_reply::{
    create_correlation_id, request_future_holder, start_expired_request_scan,
    stop_expired_request_scan, RequestResponseFuture, DEFAULT_REQUEST_TIMEOUT_MILLIS,
};
use crate::client::result::{LocalTransactionState, SendResult, SendStatus, TransactionSendResult};
use crate::client::shutdown::{run_finalize_blocking, SHUTDOWN_FINALIZE_BUDGET};
use crate::client::top_addressing::DefaultTopAddressing;
use crate::client::trace::TraceContext;
use crate::client::trace_context::{inject_trace_context, trace_context_enabled_from_env};
use crate::client::trace_dispatcher::{
    AsyncTraceDispatcher, TraceDispatcherConfig, TraceDispatcherType, TraceHost, TraceProducer,
    TraceProducerFuture,
};
use crate::client::trace_hook::{EndTransactionTraceHook, SendMessageTraceHook, TraceReportSink};
use crate::client::validators;
use crate::common::compression;
use crate::common::message::{Message, MessageBatch, MessageExt, MessageQueue};
use crate::common::message_client_id_setter::set_uniq_id;
use crate::common::message_const::{
    PROPERTY_CORRELATION_ID, PROPERTY_DELAY_TIME_LEVEL,
    PROPERTY_MESSAGE_REPLY_TO_CLIENT, PROPERTY_MESSAGE_TTL, PROPERTY_PRODUCER_GROUP,
    PROPERTY_TRANSACTION_PREPARED, PROPERTY_TIMER_DELIVER_MS, PROPERTY_TIMER_DELAY_MS,
    PROPERTY_TIMER_DELAY_SEC, PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
};
use crate::common::message_decoder::{decode_message, decode_message_id, decode_messages};
use crate::common::message_type::MessageType;
use crate::common::mix_all::MixAll;
use crate::common::recall_message_handle;
use crate::common::sysflag::{MessageSysFlag, PermName};
use crate::common::util_all::{current_time_millis, java_string_hash, monotonic_millis};
use crate::error::{client_error_code, Error, Result};
use crate::remoting::client::{RequestProcessor, ResponseSink};
use crate::remoting::protocol::codes::{request_code, response_code};
use crate::remoting::protocol::ext_fields::CustomHeader;
use crate::remoting::protocol::headers::{
    CheckTransactionStateRequestHeader, EndTransactionRequestHeader,
    GetEarliestMsgStoretimeRequestHeader, GetEarliestMsgStoretimeResponseHeader,
    RecallMessageRequestHeader,
};
use crate::remoting::protocol::heartbeat::{HeartbeatData, ProducerData};
use crate::remoting::protocol::namespace_util::NamespaceUtil;
use crate::remoting::protocol::remoting_command::{next_opaque, RemotingCommand};
use crate::remoting::rpchook::RPCHook;
use crate::remoting::socks::SocksProxyConfig;
use crate::{bail, rmq_debug, rmq_error, rmq_warn};

/// Offline differential tests of the send-retry kernel (in-process fake cluster); see that module's doc.
#[cfg(test)]
mod send_retry_tests;

#[cfg(test)]
mod detector_tests;

/// Default value of `instance_name`.
pub const DEFAULT_INSTANCE_NAME: &str = "DEFAULT";
/// Default body size cap for a single message: 4 MiB.
pub const DEFAULT_MAX_MESSAGE_SIZE: i32 = 1024 * 1024 * 4;
/// The body is compressed only when it reaches this threshold; default 4 KiB.
pub const DEFAULT_COMPRESS_MSG_BODY_OVER_HOWMUCH: i32 = 1024 * 4;
/// Default zlib compression level: level 5 (the level encoding is written into the message sysFlag).
pub const DEFAULT_COMPRESS_LEVEL: i32 = 5;
/// Default capacity of the async send queue: the classic client parks send tasks in one bounded
/// blocking queue (capacity 50000) and rejects a submission as soon as it is full.
/// Corresponds to the config item `async_sender_queue_capacity`.
pub const DEFAULT_ASYNC_SENDER_QUEUE_CAPACITY: i32 = 50_000;

/// The two wordings of the pinned-send guard (one for the sync entry, one for the async entry),
/// kept verbatim: the two wordings were always different, each entry uses its own.
const PINNED_TOPIC_MISMATCH_SYNC: &str = "message's topic not equal mq's topic";
const PINNED_TOPIC_MISMATCH_ASYNC: &str =
    "Topic of the message does not match its target message queue";

/// Default set of retryable broker business response codes (the default of the `retry_response_codes` config item).
///
/// The criterion is "another broker might answer differently": each of these codes denotes a transient
/// broker state (busy, unavailable, route not synced yet, quarantined, ...). Deterministic errors such as
/// `MESSAGE_ILLEGAL` gain nothing from a retry and must be handed to the caller unchanged.
pub const DEFAULT_RETRY_RESPONSE_CODES: [i32; 8] = [
    response_code::SYSTEM_ERROR,
    response_code::SYSTEM_BUSY,
    response_code::SERVICE_NOT_AVAILABLE,
    response_code::NO_PERMISSION,
    response_code::TOPIC_NOT_EXIST,
    response_code::NO_BUYER_ID,
    response_code::NOT_IN_CURRENT_UNIT,
    response_code::GO_AWAY,
];

// ================================================================ Queue selectors

/// Queue selector: the caller decides which queue the message is delivered to.
///
/// `arg` is a `&str`: the three built-in selectors only need
/// string/integer semantics.
pub trait MessageQueueSelector: Send + Sync + 'static {
/// Pick the target queue out of `mqs`.
    fn select(&self, mqs: &[MessageQueue], msg: &Message, arg: &str) -> Result<MessageQueue>;
}

/// Select the queue by the hash of `arg` (for ordered messages / sharded sending).
#[derive(Debug, Default, Clone, Copy)]
pub struct SelectMessageQueueByHash;

impl MessageQueueSelector for SelectMessageQueueByHash {
/// A shard key must be reproducible across processes: an environment-dependent string hash would put
/// the same `arg` on different queues. This uses the classic 31-multiplier integer hash semantics
    /// (deterministic, fit to serve as a shard key).
    fn select(&self, mqs: &[MessageQueue], _msg: &Message, arg: &str) -> Result<MessageQueue> {
        if mqs.is_empty() {
            bail!("no message queue");
        }
        let hash = java_string_hash(arg);
        // When the hash is exactly `i32::MIN` its absolute value is still negative (overflow), so converge it to 0 explicitly.
        let idx = if hash < 0 {
            (hash.checked_neg().unwrap_or(0) as usize) % mqs.len()
        } else {
            (hash as usize) % mqs.len()
        };
        Ok(mqs[idx].clone())
    }
}

/// Pick one queue at random (pure load spreading, no partitioning semantics).
#[derive(Debug, Default, Clone, Copy)]
pub struct SelectMessageQueueByRandom;

impl MessageQueueSelector for SelectMessageQueueByRandom {
    fn select(&self, mqs: &[MessageQueue], _msg: &Message, _arg: &str) -> Result<MessageQueue> {
        if mqs.is_empty() {
            bail!("no message queue");
        }
        // Modulo of a process-level random source (no wire semantics).
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as usize)
            .unwrap_or(0);
        Ok(mqs[(nanos + mqs.len()) % mqs.len()].clone())
    }
}

/// Select the queue by machine-room prefix (brokerName prefix); the prefix comes from `arg`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SelectMessageQueueByMachineRoom;

impl MessageQueueSelector for SelectMessageQueueByMachineRoom {
    fn select(&self, mqs: &[MessageQueue], _msg: &Message, arg: &str) -> Result<MessageQueue> {
        if mqs.is_empty() {
            bail!("no message queue");
        }
        Ok(mqs
            .iter()
            .find(|mq| mq.broker_name.starts_with(arg))
            .cloned()
            .unwrap_or_else(|| mqs[0].clone()))
    }
}

// ================================================================ Async send callback

/// Async send callback: hands the result back once the request reaches a terminal state (success or failure).
pub trait SendCallback: Send + Sync + 'static {
    /// Called on a successful send, carrying the final [`SendResult`].
    fn on_success(&self, result: SendResult);

    /// Called when the send failed, carrying the failure reason.
    fn on_exception(&self, err: Error);
}

/// Success callback closure.
pub type OnSuccessFn = Box<dyn FnOnce(SendResult) + Send + 'static>;

/// Failure callback closure.
pub type OnExceptionFn = Box<dyn FnOnce(Error) + Send + 'static>;

/// Convenience callback: glues two plain closures into a [`SendCallback`]
/// and guarantees each closure runs at most once.
pub struct ClosureSendCallback {
    success: Mutex<Option<OnSuccessFn>>,
    failure: Mutex<Option<OnExceptionFn>>,
}

impl ClosureSendCallback {
    /// Either `success` or `failure` may be `None`.
    pub fn new(
        success: Option<OnSuccessFn>,
        failure: Option<OnExceptionFn>,
    ) -> ClosureSendCallback {
        ClosureSendCallback {
            success: Mutex::new(success),
            failure: Mutex::new(failure),
        }
    }

    fn take_success(&self) -> Option<OnSuccessFn> {
        self.success
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }

    fn take_failure(&self) -> Option<OnExceptionFn> {
        self.failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }
}

impl SendCallback for ClosureSendCallback {
    /// An `FnOnce` can run only once: taking it empties the slot, so a repeated callback (success and
    /// failure happening together, which should never happen) has its later arrival ignored.
    fn on_success(&self, result: SendResult) {
        if let Some(f) = self.take_success() {
            f(result);
        }
    }

    fn on_exception(&self, err: Error) {
        if let Some(f) = self.take_failure() {
            f(err);
        }
    }
}

// ================================================================ Transaction listener

/// Transaction listener: runs the local transaction after the half message is sent successfully, and answers broker check-backs.
///
/// The listener may report "unknown" (treated as `UNKNOW`) and catch its own exceptions;
/// [`LocalTransactionState::Unknow`] expresses "unknown" here, and the listener turns its own exceptions
/// into `Unknow` internally (for the `catch` semantics of the send chain see
/// [`DefaultMQProducer::send_message_in_transaction`]).
pub trait TransactionListener: Send + Sync + 'static {
    /// Run the local transaction after the half message was sent successfully; return commit / rollback / unknown.
    fn execute_local_transaction(
        &self,
        msg: &Message,
        arg: Option<&AnyHolder>,
    ) -> LocalTransactionState;

    /// Called when the broker checks back the local transaction state; return commit / rollback / unknown.
    fn check_local_transaction(&self, msg: &MessageExt) -> LocalTransactionState;
}

// ================================================================ Trace dispatcher channel

/// The trace dispatcher held by a producer has to satisfy two existing seams at once:
///
/// * [`TraceReportSink`] (`trace_hook.rs`) -- the trace hooks push into it;
/// * [`TraceDispatcher`] (`mq_client.rs`) -- the lifecycle (`start` / `shutdown`).
///
/// Lifecycle contract: the producer owns the dispatcher, creates/injects it at `start()` and calls
/// `dispatcher.start(namesrv)`, and calls `dispatcher.shutdown()` at `shutdown()`.
///
/// ⚠ Why the two seams' methods are **restated** here instead of writing
/// `trait TraceDispatcherChannel: TraceDispatcher + TraceReportSink`: stable Rust
/// has no trait upcasting, so an `Arc<dyn TraceDispatcherChannel>` cannot be converted into
/// `Arc<dyn TraceReportSink>`, which is exactly what [`SendMessageTraceHook::new`] needs.
/// The blanket impl below makes any type implementing both usable automatically, and [`SinkAdapter`]
/// then reshapes it into the form the hook needs.
pub trait TraceDispatcherChannel: Send + Sync {
    /// Corresponds to `TraceDispatcher::start`.
    fn start(&self, name_server_addr: &str) -> Result<()>;

    /// Corresponds to `TraceDispatcher::shutdown`.
    fn shutdown(&self);

    /// Corresponds to `TraceReportSink::trace_topic_name`.
    fn trace_topic_name(&self) -> String;

    /// Corresponds to `TraceReportSink::report`.
    fn report(&self, context: TraceContext) -> bool;

    /// Corresponds to `TraceReportSink::client_id`.
    fn client_id(&self) -> String;
}

impl<T> TraceDispatcherChannel for T
where
    T: TraceDispatcher + TraceReportSink + Send + Sync,
{
    fn start(&self, name_server_addr: &str) -> Result<()> {
        TraceDispatcher::start(self, name_server_addr)
    }

    fn shutdown(&self) {
        TraceDispatcher::shutdown(self);
    }

    fn trace_topic_name(&self) -> String {
        TraceReportSink::trace_topic_name(self)
    }

    fn report(&self, context: TraceContext) -> bool {
        TraceReportSink::report(self, context)
    }

    fn client_id(&self) -> String {
        TraceReportSink::client_id(self)
    }
}

/// Wrap the injected channel into the [`TraceReportSink`] the trace hooks need.
///
/// `Arc` rather than `&dyn` because the hook list keeps the sink alive for a long time in registration
/// order, so its lifetime cannot hang off a borrow of the producer.
pub(crate) struct SinkAdapter(pub Arc<dyn TraceDispatcherChannel>);

impl TraceReportSink for SinkAdapter {
    fn trace_topic_name(&self) -> String {
        self.0.trace_topic_name()
    }

    fn report(&self, context: TraceContext) -> bool {
        self.0.report(context)
    }

    fn client_id(&self) -> String {
        self.0.client_id()
    }
}

// ================================================================ Configuration

/// Configurable producer options: defaults are supplied at construction; after start some can still be changed and some are rejected.
///
/// The defaults are the generic defaults of a classic remoting producer (see the
/// [`Default`] implementation).
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub producer_group: String,
    /// `None` = deferred to the `ROCKETMQ_TLS_ENABLE` environment variable.
    pub tls_enable: Option<bool>,
    pub socks_proxy: Option<SocksProxyConfig>,
    /// `None` = deferred to the
    /// `ROCKETMQ_TRACE_CONTEXT_ENABLE` environment variable.
    pub enable_trace_context: Option<bool>,
    /// `namespace` (when non-empty, both topics and the producerGroup get a `<ns>%` prefix).
    pub namespace: String,
    pub instance_name: String,
    /// `unitName` (unset by default): when non-empty it is joined into the clientId
    /// and used as the `-<unitName>` segment of the address server URL.
    pub unit_name: Option<String>,
    /// `namespaceV2` (unset by default): the 5.x **server-side** namespace;
    /// when non-empty every request carries the `nsd=true` / `ns=<namespaceV2>` extended headers (`NamespaceRpcHook`),
    /// a different mechanism altogether from the client-side `%` prefixing of v1 `namespace`.
    pub namespace_v2: Option<String>,
    /// `unitMode` (default false): travels on the wire with send, redelivery, authentication and
    /// message-filter requests, and the broker uses it to set the UNIT / UNIT_SUB bits of auto-created topics.
    pub unit_mode: bool,
    /// `enableStreamRequestType` (default false for producers):
    /// when true every request carries `ReqT=0` and the clientId gains a trailing `@STREAM` segment.
    pub enable_stream_request_type: bool,
    /// `pollNameServerInterval` (default 30000ms): the periodic route refresh interval for topics in use,
    /// passed through to `MQClientInstance` at `start()`; afterwards changing it or not has no effect on
    /// already scheduled tasks (each periodic task captures its interval once when it is scheduled).
    pub poll_name_server_interval_millis: u64,
    /// When `None`, `start()` generates `<instanceName>@<yyyyMMddHHmmss>`.
    pub client_id: Option<String>,
    pub create_topic_key: String,
    pub default_topic_queue_nums: i32,
    pub send_msg_timeout: i64,
    pub compress_msg_body_over_howmuch: i32,
    pub compress_level: i32,
    /// The **algorithm number** ([`MessageSysFlag::ZLIB_TYPE`] /
    /// `LZ4_TYPE` / `ZSTD_TYPE`, i.e. 0-3), not the `*_COMPRESSION_*_TYPE` form that is
    /// already shifted onto flag bits; both [`crate::common::compression::compress`] and
    /// [`MessageSysFlag::set_compression_type`] take the algorithm number. Default ZLIB.
    pub compress_type: i32,
    pub retry_times_when_send_failed: i32,
    /// `retry_times_when_send_async_failed` (default 2):
    /// the broker-switching retry cap for async sends **after the request has been sent**, i.e. the
    /// `timesTotal` per round of the async chain. Together with sync sending's
    /// `retry_times_when_send_failed` these are **two** independent budgets, with different criteria (async does not consult
    /// `retryResponseCodes`, see [`classify_async_failure`]).
    pub retry_times_when_send_async_failed: i32,
    /// `async_sender_queue_capacity` (default 50000, the capacity of the bounded
    /// queue it replaces): the queue capacity of [`AsyncSenderExecutor`],
    /// read exactly once at [`DefaultMQProducer::start`].
    pub async_sender_queue_capacity: i32,
    /// `retry_another_broker_when_not_store_ok` (default false):
    /// whether to resend through another broker when the broker returns a non-SEND_OK store result.
    pub retry_another_broker_when_not_store_ok: bool,
    /// `send_msg_max_timeout_per_request` (default -1 = no per-request cap,
    /// only the overall `send_msg_timeout` budget constrains it).
    pub send_msg_max_timeout_per_request: i64,
    /// When the broker answers with one of these **business response codes**, resend through another broker
    /// instead of handing the failure straight to the caller.
    pub retry_response_codes: BTreeSet<i32>,
    pub max_message_size: i32,
    /// `enable_backpressure_for_async_mode`:
    /// **off** by default. Once on, an async send must pass two fair-semaphore gates, one per dimension, before it really issues the request.
    pub enable_backpressure_for_async_mode: bool,
    /// `back_pressure_for_async_send_num`:
    /// cap on the in-flight **message count**, default 1024, floored at [`MIN_ASYNC_SEND_NUM`].
    pub back_pressure_for_async_send_num: i64,
    /// `back_pressure_for_async_send_size`:
    /// cap on the in-flight **byte count**, default 100M, floored at [`MIN_ASYNC_SEND_SIZE`].
    pub back_pressure_for_async_send_size: i64,
    pub topics: Vec<String>,
    pub name_server_addrs: Vec<String>,
    pub heartbeat_interval_millis: u64,
    pub send_latency_fault_enable: bool,
    pub start_detector_enable: bool,
    pub detect_timeout: i64,
    pub detect_interval: i64,
    pub enable_trace: bool,
    /// `trace_topic` (`None` → the system default trace topic).
    pub trace_topic: Option<String>,
    pub trace_msg_batch_num: i32,
    /// `request_timeout` (used when `request()` gets no explicit timeout).
    pub request_timeout: i64,
    /// `autoBatch` (default **false**): when on, `send(Message)`
    /// first merges messages into batches per `AggregateKey` before sending (see [`crate::client::produce_accumulator`]).
    pub auto_batch: bool,
    /// `batchMaxDelayMs`: the maximum lifetime of one batch (ms).
    /// **-1 is the "caller did not state a preference" sentinel** -- only values `> -1` are
    /// pushed into the accumulator by [`DefaultMQProducer::init_produce_accumulator`].
    pub batch_max_delay_ms: i64,
    /// `batchMaxBytes`: the byte cap of one batch. Same convention: `-1` = not stated.
    pub batch_max_bytes: i64,
    /// `totalBatchMaxBytes`: the **global** byte cap of batching in flight.
    /// Same convention: `-1` = not stated.
    pub total_batch_max_bytes: i64,
    /// Heartbeat fingerprint path (default false). On, this facade's `HEART_BEAT(34)` rounds carry
    /// a non-zero `heartbeatFingerprint`, and once the broker answers `IS_SUPPORT_HEART_BEAT_V2`
    /// and the fingerprint is unchanged the subscription set drops out of the body.
    pub use_heartbeat_v2: bool,
    /// Decode-read-body switch (default true): off, received message bodies stay unmaterialised.
    pub decode_read_body: bool,
    /// Internal-API timeout (default 3000ms) for the client's own control RPCs.
    pub mq_client_api_timeout_millis: i64,
    /// Concurrent heartbeat switch (default false).
    pub enable_concurrent_heartbeat: bool,
    /// Concurrent heartbeat pool size (default: available processors).
    pub concurrent_heartbeat_thread_pool_size: usize,
}

impl Default for ProducerConfig {
    fn default() -> ProducerConfig {
        ProducerConfig {
            producer_group: MixAll::DEFAULT_PRODUCER_GROUP.to_string(),
            tls_enable: None,
            socks_proxy: None,
            enable_trace_context: None,
            namespace: String::new(),
            instance_name: DEFAULT_INSTANCE_NAME.to_string(),
            unit_name: None,
            namespace_v2: None,
            unit_mode: false,
            // producers never touch this switch (only pull-style / lightweight consumers set it true)
            enable_stream_request_type: false,
            // pollNameServerInterval default 1000 * 30
            poll_name_server_interval_millis: 30_000,
            client_id: None,
            create_topic_key: MixAll::DEFAULT_TOPIC.to_string(),
            default_topic_queue_nums: MixAll::DEFAULT_TOPIC_QUEUE_NUMS,
            send_msg_timeout: 3000,
            compress_msg_body_over_howmuch: DEFAULT_COMPRESS_MSG_BODY_OVER_HOWMUCH,
            compress_level: DEFAULT_COMPRESS_LEVEL,
            compress_type: MessageSysFlag::ZLIB_TYPE,
            retry_times_when_send_failed: 2,
            // async retries default to 2 (the classic client's default)
            retry_times_when_send_async_failed: 2,
            // async send queue default capacity 50000 (the bounded blocking queue default)
            async_sender_queue_capacity: DEFAULT_ASYNC_SENDER_QUEUE_CAPACITY,
            retry_another_broker_when_not_store_ok: false,
            send_msg_max_timeout_per_request: -1,
            retry_response_codes: DEFAULT_RETRY_RESPONSE_CODES
                .iter()
                .copied()
                .collect::<BTreeSet<i32>>(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            // backpressure switch off by default, capacity defaults to 1024 messages / 100M
            enable_backpressure_for_async_mode: false,
            back_pressure_for_async_send_num: 1024,
            back_pressure_for_async_send_size: 100 * 1024 * 1024,
            topics: Vec::new(),
            name_server_addrs: Vec::new(),
            heartbeat_interval_millis: 30_000,
            send_latency_fault_enable: false,
            start_detector_enable: false,
            detect_timeout: 200,
            detect_interval: 2 * 1000,
            enable_trace: false,
            trace_topic: None,
            trace_msg_batch_num: 10,
            request_timeout: DEFAULT_REQUEST_TIMEOUT_MILLIS,
            // auto batching: switch off by default, the three thresholds default to -1
            // (-1 = "the caller did not state a preference", see the field docs).
            auto_batch: false,
            batch_max_delay_ms: -1,
            batch_max_bytes: -1,
            total_batch_max_bytes: -1,
            use_heartbeat_v2: false,
            decode_read_body: true,
            mq_client_api_timeout_millis: MQ_CLIENT_API_TIMEOUT_MILLIS,
            enable_concurrent_heartbeat: false,
            concurrent_heartbeat_thread_pool_size: default_concurrent_heartbeat_pool_size(),
        }
    }
}

// ================================================================ Shared state

/// The producer's shared state: configuration + the various handles + locks, shared through the outer `Arc`.
///
/// The handles are shared by [`DefaultMQProducer`] through `Arc`, and the transaction check-back processor pushed by the
/// broker only holds a `Weak` (see [`CheckTransactionStateProcessor`]), for the same reason as
/// the client instance's broker-push processor: otherwise
/// `Inner → RemotingClient → processor → Inner` would form a cycle and never be released.
struct Inner {
    /// Scattered public attributes gathered into one configuration; the `RwLock` keeps setters "callable at any time",
    /// and the async send path snapshots the fields it needs outside the read lock (holding a read lock across `.await` makes the future non-`Send`).
    cfg: RwLock<ProducerConfig>,
    /// The client instance.
    client: Mutex<Option<MQClientInstance>>,
    started: AtomicBool,
    /// Implementation note 3: the runtime handle bound lazily at `start()`; used to dispatch `send_async` and the heartbeat task.
    runtime: OnceLock<tokio::runtime::Handle>,
    /// Heartbeat stop flag (true = stop).
    stop: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// The broker fault table strategy.
    fault_strategy: MQFaultStrategy,
    metrics: ClientMetrics,
    /// Before/after send hook table.
    send_hooks: SendMessageHookList,
    /// End-transaction hook table.
    end_txn_hooks: EndTransactionHookList,
    /// Pre-send interception (check-forbidden) hook table.
    check_forbidden_hooks: CheckForbiddenHookList,
    transaction_listener: RwLock<Option<Arc<dyn TransactionListener>>>,
    /// Trace dispatcher; implementation note 4: injected by the caller.
    trace: Mutex<Option<Arc<dyn TraceDispatcherChannel>>>,
    rpc_hook: RwLock<Option<Arc<dyn RPCHook>>>,
    /// Fair semaphore for in-flight async send **message count** (`_semaphore_async_send_num`):
    /// sized from the configuration when the object is built (out-of-range values clamp to the floor);
    /// capacity changes at runtime go through [`set_back_pressure_for_async_send_num`](DefaultMQProducer::set_back_pressure_for_async_send_num).
    semaphore_async_send_num: FairSemaphore,
    /// Fair semaphore for in-flight async send **byte count** (`_semaphore_async_send_size`):
    /// likewise sized from the configuration when the object is built.
    semaphore_async_send_size: FairSemaphore,
    /// `defaultAsyncSenderExecutor` (the async send executor):
    /// bounded queue + fixed concurrency allowance + one dispatch task.
    /// Built by `start()`, closed by `shutdown()`; `None` = never started or already closed.
    async_sender: Mutex<Option<AsyncSenderExecutor>>,
    /// `produceAccumulator` (empty when uninitialized): the accumulator used for auto batching.
    /// Fetched (or created) by clientId inside `start()`; `get_auto_batch()` reports "not enabled" when it is missing.
    produce_accumulator: RwLock<Option<ProduceAccumulator>>,
}

impl Inner {
    fn producer_group(&self) -> String {
        self.cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .producer_group
            .clone()
    }

    fn namespace(&self) -> String {
        self.cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .namespace
            .clone()
    }

    /// Every spot on the send path reads `unitMode` from the configuration: both the send header and
    /// the CheckForbiddenContext take this single value.
    fn unit_mode(&self) -> bool {
        self.cfg.read().unwrap_or_else(|e| e.into_inner()).unit_mode
    }

    /// The remaining two items of the send request header: sent on every request and sourced only from the producer's
    /// `c` (createTopicKey) and `d` (defaultTopicQueueNums) configuration,
    /// i.e. the `createTopicKey` and `defaultTopicQueueNums` the send kernel prepares for a
    /// newly created topic (the broker uses them to auto-create the topic and its queue count).
    fn create_topic_key(&self) -> String {
        self.cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .create_topic_key
            .clone()
    }

    fn default_topic_queue_nums(&self) -> i32 {
        self.cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .default_topic_queue_nums
    }

    fn client(&self) -> Option<MQClientInstance> {
        self.client
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn trace_dispatcher(&self) -> Option<Arc<dyn TraceDispatcherChannel>> {
        self.trace.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// Bounded async send queue + fixed concurrency allowance: the classic shape is
/// "bounded blocking queue (50000) + cores pool threads"; this implementation uses a
/// tokio channel + semaphore + a single dispatcher task, matching the semantics point by point.
///
/// Shape matched point by point:
///   * `try_send` failing ≡ the `RejectedExecutionException` the pool's `submit` raises after its
///     queue offer fails (→ the corresponding branch of [`send_async`](DefaultMQProducer::send_async));
///   * [`close`](Self::close) (dropping the sending side) ≡ `defaultAsyncSenderExecutor.shutdown()`
///     -- **already queued tasks still run to completion**, nothing new is accepted, so `shutdown()` does not
///     cut off async sends that are in flight;
///   * concurrency allowance = [`async_sender_worker_count`] (core count): while the allowance
///     cannot be taken the task stays queued, exactly as tasks stay in the queue while all pool threads are busy.
///
/// The pool throttles the "preparation stage" (gate → route → hooks → build request → hand to the transport); once a request
/// is handed to the transport the task is over, so network waiting does not occupy the pool -- this must match, otherwise the pool size
/// becomes the cap on "in-flight async sends", a cap that is supposed to come from the backpressure gates (with backpressure off there is no cap at all).
///
/// ⚠ **Why "one dispatcher task + allowance + one `spawn` per send", instead of "`cores` consumers sharing
/// one `Mutex<Receiver>`"** (the first version did the latter and was proven unusable on a real cluster): running synchronous hooks on a pool
/// thread is **normal**, but it is fatal under tokio -- a consumer takes a task and `await`s it, and
/// one blocking sleep inside a hook parks that tokio worker thread in a synchronous call; and `tokio::sync::Mutex`
/// is a **handoff**-style fair lock, which after waking "the next waiter" puts the baton into
/// **the local queue of the current worker thread**, and local queues are never stolen -- so the relay that fetches tasks
/// breaks on that sleeping worker. Measured on a real cluster (`examples/live_backpressure.rs` B3, hook sleeps 2.5s): 10 sends were
/// waiting in the queue while the pool **dispatched only one every 2505ms**, and later sends could only watch their timeout budget get eaten by
/// queueing. Splitting into a single dispatcher task (which never blocks itself) + a semaphore limiting concurrency + a `spawn` per send
/// means the only thing relayed is "the allowance being returned", and that return happens at the end of the chain without occupying any sleeping thread.
///
/// ⚠ Two things cannot be copied over: pool threads are named (`AsyncSenderExecutor_N`) and pinned one-to-one to cores;
/// here they are tokio tasks, which have no names, and real concurrency is also bounded by the host runtime's worker thread count.
struct AsyncSenderExecutor {
    tx: mpsc::Sender<AsyncSendJob>,
    /// The only consumer (the pool's "take a task" step). It does not run tasks itself, so it never blocks.
    dispatcher: JoinHandle<()>,
}

/// One async send task parked in the queue (the message, budget and start point were already fixed on the caller's thread).
type AsyncSendJob = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Pool concurrency allowance = max(available cores, `MIN_ASYNC_SEND_NUM`).
///
/// Falls back to 1 when the core count is unavailable. The floor is `MIN_ASYNC_SEND_NUM` because that is also the
/// floor of the in-flight **message count** backpressure semaphore (10): the sender must be able to run at least that many sends concurrently,
/// or the backpressure floor would be meaningless -- on a low-core machine the semaphore could never fill up (at most 1-2 units held) and backpressure
/// would effectively be disabled. On multi-core machines the `availableProcessors` semantics are unchanged.
fn async_sender_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(MIN_ASYNC_SEND_NUM as usize)
}

impl AsyncSenderExecutor {
    fn new(handle: &tokio::runtime::Handle, capacity: i32, workers: usize) -> AsyncSenderExecutor {
        // A tokio bounded channel rejects capacity 0 (`LinkedBlockingQueue(0)` likewise raises an
        // IllegalArgumentException, except that panicking is not an option here): capacity 0 is treated as 1,
        // and the semantics stay "almost no queueing room".
        let (tx, mut rx) = mpsc::channel(usize::try_from(capacity.max(1)).unwrap_or(usize::MAX));
        let permits = Arc::new(Semaphore::new(workers.max(1)));
        let spawn_handle = handle.clone();
        let dispatcher = handle.spawn(async move {
            let permits = permits;
            loop {
                // All senders dropped = `close`: dispatch everything already queued, then exit
                let Some(job) = rx.recv().await else { return };
                // No allowance available: stay queued (waiting yields the thread and occupies nobody).
                // `acquire_owned` rather than `acquire`: the allowance must move into the `'static` task together with the job,
                // while the borrowed kind (`SemaphorePermit<'_>`) cannot be moved and can only be `forget`ted,
                // and `forget` means "never returned" -- that would lock the pool up after `cores` sends.
                // Failing to get an allowance can only mean someone `close()`d this semaphore (never happens in this codebase): in that
                // case dispatch it anyway without an allowance rather than silently swallow a send the user handed over.
                let permit = Arc::clone(&permits).acquire_owned().await.ok();
                spawn_handle.spawn(async move {
                    let _permit = permit; // returned only once the task ends (a blocking hook included, run to completion)
                    job.await;
                });
            }
        });
        AsyncSenderExecutor { tx, dispatcher }
    }

    /// ≡ the pool's `submit(runnable)`: when the queue is full it fails **immediately**, never queueing while it waits for a slot.
    /// On failure the task is handed back untouched so the caller can take the "run it in place" branch.
    ///
    /// (The return type is spelled out as `std::result::Result`: this module's `Result<T>` alias carries only one parameter,
    /// its error type is fixed to [`Error`], and what has to be handed back here is the task itself.)
    fn try_submit(&self, job: AsyncSendJob) -> std::result::Result<(), AsyncSendJob> {
        match self.tx.try_send(job) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(job)) => Err(job),
            Err(mpsc::error::TrySendError::Closed(job)) => Err(job),
        }
    }

    /// ≡ `defaultAsyncSenderExecutor.shutdown()`: no new tasks are accepted, in-flight and queued ones still run to completion.
    fn close(self) {
        let AsyncSenderExecutor { tx, dispatcher } = self;
        drop(tx);
        // Only drop the handle, never abort: dropping a `JoinHandle` is a detach in tokio, the task keeps running to completion.
        // The pool's `shutdown()` semantics: neither stops already-queued tasks nor waits for them to terminate.
        drop(dispatcher);
    }
}

/// Forgetting to call `shutdown()` must not leave background tasks running either (daemon threads die with the process,
/// whereas tokio tasks would stay alive forever inside the leaked runtime).
impl Drop for Inner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        for task in self
            .tasks
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            task.abort();
        }
    }
}

// ================================================================ DefaultMQProducer

/// The default producer: the complete producer shape of the classic remoting protocol
/// (configuration + lifecycle + all send entry points + transactions/batching/tracing).
///
/// A clone **shares the same state** as the original (implementation note 1): reference semantics
/// are realised here by `Arc<Inner>`, so after handing a producer to a callback/processor
/// one can still call `set_xxx` and clones see it immediately.
///
/// Lifecycle: [`start`](Self::start) → any number of `send*` → [`shutdown`](Self::shutdown).
/// Sending before start yields [`Error::Client`]:
/// `producer not started, call start() first`.
#[derive(Clone)]
pub struct DefaultMQProducer {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for DefaultMQProducer {
    /// Prints identity and state only: hooks/listeners are `dyn Trait` and cannot be printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cfg = self.inner.cfg.read().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("DefaultMQProducer")
            .field("producer_group", &cfg.producer_group)
            .field("namespace", &cfg.namespace)
            .field("instance_name", &cfg.instance_name)
            .field("client_id", &cfg.client_id)
            .field("name_server_addrs", &cfg.name_server_addrs)
            .field("started", &self.inner.started.load(Ordering::Acquire))
            .field("enable_trace", &cfg.enable_trace)
            .finish()
    }
}

impl DefaultMQProducer {
    /// Construct from a producer group name; an empty group name is an error
    /// (`MQClientException("producerGroup is empty")`).
    pub fn new(producer_group: &str) -> Result<DefaultMQProducer> {
        let cfg = ProducerConfig {
            producer_group: producer_group.to_string(),
            ..Default::default()
        };
        DefaultMQProducer::with_config(cfg)
    }

    pub fn with_rpc_hook(
        producer_group: &str,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Result<DefaultMQProducer> {
        let producer = DefaultMQProducer::new(producer_group)?;
        producer.set_rpc_hook(rpc_hook);
        Ok(producer)
    }

    /// Construct directly from a complete configuration (equivalent to calling the `set_*` methods one by one).
    pub fn with_config(cfg: ProducerConfig) -> Result<DefaultMQProducer> {
        if cfg.producer_group.trim().is_empty() {
            bail!("producerGroup is empty");
        }
        let fault_strategy = MQFaultStrategy::new(cfg.send_latency_fault_enable);
        let semaphore_async_send_num = FairSemaphore::new(back_pressure_permits(
            cfg.back_pressure_for_async_send_num,
            MIN_ASYNC_SEND_NUM,
            "semaphoreAsyncSendNum",
        ));
        let semaphore_async_send_size = FairSemaphore::new(back_pressure_permits(
            cfg.back_pressure_for_async_send_size,
            MIN_ASYNC_SEND_SIZE,
            "semaphoreAsyncSendSize",
        ));
        let inner = Arc::new(Inner {
            cfg: RwLock::new(cfg),
            client: Mutex::new(None),
            started: AtomicBool::new(false),
            runtime: OnceLock::new(),
            stop: watch::channel(false).0,
            tasks: Mutex::new(Vec::new()),
            fault_strategy,
            metrics: ClientMetrics::new(),
            send_hooks: SendMessageHookList::new(),
            end_txn_hooks: EndTransactionHookList::new(),
            check_forbidden_hooks: CheckForbiddenHookList::new(),
            transaction_listener: RwLock::new(None),
            trace: Mutex::new(None),
            rpc_hook: RwLock::new(None),
            semaphore_async_send_num,
            semaphore_async_send_size,
            async_sender: Mutex::new(None),
            produce_accumulator: RwLock::new(None),
        });
        Ok(DefaultMQProducer { inner })
    }

    /// A snapshot of the configuration currently in effect.
    pub fn config(&self) -> ProducerConfig {
        self.inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn producer_group(&self) -> String {
        self.inner.producer_group()
    }

    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::Acquire)
    }

    /// The [`MQClientInstance`], or `None` before start.
    pub fn client(&self) -> Option<MQClientInstance> {
        self.inner.client()
    }

    /// The clientId actually used in this process (`None` before `start()`).
    pub fn client_id(&self) -> Option<String> {
        self.inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .client_id
            .clone()
    }

    // ---------------- Configuration ----------------

    /// Split on `;`, blank entries dropped.
    pub fn set_namesrv_addr(&self, addr: &str) {
        let addrs = addr
            .split(';')
            .map(str::trim)
            .filter(|a| !a.is_empty())
            .map(str::to_string)
            .collect();
        self.write_cfg(|c| c.name_server_addrs = addrs);
    }

    pub fn set_name_server_addresses(&self, addrs: Vec<String>) {
        self.write_cfg(|c| c.name_server_addrs = addrs);
    }

    /// Joined with `;`.
    pub fn namesrv_addr(&self) -> String {
        self.inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .name_server_addrs
            .join(";")
    }

    pub fn set_instance_name(&self, name: &str) {
        self.write_cfg(|c| c.instance_name = name.to_string());
    }

    pub fn set_unit_name(&self, unit_name: Option<&str>) {
        self.write_cfg(|c| c.unit_name = unit_name.map(str::to_string));
    }

    pub fn set_namespace_v2(&self, namespace_v2: Option<&str>) {
        self.write_cfg(|c| c.namespace_v2 = namespace_v2.map(str::to_string));
    }

    pub fn get_namespace_v2(&self) -> Option<String> {
        self.config().namespace_v2
    }

    pub fn set_socks_proxy(&self, socks_proxy: Option<SocksProxyConfig>) {
        self.write_cfg(|c| c.socks_proxy = socks_proxy);
    }

    pub fn get_socks_proxy(&self) -> Option<SocksProxyConfig> {
        self.config().socks_proxy
    }

    pub fn set_unit_mode(&self, unit_mode: bool) {
        self.write_cfg(|c| c.unit_mode = unit_mode);
    }

    pub fn set_enable_stream_request_type(&self, enable: bool) {
        self.write_cfg(|c| c.enable_stream_request_type = enable);
    }

    pub fn set_poll_name_server_interval_millis(&self, millis: u64) {
        self.write_cfg(|c| c.poll_name_server_interval_millis = millis);
    }

    /// Set the namespace.
    pub fn set_namespace(&self, namespace: &str) {
        self.write_cfg(|c| c.namespace = namespace.to_string());
    }

    pub fn set_topics(&self, topics: Vec<String>) {
        self.write_cfg(|c| c.topics = topics);
    }

    /// `tls_enable` (`None` = deferred to the `ROCKETMQ_TLS_ENABLE` environment variable).
    pub fn set_tls_enable(&self, tls_enable: Option<bool>) {
        self.write_cfg(|c| c.tls_enable = tls_enable);
    }

    /// `enable_trace_context` (`None` = deferred to the
    /// `ROCKETMQ_TRACE_CONTEXT_ENABLE` environment variable, fixed at startup).
    pub fn set_enable_trace_context(&self, enable: Option<bool>) {
        self.write_cfg(|c| c.enable_trace_context = enable);
    }

    pub fn set_client_id(&self, client_id: Option<&str>) {
        self.write_cfg(|c| {
            c.client_id = client_id.map(str::to_string);
        });
    }

    pub fn set_max_message_size(&self, size: i32) {
        self.write_cfg(|c| c.max_message_size = size);
    }

    pub fn set_send_msg_timeout(&self, timeout_millis: i64) {
        self.write_cfg(|c| c.send_msg_timeout = timeout_millis);
    }

    pub fn set_retry_times_when_send_failed(&self, n: i32) {
        self.write_cfg(|c| c.retry_times_when_send_failed = n);
    }

    pub fn set_retry_times_when_send_async_failed(&self, n: i32) {
        self.write_cfg(|c| c.retry_times_when_send_async_failed = n);
    }

    pub fn get_retry_times_when_send_async_failed(&self) -> i32 {
        self.read_cfg(|c| c.retry_times_when_send_async_failed)
    }

    /// Async send queue capacity (the `async_sender_queue_capacity` property),
    /// **only applied when `start()` builds the pool**.
    pub fn set_async_sender_queue_capacity(&self, capacity: i32) {
        self.write_cfg(|c| c.async_sender_queue_capacity = capacity);
    }

    /// Read the async send queue capacity.
    pub fn get_async_sender_queue_capacity(&self) -> i32 {
        self.read_cfg(|c| c.async_sender_queue_capacity)
    }

    pub fn set_retry_another_broker_when_not_store_ok(&self, retry: bool) {
        self.write_cfg(|c| c.retry_another_broker_when_not_store_ok = retry);
    }

    pub fn is_retry_another_broker_when_not_store_ok(&self) -> bool {
        self.read_cfg(|c| c.retry_another_broker_when_not_store_ok)
    }

    /// `-1` (the default) means no per-request cap; once a finite value is set, the per-request timeouts of the attempts that
    /// **still have retries left** are clamped to it, leaving the remainder for the next brokers -- otherwise one slow
    /// broker could eat the whole `send_msg_timeout` budget without a single attempt at the remaining ones.
    pub fn set_send_msg_max_timeout_per_request(&self, timeout_millis: i64) {
        self.write_cfg(|c| c.send_msg_max_timeout_per_request = timeout_millis);
    }

    pub fn get_send_msg_max_timeout_per_request(&self) -> i64 {
        self.read_cfg(|c| c.send_msg_max_timeout_per_request)
    }

    /// Add one broker response code to the retryable code set.
    pub fn add_retry_response_code(&self, response_code: i32) {
        self.write_cfg(|c| {
            c.retry_response_codes.insert(response_code);
        });
    }

    pub fn get_retry_response_codes(&self) -> BTreeSet<i32> {
        self.read_cfg(|c| c.retry_response_codes.clone())
    }

    /// `None` (no response code was ever received) means not retryable.
    pub fn is_retry_response_code(&self, response_code: Option<i32>) -> bool {
        match response_code {
            Some(code) => self.read_cfg(|c| c.retry_response_codes.contains(&code)),
            None => false,
        }
    }

    pub fn set_compress_msg_body_over_howmuch(&self, size: i32) {
        self.write_cfg(|c| c.compress_msg_body_over_howmuch = size);
    }

    pub fn set_compress_level(&self, level: i32) {
        self.write_cfg(|c| c.compress_level = level);
    }

    /// Set the compression algorithm (takes the **algorithm number**, e.g. [`MessageSysFlag::ZLIB_TYPE`]).
    pub fn set_compress_type(&self, compression_type: i32) -> Result<()> {
        let normalized = compression::validate_compression_type(compression_type)?;
        self.write_cfg(|c| c.compress_type = normalized);
        Ok(())
    }

    pub fn set_create_topic_key(&self, key: &str) {
        self.write_cfg(|c| c.create_topic_key = key.to_string());
    }

    pub fn set_default_topic_queue_nums(&self, n: i32) {
        self.write_cfg(|c| c.default_topic_queue_nums = n);
    }

    /// Heartbeat interval (direct property change).
    pub fn set_heartbeat_interval_millis(&self, millis: u64) {
        self.write_cfg(|c| c.heartbeat_interval_millis = millis);
    }

    /// Forbidden after start (the broker side already registered the old group name).
    pub fn set_producer_group(&self, group: &str) -> Result<()> {
        if self.is_started() {
            bail!("producerGroup cannot be changed after startup");
        }
        self.write_cfg(|c| c.producer_group = group.to_string());
        Ok(())
    }

    /// Changes the configuration and the strategy together
    /// (the strategy is shared through an `Arc`, so switching it at runtime also takes effect).
    pub fn set_send_latency_fault_enable(&self, enable: bool) {
        self.write_cfg(|c| c.send_latency_fault_enable = enable);
        self.inner
            .fault_strategy
            .set_send_latency_fault_enable(enable);
    }

    pub fn set_start_detector_enable(&self, enable: bool) {
        self.write_cfg(|c| c.start_detector_enable = enable);
    }

    pub fn is_start_detector_enable(&self) -> bool {
        self.read_cfg(|c| c.start_detector_enable)
    }

    pub fn set_detect_timeout(&self, timeout_millis: i64) {
        self.write_cfg(|c| c.detect_timeout = timeout_millis);
    }

    pub fn get_detect_timeout(&self) -> i64 {
        self.read_cfg(|c| c.detect_timeout)
    }

    pub fn set_detect_interval(&self, interval_millis: i64) {
        self.write_cfg(|c| c.detect_interval = interval_millis);
    }

    pub fn get_detect_interval(&self) -> i64 {
        self.read_cfg(|c| c.detect_interval)
    }

    pub fn set_request_timeout(&self, timeout_millis: i64) {
        self.write_cfg(|c| c.request_timeout = timeout_millis);
    }

    /// Decode-read-body switch (default true). Off, received bodies stay unmaterialised; queue
    /// offsets and properties are still decoded.
    pub fn set_decode_read_body(&self, enable: bool) {
        self.write_cfg(|c| c.decode_read_body = enable);
        if let Some(client) = self.client() {
            client.set_decode_read_body(enable);
        }
    }

    pub fn is_decode_read_body(&self) -> bool {
        self.read_cfg(|c| c.decode_read_body)
    }

    /// Internal-API timeout for the client's own control RPCs (default 3000ms).
    pub fn set_mq_client_api_timeout_millis(&self, timeout_millis: i64) {
        self.write_cfg(|c| c.mq_client_api_timeout_millis = timeout_millis);
        if let Some(client) = self.client() {
            client.set_mq_client_api_timeout_millis(timeout_millis);
        }
    }

    pub fn mq_client_api_timeout_millis(&self) -> i64 {
        self.read_cfg(|c| c.mq_client_api_timeout_millis)
    }

    /// Access the metrics collector.
    pub fn metrics(&self) -> &ClientMetrics {
        &self.inner.metrics
    }

    /// Expose the fault table to ease troubleshooting/testing.
    pub fn fault_strategy(&self) -> &MQFaultStrategy {
        &self.inner.fault_strategy
    }

    // ---------------- Async send backpressure ----------------

    pub fn set_enable_backpressure_for_async_mode(&self, enable: bool) {
        self.write_cfg(|c| c.enable_backpressure_for_async_mode = enable);
    }

    pub fn is_enable_backpressure_for_async_mode(&self) -> bool {
        self.read_cfg(|c| c.enable_backpressure_for_async_mode)
    }

    pub fn set_back_pressure_for_async_send_num(&self, num: i64) {
        let num = num.max(MIN_ASYNC_SEND_NUM);
        self.write_cfg(|c| c.back_pressure_for_async_send_num = num);
        self.inner.semaphore_async_send_num.set_total_permits(num);
    }

    /// The total number of in-flight async send permits (not the available count).
    pub fn get_back_pressure_for_async_send_num(&self) -> i64 {
        self.read_cfg(|c| c.back_pressure_for_async_send_num)
    }

    /// Change the in-flight **byte count** cap at runtime, with the same semantics as
    /// [`set_back_pressure_for_async_send_num`](Self::set_back_pressure_for_async_send_num).
    pub fn set_back_pressure_for_async_send_size(&self, size: i64) {
        let size = size.max(MIN_ASYNC_SEND_SIZE);
        self.write_cfg(|c| c.back_pressure_for_async_send_size = size);
        self.inner.semaphore_async_send_size.set_total_permits(size);
    }

    pub fn get_back_pressure_for_async_send_size(&self) -> i64 {
        self.read_cfg(|c| c.back_pressure_for_async_send_size)
    }

    pub fn semaphore_async_send_num_available_permits(&self) -> i64 {
        self.inner.semaphore_async_send_num.available_permits()
    }

    /// The number of free permits of the async send semaphore.
    pub fn semaphore_async_send_size_available_permits(&self) -> i64 {
        self.inner.semaphore_async_send_size.available_permits()
    }

    // ---------------- Message trace configuration ----------------

    /// ⚠ The trace dispatcher itself is **not created here** (implementation note 4): only after
    /// [`set_trace_dispatcher`](Self::set_trace_dispatcher) injects one does `start()` register
    /// the two trace hooks in a fixed order and start it.
    pub fn set_enable_trace(&self, enable: bool) {
        self.write_cfg(|c| c.enable_trace = enable);
    }

    pub fn is_enable_trace(&self) -> bool {
        self.inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .enable_trace
    }

    /// Trace topic (`None` → the system default trace topic).
    pub fn set_trace_topic(&self, trace_topic: Option<&str>) {
        self.write_cfg(|c| {
            c.trace_topic = trace_topic.map(str::to_string);
        });
    }

    pub fn set_trace_msg_batch_num(&self, n: i32) {
        self.write_cfg(|c| c.trace_msg_batch_num = n);
    }

    /// Inject the trace dispatcher (implementation note 4). Passing `None` revokes the injection.
    pub fn set_trace_dispatcher(&self, dispatcher: Option<Arc<dyn TraceDispatcherChannel>>) {
        *self.inner.trace.lock().unwrap_or_else(|e| e.into_inner()) = dispatcher;
    }

    /// The currently injected trace dispatcher.
    pub fn trace_dispatcher(&self) -> Option<Arc<dyn TraceDispatcherChannel>> {
        self.inner.trace_dispatcher()
    }

    /// `rpc_hook`; registered on the remoting channel at `start()`.
    pub fn set_rpc_hook(&self, hook: Option<Arc<dyn RPCHook>>) {
        *self
            .inner
            .rpc_hook
            .write()
            .unwrap_or_else(|e| e.into_inner()) = hook;
    }

    /// The rpc hook currently registered (passed through to the internal producer when auto-bridging the trace dispatcher).
    pub(crate) fn rpc_hook(&self) -> Option<Arc<dyn RPCHook>> {
        self.inner
            .rpc_hook
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    // ---------------- Hook registration ----------------

    pub fn register_send_message_hook(&self, hook: Arc<dyn SendMessageHook>) {
        self.inner.send_hooks.register(hook);
    }

    pub fn has_send_message_hook(&self) -> bool {
        self.inner.send_hooks.has_hooks()
    }

    pub fn register_check_forbidden_hook(&self, hook: Arc<dyn CheckForbiddenHook>) {
        self.inner.check_forbidden_hooks.register(hook);
    }

    pub fn has_check_forbidden_hook(&self) -> bool {
        self.inner.check_forbidden_hooks.has_hooks()
    }

    pub fn register_end_transaction_hook(&self, hook: Arc<dyn EndTransactionHook>) {
        self.inner.end_txn_hooks.register(hook);
    }

    /// If either kind of hook exists, the hook-aware send kernel must be used.
    fn has_send_interceptors(&self) -> bool {
        self.inner.send_hooks.has_hooks() || self.inner.check_forbidden_hooks.has_hooks()
    }

    /// Set/clear the transaction listener.
    pub fn set_transaction_listener(&self, listener: Option<Arc<dyn TransactionListener>>) {
        *self
            .inner
            .transaction_listener
            .write()
            .unwrap_or_else(|e| e.into_inner()) = listener;
    }

    /// Take the current transaction listener.
    pub fn transaction_listener(&self) -> Option<Arc<dyn TransactionListener>> {
        self.inner
            .transaction_listener
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The single entry point for writing configuration.
    fn write_cfg(&self, f: impl FnOnce(&mut ProducerConfig)) {
        f(&mut self.inner.cfg.write().unwrap_or_else(|e| e.into_inner()));
    }

    /// The read-only counterpart of [`Self::write_cfg`]: on a poisoned lock it also carries the old values on, never panicking while reading configuration.
    fn read_cfg<T>(&self, f: impl FnOnce(&ProducerConfig) -> T) -> T {
        f(&self.inner.cfg.read().unwrap_or_else(|e| e.into_inner()))
    }

    // ---------------- Lifecycle ----------------

    /// The start steps are fixed in order: validate addresses → generate clientId →
    /// wrap the producer group with the namespace → create/reuse the [`MQClientInstance`] → register the RPC hook →
    /// start the instance → backfill dynamically obtained namesrv addresses → register the transaction check-back processor → start the heartbeat task.
    pub async fn start(&self) -> Result<()> {
        // Idempotent (differences documented above); when already started, do not register hooks again and do not start a second heartbeat.
        if self
            .inner
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        let cfg = self.config();
        let group = if cfg.namespace.is_empty() {
            cfg.producer_group.clone()
        } else {
            NamespaceUtil::wrap_namespace(&cfg.namespace, &cfg.producer_group)
        };
        if let Err(e) = validators::check_group(&group) {
            self.inner.started.store(false, Ordering::Release);
            return Err(e);
        }
        if group == MixAll::DEFAULT_PRODUCER_GROUP {
            self.inner.started.store(false, Ordering::Release);
            return Err(Error::client(
                "producerGroup can not equal DEFAULT_PRODUCER, please specify another one.",
            ));
        }
        if cfg.name_server_addrs.is_empty() && !DefaultTopAddressing::is_configured() {
            self.inner.started.store(false, Ordering::Release);
            return Err(Error::client_with_code(
                client_error_code::NO_NAME_SERVER_EXCEPTION,
                "name server address is not set",
            ));
        }
        let instance_name = MixAll::change_instance_name_to_pid(&cfg.instance_name);
        let client_id = cfg.client_id.clone().unwrap_or_else(|| {
            MixAll::build_default_client_id(
                &instance_name,
                cfg.unit_name.as_deref(),
                cfg.enable_stream_request_type,
            )
        });
        // Resolve the None into a boolean: this is decided once, at start time.
        let trace_context_on = cfg
            .enable_trace_context
            .unwrap_or_else(trace_context_enabled_from_env);
        self.write_cfg(|c| {
            c.client_id = Some(client_id.clone());
            c.instance_name = instance_name;
            c.producer_group = group.clone();
            c.enable_trace_context = Some(trace_context_on);
        });

        let instance_cfg = MQClientInstanceConfig {
            tls_enable: cfg.tls_enable,
            socks_proxy: cfg.socks_proxy.clone(),
            unit_name: cfg.unit_name.clone(),
            namespace_v2: cfg.namespace_v2.clone(),
            enable_stream_request_type: cfg.enable_stream_request_type,
            route_refresh_interval_millis: cfg.poll_name_server_interval_millis,
            use_heartbeat_v2: cfg.use_heartbeat_v2,
            decode_read_body: cfg.decode_read_body,
            mq_client_api_timeout_millis: cfg.mq_client_api_timeout_millis,
            enable_concurrent_heartbeat: cfg.enable_concurrent_heartbeat,
            concurrent_heartbeat_thread_pool_size: cfg.concurrent_heartbeat_thread_pool_size,
            ..Default::default()
        };
        let client = MQClientInstance::create_mq_client_instance(
            &client_id,
            cfg.name_server_addrs.clone(),
            instance_cfg,
        );
        // A shared instance (same clientId) may predate this facade: apply the live switches.
        client.set_decode_read_body(cfg.decode_read_body);
        client.set_mq_client_api_timeout_millis(cfg.mq_client_api_timeout_millis);
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
        *self.inner.client.lock().unwrap_or_else(|e| e.into_inner()) = Some(client.clone());
        client.register_producer(&group);

        // Dynamic name server: the instance may have obtained addresses from the address server during start; backfill them into this producer
        if cfg.name_server_addrs.is_empty() {
            let addrs = client.name_server_addrs();
            if !addrs.is_empty() {
                self.write_cfg(|c| c.name_server_addrs = addrs);
            }
        }

        // Register the processor for broker-initiated requests: transaction check-back CHECK_TRANSACTION_STATE(39).
        // Match against this producer using the message_ext PGROUP property; drop it when it does not match.
        client.remoting_client().register_processor(
            request_code::CHECK_TRANSACTION_STATE,
            Arc::new(CheckTransactionStateProcessor {
                producer: Arc::downgrade(&self.inner),
            }),
        );

        if let Some(handle) = self.runtime_handle() {
            let capacity = cfg.async_sender_queue_capacity;
            let executor = AsyncSenderExecutor::new(&handle, capacity, async_sender_worker_count());
            *self
                .inner
                .async_sender
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(executor);
        }

        // Heartbeat task: periodically register ProducerData with every broker.
        // The broker's transaction check-back reaches the producer through exactly the channel registered in this step;
        // without heartbeats COMMIT/ROLLBACK still succeeds (the client issues END_TRANSACTION itself),
        // but half messages in UNKNOW state would **never be checked back**.
        if let Some(handle) = self.runtime_handle() {
            // The stop flag left over from the previous shutdown must be cleared, otherwise the heartbeat exits at once after a restart.
            let _ = self.inner.stop.send(false);
            let mut rx = self.inner.stop.subscribe();
            let weak = Arc::downgrade(&self.inner);
            let task = handle.spawn(async move {
                loop {
                    let Some(inner) = weak.upgrade() else {
                        return; // the producer was already released (shutdown was never called), no point continuing
                    };
                    let interval_millis = inner
                        .cfg
                        .read()
                        .unwrap_or_else(|e| e.into_inner())
                        .heartbeat_interval_millis;
                    if let Some(client) = inner.client() {
                        send_heartbeat_to_all_broker(&inner, &client).await;
                    }
                    drop(inner);
                    // The watch channel replaces 1s-granularity polling:
                    // either wait until the deadline or get woken up.
                    if tokio::time::timeout(
                        Duration::from_millis(interval_millis.max(1000)),
                        rx.changed(),
                    )
                    .await
                    .is_ok()
                    {
                        return; // stop signal (or the sending side was already released)
                    }
                }
            });
            self.push_task(task);
        } else {
            // Without a runtime the heartbeat cannot run: sending itself remains usable (a thread can always be spawned in a thread model).
            // Warn explicitly here so that users do not silently lose the transaction check-back capability.
            rmq_warn!(
                "producer: no tokio runtime, heartbeat disabled; broker transaction \
                       check-back will not reach this producer"
            );
        }

        if let Some(handle) = self.runtime_handle() {
            let weak = Arc::downgrade(&self.inner);
            let task = handle.spawn(async move {
                loop {
                    tokio::time::sleep(DETECTOR_SCHEDULE_PERIOD).await;
                    let Some(inner) = weak.upgrade() else {
                        return; // the producer was already released (shutdown was never called), no point continuing
                    };
                    let (enable, detect_timeout, detect_interval) = {
                        let cfg = inner.cfg.read().unwrap_or_else(|e| e.into_inner());
                        (cfg.start_detector_enable, cfg.detect_timeout, cfg.detect_interval)
                    };
                    if !enable {
                        continue;
                    }
                    if let Some(client) = inner.client() {
                        run_detect_round(&inner, &client, detect_timeout, detect_interval).await;
                    }
                }
            });
            self.push_task(task);
        }

        self.start_trace_dispatcher();

        start_expired_request_scan();
        Ok(())
    }

    /// Shut the producer down.
    ///
    /// ⚠ Unlike the older shape that returned right after spawning: the code 35 unregister and the instance teardown run on the
    /// runtime, but this method **blocks until they land** (capped by [`SHUTDOWN_FINALIZE_BUDGET`]).
    /// "call `std::process::exit` immediately after shutdown" now also gets that frame out; inside a current_thread
    /// runtime blocking is impossible (it would panic / deadlock), and there the finalize stays a detached task with a warning
    /// (see cases 2/3 of [`run_finalize_blocking`]).
    pub fn shutdown(&self) {
        if self
            .inner
            .started
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let _ = self.inner.stop.send(true);
        for task in self
            .inner
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            task.abort();
        }
        stop_expired_request_scan();
        // ⚠ Drop the instance reference here (merely keeping it would make the next start create and overwrite INSTANCE_MAP),
        // so that `create_mq_client_instance` builds a clean new instance
        // on restart instead of reusing one that was already shut down.
        let client = self
            .inner
            .client
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let executor = self
            .inner
            .async_sender
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(executor) = executor {
            executor.close();
        }
        if let Some(client) = client {
            client.unregister_producer(&self.inner.producer_group());
            client.detach_from_registry_if_last_tenant();
            match (self.runtime_handle(), self.client_id()) {
                (Some(handle), Some(client_id)) => {
                    let group = self.inner.producer_group();
                    let finalize = async move {
                        client
                            .unregister_client_all_brokers(
                                &client_id,
                                &group,
                                "",
                                client.mq_client_api_timeout_millis(),
                            )
                            .await;
                        client.shutdown();
                    };
                    let _ = run_finalize_blocking(
                        &handle,
                        "producer shutdown",
                        SHUTDOWN_FINALIZE_BUDGET,
                        finalize,
                    );
                }
                // No runtime: this unregister cannot be sent, but at least hand the instance back.
                _ => {
                    rmq_warn!("producer shutdown: no tokio runtime, skip broker unregister");
                    client.shutdown();
                }
            }
        }
        if let Some(dispatcher) = self.inner.trace_dispatcher() {
            dispatcher.shutdown();
        }
    }

    fn require_client(&self) -> Result<MQClientInstance> {
        if !self.is_started() {
            return Err(Error::client("producer not started, call start() first"));
        }
        self.inner
            .client()
            .ok_or_else(|| Error::client("producer not started, call start() first"))
    }

    /// Implementation note 3: lazily bound exactly like `remoting::client` -- use the cache when present, otherwise
    /// take it from the current runtime context (`start()` is async, so it is necessarily inside a runtime).
    fn runtime_handle(&self) -> Option<tokio::runtime::Handle> {
        if let Some(handle) = self.inner.runtime.get() {
            return Some(handle.clone());
        }
        let handle = tokio::runtime::Handle::try_current().ok()?;
        let _ = self.inner.runtime.set(handle.clone());
        Some(handle)
    }

    fn push_task(&self, task: JoinHandle<()>) {
        self.inner
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(task);
    }

    /// An injected dispatcher wins ([`set_trace_dispatcher`](Self::set_trace_dispatcher), implementation note 4);
    /// when `enable_trace=true` and none was injected, one is built automatically through [`auto_bridged_dispatcher`]
    /// -- matching the automatic wiring used on the host side. Any failure is only logged -- broken tracing must not affect normal sending.
    fn start_trace_dispatcher(&self) {
        let cfg = self.config();
        let dispatcher: Arc<dyn TraceDispatcherChannel> = match self.inner.trace_dispatcher() {
            Some(d) => d,
            None if cfg.enable_trace => {
                match auto_bridged_dispatcher(
                    &self.inner.producer_group(),
                    TraceDispatcherType::Produce,
                    self.rpc_hook(),
                    cfg.trace_msg_batch_num,
                    cfg.trace_topic.clone(),
                ) {
                    Ok(d) => {
                        // Attach the host to the dispatcher
                        d.set_host_producer(Arc::new(self.clone()));
                        let channel: Arc<dyn TraceDispatcherChannel> = d;
                        self.set_trace_dispatcher(Some(channel.clone()));
                        channel
                    }
                    Err(e) => {
                        // The error text is verified verbatim
                        rmq_error!(
                            "system mqtrace hook init failed ,maybe can't send msg trace data: {e}"
                        );
                        return;
                    }
                }
            }
            None => return,
        };
        if cfg.enable_trace {
            // A restart must not register the trace hooks a second time (when each new dispatcher is created
            // the hooks are also attached only once).
            let sink: Arc<dyn TraceReportSink> = Arc::new(SinkAdapter(dispatcher.clone()));
            if !has_hook_named(&self.inner.send_hooks.hooks(), "SendMessageTraceHook") {
                self.register_send_message_hook(Arc::new(SendMessageTraceHook::new(sink.clone())));
            }
            if !has_end_hook_named(&self.inner.end_txn_hooks.hooks(), "EndTransactionTraceHook") {
                self.register_end_transaction_hook(Arc::new(EndTransactionTraceHook::new(sink)));
            }
        }
        if let Err(e) = dispatcher.start(&self.namesrv_addr()) {
            rmq_warn!("trace dispatcher start failed: {e}");
        }
    }
}

/// Deduplicate by `hook_name()` ("register only when a new dispatcher is created" would not duplicate anyway).
fn has_hook_named(hooks: &[Arc<dyn SendMessageHook>], name: &str) -> bool {
    hooks.iter().any(|h| h.hook_name() == name)
}

fn has_end_hook_named(hooks: &[Arc<dyn EndTransactionHook>], name: &str) -> bool {
    hooks.iter().any(|h| h.hook_name() == name)
}

// ================================================================ Active probing for latency fault avoidance

const DETECTOR_SCHEDULE_PERIOD: Duration = Duration::from_secs(3);

async fn run_detect_round(
    inner: &Inner,
    client: &MQClientInstance,
    detect_timeout: i64,
    detect_interval: i64,
) {
    let resolver = PublishAddrResolver { client: client.clone() };
    let detector = MaxOffsetServiceDetector { client: client.clone() };
    inner
        .fault_strategy
        .latency_fault_tolerance()
        .detect_by_one_round(&resolver, &detector, detect_timeout, detect_interval)
        .await;
}

struct PublishAddrResolver {
    client: MQClientInstance,
}

impl Resolver for PublishAddrResolver {
    fn resolve(&self, broker_name: &str) -> Option<String> {
        self.client.find_broker_address_in_publish(broker_name)
    }
}

struct MaxOffsetServiceDetector {
    client: MQClientInstance,
}

impl ServiceDetector for MaxOffsetServiceDetector {
    fn detect(
        &self,
        broker_name: &str,
        endpoint: &str,
        timeout_millis: i64,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        let client = self.client.clone();
        let broker_name = broker_name.to_string();
        let endpoint = endpoint.to_string();
        Box::pin(async move {
            let Some(topic) = client.pick_topic_for_broker(&broker_name) else {
                return false;
            };
            let mq = MessageQueue::new(&topic, &broker_name, 0);
            client
                .get_max_offset(&mq, timeout_millis, Some(&endpoint))
                .await
                .is_ok()
        })
    }
}

// ================================================================ Trace auto-bridging

/// Build a **real** internal producer and assemble it together with the dispatcher.
///
/// The three settings `send_msg_timeout=5000` / `max_msg_size=128000` / `enable_trace=false` are
/// applied to the producer in a fixed order by [`AsyncTraceDispatcher::with_config`], so they are not repeated here.
///
/// Division of labour with the injection style: a caller-injected [`TraceDispatcherChannel`] takes priority (the contract of implementation note 4);
/// when `enable_trace=true` and none was injected, `start_trace_dispatcher` builds one here automatically,
/// matching the automatic wiring on both the produce and consume sides -- tracing works out of the box, no hand-written bridging required.
pub fn auto_bridged_dispatcher(
    host_group: &str,
    dispatcher_type: TraceDispatcherType,
    rpc_hook: Option<Arc<dyn RPCHook>>,
    batch_num: i32,
    trace_topic: Option<String>,
) -> Result<Arc<AsyncTraceDispatcher>> {
    let producer_group =
        AsyncTraceDispatcher::next_trace_producer_group(host_group, dispatcher_type);
    let producer = DefaultMQProducer::with_rpc_hook(&producer_group, rpc_hook)?;
    Ok(Arc::new(AsyncTraceDispatcher::with_config(
        host_group,
        dispatcher_type,
        TraceDispatcherConfig {
            producer: Some(Arc::new(producer)),
            trace_producer_group: Some(producer_group),
            // `min(batch_num, 20)`; a negative number is meaningless for usize, so clamp it to 0.
            batch_num: batch_num.clamp(0, 20) as usize,
            trace_topic_name: trace_topic,
            ..Default::default()
        },
    )))
}

/// The dispatcher's host side: when the dispatcher reads a clientId it reads
/// the clientId of the host's [`MQClientInstance`], which is an empty string before start.
impl TraceHost for DefaultMQProducer {
    fn client_id(&self) -> String {
        DefaultMQProducer::client_id(self).unwrap_or_default()
    }
}

/// The real implementation of the [`TraceProducer`] seam (the internal producer already is a real
/// `DefaultMQProducer`; if the seam only had a test-stub implementation,
/// tracing would never send anything without hand-written bridging).
impl TraceProducer for DefaultMQProducer {
    fn set_name_server_addr(&self, name_server_addr: &str) {
        DefaultMQProducer::set_name_server_addresses(self, vec![name_server_addr.to_string()]);
    }

    fn set_instance_name(&self, instance_name: &str) {
        DefaultMQProducer::set_instance_name(self, instance_name);
    }

    fn set_namespace_v2(&self, namespace_v2: &str) {
        DefaultMQProducer::set_namespace_v2(self, Some(namespace_v2));
    }

    fn set_send_msg_timeout(&self, timeout_millis: i64) {
        DefaultMQProducer::set_send_msg_timeout(self, timeout_millis);
    }

    fn set_max_message_size(&self, max_msg_size: usize) {
        DefaultMQProducer::set_max_message_size(self, max_msg_size as i32);
    }

    fn set_enable_trace(&self, enable: bool) {
        DefaultMQProducer::set_enable_trace(self, enable);
    }

    fn start(self: Arc<Self>) -> TraceProducerFuture<Result<()>> {
        Box::pin(async move { DefaultMQProducer::start(&self).await })
    }

    fn shutdown(&self) {
        DefaultMQProducer::shutdown(self);
    }

    fn send(
        self: Arc<Self>,
        mut message: Message,
        timeout_millis: i64,
    ) -> TraceProducerFuture<Result<()>> {
        Box::pin(async move {
            // The internal producer has enable_trace=false (turned off by with_config), so send produces no trace of its own;
            // Some(timeout) bypasses auto batching, which is equivalent to a direct synchronous send.
            DefaultMQProducer::send(&self, &mut message, Some(timeout_millis), None)
                .await
                .map(|_| ())
        })
    }

    fn send_to_queue(
        self: Arc<Self>,
        mut message: Message,
        queue: MessageQueue,
        timeout_millis: i64,
    ) -> TraceProducerFuture<Result<()>> {
        Box::pin(async move {
            DefaultMQProducer::send(&self, &mut message, Some(timeout_millis), Some(&queue))
                .await
                .map(|_| ())
        })
    }

    fn publish_queues(
        self: Arc<Self>,
        topic: String,
    ) -> TraceProducerFuture<Result<Vec<MessageQueue>>> {
        Box::pin(
            async move { DefaultMQProducer::fetch_publish_message_queues(&self, &topic).await },
        )
    }
}

/// Send a heartbeat (including ProducerData) to every known broker.
///
/// Returns the number of brokers that accepted it; a single failure is only logged as warn.
async fn send_heartbeat_to_all_broker(inner: &Inner, client: &MQClientInstance) -> usize {
    let group = inner.producer_group();
    let addrs = client.get_route_of_all_brokers();
    if addrs.is_empty() {
        return 0;
    }
    let mut hb = HeartbeatData::new(client.client_id().to_string());
    hb.add_producer_data(ProducerData::new(group));
    // heartbeatFingerprint / withoutSub keep their defaults: absent from the payload the
    // broker deserializes them as 0/false and takes the V1 registration path, which is the
    // same effect as pinning them to 0/false explicitly.
    let mut ok = 0usize;
    for addr in addrs {
        if let Err(e) = client
            .send_heartbeat(&addr, &hb, client.mq_client_api_timeout_millis())
            .await
        {
            rmq_warn!("producer heartbeat to {addr} failed: {e}");
        } else {
            ok += 1;
        }
    }
    ok
}

/// The coarse judgement "is this failure worth asking again": everything matching `except (MQClientException, MQBrokerException,
/// RemotingException)` counts, any other error (for instance batch composition errors or argument validation)
/// propagates straight up.
///
/// Only [`DefaultMQProducer::topic_publish_info`] uses it now to decide "whether to force one route refresh";
/// the main send loop switched to the finer [`classify_send_error`] (the three error tiers are recorded differently in the fault table).
///
/// [`Error::RequestTimeout`] is also in the retryable set -- on the wire protocol it is one kind of `MQClientException`
/// and gets caught by the same tier (the send path itself never produces it; it is listed only for completeness).
fn retryable(err: &Error) -> bool {
    matches!(
        err,
        Error::Client { .. }
            | Error::Broker { .. }
            | Error::Server { .. }
            | Error::RemotingCommand(_)
            | Error::Connect { .. }
            | Error::SendRequest { .. }
            | Error::Timeout { .. }
            | Error::TooMuchRequest(_)
            | Error::RequestTimeout { .. }
    )
}

/// There is no exception inheritance tree to dispatch on by type, so the [`Error`] variants map one-to-one
/// (see [`classify_send_error`]). The three tiers differ both in the **fault table** and in **whether the retry continues**,
/// so they must not be merged into a single `retryable` check the way early versions did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SendErrorKind {
    /// `MQBrokerException`: the broker explicitly answered with a business error code ([`Error::Broker`]).
    Broker,
    /// `RemotingException` and its subclasses: cannot connect / cannot send out / timed out waiting for the response.
    Remoting,
    /// `MQClientException` and its subclasses: a problem on the client side itself.
    Client,
}

/// Classify the [`Error`] of one failed send into its error tier;
/// `None` = no tier catches it (validation error, decode error, local IO error), so it propagates straight up.
///
/// [`Error::RequestTimeout`] belongs to the `MQClientException` family
/// (`RequestTimeoutException` derives from `MQClientException`), which is why it and
/// [`Error::Timeout`] (the remoting layer's `RemotingTimeoutException`) land in two different tiers;
/// that is deliberate: the first means the broker did receive the request, the second never got through the channel at all.
fn classify_send_error(err: &Error) -> Option<SendErrorKind> {
    match err {
        Error::Broker { .. } => Some(SendErrorKind::Broker),
        Error::Connect { .. }
        | Error::SendRequest { .. }
        | Error::Timeout { .. }
        | Error::TooMuchRequest(_)
        | Error::RemotingCommand(_)
        | Error::Server { .. } => Some(SendErrorKind::Remoting),
        Error::Client { .. } | Error::RequestTimeout { .. } => Some(SendErrorKind::Client),
        Error::Decode(_) | Error::Encode(_) | Error::Io(_) | Error::Json(_) => None,
    }
}

/// Milliseconds elapsed from `began` (a start point taken from [`monotonic_millis`]) until now, rounded down.
///
/// Converting the monotonic clock difference to milliseconds: `as i64` on a positive float
/// truncates toward zero, which is the same semantics. Computing this with the wall clock (`current_time_millis`) does not work -- a
/// clock rollback yields a negative latency, which would record a slow broker as "very fast".
fn latency_since(began: f64) -> i64 {
    (monotonic_millis() - began) as i64
}

fn back_pressure_msg_len(msg: &Message) -> i64 {
    let len = msg.get_body().len();
    if len == 0 {
        return 1;
    }
    i64::try_from(len).unwrap_or(i64::MAX)
}

/// How many "byte" permits a batch send deducts: accumulated per message (an empty body still counts as 1), and 1 when the whole batch is empty --
/// the same measure as the backpressure length of batch messages.
/// Giving one value for the whole batch would let batch messages bypass the byte gate: a batch of 100 would occupy only 1 unit of capacity.
fn batch_back_pressure_msg_len(msgs: &[Message]) -> i64 {
    let mut total: i64 = 0;
    for msg in msgs {
        total = total.saturating_add(back_pressure_msg_len(msg));
    }
    if total == 0 {
        return 1;
    }
    total
}

/// Return only the permits **actually acquired this time**: when the byte gate timed out but the count gate was already acquired,
/// the count must be handed back, otherwise every trip through backpressure permanently eats one unit of capacity.
///
/// The return happens in `Drop` rather than in the caller's hands: this chain has four exits -- gate rejection, budget re-check,
/// preparation-stage failure and network failure -- and missing one return site leaks capacity permanently, and the leaked capacity is invisible
/// in unit tests and only shows up in a long run as "all async sends time out together after a handful of sends".
struct SendPermits {
    inner: Arc<Inner>,
    num_acquired: bool,
    size_acquired: bool,
    msg_len: i64,
}

impl Drop for SendPermits {
    fn drop(&mut self) {
        if self.size_acquired {
            self.inner.semaphore_async_send_size.release(self.msg_len);
        }
        if self.num_acquired {
            self.inner.semaphore_async_send_num.release(1);
        }
    }
}

/// The keys that identify a "delay-type" property.
///
/// `__STARTDELIVERTIME` / `TIMER_*` have no constants in this crate's `message_const`
/// (hard-coded literals plus a fallback are used here too), keeping the two consistent.
const DELAY_PROPERTY_KEYS: [&str; 5] = [
    "__STARTDELIVERTIME",
    crate::common::message_const::PROPERTY_DELAY_TIME_LEVEL,
    "TIMER_DELIVER_MS",
    "TIMER_DELAY_SEC",
    "TIMER_DELAY_MS",
];

fn max_delay_value(msg: &Message) -> i64 {
    let mut max = 0i64;
    if let Some(level) = msg.get_delay_time_level() {
        max = level.trim().parse::<i64>().unwrap_or(0);
    }
    for key in TIMER_DELAY_KEYS {
        if let Some(raw) = msg.get_property(key) {
            let value = raw.trim().parse::<i64>().unwrap_or(0);
            if value > max {
                max = value;
            }
        }
    }
    max
}

/// ⚠ Neither entry point goes through the public [`DefaultMQProducer::send`] / `send_async` --
/// that would evaluate `autoBatch` a second time, and the caller arrived here precisely because batching was rejected, so looping back
/// to the public entry points would be infinite recursion. They call the two kernels dedicated to "already encoded batches" instead.
impl AccumulatorSender for DefaultMQProducer {
    fn send_direct_blocking(
        &self,
        batch: MessageBatch,
        mq: Option<&MessageQueue>,
    ) -> Result<SendResult> {
        let handle = self.runtime_handle().ok_or_else(|| {
            Error::client("send_direct_blocking needs a tokio runtime; call start() first")
        })?;
        let mut batch = batch;
        handle.block_on(self.send_prepared_batch(&mut batch, mq, None))
    }

    fn send_direct_async(
        &self,
        batch: MessageBatch,
        mq: Option<&MessageQueue>,
        callback: Arc<dyn SendCallback>,
    ) -> Result<()> {
        self.send_prepared_batch_async(batch, mq.cloned(), callback, None)
    }
}

// ================================================================ Send path

impl DefaultMQProducer {
    // ---------------- Validation / compression / routing ----------------

    /// Message validation before sending.
    ///
    /// All decisions are consolidated in [`validators`]: topic (blank / 127 / character table) → forbidden topics →
    /// body (null / zero length / over max_message_size) → INNER_MULTI_DISPATCH separator,
    /// with order and error texts verified verbatim.
    fn check_message(&self, msg: &Message) -> Result<()> {
        let max = self
            .inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .max_message_size;
        validators::check_message(msg, max)
    }

    /// When the threshold is met, compress **in place** and return the sys_flag to send downstream.
    fn try_to_compress_message(&self, msg: &mut Message) -> (i32, Option<Vec<u8>>) {
        let (threshold, compress_type, level) = {
            let cfg = self.inner.cfg.read().unwrap_or_else(|e| e.into_inner());
            (
                cfg.compress_msg_body_over_howmuch,
                cfg.compress_type,
                cfg.compress_level,
            )
        };
        let body = msg.get_body();
        if body.is_empty() || (body.len() as i32) < threshold {
            return (0, None);
        }
        let compressed = match compression::compress(body, compress_type, level) {
            Ok(c) => c,
            Err(e) => {
                rmq_warn!("tryToCompressMessage failed, send uncompressed: {e}");
                return (0, None);
            }
        };
        if compressed.is_empty() {
            return (0, None);
        }
        let prev_body = msg.body.take();
        msg.body = Some(compressed);
        (
            MessageSysFlag::set_compression_type(MessageSysFlag::COMPRESSED_FLAG, compress_type),
            prev_body,
        )
    }

    /// What breaks if it is not restored: compression swaps the body in place, so a caller holding the same object and sending a
    /// second time would compress the compressed stream as if it were plain text (zlib(zlib(x))), and since the length is now below the threshold
    /// the compression flag would not be set either -- the broker stores it, the consumer receives it, the application decodes garbage, and nothing reports an error at any point.
    fn restore_caller_message(&self, msg: &mut Message, prev_body: Option<Vec<u8>>) {
        if let Some(prev) = prev_body {
            msg.body = Some(prev);
        }
        msg.topic = NamespaceUtil::without_namespace(&msg.topic, &self.inner.namespace());
    }

    /// The compression decision for the batch case (batches return 0 directly).
    ///
    /// Batches are **never compressed** (the concatenated body produced by `MessageBatch.encode()` is not compressed), so
    /// no `prevBody` is needed here either: the returned flag is always 0 and the caller has no body to restore.
    fn sys_flag_for(&self, msg: &mut PublishMessage<'_>) -> i32 {
        if msg.is_batch() {
            return 0;
        }
        self.try_to_compress_message(msg.as_message_mut()).0
    }

    fn with_namespace(&self, topic: &str) -> String {
        let namespace = self.inner.namespace();
        if namespace.is_empty() {
            return topic.to_string();
        }
        NamespaceUtil::wrap_namespace(&namespace, topic)
    }

    fn check_pinned_topic(&self, msg_topic: &str, mq: &MessageQueue, message: &str) -> Result<()> {
        if self.with_namespace(msg_topic) != self.with_namespace(&mq.topic) {
            return Err(Error::client(message));
        }
        Ok(())
    }

    async fn topic_publish_info(
        &self,
        client: &MQClientInstance,
        topic: &str,
    ) -> Result<Arc<TopicPublishInfo>> {
        client.register_topic_in_use(topic);
        match client.get_topic_publish_info(topic, false).await {
            Ok(info) => Ok(info),
            Err(e) if retryable(&e) => match client.get_topic_publish_info(topic, true).await {
                Ok(info) => Ok(info),
                Err(e) => {
                    self.validate_name_server_setting(client)?;
                    Err(e)
                }
            },
            Err(e) => Err(e),
        }
    }

    /// Runs only on the "route could not be fetched" branch, separating two completely different failures:
    /// * not a single name server address at all (an address server configured but returning no address counts too)
    ///   → 10004 `No name server address, please set it.`
    /// * addresses exist, only this topic has no route → let the original exception propagate as is (the caller qualifies it as 10005).
    fn validate_name_server_setting(&self, client: &MQClientInstance) -> Result<()> {
        if client.name_server_addrs().is_empty() {
            return Err(Error::client_with_code(
                client_error_code::NO_NAME_SERVER_EXCEPTION,
                "No name server address, please set it.",
            ));
        }
        Ok(())
    }

    async fn need_addr(&self, client: &MQClientInstance, mq: &MessageQueue) -> Result<String> {
        client.publish_addr_for(&mq.broker_name, &mq.topic).await
    }

    // ---------------- Hook context ----------------

    /// The msgType decision order is also copied over: `TRAN_MSG=true` → Trans_Msg_Half; any delay-type property
    /// → Delay_Msg; otherwise Normal_Msg.
    ///
    /// ⚠ The context carries a message snapshot taken **before the send** (later modifications on the send path are invisible to hooks);
    /// [`SendMessageContext::message`] is owned, so only a snapshot from **before the send** can be taken --
    /// the trace hooks read exactly the topic/tags/keys/bodyLength that were already fixed before the send, so there is no behavioural difference.
    fn build_send_context(
        &self,
        msg: &Message,
        group: &str,
        namespace: &str,
        mq: &MessageQueue,
        broker_addr: &str,
        mode: CommunicationMode,
    ) -> SendMessageContext {
        let mut context = SendMessageContext {
            producer_group: group.to_string(),
            message: Some(msg.clone()),
            mq: Some(mq.clone()),
            broker_addr: broker_addr.to_string(),
            communication_mode: Some(mode),
            namespace: namespace.to_string(),
            ..Default::default()
        };
        if msg.get_property(PROPERTY_TRANSACTION_PREPARED) == Some("true") {
            context.msg_type = MessageType::TransMsgHalf;
        }
        if DELAY_PROPERTY_KEYS
            .iter()
            .any(|key| msg.get_property(key).is_some())
        {
            context.msg_type = MessageType::DelayMsg;
        }
        context
    }

    /// Build the context and execute it (the exception is **not swallowed**,
    /// see [`execute_check_forbidden_hook`]).
    fn execute_check_forbidden(
        &self,
        msg: &Message,
        mq: &MessageQueue,
        broker_addr: &str,
        arg: Option<AnyHolder>,
        mode: CommunicationMode,
    ) -> Result<()> {
        let mut context = CheckForbiddenContext {
            name_srv_addr: self.namesrv_addr(),
            group: self.inner.producer_group(),
            message: Some(msg.clone()),
            mq: Some(mq.clone()),
            broker_addr: broker_addr.to_string(),
            communication_mode: Some(mode),
            arg,
            unit_mode: self.inner.unit_mode(),
            ..Default::default()
        };
        crate::client::hook::execute_check_forbidden_hook(
            &self.inner.check_forbidden_hooks,
            &mut context,
        )
    }

    // ---------------- Send kernel ----------------

    #[allow(clippy::too_many_arguments)]
    async fn send_with_hooks(
        &self,
        client: &MQClientInstance,
        msg: &mut PublishMessage<'_>,
        mq_sel: &MessageQueue,
        timeout: i64,
        sys_flag: i32,
        arg: Option<AnyHolder>,
        mode: CommunicationMode,
    ) -> Result<SendResult> {
        let group = self.inner.producer_group();
        let namespace = self.inner.namespace();
        let trace_context_on = self
            .inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .enable_trace_context
            .unwrap_or(false);
        let unit_mode = self.inner.unit_mode();
        let broker_addr = client
            .publish_addr_for(&mq_sel.broker_name, &mq_sel.topic)
            .await?;
        if self.has_check_forbidden_hook() {
            self.execute_check_forbidden(msg.as_message(), mq_sel, &broker_addr, arg, mode)?;
        }
        // W3C traceparent pass-through (opt-in): inject the root context when absent, never overwrite an existing value
        if trace_context_on {
            inject_trace_context(msg.as_message_mut());
        }
        if !self.has_send_message_hook() {
            return client
                .send_message(
                    &group,
                    msg,
                    mq_sel,
                    timeout,
                    sys_flag,
                    unit_mode,
                    &self.inner.create_topic_key(),
                    self.inner.default_topic_queue_nums(),
                )
                .await;
        }
        let mut context = self.build_send_context(
            msg.as_message(),
            &group,
            &namespace,
            mq_sel,
            &broker_addr,
            mode,
        );
        crate::client::hook::execute_send_message_hook_before(&self.inner.send_hooks, &mut context);
        match client
            .send_message(
                &group,
                msg,
                mq_sel,
                timeout,
                sys_flag,
                unit_mode,
                &self.inner.create_topic_key(),
                self.inner.default_topic_queue_nums(),
            )
            .await
        {
            Ok(result) => {
                context.send_result = Some(result.clone());
                crate::client::hook::execute_send_message_hook_after(
                    &self.inner.send_hooks,
                    &mut context,
                );
                Ok(result)
            }
            Err(e) => {
                context.exception = Some(e);
                crate::client::hook::execute_send_message_hook_after(
                    &self.inner.send_hooks,
                    &mut context,
                );
                // Hooks only read exception; take it back and raise it as is (Error is not Clone).
                match context.exception.take() {
                    Some(e) => Err(e),
                    None => Err(Error::client("send message failed")),
                }
            }
        }
    }


    pub fn produce_accumulator(&self) -> Option<ProduceAccumulator> {
        self.inner
            .produce_accumulator
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn init_produce_accumulator(&self) -> Result<()> {
        let client_id = self
            .read_cfg(|c| c.client_id.clone())
            .unwrap_or_else(|| DEFAULT_INSTANCE_NAME.to_string());
        let sender: Arc<dyn AccumulatorSender> = Arc::new(self.clone());
        let accumulator = get_or_create_produce_accumulator(&client_id, sender);
        let (delay_ms, bytes, total) = self.read_cfg(|c| {
            (
                c.batch_max_delay_ms,
                c.batch_max_bytes,
                c.total_batch_max_bytes,
            )
        });
        if delay_ms > -1 {
            accumulator.batch_max_delay_ms(delay_ms)?;
        }
        if bytes > -1 {
            accumulator.batch_max_bytes(bytes)?;
        }
        if total > -1 {
            accumulator.total_batch_max_bytes(total)?;
        }
        *self
            .inner
            .produce_accumulator
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(accumulator);
        Ok(())
    }

    pub fn get_auto_batch(&self) -> bool {
        if self.produce_accumulator().is_none() {
            return false;
        }
        self.read_cfg(|c| c.auto_batch)
    }

    pub fn set_auto_batch(&self, auto_batch: bool) {
        self.write_cfg(|c| c.auto_batch = auto_batch);
    }

    pub fn get_batch_max_delay_ms(&self) -> i64 {
        self.produce_accumulator()
            .map_or(0, |a| a.get_batch_max_delay_ms())
    }

    pub fn set_batch_max_delay_ms(&self, hold_ms: i64) -> Result<()> {
        self.write_cfg(|c| c.batch_max_delay_ms = hold_ms);
        if let Some(a) = self.produce_accumulator() {
            a.batch_max_delay_ms(hold_ms)?;
        }
        Ok(())
    }

    pub fn get_batch_max_bytes(&self) -> i64 {
        self.produce_accumulator()
            .map_or(0, |a| a.get_batch_max_bytes())
    }

    pub fn set_batch_max_bytes(&self, hold_size: i64) -> Result<()> {
        self.write_cfg(|c| c.batch_max_bytes = hold_size);
        if let Some(a) = self.produce_accumulator() {
            a.batch_max_bytes(hold_size)?;
        }
        Ok(())
    }

    pub fn get_total_batch_max_bytes(&self) -> i64 {
        self.produce_accumulator()
            .map_or(0, |a| a.get_total_batch_max_bytes())
    }

    pub fn set_total_batch_max_bytes(&self, total_hold_size: i64) -> Result<()> {
        self.write_cfg(|c| c.total_batch_max_bytes = total_hold_size);
        if let Some(a) = self.produce_accumulator() {
            a.total_batch_max_bytes(total_hold_size)?;
        }
        Ok(())
    }

    pub fn can_batch(&self, msg: &Message) -> bool {
        let Some(accumulator) = self.produce_accumulator() else {
            return false;
        };
        if !accumulator.try_add_message(msg) {
            return false;
        }
        if max_delay_value(msg) > 0 {
            return false;
        }
        if msg
            .get_topic()
            .starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX)
        {
            return false;
        }
        if msg.get_property(PROPERTY_PRODUCER_GROUP).is_some() {
            return false;
        }
        true
    }

    fn check_and_stamp(&self, msg: &mut Message) -> Result<()> {
        let max = self.read_cfg(|c| c.max_message_size);
        validators::check_message(msg, max)?;
        set_uniq_id(msg);
        Ok(())
    }

    pub async fn send_by_accumulator(
        &self,
        mut msg: Message,
        mq: Option<&MessageQueue>,
    ) -> Result<SendResult> {
        if !self.can_batch(&msg) {
            return self.send_inner(&mut msg, None, mq).await;
        }
        self.check_and_stamp(&mut msg)?;
        let accumulator = self
            .produce_accumulator()
            .ok_or_else(|| Error::client("produceAccumulator is null"))?;
        let mq_owned = mq.cloned();
        tokio::task::spawn_blocking(move || accumulator.send(msg, mq_owned.as_ref()))
            .await
            .map_err(|e| Error::client(format!("sendByAccumulator join error: {e}")))?
    }

    pub fn send_by_accumulator_async(
        &self,
        mut msg: Message,
        mq: Option<&MessageQueue>,
        callback: Arc<dyn SendCallback>,
    ) -> Result<()> {
        if !self.can_batch(&msg) {
            return self.send_async_inner(msg, callback, None, mq.cloned());
        }
        self.check_and_stamp(&mut msg)?;
        let accumulator = self
            .produce_accumulator()
            .ok_or_else(|| Error::client("produceAccumulator is null"))?;
        accumulator.send_async(msg, mq, callback);
        Ok(())
    }

    // ---------------- Normal sending ----------------

    /// Synchronous send (`send(msg, timeout_millis = None, mq = None)`).
    ///
    /// With `mq` as `Some` this is a pinned send (no retry, no fault avoidance); with `None` the queue is picked by round-robin/fault avoidance
    /// and retried according to `retry_times_when_send_failed`. For batch messages use
    /// [`send_batch`](Self::send_batch) (the type system already separates the two message kinds).
    pub async fn send(
        &self,
        msg: &mut Message,
        timeout_millis: Option<i64>,
        mq: Option<&MessageQueue>,
    ) -> Result<SendResult> {
        if timeout_millis.is_none() && self.get_auto_batch() {
            let topic = self.with_namespace(msg.get_topic());
            msg.set_topic(&topic);
            let owned = msg.clone();
            return self.send_by_accumulator(owned, mq).await;
        }
        self.send_inner(msg, timeout_millis, mq).await
    }

    async fn send_inner(
        &self,
        msg: &mut Message,
        timeout_millis: Option<i64>,
        mq: Option<&MessageQueue>,
    ) -> Result<SendResult> {
        let client = self.require_client()?;
        let (timeout, retry_times) = self.read_cfg(|cfg| {
            (
                timeout_millis.unwrap_or(cfg.send_msg_timeout),
                cfg.retry_times_when_send_failed,
            )
        });
        let topic = self.with_namespace(&msg.topic);
        msg.topic = topic;
        self.check_message(msg)?;
        if let Some(mq) = mq {
            self.check_pinned_topic(&msg.topic, mq, PINNED_TOPIC_MISMATCH_SYNC)?;
        }
        let (sys_flag, prev_body) = self.try_to_compress_message(msg);

        let outcome = {
            let mut publish = PublishMessage::Single(msg);
            if let Some(mq) = mq {
                self.send_pinned(&client, &mut publish, mq, timeout, sys_flag)
                    .await
            } else {
                self.send_with_retry(&client, &mut publish, timeout, retry_times, sys_flag)
                    .await
            }
        };
        self.restore_caller_message(msg, prev_body);
        outcome
    }

    /// Shared by three callers: pinned synchronous send ([`send_inner`](Self::send_inner)), batch send
    /// ([`send_batch`](Self::send_batch)) and the batch direct send from the accumulator's guard thread
    /// ([`send_prepared_batch`](Self::send_prepared_batch)). The caller supplies `sys_flag`:
    /// for a plain message it is the compression bit, for a batch it is always 0.
    async fn send_pinned(
        &self,
        client: &MQClientInstance,
        publish: &mut PublishMessage<'_>,
        mq: &MessageQueue,
        timeout: i64,
        sys_flag: i32,
    ) -> Result<SendResult> {
        if self.has_send_interceptors() {
            return self
                .send_with_hooks(
                    client,
                    publish,
                    mq,
                    timeout,
                    sys_flag,
                    None,
                    CommunicationMode::Sync,
                )
                .await;
        }
        client
            .send_message(
                &self.inner.producer_group(),
                publish,
                mq,
                timeout,
                sys_flag,
                self.inner.unit_mode(),
                &self.inner.create_topic_key(),
                self.inner.default_topic_queue_nums(),
            )
            .await
    }

    /// `publish` is re-borrowed on **every** round ([`PublishMessage::reborrow`]) because the request must be rebuilt with the
    /// broker selected in that round in its header; there is only one underlying message, so rounds must share the same body / UNIQ_KEY.
    async fn send_with_retry(
        &self,
        client: &MQClientInstance,
        publish: &mut PublishMessage<'_>,
        timeout: i64,
        retry_times: i32,
        sys_flag: i32,
    ) -> Result<SendResult> {
        let topic = publish.as_message().topic.clone();
        let route = match self.topic_publish_info(client, &topic).await {
            Ok(publish) => publish,
            Err(e @ Error::Client { .. }) => {
                return Err(Error::client_with_code(
                    e.response_code()
                        .unwrap_or(client_error_code::NOT_FOUND_TOPIC_EXCEPTION),
                    e.to_string(),
                ))
            }
            Err(e) => return Err(e),
        };
        let times_total = retry_times.saturating_add(1);
        let begin_first = monotonic_millis();
        let mut brokers_sent: Vec<String> = Vec::new();
        let mut last_broker_name: Option<String> = None;
        // Keep the "last successful SendResult" in a variable: when the store result is not SEND_OK and broker switching is
        // enabled, retry carrying it along, and if everything afterwards fails, return this "stored but not stored well" result as is.
        let mut result: Option<SendResult> = None;
        let mut last_exc: Option<Error> = None;
        let mut call_timeout = false;
        let mut attempt: i32 = 0;
        while attempt < times_total {
            let selected = match self.inner.fault_strategy.select_one_message_queue(
                &*route,
                last_broker_name.as_deref(),
                attempt > 0,
            ) {
                Ok(selected) => selected,
                // Failing to select a queue is an MQClientException: `selected` is still None at this point,
                // `_update_fault_item` returns immediately (nothing recorded in the fault table), and only last_exc carries over to the next round.
                Err(e) => {
                    last_exc = Some(e);
                    attempt += 1;
                    continue;
                }
            };
            let broker_name = selected.broker_name.clone();
            last_broker_name = Some(broker_name.clone());
            brokers_sent.push(broker_name.clone());
            let began = monotonic_millis();
            let mq_sel = MessageQueue::new(&topic, &broker_name, selected.queue_id);
            // Overall budget: `timeout` is the total budget for **this call**, and the part already consumed by earlier attempts must be deducted,
            // otherwise 3 retries of 3s each would take up to 9s to return.
            let cost_time = (began - begin_first) as i64;
            if timeout < cost_time {
                call_timeout = true;
                break;
            }
            let mut cur_timeout = timeout - cost_time;
            let can_retry_again = attempt + 1 < times_total;
            let max_per_request = self.read_cfg(|c| c.send_msg_max_timeout_per_request);
            if max_per_request > -1 && can_retry_again && cur_timeout > max_per_request {
                cur_timeout = max_per_request;
            }
            let send_start = self.inner.metrics.record_send_start();
            let mut publish_msg = publish.reborrow();
            let outcome = self
                .send_with_hooks(
                    client,
                    &mut publish_msg,
                    &mq_sel,
                    cur_timeout,
                    sys_flag,
                    None,
                    CommunicationMode::Sync,
                )
                .await;
            match outcome {
                Ok(ok_result) => {
                    self.inner.metrics.record_send_success(send_start);
                    // Record the send latency; exceeding the threshold quarantines this broker for a while
                    self.inner.fault_strategy.update_fault_item(
                        &broker_name,
                        latency_since(began),
                        false,
                        true,
                    );
                    if ok_result.status != SendStatus::SendOk
                        && self.read_cfg(|c| c.retry_another_broker_when_not_store_ok)
                    {
                        result = Some(ok_result);
                        attempt += 1;
                        continue;
                    }
                    return Ok(ok_result);
                }
                Err(e) => {
                    self.inner.metrics.record_send_failure(send_start);
                    match classify_send_error(&e) {
                        // An exception none of the three tiers catches (validation/decode...): propagate straight up
                        None => return Err(e),
                        Some(SendErrorKind::Broker) => {
                            // The broker explicitly answered with an error code: quarantine that broker (reachability untouched),
                            // only codes listed in retryResponseCodes are worth switching another broker for
                            self.inner.fault_strategy.update_fault_item(
                                &broker_name,
                                latency_since(began),
                                true,
                                false,
                            );
                            let retry = self.is_retry_response_code(e.response_code());
                            if retry {
                                last_exc = Some(e);
                                attempt += 1;
                                continue;
                            }
                            if let Some(result) = result {
                                return Ok(result);
                            }
                            return Err(e);
                        }
                        Some(SendErrorKind::Remoting) => {
                            let reachable = !self.read_cfg(|c| c.start_detector_enable);
                            self.inner.fault_strategy.update_fault_item(
                                &broker_name,
                                latency_since(began),
                                true,
                                reachable,
                            );
                            last_exc = Some(e);
                        }
                        Some(SendErrorKind::Client) => {
                            self.inner.fault_strategy.update_fault_item(
                                &broker_name,
                                latency_since(began),
                                false,
                                true,
                            );
                            last_exc = Some(e);
                        }
                    }
                }
            }
            attempt += 1;
        }

        if let Some(result) = result {
            return Ok(result);
        }
        if call_timeout {
            return Err(Error::TooMuchRequest(
                "sendDefaultImpl call timeout".to_string(),
            ));
        }
        let info = format!(
            "Send [{}] times, still failed, cost [{}]ms, Topic: {topic}, BrokersSent: [{}], \
             last error: {}",
            brokers_sent.len(),
            latency_since(begin_first),
            brokers_sent.join(", "),
            last_exc.as_ref().map(|e| e.to_string()).unwrap_or_default(),
        );
        let code = match &last_exc {
            Some(Error::Broker { response_code, .. }) => Some(*response_code),
            Some(Error::Connect { .. }) => Some(client_error_code::CONNECT_BROKER_EXCEPTION),
            Some(Error::Timeout { .. }) => Some(client_error_code::ACCESS_BROKER_TIMEOUT),
            Some(Error::Client { .. }) | Some(Error::RequestTimeout { .. }) => {
                Some(client_error_code::BROKER_NOT_EXIST_EXCEPTION)
            }
            _ => None,
        };
        Err(Error::Client {
            response_code: code,
            message: info,
        })
    }

    /// The batch send branch.
    ///
    /// The constraints are guaranteed by [`MessageBatch::generate_from_list`]: non-empty, same topic, same
    /// waitStoreMsgOK, no delayed messages and no retry topics. Batch messages are **never compressed**.
    pub async fn send_batch(
        &self,
        msgs: Vec<Message>,
        mq: Option<&MessageQueue>,
        timeout_millis: Option<i64>,
    ) -> Result<SendResult> {
        let client = self.require_client()?;
        let timeout = match timeout_millis {
            Some(t) => t,
            None => {
                self.inner
                    .cfg
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .send_msg_timeout
            }
        };
        if msgs.is_empty() {
            bail!("message list is empty");
        }
        let max = self
            .inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .max_message_size;
        for msg in &msgs {
            validators::check_message(msg, max)?;
        }
        let mut msgs = msgs;
        for msg in &mut msgs {
            set_uniq_id(msg);
            let topic = self.with_namespace(&msg.topic);
            msg.topic = topic;
        }
        let topic = msgs[0].topic.clone();
        let mut batch = MessageBatch::generate_from_list(msgs)?;
        set_uniq_id(&mut batch.message);
        let body = batch.encode();
        batch.message.set_body(Some(&body));
        let mut publish_msg = PublishMessage::Batch(&mut batch);
        // MessageBatch is skipped outright by the compression step (it returns 0); batch messages are never compressed
        let sys_flag = self.sys_flag_for(&mut publish_msg);
        let mq_sel = match mq {
            Some(mq) => {
                self.check_pinned_topic(&topic, mq, PINNED_TOPIC_MISMATCH_SYNC)?;
                mq.clone()
            }
            None => {
                let route = self.topic_publish_info(&client, &topic).await?;
                // The batch outer topic already equals topic, so build mq_sel from it directly
                match route.select_one_message_queue(&[])? {
                    Some(mq) => MessageQueue::new(&topic, &mq.broker_name, mq.queue_id),
                    None => return Err(Error::client("no message queue for publish info")),
                }
            }
        };
        self.send_pinned(&client, &mut publish_msg, &mq_sel, timeout, sys_flag)
            .await
    }

    /// Send an **already encoded batch** -- the send tail specific to the batching path ([`ProduceAccumulator`]).
    ///
    /// ⚠ This does **not** redo the namespace, `checkMessage`, `setUniqID` or the encoding -- those were already
    /// applied to the objects by `sendByAccumulator` (per message) and `MessageBatch::generate_from_list` (the batch itself), and doing it
    /// again would overwrite the child messages' UNIQ_KEY with new ones: after the broker splits the batch, each child message's
    /// client ID would no longer match what the sending side recorded. Compression is skipped for the same reason (`SEND_BATCH` batches are never compressed).
    ///
    /// [`ProduceAccumulator`]: crate::client::produce_accumulator::ProduceAccumulator
    async fn send_prepared_batch(
        &self,
        batch: &mut MessageBatch,
        mq: Option<&MessageQueue>,
        timeout_millis: Option<i64>,
    ) -> Result<SendResult> {
        let client = self.require_client()?;
        let (timeout, retry_times) = self.read_cfg(|cfg| {
            (
                timeout_millis.unwrap_or(cfg.send_msg_timeout),
                cfg.retry_times_when_send_failed,
            )
        });
        let mut publish = PublishMessage::Batch(batch);
        let sys_flag = self.sys_flag_for(&mut publish);
        if let Some(mq) = mq {
            let batch_topic = publish.as_message().topic.clone();
            self.check_pinned_topic(&batch_topic, mq, PINNED_TOPIC_MISMATCH_SYNC)?;
            return self
                .send_pinned(&client, &mut publish, mq, timeout, sys_flag)
                .await;
        }
        self.send_with_retry(&client, &mut publish, timeout, retry_times, sys_flag)
            .await
    }

    /// The front section is exactly the same as [`send_batch_async`](Self::send_batch_async): two backpressure gates →
    /// budget re-check → run the kernel after dequeue → deliver the callback exactly once (including the after hooks inside `complete_async`).
    /// The only difference is that the batch object is **already prepared**: no re-doing of validation, namespace, UNIQ_KEY or encoding.
    fn send_prepared_batch_async(
        &self,
        batch: MessageBatch,
        mq: Option<MessageQueue>,
        callback: Arc<dyn SendCallback>,
        timeout_millis: Option<i64>,
    ) -> Result<()> {
        let _ = self.require_client()?;
        let handle = self.runtime_handle().ok_or_else(|| {
            Error::client("send_prepared_batch_async needs a tokio runtime; call start() first")
        })?;
        let timeout = timeout_millis.unwrap_or_else(|| self.read_cfg(|c| c.send_msg_timeout));
        let msg_len = match batch.message.body.as_deref() {
            Some(body) => i64::try_from(body.len()).unwrap_or(i64::MAX),
            None => 1,
        };
        let began = monotonic_millis();
        let this = self.clone();
        let job: AsyncSendJob = Box::pin(async move {
            this.run_async_send_prepared_batch(batch, msg_len, callback, timeout, began, mq)
                .await;
        });
        self.submit_async_job(handle, job)
    }

    /// The segment after [`send_prepared_batch_async`](Self::send_prepared_batch_async) gets dequeued,
    /// line-for-line isomorphic to [`run_async_send_batch`](Self::run_async_send_batch).
    async fn run_async_send_prepared_batch(
        &self,
        mut batch: MessageBatch,
        msg_len: i64,
        callback: Arc<dyn SendCallback>,
        timeout: i64,
        began: f64,
        mq: Option<MessageQueue>,
    ) {
        let permits = match self.acquire_send_permits(msg_len, timeout, began).await {
            Ok(permits) => permits,
            Err(e) => {
                self.complete_async(&callback, Err(e), None, None);
                return;
            }
        };
        let cost = latency_since(began);
        if timeout <= cost {
            return self.fail_async(
                &callback,
                Error::TooMuchRequest("DEFAULT ASYNC send call timeout".to_string()),
                permits,
            );
        }
        if let Some(mq) = mq.as_ref() {
            if let Err(e) =
                self.check_pinned_topic(&batch.message.topic, mq, PINNED_TOPIC_MISMATCH_ASYNC)
            {
                return self.fail_async(&callback, e, permits);
            }
        }
        let outcome = self
            .send_prepared_batch(&mut batch, mq.as_ref(), Some(timeout - cost))
            .await;
        self.complete_async(&callback, outcome, None, Some(permits));
    }

    /// ⚠ Difference: an oneway call has no notion of a timeout to begin with, and
    /// [`MQClientInstance::send_message_oneway`] takes no timeout parameter either.
    pub async fn send_oneway(&self, msg: &mut Message, mq: Option<&MessageQueue>) -> Result<()> {
        let client = self.require_client()?;
        let wrapped = self.with_namespace(&msg.topic);
        msg.topic = wrapped.clone();
        self.check_message(msg)?;
        let mq_sel = match mq {
            Some(mq) => mq.clone(),
            None => {
                let publish = self.topic_publish_info(&client, &wrapped).await?;
                let selected = self
                    .inner
                    .fault_strategy
                    .select_one_message_queue(&*publish, None, false)?;
                MessageQueue::new(&wrapped, &selected.broker_name, selected.queue_id)
            }
        };
        let addr = self.need_addr(&client, &mq_sel).await?;
        let (sys_flag, prev_body) = self.try_to_compress_message(msg);
        let outcome = async {
            if self.has_check_forbidden_hook() {
                self.execute_check_forbidden(msg, &mq_sel, &addr, None, CommunicationMode::Oneway)?;
            }
            let mut publish_msg = PublishMessage::Single(msg);
            client
                .send_message_oneway(
                    &self.inner.producer_group(),
                    &mut publish_msg,
                    &mq_sel,
                    &addr,
                    sys_flag,
                    self.inner.unit_mode(),
                    &self.inner.create_topic_key(),
                    self.inner.default_topic_queue_nums(),
                )
                .await
        }
        .await;
        self.restore_caller_message(msg, prev_body);
        outcome
    }

    pub async fn send_by_selector(
        &self,
        msg: &mut Message,
        selector: &dyn MessageQueueSelector,
        arg: &str,
        timeout_millis: Option<i64>,
    ) -> Result<SendResult> {
        let client = self.require_client()?;
        let timeout = timeout_millis.unwrap_or_else(|| {
            self.inner
                .cfg
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .send_msg_timeout
        });
        let topic = self.with_namespace(&msg.topic);
        msg.topic = topic.clone();
        let publish = self.topic_publish_info(&client, &topic).await?;
        let queues = publish.msg_queue_list();
        let selected = selector.select(&queues, msg, arg)?;
        let mq_sel = MessageQueue::new(&topic, &selected.broker_name, selected.queue_id);
        // The selector uses the original message (topic/business fields); compression only affects the body
        self.check_message(msg)?;
        let (sys_flag, prev_body) = self.try_to_compress_message(msg);
        let holder: Option<AnyHolder> = Some(Arc::new(arg.to_string()));
        let outcome = {
            let mut publish = PublishMessage::Single(msg);
            if self.has_send_interceptors() {
                self.send_with_hooks(
                    &client,
                    &mut publish,
                    &mq_sel,
                    timeout,
                    sys_flag,
                    holder,
                    CommunicationMode::Sync,
                )
                .await
            } else {
                client
                    .send_message(
                        &self.inner.producer_group(),
                        &mut publish,
                        &mq_sel,
                        timeout,
                        sys_flag,
                        self.inner.unit_mode(),
                        &self.inner.create_topic_key(),
                        self.inner.default_topic_queue_nums(),
                    )
                    .await
            }
        };
        // The borrow ends only at the end of the block, so msg can be touched again here
        self.restore_caller_message(msg, prev_body);
        outcome
    }

    pub fn send_async(
        &self,
        mut msg: Message,
        callback: Arc<dyn SendCallback>,
        timeout_millis: Option<i64>,
        mq: Option<MessageQueue>,
    ) -> Result<()> {
        if timeout_millis.is_none() && self.get_auto_batch() {
            let topic = self.with_namespace(msg.get_topic());
            msg.set_topic(&topic);
            if let Err(e) = self.send_by_accumulator_async(msg, mq.as_ref(), Arc::clone(&callback))
            {
                callback.on_exception(e);
            }
            return Ok(());
        }
        self.send_async_inner(msg, callback, timeout_millis, mq)
    }

    fn send_async_inner(
        &self,
        msg: Message,
        callback: Arc<dyn SendCallback>,
        timeout_millis: Option<i64>,
        mq: Option<MessageQueue>,
    ) -> Result<()> {
        // Not started / pool already closed both raise synchronously and do not go through the callback.
        let _ = self.require_client()?;
        let handle = self
            .runtime_handle()
            .ok_or_else(|| Error::client("send_async needs a tokio runtime; call start() first"))?;
        let timeout = timeout_millis.unwrap_or_else(|| self.read_cfg(|c| c.send_msg_timeout));
        let msg_len = back_pressure_msg_len(&msg);
        let began = monotonic_millis();
        let this = self.clone();
        let job: AsyncSendJob = Box::pin(async move {
            this.run_async_send(msg, msg_len, callback, timeout, began, mq)
                .await;
        });
        self.submit_async_job(handle, job)
    }

    fn submit_async_job(&self, handle: tokio::runtime::Handle, job: AsyncSendJob) -> Result<()> {
        let submitted = {
            let slot = self
                .inner
                .async_sender
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            match slot.as_ref() {
                Some(executor) => executor.try_submit(job),
                None => return Err(Error::client("producer already shutdown")),
            }
        };
        if let Err(job) = submitted {
            if !self.is_enable_backpressure_for_async_mode() {
                return Err(Error::client("executor rejected"));
            }
            handle.spawn(job);
        }
        Ok(())
    }

    /// Does not return a `Result`: every failure on this chain is delivered through [`complete_async`](Self::complete_async),
    /// so the user callback fires **exactly once**.
    async fn run_async_send(
        &self,
        mut msg: Message,
        msg_len: i64,
        callback: Arc<dyn SendCallback>,
        timeout: i64,
        began: f64,
        mq: Option<MessageQueue>,
    ) {
        // When no permit was acquired, `acquire_send_permits` has already handed back the ones **already taken** (via Drop).
        let permits = match self.acquire_send_permits(msg_len, timeout, began).await {
            Ok(permits) => permits,
            Err(e) => {
                self.complete_async(&callback, Err(e), None, None);
                return;
            }
        };
        // Only count real cost after dequeuing -- if queueing already ate the budget, call back directly without sending a request.
        let cost = latency_since(began);
        if timeout <= cost {
            return self.fail_async(
                &callback,
                Error::TooMuchRequest("DEFAULT ASYNC send call timeout".to_string()),
                permits,
            );
        }
        self.async_send_inner(&mut msg, mq.as_ref(), callback, permits, timeout - cost)
            .await;
    }

    /// Backpressure deducts byte permits for the **whole batch** (accumulated per message, an empty body still counts as 1),
    /// otherwise a batch of 100 would occupy only 1 unit of capacity, i.e. batch messages would bypass the byte gate.
    ///
    /// ⚠ The only difference from single-message async is the **retry semantics**: the batch path has no broker-switching retry of the async
    /// chain (`on_exception`); it uses the synchronous kernel's own `retry_times_when_send_failed` loop. `SendMessageHook`
    /// (before/after) **still runs** -- the synchronous kernel internally is `send_with_hooks`, the same measure;
    /// the hook context is built by the kernel and finished by the kernel, and [`complete_async`] only
    /// delivers the user callback and returns the permits, it does not run the after hooks a second time.
    pub fn send_batch_async(
        &self,
        msgs: Vec<Message>,
        callback: Arc<dyn SendCallback>,
        timeout_millis: Option<i64>,
        mq: Option<MessageQueue>,
    ) -> Result<()> {
        let _ = self.require_client()?;
        let handle = self.runtime_handle().ok_or_else(|| {
            Error::client("send_batch_async needs a tokio runtime; call start() first")
        })?;
        let timeout = timeout_millis.unwrap_or_else(|| self.read_cfg(|c| c.send_msg_timeout));
        let msg_len = batch_back_pressure_msg_len(&msgs);
        let began = monotonic_millis();
        let this = self.clone();
        let job: AsyncSendJob = Box::pin(async move {
            this.run_async_send_batch(msgs, msg_len, callback, timeout, began, mq)
                .await;
        });
        self.submit_async_job(handle, job)
    }

    /// The segment after [`send_batch_async`](Self::send_batch_async) gets dequeued, isomorphic to
    /// [`run_async_send`](Self::run_async_send): pass the gates first, re-check the budget, and only then send the request.
    async fn run_async_send_batch(
        &self,
        msgs: Vec<Message>,
        msg_len: i64,
        callback: Arc<dyn SendCallback>,
        timeout: i64,
        began: f64,
        mq: Option<MessageQueue>,
    ) {
        let permits = match self.acquire_send_permits(msg_len, timeout, began).await {
            Ok(permits) => permits,
            Err(e) => {
                self.complete_async(&callback, Err(e), None, None);
                return;
            }
        };
        let cost = latency_since(began);
        if timeout <= cost {
            return self.fail_async(
                &callback,
                Error::TooMuchRequest("DEFAULT ASYNC send call timeout".to_string()),
                permits,
            );
        }
        if let (Some(mq), Some(first)) = (mq.as_ref(), msgs.first()) {
            if let Err(e) = self.check_pinned_topic(&first.topic, mq, PINNED_TOPIC_MISMATCH_ASYNC) {
                return self.fail_async(&callback, e, permits);
            }
        }
        // The batch kernel validates every child message, composes the namespace and checks homogeneity itself; errors here are delivered to the callback as is.
        let outcome = self
            .send_batch(msgs, mq.as_ref(), Some(timeout - cost))
            .await;
        self.complete_async(&callback, outcome, None, Some(permits));
    }

    /// An exit that terminates "without issuing a request": return the permits + deliver the error.
    fn fail_async(&self, callback: &Arc<dyn SendCallback>, error: Error, permits: SendPermits) {
        self.complete_async(callback, Err(error), None, Some(permits));
    }

    async fn async_send_inner(
        &self,
        msg: &mut Message,
        mq: Option<&MessageQueue>,
        callback: Arc<dyn SendCallback>,
        permits: SendPermits,
        timeout: i64,
    ) {
        let client = match self.require_client() {
            Ok(client) => client,
            Err(e) => return self.fail_async(&callback, e, permits),
        };
        let topic = self.with_namespace(&msg.topic);
        msg.topic = topic.clone();
        if let Err(e) = self.check_message(msg) {
            return self.fail_async(&callback, e, permits);
        }
        if let Some(mq) = mq {
            if let Err(e) = self.check_pinned_topic(&msg.topic, mq, PINNED_TOPIC_MISMATCH_ASYNC) {
                return self.fail_async(&callback, e, permits);
            }
        }
        let (sys_flag, _prev_body) = self.try_to_compress_message(msg);
        let (mq_sel, publish) = match mq {
            Some(mq) => (mq.clone(), None),
            None => {
                let publish = match self.topic_publish_info(&client, &topic).await {
                    Ok(publish) => publish,
                    Err(e @ Error::Client { .. }) => {
                        // Same as the synchronous send: 10004 passes through as is, only the rest is qualified as 10005
                        return self.fail_async(
                            &callback,
                            Error::client_with_code(
                                e.response_code()
                                    .unwrap_or(client_error_code::NOT_FOUND_TOPIC_EXCEPTION),
                                e.to_string(),
                            ),
                            permits,
                        );
                    }
                    Err(e) => return self.fail_async(&callback, e, permits),
                };
                match self
                    .inner
                    .fault_strategy
                    .select_one_message_queue(&*publish, None, false)
                {
                    Ok(selected) => (
                        MessageQueue::new(&topic, &selected.broker_name, selected.queue_id),
                        Some(publish),
                    ),
                    Err(e) => {
                        // With selected == None this reports `Send [0] times, still failed,
                        // Topic: ..., BrokersSent: []`; queue selection returns a `Result`, so the original exception
                        // can only be concatenated into the text (the same measure as the synchronous kernel).
                        return self.fail_async(
                            &callback,
                            Error::client(format!(
                                "Send [0] times, still failed, Topic: {topic}, BrokersSent: [], \
                                 last error: {e}"
                            )),
                            permits,
                        );
                    }
                }
            }
        };
        self.send_kernel_async(
            &client, msg, &mq_sel, publish, sys_flag, callback, permits, timeout,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_kernel_async(
        &self,
        client: &MQClientInstance,
        msg: &mut Message,
        mq: &MessageQueue,
        publish: Option<Arc<TopicPublishInfo>>,
        sys_flag: i32,
        callback: Arc<dyn SendCallback>,
        permits: SendPermits,
        timeout: i64,
    ) {
        let began = monotonic_millis();
        let addr = match client.publish_addr_for(&mq.broker_name, &mq.topic).await {
            Ok(addr) => addr,
            Err(e) => return self.fail_async(&callback, e, permits),
        };
        if self.has_check_forbidden_hook() {
            if let Err(e) =
                self.execute_check_forbidden(msg, mq, &addr, None, CommunicationMode::Async)
            {
                return self.fail_async(&callback, e, permits);
            }
        }
        if self.read_cfg(|c| c.enable_trace_context.unwrap_or(false)) {
            inject_trace_context(msg);
        }
        let mut publish_msg = PublishMessage::Single(msg);
        let request = MQClientInstance::build_send_request(
            &self.inner.producer_group(),
            &mut publish_msg,
            mq,
            sys_flag,
            self.inner.unit_mode(),
            &self.inner.create_topic_key(),
            self.inner.default_topic_queue_nums(),
        );
        let group = self.inner.producer_group();
        let namespace = self.inner.namespace();
        let context = if self.has_send_message_hook() {
            let mut context = self.build_send_context(
                msg,
                &group,
                &namespace,
                mq,
                &addr,
                CommunicationMode::Async,
            );
            crate::client::hook::execute_send_message_hook_before(
                &self.inner.send_hooks,
                &mut context,
            );
            Some(context)
        } else {
            None
        };
        let cost = latency_since(began);
        if timeout < cost {
            return self.complete_async(
                &callback,
                Err(Error::TooMuchRequest(
                    "sendKernelImpl call timeout".to_string(),
                )),
                context,
                Some(permits),
            );
        }
        let mut msg_only = msg.clone();
        // The body is already encoded into request, so the retry chain only needs the UNIQ_KEY (read by parse_send_response)
        msg_only.set_body(None);
        Box::new(AsyncSendChain {
            producer: self.clone(),
            client: client.clone(),
            msg: Arc::new(msg_only),
            request,
            publish,
            broker_name: mq.broker_name.clone(),
            mq: mq.clone(),
            addr,
            timeout: timeout - cost,
            attempt_began: 0.0,
            times: 0,
            context,
            permits: Some(permits),
            callback,
        })
        .attempt();
    }

    /// `permits` is released through `drop` rather than `SendPermits::release`: the return rule has exactly one implementation
    /// (see [`SendPermits`'s `Drop`](SendPermits)), leaving one fewer place on the chain that could be written wrong.
    fn complete_async(
        &self,
        callback: &Arc<dyn SendCallback>,
        outcome: Result<SendResult>,
        mut context: Option<SendMessageContext>,
        permits: Option<SendPermits>,
    ) {
        let mut outcome = outcome;
        if let Some(context) = context.as_mut() {
            match outcome {
                Ok(result) => {
                    context.send_result = Some(result.clone());
                    crate::client::hook::execute_send_message_hook_after(
                        &self.inner.send_hooks,
                        context,
                    );
                    outcome = Ok(result);
                }
                Err(e) => {
                    context.exception = Some(e);
                    crate::client::hook::execute_send_message_hook_after(
                        &self.inner.send_hooks,
                        context,
                    );
                    // Hooks only read exception; take it back and deliver it as is (Error is not Clone).
                    outcome = context
                        .exception
                        .take()
                        .map_or_else(|| Err(Error::client("send message failed")), Err);
                }
            }
        }
        drop(permits); // the return point is marked explicitly: it must happen before handing over to the user callback
        match outcome {
            Ok(result) => callback.on_success(result),
            Err(e) => callback.on_exception(e),
        }
    }

    async fn acquire_send_permits(
        &self,
        msg_len: i64,
        timeout: i64,
        began: f64,
    ) -> Result<SendPermits> {
        let mut permits = SendPermits {
            inner: self.inner.clone(),
            num_acquired: false,
            size_acquired: false,
            msg_len,
        };
        if !self.is_enable_backpressure_for_async_mode() {
            return Ok(permits);
        }
        let budget = timeout - latency_since(began);
        permits.num_acquired = budget > 0
            && self
                .inner
                .semaphore_async_send_num
                .try_acquire(1, budget)
                .await;
        if !permits.num_acquired {
            return Err(Error::TooMuchRequest(
                "send message tryAcquire semaphoreAsyncNum timeout".to_string(),
            ));
        }
        let budget = timeout - latency_since(began);
        permits.size_acquired = budget > 0
            && self
                .inner
                .semaphore_async_send_size
                .try_acquire(msg_len, budget)
                .await;
        if !permits.size_acquired {
            return Err(Error::TooMuchRequest(
                "send message tryAcquire semaphoreAsyncSize timeout".to_string(),
            ));
        }
        Ok(permits)
    }
}

// ================================================================ Async send chain

struct AsyncSendChain {
    producer: DefaultMQProducer,
    client: MQClientInstance,
    /// Only read for the UNIQ_KEY by `parse_send_response`; the body is already encoded into `request`.
    msg: Arc<Message>,
    request: RemotingCommand,
    /// `None` = pinned send (the caller supplied mq): retries do not switch brokers, only a new opaque in place.
    publish: Option<Arc<TopicPublishInfo>>,
    broker_name: String,
    mq: MessageQueue,
    addr: String,
    timeout: i64,
    attempt_began: f64,
    times: i32,
    context: Option<SendMessageContext>,
    permits: Option<SendPermits>,
    callback: Arc<dyn SendCallback>,
}

impl AsyncSendChain {
    fn attempt(mut self: Box<Self>) {
        if self.times > 0 {
            self.request.opaque = next_opaque();
        }
        self.attempt_began = monotonic_millis();
        let client = self.client.clone();
        let addr = self.addr.clone();
        let request = self.request.clone();
        let msg = Arc::clone(&self.msg);
        let mq = Arc::new(self.mq.clone());
        let timeout = self.timeout;
        client.send_message_async(
            &addr,
            request,
            msg,
            mq,
            timeout,
            Box::new(move |outcome| self.on_response(outcome)),
        );
    }

    fn on_response(self: Box<Self>, outcome: Result<SendResult>) {
        let cost = latency_since(self.attempt_began);
        match outcome {
            Ok(result) => {
                self.producer.inner.fault_strategy.update_fault_item(
                    &self.broker_name,
                    cost,
                    false,
                    true,
                );
                self.finish(Ok(result));
            }
            Err(e) => {
                self.producer.inner.fault_strategy.update_fault_item(
                    &self.broker_name,
                    cost,
                    true,
                    true,
                );
                let (wrapped, need_retry) = classify_async_failure(e, cost);
                self.on_exception(wrapped, need_retry, cost);
            }
        }
    }

    fn on_exception(mut self: Box<Self>, error: Error, need_retry: bool, cost: i64) {
        self.times += 1;
        let remaining = self.timeout - cost;
        if !(need_retry
            && self.times <= self.producer.get_retry_times_when_send_async_failed()
            && remaining > 0)
        {
            return self.finish(Err(error));
        }
        let (broker_name, mq) = match &self.publish {
            None => (self.broker_name.clone(), self.mq.clone()),
            Some(publish) => match self.producer.inner.fault_strategy.select_one_message_queue(
                &**publish,
                Some(&self.broker_name),
                false,
            ) {
                Ok(selected) => (
                    selected.broker_name.clone(),
                    MessageQueue::new(&self.mq.topic, &selected.broker_name, selected.queue_id),
                ),
                Err(_) => (self.broker_name.clone(), self.mq.clone()),
            },
        };
        let addr = match self.client.find_broker_address_in_publish(&broker_name) {
            Some(addr) => addr,
            None => {
                return self.finish(Err(Error::client(format!(
                    "The broker[{broker_name}] not exist"
                ))));
            }
        };
        rmq_warn!(
            "async send msg by retry {} times. topic={}, brokerAddr={}, brokerName={}: {}",
            self.times,
            self.mq.topic,
            addr,
            broker_name,
            error
        );
        self.broker_name = broker_name;
        self.mq = mq;
        self.addr = addr;
        self.timeout = remaining;
        self.attempt();
    }

    /// The end of the chain: hands over to [`complete_async`](DefaultMQProducer::complete_async)
    /// (after hooks first, then return the permits, finally deliver to the user callback).
    fn finish(mut self: Box<Self>, outcome: Result<SendResult>) {
        let permits = self.permits.take();
        let context = self.context.take();
        self.producer
            .complete_async(&self.callback, outcome, context, permits);
    }
}

fn classify_async_failure(err: Error, cost: i64) -> (Error, bool) {
    match err {
        e @ Error::SendRequest { .. } => (
            Error::client(format!("send request failed, last error: {e}")),
            true,
        ),
        e @ Error::Timeout { .. } => (
            Error::client(format!(
                "wait response timeout, cost={cost}, last error: {e}"
            )),
            true,
        ),
        // Every other RemotingException maps to `"unknown reason"`, but `RemotingTooMuchRequestException`
        // means "too many of our own in flight", so switching brokers would not help (`RemotingTooMuch…` is its own tier).
        e @ Error::TooMuchRequest(_) => (
            Error::client(format!("unknown reason, last error: {e}")),
            false,
        ),
        e @ (Error::Connect { .. } | Error::RemotingCommand(_) | Error::Io(_)) => (
            Error::client(format!("unknown reason, last error: {e}")),
            true,
        ),
        e => (e, false),
    }
}

// ================================================================ Transactional messages

/// Where the "post-send wrap-up information" of the transaction chain comes from.
///
/// * [`TxnEnd::Normal`]: the client commits/rolls back on its own, offset and transaction id taken from the `SendResult`;
/// * [`TxnEnd::Check`]: the wrap-up follows a broker check-back, taken from the check-back header (there is no `SendResult` then).
///
/// The wrap-up information has two mutually exclusive sources, separated by an enum so a pile of `Option`s is not passed around.
enum TxnEnd<'a> {
    Normal {
        send_result: &'a SendResult,
        msg: &'a Message,
    },
    Check {
        header: &'a CheckTransactionStateRequestHeader,
        msg_ext: &'a MessageExt,
        broker_addr: String,
    },
}

/// The four spellings of "deliver later" a transactional message may not carry: the delay
/// level plus the three 5.x timer forms. A half message is committed exactly as it stands,
/// so a schedule asked for here could never be honoured — the key alone decides, and the
/// list is deliberately shorter than the one that classifies a hook context (which also
/// counts the trace-only `__STARTDELIVERTIME`).
const TRANSACTION_FORBIDDEN_DELAY_KEYS: [&str; 4] = [
    PROPERTY_DELAY_TIME_LEVEL,
    PROPERTY_TIMER_DELAY_SEC,
    PROPERTY_TIMER_DELAY_MS,
    PROPERTY_TIMER_DELIVER_MS,
];

impl DefaultMQProducer {
    pub fn transaction_flag(state: LocalTransactionState) -> i32 {
        match state {
            LocalTransactionState::CommitMessage => MessageSysFlag::TRANSACTION_COMMIT_TYPE,
            LocalTransactionState::RollbackMessage => MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
            LocalTransactionState::Unknow => MessageSysFlag::TRANSACTION_NOT_TYPE,
        }
    }

    /// Send a transactional message.
    ///
    /// When `listener` is `None`, the one set through [`set_transaction_listener`](Self::set_transaction_listener) is used
    /// (the `TransactionMQProducer` fallback has been merged into the same entry point);
    /// an error is raised only when neither exists.
    /// The listener passed in is also recorded as "the current listener" so the broker can use it for check-backs.
    ///
    /// ⚠ Difference: "the local transaction raised an exception" was caught in the original semantics, the exception text written
    /// into the END_TRANSACTION remark while the state stayed UNKNOW; [`TransactionListener::execute_local_transaction`]
    /// does not raise exceptions, so implementors must express failure as
    /// [`LocalTransactionState::Unknow`] and let the broker check back later.
    pub async fn send_message_in_transaction(
        &self,
        msg: &mut Message,
        listener: Option<Arc<dyn TransactionListener>>,
        arg: Option<AnyHolder>,
    ) -> Result<TransactionSendResult> {
        let listener = match listener.or_else(|| self.transaction_listener()) {
            Some(listener) => listener,
            None => return Err(Error::client("tranExecutor is null")),
        };
        let topic = self.with_namespace(&msg.topic);
        msg.topic = topic.clone();

        if TRANSACTION_FORBIDDEN_DELAY_KEYS
            .iter()
            .any(|key| msg.get_property(key).is_some())
        {
            bail!("Transactional messages do not support delayed delivery");
        }

        let client = self.require_client()?;
        self.check_message(msg)?;

        // Half-message marker (the broker writes the message into RMQ_SYS_TRANS_HALF_TOPIC based on it)
        msg.put_property(PROPERTY_TRANSACTION_PREPARED, "true");
        msg.put_property(PROPERTY_PRODUCER_GROUP, &self.inner.producer_group());
        // Check-backs call this listener back (the broker locates this producer through the PGROUP property)
        self.set_transaction_listener(Some(listener.clone()));

        let publish = self.topic_publish_info(&client, &topic).await?;
        let selected = publish
            .select_one_message_queue(&[])?
            .ok_or_else(|| Error::client("no message queue for publish info"))?;
        let mq_sel = MessageQueue::new(&topic, &selected.broker_name, selected.queue_id);

        let (mut sys_flag, prev_body) = self.try_to_compress_message(msg);
        sys_flag = MessageSysFlag::reset_transaction_value(
            sys_flag,
            MessageSysFlag::TRANSACTION_PREPARED_TYPE,
        );

        let send_timeout = self
            .inner
            .cfg
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .send_msg_timeout;
        // Transactional sending also goes through sendKernelImpl (→ so it triggers the send hooks too), which means
        // with tracing enabled a transactional message first emits one Pub (Trans_Msg_Half) trace record
        let half_sent = self
            .send_with_hooks(
                &client,
                &mut PublishMessage::Single(msg),
                &mq_sel,
                send_timeout,
                sys_flag,
                None,
                CommunicationMode::Sync,
            )
            .await;
        self.restore_caller_message(msg, prev_body);
        let send_result =
            half_sent.map_err(|e| Error::client(format!("send message Exception: {e}")))?;

        let mut state = LocalTransactionState::Unknow;
        if send_result.status == SendStatus::SendOk {
            if let Some(tran_id) = &send_result.transaction_id {
                msg.put_property("__transactionId__", tran_id);
            }
            if let Some(uniq) = msg.get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) {
                let uniq = uniq.to_string();
                msg.set_transaction_id(Some(&uniq));
            }
            state = listener.execute_local_transaction(msg, arg.as_ref());
        } else if matches!(
            send_result.status,
            SendStatus::FlushDiskTimeout
                | SendStatus::FlushSlaveTimeout
                | SendStatus::SlaveNotAvailable
        ) {
            state = LocalTransactionState::RollbackMessage;
        }

        if let Err(e) = self
            .end_transaction(
                &client,
                TxnEnd::Normal {
                    send_result: &send_result,
                    msg,
                },
                state,
                None,
                false,
            )
            .await
        {
            rmq_warn!("local transaction execute {state}, but end broker transaction failed: {e}");
        }
        Ok(TransactionSendResult {
            local_transaction_state: Some(state),
            inner: send_result,
        })
    }

    async fn end_transaction(
        &self,
        client: &MQClientInstance,
        end: TxnEnd<'_>,
        state: LocalTransactionState,
        remark: Option<String>,
        from_transaction_check: bool,
    ) -> Result<()> {
        let group = self.inner.producer_group();
        let namespace = self.inner.namespace();
        let mut header = EndTransactionRequestHeader {
            producer_group: Some(group.clone()),
            commit_or_rollback: Some(Self::transaction_flag(state)),
            from_transaction_check: Some(from_transaction_check),
            ..Default::default()
        };
        let broker_addr = match &end {
            // On recall the broker puts COMPRESSED/transaction-related information in the check-back request
            TxnEnd::Check {
                header: check,
                msg_ext,
                broker_addr,
            } => {
                header.commit_log_offset = check.commit_log_offset;
                header.tran_state_table_offset = check.tran_state_table_offset;
                header.transaction_id = check.transaction_id.clone();
                header.bname = check.bname.clone();
                header.topic = check.topic.clone();
                header.msg_id = msg_ext
                    .get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                    .or(msg_ext.msg_id.as_deref())
                    .map(str::to_string);
                broker_addr.clone()
            }
            TxnEnd::Normal { send_result, msg } => {
                let id = send_result
                    .offset_msg_id
                    .as_deref()
                    .or(send_result.msg_id.as_deref())
                    .ok_or_else(|| Error::client("send result has no message id"))?;
                let (_, _, offset) = decode_message_id(id)?;
                let broker_name = send_result
                    .message_queue
                    .as_ref()
                    .map(|mq| mq.broker_name.clone())
                    .unwrap_or_default();
                header.commit_log_offset = Some(offset);
                header.tran_state_table_offset = Some(send_result.queue_offset);
                header.transaction_id = send_result.transaction_id.clone();
                header.bname = Some(broker_name.clone());
                header.topic = Some(msg.topic.clone());
                header.msg_id = send_result.msg_id.clone();
                client
                    .find_broker_address_in_publish(&broker_name)
                    .ok_or_else(|| Error::client("no broker address for end transaction"))?
            }
        };

        let mut cmd = RemotingCommand::create_request_command(
            request_code::END_TRANSACTION,
            Some(Box::new(header.clone())),
        );
        cmd.remark = remark;
        cmd.make_custom_header_to_net();
        client
            .remoting_client()
            .invoke_oneway(&broker_addr, &mut cmd)
            .await?;

        if self.inner.end_txn_hooks.has_hooks() {
            let message = match &end {
                TxnEnd::Normal { msg, .. } => Some((*msg).clone()),
                TxnEnd::Check { msg_ext, .. } => Some(msg_ext.to_message()),
            };
            let mut ctx = EndTransactionContext {
                producer_group: group,
                message,
                broker_addr,
                msg_id: header.msg_id.clone(),
                transaction_id: header.transaction_id.clone(),
                transaction_state: Some(state),
                from_transaction_check,
                namespace,
            };
            crate::client::hook::execute_end_transaction_hook(&self.inner.end_txn_hooks, &mut ctx);
        }
        Ok(())
    }

    /// Handle the transaction check-back actively sent by the broker
    /// (CHECK_TRANSACTION_STATE=39).
    fn handle_check_transaction_state(&self, request: RemotingCommand, addr: String) -> Result<()> {
        let header: CheckTransactionStateRequestHeader =
            match request.decode_command_custom_header() {
                Ok(h) => h,
                Err(e) => {
                    rmq_warn!("checkTransactionState: decode header failed from {addr}: {e}");
                    return Ok(());
                }
            };
        let body = match request.body.as_ref() {
            Some(b) => b.clone(),
            None => {
                rmq_warn!("checkTransactionState: empty message body from {addr}");
                return Ok(());
            }
        };
        let msg_ext = match decode_message(&body) {
            Ok(m) => m,
            Err(e) => {
                rmq_warn!("checkTransactionState: decode message failed: {e}");
                return Ok(());
            }
        };
        // Match against this producer using the PGROUP property; drop it when it does not match
        let group = self.inner.producer_group();
        if let Some(pg) = msg_ext.get_property(PROPERTY_PRODUCER_GROUP) {
            if pg != group {
                rmq_debug!("checkTransactionState: group {pg} not mine ({group})");
                return Ok(());
            }
        }
        let listener = match self.transaction_listener() {
            Some(l) => l,
            None => {
                rmq_warn!("checkTransactionState: no transaction listener for group {group}");
                return Ok(());
            }
        };
        let client = self.require_client()?;
        let this = self.clone();
        let spawned = client.remoting_client().runtime_handle().map(|handle| {
            handle.spawn(async move {
                let state = listener.check_local_transaction(&msg_ext);
                if let Err(e) = this
                    .end_transaction(
                        &client,
                        TxnEnd::Check {
                            header: &header,
                            msg_ext: &msg_ext,
                            broker_addr: addr.clone(),
                        },
                        state,
                        None,
                        true,
                    )
                    .await
                {
                    rmq_warn!("checkTransactionState: end transaction failed: {e}");
                }
            })
        });
        if spawned.is_none() {
            rmq_warn!("checkTransactionState: no tokio runtime to run the transaction check");
        }
        Ok(())
    }
}

/// ⚠ Holds only a [`Weak`]: otherwise `Inner → RemotingClient → this processor → Inner` would form a cycle
/// and the producer would never be released (same guard as the client instance's broker-push processor).
struct CheckTransactionStateProcessor {
    producer: Weak<Inner>,
}

impl RequestProcessor for CheckTransactionStateProcessor {
    fn process(&self, request: RemotingCommand, addr: String, sink: ResponseSink) {
        let Some(inner) = self.producer.upgrade() else {
            rmq_warn!("CHECK_TRANSACTION_STATE from {addr}: producer already dropped");
            return;
        };
        let producer = DefaultMQProducer { inner };
        if let Err(e) = producer.handle_check_transaction_state(request, addr.clone()) {
            rmq_warn!("CHECK_TRANSACTION_STATE from {addr} failed: {e}");
        }
        // The broker pushes check-back requests oneway, so there is no response to send back here (ResponseSink discards it itself)
        drop(sink);
    }
}

// ================================================================ Request-Reply

impl DefaultMQProducer {
    /// Request-Reply (5.x): send one request message and **wait synchronously for the reply**, returning the reply message.
    ///
    /// ⚠ Difference: the original semantics used an empty callback to write "send failed" into the waiting slot; here the send
    /// is `await`ed inside this function and [`RequestResponseFuture::set_failed`] is called directly with the
    /// return value, with the same effect (one callback type used only internally fewer).
    pub async fn request(
        &self,
        msg: &mut Message,
        timeout_millis: Option<i64>,
        mq: Option<&MessageQueue>,
    ) -> Result<MessageExt> {
        let timeout = timeout_millis.unwrap_or_else(|| {
            self.inner
                .cfg
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .request_timeout
        });
        let topic = self.with_namespace(&msg.topic);
        msg.topic = topic.clone();
        self.check_message(msg)?;
        let client = self.require_client()?;

        let correlation_id = create_correlation_id();
        msg.put_property(PROPERTY_CORRELATION_ID, &correlation_id);
        msg.put_property(PROPERTY_MESSAGE_REPLY_TO_CLIENT, client.client_id());
        msg.put_property(PROPERTY_MESSAGE_TTL, &timeout.to_string());

        let begin = current_time_millis();
        if let Err(e) = self.topic_publish_info(&client, &topic).await {
            rmq_debug!("request: prepare route failed: {e}");
        }
        send_heartbeat_to_all_broker(&self.inner, &client).await;

        let future = Arc::new(RequestResponseFuture::new(&correlation_id, timeout));
        request_future_holder().put_request(&correlation_id, future.clone());
        let cost = current_time_millis() - begin;
        let send_timeout = if timeout > cost {
            timeout - cost
        } else {
            timeout
        };
        if let Err(e) = self.send(msg, Some(send_timeout), mq).await {
            future.set_failed(e);
        }
        let result = self
            .wait_request_response(&topic, timeout, &future, cost)
            .await;
        // The request future must be cleaned up whether the call succeeded or failed
        request_future_holder().remove_request(&correlation_id);
        result
    }

    async fn wait_request_response(
        &self,
        topic: &str,
        timeout: i64,
        future: &Arc<RequestResponseFuture>,
        cost: i64,
    ) -> Result<MessageExt> {
        match future.wait_response_message(timeout - cost).await {
            Some(response) => Ok(response),
            None => {
                if future.is_send_request_ok() {
                    Err(Error::request_timeout(topic, timeout))
                } else {
                    Err(Error::client(format!(
                        "send request message to <{topic}> fail"
                    )))
                }
            }
        }
    }
}

// ================================================================ Timed message recall

impl DefaultMQProducer {
    /// Recall a **timer/delay** message (`RECALL_MESSAGE` 370); on success returns the uniqKey of the recalled message.
    pub async fn recall_message(&self, topic: &str, recall_handle: &str) -> Result<String> {
        let client = self.require_client()?;
        let topic = self.with_namespace(topic);
        validators::check_topic(&topic)?;
        if MixAll::is_retry_topic(Some(topic.as_str()))
            || MixAll::is_dlq_topic(Some(topic.as_str()))
        {
            return Err(Error::client("topic is not supported"));
        }
        let handle = recall_message_handle::decode_handle(recall_handle)?;
        self.topic_publish_info(&client, &topic).await?;
        let addr = client
            .find_broker_address_in_publish(&handle.broker_name)
            .or_else(|| client.find_broker_addr_by_topic(&topic))
            .ok_or_else(|| {
                rmq_warn!("can't find broker service address. {}", handle.broker_name);
                Error::client("The broker service address not found")
            })?;
        let header = RecallMessageRequestHeader {
            producer_group: Some(self.inner.producer_group()),
            topic: Some(topic.clone()),
            recall_handle: Some(recall_handle.to_string()),
            bname: Some(handle.broker_name.clone()),
        };
        let timeout = self.read_cfg(|c| c.send_msg_timeout);
        client.recall_message(&addr, header, timeout).await
    }
}

// ================================================================ Admin convenience methods

/// All of these methods require the producer to be `start()`ed (they raise an error when no [`MQClientInstance`] is available),
/// and the timeouts are fixed: 5000ms for routing/offset/topic creation,
/// 15000ms for message queries.
impl DefaultMQProducer {
    /// Get the publishable queues of a topic (fetching the route once when the cache is empty).
    pub async fn fetch_publish_message_queues(&self, topic: &str) -> Result<Vec<MessageQueue>> {
        let client = self.require_client()?;
        let publish = client.get_topic_publish_info(topic, false).await?;
        Ok(publish.msg_queue_list())
    }

    /// Create a new topic using the brokers found in the default topic's route.
    pub async fn create_topic(
        &self,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
    ) -> Result<()> {
        let client = self.require_client()?;
        validators::check_topic(new_topic)?;
        validators::is_system_topic(new_topic)?;
        let perm = PermName::PERM_READ | PermName::PERM_WRITE;
        client
            .create_topic_in_route(
                new_topic,
                queue_num,
                queue_num,
                perm,
                topic_sys_flag,
                None,
                5000,
            )
            .await
    }

    /// Find the offset on a queue by timestamp.
    pub async fn search_offset(&self, mq: &MessageQueue, timestamp: i64) -> Result<i64> {
        let client = self.require_client()?;
        client
            .search_offset_by_timestamp(mq, timestamp, 5000, None)
            .await
    }

    pub async fn max_offset(&self, mq: &MessageQueue) -> Result<i64> {
        let client = self.require_client()?;
        client.get_max_offset(mq, 5000, None).await
    }

    pub async fn min_offset(&self, mq: &MessageQueue) -> Result<i64> {
        let client = self.require_client()?;
        client.get_min_offset(mq, 5000, None).await
    }

    /// Store time of the earliest message on the queue (0 = the queue is empty).
    pub async fn earliest_msg_store_time(&self, mq: &MessageQueue) -> Result<i64> {
        let client = self.require_client()?;
        let addr = client.broker_addr(mq).await?;
        let header = GetEarliestMsgStoretimeRequestHeader {
            topic: Some(mq.topic.clone()),
            queue_id: Some(mq.queue_id),
        };
        let mut request = RemotingCommand::create_request_command(
            request_code::GET_EARLIEST_MSG_STORETIME,
            Some(Box::new(header)),
        );
        let response = client.invoke_sync(&addr, &mut request, 5000).await?;
        MQClientInstance::check_response(&response)?;
        let mut resp_header = GetEarliestMsgStoretimeResponseHeader::default();
        resp_header.from_ext_fields(response.ext_fields());
        Ok(resp_header.timestamp.unwrap_or(0))
    }

    /// Query messages by business key, returning the decoded
    /// [`MessageExt`] list (nothing found ⇒ empty list, no error).
    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Result<Vec<MessageExt>> {
        let client = self.require_client()?;
        let body = client
            .query_message(topic, key, max_num, begin, end, 15000, None, None, false)
            .await?;
        // An empty body falls back to an empty list
        Ok(body.map(|b| decode_messages(&b)).unwrap_or_default())
    }

    /// Read one message by msgId -- this also only raises an
    /// `MQClientException` (it does not silently return an empty list).
    pub fn view_message(&self, topic: &str, msg_id: &str) -> Result<Vec<MessageExt>> {
        Err(Error::client(format!(
            "viewMessage by msgId is not supported in Rust edition (topic={topic}, msgId={msg_id})"
        )))
    }
}

// ================================================================ Transactional producer

/// This is an extension of `DefaultMQProducer` that does exactly one thing more: it **pre-sets** the
/// [`TransactionListener`], so sending does not have to pass it every time. There is no inheritance here, so "wrap it +
/// [`std::ops::Deref`]" gives the same feel -- every base producer method stays usable.
///
/// ⚠ [`DefaultMQProducer::send_message_in_transaction`] itself already supports "falling back to the one set by
/// [`DefaultMQProducer::set_transaction_listener`] when `listener` is `None`"
/// (that fallback has been merged into the base type),
/// so this type only makes the "a listener must be set first" constraint explicit.
#[derive(Debug, Clone)]
pub struct TransactionMQProducer {
    producer: DefaultMQProducer,
}

impl TransactionMQProducer {
    /// Construct a transactional producer from a producer group name.
    pub fn new(producer_group: &str) -> Result<TransactionMQProducer> {
        Ok(TransactionMQProducer {
            producer: DefaultMQProducer::new(producer_group)?,
        })
    }

    /// Construct from a complete configuration (equivalent to `new` + the setters one by one).
    pub fn with_config(cfg: ProducerConfig) -> Result<TransactionMQProducer> {
        Ok(TransactionMQProducer {
            producer: DefaultMQProducer::with_config(cfg)?,
        })
    }

    /// Wrap an existing producer (composition instead of inheritance).
    pub fn from_producer(producer: DefaultMQProducer) -> TransactionMQProducer {
        TransactionMQProducer { producer }
    }

    /// Access the underlying producer (lifecycle, sending and admin methods all live on it).
    pub fn producer(&self) -> &DefaultMQProducer {
        &self.producer
    }

    pub fn set_transaction_listener(&self, listener: Option<Arc<dyn TransactionListener>>) {
        self.producer.set_transaction_listener(listener);
    }

    /// Take the current transaction listener.
    pub fn transaction_listener(&self) -> Option<Arc<dyn TransactionListener>> {
        self.producer.transaction_listener()
    }

    /// No listener passed ⇒ use the preset one;
    /// neither available ⇒ error (the base type performs the same check; this only pins the semantics down).
    pub async fn send_message_in_transaction(
        &self,
        msg: &mut Message,
        listener: Option<Arc<dyn TransactionListener>>,
        arg: Option<AnyHolder>,
    ) -> Result<TransactionSendResult> {
        let listener = listener.or_else(|| self.transaction_listener());
        self.producer
            .send_message_in_transaction(msg, listener, arg)
            .await
    }
}

impl std::ops::Deref for TransactionMQProducer {
    type Target = DefaultMQProducer;

    fn deref(&self) -> &DefaultMQProducer {
        &self.producer
    }
}

// ================================================================ Tests

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::client::hook::{execute_send_message_hook_after, execute_send_message_hook_before};
    use crate::client::trace_context::TRACE_CONTEXT_PROPERTY;
    use crate::common::message_const::PROPERTY_TRANSFER_FLAG;
    use crate::remoting::protocol::ext_fields::{CustomHeader, ExtFields};

    fn producer(group: &str) -> DefaultMQProducer {
        DefaultMQProducer::new(group).expect("a valid group name must not fail to construct")
    }

    /// The sysFlag bits this producer should set when compressing (COMPRESSED_FLAG + compression type).
    fn compression_flag(p: &DefaultMQProducer) -> i32 {
        MessageSysFlag::set_compression_type(
            MessageSysFlag::COMPRESSED_FLAG,
            p.read_cfg(|c| c.compress_type),
        )
    }

    fn mq(broker: &str, queue_id: i32) -> MessageQueue {
        MessageQueue::new("T1", broker, queue_id)
    }

    fn queues(broker_prefix: &str, n: i32) -> Vec<MessageQueue> {
        (0..n).map(|i| mq(broker_prefix, i)).collect()
    }

    /// An instance without `start()`: the route table is empty ⇒ any real RPC fails **immediately**
    /// (`name server address list is empty`), which suits offline tests of the send kernel.
    fn bare_client() -> MQClientInstance {
        MQClientInstance::new("offline-producer-test", Vec::new())
    }

    fn seed_publish_addr(client: &MQClientInstance) {
        client.seed_publish_addr_for_test("broker-a", "127.0.0.1:1");
    }

    // ---------------- Construction / configuration defaults ----------------

    #[test]
    fn config_defaults_match_golden() {
        let cfg = ProducerConfig::default();
        assert_eq!(cfg.producer_group, MixAll::DEFAULT_PRODUCER_GROUP);
        assert_eq!(cfg.instance_name, DEFAULT_INSTANCE_NAME);
        assert_eq!(cfg.client_id, None);
        assert_eq!(cfg.create_topic_key, MixAll::DEFAULT_TOPIC);
        assert_eq!(
            cfg.default_topic_queue_nums,
            MixAll::DEFAULT_TOPIC_QUEUE_NUMS
        );
        assert_eq!(cfg.send_msg_timeout, 3000);
        assert_eq!(cfg.compress_msg_body_over_howmuch, 1024 * 4);
        assert_eq!(cfg.compress_level, 5);
        // the compression type is an "algorithm id", not a sysFlag bit field
        assert_eq!(cfg.compress_type, MessageSysFlag::ZLIB_TYPE);
        assert_eq!(cfg.retry_times_when_send_failed, 2);
        assert!(!cfg.retry_another_broker_when_not_store_ok);
        assert_eq!(cfg.max_message_size, 1024 * 1024 * 4);
        assert_eq!(cfg.heartbeat_interval_millis, 30_000);
        assert!(!cfg.send_latency_fault_enable);
        assert!(!cfg.enable_trace);
        assert_eq!(cfg.trace_msg_batch_num, 10);
        assert_eq!(cfg.request_timeout, DEFAULT_REQUEST_TIMEOUT_MILLIS);
        assert_eq!(cfg.tls_enable, None);
        assert_eq!(cfg.enable_trace_context, None);
    }

    #[test]
    fn empty_producer_group_is_rejected() {
        assert!(DefaultMQProducer::new("").is_err());
        assert!(DefaultMQProducer::new("   ").is_err());
        assert!(DefaultMQProducer::new("GID_ok").is_ok());
    }

    #[test]
    fn producer_group_setter_works_before_start() {
        let p = producer("GID_keep");
        p.set_producer_group("GID_new").expect("changing the group name is allowed before start");
        assert_eq!(p.producer_group(), "GID_new");
        // an empty group name is only validated at construction (the setter does not validate)
        assert!(DefaultMQProducer::new("").is_err());
    }

    // ---------------- Queue selectors ----------------

    #[test]
    fn hash_selector_is_deterministic_and_in_range() {
        let mqs = queues("broker-a", 8);
        let msg = Message::new("T1", Some(b"x"));
        for arg in ["", "order-1", "中文 key", "12345"] {
            let first = SelectMessageQueueByHash
                .select(&mqs, &msg, arg)
                .expect("a queue can be picked from a non-empty list");
            let second = SelectMessageQueueByHash
                .select(&mqs, &msg, arg)
                .expect("a queue can be picked from a non-empty list");
            assert_eq!(first, second, "the same arg must land on the same queue");
            assert!(mqs.contains(&first));
            // the same modulo measure as java_string_hash
            let expect = (java_string_hash(arg).unsigned_abs() as usize) % mqs.len();
            assert_eq!(first.queue_id, mqs[expect].queue_id);
        }
        // java_string_hash can return i32::MIN, whose absolute value is still negative; converge to 0 here instead of panicking
        let extreme = java_string_hash("\u{0}");
        let _ = extreme;
        assert!(SelectMessageQueueByHash.select(&[], &msg, "any").is_err());
    }

    #[test]
    fn random_selector_stays_in_range() {
        let mqs = queues("broker-a", 4);
        let msg = Message::new("T1", Some(b"x"));
        for _ in 0..32 {
            let got = SelectMessageQueueByRandom
                .select(&mqs, &msg, "")
                .expect("a queue can be picked from a non-empty list");
            assert!(mqs.contains(&got));
        }
        assert!(SelectMessageQueueByRandom.select(&[], &msg, "").is_err());
    }

    #[test]
    fn machine_room_selector_prefers_prefix_then_falls_back_to_first() {
        let mqs = vec![mq("roomA-0", 0), mq("roomB-0", 1)];
        let msg = Message::new("T1", Some(b"x"));
        assert_eq!(
            SelectMessageQueueByMachineRoom
                .select(&mqs, &msg, "roomB")
                .expect("can fall back"),
            mqs[1]
        );
        assert_eq!(
            SelectMessageQueueByMachineRoom
                .select(&mqs, &msg, "roomZ")
                .expect("can fall back"),
            mqs[0]
        );
        assert!(SelectMessageQueueByMachineRoom
            .select(&[], &msg, "roomA")
            .is_err());
    }

    #[test]
    fn closure_callback_runs_each_branch_at_most_once() {
        let ok = Arc::new(AtomicUsize::new(0));
        let bad = Arc::new(AtomicUsize::new(0));
        let (o, b) = (ok.clone(), bad.clone());
        let cb = ClosureSendCallback::new(
            Some(Box::new(move |_r| {
                o.fetch_add(1, Ordering::SeqCst);
            })),
            Some(Box::new(move |_e| {
                b.fetch_add(1, Ordering::SeqCst);
            })),
        );
        cb.on_success(SendResult::default());
        cb.on_success(SendResult::default());
        cb.on_exception(Error::client("boom"));
        cb.on_exception(Error::client("boom"));
        assert_eq!(ok.load(Ordering::SeqCst), 1);
        assert_eq!(bad.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn closure_callback_tolerates_missing_branch() {
        let cb = ClosureSendCallback::new(None, None);
        cb.on_success(SendResult::default());
        cb.on_exception(Error::client("ignored"));
    }

    // ---------------- Transaction / retry criteria ----------------

    #[test]
    fn transaction_flag_matches_wire_sysflag() {
        assert_eq!(
            DefaultMQProducer::transaction_flag(LocalTransactionState::CommitMessage),
            MessageSysFlag::TRANSACTION_COMMIT_TYPE
        );
        assert_eq!(
            DefaultMQProducer::transaction_flag(LocalTransactionState::RollbackMessage),
            MessageSysFlag::TRANSACTION_ROLLBACK_TYPE
        );
        assert_eq!(
            DefaultMQProducer::transaction_flag(LocalTransactionState::Unknow),
            MessageSysFlag::TRANSACTION_NOT_TYPE
        );
    }

    #[test]
    fn retry_covers_client_broker_and_remoting_errors() {
        for e in [
            Error::client("no route"),
            Error::Broker {
                response_code: 14,
                message: "topic not exist".into(),
            },
            Error::Server {
                response_code: 3,
                remark: "system busy".into(),
            },
            Error::RemotingCommand("bad command".into()),
            Error::Connect {
                addr: "1.2.3.4:10911".into(),
            },
            Error::SendRequest {
                addr: "1.2.3.4:10911".into(),
                message: "io".into(),
            },
            Error::Timeout {
                addr: "1.2.3.4:10911".into(),
                timeout_millis: 3000,
            },
            Error::TooMuchRequest("full".into()),
            Error::RequestTimeout {
                topic: "T1".into(),
                timeout_millis: 3000,
            },
        ] {
            assert!(retryable(&e), "{e} should be retryable");
        }
        for e in [
            Error::Decode("bad frame".into()),
            Error::Encode("illegal arg".into()),
            Error::Io(std::io::Error::other("disk")),
        ] {
            assert!(!retryable(&e), "{e} should not be retried");
        }
    }

    // ---------------- Validation / compression ----------------

    #[test]
    fn check_message_rejects_empty_topic_and_oversized_body() {
        let p = producer("GID_check");
        assert!(p.check_message(&Message::new("", Some(b"x"))).is_err());

        p.set_max_message_size(4);
        assert!(p
            .check_message(&Message::new("T1", Some(b"12345")))
            .is_err());
        assert!(p.check_message(&Message::new("T1", Some(b"1234"))).is_ok());
        assert!(p.check_message(&Message::new("T1", None)).is_err());
    }

    #[test]
    fn compression_only_kicks_in_at_threshold() {
        let p = producer("GID_compress");
        p.set_compress_msg_body_over_howmuch(1024);
        let mut msg = Message::new("T1", Some(&[b'a'; 1023]));
        let before = msg.get_body().to_vec();
        let (flag, prev) = p.try_to_compress_message(&mut msg);
        assert_eq!(flag, 0);
        assert!(prev.is_none(), "prevBody must not be handed over when nothing was compressed");
        assert_eq!(msg.get_body(), before.as_slice(), "the body must stay untouched below the threshold");

        let mut big = Message::new("T1", Some(&[b'a'; 4096]));
        let (flag, prev) = p.try_to_compress_message(&mut big);
        assert_eq!(
            flag & MessageSysFlag::COMPRESSED_FLAG,
            MessageSysFlag::COMPRESSED_FLAG
        );
        assert_eq!(
            MessageSysFlag::get_compression_type(flag),
            MessageSysFlag::ZLIB_TYPE
        );
        assert_eq!(prev.as_deref(), Some(&[b'a'; 4096][..]));
        // the body really was replaced by the compressed stream and decodes back exactly
        let compressed = big.get_body().to_vec();
        assert_ne!(compressed, vec![b'a'; 4096]);
        let raw = compression::decompress(&compressed, MessageSysFlag::ZLIB_TYPE).unwrap();
        assert_eq!(raw, vec![b'a'; 4096]);
    }

    #[test]
    fn restore_caller_message_prevents_double_compression() {
        let p = producer("GID_restore");
        p.set_compress_msg_body_over_howmuch(1024);
        let mut msg = Message::new("T1", Some(&[b'a'; 4096]));

        let (first_flag, prev) = p.try_to_compress_message(&mut msg);
        assert_eq!(first_flag, compression_flag(&p));
        let first_wire_body = msg.get_body().to_vec();
        p.restore_caller_message(&mut msg, prev);
        assert_eq!(
            msg.get_body(),
            &[b'a'; 4096][..],
            "after restoring the body must equal what it was before the send"
        );

        // send once more: the bytes on the wire are still "the compressed form of the original text"
        let (second_flag, prev) = p.try_to_compress_message(&mut msg);
        assert_eq!(second_flag, compression_flag(&p));
        assert_eq!(msg.get_body(), first_wire_body.as_slice());
        p.restore_caller_message(&mut msg, prev);

        // negative control: skip the restore and the second compression fails outright (this is the regression shape this test catches)
        let mut unguarded = Message::new("T1", Some(&[b'a'; 4096]));
        let (_flag, prev) = p.try_to_compress_message(&mut unguarded);
        drop(prev);
        let (again_flag, _prev) = p.try_to_compress_message(&mut unguarded);
        assert_eq!(again_flag, 0, "without the restore the second compression is bound to fail");
    }

    #[test]
    fn restore_caller_message_strips_namespace_without_compression() {
        let p = producer("GID_restore_ns");
        p.set_namespace("ns1");
        let mut msg = Message::new("ns1%T1", Some(b"x"));

        p.restore_caller_message(&mut msg, None);

        assert_eq!(msg.topic, "T1");
        assert_eq!(msg.get_body(), b"x");
    }

    #[test]
    fn compression_type_and_empty_body_edges() {
        let p = producer("GID_compress_edge");
        p.set_compress_msg_body_over_howmuch(1);
        // empty body: no compression even at the lowest threshold
        let mut empty = Message::new("T1", Some(b""));
        let (flag, prev) = p.try_to_compress_message(&mut empty);
        assert_eq!(flag, 0);
        assert!(prev.is_none());

        assert!(
            p.set_compress_type(9).is_err(),
            "an unknown algorithm number must be rejected at set time"
        );
        assert_eq!(
            p.read_cfg(|c| c.compress_type),
            MessageSysFlag::ZLIB_TYPE,
            "the config keeps its old value when the setter fails"
        );
        // the config stayed ZLIB, so this one **compresses as usual** (no downgrade): the flag is COMPRESSED|ZLIB,
        // and it hands over the pre-compression body so that sendKernelImpl's finally can restore it.
        let mut msg = Message::new("T1", Some(b"payload payload payload"));
        let before = msg.get_body().to_vec();
        let (flag, prev) = p.try_to_compress_message(&mut msg);
        assert_eq!(
            flag,
            MessageSysFlag::set_compression_type(
                MessageSysFlag::COMPRESSED_FLAG,
                MessageSysFlag::ZLIB_TYPE
            )
        );
        assert_eq!(prev.as_deref(), Some(before.as_slice()));
        assert_ne!(msg.get_body(), before.as_slice(), "the body has been replaced by the compressed bytes");

        // legal values (including 0, kept for old-version compatibility) set successfully and get normalized
        p.set_compress_type(0).unwrap();
        assert_eq!(p.read_cfg(|c| c.compress_type), MessageSysFlag::ZLIB_TYPE);
        p.set_compress_type(MessageSysFlag::LZ4_TYPE).unwrap();
        assert_eq!(p.read_cfg(|c| c.compress_type), MessageSysFlag::LZ4_TYPE);
        p.set_compress_type(MessageSysFlag::ZSTD_TYPE).unwrap();
        assert_eq!(p.read_cfg(|c| c.compress_type), MessageSysFlag::ZSTD_TYPE);
    }

    #[test]
    fn batch_messages_never_compress() {
        let p = producer("GID_batch");
        p.set_compress_msg_body_over_howmuch(1);
        let mut batch = MessageBatch::new(vec![Message::new("T1", Some(&[b'x'; 4096]))]);
        let before = batch.message.get_body().len();
        assert_eq!(p.sys_flag_for(&mut PublishMessage::Batch(&mut batch)), 0);
        assert_eq!(batch.message.get_body().len(), before, "the batch body is never rewritten");
    }

    #[test]
    fn namespace_wraps_topic_once() {
        let p = producer("GID_ns");
        assert_eq!(p.with_namespace("T1"), "T1", "unchanged when there is no namespace");
        p.set_namespace("ns1");
        assert_eq!(p.with_namespace("T1"), "ns1%T1");
        // wrap_namespace is idempotent: re-wrapping does not produce ns1%ns1%T1
        assert_eq!(p.with_namespace(&p.with_namespace("T1")), "ns1%T1");
    }

    #[test]
    fn namespace_v2_setter_getter_round_trip() {
        let p = producer("GID_nsv2");
        assert_eq!(p.get_namespace_v2(), None, "unset by default");
        p.set_namespace_v2(Some("NS_V2"));
        assert_eq!(p.get_namespace_v2(), Some("NS_V2".to_string()));
        assert_eq!(p.config().namespace_v2, Some("NS_V2".to_string()));
        p.set_namespace_v2(None);
        assert_eq!(
            p.get_namespace_v2(),
            None,
            "None = cleared"
        );
        p.set_namespace("ns1");
        assert_eq!(p.get_namespace_v2(), None, "the v1 namespace does not write the v2 field");
    }

    #[test]
    fn socks_proxy_setter_getter_round_trip() {
        let p = producer("GID_socks");
        assert_eq!(p.get_socks_proxy(), None, "direct connection by default");
        p.set_socks_proxy(Some(SocksProxyConfig::with_auth("10.0.0.1:1080", "u", "w")));
        assert_eq!(
            p.get_socks_proxy(),
            Some(SocksProxyConfig::with_auth("10.0.0.1:1080", "u", "w"))
        );
        p.set_socks_proxy(Some(SocksProxyConfig::new("socks4://10.0.0.2:1080")));
        let proxy = p.get_socks_proxy().unwrap();
        assert_eq!(proxy.addr, "socks4://10.0.0.2:1080");
        assert_eq!(proxy.username, None);
        p.set_socks_proxy(None);
        assert_eq!(p.get_socks_proxy(), None, "None = cleared (direct connection)");
    }

    // ---------------- Hook context ----------------

    #[test]
    fn send_context_msg_type_follows_golden_priority() {
        let p = producer("GID_ctx");
        let m = mq("broker-a", 0);

        let plain = Message::new("T1", Some(b"x"));
        let ctx = p.build_send_context(
            &plain,
            "GID_ctx",
            "",
            &m,
            "127.0.0.1:10911",
            CommunicationMode::Sync,
        );
        assert_eq!(ctx.msg_type, MessageType::NormalMsg);
        assert_eq!(ctx.communication_mode, Some(CommunicationMode::Sync));
        assert_eq!(ctx.broker_addr, "127.0.0.1:10911");

        let mut half = Message::new("T1", Some(b"x"));
        half.put_property(PROPERTY_TRANSACTION_PREPARED, "true");
        let ctx = p.build_send_context(&half, "GID_ctx", "", &m, "", CommunicationMode::Sync);
        assert_eq!(ctx.msg_type, MessageType::TransMsgHalf);

        let mut delay = Message::new("T1", Some(b"x"));
        delay.put_property(PROPERTY_DELAY_TIME_LEVEL, "3");
        let ctx = p.build_send_context(&delay, "GID_ctx", "", &m, "", CommunicationMode::Sync);
        assert_eq!(ctx.msg_type, MessageType::DelayMsg);

        // both a half message and a delayed message: the evaluation order lets Delay override Trans
        let mut both = Message::new("T1", Some(b"x"));
        both.put_property(PROPERTY_TRANSACTION_PREPARED, "true");
        both.put_property("TIMER_DELIVER_MS", "123");
        let ctx = p.build_send_context(&both, "GID_ctx", "", &m, "", CommunicationMode::Oneway);
        assert_eq!(ctx.msg_type, MessageType::DelayMsg);
    }

    /// A stub hook that records the invocation order.
    #[derive(Default)]
    struct RecordingHook {
        before: AtomicUsize,
        after: AtomicUsize,
        after_saw_exception: AtomicUsize,
        after_saw_result: AtomicUsize,
    }

    impl SendMessageHook for RecordingHook {
        fn hook_name(&self) -> &str {
            "RecordingHook"
        }

        fn send_message_before(&self, _ctx: &mut SendMessageContext) -> Result<()> {
            self.before.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn send_message_after(&self, ctx: &mut SendMessageContext) -> Result<()> {
            self.after.fetch_add(1, Ordering::SeqCst);
            if ctx.exception.is_some() {
                self.after_saw_exception.fetch_add(1, Ordering::SeqCst);
            }
            if ctx.send_result.is_some() {
                self.after_saw_result.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn send_kernel_runs_after_hook_and_propagates_error() {
        let p = producer("GID_hooks");
        let hook = Arc::new(RecordingHook::default());
        p.register_send_message_hook(hook.clone());
        let client = bare_client();
        seed_publish_addr(&client);

        let mut msg = Message::new("T1", Some(b"body"));
        let mut publish = PublishMessage::Single(&mut msg);
        let err = p
            .send_with_hooks(
                &client,
                &mut publish,
                &mq("broker-a", 0),
                100,
                0,
                None,
                CommunicationMode::Sync,
            )
            .await
            .expect_err("without a name server the send must fail");
        assert!(hook.before.load(Ordering::SeqCst) >= 1, "the before hook must have run");
        assert_eq!(hook.after.load(Ordering::SeqCst), 1, "the after hook exactly once");
        assert_eq!(hook.after_saw_exception.load(Ordering::SeqCst), 1);
        assert_eq!(hook.after_saw_result.load(Ordering::SeqCst), 0);
        assert!(retryable(&err), "the send failure should be a retryable error kind: {err}");
    }

    /// A forbidden-check hook that rejects every send.
    struct ForbidAll;

    impl CheckForbiddenHook for ForbidAll {
        fn hook_name(&self) -> &str {
            "ForbidAll"
        }

        fn check_forbidden(&self, _ctx: &mut CheckForbiddenContext) -> Result<()> {
            Err(Error::client("forbidden by policy"))
        }
    }

    #[tokio::test]
    async fn check_forbidden_runs_before_send_message_hooks() {
        let p = producer("GID_forbidden");
        let hook = Arc::new(RecordingHook::default());
        p.register_send_message_hook(hook.clone());
        p.register_check_forbidden_hook(Arc::new(ForbidAll));
        let client = bare_client();
        seed_publish_addr(&client);

        let mut msg = Message::new("T1", Some(b"body"));
        let mut publish = PublishMessage::Single(&mut msg);
        let err = p
            .send_with_hooks(
                &client,
                &mut publish,
                &mq("broker-a", 0),
                100,
                0,
                None,
                CommunicationMode::Sync,
            )
            .await
            .expect_err("the intercepting hook's error must not be swallowed");
        assert!(err.to_string().contains("forbidden by policy"));
        assert_eq!(
            hook.before.load(Ordering::SeqCst),
            0,
            "the send hook must not run when intercepted"
        );
        assert_eq!(hook.after.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn traceparent_is_injected_only_when_enabled() {
        let p = producer("GID_trace");
        let client = bare_client();
        seed_publish_addr(&client);
        let m = mq("broker-a", 0);

        p.set_enable_trace_context(Some(false));
        let mut off = Message::new("T1", Some(b"body"));
        let mut publish = PublishMessage::Single(&mut off);
        let _ = p
            .send_with_hooks(
                &client,
                &mut publish,
                &m,
                100,
                0,
                None,
                CommunicationMode::Sync,
            )
            .await;
        assert!(off.get_property(TRACE_CONTEXT_PROPERTY).is_none());

        p.set_enable_trace_context(Some(true));
        let mut on = Message::new("T1", Some(b"body"));
        let mut publish = PublishMessage::Single(&mut on);
        let _ = p
            .send_with_hooks(
                &client,
                &mut publish,
                &m,
                100,
                0,
                None,
                CommunicationMode::Sync,
            )
            .await;
        let injected = on
            .get_property(TRACE_CONTEXT_PROPERTY)
            .expect("traceparent should be injected once the switch is on")
            .to_string();
        assert!(injected.starts_with("00-"), "W3C version prefix: {injected}");

        // an existing value is not overwritten
        let mut existing = Message::new("T1", Some(b"body"));
        existing.put_property(TRACE_CONTEXT_PROPERTY, "00-deadbeef-deadbeef-01");
        let mut publish = PublishMessage::Single(&mut existing);
        let _ = p
            .send_with_hooks(
                &client,
                &mut publish,
                &m,
                100,
                0,
                None,
                CommunicationMode::Sync,
            )
            .await;
        assert_eq!(
            existing.get_property(TRACE_CONTEXT_PROPERTY),
            Some("00-deadbeef-deadbeef-01")
        );
    }

    #[test]
    fn send_message_hooks_swallow_hook_errors() {
        struct Boom;
        impl SendMessageHook for Boom {
            fn hook_name(&self) -> &str {
                "Boom"
            }
            fn send_message_before(&self, _c: &mut SendMessageContext) -> Result<()> {
                Err(Error::client("hook exploded"))
            }
        }
        let hooks = SendMessageHookList::new();
        hooks.register(Arc::new(Boom));
        let mut ctx = SendMessageContext::default();
        // exceptions from before / after are only logged, they do not affect the send path
        execute_send_message_hook_before(&hooks, &mut ctx);
        execute_send_message_hook_after(&hooks, &mut ctx);
    }

    // ---------------- Lifecycle / parameter validation ----------------

    #[tokio::test]
    async fn send_before_start_fails_fast() {
        let p = producer("GID_lifecycle");
        let mut msg = Message::new("T1", Some(b"x"));
        let err = p
            .send(&mut msg, None, None)
            .await
            .expect_err("sending before start is not allowed");
        assert!(err.to_string().contains("not started"));
        assert!(p.fetch_publish_message_queues("T1").await.is_err());
        // shutdown is idempotent and does not panic before start
        p.shutdown();
        p.shutdown();
        assert!(!p.is_started());
    }

    #[tokio::test]
    async fn start_stamps_a_contract_style_client_id() {
        let p = producer("GID_clientid_shape");
        p.set_namesrv_addr("127.0.0.1:1");
        p.start().await.expect("start must not fail with a static address");
        let id = p.client_id().expect("clientId must exist after start");
        let (ip, instance) = id
            .split_once('@')
            .unwrap_or_else(|| panic!("clientId is missing the IP@instanceName separator: {id}"));
        assert_eq!(ip, MixAll::cached_ip_str(), "{id}");
        assert!(
            instance.starts_with(&format!("{}#", MixAll::cached_pid())),
            "{id}"
        );
        let cfg = p.config();
        assert_eq!(cfg.instance_name, instance, "instanceName must be written back into the config in place");
        p.shutdown();

        // a second producer within the same second must get a different clientId (the old scheme collided here)
        let q = producer("GID_clientid_shape_2");
        q.set_namesrv_addr("127.0.0.1:1");
        q.start().await.expect("start must not fail with a static address");
        assert_ne!(q.client_id(), Some(id));
        q.shutdown();
    }

    #[tokio::test]
    async fn an_explicit_instance_name_shares_one_instance() {
        let name = format!("clientid-shared-{}", MixAll::cached_pid());
        let a = producer("GID_clientid_shared_a");
        a.set_instance_name(&name);
        a.set_namesrv_addr("127.0.0.1:1");
        a.start().await.expect("start must not fail");
        let b = producer("GID_clientid_shared_b");
        b.set_instance_name(&name);
        b.set_namesrv_addr("127.0.0.1:1");
        b.start().await.expect("start must not fail");
        assert_eq!(a.client_id(), b.client_id());
        let shared = MQClientInstance::find_instance(&a.client_id().expect("clientId"))
            .expect("two producers with the same clientId must land on the same instance");
        assert!(
            shared.has_producer(&a.config().producer_group)
                && shared.has_producer(&b.config().producer_group),
            "the producers were not registered in the shared instance's producerTable"
        );
        // guard: the first one to leave must not tear down the instance that is still in use
        a.shutdown();
        assert!(shared.is_started(), "the first user to leave shut down the shared instance");
        b.shutdown();
        // unregistering (35) and tearing the instance down now sit in the same spawned task; `shutdown()` only hands it over
        for _ in 0..200 {
            if !shared.is_started() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(!shared.is_started(), "the instance was not torn down after the last user left");
    }

    /// The offline version of a pitfall the real `live_producer` P1 (sending still works after a restart that follows shutdown) stepped in:
    /// unregistering (35) and tearing the instance down are both scheduled inside a `spawn`ed task, and `shutdown()` merely hands that task over.
    /// If the registration table were emptied one step later as well, `start()` under the same clientId would, via `create_mq_client_instance`,
    /// reuse the very instance that is **about to be torn down**, and that task would then `shutdown` it too
    /// => the first send after the restart reports "client already shutdown".
    #[tokio::test]
    async fn restart_after_shutdown_never_reuses_a_dying_instance() {
        let name = format!("clientid-restart-{}", MixAll::cached_pid());
        let p = producer("GID_clientid_restart");
        p.set_instance_name(&name);
        p.set_namesrv_addr("127.0.0.1:1");
        p.start().await.expect("start must not fail");
        let client_id = p.client_id().expect("clientId");
        p.shutdown();
        // without waiting for any task: as soon as `shutdown()` returns, this instance must no longer be found under the same clientId
        assert!(
            MQClientInstance::find_instance(&client_id).is_none(),
            "the instance is still registered in INSTANCE_MAP after shutdown() returned; the restart would reuse it"
        );

        p.start().await.expect("the restart must not fail");
        let fresh = MQClientInstance::find_instance(&client_id).expect("an instance must exist after the restart");
        assert!(fresh.has_producer(&p.config().producer_group));
        // let the tail of the previous shutdown (35 + teardown) finish first
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            fresh.is_started(),
            "the instance the restart obtained was torn down by the tail task of the previous shutdown"
        );
        p.shutdown();
    }

    #[tokio::test]
    async fn start_requires_an_address_source() {
        let p = producer("GID_noaddr");
        // the static address is empty and ROCKETMQ_NAMESRV_DOMAIN is unset => it reports an error straight away
        // (note: this only asserts "no panic and returns Err"; it can only pass when the env var is injected from outside)
        if std::env::var_os("ROCKETMQ_NAMESRV_DOMAIN").is_none() {
            assert!(p.start().await.is_err());
            assert!(!p.is_started(), "a failed start must not leave the producer in the started state");
        }
    }

    #[tokio::test]
    async fn start_rejects_bad_producer_group_without_touching_network() {
        let long_group = std::iter::repeat_n('g', 121).collect::<String>();
        for (group, needle) in [
            (
                "DEFAULT_PRODUCER",
                "producerGroup can not equal DEFAULT_PRODUCER",
            ),
            ("bad group", "contains illegal characters"),
            (long_group.as_str(), "is longer than group max length"),
        ] {
            // construction only checks for whitespace; illegal characters are left to start()
            let p = DefaultMQProducer::new(group).expect("construction must not reject it up front");
            p.set_namesrv_addr("127.0.0.1:9876");
            let err = p.start().await.expect_err("an illegal group name must fail locally");
            assert!(err.to_string().contains(needle), "{group}: {err}");
            assert!(!p.is_started(), "{group}: a failed start must not stay in the started state");
        }
    }

    #[tokio::test]
    async fn poll_name_server_interval_reaches_the_instance() {
        let p = producer("GID_interval_probe");
        assert_eq!(
            p.config().poll_name_server_interval_millis,
            30_000,
            "ClientConfig:58 = 1000 * 30"
        );
        p.set_namesrv_addr("127.0.0.1:1");
        p.set_poll_name_server_interval_millis(1_500);
        p.start().await.expect("start must not fail with a static address");

        let client = p.client().expect("an instance must exist after start");
        assert_eq!(client.config().route_refresh_interval_millis, 1_500);
        assert_eq!(client.config().persist_offset_interval_millis, 5_000);
        p.shutdown();
    }

    #[tokio::test]
    async fn producer_group_is_validated_after_the_namespace_wrap() {
        let p = DefaultMQProducer::new(&"g".repeat(110)).unwrap();
        p.set_namespace("ns".repeat(30).as_str());
        // after the wrap it is ns...ns%g, length 90+1+110 > 120
        let err = p.start().await.expect_err("over length after the wrap must report an error");
        assert!(
            err.to_string().contains("is longer than group max length"),
            "{err}"
        );
        assert!(!p.is_started());
    }

    #[tokio::test]
    async fn send_batch_validates_before_touching_network() {
        let p = producer("GID_batch_check");
        // an empty list
        assert!(p.send_batch(Vec::new(), None, None).await.is_err());
        // not started => require_client reports the error first
        let mut msgs = vec![Message::new("T1", Some(b"a"))];
        assert!(p.send_batch(msgs.clone(), None, None).await.is_err());
        msgs.pop();
        assert!(p.send_batch(Vec::new(), None, None).await.is_err());
    }

    /// Outside a runtime and **never started** => it reports "not started" first, not "no runtime".
    ///
    /// Both are synchronous errors: the first would be harder to trace through a callback, the second means
    /// the pool has not been built yet. The genuine "outside a runtime" branch can only be hit after a start in a
    /// scenario with no bound runtime, and `start()` itself is async, so only the checking order can be asserted here.
    #[test]
    fn send_async_before_start_reports_not_started() {
        let p = producer("GID_async");
        let cb = Arc::new(ClosureSendCallback::new(None, None));
        let err = p
            .send_async(Message::new("T1", Some(b"x")), cb, None, None)
            .expect_err("not started must not dispatch silently");
        assert!(
            err.to_string().contains("producer not started"),
            "check the client first, then the runtime handle: {err}"
        );
    }

    /// Batch async uses the same rule as single async: when not started it **throws synchronously**, and not one callback is delivered.
    #[test]
    fn send_batch_async_before_start_reports_not_started() {
        let p = producer("GID_batch_async");
        let cb = Arc::new(ClosureSendCallback::new(None, None));
        let err = p
            .send_batch_async(vec![Message::new("T1", Some(b"x"))], cb, None, None)
            .expect_err("not started must not dispatch silently");
        assert!(
            err.to_string().contains("producer not started"),
            "check the client first, then the runtime handle: {err}"
        );
    }

    /// The whole batch is charged to the byte permit: summed per message, an empty body still counts as 1, an empty batch counts as 1.
    /// Getting this wrong does not fail any single send; it only lets batch messages **bypass the byte gate** under backpressure
    /// (a batch of 100 messages occupies only 1 unit of capacity), and the queue blowing up is something that happens only after a long run.
    #[test]
    fn batch_back_pressure_len_counts_every_message() {
        let one = Message::new("T1", Some(b"12345"));
        let empty = Message::new("T1", None);
        assert_eq!(batch_back_pressure_msg_len(std::slice::from_ref(&one)), 5);
        assert_eq!(batch_back_pressure_msg_len(&[one.clone(), one.clone()]), 10);
        // an empty body still counts as 1 (same rule as a single message: 0 means no throttling)
        assert_eq!(batch_back_pressure_msg_len(std::slice::from_ref(&empty)), 1);
        assert_eq!(
            batch_back_pressure_msg_len(&[empty.clone(), empty.clone()]),
            2
        );
        assert_eq!(batch_back_pressure_msg_len(&[one, empty]), 6);
        assert_eq!(batch_back_pressure_msg_len(&[]), 1);
    }

    #[test]
    fn view_message_is_not_supported() {
        let p = producer("GID_view");
        let err = p
            .view_message("T1", "ABCDEF")
            .expect_err("view_message reports an error outright");
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn transaction_producer_falls_back_to_preset_listener() {
        struct L;
        impl TransactionListener for L {
            fn execute_local_transaction(
                &self,
                _msg: &Message,
                _arg: Option<&AnyHolder>,
            ) -> LocalTransactionState {
                LocalTransactionState::CommitMessage
            }
            fn check_local_transaction(&self, _msg: &MessageExt) -> LocalTransactionState {
                LocalTransactionState::Unknow
            }
        }
        let tp = TransactionMQProducer::new("GID_txn").expect("the group name is legal");
        assert!(tp.transaction_listener().is_none());
        tp.set_transaction_listener(Some(Arc::new(L)));
        assert!(tp.transaction_listener().is_some());
        // Deref keeps the base producer's methods usable as usual
        assert_eq!(tp.producer_group(), "GID_txn");
        tp.set_transaction_listener(None);
        assert!(tp.transaction_listener().is_none());
    }

    #[tokio::test]
    async fn transaction_without_listener_is_rejected() {
        let p = producer("GID_txn_none");
        let mut msg = Message::new("T1", Some(b"x"));
        assert!(p
            .send_message_in_transaction(&mut msg, None, None)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn transaction_rejects_delay_properties() {
        struct L;
        impl TransactionListener for L {
            fn execute_local_transaction(
                &self,
                _msg: &Message,
                _arg: Option<&AnyHolder>,
            ) -> LocalTransactionState {
                LocalTransactionState::CommitMessage
            }
            fn check_local_transaction(&self, _msg: &MessageExt) -> LocalTransactionState {
                LocalTransactionState::Unknow
            }
        }
        let p = producer("GID_txn_delay");
        let listener: Arc<dyn TransactionListener> = Arc::new(L);

        // The key alone decides: "3", a "0" and an empty value all stamp a schedule the
        // committed half message cannot carry. The producer is not started, which matters
        // not — the refusal happens before the send path is reached.
        for name in TRANSACTION_FORBIDDEN_DELAY_KEYS {
            for value in ["3", "0", ""] {
                let mut msg = Message::new("T1", Some(b"x"));
                msg.put_property(name, value);
                let err = p
                    .send_message_in_transaction(&mut msg, Some(listener.clone()), None)
                    .await
                    .err()
                    .map(|e| e.to_string())
                    .unwrap_or_default();
                assert!(
                    err.contains("Transactional messages do not support delayed delivery"),
                    "{name}={value:?}: {err}"
                );
                assert!(
                    msg.get_property(PROPERTY_TRANSACTION_PREPARED).is_none(),
                    "a refused message must carry no half-message marker"
                );
                assert!(
                    msg.get_property(PROPERTY_PRODUCER_GROUP).is_none(),
                    "a refused message must carry no producer group"
                );
            }
        }

        // Only those four names refuse; DELAY_TIME is not one of them, so the send path is
        // reached and fails for some other reason (here: no client yet).
        let mut lookalike = Message::new("T1", Some(b"x"));
        lookalike.put_property("DELAY_TIME", "3");
        let text = p
            .send_message_in_transaction(&mut lookalike, Some(listener.clone()), None)
            .await
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(
            !text.contains("Transactional messages do not support delayed delivery"),
            "{text}"
        );
    }

    #[test]
    fn end_transaction_header_roundtrips_through_ext_fields() {
        // the transaction finish goes out as oneway, so the header encoding must match that of a normal request
        let header = EndTransactionRequestHeader {
            producer_group: Some("GID_x".to_string()),
            transaction_id: Some("txn-1".to_string()),
            commit_log_offset: Some(42),
            tran_state_table_offset: Some(7),
            commit_or_rollback: Some(DefaultMQProducer::transaction_flag(
                LocalTransactionState::CommitMessage,
            )),
            from_transaction_check: Some(true),
            ..Default::default()
        };
        let mut cmd = RemotingCommand::create_request_command(
            request_code::END_TRANSACTION,
            Some(Box::new(header.clone())),
        );
        cmd.make_custom_header_to_net();
        let back: EndTransactionRequestHeader = cmd
            .decode_command_custom_header()
            .expect("the custom header must decode back");
        assert_eq!(back.producer_group.as_deref(), Some("GID_x"));
        assert_eq!(back.commit_log_offset, Some(42));
        assert_eq!(back.from_transaction_check, Some(true));

        let resp = GetEarliestMsgStoretimeResponseHeader {
            timestamp: Some(1234),
        };
        let ext = ExtFields::from_header(&resp);
        let mut back = GetEarliestMsgStoretimeResponseHeader::default();
        back.from_ext_fields(&ext);
        assert_eq!(back.timestamp, Some(1234));
    }

    #[test]
    fn delay_keys_differ_between_hook_and_transaction() {
        // the transaction list lacks __STARTDELIVERTIME: the two lists really differ, pinned so they cannot be merged by accident
        assert!(!TRANSACTION_FORBIDDEN_DELAY_KEYS.contains(&"__STARTDELIVERTIME"));
        assert!(DELAY_PROPERTY_KEYS.contains(&"__STARTDELIVERTIME"));
        assert_eq!(TRANSACTION_FORBIDDEN_DELAY_KEYS.len(), 4);
        assert_eq!(DELAY_PROPERTY_KEYS.len(), 5);
    }

    #[test]
    fn send_context_carries_message_snapshot_and_transfer_flag_is_untouched() {
        let p = producer("GID_snapshot");
        let mut msg = Message::new("T1", Some(b"body"));
        msg.put_property(PROPERTY_TRANSFER_FLAG, "kv");
        let ctx = p.build_send_context(
            &msg,
            "GID_snapshot",
            "ns",
            &mq("broker-a", 2),
            "127.0.0.1:10911",
            CommunicationMode::Async,
        );
        let snapshot = ctx.message.expect("the context must carry the message snapshot");
        assert_eq!(snapshot.get_property(PROPERTY_TRANSFER_FLAG), Some("kv"));
        assert_eq!(snapshot.get_body(), b"body".as_slice());
        assert_eq!(ctx.namespace, "ns");
        assert_eq!(ctx.mq, Some(mq("broker-a", 2)));
    }

    // ---------------- Automatic trace bridging ----------------

    #[test]
    fn auto_bridged_dispatcher_builds_a_real_side_channel() {
        let d = auto_bridged_dispatcher(
            "GID_auto_produce",
            TraceDispatcherType::Produce,
            None,
            25,
            None,
        )
        .expect("the automatic bridging must not fail");
        let g1 = d.trace_producer_group().to_string();
        assert!(
            g1.starts_with("_INNER_TRACE_PRODUCER-GID_auto_produce-PRODUCE-"),
            "{g1}"
        );
        // batch_num is capped at 20
        assert_eq!(d.batch_num(), 20);
        // with trace_topic unset it falls back to the system trace topic
        assert_eq!(d.get_trace_topic_name(), "RMQ_SYS_TRACE_TOPIC");
        assert_eq!(d.dispatcher_type(), TraceDispatcherType::Produce);
        assert!(!d.is_started(), "not started yet, so it must not claim to be started");

        let d2 = auto_bridged_dispatcher(
            "GID_auto_produce",
            TraceDispatcherType::Consume,
            None,
            3,
            Some("CUSTOM_TRACE".to_string()),
        )
        .expect("the automatic bridging must not fail");
        let g2 = d2.trace_producer_group().to_string();
        assert!(
            g2.starts_with("_INNER_TRACE_PRODUCER-GID_auto_produce-CONSUME-"),
            "{g2}"
        );
        // the process-level counter increments: two ids taken for the same group must differ
        let n1: u64 = g1.rsplit('-').next().unwrap().parse().unwrap();
        let n2: u64 = g2.rsplit('-').next().unwrap().parse().unwrap();
        assert!(n2 > n1, "the internal producer group name counter must increment: {g1} -> {g2}");
        assert_eq!(d2.batch_num(), 3);
        assert_eq!(d2.get_trace_topic_name(), "CUSTOM_TRACE");
    }

    #[test]
    fn trace_producer_seam_forwards_setters_to_the_facade() {
        let p = producer("GID_seam");
        TraceProducer::set_name_server_addr(&p, "127.0.0.1:19876");
        TraceProducer::set_instance_name(&p, "TRACE_SEAM");
        TraceProducer::set_send_msg_timeout(&p, 5000);
        TraceProducer::set_max_message_size(&p, 128_000);
        TraceProducer::set_enable_trace(&p, false);
        let cfg = p.config();
        assert_eq!(cfg.name_server_addrs, vec!["127.0.0.1:19876".to_string()]);
        assert_eq!(cfg.instance_name, "TRACE_SEAM");
        assert_eq!(cfg.send_msg_timeout, 5000);
        assert_eq!(cfg.max_message_size, 128_000);
        assert!(!cfg.enable_trace);
    }

    #[test]
    fn trace_host_reports_empty_client_id_before_start() {
        let p = producer("GID_trace_host");
        // the host returns an empty string when it has no _mq_client
        assert_eq!(TraceHost::client_id(&p), "");
    }
}
