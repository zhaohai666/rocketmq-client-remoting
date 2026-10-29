//! Push/pull consumer layer: the consumer-side implementation of the classic remoting protocol.
//!
//! This module holds three things: the pre-delivery filter tools, `MessageSelector`, `PopProcessQueue`,
//! plus [`DefaultMQPushConsumer`] -- the four seams of listener registration + rebalance +
//! the pull/POP consume loop + offset persistence + broker-initiated requests.
//! `DefaultMQPullConsumer` and `DefaultLitePullConsumer` live in
//! [`crate::client::pull_consumer`] (see "Module layout" below).
//!
//! # How the concurrency model is realized
//!
//! The six loop kinds -- rebalance, per-queue pull, POP, consume dispatch, offset persistence, queue locking -- all run
//! on tokio, using `tokio::sync::mpsc` + timed polling for inter-task communication and wake-ups.
//!
//! # A few deliberate structural trade-offs
//!
//! 1. **One `ProcessQueue` per consumer**: the ordered-consumption red-black tree and the `msgTreeMap` lock semantics
//!    only pay off in sequential consumption, which additionally depends on broker-side queue locks; this module
//!    keeps only the concurrent-consumption buffer (pulled messages go straight into `deque`) and introduces no tree.
//! 2. **`Clone` handles + `Arc<Inner>`**: the same technique as [`crate::client::producer`] --
//!    the consumer is held at the same time by the heartbeat task, the pull tasks and the broker request handler, so the handle is copyable and
//!    the state is centralized in `Inner`, guarded by `RwLock`/`Mutex`.
//! 3. **Listeners/strategies/hooks as trait objects**: the consumer must accept user-supplied implementations at runtime,
//!    all held as `Arc<dyn ...>` (`MessageListenerConcurrently`,
//!    [`AllocateMessageQueueStrategy`](crate::client::allocate_strategy::AllocateMessageQueueStrategy)
//!    and so on).
//! 4. **Background tasks hold `Weak<Inner>`**: as with the producer, to avoid the strong-reference cycle
//!    instance -> transport -> task -> consumer; `Drop for Inner` sends the stop signal and aborts the tasks.
//! 5. **Trace dispatcher: injected or auto-bridged**: see
//!    [`set_trace_dispatcher`](Self::set_trace_dispatcher) (the same convention as the producer,
//!    see [`crate::client::producer`] module header 4). With `enable_trace=true` and nothing injected,
//!    `start()` builds an `AsyncTraceDispatcher` (Type=CONSUME) plus a real internal producer automatically,
//!    and traces are reported to the trace topic under the CONSUME type.
//!
//! # Module layout
//!
//! The four consumer classes on the consume side split into two modules:
//! `consumer.rs` (filter tools + `MessageSelector` + `PopProcessQueue` +
//! the push consumer) and `pull_consumer.rs` (`DefaultMQPullConsumer` +
//! `DefaultLitePullConsumer`), because the latter two are only facade compositions over RPC and share no
//! code with the push consumer's loop/rebalance state machine; the only thing the two modules share is the
//! tools this file exports, such as [`filter_messages_for_delivery`].

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;

use crate::client::allocate_strategy::{
    AllocateMessageQueueAveragely, AllocateMessageQueueStrategy,
};
use crate::client::consume_executor::{ConsumeExecutor, ConsumeTask};
use crate::client::consumer_stats::ConsumerStatsManager;
use crate::client::hook::{
    execute_consume_hook_after, execute_consume_hook_before, ConsumeMessageContext,
    ConsumeMessageHook, ConsumeMessageHookList, FilterMessageContext, FilterMessageHookList,
};
use crate::client::mq_client::{
    ConsumerFuture, MQClientInstance, MQClientInstanceConfig, PublishMessage, RegisteredConsumer,
    MQ_CLIENT_API_TIMEOUT_MILLIS,
};
use crate::client::producer::{auto_bridged_dispatcher, SinkAdapter, TraceDispatcherChannel};
use crate::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, ConsumeOrderlyContext,
    ConsumeOrderlyStatus, ConsumeReturnType, MessageListenerConcurrently, MessageListenerOrderly,
    PopStatus, PullStatus,
};
use crate::client::shutdown::{run_finalize_blocking, SHUTDOWN_FINALIZE_BUDGET};
use crate::client::top_addressing::DefaultTopAddressing;
use crate::client::trace::AccessChannel;
use crate::client::trace_dispatcher::{TraceDispatcherType, TraceHost};
use crate::client::trace_hook::{ConsumeMessageTraceHook, TraceReportSink};
use crate::client::validators;
use crate::common::message::{Message, MessageExt, MessageQueue};
use crate::common::message_const::{
    PROPERTY_CONSUME_START_TIMESTAMP, PROPERTY_MAX_OFFSET, PROPERTY_MAX_RECONSUME_TIMES,
    PROPERTY_ORIGIN_MESSAGE_ID, PROPERTY_POP_CK, PROPERTY_RECONSUME_TIME, PROPERTY_RETRY_TOPIC,
    PROPERTY_TRANSACTION_PREPARED,
};
use crate::common::mix_all::MixAll;
use crate::common::sysflag::{ConsumeInitMode, MessageSysFlag, PullSysFlag};
use crate::common::util_all::current_time_millis;
use crate::error::{Error, Result};
use crate::remoting::protocol::admin_body::MessageQueueKey;
use crate::remoting::protocol::body::{
    CMResult, ConsumeMessageDirectlyResult, ConsumerRunningInfo, ProcessQueueInfo,
};
use crate::remoting::protocol::ext_fields::StringMap;
use crate::remoting::protocol::extra_info;
use crate::remoting::protocol::heartbeat::{
    ConsumeFromWhere, ConsumeType, FilterAPI, MessageModel, SubscriptionData,
};
use crate::remoting::protocol::namespace_util::NamespaceUtil;
use crate::remoting::protocol::query_assignment_body::MESSAGE_REQUEST_MODE_POP;
use crate::remoting::protocol::response_code;
use crate::remoting::rpchook::RPCHook;
use crate::remoting::socks::SocksProxyConfig;
use crate::{bail, rmq_debug, rmq_error, rmq_info, rmq_warn};

/// Delay gradient for POP consumption failures (**seconds**), each entry matching the
/// default `popDelayLevel` sequence promised by the classic remoting protocol.
pub const POP_DELAY_LEVEL: [i32; 16] = [
    10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200,
];

/// Lower bound of the allowed invisible time: anything outside `[MIN, MAX]` falls back to 60000.
pub const MIN_POP_INVISIBLE_TIME: i64 = 5000;
/// See [`MIN_POP_INVISIBLE_TIME`].
pub const MAX_POP_INVISIBLE_TIME: i64 = 300000;
/// The fallback value when the invisible time is out of range (protocol default 60000ms).
pub const DEFAULT_POP_INVISIBLE_TIME: i64 = 60_000;

/// Pull idle ceiling (system property `rocketmq.client.pull.pullMaxIdleTime`,
/// default 120000ms): if one pull/POP loop has not **initiated** a pull for longer than this, rebalance deems it stalled
/// and drops and rebuilds it on its own (the stall check uses a strict `>`).
pub const PULL_MAX_IDLE_TIME: i64 = 120_000;

pub const QUERY_ASSIGNMENT_TIMEOUT_MILLIS: i64 = 3000;

/// Maximum lifetime of a queue lock granted by LOCK_BATCH_MQ (milliseconds).
pub const REBALANCE_LOCK_MAX_LIVE_TIME: i64 = 30_000;

/// The shutdown finalize budget for "draining the in-flight batches (including send-back RPC)".
///
/// It comfortably covers one batch of listener work plus per-message send-back (3000ms timeout each); for orderly
/// consumption the pending retry is clamped to at most 30s, and beyond that the wait is abandoned -- offset persistence clamps to the lower bound
/// (using the "remaining minimum offset" measure), so unsettled messages are never skipped.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

// ================================================================ Queues and filter tools

/// Queue sort key: topic -> brokerName -> queueId.
///
/// rebalance sorts the candidate queues and the consumer group members first, then assigns; a different
/// order makes different instances compute different assignment results (the same queue consumed by two instances at once).
pub fn mq_sort_key(mq: &MessageQueue) -> (String, String, i32) {
    (mq.topic.clone(), mq.broker_name.clone(), mq.queue_id)
}

/// Sort the queues by [`mq_sort_key`].
pub fn sort_mqs(mqs: &mut [MessageQueue]) {
    mqs.sort_by_key(mq_sort_key);
}

/// Client-side second pass tag filter.
///
/// The broker side filters by the tag **hash (codeSet)**, so hash collisions can let foreign messages through; the client therefore
/// re-checks by string. The constraint is that the second filter runs only when "the subscription set is non-empty and this is not classFilterMode":
/// subscribing `"*"` (SUB_ALL) means no filtering -- so `SubscriptionData::new` must
/// keep `tags_set` empty for SUB_ALL.
pub fn client_side_tag_filter(
    sub: Option<&SubscriptionData>,
    msgs: Vec<MessageExt>,
) -> Vec<MessageExt> {
    let Some(sub) = sub else {
        return msgs;
    };
    if msgs.is_empty() || sub.tags_set.is_empty() || sub.class_filter_mode {
        return msgs;
    }
    msgs.into_iter()
        .filter(|m| {
            m.get_tags()
                .is_some_and(|t| sub.tags_set.iter().any(|s| s == t))
        })
        .collect()
}

/// Pre-delivery filter = client-side second pass tag filter + FilterMessageHook (shared by the pull/POP/pull-consumer paths).
///
/// The hook receives a **mutable** `msg_list`; how the dropped messages are handled is up to the caller:
/// pull path = skip silently (the offset still advances, no ack);
/// POP path = must ack immediately, otherwise the messages come back for redelivery once `invisibleTime` expires.
pub fn filter_messages_for_delivery(
    consumer_group: &str,
    hooks: &FilterMessageHookList,
    mq: &MessageQueue,
    sub: Option<&SubscriptionData>,
    msgs: Vec<MessageExt>,
    unit_mode: bool,
) -> Vec<MessageExt> {
    let mut out = client_side_tag_filter(sub, msgs);
    if !out.is_empty() && hooks.has_hooks() {
        let mut context = FilterMessageContext::new(consumer_group, Some(out), Some(mq.clone()));
        // The context's `unitMode` comes straight from the consumer's unit-mode setting.
        context.unit_mode = unit_mode;
        crate::client::hook::execute_filter_hooks(hooks, &mut context);
        out = context.msg_list;
    }
    out
}

/// Project the offset table into a `BTreeMap` stably by "queue sort key" (sorting explicitly here,
/// which keeps the `get_consumer_status`/221 response byte-predictable when it is diffed).
pub fn offset_table_to_sorted(
    table: impl IntoIterator<Item = (MessageQueue, i64)>,
) -> BTreeMap<(String, String, i32), i64> {
    let mut out = BTreeMap::new();
    for (mq, offset) in table {
        out.insert(mq_sort_key(&mq), offset);
    }
    out
}

// ================================================================ MessageSelector

/// Where the expression type constants come from (`TAG` / `SQL92` / `CLASS_FILTER`).
///
/// Same convention as [`SubscriptionData::expression_type`]: it stores a **string**, not an enum
/// -- in the classic remoting protocol `expressionType` is simply a string field.
pub use crate::remoting::protocol::heartbeat::ExpressionType;

/// Message selector: the type + content of a subscription expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageSelector {
    /// Expression type (`ExpressionType::TAG` / `SQL92`).
    pub selector_type: String,
    /// Expression content.
    pub expression: String,
}

impl MessageSelector {
    /// Build a selector from "type + expression".
    pub fn new(selector_type: &str, expression: &str) -> MessageSelector {
        MessageSelector {
            selector_type: selector_type.to_string(),
            expression: expression.to_string(),
        }
    }

    /// Select by tag.
    pub fn by_tag(tag: &str) -> MessageSelector {
        MessageSelector::new(ExpressionType::TAG, tag)
    }

    /// Select by a SQL92 expression.
    pub fn by_sql(sql: &str) -> MessageSelector {
        MessageSelector::new(ExpressionType::SQL92, sql)
    }

    /// Subscription expression: sent up directly as the expression argument of `subscribe`.
    pub fn sub_expression(&self) -> &str {
        &self.expression
    }
}

// ================================================================ MessageQueueListener

/// Queue-change listener.
///
/// Held by all three consumers: the push consumer fires it from
/// [`DefaultMQPushConsumer::after_assignment_change`] for the topics whose assigned queue SET
/// moved, the lite-pull consumer from its rebalance step, and the classic pull consumer
/// ([`crate::client::pull_consumer::DefaultMQPullConsumer`]) from the queue set its caller supplies.
pub trait MessageQueueListener: Send + Sync {
    /// Called when the queue assignment changes. `mq_all` is every queue of the topic,
    /// `mq_divided` is the queues assigned to this instance.
    fn message_queue_changed(
        &self,
        topic: &str,
        mq_all: &[MessageQueue],
        mq_divided: &[MessageQueue],
    );
}

// ================================================================ PopProcessQueue

/// Queue state for POP mode.
///
/// Unlike the pull-mode process queue, POP has **no "pulled but unconsumed" buffer**: a message goes straight to
/// a consume thread as soon as it pops, and confirmation happens through ack. Only two things are tracked here:
///
/// - `wait_ack_counter`: how many messages are popped but not acked / not yet had their invisible time extended, used for flow control;
/// - `dropped`: whether the queue has already been taken away by rebalance (once taken away, this batch is neither consumed nor acked,
///   and the broker revives and redelivers it automatically after `invisibleTime` expires).
#[derive(Debug, Default)]
pub struct PopProcessQueue {
    wait_ack_counter: AtomicI32,
    dropped: AtomicBool,
    /// When the last pop was **initiated** (milliseconds).
    /// This field has two readers: the stall check (beyond `PULL_MAX_IDLE_TIME` counts as stalled,
    /// handled by [`State::last_pull_at`]) and the operational view of `ConsumerRunningInfo`.
    pub last_pop_timestamp: AtomicI64,
}

impl PopProcessQueue {
    /// Create a fresh POP queue state, with the pop timestamp initialized to the current time.
    pub fn new() -> Arc<PopProcessQueue> {
        Arc::new(PopProcessQueue {
            last_pop_timestamp: AtomicI64::new(crate::common::util_all::current_time_millis()),
            ..Default::default()
        })
    }

    /// Increase the "waiting for ack" count (by the number of messages popped in this batch).
    pub fn inc_found_msg(&self, count: i32) {
        self.wait_ack_counter.fetch_add(count, Ordering::SeqCst);
    }

    /// Decrease the "waiting for ack" count: pass a **positive** number to reduce how many messages await ack.
    pub fn dec_found_msg(&self, count: i32) {
        self.wait_ack_counter.fetch_add(-count, Ordering::SeqCst);
    }

    /// Ack one message: decrement and return the value after the decrement.
    pub fn ack(&self) -> i32 {
        self.wait_ack_counter.fetch_add(-1, Ordering::SeqCst) - 1
    }

    /// How many messages currently await ack.
    pub fn wait_ack_count(&self) -> i32 {
        self.wait_ack_counter.load(Ordering::SeqCst)
    }

    /// Whether the queue has already been taken away by rebalance.
    pub fn is_dropped(&self) -> bool {
        self.dropped.load(Ordering::SeqCst)
    }

    /// Mark the queue dropped / restore it.
    pub fn set_dropped(&self, dropped: bool) {
        self.dropped.store(dropped, Ordering::SeqCst);
    }

    /// Record one pop timestamp (milliseconds).
    pub fn touch(&self, timestamp_millis: i64) {
        self.last_pop_timestamp
            .store(timestamp_millis, Ordering::SeqCst);
    }
}

/// Message listener: concurrently and orderly.
///
/// The orderly/concurrent decision must be queryable at runtime (the pull loop picks the consume path from it), and Rust has no
/// runtime type introspection, so an enum tag expresses it -- the criterion is "which trait the listener implements".
#[derive(Clone)]
pub enum MessageListener {
    /// Concurrent listener.
    Concurrently(Arc<dyn MessageListenerConcurrently>),
    /// Orderly listener.
    Orderly(Arc<dyn MessageListenerOrderly>),
}

impl MessageListener {
    /// Whether this is an orderly listener.
    pub fn is_orderly(&self) -> bool {
        matches!(self, MessageListener::Orderly(_))
    }
}

impl std::fmt::Debug for MessageListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.is_orderly() {
            "MessageListener::Orderly"
        } else {
            "MessageListener::Concurrently"
        })
    }
}

/// Consumer configuration: a flat collection of every tunable parameter.
///
/// Grouped into one struct of `pub` fields, read and written as a whole by [`DefaultMQPushConsumer::config`] /
/// [`DefaultMQPushConsumer::update_config`], and replaceable at any time (the same technique on the producer side).
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Consumer group name.
    pub consumer_group: String,
    /// Client namespace (the v1 mechanism: prefixes topic/group with `%`).
    pub namespace: String,
    /// Instance name, default `"DEFAULT"`.
    pub instance_name: String,
    /// Explicitly specified clientId; when `None` it is built on the fly inside `start()`.
    pub client_id: Option<String>,
    /// Unit name `unitName` (none by default): when non-empty it goes into the clientId suffix
    /// and becomes the `-<unitName>` segment of the address server URL.
    pub unit_name: Option<String>,
    /// Server-side namespace `namespaceV2` (none by default): the 5.x **server-side** namespace;
    /// when non-empty every request carries the `nsd=true` / `ns=<namespaceV2>` extension header (`NamespaceRpcHook`),
    /// a mechanism entirely separate from the v1 `namespace` client-side `%` prefix.
    pub namespace_v2: Option<String>,
    /// Unit mode `unitMode` (default false): comes into play together with send / send-back / auth / message filtering,
    /// and the broker stamps the UNIT / UNIT_SUB bit on auto-created topics accordingly.
    pub unit_mode: bool,
    /// Streaming request type switch: when true every request carries `ReqT=0`
    /// and the clientId gains a trailing `@STREAM` segment.
    ///
    /// The push consumer never touches this switch; default false.
    pub enable_stream_request_type: bool,
    /// Route refresh interval (default 30000ms): routes of in-use topics refresh on this interval,
    /// passed through to `MQClientInstance` at `start()`.
    pub poll_name_server_interval_millis: u64,
    /// Offset flush interval (default 5000ms): the background offset flush period; the finalize flush in `shutdown()`
    /// is unrelated to it. Also read only once, at `start()`.
    pub persist_consumer_offset_interval_millis: u64,
    /// Name server address list.
    pub name_server_addrs: Vec<String>,
    /// TLS switch; `None` = defer to the environment variable `ROCKETMQ_TLS_ENABLE`.
    pub tls_enable: Option<bool>,
    pub socks_proxy: Option<SocksProxyConfig>,
    /// Message model (a constant from [`MessageModel`]).
    pub message_model: String,
    /// Where to start consuming (a constant from [`ConsumeFromWhere`]).
    pub consume_from_where: String,
    /// Start-position timestamp: `yyyyMMddHHmmss`, default 30 minutes ago.
    pub consume_timestamp: String,
    /// Lower bound of the concurrent consume thread pool, default 20.
    pub consume_thread_min: i32,
    /// Upper bound of the concurrent consume thread pool, default 20 -- equal to min is a deliberate value.
    /// It is also the upper bound of `update_core_pool_size` (the guard is `core < max`),
    /// so with the default configuration concurrency can only be adjusted **downward**.
    pub consume_thread_max: i32,
    /// Backlog threshold for dynamic thread pool growth, default 100000.
    pub adjust_thread_pool_nums_threshold: i64,
    /// Ceiling in milliseconds that `shutdown()` spends waiting for the consume executor to
    /// terminate. Default 0: any `<= 0` value keeps this port's existing bounded drain
    /// ([`SHUTDOWN_DRAIN_TIMEOUT`]); a positive value replaces that ceiling with itself.
    pub await_termination_millis_when_shutdown: i64,
    /// Maximum allowed offset span in concurrent consumption (**messages**), default 2000.
    pub consume_concurrently_max_span: i64,
    /// Per-queue cache message-count threshold, default 1000 (**count**).
    pub pull_threshold_for_queue: i32,
    /// Per-queue cache size threshold, default 100 (**MiB**).
    pub pull_threshold_size_for_queue: i32,
    /// Topic-level cache message-count threshold, default -1 (disabled).
    pub pull_threshold_for_topic: i32,
    /// Topic-level cache size threshold, default -1 (disabled).
    pub pull_threshold_size_for_topic: i32,
    /// Interval between two pulls (default 0 = no wait). The cadence paces **deliveries** on both
    /// the pull and the POP path: only a round that handed rows over waits it out, a round that
    /// handed over nothing re-pulls immediately.
    pub pull_interval: i64,
    /// The **one** generic failed-round backoff in milliseconds, default 1000.
    ///
    /// It is the wait before this client re-issues a pull (or a pop) for a queue after a round that
    /// failed for any reason other than the broker's own flow control: a broker or client error that
    /// is not `FLOW_CONTROL(215)`, a failed initial-offset lookup, and a POP round the broker pushed
    /// back with `POLLING_FULL` or answered with a status this client has no rule for.
    /// The broker's `215` answer, this client's own cache gate and a paused consumer keep their own
    /// fixed waits and ignore this knob. Each queue loop takes the configured value **when the loop
    /// is set up**, so moving the knob afterwards does not alter a loop that is already running.
    pub pull_exception_backoff_millis: i64,
    /// Pull RPC timeout (default 30000ms).
    pub pull_timeout_millis: i64,
    /// Long-polling suspend duration (default 20000ms).
    pub pull_suspend_timeout_millis: i64,
    /// Maximum messages handed to the listener at once, default 1.
    pub consume_message_batch_max_size: i32,
    /// Maximum messages per pull, default 32.
    pub pull_batch_size: i32,
    /// Maximum bytes per pull, default 256 KiB (the `maxMsgBytes` of the request).
    pub pull_batch_size_in_bytes: i32,
    /// Maximum reconsume times, default -1 (deferred to the broker, which by default moves the message to `%DLQ%` after 16 attempts).
    pub max_reconsume_times: i32,
    /// Suspend duration for the current queue in orderly consumption (default 1000ms).
    pub suspend_current_queue_time_millis: i64,
    /// Consume timeout (**minutes**, default 15), used only for the `ConsumeReturnType::TimeOut` decision.
    pub consume_timeout: i64,
    pub client_rebalance: bool,
    /// Broker heartbeat period (default 30000ms).
    pub heartbeat_interval_millis: i64,
    /// Whether to send heartbeats.
    pub heartbeat_enabled: bool,
    /// true uses POP + ack (and queue assignment switches to broker-side QUERY_ASSIGNMENT),
    /// false uses long polling + offsets (local rebalance unchanged).
    pub pop_mode: bool,
    /// POP invisible time (default 60000ms).
    pub pop_invisible_time: i64,
    /// Messages per pop, default 32 (the broker rejects values >32).
    pub pop_batch_nums: i32,
    /// Per-queue waiting-for-ack threshold, default 96.
    pub pop_threshold_for_queue: i32,
    /// Delay gradient for POP failure retries (**seconds**), see [`POP_DELAY_LEVEL`].
    pub pop_delay_level: Vec<i32>,
    /// POP long-polling suspend duration; 0 = short polling. ⚠ When non-zero the request timeout must exceed it.
    pub pop_poll_time_millis: i64,
    /// POP request timeout.
    pub pop_timeout_millis: i64,
    /// Whether to report consume traces.
    pub enable_trace: bool,
    /// Custom trace topic; unset -> `RMQ_SYS_TRACE_TOPIC`.
    pub trace_topic: Option<String>,
    /// Trace batch upload size, default 10.
    pub trace_msg_batch_num: i32,
    /// Whether to send the subscription expression along with PULL_MESSAGE (the SUBSCRIPTION bit), default false:
    /// only needed by server-side scenarios using class filter mode.
    pub post_subscription_when_pull: bool,
    /// Heartbeat fingerprint path (default false). On, this facade's `HEART_BEAT(34)` rounds carry
    /// a non-zero `heartbeatFingerprint`, and once the broker answers `IS_SUPPORT_HEART_BEAT_V2`
    /// and the fingerprint is unchanged the subscription set drops out of the body.
    pub use_heartbeat_v2: bool,
    /// Decode-read-body switch (default true): off, received message bodies stay unmaterialised.
    pub decode_read_body: bool,
    /// Internal-API timeout (default 3000ms) for the client's own control RPCs.
    pub mq_client_api_timeout_millis: i64,
    /// Concurrent heartbeat switch (default false). V2 wins when both are on.
    pub enable_concurrent_heartbeat: bool,
    /// Concurrent heartbeat pool size (default: available processors).
    pub concurrent_heartbeat_thread_pool_size: usize,
}

impl Default for ConsumerConfig {
    fn default() -> ConsumerConfig {
        ConsumerConfig {
            consumer_group: MixAll::DEFAULT_CONSUMER_GROUP.to_string(),
            namespace: String::new(),
            instance_name: DEFAULT_INSTANCE_NAME.to_string(),
            unit_name: None,
            namespace_v2: None,
            unit_mode: false,
            enable_stream_request_type: false,
            // route refresh 1000 * 30, offset flush 1000 * 5
            poll_name_server_interval_millis: 30_000,
            persist_consumer_offset_interval_millis: 5_000,
            client_id: None,
            name_server_addrs: Vec::new(),
            tls_enable: None,
            socks_proxy: None,
            message_model: MessageModel::CLUSTERING.to_string(),
            consume_from_where: ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET.to_string(),
            consume_timestamp: default_consume_timestamp(),
            consume_thread_min: 20,
            consume_thread_max: 20,
            adjust_thread_pool_nums_threshold: 100_000,
            await_termination_millis_when_shutdown: 0,
            consume_concurrently_max_span: 2000,
            pull_threshold_for_queue: 1000,
            pull_threshold_size_for_queue: 100,
            pull_threshold_for_topic: -1,
            pull_threshold_size_for_topic: -1,
            pull_interval: 0,
            pull_exception_backoff_millis: DEFAULT_PULL_EXCEPTION_BACKOFF_MILLIS,
            pull_timeout_millis: 30_000,
            pull_suspend_timeout_millis: 20_000,
            consume_message_batch_max_size: 1,
            pull_batch_size: 32,
            pull_batch_size_in_bytes: 256 * 1024,
            max_reconsume_times: -1,
            suspend_current_queue_time_millis: 1000,
            consume_timeout: 15,
            client_rebalance: true,
            heartbeat_interval_millis: 30_000,
            heartbeat_enabled: true,
            pop_mode: false,
            pop_invisible_time: 60_000,
            pop_batch_nums: 32,
            pop_threshold_for_queue: 96,
            pop_delay_level: POP_DELAY_LEVEL.to_vec(),
            pop_poll_time_millis: 15_000,
            pop_timeout_millis: 25_000,
            enable_trace: false,
            trace_topic: None,
            trace_msg_batch_num: 10,
            post_subscription_when_pull: false,
            use_heartbeat_v2: false,
            decode_read_body: true,
            mq_client_api_timeout_millis: MQ_CLIENT_API_TIMEOUT_MILLIS,
            enable_concurrent_heartbeat: false,
            concurrent_heartbeat_thread_pool_size:
                crate::client::mq_client::default_concurrent_heartbeat_pool_size(),
        }
    }
}

/// Default value of `instance_name` (a same-named constant exists on the producer, but each side keeps its own copy to avoid cross-module coupling).
pub const DEFAULT_INSTANCE_NAME: &str = "DEFAULT";
/// The upper bound of a 16-bit signed integer (32767): the second guard of `update_core_pool_size`.
pub const SHORT_MAX_VALUE: i32 = 32767;

/// Default start-consume timestamp: "now - 30 minutes" in the local timezone, formatted `%Y%m%d%H%M%S`.
pub(crate) fn default_consume_timestamp() -> String {
    let now = chrono::Local::now();
    let past = now - chrono::Duration::minutes(30);
    past.format("%Y%m%d%H%M%S").to_string()
}

/// Parse `consumeTimestamp` into a millisecond timestamp: the field is **only** a 14-digit local wall-clock date.
///
/// It must be parsed in the **local timezone**, otherwise the start point of `CONSUME_FROM_TIMESTAMP` is off by a timezone.
/// A failed parse must fail hard (the server itself rejects startup with `consumeTimestamp is invalid`) --
/// silently falling back to "now - 30 minutes" would shift the start point with nothing to notice.
pub(crate) fn consume_timestamp_millis(text: &str) -> Result<i64> {
    use chrono::TimeZone;
    chrono::NaiveDateTime::parse_from_str(text, "%Y%m%d%H%M%S")
        .ok()
        .and_then(|naive| chrono::Local.from_local_datetime(&naive).single())
        .map(|dt| dt.timestamp_millis())
        .ok_or_else(|| {
            Error::client(format!(
                "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received {text}"
            ))
        })
}

// ================================================================ Configuration numeric gates

/// The numeric part of start-up configuration validation: thirteen range/size gates, where **both the order and the bounds are the criterion**
/// (by convention no error text carries a FAQUrl suffix).
///
/// Why reject in `start()` instead of softening each field at the gate: these values directly decide
/// the buffer watermark and the thread pool size, and every wrong direction is **silent** -- a
/// `pull_threshold_*=0` reaching the cache gate means "nothing may be cached", so the very first
/// held message pauses the pull forever while the consumer reports no error;
/// `pop_batch_nums>32` makes the broker answer
/// `INVALID_PARAMETER` and the POP loop degenerates into error retries; huge values bypass the flow-control checks until OOM. Putting
/// the gates at start-up means rejecting everything at once, **before any instance is created or any task started**.
/// In other words: the legal range of every cache knob is owned here, and the gates read the
/// configured number as it stands, with no floor and no "0 switches me off" escape.
///
/// Two easy-to-get-wrong "asymmetries" must be respected, otherwise valid configurations get rejected:
/// * for `pull_threshold_for_topic` / `pull_threshold_size_for_topic`, `-1` is the
///   "unset, use the queue-level threshold" sentinel, and only `!= -1` enters the range check (`-2` must still be rejected);
/// * the lower bound of `pull_interval` is **0** (not 1), and
///   the `pop_batch_nums` criterion is `<= 0`.
///
/// `pull_exception_backoff_millis` is deliberately **not** gated here: the generic failed-round
/// backoff carries no documented ceiling. `0` is legal and means "re-issue at once", and the loop
/// clamps a negative to the same, so the number a running loop arms with is the configured one.
fn check_config_ranges(cfg: &ConsumerConfig) -> Result<()> {
    // consumeThreadMin
    if cfg.consume_thread_min < 1 || cfg.consume_thread_min > 1000 {
        bail!("consumeThreadMin Out of range [1, 1000]");
    }
    // consumeThreadMax
    if cfg.consume_thread_max < 1 || cfg.consume_thread_max > 1000 {
        bail!("consumeThreadMax Out of range [1, 1000]");
    }
    // consumeThreadMin can't be larger than consumeThreadMax
    if cfg.consume_thread_min > cfg.consume_thread_max {
        bail!(
            "consumeThreadMin ({}) is larger than consumeThreadMax ({})",
            cfg.consume_thread_min,
            cfg.consume_thread_max
        );
    }
    // consumeConcurrentlyMaxSpan
    if cfg.consume_concurrently_max_span < 1 || cfg.consume_concurrently_max_span > 65535 {
        bail!("consumeConcurrentlyMaxSpan Out of range [1, 65535]");
    }
    // pullThresholdForQueue
    if cfg.pull_threshold_for_queue < 1 || cfg.pull_threshold_for_queue > 65535 {
        bail!("pullThresholdForQueue Out of range [1, 65535]");
    }
    // pullThresholdForTopic
    if cfg.pull_threshold_for_topic != -1
        && (cfg.pull_threshold_for_topic < 1 || cfg.pull_threshold_for_topic > 6553500)
    {
        bail!("pullThresholdForTopic Out of range [1, 6553500]");
    }
    // pullThresholdSizeForQueue
    if cfg.pull_threshold_size_for_queue < 1 || cfg.pull_threshold_size_for_queue > 1024 {
        bail!("pullThresholdSizeForQueue Out of range [1, 1024]");
    }
    // pullThresholdSizeForTopic
    if cfg.pull_threshold_size_for_topic != -1
        && (cfg.pull_threshold_size_for_topic < 1 || cfg.pull_threshold_size_for_topic > 102400)
    {
        bail!("pullThresholdSizeForTopic Out of range [1, 102400]");
    }
    // pullInterval
    if cfg.pull_interval < 0 || cfg.pull_interval > 65535 {
        bail!("pullInterval Out of range [0, 65535]");
    }
    // consumeMessageBatchMaxSize
    if cfg.consume_message_batch_max_size < 1 || cfg.consume_message_batch_max_size > 1024 {
        bail!("consumeMessageBatchMaxSize Out of range [1, 1024]");
    }
    // pullBatchSize
    if cfg.pull_batch_size < 1 || cfg.pull_batch_size > 1024 {
        bail!("pullBatchSize Out of range [1, 1024]");
    }
    // popInvisibleTime
    if cfg.pop_invisible_time < MIN_POP_INVISIBLE_TIME
        || cfg.pop_invisible_time > MAX_POP_INVISIBLE_TIME
    {
        bail!("popInvisibleTime Out of range [{MIN_POP_INVISIBLE_TIME}, {MAX_POP_INVISIBLE_TIME}]");
    }
    if cfg.pop_batch_nums <= 0 || cfg.pop_batch_nums > 32 {
        bail!("popBatchNums Out of range [1, 32]");
    }
    Ok(())
}

// ================================================================ Mutable internal state

/// The batch of mutex-protected tables inside the consumer (offsets, buffers, queue ownership ...).
///
/// The keys are always the `topic+brokerName+queueId` string built by [`DefaultMQPushConsumer::mq_key`];
/// `BTreeMap` is only for deterministic iteration order -- the message order of `consumerRunningInfo` depends on it.
#[derive(Default)]
struct State {
    /// Subscription table: topic -> SubscriptionData.
    subscription_data: Vec<(String, SubscriptionData)>,
    /// The queue set currently assigned to this instance.
    assigned: Vec<MessageQueue>,
    /// **Pull cursor** (nextBeginOffset).
    offset_table: BTreeMap<String, i64>,
    /// **Consumed offsets**; this is the table persisted periodically to the broker.
    consume_offsets: BTreeMap<String, i64>,
    /// Pulled-but-unconsumed buffer (messages awaiting dispatch).
    pending: BTreeMap<String, VecDeque<MessageExt>>,
    /// In-flight batch counter: the part of `pending` that has been taken out and is being consumed by a listener.
    /// `dispatch_loop` adds 1 when it takes a batch and subtracts 1 when consumption finishes (including re-queueing after an error).
    /// `pending` plus this counter together are the queue's complete message count, and
    /// the gate in [`correct_tags_offset_locked`] checks both.
    inflight: BTreeMap<String, usize>,
    /// The message bodies dispatched to a listener but not yet settled.
    ///
    /// `pending` (pulled, undispatched) ∪ this = the complete view of the messages on the queue's books; both the
    /// "head-of-queue sweep" of cleanExpiredMsg and [`process_queue_contains_locked`] look at this union.
    /// **Only concurrent batches are registered here**: orderly-consume messages move into a separate
    /// "consuming" set, and the sweep returns early for orderly consumption.
    /// Identity uses `queue_offset` (the offset is unique within one queue; batches on this side are value copies,
    /// so reference identity would not match).
    inflight_msgs: BTreeMap<String, Vec<MessageExt>>,
    /// Offsets frozen by an `OFFSET_ILLEGAL` correction. While frozen, neither in-flight acks nor empty-response fixes may change it,
    /// until the queue is rebuilt with the corrected offset (unfrozen in the create branch of `rebalance_pull_threads`).
    frozen_offsets: BTreeSet<String>,
    /// Per queue, the "generation of the ProcessQueue", incremented on revocation.
    ///
    /// `dispatch_loop` takes the generation together with the batch and compares it before consuming/acking -- a mismatch means this batch
    /// belongs to an already-dropped ProcessQueue and the whole batch must be voided; without it, the ack of an old batch would push
    /// the offset just corrected by the broker back to an illegal value, and the two sides would bounce back and forth.
    queue_epoch: BTreeMap<String, u64>,
    /// Queue key -> MessageQueue.
    mq_map: BTreeMap<String, MessageQueue>,
    /// Queues the broker has confirmed as locked in orderly consumption.
    lock_ok: BTreeSet<String>,
    /// Per queue, when the broker granted the current lock (milliseconds); a grant older than
    /// [`REBALANCE_LOCK_MAX_LIVE_TIME`] counts as expired.
    lock_at: BTreeMap<String, i64>,
    /// Per queue, when a batch was last consumed successfully (milliseconds).
    last_consume_at: BTreeMap<String, i64>,
    /// Per-queue backlog delta for this batch (watermark estimation for topic-level flow control).
    msg_acc_cnt: BTreeMap<String, i64>,
    /// The **ownership token** of each queue's pull/POP loop.
    ///
    /// tokio tasks may migrate between threads, so a thread object cannot express ownership, hence a monotonically increasing token.
    queue_owners: BTreeMap<String, u64>,
    /// Queue key -> [`PopProcessQueue`].
    pop_queues: BTreeMap<String, Arc<PopProcessQueue>>,
    /// Dedup set for POP orderly consumption: one (popping queue, queue) pair registers only one consume request.
    /// Equality is decided by "the reference of the popping queue + the queue key",
    /// expressed here as the Arc pointer address for the reference and [`mq_key`] for the queue.
    pop_orderly_requests: BTreeSet<(usize, String)>,
    /// Per queue, when the last pull/pop was **initiated** (milliseconds).
    ///
    /// The stamp happens before the loop **initiates** the network request, so neither long-polling suspension nor flow-control waiting
    /// can make a still-running loop look stalled. It is written as `0` when the loop has exited but still holds ownership (Rust tasks have no `is_alive()`,
    /// so this sentinel expresses "dead").
    last_pull_at: BTreeMap<String, i64>,
    /// Queue -> the brokerId to pick for the next pull.
    ///
    /// Driven by the `suggestWhichBrokerId` header of the pull response, accounted as master(=0) by default;
    /// the next pull picks master/slave accordingly. `HashMap` rather than `BTreeMap`: `MessageQueue`
    /// already implements `Hash + Eq`, and this table is never serialized, so iteration order has no effect.
    pull_from_which_node: HashMap<MessageQueue, i64>,
}

impl State {
    fn subscription(&self, topic: &str) -> Option<&SubscriptionData> {
        self.subscription_data
            .iter()
            .find(|(k, _)| k == topic)
            .map(|(_, v)| v)
    }
}

struct Inner {
    cfg: RwLock<ConsumerConfig>,
    state: Mutex<State>,
    client: Mutex<Option<MQClientInstance>>,
    started: AtomicBool,
    runtime: OnceLock<tokio::runtime::Handle>,
    stop: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// Handle of the dispatch loop, **not kept** in [`Inner::tasks`]: at shutdown it may be doing a
    /// send-back RPC with a batch of messages, and abort would cut the in-flight send-back in half. Shutdown therefore
    /// takes the "wait a bounded time for it to finish the current batch" route (see the finalize task of [`DefaultMQPushConsumer::shutdown`]).
    dispatch_task: Mutex<Option<JoinHandle<()>>>,
    listener: Mutex<Option<MessageListener>>,
    /// The application's queue-change listener, fired for the topics whose assignment SET moved.
    ///
    /// A change notification, not a periodic report: an unchanged round calls it zero times.
    queue_listener: Mutex<Option<Arc<dyn MessageQueueListener>>>,
    strategy: RwLock<Arc<dyn AllocateMessageQueueStrategy>>,
    consume_hooks: ConsumeMessageHookList,
    filter_hooks: FilterMessageHookList,
    stats: Mutex<Option<Arc<ConsumerStatsManager>>>,
    pop_executor: Mutex<Option<ConsumeExecutor>>,
    trace: Mutex<Option<Arc<dyn TraceDispatcherChannel>>>,
    rpc_hook: RwLock<Option<Arc<dyn RPCHook>>>,
    /// Heartbeats sent so far (for observation / unit tests).
    heartbeat_count: AtomicUsize,
    /// Local flow-control trigger count (lock-free increment).
    flow_control_triggered: AtomicUsize,
    /// The declared value of the thread pool core size.
    core_pool_size: AtomicI32,
    /// This implementation does not support an injected executor, so this is always true;
    /// the field is kept so that the guard shape of `update_core_pool_size` stays complete.
    owns_consume_executor: AtomicBool,
    /// Start-up time (milliseconds).
    start_time_millis: AtomicI64,
    /// The "rebalance right now" request flag.
    ///
    /// ⚠ Uses "a flag + `Notify`" rather than a bare `Notify::notify_waiters()`: the latter wakes only
    /// the tasks waiting **at that moment**, so a notification landing between two rounds is lost; the flag is persistent,
    /// which closes that semantic gap.
    rebalance_now: AtomicBool,
    rebalance_signal: Notify,
    /// Token issuer for [`State::queue_owners`].
    next_token: AtomicU64,
    /// Pause flag: [`suspend`](DefaultMQPushConsumer::suspend) sets it true,
    /// [`resume`](DefaultMQPushConsumer::resume) sets it false.
    /// Seeing true, the pull/pop loop sends no request and retries after suspending for
    /// [`PULL_TIME_DELAY_MILLS_WHEN_SUSPEND`].
    pause: AtomicBool,
}

// ================================================================ Flow control constants

/// Default of [`ConsumerConfig::pull_exception_backoff_millis`]: the wait a pull (or POP) round
/// arms before re-issuing its request whenever the round failed for a reason other than the
/// broker's own flow control. It is a per-client knob, not a constant: this number is only the
/// value the knob carries when nobody sets it.
pub const DEFAULT_PULL_EXCEPTION_BACKOFF_MILLIS: i64 = 1000;
/// Backoff when the local cache (to consume / to ack) saturates flow control.
pub const PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL: u64 = 50;
/// Backoff when the broker answers `FLOW_CONTROL(215)`
/// (much shorter than a normal exception: the broker only asks the client to slow down).
pub const PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL: u64 = 20;
/// Polling interval after `suspend()`.
pub const PULL_TIME_DELAY_MILLS_WHEN_SUSPEND: u64 = 1000;
/// A throttled queue trips a cache gate on every pull round, so warning every round buries the
/// signal: only every Nth trip warns.
pub const FLOW_CONTROL_WARN_EVERY: usize = 1000;
/// One MiB. Every cached-size gate in this client states its threshold in **whole MiB of message
/// body**, so the comparison runs on the integer division result and a partially filled MiB of
/// backlog never costs a unit of the budget.
pub(crate) const CACHE_SIZE_GATE_MIB: i64 = 1024 * 1024;

/// Whether the n-th (1-based) flow-control trip is one that logs a warn: the 1st, then every
/// [`FLOW_CONTROL_WARN_EVERY`]-th trip after it.
pub fn is_flow_control_warn_trip(times: usize) -> bool {
    times > 0 && (times - 1) % FLOW_CONTROL_WARN_EVERY == 0
}

/// The wait a failed pull (or POP) request arms before this client re-issues it, decided by the
/// answer alone:
///
/// * the broker's own throttle (`FLOW_CONTROL(215)`) gets its short fixed pause, which says "slow
///   down" and nothing about a real failure;
/// * every other failure -- a broker error, a client error, anything the round did not expect --
///   gets the consumer's generic failed-round backoff.
///
/// The backoff is a **parameter**, not a read of the live knob: the loop captures the configured
/// value when it is set up and passes that in, so moving the knob afterwards cannot change a wait a
/// running loop already arms with.
pub(crate) fn failed_request_backoff_millis(
    err: &Error,
    pull_exception_backoff_millis: u64,
) -> u64 {
    match err {
        Error::Server {
            response_code: response_code::FLOW_CONTROL,
            ..
        }
        | Error::Broker {
            response_code: response_code::FLOW_CONTROL,
            ..
        } => PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL,
        _ => pull_exception_backoff_millis,
    }
}

impl Drop for Inner {
    /// Forgetting `shutdown()` must not leave loops still running (see the same-named note in the producer module header).
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        for task in self
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            task.abort();
        }
    }
}

/// Acquire the lock; on poisoning keep using it, for the same reason as `consume_executor::lock`.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn read_cfg(inner: &Inner) -> ConsumerConfig {
    inner.cfg.read().unwrap_or_else(|e| e.into_inner()).clone()
}

fn require_client(inner: &Inner) -> Result<MQClientInstance> {
    if !inner.started.load(Ordering::Acquire) {
        return Err(Error::client("consumer not started, call start() first"));
    }
    lock(&inner.client)
        .clone()
        .ok_or_else(|| Error::client("consumer not started, call start() first"))
}

/// Getting the client instance for **internal send-back** inside the shutdown drain window: it only checks that the instance exists, not `started`.
///
/// The first step of `shutdown()` sets `started` to false, and only then drains the in-flight batches (waiting for send-back RPCs to
/// get their response). If send-back reused the started gate of [`require_client`], the drain would get nothing but
/// "consumer not started" fast failures -- not one send-back could go out, and messages could only be redelivered from the original
/// topic via the offset lower bound, never entering `%RETRY%`/`%DLQ%` (caught by the real-broker exit-immediately test, see
/// `examples/live_shutdown_race.rs`). The send-back RPC itself depends only on the client instance, not on the consumer's own
/// serviceState; the instance itself is removed only in the very last step of the shutdown finalize,
/// so requiring it to exist here is all that is needed.
fn require_client_for_send_back(inner: &Inner) -> Result<MQClientInstance> {
    lock(&inner.client)
        .clone()
        .ok_or_else(|| Error::client("consumer not started, call start() first"))
}

/// The queue key built from topic + brokerName + queueId (no separator).
pub fn mq_key(mq: &MessageQueue) -> String {
    format!("{}{}{}", mq.topic, mq.broker_name, mq.queue_id)
}

// ================================================================ DefaultMQPushConsumer

/// Push-mode consumer: the facade handle over subscription + rebalance + the pull/POP consume loop.
///
/// Lifecycle: `subscribe(..)` -> [`start`](Self::start) -> background rebalance + consumption ->
/// [`shutdown`](Self::shutdown). Clones share one and the same state (structural trade-off 2).
#[derive(Clone)]
pub struct DefaultMQPushConsumer {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for DefaultMQPushConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cfg = read_cfg(&self.inner);
        f.debug_struct("DefaultMQPushConsumer")
            .field("consumer_group", &cfg.consumer_group)
            .field("namespace", &cfg.namespace)
            .field("instance_name", &cfg.instance_name)
            .field("client_id", &cfg.client_id)
            .field("name_server_addrs", &cfg.name_server_addrs)
            .field("message_model", &cfg.message_model)
            .field("pop_mode", &cfg.pop_mode)
            .field("started", &self.inner.started.load(Ordering::Acquire))
            .field(
                "subscriptions",
                &lock(&self.inner.state).subscription_data.len(),
            )
            .finish()
    }
}

impl DefaultMQPushConsumer {
    /// Construct for a consumer group only; a blank group name is an error
    /// (`MQClientException("consumerGroup is empty")`).
    pub fn new(consumer_group: &str) -> Result<DefaultMQPushConsumer> {
        let cfg = ConsumerConfig {
            consumer_group: consumer_group.to_string(),
            ..Default::default()
        };
        DefaultMQPushConsumer::with_config(cfg)
    }

    /// Construct with an RPC hook (request signing).
    pub fn with_rpc_hook(
        consumer_group: &str,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Result<DefaultMQPushConsumer> {
        let consumer = DefaultMQPushConsumer::new(consumer_group)?;
        consumer.set_rpc_hook(rpc_hook);
        Ok(consumer)
    }

    /// Construct directly from a complete configuration (equivalent to setting the properties one by one and then starting).
    pub fn with_config(cfg: ConsumerConfig) -> Result<DefaultMQPushConsumer> {
        if cfg.consumer_group.trim().is_empty() {
            bail!("consumerGroup is empty");
        }
        let inner = Arc::new(Inner {
            cfg: RwLock::new(cfg),
            state: Mutex::new(State::default()),
            client: Mutex::new(None),
            started: AtomicBool::new(false),
            runtime: OnceLock::new(),
            stop: watch::channel(false).0,
            tasks: Mutex::new(Vec::new()),
            dispatch_task: Mutex::new(None),
            listener: Mutex::new(None),
            queue_listener: Mutex::new(None),
            strategy: RwLock::new(Arc::new(AllocateMessageQueueAveragely)),
            consume_hooks: ConsumeMessageHookList::new(),
            filter_hooks: FilterMessageHookList::new(),
            stats: Mutex::new(None),
            pop_executor: Mutex::new(None),
            trace: Mutex::new(None),
            rpc_hook: RwLock::new(None),
            heartbeat_count: AtomicUsize::new(0),
            flow_control_triggered: AtomicUsize::new(0),
            core_pool_size: AtomicI32::new(0),
            owns_consume_executor: AtomicBool::new(true),
            start_time_millis: AtomicI64::new(0),
            rebalance_now: AtomicBool::new(false),
            rebalance_signal: Notify::new(),
            next_token: AtomicU64::new(1),
            pause: AtomicBool::new(false),
        });
        let consumer = DefaultMQPushConsumer { inner };
        // The declared core pool capacity starts equal to consume_thread_min
        let min = consumer.config().consume_thread_min;
        consumer
            .inner
            .core_pool_size
            .store(min.max(1), Ordering::SeqCst);
        Ok(consumer)
    }

    /// Snapshot of the current configuration.
    pub fn config(&self) -> ConsumerConfig {
        read_cfg(&self.inner)
    }

    /// Change the configuration. After `start()` this should be rejected except for the few runtime-tunable items.
    pub fn update_config(&self, f: impl FnOnce(&mut ConsumerConfig)) {
        {
            let mut w = self.inner.cfg.write().unwrap_or_else(|e| e.into_inner());
            f(&mut w);
        }
        // When the thread bounds change, also sync the declared core pool capacity
        let cfg = self.config();
        let cur = self.inner.core_pool_size.load(Ordering::SeqCst);
        if cur < cfg.consume_thread_min {
            self.inner
                .core_pool_size
                .store(cfg.consume_thread_min, Ordering::SeqCst);
        }
        self.apply_core_pool_size();
    }

    pub fn consumer_group(&self) -> String {
        read_cfg(&self.inner).consumer_group
    }

    pub fn client_id(&self) -> String {
        read_cfg(&self.inner).client_id.clone().unwrap_or_default()
    }

    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::Acquire)
    }

    /// The shared [`MQClientInstance`] handle; `None` before start-up.
    pub fn client(&self) -> Option<MQClientInstance> {
        lock(&self.inner.client).clone()
    }

    pub fn set_namesrv_addr(&self, addr: &str) {
        let addrs = addr
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect::<Vec<String>>();
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

    /// Set the unit name: `None`/blank is equivalent to not setting it (blank is judged when building the clientId).
    pub fn set_unit_name(&self, unit_name: Option<&str>) {
        let unit_name = unit_name.map(str::to_string);
        self.update_config(|c| c.unit_name = unit_name);
    }

    /// Set the 5.x **server-side** namespace (the `ns`/`nsd`
    /// extension headers, see `NamespaceRpcHook`). `None`/empty string = not set, and the hook degenerates to a no-op.
    /// Passed through to `MQClientInstance` at `start()`; changing it after start does not affect an instance already built.
    pub fn set_namespace_v2(&self, namespace_v2: Option<&str>) {
        let namespace_v2 = namespace_v2.map(str::to_string);
        self.update_config(|c| c.namespace_v2 = namespace_v2);
    }

    /// Read the current server-side namespace.
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

    /// Set the streaming request type switch (when true requests carry `ReqT=0` and the clientId gains a trailing `@STREAM`).
    pub fn set_enable_stream_request_type(&self, enable: bool) {
        self.update_config(|c| c.enable_stream_request_type = enable);
    }

    /// Set whether the subscription expression is sent up with PULL_MESSAGE (default false).
    ///
    /// Once on, every PULL_MESSAGE posts the subscription expression along with the request (the SUBSCRIPTION bit is set),
    /// letting the broker side do the filtering; off by default -- filtering is
    /// covered by the client-side `filter_messages_for_delivery` fallback, so this should be on only when the broker needs the subscription expression for class filter.
    pub fn set_post_subscription_when_pull(&self, enable: bool) {
        self.update_config(|c| c.post_subscription_when_pull = enable);
    }

    /// Set the start-position strategy (a constant from [`ConsumeFromWhere`]).
    pub fn set_consume_from_where(&self, where_: &str) {
        let where_ = where_.to_string();
        self.update_config(|c| c.consume_from_where = where_);
    }

    /// Set the start point used by `CONSUME_FROM_TIMESTAMP`, formatted `yyyyMMddHHmmss`.
    pub fn set_consume_timestamp(&self, timestamp: &str) {
        let timestamp = timestamp.to_string();
        self.update_config(|c| c.consume_timestamp = timestamp);
    }

    /// Set the route refresh period for in-use topics.
    pub fn set_poll_name_server_interval_millis(&self, millis: u64) {
        self.update_config(|c| c.poll_name_server_interval_millis = millis);
    }

    /// Decode-read-body switch (default true). Off, received bodies stay unmaterialised; queue
    /// offsets and properties are still decoded.
    pub fn set_decode_read_body(&self, enable: bool) {
        self.update_config(|c| c.decode_read_body = enable);
        if let Some(client) = self.client() {
            client.set_decode_read_body(enable);
        }
    }

    pub fn is_decode_read_body(&self) -> bool {
        read_cfg(&self.inner).decode_read_body
    }

    /// Internal-API timeout for the client's own control RPCs (default 3000ms).
    pub fn set_mq_client_api_timeout_millis(&self, timeout_millis: i64) {
        self.update_config(|c| c.mq_client_api_timeout_millis = timeout_millis);
        if let Some(client) = self.client() {
            client.set_mq_client_api_timeout_millis(timeout_millis);
        }
    }

    pub fn mq_client_api_timeout_millis(&self) -> i64 {
        read_cfg(&self.inner).mq_client_api_timeout_millis
    }

    /// Set the background offset flush period.
    pub fn set_persist_consumer_offset_interval_millis(&self, millis: u64) {
        self.update_config(|c| c.persist_consumer_offset_interval_millis = millis);
    }

    /// Cap how long `shutdown()` waits for the consume executor to terminate, in milliseconds.
    ///
    /// `<= 0` (the default 0) keeps this port's existing bounded drain; a positive value
    /// replaces that bound with the configured ceiling. The value is read once per `shutdown()`
    /// call, so changing it afterwards only takes effect on the next shutdown.
    pub fn set_await_termination_millis_when_shutdown(&self, millis: i64) {
        self.update_config(|c| c.await_termination_millis_when_shutdown = millis);
    }

    /// Read the configured consume-executor termination ceiling back.
    pub fn await_termination_millis_when_shutdown(&self) -> i64 {
        read_cfg(&self.inner).await_termination_millis_when_shutdown
    }

    /// Set the **one** generic failed-round backoff: the wait before this client re-issues a pull
    /// (or a pop) for a queue after a round that failed for any reason other than the broker's own
    /// flow control -- a non-`FLOW_CONTROL(215)` error, a failed initial-offset lookup, and a POP
    /// round answered with `POLLING_FULL` or with a status this client has no rule for.
    /// 1000 ms when untouched.
    ///
    /// The broker's `215` answer (20 ms), this client's own cache gate (50 ms) and a paused
    /// consumer (1000 ms) keep their own fixed waits and ignore this knob. Each queue loop takes the
    /// configured value when it is set up, so changing it after `start()` does not alter a loop that
    /// is already running.
    pub fn set_pull_exception_backoff_millis(&self, millis: i64) {
        self.update_config(|c| c.pull_exception_backoff_millis = millis);
    }

    /// Read the configured generic failed-round backoff back.
    pub fn pull_exception_backoff_millis(&self) -> i64 {
        read_cfg(&self.inner).pull_exception_backoff_millis
    }

    /// Read-only access to the shared [`MQClientInstance`]: the static-topic leg uses it to read endpoint rewrite results; errors when not started.
    pub fn mq_client(&self) -> Result<MQClientInstance> {
        if !self.inner.started.load(Ordering::Acquire) {
            return Err(Error::client("consumer not started, call start() first"));
        }
        lock(&self.inner.client)
            .clone()
            .ok_or_else(|| Error::client("consumer not started, call start() first"))
    }

    /// Register the application's queue-change listener.
    ///
    /// It hears about the topics whose assigned queue SET moved (growth, shrink, a same-count
    /// hand-over to a different broker, a topic that appears or disappears); a round that re-pulled
    /// the same queues calls it zero times.
    pub fn set_message_queue_listener(&self, listener: Arc<dyn MessageQueueListener>) {
        *lock(&self.inner.queue_listener) = Some(listener);
    }

    /// Register a message listener (concurrent or orderly).
    pub fn set_message_listener(&self, listener: MessageListener) {
        *lock(&self.inner.listener) = Some(listener);
    }

    /// Register a concurrent consume listener.
    pub fn set_message_listener_concurrently(
        &self,
        listener: Arc<dyn MessageListenerConcurrently>,
    ) {
        self.set_message_listener(MessageListener::Concurrently(listener));
    }

    /// Register an orderly consume listener.
    pub fn set_message_listener_orderly(&self, listener: Arc<dyn MessageListenerOrderly>) {
        self.set_message_listener(MessageListener::Orderly(listener));
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

    /// The current queue allocation strategy.
    pub fn allocate_message_queue_strategy(&self) -> Arc<dyn AllocateMessageQueueStrategy> {
        self.inner
            .strategy
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn set_rpc_hook(&self, hook: Option<Arc<dyn RPCHook>>) {
        *self
            .inner
            .rpc_hook
            .write()
            .unwrap_or_else(|e| e.into_inner()) = hook;
    }

    /// The currently registered rpc hook (passed through to the internal producer when auto-bridging the trace dispatcher).
    pub(crate) fn rpc_hook(&self) -> Option<Arc<dyn RPCHook>> {
        self.inner
            .rpc_hook
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Inject the trace dispatcher (structural trade-off 5).
    pub fn set_trace_dispatcher(&self, dispatcher: Option<Arc<dyn TraceDispatcherChannel>>) {
        *lock(&self.inner.trace) = dispatcher;
    }

    pub fn trace_dispatcher(&self) -> Option<Arc<dyn TraceDispatcherChannel>> {
        lock(&self.inner.trace).clone()
    }

    pub fn register_consume_message_hook(&self, hook: Arc<dyn ConsumeMessageHook>) {
        self.inner.consume_hooks.register(hook);
    }

    pub fn has_consume_message_hook(&self) -> bool {
        self.inner.consume_hooks.has_hooks()
    }

    pub fn register_filter_message_hook(
        &self,
        hook: Arc<dyn crate::client::hook::FilterMessageHook>,
    ) {
        self.inner.filter_hooks.register(hook);
    }

    pub fn has_filter_message_hook(&self) -> bool {
        self.inner.filter_hooks.has_hooks()
    }

    // ---------------- Subscription ----------------

    /// Subscribe to a topic; the expression defaults to `"*"`.
    ///
    /// This **may still** be called after `start()`: the subscription table is live (the heartbeat and every rebalance round re-read it),
    /// and right after the put a heartbeat round is pushed (see [`Self::notify_subscription_changed`]).
    pub fn subscribe(&self, topic: &str, sub_expression: &str) -> Result<()> {
        let topic = self.with_namespace(topic);
        let sub = FilterAPI::build_subscription_data(&topic, Some(sub_expression))?;
        self.put_subscription(topic.clone(), sub);
        self.notify_subscription_changed(&topic);
        Ok(())
    }

    /// Subscribe to all messages of a topic (equivalent to `subscribe(topic, "*")`).
    pub fn subscribe_all(&self, topic: &str) -> Result<()> {
        self.subscribe(topic, "*")
    }

    /// Subscribe using a [`MessageSelector`].
    ///
    /// Dispatched by expression type:
    /// TAG (or an empty type) goes through `buildSubscriptionData`, filling `tagsSet`/`codeSet`;
    /// SQL92 / CLASS_FILTER set only topic/subString/expressionType and leave both collections empty.
    /// Same as `subscribe`: callable after start(), and it pushes a heartbeat round immediately.
    pub fn subscribe_with_selector(&self, topic: &str, selector: &MessageSelector) -> Result<()> {
        let topic = self.with_namespace(topic);
        let sub = if selector.selector_type == ExpressionType::TAG {
            let mut s = FilterAPI::build_subscription_data(&topic, Some(&selector.expression))?;
            s.expression_type = selector.selector_type.clone();
            s
        } else {
            if selector.expression.is_empty() {
                bail!("Expression can't be null! {}", selector.selector_type);
            }
            SubscriptionData {
                topic: topic.clone(),
                sub_string: selector.expression.clone(),
                expression_type: selector.selector_type.clone(),
                ..Default::default()
            }
        };
        self.put_subscription(topic.clone(), sub);
        self.notify_subscription_changed(&topic);
        Ok(())
    }

    /// Unsubscribe from a topic.
    ///
    /// This **only removes the table entry and sends no heartbeat** (the next heartbeat round naturally carries the new subscription set),
    /// so it deliberately does not call `notify_subscription_changed`.
    pub fn unsubscribe(&self, topic: &str) {
        let topic = self.with_namespace(topic);
        let mut state = lock(&self.inner.state);
        state.subscription_data.retain(|(k, _)| *k != topic);
    }

    /// Immediate actions after an insert/update of the subscription table: push a heartbeat round synchronously + register the topic as "in use".
    ///
    /// Why it must be immediate:
    /// the broker's `ConsumerManager` records topic->group into its own
    /// topicGroupTable only when it receives a heartbeat (the registration is what creates it), and
    /// `QUERY_TOPIC_CONSUME_BY_WHO(300)` reads exactly that table; being one round late is a 30s window by default.
    ///
    /// It also registers the new topic as "in use" so the background route refresh task covers it --
    /// when the refresh task collects "which topics to refresh" it reads the consumer's **current** subscription table,
    /// so a new subscription must be visible on the spot to be covered.
    /// Before start-up there is no client, so only the subscription table is written.
    ///
    /// `subscribe` has a synchronous signature while the heartbeat is an async RPC: it is queued on the runtime as fire-and-forget
    /// (the same `runtime_handle()` technique the `rebalance` step uses to start each per-queue pull loop).
    fn notify_subscription_changed(&self, topic: &str) {
        let Ok(client) = require_client(&self.inner) else {
            return;
        };
        client.register_topic_in_use(topic);
        let Some(handle) = self.runtime_handle() else {
            rmq_debug!("subscribe: no tokio runtime, immediate heartbeat skipped");
            return;
        };
        let weak = Arc::downgrade(&self.inner);
        handle.spawn(async move {
            if let Some(inner) = weak.upgrade() {
                heartbeat_once(&inner).await;
            }
        });
    }

    fn put_subscription(&self, topic: String, sub: SubscriptionData) {
        let mut state = lock(&self.inner.state);
        match state
            .subscription_data
            .iter_mut()
            .find(|(k, _)| *k == topic)
        {
            Some(slot) => slot.1 = sub,
            None => state.subscription_data.push((topic, sub)),
        }
    }

    /// Snapshot of the current subscription table.
    pub fn subscriptions(&self) -> Vec<SubscriptionData> {
        lock(&self.inner.state)
            .subscription_data
            .iter()
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// Prefix the topic with the client namespace `%` (every subscription entry point passes through this step first).
    fn with_namespace(&self, topic: &str) -> String {
        let namespace = read_cfg(&self.inner).namespace;
        if namespace.is_empty() {
            topic.to_string()
        } else {
            NamespaceUtil::wrap_namespace(&namespace, topic)
        }
    }

    // ---------------- Lifecycle ----------------

    /// Start the consumer: create the instance, register, fetch routes, first heartbeat and rebalance, then launch the background loops.
    ///
    /// The order is the criterion: validate -> build namespaces -> create and start the instance -> register the consumer ->
    /// auto-subscribe `%RETRY%` -> register NOTIFY_CONSUMER_IDS_CHANGED(40) -> build the POP executor ->
    /// fetch routes -> **synchronous** first heartbeat -> **synchronous** first rebalance -> start each loop -> trace dispatcher.
    ///
    /// Two deliberate implementation choices (behaviour unchanged):
    /// 1. The heartbeat message is assembled by [`MQClientInstance::send_heartbeat_to_all_broker`] from the registry into a
    ///    full `ConsumerData`, instead of the consumer building one itself. Because `register_consumer` runs before it,
    ///    this consumer is certainly present in the message.
    /// 2. The background loops are tokio tasks; a missing runtime handle (no runtime) only warns and does not error, as on the producer side.
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
        let group = if cfg.namespace.is_empty() {
            cfg.consumer_group.clone()
        } else {
            NamespaceUtil::wrap_namespace(&cfg.namespace, &cfg.consumer_group)
        };
        if let Err(e) = validators::check_group(&group) {
            self.inner.started.store(false, Ordering::Release);
            return Err(e);
        }
        if group == MixAll::DEFAULT_CONSUMER_GROUP {
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
            if state.subscription_data.is_empty() {
                self.inner.started.store(false, Ordering::Release);
                bail!("subscription is not set, call subscribe() first");
            }
        }
        if !lock(&self.inner.listener).is_some() {
            self.inner.started.store(false, Ordering::Release);
            bail!("message listener is not set");
        }
        if let Err(e) = consume_timestamp_millis(&cfg.consume_timestamp) {
            self.inner.started.store(false, Ordering::Release);
            return Err(e);
        }
        if let Err(e) = check_config_ranges(&cfg) {
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
            c.consumer_group = group.clone();
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
            decode_read_body: cfg.decode_read_body,
            mq_client_api_timeout_millis: cfg.mq_client_api_timeout_millis,
            enable_concurrent_heartbeat: cfg.enable_concurrent_heartbeat,
            concurrent_heartbeat_thread_pool_size: cfg.concurrent_heartbeat_thread_pool_size,
            persist_offset_interval_millis: cfg.persist_consumer_offset_interval_millis,
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
        *lock(&self.inner.client) = Some(client.clone());
        // Dynamic name server: the instance may already have obtained addresses from the address server, so backfill them.
        if cfg.name_server_addrs.is_empty() {
            let addrs = client.name_server_addrs();
            if !addrs.is_empty() {
                self.update_config(|c| c.name_server_addrs = addrs);
            }
        }
        *lock(&self.inner.stats) = Some(client.consumer_stats_manager().clone());

        if self.config().message_model != MessageModel::BROADCASTING {
            let retry_topic = MixAll::get_retry_topic(&group);
            let exist = lock(&self.inner.state)
                .subscription_data
                .iter()
                .any(|(k, _)| *k == retry_topic);
            if !exist {
                let sub = FilterAPI::build_subscription_data(&retry_topic, Some("*"))?;
                self.put_subscription(retry_topic, sub);
            }
        }

        // Broker-side NOTIFY_CONSUMER_IDS_CHANGED(40) is handled centrally by the **instance** and fanned back out
        // (`RegisteredConsumer::rebalance_immediately`, see mq_client.rs); this consumer
        // does not register its own handler -- the signal flag below must be set before register_consumer,
        // otherwise the first `do_rebalance` would be triggered one extra time by its own wake-up.
        self.inner
            .start_time_millis
            .store(current_time_millis(), Ordering::SeqCst);
        self.inner.rebalance_now.store(false, Ordering::SeqCst);
        let _ = self.inner.stop.send(false);
        client.register_consumer(&group, Arc::new(self.clone()));

        if self.config().pop_mode {
            let cfg = self.config();
            let executor = ConsumeExecutor::with_params(
                self.inner.core_pool_size.load(Ordering::SeqCst).max(1),
                cfg.consume_thread_max.max(1),
                Duration::from_secs(60),
                format!("rmq-popconsume-{}", cfg.consumer_group),
            );
            if let Some(handle) = self.runtime_handle() {
                let executor = executor.with_handle(&handle);
                *lock(&self.inner.pop_executor) = Some(executor);
            } else {
                *lock(&self.inner.pop_executor) = Some(executor);
            }
        }

        // Fetch routes once -> synchronous first heartbeat -> synchronous first rebalance -> start the loops (fixed order)
        self.refresh_routes().await;
        if let Err(e) = client.check_client_in_broker().await {
            self.shutdown();
            return Err(e);
        }
        let ok = client
            .send_heartbeat_to_all_broker(client.mq_client_api_timeout_millis())
            .await;
        if ok > 0 {
            self.inner.heartbeat_count.fetch_add(1, Ordering::SeqCst);
        }
        if let Err(e) = self.do_rebalance().await {
            rmq_warn!("initial rebalance failed: {e}");
        }
        self.spawn_loops();
        self.start_trace_dispatcher();
        Ok(())
    }

    /// Shutdown: drain the in-flight batches, flush offsets, unlock the queues, unregister from the instance.
    ///
    /// ⚠ Unlike the old "spawn and return immediately" shape: the cleanup (wait for in-flight batches -> flush offsets/unlock ->
    /// unregister via code 35 -> tear down the instance) runs on the runtime, but this method **blocks waiting for it to land** (bounded by
    /// [`SHUTDOWN_FINALIZE_BUDGET`]). "Exit the process right after shutdown() returns" no longer loses
    /// in-flight send-backs -- messages bound for `%DLQ%` have already been sent before the return. Inside a
    /// current_thread runtime it cannot block (that would panic / deadlock), so the finalize stays a detached task and warns (see
    /// cases 2/3 of [`run_finalize_blocking`]).
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
        {
            // POP: mark every queue dropped so in-flight batches no longer ack (leave redelivery to the broker's revival)
            let mut state = lock(&self.inner.state);
            if self.config().pop_mode {
                for pq in state.pop_queues.values() {
                    pq.set_dropped(true);
                }
                state.pop_queues.clear();
            }
            state.queue_owners.clear();
            // Offsets left behind after shutdown would make the next
            // start()'s self-healing check read the stamp of the old instance.
            state.last_pull_at.clear();
        }
        for task in self
            .inner
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            task.abort();
        }
        // The dispatch loop is not in tasks (see the field comment): it may be mid send-back RPC with a batch of messages,
        // and abort would cut the in-flight send-back off in the middle. The finalize wait below (`run_finalize_blocking`)
        // waits a bounded time for it to finish the current batch.
        let dispatch_task = lock(&self.inner.dispatch_task).take();
        // The POP executor moves into the finalize task the same way: stop accepting first, then wait a bounded time for submitted tasks.
        let pop_executor = lock(&self.inner.pop_executor).take();
        let client = lock(&self.inner.client).clone();
        // Ceiling for the consume-executor termination wait inside the finalize below: a
        // positive configured value replaces the default bounded drain; 0 (or any `<= 0`)
        // keeps the wait exactly as it has always been.
        let executor_drain_budget = {
            let millis = read_cfg(&self.inner).await_termination_millis_when_shutdown;
            if millis > 0 {
                Duration::from_millis(millis as u64)
            } else {
                SHUTDOWN_DRAIN_TIMEOUT
            }
        };
        if let Some(client) = client {
            let group = self.consumer_group();
            let broadcast = self.config().message_model == MessageModel::BROADCASTING;
            let orderly = lock(&self.inner.listener)
                .as_ref()
                .is_some_and(MessageListener::is_orderly);
            let locked_mqs: Vec<MessageQueue> = if orderly && !broadcast && !self.config().pop_mode
            {
                lock(&self.inner.state).assigned.clone()
            } else {
                Vec::new()
            };
            if broadcast {
                if let Err(e) = save_local_offsets(&self.inner) {
                    rmq_debug!("persist local offsets on shutdown failed: {e}");
                }
            }
            client.unregister_consumer(&group);
            // As on the producer: the code-35 unregister happens inside the spawned task, and the registry must not wait for that task -- restarting
            // immediately would reuse this very instance that is about to be torn down (see the comment on `MQClientInstance`).
            client.detach_from_registry_if_last_tenant();
            if let Some(handle) = self.runtime_handle() {
                let client_id = self.client_id();
                let inner = Arc::clone(&self.inner);
                let finalize = async move {
                    // (1) Bounded drain of in-flight batches: wait for the dispatch loop to consume/send-back the batch in hand (send-back RPC
                    //    running to a full response), and for the POP executor to finish the submitted consume tasks. On timeout, give up
                    //    -- the offset floor clamp in (2) below still makes the broker redeliver the unsettled messages.
                    if let Some(task) = dispatch_task {
                        if tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, task)
                            .await
                            .is_err()
                        {
                            rmq_warn!(
                                "push shutdown: dispatch loop still busy after \
                                 {SHUTDOWN_DRAIN_TIMEOUT:?}; persisting floored offsets"
                            );
                        }
                    }
                    if let Some(executor) = pop_executor {
                        executor.shutdown();
                        if tokio::time::timeout(executor_drain_budget, executor.await_termination())
                            .await
                            .is_err()
                        {
                            rmq_warn!(
                                "push shutdown: pop consume executor still busy after \
                                 {executor_drain_budget:?}"
                            );
                        }
                    }
                    // (2) The three cleanup steps must run **serially** (flush offsets -> unlock -> unregister): running them concurrently lets the
                    // unregister slip before the unlock, at which point the broker has already dropped the records by clientId and the offset and lock flushes are wasted.
                    if !broadcast {
                        let items = shutdown_persist_items(&inner);
                        for (mq, off) in &items {
                            if let Err(e) = client
                                .update_consumer_offset(&group, mq, *off, 5000, None)
                                .await
                            {
                                rmq_debug!("persist offset on shutdown failed for {mq:?}: {e}");
                            }
                        }
                        if !locked_mqs.is_empty() {
                            let _ = client
                                .unlock_batch_mq(&group, &client_id, &locked_mqs, 1000)
                                .await;
                        }
                    }
                    client
                        .unregister_client_all_brokers(
                            &client_id,
                            "",
                            &group,
                            client.mq_client_api_timeout_millis(),
                        )
                        .await;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if client.find_consumer(&group).is_none() {
                        client
                            .unregister_client_all_brokers(
                                &client_id,
                                "",
                                &group,
                                client.mq_client_api_timeout_millis(),
                            )
                            .await;
                    }
                    client.shutdown();
                };
                let _ = run_finalize_blocking(
                    &handle,
                    "push consumer shutdown",
                    SHUTDOWN_FINALIZE_BUDGET,
                    finalize,
                );
            } else {
                // No runtime: neither the last persist nor the unregister can be sent,
                // but at least hand back what is owed.
                rmq_warn!(
                    "push shutdown: no tokio runtime, skip final offset persist and \
                           consumer unregister"
                );
                client.shutdown();
            }
        }
        lock(&self.inner.client).take();
        self.inner.started.store(false, Ordering::Release);
        if let Some(dispatcher) = self.trace_dispatcher() {
            dispatcher.shutdown();
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

    /// With `enable_trace`, register the consume trace hook and start the channel.
    ///
    /// Injection takes priority ([`set_trace_dispatcher`](Self::set_trace_dispatcher), structural trade-off 5);
    /// with `enable_trace=true` and nothing injected, [`auto_bridged_dispatcher`] builds a whole set
    /// automatically (Type=CONSUME + a real internal producer).
    fn start_trace_dispatcher(&self) {
        let cfg = self.config();
        let dispatcher: Arc<dyn TraceDispatcherChannel> = match self.trace_dispatcher() {
            Some(d) => d,
            None if cfg.enable_trace => {
                match auto_bridged_dispatcher(
                    &cfg.consumer_group,
                    TraceDispatcherType::Consume,
                    self.rpc_hook(),
                    cfg.trace_msg_batch_num,
                    cfg.trace_topic.clone(),
                ) {
                    Ok(d) => {
                        // After the bridge is built, register this consumer as the host of the trace channel
                        d.set_host_consumer(Arc::new(self.clone()));
                        let channel: Arc<dyn TraceDispatcherChannel> = d;
                        self.set_trace_dispatcher(Some(channel.clone()));
                        channel
                    }
                    Err(e) => {
                        // This error text matches the production baseline verbatim
                        rmq_error!(
                            "system mqtrace hook init failed ,maybe can't send msg trace data: {e}"
                        );
                        return;
                    }
                }
            }
            None => return,
        };
        if cfg.enable_trace
            && !self
                .inner
                .consume_hooks
                .hooks()
                .iter()
                .any(|h| h.hook_name() == "ConsumeMessageTraceHook")
        {
            let sink: Arc<dyn TraceReportSink> = Arc::new(SinkAdapter(dispatcher.clone()));
            self.register_consume_message_hook(Arc::new(ConsumeMessageTraceHook::new(sink)));
        }
        if let Err(e) = dispatcher.start(&self.namesrv_addr()) {
            rmq_warn!("trace dispatcher start failed: {e}");
        }
    }

    fn namesrv_addr(&self) -> String {
        self.config().name_server_addrs.join(";")
    }

    // ---------------- Heartbeat ----------------

    /// Register the subscribed topics as "in use" and fetch the route once for each.
    async fn refresh_routes(&self) {
        let Ok(client) = require_client(&self.inner) else {
            return;
        };
        let topics: Vec<String> = lock(&self.inner.state)
            .subscription_data
            .iter()
            .map(|(k, _)| k.clone())
            .collect();
        for topic in topics {
            client.register_topic_in_use(&topic);
            if let Err(e) = client.get_topic_publish_info(&topic, false).await {
                rmq_debug!("refresh route for {topic} failed: {e}");
            }
        }
    }

    /// Return the number of brokers that succeeded.
    pub async fn send_heartbeat_to_all_broker(&self) -> usize {
        heartbeat_once(&self.inner).await
    }

    /// Cumulative successful heartbeats (for real-cluster verification).
    pub fn heartbeat_count(&self) -> usize {
        self.inner.heartbeat_count.load(Ordering::SeqCst)
    }

    /// Cumulative flow-control triggers (for real-cluster verification that flow control works).
    pub fn flow_control_triggered(&self) -> usize {
        self.inner.flow_control_triggered.load(Ordering::SeqCst)
    }

    /// How many queues are currently assigned.
    pub fn assigned_queue_count(&self) -> usize {
        lock(&self.inner.state).assigned.len()
    }

    /// Keys of the currently assigned queues (for verifying multi-instance allocation covers "no overlap, no gap").
    pub fn assigned_queue_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = lock(&self.inner.state)
            .assigned
            .iter()
            .map(mq_key)
            .collect();
        keys.sort();
        keys
    }

    /// Set of locked queues: the queues `LOCK_BATCH_MQ` actually acquired (only filled by orderly consumption).
    pub fn locked_queue_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = lock(&self.inner.state).lock_ok.iter().cloned().collect();
        keys.sort();
        keys
    }

    pub fn pop_orderly_request_count(&self) -> usize {
        lock(&self.inner.state).pop_orderly_requests.len()
    }

    // ---------------- Observability seams for pull-stall self-healing ----------------
    //
    // These seams are public together: the real-cluster verification script needs to move the clock back, trigger one
    // rebalance, and then confirm "the stalled queue is dropped and rebuilt, with no duplicate and no lost message". The criteria themselves
    // are internal state, and without seams one could only guess by sleeping 120s.

    pub fn pull_stalled(&self, key: &str) -> bool {
        let state = lock(&self.inner.state);
        pull_stalled_locked(&state, key, current_time_millis())
    }

    /// When the last pull/pop was initiated (milliseconds); `None` means this queue was never stamped.
    pub fn last_pull_at(&self, key: &str) -> Option<i64> {
        lock(&self.inner.state).last_pull_at.get(key).copied()
    }

    /// Force the stamp to a given time (for verification: equivalent to "this loop has been silent for X milliseconds").
    pub fn set_last_pull_at(&self, key: &str, millis: i64) {
        lock(&self.inner.state)
            .last_pull_at
            .insert(key.to_string(), millis);
    }

    // ---------------- Observability seams for OFFSET_ILLEGAL correction ----------------
    //
    // These seams are public together: the real-cluster verification script needs to assert "the queue was dropped (epoch +1),
    // the offset was frozen, and the stale messages in the buffer were voided as a batch"; all of these are internal state, and without
    // seams one could only guess indirectly from "whether the message got delivered".

    pub fn queue_epoch(&self, key: &str) -> u64 {
        lock(&self.inner.state)
            .queue_epoch
            .get(key)
            .copied()
            .unwrap_or(0)
    }

    pub fn offset_frozen(&self, key: &str) -> bool {
        lock(&self.inner.state).frozen_offsets.contains(key)
    }

    pub fn cached_message_count(&self, key: &str) -> usize {
        lock(&self.inner.state)
            .pending
            .get(key)
            .map_or(0, VecDeque::len)
    }

    // ---------------- Observability seams for cleanExpiredMsg ----------------
    //
    // These seams are public together: real-cluster verification needs to assert "the stuck batch is on the books (with
    // CONSUME_START_TIME), the sweep removes it from the view, and the send-back message lands in which queue's buffer"; the
    // criteria are all internal state, and without seams one could only guess indirectly from "whether a second copy arrives".

    pub fn process_queue_entries(&self, key: &str) -> Vec<MessageExt> {
        let state = lock(&self.inner.state);
        process_queue_entries_locked(&state, key)
    }

    pub fn pending_messages(&self, key: &str) -> Vec<MessageExt> {
        lock(&self.inner.state)
            .pending
            .get(key)
            .map(|dq| dq.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Run one loop-set synchronization immediately (equivalent to the rebalance step; for verification).
    ///
    /// Deliberately `async`: the finalize sends RPCs, and calling `block_on` again from a caller that is already in an async
    /// context would panic outright.
    pub async fn sync_pull_threads(&self) {
        self.rebalance_pull_threads().await;
    }

    // ---------------- Rebalance ----------------

    async fn all_queues_of_topic(&self, topic: &str) -> Vec<MessageQueue> {
        let Ok(client) = require_client(&self.inner) else {
            return Vec::new();
        };
        let queues = client.get_topic_subscribe_info(topic).await;
        if queues.is_empty() {
            rmq_debug!("rebalance: no subscribe info for topic {topic}");
        }
        queues
    }

    /// Everything a rebalance round whose per-topic assignment SET moved owes, in one order that
    /// cannot drift, and only **after** [`rebalance_pull_threads`] rebuilt the per-queue pull state
    /// (that call is the sole writer of the table the divisor is read from, so dividing earlier
    /// would divide by the *previous* round's queue count -- a shrink from 4 queues to 2 must
    /// divide by 2).
    ///
    /// 1. the moved topics' subscription version is re-stamped with one wall-clock millisecond
    ///    stamp shared by the round;
    /// 2. the topic cache budgets are divided over this round's queue count;
    /// 3. one immediate heartbeat goes out to every broker, because the broker keys the filtered
    ///    result it caches for a subscription by its version -- a moved topic must not wait for the
    ///    next periodic beat;
    /// 4. the application's queue-change listener hears about exactly the moved topics, each with
    ///    that topic's full route queue set and this instance's share.
    ///
    /// An unchanged round pays nothing, and neither does a stall self-heal round: the listener is a
    /// change notification, not a heartbeat.
    async fn after_assignment_change(&self, moved: &[String]) {
        if moved.is_empty() {
            return;
        }
        for line in restamp_subscription_versions(&self.inner, moved, current_time_millis()) {
            rmq_info!("{line}");
        }
        self.divide_topic_thresholds();
        let brokers = heartbeat_once(&self.inner).await;
        if brokers == 0 {
            rmq_debug!("no broker reachable for the rebalance-change heartbeat");
        }
        self.notify_queue_changed(moved).await;
    }

    /// Fire the application's queue-change listener for the moved topics only.
    ///
    /// The callback signature is `(topic, mqAll, mqDivided)`: the topic's FULL queue set from the
    /// route plus this instance's share of it, so the application can tell a rebalance from a route
    /// change without asking again.
    async fn notify_queue_changed(&self, moved: &[String]) {
        let listener = lock(&self.inner.queue_listener).clone();
        let Some(listener) = listener else {
            return;
        };
        let by_topic: BTreeMap<String, Vec<MessageQueue>> = {
            let state = lock(&self.inner.state);
            let mut map: BTreeMap<String, Vec<MessageQueue>> = BTreeMap::new();
            for mq in &state.assigned {
                map.entry(mq.topic.clone()).or_default().push(mq.clone());
            }
            map
        };
        for topic in moved {
            let mq_all = self.all_queues_of_topic(topic).await;
            let empty: Vec<MessageQueue> = Vec::new();
            let divided = by_topic.get(topic).unwrap_or(&empty);
            listener.message_queue_changed(topic, &mq_all, divided);
        }
    }

    /// BROADCASTING keeps every queue for itself; CLUSTERING queries the consumer list on the broker -> sorts ->
    /// applies the allocation strategy. **When the consumer list cannot be fetched, the current assignment is kept** (never
    /// falling back to "monopolize all queues", otherwise multiple instances of one group would consume the same messages).
    pub async fn do_rebalance(&self) -> Result<()> {
        let client = require_client(&self.inner)?;
        let group = self.consumer_group();
        let cfg = self.config();
        let was_queues: Vec<MessageQueue> = lock(&self.inner.state).assigned.clone();
        let was: BTreeSet<String> = was_queues.iter().map(mq_key).collect();
        let topics: Vec<String> = lock(&self.inner.state)
            .subscription_data
            .iter()
            .map(|(k, _)| k.clone())
            .collect();
        let orderly = lock(&self.inner.listener)
            .as_ref()
            .is_some_and(MessageListener::is_orderly);
        let broker_assignment = cfg.pop_mode
            && !orderly
            && cfg.message_model == MessageModel::CLUSTERING;

        let mut assigned: Vec<MessageQueue> = Vec::new();
        if cfg.message_model == MessageModel::BROADCASTING {
            for topic in &topics {
                assigned.extend(self.all_queues_of_topic(topic).await);
            }
        } else {
            for topic in &topics {
                if broker_assignment {
                    match self.assigned_from_broker(&group, topic).await {
                        Some(queues) => assigned.extend(queues),
                        None => {
                            let keep: Vec<MessageQueue> = lock(&self.inner.state)
                                .assigned
                                .iter()
                                .filter(|mq| mq.topic == *topic)
                                .cloned()
                                .collect();
                            assigned.extend(keep);
                        }
                    }
                    continue;
                }
                let mut mq_all = self.all_queues_of_topic(topic).await;
                sort_mqs(&mut mq_all);
                if mq_all.is_empty() {
                    continue;
                }
                let cid_all = client
                    .get_consumer_id_list_by_group(
                        topic,
                        &group,
                        client.mq_client_api_timeout_millis(),
                    )
                    .await;
                let Some(mut cid_all) = cid_all else {
                    rmq_debug!("rebalance: no consumer id list for {group}/{topic}, keep current");
                    let keep: Vec<MessageQueue> = lock(&self.inner.state)
                        .assigned
                        .iter()
                        .filter(|mq| mq.topic == *topic)
                        .cloned()
                        .collect();
                    assigned.extend(keep);
                    continue;
                };
                if cid_all.is_empty() {
                    rmq_debug!("rebalance: empty consumer id list for {group}/{topic}");
                    continue;
                }
                cid_all.sort();
                let strategy = self
                    .inner
                    .strategy
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                match strategy.allocate(
                    &group,
                    &cfg.client_id.clone().unwrap_or_default(),
                    &mq_all,
                    &cid_all,
                ) {
                    Ok(got) => assigned.extend(got),
                    Err(e) => {
                        // Abandon this round outright: the whole allocation result is voided and
                        // not even the already-computed topics are written (retry in the next round, 20s later). Verbatim behaviour.
                        rmq_error!(
                            "allocate message queue exception. strategy name: {}, ex: {}",
                            strategy.get_name(),
                            e
                        );
                        return Ok(());
                    }
                }
            }
        }
        {
            let mut state = lock(&self.inner.state);
            state.assigned = assigned.clone();
        }
        // The change is judged per topic on the **set of queue keys**, not on the instance total:
        // growth, shrink, a same-count hand-over to a different broker, a topic that appears and a
        // topic that disappears all count, while a re-ordering of the same set does not.
        let moved = changed_topics(&queues_by_topic(&was_queues), &queues_by_topic(&assigned));
        if !moved.is_empty() {
            rmq_info!(
                "rebalance result changed, group={group} clientId={} assigned={} topics={}",
                cfg.client_id.clone().unwrap_or_default(),
                assigned.len(),
                moved.join(",")
            );
        }
        for mq in &assigned {
            let key = mq_key(mq);
            if was.contains(&key) {
                continue;
            }
            match resolve_initial_offset(&self.inner, &client, mq).await {
                Ok(off) => {
                    let mut state = lock(&self.inner.state);
                    state.offset_table.entry(key).or_insert(off);
                }
                Err(e) => rmq_debug!("resolve initial offset failed for {mq:?}: {e}"),
            }
        }
        self.rebalance_pull_threads().await;
        // Everything the moved set owes is paid in one place, in one order that cannot drift, and
        // only **after** the pull-state update above.
        //
        // The order is load-bearing: `rebalance_pull_threads` is the sole writer of the per-queue
        // table the divisor is read from (it registers a queue when it builds that queue's pull loop
        // and drops it in `retire_queue_locked`), so running the division before it would divide by
        // the *previous* round's count -- a shrink from 4 queues to 2 must divide by 2, not by 4.
        //
        // The broker-side assignment path (`assigned_from_broker`, POP mode) needs no call site of
        // its own: it only hands the queue set back into `state.assigned` and never touches
        // `queue_owners` / `pop_queues`, so this one change-gated call already covers it. An
        // unchanged round -- including one that only self-heals a stalled pull loop -- pays nothing,
        // because `moved` is empty and the whole duty list is gated on it.
        self.after_assignment_change(&moved).await;
        Ok(())
    }

    /// Divide the two **topic-level** cache budgets (`pull_threshold_for_topic` /
    /// `pull_threshold_size_for_topic`, `-1` = off) into the corresponding per-queue knobs over the
    /// queues this consumer owns right now. This is the whole enforcement mechanism: the pull path
    /// never aggregates a topic's queues (see [`flow_control_hit`]).
    ///
    /// `owned` is the live per-queue pull-state table -- [`State::queue_owners`] (POP mode:
    /// [`State::pop_queues`]) -- which counts queues across **all** subscribed topics, not one
    /// topic's queues: a topic-wide budget is a budget for this client as a whole.
    ///
    /// Two consequences are deliberate, not bugs:
    /// - The numerator is **always the topic knob**, never the current per-queue value, so repeating
    ///   the division with an unchanged queue count writes the same number again (idempotent -- a
    ///   second round would otherwise divide the already-divided value).
    /// - While a topic knob is enabled, the user's own per-queue value is **overwritten** by the
    ///   quotient, so a config getter shows the divided number rather than what was set.
    ///
    /// `owned == 0` (nothing assigned yet) returns without touching either knob, so both keep the
    /// user's values instead of dividing by zero.
    fn divide_topic_thresholds(&self) {
        let pop = self.config().pop_mode;
        let owned = {
            let state = lock(&self.inner.state);
            if pop {
                state.pop_queues.len()
            } else {
                state.queue_owners.len()
            }
        };
        if owned == 0 {
            return;
        }
        let mut lines = Vec::new();
        self.update_config(|cfg| {
            lines = apply_topic_threshold_division(cfg, owned);
        });
        for line in lines {
            rmq_info!("{line}");
        }
    }

    /// The mode (POP / PULL) carried by an assignment item is only **observed, never obeyed**: this consumer's consume mode is fixed
    /// before start-up (`pop_mode`), and rebalance consumes the queue **set**; a mixed response only warns and is not split
    /// -- that shape almost always means `SET_MESSAGE_REQUEST_MODE` did not take effect fully.
    async fn assigned_from_broker(
        &self,
        group: &str,
        topic: &str,
    ) -> Option<Vec<MessageQueue>> {
        let client = self.client()?;
        let cfg = self.config();
        let strategy_name = self
            .inner
            .strategy
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get_name()
            .to_string();
        let assignments = match client
            .query_assignment(
                topic,
                group,
                &strategy_name,
                &cfg.message_model,
                QUERY_ASSIGNMENT_TIMEOUT_MILLIS,
            )
            .await
        {
            Ok(opt) => opt,
            Err(e) => {
                if e.response_code() == Some(response_code::TOPIC_NOT_EXIST) {
                    rmq_warn!(
                        "queryAssignment: topic {topic} not exist on broker, clear assignment \
                         for group {group}"
                    );
                    return Some(Vec::new());
                }
                rmq_error!(
                    "allocate message queue exception. strategy name: {strategy_name}, \
                     topic {topic}: {e} (keep current assignment)"
                );
                return None;
            }
        };
        let assignments = match assignments {
            Some(list) => list,
            None => {
                rmq_warn!(
                    "queryAssignment: no assignment set for topic {topic} group {group}, \
                     keep current assignment"
                );
                return None;
            }
        };
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut out: Vec<MessageQueue> = Vec::with_capacity(assignments.len());
        let mut pop_cnt = 0usize;
        let mut pull_cnt = 0usize;
        for a in assignments {
            let Some(mq) = a.message_queue else { continue };
            if mq.topic.is_empty() {
                continue;
            }
            let mq = MessageQueue::new(&mq.topic, &mq.broker_name, mq.queue_id);
            if !seen.insert(mq_key(&mq)) {
                continue;
            }
            if a.mode == MESSAGE_REQUEST_MODE_POP {
                pop_cnt += 1;
            } else {
                pull_cnt += 1;
            }
            out.push(mq);
        }
        if pop_cnt > 0 && pull_cnt > 0 {
            rmq_warn!(
                "queryAssignment for {group}/{topic} returned mixed modes \
                 (pop={pop_cnt} pull={pull_cnt}): the queue set is taken as-is, \
                 each queue is consumed per this client's own mode"
            );
        }
        Some(out)
    }

    /// Sync the loop task set to the current assignment and clean up revoked queues.
    ///
    /// When a queue is revoked it must (1) persist the consumed offset (2) drop the buffer (in-flight messages stop being consumed)
    /// (3) for orderly consumption also UNLOCK_BATCH_MQ -- missing any step lets the old instance keep consuming the in-flight
    /// messages of the revoked queue, duplicating the new owner.
    async fn rebalance_pull_threads(&self) {
        let pop = self.config().pop_mode;
        let current: BTreeMap<String, MessageQueue> = lock(&self.inner.state)
            .assigned
            .iter()
            .map(|mq| (mq_key(mq), mq.clone()))
            .collect();
        let mut revoked: Vec<(MessageQueue, Option<i64>)> = Vec::new();
        let mut to_spawn: Vec<(MessageQueue, u64)> = Vec::new();
        let group = self.consumer_group();
        let now = current_time_millis();
        {
            let mut state = lock(&self.inner.state);
            // (1) Revoke: collect first, then remove everything at once (including stall self-healing)
            let doomed: Vec<(String, Option<MessageQueue>)> = state
                .queue_owners
                .keys()
                .filter_map(|key| {
                    if !current.contains_key(key) {
                        return Some((key.clone(), None));
                    }
                    if self.inner.started.load(Ordering::Acquire)
                        && pull_stalled_locked(&state, key, now)
                    {
                        rmq_error!(
                            "[BUG]doRebalance, {group}, try remove unnecessary mq, {key}, \
                             because pull is pause, so try to fixed it"
                        );
                        return Some((key.clone(), current.get(key).cloned()));
                    }
                    None
                })
                .collect();
            for (key, fallback) in doomed {
                Self::retire_queue_locked(&mut state, &key, fallback.as_ref(), &mut revoked, pop);
            }
            // (2) Create (only register ownership and the token here; actually spawning the task waits until after (3) finishes):
            // queues without ownership in this round (newly assigned + just revoked by self-healing) take over again here
            for (key, mq) in &current {
                if state.queue_owners.contains_key(key) {
                    continue;
                }
                if pop && !state.pop_queues.contains_key(key) {
                    state.pop_queues.insert(key.clone(), PopProcessQueue::new());
                }
                state.frozen_offsets.remove(key);
                let token = self.inner.next_token.fetch_add(1, Ordering::SeqCst);
                state.queue_owners.insert(key.clone(), token);
                // Register `_mq_map[key] = mq` at the moment of assignment: even if the queue never pulled a single
                // message, it must still show up in 307/220 and its offset must still be persistable.
                state.mq_map.insert(key.clone(), mq.clone());
                // The new loop stamps its first pull itself when it initiates it; give a starting point here so
                // a freshly built loop is not misjudged by the next rebalance as "never stamped = stalled".
                state.last_pull_at.insert(key.clone(), now);
                to_spawn.push((mq.clone(), token));
            }
        }
        if !revoked.is_empty() || !to_spawn.is_empty() {
            // The assignment moved, so every broker that speaks the fingerprint protocol has to
            // be handed the subscriptions again rather than the cheap subscription-free body.
            if let Some(client) = self.client() {
                client.reset_broker_addr_heartbeat_fingerprint_table();
            }
        }
        if !revoked.is_empty() {
            self.on_queues_revoked(&revoked).await;
        }
        let weak = Arc::downgrade(&self.inner);
        if let Some(handle) = self.runtime_handle() {
            for (mq, token) in to_spawn {
                let w = weak.clone();
                let task = handle.spawn(async move {
                    if let Some(inner) = w.upgrade() {
                        if pop {
                            queue_pop_loop(inner, mq, token).await;
                        } else {
                            queue_pull_loop(inner, mq, token).await;
                        }
                    }
                });
                self.inner
                    .tasks
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(task);
            }
        }
    }

    /// Remove every trace of a queue and hand the consumed offset it must persist
    /// to the `revoked` list.
    ///
    /// `fallback_mq` is for stall self-healing: the queue is still in the assignment (so `mq_map` must not stay stale),
    /// while a genuinely revoked queue only needs the `mq_map` lookup.
    fn retire_queue_locked(
        state: &mut State,
        key: &str,
        fallback_mq: Option<&MessageQueue>,
        revoked: &mut Vec<(MessageQueue, Option<i64>)>,
        pop: bool,
    ) {
        state.queue_owners.remove(key); // the next round inside the loop sees owns_queue as invalid
        state.pending.remove(key);
        state.inflight_msgs.remove(key);
        state.lock_ok.remove(key);
        state.lock_at.remove(key);
        state.last_consume_at.remove(key);
        state.last_pull_at.remove(key); // a reused queue name must not inherit the old timestamp
        *state.queue_epoch.entry(key.to_string()).or_insert(0) += 1;
        let off = state.consume_offsets.remove(key);
        state.offset_table.remove(key);
        let mq = state.mq_map.remove(key).or_else(|| fallback_mq.cloned());
        if pop {
            if let Some(pq) = state.pop_queues.remove(key) {
                pq.set_dropped(true);
            }
        }
        if let Some(mq) = mq {
            revoked.push((mq, off));
        }
    }

    async fn on_queues_revoked(&self, revoked: &[(MessageQueue, Option<i64>)]) {
        revoke_queues(&self.inner, revoked).await;
    }

    /// The broker signals that the consumer group's instances changed (response code 40) -> request an immediate rebalance.
    fn request_rebalance(&self) {
        self.inner.rebalance_now.store(true, Ordering::SeqCst);
        self.inner.rebalance_signal.notify_waiters();
    }
}

// ---------------- Offset resolution ----------------

/// Free function: the pull loop only holds `Arc<Inner>` (structural trade-off 4) and cannot reach the consumer handle.
/// The decision half of the initial-offset lookup, kept apart from the RPCs that
/// execute it (the stored offset and the queue tail are both broker reads).
///
/// Two invariants hold it together:
/// 1. the start NEVER depends on the subscription type. An earlier draft
///    short-circuited SQL92 subscriptions to the queue tail, which silently skipped
///    every message produced *before* the consumer started (send-first +
///    CONSUME_FROM_FIRST_OFFSET received nothing; consumer-first only passed because
///    the queues were empty and the tail was 0 anyway). The broker applies the SQL92
///    filter to whatever range the pull covers, so the policy alone decides.
/// 2. broadcasting and clustering differ ONLY in where the stored offset is read from
///    (local file vs broker). Both fall through to this same decision when nothing is
///    stored, so a CONSUME_FROM_TIMESTAMP or %RETRY% start can never become
///    mode-dependent.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StartOffsetPlan {
    Stored(i64),
    FromZero,
    QueueTail,
    SearchAtConfiguredTime,
}

pub(crate) fn plan_initial_offset(stored: i64, from_where: &str, is_retry_topic: bool) -> StartOffsetPlan {
    if stored >= 0 {
        return StartOffsetPlan::Stored(stored);
    }
    if from_where == ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET {
        // Nothing stored: the start is the literal 0 and no minOffset query is sent --
        // minOffset is the admin-side policy (master only), so while the master is down a
        // freshly started consumer would pull nothing at all. Starting at 0 works through
        // a slave and the broker corrects an out-of-range start with PULL_OFFSET_MOVED.
        return StartOffsetPlan::FromZero;
    }
    if from_where == ConsumeFromWhere::CONSUME_FROM_TIMESTAMP {
        // A %RETRY% queue has no meaningful "time of the oldest retry" and every queued
        // retry must be delivered, so it starts at the tail rather than searching
        // backwards from a timestamp.
        return if is_retry_topic {
            StartOffsetPlan::QueueTail
        } else {
            StartOffsetPlan::SearchAtConfiguredTime
        };
    }
    // Default CONSUME_FROM_LAST_OFFSET: a %RETRY% queue starts at 0 (retry messages must
    // be fully retried) instead of skipping from the tail.
    if is_retry_topic {
        return StartOffsetPlan::FromZero;
    }
    StartOffsetPlan::QueueTail
}

async fn resolve_initial_offset(
    inner: &Inner,
    client: &MQClientInstance,
    mq: &MessageQueue,
) -> Result<i64> {
    let cfg = read_cfg(inner);
    let key = mq_key(mq);
    let mut stored = -1i64;
    if cfg.message_model == MessageModel::BROADCASTING {
        if let Some(off) = load_local_offsets(inner).get(&key) {
            stored = *off;
        }
    } else {
        match client
            .query_consumer_offset(&cfg.consumer_group, mq, 5000, None, false)
            .await
        {
            Ok(Some(off)) => stored = off,
            Ok(None) => {}
            // Anything other than "not found" is a store read failure: the caller must skip this
            // queue for the current round instead of starting it from the tail.
            Err(e) => return Err(e),
        }
    }

    match plan_initial_offset(stored, &cfg.consume_from_where, NamespaceUtil::is_retry_topic(&mq.topic)) {
        StartOffsetPlan::Stored(off) => Ok(off),
        StartOffsetPlan::FromZero => Ok(0),
        StartOffsetPlan::QueueTail => client.get_max_offset(mq, 5000, None).await,
        StartOffsetPlan::SearchAtConfiguredTime => {
            let ts = consume_timestamp_millis(&cfg.consume_timestamp)?;
            client.search_offset_by_timestamp(mq, ts, 5000, None).await
        }
    }
}

// ---------------- Queue revocation and offset correction ----------------

/// Free function (same reason as `handle_offset_illegal` below): the pull loop only holds `Arc<Inner>`.
async fn revoke_queues(inner: &Arc<Inner>, revoked: &[(MessageQueue, Option<i64>)]) {
    if revoked.is_empty() {
        return;
    }
    let cfg = read_cfg(inner);
    if cfg.message_model == MessageModel::BROADCASTING {
        if let Err(e) = save_local_offsets_with(inner, revoked) {
            rmq_debug!("save local offsets failed: {e}");
        }
        return;
    }
    let Ok(client) = require_client(inner) else {
        return;
    };
    let group = cfg.consumer_group.clone();
    let orderly = lock(&inner.listener)
        .as_ref()
        .is_some_and(MessageListener::is_orderly);
    let client_id = cfg.client_id.clone().unwrap_or_default();
    for (mq, off) in revoked {
        if let Some(off) = off {
            if let Err(e) = client
                .update_consumer_offset(&group, mq, *off, 5000, None)
                .await
            {
                rmq_debug!("persist offset on revoke failed for {mq:?}: {e}");
            }
        }
        // The revoked queue must not linger in the in-memory store after its final persist.
        lock(&inner.state).consume_offsets.remove(&mq_key(mq));
        if orderly && !cfg.pop_mode {
            let _ = client
                .unlock_batch_mq(&group, &client_id, std::slice::from_ref(mq), 1000)
                .await;
        }
    }
    rmq_info!("queues revoked, group={group} count={}", revoked.len());
}

/// The broker says "the offset you requested is illegal" (the queue was truncated, the commitlog expired, or the server reset it; see
/// the broker answers `PULL_OFFSET_MOVED` for OFFSET_OVERFLOW_BADLY / NO_MESSAGE_IN_QUEUE / OFFSET_RESET alike)
/// and puts the corrected value in `nextBeginOffset`. Correcting is not just moving the cursor:
/// **every message already fetched on this queue but not yet consumed/acked is voided** (they fall inside the skipped
/// range, and acking them would push the offset back to an illegal value), then the queue is rebuilt at the corrected offset.
///
///
/// Must be called without the lock: it performs network RPCs. The call sites sit after the locked block of the pull callback.
async fn handle_offset_illegal(inner: &Arc<Inner>, key: &str) {
    let mut revoked: Vec<(MessageQueue, Option<i64>)> = Vec::new();
    {
        let mut state = lock(&inner.state);
        DefaultMQPushConsumer::retire_queue_locked(
            &mut state,
            key,
            None,
            &mut revoked,
            read_cfg(inner).pop_mode,
        );
    }
    revoke_queues(inner, &revoked).await;
    inner.rebalance_now.store(true, Ordering::SeqCst);
    inner.rebalance_signal.notify_waiters();
    rmq_warn!("the pull request offset illegal, fix it, queue={key}");
}

// ================================================================ Background loops
//
// Written as free functions instead of `&self` methods: tasks hold only `Weak<Inner>` (structural trade-off 4),
// otherwise `instance -> transport -> task -> consumer` forms a cycle and the consumer is never released.

/// Whether the `stop` channel is already set (including the sender having been released).
fn stopped(rx: &watch::Receiver<bool>) -> bool {
    *rx.borrow() || rx.has_changed().is_err()
}

/// Wait for a timeout window: returns false when it elapses, returns true when a stop signal arrives.
async fn wait_or_stop(rx: &mut watch::Receiver<bool>, millis: u64) -> bool {
    match tokio::time::timeout(Duration::from_millis(millis), rx.changed()).await {
        Ok(Ok(())) => stopped(rx),
        // The sender was released (Inner is being dropped) = exit immediately
        Ok(Err(_)) => true,
        Err(_) => false,
    }
}

/// Whether this loop still owns the queue.
fn owns_queue(inner: &Inner, key: &str, token: u64) -> bool {
    lock(&inner.state)
        .queue_owners
        .get(key)
        .is_some_and(|t| *t == token)
}

/// The two stamps at loop entry: `last_pull_table[key]` +
/// `pq.last_pop_timestamp = now`, written **before** this round's pull/pop is initiated.
///
/// In POP mode both timestamps are written together: `_last_pull_table` lets rebalance judge stalls,
/// [`PopProcessQueue::last_pop_timestamp`] feeds the operational view of `ConsumerRunningInfo`.
fn stamp_pull_at(inner: &Inner, key: &str, pop: bool) {
    let now = current_time_millis();
    let pq = if pop {
        lock(&inner.state).pop_queues.get(key).cloned()
    } else {
        None
    };
    lock(&inner.state).last_pull_at.insert(key.to_string(), now);
    if let Some(pq) = pq {
        pq.touch(now);
    }
}

/// Pull-stall criterion: a queue that still belongs to this instance but whose pull loop has stalled.
fn pull_stalled_locked(state: &State, key: &str, now: i64) -> bool {
    match state.last_pull_at.get(key) {
        None => false,
        Some(&0) => true,
        Some(&began) => now - began > PULL_MAX_IDLE_TIME,
    }
}

/// Pull loop exit marker: the loop leaves a sentinel when **it** exits,
/// so the next rebalance round treats it as stalled and rebuilds it. It is written only while the queue is still in the ownership table;
/// otherwise the queue was revoked and a new owner took over, and writing `0` would wrongly harm that owner.
fn mark_pull_loop_exited(inner: &Inner, key: &str, token: u64) {
    let mut state = lock(&inner.state);
    if state.queue_owners.get(key).is_some_and(|t| *t == token) {
        state.last_pull_at.insert(key.to_string(), 0);
    }
}

fn pull_subscription_expression(cfg: &ConsumerConfig, sub: &SubscriptionData) -> Option<String> {
    if cfg.post_subscription_when_pull && !sub.class_filter_mode {
        Some(sub.sub_string.clone())
    } else {
        None
    }
}

fn recalc_pull_from_which_node(state: &State, mq: &MessageQueue) -> i64 {
    state
        .pull_from_which_node
        .get(mq)
        .copied()
        .unwrap_or(MixAll::MASTER_ID as i64)
}

fn update_pull_from_which_node(state: &mut State, mq: &MessageQueue, suggest: Option<i64>) {
    state
        .pull_from_which_node
        .insert(mq.clone(), suggest.unwrap_or(MixAll::MASTER_ID as i64));
}

/// The wait before the next pull on the same queue, decided by what the round handed over:
///
/// * a round that **delivered** rows -- status FOUND with the post-filter list still non-empty,
///   the same list the throughput count is fed from -- waits the configured pull cadence, and a
///   cadence of `0` means straight back to the top;
/// * a round that delivered nothing re-pulls immediately: FOUND emptied by the filter, NO_NEW_MSG
///   and NO_MATCHED_MSG included. The cadence paces **deliveries**; parking on top of a round that
///   fetched nothing would only delay the next fetch.
///
/// An OFFSET_ILLEGAL round is not a delivery round (its status is not FOUND), so it reaches no
/// cadence wait here -- that path runs its own freeze/persist/revoke handling.
fn pull_next_round_delay_millis(found: bool, delivered: usize, pull_interval: i64) -> u64 {
    if found && delivered > 0 {
        pull_interval.max(0) as u64
    } else {
        0
    }
}

/// Whether the offsets a FOUND round came back with contradict the cursor it requested with:
///
/// * the cursor the round advanced to sits **below** the offset it asked from, or
/// * the first row it handed over sits **below** that same offset.
///
/// Either shape means the round's own answer is inconsistent with the request it was made under,
/// which is worth a line in the log and nothing else: the check reads the round's numbers, it
/// decides no control flow, no offset, no wait.
///
/// `first_msg_offset` is the queue offset of the first row the round delivered **after** this
/// client's own pre-delivery filter ran; an empty list is passed as `i64::MAX` so it can never
/// look like a row from before the cursor.
fn pull_offsets_look_wrong(
    next_begin_offset: i64,
    first_msg_offset: i64,
    prev_request_offset: i64,
) -> bool {
    next_begin_offset < prev_request_offset || first_msg_offset < prev_request_offset
}

/// The one log line a round flagged by [`pull_offsets_look_wrong`] ends with: the queue plus all
/// three offsets it compares, so a reader can tell which leg fired without guessing.
fn pull_offset_warning(
    key: &str,
    next_begin_offset: i64,
    first_msg_offset: i64,
    prev_request_offset: i64,
) -> String {
    format!(
        "pull offset sanity warning: queue={key} next_begin_offset={next_begin_offset} \
         first_msg_offset={first_msg_offset} requested_offset={prev_request_offset}"
    )
}

/// One queue's long-polling pull -> push into the to-consume buffer.
///
/// One loop per queue (not a shared thread pool), because each queue's long poll is suspended on
/// its own; sharing one loop would let an empty queue's suspend window block delivery for all the other queues.
///
/// This shell only handles the wrap-up: wherever the task body `return`s, it leaves a stall sentinel as long as it still
/// owns the queue, and the next rebalance round rebuilds it (tokio tasks have no equivalent of `thread.is_alive()`).
async fn queue_pull_loop(inner: Arc<Inner>, mq: MessageQueue, token: u64) {
    let key = mq_key(&mq);
    run_queue_pull_loop(inner.clone(), mq, token).await;
    mark_pull_loop_exited(&inner, &key, token);
}

async fn run_queue_pull_loop(inner: Arc<Inner>, mq: MessageQueue, token: u64) {
    let Ok(client) = require_client(&inner) else {
        return;
    };
    let key = mq_key(&mq);
    // The generic failed-round backoff is taken ONCE here, when the loop is set up: a knob moved
    // later must not change the wait this running loop arms with.
    let failed_round_backoff = read_cfg(&inner).pull_exception_backoff_millis.max(0) as u64;
    let mut rx = inner.stop.subscribe();
    while !stopped(&rx) && inner.started.load(Ordering::Acquire) {
        if !owns_queue(&inner, &key, token) {
            return;
        }
        stamp_pull_at(&inner, &key, false);
        if inner.pause.load(Ordering::SeqCst) {
            if wait_or_stop(&mut rx, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND).await {
                return;
            }
            continue;
        }
        let sub = lock(&inner.state).subscription(&mq.topic).cloned();
        let Some(sub) = sub else { return };
        let cfg = read_cfg(&inner);
        let orderly = lock(&inner.listener)
            .as_ref()
            .is_some_and(MessageListener::is_orderly);
        // Orderly consumption: a clustering queue must hold a broker grant from LOCK_BATCH_MQ before it is
        // pulled, and a grant older than the maximum lock lifetime counts as expired. Broadcasting has no
        // broker-side lock, so there every queue is implicitly locked.
        if orderly && cfg.message_model == MessageModel::CLUSTERING {
            let now = current_time_millis();
            let (known, fresh) = {
                let state = lock(&inner.state);
                (
                    state.lock_ok.contains(&key),
                    state
                        .lock_at
                        .get(&key)
                        .is_some_and(|at| now - *at <= REBALANCE_LOCK_MAX_LIVE_TIME),
                )
            };
            if !(known && fresh) {
                if known {
                    rmq_warn!(
                        "lock of {mq:?} is older than {REBALANCE_LOCK_MAX_LIVE_TIME} ms, \
                         waiting for the next lock round"
                    );
                }
                if wait_or_stop(&mut rx, 200).await {
                    return;
                }
                continue;
            }
        }
        if flow_control_hit(&inner, &key) {
            if wait_or_stop(&mut rx, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL).await {
                return;
            }
            continue;
        }
        let cached = lock(&inner.state).offset_table.get(&key).copied();
        let offset = match cached {
            Some(off) => off,
            None => match resolve_initial_offset(&inner, &client, &mq).await {
                Ok(off) => {
                    lock(&inner.state).offset_table.insert(key.clone(), off);
                    off
                }
                Err(e) => {
                    rmq_debug!("resolve initial offset failed for {mq:?}: {e}");
                    if wait_or_stop(&mut rx, failed_round_backoff).await {
                        return;
                    }
                    continue;
                }
            },
        };
        // The cursor this round requests with, kept under its own name because the round-end offset
        // check compares the answer against it and the cursor itself moves on before then.
        let prev_request_offset = offset;
        let sub_expression = pull_subscription_expression(&cfg, &sub);
        let sys_flag =
            PullSysFlag::build_sys_flag_basic(false, true, sub_expression.is_some(), false);
        let broker_id = {
            let state = lock(&inner.state);
            recalc_pull_from_which_node(&state, &mq)
        };
        let began = current_time_millis();
        let result = match client
            .pull_message(
                &cfg.consumer_group,
                &mq,
                offset,
                cfg.pull_batch_size,
                sys_flag,
                0,
                sub_expression.as_deref().unwrap_or(""),
                sub.sub_version,
                &sub.expression_type,
                cfg.pull_timeout_millis,
                cfg.pull_batch_size_in_bytes,
                cfg.pull_suspend_timeout_millis,
                None,
                0,
                Some(broker_id),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                // A client-side timeout while long polling in suspend with no new message is **normal behaviour**: an
                // idle queue answers on its own schedule, so our own request timeout can expire before the reply does.
                let benign = matches!(e, Error::Timeout { .. } | Error::TooMuchRequest(_));
                if benign {
                    rmq_debug!("pull long-poll timeout for {mq:?} (benign, will retry): {e}");
                    continue;
                }
                let delay = failed_request_backoff_millis(&e, failed_round_backoff);
                // Most of the rest are expected paths such as the topic not existing yet: debug plus a short backoff keeps the loop from spinning hot
                rmq_debug!("pull error for {mq:?}: {e}");
                if wait_or_stop(&mut rx, delay).await {
                    return;
                }
                continue;
            }
        };
        {
            let mut state = lock(&inner.state);
            update_pull_from_which_node(&mut state, &mq, result.suggest_which_broker_id);
        }
        // Consume statistics: only a FOUND round records the pull round trip, and it records it even
        // when no row came back. Idle rounds contribute nothing, so PULL_RT stays the latency of the
        // rounds that carried work instead of an average diluted by every suspend-length poll that
        // returned nothing. The round trip is read here, before the pre-delivery filter runs, so a
        // slow user hook never lands inside the pull's own latency; the throughput count is fed from
        // the list that survives that filter.
        let pull_found = result.status == PullStatus::Found;
        let pull_rt = current_time_millis() - began;
        let mut msgs = result.msg_found_list;
        if pull_found && !msgs.is_empty() {
            msgs = filter_messages_for_delivery(
                &cfg.consumer_group,
                &inner.filter_hooks,
                &mq,
                Some(&sub),
                msgs,
                cfg.unit_mode,
            );
        }
        if let Some(stats) = lock(&inner.stats).clone() {
            record_pull_round_stats(
                &stats,
                &cfg.consumer_group,
                &mq.topic,
                pull_found,
                pull_rt,
                msgs.len(),
            );
        }
        let mut illegal = false;
        {
            let mut state = lock(&inner.state);
            if state.queue_owners.get(&key).map_or(true, |t| *t != token) {
                rmq_debug!(
                    "queue {key} revoked during pull, discard {} fetched messages",
                    msgs.len()
                );
                return;
            }
            if !msgs.is_empty() {
                state.mq_map.insert(key.clone(), mq.clone());
                state
                    .pending
                    .entry(key.clone())
                    .or_default()
                    .extend(msgs.iter().cloned());
            }
            state
                .offset_table
                .insert(key.clone(), result.next_begin_offset);
            if result.status == PullStatus::OffsetIllegal {
                if result.next_begin_offset >= 0 {
                    state
                        .consume_offsets
                        .insert(key.clone(), result.next_begin_offset);
                }
                state.frozen_offsets.insert(key.clone());
                illegal = true;
            } else {
                correct_tags_offset_locked(
                    &mut state,
                    &key,
                    result.status,
                    result.next_begin_offset,
                );
            }
        }
        // update_msg_acc_cnt takes the same lock itself, so it must be called after the block above releases it.
        update_msg_acc_cnt(&inner, &key, &msgs);
        if illegal {
            handle_offset_illegal(&inner, &key).await;
        }
        // Offset sanity, at the very end of a FOUND round: the cursor has moved on and the batch has
        // reached the consume side, so all this does is say out loud when the round's own answer
        // contradicts the cursor it was asked from. Nothing below reads it.
        if pull_found {
            let first_msg_offset = msgs.first().map_or(i64::MAX, MessageExt::get_queue_offset);
            if pull_offsets_look_wrong(
                result.next_begin_offset,
                first_msg_offset,
                prev_request_offset,
            ) {
                rmq_warn!(
                    "{}",
                    pull_offset_warning(
                        &key,
                        result.next_begin_offset,
                        first_msg_offset,
                        prev_request_offset,
                    )
                );
            }
        }
        // Round cadence: only a round that handed over rows waits the configured pull interval, so
        // the cadence paces deliveries and never an idle or illegal round. An OFFSET_ILLEGAL round
        // is not a delivery round, so it takes no wait here and its revoke path still exits at the
        // next top-of-loop ownership check.
        let delay = pull_next_round_delay_millis(pull_found, msgs.len(), cfg.pull_interval);
        if delay > 0 && wait_or_stop(&mut rx, delay).await {
            return;
        }
    }
}

fn update_msg_acc_cnt(inner: &Inner, key: &str, msgs: &[MessageExt]) {
    let Some(last) = msgs.last() else {
        return;
    };
    let Some(prop) = last.get_property(PROPERTY_MAX_OFFSET) else {
        return;
    };
    let Ok(max_offset) = prop.parse::<i64>() else {
        return;
    };
    let acc_total = max_offset - last.queue_offset;
    if acc_total > 0 {
        lock(&inner.state)
            .msg_acc_cnt
            .insert(key.to_string(), acc_total);
    }
}

/// Whether the cache gates pause this queue's pull.
///
/// The gate judges **one queue's own held set** against exactly three per-queue gates, checked in
/// one fixed order: message **count**, then **size**, then offset **span**. A topic-level budget is
/// **not** aggregated here at all: it is divided into the per-queue knob at rebalance time
/// ([`DefaultMQPushConsumer::divide_topic_thresholds`]), so the pull path never has to look at --
/// or wait on a lock held for -- its sibling queues.
///
/// Every gate compares **strictly greater than** its threshold, with the threshold taken exactly as
/// it stands: no floor of 1 and no "0 switches this gate off" escape. A queue that has reached a
/// threshold exactly is therefore still allowed to pull, so the knobs read as "how many may stay
/// cached", not "how many stop the pull". Keeping each knob inside its legal range is
/// [`check_config_ranges`]'s job at start-up, not this gate's.
///
/// What all three measure is everything this queue still **holds**: the pulled-but-undispatched
/// buffer union the batches already handed to the listener and not yet settled
/// ([`process_queue_entries_locked`], deduplicated by `queue_offset`). A dispatched batch has only
/// moved, it has not left the queue -- it leaves when it settles through an ack/complete or is
/// requeued. Because the dispatch window is ordered, the batch in the listener's hands carries the
/// **lower** queue offsets of the two parts, so the span reads the same held set too. A gate that
/// judged the buffer alone would under-count and keep pulling while the listener still sits on
/// messages, which is exactly the situation these thresholds exist to stop.
///
/// The size term is the integer (truncating) whole MiB of the held messages' **body** bytes. The
/// stored-record size a broker charges for the whole storage record is a telemetry number, not
/// something this client holds in memory, and it never enters this comparison.
///
/// The one per-term difference is that the **span gate does not run at all for an orderly
/// listener**: the knob is named after concurrent consumption, and an orderly queue has no
/// run-ahead to guard against because its pull cursor only moves once the batch in the listener's
/// hands has settled -- so the orderly path leaves its buffer wide rather than stalling
/// head-of-line. Count and size still bind it, over the same held set.
fn flow_control_hit(inner: &Inner, key: &str) -> bool {
    let cfg = read_cfg(inner);
    let orderly = lock(&inner.listener)
        .as_ref()
        .is_some_and(MessageListener::is_orderly);
    let (count, size_mib, span) = {
        let state = lock(&inner.state);
        let held = process_queue_entries_locked(&state, key);
        let count = held.len();
        let body_bytes = held
            .iter()
            .map(|m| i64::try_from(m.get_body().len()).unwrap_or(i64::MAX))
            .sum::<i64>();
        let size_mib = body_bytes / CACHE_SIZE_GATE_MIB;
        let span = if held.is_empty() {
            0
        } else {
            let mut min = i64::MAX;
            let mut max = i64::MIN;
            for m in &held {
                min = min.min(m.queue_offset);
                max = max.max(m.queue_offset);
            }
            max - min
        };
        (count, size_mib, span)
    };
    // Widened to `i64` before comparing: the counts come from `usize` and the knobs are signed,
    // and a comparison that silently rebased a negative knob onto the unsigned axis would turn a
    // hand-built config into "this gate never trips".
    let count = i64::try_from(count).unwrap_or(i64::MAX);
    let reason: Option<String> = if count > i64::from(cfg.pull_threshold_for_queue) {
        Some(format!("count={count}"))
    } else if size_mib > i64::from(cfg.pull_threshold_size_for_queue) {
        Some(format!("size={size_mib}MB"))
    } else if !orderly && span > cfg.consume_concurrently_max_span {
        Some(format!("span={span}"))
    } else {
        None
    };
    match reason {
        None => false,
        Some(reason) => {
            let times = inner.flow_control_triggered.fetch_add(1, Ordering::SeqCst) + 1;
            rmq_debug!("flow control: queue {key} {reason}, pause pull");
            if is_flow_control_warn_trip(times) {
                rmq_warn!(
                    "flow control: queue {key} {reason} exceeds its cache threshold, pause pull for \
                     {PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL} ms (flowControlTimes={times})"
                );
            }
            true
        }
    }
}

/// The POP path's cache gate: **this queue's own** popped-but-unacked debt, compared strictly
/// greater than `pop_threshold_for_queue` (default 96).
///
/// POP keeps no "pulled but not consumed" buffer, so the count / bytes / span checks of
/// [`flow_control_hit`] have nothing to read here -- once a message is popped, consumption has
/// already begun and what matters is the ack debt. The debt is per queue and never summed, and the
/// threshold is a plain client knob: the topic budgets divided at rebalance belong to the pull path
/// and do not move it.
///
/// A hit is counted on the **same** `flow_control_triggered` total the pull path uses, so one
/// client-wide flow-control count spans both paths and the throttled warn cadence (see
/// [`is_flow_control_warn_trip`]) also sees the POP trips.
fn pop_flow_control_hit(inner: &Inner, key: &str, pq: &PopProcessQueue) -> bool {
    let wait_ack = pq.wait_ack_count();
    let threshold = read_cfg(inner).pop_threshold_for_queue;
    if wait_ack <= threshold {
        return false;
    }
    let times = inner.flow_control_triggered.fetch_add(1, Ordering::SeqCst) + 1;
    rmq_debug!(
        "flow control: queue {key} wait_ack={wait_ack} over pop_threshold_for_queue={threshold}, pause pop"
    );
    if is_flow_control_warn_trip(times) {
        rmq_warn!(
            "flow control: queue {key} wait_ack={wait_ack} exceeds its pop_threshold_for_queue={threshold}, \
             pause pop for {PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL} ms (flowControlTimes={times})"
        );
    }
    true
}

/// Group a queue list as topic -> the set of queue **keys** owned there.
///
/// The rebalance change detection compares these per-topic sets and never a count: losing one
/// queue and gaining another keeps the total while changing which buffers exist, so the budgets
/// have to be re-divided over the new set and the broker has to learn about the queues this client
/// just picked up.
pub(crate) fn queues_by_topic(queues: &[MessageQueue]) -> BTreeMap<String, BTreeSet<String>> {
    let mut grouped: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for mq in queues {
        grouped
            .entry(mq.topic.clone())
            .or_default()
            .insert(mq_key(mq));
    }
    grouped
}

/// The sorted topics whose assigned-queue SET differs between two rebalance rounds.
///
/// A topic that appeared or vanished entirely counts too: its queues moved. A re-ordering of the
/// same set is **not** a move -- the identity of the owned queues is what the duties below key on,
/// and the sort order is decided by the strategy, not by ownership.
pub(crate) fn changed_topics(
    before: &BTreeMap<String, BTreeSet<String>>,
    after: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<String> {
    before
        .keys()
        .chain(after.keys())
        .cloned()
        .collect::<BTreeSet<String>>()
        .into_iter()
        .filter(|topic| {
            before.get(topic).unwrap_or(&EMPTY_KEY_SET) != after.get(topic).unwrap_or(&EMPTY_KEY_SET)
        })
        .collect()
}

static EMPTY_KEY_SET: BTreeSet<String> = BTreeSet::new();

/// Re-share the subscription version of the topics whose assignment moved and return the INFO
/// lines to emit (`"<topic> rebalance changed, also update version: <old>, <new>"`).
///
/// The version is what makes a broker re-read a subscription instead of reusing the filtered result
/// it cached for the old generation, so a topic whose queues moved must look new to the broker even
/// when the filter expression did not change. Topics that did not move keep their version:
/// re-stamping the whole table would put subscriptions the broker already holds back on the wire on
/// every changed round, and would also make a moved topic impossible to tell apart from an unmoved
/// one in the beat that follows.
///
/// One wall-clock millisecond stamp is shared by the round, so two moved topics never carry two
/// versions from the same rebalance. The subscription table holds the very values the heartbeat
/// builder serialises, so writing here is what makes the next beat carry the fresh version.
fn restamp_subscription_versions(
    inner: &Inner,
    moved: &[String],
    new_version: i64,
) -> Vec<String> {
    let mut lines = Vec::new();
    if moved.is_empty() {
        return lines;
    }
    let mut state = lock(&inner.state);
    for topic in moved {
        let Some((_, sub)) = state
            .subscription_data
            .iter_mut()
            .find(|(k, _)| k == topic)
        else {
            continue;
        };
        lines.push(format!(
            "{topic} rebalance changed, also update version: {}, {new_version}",
            sub.sub_version
        ));
        sub.sub_version = new_version;
    }
    lines
}

/// Divide one enabled topic budget over `owned` queues and state the result with the literal
/// log wording the gate documents.
///
/// Returns the new per-queue value plus `Some(INFO line)` **only when the value actually moved** --
/// an unchanged quotient logs nothing, and a `-1` (off) budget never reaches this function at all.
/// The `<old>` in the line is the per-queue value **before** the write (what the user or a previous
/// round sees), not the topic knob.
///
/// The numerator is `topic_threshold`, **never** `current_queue_threshold`: that is what makes the
/// division idempotent across rounds with the same queue count. The quotient is floored at 1 so a
/// budget smaller than the queue count still lets every queue hold one message instead of freezing
/// at 0 (a 0 gate would pause every pull).
///
/// Width: the arithmetic runs in `i64` (`owned` is a `usize` that may exceed `i32::MAX` in
/// principle, and `i32` division would truncate first), and the result is narrowed back to the
/// `i32` config field. Both topic knobs are validated to at most 6553500 messages / 102400 MiB and
/// a quotient is never larger than its numerator, so the narrowing cannot wrap; `i32::MAX` clamps it
/// anyway so a hand-built config outside the validated domain cannot produce a negative knob.
fn divided_queue_threshold(
    log_name: &str,
    topic_threshold: i32,
    current_queue_threshold: i32,
    owned: usize,
) -> (i32, Option<String>) {
    let quotient = i64::from(topic_threshold) / i64::max(owned as i64, 1);
    let quotient = quotient.clamp(1, i64::from(i32::MAX)) as i32;
    if quotient == current_queue_threshold {
        return (quotient, None);
    }
    (
        quotient,
        Some(format!(
            "The {log_name} is changed from {current_queue_threshold} to {quotient}"
        )),
    )
}

/// The pure half of [`DefaultMQPushConsumer::divide_topic_thresholds`]: divide every **enabled**
/// topic-level budget (`!= -1`) of `cfg` into its per-queue knob over `owned` queues, and return the
/// INFO lines to emit (one per knob that moved, in count-then-size order).
///
/// `owned` must be >= 1; the caller's `owned == 0` early return is what keeps both knobs at the
/// user's values when nothing is assigned, so this function never sees a zero divisor.
fn apply_topic_threshold_division(cfg: &mut ConsumerConfig, owned: usize) -> Vec<String> {
    let mut lines = Vec::new();
    if cfg.pull_threshold_for_topic != -1 {
        let (new, line) = divided_queue_threshold(
            "pullThresholdForQueue",
            cfg.pull_threshold_for_topic,
            cfg.pull_threshold_for_queue,
            owned,
        );
        cfg.pull_threshold_for_queue = new;
        lines.extend(line);
    }
    if cfg.pull_threshold_size_for_topic != -1 {
        let (new, line) = divided_queue_threshold(
            "pullThresholdSizeForQueue",
            cfg.pull_threshold_size_for_topic,
            cfg.pull_threshold_size_for_queue,
            owned,
        );
        cfg.pull_threshold_size_for_queue = new;
        lines.extend(line);
    }
    lines
}

/// Within 60s of start-up and with no assignment at all, this shortens to 2s: the consumer may start before the topic is created
/// (under `autoCreateTopicEnable` the topic is created by the producer's first send), and waiting out 20s would mean not consuming for a long time.
async fn rebalance_loop(consumer: DefaultMQPushConsumer, rx: watch::Receiver<bool>) {
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut next_run = current_time_millis();
    loop {
        if stopped(&rx) || !consumer.is_started() {
            return;
        }
        let starting_up = current_time_millis()
            - consumer.inner.start_time_millis.load(Ordering::SeqCst)
            < 60_000;
        let interval = if starting_up && consumer.assigned_queue_count() == 0 {
            2_000
        } else {
            20_000
        };
        let requested = consumer.inner.rebalance_now.swap(false, Ordering::SeqCst);
        if !requested && current_time_millis() < next_run {
            let _ = ticker.tick().await;
            continue;
        }
        next_run = current_time_millis() + interval;
        if let Err(e) = consumer.do_rebalance().await {
            rmq_debug!("rebalance error: {e}");
        }
    }
}

/// Send one heartbeat round (shared by the periodic heartbeat of `heartbeat_loop`, the explicit
/// `send_heartbeat_to_all_broker` call, and the immediate heartbeat after `subscribe`), returning the number of brokers that succeeded.
async fn heartbeat_once(inner: &Arc<Inner>) -> usize {
    let Ok(client) = require_client(inner) else {
        return 0;
    };
    let ok = client
        .send_heartbeat_to_all_broker(client.mq_client_api_timeout_millis())
        .await;
    if ok > 0 {
        inner.heartbeat_count.fetch_add(1, Ordering::SeqCst);
    }
    ok
}

/// Heartbeat loop: the consumer **must** be registered on the broker, otherwise rebalance's
/// GET_CONSUMER_LIST_BY_GROUP cannot get the list.
async fn heartbeat_loop(inner: Arc<Inner>, mut rx: watch::Receiver<bool>) {
    loop {
        let interval = read_cfg(&inner).heartbeat_interval_millis.max(1000) as u64;
        if wait_or_stop(&mut rx, interval).await {
            return;
        }
        let cfg = read_cfg(&inner);
        if !cfg.heartbeat_enabled || !inner.started.load(Ordering::Acquire) {
            continue;
        }
        heartbeat_once(&inner).await;
    }
}

async fn lock_loop(inner: Arc<Inner>, mut rx: watch::Receiver<bool>) {
    loop {
        let cfg = read_cfg(&inner);
        let orderly = lock(&inner.listener)
            .as_ref()
            .is_some_and(MessageListener::is_orderly);
        // Only orderly clustering consumption needs broker-side queue locks: broadcasting has no lock
        // arbitration and POP uses its own invisible-time mechanism.
        if orderly && !cfg.pop_mode && cfg.message_model == MessageModel::CLUSTERING {
            let mqs: Vec<MessageQueue> = lock(&inner.state).assigned.clone();
            if !mqs.is_empty() {
                let client = match require_client(&inner) {
                    Ok(c) => c,
                    Err(_) => return,
                };
                let client_id = cfg.client_id.clone().unwrap_or_default();
                match client
                    .lock_batch_mq(&cfg.consumer_group, &client_id, &mqs, 1000)
                    .await
                {
                    Ok(ok) => {
                        let keys: BTreeSet<String> = ok.iter().map(mq_key).collect();
                        let n = keys.len();
                        let now = current_time_millis();
                        let mut state = lock(&inner.state);
                        state.lock_ok = keys;
                        state.lock_at.clear();
                        for mq in &ok {
                            state.lock_at.insert(mq_key(mq), now);
                        }
                        rmq_debug!("lock_batch_mq: {n}/{} queues locked", mqs.len());
                    }
                    Err(e) => rmq_debug!("lock mq error: {e}"),
                }
            }
        }
        if wait_or_stop(&mut rx, 20_000).await {
            return;
        }
    }
}

/// Hand the batches in each queue buffer to the listener sequentially.
async fn dispatch_loop(inner: Arc<Inner>, mut rx: watch::Receiver<bool>) {
    loop {
        if stopped(&rx) || !inner.started.load(Ordering::Acquire) {
            return;
        }
        let keys: Vec<String> = lock(&inner.state).pending.keys().cloned().collect();
        let mut progressed = false;
        for key in keys {
            if stopped(&rx) || !inner.started.load(Ordering::Acquire) {
                return;
            }
            let (mq, batch, epoch) = {
                let mut state = lock(&inner.state);
                let Some(mq) = state.mq_map.get(&key).cloned() else {
                    continue;
                };
                let batch = {
                    let Some(dq) = state.pending.get_mut(&key) else {
                        continue;
                    };
                    if dq.is_empty() {
                        continue;
                    }
                    let cfg = read_cfg(&inner);
                    let n = dq
                        .len()
                        .min(cfg.consume_message_batch_max_size.max(1) as usize);
                    let batch: Vec<MessageExt> = dq.drain(..n).collect();
                    batch
                };
                // Registered as in-flight in the same critical section as "taking the batch": the gate in correct_tags_offset_locked relies on it
                // to tell "messages still in the buffer" from "messages in the listener's hands" -- neither counts as empty.
                *state.inflight.entry(key.clone()).or_insert(0) += 1;
                let epoch = state.queue_epoch.get(&key).copied().unwrap_or(0);
                (mq, batch, epoch)
            };
            if batch.is_empty() {
                continue;
            }
            let outcome = consume_batch(&inner, &key, &mq, batch.clone(), Some(epoch)).await;
            {
                let mut state = lock(&inner.state);
                let left = match state.inflight.get_mut(&key) {
                    Some(v) => {
                        *v = v.saturating_sub(1);
                        *v
                    }
                    None => 0,
                };
                if left == 0 {
                    state.inflight.remove(&key);
                }
            }
            match outcome {
                Ok(done) => progressed = progressed || done,
                Err(e) => {
                    // Unexpected exception on the dispatch path: push the batch back to the head of the queue and retry later (it must not kill the dispatch task)
                    rmq_error!("dispatch batch error (will retry): {e}");
                    {
                        let mut state = lock(&inner.state);
                        if let Some(dq) = state.pending.get_mut(&key) {
                            for m in batch.iter().rev() {
                                dq.push_front(m.clone());
                            }
                        }
                    }
                    if wait_or_stop(&mut rx, 100).await {
                        return;
                    }
                }
            }
        }
        if !progressed && wait_or_stop(&mut rx, 50).await {
            return;
        }
    }
}

impl DefaultMQPushConsumer {
    /// Start all background loops.
    fn spawn_loops(&self) {
        let Some(handle) = self.runtime_handle() else {
            rmq_warn!("consumer: no tokio runtime, background loops disabled");
            return;
        };
        let weak = Arc::downgrade(&self.inner);
        macro_rules! spawn_loop {
            ($f:path) => {{
                let w = weak.clone();
                let rx = self.inner.stop.subscribe();
                handle.spawn(async move {
                    if let Some(inner) = w.upgrade() {
                        $f(inner, rx).await;
                    }
                })
            }};
        }
        let tasks = &mut *lock(&self.inner.tasks);
        tasks.push(spawn_loop!(heartbeat_loop));
        let orderly = lock(&self.inner.listener)
            .as_ref()
            .is_some_and(MessageListener::is_orderly);
        // The lock sweep exists only for orderly clustering consumption.
        let cfg = read_cfg(&self.inner);
        if orderly && !cfg.pop_mode && cfg.message_model == MessageModel::CLUSTERING {
            tasks.push(spawn_loop!(lock_loop));
        }
        // Dispatch and rebalance: rebalance needs the handle (it starts the per-queue loops), so it uses a clone of the consumer.
        // The dispatch loop does not go into tasks (shutdown must not abort it -- it may be holding an in-flight send-back RPC),
        // and is registered separately in dispatch_task so the finalize task waits for it within a bound.
        let consumer = self.clone();
        let rx = self.inner.stop.subscribe();
        tasks.push(handle.spawn(async move { rebalance_loop(consumer, rx).await }));
        *lock(&self.inner.dispatch_task) = Some(spawn_loop!(dispatch_loop));
        if !orderly && !cfg.pop_mode {
            tasks.push(spawn_loop!(clean_expire_loop));
        }
    }
}

/// Offset persistence list for shutdown: the smaller of `consume_offsets` and "the minimum queueOffset among messages still unsettled".
fn shutdown_persist_items(inner: &Inner) -> Vec<(MessageQueue, i64)> {
    let state = lock(&inner.state);
    state
        .consume_offsets
        .iter()
        .filter_map(|(k, off)| {
            let mq = state.mq_map.get(k)?;
            let floor = state
                .inflight_msgs
                .get(k)
                .into_iter()
                .flatten()
                .chain(state.pending.get(k).into_iter().flatten())
                .map(|m| m.queue_offset)
                .min();
            Some((mq.clone(), floor.map_or(*off, |f| (*off).min(f))))
        })
        .collect()
}

/// One round of offset persistence (cluster mode flushes to the broker / broadcasting mode flushes locally).
async fn persist_offsets_once(inner: &Arc<Inner>, client: &MQClientInstance) {
    let cfg = read_cfg(inner);
    if cfg.message_model == MessageModel::BROADCASTING {
        if let Err(e) = save_local_offsets(inner) {
            rmq_debug!("save local offsets failed: {e}");
        }
        return;
    }
    let items: Vec<(MessageQueue, i64)> = {
        let state = lock(&inner.state);
        state
            .consume_offsets
            .iter()
            .filter_map(|(k, off)| state.mq_map.get(k).map(|mq| (mq.clone(), *off)))
            .collect()
    };
    for (mq, off) in items {
        if let Err(e) = client
            .update_consumer_offset(&cfg.consumer_group, &mq, off, 5000, None)
            .await
        {
            rmq_debug!("update consumer offset failed for {mq:?}: {e}");
        }
    }
}

fn local_offset_path(inner: &Inner) -> Option<std::path::PathBuf> {
    let cfg = read_cfg(inner);
    local_offset_store_path(cfg.client_id.as_deref(), &cfg.consumer_group)
}

/// Local offset file location for a consumer identity: `~/.rocketmq_offsets/<clientId>/<group>/offsets.json`.
pub(crate) fn local_offset_store_path(
    client_id: Option<&str>,
    group: &str,
) -> Option<std::path::PathBuf> {
    let home = crate::common::util_all::user_home()?;
    let base = std::path::Path::new(&home)
        .join(".rocketmq_offsets")
        .join(client_id.unwrap_or("DEFAULT"))
        .join(group);
    Some(base.join("offsets.json"))
}

fn build_local_offsets_json(
    items: &BTreeMap<String, i64>,
    mq_map: &BTreeMap<String, MessageQueue>,
) -> String {
    fn jstr(s: &str) -> String {
        serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string())
    }
    let mut s = String::from("{\"offsetTable\":{");
    let mut first = true;
    for (key, off) in items {
        let Some(mq) = mq_map.get(key) else { continue };
        if !first {
            s.push(',');
        }
        first = false;
        s.push_str(&format!(
            "{{\"brokerName\":{},\"queueId\":{},\"topic\":{}}}:{}",
            jstr(&mq.broker_name),
            mq.queue_id,
            jstr(&mq.topic),
            off
        ));
    }
    s.push_str("}}");
    s
}

fn parse_local_offsets_text(text: &str) -> Option<BTreeMap<String, i64>> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(flat) = serde_json::from_str::<BTreeMap<String, i64>>(trimmed) {
        return Some(flat);
    }
    let entries = scan_offset_store_entries(trimmed)?;
    Some(
        entries
            .into_iter()
            .map(|(topic, broker, qid, off)| {
                (mq_key(&MessageQueue::new(&topic, &broker, qid)), off)
            })
            .collect(),
    )
}

/// No regex crate, so a hand-written scanner: `{"offsetTable":{{...}:9,...}}` (tolerant of the newline/indent
/// of the fastjson2 pretty version). Returns a list of `(topic, brokerName, queueId, offset)`.
fn scan_offset_store_entries(text: &str) -> Option<Vec<(String, String, i32, i64)>> {
    struct Scanner<'a> {
        b: &'a [u8],
        pos: usize,
    }
    impl<'a> Scanner<'a> {
        fn skip_ws(&mut self) {
            while self.pos < self.b.len() && self.b[self.pos].is_ascii_whitespace() {
                self.pos += 1;
            }
        }
        fn eat(&mut self, c: u8) -> bool {
            self.skip_ws();
            if self.b.get(self.pos) == Some(&c) {
                self.pos += 1;
                true
            } else {
                false
            }
        }
        fn peek(&mut self) -> Option<u8> {
            self.skip_ws();
            self.b.get(self.pos).copied()
        }
        fn scan_string(&mut self) -> Option<String> {
            if self.peek()? != b'"' {
                return None;
            }
            self.pos += 1;
            let mut out = String::new();
            let mut raw_start = self.pos;
            loop {
                let c = *self.b.get(self.pos)?;
                if c == b'"' {
                    out.push_str(std::str::from_utf8(&self.b[raw_start..self.pos]).ok()?);
                    self.pos += 1;
                    return Some(out);
                }
                if c == b'\\' {
                    out.push_str(std::str::from_utf8(&self.b[raw_start..self.pos]).ok()?);
                    self.pos += 1;
                    let e = *self.b.get(self.pos)?;
                    self.pos += 1;
                    match e {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let hex =
                                std::str::from_utf8(self.b.get(self.pos..self.pos + 4)?).ok()?;
                            out.push(char::from_u32(u32::from_str_radix(hex, 16).ok()?)?);
                            self.pos += 4;
                        }
                        _ => return None,
                    }
                    raw_start = self.pos;
                    continue;
                }
                self.pos += 1;
            }
        }
        fn scan_int(&mut self) -> Option<i64> {
            self.skip_ws();
            let start = self.pos;
            if self.b.get(self.pos) == Some(&b'-') {
                self.pos += 1;
            }
            let digits = self.pos;
            while matches!(self.b.get(self.pos), Some(c) if c.is_ascii_digit()) {
                self.pos += 1;
            }
            if self.pos == digits {
                return None;
            }
            std::str::from_utf8(&self.b[start..self.pos])
                .ok()?
                .parse()
                .ok()
        }
    }

    let mut s = Scanner {
        b: text.as_bytes(),
        pos: 0,
    };
    if !s.eat(b'{') {
        return None;
    }
    if s.scan_string()?.as_str() != "offsetTable" {
        return None;
    }
    if !s.eat(b':') || !s.eat(b'{') {
        return None;
    }
    let mut out = Vec::new();
    loop {
        if s.eat(b'}') {
            break;
        }
        if !s.eat(b'{') {
            return None;
        }
        let (mut topic, mut broker, mut qid) = (None, None, None);
        loop {
            if s.eat(b'}') {
                break;
            }
            let fkey = s.scan_string()?;
            if !s.eat(b':') {
                return None;
            }
            if s.peek() == Some(b'"') {
                let v = s.scan_string()?;
                match fkey.as_str() {
                    "topic" => topic = Some(v),
                    "brokerName" => broker = Some(v),
                    _ => {}
                }
            } else {
                let v = s.scan_int()?;
                if fkey == "queueId" {
                    qid = Some(i32::try_from(v).ok()?);
                }
            }
            if s.eat(b',') {
                continue;
            }
            if s.eat(b'}') {
                break;
            }
            return None;
        }
        if !s.eat(b':') {
            return None;
        }
        let off = s.scan_int()?;
        out.push((topic?, broker?, qid?, off));
        if s.eat(b',') {
            continue;
        }
        if s.eat(b'}') {
            break;
        }
        return None;
    }
    // After the table ends there is still the wrapper's own `}` ({"offsetTable":{...}} has two brace levels)
    if !s.eat(b'}') {
        return None;
    }
    s.skip_ws();
    if s.pos != s.b.len() {
        return None;
    }
    Some(out)
}

fn save_local_offsets(inner: &Inner) -> Result<()> {
    save_local_offsets_with(inner, &[])
}

/// `save_local_offsets` plus "the offsets of queues just revoked, no longer present in `consume_offsets`/`mq_map`".
fn save_local_offsets_with(inner: &Inner, extra: &[(MessageQueue, Option<i64>)]) -> Result<()> {
    let Some(path) = local_offset_path(inner) else {
        return Err(Error::client("HOME is not set, cannot store local offsets"));
    };
    let (mut items, mut mq_map) = load_local_offset_table_at(&path)?;
    {
        let st = lock(&inner.state);
        for (key, off) in &st.consume_offsets {
            items.insert(key.clone(), *off);
        }
        for (key, mq) in &st.mq_map {
            mq_map.insert(key.clone(), mq.clone());
        }
    }
    merge_retired_offsets(&mut items, &mut mq_map, extra);
    save_local_offsets_at(&path, &items, &mq_map)
}

/// Merge the offsets of just-revoked queues (no longer in `consume_offsets`/`mq_map`) into the snapshot being flushed.
///
/// The queue info must be added to `mq_map` too: `build_local_offsets_json` skips an entry whose queue it cannot find,
/// so merging only the offsets would be the same as merging nothing.
fn merge_retired_offsets(
    items: &mut BTreeMap<String, i64>,
    mq_map: &mut BTreeMap<String, MessageQueue>,
    extra: &[(MessageQueue, Option<i64>)],
) {
    for (mq, off) in extra {
        let Some(off) = off else { continue };
        let key = mq_key(mq);
        items.insert(key.clone(), *off);
        mq_map.entry(key).or_insert_with(|| mq.clone());
    }
}

fn save_local_offsets_at(
    path: &std::path::Path,
    items: &BTreeMap<String, i64>,
    mq_map: &BTreeMap<String, MessageQueue>,
) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    if let Ok(prev) = std::fs::read_to_string(path) {
        if !prev.is_empty() {
            let _ = std::fs::write(path.with_file_name("offsets.json.bak"), prev);
        }
    }
    let tmp = path.with_file_name("offsets.json.tmp");
    std::fs::write(&tmp, build_local_offsets_json(items, mq_map))?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// When the main file is missing or corrupt, fall back to `.bak`; if both fail, treat it as "there are
/// no local offsets".
fn load_local_offsets(inner: &Inner) -> BTreeMap<String, i64> {
    let Some(path) = local_offset_path(inner) else {
        return BTreeMap::new();
    };
    load_local_offsets_at(&path)
}

fn load_local_offsets_at(path: &std::path::Path) -> BTreeMap<String, i64> {
    for candidate in [path.to_path_buf(), path.with_file_name("offsets.json.bak")] {
        let Ok(text) = std::fs::read_to_string(&candidate) else {
            continue;
        };
        if let Some(parsed) = parse_local_offsets_text(&text) {
            return parsed;
        }
    }
    BTreeMap::new()
}

/// The existing file's entries together with their queue identities, so the union write can keep
/// entries for queues that are not part of this flush. A file that exists but cannot be read or
/// parsed aborts the flush instead of being overwritten with a partial table.
fn load_local_offset_table_at(
    path: &std::path::Path,
) -> Result<(BTreeMap<String, i64>, BTreeMap<String, MessageQueue>)> {
    let mut unreadable = None;
    for candidate in [path.to_path_buf(), path.with_file_name("offsets.json.bak")] {
        let text = match std::fs::read_to_string(&candidate) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                unreadable = Some(format!("{}: {e}", candidate.display()));
                continue;
            }
        };
        if text.trim().is_empty() {
            continue;
        }
        match parse_local_offsets_with_queues(&text) {
            Some(parsed) => return Ok(parsed),
            None => unreadable = Some(format!("{}: malformed offset table", candidate.display())),
        }
    }
    match unreadable {
        Some(why) => Err(Error::client(format!(
            "local offset store is unreadable, keeping the file: {why}"
        ))),
        None => Ok((BTreeMap::new(), BTreeMap::new())),
    }
}

fn parse_local_offsets_with_queues(
    text: &str,
) -> Option<(BTreeMap<String, i64>, BTreeMap<String, MessageQueue>)> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(flat) = serde_json::from_str::<BTreeMap<String, i64>>(trimmed) {
        // A legacy flat table carries no queue identities, so its entries cannot be rewritten.
        return Some((flat, BTreeMap::new()));
    }
    let entries = scan_offset_store_entries(trimmed)?;
    let mut items = BTreeMap::new();
    let mut mq_map = BTreeMap::new();
    for (topic, broker, qid, off) in entries {
        let mq = MessageQueue::new(&topic, &broker, qid);
        let key = mq_key(&mq);
        items.insert(key.clone(), off);
        mq_map.insert(key, mq);
    }
    Some((items, mq_map))
}

/// Read the local offset of one queue (broadcasting pull/lite offset store).
pub(crate) fn read_local_offset(
    client_id: Option<&str>,
    group: &str,
    mq: &MessageQueue,
) -> Option<i64> {
    let path = local_offset_store_path(client_id, group)?;
    load_local_offsets_at(&path).get(&mq_key(mq)).copied()
}

/// Write the given queue offsets into the local file, keeping every entry already present there.
pub(crate) fn write_local_offsets(
    client_id: Option<&str>,
    group: &str,
    entries: &[(MessageQueue, i64)],
) -> Result<()> {
    let Some(path) = local_offset_store_path(client_id, group) else {
        return Err(Error::client("HOME is not set, cannot store local offsets"));
    };
    write_local_offsets_at(&path, entries)
}

/// Path-taking core of [`write_local_offsets`]: read the existing table, overlay `entries` and
/// write the union.
pub(crate) fn write_local_offsets_at(
    path: &std::path::Path,
    entries: &[(MessageQueue, i64)],
) -> Result<()> {
    let (mut items, mut mq_map) = load_local_offset_table_at(path)?;
    for (mq, off) in entries {
        let key = mq_key(mq);
        items.insert(key.clone(), *off);
        mq_map.insert(key, mq.clone());
    }
    save_local_offsets_at(path, &items, &mq_map)
}

/// Broker-backed consumers keep their offsets on the broker; broadcasting instances own a local
/// file, so two hosts in one group cannot overwrite each other's progress.
pub(crate) fn uses_local_offset_store(message_model: &str) -> bool {
    message_model == MessageModel::BROADCASTING
}

// ================================================================ POP consumption

/// Split into its own function because a missed record is a **silent** failure: messages keep popping and acking, consumption looks perfectly
/// normal, and only the 307 status table (the operations dashboard) shows a field of zeros -- while "this consumer is not pulling" and "it never started at all" call for different responses.
fn record_pop_pull_stats(
    stats: &ConsumerStatsManager,
    group: &str,
    topic: &str,
    status: PopStatus,
    delivered: usize,
    began: i64,
) {
    if status != PopStatus::Found {
        return;
    }
    stats.inc_pull_rt(group, topic, current_time_millis() - began);
    if delivered > 0 {
        stats.inc_pull_tps(group, topic, i64::try_from(delivered).unwrap_or(i64::MAX));
    }
}

/// The key difference from the pull path: **neither query nor commit a consume offset** (progress is tracked by the
/// broker-side checkpoint and confirmation relies only on ack); a pop delivers straight to the consume thread; `POLLING_NOT_FOUND` is a normal state.
async fn queue_pop_loop(inner: Arc<Inner>, mq: MessageQueue, token: u64) {
    let key = mq_key(&mq);
    run_queue_pop_loop(inner.clone(), mq, token).await;
    mark_pull_loop_exited(&inner, &key, token);
}

/// Pull-path twin of [`record_pop_pull_stats`], split out so the FOUND gate can be tested instead of
/// buried in the loop. Both legs are easy to lose silently: recording the round trip on every status
/// buries the pull's own latency under idle rounds, and feeding the raw response to PULL_TPS
/// overstates throughput by exactly the rows this client dropped before delivery.
fn record_pull_round_stats(
    stats: &ConsumerStatsManager,
    group: &str,
    topic: &str,
    found: bool,
    rt: i64,
    delivered: usize,
) {
    if !found {
        return;
    }
    stats.inc_pull_rt(group, topic, rt);
    if delivered > 0 {
        stats.inc_pull_tps(group, topic, i64::try_from(delivered).unwrap_or(i64::MAX));
    }
}

async fn run_queue_pop_loop(inner: Arc<Inner>, mq: MessageQueue, token: u64) {
    let Ok(client) = require_client(&inner) else {
        return;
    };
    let key = mq_key(&mq);
    let cfg = read_cfg(&inner);
    // Same capture rule as the pull loop: the generic failed-round backoff is read once when this
    // loop is set up, so a knob moved afterwards cannot change a wait this loop already arms with.
    let failed_round_backoff = cfg.pull_exception_backoff_millis.max(0) as u64;
    let mut invisible = cfg.pop_invisible_time;
    if !(MIN_POP_INVISIBLE_TIME..=MAX_POP_INVISIBLE_TIME).contains(&invisible) {
        invisible = DEFAULT_POP_INVISIBLE_TIME;
    }
    let init_mode = if cfg.consume_from_where == ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET {
        ConsumeInitMode::MIN
    } else {
        ConsumeInitMode::MAX
    };
    let mut rx = inner.stop.subscribe();
    while !stopped(&rx) && inner.started.load(Ordering::Acquire) {
        if !owns_queue(&inner, &key, token) {
            return;
        }
        let pq = match lock(&inner.state).pop_queues.get(&key) {
            Some(pq) => pq.clone(),
            None => return,
        };
        if pq.is_dropped() {
            return;
        }
        stamp_pull_at(&inner, &key, true);
        if inner.pause.load(Ordering::SeqCst) {
            if wait_or_stop(&mut rx, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND).await {
                return;
            }
            continue;
        }
        let sub = match lock(&inner.state).subscription(&mq.topic).cloned() {
            Some(sub) => sub,
            None => return,
        };
        let cfg = read_cfg(&inner);
        if pop_flow_control_hit(&inner, &key, &pq) {
            if wait_or_stop(&mut rx, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL).await {
                return;
            }
            continue;
        }
        let began = current_time_millis();
        let exp = if sub.sub_string.is_empty() {
            "*"
        } else {
            &sub.sub_string
        };
        let result = match client
            .pop_message(
                &cfg.consumer_group,
                &mq.topic,
                mq.queue_id,
                cfg.pop_batch_nums,
                invisible,
                cfg.pop_poll_time_millis,
                init_mode,
                Some(exp),
                Some(&sub.expression_type),
                false,
                Some(&mq.broker_name),
                cfg.pop_timeout_millis,
                None,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                if matches!(e, Error::Timeout { .. }) {
                    rmq_debug!("pop long-poll timeout for {mq:?} (benign, will retry)");
                    continue;
                }
                let delay = failed_request_backoff_millis(&e, failed_round_backoff);
                rmq_debug!("pop error for {mq:?}: {e}");
                if wait_or_stop(&mut rx, delay).await {
                    return;
                }
                continue;
            }
        };
        if !owns_queue(&inner, &key, token) || pq.is_dropped() {
            rmq_debug!(
                "queue {key} revoked during pop, discard {} messages un-acked",
                result.msg_found_list.len()
            );
            return;
        }
        pq.touch(current_time_millis());
        // The pre-delivery pipeline runs first, so what it removes is never booked, never
        // counted as delivered and never reaches the listener; the rows it removes are acked
        // there and then.
        let kept = if result.status == PopStatus::Found && !result.msg_found_list.is_empty() {
            pq.inc_found_msg(i32::try_from(result.msg_found_list.len()).unwrap_or(i32::MAX));
            prepare_pop_batch_for_delivery(
                &inner,
                &cfg,
                &mq,
                &sub,
                &result.msg_found_list,
                &pq,
            )
        } else {
            Vec::new()
        };
        // Pull statistics: the same status table as the pull path, POP mode only; placed after
        // the queue-revoked early exit and after the pre-delivery pipeline, so a discarded batch
        // is not counted at all and a dropped row counts as nothing but the round it cost.
        if let Some(stats) = lock(&inner.stats).clone() {
            let group = read_cfg(&inner).consumer_group;
            record_pop_pull_stats(
                &stats,
                &group,
                &mq.topic,
                result.status,
                kept.len(),
                began,
            );
        }
        let kept_len = kept.len();
        if !kept.is_empty() {
            submit_pop_consume_request(&inner, kept, pq.clone(), mq.clone()).await;
        }
        let delay = pop_next_round_delay_millis(
            result.status,
            kept_len,
            cfg.pull_interval,
            failed_round_backoff,
        );
        if delay > 0 {
            if wait_or_stop(&mut rx, delay).await {
                return;
            }
        } else if kept_len == 0 && current_time_millis() - began < 200 {
            // The table said no wait, and the round handed over nothing in under 200 ms: the
            // broker did not hold this poll open, so straight back to the top would spin hot
            // against it. A round that really suspended never takes this nap.
            if wait_or_stop(&mut rx, 200).await {
                return;
            }
        }
    }
}

/// What a FOUND answer is reduced to before anything is booked or handed over: the same
/// client-side tag pass and hook list a pulled batch gets, then the retry budget as an **early**
/// guard -- a row strictly past the effective budget is dropped here instead of reaching the
/// listener, while a row that has exactly used up its budget still goes through and stays under
/// the listener-side handling.
///
/// Every row either step removed is acked immediately, with the ack the success path sends. An
/// un-acked POP row simply comes back when its invisibility window closes, so "filtered out but
/// not acked" would replay the same rows round after round. The rows acked here never entered the
/// queue's answer debt -- the caller fed it with the whole popped count, so one credit back per
/// row confirmed is what settles that bookkeeping.
fn prepare_pop_batch_for_delivery(
    inner: &Arc<Inner>,
    cfg: &ConsumerConfig,
    mq: &MessageQueue,
    sub: &SubscriptionData,
    popped: &[MessageExt],
    pq: &Arc<PopProcessQueue>,
) -> Vec<MessageExt> {
    let filtered = filter_messages_for_delivery(
        &cfg.consumer_group,
        &inner.filter_hooks,
        mq,
        Some(sub),
        popped.to_vec(),
        cfg.unit_mode,
    );
    let budget = pop_max_reconsume_times(cfg);
    let kept: Vec<MessageExt> = filtered
        .into_iter()
        .filter(|msg| msg.reconsume_times <= budget)
        .collect();
    if kept.len() == popped.len() {
        return kept;
    }
    let kept_ids: BTreeSet<(String, i64)> = kept
        .iter()
        .map(|m| (m.msg_id.clone().unwrap_or_default(), m.queue_offset))
        .collect();
    let mut dropped_cnt = 0usize;
    for msg in popped {
        let id = (msg.msg_id.clone().unwrap_or_default(), msg.queue_offset);
        if !kept_ids.contains(&id) {
            ack_pop_msg(inner, msg);
            pq.ack();
            dropped_cnt += 1;
        }
    }
    rmq_info!(
        "pop filter dropped {dropped_cnt} of {} messages (acked)",
        popped.len()
    );
    kept
}

/// The retry budget a POP round measures its rows against: the consumer's own number, or the
/// default 16 while the knob is left unset.
fn pop_max_reconsume_times(cfg: &ConsumerConfig) -> i32 {
    if cfg.max_reconsume_times == -1 {
        16
    } else {
        cfg.max_reconsume_times
    }
}

/// The wait before the next pop on the same queue, decided by the answer the broker gave:
///
/// - FOUND with rows handed over: the configured pull cadence (`0` = straight back to the top);
/// - FOUND with nothing left, and NO_NEW_MSG / POLLING_NOT_FOUND: nothing to wait for -- the
///   broker already held this poll open, so the next round starts immediately;
/// - POLLING_FULL, and any status this client has no rule for: the round failed to produce
///   anything this side can act on, so it takes the SAME generic failed-round backoff the pull and
///   POP error arms take -- never a private fixed park.
///
/// Like the pull path, the backoff comes in as a parameter: the loop captured the configured value
/// when it was set up, so this stays pure and a post-start knob change cannot reach it.
fn pop_next_round_delay_millis(
    status: PopStatus,
    kept_len: usize,
    pull_interval: i64,
    pull_exception_backoff_millis: u64,
) -> u64 {
    match status {
        PopStatus::Found if kept_len > 0 => pull_interval.max(0) as u64,
        PopStatus::Found | PopStatus::NoNewMsg | PopStatus::PollingNotFound => 0,
        _ => pull_exception_backoff_millis,
    }
}

async fn submit_pop_consume_request(
    inner: &Arc<Inner>,
    msgs: Vec<MessageExt>,
    pq: Arc<PopProcessQueue>,
    mq: MessageQueue,
) {
    let orderly = lock(&inner.listener)
        .as_ref()
        .is_some_and(MessageListener::is_orderly);
    if orderly {
        submit_pop_orderly_request(inner, pq, mq, false).await;
        return;
    }
    let size = read_cfg(inner).consume_message_batch_max_size.max(1) as usize;
    let mut batches: Vec<Vec<MessageExt>> = Vec::new();
    let mut rest = msgs;
    while rest.len() > size {
        batches.push(rest.drain(..size).collect());
    }
    if !rest.is_empty() {
        batches.push(rest);
    }
    let executor = lock(&inner.pop_executor).clone();
    for batch in batches {
        let pq = Arc::clone(&pq);
        let mq = mq.clone();
        match &executor {
            Some(executor) => {
                let w = Arc::downgrade(inner);
                let task: ConsumeTask = Box::pin(async move {
                    if let Some(inner) = w.upgrade() {
                        consume_pop_batch(inner, batch, pq, mq).await;
                    }
                });
                if let Err(e) = executor.submit(task) {
                    rmq_debug!("pop executor submit failed: {e}");
                }
            }
            // No executor started (unit tests or not started): run the batch synchronously in place, never dropping it
            None => consume_pop_batch(inner.clone(), batch, pq, mq).await,
        }
    }
}

async fn submit_pop_orderly_request(
    inner: &Arc<Inner>,
    pq: Arc<PopProcessQueue>,
    mq: MessageQueue,
    force: bool,
) {
    let key = (Arc::as_ptr(&pq) as usize, mq_key(&mq));
    {
        let mut state = lock(&inner.state);
        let is_new = state.pop_orderly_requests.insert(key.clone());
        if !force && !is_new {
            return;
        }
    }
    let executor = lock(&inner.pop_executor).clone();
    match executor {
        Some(executor) => {
            let w = Arc::downgrade(inner);
            let task_mq = mq.clone();
            let task: ConsumeTask = Box::pin(async move {
                if let Some(inner) = w.upgrade() {
                    run_pop_orderly_request(inner, pq, task_mq).await;
                }
            });
            if let Err(e) = executor.submit(task) {
                rmq_error!("error submit pop orderly request: {e}, mq: {mq:?}");
            }
        }
        // No executor started (unit tests or not started): run synchronously, as on the concurrent path
        None => run_pop_orderly_request(inner.clone(), pq, mq).await,
    }
}

async fn run_pop_orderly_request(inner: Arc<Inner>, pq: Arc<PopProcessQueue>, mq: MessageQueue) {
    if pq.is_dropped() {
        rmq_warn!("run, message queue not be able to consume, because it's dropped. {mq:?}");
        let key = (Arc::as_ptr(&pq) as usize, mq_key(&mq));
        lock(&inner.state).pop_orderly_requests.remove(&key);
    }
}

async fn consume_pop_batch(
    inner: Arc<Inner>,
    msgs: Vec<MessageExt>,
    pq: Arc<PopProcessQueue>,
    mq: MessageQueue,
) {
    if pq.is_dropped() || msgs.is_empty() {
        return;
    }
    let (mut pop_time, mut invisible) = (0i64, 0i64);
    match msgs[0].get_property(PROPERTY_POP_CK) {
        Some(ck) => match extra_info::split(ck) {
            Ok(segments) => {
                pop_time = extra_info::get_pop_time(&segments).unwrap_or(0);
                invisible = extra_info::get_invisible_time(&segments).unwrap_or(0);
            }
            Err(e) => rmq_debug!("parse pop ck failed for {mq:?}, treat as timed out: {e}"),
        },
        None => rmq_debug!("message without POP_CK, treat as timed out"),
    }
    if is_pop_timeout(&msgs, pop_time, invisible) {
        // Already past invisibleTime: an ack would not be accepted either, so abandon this batch outright (wait for the broker to revive and redeliver)
        rmq_debug!(
            "pop timeout, abort consume for {mq:?}: popTime={pop_time} invisible={invisible}"
        );
        pq.dec_found_msg(i32::try_from(msgs.len()).unwrap_or(i32::MAX));
        return;
    }
    let mut msgs = msgs;
    // A POP retry row reaches this entry still wearing `%RETRY%<group>_+<topic>`; the
    // listener is handed the business topic it subscribed to instead. This batch only ever
    // comes from the concurrent path, so the cut is unconditional here.
    reset_pop_retry_topic(&inner, &mut msgs);
    reset_retry_topic_and_namespace(&inner, &mut msgs);
    let mut context = ConsumeConcurrentlyContext::new(Some(mq.clone()));
    context.ack_index = i32::try_from(msgs.len()).unwrap_or(i32::MAX) - 1;

    let mut hook_ctx = if inner.consume_hooks.has_hooks() {
        Some(build_consume_hook_context(&inner, &msgs, &mq))
    } else {
        None
    };
    if let Some(ctx) = hook_ctx.as_mut() {
        execute_consume_hook_before(&inner.consume_hooks, ctx);
    }
    let begin_ms = current_time_millis();
    let pop_stamp_now = current_time_millis();
    for m in msgs.iter_mut() {
        m.put_property(PROPERTY_CONSUME_START_TIMESTAMP, &pop_stamp_now.to_string());
    }
    let (status, has_exception) = call_concurrently_listener(&inner, &msgs, &mut context).await;
    let raw_status = status;
    let status = match status {
        Some(s) => s,
        None => {
            rmq_warn!(
                "consumeMessage return null, Group: {} Msgs: {} MQ: {mq:?}",
                read_cfg(&inner).consumer_group,
                msgs.len()
            );
            ConsumeConcurrentlyStatus::ReconsumeLater
        }
    };
    let failed = matches!(status, ConsumeConcurrentlyStatus::ReconsumeLater);
    let succeeded = matches!(status, ConsumeConcurrentlyStatus::ConsumeSuccess);
    record_consume_stats(&inner, &mq.topic, msgs.len(), begin_ms, failed, None);
    if let Some(ctx) = hook_ctx.as_mut() {
        finish_consume_hook(
            &inner,
            ctx,
            raw_status.map(ConsumeConcurrentlyStatus::name),
            status.name(),
            has_exception,
            begin_ms,
            failed,
            succeeded,
        );
    }
    // The queue was revoked or the batch already timed out while consuming: the result is no longer processed
    if pq.is_dropped() || is_pop_timeout(&msgs, pop_time, invisible) {
        pq.dec_found_msg(i32::try_from(msgs.len()).unwrap_or(i32::MAX));
        return;
    }
    process_pop_consume_result(&inner, status, &context, &msgs, &pq).await;
}

fn is_pop_timeout(msgs: &[MessageExt], pop_time: i64, invisible: i64) -> bool {
    if msgs.is_empty() || pop_time <= 0 || invisible <= 0 {
        return true;
    }
    current_time_millis() - pop_time >= invisible
}

async fn process_pop_consume_result(
    inner: &Arc<Inner>,
    status: ConsumeConcurrentlyStatus,
    context: &ConsumeConcurrentlyContext,
    msgs: &[MessageExt],
    pq: &Arc<PopProcessQueue>,
) {
    let mut ack_index = context.ack_index;
    if status == ConsumeConcurrentlyStatus::ConsumeSuccess {
        let len = i32::try_from(msgs.len()).unwrap_or(i32::MAX);
        if ack_index >= len {
            ack_index = len - 1;
        }
    } else {
        // RECONSUME_LATER: ack nothing
        ack_index = -1;
    }
    // `acked` is both the "the first `acked` messages succeeded, ack them one by one" boundary and the
    // start point of the second pass of per-message acks: when ack_index == -1 it is 0,
    // and every remaining message still needs pq.ack() plus an invisible-time extension.
    let acked = if ack_index >= 0 {
        usize::try_from(ack_index).unwrap_or(0) + 1
    } else {
        0
    };
    for msg in msgs.iter().take(acked) {
        ack_pop_msg(inner, msg);
        pq.ack();
    }
    let cfg = read_cfg(inner);
    for msg in msgs.iter().skip(acked) {
        pq.ack();
        if cfg.max_reconsume_times >= 0 && msg.reconsume_times >= cfg.max_reconsume_times {
            check_need_ack_or_delay(inner, msg, &cfg);
            continue;
        }
        change_pop_invisible_time(inner, msg, context.delay_level_when_next_consume, &cfg);
    }
}

/// The fallback once the retry budget is exhausted.
fn check_need_ack_or_delay(inner: &Inner, msg: &MessageExt, cfg: &ConsumerConfig) {
    let table = &cfg.pop_delay_level;
    let Some((last_second,)) = table.last().map(|v| (*v,)) else {
        return;
    };
    let msg_delay_time = current_time_millis() - msg.born_timestamp;
    if msg_delay_time > i64::from(last_second) * 1000 * 2 {
        rmq_warn!(
            "pop consume too many times, ack and drop: key={}",
            msg.get_keys().unwrap_or_default()
        );
        ack_pop_msg(inner, msg);
        return;
    }
    let mut level = table.len() as i32 - 1;
    while level >= 0 {
        if msg_delay_time >= i64::from(table[level as usize]) * 1000 {
            level += 1;
            break;
        }
        level -= 1;
    }
    change_pop_invisible_time(inner, msg, level, cfg);
}

/// Decode the five-tuple needed for ack / invisible-time extension out of POP_CK.
///
/// Neither place may be assumed: (1) the topic must be restored with `getRealTopic` using the CK's retryFlag (the real
/// topic of a revived message is `%RETRY%<group>_<topic>`, not the topic on the message); (2) the address must be looked up by the
/// brokerName inside the CK, not by asking for the topic's route (a retry topic usually has no route entry of its own).
fn pop_ck_target(inner: &Inner, msg: &MessageExt) -> Option<(String, String, i32, i64, String)> {
    let ck = msg.get_property(PROPERTY_POP_CK)?;
    if ck.is_empty() {
        rmq_debug!("pop message without POP_CK, cannot ack");
        return None;
    }
    let segments = match extra_info::split(ck) {
        Ok(s) => s,
        Err(e) => {
            rmq_debug!("bad POP_CK {ck:?}: {e}");
            return None;
        }
    };
    let broker_name = extra_info::get_broker_name(&segments).ok()?;
    let queue_id = extra_info::get_queue_id(&segments).ok()?;
    let offset = extra_info::get_queue_offset(&segments).ok()?;
    let retry = extra_info::get_retry(&segments).ok()?;
    let group = read_cfg(inner).consumer_group;
    let topic = extra_info::get_real_topic(&msg.topic, &group, &retry).ok()?;
    Some((topic, broker_name, queue_id, offset, ck.to_string()))
}

/// ⚠ ack is an RPC, and the call sites that deliberately keep **synchronous semantics** (the filter-removal path) simply `let _ =` it
/// in an async context: an ack failure is not fatal, since the message revives for redelivery when invisibleTime expires.
/// To serve all three caller shapes, this provides a blocking-agnostic "queue the task" implementation.
fn ack_pop_msg(inner: &Inner, msg: &MessageExt) {
    let Some((topic, broker_name, queue_id, offset, ck)) = pop_ck_target(inner, msg) else {
        return;
    };
    let Ok(client) = require_client(inner) else {
        return;
    };
    let group = read_cfg(inner).consumer_group;
    let Some(handle) = inner
        .runtime
        .get()
        .cloned()
        .or_else(|| tokio::runtime::Handle::try_current().ok())
    else {
        rmq_debug!("ack skipped: no runtime to run the RPC");
        return;
    };
    handle.spawn(async move {
        let addr = match client.pop_ack_addr(&broker_name, &topic, queue_id).await {
            Ok(addr) => addr,
            Err(e) => {
                rmq_debug!("ack failed: {e}");
                return;
            }
        };
        match client
            .ack_message(
                &group,
                &topic,
                queue_id,
                &ck,
                offset,
                Some(&broker_name),
                3000,
                Some(&addr),
            )
            .await
        {
            Ok(_) => {}
            Err(e) => rmq_debug!("ack failed: {e}"),
        }
    });
}

/// With `delay_level == 0` the message's already-retried count is used as the level;
/// the level table is in **seconds** while the interface wants milliseconds.
fn change_pop_invisible_time(
    inner: &Inner,
    msg: &MessageExt,
    delay_level: i32,
    cfg: &ConsumerConfig,
) {
    let Some((topic, broker_name, queue_id, offset, ck)) = pop_ck_target(inner, msg) else {
        return;
    };
    let level = if delay_level == 0 {
        msg.reconsume_times
    } else {
        delay_level
    };
    let table = &cfg.pop_delay_level;
    let Some(last) = table.last() else {
        return;
    };
    let delay_second = if level as usize >= table.len() {
        *last
    } else {
        table[level.max(0) as usize]
    };
    let Ok(client) = require_client(inner) else {
        return;
    };
    let group = cfg.consumer_group.clone();
    let Some(handle) = inner
        .runtime
        .get()
        .cloned()
        .or_else(|| tokio::runtime::Handle::try_current().ok())
    else {
        rmq_debug!("change invisible time skipped: no runtime to run the RPC");
        return;
    };
    handle.spawn(async move {
        let addr = match client.pop_ack_addr(&broker_name, &topic, queue_id).await {
            Ok(addr) => addr,
            Err(e) => {
                rmq_debug!("change invisible time failed: {e}");
                return;
            }
        };
        match client
            .change_invisible_time(
                &group,
                &topic,
                queue_id,
                &ck,
                offset,
                i64::from(delay_second) * 1000,
                Some(&broker_name),
                3000,
                Some(&addr),
            )
            .await
        {
            Ok(_) => {}
            Err(e) => rmq_debug!("change invisible time failed: {e}"),
        }
    });
}

// ================================================================ Consume and hooks

/// Redelivered messages actually live under `%RETRY%<group>`, and the broker writes the original topic into the `RETRY_TOPIC` property;
/// without restoring it, user code branching on the topic goes down the wrong path.
fn reset_retry_topic_and_namespace(inner: &Inner, msgs: &mut [MessageExt]) {
    let cfg = read_cfg(inner);
    let group_topic = MixAll::get_retry_topic(&cfg.consumer_group);
    for msg in msgs.iter_mut() {
        let retry_topic = msg.get_property(PROPERTY_RETRY_TOPIC).map(str::to_string);
        if let Some(retry_topic) = retry_topic {
            if msg.topic == group_topic {
                msg.topic = retry_topic;
            }
        }
        if !cfg.namespace.is_empty() {
            msg.topic = NamespaceUtil::without_namespace(&msg.topic, &cfg.namespace);
        }
    }
}

/// Cut the POP retry naming -- `%RETRY%<group>_<topic>` or `%RETRY%<group>+<topic>` -- off the
/// topic a row is about to be delivered under, so the listener sees the topic it subscribed to.
///
/// The property-based restore cannot do this job: a POP retry row is stored under a name built
/// from the business topic, and the retry-flag half of its ack checkpoint is what puts it back, so
/// the name itself has to be read here. Getting it wrong is not cosmetic -- the ack target is
/// rebuilt from the delivery name plus that flag, so a name left wrapped gets wrapped a second
/// time when the row is confirmed.
///
/// Only this consumer's own group is recognised, in both spellings it can reach the broker in
/// (start() may have folded the namespace into the group), and an empty tail is not a topic name.
/// Runs at the concurrent delivery entries only: an orderly consumer keeps consuming the retry
/// queue under the name it locked it in, because that name keys its offset bookkeeping.
fn reset_pop_retry_topic(inner: &Inner, msgs: &mut [MessageExt]) {
    if msgs.is_empty() {
        return;
    }
    let cfg = read_cfg(inner);
    let mut groups = vec![cfg.consumer_group.clone()];
    if !cfg.namespace.is_empty() {
        let plain = NamespaceUtil::without_namespace(&cfg.consumer_group, &cfg.namespace);
        if plain != cfg.consumer_group {
            groups.push(plain);
        }
    }
    for msg in msgs.iter_mut() {
        for group in &groups {
            let normal = extra_info::parse_pop_normal_topic(&msg.topic, group);
            if normal != msg.topic {
                msg.topic = normal;
                break;
            }
        }
    }
}

fn build_consume_hook_context(
    inner: &Inner,
    msgs: &[MessageExt],
    mq: &MessageQueue,
) -> ConsumeMessageContext {
    let group = read_cfg(inner).consumer_group;
    let mut context = ConsumeMessageContext::new(&group, Some(msgs.to_vec()), Some(mq.clone()));
    context.success = false;
    context.props = Some(Default::default());
    context.access_channel = Some(AccessChannel::Local.name().to_string());
    context
}

/// Consume return type (decides the trace's contextCode).
///
/// The "whether successful" input is not needed here -- both non-failed cases return SUCCESS,
/// so this function does not take that parameter.
fn consume_return_type(
    status: Option<&str>,
    has_exception: bool,
    consume_rt_ms: i64,
    consume_timeout_minutes: i64,
    failed: bool,
) -> ConsumeReturnType {
    if status.is_none() {
        return if has_exception {
            ConsumeReturnType::Exception
        } else {
            ConsumeReturnType::ReturnNull
        };
    }
    if consume_rt_ms >= consume_timeout_minutes * 60 * 1000 {
        return ConsumeReturnType::TimeOut;
    }
    if failed {
        ConsumeReturnType::Failed
    } else {
        // Both branches yield the same value, so they are merged
        ConsumeReturnType::Success
    }
}

fn record_consume_stats(
    inner: &Inner,
    topic: &str,
    msg_count: usize,
    begin_ms: i64,
    failed: bool,
    ack_count: Option<usize>,
) {
    let Some(stats) = lock(&inner.stats).clone() else {
        return;
    };
    let group = read_cfg(inner).consumer_group;
    let rt = current_time_millis() - begin_ms;
    let msgs = i64::try_from(msg_count).unwrap_or(0);
    if failed {
        stats.inc_consume_failed_tps(&group, topic, msgs);
    } else {
        let ok = ack_count.unwrap_or(msg_count);
        let ok64 = i64::try_from(ok).unwrap_or(0);
        stats.inc_consume_ok_tps(&group, topic, ok64);
        if msgs > ok64 {
            stats.inc_consume_failed_tps(&group, topic, msgs - ok64);
        }
    }
    stats.inc_consume_rt(&group, topic, rt);
}

fn record_consume_rt(inner: &Inner, topic: &str, begin_ms: i64) {
    let Some(stats) = lock(&inner.stats).clone() else {
        return;
    };
    let rt = current_time_millis() - begin_ms;
    stats.inc_consume_rt(&read_cfg(inner).consumer_group, topic, rt);
}

/// Write returnType/status/success back into the context and trigger the after hooks.
#[allow(clippy::too_many_arguments)]
fn finish_consume_hook(
    inner: &Inner,
    ctx: &mut ConsumeMessageContext,
    raw_status_name: Option<&str>,
    hook_status_name: &str,
    has_exception: bool,
    begin_ms: i64,
    failed: bool,
    succeeded: bool,
) {
    let cfg = read_cfg(inner);
    let rt = current_time_millis() - begin_ms;
    let ret = consume_return_type(
        raw_status_name,
        has_exception,
        rt,
        cfg.consume_timeout,
        failed,
    );
    ctx.props
        .get_or_insert_with(Default::default)
        .insert("ConsumeContextType".to_string(), ret.name().to_string());
    ctx.status = Some(hook_status_name.to_string());
    ctx.success = succeeded;
    execute_consume_hook_after(&inner.consume_hooks, ctx);
}

/// Call the concurrent listener. Returns `(status, has_exception)`.
///
/// Two conventions of the concurrent listener call:
/// 1. The trait's return type is a non-Optional enum, so "the listener itself returns no status"
///    cannot happen; when **no listener is set** it is handled as "exception -> RECONSUME_LATER",
///    and `status=None` is exactly that path.
/// 2. The listener is user code and may block. It is put into
///    `spawn_blocking` here: the dispatch loop keeps the sequential "wait for this batch to finish before continuing" semantics,
///    but does not occupy a tokio worker thread. A listener panic is captured as a
///    `JoinError`, which is precisely "threw an exception -> RECONSUME_LATER".
async fn call_concurrently_listener(
    inner: &Arc<Inner>,
    msgs: &[MessageExt],
    context: &mut ConsumeConcurrentlyContext,
) -> (Option<ConsumeConcurrentlyStatus>, bool) {
    let listener = match lock(&inner.listener).as_ref().cloned() {
        Some(MessageListener::Concurrently(l)) => l,
        Some(MessageListener::Orderly(_)) => {
            // Orderly listener called on the concurrent path: callers dispatch by listener type, so this is unreachable
            return (None, true);
        }
        None => return (None, true),
    };
    let owned = msgs.to_vec();
    let mut ctx = context.clone();
    match tokio::task::spawn_blocking(move || {
        let status = listener.consume_message(&owned, &mut ctx);
        (status, ctx)
    })
    .await
    {
        Ok((status, ctx)) => {
            *context = ctx;
            (Some(status), false)
        }
        Err(e) => {
            rmq_debug!("listener error, treat as RECONSUME_LATER: {e}");
            (None, true)
        }
    }
}

/// Call the orderly listener (same as above, returning `(status, has_exception)`).
async fn call_orderly_listener(
    inner: &Arc<Inner>,
    msgs: &[MessageExt],
    context: &mut ConsumeOrderlyContext,
) -> (Option<ConsumeOrderlyStatus>, bool) {
    let listener = match lock(&inner.listener).as_ref().cloned() {
        Some(MessageListener::Orderly(l)) => l,
        _ => return (None, true),
    };
    let owned = msgs.to_vec();
    let mut ctx = context.clone();
    match tokio::task::spawn_blocking(move || {
        let status = listener.consume_message(&owned, &mut ctx);
        (status, ctx)
    })
    .await
    {
        Ok((status, ctx)) => {
            *context = ctx;
            (Some(status), false)
        }
        Err(e) => {
            rmq_debug!("orderly listener error (retry in place): {e}");
            (None, true)
        }
    }
}

/// Consume one batch and handle send-back/suspension, returning whether the offset advanced.
async fn consume_batch(
    inner: &Arc<Inner>,
    key: &str,
    mq: &MessageQueue,
    batch: Vec<MessageExt>,
    epoch: Option<u64>,
) -> Result<bool> {
    {
        let state = lock(&inner.state);
        let dropped = state.queue_epoch.get(key).copied().unwrap_or(0);
        if epoch.is_some_and(|e| e != dropped) {
            rmq_warn!(
                "the message queue not be able to consume, because it's dropped. \
                 group={} mq={mq:?} msgs={} epoch={epoch:?}->{dropped}",
                read_cfg(inner).consumer_group,
                batch.len()
            );
            return Ok(false);
        }
    }
    let cfg = read_cfg(inner);
    let broadcast = cfg.message_model == MessageModel::BROADCASTING;
    let mut batch = batch;
    let orderly = lock(&inner.listener)
        .as_ref()
        .is_some_and(MessageListener::is_orderly);
    // The POP retry naming is cut at the CONCURRENT delivery entry, ahead of the
    // property-based restore below; a pulled row simply matches nothing there. An
    // orderly consumer keeps the name it locked the queue under, because a retry
    // queue's offset bookkeeping is keyed on it.
    if !orderly {
        reset_pop_retry_topic(inner, &mut batch);
    }
    reset_retry_topic_and_namespace(inner, &mut batch);

    if orderly {
        let mut ocontext = ConsumeOrderlyContext::new(Some(mq.clone()));
        let mut hook_ctx = if inner.consume_hooks.has_hooks() {
            Some(build_consume_hook_context(inner, &batch, mq))
        } else {
            None
        };
        if let Some(ctx) = hook_ctx.as_mut() {
            execute_consume_hook_before(&inner.consume_hooks, ctx);
        }
        let begin_ms = current_time_millis();
        // `raw_status` being None can only mean the listener threw (the join of call_orderly_listener failed)
        let (raw_status, has_exception) = call_orderly_listener(inner, &batch, &mut ocontext).await;
        if matches!(
            raw_status,
            None | Some(ConsumeOrderlyStatus::Rollback)
                | Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment)
        ) {
            rmq_warn!(
                "consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {mq:?}",
                cfg.consumer_group,
                batch.len()
            );
        }
        let status = raw_status.unwrap_or(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        let failed = status == ConsumeOrderlyStatus::SuspendCurrentQueueAMoment;
        let succeeded = matches!(
            status,
            ConsumeOrderlyStatus::Success | ConsumeOrderlyStatus::Commit
        );
        if let Some(ctx) = hook_ctx.as_mut() {
            finish_consume_hook(
                inner,
                ctx,
                raw_status.map(|s| s.name()),
                status.name(),
                has_exception,
                begin_ms,
                failed,
                succeeded,
            );
        }
        if ocontext.auto_commit {
            let status = match status {
                ConsumeOrderlyStatus::Commit | ConsumeOrderlyStatus::Rollback => {
                    rmq_warn!(
                        "the message queue consume result is illegal, \
                         we think you want to ack these message {mq:?}"
                    );
                    ConsumeOrderlyStatus::Success
                }
                other => other,
            };
            if status == ConsumeOrderlyStatus::SuspendCurrentQueueAMoment {
                record_consume_stats(inner, &mq.topic, batch.len(), begin_ms, true, None);
                if check_orderly_reconsume_times(inner, &mut batch).await {
                    requeue_pending(inner, key, &batch);
                    let suspend = orderly_suspend_millis(&cfg, &ocontext);
                    tokio::time::sleep(Duration::from_millis(suspend)).await;
                    return Ok(false);
                }
            } else {
                record_consume_stats(inner, &mq.topic, batch.len(), begin_ms, false, None);
            }
            advance_consume_offset(inner, key, &batch, None, epoch);
            return Ok(true);
        }
        match status {
            ConsumeOrderlyStatus::Commit => {
                record_consume_rt(inner, &mq.topic, begin_ms);
                advance_consume_offset(inner, key, &batch, None, epoch);
                return Ok(true);
            }
            ConsumeOrderlyStatus::Rollback => {
                record_consume_rt(inner, &mq.topic, begin_ms);
                requeue_pending(inner, key, &batch);
                let suspend = orderly_suspend_millis(&cfg, &ocontext);
                tokio::time::sleep(Duration::from_millis(suspend)).await;
                return Ok(false);
            }
            ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                record_consume_stats(inner, &mq.topic, batch.len(), begin_ms, true, None);
                if check_orderly_reconsume_times(inner, &mut batch).await {
                    requeue_pending(inner, key, &batch);
                    let suspend = orderly_suspend_millis(&cfg, &ocontext);
                    tokio::time::sleep(Duration::from_millis(suspend)).await;
                }
                return Ok(false);
            }
            ConsumeOrderlyStatus::Success => {
                // Manual commit: SUCCESS only records the successful consumption. The batch is
                // neither committed nor put back, so it is not delivered again.
                record_consume_stats(inner, &mq.topic, batch.len(), begin_ms, false, None);
                return Ok(true);
            }
        }
    }

    let hook_ctx = if inner.consume_hooks.has_hooks() {
        let mut ctx = build_consume_hook_context(inner, &batch, mq);
        execute_consume_hook_before(&inner.consume_hooks, &mut ctx);
        Some(ctx)
    } else {
        None
    };
    let begin_ms = current_time_millis();
    let stamp_now = current_time_millis();
    for m in batch.iter_mut() {
        m.put_property(PROPERTY_CONSUME_START_TIMESTAMP, &stamp_now.to_string());
    }
    register_inflight_msgs(inner, key, &batch);
    let result = consume_concurrent_batch(
        inner, key, mq, &batch, epoch, &cfg, broadcast, hook_ctx, begin_ms,
    )
    .await;
    // Normal wrap-up (including re-queueing after send-back failure and removal by the sweep) leaves this batch no longer "in the listener's hands";
    // entries already removed early by the sweep / re-queue are handled idempotently here.
    deregister_inflight_msgs(inner, key, &batch);
    result
}

/// Split out of [`consume_batch`] only to wrap "register in-flight -> consume -> deregister in-flight" in one span:
/// the concurrent branch has a pile of early returns (whole batch acknowledged, broadcasting drop, send-back failure, ...), and if the
/// registration table is not cleaned, the offset is blocked forever by the in-flight gate of `correct_tags_offset_locked`. The before hooks and
/// `begin_ms` are done by the caller (they must precede the stamping/registration), so only the listener and what follows live here.
#[allow(clippy::too_many_arguments)]
async fn consume_concurrent_batch(
    inner: &Arc<Inner>,
    key: &str,
    mq: &MessageQueue,
    batch: &[MessageExt],
    epoch: Option<u64>,
    cfg: &ConsumerConfig,
    broadcast: bool,
    mut hook_ctx: Option<ConsumeMessageContext>,
    begin_ms: i64,
) -> Result<bool> {
    let mut context = ConsumeConcurrentlyContext::new(Some(mq.clone()));
    let (raw_status, has_exception) = call_concurrently_listener(inner, batch, &mut context).await;
    if raw_status.is_none() {
        rmq_warn!(
            "consumeMessage return null, Group: {} Msgs: {} MQ: {mq:?}",
            cfg.consumer_group,
            batch.len()
        );
    }
    let failed = matches!(raw_status, Some(ConsumeConcurrentlyStatus::ReconsumeLater))
        || raw_status.is_none();
    let succeeded = matches!(raw_status, Some(ConsumeConcurrentlyStatus::ConsumeSuccess));
    // Hooks and statistics still see the **raw** return value (None must record RETURNNULL); only the ackIndex decision is normalized.
    let status = raw_status.unwrap_or(ConsumeConcurrentlyStatus::ReconsumeLater);
    let batch_len = i32::try_from(batch.len()).unwrap_or(i32::MAX);
    let mut ack_index = context.ack_index;
    if status == ConsumeConcurrentlyStatus::ConsumeSuccess {
        if ack_index >= batch_len {
            ack_index = batch_len - 1;
        }
    } else {
        ack_index = -1;
    }
    let acked = if ack_index >= 0 {
        usize::try_from(ack_index).unwrap_or(0) + 1
    } else {
        0
    };
    record_consume_stats(inner, &mq.topic, batch.len(), begin_ms, failed, Some(acked));
    if let Some(ctx) = hook_ctx.as_mut() {
        finish_consume_hook(
            inner,
            ctx,
            raw_status.map(|s| s.name()),
            status.name(),
            has_exception,
            begin_ms,
            failed,
            succeeded,
        );
    }
    if broadcast {
        let dropped = batch.len() - acked;
        if dropped > 0 {
            rmq_warn!(
                "BROADCASTING, the message consume failed, drop it: {dropped} msgs in {mq:?}"
            );
        }
        advance_consume_offset(inner, key, batch, None, epoch);
        return Ok(true);
    }
    if acked >= batch.len() {
        // Whole batch acknowledged (the default path): nothing to send back, the offset advances directly
        advance_consume_offset(inner, key, batch, None, epoch);
        return Ok(true);
    }
    // Cluster mode: the unacknowledged [acked..) are sent back one by one to %RETRY%topic (delay level 3+reconsumeTimes;
    // beyond the limit the broker moves them to %DLQ%)
    let msg_back_failed = send_back_batch(
        inner,
        key,
        &batch[acked..],
        acked,
        context.delay_level_when_next_consume,
    )
    .await;
    if !msg_back_failed.is_empty() {
        {
            let mut state = lock(&inner.state);
            if let Some(dq) = state.pending.get_mut(key) {
                for (_, m) in msg_back_failed.iter().rev() {
                    dq.push_front(m.clone());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let floor = msg_back_failed.iter().map(|(_, m)| m.queue_offset).min();
    let handled: Vec<MessageExt> = batch
        .iter()
        .enumerate()
        .filter(|(i, _)| !msg_back_failed.iter().any(|(fi, _)| fi == i))
        .map(|(_, m)| m.clone())
        .collect();
    advance_consume_offset(inner, key, &handled, floor, epoch);
    Ok(msg_back_failed.is_empty())
}

async fn send_back_batch(
    inner: &Arc<Inner>,
    key: &str,
    batch: &[MessageExt],
    base: usize,
    delay_level_from_context: i32,
) -> Vec<(usize, MessageExt)> {
    let mut failed: Vec<(usize, MessageExt)> = Vec::new();
    for (i, msg) in batch.iter().enumerate() {
        {
            let state = lock(&inner.state);
            if !process_queue_contains_locked(&state, key, msg.queue_offset) {
                rmq_info!(
                    "Message is not found in its process queue; skip send-back-procedure, \
                     topic={}, brokerName={}, queueId={}, queueOffset={}",
                    msg.topic,
                    msg.broker_name.as_deref().unwrap_or(""),
                    msg.queue_id,
                    msg.queue_offset
                );
                continue;
            }
        }
        let mut delay_level = delay_level_from_context;
        if delay_level == 0 {
            delay_level = 3 + msg.reconsume_times;
        }
        let mut msg = msg.clone();
        if let Err(e) = send_message_back(inner, &msg, delay_level, None).await {
            rmq_debug!("send message back failed: {e}");
            msg.reconsume_times += 1;
            failed.push((base + i, msg));
        }
    }
    failed
}

/// Take this batch's maximum queueOffset + 1, and **never move backwards**.
/// `floor` is "the offset that must not be crossed" (the smallest offset among the messages pushed back to the head of the queue after send-back failed).
fn advance_consume_offset(
    inner: &Inner,
    key: &str,
    batch: &[MessageExt],
    floor: Option<i64>,
    epoch: Option<u64>,
) {
    let mut state = lock(&inner.state);
    if epoch.is_some_and(|e| e != state.queue_epoch.get(key).copied().unwrap_or(0)) {
        rmq_debug!("drop ack for {key}: process queue was dropped");
        return;
    }
    if state.frozen_offsets.contains(key) {
        return;
    }
    if batch.is_empty() {
        // When every message of the batch failed to send back, no entry was acknowledged, so the offset stays put
        return;
    }
    let mut next_off = batch.iter().map(|m| m.queue_offset).max().unwrap_or(0) + 1;
    if let Some(floor) = floor {
        next_off = next_off.min(floor);
    }
    let cur = state.consume_offsets.get(key).copied().unwrap_or(0);
    state
        .consume_offsets
        .insert(key.to_string(), cur.max(next_off));
    state
        .last_consume_at
        .insert(key.to_string(), current_time_millis());
}

fn requeue_pending(inner: &Inner, key: &str, batch: &[MessageExt]) {
    let mut state = lock(&inner.state);
    if let Some(dq) = state.pending.get_mut(key) {
        for m in batch.iter().rev() {
            dq.push_front(m.clone());
        }
    }
}


/// Register this batch into the "in the listener's hands" view (must pair with [`deregister_inflight_msgs`]).
fn register_inflight_msgs(inner: &Inner, key: &str, batch: &[MessageExt]) {
    if batch.is_empty() {
        return;
    }
    let mut state = lock(&inner.state);
    state
        .inflight_msgs
        .entry(key.to_string())
        .or_default()
        .extend(batch.iter().cloned());
}

/// Remove this batch of messages from the "in the listener's hands" view (by offset, idempotent).
fn deregister_inflight_msgs(inner: &Inner, key: &str, batch: &[MessageExt]) {
    if batch.is_empty() {
        return;
    }
    let offsets: std::collections::BTreeSet<i64> = batch.iter().map(|m| m.queue_offset).collect();
    let mut state = lock(&inner.state);
    let Some(bucket) = state.inflight_msgs.get_mut(key) else {
        return;
    };
    bucket.retain(|m| !offsets.contains(&m.queue_offset));
    if bucket.is_empty() {
        state.inflight_msgs.remove(key);
    }
}

/// The paired operations (register/remove, push back into the buffer) both complete inside the `state` lock, so at any instant a message
/// appears on only one side; the extra dedup by offset here is purely defensive against future call sites producing overlap.
fn process_queue_entries_locked(state: &State, key: &str) -> Vec<MessageExt> {
    let mut seen: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    let mut out: Vec<MessageExt> = Vec::new();
    for m in state
        .inflight_msgs
        .get(key)
        .into_iter()
        .flatten()
        .chain(state.pending.get(key).into_iter().flatten())
    {
        if seen.insert(m.queue_offset) {
            out.push(m.clone());
        }
    }
    out
}

fn process_queue_first_entry_locked(state: &State, key: &str) -> Option<MessageExt> {
    process_queue_entries_locked(state, key)
        .into_iter()
        .min_by_key(|m| m.queue_offset)
}

/// Anything swept by cleanExpiredMsg does not count (it is already on its way back to the broker).
fn process_queue_contains_locked(state: &State, key: &str, queue_offset: i64) -> bool {
    state
        .inflight_msgs
        .get(key)
        .into_iter()
        .flatten()
        .chain(state.pending.get(key).into_iter().flatten())
        .any(|m| m.queue_offset == queue_offset)
}

/// Remove the entry just swept (the caller has confirmed it is the queue head). `state` lock held.
///
/// Removed from both sides: normally the entry sits on only one of them; between the concurrent
/// "failed redelivery requeued into the buffer" and "finish-time deregistration" paths there is a
/// moment when it is on both, so dropping it from both places is what keeps the next sweep from seeing it again. The buffer side is scanned by offset, not head-only -- entries requeued after a failed redelivery arrive via `push_front`, so the buffer is not necessarily ordered strictly by offset.
fn remove_process_queue_entry_locked(state: &mut State, key: &str, queue_offset: i64) {
    if let Some(bucket) = state.inflight_msgs.get_mut(key) {
        bucket.retain(|m| m.queue_offset != queue_offset);
        if bucket.is_empty() {
            state.inflight_msgs.remove(key);
        }
    }
    if let Some(dq) = state.pending.get_mut(key) {
        dq.retain(|m| m.queue_offset != queue_offset);
    }
}

/// The "is still the head" question cannot be skipped: if the listener happens to finish normally
/// during this window (the message already removed by `deregister_inflight_msgs`), or a smaller
/// offset surfaces ahead (an entry requeued after a failed redelivery), both must yield to the normal path -- a preemptive removal would erase from the view an entry that should not have been touched. `state` lock held.
fn remove_expired_entry_if_still_head(state: &mut State, key: &str, queue_offset: i64) -> bool {
    let still_head = process_queue_first_entry_locked(state, key)
        .is_some_and(|m| m.queue_offset == queue_offset);
    if still_head {
        remove_process_queue_entry_locked(state, key, queue_offset);
    }
    still_head
}

fn consume_start_timestamp(msg: &MessageExt) -> Option<i64> {
    let raw = msg.get_property(PROPERTY_CONSUME_START_TIMESTAMP)?;
    if raw.is_empty() {
        return None;
    }
    raw.parse::<i64>().ok()
}

fn is_consume_expired(stamp: Option<i64>, now_ms: i64, consume_timeout_minutes: i64) -> bool {
    match stamp {
        Some(ts) => now_ms - ts > consume_timeout_minutes * 60 * 1000,
        None => false,
    }
}

fn clean_expire_period_millis(consume_timeout_minutes: i64) -> u64 {
    let minutes = consume_timeout_minutes.max(1);
    u64::try_from(minutes).unwrap_or(u64::MAX / 60_000) * 60_000
}

/// The semantic trio: inspect only the head (smallest offset), judge one message strictly greater than consumeTimeout, at most 16 messages per round.
async fn clean_expired_queue(inner: &Arc<Inner>, key: &str) -> usize {
    if lock(&inner.listener)
        .as_ref()
        .is_some_and(MessageListener::is_orderly)
    {
        return 0;
    }
    let cfg = read_cfg(inner);
    let loop_n = {
        let state = lock(&inner.state);
        process_queue_entries_locked(&state, key).len().min(16)
    };
    let mut attempts = 0usize;
    for _ in 0..loop_n {
        let head = {
            let state = lock(&inner.state);
            match process_queue_first_entry_locked(&state, key) {
                Some(m) => m,
                None => break,
            }
        };
        if !is_consume_expired(
            consume_start_timestamp(&head),
            current_time_millis(),
            cfg.consume_timeout,
        ) {
            break;
        }
        attempts += 1;
        if let Err(e) = send_message_back(inner, &head, 3, None).await {
            rmq_error!("send expired msg exception: {e}");
            continue;
        }
        rmq_info!(
            "send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}",
            head.topic,
            head.msg_id.as_deref().unwrap_or(""),
            head.get_store_host().unwrap_or(""),
            head.queue_id,
            head.queue_offset
        );
        let mut state = lock(&inner.state);
        remove_expired_entry_if_still_head(&mut state, key, head.queue_offset);
    }
    attempts
}

async fn clean_expired_msg_once(inner: &Arc<Inner>) -> usize {
    let keys: Vec<String> = lock(&inner.state).mq_map.keys().cloned().collect();
    let mut attempts = 0;
    for key in keys {
        attempts += clean_expired_queue(inner, &key).await;
    }
    attempts
}

async fn clean_expire_loop(inner: Arc<Inner>, mut rx: watch::Receiver<bool>) {
    // initialDelay equals the period (both are consumeTimeout minutes), so the first sweep also waits
    // out a full cycle -- sweeping immediately would walk over every message that just started consuming and has not expired yet (harmless, but unnecessary).
    let period = clean_expire_period_millis(read_cfg(&inner).consume_timeout);
    if wait_or_stop(&mut rx, period).await {
        return;
    }
    loop {
        if stopped(&rx) || !inner.started.load(Ordering::Acquire) {
            return;
        }
        clean_expired_msg_once(&inner).await;
        if wait_or_stop(&mut rx, period).await {
            return;
        }
    }
}

/// When the pull response is `NO_NEW_MSG` (the queue truly holds no messages) or `NO_MATCHED_MSG`
/// (the broker ran one pass over the subscription expression and matched nothing; this crate maps
/// the `PULL_RETRY_IMMEDIATELY` response code to that status in `client/mq_client.rs`), this queue's
/// consumed offset must follow the pull cursor, otherwise it **stalls forever**: messages nobody acked
/// belong to nobody (those filtered out broker-side are absent from the response, and those the client's second tag filter dropped are explicitly not acked), so an unmoved offset never advances and redelivery/restart scans this unwanted batch from its beginning.
fn correct_tags_offset_locked(state: &mut State, key: &str, status: PullStatus, next_off: i64) {
    if !matches!(status, PullStatus::NoNewMsg | PullStatus::NoMatchedMsg) {
        return;
    }
    if state.frozen_offsets.contains(key) {
        return;
    }
    if !state.pending.get(key).map_or(true, |dq| dq.is_empty()) {
        return;
    }
    if state.inflight.get(key).copied().unwrap_or(0) != 0 {
        return;
    }
    match state.consume_offsets.get(key).copied() {
        None => {
            state.consume_offsets.insert(key.to_string(), next_off);
        }
        Some(cur) if next_off > cur => {
            state.consume_offsets.insert(key.to_string(), next_off);
        }
        Some(_) => {}
    }
}

fn orderly_suspend_millis(cfg: &ConsumerConfig, ctx: &ConsumeOrderlyContext) -> u64 {
    let mut ms = ctx.suspend_current_queue_time_millis;
    if ms == -1 {
        ms = cfg.suspend_current_queue_time_millis;
    }
    ms.clamp(10, 30000) as u64
}

/// Return the message to the broker's retry queue (CONSUMER_SEND_MSG_BACK=36). On failure the
/// message degrades to an ordinary send to `%RETRY%group` (the broker then redelivers it by delay
/// level); when that fails too, the original send-back error is returned.
async fn send_message_back(
    inner: &Arc<Inner>,
    msg: &MessageExt,
    delay_level: i32,
    broker_name: Option<&str>,
) -> Result<()> {
    let client = require_client_for_send_back(inner)?;
    let cfg = read_cfg(inner);
    let broker = broker_name
        .map(str::to_string)
        .or_else(|| msg.broker_name.clone())
        .unwrap_or_default();
    let max_reconsume = if cfg.max_reconsume_times == -1 {
        16
    } else {
        cfg.max_reconsume_times
    };
    let primary: Result<()> = match client.find_broker_address_in_publish(&broker) {
        Some(addr) => {
            client
                .consumer_send_msg_back(
                    &cfg.consumer_group,
                    msg,
                    delay_level,
                    Some(max_reconsume),
                    3_000,
                    &addr,
                    cfg.unit_mode,
                )
                .await
        }
        None => Err(Error::client(format!(
            "Broker[{broker}] master node does not exist"
        ))),
    };
    match primary {
        Ok(()) => Ok(()),
        Err(original) => {
            rmq_error!(
                "Failed to send message back, consumerGroup={}, brokerName={}, msg={}, fallback to normal send: {}",
                cfg.consumer_group,
                broker,
                msg.msg_id.as_deref().unwrap_or(""),
                original
            );
            match send_retry_copy(
                &client,
                &cfg.consumer_group,
                msg,
                max_reconsume,
                cfg.unit_mode,
            )
            .await
            {
                Ok(()) => Ok(()),
                Err(fallback_error) => {
                    rmq_error!(
                        "Failed to send message back as normal message, consumerGroup={}, msg={}: {}",
                        cfg.consumer_group,
                        msg.msg_id.as_deref().unwrap_or(""),
                        fallback_error
                    );
                    Err(original)
                }
            }
        }
    }
}

/// The ordinary-send leg a failed send-back degrades to: the retry copy goes out through the
/// internal producer to `%RETRY%group` and the broker redelivers it by its delay level.
pub(crate) async fn send_retry_copy(
    client: &MQClientInstance,
    consumer_group: &str,
    msg: &MessageExt,
    max_reconsume_times: i32,
    unit_mode: bool,
) -> Result<()> {
    let mut new_msg = build_retry_message(consumer_group, msg, max_reconsume_times);
    let publish = client.get_topic_publish_info(&new_msg.topic, true).await?;
    let mq = publish
        .select_one_message_queue(&[])?
        .ok_or_else(|| Error::client(format!("no writable queue for {}", new_msg.topic)))?;
    client
        .send_message(
            MixAll::CLIENT_INNER_PRODUCER_GROUP,
            &mut PublishMessage::Single(&mut new_msg),
            &mq,
            3000,
            0,
            unit_mode,
            MixAll::DEFAULT_TOPIC,
            MixAll::DEFAULT_TOPIC_QUEUE_NUMS,
        )
        .await?;
    Ok(())
}

/// Orderly-consumed messages keep retrying in place in the local queue and the broker never counts redeliveries,
/// so the default must retry until success -- `-1` reads as `i32::MAX` here. **Not** the 16 of the concurrent
/// send-back chain ([`send_message_back`], which passes the broker every round; 16 is the broker default retryMaxTimes):
/// merging the two into one constant would either invent dead letters for orderly consumption or let concurrent messages redeliver forever.
fn orderly_max_reconsume_times(cfg: &ConsumerConfig) -> i32 {
    if cfg.max_reconsume_times == -1 {
        i32::MAX
    } else {
        cfg.max_reconsume_times
    }
}

async fn check_orderly_reconsume_times(inner: &Arc<Inner>, batch: &mut [MessageExt]) -> bool {
    let cfg = read_cfg(inner);
    let max_times = orderly_max_reconsume_times(&cfg);
    let mut suspend = false;
    for msg in batch {
        if msg.reconsume_times >= max_times {
            if !orderly_send_message_back(inner, msg, &cfg, max_times).await {
                suspend = true;
                msg.reconsume_times += 1;
            }
        } else {
            suspend = true;
            msg.reconsume_times += 1;
        }
    }
    suspend
}

/// On failure only returns false and never panics: see [`check_orderly_reconsume_times`].
async fn orderly_send_message_back(
    inner: &Arc<Inner>,
    msg: &MessageExt,
    cfg: &ConsumerConfig,
    max_times: i32,
) -> bool {
    let outcome: Result<()> = async {
        let client = require_client_for_send_back(inner)?;
        let mut new_msg = build_retry_message(&cfg.consumer_group, msg, max_times);
        let publish = client.get_topic_publish_info(&new_msg.topic, true).await?;
        let mq = publish
            .select_one_message_queue(&[])?
            .ok_or_else(|| Error::client(format!("no writable queue for {}", new_msg.topic)))?;
        client
            .send_message(
                MixAll::CLIENT_INNER_PRODUCER_GROUP,
                &mut PublishMessage::Single(&mut new_msg),
                &mq,
                3000,
                0,
                cfg.unit_mode,
                MixAll::DEFAULT_TOPIC,
                MixAll::DEFAULT_TOPIC_QUEUE_NUMS,
            )
            .await?;
        Ok(())
    }
    .await;
    match outcome {
        Ok(()) => true,
        Err(e) => {
            rmq_debug!(
                "orderly send message back failed, group={}: {e}",
                read_cfg(inner).consumer_group
            );
            false
        }
    }
}

fn build_retry_message(consumer_group: &str, msg: &MessageExt, max_times: i32) -> Message {
    let mut new_msg = Message::new(
        MixAll::get_retry_topic(consumer_group).as_str(),
        msg.body.as_deref(),
    );
    new_msg.properties = msg.properties.clone();
    new_msg.flag = msg.flag;
    let origin_msg_id = msg
        .get_property(PROPERTY_ORIGIN_MESSAGE_ID)
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .or_else(|| msg.msg_id.clone())
        .unwrap_or_default();
    if !origin_msg_id.is_empty() {
        new_msg.put_property(PROPERTY_ORIGIN_MESSAGE_ID, &origin_msg_id);
    }
    new_msg.put_property(PROPERTY_RETRY_TOPIC, &msg.topic);
    new_msg.put_property(
        PROPERTY_RECONSUME_TIME,
        &(msg.reconsume_times + 1).to_string(),
    );
    new_msg.put_property(PROPERTY_MAX_RECONSUME_TIMES, &max_times.to_string());
    // The half-message flag must be removed, or the broker would treat it as a transaction check-back message again
    new_msg.properties.remove(PROPERTY_TRANSACTION_PREPARED);
    new_msg.set_delay_time_level(3 + msg.reconsume_times);
    new_msg
}

// ================================================================ broker-initiated requests
//
// The handlers for 220/221/307/309 are registered on MQClientInstance (which looks up the
// **matching** consumer by consumerGroup); only the callback side is implemented here. 40 is the only one
// the consumer registers itself, because its action is just "wake this consumer's rebalance loop".

/// Host side of the trace dispatcher:
/// the `client_id` the dispatcher reads is the host's held instance `client_id`, an empty string when not started.
impl TraceHost for DefaultMQPushConsumer {
    fn client_id(&self) -> String {
        DefaultMQPushConsumer::client_id(self)
    }
}

/// Broker notifies that the consumer group's instances changed -> recompute immediately.
impl RegisteredConsumer for DefaultMQPushConsumer {
    fn client_id(&self) -> String {
        self.client_id()
    }

    fn consumer_group(&self) -> String {
        self.consumer_group()
    }

    fn consume_type(&self) -> String {
        ConsumeType::CONSUME_PASSIVELY.to_string()
    }

    fn message_model(&self) -> String {
        self.config().message_model
    }

    fn consume_from_where(&self) -> String {
        self.config().consume_from_where
    }

    fn is_unit_mode(&self) -> bool {
        self.config().unit_mode
    }

    fn subscription(&self) -> Vec<String> {
        lock(&self.inner.state)
            .subscription_data
            .iter()
            .map(|(k, _)| k.clone())
            .collect()
    }

    fn subscriptions(&self) -> Vec<SubscriptionData> {
        self.subscriptions()
    }

    fn rebalance_immediately(&self) {
        self.inner.rebalance_now.store(true, Ordering::SeqCst);
        self.inner.rebalance_signal.notify_waiters();
    }

    fn adjust_thread_pool(&self) {
        let acc_total = self.compute_accumulation_total();
        let threshold = self.config().adjust_thread_pool_nums_threshold;
        if acc_total >= threshold {
            rmq_debug!("adjustThreadPool: acc={acc_total} >= incThreshold={threshold} (inc is a no-op upstream)");
        }
        if acc_total < threshold * 4 / 5 {
            rmq_debug!(
                "adjustThreadPool: acc={acc_total} < decThreshold={}/0.8 (dec is a no-op upstream)",
                threshold
            );
        }
    }

    fn reset_offset(
        self: Arc<Self>,
        topic: String,
        offset_table: Vec<(MessageQueue, i64)>,
    ) -> ConsumerFuture<()> {
        Box::pin(async move {
            if topic.is_empty() || offset_table.is_empty() {
                return Ok(());
            }
            let pop = self.config().pop_mode;
            let mut revoked: Vec<(MessageQueue, Option<i64>)> = Vec::new();
            {
                // Collect keys first, then revoke one by one: `retire_queue_locked` erases `mq_map` entries
                let mut state = lock(&self.inner.state);
                let hits: Vec<(String, MessageQueue, i64)> = state
                    .mq_map
                    .iter()
                    .filter(|(_, mq)| mq.topic == topic)
                    .filter_map(|(key, mq)| {
                        offset_table
                            .iter()
                            .find(|(m, _)| m == mq)
                            .map(|(_, off)| (key.clone(), mq.clone(), *off))
                    })
                    .collect();
                for (key, mq, off) in hits {
                    state.consume_offsets.insert(key.clone(), off);
                    DefaultMQPushConsumer::retire_queue_locked(
                        &mut state,
                        &key,
                        Some(&mq),
                        &mut revoked,
                        pop,
                    );
                }
            }
            if revoked.is_empty() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            self.on_queues_revoked(&revoked).await;
            if let Err(e) = self.do_rebalance().await {
                rmq_debug!("rebalance after reset offset failed: {e}");
            }
            rmq_info!(
                "reset offset applied, group={} topic={topic} queues={}",
                self.consumer_group(),
                revoked.len()
            );
            Ok(())
        })
    }

    fn get_consumer_status(&self, topic: Option<&str>) -> Vec<(MessageQueue, i64)> {
        let state = lock(&self.inner.state);
        let mut out: Vec<(MessageQueue, i64)> = state
            .consume_offsets
            .iter()
            .filter_map(|(k, off)| {
                let mq = state.mq_map.get(k)?;
                // A blank topic is not a filter: the caller asked for everything
                // this consumer holds, so an empty string must not read as "match
                // a topic literally named empty" and report nothing.
                if let Some(topic) = topic.filter(|t| !t.is_empty()) {
                    if mq.topic != topic {
                        return None;
                    }
                }
                Some((mq.clone(), *off))
            })
            .collect();
        out.sort_by_key(|(mq, _)| mq_sort_key(mq));
        out
    }

    fn consumer_running_info(&self) -> ConsumerRunningInfo {
        let cfg = self.config();
        let mut properties = StringMap::new();
        properties.insert(
            ConsumerRunningInfo::PROP_NAMESERVER_ADDR,
            format!("{};", cfg.name_server_addrs.join(";")),
        );
        properties.insert(
            ConsumerRunningInfo::PROP_CONSUME_TYPE,
            ConsumeType::CONSUME_PASSIVELY,
        );
        let orderly = lock(&self.inner.listener)
            .as_ref()
            .is_some_and(MessageListener::is_orderly);
        properties.insert(
            ConsumerRunningInfo::PROP_CONSUME_ORDERLY,
            orderly.to_string(),
        );
        properties.insert(
            ConsumerRunningInfo::PROP_THREADPOOL_CORE_SIZE,
            self.get_core_pool_size().to_string(),
        );
        properties.insert(
            ConsumerRunningInfo::PROP_CONSUMER_START_TIMESTAMP,
            self.inner
                .start_time_millis
                .load(Ordering::SeqCst)
                .to_string(),
        );
        properties.insert(ConsumerRunningInfo::PROP_CLIENT_VERSION, "V5_5_1");
        let mut info = ConsumerRunningInfo {
            properties,
            ..Default::default()
        };
        let state = lock(&self.inner.state);
        let now = current_time_millis();
        for (key, mq) in &state.mq_map {
            if cfg.pop_mode && state.pop_queues.contains_key(key) {
                continue;
            }
            let cached: Vec<&MessageExt> = state
                .pending
                .get(key)
                .into_iter()
                .flatten()
                .chain(state.inflight_msgs.get(key).into_iter().flatten())
                .collect();
            let mut cached_min = 0i64;
            let mut cached_max = 0i64;
            let mut transaction_min = 0i64;
            let mut transaction_max = 0i64;
            let mut transaction_count = 0i32;
            let mut body_bytes = 0usize;
            for (index, msg) in cached.iter().enumerate() {
                if index == 0 || msg.queue_offset < cached_min {
                    cached_min = msg.queue_offset;
                }
                if index == 0 || msg.queue_offset > cached_max {
                    cached_max = msg.queue_offset;
                }
                body_bytes += msg.body.as_ref().map_or(0, |body| body.len());
                if MessageSysFlag::get_transaction_value(msg.sys_flag)
                    == MessageSysFlag::TRANSACTION_PREPARED_TYPE
                {
                    if transaction_count == 0 || msg.queue_offset < transaction_min {
                        transaction_min = msg.queue_offset;
                    }
                    if transaction_count == 0 || msg.queue_offset > transaction_max {
                        transaction_max = msg.queue_offset;
                    }
                    transaction_count += 1;
                }
            }
            let locked = state.lock_ok.contains(key)
                && state
                    .lock_at
                    .get(key)
                    .is_some_and(|at| now - *at <= REBALANCE_LOCK_MAX_LIVE_TIME);
            let pqi = ProcessQueueInfo {
                commit_offset: state.consume_offsets.get(key).copied().unwrap_or(-1),
                cached_msg_min_offset: cached_min,
                cached_msg_max_offset: cached_max,
                cached_msg_count: i32::try_from(cached.len()).unwrap_or(i32::MAX),
                cached_msg_size_in_mib: i32::try_from(body_bytes / (1024 * 1024)).unwrap_or(i32::MAX),
                transaction_msg_min_offset: transaction_min,
                transaction_msg_max_offset: transaction_max,
                transaction_msg_count: transaction_count,
                locked,
                try_unlock_times: 0,
                last_lock_timestamp: state.lock_at.get(key).copied().unwrap_or(0),
                droped: false,
                last_pull_timestamp: state.last_pull_at.get(key).copied().unwrap_or(0),
                last_consume_timestamp: state.last_consume_at.get(key).copied().unwrap_or(0),
            };
            info.mq_table.push((
                MessageQueueKey::new(&mq.topic, &mq.broker_name, mq.queue_id),
                pqi.to_json_value(),
            ));
        }
        if cfg.pop_mode {
            let mut mq_of: BTreeMap<String, &MessageQueue> = BTreeMap::new();
            for mq in &state.assigned {
                mq_of.insert(mq_key(mq), mq);
            }
            for (key, mq) in &state.mq_map {
                mq_of.insert(key.clone(), mq);
            }
            for (key, pq) in &state.pop_queues {
                let Some(mq) = mq_of.get(key) else {
                    continue;
                };
                let pqi = serde_json::json!({
                    "waitAckCount": pq.wait_ack_count(),
                    "droped": pq.is_dropped(),
                    "lastPopTimestamp": pq.last_pop_timestamp.load(Ordering::SeqCst),
                });
                info.mq_pop_table.push((
                    MessageQueueKey::new(&mq.topic, &mq.broker_name, mq.queue_id),
                    pqi,
                ));
            }
        }
        info.subscription_set = state
            .subscription_data
            .iter()
            .map(|(_, sub)| sub.to_json_value())
            .collect();
        let stats = lock(&self.inner.stats).clone();
        for (topic, _) in &state.subscription_data {
            let status = match &stats {
                Some(stats) => stats.consume_status(&cfg.consumer_group, topic),
                None => crate::remoting::protocol::body::ConsumeStatus::default(),
            };
            info.status_table
                .push((topic.clone(), status.to_json_value()));
        }
        info
    }

    fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<String>,
    ) -> Result<ConsumeMessageDirectlyResult> {
        let mut result = ConsumeMessageDirectlyResult::default();
        let mq = MessageQueue::new(
            &msg.topic,
            broker_name.as_deref().unwrap_or(""),
            msg.queue_id,
        );
        let mut msgs = vec![msg];
        reset_retry_topic_and_namespace(&self.inner, &mut msgs);
        let mut context = ConsumeConcurrentlyContext::new(Some(mq.clone()));
        let begin = current_time_millis();
        // ⚠ This is a **synchronous** interface (`RegisteredConsumer::consume_message_directly` is not
        // async), so unlike the normal consume path there is no `spawn_blocking`; the listener can only be
        // called directly on the remoting read thread. The cost: user code on this path occupies the read thread.
        let status = match lock(&self.inner.listener).as_ref().cloned() {
            Some(MessageListener::Concurrently(listener)) => {
                result.order = false;
                match listener.consume_message(&msgs, &mut context) {
                    ConsumeConcurrentlyStatus::ConsumeSuccess => CMResult::CR_SUCCESS,
                    ConsumeConcurrentlyStatus::ReconsumeLater => CMResult::CR_LATER,
                }
            }
            Some(MessageListener::Orderly(listener)) => {
                let mut octx = ConsumeOrderlyContext::new(Some(mq));
                let status = listener.consume_message(&msgs, &mut octx);
                result.order = true;
                result.auto_commit = octx.auto_commit;
                match status {
                    ConsumeOrderlyStatus::Success => CMResult::CR_SUCCESS,
                    ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => CMResult::CR_LATER,
                    ConsumeOrderlyStatus::Commit => CMResult::CR_COMMIT,
                    ConsumeOrderlyStatus::Rollback => CMResult::CR_ROLLBACK,
                }
            }
            // No listener -> status None -> CR_RETURN_NULL
            None => CMResult::CR_RETURN_NULL,
        };
        result.consume_result = Some(status.to_string());
        result.spent_time_mills = current_time_millis() - begin;
        let _ = msgs;
        Ok(result)
    }

    fn persist_consumer_offset(self: Arc<Self>) -> ConsumerFuture<()> {
        Box::pin(async move {
            if let Ok(client) = require_client(&self.inner) {
                persist_offsets_once(&self.inner, &client).await;
            }
            Ok(())
        })
    }
}

// ================================================================ public capabilities and resilience

impl DefaultMQPushConsumer {
    /// Consumer group name (alias kept under the historical naming).
    pub fn get_consumer_group(&self) -> String {
        self.consumer_group()
    }

    /// Fetch the current queue list of a subscribed topic.
    pub async fn fetch_subscribe_message_queues(&self, topic: &str) -> Result<Vec<MessageQueue>> {
        let client = require_client(&self.inner)?;
        Ok(client.get_topic_subscribe_info(topic).await)
    }

    /// With a key, read the single-queue value; without, sum all queues.
    pub fn msg_acc_cnt(&self, key: Option<&str>) -> i64 {
        let state = lock(&self.inner.state);
        match key {
            None => state.msg_acc_cnt.values().sum(),
            Some(key) => state.msg_acc_cnt.get(key).copied().unwrap_or(0),
        }
    }

    /// Sum of `msgAccCnt` over all ProcessQueues.
    pub fn compute_accumulation_total(&self) -> i64 {
        self.msg_acc_cnt(None)
    }


    pub fn suspend(&self) {
        self.inner.pause.store(true, Ordering::SeqCst);
        rmq_info!("suspend this consumer, {}", self.consumer_group());
    }

    pub fn resume(&self) {
        self.inner.pause.store(false, Ordering::SeqCst);
        self.rebalance_immediately();
        rmq_info!("resume this consumer, {}", self.consumer_group());
    }

    pub fn is_pause(&self) -> bool {
        self.inner.pause.load(Ordering::SeqCst)
    }


    pub async fn send_message_back(&self, msg: &MessageExt, delay_level: i32) -> Result<()> {
        send_message_back(&self.inner, msg, delay_level, None).await
    }

    pub async fn send_message_back_to_broker(
        &self,
        msg: &MessageExt,
        delay_level: i32,
        broker_name: &str,
    ) -> Result<()> {
        send_message_back(&self.inner, msg, delay_level, Some(broker_name)).await
    }

    pub fn update_core_pool_size(&self, core_pool_size: i32) -> bool {
        if !self.inner.owns_consume_executor.load(Ordering::SeqCst) {
            return false;
        }
        if !(0 < core_pool_size && core_pool_size <= SHORT_MAX_VALUE) {
            return false;
        }
        if core_pool_size >= self.config().consume_thread_max {
            return false;
        }
        self.inner
            .core_pool_size
            .store(core_pool_size, Ordering::SeqCst);
        self.apply_core_pool_size();
        true
    }

    /// Returns -1 when the executor is not owned by this consumer.
    pub fn get_core_pool_size(&self) -> i32 {
        if !self.inner.owns_consume_executor.load(Ordering::SeqCst) {
            return -1;
        }
        match lock(&self.inner.pop_executor).as_ref() {
            Some(executor) => executor.get_core_pool_size(),
            None => self.inner.core_pool_size.load(Ordering::SeqCst),
        }
    }

    /// Applies the declared value to the real executor (records the declared value only when there is none).
    fn apply_core_pool_size(&self) {
        let core = self.inner.core_pool_size.load(Ordering::SeqCst);
        if let Some(executor) = lock(&self.inner.pop_executor).as_ref() {
            if let Err(e) = executor.set_core_pool_size(core) {
                rmq_debug!("set_core_pool_size({core}) failed: {e}");
            }
        }
    }

    /// Total count buffered, pulled but not yet consumed (for live-cluster flow-control/backpressure verification).
    pub fn buffered_message_count(&self) -> usize {
        lock(&self.inner.state)
            .pending
            .values()
            .map(VecDeque::len)
            .sum()
    }

    /// Total count popped but not yet acked (POP mode, for live-cluster verification).
    pub fn wait_ack_count(&self) -> i32 {
        lock(&self.inner.state)
            .pop_queues
            .values()
            .map(|pq| pq.wait_ack_count())
            .sum()
    }

    /// Triggers one immediate rebalance (equivalent to the broker's 40 notification; for tests and manual ops triggers).
    pub fn rebalance_immediately(&self) {
        self.request_rebalance();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::result::PullResult;
    use crate::common::message::Message;
    use crate::common::message_const::PROPERTY_KEYS;
    use crate::remoting::protocol::heartbeat::ExpressionType;

    fn ext(topic: &str, tags: Option<&str>) -> MessageExt {
        let mut m = MessageExt::new();
        m.topic = topic.to_string();
        if let Some(t) = tags {
            let mut src = Message::new(topic, Some(b"x"));
            src.set_tags(t);
            m.properties = src.properties.clone();
        }
        m
    }

    fn sub(topic: &str, expression: &str) -> SubscriptionData {
        let mut s = SubscriptionData::new(topic, expression);
        if expression != "*" {
            s.tags_set = expression.split("||").map(str::to_string).collect();
        }
        s
    }

    /// The plan takes NO subscription argument at all: the start offset is decided by the
    /// policy and the queue name, never by TAG vs SQL92. A SQL92 subscription that
    /// short-circuits to the queue tail silently drops everything produced before the
    /// consumer came up.
    #[test]
    fn initial_offset_plan_never_takes_the_subscription_into_account() {
        assert_eq!(
            plan_initial_offset(-1, ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET, false),
            StartOffsetPlan::FromZero
        );
        assert_eq!(
            plan_initial_offset(-1, ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET, false),
            StartOffsetPlan::QueueTail
        );
    }

    #[test]
    fn initial_offset_plan_stored_offset_beats_every_policy() {
        for where_ in [
            ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET,
            ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET,
            ConsumeFromWhere::CONSUME_FROM_TIMESTAMP,
        ] {
            assert_eq!(
                plan_initial_offset(0, where_, false),
                StartOffsetPlan::Stored(0),
                "a stored 0 is a real position, not 'nothing stored'"
            );
            assert_eq!(
                plan_initial_offset(42, where_, true),
                StartOffsetPlan::Stored(42),
                "even a %RETRY% queue resumes where it left off"
            );
        }
    }

    #[test]
    fn initial_offset_plan_sends_every_retry_queue_to_zero() {
        assert_eq!(
            plan_initial_offset(-1, ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET, true),
            StartOffsetPlan::FromZero,
            "queued retries must all be delivered"
        );
        assert_eq!(
            plan_initial_offset(-1, ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET, true),
            StartOffsetPlan::FromZero
        );
    }

    #[test]
    fn initial_offset_plan_timestamp_searches_business_and_starts_retry_at_tail() {
        assert_eq!(
            plan_initial_offset(-1, ConsumeFromWhere::CONSUME_FROM_TIMESTAMP, false),
            StartOffsetPlan::SearchAtConfiguredTime
        );
        assert_eq!(
            plan_initial_offset(-1, ConsumeFromWhere::CONSUME_FROM_TIMESTAMP, true),
            StartOffsetPlan::QueueTail,
            "a retry queue has no meaningful 'time of the oldest retry'"
        );
    }

    #[test]
    fn namespace_v2_setter_getter_round_trip() {
        let c = DefaultMQPushConsumer::new("GID_nsv2").expect("a legal group name must not fail to construct");
        assert_eq!(c.get_namespace_v2(), None, "not set by default");
        c.set_namespace_v2(Some("NS_V2"));
        assert_eq!(c.get_namespace_v2(), Some("NS_V2".to_string()));
        assert_eq!(c.config().namespace_v2, Some("NS_V2".to_string()));
        c.set_namespace_v2(None);
        assert_eq!(
            c.get_namespace_v2(),
            None,
            "None = clears"
        );
    }

    #[test]
    fn sort_key_orders_topic_then_broker_then_queue() {
        let mut mqs = vec![
            MessageQueue::new("b", "broker-a", 1),
            MessageQueue::new("a", "broker-b", 0),
            MessageQueue::new("a", "broker-a", 3),
            MessageQueue::new("a", "broker-a", 0),
        ];
        sort_mqs(&mut mqs);
        let keys: Vec<(String, String, i32)> = mqs.iter().map(mq_sort_key).collect();
        assert_eq!(
            keys,
            vec![
                ("a".to_string(), "broker-a".to_string(), 0),
                ("a".to_string(), "broker-a".to_string(), 3),
                ("a".to_string(), "broker-b".to_string(), 0),
                ("b".to_string(), "broker-a".to_string(), 1),
            ]
        );
    }

    #[test]
    fn tag_filter_keeps_only_string_matches() {
        let msgs = vec![
            ext("T", Some("TagA")),
            ext("T", Some("TagB")),
            ext("T", None),
        ];
        let kept = client_side_tag_filter(Some(&sub("T", "TagA")), msgs.clone());
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].get_tags(), Some("TagA"));

        // Subscribe "*": empty tags_set => no filtering (even tag-less messages are kept).
        assert_eq!(
            client_side_tag_filter(Some(&sub("T", "*")), msgs.clone()).len(),
            3
        );

        let mut class_sub = sub("T", "TagA");
        class_sub.class_filter_mode = true;
        assert_eq!(
            client_side_tag_filter(Some(&class_sub), msgs.clone()).len(),
            3
        );

        // With no subscription info, return the input as-is.
        assert_eq!(client_side_tag_filter(None, msgs.clone()).len(), 3);
        assert!(client_side_tag_filter(Some(&sub("T", "TagA")), Vec::new()).is_empty());
    }

    #[test]
    fn delivery_filter_combines_tag_filter_and_hooks() {
        let hooks = Arc::new(crate::client::hook::FilterMessageHookList::new());
        let msgs = vec![ext("T", Some("TagA")), ext("T", Some("TagC"))];
        let out = filter_messages_for_delivery(
            "G",
            &hooks,
            &MessageQueue::new("T", "broker-a", 0),
            Some(&sub("T", "TagA")),
            msgs,
            false,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get_tags(), Some("TagA"));
    }

    #[test]
    fn selector_factories_match_expression_type() {
        assert_eq!(
            MessageSelector::by_tag("a||b"),
            MessageSelector {
                selector_type: ExpressionType::TAG.to_string(),
                expression: "a||b".to_string()
            }
        );
        let sql = MessageSelector::by_sql("a > 1");
        assert_eq!(sql.selector_type, ExpressionType::SQL92);
        assert_eq!(sql.sub_expression(), "a > 1");
    }

    /// Offline we can only prove "no longer rejected + all land in the live subscription table + unsubscribe sends no
    /// heartbeat": the immediate heartbeat is an async RPC (`notify_subscription_changed` fire-and-forget to the
    /// runtime) and sends no packets with zero brokers. For live wire comparison against the broker-side topicGroupTable see `examples/live_subscribe.rs`.
    #[tokio::test]
    async fn subscribe_is_allowed_after_start_and_lands_in_the_live_table() {
        let consumer = DefaultMQPushConsumer::new("GID_SubAfterStartUnit").unwrap();
        // Build a "started + has client" state; the client points at a zero-route instance and sends no packets.
        *lock(&consumer.inner.client) = Some(MQClientInstance::new("unit-sub-after-start", vec![]));
        consumer.inner.started.store(true, Ordering::Release);

        consumer.subscribe("T_A", "*").unwrap();
        consumer.subscribe_all("T_B").unwrap();
        consumer
            .subscribe_with_selector("T_C", &MessageSelector::by_sql("a > 1"))
            .unwrap();

        let topics: Vec<String> = consumer
            .subscriptions()
            .into_iter()
            .map(|s| s.topic)
            .collect();
        assert!(
            topics.contains(&"T_A".to_string()),
            "a post-start subscribe must land in the live subscription table"
        );
        assert!(topics.contains(&"T_B".to_string()));
        assert!(topics.contains(&"T_C".to_string()));

        let before = consumer.heartbeat_count();
        consumer.unsubscribe("T_B");
        assert_eq!(consumer.heartbeat_count(), before);
        assert!(!consumer.subscriptions().iter().any(|s| s.topic == "T_B"));
    }

    #[test]
    fn pop_process_queue_tracks_wait_ack() {
        let pq = PopProcessQueue::new();
        pq.inc_found_msg(3);
        assert_eq!(pq.wait_ack_count(), 3);
        assert_eq!(pq.ack(), 2);
        assert_eq!(pq.ack(), 1);
        // Semantics: dec_found_msg(positive) = decrease by that many messages.
        pq.dec_found_msg(1);
        assert_eq!(pq.wait_ack_count(), 0);

        assert!(!pq.is_dropped());
        pq.set_dropped(true);
        assert!(pq.is_dropped());

        pq.touch(1234);
        assert_eq!(pq.last_pop_timestamp.load(Ordering::SeqCst), 1234);
    }

    #[test]
    fn suspend_and_resume_toggle_pause_and_trigger_rebalance() {
        let c = DefaultMQPushConsumer::new("G_suspend").unwrap();
        assert!(!c.is_pause());
        c.suspend();
        assert!(c.is_pause());
        c.suspend();
        assert!(c.is_pause());
        c.resume();
        assert!(!c.is_pause());
        assert!(c.inner.rebalance_now.load(Ordering::SeqCst));
    }

    #[test]
    fn flow_control_constants_match_contract() {
        assert_eq!(PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL, 50);
        assert_eq!(PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL, 20);
        assert_eq!(PULL_TIME_DELAY_MILLS_WHEN_SUSPEND, 1000);
        assert_eq!(FLOW_CONTROL_WARN_EVERY, 1000);
        // The generic failed-round wait is NOT one of these constants any more: it is a knob, and
        // only its default lives here.
        assert_eq!(DEFAULT_PULL_EXCEPTION_BACKOFF_MILLIS, 1000);
        assert_eq!(
            ConsumerConfig::default().pull_exception_backoff_millis,
            DEFAULT_PULL_EXCEPTION_BACKOFF_MILLIS
        );
    }

    /// Parameter wiring of the public send-back: errors when not started; broker name comes from the message / explicit argument.
    #[tokio::test]
    async fn public_send_message_back_requires_started_client() {
        let c = DefaultMQPushConsumer::new("G_send_back_pub").unwrap();
        let msg = MessageExt::from_message(&Message::new("T", Some(b"x")));
        // Not started: require_client_for_send_back cannot get an instance
        let err = c.send_message_back(&msg, 3).await.unwrap_err();
        assert!(err.to_string().contains("not started"), "{err}");
        let err = c
            .send_message_back_to_broker(&msg, 3, "broker-a")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not started"), "{err}");
    }

    #[test]
    fn offset_table_sorted_by_queue_key() {
        let table = offset_table_to_sorted(vec![
            (MessageQueue::new("T", "broker-b", 0), 5),
            (MessageQueue::new("T", "broker-a", 1), 7),
            (MessageQueue::new("T", "broker-a", 0), 3),
        ]);
        let pairs: Vec<((String, String, i32), i64)> = table.into_iter().collect();
        assert_eq!(pairs[0].0, ("T".to_string(), "broker-a".to_string(), 0));
        assert_eq!(pairs[0].1, 3);
        assert_eq!(pairs[2].1, 5);
    }

    // ---------------- config and startup validation ----------------

    /// Each default checked item by item against the `DefaultMQPushConsumer` construction contract.
    #[test]
    fn config_defaults_match_the_golden_reference() {
        let cfg = ConsumerConfig::default();
        assert_eq!(cfg.consumer_group, MixAll::DEFAULT_CONSUMER_GROUP);
        assert_eq!(cfg.instance_name, "DEFAULT");
        assert_eq!(cfg.client_id, None);
        assert!(cfg.name_server_addrs.is_empty());
        assert_eq!(cfg.tls_enable, None);
        assert_eq!(cfg.message_model, MessageModel::CLUSTERING);
        assert_eq!(
            cfg.consume_from_where,
            ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET
        );
        assert_eq!((cfg.consume_thread_min, cfg.consume_thread_max), (20, 20));
        assert_eq!(cfg.adjust_thread_pool_nums_threshold, 100_000);
        assert_eq!(cfg.consume_concurrently_max_span, 2000);
        assert_eq!(
            (
                cfg.pull_threshold_for_queue,
                cfg.pull_threshold_size_for_queue
            ),
            (1000, 100)
        );
        assert_eq!(
            (
                cfg.pull_threshold_for_topic,
                cfg.pull_threshold_size_for_topic
            ),
            (-1, -1)
        );
        assert_eq!(cfg.pull_interval, 0);
        // The one generic failed-round backoff: a knob, defaulting to 1000 ms.
        assert_eq!(cfg.pull_exception_backoff_millis, 1000);
        assert_eq!(
            (cfg.pull_timeout_millis, cfg.pull_suspend_timeout_millis),
            (30_000, 20_000)
        );
        assert_eq!(cfg.consume_message_batch_max_size, 1);
        assert_eq!(cfg.pull_batch_size, 32);
        assert_eq!(cfg.pull_batch_size_in_bytes, 256 * 1024);
        assert_eq!(cfg.max_reconsume_times, -1);
        assert_eq!(cfg.suspend_current_queue_time_millis, 1000);
        // consumeTimeout's unit is **minutes** (only used for ConsumeReturnType::TimeOut)
        assert_eq!(cfg.consume_timeout, 15);
        assert!(cfg.client_rebalance);
        assert_eq!(cfg.heartbeat_interval_millis, 30_000);
        assert!(cfg.heartbeat_enabled);
        // Default is long-polling + offsets, not POP + ack
        assert!(!cfg.pop_mode);
        assert_eq!(cfg.pop_invisible_time, 60_000);
        assert_eq!((cfg.pop_batch_nums, cfg.pop_threshold_for_queue), (32, 96));
        assert_eq!(cfg.pop_delay_level.to_vec(), POP_DELAY_LEVEL.to_vec());
        assert_eq!(
            (cfg.pop_poll_time_millis, cfg.pop_timeout_millis),
            (15_000, 25_000)
        );
        // POP long-polling suspend time must be shorter than the request timeout, otherwise every pull hits a client timeout
        assert!(cfg.pop_poll_time_millis < cfg.pop_timeout_millis);
        assert!(!cfg.enable_trace);
        assert_eq!(cfg.trace_topic, None);
        assert_eq!(cfg.trace_msg_batch_num, 10);
        // CONSUME_FROM_TIMESTAMP start point: default "30 minutes ago", format yyyyMMddHHmmss
        assert_eq!(cfg.consume_timestamp.len(), 14);
        let past = consume_timestamp_millis(&cfg.consume_timestamp).unwrap();
        assert!((past..current_time_millis()).contains(&(current_time_millis() - 1000)));
        // Regression guard: 14 plain digits are a **local wall-clock date**, not an epoch (otherwise it would be year 2611)
        let expected = {
            use chrono::TimeZone;
            chrono::Local
                .with_ymd_and_hms(2023, 1, 1, 0, 0, 0)
                .single()
                .expect("local 2023-01-01 00:00:00 should resolve")
        };
        assert_eq!(
            consume_timestamp_millis("20230101000000").unwrap(),
            expected.timestamp_millis()
        );
        let err = consume_timestamp_millis("not-a-time").unwrap_err();
        assert!(
            format!("{err}").contains("consumeTimestamp is invalid"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn blank_consumer_group_is_rejected() {
        // Rejection text verified verbatim: "consumerGroup is empty"
        for group in ["", "   ", "\t\n"] {
            assert!(
                DefaultMQPushConsumer::new(group).is_err(),
                "group {group:?} must be rejected"
            );
        }
        assert!(DefaultMQPushConsumer::new("G").is_ok());
    }

    #[test]
    fn update_core_pool_size_guards_match_contract() {
        let c = DefaultMQPushConsumer::new("G").unwrap();
        assert_eq!(
            c.config().consume_thread_max,
            20,
            "both sides default to 20"
        );
        // Unbounded queue => real concurrency == core; default max=20, so it can only be adjusted **down**
        assert!(c.update_core_pool_size(15));
        assert_eq!(c.get_core_pool_size(), 15);
        assert!(!c.update_core_pool_size(0));
        assert!(!c.update_core_pool_size(-1));
        assert!(!c.update_core_pool_size(20), "== consume_thread_max is also rejected");
        assert!(
            !c.update_core_pool_size(30),
            "regression: the default max must not be 4.x's 64"
        );
        assert!(c.update_core_pool_size(19), "just below max");
        assert_eq!(c.get_core_pool_size(), 19);
        c.update_config(|x| x.consume_thread_max = 40_000);
        assert!(!c.update_core_pool_size(32768), "above Short.MAX_VALUE");
        assert!(c.update_core_pool_size(32767), "the upper bound itself is legal");
        assert_eq!(c.get_core_pool_size(), 32767);
    }

    #[test]
    fn push_consumer_exposes_the_allocate_strategy() {
        use crate::client::allocate_strategy::{
            AllocateMessageQueueAveragelyByCircle, AllocateMessageQueueByConfig,
        };

        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        assert_eq!(
            consumer.allocate_message_queue_strategy().get_name(),
            "AVG",
            "the default strategy must be AVG"
        );

        consumer
            .set_allocate_message_queue_strategy(Arc::new(AllocateMessageQueueAveragelyByCircle));
        assert_eq!(
            consumer.allocate_message_queue_strategy().get_name(),
            "AVG_BY_CIRCLE",
            "the strategy is replaceable, the getter reads back the same one"
        );

        let by_config = Arc::new(AllocateMessageQueueByConfig::new(vec![MessageQueue::new(
            "T", "broker-a", 1,
        )]));
        consumer.set_allocate_message_queue_strategy(by_config.clone());
        by_config.set_message_queue_list(vec![
            MessageQueue::new("T", "broker-a", 0),
            MessageQueue::new("T", "broker-a", 1),
        ]);
        let strategy = consumer.allocate_message_queue_strategy();
        assert_eq!(strategy.get_name(), "CONFIG");
        assert_eq!(
            strategy
                .allocate("G", "cid", &[], &["cid".to_string()])
                .unwrap()
                .len(),
            2
        );
    }

    /// Three segments **directly concatenated**, no separator.
    #[test]
    fn mq_key_concatenates_topic_broker_and_queue_id() {
        assert_eq!(
            mq_key(&MessageQueue::new("Topic", "broker-a", 3)),
            "Topicbroker-a3"
        );
    }

    struct NoopListener;

    impl MessageListenerConcurrently for NoopListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            _context: &mut ConsumeConcurrentlyContext,
        ) -> ConsumeConcurrentlyStatus {
            ConsumeConcurrentlyStatus::ConsumeSuccess
        }
    }

    struct NoopOrderlyListener;

    impl MessageListenerOrderly for NoopOrderlyListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            _context: &mut ConsumeOrderlyContext,
        ) -> ConsumeOrderlyStatus {
            ConsumeOrderlyStatus::Success
        }
    }

    /// The listener is **stored** in `Inner` rather than taken out: delivery, rebalance and ops APIs all read it
    /// repeatedly, and `is_orderly` decides whether the pull loop asks for the broker lock (`LOCK_BATCH_MQ`).
    #[test]
    fn listener_kind_is_recorded_and_not_consumed() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        assert!(lock(&consumer.inner.listener).is_none());

        consumer.set_message_listener_concurrently(Arc::new(NoopListener));
        for _ in 0..2 {
            let listener = lock(&consumer.inner.listener).clone();
            assert_eq!(
                listener.as_ref().map(MessageListener::is_orderly),
                Some(false)
            );
        }

        // Switch to an orderly listener: overwrite, not append
        consumer.set_message_listener_orderly(Arc::new(NoopOrderlyListener));
        let listener = lock(&consumer.inner.listener).clone();
        assert_eq!(
            listener.as_ref().map(MessageListener::is_orderly),
            Some(true)
        );
    }

    /// The three `start()` gates must run before any network action (validation happens at the top of `start()`).
    ///
    /// The name server address is `127.0.0.1:1`: if the gate order ever breaks, the test fails instead of
    /// connecting to a real cluster and consuming someone else's messages.
    #[tokio::test]
    async fn start_validates_configuration_before_any_network_io() {
        if DefaultTopAddressing::is_configured() {
            // Domain addressing is configured in the environment, bypassing the name_server_addrs gate; skip the first round of asserts
            return;
        }
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        assert!(
            consumer.start().await.is_err(),
            "neither name server addresses nor domain addressing"
        );
        assert!(!consumer.is_started(), "a failed start must roll back the started flag");

        consumer.set_namesrv_addr("127.0.0.1:1");
        assert!(consumer.start().await.is_err(), "no subscription");
        assert!(!consumer.is_started());

        consumer.subscribe("T", "TagA").unwrap();
        assert!(consumer.start().await.is_err(), "no listener set");
        assert!(!consumer.is_started());
    }


    struct RangeGate {
        field: &'static str,
        lo: i64,
        hi: i64,
        message: &'static str,
        set: fn(&mut ConsumerConfig, i64),
        minus_one_off: bool,
    }

    const RANGE_GATES: &[RangeGate] = &[
        RangeGate {
            field: "consume_thread_min",
            lo: 1,
            hi: 1000,
            message: "consumeThreadMin Out of range [1, 1000]",
            set: |c, v| c.consume_thread_min = v as i32,
            minus_one_off: false,
        },
        RangeGate {
            field: "consume_thread_max",
            lo: 1,
            hi: 1000,
            message: "consumeThreadMax Out of range [1, 1000]",
            set: |c, v| c.consume_thread_max = v as i32,
            minus_one_off: false,
        },
        RangeGate {
            field: "consume_concurrently_max_span",
            lo: 1,
            hi: 65535,
            message: "consumeConcurrentlyMaxSpan Out of range [1, 65535]",
            set: |c, v| c.consume_concurrently_max_span = v,
            minus_one_off: false,
        },
        RangeGate {
            field: "pull_threshold_for_queue",
            lo: 1,
            hi: 65535,
            message: "pullThresholdForQueue Out of range [1, 65535]",
            set: |c, v| c.pull_threshold_for_queue = v as i32,
            minus_one_off: false,
        },
        RangeGate {
            field: "pull_threshold_for_topic",
            lo: 1,
            hi: 6553500,
            message: "pullThresholdForTopic Out of range [1, 6553500]",
            set: |c, v| c.pull_threshold_for_topic = v as i32,
            minus_one_off: true,
        },
        RangeGate {
            field: "pull_threshold_size_for_queue",
            lo: 1,
            hi: 1024,
            message: "pullThresholdSizeForQueue Out of range [1, 1024]",
            set: |c, v| c.pull_threshold_size_for_queue = v as i32,
            minus_one_off: false,
        },
        RangeGate {
            field: "pull_threshold_size_for_topic",
            lo: 1,
            hi: 102400,
            message: "pullThresholdSizeForTopic Out of range [1, 102400]",
            set: |c, v| c.pull_threshold_size_for_topic = v as i32,
            minus_one_off: true,
        },
        RangeGate {
            field: "pull_interval",
            lo: 0,
            hi: 65535,
            message: "pullInterval Out of range [0, 65535]",
            set: |c, v| c.pull_interval = v,
            minus_one_off: false,
        },
        RangeGate {
            field: "consume_message_batch_max_size",
            lo: 1,
            hi: 1024,
            message: "consumeMessageBatchMaxSize Out of range [1, 1024]",
            set: |c, v| c.consume_message_batch_max_size = v as i32,
            minus_one_off: false,
        },
        RangeGate {
            field: "pull_batch_size",
            lo: 1,
            hi: 1024,
            message: "pullBatchSize Out of range [1, 1024]",
            set: |c, v| c.pull_batch_size = v as i32,
            minus_one_off: false,
        },
        RangeGate {
            field: "pop_invisible_time",
            lo: MIN_POP_INVISIBLE_TIME,
            hi: MAX_POP_INVISIBLE_TIME,
            message: "popInvisibleTime Out of range [5000, 300000]",
            set: |c, v| c.pop_invisible_time = v,
            minus_one_off: false,
        },
        RangeGate {
            field: "pop_batch_nums",
            lo: 1,
            hi: 32,
            message: "popBatchNums Out of range [1, 32]",
            set: |c, v| c.pop_batch_nums = v as i32,
            minus_one_off: false,
        },
    ];

    /// Change only one gate, keeping the rest at defaults; spread the thread counts across the whole legal domain
    /// first, otherwise raising only `consume_thread_min` trips the relative "min > max" check first and yields a
    /// different message, so the range branch under test is never reached.
    fn gate_cfg(gate: &RangeGate, value: i64) -> ConsumerConfig {
        let mut cfg = ConsumerConfig {
            consume_thread_min: 1,
            consume_thread_max: 1000,
            ..Default::default()
        };
        (gate.set)(&mut cfg, value);
        cfg
    }

    fn gate_error(cfg: &ConsumerConfig) -> String {
        match check_config_ranges(cfg) {
            Ok(()) => "<accepted>".to_string(),
            Err(e) => e.to_string(),
        }
    }

    fn expect_rejected(gate: &RangeGate, value: i64) {
        let want = format!("MQClientException: {}", gate.message);
        assert_eq!(
            gate_error(&gate_cfg(gate, value)),
            want,
            "{}={}",
            gate.field,
            value
        );
    }

    fn expect_accepted(gate: &RangeGate, value: i64) {
        assert_eq!(
            gate_error(&gate_cfg(gate, value)),
            "<accepted>",
            "{}={} must be legal",
            gate.field,
            value
        );
    }

    #[test]
    fn defaults_pass_check_config_ranges() {
        check_config_ranges(&ConsumerConfig::default()).expect("default config must pass checkConfig");
    }

    #[test]
    fn each_range_gate_accepts_both_bounds() {
        for gate in RANGE_GATES {
            expect_accepted(gate, gate.lo);
            expect_accepted(gate, gate.hi);
        }
    }

    #[test]
    fn each_range_gate_rejects_both_ends() {
        for gate in RANGE_GATES {
            expect_rejected(gate, gate.lo - 1);
            expect_rejected(gate, gate.hi + 1);
        }
    }

    #[test]
    fn zero_is_illegal_except_for_the_gates_whose_floor_is_zero() {
        for gate in RANGE_GATES {
            if gate.lo == 0 {
                expect_accepted(gate, 0);
            } else {
                expect_rejected(gate, 0);
            }
        }
    }

    #[test]
    fn minus_one_sentinel_only_closes_topic_gates() {
        for gate in RANGE_GATES {
            if gate.minus_one_off {
                expect_accepted(gate, -1);
            } else {
                expect_rejected(gate, -1);
            }
        }
    }

    #[test]
    fn thread_min_must_not_exceed_thread_max() {
        assert_eq!(
            gate_error(&ConsumerConfig {
                consume_thread_min: 8,
                consume_thread_max: 4,
                ..Default::default()
            }),
            "MQClientException: consumeThreadMin (8) is larger than consumeThreadMax (4)"
        );

        check_config_ranges(&ConsumerConfig {
            consume_thread_min: 8,
            consume_thread_max: 8,
            ..Default::default()
        })
        .expect("min == max must be legal");

        // The relative check ranks **after** the two range checks: with min=0 the range error is reported, not min>max
        assert_eq!(
            gate_error(&ConsumerConfig {
                consume_thread_min: 0,
                consume_thread_max: 0,
                ..Default::default()
            }),
            "MQClientException: consumeThreadMin Out of range [1, 1000]"
        );
    }

    #[test]
    fn gates_are_checked_in_contract_order() {
        assert_eq!(
            gate_error(&ConsumerConfig {
                consume_concurrently_max_span: 0,
                pull_batch_size: 0,
                ..Default::default()
            }),
            "MQClientException: consumeConcurrentlyMaxSpan Out of range [1, 65535]",
            "the span gate comes before the count gate, the span error is reported first"
        );
        assert_eq!(
            gate_error(&ConsumerConfig {
                pull_threshold_for_queue: 0,
                pull_batch_size: 0,
                ..Default::default()
            }),
            "MQClientException: pullThresholdForQueue Out of range [1, 65535]",
            "the count gate comes before pullBatchSize"
        );
        assert_eq!(
            gate_error(&ConsumerConfig {
                pull_batch_size: 0,
                ..Default::default()
            }),
            "MQClientException: pullBatchSize Out of range [1, 1024]",
            "pullBatchSize is only reached once the earlier gates are fixed"
        );
    }

    /// A bad config must be rejected **before the instance is built**: raising background tasks and then erroring
    /// leaks those tasks, and `started` stays true, so start() after `shutdown()` silently returns.
    #[tokio::test]
    async fn start_rejects_out_of_range_config_before_any_network_io() {
        let consumer = DefaultMQPushConsumer::new("CID_check_config_ranges_rust").unwrap();
        consumer.set_namesrv_addr("127.0.0.1:1");
        consumer.subscribe("T", "TagA").unwrap();
        consumer.set_message_listener_concurrently(Arc::new(NoopListener));
        consumer.update_config(|c| c.pull_batch_size = 1025);

        let e = consumer
            .start()
            .await
            .expect_err("pullBatchSize=1025 must be rejected at startup");
        assert_eq!(
            e.to_string(),
            "MQClientException: pullBatchSize Out of range [1, 1024]"
        );
        assert!(
            !consumer.is_started(),
            "a rejected start() must roll back the started flag"
        );
        assert!(
            lock(&consumer.inner.client).is_none(),
            "the numeric gates must rank before building MQClientInstance, otherwise background tasks leak"
        );

        // After fixing the value the same object still starts (validation leaves no dirty state)
        consumer.update_config(|c| c.pull_batch_size = 1024);
        consumer.start().await.expect("must start once the value is fixed");
        consumer.shutdown();
    }

    #[tokio::test]
    async fn start_accepts_every_gate_boundary() {
        for (label, at_max) in [("min", false), ("max", true)] {
            let consumer =
                DefaultMQPushConsumer::new(&format!("CID_check_config_boundary_rust_{label}"))
                    .unwrap();
            consumer.set_namesrv_addr("127.0.0.1:1");
            consumer.subscribe("T", "TagA").unwrap();
            consumer.set_message_listener_concurrently(Arc::new(NoopListener));
            consumer.update_config(|c| {
                for gate in RANGE_GATES {
                    let value = if at_max { gate.hi } else { gate.lo };
                    (gate.set)(c, value);
                }
            });
            if let Err(e) = consumer.start().await {
                consumer.shutdown();
                panic!("boundary config {label} wrongly rejected: {e}");
            }
            consumer.shutdown();
        }
    }

    #[tokio::test]
    async fn scheduled_intervals_reach_the_instance() {
        let consumer = DefaultMQPushConsumer::new("CID_interval_probe_rust").unwrap();
        assert_eq!(
            consumer.config().poll_name_server_interval_millis,
            30_000,
            "ClientConfig:58 = 1000 * 30"
        );
        assert_eq!(
            consumer.config().persist_consumer_offset_interval_millis,
            5_000,
            "ClientConfig:66 = 1000 * 5"
        );
        consumer.set_namesrv_addr("127.0.0.1:1");
        consumer.subscribe("T", "TagA").unwrap();
        consumer.set_message_listener_concurrently(Arc::new(NoopListener));
        consumer.set_poll_name_server_interval_millis(2_000);
        consumer.set_persist_consumer_offset_interval_millis(700);

        consumer.start().await.expect("start must not fail with a static address");
        let client = lock(&consumer.inner.client)
            .clone()
            .expect("an instance must already exist after start");
        assert_eq!(client.config().route_refresh_interval_millis, 2_000);
        assert_eq!(client.config().persist_offset_interval_millis, 700);
        consumer.shutdown();
    }

    #[tokio::test]
    async fn start_rejects_unparsable_consume_timestamp() {
        let consumer = DefaultMQPushConsumer::new("CID_check_config_ts_rust").unwrap();
        consumer.set_namesrv_addr("127.0.0.1:1");
        consumer.subscribe("T", "TagA").unwrap();
        consumer.set_message_listener_concurrently(Arc::new(NoopListener));
        consumer.update_config(|c| {
            c.consume_from_where = ConsumeFromWhere::CONSUME_FROM_TIMESTAMP.to_string();
            c.consume_timestamp = "2026-01-01 00:00:00".to_string();
        });
        let e = consumer
            .start()
            .await
            .expect_err("non-yyyyMMddHHmmss must be rejected");
        assert!(
            e.to_string().contains("consumeTimestamp is invalid"),
            "unexpected error: {e}"
        );
        assert!(!consumer.is_started());
    }

    #[tokio::test]
    async fn client_id_follows_buildmqclientid_contract() {
        let clustering = DefaultMQPushConsumer::new("CID-clientid-parity-clustering").unwrap();
        clustering.set_namesrv_addr("127.0.0.1:1");
        clustering.subscribe("T", "TagA").unwrap();
        clustering.set_message_listener_concurrently(Arc::new(NoopListener));
        clustering
            .start()
            .await
            .expect("the CLUSTERING consumer should start");

        let id = clustering.client_id();
        let (ip, instance) = id
            .split_once('@')
            .unwrap_or_else(|| panic!("clientId missing the IP@instanceName separator: {id}"));
        assert_eq!(ip, MixAll::cached_ip_str(), "{id}");
        assert!(
            instance.starts_with(&format!("{}#", MixAll::cached_pid())),
            "{id}"
        );
        assert_eq!(
            clustering.config().instance_name,
            instance,
            "instanceName must be written back into the config in place, otherwise a restart would mint a new clientId"
        );
        clustering.shutdown();
        clustering.start().await.expect("restart");
        assert_eq!(clustering.client_id(), id, "the restart changed the clientId");
        let clustering_id = clustering.client_id();
        clustering.shutdown();

        let broadcast = |group: &str| {
            let cfg = ConsumerConfig {
                consumer_group: group.to_string(),
                name_server_addrs: vec!["127.0.0.1:1".to_string()],
                message_model: MessageModel::BROADCASTING.to_string(),
                ..Default::default()
            };
            let c = DefaultMQPushConsumer::with_config(cfg).unwrap();
            c.subscribe("T", "TagA").unwrap();
            c.set_message_listener_concurrently(Arc::new(NoopListener));
            c
        };
        let first = broadcast("CID-clientid-parity-broadcast-a");
        first.start().await.expect("the broadcasting consumer should start");
        assert_eq!(
            first.client_id(),
            format!("{}@DEFAULT", MixAll::cached_ip_str()),
            "broadcast mode must not rewrite instanceName"
        );
        assert_eq!(first.config().instance_name, "DEFAULT");
        assert_ne!(first.client_id(), clustering_id);

        let second = broadcast("CID-clientid-parity-broadcast-b");
        second.start().await.expect("the second broadcasting consumer should start");
        assert_eq!(second.client_id(), first.client_id());
        let shared = MQClientInstance::find_instance(&first.client_id()).expect("the instance must be registered");
        assert!(
            shared
                .find_consumer(&first.config().consumer_group)
                .is_some()
                && shared
                    .find_consumer(&second.config().consumer_group)
                    .is_some(),
            "the two broadcasting consumers did not land on the same instance"
        );
        second.shutdown();
        first.shutdown();
        // The push consumer's `client.shutdown()` ranks last in the teardown chain (persist offsets -> unlock ->
        // unregister -> close instance, see `shutdown`), so poll until it finishes instead of asserting synchronously.
        let mut torn_down = false;
        for _ in 0..100 {
            if MQClientInstance::find_instance(&first.client_id()).is_none() {
                torn_down = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(torn_down, "the instance was not torn down after the last tenant left");
    }

    /// Group-name validation is the first of the three gates: with address, subscription and listener all present it
    /// still fails on the group name alone. Falls back to `127.0.0.1:1`, so a broken order fails to connect instead of hitting a real cluster.
    #[tokio::test]
    async fn start_rejects_bad_consumer_group_before_the_other_gates() {
        let long_group = std::iter::repeat_n('g', 121).collect::<String>();
        for (group, needle) in [
            (
                MixAll::DEFAULT_CONSUMER_GROUP,
                "consumerGroup can not equal DEFAULT_CONSUMER",
            ),
            ("bad group", "contains illegal characters"),
            (long_group.as_str(), "is longer than group max length"),
        ] {
            let consumer = DefaultMQPushConsumer::new(group).expect("construction must not reject early");
            consumer.set_namesrv_addr("127.0.0.1:1");
            consumer.subscribe("T", "TagA").unwrap();
            consumer.set_message_listener_concurrently(Arc::new(NoopListener));
            let err = consumer.start().await.expect_err("an illegal group name must fail locally");
            assert!(err.to_string().contains(needle), "{group}: {err}");
            assert!(
                !consumer.is_started(),
                "{group}: a failed start must roll back started"
            );
        }
    }

    /// Subscription table: expressions parsed via `FilterAPI`, re-subscribing overwrites, `unsubscribe` removes.
    #[test]
    fn subscriptions_are_namespaced_and_overwritable() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.subscribe("T", "TagA||TagB").unwrap();
        let subs = consumer.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].topic, "T");
        assert_eq!(subs[0].sub_string, "TagA||TagB");
        assert_eq!(subs[0].expression_type, ExpressionType::TAG);

        // Re-subscribing the same topic **replaces**, it does not append
        consumer
            .subscribe_with_selector("T", &MessageSelector::by_sql("a > 1"))
            .unwrap();
        let subs = consumer.subscriptions();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].sub_string, "a > 1");
        assert_eq!(subs[0].expression_type, ExpressionType::SQL92);
        assert!(lock(&consumer.inner.state).subscription("T").is_some());

        // The namespace is joined into the topic at subscribe time
        consumer.update_config(|c| c.namespace = "Ns".to_string());
        consumer.subscribe("Other", "*").unwrap();
        assert!(lock(&consumer.inner.state)
            .subscription("Ns%Other")
            .is_some());

        consumer.unsubscribe("Other");
        assert!(lock(&consumer.inner.state)
            .subscription("Ns%Other")
            .is_none());
    }

    // ---------------- flow control and stats ----------------

    fn queue(topic: &str, broker: &str, id: i32) -> MessageQueue {
        MessageQueue::new(topic, broker, id)
    }

    /// A message whose cached cost is `size` bytes: the **body** is that long, and `store_size`
    /// carries the same number so a fixture that only cares about the count cannot accidentally be
    /// reading one field or the other.
    fn sized_msg(size: i32, queue_offset: i64) -> MessageExt {
        let mut m = ext("T", None);
        let body = vec![b'x'; usize::try_from(size).unwrap_or(0)];
        m.set_body(Some(&body));
        m.store_size = size;
        m.queue_offset = queue_offset;
        m
    }

    /// A message whose two size fields disagree -- the fixture the size gate needs, because a
    /// `store_size` the broker charges for the whole storage record must never be the number that
    /// refuses a pull.
    fn split_size_msg(body_len: usize, store_size: i32, queue_offset: i64) -> MessageExt {
        let mut m = ext("T", None);
        m.set_body(Some(&vec![b'x'; body_len]));
        m.store_size = store_size;
        m.queue_offset = queue_offset;
        m
    }

    fn stage(consumer: &DefaultMQPushConsumer, mq: &MessageQueue, msgs: Vec<MessageExt>) {
        let key = mq_key(mq);
        let mut state = lock(&consumer.inner.state);
        state.mq_map.insert(key.clone(), mq.clone());
        state.pending.insert(key, msgs.into_iter().collect());
    }

    /// Put a batch into the listener's hands without settling it: exactly what the dispatch window
    /// does between "handed over" and "ack/complete", and the part of the held set a buffer-only gate
    /// cannot see.
    fn stage_listener(
        consumer: &DefaultMQPushConsumer,
        mq: &MessageQueue,
        msgs: Vec<MessageExt>,
    ) {
        let key = mq_key(mq);
        let mut state = lock(&consumer.inner.state);
        state.mq_map.insert(key.clone(), mq.clone());
        state.inflight_msgs.insert(key, msgs);
    }

    /// Isolate one gate by pushing the other two thresholds to their widest legal value. There is no
    /// "0 switches this gate off" to lean on any more: 0 is a refused configuration at start-up and
    /// the gate compares the number it was given, so a 0 left here would trip that gate first and
    /// mask the branch under test.
    fn only(consumer: &DefaultMQPushConsumer, f: impl FnOnce(&mut ConsumerConfig)) {
        consumer.update_config(|c| {
            c.pull_threshold_for_queue = i32::MAX;
            c.pull_threshold_size_for_queue = i32::MAX;
            c.consume_concurrently_max_span = i64::MAX;
            c.pull_threshold_for_topic = -1;
            c.pull_threshold_size_for_topic = -1;
            f(c);
        });
    }

    #[test]
    fn flow_control_hits_each_threshold() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        let triggered = || consumer.inner.flow_control_triggered.load(Ordering::SeqCst);

        // Empty buffer: no threshold triggers
        assert!(!flow_control_hit(&consumer.inner, &key));
        assert_eq!(triggered(), 0);

        only(&consumer, |c| c.pull_threshold_for_queue = 3);
        stage(
            &consumer,
            &mq,
            vec![sized_msg(100, 0), sized_msg(100, 1), sized_msg(100, 2)],
        );
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "exactly the threshold 3 is still allowed to pull (strictly greater)"
        );
        assert_eq!(triggered(), 0, "a round that does not trip counts nothing");
        stage(
            &consumer,
            &mq,
            vec![
                sized_msg(100, 0),
                sized_msg(100, 1),
                sized_msg(100, 2),
                sized_msg(100, 3),
            ],
        );
        assert!(flow_control_hit(&consumer.inner, &key), "count 4 > 3");
        assert_eq!(triggered(), 1, "one trip, one counted hit");

        // The size gate reads whole MiB of **body**: 300 B of body is nothing to a 1 MiB budget, and
        // exactly 1 MiB still pulls because the refusal is strictly above it.
        only(&consumer, |c| c.pull_threshold_size_for_queue = 1);
        stage(&consumer, &mq, vec![sized_msg(300, 0)]);
        assert!(!flow_control_hit(&consumer.inner, &key), "300B < 1MiB");
        stage(&consumer, &mq, vec![sized_msg(1024 * 1024, 0)]);
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "exactly 1 MiB is still allowed under a 1 MiB threshold (strictly greater)"
        );
        // Integer (truncating) MiB: 1.9 MiB of body still reads as 1 MiB against a 1 MiB budget.
        stage(
            &consumer,
            &mq,
            vec![sized_msg(1024 * 1024, 0), sized_msg(900 * 1024, 1)],
        );
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "1.9 MiB of body truncates to 1 MiB, which is not above a 1 MiB budget"
        );
        stage(&consumer, &mq, vec![sized_msg(1024 * 1024, 0), sized_msg(1024 * 1024, 1)]);
        assert!(flow_control_hit(&consumer.inner, &key), "2MiB > 1MiB");

        only(&consumer, |c| c.consume_concurrently_max_span = 10);
        stage(&consumer, &mq, vec![sized_msg(1, 0), sized_msg(1, 100)]);
        assert!(
            flow_control_hit(&consumer.inner, &key),
            "span 100 > 10"
        );
        only(&consumer, |c| c.consume_concurrently_max_span = 100);
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "span 100 is not strictly greater than 100"
        );

        // Topic-level knobs are **inert in the pull path**: they are divided into the per-queue knob
        // at rebalance (`divide_topic_thresholds`), and the gate only ever judges this queue's own
        // buffer. Enabling a topic budget here, where its quotient would be far below the queue-level
        // gates, must not pause anything -- the old code summed the topic's queues at pull time.
        only(&consumer, |c| {
            c.pull_threshold_for_topic = 2;
            c.pull_threshold_size_for_topic = 1;
        });
        stage(&consumer, &mq, vec![sized_msg(1, 0)]);
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "a topic budget alone does not pause the pull"
        );
        stage(&consumer, &mq, vec![sized_msg(2 * 1024 * 1024, 0)]);
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "not even a 2MiB buffer of one queue trips a gate the topic budget never moved"
        );

        // Default config: topic-level thresholds are off, only the queue-level ones apply
        let plain = DefaultMQPushConsumer::new("G").unwrap();
        stage(&plain, &mq, vec![sized_msg(1, 0)]);
        assert!(!flow_control_hit(&plain.inner, &key));
    }

    /// What the gate measures is what this queue still **holds**: the pulled-but-undispatched buffer
    /// *plus* the batch already handed to the listener and not yet settled, with a message that sits
    /// on both sides counted once.
    ///
    /// The discriminating shape is a thin buffer over a fat listener: 2 rows in the buffer alone pull,
    /// and the same 2 rows with 2 unsettled listener rows pause. An implementation that judged
    /// `pending` only never sees the dispatched half and keeps reserving while the listener still sits
    /// on messages.
    #[test]
    fn flow_control_held_set_is_the_buffer_union_the_listeners_batch() {
        let consumer = DefaultMQPushConsumer::new("G_held_set").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);

        only(&consumer, |c| c.pull_threshold_for_queue = 3);
        // Buffer-only: 2 of the 3 allowed.
        stage(&consumer, &mq, vec![sized_msg(1, 10), sized_msg(1, 11)]);
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "a 2-message buffer under a 3-message gate pulls"
        );

        // The dispatch window hands the LOWER offsets to the listener and does not settle them.
        stage_listener(
            &consumer,
            &mq,
            vec![sized_msg(1, 8), sized_msg(1, 9)],
        );
        assert!(
            flow_control_hit(&consumer.inner, &key),
            "2 buffered + 2 unsettled listener rows is 4 held against a 3-message gate"
        );

        // Deduplication control: the requeued head sits on BOTH sides (the failed-redelivery path
        // pushes it back into the buffer before the finish-time deregistration removes it from the
        // listener batch). Counting it twice would read 4 held rows where the queue holds 3, so a
        // gate that does not key on `queue_offset` pauses here instead of pulling.
        stage_listener(&consumer, &mq, vec![sized_msg(1, 8), sized_msg(1, 9)]);
        stage(&consumer, &mq, vec![sized_msg(1, 9), sized_msg(1, 10)]);
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "offset 9 on both sides is one held row, not two: 8/9/10 is 3, not 4"
        );

        // Settling the listener's batch releases its rows: the same buffer pulls again, and one row
        // more than the gate is enough to pause it.
        lock(&consumer.inner.state).inflight_msgs.remove(&key);
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "once the dispatched batch settles, only the 2-row buffer is left"
        );
        stage(
            &consumer,
            &mq,
            vec![
                sized_msg(1, 9),
                sized_msg(1, 10),
                sized_msg(1, 11),
                sized_msg(1, 12),
            ],
        );
        assert!(
            flow_control_hit(&consumer.inner, &key),
            "a 4-row buffer with nothing in the listener still reads as 4 held"
        );
    }

    /// The size gate sums the held messages' **body** bytes and divides the total by whole MiB
    /// (truncating). The stored-record size the broker charges for the whole storage record is
    /// telemetry about the disk, not something this client keeps in memory, so it must never be the
    /// number that refuses a pull -- and a fat stored record must not make a small buffer look heavy.
    #[test]
    fn flow_control_size_reads_body_bytes_never_the_stored_record() {
        let consumer = DefaultMQPushConsumer::new("G_body_size").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);

        only(&consumer, |c| c.pull_threshold_size_for_queue = 1);

        // A 100-byte body carrying a 4 MiB stored record: the buffer is 0 MiB of body.
        stage(
            &consumer,
            &mq,
            vec![split_size_msg(100, 4 * 1024 * 1024, 0)],
        );
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "a 4MiB stored record over a 100B body does not move a body-based gate"
        );

        // The mirror image: a 2 MiB body the broker reports as 10 stored bytes must pause.
        stage(
            &consumer,
            &mq,
            vec![split_size_msg(2 * 1024 * 1024, 10, 0)],
        );
        assert!(
            flow_control_hit(&consumer.inner, &key),
            "2MiB of body is 2MiB to a 1MiB budget whatever the stored size says"
        );

        // Accumulation happens on the body side across the held set, and it truncates: 1.9 MiB reads
        // as 1 MiB and still pulls, while the same total plus a 300B stored-only row does not change.
        stage(
            &consumer,
            &mq,
            vec![
                split_size_msg(1024 * 1024, 3 * 1024 * 1024, 0),
                split_size_msg(900 * 1024, 3 * 1024 * 1024, 1),
            ],
        );
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "1.9MiB of body truncates to 1MiB; 6MiB of stored records is ignored"
        );
    }

    /// The span knob is named after concurrent consumption, so an **orderly** listener is never
    /// judged by it: an orderly queue's cursor only moves once the batch in the listener's hands has
    /// settled, which means the buffer is allowed to sit wide without a run-ahead to guard. Count and
    /// size still bind the orderly path over the same held set.
    #[test]
    fn flow_control_span_gate_skips_the_orderly_listener() {
        let consumer = DefaultMQPushConsumer::new("G_span_orderly").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);

        only(&consumer, |c| c.consume_concurrently_max_span = 10);
        stage(&consumer, &mq, vec![sized_msg(1, 0), sized_msg(1, 100)]);
        assert!(
            flow_control_hit(&consumer.inner, &key),
            "span 100 over a 10 span budget pauses the concurrent path"
        );

        consumer.set_message_listener_orderly(Arc::new(NoopOrderlyListener));
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "the identical wide span is not judged at all for an orderly listener"
        );

        // Control: the exemption is only the span term -- an orderly queue still obeys its count gate
        // over the same held set, so the listener is not skipped wholesale.
        only(&consumer, |c| {
            c.pull_threshold_for_queue = 1;
            c.consume_concurrently_max_span = i64::MAX;
        });
        assert!(
            flow_control_hit(&consumer.inner, &key),
            "2 held rows over a 1-message count gate pause an orderly queue too"
        );
    }

    /// Each gate compares the number it was given, strictly greater than: no floor of 1 and no
    /// "0 or a negative number switches this gate off" escape. Keeping the knobs inside their legal ranges is the
    /// start-up validation's job; the gate itself has no opinion about a hand-built config, and a
    /// reader who assumes otherwise will think a 0 budget means "unlimited" where it means "cache
    /// nothing".
    #[test]
    fn flow_control_thresholds_are_compared_as_configured() {
        let consumer = DefaultMQPushConsumer::new("G_no_floor").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);

        // A count budget of 0 with a 1-byte body and a single offset: only the count term can be the
        // one that refuses, and an implementation that read `max(1, threshold)` pulls here.
        consumer.update_config(|c| {
            c.pull_threshold_for_queue = 0;
            c.pull_threshold_size_for_queue = i32::MAX;
            c.consume_concurrently_max_span = i64::MAX;
            c.pull_threshold_for_topic = -1;
            c.pull_threshold_size_for_topic = -1;
        });
        stage(&consumer, &mq, vec![sized_msg(1, 0)]);
        assert!(
            flow_control_hit(&consumer.inner, &key),
            "a 0-message budget holds nothing, so one cached message pauses the pull"
        );
        lock(&consumer.inner.state).pending.remove(&key);
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "an empty queue is at 0, which is not above a 0 budget"
        );

        // The same for the size term: 1 byte of body truncates to 0 MiB, so a 0 MiB budget is not
        // tripped by nothing -- and a whole MiB is. The count term is widened out of the way first,
        // otherwise it refuses for the reason the previous block just proved.
        consumer.update_config(|c| {
            c.pull_threshold_for_queue = i32::MAX;
            c.pull_threshold_size_for_queue = 0;
        });
        stage(&consumer, &mq, vec![sized_msg(1, 0)]);
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "0 MiB of body is not strictly above a 0 MiB budget"
        );
        stage(&consumer, &mq, vec![sized_msg(1024 * 1024, 0)]);
        assert!(
            flow_control_hit(&consumer.inner, &key),
            "1 MiB of body is above a 0 MiB budget"
        );

        // And the span term: two adjacent offsets are span 1, which a 0 budget refuses rather than
        // ignores.
        consumer.update_config(|c| {
            c.pull_threshold_size_for_queue = i32::MAX;
            c.consume_concurrently_max_span = 0;
        });
        stage(&consumer, &mq, vec![sized_msg(1, 0), sized_msg(1, 1)]);
        assert!(
            flow_control_hit(&consumer.inner, &key),
            "a span budget of 0 refuses span 1 instead of switching the gate off"
        );
        stage(&consumer, &mq, vec![sized_msg(1, 0)]);
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "a single cached row is span 0, level with the budget"
        );
    }

    /// The pull gate never sums its siblings: under a 1000-message **topic** budget divided over 4
    /// owned queues, each queue may hold its full 250-message share. An implementation that
    /// aggregates the topic at pull time pauses all four at once (4 x 250 = 1000 >= 1000), so the
    /// control is the 251st message: exactly one queue pauses on its **own** count reason while the
    /// three untouched siblings keep pulling.
    #[test]
    fn flow_control_judges_one_queue_never_the_topic_sum() {
        let consumer = DefaultMQPushConsumer::new("G_fc_per_queue_only").unwrap();
        let queues = owned_queue_set("T", 4);
        for mq in &queues {
            stage(&consumer, mq, (0..250_i64).map(|i| sized_msg(100, i)).collect());
        }
        consumer.update_config(|c| {
            c.pull_threshold_for_topic = 1000;
            // The user's own per-queue value, deliberately large: the division replaces it.
            c.pull_threshold_for_queue = 65535;
            // Isolation by width, not by 0: a 0 size/span threshold is not an off switch, it would
            // trip first and hide the count branch this test is about.
            c.pull_threshold_size_for_queue = i32::MAX;
            c.consume_concurrently_max_span = i64::MAX;
        });
        own_pull_queues(&consumer, &queues);
        consumer.divide_topic_thresholds();
        assert_eq!(
            consumer.config().pull_threshold_for_queue, 250,
            "1000 divided over the 4 owned queues is the gate the pull path reads"
        );
        for mq in &queues {
            assert!(
                !flow_control_hit(&consumer.inner, &mq_key(mq)),
                "{} at its divided share 250 of the 1000 budget must keep pulling",
                mq_key(mq)
            );
        }
        assert_eq!(consumer.flow_control_triggered(), 0, "1000 buffered in total, none over budget");

        // Control: one queue over its **own** share pauses, on the per-queue count reason.
        let hot = &queues[1];
        stage(
            &consumer,
            hot,
            (0..251).map(|i| sized_msg(100, i)).collect(),
        );
        assert!(
            flow_control_hit(&consumer.inner, &mq_key(hot)),
            "251 > the divided 250 pauses that queue"
        );
        assert_eq!(consumer.flow_control_triggered(), 1, "only the one over-budget queue counts");
        for mq in queues.iter().filter(|q| mq_key(q) != mq_key(hot)) {
            assert!(
                !flow_control_hit(&consumer.inner, &mq_key(mq)),
                "the sibling {} at 250 must stay unpaused while {}",
                mq_key(mq),
                mq_key(hot)
            );
        }
    }

    /// A throttled queue trips on every pull round, so the warn is throttled too: the 1st trip and
    /// then every [`FLOW_CONTROL_WARN_EVERY`]-th after it. The predicate is pinned directly, and the
    /// wiring is pinned against the counter the getter exposes -- a helper tested in isolation but
    /// wired to a different variable would pass the pure assertions and still warn every round.
    #[test]
    fn flow_control_warn_cadence_is_the_first_trip_and_every_1000th_after() {
        for n in [1usize, 1001, 2001] {
            assert!(is_flow_control_warn_trip(n), "trip {n} must log a warn");
        }
        // Control: the trips in between stay on the debug channel only. A cadence degraded to
        // "warn always" or "warn once and stop" is caught here, not by the boundaries above.
        for n in [0usize, 2, 999, 1000, 1002, 2000] {
            assert!(
                !is_flow_control_warn_trip(n),
                "trip {n} must not log a warn"
            );
        }

        let consumer = DefaultMQPushConsumer::new("G_fc_warn_cadence").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        only(&consumer, |c| c.pull_threshold_for_queue = 1);
        stage(&consumer, &mq, vec![sized_msg(100, 0), sized_msg(100, 1)]);
        for round in 1..=1000_usize {
            assert!(
                flow_control_hit(&consumer.inner, &key),
                "round {round} must pause"
            );
        }
        assert_eq!(consumer.flow_control_triggered(), 1000);
        assert!(
            !is_flow_control_warn_trip(consumer.flow_control_triggered()),
            "1000 rounds leave the cadence silent"
        );
        assert!(flow_control_hit(&consumer.inner, &key));
        assert_eq!(consumer.flow_control_triggered(), 1001);
        assert!(
            is_flow_control_warn_trip(consumer.flow_control_triggered()),
            "the 1001st round is the next warn"
        );
    }

    /// The POP path's gate reads **this queue's own** ack debt and nothing else: a queue sitting
    /// exactly on the default 96 still pops, the 97th awaited ack pauses it, and a sibling that
    /// carries no debt stays open while its neighbour is throttled.
    #[test]
    fn pop_gate_judges_this_queue_own_ack_debt() {
        let consumer = DefaultMQPushConsumer::new("G_pop_gate").unwrap();
        let queues = owned_queue_set("T", 2);
        own_pop_queues(&consumer, &queues);
        let pq_of = |mq: &MessageQueue| -> Arc<PopProcessQueue> {
            lock(&consumer.inner.state)
                .pop_queues
                .get(&mq_key(mq))
                .cloned()
                .unwrap()
        };
        let hot_key = mq_key(&queues[0]);
        let cold_key = mq_key(&queues[1]);
        let hot = pq_of(&queues[0]);
        let cold = pq_of(&queues[1]);

        hot.inc_found_msg(96);
        assert!(
            !pop_flow_control_hit(&consumer.inner, &hot_key, &hot),
            "96 awaited acks are not strictly greater than the default threshold 96"
        );
        assert_eq!(
            consumer.flow_control_triggered(),
            0,
            "a round that does not trip counts nothing"
        );
        hot.inc_found_msg(1);
        assert!(
            pop_flow_control_hit(&consumer.inner, &hot_key, &hot),
            "the 97th awaited ack pauses this queue"
        );

        // Paired control for the summing bug: the instance carries 97 awaited acks in total, all of
        // them the hot queue's, so the empty sibling must keep popping.
        assert_eq!(hot.wait_ack_count() + cold.wait_ack_count(), 97);
        assert!(
            !pop_flow_control_hit(&consumer.inner, &cold_key, &cold),
            "the gate never sums its sibling queues"
        );

        // A tightened knob keeps the same strict comparison.
        only(&consumer, |c| c.pop_threshold_for_queue = 3);
        hot.dec_found_msg(94);
        assert_eq!(hot.wait_ack_count(), 3);
        assert!(
            !pop_flow_control_hit(&consumer.inner, &hot_key, &hot),
            "exactly the threshold 3 still pops"
        );
        hot.inc_found_msg(1);
        assert!(
            pop_flow_control_hit(&consumer.inner, &hot_key, &hot),
            "debt 4 > threshold 3 pauses"
        );
    }

    /// POP trips are counted on the pull path's counter, not next to it: one pull hit plus one pop
    /// hit reads 2, and the throttled warn therefore counts the POP rounds too. An implementation
    /// with its own POP counter passes every pure-gate assertion above and still buries the operator
    /// in one warn per round.
    #[test]
    fn pop_and_pull_trips_share_one_flow_control_counter() {
        let consumer = DefaultMQPushConsumer::new("G_pop_fc_counter").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        own_pop_queues(&consumer, std::slice::from_ref(&mq));
        let pq = lock(&consumer.inner.state)
            .pop_queues
            .get(&key)
            .cloned()
            .unwrap();
        only(&consumer, |c| {
            c.pull_threshold_for_queue = 1;
            c.pop_threshold_for_queue = 0;
        });
        pq.inc_found_msg(1);
        stage(&consumer, &mq, vec![sized_msg(100, 0), sized_msg(100, 1)]);

        assert!(flow_control_hit(&consumer.inner, &key), "buffered 2 > 1 pauses the pull");
        assert_eq!(consumer.flow_control_triggered(), 1);
        assert!(
            pop_flow_control_hit(&consumer.inner, &key, &pq),
            "wait ack 1 > 0 pauses the pop"
        );
        assert_eq!(
            consumer.flow_control_triggered(),
            2,
            "one client-wide flow-control count spans the pull and POP paths"
        );

        for round in 3..=1000 {
            assert!(
                pop_flow_control_hit(&consumer.inner, &key, &pq),
                "pop round {round} must still pause"
            );
        }
        assert_eq!(consumer.flow_control_triggered(), 1000);
        assert!(
            !is_flow_control_warn_trip(consumer.flow_control_triggered()),
            "1000 pop trips leave the cadence silent"
        );
        assert!(pop_flow_control_hit(&consumer.inner, &key, &pq));
        assert_eq!(consumer.flow_control_triggered(), 1001);
        assert!(
            is_flow_control_warn_trip(consumer.flow_control_triggered()),
            "the 1001st trip is the next warn"
        );
    }

    // ---------------- topic-level budgets are divided into the per-queue knobs ----------------

    /// Register queues in the **live per-queue pull-state table** exactly as
    /// `rebalance_pull_threads` does (`queue_owners` is the PULL divisor's source table,
    /// `pop_queues` the POP one; `mq_map` mirrors the real registration).
    fn own_pull_queues(consumer: &DefaultMQPushConsumer, queues: &[MessageQueue]) {
        let mut state = lock(&consumer.inner.state);
        for (i, mq) in queues.iter().enumerate() {
            let key = mq_key(mq);
            state.queue_owners.insert(key.clone(), i as u64 + 1);
            state.mq_map.insert(key, mq.clone());
        }
    }

    /// The same, in POP mode: the queue also gets a `PopProcessQueue`, which is what the POP divisor counts.
    fn own_pop_queues(consumer: &DefaultMQPushConsumer, queues: &[MessageQueue]) {
        let mut state = lock(&consumer.inner.state);
        for (i, mq) in queues.iter().enumerate() {
            let key = mq_key(mq);
            state.queue_owners.insert(key.clone(), i as u64 + 1);
            state.pop_queues.insert(key.clone(), PopProcessQueue::new());
            state.mq_map.insert(key, mq.clone());
        }
    }

    fn owned_queue_set(topic: &str, count: usize) -> Vec<MessageQueue> {
        (0..count as i32)
            .map(|id| queue(topic, "broker-a", id))
            .collect()
    }

    /// Each enabled budget divides over the owned queues and moves **only its own** knob; the
    /// disabled one leaves its knob and its log line alone. The wording is pinned literally because
    /// it is the operator's only signal that a per-queue number is no longer the one they set.
    #[test]
    fn divide_topic_thresholds_moves_only_the_knob_of_each_enabled_budget() {
        // Count budget only (the size budget is off at -1). The defaults pin the knobs the user set:
        // `pullThresholdForQueue` 1000, `pullThresholdSizeForQueue` 100.
        let mut cfg = ConsumerConfig {
            pull_threshold_for_topic: 2000,
            ..Default::default()
        };
        let lines = apply_topic_threshold_division(&mut cfg, 4);
        assert_eq!(cfg.pull_threshold_for_queue, 500, "2000 / 4");
        assert_eq!(
            cfg.pull_threshold_size_for_queue, 100,
            "an off size budget must not touch the size knob"
        );
        assert_eq!(
            cfg.pull_threshold_for_topic, 2000,
            "the numerator is never consumed: the budget itself stays as the user set it"
        );
        assert_eq!(
            lines,
            vec!["The pullThresholdForQueue is changed from 1000 to 500".to_string()],
            "exactly one line, for the knob that moved, in the documented wording"
        );

        // Size budget only (the count budget is off).
        let mut cfg = ConsumerConfig {
            pull_threshold_size_for_topic: 600,
            ..Default::default()
        };
        let lines = apply_topic_threshold_division(&mut cfg, 4);
        assert_eq!(cfg.pull_threshold_size_for_queue, 150, "600 / 4");
        assert_eq!(
            cfg.pull_threshold_for_queue, 1000,
            "an off count budget must not touch the count knob"
        );
        assert_eq!(
            lines,
            vec!["The pullThresholdSizeForQueue is changed from 100 to 150".to_string()],
            "no line is produced for the budget that is off"
        );

        // Both budgets: two lines, count first then size.
        let mut cfg = ConsumerConfig {
            pull_threshold_for_topic: 1000,
            pull_threshold_size_for_topic: 100,
            ..Default::default()
        };
        let lines = apply_topic_threshold_division(&mut cfg, 4);
        assert_eq!((cfg.pull_threshold_for_queue, cfg.pull_threshold_size_for_queue), (250, 25));
        assert_eq!(
            lines,
            vec![
                "The pullThresholdForQueue is changed from 1000 to 250".to_string(),
                "The pullThresholdSizeForQueue is changed from 100 to 25".to_string(),
            ]
        );

        // Through the real method: the divisor is the live pull-state table and the getter shows the
        // divided number -- the user's per-queue value is deliberately overwritten while a topic
        // budget is enabled (a getter that still reported 1000 here would mean the gate reads a
        // value nobody enforces).
        let consumer = DefaultMQPushConsumer::new("G_divide_knob").unwrap();
        consumer.update_config(|c| {
            c.pull_threshold_for_queue = 1000;
            c.pull_threshold_size_for_queue = 100;
            c.pull_threshold_for_topic = 2000;
            c.pull_threshold_size_for_topic = -1;
        });
        own_pull_queues(&consumer, &owned_queue_set("T", 3));
        consumer.divide_topic_thresholds();
        let cfg = consumer.config();
        assert_eq!(cfg.pull_threshold_for_queue, 666, "2000 / 3 owned queues");
        assert_eq!(
            cfg.pull_threshold_size_for_queue, 100,
            "the size budget is off, so the size knob keeps the user's 100"
        );
    }

    /// Control for every division assertion above: with both budgets off (`-1` is the off spelling
    /// in this port) the knobs stay exactly where the user put them even though queues are owned.
    #[test]
    fn divide_topic_thresholds_leaves_both_knobs_alone_when_both_budgets_are_off() {
        let mut cfg = ConsumerConfig {
            pull_threshold_for_queue: 77,
            pull_threshold_size_for_queue: 9,
            ..Default::default()
        };
        let lines = apply_topic_threshold_division(&mut cfg, 4);
        assert!(lines.is_empty(), "an off budget logs nothing at all");
        assert_eq!(
            (cfg.pull_threshold_for_queue, cfg.pull_threshold_size_for_queue),
            (77, 9),
            "off budgets must not rewrite the knobs"
        );

        let consumer = DefaultMQPushConsumer::new("G_divide_off").unwrap();
        consumer.update_config(|c| {
            c.pull_threshold_for_queue = 77;
            c.pull_threshold_size_for_queue = 9;
            c.pull_threshold_for_topic = -1;
            c.pull_threshold_size_for_topic = -1;
        });
        own_pull_queues(&consumer, &owned_queue_set("T", 3));
        consumer.divide_topic_thresholds();
        let cfg = consumer.config();
        assert_eq!((cfg.pull_threshold_for_queue, cfg.pull_threshold_size_for_queue), (77, 9));
    }

    /// A budget smaller than the owned queue count floors at **1, never 0**: the count gate pauses a
    /// pull as soon as the buffer is strictly greater than its (max-1-clamped) threshold, so a 0
    /// would still throttle -- but 1 is the documented floor and keeps one message flowing.
    #[test]
    fn divide_topic_thresholds_floors_a_small_budget_at_one_not_zero() {
        let mut cfg = ConsumerConfig {
            pull_threshold_for_topic: 3,
            pull_threshold_size_for_topic: 1,
            ..Default::default()
        };
        let lines = apply_topic_threshold_division(&mut cfg, 8);
        assert_eq!(cfg.pull_threshold_for_queue, 1, "3 / 8 = 0 must be floored to 1");
        assert_eq!(cfg.pull_threshold_size_for_queue, 1, "1 / 8 = 0 must be floored to 1");
        assert_eq!(
            lines,
            vec![
                "The pullThresholdForQueue is changed from 1000 to 1".to_string(),
                "The pullThresholdSizeForQueue is changed from 100 to 1".to_string(),
            ],
            "the floor is what the operator is told the knob became"
        );

        // The floored knob is the value the gate actually uses: one message still pulls, the second pauses.
        let consumer = DefaultMQPushConsumer::new("G_divide_floor").unwrap();
        consumer.update_config(|c| {
            c.pull_threshold_for_topic = 3;
            c.pull_threshold_size_for_topic = -1;
            c.pull_threshold_size_for_queue = 0;
            c.consume_concurrently_max_span = 0;
        });
        own_pull_queues(&consumer, &owned_queue_set("T", 8));
        consumer.divide_topic_thresholds();
        assert_eq!(consumer.config().pull_threshold_for_queue, 1);
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        stage(&consumer, &mq, vec![sized_msg(100, 0)]);
        assert!(
            !flow_control_hit(&consumer.inner, &key),
            "the floored knob of 1 still lets one message sit in the buffer"
        );
        stage(&consumer, &mq, vec![sized_msg(100, 0), sized_msg(100, 1)]);
        assert!(
            flow_control_hit(&consumer.inner, &key),
            "the second message is over the divided gate"
        );
    }

    /// Owning nothing must not divide by zero: the method returns before touching either knob, so
    /// both keep exactly the values the user set. The control is the same config with one owned
    /// queue, where both knobs do move -- otherwise "nothing happened" could also mean "the division
    /// is not wired up at all".
    #[test]
    fn divide_topic_thresholds_with_no_owned_queue_keeps_the_users_values() {
        let consumer = DefaultMQPushConsumer::new("G_divide_zero_owned").unwrap();
        consumer.update_config(|c| {
            c.pull_threshold_for_queue = 1000;
            c.pull_threshold_size_for_queue = 100;
            c.pull_threshold_for_topic = 2000;
            c.pull_threshold_size_for_topic = 50;
        });
        consumer.divide_topic_thresholds();
        let cfg = consumer.config();
        assert_eq!(
            (cfg.pull_threshold_for_queue, cfg.pull_threshold_size_for_queue),
            (1000, 100),
            "no owned queue = no division, the user's values survive"
        );

        own_pull_queues(&consumer, &owned_queue_set("T", 1));
        consumer.divide_topic_thresholds();
        let cfg = consumer.config();
        assert_eq!(
            (cfg.pull_threshold_for_queue, cfg.pull_threshold_size_for_queue),
            (2000, 50),
            "control: one owned queue divides both budgets"
        );
    }

    /// The numerator is ALWAYS the topic budget, so the division is idempotent and follows only the
    /// queue count: 4 queues -> 1000/4 = 250, 8 queues -> 1000/8 = **125**, not 250/8 = 31.
    #[test]
    fn divide_topic_thresholds_is_idempotent_and_tracks_the_queue_count() {
        let mut cfg = ConsumerConfig {
            pull_threshold_for_topic: 1000,
            ..Default::default()
        };
        assert_eq!(apply_topic_threshold_division(&mut cfg, 4).len(), 1);
        assert_eq!(cfg.pull_threshold_for_queue, 250);

        let lines = apply_topic_threshold_division(&mut cfg, 4);
        assert_eq!(
            cfg.pull_threshold_for_queue, 250,
            "a second round with the same count must not divide the already-divided knob"
        );
        assert!(lines.is_empty(), "an unchanged quotient logs no line");

        let lines = apply_topic_threshold_division(&mut cfg, 8);
        assert_eq!(
            cfg.pull_threshold_for_queue, 125,
            "1000/8 = 125; 31 would mean the divisor was fed the current knob"
        );
        assert_eq!(
            lines,
            vec!["The pullThresholdForQueue is changed from 250 to 125".to_string()],
            "<old> in the line is the knob as it stood, not the topic budget"
        );

        // Same contract through the consumer, where the count genuinely changes between rounds.
        let consumer = DefaultMQPushConsumer::new("G_divide_tracks").unwrap();
        consumer.update_config(|c| {
            c.pull_threshold_for_queue = 1000;
            c.pull_threshold_for_topic = 1000;
            c.pull_threshold_size_for_topic = -1;
        });
        own_pull_queues(&consumer, &owned_queue_set("T", 4));
        consumer.divide_topic_thresholds();
        assert_eq!(consumer.config().pull_threshold_for_queue, 250);
        consumer.divide_topic_thresholds();
        assert_eq!(
            consumer.config().pull_threshold_for_queue, 250,
            "repeating with an unchanged owned set writes the same number"
        );
        own_pull_queues(&consumer, &(4..8).map(|id| queue("T", "broker-a", id)).collect::<Vec<_>>());
        consumer.divide_topic_thresholds();
        assert_eq!(consumer.config().pull_threshold_for_queue, 125);
    }

    /// The divisor is this consumer's **whole** load: every queue it owns, across all subscribed
    /// topics. A per-topic divisor (here: 2 queues of `T`) would write 500 instead of 333.
    #[test]
    fn divide_topic_thresholds_divisor_counts_queues_across_all_topics() {
        let consumer = DefaultMQPushConsumer::new("G_divide_all_topics").unwrap();
        consumer.update_config(|c| {
            c.pull_threshold_for_topic = 1000;
            c.pull_threshold_size_for_topic = 100;
        });
        let mut owned = owned_queue_set("T", 2);
        owned.push(queue("Other", "broker-b", 0));
        own_pull_queues(&consumer, &owned);
        consumer.divide_topic_thresholds();
        let cfg = consumer.config();
        assert_eq!(
            cfg.pull_threshold_for_queue, 333,
            "1000 / 3 owned queues (2 topics), not 1000 / 2 queues of one topic"
        );
        assert_eq!(
            cfg.pull_threshold_size_for_queue, 33,
            "the size budget divides by the same all-topic count"
        );
    }

    /// POP mode divides over the POP per-queue table (`pop_queues`), not the PULL one. Here the two
    /// tables deliberately disagree: 4 pull-owned queues vs 2 POP queues, so the two answers differ.
    #[test]
    fn divide_topic_thresholds_uses_the_pop_table_in_pop_mode() {
        let consumer = DefaultMQPushConsumer::new("G_divide_pop").unwrap();
        consumer.update_config(|c| {
            c.pop_mode = true;
            c.pull_threshold_for_queue = 1000;
            c.pull_threshold_for_topic = 1000;
            c.pull_threshold_size_for_topic = -1;
        });
        own_pull_queues(&consumer, &owned_queue_set("T", 4));
        own_pop_queues(&consumer, &owned_queue_set("T", 2));
        consumer.divide_topic_thresholds();
        assert_eq!(
            consumer.config().pull_threshold_for_queue, 500,
            "POP divides over the 2 POP queues; 250 would mean it read the pull table"
        );

        // Control: the identical tables under PULL mode divide by the 4 pull-owned queues.
        consumer.update_config(|c| {
            c.pop_mode = false;
            c.pull_threshold_for_queue = 1000;
        });
        consumer.divide_topic_thresholds();
        assert_eq!(consumer.config().pull_threshold_for_queue, 250);
    }

    /// The moved-set judgement is per topic and keyed on the **set** of queue keys, so the four
    /// shapes that matter all read as a move and the one shape that must stay quiet does: a
    /// re-ordering of the queues this instance already had. A same-count hand-over to a different
    /// broker is the case a count comparison would miss.
    #[test]
    fn changed_topics_names_exactly_the_sets_that_moved() {
        let q = |topic: &str, broker: &str, id: i32| MessageQueue::new(topic, broker, id);
        let t1 = || vec![q("T1", "broker-a", 0), q("T1", "broker-a", 1)];
        let by_topic = |qs: &[MessageQueue]| queues_by_topic(qs);

        // Same set, different arrival order: nothing moved.
        let mut reordered = t1();
        reordered.reverse();
        assert!(
            changed_topics(&by_topic(&t1()), &by_topic(&reordered)).is_empty(),
            "the strategy's sort order is not an ownership change"
        );
        // Identical round: nothing moved.
        assert!(changed_topics(&by_topic(&t1()), &by_topic(&t1())).is_empty());

        // A growth and a shrink each name the topic once.
        assert_eq!(
            changed_topics(&by_topic(&t1()), &by_topic(&[t1(), vec![q("T1", "broker-a", 2)]].concat())),
            vec!["T1".to_string()],
            "one queue added"
        );
        assert_eq!(
            changed_topics(&by_topic(&t1()), &by_topic(&[q("T1", "broker-a", 1)])),
            vec!["T1".to_string()],
            "one queue lost"
        );

        // Same count, different broker: the queues changed hands, and a count comparison would miss
        // exactly this.
        let hand_over = vec![q("T1", "broker-b", 0), q("T1", "broker-b", 1)];
        assert_eq!(
            changed_topics(&by_topic(&t1()), &by_topic(&hand_over)),
            vec!["T1".to_string()],
            "two queues in, two queues out, but a different broker"
        );

        // A topic that appears and one that vanishes are both moves (the empty side is the before).
        assert_eq!(
            changed_topics(
                &by_topic(&[] as &[MessageQueue]),
                &by_topic(&t1())
            ),
            vec!["T1".to_string()],
            "a topic the instance had no queues for now has some"
        );
        assert_eq!(
            changed_topics(&by_topic(&t1()), &by_topic(&[] as &[MessageQueue])),
            vec!["T1".to_string()],
            "and the reverse"
        );

        // Two topics, one moved: only that one is named, and the names come back sorted.
        let before = {
            let mut v = t1();
            v.extend(vec![q("T2", "broker-a", 0)]);
            v
        };
        let after = {
            let mut v = vec![q("T1", "broker-a", 0), q("T1", "broker-a", 1)];
            v.extend(vec![q("T2", "broker-a", 0), q("T2", "broker-a", 1)]);
            v
        };
        assert_eq!(
            changed_topics(&by_topic(&before), &by_topic(&after)),
            vec!["T2".to_string()],
            "T1 kept the identical two queues while T2 grew"
        );
    }

    /// The restamp is the half that makes the broker look at the subscription again, so its scope is
    /// the moved set and nothing else: one line per moved topic, carrying the version it replaced and
    /// the round's single stamp.
    #[test]
    fn restamp_subscription_versions_rewrites_only_the_moved_topics() {
        let consumer = DefaultMQPushConsumer::new("G_restamp").unwrap();
        consumer.subscribe("T1", "*").unwrap();
        consumer.subscribe("T2", "tagA").unwrap();
        {
            let mut state = lock(&consumer.inner.state);
            for (_, sub) in state.subscription_data.iter_mut() {
                sub.sub_version = 100;
            }
        }

        let lines = restamp_subscription_versions(&consumer.inner, &["T2".to_string()], 999);
        assert_eq!(
            lines,
            vec!["T2 rebalance changed, also update version: 100, 999".to_string()],
            "one line, naming the old version then the new one"
        );
        assert_eq!(subscription_version(&consumer, "T2"), 999);
        assert_eq!(
            subscription_version(&consumer, "T1"),
            100,
            "a subscription whose queues did not move keeps its version -- re-stamping the whole \
             table would put every subscription back on the wire each changed round"
        );

        // A moved topic with no subscription entry (a share that vanished with its subscribe) is
        // skipped rather than invented, and an empty moved list writes nothing at all.
        assert!(restamp_subscription_versions(&consumer.inner, &["T3".to_string()], 1000).is_empty());
        assert_eq!(subscription_versions(&consumer).len(), 2, "no subscription was created");
        assert!(restamp_subscription_versions(&consumer.inner, &[], 1000).is_empty());
        assert_eq!(subscription_version(&consumer, "T2"), 999, "an empty list restamps nothing");
    }

    /// The division is **change-gated** and runs **after** the pull-state update, through the real
    /// `do_rebalance` against the in-process fake cluster:
    /// - an unchanged round leaves a hand-set knob alone (it does not divide every 20s tick);
    /// - a shrinking round divides by the count the round just produced (1), which also pins the
    ///   order: running before `rebalance_pull_threads` would still see the 2 queues it replaced.
    #[tokio::test]
    async fn topic_threshold_division_is_change_gated_and_runs_after_the_pull_state_update() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster("pop-divide-order", &cluster);
        c.update_config(|cfg| {
            cfg.pull_threshold_for_queue = 1000;
            cfg.pull_threshold_for_topic = 1000;
            cfg.pull_threshold_size_for_topic = -1;
        });

        // Round 1: the default script grants queues 0/1, so the table this round filled gives 1000/2.
        c.do_rebalance().await.expect("rebalance must not error");
        assert_eq!(
            lock(&c.inner.state).pop_queues.len(),
            2,
            "the divisor source table is filled by this round's pull-state update"
        );
        assert_eq!(
            c.config().pull_threshold_for_queue, 500,
            "1000 / the 2 queues this round owns"
        );

        // Round 2: same assignment -> `changed` is false, so a hand-set knob survives.
        c.update_config(|cfg| cfg.pull_threshold_for_queue = 12345);
        c.do_rebalance().await.unwrap();
        assert_eq!(
            c.config().pull_threshold_for_queue, 12345,
            "an unchanged assignment must not re-run the division"
        );

        // Round 3: the set shrinks 2 -> 1, so the divisor is the NEW count.
        cluster.set(Some(AssignFixture::with_assignments(&[(
            "broker-a", 7, "POP",
        )])));
        c.do_rebalance().await.unwrap();
        assert_eq!(lock(&c.inner.state).pop_queues.len(), 1, "the shrunk table");
        assert_eq!(
            c.config().pull_threshold_for_queue, 1000,
            "1000 / 1; 500 would mean the division ran before the pull-state update, on the old count of 2"
        );
    }

    #[test]
    fn msg_acc_cnt_uses_the_last_message_only() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let key = mq_key(&queue("T", "broker-a", 0));
        let key = key.as_str();
        let table = || lock(&consumer.inner.state).msg_acc_cnt.clone();

        // Empty batch / missing MAX_OFFSET property: table left untouched
        update_msg_acc_cnt(&consumer.inner, key, &[]);
        assert!(table().is_empty());
        update_msg_acc_cnt(&consumer.inner, key, &[ext("T", None)]);
        assert!(table().is_empty());

        let with_max = |offset: i64, max: &str| {
            let mut m = ext("T", None);
            m.queue_offset = offset;
            m.put_property(PROPERTY_MAX_OFFSET, max);
            m
        };
        // Take the last message: 100 - 90 = 10 (not the first message's 100 - 10 = 90)
        update_msg_acc_cnt(
            &consumer.inner,
            key,
            &[with_max(10, "100"), with_max(90, "100")],
        );
        assert_eq!(table().get(key), Some(&10));

        // Delta <= 0 does not update (keeps the previous value); a non-numeric MAX_OFFSET is ignored
        update_msg_acc_cnt(&consumer.inner, key, &[with_max(100, "100")]);
        assert_eq!(table().get(key), Some(&10));
        update_msg_acc_cnt(&consumer.inner, key, &[with_max(100, "abc")]);
        assert_eq!(table().get(key), Some(&10));
        update_msg_acc_cnt(&consumer.inner, key, &[with_max(10, "1000")]);
        assert_eq!(table().get(key), Some(&990));
    }

    // ---------------- ackIndex of concurrent consumption (classic path) ----------------

    /// These asserts can **only** run offline: here a send-back on an instance that never `start()`ed always fails
    /// (`require_client` returns Err directly), so the visible outcome of the "send-back failed" branch --
    /// tail pushed back to the queue head, `reconsumeTimes+1`, offset not passing it -- is exactly the part easiest to write wrong.
    /// The semantics when send-back **succeeds** (tail handed to the broker, offset advances by the whole batch) is
    /// evidenced on a live cluster by `examples/live_ack_index.rs`.
    struct AckListener {
        ack_index: Option<i32>,
        status: ConsumeConcurrentlyStatus,
    }

    impl MessageListenerConcurrently for AckListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            context: &mut ConsumeConcurrentlyContext,
        ) -> ConsumeConcurrentlyStatus {
            if let Some(i) = self.ack_index {
                context.ack_index = i;
            }
            self.status
        }
    }

    /// A batch with queueOffset = 0..n on the same queue.
    fn offset_batch(n: i64) -> Vec<MessageExt> {
        (0..n)
            .map(|i| {
                let mut m = ext("T", None);
                m.broker_name = Some("broker-a".to_string());
                m.queue_offset = i;
                m
            })
            .collect()
    }

    struct AckHarness {
        c: DefaultMQPushConsumer,
        key: String,
        mq: MessageQueue,
    }

    impl AckHarness {
        fn new(
            broadcast: bool,
            ack_index: Option<i32>,
            status: ConsumeConcurrentlyStatus,
        ) -> AckHarness {
            let cfg = ConsumerConfig {
                consumer_group: "G".to_string(),
                message_model: if broadcast {
                    MessageModel::BROADCASTING.to_string()
                } else {
                    MessageModel::CLUSTERING.to_string()
                },
                ..Default::default()
            };
            let c = DefaultMQPushConsumer::with_config(cfg).unwrap();
            c.set_message_listener_concurrently(Arc::new(AckListener { ack_index, status }));
            let mq = queue("T", "broker-a", 0);
            let key = mq_key(&mq);
            lock(&c.inner.state)
                .pending
                .insert(key.clone(), VecDeque::new());
            AckHarness { c, key, mq }
        }

        async fn run(&self, n: i64) -> bool {
            consume_batch(&self.c.inner, &self.key, &self.mq, offset_batch(n), None)
                .await
                .expect("consume_batch must not error")
        }

        fn offset(&self) -> i64 {
            lock(&self.c.inner.state)
                .consume_offsets
                .get(&self.key)
                .copied()
                .unwrap_or(0)
        }

        fn pending(&self) -> Vec<(i64, i32)> {
            lock(&self.c.inner.state)
                .pending
                .get(&self.key)
                .map(|dq| {
                    dq.iter()
                        .map(|m| (m.queue_offset, m.reconsume_times))
                        .collect()
                })
                .unwrap_or_default()
        }
    }

    #[tokio::test]
    async fn success_with_default_ack_index_never_send_backs() {
        // Default Integer.MAX_VALUE clamps to size-1: the whole batch acked, none sent back
        let h = AckHarness::new(false, None, ConsumeConcurrentlyStatus::ConsumeSuccess);
        assert!(h.run(3).await);
        assert_eq!(h.offset(), 3);
        assert_eq!(h.pending(), vec![]);
    }

    #[tokio::test]
    async fn success_acks_prefix_and_holds_the_offset_at_the_first_unacked() {
        let h = AckHarness::new(false, Some(0), ConsumeConcurrentlyStatus::ConsumeSuccess);
        // Without start() here, the tail's send-back always fails -> the "push back to queue head + offset does not pass" branch
        assert!(!h.run(3).await);
        assert_eq!(
            h.pending(),
            vec![(1, 1), (2, 1)],
            "the unacked tail must have reconsumeTimes bumped before redelivery"
        );
        assert_eq!(h.offset(), 1, "the offset may only stop at the first unacked message");
    }

    #[tokio::test]
    async fn reconsume_later_overrides_a_wider_ack_index() {
        let h = AckHarness::new(false, Some(2), ConsumeConcurrentlyStatus::ReconsumeLater);
        assert!(!h.run(3).await);
        assert_eq!(h.pending(), vec![(0, 1), (1, 1), (2, 1)]);
        assert_eq!(h.offset(), 0, "the whole batch went unacked, the offset stays put");
    }

    #[tokio::test]
    async fn broadcasting_drops_the_tail_without_send_back() {
        let h = AckHarness::new(true, Some(0), ConsumeConcurrentlyStatus::ConsumeSuccess);
        assert!(h.run(3).await);
        assert_eq!(h.offset(), 3);
        assert_eq!(h.pending(), vec![]);
    }

    #[tokio::test]
    async fn broadcasting_reconsume_later_still_advances() {
        let h = AckHarness::new(true, None, ConsumeConcurrentlyStatus::ReconsumeLater);
        assert!(h.run(2).await);
        assert_eq!(h.offset(), 2);
        assert_eq!(h.pending(), vec![]);
    }

    // ---------------- orderly consumption's checkReconsumeTimes ----------------

    /// All three breakages are **silent**: missing +1 never reaches the threshold (a poison message spins in place
    /// forever); reading `-1` as 16 invents dead letters for orderly consumption; still suspending after a successful
    /// send-back lets one poison message hold the queue forever -- on a live cluster indistinguishable from "consumer down".
    /// So the criteria are pinned offline; only the "send-back succeeds -> offset advances" branch (unreachable without `start()`) goes to `examples/live_consumer.rs`.
    struct OrderlyListener {
        status: ConsumeOrderlyStatus,
        auto_commit: Option<bool>,
    }

    impl MessageListenerOrderly for OrderlyListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            context: &mut ConsumeOrderlyContext,
        ) -> ConsumeOrderlyStatus {
            if let Some(auto) = self.auto_commit {
                context.auto_commit = auto;
            }
            self.status
        }
    }

    /// Records the `(status, success, ConsumeContextType)` seen by the after hook.
    struct RecordingHook {
        after: Mutex<Vec<(String, bool, String)>>,
    }

    impl ConsumeMessageHook for RecordingHook {
        fn hook_name(&self) -> &str {
            "recording-hook"
        }

        fn consume_message_after(&self, ctx: &mut ConsumeMessageContext) -> Result<()> {
            let context_type = ctx
                .props
                .as_ref()
                .and_then(|p| p.get("ConsumeContextType").cloned())
                .unwrap_or_default();
            lock(&self.after).push((
                ctx.status.clone().unwrap_or_default(),
                ctx.success,
                context_type,
            ));
            Ok(())
        }
    }

    struct OrderlyHarness {
        c: DefaultMQPushConsumer,
        key: String,
        mq: MessageQueue,
    }

    impl OrderlyHarness {
        fn new(max_reconsume_times: i32, status: ConsumeOrderlyStatus) -> OrderlyHarness {
            OrderlyHarness::build(max_reconsume_times, status, None)
        }

        fn new_manual(max_reconsume_times: i32, status: ConsumeOrderlyStatus) -> OrderlyHarness {
            OrderlyHarness::build(max_reconsume_times, status, Some(false))
        }

        fn build(
            max_reconsume_times: i32,
            status: ConsumeOrderlyStatus,
            auto_commit: Option<bool>,
        ) -> OrderlyHarness {
            let cfg = ConsumerConfig {
                consumer_group: "G".to_string(),
                max_reconsume_times,
                suspend_current_queue_time_millis: 0,
                ..Default::default()
            };
            let c = DefaultMQPushConsumer::with_config(cfg).unwrap();
            c.set_message_listener_orderly(Arc::new(OrderlyListener {
                status,
                auto_commit,
            }));
            let mq = queue("T", "broker-a", 0);
            let key = mq_key(&mq);
            lock(&c.inner.state)
                .pending
                .insert(key.clone(), VecDeque::new());
            OrderlyHarness { c, key, mq }
        }

        fn with_hook(self, hook: Arc<dyn ConsumeMessageHook>) -> OrderlyHarness {
            self.c.register_consume_message_hook(hook);
            self
        }

        async fn run(&self, batch: Vec<MessageExt>) -> bool {
            consume_batch(&self.c.inner, &self.key, &self.mq, batch, None)
                .await
                .expect("consume_batch must not error")
        }

        fn offset(&self) -> i64 {
            lock(&self.c.inner.state)
                .consume_offsets
                .get(&self.key)
                .copied()
                .unwrap_or(0)
        }

        fn pending(&self) -> Vec<(i64, i32)> {
            lock(&self.c.inner.state)
                .pending
                .get(&self.key)
                .map(|dq| {
                    dq.iter()
                        .map(|m| (m.queue_offset, m.reconsume_times))
                        .collect()
                })
                .unwrap_or_default()
        }
    }

    /// queueOffset = `start`..`start+n`, each carrying its own reconsumeTimes.
    fn orderly_batch(start: i64, reconsume_times: &[i32]) -> Vec<MessageExt> {
        reconsume_times
            .iter()
            .enumerate()
            .map(|(i, rt)| {
                let i = i64::try_from(i).expect("batch length never exceeds i64");
                let mut m = ext("T", None);
                m.broker_name = Some("broker-a".to_string());
                m.queue_offset = start + i;
                m.reconsume_times = *rt;
                m.msg_id = Some(format!("mid-{}", start + i));
                m
            })
            .collect()
    }

    fn cfg_with(max_reconsume_times: i32) -> ConsumerConfig {
        ConsumerConfig {
            consumer_group: "G".to_string(),
            max_reconsume_times,
            ..Default::default()
        }
    }

    #[test]
    fn orderly_minus_one_means_unlimited_not_the_concurrent_sixteen() {
        assert_eq!(orderly_max_reconsume_times(&cfg_with(-1)), i32::MAX);
        assert_eq!(orderly_max_reconsume_times(&cfg_with(3)), 3);
    }

    #[tokio::test]
    async fn default_cap_never_judges_the_poison_message_exhausted() {
        // With the default config (-1), even reconsumeTimes at i32::MAX-1 only bumps locally by +1 and keeps
        // suspending to retry, never sending back (let alone the broker routing it into %DLQ%).
        let h = OrderlyHarness::new(-1, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        let inner = h.c.inner.clone();
        let mut batch = orderly_batch(0, &[i32::MAX - 1]);
        assert!(check_orderly_reconsume_times(&inner, &mut batch).await);
        assert_eq!(batch[0].reconsume_times, i32::MAX);
    }

    #[tokio::test]
    async fn below_cap_counts_locally_and_suspends() {
        // Below the cap it never sends back (it cannot without start either, so this criterion must call the check function itself)
        let h = OrderlyHarness::new(3, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        let inner = h.c.inner.clone();
        let mut batch = orderly_batch(0, &[0, 2]);
        assert!(check_orderly_reconsume_times(&inner, &mut batch).await);
        assert_eq!(
            batch.iter().map(|m| m.reconsume_times).collect::<Vec<_>>(),
            vec![1, 3],
            "the broker did not record this failure, so without a local +1 the client never reaches the threshold"
        );
    }

    #[tokio::test]
    async fn at_cap_with_failed_send_back_still_suspends() {
        let h = OrderlyHarness::new(2, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        let inner = h.c.inner.clone();
        let mut batch = orderly_batch(7, &[2]);
        assert!(check_orderly_reconsume_times(&inner, &mut batch).await);
        assert_eq!(batch[0].reconsume_times, 3);
    }

    #[tokio::test]
    async fn empty_batch_does_not_suspend() {
        let h = OrderlyHarness::new(0, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        let inner = h.c.inner.clone();
        let mut empty: Vec<MessageExt> = Vec::new();
        assert!(!check_orderly_reconsume_times(&inner, &mut empty).await);
    }

    #[test]
    fn retry_message_carries_exactly_the_contract_fields() {
        let cfg = cfg_with(2);
        let poison = {
            let mut m = orderly_batch(7, &[2]).remove(0);
            m.put_property(PROPERTY_TRANSACTION_PREPARED, "true");
            m.put_property(PROPERTY_KEYS, "k7");
            m
        };
        let new_msg = build_retry_message(
            &cfg.consumer_group,
            &poison,
            orderly_max_reconsume_times(&cfg),
        );
        assert_eq!(new_msg.topic, "%RETRY%G", "orderly send-back goes to %RETRY%<group>");
        assert_eq!(new_msg.body, poison.body);
        assert_eq!(new_msg.get_property(PROPERTY_KEYS), Some("k7"));
        assert_eq!(new_msg.get_property(PROPERTY_RETRY_TOPIC), Some("T"));
        assert_eq!(new_msg.get_property(PROPERTY_RECONSUME_TIME), Some("3"));
        assert_eq!(
            new_msg.get_property(PROPERTY_MAX_RECONSUME_TIMES),
            Some("2")
        );
        assert_eq!(
            new_msg.get_property("DELAY"),
            Some("5"),
            "delayLevel = 3 + 2"
        );
        assert_eq!(
            new_msg.get_property(PROPERTY_ORIGIN_MESSAGE_ID),
            Some("mid-7"),
            "the trace needs ORIGIN_MESSAGE_ID to link the redelivery back to the original message"
        );
        assert_eq!(
            new_msg.get_property(PROPERTY_TRANSACTION_PREPARED),
            None,
            "the half-message flag must be removed, or the broker treats it as a check-back message"
        );
    }

    #[tokio::test]
    async fn suspend_requeues_batch_while_below_cap() {
        let h = OrderlyHarness::new(3, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        // The queue tail still holds the next message (offset 2); this batch must be inserted before it in original order
        let mut tail: VecDeque<MessageExt> = orderly_batch(2, &[0]).into_iter().collect();
        lock(&h.c.inner.state)
            .pending
            .get_mut(&h.key)
            .expect("pending")
            .append(&mut tail);
        assert!(!h.run(orderly_batch(0, &[0, 1])).await);
        assert_eq!(h.offset(), 0, "still retrying in place, the offset must not pass it");
        assert_eq!(
            h.pending(),
            vec![(0, 1), (1, 2), (2, 0)],
            "the whole batch is pushed back to the queue head in original order (orderly consumption forbids reordering)"
        );
    }

    #[tokio::test]
    async fn success_path_skips_the_reconsume_gate() {
        // Threshold 0: merely reaching the gate would attempt a send-back (always fails offline -> suspend), so
        // "the offset advances" by itself proves SUCCESS never touched the gate.
        let h = OrderlyHarness::new(0, ConsumeOrderlyStatus::Success);
        assert!(h.run(orderly_batch(0, &[0])).await);
        assert_eq!(h.offset(), 1);
        assert_eq!(h.pending(), vec![]);
    }

    // ---------------- suspend duration: context wins + clamped to [10, 30000] ----------------

    #[test]
    fn orderly_suspend_millis_resolves_then_clamps() {
        let cfg = |ms: i64| ConsumerConfig {
            suspend_current_queue_time_millis: ms,
            ..Default::default()
        };
        let ctx = |ms: i64| {
            let mut c = ConsumeOrderlyContext::new(None);
            c.suspend_current_queue_time_millis = ms;
            c
        };
        assert_eq!(
            orderly_suspend_millis(&cfg(1000), &ctx(-1)),
            1000,
            "-1 = use the config"
        );
        assert_eq!(orderly_suspend_millis(&cfg(250), &ctx(-1)), 250);
        assert_eq!(
            orderly_suspend_millis(&cfg(0), &ctx(-1)),
            10,
            "the config side goes through the clamp too"
        );
        assert_eq!(orderly_suspend_millis(&cfg(60_000), &ctx(-1)), 30_000);
        assert_eq!(
            orderly_suspend_millis(&cfg(900), &ctx(70)),
            70,
            "a given value overrides the config"
        );
        for (asked, expect) in [
            (0i64, 10u64),
            (9, 10),
            (10, 10),
            (250, 250),
            (30_000, 30_000),
            (30_001, 30_000),
            (i64::MAX, 30_000),
        ] {
            assert_eq!(
                orderly_suspend_millis(&cfg(1000), &ctx(asked)),
                expect,
                "asked={asked}"
            );
        }
    }

    // ---------------- autoCommit=true: COMMIT/ROLLBACK are illegal usages ----------------

    #[tokio::test]
    async fn illegal_commit_and_rollback_under_auto_commit_are_acked() {
        for status in [ConsumeOrderlyStatus::Commit, ConsumeOrderlyStatus::Rollback] {
            let h = OrderlyHarness::new(0, status);
            assert!(
                h.run(orderly_batch(0, &[0])).await,
                "{status:?} must be treated as a whole-batch ack"
            );
            assert_eq!(h.offset(), 1, "{status:?} the offset must advance");
            assert_eq!(h.pending(), vec![], "{status:?} must not send back");
        }
    }


    #[tokio::test]
    async fn manual_commit_advances_without_requeueing() {
        let h = OrderlyHarness::new_manual(0, ConsumeOrderlyStatus::Commit);
        assert!(h.run(orderly_batch(0, &[0])).await);
        assert_eq!(h.offset(), 1, "COMMIT means committing the offset");
        assert_eq!(h.pending(), vec![]);
    }

    #[tokio::test]
    async fn manual_rollback_requeues_and_holds_the_offset() {
        let h = OrderlyHarness::new_manual(0, ConsumeOrderlyStatus::Rollback);
        assert!(!h.run(orderly_batch(0, &[0])).await);
        assert_eq!(h.offset(), 0, "rollback must not commit the offset");
        assert_eq!(h.pending(), vec![(0, 0)], "the message returns to the queue head for retry");
    }

    #[tokio::test]
    async fn manual_success_holds_the_batch_without_committing() {
        let h = OrderlyHarness::new_manual(0, ConsumeOrderlyStatus::Success);
        // Both are reconsumeTimes=0: in manual mode SUCCESS must not touch even the local counter.
        // The batch is finished (not requeued) but the offset stays where it was.
        assert!(h.run(orderly_batch(0, &[0, 0])).await);
        assert_eq!(h.offset(), 0, "in manual mode SUCCESS does not commit");
        assert!(
            h.pending().is_empty(),
            "in manual mode SUCCESS must not put the batch back"
        );
    }

    #[tokio::test]
    async fn manual_suspend_never_commits_the_offset() {
        let h = OrderlyHarness::new_manual(2, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        assert!(!h.run(orderly_batch(7, &[2])).await);
        assert_eq!(h.offset(), 0, "manual mode suspends without committing the offset");
    }

    #[tokio::test]
    async fn orderly_hook_gets_raw_status_and_normalized_success() {
        let cases = [
            (ConsumeOrderlyStatus::Commit, "COMMIT", true),
            (ConsumeOrderlyStatus::Rollback, "ROLLBACK", false),
            (
                ConsumeOrderlyStatus::SuspendCurrentQueueAMoment,
                "SUSPEND_CURRENT_QUEUE_A_MOMENT",
                false,
            ),
            (ConsumeOrderlyStatus::Success, "SUCCESS", true),
        ];
        for (status, expect_status, expect_success) in cases {
            let hook = Arc::new(RecordingHook {
                after: Mutex::new(Vec::new()),
            });
            let h = OrderlyHarness::new(0, status).with_hook(hook.clone());
            let _ = h.run(orderly_batch(0, &[0])).await;
            let seen = lock(&hook.after);
            assert_eq!(seen.len(), 1, "{status:?}: the after hook must fire once");
            assert_eq!(seen[0].0, expect_status, "{status:?} hook status");
            assert_eq!(seen[0].1, expect_success, "{status:?} hook success");
        }
    }

    // ---------------- 309 CONSUME_MESSAGE_DIRECTLY ----------------

    #[test]
    fn direct_consume_maps_orderly_commit_rollback_and_auto_commit() {
        let cases = [
            (ConsumeOrderlyStatus::Success, CMResult::CR_SUCCESS),
            (
                ConsumeOrderlyStatus::SuspendCurrentQueueAMoment,
                CMResult::CR_LATER,
            ),
            (ConsumeOrderlyStatus::Commit, CMResult::CR_COMMIT),
            (ConsumeOrderlyStatus::Rollback, CMResult::CR_ROLLBACK),
        ];
        for (status, expect) in cases {
            let h = OrderlyHarness::new_manual(0, status);
            let r =
                h.c.consume_message_directly(
                    orderly_batch(0, &[0]).remove(0),
                    Some("broker-a".to_string()),
                )
                .expect("direct consume must not error");
            assert_eq!(
                r.consume_result.as_deref(),
                Some(expect),
                "{status:?} mapping"
            );
            assert!(r.order, "{status:?}: an orderly listener must report order=true");
            assert!(
                !r.auto_commit,
                "{status:?}: the listener-set autoCommit=false must go back to the broker as-is"
            );
        }
    }

    #[test]
    fn direct_consume_maps_concurrent_success_and_auto_commit_default() {
        let c = DefaultMQPushConsumer::with_config(cfg_with(0)).expect("consumer");
        c.set_message_listener_concurrently(Arc::new(AckListener {
            ack_index: None,
            status: ConsumeConcurrentlyStatus::ConsumeSuccess,
        }));
        let r = c
            .consume_message_directly(ext("T", None), Some("broker-a".to_string()))
            .expect("direct consume must not error");
        assert_eq!(r.consume_result.as_deref(), Some(CMResult::CR_SUCCESS));
        assert!(!r.order, "a concurrent listener must report order=false");
        assert!(r.auto_commit, "the concurrent side keeps the initial autoCommit=true");
    }

    // ---------------- POP ----------------

    #[test]
    fn pop_loop_records_pull_rt_even_when_the_pop_is_empty() {
        let group = "GID_PopStats";
        let topic = "PopStatsTopic";
        let key = ConsumerStatsManager::key(topic, group);
        let stats = ConsumerStatsManager::new();
        // Roll back 30ms: RT is `now - began`, so a forward-facing >0 assert does not depend on scheduling precision
        let began = current_time_millis() - 30;

        let rt = || {
            stats
                .topic_and_group_pull_rt()
                .find(&key)
                .unwrap_or_else(|| panic!("PULL_RT should have been recorded under {key}"))
        };
        let tps = || {
            stats
                .topic_and_group_pull_tps()
                .find(&key)
                .unwrap_or_else(|| panic!("PULL_TPS should have been recorded under {key}"))
        };

        // (1) FOUND + 2 messages: RT recorded once, TPS recorded once per message
        record_pop_pull_stats(&stats, group, topic, PopStatus::Found, 2, began);
        assert_eq!(rt().times(), 1);
        assert!(rt().value() >= 30, "RT should be this pop round's elapsed time: {}", rt().value());
        assert_eq!(tps().times(), 1);
        assert_eq!(tps().value(), 2);

        record_pop_pull_stats(&stats, group, topic, PopStatus::Found, 0, began);
        assert_eq!(rt().times(), 2, "an empty-handed FOUND round still records RT");
        assert_eq!(tps().times(), 1, "an empty list must not record TPS");

        // (3) POLLING_NOT_FOUND: neither cell records. Empty POP polls are the norm; counting the suspend
        //     duration into the average pull time would render the dashboard's pullRT meaningless.
        record_pop_pull_stats(&stats, group, topic, PopStatus::PollingNotFound, 0, began);
        assert_eq!(rt().times(), 2);
        assert_eq!(tps().times(), 1);

        // (4) The count the stats take is the one left AFTER the pre-delivery pipeline, not the
        //     number of rows the broker popped: three rows popped, one of them dropped before the
        //     listener, books two deliveries.
        record_pop_pull_stats(&stats, group, topic, PopStatus::Found, 1, began);
        assert_eq!(rt().times(), 3, "a round that delivered something records RT");
        assert_eq!(tps().times(), 2);
        assert_eq!(tps().value(), 3, "TPS counts only what was handed over");
    }

    /// The pull path books the same two cells under the same two gates: the round trip belongs to
    /// FOUND rounds alone, and throughput counts the rows that survive the pre-delivery filter, not
    /// the rows the broker sent.
    #[test]
    fn pull_round_stats_follow_the_found_gate_and_the_post_filter_count() {
        let group = "GID_PullStats";
        let topic = "PullStatsTopic";
        let key = ConsumerStatsManager::key(topic, group);
        let stats = ConsumerStatsManager::new();
        let began = current_time_millis() - 30;

        let rt = || {
            stats
                .topic_and_group_pull_rt()
                .find(&key)
                .unwrap_or_else(|| panic!("PULL_RT should have been recorded under {key}"))
        };
        let tps = || {
            stats
                .topic_and_group_pull_tps()
                .find(&key)
                .unwrap_or_else(|| panic!("PULL_TPS should have been recorded under {key}"))
        };

        // (1) A round that returned nothing books neither cell. Idle rounds are the norm, and an
        //     average pull time diluted by them is the number the dashboard stops being able to use.
        record_pull_round_stats(&stats, group, topic, false, 4_000, 0);
        assert!(
            stats.topic_and_group_pull_rt().find(&key).is_none(),
            "a non-FOUND round must not record PULL_RT"
        );
        assert!(
            stats.topic_and_group_pull_tps().find(&key).is_none(),
            "a non-FOUND round must not record PULL_TPS"
        );

        // (2) FOUND with rows: the round trip of this round plus one delivery count per row.
        record_pull_round_stats(&stats, group, topic, true, current_time_millis() - began, 2);
        assert_eq!(rt().times(), 1);
        assert!(
            rt().value() >= 30,
            "RT is this round's elapsed time, not a placeholder: {}",
            rt().value()
        );
        assert_eq!(tps().times(), 1);
        assert_eq!(tps().value(), 2);

        // (3) FOUND but empty-handed: the round trip still belongs in the average, the nothing does
        //     not belong in throughput.
        record_pull_round_stats(&stats, group, topic, true, current_time_millis() - began, 0);
        assert_eq!(rt().times(), 2, "an empty-handed FOUND round still records RT");
        assert_eq!(tps().times(), 1, "an empty list must not record TPS");

        // (4) The count the stats take is the one left after the pre-delivery pipeline: five rows
        //     pulled, two dropped before the listener, so three deliveries.
        record_pull_round_stats(&stats, group, topic, true, current_time_millis() - began, 3);
        assert_eq!(rt().times(), 3);
        assert_eq!(tps().times(), 2);
        assert_eq!(tps().value(), 5, "PULL_TPS sums the delivered rows: 2 + 3");
    }

    /// The wait the next pop on the same queue waits out is decided by the answer, and only by
    /// the answer: a FOUND round honors the configured cadence, an empty or nothing-new answer
    /// re-pops at once, and a queue the broker says is full waits the generic failed-round
    /// backoff the caller passes in.
    #[test]
    fn pop_round_pacing_follows_the_answer() {
        assert_eq!(
            pop_next_round_delay_millis(PopStatus::Found, 2, 500, 1000),
            500
        );
        assert_eq!(
            pop_next_round_delay_millis(PopStatus::Found, 2, 0, 1000),
            0,
            "no configured cadence means straight back to the top"
        );
        assert_eq!(
            pop_next_round_delay_millis(PopStatus::Found, 0, 500, 1000),
            0,
            "a round that handed over nothing does not wait out the cadence"
        );
        assert_eq!(
            pop_next_round_delay_millis(PopStatus::NoNewMsg, 0, 500, 1000),
            0
        );
        assert_eq!(
            pop_next_round_delay_millis(PopStatus::PollingNotFound, 0, 500, 1000),
            0,
            "the broker already held this poll open"
        );
        assert_eq!(
            pop_next_round_delay_millis(PopStatus::PollingFull, 0, 500, 1000),
            1000,
            "a full queue is not ready: the wait is the generic failed-round backoff, not pullInterval"
        );
    }

    /// The cadence a pull round waits out is decided by one thing only: whether the round handed
    /// rows over. The `found` flag below is exactly the expression the loop feeds the helper with,
    /// `status == PullStatus::Found`, so each leg is this client's own computed wait.
    #[test]
    fn pull_round_pacing_follows_the_delivery() {
        let found = |status: PullStatus| status == PullStatus::Found;

        // Delivered rows and a configured cadence: wait it out.
        assert_eq!(
            pull_next_round_delay_millis(found(PullStatus::Found), 2, 40),
            40
        );
        // Delivered rows and no configured cadence: straight back to the top.
        assert_eq!(
            pull_next_round_delay_millis(found(PullStatus::Found), 2, 0),
            0,
            "a cadence of 0 means no wait at all"
        );
        // A FOUND round the pre-delivery filter emptied handed over nothing, so nothing is paced.
        assert_eq!(
            pull_next_round_delay_millis(found(PullStatus::Found), 0, 40),
            0,
            "the cadence paces deliveries, not fetches"
        );
        // An idle round re-pulls at once, whatever the cadence says.
        for status in [PullStatus::NoNewMsg, PullStatus::NoMatchedMsg] {
            assert_eq!(
                pull_next_round_delay_millis(found(status), 0, 40),
                0,
                "{status:?} does not wait out the cadence"
            );
        }
    }

    /// An illegal round runs its own freeze/persist/revoke path and must not gain a cadence wait:
    /// the loop derives `found` from the status alone, so this status is never a delivery round,
    /// and the cadence is reachable through no other route.
    #[test]
    fn pull_illegal_round_takes_no_cadence() {
        let found = PullStatus::OffsetIllegal == PullStatus::Found;
        assert!(!found, "an illegal round is not a delivery round");
        assert_eq!(pull_next_round_delay_millis(found, 0, 40), 0);
        // No row count arms the cadence on that round either.
        assert_eq!(pull_next_round_delay_millis(found, 8, 40), 0);
    }

    /// A message row carrying only what the offset checks read: its place in the queue.
    fn offset_row(queue_offset: i64) -> MessageExt {
        let mut m = ext("T", None);
        m.queue_offset = queue_offset;
        m
    }

    /// Leg one and the line it logs: the cursor the round advanced to sitting below the cursor it
    /// requested with. All three numbers the check compared have to be readable from that one line,
    /// because telling the two legs apart is exactly what a reader of the log needs to do.
    #[test]
    fn pull_offsets_sanity_warns_with_all_three_numbers() {
        assert!(pull_offsets_look_wrong(90, 90, 100));

        let no_rows = i64::MAX;
        let line = pull_offset_warning("T@broker-a@0", 90, no_rows, 100);
        assert!(line.contains("next_begin_offset=90"), "{line}");
        assert!(
            line.contains(&format!("first_msg_offset={no_rows}")),
            "{line}"
        );
        assert!(line.contains("requested_offset=100"), "{line}");
        assert!(line.contains("T@broker-a@0"), "{line}");
    }

    /// Leg two on its own: the cursor advanced as usual, but the first row the round hands over sits
    /// below the offset the round requested with.
    #[test]
    fn pull_offsets_sanity_warns_when_a_row_precedes_the_request() {
        assert!(pull_offsets_look_wrong(120, 95, 100));
        // The same cursor and request with a row that does not precede it: nothing to warn about.
        assert!(!pull_offsets_look_wrong(120, 100, 100));
    }

    /// A round whose numbers agree stays out of the log: this line is only worth reading when the
    /// round contradicts the request it was made under.
    #[test]
    fn pull_offsets_sanity_stays_quiet_on_a_consistent_round() {
        assert!(!pull_offsets_look_wrong(120, 100, 100));
        assert!(!pull_offsets_look_wrong(100, 100, 100));
        assert!(!pull_offsets_look_wrong(i64::MAX, i64::MAX, 100));
    }

    /// A FOUND round the pre-delivery filter emptied delivers no row, so its first-row number is the
    /// `i64::MAX` stand-in -- and that stand-in must never read as a row from before the cursor.
    #[test]
    fn pull_offsets_sanity_ignores_an_empty_found_round() {
        // The stand-in is derived the way the loop derives it: from the list this round delivers.
        let msgs: Vec<MessageExt> = vec![];
        let first_msg_offset = msgs.first().map_or(i64::MAX, MessageExt::get_queue_offset);
        assert_eq!(first_msg_offset, i64::MAX);
        assert!(!pull_offsets_look_wrong(120, first_msg_offset, 100));
    }

    /// The three inputs the loop feeds the check with, read off a real round the way the loop reads
    /// them at its end, so these legs are this client's own round and not a hand-made argument list.
    #[test]
    fn pull_offsets_sanity_reads_the_round_the_way_the_loop_reads_it() {
        let result = PullResult {
            status: PullStatus::Found,
            next_begin_offset: 120,
            msg_found_list: vec![offset_row(95), offset_row(120)],
            ..Default::default()
        };
        let prev_request_offset = 100_i64;

        let pull_found = result.status == PullStatus::Found;
        let msgs = result.msg_found_list.clone();
        let first_msg_offset = msgs.first().map_or(i64::MAX, MessageExt::get_queue_offset);

        assert!(pull_found, "only a FOUND round runs this check");
        assert_eq!(first_msg_offset, 95);
        assert!(
            pull_offsets_look_wrong(
                result.next_begin_offset,
                first_msg_offset,
                prev_request_offset
            ),
            "the numbers this round actually carries fire the warning"
        );

        // The very same round requested from its first row instead: consistent, so quiet.
        assert!(!pull_offsets_look_wrong(
            result.next_begin_offset,
            first_msg_offset,
            95
        ));
    }

    /// The check only reads: a round it flags lands its cursor and arms its cadence exactly like the
    /// identical round it does not flag. The two rounds below differ in nothing but the offset they
    /// were requested with, and that is the one number deciding the flag.
    #[test]
    fn pull_offsets_sanity_leaves_the_round_bookkeeping_untouched() {
        assert!(pull_offsets_look_wrong(120, 95, 200));
        assert!(!pull_offsets_look_wrong(120, 95, 90));

        let c = DefaultMQPushConsumer::new("G_pull_offsets_sanity").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);

        // The bookkeeping a FOUND round leaves behind when it requested from `requested` and answered
        // with cursor 120: the cursor that landed, the correction that ran with it, and the wait the
        // next round arms with out. Both rounds hand over two rows, so only the flag differs.
        let round_bookkeeping = |requested: i64| -> (i64, i64, u64) {
            lock(&c.inner.state)
                .offset_table
                .insert(key.clone(), requested);
            {
                let mut state = lock(&c.inner.state);
                state.offset_table.insert(key.clone(), 120);
                correct_tags_offset_locked(&mut state, &key, PullStatus::Found, 120);
            }
            let state = lock(&c.inner.state);
            (
                state.offset_table[&key],
                state.consume_offsets.get(&key).copied().unwrap_or(-1),
                pull_next_round_delay_millis(true, 2, 40),
            )
        };

        let fired = round_bookkeeping(200);
        let quiet = round_bookkeeping(90);
        assert_eq!(fired.0, 120, "a flagged round still advances the cursor");
        assert_eq!(fired.0, quiet.0, "the same cursor either way");
        assert_eq!(fired.1, quiet.1, "the same correction either way");
        assert_eq!(fired.2, 40, "a flagged round still waits out the cadence");
        assert_eq!(fired.2, quiet.2, "the same wait either way");
    }

    /// The one generic failed-round backoff, pinned on all four legs. Every wait below is this
    /// client's own armed backoff, computed from the mapping each loop runs -- no broker answer is
    /// claimed here except the `FLOW_CONTROL(215)` code the mapping matches on.
    #[test]
    fn pull_and_pop_failed_rounds_share_one_backoff_knob() {
        // (1) The default is 1000 ms, on the config, on the constant behind it, and on a fresh
        //     consumer read back through its getter.
        assert_eq!(DEFAULT_PULL_EXCEPTION_BACKOFF_MILLIS, 1000);
        assert_eq!(
            ConsumerConfig::default().pull_exception_backoff_millis,
            1000
        );
        let consumer = DefaultMQPushConsumer::new("G_generic_backoff").unwrap();
        assert_eq!(consumer.pull_exception_backoff_millis(), 1000);

        // (2) The broker's own throttle is NOT this knob: 215 keeps its short fixed pause on both
        //     the pull and the POP answer shape, whatever the knob says.
        for knob in [1000_i64, 250] {
            for err in [
                Error::Broker {
                    response_code: response_code::FLOW_CONTROL,
                    message: "throttled".to_string(),
                },
                Error::Server {
                    response_code: response_code::FLOW_CONTROL,
                    remark: "throttled".to_string(),
                },
            ] {
                assert_eq!(
                    failed_request_backoff_millis(&err, knob.max(0) as u64),
                    PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL,
                    "FLOW_CONTROL(215) keeps its own 20 ms wait at knob={knob}"
                );
            }
        }

        // (3) Moving the knob moves the wait BOTH the pull error arm and the POP error arm take.
        consumer.set_pull_exception_backoff_millis(250);
        assert_eq!(consumer.pull_exception_backoff_millis(), 250);
        let knob = consumer.pull_exception_backoff_millis().max(0) as u64;
        for err in [
            Error::Broker {
                response_code: response_code::SYSTEM_ERROR,
                message: "broker says no".to_string(),
            },
            Error::client("no route for the topic"),
        ] {
            assert_eq!(
                failed_request_backoff_millis(&err, knob),
                250,
                "a non-215 failure takes the configured wait, not a hardcoded 3000"
            );
        }
        // The wait a running loop arms with comes from what it captured at setup, so a pure
        // mapping fed the captured number is exactly what the loop does.
        assert_eq!(
            pop_next_round_delay_millis(PopStatus::PollingFull, 0, 500, knob),
            250
        );

        // (4) POLLING_FULL and a status this client has no rule for both take the knob value: no
        //     private 3000 ms park anywhere on the POP path.
        assert_eq!(
            pop_next_round_delay_millis(PopStatus::PollingFull, 0, 500, 250),
            250
        );
        // An unrecognised POP answer reaches the client as a broker error carrying its own code,
        // and that arm resolves to the same knob.
        let unknown_status_answer = Error::Broker {
            response_code: 65534,
            message: "a status this client has no rule for".to_string(),
        };
        assert_eq!(
            failed_request_backoff_millis(&unknown_status_answer, 250),
            250
        );
        assert_eq!(
            failed_request_backoff_millis(&unknown_status_answer, 1000),
            1000
        );
    }

    /// The budget the early guard measures against is the consumer's own number, or 16 while the
    /// knob is left unset -- and it is a strictly-greater cut, so the row that has exactly used up
    /// its budget is still delivered.
    #[test]
    fn pop_budget_defaults_to_sixteen_and_is_strict() {
        let cfg_with = |max_reconsume_times: i32| ConsumerConfig {
            max_reconsume_times,
            ..ConsumerConfig::default()
        };
        assert_eq!(pop_max_reconsume_times(&cfg_with(-1)), 16);
        assert_eq!(pop_max_reconsume_times(&cfg_with(3)), 3);
        assert_eq!(pop_max_reconsume_times(&cfg_with(0)), 0);
    }

    /// The budget guard runs BEFORE delivery and is a strictly-greater cut: the row that has
    /// exactly used up its budget still reaches the listener, the row past it is dropped. The
    /// dropped rows are acked there and then, and each one gives back the answer credit the
    /// round took for it, so the queue's debt ends up holding only what really waits.
    #[tokio::test]
    async fn rows_past_the_budget_are_dropped_before_the_listener() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| {
            c.pop_mode = true;
            c.max_reconsume_times = 3;
        });
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        consumer.set_message_listener_concurrently(Arc::new(TopicRecordingListener(seen.clone())));
        let inner = consumer.inner.clone();
        let mq = queue("T", "broker-a", 0);
        let sub = FilterAPI::build_subscription_data("T", Some("*")).unwrap();
        let pq = PopProcessQueue::new();
        pq.inc_found_msg(3);
        let popped = vec![
            pop_row("T", 100, 3),
            pop_row("T", 101, 4),
            pop_row("T", 102, 5),
        ];
        let kept = prepare_pop_batch_for_delivery(
            &inner,
            &read_cfg(&inner),
            &mq,
            &sub,
            &popped,
            &pq,
        );
        assert_eq!(kept.len(), 1, "only the row at the budget is deliverable");
        assert_eq!(kept[0].queue_offset, 100);
        assert_eq!(
            pq.wait_ack_count(),
            1,
            "the two dropped rows gave their answer credits back"
        );

        // Control: the same three rows under a budget they all clear are handed over whole, and
        // nothing is given back.
        consumer.update_config(|c| c.max_reconsume_times = 9);
        let pq2 = PopProcessQueue::new();
        pq2.inc_found_msg(3);
        let kept_all = prepare_pop_batch_for_delivery(
            &inner,
            &read_cfg(&inner),
            &mq,
            &sub,
            &popped,
            &pq2,
        );
        assert_eq!(kept_all.len(), 3, "a budget every row clears drops nothing");
        assert_eq!(pq2.wait_ack_count(), 3);
    }

    /// Whenever popTime/invisibleTime is unavailable, **always treat it as a timeout**,
    /// so the "recompute visibility" path runs instead of "redeliver as-is".
    #[test]
    fn pop_timeout_compares_the_invisible_window() {
        let now = current_time_millis();
        let one = || vec![ext("T", None)];
        assert!(is_pop_timeout(&[], now, 60_000));
        assert!(is_pop_timeout(&one(), 0, 60_000));
        assert!(is_pop_timeout(&one(), now - 1000, 0));
        assert!(!is_pop_timeout(&one(), now, 60_000), "still inside the invisible window");
        assert!(is_pop_timeout(&one(), now - 60_000, 60_000), "the window has passed");
    }

    /// The two-pass traversal of POP consume-result handling: whatever the consume result is,
    /// every message must call `pq.ack()` **exactly** once -- miss one and `waitAckCounter`
    /// leaks forever; once it accumulates to `popThresholdForQueue` that queue never pops new messages again.
    #[tokio::test]
    async fn every_pop_message_is_acked_whatever_the_result() {
        for (status, ack_index, len) in [
            (ConsumeConcurrentlyStatus::ConsumeSuccess, 1, 3),
            // ack_index out of bounds: clamped to len-1
            (ConsumeConcurrentlyStatus::ConsumeSuccess, 99, 3),
            // Default -1: none counts as a "successful ack", but every other message still gets its invisible time extended
            (ConsumeConcurrentlyStatus::ConsumeSuccess, -1, 3),
            // RECONSUME_LATER still traverses every message
            (ConsumeConcurrentlyStatus::ReconsumeLater, -1, 3),
            (ConsumeConcurrentlyStatus::ReconsumeLater, 0, 1),
        ] {
            let consumer = DefaultMQPushConsumer::new("G").unwrap();
            let inner = consumer.inner.clone();
            let pq = PopProcessQueue::new();
            pq.inc_found_msg(len);
            let msgs: Vec<MessageExt> = (0..len).map(|_| ext("T", None)).collect();
            let mut context = ConsumeConcurrentlyContext::new(Some(queue("T", "broker-a", 0)));
            context.ack_index = ack_index;
            // Without start(): the ack / visibility-extension RPCs degrade to no-ops; only the counter clearing is verified
            process_pop_consume_result(&inner, status, &context, &msgs, &pq).await;
            assert_eq!(
                pq.wait_ack_count(),
                0,
                "{status:?} with ack_index={ack_index} over {len} msgs"
            );
        }
    }

    /// Missing/malformed POP_CK both count as "cannot ack", no panic.
    #[test]
    fn pop_ck_target_rejects_missing_or_malformed_ck() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let inner = &consumer.inner;

        assert!(pop_ck_target(inner, &ext("T", None)).is_none());

        let mut empty_ck = ext("T", None);
        empty_ck.put_property(PROPERTY_POP_CK, "");
        assert!(pop_ck_target(inner, &empty_ck).is_none());

        let mut junk = ext("T", None);
        junk.put_property(PROPERTY_POP_CK, "only-three-segments");
        assert!(pop_ck_target(inner, &junk).is_none());

        // A real CK has **8 segments**: when the broker re-encodes the CK it appends msgQueueOffset as the 8th
        // segment, and `getQueueOffset` reads exactly that segment.
        let mut ok = ext("T", None);
        ok.put_property(
            PROPERTY_POP_CK,
            "1234 1700000000000 60000 0 0 broker-a 3 99",
        );
        assert_eq!(
            pop_ck_target(inner, &ok),
            Some((
                "T".to_string(),
                "broker-a".to_string(),
                3,
                99,
                "1234 1700000000000 60000 0 0 broker-a 3 99".to_string()
            ))
        );
        // retryFlag=1: the real topic is %RETRY%<group>_<topic>, not the message's topic
        let mut retry = ok.clone();
        retry.put_property(
            PROPERTY_POP_CK,
            "1234 1700000000000 60000 0 1 broker-a 3 99",
        );
        let (topic, ..) = pop_ck_target(inner, &retry).unwrap();
        assert_eq!(topic, "%RETRY%G_T");
        // retryFlag=2: v2 format %RETRY%<group>+<topic>
        let mut retry_v2 = ok.clone();
        retry_v2.put_property(
            PROPERTY_POP_CK,
            "1234 1700000000000 60000 0 2 broker-a 3 99",
        );
        let (topic, ..) = pop_ck_target(inner, &retry_v2).unwrap();
        assert_eq!(topic, "%RETRY%G+T");
        let mut seven = ok.clone();
        seven.put_property(PROPERTY_POP_CK, "1234 1700000000000 60000 0 0 broker-a 3");
        assert!(pop_ck_target(inner, &seven).is_none());
        // Unknown retryFlag is rejected too (getRetry only accepts 0/1/2)
        let mut bad_flag = ok.clone();
        bad_flag.put_property(
            PROPERTY_POP_CK,
            "1234 1700000000000 60000 0 T broker-a 3 99",
        );
        assert!(pop_ck_target(inner, &bad_flag).is_none());
    }


    struct CountingOrderlyListener(Arc<AtomicUsize>);

    impl MessageListenerOrderly for CountingOrderlyListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            _context: &mut ConsumeOrderlyContext,
        ) -> ConsumeOrderlyStatus {
            self.0.fetch_add(1, Ordering::SeqCst);
            ConsumeOrderlyStatus::Success
        }
    }

    struct CountingConcurrentlyListener(Arc<AtomicUsize>);

    impl MessageListenerConcurrently for CountingConcurrentlyListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            _context: &mut ConsumeConcurrentlyContext,
        ) -> ConsumeConcurrentlyStatus {
            self.0.fetch_add(1, Ordering::SeqCst);
            ConsumeConcurrentlyStatus::ConsumeSuccess
        }
    }

    fn pop_ck_msg(queue_offset: i64) -> MessageExt {
        let mut m = ext("T", None);
        m.put_property(
            PROPERTY_POP_CK,
            &extra_info::build_extra_info(
                queue_offset,
                current_time_millis(),
                60_000,
                0,
                "T",
                "broker-a",
                0,
                Some(queue_offset),
            ),
        );
        m
    }

    #[tokio::test]
    async fn pop_orderly_dispatch_never_invokes_the_listener() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.pop_mode = true);
        let calls = Arc::new(AtomicUsize::new(0));
        consumer.set_message_listener_orderly(Arc::new(CountingOrderlyListener(calls.clone())));
        let inner = consumer.inner.clone();
        let mq = queue("T", "broker-a", 0);
        let pq = PopProcessQueue::new();
        pq.inc_found_msg(2);
        submit_pop_consume_request(
            &inner,
            vec![pop_ck_msg(0), pop_ck_msg(1)],
            pq.clone(),
            mq.clone(),
        )
        .await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "orderly POP stalled"
        );
        assert_eq!(
            pq.wait_ack_count(),
            2,
            "not acked: wait for invisibleTime to expire and the broker to revive and redeliver"
        );
        assert_eq!(consumer.pop_orderly_request_count(), 1);
    }

    /// Same entry point, concurrent listener: goes through the concurrent POP listener, consuming + acking as usual.
    #[tokio::test]
    async fn pop_concurrent_dispatch_still_consumes() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.pop_mode = true);
        let calls = Arc::new(AtomicUsize::new(0));
        consumer.set_message_listener_concurrently(Arc::new(CountingConcurrentlyListener(
            calls.clone(),
        )));
        let inner = consumer.inner.clone();
        let mq = queue("T", "broker-a", 0);
        let pq = PopProcessQueue::new();
        pq.inc_found_msg(1);
        submit_pop_consume_request(&inner, vec![pop_ck_msg(0)], pq.clone(), mq.clone()).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(pq.wait_ack_count(), 0);
    }

    #[tokio::test]
    async fn pop_orderly_requests_dedup_by_pq_and_queue() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let inner = consumer.inner.clone();
        let pq = PopProcessQueue::new();
        let q0 = queue("T", "broker-a", 0);
        let q1 = queue("T", "broker-a", 1);
        submit_pop_orderly_request(&inner, pq.clone(), q0.clone(), false).await;
        submit_pop_orderly_request(&inner, pq.clone(), q0.clone(), false).await;
        assert_eq!(consumer.pop_orderly_request_count(), 1, "dedup by (pq, mq)");
        submit_pop_orderly_request(&inner, pq.clone(), q1.clone(), false).await;
        assert_eq!(consumer.pop_orderly_request_count(), 2, "a different mq counts as a new request");
        let pq2 = PopProcessQueue::new();
        submit_pop_orderly_request(&inner, pq2.clone(), q0.clone(), false).await;
        assert_eq!(consumer.pop_orderly_request_count(), 3, "a different pq counts as a new request");
        submit_pop_orderly_request(&inner, pq.clone(), q0.clone(), true).await;
        assert_eq!(
            consumer.pop_orderly_request_count(),
            3,
            "force only lets the delivery through, it adds no set entry"
        );
    }

    #[tokio::test]
    async fn pop_orderly_dropped_request_is_removed_from_the_set() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let inner = consumer.inner.clone();
        let pq = PopProcessQueue::new();
        let mq = queue("T", "broker-a", 0);
        submit_pop_orderly_request(&inner, pq.clone(), mq.clone(), false).await;
        assert_eq!(consumer.pop_orderly_request_count(), 1);
        pq.set_dropped(true);
        submit_pop_orderly_request(&inner, pq.clone(), mq.clone(), true).await;
        assert_eq!(
            consumer.pop_orderly_request_count(),
            0,
            "revoked queue: out of the set, no lingering"
        );

        // Dropped first, submitted after: it can enter the set, and run removes it right away
        let pq2 = PopProcessQueue::new();
        pq2.set_dropped(true);
        submit_pop_orderly_request(&inner, pq2, mq.clone(), false).await;
        assert_eq!(consumer.pop_orderly_request_count(), 0);
    }

    #[tokio::test]
    async fn pop_orderly_run_on_live_queue_is_noop() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let inner = consumer.inner.clone();
        let pq = PopProcessQueue::new();
        let mq = queue("T", "broker-a", 0);
        submit_pop_orderly_request(&inner, pq.clone(), mq.clone(), false).await;
        assert_eq!(consumer.pop_orderly_request_count(), 1);
        run_pop_orderly_request(inner, pq, mq).await;
        assert_eq!(consumer.pop_orderly_request_count(), 1);
    }

    // ---------------- consume result and pre-delivery restoration ----------------

    /// Consume return type (decides the trace contextCode).
    #[test]
    fn consume_return_type_follows_the_recorded_branches() {
        // status None: with an exception -> EXCEPTION, without -> RETURNNULL
        assert_eq!(
            consume_return_type(None, true, 0, 15, true),
            ConsumeReturnType::Exception
        );
        assert_eq!(
            consume_return_type(None, false, 0, 15, false),
            ConsumeReturnType::ReturnNull
        );
        assert_eq!(
            consume_return_type(Some("CONSUME_SUCCESS"), false, 15 * 60 * 1000, 15, true),
            ConsumeReturnType::TimeOut
        );
        assert_eq!(
            consume_return_type(Some("RECONSUME_LATER"), false, 15 * 60 * 1000 - 1, 15, true),
            ConsumeReturnType::Failed
        );
        assert_eq!(
            consume_return_type(Some("RECONSUME_LATER"), false, 100, 15, true),
            ConsumeReturnType::Failed
        );
        assert_eq!(
            consume_return_type(Some("CONSUME_SUCCESS"), false, 100, 15, false),
            ConsumeReturnType::Success
        );
        assert_eq!(ConsumeReturnType::Success.name(), "SUCCESS");
        assert_eq!(ConsumeReturnType::TimeOut.name(), "TIME_OUT");
        assert_eq!(ConsumeReturnType::Exception.name(), "EXCEPTION");
        assert_eq!(ConsumeReturnType::ReturnNull.name(), "RETURNNULL");
        assert_eq!(ConsumeReturnType::Failed.name(), "FAILED");
        assert_eq!(ConsumeReturnType::Failed.code(), 4);
    }

    /// Restores the topic before handing to the listener.
    #[test]
    fn retry_topic_and_namespace_are_reset_before_delivery() {
        let consumer = DefaultMQPushConsumer::new("MyGroup").unwrap();
        let group_topic = MixAll::get_retry_topic("MyGroup");
        assert_eq!(group_topic, "%RETRY%MyGroup");

        // Only when "the topic really is this group's retry topic" is the original restored via the RETRY_TOPIC property
        let mut retried = ext(&group_topic, None);
        retried.put_property(PROPERTY_RETRY_TOPIC, "OrderTopic");
        let mut unrelated = ext("T", None);
        unrelated.put_property(PROPERTY_RETRY_TOPIC, "OrderTopic");
        let mut msgs = vec![retried.clone(), unrelated.clone()];
        reset_retry_topic_and_namespace(&consumer.inner, &mut msgs);
        assert_eq!(msgs[0].topic, "OrderTopic");
        assert_eq!(msgs[1].topic, "T", "a non-group retry topic is left alone");

        // With a namespace: strip the prefix after restoring the original topic, a two-step composite
        consumer.update_config(|c| c.namespace = "Ns".to_string());
        let mut namespaced_retry = ext(&group_topic, None);
        namespaced_retry.put_property(PROPERTY_RETRY_TOPIC, "Ns%Order");
        let plain_namespaced = ext("Ns%T", None);
        let mut msgs = vec![namespaced_retry.clone(), plain_namespaced.clone()];
        reset_retry_topic_and_namespace(&consumer.inner, &mut msgs);
        assert_eq!(msgs[0].topic, "Order");
        assert_eq!(msgs[1].topic, "T");

        // An empty batch does not error
        reset_retry_topic_and_namespace(&consumer.inner, &mut []);
    }

    /// A retried POP row is stored under `%RETRY%<group>_<topic>` or, under the newer retry
    /// naming, `%RETRY%<group>+<topic>`. Both spellings of this consumer's own group are cut;
    /// everything else is left exactly as it arrived.
    #[test]
    fn pop_retry_topic_is_cut_at_both_wire_spellings() {
        let consumer = DefaultMQPushConsumer::new("MyGroup").unwrap();
        let cases = [
            ("%RETRY%MyGroup_OrderTopic", "OrderTopic"),
            ("%RETRY%MyGroup+OrderTopic", "OrderTopic"),
            // The '+' right after the group is the separator, so a business topic carrying an
            // underscore is read whole instead of being cut at its first '_'.
            ("%RETRY%MyGroup+A_B", "A_B"),
            // Nothing after the separator is not a topic name, and a name built on another
            // group is not this consumer's to rewrite.
            ("%RETRY%MyGroup_", "%RETRY%MyGroup_"),
            ("%RETRY%MyGroup+", "%RETRY%MyGroup+"),
            ("%RETRY%MyGroup", "%RETRY%MyGroup"),
            ("%RETRY%Other+OrderTopic", "%RETRY%Other+OrderTopic"),
            ("OrderTopic", "OrderTopic"),
        ];
        for (arrived, delivered) in cases {
            let mut msgs = vec![ext(arrived, None)];
            reset_pop_retry_topic(&consumer.inner, &mut msgs);
            assert_eq!(msgs[0].topic, delivered, "a POP row under {arrived}");
        }
    }

    /// With a namespace, the group this consumer registers under is the wrapped one, and the
    /// wire name can carry the wrap on both sides of the separator. Both spellings are
    /// recognised, and the wrap left on the business topic is taken off by the restore that
    /// follows -- the cut itself only ever removes the retry naming.
    #[test]
    fn pop_retry_topic_cut_names_both_spellings_of_this_group() {
        let consumer = DefaultMQPushConsumer::new("Ns%MyGroup").unwrap();
        consumer.update_config(|c| c.namespace = "Ns".to_string());
        let cases = [
            ("%RETRY%Ns%MyGroup+Ns%OrderTopic", "Ns%OrderTopic"),
            ("%RETRY%Ns%MyGroup_Ns%OrderTopic", "Ns%OrderTopic"),
            ("%RETRY%MyGroup+OrderTopic", "OrderTopic"),
            ("%RETRY%MyGroup_OrderTopic", "OrderTopic"),
        ];
        for (arrived, cut) in cases {
            let mut msgs = vec![ext(arrived, None)];
            reset_pop_retry_topic(&consumer.inner, &mut msgs);
            assert_eq!(msgs[0].topic, cut, "a POP row under {arrived}");
            // The delivery name the listener finally gets is the plain business topic.
            reset_retry_topic_and_namespace(&consumer.inner, &mut msgs);
            assert_eq!(msgs[0].topic, "OrderTopic", "and the wrap comes off after the cut");
        }
    }

    /// Where the cut is allowed to happen: both concurrent entries carry it and the orderly
    /// entry does not, because an orderly consumer keeps consuming the retry queue under the
    /// name that queue has. The legs differ only in the listener, so a cut that moved up into
    /// the shared restore would be caught here.
    #[tokio::test]
    async fn pop_retry_topic_is_cut_at_the_concurrent_entries_only() {
        let retry_topic = "%RETRY%MyGroup+OrderTopic";

        // Leg 1: the POP batch entry.
        let popper = DefaultMQPushConsumer::new("MyGroup").unwrap();
        popper.update_config(|c| c.pop_mode = true);
        let pop_seen = Arc::new(Mutex::new(Vec::<String>::new()));
        popper.set_message_listener_concurrently(Arc::new(TopicRecordingListener(pop_seen.clone())));
        let pq = PopProcessQueue::new();
        pq.inc_found_msg(1);
        submit_pop_consume_request(
            &popper.inner,
            vec![pop_row(retry_topic, 0, 0)],
            pq,
            queue(retry_topic, "broker-a", 0),
        )
        .await;
        assert_eq!(
            pop_seen.lock().unwrap().clone(),
            vec!["OrderTopic".to_string()],
            "a POP batch reaches the listener under the business topic"
        );

        // Leg 2: the pulled batch entry, same name, same cut.
        let puller = DefaultMQPushConsumer::new("MyGroup").unwrap();
        let pull_seen = Arc::new(Mutex::new(Vec::<String>::new()));
        puller.set_message_listener_concurrently(Arc::new(TopicRecordingListener(pull_seen.clone())));
        let mq = queue(retry_topic, "broker-a", 0);
        let key = mq_key(&mq);
        consume_batch(&puller.inner, &key, &mq, vec![ext(retry_topic, None)], None)
            .await
            .expect("consume_batch must not error");
        assert_eq!(
            pull_seen.lock().unwrap().clone(),
            vec!["OrderTopic".to_string()],
            "a pulled batch is cut the same way at the concurrent entry"
        );

        // Leg 3: the orderly entry keeps the name the queue is locked under.
        let orderly = DefaultMQPushConsumer::new("MyGroup").unwrap();
        let orderly_seen = Arc::new(Mutex::new(Vec::<String>::new()));
        orderly.set_message_listener_orderly(Arc::new(TopicRecordingOrderlyListener(
            orderly_seen.clone(),
        )));
        let mq = queue(retry_topic, "broker-a", 0);
        let key = mq_key(&mq);
        consume_batch(&orderly.inner, &key, &mq, vec![ext(retry_topic, None)], None)
            .await
            .expect("consume_batch must not error");
        assert_eq!(
            orderly_seen.lock().unwrap().clone(),
            vec![retry_topic.to_string()],
            "the orderly entry is handed exactly the name its queue was locked under"
        );
    }

    /// Records the topic of every row the listener was handed.
    struct TopicRecordingListener(Arc<Mutex<Vec<String>>>);

    impl MessageListenerConcurrently for TopicRecordingListener {
        fn consume_message(
            &self,
            msgs: &[MessageExt],
            _context: &mut ConsumeConcurrentlyContext,
        ) -> ConsumeConcurrentlyStatus {
            self.0
                .lock()
                .unwrap()
                .extend(msgs.iter().map(|m| m.topic.clone()));
            ConsumeConcurrentlyStatus::ConsumeSuccess
        }
    }

    struct TopicRecordingOrderlyListener(Arc<Mutex<Vec<String>>>);

    impl MessageListenerOrderly for TopicRecordingOrderlyListener {
        fn consume_message(
            &self,
            msgs: &[MessageExt],
            _context: &mut ConsumeOrderlyContext,
        ) -> ConsumeOrderlyStatus {
            self.0
                .lock()
                .unwrap()
                .extend(msgs.iter().map(|m| m.topic.clone()));
            ConsumeOrderlyStatus::Success
        }
    }

    /// A POP row under a given topic: its own offset as both CK offsets, a live popTime so the
    /// batch is not judged timed out, and the reconsume count the budget is judged on.
    fn pop_row(topic: &str, queue_offset: i64, reconsume_times: i32) -> MessageExt {
        let mut m = ext(topic, None);
        m.queue_id = 0;
        m.queue_offset = queue_offset;
        m.broker_name = Some("broker-a".to_string());
        m.reconsume_times = reconsume_times;
        m.put_property(
            PROPERTY_POP_CK,
            &extra_info::build_extra_info(
                queue_offset,
                current_time_millis(),
                60_000,
                0,
                topic,
                "broker-a",
                0,
                Some(queue_offset),
            ),
        );
        m
    }

    // ---------------- local (BROADCASTING) offsets ----------------

    /// The offset file is a flat JSON object of "queue key -> offset".
    #[test]
    fn local_offsets_are_a_flat_json_object_of_queue_key() {
        let consumer = DefaultMQPushConsumer::new("RustTestNeverWrittenGroup").unwrap();
        let Some(path) = local_offset_path(&consumer.inner) else {
            // Without a HOME env var there is no local path and loading degrades to an empty table
            assert!(load_local_offsets(&consumer.inner).is_empty());
            return;
        };
        assert!(path.ends_with("RustTestNeverWrittenGroup/offsets.json"));
        assert!(!path.exists(), "this test writes no file");
        // File missing -> empty table (any read failure counts as "no offsets", no exception)
        assert!(load_local_offsets(&consumer.inner).is_empty());

        let items: BTreeMap<String, i64> =
            BTreeMap::from([(mq_key(&queue("T", "broker-a", 0)), 42)]);
        let text = serde_json::to_string(&items).unwrap();
        assert_eq!(text, r#"{"Tbroker-a0":42}"#);
        let back: BTreeMap<String, i64> = serde_json::from_str(&text).unwrap();
        assert_eq!(back.get("Tbroker-a0"), Some(&42));
        // Bad JSON makes the load fail -> degrades to an empty table
        assert!(serde_json::from_str::<BTreeMap<String, i64>>("[not, an, object]").is_err());
    }

    // ---------------- the pop view of runningInfo ----------------

    #[test]
    fn pop_running_info_reports_every_popped_queue() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.pop_mode = true);
        let q0 = queue("T", "broker-a", 0);
        let q1 = queue("T", "broker-a", 1);
        let (k0, k1) = (mq_key(&q0), mq_key(&q1));
        {
            let mut state = lock(&consumer.inner.state);
            // Deliberately inserted out of order to verify the output is sorted by key (stable when the broker replies to ops)
            state.assigned = vec![q1.clone(), q0.clone()];
            state.pop_queues.insert(k1.clone(), PopProcessQueue::new());
            let pq0 = PopProcessQueue::new();
            pq0.inc_found_msg(3);
            state.pop_queues.insert(k0, pq0);
        }
        let info = consumer.consumer_running_info();
        let keys: Vec<String> = info
            .mq_pop_table
            .iter()
            .map(|(k, _)| format!("{}{}{}", k.topic, k.broker_name, k.queue_id))
            .collect();
        assert_eq!(
            keys,
            vec!["Tbroker-a0".to_string(), "Tbroker-a1".to_string()]
        );
        assert!(
            info.mq_table.is_empty(),
            "pop mode has no pull queues, mqTable must be empty"
        );
        // The in-flight unacked count goes through waitAckCount, and the entry carries exactly
        // the three pop fields.
        let first = info.mq_pop_table[0].1.as_object().unwrap();
        assert_eq!(
            first.get("waitAckCount"),
            Some(&serde_json::Value::from(3))
        );
        assert_eq!(first.get("droped"), Some(&serde_json::Value::from(false)));
        assert!(first.contains_key("lastPopTimestamp"));
        assert_eq!(first.len(), 3);
        assert_eq!(
            info.mq_pop_table[1].1.get("waitAckCount"),
            Some(&serde_json::Value::from(0))
        );
    }

    /// `locked_queue_keys` is the only observable of the orderly-consumption lock state: `ProcessQueueInfo.locked`
    /// here is never backfilled.
    #[test]
    fn locked_queue_keys_are_sorted_and_start_empty() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        assert!(consumer.locked_queue_keys().is_empty());
        {
            let mut state = lock(&consumer.inner.state);
            state.lock_ok.insert(mq_key(&queue("T", "broker-a", 1)));
            state.lock_ok.insert(mq_key(&queue("T", "broker-a", 0)));
        }
        assert_eq!(
            consumer.locked_queue_keys(),
            vec!["Tbroker-a0".to_string(), "Tbroker-a1".to_string()]
        );
    }


    /// Stamps the clock directly, bypassing the real loop (this module has no `MQClientInstance`; the loop would exit immediately).
    fn stamp_at(consumer: &DefaultMQPushConsumer, key: &str, at: i64) {
        lock(&consumer.inner.state)
            .last_pull_at
            .insert(key.to_string(), at);
    }

    fn stalled(consumer: &DefaultMQPushConsumer, key: &str, now: i64) -> bool {
        let state = lock(&consumer.inner.state);
        pull_stalled_locked(&state, key, now)
    }

    /// Makes the consumer look "started" so the self-healing checks take part in rebalance;
    /// but no `MQClientInstance` is injected, so persisting a revoked queue's offsets short-circuits on this path.
    fn mark_started(consumer: &DefaultMQPushConsumer) {
        consumer.inner.started.store(true, Ordering::SeqCst);
    }

    #[test]
    fn pull_idle_threshold_matches_contract_and_is_strictly_greater() {
        assert_eq!(PULL_MAX_IDLE_TIME, 120_000);
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let key = mq_key(&queue("T", "broker-a", 0));
        let began = 1_700_000_000_000;

        // Never stamped = just assigned, the first round has not started yet; must not be judged stalled
        assert!(!stalled(&consumer, &key, began + 10 * 60_000));
        stamp_at(&consumer, &key, began);
        assert!(!stalled(&consumer, &key, began + PULL_MAX_IDLE_TIME - 1));
        assert!(!stalled(&consumer, &key, began + PULL_MAX_IDLE_TIME));
        assert!(stalled(&consumer, &key, began + PULL_MAX_IDLE_TIME + 1));
        // Sentinel 0 = the loop has exited (a tokio task has no `Thread.is_alive()` to ask)
        stamp_at(&consumer, &key, 0);
        assert!(stalled(&consumer, &key, 0));
        // Per-queue independence: one stalled queue does not drag in another
        let other = mq_key(&queue("T", "broker-a", 1));
        stamp_at(&consumer, &other, began);
        assert!(!stalled(&consumer, &other, began + 1000));
    }

    #[test]
    fn stamping_marks_both_pop_clocks() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        let pq = PopProcessQueue::new();
        pq.set_dropped(true); // a once-revoked queue must not be revived by stamping
        lock(&consumer.inner.state)
            .pop_queues
            .insert(key.clone(), pq.clone());
        stamp_at(&consumer, &key, 0);
        assert!(stalled(&consumer, &key, current_time_millis()));

        stamp_pull_at(&consumer.inner, &key, true);
        let popped = {
            let state = lock(&consumer.inner.state);
            assert!(!pull_stalled_locked(&state, &key, current_time_millis()));
            pq.last_pop_timestamp.load(Ordering::SeqCst)
        };
        assert!(current_time_millis() - popped < 5000);

        // Loop exit leaves the sentinel behind, so the next rebalance round can see it
        lock(&consumer.inner.state)
            .queue_owners
            .insert(key.clone(), 42);
        mark_pull_loop_exited(&consumer.inner, &key, 42);
        assert_eq!(
            lock(&consumer.inner.state).last_pull_at.get(&key).copied(),
            Some(0)
        );
        // When ownership has moved on, the sentinel must not be written again, or it would hurt the new owner
        let other = mq_key(&queue("T", "broker-a", 1));
        stamp_at(&consumer, &other, 555);
        lock(&consumer.inner.state)
            .queue_owners
            .insert(other.clone(), 7);
        mark_pull_loop_exited(&consumer.inner, &other, 999); // token mismatch
        assert_eq!(
            lock(&consumer.inner.state)
                .last_pull_at
                .get(&other)
                .copied(),
            Some(555)
        );
    }

    /// `retire_queue_locked`: revoking a queue must strip every trace and hand the consumed offset to the persist path.
    #[test]
    fn retiring_clears_every_trace_and_reports_the_offset_to_persist() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        let pq = PopProcessQueue::new();
        {
            let mut state = lock(&consumer.inner.state);
            state.queue_owners.insert(key.clone(), 7);
            state.mq_map.insert(key.clone(), mq.clone());
            state.pending.insert(key.clone(), VecDeque::new());
            state.offset_table.insert(key.clone(), 99);
            state.consume_offsets.insert(key.clone(), 42);
            state.lock_ok.insert(key.clone());
            state.last_pull_at.insert(key.clone(), 123);
            state.pop_queues.insert(key.clone(), pq.clone());
            state
                .inflight_msgs
                .insert(key.clone(), vec![ext("T", None)]);
        }
        let mut revoked = Vec::new();
        let mut state = lock(&consumer.inner.state);
        DefaultMQPushConsumer::retire_queue_locked(&mut state, &key, None, &mut revoked, true);
        assert_eq!(revoked, vec![(mq.clone(), Some(42))]);
        assert!(state.queue_owners.is_empty());
        assert!(state.pending.is_empty());
        assert!(
            state.inflight_msgs.is_empty(),
            "after revocation the old batch's references must not linger in the sweep view"
        );
        assert!(state.offset_table.is_empty());
        assert!(state.consume_offsets.is_empty());
        assert!(state.lock_ok.is_empty());
        assert!(state.pop_queues.is_empty());
        assert!(pq.is_dropped());
        // The stamp must be cleared with everything else: a reused queue key inheriting the old moment would be misjudged stalled at once
        assert!(state.last_pull_at.is_empty());

        // Queue not in `mq_map` (stall self-healing: still owned by this instance, only the loop died) takes the fallback
        let mut revoked = Vec::new();
        state.mq_map.insert("gone".to_string(), mq.clone());
        state.queue_owners.insert("gone".to_string(), 3);
        DefaultMQPushConsumer::retire_queue_locked(
            &mut state,
            "gone",
            Some(&mq),
            &mut revoked,
            false,
        );
        // With no consumed offset there is nothing to persist, but the queue still enters the teardown list
        assert_eq!(revoked, vec![(mq, None)]);
        // Non-POP mode must not touch pop_queues (nor conjure one out of thin air)
        assert!(state.pop_queues.is_empty());
    }

    /// Shutdown offset floor clamp ([`shutdown_persist_items`]): if the advanced offset has passed the minimum
    /// offset of messages not yet settled (send-back failures parked in pending / still held by the listener),
    /// the persist list must be clamped back -- otherwise the broker will never redeliver these (real loss on immediate shutdown).
    #[test]
    fn shutdown_persist_items_floors_offsets_below_unsettled_messages() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        let mut m_low = ext("T", None);
        m_low.queue_offset = 5;
        let mut m_high = ext("T", None);
        m_high.queue_offset = 9;
        {
            let mut state = lock(&consumer.inner.state);
            state.mq_map.insert(key.clone(), mq.clone());
            // The offset has already advanced to 10 (e.g. the send-back successes of the same batch settled first)
            state.consume_offsets.insert(key.clone(), 10);
            // One send-back failure parked in pending, one still held by the listener
            state
                .pending
                .insert(key.clone(), VecDeque::from(vec![m_low]));
            state.inflight_msgs.insert(key.clone(), vec![m_high]);
        }
        let items = shutdown_persist_items(&consumer.inner);
        assert_eq!(items, vec![(mq, 5)]);

        // A queue without unsettled messages persists at the advanced offset as-is
        let other = queue("T", "broker-a", 1);
        let okey = mq_key(&other);
        {
            let mut state = lock(&consumer.inner.state);
            state.mq_map.insert(okey.clone(), other.clone());
            state.consume_offsets.insert(okey, 7);
        }
        let items = shutdown_persist_items(&consumer.inner);
        assert!(items.contains(&(other, 7)), "items={items:?}");
    }

    #[tokio::test]
    async fn a_stalled_queue_is_dropped_and_rebuilt_in_one_pass() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        mark_started(&consumer);
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        let before = current_time_millis();
        {
            let mut state = lock(&consumer.inner.state);
            state.assigned = vec![mq.clone()];
            // The old token deliberately uses a value that will never be reissued, so "ownership really changed" is observable
            state.queue_owners.insert(key.clone(), 999);
            state.mq_map.insert(key.clone(), mq.clone());
            state.consume_offsets.insert(key.clone(), 42);
            state
                .pending
                .insert(key.clone(), VecDeque::from(vec![ext("T", None)]));
            state.offset_table.insert(key.clone(), 7);
            state
                .last_pull_at
                .insert(key.clone(), before - PULL_MAX_IDLE_TIME - 1000);
        }
        consumer.rebalance_pull_threads().await;
        let state = lock(&consumer.inner.state);
        // Ownership token changed = the old loop is dead and a new loop took over
        assert_ne!(state.queue_owners.get(&key).copied(), Some(999));
        // Buffer and cursor fully cleared: in-flight messages are no longer consumed by the old loop
        assert!(state.pending.is_empty());
        assert!(state.offset_table.is_empty());
        // The consumed offset is handed to the persist path (no client here; only its removal is verified)
        assert!(!state.consume_offsets.contains_key(&key));
        // The queue object stays registered: self-healing must not make the subscription invisible
        assert_eq!(state.mq_map.get(&key), Some(&mq));
        // The new loop re-times from "now", so the next round will not immediately re-judge it stalled
        let seeded = *state.last_pull_at.get(&key).unwrap_or(&0);
        assert!(
            seeded >= before || seeded == 0,
            "stamp should be reseeded, got {seeded} (before={before})"
        );
    }

    /// A healthy queue must stay **untouched**: swapping the token equals dropping the in-flight messages.
    #[tokio::test]
    async fn a_healthy_queue_keeps_its_loop_and_offset() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        mark_started(&consumer);
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        {
            let mut state = lock(&consumer.inner.state);
            state.assigned = vec![mq.clone()];
            state.queue_owners.insert(key.clone(), 5);
            state.mq_map.insert(key.clone(), mq.clone());
            state.consume_offsets.insert(key.clone(), 42);
            state
                .pending
                .insert(key.clone(), VecDeque::from(vec![ext("T", None)]));
            state
                .last_pull_at
                .insert(key.clone(), current_time_millis());
        }
        consumer.rebalance_pull_threads().await;
        let state = lock(&consumer.inner.state);
        assert_eq!(state.queue_owners.get(&key).copied(), Some(5));
        assert_eq!(state.pending.get(&key).map(VecDeque::len), Some(1));
        assert_eq!(state.consume_offsets.get(&key).copied(), Some(42));
    }

    /// No self-healing while not started (including mid-shutdown): that would turn the teardown into a `[BUG]` log
    /// storm, and the stall criterion itself is meaningless with `started=false`.
    #[tokio::test]
    async fn stalled_sweep_only_runs_while_started() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        {
            let mut state = lock(&consumer.inner.state);
            state.assigned = vec![mq.clone()];
            state.queue_owners.insert(key.clone(), 1);
            state.mq_map.insert(key.clone(), mq.clone());
        }
        stamp_at(&consumer, &key, 1); // as old as it gets
        consumer.rebalance_pull_threads().await;
        assert_eq!(
            lock(&consumer.inner.state).queue_owners.get(&key).copied(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn pop_mode_swaps_the_process_queue_when_self_healing() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        mark_started(&consumer);
        consumer.update_config(|c| c.pop_mode = true);
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        let old = PopProcessQueue::new();
        {
            let mut state = lock(&consumer.inner.state);
            state.assigned = vec![mq.clone()];
            state.queue_owners.insert(key.clone(), 999);
            state.mq_map.insert(key.clone(), mq.clone());
            state.pop_queues.insert(key.clone(), old.clone());
        }
        stamp_at(
            &consumer,
            &key,
            current_time_millis() - PULL_MAX_IDLE_TIME - 1,
        );
        consumer.rebalance_pull_threads().await;
        let state = lock(&consumer.inner.state);
        assert!(old.is_dropped());
        let fresh = state.pop_queues.get(&key).cloned().expect("rebuilt");
        assert!(!Arc::ptr_eq(&old, &fresh));
        assert!(!fresh.is_dropped());
        assert_ne!(state.queue_owners.get(&key).copied(), Some(999));
    }

    #[test]
    fn running_info_publishes_the_pull_clock() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        {
            let mut state = lock(&consumer.inner.state);
            state.mq_map.insert(key.clone(), mq.clone());
        }
        stamp_at(&consumer, &key, 1_700_000_000_456);
        let info = consumer.consumer_running_info();
        let pqi = &info.mq_table[0].1;
        assert_eq!(
            pqi.get("lastPullTimestamp"),
            Some(&serde_json::Value::from(1_700_000_000_456_i64))
        );
        // An unstamped queue reports 0 instead of passing the current time off as "still pulling"
        let other = queue("T", "broker-a", 1);
        lock(&consumer.inner.state)
            .mq_map
            .insert(mq_key(&other), other.clone());
        let info = consumer.consumer_running_info();
        let pqi = &info.mq_table[1].1;
        assert_eq!(
            pqi.get("lastPullTimestamp"),
            Some(&serde_json::Value::from(0))
        );
    }

    #[test]
    fn pop_running_info_publishes_the_pop_clock() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.pop_mode = true);
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);
        let pq = PopProcessQueue::new();
        pq.touch(1_700_000_000_789);
        {
            let mut state = lock(&consumer.inner.state);
            state.assigned = vec![mq.clone()];
            state.pop_queues.insert(key.clone(), pq);
        }
        let info = consumer.consumer_running_info();
        assert_eq!(info.mq_pop_table.len(), 1);
        assert_eq!(
            info.mq_pop_table[0].1.get("lastPopTimestamp"),
            Some(&serde_json::Value::from(1_700_000_000_789_i64))
        );
    }

    /// 221's topic argument: only a non-empty topic filters. The admin side that
    /// asks for "everything this client has consumed" sends the field empty, so
    /// an empty string must not read as "match a topic literally named empty".
    #[test]
    fn consumer_status_blank_topic_reports_the_whole_table() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let mine = queue("T", "broker-a", 0);
        let other = queue("Other", "broker-a", 0);
        {
            let mut state = lock(&consumer.inner.state);
            state
                .mq_map
                .insert(mq_key(&mine), mine.clone());
            state
                .mq_map
                .insert(mq_key(&other), other.clone());
            state.consume_offsets.insert(mq_key(&mine), 42);
            state.consume_offsets.insert(mq_key(&other), 7);
        }
        let keys = |t: Option<&str>| -> Vec<(String, i64)> {
            consumer
                .get_consumer_status(t)
                .into_iter()
                .map(|(mq, off)| (mq.topic, off))
                .collect()
        };
        // Absent, empty and a name that matches nothing are three different asks;
        // the first two both mean "no filter".
        assert_eq!(keys(None), vec![("Other".to_string(), 7), ("T".to_string(), 42)]);
        assert_eq!(
            keys(Some("")),
            vec![("Other".to_string(), 7), ("T".to_string(), 42)],
            "a blank topic must not silently report nothing"
        );
        // Control: the named filter is real, so the blank case above is the
        // no-filter rule and not a handler that ignores its argument.
        assert_eq!(keys(Some("T")), vec![("T".to_string(), 42)]);
        assert!(keys(Some("Never")).is_empty());
    }

    /// Shutdown must clear the stamp table: the next start() must not inherit the old instance's moments.
    #[test]
    fn shutdown_clears_the_pull_clock() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        mark_started(&consumer);
        let key = mq_key(&queue("T", "broker-a", 0));
        stamp_at(&consumer, &key, 123);
        consumer.shutdown();
        assert!(lock(&consumer.inner.state).last_pull_at.is_empty());
        assert!(lock(&consumer.inner.state).queue_owners.is_empty());
    }


    // Contract fixture: output of real fastjson2 2.0.59 + rocketmq-client 5.5.0 (pretty variant included)
    const CONTRACT_OFFSETS_COMPACT: &str = concat!(
        r#"{"offsetTable":{{"brokerName":"broker-a","queueId":1,"topic":"Tt"}:9,"#,
        r#"{"brokerName":"broker-a","queueId":0,"topic":"Tt"}:7}}"#,
    );
    const CONTRACT_OFFSETS_PRETTY: &str =
        "{\n\t\"offsetTable\":{\n\t\t{\"brokerName\":\"broker-a\",\"queueId\":0,\"topic\":\"Tt\"}:7\n\t}\n}";

    fn test_mq_map() -> BTreeMap<String, MessageQueue> {
        (0..3)
            .map(|i| {
                let mq = MessageQueue::new("Tt", "broker-a", i);
                (mq_key(&mq), mq)
            })
            .collect()
    }

    #[test]
    fn local_offsets_build_uses_contract_object_as_key_format() {
        let text = build_local_offsets_json(
            &BTreeMap::from([
                ("Ttbroker-a0".to_string(), 7),
                ("Ttbroker-a1".to_string(), 9),
            ]),
            &test_mq_map(),
        );
        assert!(text.starts_with("{\"offsetTable\":{"));
        // Field order brokerName/queueId/topic (fastjson2 alphabetical), each key is a nested object
        assert!(text.contains(r#""brokerName":"broker-a","queueId":0,"topic":"Tt"}:7"#));
        assert!(text.contains(r#""brokerName":"broker-a","queueId":1,"topic":"Tt"}:9"#));
    }

    #[test]
    fn local_offsets_build_skips_entries_without_queue_info() {
        let text = build_local_offsets_json(
            &BTreeMap::from([("Ttbroker-a0".to_string(), 7), ("Orphan_b0".to_string(), 3)]),
            &test_mq_map(),
        );
        assert!(!text.contains("Orphan"));
    }

    #[test]
    fn local_offsets_parse_contract_format_compact_and_pretty() {
        let compact = parse_local_offsets_text(CONTRACT_OFFSETS_COMPACT).unwrap();
        assert_eq!(compact.get("Ttbroker-a1"), Some(&9));
        assert_eq!(compact.get("Ttbroker-a0"), Some(&7));
        let pretty = parse_local_offsets_text(CONTRACT_OFFSETS_PRETTY).unwrap();
        assert_eq!(pretty.get("Ttbroker-a0"), Some(&7));
    }

    #[test]
    fn local_offsets_parse_accepts_legacy_flat_map() {
        let flat = parse_local_offsets_text(r#"{"Ttbroker-a0":5}"#).unwrap();
        assert_eq!(flat.get("Ttbroker-a0"), Some(&5));
    }

    #[test]
    fn local_offsets_parse_rejects_garbage() {
        assert!(parse_local_offsets_text("{\"offsetTable\":{{{").is_none());
        assert!(parse_local_offsets_text("{\"offsetTable\":").is_none());
        assert!(parse_local_offsets_text("").is_none());
    }

    #[test]
    fn local_offsets_roundtrip_built_text() {
        let text = build_local_offsets_json(
            &BTreeMap::from([
                ("Ttbroker-a0".to_string(), 7),
                ("Ttbroker-a1".to_string(), 9),
                ("Ttbroker-a2".to_string(), 11),
            ]),
            &test_mq_map(),
        );
        let parsed = parse_local_offsets_text(&text).unwrap();
        assert_eq!(parsed.get("Ttbroker-a0"), Some(&7));
        assert_eq!(parsed.get("Ttbroker-a1"), Some(&9));
        assert_eq!(parsed.get("Ttbroker-a2"), Some(&11));
    }

    #[test]
    fn local_offsets_save_writes_contract_format_and_rolls_bak() {
        let dir = std::env::temp_dir().join(format!(
            "rmq_rust_offsets_test_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = dir.join("offsets.json");
        let items = BTreeMap::from([
            ("Ttbroker-a0".to_string(), 7),
            ("Ttbroker-a1".to_string(), 9),
            ("Ttbroker-a2".to_string(), 11),
        ]);
        let mq_map = test_mq_map();

        save_local_offsets_at(&path, &items, &mq_map).unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.starts_with("{\"offsetTable\":{"));
        assert!(!path.with_file_name("offsets.json.bak").exists());

        save_local_offsets_at(
            &path,
            &BTreeMap::from([("Ttbroker-a0".to_string(), 99)]),
            &mq_map,
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(path.with_file_name("offsets.json.bak")).unwrap(),
            raw
        );

        // main file present -> read it; main file missing -> .bak (the previous generation)
        let loaded = load_local_offsets_at(&path);
        assert_eq!(loaded.get("Ttbroker-a0"), Some(&99));
        std::fs::remove_file(&path).unwrap();
        let loaded = load_local_offsets_at(&path);
        assert_eq!(loaded.get("Ttbroker-a0"), Some(&7));

        // .bak missing too -> empty (handled as a first startup)
        std::fs::remove_file(path.with_file_name("offsets.json.bak")).unwrap();
        assert!(load_local_offsets_at(&path).is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn local_offsets_load_reads_contract_written_file() {
        let dir = std::env::temp_dir().join(format!(
            "rmq_rust_offsets_wire_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("offsets.json");
        std::fs::write(&path, CONTRACT_OFFSETS_COMPACT).unwrap();
        let loaded = load_local_offsets_at(&path);
        assert_eq!(loaded.get("Ttbroker-a1"), Some(&9));
        assert_eq!(loaded.get("Ttbroker-a0"), Some(&7));
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Two partial flushes must not lose each other: the file is the union, so a queue absent
    /// from this round (for example one no longer held) keeps the offset it had.
    #[test]
    fn local_offsets_write_merges_with_the_existing_file() {
        let dir = std::env::temp_dir().join(format!(
            "rmq_rust_offsets_merge_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = dir.join("offsets.json");
        let q0 = MessageQueue::new("T", "broker-a", 0);
        let q1 = MessageQueue::new("T", "broker-a", 1);
        write_local_offsets_at(&path, &[(q0.clone(), 5)]).unwrap();
        write_local_offsets_at(&path, &[(q1.clone(), 9)]).unwrap();

        let loaded = load_local_offsets_at(&path);
        assert_eq!(loaded.get(&mq_key(&q0)), Some(&5));
        assert_eq!(loaded.get(&mq_key(&q1)), Some(&9));
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A file that exists but cannot be parsed is not silently replaced by a one-queue table.
    #[test]
    fn local_offsets_write_refuses_an_unreadable_file() {
        let dir = std::env::temp_dir().join(format!(
            "rmq_rust_offsets_corrupt_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("offsets.json");
        let garbage = "{\"offsetTable\":{{{";
        std::fs::write(&path, garbage).unwrap();

        let err = write_local_offsets_at(&path, &[(MessageQueue::new("T", "broker-a", 0), 5)])
            .expect_err("an unreadable table must abort the flush");
        assert!(
            err.to_string().contains("local offset store is unreadable"),
            "{err}"
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), garbage);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn only_broadcasting_uses_the_local_offset_store() {
        assert!(uses_local_offset_store(MessageModel::BROADCASTING));
        assert!(!uses_local_offset_store(MessageModel::CLUSTERING));
    }

    // ---------------- P5: postSubscriptionWhenPull / pullFromWhichNode ----------------
    //
    // Covers the criteria for these two push-side knobs. This layer is pure functions;
    // the real wire shape (whether the SUBSCRIPTION flag and the `subscription` key go online, slave address selection, slave
    // COMMIT_OFFSET clearing, suggestWhichBrokerId passthrough) is pinned down by the fake-broker cases in `mq_client.rs`.

    const P5_TOPIC: &str = "P5PostSubscriptionTopic";

    #[test]
    fn pull_subscription_expression_follows_contract_default() {
        let s = sub(P5_TOPIC, "TagA");
        // default off
        assert_eq!(
            pull_subscription_expression(&ConsumerConfig::default(), &s),
            None
        );
        // enabled
        let cfg = ConsumerConfig {
            post_subscription_when_pull: true,
            ..ConsumerConfig::default()
        };
        assert_eq!(
            pull_subscription_expression(&cfg, &s).as_deref(),
            Some("TagA")
        );
        // class-filter mode: not sent even when enabled (the expression is a filter class name; broker-side TAG filtering would misjudge it)
        let mut class_mode = sub(P5_TOPIC, "com.example.MyFilter");
        class_mode.class_filter_mode = true;
        assert_eq!(pull_subscription_expression(&cfg, &class_mode), None);
    }

    #[test]
    fn pull_from_which_node_defaults_to_master_and_round_trips() {
        let mut state = State::default();
        let mq = MessageQueue::new(P5_TOPIC, "broker-a", 0);
        let other = MessageQueue::new(P5_TOPIC, "broker-a", 1);
        let master = i64::from(MixAll::MASTER_ID);

        assert_eq!(
            recalc_pull_from_which_node(&state, &mq),
            master,
            "first round hits the master"
        );

        // an old broker sends no suggest: bookkeep as master instead of keeping the old value
        update_pull_from_which_node(&mut state, &mq, Some(master + 1));
        assert_eq!(recalc_pull_from_which_node(&state, &mq), master + 1);
        update_pull_from_which_node(&mut state, &mq, None);
        assert_eq!(recalc_pull_from_which_node(&state, &mq), master);

        // isolated per queue
        update_pull_from_which_node(&mut state, &mq, Some(2));
        assert_eq!(recalc_pull_from_which_node(&state, &mq), 2);
        assert_eq!(recalc_pull_from_which_node(&state, &other), master);
    }


    const CTO_TOPIC: &str = "P6CorrectTagsOffsetTopic";

    fn cto_mq() -> MessageQueue {
        queue(CTO_TOPIC, "broker-a", 0)
    }

    /// Drives state directly and calls [`correct_tags_offset_locked`] directly: the state criteria are folded into the function too,
    /// so one call pins down both "which statuses get corrected" and "when correction is allowed".
    struct CtoHarness {
        state: State,
        key: String,
    }

    impl CtoHarness {
        fn new() -> CtoHarness {
            let key = mq_key(&cto_mq());
            let mut state = State::default();
            state.pending.insert(key.clone(), VecDeque::new());
            CtoHarness { state, key }
        }

        fn correct(&mut self, status: PullStatus, next_off: i64) {
            correct_tags_offset_locked(&mut self.state, &self.key, status, next_off);
        }

        fn offset(&self) -> Option<i64> {
            self.state.consume_offsets.get(&self.key).copied()
        }
    }

    /// Both empty-pull statuses (NO_NEW_MSG / NO_MATCHED_MSG) advance; all other statuses leave the offset untouched.
    ///
    /// NO_MATCHED_MSG in this crate comes from the broker's `PULL_RETRY_IMMEDIATELY`
    /// (mapped during pull-response decoding): when every message in a stretch is filtered out by tags/expression,
    /// the client receives no messages and `msgCount` stays 0 -- without a correction it would never advance.
    #[test]
    fn correct_tags_offset_advances_only_on_empty_pull_statuses() {
        for status in [PullStatus::NoNewMsg, PullStatus::NoMatchedMsg] {
            let mut h = CtoHarness::new();
            h.correct(status, 42);
            assert_eq!(h.offset(), Some(42), "{status:?} must lift the offset from 0 to the replied offset");
            h.correct(status, 43);
            assert_eq!(h.offset(), Some(43), "keeps advancing by the replied offset once a record exists");
        }
        for status in [PullStatus::Found, PullStatus::OffsetIllegal] {
            let mut h = CtoHarness::new();
            h.correct(status, 42);
            assert_eq!(h.offset(), None, "{status:?} is not an empty pull; the offset must not be touched");
        }
    }

    #[test]
    fn correct_tags_offset_creates_the_record_even_for_a_zero_offset() {
        let mut h = CtoHarness::new();
        h.correct(PullStatus::NoNewMsg, 0);
        assert_eq!(h.offset(), Some(0), "even a 0 for an empty queue must create the record");
        h.correct(PullStatus::NoNewMsg, 0);
        assert_eq!(h.offset(), Some(0), "correcting again after creation: neither advances nor regresses");
    }

    /// `increaseOnly=true`: advance only, never regress.
    #[test]
    fn correct_tags_offset_never_regresses() {
        let mut h = CtoHarness::new();
        h.state.consume_offsets.insert(h.key.clone(), 100);
        h.correct(PullStatus::NoNewMsg, 50);
        assert_eq!(h.offset(), Some(100), "replied offset is smaller: stay in place");
        h.correct(PullStatus::NoNewMsg, 100);
        assert_eq!(h.offset(), Some(100), "equal: no change");
        h.correct(PullStatus::NoNewMsg, 101);
        assert_eq!(h.offset(), Some(101));
    }

    /// Gate: while `ProcessQueue` still holds messages (buffered plus in the listener's hands), the offset is never touched.
    #[test]
    fn correct_tags_offset_respects_the_process_queue_guard() {
        // one message still buffered and unconsumed
        let mut h = CtoHarness::new();
        h.state
            .pending
            .get_mut(&h.key)
            .unwrap()
            .push_back(ext(CTO_TOPIC, None));
        h.correct(PullStatus::NoNewMsg, 42);
        assert_eq!(h.offset(), None, "pending is not empty");

        // buffer is empty, but one message is in the listener's hands
        let mut h = CtoHarness::new();
        h.state.inflight.insert(h.key.clone(), 1);
        h.correct(PullStatus::NoMatchedMsg, 42);
        assert_eq!(h.offset(), None, "in-flight also counts as msgCount != 0");

        // an explicit 0 (briefly possible while wrapping up) does not block
        h.state.inflight.insert(h.key.clone(), 0);
        h.correct(PullStatus::NoNewMsg, 42);
        assert_eq!(h.offset(), Some(42), "correction is allowed once inflight drops to zero");

        // no pending entry at all (queue never pulled) counts as empty; a missing entry must not skip the correction
        let mut fresh = CtoHarness {
            state: State::default(),
            key: mq_key(&cto_mq()),
        };
        fresh.correct(PullStatus::NoNewMsg, 7);
        assert_eq!(fresh.offset(), Some(7));
    }

    /// The latch on the listener side:
    /// - on entry, first record the result of "one correction call while the batch is in flight" (do **not** assert here: the listener runs on
    ///   a `spawn_blocking` thread, where a panic is swallowed by `call_concurrently_listener` into
    ///   `RECONSUME_LATER`, and the test would "pass" after all);
    /// - then block until the main thread releases it -- freezing exactly the instant when the batch is in flight.
    ///
    /// The latch uses tokio channels rather than `std::sync::mpsc`: the latter's `Sender` is not `Sync`,
    /// so it cannot enter the `Send + Sync` listener trait.
    struct GatedListener {
        inner: Arc<Inner>,
        key: String,
        entered: tokio::sync::mpsc::UnboundedSender<Option<i64>>,
        release: Mutex<tokio::sync::mpsc::UnboundedReceiver<()>>,
    }

    impl MessageListenerConcurrently for GatedListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            _context: &mut ConsumeConcurrentlyContext,
        ) -> ConsumeConcurrentlyStatus {
            let observed = {
                let mut state = lock(&self.inner.state);
                correct_tags_offset_locked(&mut state, &self.key, PullStatus::NoNewMsg, 110);
                state.consume_offsets.get(&self.key).copied()
            };
            let _ = self.entered.send(observed);
            let _ = self.release.lock().expect("release lock").blocking_recv();
            ConsumeConcurrentlyStatus::ConsumeSuccess
        }
    }

    /// For the live-cluster path (broker filtering -> NO_MATCHED_MSG -> broker-side offset advance), see
    /// `examples/live_correct_tags_offset.rs`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_loop_keeps_the_correction_out_until_the_listener_returns() {
        let cfg = ConsumerConfig {
            consume_message_batch_max_size: 2,
            ..Default::default()
        };
        let c = DefaultMQPushConsumer::with_config(cfg).unwrap();
        let mq = cto_mq();
        let key = mq_key(&mq);

        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let (release_tx, release_rx) = tokio::sync::mpsc::unbounded_channel();
        c.set_message_listener_concurrently(Arc::new(GatedListener {
            inner: c.inner.clone(),
            key: key.clone(),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        }));

        {
            let mut state = lock(&c.inner.state);
            state.mq_map.insert(key.clone(), mq.clone());
            let mut batch = offset_batch(2);
            for m in batch.iter_mut() {
                m.topic = CTO_TOPIC.to_string();
            }
            state
                .pending
                .insert(key.clone(), batch.into_iter().collect());
        }
        c.inner.started.store(true, Ordering::Release);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(dispatch_loop(c.inner.clone(), stop_rx));

        // listener has entered: batch in flight
        let observed = tokio::time::timeout(Duration::from_secs(5), entered_rx.recv())
            .await
            .expect("listener was not called (5s)")
            .expect("entered channel closed unexpectedly");
        assert_eq!(observed, None, "the offset must not move while the batch is in flight");
        assert_eq!(
            lock(&c.inner.state).inflight.get(&key).copied(),
            Some(1),
            "inflight must be 1 while the batch is in flight"
        );

        release_tx.send(()).expect("release channel already closed");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if lock(&c.inner.state)
                .inflight
                .get(&key)
                .copied()
                .unwrap_or(0)
                == 0
            {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "batch not settled within 5s");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // successful consumption has already pushed the offset to max(queueOffset)+1; only now may the empty-pull correction take effect
        assert_eq!(
            lock(&c.inner.state).consume_offsets.get(&key).copied(),
            Some(2)
        );
        {
            let mut state = lock(&c.inner.state);
            correct_tags_offset_locked(&mut state, &key, PullStatus::NoMatchedMsg, 110);
            assert_eq!(state.consume_offsets.get(&key).copied(), Some(110));
        }
        // once the counter reaches zero the entry should be removed; do not pile up orphaned keys
        assert!(!lock(&c.inner.state).inflight.contains_key(&key));

        drop(stop_tx);
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }

    // ---------------- cleanExpiredMsg (escape hatch for a hung listener) ----------------

    #[test]
    fn clean_expire_period_equals_consume_timeout_minutes() {
        assert_eq!(clean_expire_period_millis(15), 900_000, "default 15 minutes");
        assert_eq!(clean_expire_period_millis(1), 60_000);
        assert_eq!(
            clean_expire_period_millis(0),
            60_000,
            "0 clamps to 1 minute; avoids busy-waiting"
        );
        assert_eq!(clean_expire_period_millis(-3), 60_000);
    }

    #[test]
    fn consume_start_timestamp_reads_only_well_formed_stamps() {
        let mut m = ext("T", None);
        assert_eq!(consume_start_timestamp(&m), None, "no stamp");
        m.put_property(PROPERTY_CONSUME_START_TIMESTAMP, "");
        assert_eq!(consume_start_timestamp(&m), None, "empty string");
        m.put_property(PROPERTY_CONSUME_START_TIMESTAMP, "not-a-number");
        assert_eq!(consume_start_timestamp(&m), None, "malformed value");
        m.put_property(PROPERTY_CONSUME_START_TIMESTAMP, "1700000000000");
        assert_eq!(consume_start_timestamp(&m), Some(1_700_000_000_000));
    }

    /// Expiry uses a **strictly** greater comparison (equal to the threshold does nothing); a missing stamp is not treated as expired.
    #[test]
    fn is_consume_expired_is_strict_and_ignores_missing_stamps() {
        let now = 10_000_000i64;
        assert!(
            is_consume_expired(Some(now - 60_001), now, 1),
            "1ms past the threshold"
        );
        assert!(
            !is_consume_expired(Some(now - 60_000), now, 1),
            "exactly at the threshold: only strictly greater acts"
        );
        assert!(!is_consume_expired(Some(now), now, 1));
        assert!(!is_consume_expired(None, now, 1), "a missing stamp is not treated as expired");
        // default 15 minutes: 14m59.999s does nothing, 15m plus 1ms acts
        assert!(!is_consume_expired(Some(now - 899_999), now, 15));
        assert!(is_consume_expired(Some(now - 900_001), now, 15));
    }

    #[tokio::test]
    async fn clean_expired_queue_stops_at_a_fresh_head() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.consume_timeout = 1);
        let key = mq_key(&queue("T", "broker-a", 0));
        let now = current_time_millis();
        {
            let mut state = lock(&consumer.inner.state);
            let mut head = ext("T", None); // head carries no stamp
            head.queue_offset = 0;
            let mut tail = ext("T", None); // the tail is expired but must still not be touched
            tail.queue_offset = 1;
            tail.put_property(
                PROPERTY_CONSUME_START_TIMESTAMP,
                &(now - 120_000).to_string(),
            );
            state
                .pending
                .insert(key.clone(), VecDeque::from(vec![head, tail]));
        }
        assert_eq!(
            clean_expired_queue(&consumer.inner, &key).await,
            0,
            "head not expired: later entries cannot be either (stop at the first hit)"
        );
        let state = lock(&consumer.inner.state);
        assert_eq!(state.pending.get(&key).unwrap().len(), 2, "not a single entry may be touched");
    }

    #[tokio::test]
    async fn clean_expired_queue_leaves_unsigned_messages_alone() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.consume_timeout = 1);
        let key = mq_key(&queue("T", "broker-a", 0));
        {
            let mut state = lock(&consumer.inner.state);
            let mut m = ext("T", None);
            m.queue_offset = 0;
            state.pending.insert(key.clone(), VecDeque::from(vec![m]));
        }
        assert_eq!(clean_expired_queue(&consumer.inner, &key).await, 0);
        assert_eq!(
            lock(&consumer.inner.state).pending.get(&key).unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn clean_expired_queue_keeps_the_entry_when_send_back_fails() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.consume_timeout = 1);
        let key = mq_key(&queue("T", "broker-a", 0));
        let now = current_time_millis();
        let expired = |off: i64| {
            let mut m = ext("T", None);
            m.queue_offset = off;
            m.put_property(
                PROPERTY_CONSUME_START_TIMESTAMP,
                &(now - 120_000).to_string(),
            );
            m
        };
        {
            let mut state = lock(&consumer.inner.state);
            state
                .pending
                .insert(key.clone(), VecDeque::from(vec![expired(0), expired(1)]));
        }
        assert_eq!(
            clean_expired_queue(&consumer.inner, &key).await,
            2,
            "loop=min(2,16)=2: after a failed send-back this round still retries the same head once more"
        );
        let state = lock(&consumer.inner.state);
        assert_eq!(
            state.pending.get(&key).unwrap().len(),
            2,
            "a failed send-back never removes the entry (removing it would truly lose the message)"
        );
        assert!(!state.consume_offsets.contains_key(&key), "the offset must not be touched");
    }

    #[tokio::test]
    async fn clean_expired_queue_caps_one_round_at_sixteen() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.consume_timeout = 1);
        let key = mq_key(&queue("T", "broker-a", 0));
        let now = current_time_millis();
        let batch: Vec<MessageExt> = (0..20)
            .map(|i| {
                let mut m = ext("T", None);
                m.queue_offset = i;
                m.put_property(
                    PROPERTY_CONSUME_START_TIMESTAMP,
                    &(now - 120_000).to_string(),
                );
                m
            })
            .collect();
        lock(&consumer.inner.state)
            .pending
            .insert(key.clone(), batch.into());
        assert_eq!(
            clean_expired_queue(&consumer.inner, &key).await,
            16,
            "loop is computed once before the loop: cap 16"
        );
    }

    /// Orderly consumption has no such path (the orderly `ProcessQueue` early-returns right away).
    #[tokio::test]
    async fn clean_expired_queue_skips_orderly_consumers() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.consume_timeout = 1);
        consumer.set_message_listener_orderly(Arc::new(NoopOrderlyListener));
        let key = mq_key(&queue("T", "broker-a", 0));
        let mut m = ext("T", None);
        m.queue_offset = 0;
        m.put_property(
            PROPERTY_CONSUME_START_TIMESTAMP,
            &(current_time_millis() - 120_000).to_string(),
        );
        lock(&consumer.inner.state)
            .pending
            .insert(key.clone(), VecDeque::from(vec![m]));
        assert_eq!(clean_expired_queue(&consumer.inner, &key).await, 0);
    }

    /// The two ways "remove only while still head" yields: normal wrap-up (entry already removed/advanced) and a smaller offset appearing.
    #[test]
    fn remove_expired_entry_only_when_still_head() {
        let key = mq_key(&queue("T", "broker-a", 0));
        let msg = |off: i64| {
            let mut m = ext("T", None);
            m.queue_offset = off;
            m
        };

        // still the head: remove from both sides together
        let mut state = State::default();
        state
            .inflight_msgs
            .insert(key.clone(), vec![msg(0), msg(1)]);
        state
            .pending
            .insert(key.clone(), VecDeque::from(vec![msg(0)]));
        assert!(remove_expired_entry_if_still_head(&mut state, &key, 0));
        assert_eq!(
            state.inflight_msgs.get(&key).unwrap().len(),
            1,
            "removed from the registry side"
        );
        assert!(state.pending.get(&key).unwrap().is_empty(), "removed from the buffer side too");

        // a smaller offset appeared ahead: yield; must not race to remove it
        let mut state = State::default();
        state.inflight_msgs.insert(key.clone(), vec![msg(5)]);
        state
            .pending
            .insert(key.clone(), VecDeque::from(vec![msg(3)]));
        assert!(!remove_expired_entry_if_still_head(&mut state, &key, 5));
        assert_eq!(state.inflight_msgs.get(&key).unwrap().len(), 1);

        // already gone from the view (the listener removed it during normal wrap-up): still false, and no panic
        let mut state = State::default();
        assert!(!remove_expired_entry_if_still_head(&mut state, &key, 0));
    }

    /// The union view and containsMessage: entries removed by the sweep become invisible to the send-back criteria immediately.
    #[test]
    fn process_queue_view_unions_inflight_and_pending_by_offset() {
        let key = mq_key(&queue("T", "broker-a", 0));
        let msg = |off: i64| {
            let mut m = ext("T", None);
            m.queue_offset = off;
            m
        };
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        {
            let mut state = lock(&consumer.inner.state);
            state.inflight_msgs.insert(key.clone(), vec![msg(1)]);
            state
                .pending
                .insert(key.clone(), VecDeque::from(vec![msg(2), msg(1)]));
            assert_eq!(
                process_queue_entries_locked(&state, &key).len(),
                2,
                "deduplicated by offset (registry and buffer overlap counts as one)"
            );
            assert_eq!(
                process_queue_first_entry_locked(&state, &key)
                    .unwrap()
                    .queue_offset,
                1
            );
            assert!(process_queue_contains_locked(&state, &key, 1));
            assert!(process_queue_contains_locked(&state, &key, 2));
            assert!(!process_queue_contains_locked(&state, &key, 3));
        }
        {
            let mut state = lock(&consumer.inner.state);
            remove_process_queue_entry_locked(&mut state, &key, 1);
            assert!(!process_queue_contains_locked(&state, &key, 1));
            assert!(process_queue_contains_locked(&state, &key, 2));
            assert!(!state.inflight_msgs.contains_key(&key), "empty buckets must be dropped");
        }
    }

    #[tokio::test]
    async fn send_back_batch_skips_entries_swept_away() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        let key = mq_key(&queue("T", "broker-a", 0));
        let batch: Vec<MessageExt> = {
            let mut a = ext("T", None);
            a.queue_offset = 0;
            let mut b = ext("T", None);
            b.queue_offset = 1;
            vec![a, b]
        };
        // both entries are in the sweep view (normal in-flight state)
        register_inflight_msgs(&consumer.inner, &key, &batch);
        // the sweep removed o1 (simulating a send-back to the broker after the hang timeout): it is gone from the view from then on
        {
            let mut state = lock(&consumer.inner.state);
            remove_process_queue_entry_locked(&mut state, &key, 1);
            assert!(!process_queue_contains_locked(&state, &key, 1));
        }

        let failed = send_back_batch(&consumer.inner, &key, &batch, 0, 0).await;
        assert_eq!(
            failed.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            vec![0],
            "o0 attempts a send-back but fails (not started); o1 was swept away and is skipped outright"
        );
        assert_eq!(failed[0].1.reconsume_times, 1, "failure increments in place");
        assert_eq!(batch[1].reconsume_times, 0, "skipped entries must not get their count bumped");
    }

    #[test]
    fn send_back_lookup_ignores_started_flag_until_instance_is_taken() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        assert!(
            require_client(&consumer.inner).is_err(),
            "the public path still requires the started gate"
        );
        assert!(
            require_client_for_send_back(&consumer.inner).is_err(),
            "with no instance, nobody gets through"
        );

        *lock(&consumer.inner.client) =
            Some(MQClientInstance::new("gid-shutdown-sendback", vec![]));
        // first step of shutdown(): set the flag false before draining in-flight batches.
        consumer.inner.started.store(false, Ordering::Release);
        assert!(require_client(&consumer.inner).is_err());
        assert!(
            require_client_for_send_back(&consumer.inner).is_ok(),
            "during the drain window a send-back must still be able to fetch the instance"
        );

        lock(&consumer.inner.client).take();
        assert!(require_client_for_send_back(&consumer.inner).is_err());
    }

    /// Wiring evidence: when `dispatch_loop` takes the concurrent path it must register this batch **after stamping** into the sweep view,
    /// and unregister it after wrap-up. Registering too early (before the stamp) would put unstamped copies in the table and the sweep would never see them;
    /// skipping the unregister leaves the offset permanently stuck behind the correctTagsOffset in-flight gate.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_registers_a_stamped_inflight_view() {
        let cfg = ConsumerConfig {
            consume_message_batch_max_size: 2,
            consume_timeout: 1,
            ..Default::default()
        };
        let c = DefaultMQPushConsumer::with_config(cfg).unwrap();
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);

        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let (release_tx, release_rx) = tokio::sync::mpsc::unbounded_channel();
        c.set_message_listener_concurrently(Arc::new(GatedListener {
            inner: c.inner.clone(),
            key: key.clone(),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        }));
        {
            let mut state = lock(&c.inner.state);
            state.mq_map.insert(key.clone(), mq.clone());
            state.pending.insert(key.clone(), offset_batch(2).into());
        }
        c.inner.started.store(true, Ordering::Release);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(dispatch_loop(c.inner.clone(), stop_rx));

        let _ = tokio::time::timeout(Duration::from_secs(5), entered_rx.recv())
            .await
            .expect("listener was not called (5s)")
            .expect("entered channel closed unexpectedly");
        {
            let state = lock(&c.inner.state);
            let entries = process_queue_entries_locked(&state, &key);
            assert_eq!(entries.len(), 2, "the in-flight batch must appear in the sweep view");
            assert!(
                entries.iter().all(|m| consume_start_timestamp(m).is_some()),
                "the copies registered into the view must already carry the CONSUME_START_TIME stamp"
            );
            assert!(
                !is_consume_expired(
                    consume_start_timestamp(&entries[0]),
                    current_time_millis(),
                    1
                ),
                "a freshly registered message must not be judged expired right away"
            );
        }

        release_tx.send(()).expect("release channel already closed");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if !lock(&c.inner.state).inflight_msgs.contains_key(&key) {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "registration not unregistered within 5s");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // the sweep view thereby returns to the "only buffered messages" state
        assert!(process_queue_entries_locked(&lock(&c.inner.state), &key).is_empty());

        drop(stop_tx);
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }

    #[tokio::test]
    async fn clean_expired_msg_once_scans_held_queues_only() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.consume_timeout = 1);
        let held = mq_key(&queue("T", "broker-a", 0));
        let other = mq_key(&queue("T", "broker-a", 1));
        let now = current_time_millis();
        let expired = {
            let mut m = ext("T", None);
            m.queue_offset = 0;
            m.put_property(
                PROPERTY_CONSUME_START_TIMESTAMP,
                &(now - 120_000).to_string(),
            );
            m
        };
        {
            let mut state = lock(&consumer.inner.state);
            state.mq_map.insert(held.clone(), queue("T", "broker-a", 0));
            state
                .pending
                .insert(held.clone(), VecDeque::from(vec![expired.clone()]));
            // the other queue is not in mq_map (already revoked): even a leftover buffer entry must not be swept
            state
                .pending
                .insert(other.clone(), VecDeque::from(vec![expired]));
        }
        assert_eq!(
            clean_expired_msg_once(&consumer.inner).await,
            1,
            "only the still-held queue gets scanned"
        );
    }

    #[tokio::test]
    async fn pop_dispatch_stamps_the_consume_start_time() {
        let consumer = DefaultMQPushConsumer::new("G").unwrap();
        consumer.update_config(|c| c.pop_mode = true);
        let seen = Arc::new(Mutex::new(Vec::<Option<i64>>::new()));
        consumer.set_message_listener_concurrently(Arc::new(StampRecordingListener(seen.clone())));
        let inner = consumer.inner.clone();
        let mq = queue("T", "broker-a", 0);
        let pq = PopProcessQueue::new();
        pq.inc_found_msg(1);
        submit_pop_consume_request(&inner, vec![pop_ck_msg(0)], pq.clone(), mq.clone()).await;
        let stamps = seen.lock().unwrap().clone();
        assert_eq!(stamps.len(), 1);
        assert!(
            stamps[0].is_some(),
            "a POP batch must be stamped with CONSUME_START_TIME before the listener"
        );
    }

    /// Records the CONSUME_START_TIME the listener sees (POP stamping evidence).
    struct StampRecordingListener(Arc<Mutex<Vec<Option<i64>>>>);

    impl MessageListenerConcurrently for StampRecordingListener {
        fn consume_message(
            &self,
            msgs: &[MessageExt],
            _context: &mut ConsumeConcurrentlyContext,
        ) -> ConsumeConcurrentlyStatus {
            self.0
                .lock()
                .unwrap()
                .extend(msgs.iter().map(consume_start_timestamp));
            ConsumeConcurrentlyStatus::ConsumeSuccess
        }
    }

    // Getting this path wrong fails **silently** in two opposite ways --
    //
    //   * moving only the pull cursor to `nextBeginOffset` without dropping the queue/freezing it: the broker just corrected the offset into the legal
    //     range, but the old batches on this queue that were **already fetched but not acked** push the offset back to the illegal value the moment they ack,
    //     and the next pull gets rejected by the broker again -- the client and the broker bounce back and forth, forever;
    //   * the corrected offset is not persisted right away: if the process crashes before the next periodic persist (default 5s), what remains on the
    //     broker is still the illegal offset, and pulls after a restart resume from it -- the correction might as well never have happened.
    //
    // Neither direction is distinguishable in a short live window (messages still get consumed; only constant bouncing or a single crash exposes it),
    // so the assertions are all offline; a separate live path proves it (see `examples/live_offset_illegal.rs`).

    /// Simulates the offset advance after the listener returns (exactly what `consume_batch` calls at wrap-up).
    fn ack_batch(c: &DefaultMQPushConsumer, key: &str, offsets: &[i64], epoch: u64) {
        let batch: Vec<MessageExt> = offsets
            .iter()
            .map(|o| {
                let mut m = ext("T", None);
                m.broker_name = Some("broker-a".to_string());
                m.queue_offset = *o;
                m
            })
            .collect();
        advance_consume_offset(&c.inner, key, &batch, None, Some(epoch));
    }

    struct IllegalHarness {
        c: DefaultMQPushConsumer,
        key: String,
        mq: MessageQueue,
    }

    impl IllegalHarness {
        /// `assigned=false` simulates "the queue is no longer owned by this instance"; a non-zero `epoch` presets the epoch.
        fn new(assigned: bool, epoch: u64) -> IllegalHarness {
            let cfg = ConsumerConfig {
                consumer_group: "G".to_string(),
                message_model: MessageModel::CLUSTERING.to_string(),
                ..Default::default()
            };
            let c = DefaultMQPushConsumer::with_config(cfg).unwrap();
            let mq = queue("T", "broker-a", 0);
            let key = mq_key(&mq);
            if assigned {
                let mut state = lock(&c.inner.state);
                state.mq_map.insert(key.clone(), mq.clone());
                state.queue_owners.insert(key.clone(), 1);
                state.offset_table.insert(key.clone(), 3);
                state.consume_offsets.insert(key.clone(), 3);
                state.last_pull_at.insert(key.clone(), 1_700_000_000_000);
                if epoch != 0 {
                    state.queue_epoch.insert(key.clone(), epoch);
                }
            }
            IllegalHarness { c, key, mq }
        }

        /// Simulates the in-lock actions for `OFFSET_ILLEGAL` in the pull callback (cursor + freeze + corrected value).
        fn freeze(&self, off: i64) {
            let mut state = lock(&self.c.inner.state);
            state.offset_table.insert(self.key.clone(), off);
            state.frozen_offsets.insert(self.key.clone());
            state.consume_offsets.insert(self.key.clone(), off);
        }

        /// The queue-retiring half of `handle_offset_illegal` (in-lock); returns the objects to persist.
        fn retire(&self) -> Vec<(MessageQueue, Option<i64>)> {
            let mut revoked = Vec::new();
            let mut state = lock(&self.c.inner.state);
            DefaultMQPushConsumer::retire_queue_locked(
                &mut state,
                &self.key,
                None,
                &mut revoked,
                false,
            );
            revoked
        }

        fn offset(&self) -> Option<i64> {
            lock(&self.c.inner.state)
                .consume_offsets
                .get(&self.key)
                .copied()
        }

        fn epoch(&self) -> u64 {
            lock(&self.c.inner.state)
                .queue_epoch
                .get(&self.key)
                .copied()
                .unwrap_or(0)
        }

        fn frozen(&self) -> bool {
            lock(&self.c.inner.state).frozen_offsets.contains(&self.key)
        }

        fn ack(&self, offsets: &[i64], epoch: u64) {
            ack_batch(&self.c, &self.key, offsets, epoch);
        }
    }

    /// Counts how many times the listener was called (dropped batches must not even reach the listener).
    struct CountingListener {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl MessageListenerConcurrently for CountingListener {
        fn consume_message(
            &self,
            _msgs: &[MessageExt],
            _context: &mut ConsumeConcurrentlyContext,
        ) -> ConsumeConcurrentlyStatus {
            self.calls.fetch_add(1, Ordering::SeqCst);
            ConsumeConcurrentlyStatus::ConsumeSuccess
        }
    }

    #[test]
    fn retire_drops_every_local_trace_and_hands_the_offset_to_persist() {
        let h = IllegalHarness::new(true, 0);
        lock(&h.c.inner.state)
            .pending
            .insert(h.key.clone(), VecDeque::from(offset_batch(2)));
        h.freeze(0);
        let revoked = h.retire();

        // the consumed offset is handed to the caller to persist (in `handle_offset_illegal` this is `revoke_queues`)
        assert_eq!(revoked.len(), 1);
        assert_eq!(revoked[0].0, h.mq);
        assert_eq!(revoked[0].1, Some(0), "what gets persisted is the corrected frozen offset");

        let state = lock(&h.c.inner.state);
        assert!(!state.pending.contains_key(&h.key), "the buffered batch is invalidated wholesale");
        assert!(!state.offset_table.contains_key(&h.key));
        assert!(!state.consume_offsets.contains_key(&h.key));
        assert!(!state.mq_map.contains_key(&h.key));
        assert!(!state.queue_owners.contains_key(&h.key));
        assert!(
            !state.last_pull_at.contains_key(&h.key),
            "a queue reused under the same name must not inherit the old timestamp"
        );
        assert_eq!(
            state.queue_epoch.get(&h.key).copied(),
            Some(1),
            "epoch +1 (setDropped)"
        );
        assert!(state.frozen_offsets.contains(&h.key), "the freeze must survive until the queue is rebuilt");
    }

    #[test]
    fn retire_without_a_queue_is_a_noop() {
        // the queue is no longer owned by this instance (concurrent revoke): no mq means nothing to persist, and none may be fabricated
        let h = IllegalHarness::new(false, 0);
        assert!(h.retire().is_empty());
        assert_eq!(
            h.epoch(),
            1,
            "retiring still clears state and advances the epoch (rebuilding is the caller's next step)"
        );
    }

    #[test]
    fn retire_twice_keeps_the_epoch_monotonic() {
        let h = IllegalHarness::new(true, 0);
        h.retire();
        assert_eq!(h.epoch(), 1);
        h.retire();
        assert_eq!(
            h.epoch(),
            2,
            "another illegal event before the rebuild: the epoch keeps advancing and old batches stay invalidated"
        );
    }

    #[tokio::test]
    async fn handle_offset_illegal_retires_and_wakes_the_rebalance_loop() {
        let h = IllegalHarness::new(true, 0);
        h.freeze(0);
        assert!(!h.c.inner.rebalance_now.load(Ordering::SeqCst));
        handle_offset_illegal(&h.c.inner, &h.key).await;
        assert_eq!(h.epoch(), 1);
        assert!(!lock(&h.c.inner.state).mq_map.contains_key(&h.key));
        assert!(
            h.c.inner.rebalance_now.load(Ordering::SeqCst),
            "after removeProcessQueue the queue must be rebuilt via rebalanceImmediately"
        );
    }

    #[test]
    fn frozen_offset_ignores_ack() {
        let h = IllegalHarness::new(true, 0);
        h.freeze(0);
        h.ack(&[0, 1, 2], 0);
        assert_eq!(h.offset(), Some(0));
    }

    #[test]
    fn frozen_offset_ignores_correct_tags_offset() {
        let h = IllegalHarness::new(true, 0);
        h.freeze(0);
        {
            let mut state = lock(&h.c.inner.state);
            correct_tags_offset_locked(&mut state, &h.key, PullStatus::NoNewMsg, 110);
        }
        assert_eq!(h.offset(), Some(0));
    }

    #[test]
    fn freeze_survives_the_retire_until_the_queue_is_rebuilt() {
        let h = IllegalHarness::new(true, 0);
        h.freeze(0);
        h.retire();
        assert!(h.frozen());
    }

    #[tokio::test]
    async fn rebuild_clears_the_freeze() {
        let h = IllegalHarness::new(true, 0);
        h.freeze(0);
        h.retire();
        // rebuild: assign the queue to self again (the build branch of `rebalance_pull_threads`)
        lock(&h.c.inner.state).assigned = vec![h.mq.clone()];
        h.c.rebalance_pull_threads().await;
        assert!(
            !h.frozen(),
            "the new ProcessQueue must unfreeze as soon as it is in place, or the queue would from then on only pull and never ack"
        );
        // after the rebuild acks work again: only batches with the **new epoch** count; old-epoch ones stay invalidated
        h.ack(&[0, 1], 0);
        assert_eq!(h.offset(), None, "an old-epoch ack still must not move the offset");
        h.ack(&[0, 1], h.epoch());
        assert_eq!(h.offset(), Some(2));
    }

    #[tokio::test]
    async fn stale_epoch_batch_is_not_consumed() {
        // after the queue is revoked/rebuilt (epoch 0 -> 1), batches dropped while queued up must not be consumed
        let h = IllegalHarness::new(true, 1);
        let listener = Arc::new(CountingListener {
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        });
        h.c.set_message_listener_concurrently(listener.clone());
        let done = consume_batch(&h.c.inner, &h.key, &h.mq, offset_batch(2), Some(0))
            .await
            .expect("consume_batch must not error");
        assert!(!done);
        assert_eq!(
            listener.calls.load(Ordering::SeqCst),
            0,
            "the listener must not even be entered"
        );
        assert_eq!(h.offset(), Some(3), "the old batch leaves the offset alone");
    }

    #[tokio::test]
    async fn current_epoch_batch_is_consumed() {
        let h = IllegalHarness::new(true, 1);
        let listener = Arc::new(CountingListener {
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        });
        h.c.set_message_listener_concurrently(listener.clone());
        lock(&h.c.inner.state)
            .consume_offsets
            .insert(h.key.clone(), 0);
        let done = consume_batch(&h.c.inner, &h.key, &h.mq, offset_batch(2), Some(1))
            .await
            .expect("consume_batch must not error");
        assert!(done);
        assert_eq!(listener.calls.load(Ordering::SeqCst), 1);
        assert_eq!(h.offset(), Some(2));
    }

    #[test]
    fn stale_epoch_ack_is_dropped_and_current_epoch_advances() {
        let h = IllegalHarness::new(true, 1);
        lock(&h.c.inner.state)
            .consume_offsets
            .insert(h.key.clone(), 0);
        h.ack(&[0, 1, 2], 0);
        assert_eq!(h.offset(), Some(0), "an old-epoch ack must not move the offset");
        h.ack(&[0, 1, 2], 1);
        assert_eq!(h.offset(), Some(3));
    }

    #[test]
    fn illegal_recovery_does_not_touch_sibling_queues() {
        // another queue of the same instance stays unaffected: retiring/freezing must be isolated by key
        let h = IllegalHarness::new(true, 0);
        let other = queue("T", "broker-a", 1);
        let other_key = mq_key(&other);
        {
            let mut state = lock(&h.c.inner.state);
            state.mq_map.insert(other_key.clone(), other.clone());
            state.queue_owners.insert(other_key.clone(), 2);
            state.consume_offsets.insert(other_key.clone(), 7);
            state
                .last_pull_at
                .insert(other_key.clone(), 1_700_000_000_000);
        }
        h.freeze(0);
        h.retire();
        let state = lock(&h.c.inner.state);
        assert!(state.mq_map.contains_key(&other_key));
        assert!(state.consume_offsets.contains_key(&other_key));
        assert!(state.queue_owners.contains_key(&other_key));
        assert_eq!(
            state.last_pull_at.get(&other_key).copied(),
            Some(1_700_000_000_000)
        );
        assert_eq!(state.queue_epoch.get(&other_key).copied().unwrap_or(0), 0);
        assert!(!state.frozen_offsets.contains(&other_key));
    }

    // ---------------- handling of 220 RESET_CONSUMER_CLIENT_OFFSET ----------------
    //
    //
    // Writing the new offset only into the in-memory table fails **silently**: a batch fetched before the reset but returned after it pushes
    // the offset back to its old place the moment it acks, the freshly written broker-side offset gets overwritten, and consumption
    // continues from the pre-reset position -- while the offsetTable group in the 222 response looks perfectly normal. Revocation + epoch (setDropped) is the real criterion.
    // For the live path see `examples/live_reset_offset.rs` (including reading the broker-side offset back).

    struct ResetHarness {
        c: Arc<DefaultMQPushConsumer>,
        key: String,
        mq: MessageQueue,
    }

    impl ResetHarness {
        fn new(model: &str) -> ResetHarness {
            let cfg = ConsumerConfig {
                consumer_group: "G".to_string(),
                message_model: model.to_string(),
                ..Default::default()
            };
            let c = Arc::new(DefaultMQPushConsumer::with_config(cfg).unwrap());
            let mq = queue("T", "broker-a", 0);
            let key = mq_key(&mq);
            {
                let mut state = lock(&c.inner.state);
                state.mq_map.insert(key.clone(), mq.clone());
                state.queue_owners.insert(key.clone(), 1);
                state.offset_table.insert(key.clone(), 7);
                state.consume_offsets.insert(key.clone(), 7);
                state
                    .pending
                    .insert(key.clone(), VecDeque::from(offset_batch(2)));
                state.last_pull_at.insert(key.clone(), 1_700_000_000_000);
            }
            ResetHarness { c, key, mq }
        }

        async fn reset(&self, off: i64) {
            self.c
                .clone()
                .reset_offset("T".to_string(), vec![(self.mq.clone(), off)])
                .await
                .expect("reset_offset must not error");
        }

        fn offset(&self) -> Option<i64> {
            lock(&self.c.inner.state)
                .consume_offsets
                .get(&self.key)
                .copied()
        }

        fn epoch(&self) -> u64 {
            lock(&self.c.inner.state)
                .queue_epoch
                .get(&self.key)
                .copied()
                .unwrap_or(0)
        }
    }

    /// On an instance that has not start()ed, neither persist nor rebalance has a client to send with (the same
    /// constraint as the offline `handle_offset_illegal` tests), so what is pinned here is the **state transition**:
    /// the queue is retired (epoch +1), table entries cleared, and old-epoch acks no longer take effect.
    #[tokio::test]
    async fn reset_offset_retires_the_queue_and_drops_the_pre_reset_batch() {
        let h = ResetHarness::new(MessageModel::CLUSTERING);
        h.reset(0).await;
        assert_eq!(h.epoch(), 1, "the queue was retired, epoch +1");
        assert_eq!(h.offset(), None, "the offset is handed over for persistence");
        {
            let state = lock(&h.c.inner.state);
            assert!(!state.mq_map.contains_key(&h.key));
            assert!(!state.pending.contains_key(&h.key), "the buffered batch is invalidated wholesale");
            assert!(!state.offset_table.contains_key(&h.key));
            assert!(!state.queue_owners.contains_key(&h.key));
        }
        // a batch fetched before the reset but returned after it (epoch 0): its ack must be dropped; the offset must not come back to life
        ack_batch(&h.c, &h.key, &[0, 1], 0);
        assert_eq!(h.offset(), None, "the old batch's ack is blocked by the epoch");
        // the rebuilt queue (epoch 1) acks as usual
        ack_batch(&h.c, &h.key, &[4, 5], 1);
        assert_eq!(h.offset(), Some(6));
    }

    #[tokio::test]
    async fn reset_offset_only_touches_the_matching_topic_and_table_entries() {
        let h = ResetHarness::new(MessageModel::CLUSTERING);
        let other = queue("T", "broker-a", 1);
        let other_key = mq_key(&other);
        {
            let mut state = lock(&h.c.inner.state);
            state.mq_map.insert(other_key.clone(), other.clone());
            state.queue_owners.insert(other_key.clone(), 2);
            state.offset_table.insert(other_key.clone(), 9);
            state.consume_offsets.insert(other_key.clone(), 9);
            state
                .last_pull_at
                .insert(other_key.clone(), 1_700_000_000_000);
        }
        // the offsetTable contains only queue 0: queue 1 of the same topic stays unaffected
        h.reset(0).await;
        {
            let state = lock(&h.c.inner.state);
            assert!(state.mq_map.contains_key(&other_key));
            assert!(
                !state.pending.contains_key(&other_key),
                "queue 1 has no buffer to begin with"
            );
            assert_eq!(state.consume_offsets.get(&other_key).copied(), Some(9));
            assert_eq!(state.queue_epoch.get(&other_key).copied().unwrap_or(0), 0);
            assert_eq!(state.queue_owners.get(&other_key).copied(), Some(2));
        }
        // the table holds no queue owned by this instance (another instance/another broker): no state is touched
        h.c.clone()
            .reset_offset("T".to_string(), vec![(queue("T", "broker-b", 0), 0)])
            .await
            .expect("reset_offset must not error");
        assert_eq!(h.epoch(), 1, "the previous entry's epoch does not regress");
        assert_eq!(h.offset(), None);
        assert_eq!(
            lock(&h.c.inner.state)
                .consume_offsets
                .get(&other_key)
                .copied(),
            Some(9)
        );
    }

    /// Broadcast revocation must merge the revoked offsets into the local offset file together with their queues: merging offsets alone is not enough,
    /// `build_local_offsets_json` **silently skips** any entry whose queue it cannot find in `mq_map`.
    #[test]
    fn retired_offsets_are_merged_into_the_local_snapshot() {
        let mq = queue("T", "broker-a", 0);
        let key = mq_key(&mq);

        let mut items = BTreeMap::new();
        let mut mq_map = BTreeMap::new();
        merge_retired_offsets(&mut items, &mut mq_map, &[(mq.clone(), Some(3))]);
        assert_eq!(items.get(&key), Some(&3));
        assert_eq!(mq_map.get(&key), Some(&mq));
        // it must really read back (contrast: merging only items without mq_map would make the builder skip this entry)
        let text = build_local_offsets_json(&items, &mq_map);
        assert_eq!(parse_local_offsets_text(&text).unwrap().get(&key), Some(&3));

        // a queue never consumed (offset None at revoke time) must not get a fabricated entry
        let mut items = BTreeMap::new();
        let mut mq_map = BTreeMap::new();
        merge_retired_offsets(&mut items, &mut mq_map, &[(mq.clone(), None)]);
        assert!(items.is_empty() && mq_map.is_empty());

        // when the snapshot already holds this queue (revoked several times in one batch / leftovers), the revoked value overwrites
        let mut items = BTreeMap::from([(key.clone(), 1)]);
        let mut mq_map = BTreeMap::from([(key.clone(), mq.clone())]);
        merge_retired_offsets(&mut items, &mut mq_map, &[(mq.clone(), Some(9))]);
        assert_eq!(items.get(&key), Some(&9));
    }

    // ---------------- trace auto-bridging ----------------

    #[test]
    fn trace_host_reports_empty_client_id_before_start() {
        let c = DefaultMQPushConsumer::new("GID_trace_host").expect("a legal group name must not fail to construct");
        // returns an empty string when the host has no _mq_client
        assert_eq!(TraceHost::client_id(&c), "");
    }

    // ================================================================
    // POP-mode broker-side assignment (QUERY_ASSIGNMENT(400)) -- in-process fake cluster
    // ================================================================

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{TcpListener, TcpStream};

    use crate::common::sysflag::PermName;
    use crate::remoting::protocol::codes::request_code;
    use crate::remoting::protocol::remoting_command::RemotingCommand;
    use crate::remoting::protocol::route::{BrokerData, QueueData, TopicRouteData};

    /// Script for the 400 response: the `(code, body)` pair combinations plus a "no reply, close the connection" mode.
    #[derive(Clone)]
    struct AssignFixture {
        /// The response code (`response_code::SUCCESS` / `SYSTEM_ERROR` / `TOPIC_NOT_EXIST`...).
        code: i32,
        body: Option<Vec<u8>>,
        /// Some = answer **per requested topic** from this table (a topic missing from it gets an
        /// empty set). This is what lets one round move exactly one topic's queue set, which the
        /// whole-assignment scripts cannot express.
        by_topic: Option<BTreeMap<String, Vec<(String, i32)>>>,
        /// true = after receiving the request, **close the connection without replying**: the client gets a transport-layer
        /// error (an Err with response_code None) and takes the "keep the previous assignment" branch.
        silent_close: bool,
    }

    impl AssignFixture {
        /// SUCCESS plus the given assignment array (an empty Vec encodes an explicit `[]` = "clear all assignments").
        fn with_assignments(items: &[(&str, i32, &str)]) -> AssignFixture {
            AssignFixture {
                code: response_code::SUCCESS,
                body: Some(assignment_body(items)),
                by_topic: None,
                silent_close: false,
            }
        }
        /// SUCCESS plus a per-topic queue table: `(topic, broker, queueId)`. A topic the request
        /// asks about that is absent here is answered with an empty set, so a round can shrink one
        /// topic's share while leaving its sibling's share byte-identical.
        fn with_topic_assignments(items: &[(&str, &str, i32)]) -> AssignFixture {
            let mut map: BTreeMap<String, Vec<(String, i32)>> = BTreeMap::new();
            for (topic, broker, id) in items {
                map.entry((*topic).to_string())
                    .or_default()
                    .push(((*broker).to_string(), *id));
            }
            AssignFixture {
                code: response_code::SUCCESS,
                body: None,
                by_topic: Some(map),
                silent_close: false,
            }
        }
        fn null_set() -> AssignFixture {
            AssignFixture {
                code: response_code::SUCCESS,
                body: None,
                by_topic: None,
                silent_close: false,
            }
        }
        fn with_code(code: i32) -> AssignFixture {
            AssignFixture {
                code,
                body: None,
                by_topic: None,
                silent_close: false,
            }
        }
        fn drop_connection() -> AssignFixture {
            AssignFixture {
                code: response_code::SUCCESS,
                body: None,
                by_topic: None,
                silent_close: true,
            }
        }
    }

    /// Encodes the 400 response body (through the real encoder, so the test frames have the same shape as production ones).
    fn assignment_body(items: &[(&str, i32, &str)]) -> Vec<u8> {
        use crate::remoting::protocol::query_assignment_body::MessageQueueAssignment;
        let list: Vec<MessageQueueAssignment> = items
            .iter()
            .map(|(broker, id, mode)| {
                MessageQueueAssignment::new(MessageQueueKey::new("T", *broker, *id), mode)
            })
            .collect();
        crate::remoting::protocol::query_assignment_body::QueryAssignmentResponseBody::encode(
            Some(&list),
        )
    }

    /// The same body for one named topic (the per-topic script answers each 400 with this).
    fn topic_assignment_body(topic: &str, items: &[(String, i32)]) -> Vec<u8> {
        use crate::remoting::protocol::query_assignment_body::MessageQueueAssignment;
        let list: Vec<MessageQueueAssignment> = items
            .iter()
            .map(|(broker, id)| {
                MessageQueueAssignment::new(MessageQueueKey::new(topic, broker, *id), "POP")
            })
            .collect();
        crate::remoting::protocol::query_assignment_body::QueryAssignmentResponseBody::encode(
            Some(&list),
        )
    }

    struct AssignState {
        /// None = the default script: SUCCESS with the two POP queues 0/1 on broker-a.
        fixture: Mutex<Option<AssignFixture>>,
        /// The captured QUERY_ASSIGNMENT request bodies (the place to assert on the 400 body fields).
        bodies: Mutex<Vec<Vec<u8>>>,
        /// The captured HEARTBEAT(34) request bodies, oldest first -- the only place a test can read
        /// the subscription versions exactly as they went on the wire.
        heartbeats: Mutex<Vec<Vec<u8>>>,
    }

    /// 1 name server (any topic gets the same route back: broker-a with 4 read queues)
    /// + 1 broker (QUERY_ASSIGNMENT answered per the script, POP gets POLLING no-ops, everything else SUCCESS).
    struct AssignCluster {
        namesrv_addr: String,
        state: Arc<AssignState>,
    }

    const ASSIGN_MAX_FRAME: i32 = 20 * 1024 * 1024;

    impl AssignCluster {
        async fn start() -> AssignCluster {
            let broker = TcpListener::bind("127.0.0.1:0").await.expect("bind mock broker");
            let broker_addr = broker.local_addr().expect("mock broker addr").to_string();
            let namesrv = TcpListener::bind("127.0.0.1:0").await.expect("bind mock namesrv");
            let namesrv_addr = namesrv.local_addr().expect("mock namesrv addr").to_string();
            let state = Arc::new(AssignState {
                fixture: Mutex::new(None),
                bodies: Mutex::new(Vec::new()),
                heartbeats: Mutex::new(Vec::new()),
            });
            let _namesrv_task = spawn_assign_namesrv(namesrv, broker_addr);
            let _broker_task = spawn_assign_broker(broker, state.clone());
            AssignCluster {
                namesrv_addr,
                state,
            }
        }

        fn set(&self, fixture: Option<AssignFixture>) {
            *lock(&self.state.fixture) = fixture;
        }

        /// All 400 request bodies the fake broker has received (decoded).
        fn requests(&self) -> Vec<crate::remoting::protocol::query_assignment_body::QueryAssignmentRequestBody> {
            let raw = lock(&self.state.bodies).clone();
            raw.iter()
                .map(|b| {
                    crate::remoting::protocol::query_assignment_body::QueryAssignmentRequestBody::decode(b)
                        .expect("request body must be valid JSON")
                })
                .collect()
        }

        /// How many HEARTBEAT(34) frames reached the fake broker so far -- the observation point for
        /// "this round did not beat" versus "this round beat once".
        fn heartbeat_count(&self) -> usize {
            lock(&self.state.heartbeats).len()
        }

        /// The `topic -> subVersion` map of one recorded heartbeat, in the order the beats arrived.
        ///
        /// Read off the wire JSON rather than the client's own table: the point of the assertion is
        /// what the broker was *handed*, which a getter on the client cannot show.
        fn heartbeat_versions(&self) -> Vec<BTreeMap<String, i64>> {
            lock(&self.state.heartbeats)
                .iter()
                .map(|body| {
                    let json: serde_json::Value =
                        serde_json::from_slice(body).expect("heartbeat body must be JSON");
                    let mut out = BTreeMap::new();
                    for cd in json["consumerDataSet"].as_array().into_iter().flatten() {
                        for sd in cd["subscriptionDataSet"].as_array().into_iter().flatten() {
                            let topic = sd["topic"].as_str().unwrap_or_default().to_string();
                            let version = match &sd["subVersion"] {
                                serde_json::Value::String(s) => {
                                    s.parse::<i64>().unwrap_or_default()
                                }
                                other => other.as_i64().unwrap_or_default(),
                            };
                            out.insert(topic, version);
                        }
                    }
                    out
                })
                .collect()
        }
    }

    async fn read_assign_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
        let mut len_buf = [0_u8; 4];
        stream.read_exact(&mut len_buf).await.ok()?;
        let total = i32::from_be_bytes(len_buf);
        if total <= 0 || total > ASSIGN_MAX_FRAME {
            return None;
        }
        let mut frame = vec![0_u8; 4 + total as usize];
        frame[..4].copy_from_slice(&len_buf);
        stream.read_exact(&mut frame[4..]).await.ok()?;
        Some(frame)
    }

    async fn write_assign_frame(stream: &mut TcpStream, response: &mut RemotingCommand) {
        let bytes = response.encode();
        let _ = stream.write_all(&bytes).await;
        let _ = stream.flush().await;
    }

    fn assign_response_for(request: &RemotingCommand, code: i32) -> RemotingCommand {
        let mut response = RemotingCommand::create_response(code, Some("mock".to_string()));
        response.opaque = request.opaque;
        response.serialize_type_current_rpc = request.serialize_type_current_rpc;
        response
    }

    /// Route: 4 read queues on broker-a (the fake broker address) -- when the assignment grants only 2 of them,
    /// a "degenerate into wanting everything" regression shows up in the assertions immediately.
    fn assign_route_json(broker_addr: &str) -> Vec<u8> {
        let route = TopicRouteData {
            queue_datas: vec![QueueData::new(
                "broker-a".to_string(),
                4,
                4,
                PermName::PERM_READ | PermName::PERM_WRITE,
                0,
            )],
            broker_datas: vec![BrokerData::new(
                "MockCluster",
                "broker-a".to_string(),
                vec![(i64::from(MixAll::MASTER_ID), broker_addr.to_string())],
                "",
            )],
            ..Default::default()
        };
        serde_json::to_vec(&route.to_json_value()).expect("route must be serializable")
    }

    fn spawn_assign_namesrv(listener: TcpListener, broker_addr: String) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { return };
                let addr = broker_addr.clone();
                tokio::spawn(async move {
                    while let Some(frame) = read_assign_frame(&mut stream).await {
                        let Ok(request) = RemotingCommand::decode(&frame) else { return };
                        let mut response =
                            if request.code == request_code::GET_ROUTEINFO_BY_TOPIC {
                                let mut r = assign_response_for(&request, response_code::SUCCESS);
                                r.set_body(Some(assign_route_json(&addr)));
                                r
                            } else {
                                assign_response_for(&request, response_code::SUCCESS)
                            };
                        write_assign_frame(&mut stream, &mut response).await;
                    }
                });
            }
        })
    }

    fn spawn_assign_broker(listener: TcpListener, state: Arc<AssignState>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { return };
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    while let Some(frame) = read_assign_frame(&mut stream).await {
                        let Ok(request) = RemotingCommand::decode(&frame) else { return };
                        if request.code == request_code::HEART_BEAT {
                            state.heartbeats.lock().unwrap_or_else(|e| e.into_inner()).push(
                                request.body().unwrap_or(&[]).to_vec(),
                            );
                            let mut response = assign_response_for(&request, response_code::SUCCESS);
                            write_assign_frame(&mut stream, &mut response).await;
                            continue;
                        }
                        if request.code == request_code::QUERY_ASSIGNMENT {
                            state.bodies.lock().unwrap_or_else(|e| e.into_inner()).push(
                                request.body().unwrap_or(&[]).to_vec(),
                            );
                            let fixture = lock(&state.fixture).clone();
                            match fixture {
                                Some(f) if f.silent_close => return, // no reply; tear down the connection directly
                                Some(f) => {
                                    let mut response = assign_response_for(&request, f.code);
                                    let body = match &f.by_topic {
                                        Some(map) if f.code == response_code::SUCCESS => {
                                            let topic = crate::remoting::protocol::query_assignment_body::QueryAssignmentRequestBody::decode(
                                                request.body().unwrap_or(&[]),
                                            )
                                            .map(|r| r.topic)
                                            .unwrap_or_default();
                                            let queues = map.get(&topic).cloned().unwrap_or_default();
                                            Some(topic_assignment_body(&topic, &queues))
                                        }
                                        _ => f.body.clone(),
                                    };
                                    if let Some(body) = body {
                                        response.set_body(Some(body));
                                    }
                                    write_assign_frame(&mut stream, &mut response).await;
                                }
                                None => {
                                    // default script: the two POP queues 0/1 on broker-a
                                    let mut response = assign_response_for(
                                        &request,
                                        response_code::SUCCESS,
                                    );
                                    response.set_body(Some(assignment_body(&[
                                        ("broker-a", 0, "POP"),
                                        ("broker-a", 1, "POP"),
                                    ])));
                                    write_assign_frame(&mut stream, &mut response).await;
                                }
                            }
                            continue;
                        }
                        if request.code == request_code::POP_MESSAGE {
                            // a started POP loop would spin here (with started=false the loop would in fact exit
                            // immediately; this delay is only a fallback for the "loop started by mistake" shape, keeping it from pinning the CPU).
                            tokio::time::sleep(Duration::from_millis(150)).await;
                            let mut response =
                                assign_response_for(&request, response_code::PULL_NOT_FOUND);
                            write_assign_frame(&mut stream, &mut response).await;
                            continue;
                        }
                        let mut response = assign_response_for(&request, response_code::SUCCESS);
                        if request.code == request_code::QUERY_CONSUMER_OFFSET
                            || request.code == request_code::GET_MAX_OFFSET
                        {
                            response.add_ext_field("offset", "0");
                        }
                        write_assign_frame(&mut stream, &mut response).await;
                    }
                });
            }
        })
    }

    /// Builds a consumer in the "POP mode + instance attached and pointing at the fake cluster" shape (no start call,
    /// no background loops spawned; the assignment is driven entirely by manual do_rebalance calls).
    fn pop_consumer_on_cluster(instance: &str, cluster: &AssignCluster) -> DefaultMQPushConsumer {
        pop_consumer_on_cluster_topics(instance, cluster, &["T"])
    }

    /// The same shape over an explicit topic list, with the consumer **registered in the instance**
    /// the way `start()` does it: the heartbeat body is built from the registry's consumer list, so
    /// an unregistered facade has nothing to send and a test could never tell a skipped beat from a
    /// beat that carried no subscriptions.
    fn pop_consumer_on_cluster_topics(
        instance: &str,
        cluster: &AssignCluster,
        topics: &[&str],
    ) -> DefaultMQPushConsumer {
        let c = DefaultMQPushConsumer::new("GID_pop_assign").expect("legal group name");
        for topic in topics {
            c.subscribe(topic, "*").expect("subscribe");
        }
        c.set_message_listener_concurrently(Arc::new(NoopListener));
        c.update_config(|cfg| {
            cfg.pop_mode = true;
            cfg.name_server_addrs = vec![cluster.namesrv_addr.clone()];
        });
        let client = MQClientInstance::new(
            &format!("cid-{instance}"),
            vec![cluster.namesrv_addr.clone()],
        );
        client.register_consumer("GID_pop_assign", Arc::new(c.clone()));
        *lock(&c.inner.client) = Some(client);
        // require_client honors this flag; the background loop tasks (rebalance/heartbeat/lock/persist)
        // are spawned only by start(), so manual pacing is not disturbed here.
        c.inner.started.store(true, Ordering::Release);
        c
    }

    /// The subscription versions the heartbeat builder reads, straight off the client's own table --
    /// the side of the pair the recorded beat is compared against.
    fn subscription_versions(consumer: &DefaultMQPushConsumer) -> BTreeMap<String, i64> {
        lock(&consumer.inner.state)
            .subscription_data
            .iter()
            .map(|(topic, sub)| (topic.clone(), sub.sub_version))
            .collect()
    }

    fn subscription_version(consumer: &DefaultMQPushConsumer, topic: &str) -> i64 {
        subscription_versions(consumer)
            .get(topic)
            .copied()
            .unwrap_or_else(|| panic!("{topic} has no subscription"))
    }

    /// Records every `(topic, mqAll, mqDivided)` callback the consumer fires, in order.
    #[derive(Default)]
    struct QueueChangeSpy {
        calls: Mutex<Vec<(String, usize, Vec<String>)>>,
    }

    impl MessageQueueListener for QueueChangeSpy {
        fn message_queue_changed(
            &self,
            topic: &str,
            mq_all: &[MessageQueue],
            mq_divided: &[MessageQueue],
        ) {
            lock(&self.calls).push((
                topic.to_string(),
                mq_all.len(),
                mq_divided.iter().map(mq_key).collect(),
            ));
        }
    }

    fn sorted_keys(c: &DefaultMQPushConsumer) -> Vec<String> {
        let mut keys = c.assigned_queue_keys();
        keys.sort();
        keys
    }

    #[tokio::test]
    async fn pop_assignment_comes_from_broker_query_assignment() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster("pop-qa-ok", &cluster);

        c.do_rebalance().await.expect("rebalance must not error");

        assert_eq!(
            sorted_keys(&c),
            vec!["Tbroker-a0".to_string(), "Tbroker-a1".to_string()],
            "assignment = the broker-granted 0/1, not every queue 0..3 from the route"
        );
        let reqs = cluster.requests();
        assert_eq!(reqs.len(), 1, "one 400 per topic");
        assert_eq!(reqs[0].topic, "T");
        assert_eq!(reqs[0].consumer_group, "GID_pop_assign");
        assert_eq!(reqs[0].client_id, "cid-pop-qa-ok", "clientId comes from the instance");
        assert_eq!(reqs[0].strategy_name, "AVG", "the strategy name rides in the body; the broker picks the strategy by it");
        assert_eq!(reqs[0].message_model, "CLUSTERING");
        // POP mode no longer sends GET_CONSUMER_LIST_BY_GROUP: the queue set is decided entirely by the 400
    }

    /// An explicit empty array = the broker says "hand back all your previous load": the old assignment must be cleared.
    #[tokio::test]
    async fn pop_empty_assignment_clears_previous_queues() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster("pop-qa-empty", &cluster);

        c.do_rebalance().await.unwrap();
        assert_eq!(sorted_keys(&c).len(), 2, "the default assignment arrives first");

        cluster.set(Some(AssignFixture::with_assignments(&[])));
        c.do_rebalance().await.unwrap();
        assert!(
            sorted_keys(&c).is_empty(),
            "an empty set is an explicit clear instruction, not a keep"
        );
    }

    /// TOPIC_NOT_EXIST(17): the topic is gone on the broker side -> converge on an empty set (a hard rule of this crate),
    /// never letting the POP loop cling to queues destined to hit a wall.
    #[tokio::test]
    async fn pop_topic_not_exist_clears_assignment() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster("pop-qa-gone", &cluster);

        c.do_rebalance().await.unwrap();
        assert_eq!(sorted_keys(&c).len(), 2);

        cluster.set(Some(AssignFixture::with_code(
            response_code::TOPIC_NOT_EXIST,
        )));
        c.do_rebalance().await.unwrap();
        assert!(
            sorted_keys(&c).is_empty(),
            "17 = the topic is gone; the assignment must be cleared, not left clinging to the old queues"
        );
    }

    #[tokio::test]
    async fn pop_assignment_failure_keeps_previous_never_all() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster("pop-qa-keep", &cluster);

        c.do_rebalance().await.unwrap();
        let before = sorted_keys(&c);
        assert_eq!(before, vec!["Tbroker-a0".to_string(), "Tbroker-a1".to_string()]);

        // (1) broker error code
        cluster.set(Some(AssignFixture::with_code(response_code::SYSTEM_ERROR)));
        c.do_rebalance().await.unwrap();
        assert_eq!(
            sorted_keys(&c),
            before,
            "non-SUCCESS (except 17) = keep the previous assignment; the route has 0..3, so degenerating into wanting everything is a regression"
        );

        // (2) SUCCESS + null set
        cluster.set(Some(AssignFixture::null_set()));
        c.do_rebalance().await.unwrap();
        assert_eq!(sorted_keys(&c), before, "null means invalid result, skip the update");

        // (3) transport-layer failure: the 400 closes the connection right after receiving the request (the error necessarily returns within the 3s RPC timeout budget)
        cluster.set(Some(AssignFixture::drop_connection()));
        c.do_rebalance().await.unwrap();
        assert_eq!(sorted_keys(&c), before, "when the RPC cannot get out, keeping the previous assignment is likewise the only option");
    }

    /// Assignment change detection: the next rebalance round (what the periodic loop calls each tick) switches immediately
    /// upon a different set -- the old ones retired, the new ones installed.
    #[tokio::test]
    async fn pop_assignment_change_is_applied_on_next_rebalance_round() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster("pop-qa-change", &cluster);

        c.do_rebalance().await.unwrap();
        assert_eq!(sorted_keys(&c), vec!["Tbroker-a0", "Tbroker-a1"]);

        // the broker side moved/replaced these queues (the real shape of POP rebalancing)
        cluster.set(Some(AssignFixture::with_assignments(&[
            ("broker-a", 2, "POP"),
            ("broker-a", 3, "POP"),
        ])));
        c.do_rebalance().await.unwrap();
        assert_eq!(
            sorted_keys(&c),
            vec!["Tbroker-a2", "Tbroker-a3"],
            "the second rebalance round must notice the new set and switch over"
        );
        assert_eq!(cluster.requests().len(), 2, "one 400 per topic per round");
    }

    /// Everything one moved topic owes, observed through the real `do_rebalance` against the fake
    /// cluster with **two** subscriptions, where only one topic's queue set moves: the restamp, the
    /// division, the immediate beat and the application callback all have to name the same set and no
    /// more, and an unchanged round has to pay none of them.
    #[tokio::test]
    async fn rebalance_change_pays_all_four_duties_for_the_moved_topic_only() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster_topics("rebalance-duties", &cluster, &["T1", "T2"]);
        let spy = Arc::new(QueueChangeSpy::default());
        c.set_message_queue_listener(spy.clone());
        c.update_config(|cfg| {
            cfg.pull_threshold_for_queue = 65_535;
            cfg.pull_threshold_for_topic = 1_000;
            cfg.pull_threshold_size_for_topic = -1;
        });
        cluster.set(Some(AssignFixture::with_topic_assignments(&[
            ("T1", "broker-a", 0),
            ("T1", "broker-a", 1),
            ("T2", "broker-a", 0),
        ])));
        // POP mode never reads the route to decide its share, so warm the route cache once: without a
        // broker address the beat has nowhere to go and the test would measure its own harness.
        {
            let client = lock(&c.inner.client).clone().expect("instance attached");
            for topic in ["T1", "T2"] {
                assert_eq!(client.get_topic_subscribe_info(topic).await.len(), 4);
            }
        }

        // Round 1: both topics appear, so both moved.
        let beats_before = cluster.heartbeat_count();
        c.do_rebalance().await.unwrap();
        assert_eq!(
            sorted_keys(&c),
            vec!["T1broker-a0", "T1broker-a1", "T2broker-a0"]
        );
        assert_eq!(
            cluster.heartbeat_count() - beats_before,
            1,
            "a changed round beats every broker once, without waiting for the periodic loop"
        );
        assert_eq!(
            c.config().pull_threshold_for_queue,
            333,
            "1000 divided over the 3 queues this round owns"
        );
        let calls = lock(&spy.calls).clone();
        assert_eq!(calls.len(), 2, "both topics moved, so both are reported");
        assert_eq!(calls[0].0, "T1");
        assert_eq!(calls[1].0, "T2");
        assert_eq!(
            (calls[0].1, calls[0].2.as_slice()),
            (4usize, &["T1broker-a0".to_string(), "T1broker-a1".to_string()][..]),
            "one call carries the FULL route set and this instance's share of it"
        );
        let beat = cluster.heartbeat_versions().pop().expect("the beat was recorded");
        let t1_v1 = beat["T1"];
        let t2_v1 = beat["T2"];
        assert!(t1_v1 > 0 && t2_v1 > 0, "a restamp is a wall-clock stamp: {beat:?}");
        assert_eq!(
            t1_v1, t2_v1,
            "one round shares a single stamp across the topics it moved"
        );

        // Round 2: the identical grant moves nothing, so the round is silent on all four duties --
        // the beat is not repeated, the callback does not re-report, and a hand-set per-queue knob
        // survives because the division is not re-run.
        c.update_config(|cfg| cfg.pull_threshold_for_queue = 12_345);
        let beats_before = cluster.heartbeat_count();
        c.do_rebalance().await.unwrap();
        assert_eq!(cluster.heartbeat_count(), beats_before, "an unchanged round sends no beat");
        assert_eq!(lock(&spy.calls).len(), 2, "an unchanged round reports nothing to the app");
        assert_eq!(c.config().pull_threshold_for_queue, 12_345, "not re-divided");
        assert_eq!(subscription_version(&c, "T1"), t1_v1, "an unmoved subscription keeps its version");
        assert_eq!(subscription_version(&c, "T2"), t2_v1);

        // Round 3: T2's share shrinks to nothing while T1 keeps the identical two queues. Only T2
        // moved -- and that is what each of the four duties must show.
        cluster.set(Some(AssignFixture::with_topic_assignments(&[
            ("T1", "broker-a", 0),
            ("T1", "broker-a", 1),
        ])));
        let beats_before = cluster.heartbeat_count();
        c.do_rebalance().await.unwrap();
        assert_eq!(sorted_keys(&c), vec!["T1broker-a0", "T1broker-a1"]);
        assert_eq!(
            cluster.heartbeat_count() - beats_before,
            1,
            "the beat goes out for the moved topic, immediately"
        );
        assert_eq!(
            c.config().pull_threshold_for_queue,
            500,
            "the shrunk round divides over the 2 queues it owns now, not the 3 it had"
        );
        let calls = lock(&spy.calls).clone();
        assert_eq!(calls.len(), 3, "exactly one more callback for one moved topic");
        assert_eq!(
            (calls[2].0.as_str(), calls[2].1, calls[2].2.as_slice()),
            ("T2", 4, &[] as &[String]),
            "T2 is told with its full route set and an empty share; T1 is never re-reported"
        );
        let versions = subscription_versions(&c);
        assert_eq!(versions["T1"], t1_v1, "the unmoved topic's version is left alone");
        assert!(
            versions["T2"] > t2_v1,
            "the moved topic carries a fresh stamp: {versions:?}"
        );
        let beat = cluster.heartbeat_versions().pop().expect("the second beat");
        assert_eq!(
            (beat["T1"], beat["T2"]),
            (t1_v1, versions["T2"]),
            "the beat the broker just received carries the fresh version for the moved topic and the \
             old one for the topic that never changed hands"
        );
    }

    #[tokio::test]
    async fn pop_assignment_dedups_and_survives_mixed_modes() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster("pop-qa-dedup", &cluster);
        let body = crate::remoting::protocol::query_assignment_body::QueryAssignmentResponseBody::encode(
            Some(&{
                let mut items = assignment_items(&[("broker-a", 0, "POP")]);
                items.extend(assignment_items(&[("broker-a", 0, "POP")])); // duplicate entry
                items.push(crate::remoting::protocol::query_assignment_body::MessageQueueAssignment {
                    message_queue: None,
                    mode: "POP".to_string(),
                });
                items.push(crate::remoting::protocol::query_assignment_body::MessageQueueAssignment {
                    message_queue: Some(MessageQueueKey::new("T", "broker-a", 1)),
                    mode: "PULL".to_string(), // mixed mode: observe only
                });
                items
            }),
        );
        cluster.set(Some(AssignFixture {
            code: response_code::SUCCESS,
            body: Some(body),
            by_topic: None,
            silent_close: false,
        }));

        c.do_rebalance().await.unwrap();
        assert_eq!(
            sorted_keys(&c),
            vec!["Tbroker-a0", "Tbroker-a1"],
            "duplicate (broker-a,0) entries deduplicate, null queues are skipped, and PULL entries join the set as usual"
        );
    }

    fn assignment_items(
        items: &[(&str, i32, &str)],
    ) -> Vec<crate::remoting::protocol::query_assignment_body::MessageQueueAssignment> {
        items
            .iter()
            .map(|(broker, id, mode)| {
                crate::remoting::protocol::query_assignment_body::MessageQueueAssignment::new(
                    MessageQueueKey::new("T", *broker, *id),
                    mode,
                )
            })
            .collect()
    }

    // ================================================================
    // Configurable ceiling on the consume-executor termination wait at shutdown
    // ================================================================

    #[test]
    fn await_termination_millis_when_shutdown_defaults_to_zero_and_round_trips() {
        let c = DefaultMQPushConsumer::new("GID_atms").expect("a legal group name must not fail");
        assert_eq!(
            c.config().await_termination_millis_when_shutdown,
            0,
            "default is 0 (= keep the existing bounded drain)"
        );
        assert_eq!(c.await_termination_millis_when_shutdown(), 0);
        c.set_await_termination_millis_when_shutdown(2_000);
        assert_eq!(c.await_termination_millis_when_shutdown(), 2_000);
        assert_eq!(c.config().await_termination_millis_when_shutdown, 2_000);
        c.set_await_termination_millis_when_shutdown(-1);
        assert_eq!(
            c.await_termination_millis_when_shutdown(),
            -1,
            "a negative value is stored as written; `<= 0` means wait as today"
        );
    }

    /// A positive ceiling bounds the executor drain: `shutdown()` returns while the consume
    /// worker is still busy. Reverting the bound to the fixed default makes both state gates
    /// red (the task ends at 4s, `shutdown` could only return after it).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_bounds_pop_executor_wait_by_configured_millis() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster("pop_drain_bound", &cluster);
        let executor = ConsumeExecutor::new(1, 1).with_handle(&tokio::runtime::Handle::current());
        *lock(&c.inner.pop_executor) = Some(executor.clone());
        let done = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&done);
        executor
            .submit(Box::pin(async move {
                // Holds the single worker longer than the whole shutdown window.
                tokio::time::sleep(Duration::from_millis(4_000)).await;
                flag.store(true, Ordering::Release);
            }))
            .expect("submit");

        c.set_await_termination_millis_when_shutdown(300);
        let began = std::time::Instant::now();
        c.shutdown();
        let elapsed = began.elapsed();

        // State gates: the executor had NOT terminated -- shutdown returned anyway because the
        // configured ceiling bounded its wait.
        assert!(
            !done.load(Ordering::Acquire),
            "shutdown must return while the consume task still runs (elapsed {elapsed:?})"
        );
        assert!(
            executor.worker_count() >= 1,
            "the worker was still occupied when shutdown returned"
        );
        assert!(
            elapsed < Duration::from_secs(4),
            "the executor drain was bounded by the configured 300ms: elapsed {elapsed:?}"
        );

        // Test hygiene: let the task and its worker finish before the runtime drops.
        tokio::time::timeout(Duration::from_secs(15), executor.await_termination())
            .await
            .expect("the worker retires on its own once the task ends");
        assert!(done.load(Ordering::Acquire));
    }

    /// Default 0: shutdown still runs the executor wait through to termination (the pre-existing
    /// bounded drain), so a short task is fully settled by the time `shutdown()` returns.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_waits_for_pop_executor_termination_when_ceiling_is_default() {
        let cluster = AssignCluster::start().await;
        let c = pop_consumer_on_cluster("pop_drain_default", &cluster);
        let executor = ConsumeExecutor::new(1, 1).with_handle(&tokio::runtime::Handle::current());
        *lock(&c.inner.pop_executor) = Some(executor.clone());
        let done = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&done);
        executor
            .submit(Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(400)).await;
                flag.store(true, Ordering::Release);
            }))
            .expect("submit");

        c.shutdown();
        assert!(
            done.load(Ordering::Acquire),
            "with the default ceiling shutdown waits for the executor to terminate"
        );
        assert_eq!(
            executor.worker_count(),
            0,
            "every worker has left by the time shutdown returns"
        );
    }
}
