//! Async trace dispatcher.
//!
//! Role: hooks drop [`TraceContext`] into an in-memory queue ([`append`]); a background
//! task flushes on two triggers -- "accumulated `batch_num` contexts" or "more than 5s since the last send" --
//! and sends the encoded text to the trace topic (default [`MixAll::TRACE_TOPIC`]) via a **separate internal producer**.
//!
//! [`append`]: AsyncTraceDispatcher::append
//!
//! ## Relationship with existing seams
//!
//! * [`crate::client::mq_client::TraceDispatcher`] -- lifecycle `start` / `shutdown`;
//! * [`crate::client::trace_hook::TraceReportSink`] -- the three things trace hooks need:
//!   `trace_topic_name` / `report`(=`append`) / `client_id`;
//! * [`TraceProducer`] -- the "internal trace producer" seam defined in this module: the
//!   set of methods the internal producer is called through during construction and startup (see below).
//!
//! ## Design points (each one is re-flagged on the doc of the corresponding item)
//!
//! 1. **Internal producer: injected, or built automatically via `auto_bridged_dispatcher`** ([`TraceDispatcherConfig::producer`]).
//!    The seam is [`TraceProducer`]; its method set = the set of methods the dispatcher actually calls
//!    on the internal producer (`set_send_msg_timeout` / `set_max_message_size` / `set_enable_trace` /
//!    `set_namesrv_addr` / `set_instance_name` / `set_namespace_v2` / `start` /
//!    `shutdown` / `send` / send-to-a-fixed-queue / fetch publish queues), implemented by
//!    `DefaultMQProducer` in `producer.rs`; when a host (producer/consumer) has `enable_trace=true`
//!    and no dispatcher is injected, `start_trace_dispatcher` builds a whole set (internal producer +
//!    dispatcher + host wiring) via `crate::client::producer::auto_bridged_dispatcher`.
//!    When `TraceDispatcherConfig { producer: None, .. }` is injected (bare `with_config`),
//!    [`DisabledTraceProducer`] is used (logs only, every send fails) and one WARN is logged at construction.
//!    The group-name literal was split into a **pure** function
//!    [`format_trace_producer_group`]; `next_trace_producer_group` is just "take a number + call it";
//!    the split exists so unit tests can pin the literal format deterministically (the process-wide counter interleaves numbers under parallel tests).
//! 2. **"Select a queue by broker set, then send" is split into two steps**: the queue-selection
//!    logic (round-robin within the broker set + route fetch) lives in the dispatcher
//!    ([`BrokerSetSelector::select`]); the send goes through [`TraceProducer::send_to_queue`] on the producer seam.
//!    Equivalence: send-to-a-fixed-queue and "the producer picks internally" are fully identical except for "who fetches the route / whether `arg` is forwarded to
//!    `CheckForbiddenContext`", and the internal trace producer **registers no hooks at all**
//!    (construction only sets group/timeout/maxSize/enableTrace=false) and **needs no** `arg`,
//!    so both paths produce the same on-wire bytes and pick the same queue.
//! 3. **Publish info exposes only `msg_queue_list`** ([`TraceProducer::publish_queues`]):
//!    the dispatcher only reads the queue list from publish info, and here
//!    [`TopicPublishInfo`](crate::client::mq_client::TopicPublishInfo) queues can only be written by instance route
//!    refresh and cannot be constructed in unit tests, so the seam hands out the queue list directly.
//! 4. **`start` has two versions**: the concrete type offers [`AsyncTraceDispatcher::start`] (`async fn`,
//!    the internal producer's start result can be awaited), while the object-safe
//!    [`TraceDispatcher::start`] throws the same logic into `tokio::spawn` (no await result,
//!    failures only log ERROR), and returns `Err` when **no tokio runtime** is present. The runtime handle
//!    used for dispatching is lazily bound via `OnceLock` (construction may happen outside a runtime).
//! 5. **Thread pool → `tokio::spawn` + an in-flight task table**. The pool's unbounded work queue is just
//!    "spawned futures" in Rust; the thread-name prefix `MQTraceSendThread_<id>_` and the worker thread name
//!    `MQ-AsyncArrayDispatcher-Thread<id>` have no counterpart in Rust and only affect logs.
//!    `WAIT_FOR_SHUTDOWN = 5000` is the wait bound for [`flush_and_wait`] /
//!    [`shutdown_gracefully`].
//!
//! [`flush_and_wait`]: AsyncTraceDispatcher::flush_and_wait
//! [`shutdown_gracefully`]: AsyncTraceDispatcher::shutdown_gracefully
//! 6. **No automatic flush before process exit** (no atexit / shutdown-hook equivalent); callers must
//!    call `shutdown()` explicitly. To avoid "forgetting shutdown leaks a background task forever", the
//!    worker task holds only a `Weak<Inner>` and exits by itself once all handles are dropped (equivalent to a daemon thread dying with the process).
//! 7. **The 5ms busy-wait guard**: `flush_trace_context` stays synchronous (the round only yields when
//!    it flushed no batch); `tokio::time::sleep` sits in the worker loop, with exactly the same trigger conditions.
//! 8. **Re-entry lock → `tokio::sync::Mutex`**: the critical section spans `await` (`producer.start()`),
//!    so an async lock is used; the started flag is set with CAS semantics to keep it idempotent.
//! 9. **`shutdown()` `take()`s the worker handle**, so `start()` can be called again afterwards and
//!    each `start()` creates a new worker. Rust additionally `abort()`s the task after setting `stopped`:
//!    nothing is lost -- the flush section is synchronous and is not interrupted before the next await point, and submitted
//!    send batches live in separate tasks tracked by the in-flight table (see point 5).
//! 10. **`flush()` does not spin forever when `batch_num == 0`**: when it finds
//!     "the queue is non-empty but this round flushed no batch" it logs one WARN and exits.
//! 11. **The chunking threshold compares code points** (`chars().count()`), against this dispatcher's own
//!     `max_msg_size` (128000, changeable via `set_max_msg_size`) rather than the producer's limit.
//! 12. **The grouping container keeps insertion order**: a "Vec + index HashMap" fixes the per-group send
//!     order (trace arrival order is never guaranteed under multithreading, but the order inside one sent batch is testable on-wire behavior).
//! 13. **Group key unpacking**: the group key is `topic \x01 trace_topic`, split on the **first** `\x01`
//!     only, once (`split_once`); if a topic really contains `\x01` and the split yields neither
//!     expected part, log at DEBUG and skip the group -- dirty keys must not break flushing.
//! 14. **`is_started` stays `true` after `shutdown()`** -- the state machine never resets it.
//!     But a second `shutdown()` does not close the internal producer again: finalization runs once after
//!     in-flight sends converge, and later calls return directly (re-closing would cut off the sends the first round did not finish waiting for).
//! 15. **`namespace_v2` is wired**: at `start()` the value configured via [`AsyncTraceDispatcher::set_namespace_v2`]
//!     is forwarded to the internal producer, whose instance then registers `NamespaceRpcHook`
//!     (the `nsd`/`ns` extension headers). When **unset** (empty string) this setter call is skipped --
//!     an empty namespaceV2 produces no on-wire effect anyway.
//! 16. **`set_host_producer` / `set_host_consumer` go through the [`TraceHost`] seam**:
//!     it only offers the tiny `client_id()` seam, already implemented by [`MQClientInstance`].
//! 17. Encoding (`TraceDataEncoder::encoder_from_context_bean`), the `TraceContext` fields and the
//!     `\x01`/`\x02` string building are all reused from [`crate::client::trace`]; this module **does not**
//!     reimplement them or duplicate their unit tests; it only covers "batching / grouping / chunking / KEYS building / queue selection / lifecycle".

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::client::mq_client::{MQClientInstance, TraceDispatcher};
use crate::client::producer::DefaultMQProducer;
use crate::client::shutdown::{run_finalize_blocking, SHUTDOWN_FINALIZE_BUDGET};
use crate::client::trace::{
    AccessChannel, TraceConstants, TraceContext, TraceDataEncoder, TraceTransferBean,
};
use crate::client::trace_hook::TraceReportSink;
use crate::common::message::{Message, MessageQueue};
use crate::common::message_const::KEY_SEPARATOR;
use crate::common::mix_all::MixAll;
use crate::common::util_all::current_time_millis;
use crate::error::{Error, Result};
use crate::{rmq_debug, rmq_error, rmq_info, rmq_warn};

// ================================================================ Enum / process-wide counter

/// Dispatcher type (producer side / consumer side).
///
/// Only affects the literal segment in the internal producer group name (`PRODUCE` / `CONSUME`);
/// no trace body is put on the wire for it, so there is no encoding risk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TraceDispatcherType {
    /// Producer side.
    Produce,
    /// Consumer side.
    Consume,
}

impl TraceDispatcherType {
    /// Type name: `PRODUCE` | `CONSUME` (the segment in the group name, the literals are fixed).
    pub fn name(self) -> &'static str {
        match self {
            TraceDispatcherType::Produce => "PRODUCE",
            TraceDispatcherType::Consume => "CONSUME",
        }
    }
}

/// Lets group-name building write the type directly into `format!` (equivalent to `self.name()`).
impl std::fmt::Display for TraceDispatcherType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Process-wide internal producer group-name sequence, **starting at 1**.
fn next_group_counter() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::AcqRel) + 1
}

/// The **pure** formatting part of [`AsyncTraceDispatcher::next_trace_producer_group`]
/// (the incremented number comes in as a parameter).
///
/// The group-name format bundles "format + increment"; splitting it here only so unit tests can
/// deterministically assert the literal format -- the process-wide counter interleaves numbers under `cargo test`'s parallel cases, making "exactly +1" assertions unreliable.
fn format_trace_producer_group(
    group: &str,
    dispatcher_type: TraceDispatcherType,
    seq: u64,
) -> String {
    format!(
        "{}-{group}-{dispatcher_type}-{seq}",
        TraceConstants::GROUP_NAME_PREFIX
    )
}

fn next_instance_id() -> u64 {
    static INSTANCE_NUM: AtomicU64 = AtomicU64::new(0);
    INSTANCE_NUM.fetch_add(1, Ordering::AcqRel)
}

// ================================================================ Internal producer seam

/// Async return type of the internal trace producer (a boxed future with no async-trait
/// dependency, in the same style as [`crate::client::mq_client::ConsumerFuture`]).
///
/// All calls hitting the internal producer are synchronous and blocking (`start` / `shutdown` /
/// `send` / send-to-a-fixed-queue / fetch publish queues); after moving to tokio async they are all boxed with this type.
pub type TraceProducerFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Internal trace producer seam: a `DefaultMQProducer` with `enable_trace = false`.
///
/// Object safety: methods needing `await` take an `self: Arc<Self>` receiver + a boxed future.
pub trait TraceProducer: Send + Sync {
    fn set_name_server_addr(&self, name_server_addr: &str);

    fn set_instance_name(&self, instance_name: &str);

    fn set_namespace_v2(&self, namespace_v2: &str);

    fn set_send_msg_timeout(&self, timeout_millis: i64);

    fn set_max_message_size(&self, max_msg_size: usize);

    /// The dispatcher only ever passes `false`; the signature keeps a bool so implementors can reuse their existing setter.
    fn set_enable_trace(&self, enable: bool);

    /// Start the internal producer. Failure => `Err`, propagated as-is by [`AsyncTraceDispatcher::start`]
    /// (host-side wiring logs only a warning and never turns a start failure into a crash).
    fn start(self: Arc<Self>) -> TraceProducerFuture<Result<()>>;

    /// Shut down the internal producer: the seam has no return value, and
    /// the implementor (`DefaultMQProducer::shutdown` and the like) guarantees not to panic on its own.
    fn shutdown(&self);

    /// The send result `SendResult` is useless here, so only `Result<()>` is returned.
    fn send(
        self: Arc<Self>,
        message: Message,
        timeout_millis: i64,
    ) -> TraceProducerFuture<Result<()>>;

    /// Send to a fixed queue (pins the message to the selected queue, see design point 2):
    /// `queue` is one picked from [`Self::publish_queues`].
    fn send_to_queue(
        self: Arc<Self>,
        message: Message,
        queue: MessageQueue,
        timeout_millis: i64,
    ) -> TraceProducerFuture<Result<()>>;

    fn publish_queues(
        self: Arc<Self>,
        topic: String,
    ) -> TraceProducerFuture<Result<Vec<MessageQueue>>>;
}

/// The "not wired" implementation of [`TraceProducer`] (design point 1): sends nothing, logs only.
/// The internal producer is now injected; when none is injected this type serves as the fallback.
#[derive(Debug, Default, Clone, Copy)]
pub struct DisabledTraceProducer;

impl TraceProducer for DisabledTraceProducer {
    fn set_name_server_addr(&self, name_server_addr: &str) {
        rmq_debug!("trace producer not wired: ignore namesrv_addr {name_server_addr}");
    }

    fn set_instance_name(&self, instance_name: &str) {
        rmq_debug!("trace producer not wired: ignore instanceName {instance_name}");
    }

    fn set_namespace_v2(&self, namespace_v2: &str) {
        rmq_debug!("trace producer not wired: ignore namespaceV2 {namespace_v2}");
    }

    fn set_send_msg_timeout(&self, timeout_millis: i64) {
        rmq_debug!("trace producer not wired: ignore send_msg_timeout {timeout_millis}");
    }

    fn set_max_message_size(&self, max_msg_size: usize) {
        rmq_debug!("trace producer not wired: ignore max_message_size {max_msg_size}");
    }

    fn set_enable_trace(&self, enable: bool) {
        rmq_debug!("trace producer not wired: ignore enable_trace {enable}");
    }

    fn start(self: Arc<Self>) -> TraceProducerFuture<Result<()>> {
        Box::pin(async {
            Err(Error::client(
                "trace producer is not wired (inject TraceDispatcherConfig::producer)",
            ))
        })
    }

    fn shutdown(&self) {}

    fn send(
        self: Arc<Self>,
        _message: Message,
        _timeout_millis: i64,
    ) -> TraceProducerFuture<Result<()>> {
        Box::pin(async { Err(Error::client("trace producer is not wired")) })
    }

    fn send_to_queue(
        self: Arc<Self>,
        _message: Message,
        _queue: MessageQueue,
        _timeout_millis: i64,
    ) -> TraceProducerFuture<Result<()>> {
        Box::pin(async { Err(Error::client("trace producer is not wired")) })
    }

    fn publish_queues(
        self: Arc<Self>,
        _topic: String,
    ) -> TraceProducerFuture<Result<Vec<MessageQueue>>> {
        Box::pin(async { Err(Error::client("trace producer is not wired")) })
    }
}

// ================================================================ Host seam

/// Dispatcher host: the producer or consumer it is mounted on, exposing only `client_id()`.
pub trait TraceHost: Send + Sync {
    fn client_id(&self) -> String;
}

/// Use [`MQClientInstance`] directly as a host.
impl TraceHost for MQClientInstance {
    fn client_id(&self) -> String {
        MQClientInstance::client_id(self).to_string()
    }
}

// ================================================================ Configuration

/// Construction parameters of [`AsyncTraceDispatcher`] (the defaults are the dispatcher's fixed constants).
#[derive(Clone)]
pub struct TraceDispatcherConfig {
    /// The internal trace producer (design point 1). `None` => [`DisabledTraceProducer`].
    pub producer: Option<Arc<dyn TraceProducer>>,
    /// Internal producer group name. `None` => generated on the spot by
    /// [`AsyncTraceDispatcher::next_trace_producer_group`] (fixed prefix + process-wide increment).
    /// When injecting a producer, pass in the same name originally used to build that producer, so the process-wide counter does not advance an extra step.
    pub trace_producer_group: Option<String>,
    pub batch_num: usize,
    pub trace_topic_name: Option<String>,
    pub max_msg_size: usize,
    /// The internal producer's send timeout and the `5000` passed at send time -- the same constant.
    pub send_msg_timeout_millis: i64,
    pub flush_trace_interval_millis: i64,
    pub wait_for_shutdown_millis: u64,
    pub queue_capacity: usize,
}

impl Default for TraceDispatcherConfig {
    fn default() -> Self {
        TraceDispatcherConfig {
            producer: None,
            trace_producer_group: None,
            batch_num: 10,
            trace_topic_name: None,
            max_msg_size: 128_000,
            send_msg_timeout_millis: 5_000,
            flush_trace_interval_millis: 5_000,
            wait_for_shutdown_millis: 5_000,
            queue_capacity: 2_048,
        }
    }
}

/// Hand-written `Debug`: the injected seam (`Arc<dyn TraceProducer>`) does not require `Debug`.
impl std::fmt::Debug for TraceDispatcherConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceDispatcherConfig")
            .field(
                "producer",
                &if self.producer.is_some() {
                    "injected"
                } else {
                    "none"
                },
            )
            .field("trace_producer_group", &self.trace_producer_group)
            .field("batch_num", &self.batch_num)
            .field("trace_topic_name", &self.trace_topic_name)
            .field("max_msg_size", &self.max_msg_size)
            .field("send_msg_timeout_millis", &self.send_msg_timeout_millis)
            .field(
                "flush_trace_interval_millis",
                &self.flush_trace_interval_millis,
            )
            .field("wait_for_shutdown_millis", &self.wait_for_shutdown_millis)
            .field("queue_capacity", &self.queue_capacity)
            .finish()
    }
}

// ================================================================ Selector

/// `counter` is the host dispatcher's round-robin counter (process-wide, starting at 0):
/// a new selector object is built for every send, but the **counter is shared**,
/// so the polling position stays continuous across batches.
struct BrokerSetSelector {
    counter: Arc<AtomicU64>,
}

impl BrokerSetSelector {
    fn select(&self, mqs: &[MessageQueue], broker_set: &HashSet<String>) -> Option<MessageQueue> {
        let mut filtered: Vec<&MessageQueue> = mqs
            .iter()
            .filter(|q| broker_set.contains(&q.broker_name))
            .collect();
        if filtered.is_empty() {
            filtered = mqs.iter().collect();
        }
        if filtered.is_empty() {
            return None;
        }
        let pos = self.counter.fetch_add(1, Ordering::AcqRel) % filtered.len() as u64;
        Some(filtered[pos as usize].clone())
    }
}

// ================================================================ AsyncTraceDispatcher

/// The dispatcher entity: internal state shared by the handle, the hooks and the background task.
///
/// It is split into `Arc<Inner>` + a handle because the dispatcher must be shared by the hooks, the host
/// producer and the background task at once; the [`TraceReportSink`] / [`TraceDispatcher`] seams both need
/// `Arc<dyn ...>`, while the background task needs a weak reference that "does not prevent reclamation" (design point 6).
struct Inner {
    group: String,
    dispatcher_type: TraceDispatcherType,
    trace_instance_id: u64,
    /// Internal producer group name (fixed prefix + process-wide increment).
    trace_producer_group: String,
    batch_num: usize,
    /// Chunking threshold: directly settable, see [`AsyncTraceDispatcher::set_max_msg_size`].
    max_msg_size: AtomicUsize,
    trace_topic_name: String,
    access_channel: AtomicU8,
    queue: Mutex<VecDeque<TraceContext>>,
    queue_capacity: usize,
    discard_count: AtomicU64,
    stopped: AtomicBool,
    /// Whether the synchronous [`Inner::shutdown`] has hung/executed finalization: prevents a second
    /// `shutdown()` from cutting off, mid-way, the round that is still waiting on in-flight sends.
    finalizing: AtomicBool,
    is_started: AtomicBool,
    last_flush_time: AtomicI64,
    flush_trace_interval_millis: i64,
    send_which_queue: Arc<AtomicU64>,
    /// Start re-entry lock: protects only the `is_started` section inside `start()`,
    /// differences noted in module-header point 8.
    start_lock: tokio::sync::Mutex<()>,
    worker: Mutex<Option<JoinHandle<()>>>,
    /// In-flight send task handles (for graceful waiting).
    pending: Mutex<Vec<JoinHandle<()>>>,
    trace_producer: Arc<dyn TraceProducer>,
    namespace_v2: Mutex<String>,
    /// Host (producer side / consumer side, one of the two).
    host_producer: Mutex<Option<Arc<dyn TraceHost>>>,
    host_consumer: Mutex<Option<Arc<dyn TraceHost>>>,
    /// The 5000 on the internal producer + the `5000` passed at send time.
    send_msg_timeout_millis: i64,
    wait_for_shutdown_millis: u64,
    /// Lazily bound tokio runtime handle (design point 4).
    handle: OnceLock<Handle>,
    /// Weak reference to itself, used by `submit` to build a `'static` future.
    this: Weak<Inner>,
}

/// Usage (wire up the host, then start the dispatcher):
/// ```text
/// let group = AsyncTraceDispatcher::next_trace_producer_group("GID_test", Produce);
/// let producer = /* build the internal producer with group + rpc_hook (enable_trace must be false) */;
/// let d = Arc::new(AsyncTraceDispatcher::with_config("GID_test", Produce,
///     TraceDispatcherConfig { producer: Some(producer), trace_producer_group: Some(group),
///                             ..Default::default() }));
/// d.start(namesrv, Some(AccessChannel::Local)).await?;   // or TraceDispatcher::start(&*d, ..)
/// SendMessageTraceHook::new(d.clone()) ...                // d is the TraceReportSink
/// d.shutdown_gracefully().await;                          // or TraceDispatcher::shutdown(&*d)
/// ```
#[derive(Clone)]
pub struct AsyncTraceDispatcher {
    inner: Arc<Inner>,
}

impl Inner {
    /// Busy-wait guard interval on top of the flush interval (5ms).
    const IDLE_SLEEP_MILLIS: u64 = 5;

    /// Read side of access_channel.
    fn access_channel(&self) -> AccessChannel {
        access_channel_from_u8(self.access_channel.load(Ordering::Acquire))
    }
}

impl AsyncTraceDispatcher {
    /// Convenient construction: `batch_num = 10`, `trace_topic_name = None`;
    /// `producer` is the internal producer (design point 1).
    pub fn new(
        group: &str,
        dispatcher_type: TraceDispatcherType,
        producer: Arc<dyn TraceProducer>,
    ) -> AsyncTraceDispatcher {
        AsyncTraceDispatcher::with_config(
            group,
            dispatcher_type,
            TraceDispatcherConfig {
                producer: Some(producer),
                ..Default::default()
            },
        )
    }

    /// Full-injection version: every constant is given by the caller.
    pub fn with_config(
        group: &str,
        dispatcher_type: TraceDispatcherType,
        config: TraceDispatcherConfig,
    ) -> AsyncTraceDispatcher {
        if config.producer.is_none() {
            rmq_info!(
                "trace dispatcher for group {group} builds its own internal trace producer \
                 (none was injected, so it is built on demand)"
            );
        }
        // The group name increments once (unless the caller already injected a pre-computed one),
        // and these three settings are applied directly to the internal producer.
        let trace_producer_group = config.trace_producer_group.clone().unwrap_or_else(|| {
            AsyncTraceDispatcher::next_trace_producer_group(group, dispatcher_type)
        });
        // When nothing is injected, build a real DefaultMQProducer in-house
        // (group name = _INNER_TRACE_PRODUCER-<group>-<type>-<seq>, send_msg_timeout=5000,
        // max_message_size=same as the dispatcher, enable_trace=false to prevent recursion).
        // Falling back to DisabledTraceProducer would silently break the trace path by default;
        // so when nothing is injected we build the real implementation, keeping the `TraceProducer`
        // seam for test stubs and explicit customization.
        let producer: Arc<dyn TraceProducer> = match config.producer.clone() {
            Some(injected) => injected,
            None => match DefaultMQProducer::new(&trace_producer_group) {
                Ok(inner) => {
                    inner.set_enable_trace(false);
                    Arc::new(inner)
                }
                Err(e) => {
                    rmq_warn!(
                        "trace dispatcher for group {group} failed to build its internal \
                             trace producer ({e}); trace data will be dropped"
                    );
                    Arc::new(DisabledTraceProducer)
                }
            },
        };
        producer.set_send_msg_timeout(config.send_msg_timeout_millis);
        producer.set_max_message_size(config.max_msg_size);
        producer.set_enable_trace(false);
        AsyncTraceDispatcher {
            inner: Arc::new_cyclic(|weak: &Weak<Inner>| {
                let max_msg_size = AtomicUsize::new(config.max_msg_size);
                Inner {
                    group: group.to_string(),
                    dispatcher_type,
                    trace_instance_id: next_instance_id(),
                    trace_producer_group,
                    batch_num: config.batch_num.min(20),
                    max_msg_size,
                    trace_topic_name: config
                        .trace_topic_name
                        .filter(|topic| !topic.is_empty())
                        .unwrap_or_else(|| MixAll::TRACE_TOPIC.to_string()),
                    access_channel: AtomicU8::new(0),
                    queue: Mutex::new(VecDeque::new()),
                    queue_capacity: config.queue_capacity,
                    discard_count: AtomicU64::new(0),
                    stopped: AtomicBool::new(false),
                    finalizing: AtomicBool::new(false),
                    is_started: AtomicBool::new(false),
                    last_flush_time: AtomicI64::new(current_time_millis()),
                    flush_trace_interval_millis: config.flush_trace_interval_millis,
                    send_which_queue: Arc::new(AtomicU64::new(0)),
                    start_lock: tokio::sync::Mutex::new(()),
                    worker: Mutex::new(None),
                    pending: Mutex::new(Vec::new()),
                    trace_producer: producer,
                    namespace_v2: Mutex::new(String::new()),
                    host_producer: Mutex::new(None),
                    host_consumer: Mutex::new(None),
                    send_msg_timeout_millis: config.send_msg_timeout_millis,
                    wait_for_shutdown_millis: config.wait_for_shutdown_millis,
                    handle: OnceLock::new(),
                    this: weak.clone(),
                }
            }),
        }
    }

    /// Difference from the instance-method version [`Self::trace_producer_group`]: this is a **module function**,
    /// letting callers compute the name before constructing the internal producer (when the internal producer
    /// builds itself the name is computed on the spot and the order cannot be externalized -- see design point 1).
    pub fn next_trace_producer_group(group: &str, dispatcher_type: TraceDispatcherType) -> String {
        format_trace_producer_group(group, dispatcher_type, next_group_counter())
    }

    // ---------------- Property reads ----------------

    pub fn group(&self) -> &str {
        &self.inner.group
    }

    pub fn dispatcher_type(&self) -> TraceDispatcherType {
        self.inner.dispatcher_type
    }

    pub fn trace_instance_id(&self) -> u64 {
        self.inner.trace_instance_id
    }

    /// The internal producer group name this dispatcher settled on (exists only inside the producer object).
    pub fn trace_producer_group(&self) -> &str {
        &self.inner.trace_producer_group
    }

    pub fn batch_num(&self) -> usize {
        self.inner.batch_num
    }

    /// Chunking threshold (code points, design point 11).
    pub fn max_msg_size(&self) -> usize {
        self.inner.max_msg_size.load(Ordering::Acquire)
    }

    pub fn set_max_msg_size(&self, max_msg_size: usize) {
        self.inner
            .max_msg_size
            .store(max_msg_size, Ordering::Release);
    }

    pub fn set_namespace_v2(&self, namespace_v2: &str) {
        *self
            .inner
            .namespace_v2
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = namespace_v2.to_string();
    }

    pub fn get_namespace_v2(&self) -> String {
        self.inner
            .namespace_v2
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Read side of the trace topic name.
    pub fn get_trace_topic_name(&self) -> String {
        self.inner.trace_topic_name.clone()
    }

    pub fn access_channel(&self) -> AccessChannel {
        access_channel_from_u8(self.inner.access_channel.load(Ordering::Acquire))
    }

    pub fn discard_count(&self) -> u64 {
        self.inner.discard_count.load(Ordering::Acquire)
    }

    pub fn queue_size(&self) -> usize {
        self.inner
            .queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    pub fn is_started(&self) -> bool {
        self.inner.is_started.load(Ordering::Acquire)
    }

    pub fn is_stopped(&self) -> bool {
        self.inner.stopped.load(Ordering::Acquire)
    }

    /// Read side of the last flush time.
    pub fn last_flush_time(&self) -> i64 {
        self.inner.last_flush_time.load(Ordering::Acquire)
    }

    /// Write side of the last flush time (on the wire it is only assigned inside trace sending, with no setter --
    /// exposed to unit tests here: verify the "over 5s triggers a flush" branch without a background task).
    pub fn set_last_flush_time(&self, last_flush_time: i64) {
        self.inner
            .last_flush_time
            .store(last_flush_time, Ordering::Release);
    }

    // ---------------- Host ----------------

    pub fn set_host_producer(&self, host: Arc<dyn TraceHost>) {
        *self
            .inner
            .host_producer
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(host);
    }

    pub fn set_host_consumer(&self, host: Arc<dyn TraceHost>) {
        *self
            .inner
            .host_consumer
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(host);
    }

    // ---------------- Lifecycle ----------------

    /// Fixed order: inside the lock (when `is_started` is false) set namesrv / instanceName /
    /// `enable_trace = false` and start the internal producer => overwrite `access_channel` =>
    /// spawn the background task if no worker exists. There is no process-exit hook (atexit)
    /// equivalent (design point 6).
    ///
    /// Failure: the internal producer fails to start => `Err` propagated as-is (host wiring logs only a
    /// warning), and `is_started` stays `false`. Must be called inside a tokio runtime context
    /// (design point 4).
    pub async fn start(
        &self,
        name_server_addr: &str,
        access_channel: Option<AccessChannel>,
    ) -> Result<()> {
        self.inner.bind_handle();
        {
            let _lock = self.inner.start_lock.lock().await;
            if !self.inner.is_started.load(Ordering::Acquire) {
                // instanceName = "PID_CLIENT_INNER_TRACE_PRODUCER_<namesrv>"
                self.inner
                    .trace_producer
                    .set_name_server_addr(name_server_addr);
                self.inner.trace_producer.set_instance_name(&format!(
                    "{}_{name_server_addr}",
                    TraceConstants::TRACE_INSTANCE_NAME
                ));
                let namespace_v2 = self.get_namespace_v2();
                if !namespace_v2.is_empty() {
                    self.inner.trace_producer.set_namespace_v2(&namespace_v2);
                }
                // ⚠ Must disable tracing on itself, otherwise trace messages would be traced again
                self.inner.trace_producer.set_enable_trace(false);
                Arc::clone(&self.inner.trace_producer).start().await?;
                self.inner.is_started.store(true, Ordering::Release);
            }
        }
        // `access_channel or AccessChannel.LOCAL` (None => LOCAL)
        self.inner
            .set_access_channel(access_channel.unwrap_or_default());
        self.inner.start_worker();
        Ok(())
    }

    /// The producer/consumer porting layer in production code should prefer this
    /// (closing the producer before the last batch is flushed lets the two race each other).
    pub async fn shutdown_gracefully(&self) {
        self.inner.flush_and_wait().await;
        if self.inner.finalizing.swap(true, Ordering::AcqRel) {
            // The synchronous shutdown() is already running finalization (on a multithreaded runtime it has usually finished by the time it returns):
            // it closes the producer after in-flight sends converge, so closing again here would only race with it.
            return;
        }
        self.inner.finalize_now();
    }

    // ---------------- Enqueue / flush ----------------

    pub fn append(&self, context: TraceContext) -> bool {
        match self.inner.push_context(context) {
            Some(rejected) => {
                let discard = self.inner.discard_count.fetch_add(1, Ordering::AcqRel) + 1;
                // Error text verified verbatim: the missing space and comma are on-wire literals
                rmq_info!("buffer full{discard} ,context is {rejected}");
                false
            }
            None => true,
        }
    }

    pub fn flush(&self) {
        self.inner.flush();
    }

    pub async fn flush_and_wait(&self) {
        self.inner.flush_and_wait().await;
    }
}

// ---------------------------------------------------------------- Private implementation
//
// Everything below are `Inner` methods, covering batching / grouping / chunking / queue selection / the send path;
// they live on `Inner` because the background task and the send path hold only `Weak<Inner>`/`Arc<Inner>`
// and must not depend on the public [`AsyncTraceDispatcher`] handle (design point 6).

impl Inner {
    /// Runs `flush_trace_context(false)` each round and sleeps 5ms when this round flushed no batch
    /// (design point 7); exits once `stopped` is set. Holds only a `Weak`, so it exits automatically
    /// after all host handles are released (design point 6).
    async fn async_run(inner: Weak<Inner>) {
        loop {
            let Some(inner) = inner.upgrade() else {
                rmq_debug!("trace dispatcher released, worker exits");
                return;
            };
            if inner.stopped.load(Ordering::Acquire) {
                return;
            }
            if inner.flush_trace_context(false) == 0 {
                tokio::time::sleep(Duration::from_millis(Self::IDLE_SLEEP_MILLIS)).await;
            }
        }
    }

    /// The trigger condition is any of three: `force_flush`, queue length `>= batch_num`, or
    /// more than `FLUSH_TRACE_INTERVAL` since the last flush; at most `batch_num` contexts are taken per round.
    ///
    /// The return value is how many contexts this round actually drained (0 => nothing flushed, the caller sleeps 5ms).
    /// The busy-wait sleep and the swallowed exception are replaced by logging inside
    /// [`Self::async_run`] and [`Inner::async_send_trace_message`] respectively (design point 7).
    fn flush_trace_context(&self, force_flush: bool) -> usize {
        let size = self.queue_len();
        if size != 0 {
            let now = current_time_millis();
            if force_flush
                || size >= self.batch_num
                || now - self.last_flush_time.load(Ordering::Acquire)
                    > self.flush_trace_interval_millis
            {
                let mut context_list: Vec<TraceContext> = Vec::with_capacity(self.batch_num);
                for _ in 0..self.batch_num {
                    match self.pop_context() {
                        Some(context) => context_list.push(context),
                        None => break,
                    }
                }
                let drained = context_list.len();
                self.async_send_trace_message(context_list);
                return drained;
            }
        }
        0
    }

    fn async_send_trace_message(&self, context_list: Vec<TraceContext>) {
        if context_list.is_empty() {
            return;
        }
        self.last_flush_time
            .store(current_time_millis(), Ordering::Release);
        let drained = context_list.len();
        if let Err(e) = self.submit(context_list) {
            // A submit failure would otherwise bubble up to the catch in `async_run` / `flush`;
            // log a same-magnitude error message in place here instead.
            rmq_error!("flushTraceContext error: {e} ({drained} contexts dropped)");
        }
    }

    /// Hand a batch of contexts to the "executor" (submitted as a standalone tokio task).
    fn submit(&self, context_list: Vec<TraceContext>) -> Result<()> {
        let handle = self.runtime_handle()?;
        let me = self
            .this
            .upgrade()
            .ok_or_else(|| Error::client("trace dispatcher already released"))?;
        let task = handle.spawn(async move {
            me.send_trace_data(context_list).await;
        });
        self.track(task);
        Ok(())
    }

    /// In-flight task accounting (keeps the unfinished handles so [`Inner::flush_and_wait`] can wait for them).
    fn track(&self, handle: JoinHandle<()>) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.retain(|h| !h.is_finished());
        pending.push(handle);
    }

    fn take_pending(&self) -> Vec<JoinHandle<()>> {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *pending)
    }

    /// Lazily bind the runtime handle (design point 4): construction never touches `Handle`; it is
    /// fetched once and cached only when work actually needs dispatching.
    fn bind_handle(&self) -> Option<Handle> {
        if let Some(handle) = self.handle.get() {
            return Some(handle.clone());
        }
        let handle = Handle::try_current().ok()?;
        let _ = self.handle.set(handle.clone());
        Some(handle)
    }

    fn runtime_handle(&self) -> Result<Handle> {
        self.bind_handle().ok_or_else(|| {
            Error::client(
                "no tokio runtime available: trace dispatcher needs start() inside a runtime",
            )
        })
    }

    /// Check emptiness under the lock: spawn the background task only when `worker` is absent.
    fn start_worker(&self) {
        let mut worker = self.worker.lock().unwrap_or_else(|e| e.into_inner());
        if worker.is_none() {
            self.stopped.store(false, Ordering::Release);
            let weak = match self.this.upgrade() {
                Some(strong) => Arc::downgrade(&strong),
                None => return,
            };
            // The thread name "MQ-AsyncArrayDispatcher-Thread<id>" cannot be
            // reproduced on a tokio task (design point 5); only the instance number goes into the debug log here.
            rmq_debug!("start trace worker for instance {}", self.trace_instance_id);
            *worker = Some(tokio::spawn(Inner::async_run(weak)));
        }
    }

    fn flush(&self) {
        while self.queue_len() != 0 {
            if self.flush_trace_context(true) == 0 {
                rmq_warn!(
                    "flushTraceContext made no progress with {} queued (batch_num=0?)",
                    self.queue_len()
                );
                return;
            }
        }
    }

    async fn flush_and_wait(&self) {
        let deadline = Instant::now() + Duration::from_millis(self.wait_for_shutdown_millis);
        loop {
            self.flush();
            let pending = self.take_pending();
            if pending.is_empty() {
                return;
            }
            for join in pending {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    rmq_warn!(
                        "trace dispatcher wait for shutdown timeout ({}ms), \
                         pending trace batches may be lost",
                        self.wait_for_shutdown_millis
                    );
                    return;
                }
                match tokio::time::timeout(remaining, join).await {
                    Err(_) => rmq_warn!(
                        "trace dispatcher wait for shutdown timeout ({}ms)",
                        self.wait_for_shutdown_millis
                    ),
                    Ok(Err(e)) => rmq_debug!("trace send task cancelled: {e}"),
                    Ok(Ok(())) => {}
                }
            }
        }
    }

    /// The synchronous part of `shutdown()`.
    ///
    /// ⚠ The internal producer must not be closed in place here: the `flush()` above just spawned the
    /// remaining queued batches into in-flight send tasks, and an immediate `trace_producer.shutdown()` would make
    /// them hit "producer not started / connection closed" -- short-lived clients lost their last
    /// trace batch exactly this way. The internal producer's instance name is independent of the
    /// host (`TRACE_INSTANCE_NAME`) and shares no connection with the host client, so "wait for
    /// in-flight sends to converge -> close the producer -> stop the worker" is handed to
    /// [`run_finalize_blocking`]: hung on the runtime and waited for, bounded, on the calling thread
    /// -- "exiting right after shutdown" no longer loses the last trace batch. The budget is the
    /// larger of [`SHUTDOWN_FINALIZE_BUDGET`] and "the user-configured wait + slack" (the inner send
    /// wait is bounded by `wait_for_shutdown_millis`, configurable). A current_thread runtime cannot
    /// block here, so finalization stays a detached task with a warning (see that function's cases 2/3); to wait **synchronously** before closing, use [`AsyncTraceDispatcher::shutdown_gracefully`].
    fn shutdown(&self) {
        self.flush();
        if self.finalizing.swap(true, Ordering::AcqRel) {
            // Finalization is already running/done: a second shutdown does not repeat the producer-closing
            // verbatim behavior (design point 14), but re-hanging finalization here would cut, in the middle,
            // the in-flight sends the first round has not finished waiting for.
            return;
        }
        match (self.this.upgrade(), self.handle.get().cloned()) {
            (Some(me), Some(handle)) => {
                let budget = SHUTDOWN_FINALIZE_BUDGET.max(
                    Duration::from_millis(self.wait_for_shutdown_millis) + Duration::from_secs(5),
                );
                let _ = run_finalize_blocking(
                    &handle,
                    "trace dispatcher shutdown",
                    budget,
                    async move { me.finalize().await },
                );
            }
            // Never started (no runtime / no worker): no in-flight sends exist, finalize in place.
            _ => self.finalize_now(),
        }
    }

    /// Full finalization (hung and awaited by `run_finalize_blocking`): wait for the in-flight sends to finish
    /// (bounded by `wait_for_shutdown_millis`) before closing the producer.
    async fn finalize(&self) {
        self.flush_and_wait().await;
        self.finalize_now();
    }

    /// Finalize in place (no in-flight sends remain at this point).
    fn finalize_now(&self) {
        // Close the internal producer only if it was started
        if self.is_started.load(Ordering::Acquire) {
            self.trace_producer.shutdown();
        }
        // There is no process-exit hook (atexit) counterpart (design point 6)
        self.stored_stop();
    }

    /// Set `stopped` + terminate the background task (design point 9).
    fn stored_stop(&self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(handle) = self.worker.lock().unwrap_or_else(|e| e.into_inner()).take() {
            handle.abort();
        }
    }

    // ---------------- Sending ----------------

    async fn send_trace_data(&self, context_list: Vec<TraceContext>) {
        let default_channel = self.access_channel();
        let mut groups: Vec<(String, Vec<TraceTransferBean>)> = Vec::new();
        let mut positions: HashMap<String, usize> = HashMap::new();
        for context in context_list {
            let channel = context.access_channel.unwrap_or(default_channel);
            if context.region_id.is_empty() || context.trace_beans.is_empty() {
                continue;
            }
            let trace_topic = if channel.is_cloud() {
                format!(
                    "{}{}",
                    TraceConstants::TRACE_TOPIC_PREFIX,
                    context.region_id
                )
            } else {
                self.trace_topic_name.clone()
            };
            // `context.trace_beans[0].topic` (checked non-empty above)
            let Some(first) = context.trace_beans.first() else {
                continue;
            };
            let key = format!(
                "{}{}{trace_topic}",
                first.topic,
                TraceConstants::CONTENT_SPLITOR
            );
            let Some(bean) = TraceDataEncoder::encoder_from_context_bean(Some(&context)) else {
                continue;
            };
            match positions.get(&key) {
                Some(&index) => groups[index].1.push(bean),
                None => {
                    positions.insert(key.clone(), groups.len());
                    groups.push((key, vec![bean]));
                }
            }
        }
        for (key, bean_list) in groups {
            // The key looks like `topic + CONTENT_SPLITOR + trace_topic`; split only on the first separator
            let Some((topic, trace_topic)) = key.split_once(TraceConstants::CONTENT_SPLITOR) else {
                rmq_debug!("trace group key {key:?} has no content splitor, skipped");
                continue;
            };
            self.flush_data(bean_list, topic, trace_topic).await;
        }
    }

    /// The trailing "clear the pending list" is taken over by Rust's ownership.
    async fn flush_data(
        &self,
        trans_bean_list: Vec<TraceTransferBean>,
        _topic: &str,
        trace_topic: &str,
    ) {
        if trans_bean_list.is_empty() {
            return;
        }
        let limit = self.max_msg_size.load(Ordering::Acquire);
        let mut buffer = String::new();
        let mut buffer_chars = 0usize;
        let mut key_set: BTreeSet<String> = BTreeSet::new();
        let mut count = 0usize;
        for bean in trans_bean_list {
            key_set.extend(bean.trans_key);
            buffer_chars += bean.trans_data.chars().count();
            buffer.push_str(&bean.trans_data);
            count += 1;
            if buffer_chars >= limit {
                self.send_trace_data_by_mq(
                    std::mem::take(&mut key_set),
                    std::mem::take(&mut buffer),
                    trace_topic,
                )
                .await;
                buffer_chars = 0;
                count = 0;
            }
        }
        if count > 0 {
            self.send_trace_data_by_mq(key_set, buffer, trace_topic)
                .await;
        }
    }

    async fn send_trace_data_by_mq(
        &self,
        key_set: BTreeSet<String>,
        data: String,
        trace_topic: &str,
    ) {
        let mut msg = Message::new(trace_topic, Some(data.as_bytes()));
        let keys: Vec<&str> = key_set
            .iter()
            .filter(|key| !key.is_empty())
            .map(|key| key.as_str())
            .collect();
        msg.set_keys(&keys.join(KEY_SEPARATOR));

        let broker_set = self.try_get_message_queue_broker_set(trace_topic).await;
        let producer = Arc::clone(&self.trace_producer);
        let outcome = if broker_set.is_empty() {
            // No broker set available => plain send (the producer round-robins internally)
            producer.send(msg, self.send_msg_timeout_millis).await
        } else {
            let queues = producer
                .clone()
                .publish_queues(trace_topic.to_string())
                .await
                .unwrap_or_default();
            let selector = BrokerSetSelector {
                counter: Arc::clone(&self.send_which_queue),
            };
            match selector.select(&queues, &broker_set) {
                Some(queue) => {
                    producer
                        .send_to_queue(msg, queue, self.send_msg_timeout_millis)
                        .await
                }
                None => Err(Error::client(format!(
                    "no message queue of trace topic {trace_topic} for broker set {:?}",
                    sorted_broker_names(&broker_set)
                ))),
            }
        };
        if let Err(e) = outcome {
            // Error text verified verbatim
            rmq_error!("send trace data failed, the traceData is {data}: {e}");
        }
    }

    /// Only cross-cluster (CLOUD) scenarios need filtering by broker; when the route is unavailable an empty set is returned => plain round-robin.
    /// The log level is DEBUG.
    async fn try_get_message_queue_broker_set(&self, topic: &str) -> HashSet<String> {
        let mut broker_set = HashSet::new();
        let producer = Arc::clone(&self.trace_producer);
        match producer.publish_queues(topic.to_string()).await {
            Ok(queues) => {
                for queue in queues {
                    broker_set.insert(queue.broker_name);
                }
            }
            Err(e) => rmq_debug!("tryGetMessageQueueBrokerSet({topic}) failed: {e}"),
        }
        broker_set
    }

    /// Get the host clientId: **producer takes precedence over consumer**;
    /// returns an empty string when there is no host (or the host has no `_mq_client`).
    fn client_id(&self) -> String {
        let host = {
            let producer = self.host_producer.lock().unwrap_or_else(|e| e.into_inner());
            producer.clone().or_else(|| {
                self.host_consumer
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone()
            })
        };
        match host {
            Some(host) => host.client_id(),
            None => String::new(),
        }
    }

    fn set_access_channel(&self, channel: AccessChannel) {
        self.access_channel
            .store(access_channel_to_u8(channel), Ordering::Release);
    }

    // ---------------- Queue ----------------

    /// Enqueue; when full, hands the original value back (the equivalent of a failed non-blocking enqueue).
    fn push_context(&self, context: TraceContext) -> Option<TraceContext> {
        let mut queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        if queue.len() >= self.queue_capacity {
            return Some(context);
        }
        queue.push_back(context);
        None
    }

    /// Dequeue (non-blocking; an empty queue returns `None`).
    fn pop_context(&self) -> Option<TraceContext> {
        self.queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front()
    }

    fn queue_len(&self) -> usize {
        self.queue.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

impl TraceDispatcher for AsyncTraceDispatcher {
    /// The **synchronous** entry for `start(name_srv_addr, access_channel)`
    /// (object-safety requirement, see design point 4): the whole startup logic is handed to a tokio task,
    /// so `Ok` here only guarantees "the task is queued"; whether the internal producer came up is visible only in the logs.
    ///
    /// The only synchronously visible error: the current thread is not in a tokio runtime context and no handle was
    /// bound earlier
    /// => `Err`. Callers that can await should use [`AsyncTraceDispatcher::start`].
    fn start(&self, name_server_addr: &str) -> Result<()> {
        let handle = self.inner.runtime_handle()?;
        let me = self.clone();
        let addr = name_server_addr.to_string();
        handle.spawn(async move {
            if let Err(e) = AsyncTraceDispatcher::start(&me, &addr, None).await {
                rmq_error!("trace dispatcher start failed: {e}");
            }
        });
        Ok(())
    }

    /// `shutdown()`: flush (without waiting) + close the internal producer + set `stopped`.
    /// The producer-closing step is executed by the finalization task after in-flight sends converge (otherwise the last
    /// trace batch would hit an already-closed producer); this method **waits, bounded, for it to land** (on a multithreaded runtime;
    /// on a current_thread runtime it degrades to a detached task with a warning). To wait
    /// **asynchronously**, use [`AsyncTraceDispatcher::shutdown_gracefully`].
    fn shutdown(&self) {
        self.inner.shutdown();
    }
}

impl TraceReportSink for AsyncTraceDispatcher {
    fn trace_topic_name(&self) -> String {
        self.inner.trace_topic_name.clone()
    }

    fn report(&self, context: TraceContext) -> bool {
        self.append(context)
    }

    fn client_id(&self) -> String {
        self.inner.client_id()
    }
}

/// Representable form of `AccessChannel` (stored as an atomic value here, see design point 15).
fn access_channel_to_u8(channel: AccessChannel) -> u8 {
    match channel {
        AccessChannel::Local => 0,
        AccessChannel::Cloud => 1,
    }
}

fn access_channel_from_u8(raw: u8) -> AccessChannel {
    match raw {
        1 => AccessChannel::Cloud,
        _ => AccessChannel::Local,
    }
}

/// Only so the "no queue selectable" error log is stably assertable (`HashSet` iteration order is nondeterministic).
fn sorted_broker_names(broker_set: &HashSet<String>) -> Vec<&str> {
    let mut names: Vec<&str> = broker_set.iter().map(|name| name.as_str()).collect();
    names.sort_unstable();
    names
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::result::LocalTransactionState;
    use crate::client::trace::{TraceBean, TraceType};
    use crate::common::message_type::MessageType;
    use std::sync::atomic::AtomicUsize;

    // Golden reference source: real encoded results captured by a local probe (not hand-derived).
    const SOH: char = TraceConstants::CONTENT_SPLITOR;
    const STX: char = TraceConstants::FIELD_SPLITOR;
    const MSG_ID_1: &str = "AC1400A1F0A018B4AAC2A1B2C3D4E5F6";
    const MSG_ID_2: &str = "AC1400A1F0A018B4AAC2A1B2C3D4E5F7";
    const OFFSET_MSG_ID: &str = "AC1400A1000027100000000000000001";
    const TS: i64 = 1_700_000_000_000;
    /// `EXPECTED_PUB` = the body golden reference captured by the probe.
    const EXPECTED_PUB: &str = "Pub\u{1}1700000000000\u{1}DefaultRegion\u{1}GID_test\
         \u{1}TopicTest\u{1}AC1400A1F0A018B4AAC2A1B2C3D4E5F6\u{1}TagA\u{1}KeyA KeyB\
         \u{1}127.0.0.1:10911\u{1}42\u{1}7\u{1}0\
         \u{1}AC1400A1000027100000000000000001\u{1}true\u{2}";
    const EXPECTED_PUB_KEYS: &str = "AC1400A1F0A018B4AAC2A1B2C3D4E5F6 KeyA KeyB";

    // ---------------- Fake producer (a test stub recording the call sequence) ----------------

    #[derive(Debug, Clone)]
    struct SentRecord {
        /// `send` / `send_to_queue`: distinguishes the two send paths.
        path: &'static str,
        topic: String,
        body: String,
        keys: String,
        timeout_millis: i64,
        queue: Option<MessageQueue>,
    }

    #[derive(Default)]
    struct Recorder {
        records: Mutex<Vec<SentRecord>>,
        setters: Mutex<Vec<String>>,
        started: AtomicUsize,
        shutdowns: AtomicUsize,
        publish_calls: AtomicUsize,
        queues: Mutex<Vec<MessageQueue>>,
    }

    impl Recorder {
        fn push_setter(&self, text: String) {
            self.setters
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(text);
        }

        fn setters(&self) -> Vec<String> {
            self.setters
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
        }

        fn records(&self) -> Vec<SentRecord> {
            self.records
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
        }

        fn set_queues(&self, queues: Vec<MessageQueue>) {
            *self.queues.lock().unwrap_or_else(|e| e.into_inner()) = queues;
        }

        fn push(&self, record: SentRecord) {
            self.records
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(record);
        }
    }

    struct FakeProducer {
        recorder: Arc<Recorder>,
        /// When enabled every send returns `Err`, used to verify the path that
        /// "catches the send exception, logs only, and keeps sending the next chunk".
        fail_send: bool,
    }

    impl TraceProducer for FakeProducer {
        fn set_name_server_addr(&self, name_server_addr: &str) {
            self.recorder
                .push_setter(format!("namesrv={name_server_addr}"));
        }

        fn set_instance_name(&self, instance_name: &str) {
            self.recorder
                .push_setter(format!("instance={instance_name}"));
        }

        fn set_namespace_v2(&self, namespace_v2: &str) {
            self.recorder.push_setter(format!("ns={namespace_v2}"));
        }

        fn set_send_msg_timeout(&self, timeout_millis: i64) {
            self.recorder
                .push_setter(format!("send_msg_timeout={timeout_millis}"));
        }

        fn set_max_message_size(&self, max_msg_size: usize) {
            self.recorder
                .push_setter(format!("max_message_size={max_msg_size}"));
        }

        fn set_enable_trace(&self, enable: bool) {
            self.recorder.push_setter(format!("enable_trace={enable}"));
        }

        fn start(self: Arc<Self>) -> TraceProducerFuture<Result<()>> {
            let recorder = Arc::clone(&self.recorder);
            Box::pin(async move {
                recorder.started.fetch_add(1, Ordering::AcqRel);
                Ok(())
            })
        }

        fn shutdown(&self) {
            self.recorder.shutdowns.fetch_add(1, Ordering::AcqRel);
        }

        fn send(
            self: Arc<Self>,
            message: Message,
            timeout_millis: i64,
        ) -> TraceProducerFuture<Result<()>> {
            let fail_send = self.fail_send;
            Box::pin(async move {
                self.recorder
                    .push(record_of("send", &message, timeout_millis, None));
                if fail_send {
                    return Err(Error::client("fake trace send failure"));
                }
                Ok(())
            })
        }

        fn send_to_queue(
            self: Arc<Self>,
            message: Message,
            queue: MessageQueue,
            timeout_millis: i64,
        ) -> TraceProducerFuture<Result<()>> {
            Box::pin(async move {
                self.recorder.push(record_of(
                    "send_to_queue",
                    &message,
                    timeout_millis,
                    Some(queue),
                ));
                Ok(())
            })
        }

        fn publish_queues(
            self: Arc<Self>,
            _topic: String,
        ) -> TraceProducerFuture<Result<Vec<MessageQueue>>> {
            Box::pin(async move {
                self.recorder.publish_calls.fetch_add(1, Ordering::AcqRel);
                let queues = self
                    .recorder
                    .queues
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                if queues.is_empty() {
                    // In the test stub, fetching publish queues errors out directly
                    Err(Error::client("no route in unit test"))
                } else {
                    Ok(queues)
                }
            })
        }
    }

    fn record_of(
        path: &'static str,
        message: &Message,
        timeout_millis: i64,
        queue: Option<MessageQueue>,
    ) -> SentRecord {
        SentRecord {
            path,
            topic: message.topic.clone(),
            body: String::from_utf8_lossy(message.body.as_deref().unwrap_or_default()).into_owned(),
            keys: message.get_keys().unwrap_or_default().to_string(),
            timeout_millis,
            queue,
        }
    }

    fn dispatcher_with(
        batch_num: usize,
        trace_topic: Option<&str>,
    ) -> (AsyncTraceDispatcher, Arc<Recorder>) {
        build_dispatcher(batch_num, trace_topic, false)
    }

    /// Same as [`dispatcher_with`], but every send of the internal producer fails.
    fn failing_dispatcher(batch_num: usize) -> (AsyncTraceDispatcher, Arc<Recorder>) {
        build_dispatcher(batch_num, None, true)
    }

    fn build_dispatcher(
        batch_num: usize,
        trace_topic: Option<&str>,
        fail_send: bool,
    ) -> (AsyncTraceDispatcher, Arc<Recorder>) {
        // Two Arcs point at the same Recorder: FakeProducer writes the book, the test reads it
        let recorder = Arc::new(Recorder::default());
        let producer = Arc::new(FakeProducer {
            recorder: Arc::clone(&recorder),
            fail_send,
        });
        let dispatcher = AsyncTraceDispatcher::with_config(
            "GID_test",
            TraceDispatcherType::Produce,
            TraceDispatcherConfig {
                producer: Some(producer),
                batch_num,
                trace_topic_name: trace_topic.map(|t| t.to_string()),
                ..Default::default()
            },
        );
        (dispatcher, recorder)
    }

    fn dispatcher() -> (AsyncTraceDispatcher, Arc<Recorder>) {
        dispatcher_with(10, None)
    }

    // ---------------- Context construction (same shape as the golden cases) ----------------

    fn bean(topic: &str, msg_id: &str, keys: &str) -> TraceBean {
        TraceBean {
            topic: topic.to_string(),
            msg_id: msg_id.to_string(),
            offset_msg_id: OFFSET_MSG_ID.to_string(),
            tags: "TagA".into(),
            keys: keys.into(),
            store_host: "127.0.0.1:10911".into(),
            client_host: "127.0.0.1:10911".into(),
            store_time: 1_700_000_000_123,
            retry_times: 2,
            body_length: 42,
            msg_type: MessageType::NormalMsg,
            transaction_id: Some("TRAN-001".into()),
            transaction_state: Some(LocalTransactionState::CommitMessage.to_string()),
            from_transaction_check: false,
        }
    }

    fn pub_context(topic: &str, msg_id: &str, region: &str) -> TraceContext {
        TraceContext {
            trace_type: Some(TraceType::Pub),
            time_stamp: TS,
            region_id: region.to_string(),
            region_name: String::new(),
            group_name: "GID_test".into(),
            cost_time: 7,
            is_success: true,
            request_id: "REQ-PUB-001".into(),
            context_code: 0,
            access_channel: None,
            trace_beans: vec![bean(topic, msg_id, "KeyA KeyB")],
        }
    }

    fn simple_pub() -> TraceContext {
        pub_context("TopicTest", MSG_ID_1, "DefaultRegion")
    }

    fn record_count(body: &str) -> usize {
        body.chars().filter(|c| *c == STX).count()
    }

    // ---------------- Defaults / constants (probe section 1) ----------------

    #[test]
    fn defaults_match_golden_reference() {
        let (d, recorder) = dispatcher();
        assert_eq!(d.batch_num(), 10);
        assert_eq!(d.max_msg_size(), 128_000);
        assert_eq!(d.get_trace_topic_name(), MixAll::TRACE_TOPIC);
        assert_eq!(d.group(), "GID_test");
        assert_eq!(d.dispatcher_type(), TraceDispatcherType::Produce);
        assert_eq!(d.discard_count(), 0);
        assert!(!d.is_started());
        assert!(!d.is_stopped());
        assert_eq!(d.access_channel(), AccessChannel::Local);
        assert_eq!(d.queue_size(), 0);
        // The three defaults applied to the internal producer at construction
        assert_eq!(
            recorder.setters(),
            vec![
                "send_msg_timeout=5000",
                "max_message_size=128000",
                "enable_trace=false"
            ]
        );
    }

    #[test]
    fn batch_num_is_clamped_to_twenty() {
        // `batch_num` is capped at 20
        for (input, want) in [(50usize, 20usize), (1, 1), (100, 20), (20, 20)] {
            let (d, _) = dispatcher_with(input, None);
            assert_eq!(d.batch_num(), want, "batch_num({input})");
        }
    }

    #[test]
    fn trace_topic_falls_back_on_none_and_empty_string() {
        // When `trace_topic_name` is missing, fall back to `MixAll::TRACE_TOPIC`
        let (d, _) = dispatcher_with(10, Some("MyTraceTopic"));
        assert_eq!(d.get_trace_topic_name(), "MyTraceTopic");
        assert_eq!(
            TraceReportSink::trace_topic_name(&d),
            "MyTraceTopic",
            "the hook side reads the same value"
        );
        let (d, _) = dispatcher_with(10, Some(""));
        assert_eq!(d.get_trace_topic_name(), "RMQ_SYS_TRACE_TOPIC");
    }

    #[test]
    fn trace_producer_group_format() {
        // Literal-format golden reference:
        // after building two dispatchers in a row, gen is successively
        //   _INNER_TRACE_PRODUCER-GID_test-PRODUCE-3
        //   _INNER_TRACE_PRODUCER-CID_test-CONSUME-4
        // (the first two constructions took numbers 1 and 2 -- the counter starts at 1)
        assert_eq!(
            format_trace_producer_group("GID_test", TraceDispatcherType::Produce, 3),
            "_INNER_TRACE_PRODUCER-GID_test-PRODUCE-3"
        );
        assert_eq!(
            format_trace_producer_group("CID_test", TraceDispatcherType::Consume, 4),
            "_INNER_TRACE_PRODUCER-CID_test-CONSUME-4"
        );
        // The auto-increment version: can only assert "strictly growing" -- parallel tests are also taking numbers
        let first = AsyncTraceDispatcher::next_trace_producer_group(
            "GID_test",
            TraceDispatcherType::Produce,
        );
        let second = AsyncTraceDispatcher::next_trace_producer_group(
            "GID_test",
            TraceDispatcherType::Produce,
        );
        assert!(
            first.starts_with("_INNER_TRACE_PRODUCER-GID_test-PRODUCE-"),
            "{first}"
        );
        assert!(
            group_seq(&second) > group_seq(&first),
            "{first} -> {second}"
        );
        // Construction also takes a number (the name is needed when building the internal producer)
        let (d, _) = dispatcher();
        assert!(
            d.trace_producer_group()
                .starts_with("_INNER_TRACE_PRODUCER-GID_test-PRODUCE-"),
            "{}",
            d.trace_producer_group()
        );
    }

    #[test]
    fn injected_group_name_is_used_verbatim() {
        // Design point 1: the caller computes the name first (needed to build the internal producer), then injects it back when constructing the dispatcher
        let group = format_trace_producer_group("G", TraceDispatcherType::Produce, 999);
        let d = AsyncTraceDispatcher::with_config(
            "G",
            TraceDispatcherType::Produce,
            TraceDispatcherConfig {
                producer: Some(Arc::new(DisabledTraceProducer)),
                trace_producer_group: Some(group.clone()),
                ..Default::default()
            },
        );
        assert_eq!(d.trace_producer_group(), group, "the injected name is kept verbatim");
        assert_eq!(d.group(), "G", "the business group name is unaffected");
        // Only without injection does the process-wide counter get used (fixed name prefix, number > 0)
        let (built, _) = dispatcher();
        assert!(
            built
                .trace_producer_group()
                .starts_with("_INNER_TRACE_PRODUCER-GID_test-PRODUCE-"),
            "{}",
            built.trace_producer_group()
        );
        assert!(group_seq(built.trace_producer_group()) > 0);
    }

    /// Take the incremented number from the tail of the group name; returns 0 when parsing fails (`unwrap_or` keeps the test from panicking).
    fn group_seq(name: &str) -> u64 {
        name.rsplit('-')
            .next()
            .unwrap_or_default()
            .parse::<u64>()
            .unwrap_or(0)
    }

    #[test]
    fn instance_id_is_process_wide_and_increments() {
        // Process-wide instance number: increments starting from 0.
        // Parallel tests also construct dispatchers => assert strict growth, not +1.
        let (a, _) = dispatcher();
        let (b, _) = dispatcher();
        assert!(
            b.trace_instance_id() > a.trace_instance_id(),
            "{} !> {}",
            b.trace_instance_id(),
            a.trace_instance_id()
        );
    }

    // ---------------- append / overflow ----------------

    #[test]
    fn append_returns_true_and_queues() {
        let (d, _) = dispatcher();
        assert!(d.append(simple_pub()));
        assert_eq!(d.queue_size(), 1);
        assert_eq!(d.discard_count(), 0);
    }

    #[test]
    fn append_returns_false_when_queue_full() {
        // Probe golden reference: 2048 accepted, the 2049th dropped + discard_count=1
        let (d, _) = dispatcher();
        for _ in 0..2048 {
            assert!(d.append(simple_pub()));
        }
        assert_eq!(d.queue_size(), 2048);
        assert!(!d.append(simple_pub()));
        assert_eq!(d.discard_count(), 1);
        assert!(!d.append(simple_pub()));
        assert_eq!(d.discard_count(), 2);
        assert_eq!(d.queue_size(), 2048);
    }

    #[test]
    fn report_sink_delegates_to_append() {
        let (d, _) = dispatcher();
        assert!(TraceReportSink::report(&d, simple_pub()));
        assert_eq!(d.queue_size(), 1);
    }

    // ---------------- Sending: byte-for-byte against the golden reference ----------------

    #[tokio::test]
    async fn send_trace_data_single_pub_matches_golden() {
        let (d, recorder) = dispatcher();
        d.inner.send_trace_data(vec![simple_pub()]).await;
        let records = recorder.records();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        // No route => empty broker set => the plain `send(msg, 5000)` path
        assert_eq!(record.path, "send");
        assert_eq!(record.topic, "RMQ_SYS_TRACE_TOPIC");
        assert_eq!(record.body, EXPECTED_PUB);
        assert_eq!(record.keys, EXPECTED_PUB_KEYS);
        assert_eq!(record.timeout_millis, 5_000);
        assert_eq!(record.queue, None);
    }

    #[tokio::test]
    async fn send_trace_data_cloud_channel_uses_region_topic() {
        // Probe section 5: rmq_sys_TRACE_DATA_DefaultRegion
        let (d, recorder) = dispatcher();
        let mut ctx = simple_pub();
        ctx.access_channel = Some(AccessChannel::Cloud);
        d.inner.send_trace_data(vec![ctx]).await;
        let records = recorder.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].topic, "rmq_sys_TRACE_DATA_DefaultRegion");
        assert_eq!(records[0].body, EXPECTED_PUB);
    }

    #[tokio::test]
    async fn dispatcher_access_channel_applies_to_context_without_one() {
        // The context's own access_channel takes precedence; the dispatcher's is used only when absent
        let (d, recorder) = dispatcher();
        d.inner.set_access_channel(AccessChannel::Cloud);
        d.inner.send_trace_data(vec![simple_pub()]).await;
        assert_eq!(
            recorder.records()[0].topic,
            "rmq_sys_TRACE_DATA_DefaultRegion"
        );
    }

    #[tokio::test]
    async fn contexts_without_region_or_beans_are_skipped() {
        // Probe section 7: empty region / empty beans both send nothing
        let (d, recorder) = dispatcher();
        d.inner
            .send_trace_data(vec![pub_context("TopicTest", MSG_ID_1, ""), {
                let mut ctx = simple_pub();
                ctx.trace_beans = Vec::new();
                ctx
            }])
            .await;
        assert!(recorder.records().is_empty());
    }

    #[tokio::test]
    async fn groups_by_business_topic_in_insertion_order() {
        // Probe section 6: 2 groups, TopicA group 2 records, TopicB group 1 (order = insertion order)
        let (d, recorder) = dispatcher();
        d.inner
            .send_trace_data(vec![
                pub_context("TopicA", MSG_ID_1, "DefaultRegion"),
                pub_context("TopicB", MSG_ID_1, "DefaultRegion"),
                pub_context("TopicA", MSG_ID_2, "DefaultRegion"),
            ])
            .await;
        let records = recorder.records();
        assert_eq!(records.len(), 2);
        assert_eq!(record_count(&records[0].body), 2, "the TopicA group has 2 records");
        assert_eq!(record_count(&records[1].body), 1);
        assert!(records[0].body.contains("TopicA"), "{}", records[0].body);
        assert!(records[1].body.contains("TopicB"), "{}", records[1].body);
        // Each group's KEYS contain only that group's msgId
        assert!(records[0].body.contains(MSG_ID_2));
        assert_eq!(records[1].keys, EXPECTED_PUB_KEYS);
    }

    #[tokio::test]
    async fn two_records_concatenate_in_order() {
        // Body / keys of probe section 17
        let (d, recorder) = dispatcher();
        d.inner
            .send_trace_data(vec![
                pub_context("TopicTest", MSG_ID_1, "DefaultRegion"),
                pub_context("TopicTest", MSG_ID_2, "DefaultRegion"),
            ])
            .await;
        let records = recorder.records();
        assert_eq!(records.len(), 1);
        let expected_second = EXPECTED_PUB.replace(MSG_ID_1, MSG_ID_2);
        assert_eq!(records[0].body, format!("{EXPECTED_PUB}{expected_second}"));
        assert_eq!(records[0].keys, format!("{MSG_ID_1} {MSG_ID_2} KeyA KeyB"));
    }

    #[tokio::test]
    async fn flush_data_splits_by_max_msg_size() {
        // Probe section 8: one record is 160 code points
        //   limit=160 / 3 records => [1,1,1]
        //   limit=320 / 5 records => [2,2,1]
        //   limit=319 / 5 records => [2,2,1]
        //   limit=1_000_000 / 4 records => [4]
        let bean_len = EXPECTED_PUB.chars().count();
        assert_eq!(bean_len, 160, "golden: len(trans_data)=160");
        for (limit, n, expect_records) in [
            (160, 3, vec![1, 1, 1]),
            (320, 5, vec![2, 2, 1]),
            (319, 5, vec![2, 2, 1]),
            (1_000_000, 4, vec![4]),
        ] {
            let (d, recorder) = dispatcher();
            d.set_max_msg_size(limit);
            let beans: Vec<TraceTransferBean> = (0..n)
                .map(|i| {
                    let ctx = pub_context(
                        "TopicTest",
                        if i % 2 == 0 { MSG_ID_1 } else { MSG_ID_2 },
                        "DefaultRegion",
                    );
                    TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap_or_default()
                })
                .collect();
            d.inner
                .flush_data(beans, "TopicTest", "RMQ_SYS_TRACE_TOPIC")
                .await;
            let records = recorder.records();
            assert_eq!(
                records
                    .iter()
                    .map(|r| record_count(&r.body))
                    .collect::<Vec<_>>(),
                expect_records,
                "limit={limit} n={n}"
            );
            assert!(records.iter().all(|r| r.topic == "RMQ_SYS_TRACE_TOPIC"));
        }
    }

    #[tokio::test]
    async fn flush_data_keys_are_union_of_chunk() {
        // Probe section 8 limit=320: the first two chunks' keys contain both msgIds, the third chunk keeps only the later one
        let (d, recorder) = dispatcher();
        d.set_max_msg_size(320);
        let beans: Vec<TraceTransferBean> = [MSG_ID_2, MSG_ID_1, MSG_ID_2, MSG_ID_1, MSG_ID_2]
            .iter()
            .map(|msg_id| {
                let ctx = pub_context("TopicTest", msg_id, "DefaultRegion");
                TraceDataEncoder::encoder_from_context_bean(Some(&ctx)).unwrap_or_default()
            })
            .collect();
        d.inner
            .flush_data(beans, "TopicTest", "RMQ_SYS_TRACE_TOPIC")
            .await;
        let keys: Vec<String> = recorder.records().iter().map(|r| r.keys.clone()).collect();
        assert_eq!(
            keys,
            vec![
                format!("{MSG_ID_1} {MSG_ID_2} KeyA KeyB"),
                format!("{MSG_ID_1} {MSG_ID_2} KeyA KeyB"),
                format!("{MSG_ID_2} KeyA KeyB"),
            ]
        );
    }

    #[tokio::test]
    async fn empty_bean_list_sends_nothing() {
        let (d, recorder) = dispatcher();
        d.inner
            .flush_data(Vec::new(), "TopicTest", "RMQ_SYS_TRACE_TOPIC")
            .await;
        assert!(recorder.records().is_empty());
    }

    #[tokio::test]
    async fn blank_keys_are_filtered_out() {
        // Probe section 9: empty-string keys are filtered; all empty => KEYS is an empty string
        let (d, recorder) = dispatcher();
        let mut bean =
            TraceDataEncoder::encoder_from_context_bean(Some(&simple_pub())).unwrap_or_default();
        bean.trans_key.insert(String::new());
        d.inner
            .flush_data(vec![bean], "TopicTest", "RMQ_SYS_TRACE_TOPIC")
            .await;
        assert_eq!(recorder.records()[0].keys, EXPECTED_PUB_KEYS);

        let (d, recorder) = dispatcher();
        let mut bean =
            TraceDataEncoder::encoder_from_context_bean(Some(&simple_pub())).unwrap_or_default();
        bean.trans_key = BTreeSet::new();
        d.inner
            .flush_data(vec![bean], "TopicTest", "RMQ_SYS_TRACE_TOPIC")
            .await;
        assert_eq!(recorder.records()[0].keys, "");
    }

    // ---------------- Send failure: logged only, never interrupts ----------------

    #[tokio::test]
    async fn send_failure_is_logged_and_next_chunk_still_sent() {
        let (d, recorder) = failing_dispatcher(10);
        d.set_max_msg_size(1); // each record forms its own chunk => two sends
        let bean_a =
            TraceDataEncoder::encoder_from_context_bean(Some(&simple_pub())).unwrap_or_default();
        let bean_b = TraceDataEncoder::encoder_from_context_bean(Some(&pub_context(
            "TopicTest",
            MSG_ID_2,
            "DefaultRegion",
        )))
        .unwrap_or_default();
        d.inner
            .flush_data(vec![bean_a, bean_b], "TopicTest", "RMQ_SYS_TRACE_TOPIC")
            .await;
        let records = recorder.records();
        assert_eq!(records.len(), 2, "both chunks were attempted");
        assert!(
            records.iter().all(|r| r.path == "send"),
            "no route in the unit test => broker_set is empty, plain send is used"
        );
    }

    #[tokio::test]
    async fn failing_send_does_not_block_shutdown() {
        // The queue is still drained and the internal producer is still closed (`shutdown` ignores send results;
        // batch_num=1 + send throwing: queue goes to 0, both records attempted)
        let (d, recorder) = failing_dispatcher(1);
        d.start("127.0.0.1:9876", None).await.unwrap();
        d.set_last_flush_time(current_time_millis());
        d.append(simple_pub());
        d.append(pub_context("TopicTest", MSG_ID_2, "DefaultRegion"));
        d.shutdown_gracefully().await;
        assert_eq!(d.queue_size(), 0);
        assert_eq!(recorder.records().len(), 2);
        assert_eq!(recorder.shutdowns.load(Ordering::Acquire), 1);
        assert!(d.is_stopped());
    }

    // ---------------- Broker-set queue selection (probe section 10) ----------------

    #[tokio::test]
    async fn broker_set_round_robin_matches_golden() {
        let (d, recorder) = dispatcher();
        recorder.set_queues(vec![
            MessageQueue::new("RMQ_SYS_TRACE_TOPIC", "broker-a", 0),
            MessageQueue::new("RMQ_SYS_TRACE_TOPIC", "broker-b", 3),
            MessageQueue::new("RMQ_SYS_TRACE_TOPIC", "broker-a", 1),
        ]);
        for msg_id in [MSG_ID_1, MSG_ID_2, MSG_ID_1] {
            d.inner
                .send_trace_data(vec![pub_context("TopicTest", msg_id, "DefaultRegion")])
                .await;
        }
        let records = recorder.records();
        assert_eq!(records.len(), 3);
        assert!(
            records.iter().all(|r| r.path == "send_to_queue"),
            "with a route present the selector path is used"
        );
        let selected: Vec<(String, i32)> = records
            .iter()
            .map(|r| {
                let q = r.queue.clone().unwrap_or_default();
                (q.broker_name, q.queue_id)
            })
            .collect();
        assert_eq!(
            selected,
            vec![
                ("broker-a".to_string(), 0),
                ("broker-b".to_string(), 3),
                ("broker-a".to_string(), 1),
            ]
        );
        // The counter starts at 0
        assert_eq!(d.inner.send_which_queue.load(Ordering::Acquire), 3);
        // Two route lookups: one to compute the broker set, one for the selector to pick a queue
        assert_eq!(recorder.publish_calls.load(Ordering::Acquire), 6);
    }

    #[tokio::test]
    async fn broker_set_filter_only_touches_listed_brokers() {
        let (d, _) = dispatcher();
        let queues = vec![
            MessageQueue::new("T", "broker-a", 0),
            MessageQueue::new("T", "broker-b", 7),
        ];
        let selector = BrokerSetSelector {
            counter: Arc::clone(&d.inner.send_which_queue),
        };
        let only_a: HashSet<String> = ["broker-a".to_string()].into_iter().collect();
        for _ in 0..4 {
            let q = selector.select(&queues, &only_a).unwrap_or_default();
            assert_eq!(q.broker_name, "broker-a");
            assert_eq!(q.queue_id, 0);
        }
        let none: HashSet<String> = ["broker-z".to_string()].into_iter().collect();
        let mut seen = Vec::new();
        for _ in 0..4 {
            seen.push(
                selector
                    .select(&queues, &none)
                    .unwrap_or_else(|| MessageQueue::new("T", "none", -1))
                    .broker_name,
            );
        }
        // The counter was already used 4 times above => 4 % 2 == 0, starting from the first entry of the route list
        // (probe measurement: the 4 intersection hits were all Q(T,broker-a,0); after degradation the sequence is
        // Q(T,broker-a,0), Q(T,broker-b,7), Q(T,broker-a,0), Q(T,broker-b,7))
        assert_eq!(seen, vec!["broker-a", "broker-b", "broker-a", "broker-b"]);
        assert!(selector.select(&[], &none).is_none());
    }

    // ---------------- Batching / flushing (probe sections 12, 13) ----------------

    #[tokio::test]
    async fn below_batch_and_interval_flushes_nothing() {
        let (d, recorder) = dispatcher_with(10, None);
        for _ in 0..5 {
            assert!(d.append(simple_pub()));
        }
        assert_eq!(d.inner.flush_trace_context(false), 0);
        assert_eq!(d.queue_size(), 5);
        assert!(recorder.records().is_empty());
    }

    #[tokio::test]
    async fn interval_overrun_triggers_flush() {
        let (d, recorder) = dispatcher_with(10, None);
        for _ in 0..5 {
            d.append(simple_pub());
        }
        d.set_last_flush_time(current_time_millis() - 6_000);
        assert_eq!(d.inner.flush_trace_context(false), 5);
        assert_eq!(d.queue_size(), 0);
        // last_flush_time is refreshed after the flush
        assert!(current_time_millis() - d.last_flush_time() < 5_000);
        d.flush_and_wait().await;
        assert_eq!(record_count(&recorder.records()[0].body), 5);
    }

    #[tokio::test]
    async fn batch_size_caps_drained_contexts() {
        // Probe section 12: batch_num=2 / 5 records, force takes only 2; consecutive flushes give 2/2/1
        let (d, _) = dispatcher_with(2, None);
        for _ in 0..5 {
            d.append(simple_pub());
        }
        assert_eq!(d.inner.flush_trace_context(true), 2);
        assert_eq!(d.queue_size(), 3);
        assert_eq!(d.inner.flush_trace_context(true), 2);
        assert_eq!(d.queue_size(), 1);
        assert_eq!(d.inner.flush_trace_context(true), 1);
        assert_eq!(d.queue_size(), 0);
        // Empty queue: flush_trace_context returns 0 directly (the caller sleeps 5ms in this case)
        assert_eq!(d.inner.flush_trace_context(true), 0);
        assert_eq!(d.inner.flush_trace_context(false), 0);
    }

    #[tokio::test]
    async fn flush_drains_queue_in_batch_sizes() {
        // Probe section 12: flush() drains batch_num=2 / 7 records
        let (d, recorder) = dispatcher_with(2, None);
        for _ in 0..7 {
            d.append(simple_pub());
        }
        d.flush();
        assert_eq!(d.queue_size(), 0);
        d.flush_and_wait().await;
        let records = recorder.records();
        assert_eq!(records.len(), 4, "four chunks: 2+2+2+1");
        assert_eq!(
            records
                .iter()
                .map(|r| record_count(&r.body))
                .collect::<Vec<_>>(),
            vec![2, 2, 2, 1]
        );
    }

    #[test]
    fn empty_context_list_is_not_submitted() {
        let (d, recorder) = dispatcher();
        d.inner.async_send_trace_message(Vec::new());
        assert_eq!(recorder.publish_calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn submit_without_runtime_only_logs() {
        // Design point 4: submitting outside a runtime context => the batch is dropped and logged at ERROR (no panic)
        let (d, recorder) = dispatcher_with(1, None);
        d.append(simple_pub());
        assert!(d.inner.submit(vec![simple_pub()]).is_err());
        d.flush();
        assert_eq!(d.queue_size(), 0, "dequeue happens before submit");
        assert!(recorder.records().is_empty());
    }

    #[test]
    fn zero_batch_num_does_not_spin_forever() {
        // Design point 10
        let (d, _) = dispatcher_with(0, None);
        d.append(simple_pub());
        d.flush();
        assert_eq!(d.queue_size(), 1);
    }

    // ---------------- Lifecycle (probe sections 14, 15) ----------------

    #[tokio::test]
    async fn start_configures_and_starts_inner_producer() {
        let (d, recorder) = dispatcher();
        d.start("127.0.0.1:9876;127.0.0.2:9876", Some(AccessChannel::Cloud))
            .await
            .unwrap();
        assert_eq!(
            recorder.setters(),
            vec![
                "send_msg_timeout=5000",
                "max_message_size=128000",
                "enable_trace=false",
                "namesrv=127.0.0.1:9876;127.0.0.2:9876",
                "instance=PID_CLIENT_INNER_TRACE_PRODUCER_127.0.0.1:9876;127.0.0.2:9876",
                "enable_trace=false",
            ]
        );
        assert_eq!(recorder.started.load(Ordering::Acquire), 1);
        assert!(d.is_started());
        assert!(!d.is_stopped());
        assert_eq!(d.access_channel(), AccessChannel::Cloud);
    }

    #[tokio::test]
    async fn start_propagates_namespace_v2_to_inner_producer() {
        let (d, recorder) = dispatcher();
        assert_eq!(
            d.get_namespace_v2(),
            "",
            "defaults to unset (empty string)"
        );
        d.set_namespace_v2("NS_V2");
        assert_eq!(d.get_namespace_v2(), "NS_V2");
        d.start("127.0.0.1:9876", None).await.unwrap();
        assert_eq!(
            recorder.setters(),
            vec![
                "send_msg_timeout=5000",
                "max_message_size=128000",
                "enable_trace=false",
                "namesrv=127.0.0.1:9876",
                "instance=PID_CLIENT_INNER_TRACE_PRODUCER_127.0.0.1:9876",
                "ns=NS_V2",
                "enable_trace=false",
            ]
        );
        TraceDispatcher::shutdown(&d);
    }

    #[tokio::test]
    async fn start_without_namespace_v2_skips_the_setter() {
        // Unset => no empty setter call (the empty-string skip branch of design point 15)
        let (d, recorder) = dispatcher();
        d.start("127.0.0.1:9876", None).await.unwrap();
        assert!(
            !recorder.setters().iter().any(|s| s.starts_with("ns=")),
            "actual: {:?}",
            recorder.setters()
        );
        TraceDispatcher::shutdown(&d);
    }

    #[tokio::test]
    async fn second_start_keeps_producer_but_resets_access_channel() {
        // Probe section 14: after `is_started`, namesrv/instance are not set again and start is not called again;
        // `access_channel or LOCAL` => passing None resets the channel back to LOCAL
        let (d, recorder) = dispatcher();
        d.start("127.0.0.1:9876", Some(AccessChannel::Cloud))
            .await
            .unwrap();
        d.start("127.0.0.1:9876", None).await.unwrap();
        assert_eq!(recorder.started.load(Ordering::Acquire), 1);
        assert_eq!(
            recorder
                .setters()
                .iter()
                .filter(|s| s.starts_with("namesrv="))
                .count(),
            1
        );
        assert_eq!(d.access_channel(), AccessChannel::Local);
    }

    #[tokio::test]
    async fn start_worker_flushes_batched_contexts() {
        // Background loop: send once batch_num contexts have accumulated
        let (d, recorder) = dispatcher_with(2, None);
        d.start("127.0.0.1:9876", None).await.unwrap();
        d.append(simple_pub());
        d.append(pub_context("TopicTest", MSG_ID_2, "DefaultRegion"));
        for _ in 0..100 {
            if !recorder.records().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let records = recorder.records();
        assert_eq!(records.len(), 1, "one batch of two: {records:?}");
        assert_eq!(record_count(&records[0].body), 2);
        TraceDispatcher::shutdown(&d);
    }

    #[tokio::test]
    async fn shutdown_flushes_queue_and_stops_producer() {
        // Probe section 15
        let (d, recorder) = dispatcher_with(20, None);
        d.append(simple_pub());
        d.append(pub_context("TopicTest", MSG_ID_2, "DefaultRegion"));
        d.start("127.0.0.1:9876", None).await.unwrap();
        // After start, start_worker drains whatever is queued; refill here before closing
        d.set_last_flush_time(current_time_millis());
        d.append(simple_pub());
        TraceDispatcher::shutdown(&d);
        assert_eq!(d.queue_size(), 0);
        // Finalization (wait for in-flight sends -> close the internal producer -> stop the worker) is hung on the runtime;
        // this test's current_thread runtime cannot block (case 2 of `run_finalize_blocking`),
        // so wait for convergence here: is_stopped only becomes true after finalize_now.
        for _ in 0..500 {
            if d.is_stopped() && recorder.shutdowns.load(Ordering::Acquire) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(d.is_stopped());
        assert_eq!(recorder.shutdowns.load(Ordering::Acquire), 1);
        assert!(d.is_started());
        assert!(!recorder.records().is_empty());
    }

    #[tokio::test]
    async fn shutdown_without_start_does_not_stop_producer() {
        // Skipped directly when never started
        let (d, recorder) = dispatcher();
        d.append(simple_pub());
        TraceDispatcher::shutdown(&d);
        assert_eq!(d.queue_size(), 0);
        // is_started=false => finalize_now does not close the internal producer; stopped is set by finalization
        for _ in 0..500 {
            if d.is_stopped() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(recorder.shutdowns.load(Ordering::Acquire), 0);
        assert!(d.is_stopped());
    }

    #[tokio::test]
    async fn shutdown_gracefully_waits_for_inflight_sends() {
        let (d, recorder) = dispatcher_with(20, None);
        d.start("127.0.0.1:9876", None).await.unwrap();
        d.append(simple_pub());
        d.shutdown_gracefully().await;
        assert_eq!(recorder.records().len(), 1);
        assert_eq!(recorder.records()[0].keys, EXPECTED_PUB_KEYS);
        assert!(d.is_stopped());
    }

    /// Lands on return: on a multithreaded runtime `shutdown()` waits for the last trace batch to be sent and the
    /// internal producer to be closed before coming back -- "exiting the process right after shutdown" no longer loses traces. The assertions run
    /// **without await and without yielding a tick** after it returns: under the old implementation (return right after spawn) the finalization task would not even be scheduled and the record would be missing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sync_shutdown_finishes_before_returning() {
        let (d, recorder) = dispatcher_with(20, None);
        d.start("127.0.0.1:9876", None).await.unwrap();
        // After start, start_worker drains whatever is queued; refill here before closing
        d.set_last_flush_time(current_time_millis());
        d.append(simple_pub());
        TraceDispatcher::shutdown(&d);
        assert!(d.is_stopped(), "worker already stopped on return");
        assert_eq!(
            recorder.shutdowns.load(Ordering::Acquire),
            1,
            "internal producer already closed on return"
        );
        assert_eq!(recorder.records().len(), 1, "last trace batch already sent on return");
        assert_eq!(d.queue_size(), 0);
    }

    /// Even an immediate process exit loses nothing: the whole runtime is torn down right after `shutdown()` returns
    /// (equivalent to "process::exit right after shutdown"). Under the old implementation the finalization task was
    /// cancelled together with the runtime and the last trace batch could not go out; now it has already landed before return, and tearing down the runtime only sweeps up.
    #[test]
    fn shutdown_survives_immediate_runtime_drop() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("multi-thread runtime");
        let (d, recorder) = runtime.block_on(async {
            let (d, recorder) = dispatcher_with(20, None);
            d.start("127.0.0.1:9876", None).await.unwrap();
            (d, recorder)
        });
        d.set_last_flush_time(current_time_millis());
        d.append(simple_pub());
        // shutdown called outside the runtime (pure std thread) -- exactly the shape of `main()` teardown.
        TraceDispatcher::shutdown(&d);
        drop(runtime);
        assert!(d.is_stopped());
        assert_eq!(recorder.shutdowns.load(Ordering::Acquire), 1);
        assert_eq!(
            recorder.records().len(),
            1,
            "the last trace batch must already be sent before process exit"
        );
    }

    #[tokio::test]
    async fn trait_start_requires_and_uses_runtime() {
        // Design point 4: the synchronous entry throws the whole start into a tokio task
        let (d, recorder) = dispatcher();
        TraceDispatcher::start(&d, "127.0.0.1:9876").unwrap();
        for _ in 0..100 {
            if d.is_started() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(d.is_started());
        assert_eq!(recorder.started.load(Ordering::Acquire), 1);
        assert_eq!(d.access_channel(), AccessChannel::Local);
        TraceDispatcher::shutdown(&d);
    }

    #[tokio::test]
    async fn start_failure_keeps_dispatcher_unstarted() {
        struct FailingProducer;
        impl TraceProducer for FailingProducer {
            fn set_name_server_addr(&self, _: &str) {}
            fn set_instance_name(&self, _: &str) {}
            fn set_namespace_v2(&self, _: &str) {}
            fn set_send_msg_timeout(&self, _: i64) {}
            fn set_max_message_size(&self, _: usize) {}
            fn set_enable_trace(&self, _: bool) {}
            fn start(self: Arc<Self>) -> TraceProducerFuture<Result<()>> {
                Box::pin(async { Err(Error::client("producer boom")) })
            }
            fn shutdown(&self) {}
            fn send(self: Arc<Self>, _: Message, _: i64) -> TraceProducerFuture<Result<()>> {
                Box::pin(async { Ok(()) })
            }
            fn send_to_queue(
                self: Arc<Self>,
                _: Message,
                _: MessageQueue,
                _: i64,
            ) -> TraceProducerFuture<Result<()>> {
                Box::pin(async { Ok(()) })
            }
            fn publish_queues(
                self: Arc<Self>,
                _: String,
            ) -> TraceProducerFuture<Result<Vec<MessageQueue>>> {
                Box::pin(async { Ok(Vec::new()) })
            }
        }
        let d = AsyncTraceDispatcher::new(
            "GID_test",
            TraceDispatcherType::Produce,
            Arc::new(FailingProducer),
        );
        assert!(d.start("127.0.0.1:9876", None).await.is_err());
        assert!(!d.is_started());
        assert!(!d.is_stopped());
    }

    #[tokio::test]
    async fn disabled_producer_path_only_logs() {
        // Explicitly injecting [`DisabledTraceProducer`]: the whole path only logs and does not affect business
        // (the injection seam is kept for test stubs and host customization).
        let d = AsyncTraceDispatcher::with_config(
            "GID_test",
            TraceDispatcherType::Consume,
            TraceDispatcherConfig {
                producer: Some(Arc::new(DisabledTraceProducer)),
                batch_num: 1,
                ..Default::default()
            },
        );
        assert!(d.start("127.0.0.1:9876", None).await.is_err());
        d.append(simple_pub());
        d.flush_and_wait().await;
        assert_eq!(d.queue_size(), 0);
    }

    #[tokio::test]
    async fn missing_producer_builds_a_real_one() {
        let d = AsyncTraceDispatcher::with_config(
            "GID_test",
            TraceDispatcherType::Produce,
            TraceDispatcherConfig {
                batch_num: 1,
                ..Default::default()
            },
        );
        // The group name takes a number via `_INNER_TRACE_PRODUCER-<group>-<type>-<seq>`. The real producer's
        // start does not depend on namesrv reachability (it connects only at send time), so start **succeeds** -- versus
        // DisabledTraceProducer which is always Err; Ok here proves the self-built instance is the real implementation.
        assert!(d.start("127.0.0.1:1", None).await.is_ok());
        assert!(d.append(simple_pub()));
        d.flush_and_wait().await;
        assert_eq!(d.queue_size(), 0);
        d.shutdown_gracefully().await;
    }

    // ---------------- _client_id (probe section 16) ----------------

    #[tokio::test]
    async fn client_id_prefers_host_producer() {
        struct Host(FakeClientId);
        struct FakeClientId(&'static str);
        impl TraceHost for Host {
            fn client_id(&self) -> String {
                self.0 .0.to_string()
            }
        }
        let (d, _) = dispatcher();
        assert_eq!(TraceReportSink::client_id(&d), "");
        d.set_host_consumer(Arc::new(Host(FakeClientId("consumer-id"))));
        assert_eq!(TraceReportSink::client_id(&d), "consumer-id");
        d.set_host_producer(Arc::new(Host(FakeClientId("producer-id"))));
        assert_eq!(TraceReportSink::client_id(&d), "producer-id");
        // Host exists but clientId is empty => return the empty string, no more fallback to consumer
        d.set_host_producer(Arc::new(Host(FakeClientId(""))));
        assert_eq!(TraceReportSink::client_id(&d), "");
        // MQClientInstance used directly as the host (design point 16)
        let client = MQClientInstance::new("10.0.0.1@inst", vec!["127.0.0.1:9876".to_string()]);
        d.set_host_producer(Arc::new(client));
        assert_eq!(TraceReportSink::client_id(&d), "10.0.0.1@inst");
    }

    // ---------------- Misc ----------------

    #[test]
    fn trace_dispatcher_type_names() {
        assert_eq!(TraceDispatcherType::Produce.name(), "PRODUCE");
        assert_eq!(TraceDispatcherType::Consume.name(), "CONSUME");
        assert_eq!(TraceDispatcherType::Consume.to_string(), "CONSUME");
    }

    #[test]
    fn content_splitor_stays_between_topic_and_trace_topic() {
        // key = topic + CONTENT_SPLITOR + trace_topic
        let key = format!("TopicTest{SOH}RMQ_SYS_TRACE_TOPIC");
        let (topic, trace_topic) = key.split_once(SOH).unwrap_or_default();
        assert_eq!((topic, trace_topic), ("TopicTest", "RMQ_SYS_TRACE_TOPIC"));
    }

    #[test]
    fn config_debug_does_not_leak_producer() {
        let cfg = TraceDispatcherConfig::default();
        let text = format!("{cfg:?}");
        assert!(text.contains("producer: \"none\""), "{text}");
        assert!(text.contains("max_msg_size: 128000"), "{text}");
    }
}
