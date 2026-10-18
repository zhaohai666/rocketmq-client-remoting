//! `MQPullConsumerScheduleService`: per-queue scheduled pulling on top of [`DefaultMQPullConsumer`].
//!
//! Port of Java `consumer/MQPullConsumerScheduleService.java` (+ the scheduling role that
//! `impl/consumer/PullRequest.java` / `impl/consumer/PullMessageService.java` play for the push
//! path -- this facade needs neither: the "pull request" here is just the per-queue task loop).
//!
//! # Shape
//!
//! * One [`PullTaskCallback`] per topic ([`register_pull_task_callback`]); the callback owns the
//!   pulling itself, through the consumer exposed by [`PullTaskContext::pull_consumer`]
//!   (Java: `context.getPullConsumer().pull(..)`).
//! * One tokio task per [`MessageQueue`] ([`put_task`] expands/retires the task table exactly like
//!   Java `putTask`: queues that fell out of the set are cancelled, new queues spawn immediately).
//!   The same queue is therefore **strictly serial** -- the next round starts only after the
//!   previous `do_pull_task` returned.
//! * Queue sets come from the local rebalance view: CLUSTERING uses
//!   [`fetch_message_queues_in_balance`](DefaultMQPullConsumer::fetch_message_queues_in_balance),
//!   BROADCASTING uses
//!   [`fetch_subscribe_message_queues`](DefaultMQPullConsumer::fetch_subscribe_message_queues).
//!   The POP path (broker-side `QUERY_ASSIGNMENT`) is a push-consumer concern and does not apply here.
//!
//! # Rescheduling: the three channels of [`PullTaskContext`], by priority
//!
//! 1. the **return value** of `do_pull_task` (`Some(delay)`) -- one-shot, highest priority;
//! 2. [`pull_message_later`](PullTaskContext::pull_message_later) -- one-shot delay for the next
//!    round (= Java `setPullNextDelayTimeMillis`);
//! 3. [`set_pull_interval`](PullTaskContext::set_pull_interval) -- standing per-queue interval,
//!    remembered across rounds until changed again;
//! 4. none of the above: [`DEFAULT_PULL_NEXT_DELAY_MILLIS`] (= Java's default 200ms).
//!
//! A panicking callback is caught (the round runs on a child task) and reschedules with
//! [`PULL_TASK_EXCEPTION_DELAY_MILLIS`] -- Java's `catch (Throwable) { setPullNextDelayTimeMillis(1000) }`.
//!
//! # Deviations from Java (deliberate)
//!
//! * Java schedules on a `ScheduledThreadPoolExecutor(pullThreadNums = 20)`; here each queue is a
//!   tokio task, so `pullThreadNums` has no meaning and is not ported.
//! * Java is push-driven by the client instance's `RebalanceService` through a
//!   `MessageQueueListener`; this port reconciles the balance view periodically
//!   ([`reconcile_interval_millis`](MQPullConsumerScheduleService::reconcile_interval_millis),
//!   default 20s = the Java rebalance period), once immediately inside
//!   [`start`](MQPullConsumerScheduleService::start), plus on-demand
//!   [`reconcile`](MQPullConsumerScheduleService::reconcile) / manual
//!   [`put_task`](MQPullConsumerScheduleService::put_task).
//! * `do_pull_task` carries an extra `pull_result: Option<PullResult>` argument for signature
//!   parity with the task spec; the scheduler never pulls itself, so it is always `None` today.
//! * [`shutdown`](MQPullConsumerScheduleService::shutdown) **drains**: in-flight callbacks are
//!   awaited (bounded by `SHUTDOWN_FINALIZE_BUDGET`), never aborted mid-flight.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::client::consumer::mq_key;
use crate::client::pull_consumer::DefaultMQPullConsumer;
use crate::client::result::PullResult;
use crate::client::shutdown::{run_finalize_blocking, SHUTDOWN_FINALIZE_BUDGET};
use crate::common::message::MessageQueue;
use crate::error::Result;
use crate::remoting::protocol::heartbeat::MessageModel;
use crate::remoting::protocol::namespace_util::NamespaceUtil;
use crate::remoting::rpchook::RPCHook;
use crate::{rmq_debug, rmq_error, rmq_warn};

/// Default delay to the next round when the callback expressed nothing (Java `pullNextDelayTimeMillis = 200`).
pub const DEFAULT_PULL_NEXT_DELAY_MILLIS: u64 = 200;
/// Java's `catch (Throwable)` branch: `context.setPullNextDelayTimeMillis(1000)`.
pub const PULL_TASK_EXCEPTION_DELAY_MILLIS: u64 = 1_000;
/// Default reconcile period, matching the 20s cadence of the Java `RebalanceService`.
pub const DEFAULT_RECONCILE_INTERVAL_MILLIS: u64 = 20_000;

/// What a [`PullTaskCallback::do_pull_task`] returns: `Some(delay)` is the highest-priority
/// rescheduling channel, `None` defers to the [`PullTaskContext`] channels.
pub type PullTaskFuture = Pin<Box<dyn Future<Output = Option<Duration>> + Send + 'static>>;

/// Per-topic pull task (Java `PullTaskCallback`).
///
/// The callback does the actual pulling through [`PullTaskContext::pull_consumer`] and expresses
/// the next-round delay through the three channels documented on [`PullTaskContext`].
pub trait PullTaskCallback: Send + Sync + 'static {
    /// Runs one round for `mq`. `pull_result` is always `None` in this port (the scheduler never
    /// pulls itself; see the module header). Runs on a child task, so a panic only costs this
    /// round (rescheduled after [`PULL_TASK_EXCEPTION_DELAY_MILLIS`]), never the queue loop.
    fn do_pull_task(
        self: Arc<Self>,
        mq: MessageQueue,
        pull_result: Option<PullResult>,
        context: Arc<PullTaskContext>,
    ) -> PullTaskFuture;
}

/// Convenience adapter: glues a plain closure into a [`PullTaskCallback`]
/// (same role as `producer::ClosureSendCallback`).
pub struct FnPullTaskCallback<F> {
    f: F,
}

impl<F> FnPullTaskCallback<F>
where
    F: Fn(MessageQueue, Option<PullResult>, Arc<PullTaskContext>) -> PullTaskFuture
        + Send
        + Sync
        + 'static,
{
    pub fn new(f: F) -> FnPullTaskCallback<F> {
        FnPullTaskCallback { f }
    }
}

impl<F> PullTaskCallback for FnPullTaskCallback<F>
where
    F: Fn(MessageQueue, Option<PullResult>, Arc<PullTaskContext>) -> PullTaskFuture
        + Send
        + Sync
        + 'static,
{
    fn do_pull_task(
        self: Arc<Self>,
        mq: MessageQueue,
        pull_result: Option<PullResult>,
        context: Arc<PullTaskContext>,
    ) -> PullTaskFuture {
        (self.f)(mq, pull_result, context)
    }
}

/// Mutable state of one round: the standing interval and the one-shot delay.
#[derive(Default)]
struct CtxCell {
    /// Channel 3: standing per-queue interval, carried across rounds by the queue loop.
    interval: Option<Duration>,
    /// Channel 2: one-shot delay for the next round (Java `pullNextDelayTimeMillis`).
    later: Option<Duration>,
}

/// Handed to every [`PullTaskCallback::do_pull_task`] round (Java `PullTaskContext`).
///
/// Fresh per round; the standing interval set through [`set_pull_interval`](Self::set_pull_interval)
/// is carried over to the following rounds of the same queue by the loop.
pub struct PullTaskContext {
    consumer: DefaultMQPullConsumer,
    cell: Mutex<CtxCell>,
}

impl PullTaskContext {
    fn new(consumer: DefaultMQPullConsumer, interval: Option<Duration>) -> PullTaskContext {
        PullTaskContext {
            consumer,
            cell: Mutex::new(CtxCell {
                interval,
                later: None,
            }),
        }
    }

    fn lock_cell(&self) -> MutexGuard<'_, CtxCell> {
        self.cell.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The consumer this service schedules for (= Java `getPullConsumer()`); clones share state.
    pub fn pull_consumer(&self) -> DefaultMQPullConsumer {
        self.consumer.clone()
    }

    /// Channel 3: standing interval for this queue, remembered across rounds.
    pub fn set_pull_interval(&self, interval: Duration) {
        self.lock_cell().interval = Some(interval);
    }

    /// The standing interval currently in effect (without the default fallback).
    pub fn pull_interval(&self) -> Option<Duration> {
        self.lock_cell().interval
    }

    /// Channel 2: one-shot delay for the next round (= Java `setPullNextDelayTimeMillis`).
    pub fn pull_message_later(&self, delay: Duration) {
        self.lock_cell().later = Some(delay);
    }

    /// Java `getPullNextDelayTimeMillis()`: the delay the context channels alone would produce
    /// (the return-value channel is not visible here).
    pub fn pull_next_delay_millis(&self) -> i64 {
        self.resolve(None).as_millis() as i64
    }

    /// Priority: returned `Some` > one-shot `later` > standing `interval` > 200ms default.
    fn resolve(&self, returned: Option<Duration>) -> Duration {
        let cell = self.lock_cell();
        returned
            .or(cell.later)
            .or(cell.interval)
            .unwrap_or_else(|| Duration::from_millis(DEFAULT_PULL_NEXT_DELAY_MILLIS))
    }

    /// The standing interval to carry into the next round.
    fn persistent_interval(&self) -> Option<Duration> {
        self.lock_cell().interval
    }

    /// Java's `catch (Throwable)` branch: force the 1000ms error delay over whatever the
    /// callback had set as its one-shot delay.
    fn mark_exception(&self) {
        self.lock_cell().later = Some(Duration::from_millis(PULL_TASK_EXCEPTION_DELAY_MILLIS));
    }
}

/// One entry of the task table (Java `PullTaskImpl` + its scheduled future).
struct QueueTask {
    topic: String,
    mq: MessageQueue,
    /// `true` = retired by `put_task` (Java `setCancelled(true)`); the loop notices and exits.
    cancel: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

struct ScheduleInner {
    consumer: DefaultMQPullConsumer,
    /// Namespaced topic -> callback (Java `callbackTable`).
    callbacks: RwLock<BTreeMap<String, Arc<dyn PullTaskCallback>>>,
    /// mq key -> live task (Java `taskTable`).
    tasks: Mutex<BTreeMap<String, QueueTask>>,
    /// Handles of tasks retired by `put_task`; drained/aborted at shutdown, pruned when finished.
    retired: Mutex<Vec<JoinHandle<()>>>,
    reconcile_task: Mutex<Option<JoinHandle<()>>>,
    /// `true` = stopping; wakes every sleeping loop (the same signal shape as the pull consumers).
    stop: watch::Sender<bool>,
    started: AtomicBool,
    runtime: OnceLock<tokio::runtime::Handle>,
    reconcile_interval_millis: AtomicU64,
}

impl ScheduleInner {
    fn new(consumer: DefaultMQPullConsumer) -> ScheduleInner {
        ScheduleInner {
            consumer,
            callbacks: RwLock::new(BTreeMap::new()),
            tasks: Mutex::new(BTreeMap::new()),
            retired: Mutex::new(Vec::new()),
            reconcile_task: Mutex::new(None),
            stop: watch::channel(false).0,
            started: AtomicBool::new(false),
            runtime: OnceLock::new(),
            reconcile_interval_millis: AtomicU64::new(DEFAULT_RECONCILE_INTERVAL_MILLIS),
        }
    }

    fn runtime_handle(&self) -> Option<tokio::runtime::Handle> {
        if let Some(handle) = self.runtime.get() {
            return Some(handle.clone());
        }
        let handle = tokio::runtime::Handle::try_current().ok()?;
        let _ = self.runtime.set(handle.clone());
        Some(handle)
    }
}

impl Drop for ScheduleInner {
    /// Forgetting `shutdown()` must not leave queue loops behind (same guard as the consumers).
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(h) = self
            .reconcile_task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            h.abort();
        }
        for (_, task) in std::mem::take(&mut *self.tasks.lock().unwrap_or_else(|e| e.into_inner()))
        {
            task.handle.abort();
        }
        for h in self.retired.lock().unwrap_or_else(|e| e.into_inner()).drain(..) {
            h.abort();
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// `sleep ∥ stop`: `true` = stop signal received, the loop should exit.
async fn wait_or_stop(rx: &mut watch::Receiver<bool>, millis: u64) -> bool {
    if *rx.borrow_and_update() {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(millis)) => false,
        _ = rx.changed() => true,
    }
}

/// The per-queue loop (Java `PullTaskImpl.run` + its reschedule).
///
/// Serial by construction: one iteration = one full `do_pull_task` round + the resolved delay.
async fn queue_loop(
    mq: MessageQueue,
    inner: Arc<ScheduleInner>,
    mut stop: watch::Receiver<bool>,
    mut cancel: watch::Receiver<bool>,
) {
    // The standing interval channel survives across rounds through this local (the context is fresh per round).
    let mut interval: Option<Duration> = None;
    loop {
        if *stop.borrow_and_update() {
            rmq_debug!("pull schedule task stopping: {mq:?}");
            return;
        }
        if *cancel.borrow_and_update() {
            rmq_warn!("The Pull Task is cancelled, {mq:?}");
            return;
        }
        let Some(callback) = inner
            .callbacks
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&mq.topic)
            .cloned()
        else {
            rmq_warn!("Pull Task Callback not exist, {}", mq.topic);
            return;
        };
        let context = Arc::new(PullTaskContext::new(inner.consumer.clone(), interval));
        // The round runs on a child task so that a panicking callback cannot take the loop down
        // (Java catches Throwable here and reschedules after 1000ms).
        let round = tokio::spawn({
            let callback = Arc::clone(&callback);
            let mq = mq.clone();
            let context = Arc::clone(&context);
            async move { callback.do_pull_task(mq, None, context).await }
        });
        let returned = match round.await {
            Ok(returned) => returned,
            Err(join_err) => {
                rmq_error!("doPullTask Exception on {mq:?}: {join_err}");
                context.mark_exception();
                None
            }
        };
        interval = context.persistent_interval();
        let delay = context.resolve(returned);
        if *stop.borrow() || *cancel.borrow() {
            rmq_warn!("The Pull Task is cancelled after doPullTask, {mq:?}");
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = stop.changed() => return,
            _ = cancel.changed() => return,
        }
    }
}

/// Java `MQPullConsumerScheduleService`. Clones share the same state.
#[derive(Clone)]
pub struct MQPullConsumerScheduleService {
    inner: Arc<ScheduleInner>,
}

impl std::fmt::Debug for MQPullConsumerScheduleService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MQPullConsumerScheduleService")
            .field("consumer_group", &self.inner.consumer.consumer_group())
            .field(
                "started",
                &self.inner.started.load(Ordering::Acquire),
            )
            .field(
                "callbacks",
                &self
                    .inner
                    .callbacks
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .len(),
            )
            .field("tasks", &lock(&self.inner.tasks).len())
            .finish()
    }
}

impl MQPullConsumerScheduleService {
    /// Java constructor: owns a fresh [`DefaultMQPullConsumer`] pinned to CLUSTERING.
    pub fn new(consumer_group: &str) -> Result<MQPullConsumerScheduleService> {
        let consumer = DefaultMQPullConsumer::new(consumer_group)?;
        consumer.set_message_model(MessageModel::CLUSTERING);
        Ok(MQPullConsumerScheduleService::from_consumer(consumer))
    }

    /// Java's `(consumerGroup, rpcHook)` constructor.
    pub fn with_rpc_hook(
        consumer_group: &str,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Result<MQPullConsumerScheduleService> {
        let consumer = DefaultMQPullConsumer::with_rpc_hook(consumer_group, rpc_hook)?;
        consumer.set_message_model(MessageModel::CLUSTERING);
        Ok(MQPullConsumerScheduleService::from_consumer(consumer))
    }

    /// Schedule on an existing (typically pre-configured) consumer
    /// (covers Java `setDefaultMQPullConsumer` at construction time).
    pub fn from_consumer(consumer: DefaultMQPullConsumer) -> MQPullConsumerScheduleService {
        MQPullConsumerScheduleService {
            inner: Arc::new(ScheduleInner::new(consumer)),
        }
    }

    /// The consumer being scheduled (= Java `getDefaultMQPullConsumer()`); clones share state.
    pub fn pull_consumer(&self) -> DefaultMQPullConsumer {
        self.inner.consumer.clone()
    }

    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::Acquire)
    }

    // ---------------- Configuration ----------------

    /// How often the reconcile loop re-reads the rebalance view (default 20s, the Java
    /// `RebalanceService` cadence). Also runs once immediately inside [`start`](Self::start)
    /// and on every [`reconcile`](Self::reconcile) call.
    pub fn set_reconcile_interval_millis(&self, millis: u64) {
        self.inner
            .reconcile_interval_millis
            .store(millis, Ordering::Release);
    }

    pub fn reconcile_interval_millis(&self) -> u64 {
        self.inner.reconcile_interval_millis.load(Ordering::Acquire)
    }

    // ---------------- Callback registration ----------------

    /// Java `registerPullTaskCallback(topic, callback)`: the topic is namespaced for the callback
    /// table and registered on the consumer (so heartbeats carry its subscription and the route
    /// gets refreshed). Re-registering overwrites.
    pub fn register_pull_task_callback(&self, topic: &str, callback: Arc<dyn PullTaskCallback>) {
        let namespace = self.inner.consumer.config().namespace;
        let key = if namespace.is_empty() {
            topic.to_string()
        } else {
            NamespaceUtil::wrap_namespace(&namespace, topic)
        };
        self.inner
            .callbacks
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, callback);
        self.inner.consumer.register_topic(topic);
    }

    /// The topics with a registered callback (namespaced keys, sorted).
    pub fn callback_topics(&self) -> Vec<String> {
        self.inner
            .callbacks
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    // ---------------- Lifecycle ----------------

    /// Idempotent. Starts the consumer, reconciles once immediately, then spawns the periodic
    /// reconcile loop. Requires a tokio runtime (like every facade here).
    pub async fn start(&self) -> Result<()> {
        if self
            .inner
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        // Bind the runtime while inside one: put_task from non-async contexts relies on it.
        let _ = self.inner.runtime_handle();
        if let Err(e) = self.inner.consumer.start().await {
            self.inner.started.store(false, Ordering::Release);
            return Err(e);
        }
        let _ = self.inner.stop.send(false);
        self.reconcile().await;
        self.spawn_reconcile_loop();
        rmq_debug!(
            "MQPullConsumerScheduleService start OK, {} {:?}",
            self.inner.consumer.consumer_group(),
            self.callback_topics(),
        );
        Ok(())
    }

    /// Draining shutdown: signals every loop, waits (bounded) for the in-flight callbacks to
    /// finish, then shuts the consumer down. No-op safe before start / after a previous shutdown.
    pub fn shutdown(&self) {
        let _ = self.inner.stop.send(true);
        self.inner.started.store(false, Ordering::Release);
        let mut handles: Vec<JoinHandle<()>> = Vec::new();
        if let Some(h) = lock(&self.inner.reconcile_task).take() {
            handles.push(h);
        }
        handles.extend(std::mem::take(&mut *lock(&self.inner.tasks)).into_values().map(|t| t.handle));
        handles.extend(lock(&self.inner.retired).drain(..));
        if !handles.is_empty() {
            match self.inner.runtime_handle() {
                Some(handle) => {
                    let _ = run_finalize_blocking(
                        &handle,
                        "pull schedule service shutdown",
                        SHUTDOWN_FINALIZE_BUDGET,
                        async move {
                            for h in handles {
                                // Drain, never abort: an in-flight do_pull_task round runs to its end;
                                // the loops exit on the stop signal right after.
                                let _ = h.await;
                            }
                        },
                    );
                }
                None => {
                    rmq_warn!("pull schedule service shutdown: no tokio runtime, aborting tasks");
                }
            }
        }
        self.inner.consumer.shutdown();
    }

    fn spawn_reconcile_loop(&self) {
        let Some(handle) = self.inner.runtime_handle() else {
            rmq_warn!("pull schedule service: no tokio runtime, reconcile loop disabled");
            return;
        };
        let inner = Arc::clone(&self.inner);
        let mut rx = inner.stop.subscribe();
        let task = handle.spawn(async move {
            // Sleep first: start() already reconciled synchronously (same shape as the heartbeat loops).
            while !wait_or_stop(
                &mut rx,
                inner.reconcile_interval_millis.load(Ordering::Acquire),
            )
            .await
            {
                reconcile_round(&inner).await;
            }
        });
        *lock(&self.inner.reconcile_task) = Some(task);
    }

    // ---------------- Task table ----------------

    /// Java `putTask(topic, mqNewSet)`: retire the tasks of `topic` that fell out of the set and
    /// spawn the missing ones. Queues already in the table keep their loop untouched.
    /// Retired loops are cancelled at their next checkpoint (an in-flight round still completes).
    pub fn put_task(&self, topic: &str, mq_new_set: &[MessageQueue]) {
        put_task_on(&self.inner, topic, mq_new_set);
    }

    /// One reconcile round over every registered topic, against the consumer's current view:
    /// CLUSTERING -> `fetch_message_queues_in_balance`, BROADCASTING -> `fetch_subscribe_message_queues`.
    /// A failed round (consumer not started / RPC down) only logs; the task table is kept as-is.
    pub async fn reconcile(&self) {
        reconcile_round(&self.inner).await;
    }

    /// The queues with a live task-table entry, sorted by queue key.
    pub fn task_queues(&self) -> Vec<MessageQueue> {
        lock(&self.inner.tasks)
            .values()
            .map(|t| t.mq.clone())
            .collect()
    }

    /// Whether the queue's loop task is still alive (table entry exists and has not exited).
    pub fn task_alive(&self, mq: &MessageQueue) -> bool {
        lock(&self.inner.tasks)
            .get(&mq_key(mq))
            .is_some_and(|t| !t.handle.is_finished())
    }
}

fn put_task_on(inner: &Arc<ScheduleInner>, topic: &str, mq_new_set: &[MessageQueue]) {
    let new_keys: BTreeSet<String> = mq_new_set.iter().map(mq_key).collect();
    let mut tasks = lock(&inner.tasks);
    // Retire the queues of this topic that are no longer assigned (Java: setCancelled + remove).
    let stale: Vec<String> = tasks
        .iter()
        .filter(|(key, task)| task.topic == topic && !new_keys.contains(*key))
        .map(|(key, _)| key.clone())
        .collect();
    if !stale.is_empty() {
        let mut retired = lock(&inner.retired);
        retired.retain(|h| !h.is_finished());
        for key in stale {
            if let Some(task) = tasks.remove(&key) {
                let _ = task.cancel.send(true);
                // No abort: an in-flight round finishes, the loop exits at its next checkpoint.
                retired.push(task.handle);
            }
        }
    }
    let Some(handle) = inner.runtime_handle() else {
        rmq_warn!("put_task({topic}): no tokio runtime; call start() first");
        return;
    };
    for mq in mq_new_set {
        let key = mq_key(mq);
        if tasks.contains_key(&key) {
            continue;
        }
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let loop_task = handle.spawn(queue_loop(
            mq.clone(),
            Arc::clone(inner),
            inner.stop.subscribe(),
            cancel_rx,
        ));
        tasks.insert(
            key,
            QueueTask {
                topic: topic.to_string(),
                mq: mq.clone(),
                cancel: cancel_tx,
                handle: loop_task,
            },
        );
    }
}

async fn reconcile_round(inner: &Arc<ScheduleInner>) {
    let topics: Vec<String> = inner
        .callbacks
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .keys()
        .cloned()
        .collect();
    let model = inner.consumer.config().message_model;
    for topic in topics {
        let queues = if model == MessageModel::BROADCASTING {
            inner.consumer.fetch_subscribe_message_queues(&topic).await
        } else {
            inner.consumer.fetch_message_queues_in_balance(&topic).await
        };
        match queues {
            Ok(queues) => put_task_on(inner, &topic, &queues),
            Err(e) => {
                rmq_debug!("pull schedule reconcile: queue fetch for {topic} failed: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // In-process fake cluster (the same frame discipline as consumer.rs `AssignCluster` /
    // pull_consumer.rs `FakeBroker`): a namesrv answering routes with a live queue count and a
    // broker answering GET_CONSUMER_LIST_BY_GROUP; everything else is SUCCESS.
    use std::sync::atomic::AtomicI32;
    use std::time::Instant;

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{TcpListener, TcpStream};

    use crate::common::sysflag::PermName;
    use crate::remoting::protocol::body::GetConsumerListByGroupResponseBody;
    use crate::remoting::protocol::codes::{request_code, response_code};
    use crate::remoting::protocol::remoting_command::RemotingCommand;
    use crate::remoting::protocol::route::{BrokerData, QueueData, TopicRouteData};

    fn queue(topic: &str, broker: &str, id: i32) -> MessageQueue {
        MessageQueue::new(topic, broker, id)
    }

    fn lockm<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock().unwrap_or_else(|e| e.into_inner())
    }

    // ---------------------------------------------------------- test callback

    /// Records every round: (queue key, start instant). Optional per-round behaviour via closures.
    #[derive(Default)]
    struct Recorder {
        calls: Mutex<Vec<(String, Instant)>>,
        /// Per-queue in-flight depth and its high-water mark (the "same queue serial" proof).
        in_flight: Mutex<BTreeMap<String, (usize, usize)>>,
        started_at: Mutex<Option<Instant>>,
    }

    impl Recorder {
        fn call_keys(&self) -> Vec<String> {
            lockm(&self.calls).iter().map(|(k, _)| k.clone()).collect()
        }

        fn call_count(&self) -> usize {
            lockm(&self.calls).len()
        }

        fn count_for(&self, key: &str) -> usize {
            lockm(&self.calls).iter().filter(|(k, _)| k == key).count()
        }

        fn max_depth_for(&self, key: &str) -> usize {
            lockm(&self.in_flight).get(key).map(|(_, max)| *max).unwrap_or(0)
        }

        fn enter(&self, key: &str) {
            let mut flights = lockm(&self.in_flight);
            let slot = flights.entry(key.to_string()).or_insert((0, 0));
            slot.0 += 1;
            slot.1 = slot.1.max(slot.0);
            let mut started = lockm(&self.started_at);
            if started.is_none() {
                *started = Some(Instant::now());
            }
            lockm(&self.calls).push((key.to_string(), Instant::now()));
        }

        fn exit(&self, key: &str) {
            if let Some(slot) = lockm(&self.in_flight).get_mut(key) {
                slot.0 -= 1;
            }
        }

        fn elapsed_since_first_call(&self) -> Duration {
            lockm(&self.started_at)
                .map(|t| t.elapsed())
                .unwrap_or_default()
        }
    }

    /// A callback whose every round just records and then runs `action`.
    fn recording(
        rec: Arc<Recorder>,
        action: impl Fn(MessageQueue, Arc<PullTaskContext>) -> PullTaskFuture + Send + Sync + 'static,
    ) -> Arc<dyn PullTaskCallback> {
        Arc::new(FnPullTaskCallback::new(
            move |mq: MessageQueue, pull_result: Option<PullResult>, ctx: Arc<PullTaskContext>| {
                assert!(
                    pull_result.is_none(),
                    "this port never hands a pull_result to the callback"
                );
                rec.enter(&mq_key(&mq));
                let rec = Arc::clone(&rec);
                let key = mq_key(&mq);
                let fut = action(mq, ctx);
                Box::pin(async move {
                    let out = fut.await;
                    rec.exit(&key);
                    out
                })
            },
        ))
    }

    /// Poll until the condition holds (at most 5s); panic on failure so a fixed sleep cannot mask a race.
    async fn wait_until(mut cond: impl FnMut() -> bool, what: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("wait timed out: {what}");
    }

    fn service_with_recorder(
        group: &str,
        topic: &str,
        rec: Arc<Recorder>,
        action: impl Fn(MessageQueue, Arc<PullTaskContext>) -> PullTaskFuture + Send + Sync + 'static,
    ) -> MQPullConsumerScheduleService {
        let consumer = DefaultMQPullConsumer::new(group).expect("legal group");
        let svc = MQPullConsumerScheduleService::from_consumer(consumer);
        svc.register_pull_task_callback(topic, recording(rec, action));
        svc
    }

    // ---------------------------------------------------------- context channels (pure)

    #[test]
    fn context_priority_is_return_then_later_then_interval_then_default() {
        let consumer = DefaultMQPullConsumer::new("PG_CtxPrio").unwrap();
        // Channel 1 beats everything
        let ctx = PullTaskContext::new(consumer.clone(), Some(Duration::from_millis(700)));
        ctx.set_pull_interval(Duration::from_millis(700));
        ctx.pull_message_later(Duration::from_millis(500));
        assert_eq!(ctx.resolve(Some(Duration::from_millis(300))), Duration::from_millis(300));
        // Channel 2 beats the standing interval
        assert_eq!(ctx.resolve(None), Duration::from_millis(500));
        assert_eq!(ctx.pull_next_delay_millis(), 500, "Java getter shape");
        // Channel 3 alone
        let ctx2 = PullTaskContext::new(consumer.clone(), Some(Duration::from_millis(700)));
        assert_eq!(ctx2.resolve(None), Duration::from_millis(700));
        assert_eq!(ctx2.pull_interval(), Some(Duration::from_millis(700)));
        // Nothing set: the Java default 200ms
        let ctx3 = PullTaskContext::new(consumer.clone(), None);
        assert_eq!(ctx3.resolve(None), Duration::from_millis(DEFAULT_PULL_NEXT_DELAY_MILLIS));
        assert_eq!(ctx3.pull_next_delay_millis(), 200);
        // The Throwable branch overwrites the one-shot channel with 1000ms
        ctx3.pull_message_later(Duration::from_millis(50));
        ctx3.mark_exception();
        assert_eq!(ctx3.resolve(None), Duration::from_millis(PULL_TASK_EXCEPTION_DELAY_MILLIS));
        // The consumer is reachable from the context (Java getPullConsumer)
        assert_eq!(ctx3.pull_consumer().consumer_group(), "PG_CtxPrio");
    }

    // ---------------------------------------------------------- task table expansion

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_task_expands_per_queue_and_retires_the_lost_ones() {
        let rec = Arc::new(Recorder::default());
        let svc = service_with_recorder("PG_SchedExpand", "T", Arc::clone(&rec), |_, _| {
            Box::pin(async { Some(Duration::from_millis(30)) })
        });
        let q = |id| queue("T", "broker-a", id);
        svc.put_task("T", &[q(0), q(1), q(2)]);
        wait_until(|| svc.task_queues().len() == 3, "three queue tasks").await;
        wait_until(
            || rec.count_for(&mq_key(&q(0))) >= 1
                && rec.count_for(&mq_key(&q(1))) >= 1
                && rec.count_for(&mq_key(&q(2))) >= 1,
            "every queue got its own loop",
        )
        .await;
        // Idempotent re-put: same set -> no respawn, table unchanged
        let before = rec.call_count();
        svc.put_task("T", &[q(0), q(1), q(2)]);
        assert_eq!(svc.task_queues().len(), 3);
        // Shrink: 1 and 2 are retired, 0 keeps running
        svc.put_task("T", &[q(0)]);
        assert_eq!(svc.task_queues(), vec![q(0)]);
        // Let a possibly in-flight round on the retired queue finish first (drain, not abort),
        // then the count must freeze while the kept queue keeps pulling.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let at_shrink = rec.count_for(&mq_key(&q(2)));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            rec.count_for(&mq_key(&q(2))),
            at_shrink,
            "a retired queue must stop pulling"
        );
        assert!(rec.count_for(&mq_key(&q(0))) > before, "the kept queue keeps running");
        assert!(svc.task_alive(&q(0)));
        svc.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing_callback_ends_the_task_like_java() {
        let consumer = DefaultMQPullConsumer::new("PG_SchedNoCb").unwrap();
        let svc = MQPullConsumerScheduleService::from_consumer(consumer);
        let q = queue("T", "broker-a", 0);
        // put_task before any callback: the loop exits on Java's "Pull Task Callback not exist" branch
        svc.put_task("T", std::slice::from_ref(&q));
        wait_until(|| !svc.task_alive(&q), "the loop exits without a callback").await;
        // Java keeps the dead entry in taskTable, so re-putting the same set does NOT respawn;
        // the queue revives once it leaves the set and comes back (same semantics here).
        svc.register_pull_task_callback(
            "T",
            recording(Arc::new(Recorder::default()), |_, _| {
                Box::pin(async { Some(Duration::from_millis(30)) })
            }),
        );
        assert_eq!(svc.callback_topics(), vec!["T".to_string()]);
        svc.put_task("T", std::slice::from_ref(&q));
        assert!(!svc.task_alive(&q), "a dead entry is not respawned by an unchanged set (Java taskTable semantics)");
        let rec = Arc::new(Recorder::default());
        svc.register_pull_task_callback(
            "T",
            recording(Arc::clone(&rec), |_, _| {
                Box::pin(async { Some(Duration::from_millis(30)) })
            }),
        );
        svc.put_task("T", &[]); // retire the dead entry
        svc.put_task("T", std::slice::from_ref(&q)); // respawn
        wait_until(|| rec.call_count() >= 1, "the revived loop pulls").await;
        svc.shutdown();
    }

    // ---------------------------------------------------------- delays and priorities

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn return_value_beats_a_long_standing_interval() {
        let rec = Arc::new(Recorder::default());
        // Standing interval 30s, but the return value forces 30ms: rounds must keep coming.
        let svc = service_with_recorder("PG_SchedPrio", "T", Arc::clone(&rec), |_, ctx| {
            ctx.set_pull_interval(Duration::from_secs(30));
            Box::pin(async { Some(Duration::from_millis(30)) })
        });
        svc.put_task("T", &[queue("T", "broker-a", 0)]);
        wait_until(|| rec.call_count() >= 3, "return value overrides the interval").await;
        assert!(
            rec.elapsed_since_first_call() < Duration::from_secs(5),
            "three rounds inside the wait budget prove the 30s interval lost"
        );
        svc.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn standing_interval_survives_across_rounds_and_delays_expire() {
        let rec = Arc::new(Recorder::default());
        // Only the standing interval is set (channel 3): 120ms cadence, remembered per queue.
        let svc = service_with_recorder("PG_SchedInterval", "T", Arc::clone(&rec), |_, ctx| {
            if ctx.pull_interval().is_none() {
                ctx.set_pull_interval(Duration::from_millis(120));
            }
            Box::pin(async { None })
        });
        let q = queue("T", "broker-a", 0);
        svc.put_task("T", std::slice::from_ref(&q));
        wait_until(|| rec.call_count() >= 3, "interval-driven rounds").await;
        let stamps: Vec<Instant> = lockm(&rec.calls).iter().map(|(_, t)| *t).collect();
        let gaps: Vec<Duration> = stamps.windows(2).map(|w| w[1] - w[0]).collect();
        assert!(
            gaps.windows(2).all(|g| g[1] >= Duration::from_millis(100)),
            "every gap honours the standing interval: {gaps:?}"
        );
        // pull_message_later (channel 2) delays exactly the next round, then the interval resumes
        let rec2 = Arc::new(Recorder::default());
        let once = Arc::new(AtomicBool::new(false));
        let svc2 = service_with_recorder("PG_SchedLater", "T", Arc::clone(&rec2), move |_, ctx| {
            let once = Arc::clone(&once);
            if !once.swap(true, Ordering::AcqRel) {
                ctx.pull_message_later(Duration::from_millis(400));
            }
            ctx.set_pull_interval(Duration::from_millis(30));
            Box::pin(async { None })
        });
        svc2.put_task("T", std::slice::from_ref(&q));
        wait_until(|| rec2.call_count() >= 4, "later only delays one round").await;
        let stamps: Vec<Instant> = lockm(&rec2.calls).iter().map(|(_, t)| *t).collect();
        let first_gap = stamps[1] - stamps[0];
        assert!(
            first_gap >= Duration::from_millis(380),
            "pull_message_later(400ms) delayed the second round: {first_gap:?}"
        );
        let second_gap = stamps[2] - stamps[1];
        assert!(
            second_gap < Duration::from_millis(250),
            "the third round is back on the 30ms standing interval: {second_gap:?}"
        );
        svc.shutdown();
        svc2.shutdown();
    }

    // ---------------------------------------------------------- serialization

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn same_queue_is_serial_while_queues_run_in_parallel() {
        let rec = Arc::new(Recorder::default());
        let svc = service_with_recorder("PG_SchedSerial", "T", Arc::clone(&rec), |_mq, _| {
            Box::pin(async move {
                // A slow round: an overlapping second round on the same queue would show up as depth 2.
                tokio::time::sleep(Duration::from_millis(80)).await;
                Some(Duration::ZERO)
            })
        });
        let q0 = queue("T", "broker-a", 0);
        let q1 = queue("T", "broker-a", 1);
        svc.put_task("T", &[q0.clone(), q1.clone()]);
        wait_until(
            || rec.count_for(&mq_key(&q0)) >= 2 && rec.count_for(&mq_key(&q1)) >= 2,
            "both queues ran several rounds",
        )
        .await;
        assert_eq!(rec.max_depth_for(&mq_key(&q0)), 1, "queue 0 never overlaps itself");
        assert_eq!(rec.max_depth_for(&mq_key(&q1)), 1, "queue 1 never overlaps itself");
        svc.shutdown();
    }

    // ---------------------------------------------------------- panic isolation

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn panicking_callback_costs_one_round_not_the_loop() {
        let rec = Arc::new(Recorder::default());
        let first = Arc::new(AtomicBool::new(true));
        let svc = service_with_recorder("PG_SchedPanic", "T", Arc::clone(&rec), move |_, _| {
            let first = Arc::clone(&first);
            Box::pin(async move {
                if first.swap(false, Ordering::AcqRel) {
                    panic!("doPullTask boom");
                }
                Some(Duration::from_millis(30))
            })
        });
        svc.put_task("T", &[queue("T", "broker-a", 0)]);
        // Round 1 panics -> the Java Throwable branch reschedules after 1000ms -> round 2 arrives.
        wait_until(|| rec.call_count() >= 2, "the loop survived the panic").await;
        svc.shutdown();
    }

    // ---------------------------------------------------------- shutdown drain

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_drains_the_in_flight_callback() {
        let rec = Arc::new(Recorder::default());
        let gate = Arc::new(tokio::sync::Notify::new());
        let entered = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let gate_cb = Arc::clone(&gate);
        let entered_cb = Arc::clone(&entered);
        let done_cb = Arc::clone(&done);
        let svc = service_with_recorder("PG_SchedDrain", "T", Arc::clone(&rec), move |_, _| {
            let gate = Arc::clone(&gate_cb);
            let entered = Arc::clone(&entered_cb);
            let done = Arc::clone(&done_cb);
            Box::pin(async move {
                entered.store(true, Ordering::Release);
                gate.notified().await; // hold the round open until the test releases it
                done.store(true, Ordering::Release);
                None
            })
        });
        svc.put_task("T", &[queue("T", "broker-a", 0)]);
        wait_until(|| entered.load(Ordering::Acquire), "callback in flight").await;
        // Open the gate shortly AFTER shutdown began: shutdown must still be waiting (draining).
        let opener = tokio::spawn({
            let gate = Arc::clone(&gate);
            async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                gate.notify_waiters();
            }
        });
        svc.shutdown();
        opener.await.unwrap();
        assert!(done.load(Ordering::Acquire), "shutdown returned only after the in-flight round completed");
        assert!(!svc.is_started());
        let calls = rec.call_count();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(rec.call_count(), calls, "no rounds after shutdown");
        // Idempotent
        svc.shutdown();
    }

    #[test]
    fn shutdown_before_start_is_a_noop() {
        let svc = MQPullConsumerScheduleService::new("PG_SchedNoStart").unwrap();
        svc.shutdown();
        assert!(!svc.is_started());
    }

    // ---------------------------------------------------------- fake cluster (rebalance view)

    const MAX_FRAME: i32 = 20 * 1024 * 1024;

    struct FakeCluster {
        namesrv_addr: String,
        read_queues: Arc<AtomicI32>,
        cid_list: Arc<Mutex<Vec<String>>>,
        tasks: Vec<JoinHandle<()>>,
    }

    impl FakeCluster {
        async fn start(read_queues: i32) -> FakeCluster {
            let broker = TcpListener::bind("127.0.0.1:0").await.expect("bind fake broker");
            let broker_addr = broker.local_addr().expect("broker addr").to_string();
            let namesrv = TcpListener::bind("127.0.0.1:0").await.expect("bind fake namesrv");
            let namesrv_addr = namesrv.local_addr().expect("namesrv addr").to_string();
            let read_queues = Arc::new(AtomicI32::new(read_queues));
            let cid_list = Arc::new(Mutex::new(Vec::<String>::new()));
            let ns_task = spawn_namesrv(namesrv, broker_addr, Arc::clone(&read_queues));
            let broker_task = spawn_broker(broker, Arc::clone(&cid_list));
            FakeCluster {
                namesrv_addr,
                read_queues,
                cid_list,
                tasks: vec![ns_task, broker_task],
            }
        }

        fn scale_to(&self, n: i32) {
            self.read_queues.store(n, Ordering::Release);
        }

        fn set_cid_list(&self, cids: &[&str]) {
            *lockm(&self.cid_list) = cids.iter().map(|s| s.to_string()).collect();
        }
    }

    impl Drop for FakeCluster {
        fn drop(&mut self) {
            for task in self.tasks.drain(..) {
                task.abort();
            }
        }
    }

    async fn read_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
        let mut len_buf = [0_u8; 4];
        stream.read_exact(&mut len_buf).await.ok()?;
        let total = i32::from_be_bytes(len_buf);
        if total <= 0 || total > MAX_FRAME {
            return None;
        }
        let mut frame = vec![0_u8; 4 + total as usize];
        frame[..4].copy_from_slice(&len_buf);
        stream.read_exact(&mut frame[4..]).await.ok()?;
        Some(frame)
    }

    async fn write_frame(stream: &mut TcpStream, response: &mut RemotingCommand) {
        let bytes = response.encode();
        let _ = stream.write_all(&bytes).await;
        let _ = stream.flush().await;
    }

    fn answer_for(request: &RemotingCommand, code: i32) -> RemotingCommand {
        let mut response = RemotingCommand::create_response(code, Some("mock".to_string()));
        response.opaque = request.opaque;
        response.serialize_type_current_rpc = request.serialize_type_current_rpc;
        response
    }

    fn route_json(broker_addr: &str, read_queues: i32) -> Vec<u8> {
        let route = TopicRouteData {
            queue_datas: vec![QueueData::new(
                "broker-a".to_string(),
                read_queues,
                read_queues,
                PermName::PERM_READ | PermName::PERM_WRITE,
                0,
            )],
            broker_datas: vec![BrokerData::new(
                "MockCluster",
                "broker-a".to_string(),
                vec![(i64::from(crate::common::mix_all::MixAll::MASTER_ID), broker_addr.to_string())],
                "",
            )],
            ..Default::default()
        };
        serde_json::to_vec(&route.to_json_value()).expect("route must serialize")
    }

    fn spawn_namesrv(
        listener: TcpListener,
        broker_addr: String,
        read_queues: Arc<AtomicI32>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { return };
                let addr = broker_addr.clone();
                let read_queues = Arc::clone(&read_queues);
                tokio::spawn(async move {
                    while let Some(frame) = read_frame(&mut stream).await {
                        let Ok(request) = RemotingCommand::decode(&frame) else { return };
                        let mut response = if request.code == request_code::GET_ROUTEINFO_BY_TOPIC {
                            let mut r = answer_for(&request, response_code::SUCCESS);
                            let n = read_queues.load(Ordering::Acquire);
                            r.set_body(Some(route_json(&addr, n)));
                            r
                        } else {
                            answer_for(&request, response_code::SUCCESS)
                        };
                        write_frame(&mut stream, &mut response).await;
                    }
                });
            }
        })
    }

    fn spawn_broker(listener: TcpListener, cid_list: Arc<Mutex<Vec<String>>>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { return };
                let cid_list = Arc::clone(&cid_list);
                tokio::spawn(async move {
                    while let Some(frame) = read_frame(&mut stream).await {
                        let Ok(request) = RemotingCommand::decode(&frame) else { return };
                        let mut response = answer_for(&request, response_code::SUCCESS);
                        if request.code == request_code::GET_CONSUMER_LIST_BY_GROUP {
                            let body = GetConsumerListByGroupResponseBody {
                                consumer_id_list: lockm(&cid_list).clone(),
                            };
                            response.set_body(Some(body.encode()));
                        }
                        write_frame(&mut stream, &mut response).await;
                    }
                });
            }
        })
    }

    /// Start a consumer + schedule service against the fake cluster; the group holds only this
    /// instance, so the balance view is every read queue of the route.
    async fn started_service_on_cluster(
        instance: &str,
        group: &str,
        cluster: &FakeCluster,
    ) -> (DefaultMQPullConsumer, MQPullConsumerScheduleService) {
        let consumer = DefaultMQPullConsumer::new(group).expect("legal group");
        consumer.set_instance_name(instance);
        consumer.set_namesrv_addr(&cluster.namesrv_addr);
        // Fast route refresh so the scale-out case converges inside the test budget.
        consumer.set_poll_name_server_interval_millis(300);
        consumer.register_topic("T");
        consumer.start().await.expect("start on the fake cluster");
        cluster.set_cid_list(&[&consumer.client_id()]);
        let svc = MQPullConsumerScheduleService::from_consumer(consumer.clone());
        (consumer, svc)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconcile_expands_tasks_from_the_clustering_balance_view() {
        let cluster = FakeCluster::start(4).await;
        let (_consumer, svc) =
            started_service_on_cluster("sched_expand", "PG_SchedView", &cluster).await;
        let rec = Arc::new(Recorder::default());
        svc.register_pull_task_callback(
            "T",
            recording(Arc::clone(&rec), |_, _| {
                Box::pin(async { Some(Duration::from_millis(50)) })
            }),
        );
        svc.reconcile().await;
        let tasks = svc.task_queues();
        assert_eq!(tasks.len(), 4, "single instance in the group -> all 4 read queues");
        assert!(
            tasks.iter().all(|mq| mq.topic == "T" && mq.broker_name == "broker-a"),
            "tasks carry the routed queues: {tasks:?}"
        );
        wait_until(|| rec.call_keys().len() >= 4, "every expanded queue pulls").await;
        svc.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn start_and_periodic_reconcile_follow_the_route_and_the_group() {
        let cluster = FakeCluster::start(2).await;
        let (consumer, svc) =
            started_service_on_cluster("sched_loop", "PG_SchedLoop", &cluster).await;
        let rec = Arc::new(Recorder::default());
        svc.register_pull_task_callback(
            "T",
            recording(Arc::clone(&rec), |_, _| {
                Box::pin(async { Some(Duration::from_millis(50)) })
            }),
        );
        svc.set_reconcile_interval_millis(300);
        // start() reconciles immediately, and the periodic loop must then pick the route
        // scale-out up without further help.
        svc.start().await.expect("service start on the fake cluster");
        wait_until(|| svc.task_queues().len() == 2, "the immediate round expands to 2").await;
        cluster.scale_to(5);
        wait_until(|| svc.task_queues().len() == 5, "the reconcile loop follows the route").await;
        // A second instance joins the group: the AVG strategy shrinks this instance's share
        // (5 queues over 2 sorted clients -> 2 or 3 for this one, never all 5).
        let other = format!("{}-other", consumer.client_id());
        cluster.set_cid_list(&[&consumer.client_id(), &other]);
        wait_until(
            || {
                let n = svc.task_queues().len();
                n == 2 || n == 3
            },
            "the share shrinks when another consumer joins",
        )
        .await;
        svc.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn broadcasting_reconcile_takes_every_queue_without_the_consumer_list() {
        let cluster = FakeCluster::start(3).await;
        let (consumer, svc) =
            started_service_on_cluster("sched_bcast", "PG_SchedBcast", &cluster).await;
        consumer.set_message_model(MessageModel::BROADCASTING);
        let rec = Arc::new(Recorder::default());
        svc.register_pull_task_callback(
            "T",
            recording(Arc::clone(&rec), |_, _| {
                Box::pin(async { Some(Duration::from_millis(50)) })
            }),
        );
        // No cid list on purpose: broadcasting never asks the broker for the group.
        svc.reconcile().await;
        assert_eq!(svc.task_queues().len(), 3, "broadcasting takes all read queues");
        svc.shutdown();
    }
}
