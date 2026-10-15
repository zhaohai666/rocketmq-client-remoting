//! * The queue is **unbounded**, so the "threads beyond core" path in `execute` can never trigger
//!   (only `poolSize < corePoolSize` spawns a thread, otherwise the task is queued), **the real concurrency == corePoolSize**;
//! * `AbstractConsumeMessageService#updateCorePoolSize` → `ThreadPoolExecutor#setCorePoolSize`
//!   -- what changes at runtime is exactly this concurrency, and `getCorePoolSize()` reads it back, so
//!   `update_core_pool_size` / `get_core_pool_size` (on the `consumer.rs` side) carry real semantics.
//!
//!
//!
//! # The seam left for `consumer.rs`
//! This module **knows nothing** about messages, listeners, or `MQClientInstance`, so it needs no consumer-side trait:
//! the only seam is [`ConsumeTask`] (= `Pin<Box<dyn Future<Output = ()> + Send>>`,
//! a self-contained async closure). The seam for consume callbacks
//! is already defined in [`super::result`] --
//! [`MessageListenerConcurrently`](super::result::MessageListenerConcurrently) and
//! [`MessageListenerOrderly`](super::result::MessageListenerOrderly), both `Send + Sync`
//! object-safe traits that this module deliberately does not redefine (reuse rule). What `consumer.rs` writes is
//! the submission of POP batch tasks (pack `batch/pq/mq` into a task and `submit`):
//!
//! ```text
//! executor.submit(Box::pin(async move { consume_pop_batch(listener, client, batch, pq, mq).await }))
//! ```

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::Notify;

use crate::error::{Error, Result};
use crate::{bail, rmq_debug, rmq_error, rmq_warn};

pub const DEFAULT_KEEP_ALIVE: Duration = Duration::from_secs(60);

pub const DEFAULT_THREAD_NAME_PREFIX: &str = "rmq-consume";

pub const SHORT_MAX_VALUE: i32 = 32767;

/// One task handed to the executor.
///
/// This is the module's **only outward seam**: futures are inherently lazy, so "construction" (evaluating the arguments) and
/// "execution" (a worker picks up the task) are naturally separate points in time. Usage: `executor.submit(Box::pin(async move { .. }))`.
pub type ConsumeTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Takes the lock.
///
/// Used as-is when poisoned: the pool state is only counters and a queue, a panicked holder cannot leave half-consistent invariants,
/// and losing the whole consume pool outright is the bigger problem.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// Pool state: core/max concurrency, task queue, live worker count, and so on.
struct State {
    core: i32,
    max: i32,
    queue: VecDeque<ConsumeTask>,
    workers: i32,
    /// Thread-name sequence number, name format `<prefix>-<seq>`, incremented on every spawn
    seq: u64,
    /// `self._shutdown`
    shutdown: bool,
}

impl State {
    fn plan_spawn_locked(&mut self, prefix: &str) -> String {
        self.workers += 1;
        let name = format!("{prefix}-{}", self.seq);
        self.seq += 1;
        name
    }

    /// Accounts for a worker leaving (the roster-removal accounting; see difference #6).
    ///
    /// **The caller must already hold the lock, and the "should this worker exit" check and this accounting must sit in the same critical section**:
    /// otherwise two over-core workers timing out at once would also deduct a core worker.
    fn retire_locked(&mut self, name: &str) {
        self.workers -= 1;
        rmq_debug!(
            "consume executor worker {name} retired, alive={} queued={}",
            self.workers,
            self.queue.len()
        );
    }
}

/// Hand-written: the queued [`ConsumeTask`] is a `dyn Future` without `Debug`, so only observable counters are reported.
impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("State")
            .field("core", &self.core)
            .field("max", &self.max)
            .field("workers", &self.workers)
            .field("queued", &self.queue.len())
            .field("seq", &self.seq)
            .field("shutdown", &self.shutdown)
            .finish()
    }
}

struct Inner {
    state: Mutex<State>,
    /// Condition variable: wakes idle workers on new tasks / shutdown.
    work_available: Notify,
    /// Wakes [`ConsumeExecutor::await_termination`] when the last worker leaves.
    retired: Notify,
    /// `self._handler_exceptions`: count of swallowed task exceptions.
    handler_exceptions: AtomicUsize,
    keep_alive: Duration,
    prefix: String,
    /// The handle injected via [`ConsumeExecutor::with_handle`]; `None` means `Handle::try_current()` is fetched each time.
    pinned: Mutex<Option<Handle>>,
}

/// `Clone` shares the same pool (internally an `Arc`): `consumer.rs`
/// can simply clone it into each POP loop instead of wrapping it in another `Arc<ConsumeExecutor>`.
#[derive(Clone)]
pub struct ConsumeExecutor {
    inner: Arc<Inner>,
}

impl fmt::Debug for ConsumeExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = lock(&self.inner.state);
        f.debug_struct("ConsumeExecutor")
            .field("core_pool_size", &state.core)
            .field("maximum_pool_size", &state.max)
            .field("workers", &state.workers)
            .field("queued", &state.queue.len())
            .field("keep_alive", &self.inner.keep_alive)
            .field("thread_name_prefix", &self.inner.prefix)
            .finish_non_exhaustive()
    }
}

impl ConsumeExecutor {
    /// keep_alive 60s, thread name prefix `rmq-consume` (default parameters).
    pub fn new(core_pool_size: i32, maximum_pool_size: i32) -> ConsumeExecutor {
        ConsumeExecutor::with_params(
            core_pool_size,
            maximum_pool_size,
            DEFAULT_KEEP_ALIVE,
            DEFAULT_THREAD_NAME_PREFIX,
        )
    }

    /// Constructor with all parameters given explicitly (the shape actually used by the POP path).
    pub fn with_params(
        core_pool_size: i32,
        maximum_pool_size: i32,
        keep_alive: Duration,
        thread_name_prefix: impl Into<String>,
    ) -> ConsumeExecutor {
        let core = core_pool_size.max(0);
        ConsumeExecutor {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    core,
                    max: core.max(maximum_pool_size),
                    queue: VecDeque::new(),
                    workers: 0,
                    seq: 0,
                    shutdown: false,
                }),
                work_available: Notify::new(),
                retired: Notify::new(),
                handler_exceptions: AtomicUsize::new(0),
                keep_alive,
                prefix: thread_name_prefix.into(),
                pinned: Mutex::new(None),
            }),
        }
    }

    /// Always builds workers on a fixed runtime (same accounting as `ConsumerStatsManager::start_with_handle`).
    ///
    /// When unset, `Handle::try_current()` is fetched each time; when `MQClientInstance` carries its own dedicated runtime,
    /// injecting it is what guarantees the consume workers share a pool with the transport layer.
    #[must_use]
    pub fn with_handle(self, handle: &Handle) -> ConsumeExecutor {
        *lock(&self.inner.pinned) = Some(handle.clone());
        self
    }

    // ---------------- public API ----------------

    /// There are only two failures: the pool is shut down (the error text is verified verbatim) and no tokio
    /// runtime available (difference #1). The task itself has no result, so `Ok(())` only means "enqueued".
    pub fn submit(&self, task: ConsumeTask) -> Result<()> {
        let handle = self.resolve_handle()?;
        self.submit_with(&handle, task)
    }

    /// Explicit-handle variant of [`ConsumeExecutor::submit`], for callers outside async contexts (standalone std threads and the like).
    pub fn submit_with(&self, handle: &Handle, task: ConsumeTask) -> Result<()> {
        let spawn = {
            let mut state = lock(&self.inner.state);
            if state.shutdown {
                // error text verified verbatim
                bail!("ConsumeExecutor has been shut down");
            }
            state.queue.push_back(task);
            if state.workers < state.core || state.workers == 0 {
                Some(state.plan_spawn_locked(&self.inner.prefix))
            } else {
                None
            }
        };
        if let Some(name) = spawn {
            spawn_worker(&self.inner, handle, name);
        }
        // this is the wake-everyone path (see difference #4)
        self.inner.work_available.notify_waiters();
        Ok(())
    }

    /// ```text
    /// int delta = corePoolSize - this.corePoolSize;
    /// this.corePoolSize = corePoolSize;
    /// if (workerCountOf(ctl.get()) > corePoolSize) interruptIdleWorkers();
    /// else if (delta > 0) {
    ///     int k = Math.min(delta, workQueue.size());
    ///     while (k-- > 0 && addWorker(null, true)) { if (workQueue.isEmpty()) break; }
    /// }
    /// ```
    ///
    /// That is: when core grows, **top up** workers by `min(delta, queue length)` (not "match the queue length"),
    /// and when core shrinks, only over-core idle workers go through the keep_alive exit path (running tasks are not interrupted).
    /// The accounting (`workers` raised) completes before returning, so `worker_count()` is readable at once.
    ///
    /// `n < 0` returns `Err` (the error text is verified verbatim).
    pub fn set_core_pool_size(&self, n: i32) -> Result<()> {
        let handle = self.resolve_handle()?;
        self.set_core_pool_size_with(&handle, n)
    }

    /// Explicit-handle variant of [`ConsumeExecutor::set_core_pool_size`].
    pub fn set_core_pool_size_with(&self, handle: &Handle, n: i32) -> Result<()> {
        if n < 0 {
            // error text verified verbatim
            bail!("core pool size must be >= 0");
        }
        let spawned = {
            let mut state = lock(&self.inner.state);
            let delta = n - state.core;
            state.core = n;
            if n > state.max {
                state.max = n;
            }
            let mut spawned = Vec::new();
            if delta > 0 && !state.shutdown {
                let mut k = delta.min(state.queue.len() as i32);
                while k > 0 && state.workers < state.max {
                    spawned.push(state.plan_spawn_locked(&self.inner.prefix));
                    k -= 1;
                    if state.queue.is_empty() {
                        break;
                    }
                }
            }
            spawned
        };
        for name in spawned {
            spawn_worker(&self.inner, handle, name);
        }
        // setCorePoolSize itself does not notify (fresh threads look at the queue themselves); this extra wake-up
        // lets idle workers pick up tasks at once even when "core rose but max blocked any new thread".
        self.inner.work_available.notify_waiters();
        Ok(())
    }

    pub fn get_core_pool_size(&self) -> i32 {
        lock(&self.inner.state).core
    }

    /// Under an unbounded queue it is only the upper bound of the top-up path; normally unreachable.
    pub fn get_max_pool_size(&self) -> i32 {
        lock(&self.inner.state).max
    }

    pub fn worker_count(&self) -> i32 {
        lock(&self.inner.state).workers
    }

    pub fn queued_count(&self) -> usize {
        lock(&self.inner.state).queue.len()
    }

    /// Count of swallowed task exceptions (for observation/tests only).
    pub fn handler_exception_count(&self) -> usize {
        self.inner.handler_exceptions.load(Ordering::Acquire)
    }

    pub fn shutdown(&self) {
        let first_time = {
            let mut state = lock(&self.inner.state);
            let first_time = !state.shutdown;
            state.shutdown = true;
            first_time
        };
        // wake everyone
        self.inner.work_available.notify_waiters();
        if !first_time {
            rmq_debug!("consume executor already shut down, nothing to do");
        }
    }

    /// Waits for all workers to exit.
    pub async fn await_termination(&self) {
        loop {
            // Register the wake-up ticket before reading the count: "the last worker exits right now" cannot lose the notification
            let notified = self.inner.retired.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if lock(&self.inner.state).workers == 0 {
                return;
            }
            notified.await;
        }
    }

    pub async fn shutdown_gracefully(&self) {
        self.shutdown();
        self.await_termination().await;
    }

    // ---------------- internals ----------------

    fn resolve_handle(&self) -> Result<Handle> {
        let pinned = lock(&self.inner.pinned).clone();
        if let Some(handle) = pinned {
            return Ok(handle);
        }
        Handle::try_current().map_err(|_| {
            Error::client(
                "ConsumeExecutor needs a tokio runtime to spawn workers; call submit / \
                 set_core_pool_size from an async context or build the executor with with_handle()",
            )
        })
    }
}

/// Spawns one worker task.
fn spawn_worker(inner: &Arc<Inner>, handle: &Handle, name: String) {
    rmq_debug!("consume executor spawn worker {name}");
    handle.spawn(worker_loop(Arc::clone(inner), handle.clone(), name));
}

/// Worker main loop: take a task → run it → take the next, until it should exit.
async fn worker_loop(inner: Arc<Inner>, handle: Handle, name: String) {
    rmq_debug!("consume executor worker {name} started");
    loop {
        let task = match take_task(&inner, &name).await {
            Some(task) => task,
            None => return, // the exit accounting is already done inside `take_task`
        };
        run_task(&inner, &handle, task, &name).await;
    }
}

/// The hold-lock-wait + pop-from-queue-front step.
///
/// Returning `None` means this worker should exit (equivalent to `workers -= 1` + roster removal).
async fn take_task(inner: &Arc<Inner>, name: &str) -> Option<ConsumeTask> {
    loop {
        // Register the wake-up ticket before looking at the queue: `notify_waiters` only wakes those already waiting at that moment
        let notified = inner.work_available.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        // ---- exit if due, otherwise take the task at the queue front ----
        {
            let mut state = lock(&inner.state);
            if state.shutdown && state.queue.is_empty() {
                state.retire_locked(name);
                let drained = state.workers == 0;
                drop(state);
                if drained {
                    inner.retired.notify_waiters();
                }
                return None;
            }
            if let Some(task) = state.queue.pop_front() {
                return Some(task);
            }
        }

        // ---- wait with a timeout to be woken (new task / shutdown) ----
        let keep_alive = inner.keep_alive;
        if tokio::time::timeout(keep_alive, notified).await.is_ok() {
            // woken by a new task / shutdown: back to the loop top to re-check the queue
            continue;
        }

        // ---- idle timeout: only over-core threads (> core) exit ----
        let mut state = lock(&inner.state);
        if state.queue.is_empty() && !state.shutdown && state.workers > state.core {
            state.retire_locked(name);
            let drained = state.workers == 0;
            drop(state);
            if drained {
                inner.retired.notify_waiters();
            }
            rmq_debug!(
                "consume executor worker {name} idle past {keep_alive:?}, retired (over core)"
            );
            return None;
        }
        drop(state);
    }
}

/// Runs one task: swallows any exception and counts it.
async fn run_task(inner: &Arc<Inner>, handle: &Handle, task: ConsumeTask, name: &str) {
    match handle.spawn(task).await {
        Ok(()) => {}
        Err(err) if err.is_panic() => {
            inner.handler_exceptions.fetch_add(1, Ordering::AcqRel);
            rmq_error!("consume executor task raised, worker kept alive: {name} -> {err}");
        }
        Err(err) => {
            // the task was reclaimed by the runtime before finishing (e.g. the whole runtime is shutting down), not counted as a task exception
            rmq_warn!("consume executor task dropped before completion: {name} -> {err}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicI32;
    use tokio::sync::oneshot;

    /// Turns any async block into a [`ConsumeTask`].
    fn task<F>(future: F) -> ConsumeTask
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Box::pin(future)
    }

    /// Waits for a condition by only yielding the scheduler, never sleeping a time window (asserts on failure, deterministic result).
    async fn until(label: &str, actual: impl FnMut() -> bool) {
        let mut actual = actual;
        for _ in 0..200 {
            if actual() {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert!(actual(), "{label} never happened");
    }

    /// A task that "holds a worker until the gate opens" plus its check-in channel.
    ///
    /// The gate is a [`tokio::sync::Mutex`]: while the test holds it, tasks all block inside,
    /// and **check-in happens before blocking**, so once the test observes
    /// `started` it can be sure the worker is held.
    fn blocked_task(gate: &Arc<tokio::sync::Mutex<()>>) -> (ConsumeTask, oneshot::Receiver<()>) {
        let (started_tx, started_rx) = oneshot::channel();
        let gate = Arc::clone(gate);
        (
            task(async move {
                let _ = started_tx.send(());
                let _hold = gate.lock().await;
            }),
            started_rx,
        )
    }

    // ---------------------------------------------------- executor behaviour (7 items)

    #[tokio::test]
    async fn spawns_up_to_core_then_queues() {
        // core=2: the first two tasks each spawn a worker; the 3rd task only queues, no more workers
        let ex = ConsumeExecutor::with_params(2, 8, Duration::from_secs(30), "rmq-test");
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let _hold = gate.lock().await;

        let (a, a_started) = blocked_task(&gate);
        ex.submit(a).expect("submit a");
        a_started.await.expect("worker a running");
        let (b, b_started) = blocked_task(&gate);
        ex.submit(b).expect("submit b");
        b_started.await.expect("worker b running");

        // both workers are held, so the 3rd task can only wait in the queue
        ex.submit(task(async {})).expect("submit c");
        assert_eq!(ex.worker_count(), 2);
        assert_eq!(ex.queued_count(), 1);

        drop(_hold);
        until("queued task drained", || ex.queued_count() == 0).await;
        ex.shutdown_gracefully().await;
        assert_eq!(ex.worker_count(), 0);
    }

    #[tokio::test]
    async fn raising_core_spawns_for_queued_tasks() {
        let ex = ConsumeExecutor::with_params(1, 8, Duration::from_secs(30), "rmq-test");
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let _hold = gate.lock().await;

        let (first, first_started) = blocked_task(&gate);
        ex.submit(first).expect("submit first");
        first_started.await.expect("the only worker is busy");
        for _ in 0..3 {
            ex.submit(task(async {})).expect("submit queued");
        }
        assert_eq!(ex.worker_count(), 1);
        assert_eq!(ex.queued_count(), 3);

        // delta=3, queue length=3 → min yields a top-up of 3; the accounting completes before the call returns
        ex.set_core_pool_size(4).expect("valid core");
        assert_eq!(ex.worker_count(), 4);
        assert_eq!(ex.get_core_pool_size(), 4);

        drop(_hold);
        ex.shutdown_gracefully().await;
        assert_eq!(ex.worker_count(), 0);
        assert_eq!(ex.queued_count(), 0);
    }

    #[tokio::test]
    async fn raising_core_on_empty_queue_does_not_spawn() {
        let ex = ConsumeExecutor::with_params(1, 8, Duration::from_secs(30), "rmq-test");
        ex.set_core_pool_size(6).expect("valid core");
        assert_eq!(ex.get_core_pool_size(), 6);
        assert_eq!(ex.worker_count(), 0);
        // the first task afterwards raises workers to 1 (still <= core), so it never builds them all at once
        ex.submit(task(async {})).expect("submit");
        assert_eq!(ex.worker_count(), 1);
        ex.shutdown_gracefully().await;
    }

    #[tokio::test]
    async fn extra_worker_retires_after_keep_alive_core_worker_does_not() {
        // a worker above core exits after idling out keep_alive; one at or below core never exits
        // paused clock + manual advance: no racing a time window with sleep
        tokio::time::pause();
        let ex = ConsumeExecutor::with_params(1, 4, Duration::from_millis(200), "rmq-test");

        ex.submit(task(async {})).expect("submit 1");
        until("task 1 drained", || ex.queued_count() == 0).await;
        ex.set_core_pool_size(2).expect("raise core"); // empty queue → no top-up, core is now 2
        assert_eq!(ex.get_core_pool_size(), 2);
        assert_eq!(ex.worker_count(), 1);

        // submit one more task to raise workers to 2 (still <= core, must survive the timeout)
        ex.submit(task(async {})).expect("submit 2");
        until("task 2 drained", || ex.queued_count() == 0).await;
        assert_eq!(ex.worker_count(), 2);

        // the extra one is now "over core"; advance the clock step by step: whoever parks first times out first,
        // but the outcome is deterministic -- only the core one remains
        ex.set_core_pool_size(1).expect("lower core");
        for _ in 0..5 {
            if ex.worker_count() == 1 {
                break;
            }
            tokio::time::advance(Duration::from_millis(300)).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(ex.worker_count(), 1, "the over-core worker should exit after keep_alive");

        for _ in 0..10 {
            tokio::time::advance(Duration::from_millis(300)).await;
            tokio::task::yield_now().await;
            assert_eq!(ex.worker_count(), 1, "the core worker must not exit");
        }

        // the surviving worker can still do work
        let (done_tx, done_rx) = oneshot::channel();
        ex.submit(task(async move {
            let _ = done_tx.send(());
        }))
        .expect("submit after retirements");
        done_rx.await.expect("core worker still consumes tasks");

        ex.shutdown_gracefully().await;
        assert_eq!(ex.worker_count(), 0);
    }

    #[tokio::test]
    async fn task_panic_does_not_kill_worker() {
        let ex = ConsumeExecutor::with_params(1, 2, Duration::from_secs(30), "rmq-test");
        assert_eq!(ex.handler_exception_count(), 0);

        ex.submit(task(async {
            panic!("boom");
        }))
        .expect("submit boom");
        let (done_tx, done_rx) = oneshot::channel();
        ex.submit(task(async move {
            let _ = done_tx.send(());
        }))
        .expect("submit next");

        // the same worker (core=1, there cannot be a second) picked up the next task, so the panic did not kill it;
        // and it ran through/swallowed boom before taking the task, so the counter is already readable.
        done_rx.await.expect("worker survived the panic");
        assert_eq!(ex.handler_exception_count(), 1);
        assert_eq!(ex.worker_count(), 1);

        ex.shutdown_gracefully().await;
    }

    #[tokio::test]
    async fn submit_after_shutdown_raises() {
        let ex = ConsumeExecutor::new(1, 2);
        ex.shutdown();
        let err = ex
            .submit(task(async {}))
            .expect_err("shut-down pool rejects tasks");
        assert_eq!(
            err.to_string(),
            "MQClientException: ConsumeExecutor has been shut down"
        );
        // a repeated shutdown is a no-op
        ex.shutdown();
        ex.await_termination().await;
    }

    #[tokio::test]
    async fn shutdown_waits_for_queued_tasks_to_drain() {
        let ex = ConsumeExecutor::with_params(1, 2, Duration::from_secs(30), "rmq-test");
        let counter = Arc::new(AtomicI32::new(0));
        for _ in 0..5 {
            let counter = Arc::clone(&counter);
            ex.submit(task(async move {
                counter.fetch_add(1, Ordering::AcqRel);
            }))
            .expect("submit work");
        }
        ex.shutdown_gracefully().await;
        assert_eq!(counter.load(Ordering::Acquire), 5);
        assert_eq!(ex.worker_count(), 0);
        assert_eq!(ex.queued_count(), 0);
        // submissions after the drain are still rejected
        assert!(ex.submit(task(async {})).is_err());
    }

    #[tokio::test]
    async fn zero_core_still_runs_tasks() {
        // with core=0 (no resident workers) tasks still execute (one is spawned on demand at submit time)
        let ex = ConsumeExecutor::with_params(0, 2, Duration::from_millis(50), "rmq-test");
        let (done_tx, done_rx) = oneshot::channel();
        ex.submit(task(async move {
            let _ = done_tx.send(());
        }))
        .expect("submit with core=0");
        done_rx.await.expect("task ran to completion");
        assert_eq!(ex.get_core_pool_size(), 0);
        ex.shutdown_gracefully().await;
        assert_eq!(ex.worker_count(), 0);
    }

    // ---------------------------------------------------- parameter clamping / guards / boundaries

    #[test]
    fn sizes_are_clamped_like_golden() {
        // core = max(0, core); max = max(core, max)
        let ex = ConsumeExecutor::new(-3, -7);
        assert_eq!(ex.get_core_pool_size(), 0);
        assert_eq!(ex.get_max_pool_size(), 0);

        let ex = ConsumeExecutor::new(4, 2);
        assert_eq!(ex.get_core_pool_size(), 4);
        assert_eq!(ex.get_max_pool_size(), 4, "max must not go below core");

        let ex = ConsumeExecutor::new(2, 8);
        assert_eq!(ex.worker_count(), 0);
        assert_eq!(ex.queued_count(), 0);
        assert_eq!(ex.handler_exception_count(), 0);
    }

    #[tokio::test]
    async fn set_core_pool_size_rejects_negative() {
        let ex = ConsumeExecutor::new(1, 2);
        let err = ex
            .set_core_pool_size(-1)
            .expect_err("negative core must be rejected");
        assert_eq!(
            err.to_string(),
            "MQClientException: core pool size must be >= 0"
        );
        assert_eq!(ex.get_core_pool_size(), 1);
    }

    #[tokio::test]
    async fn raising_core_above_max_lifts_max() {
        let ex = ConsumeExecutor::new(2, 4);
        ex.set_core_pool_size(SHORT_MAX_VALUE).expect("valid core");
        assert_eq!(ex.get_core_pool_size(), SHORT_MAX_VALUE);
        assert_eq!(
            ex.get_max_pool_size(),
            SHORT_MAX_VALUE,
            "max is lifted to core along with it"
        );
        assert_eq!(ex.worker_count(), 0, "empty queue → not a single worker is built");
    }

    #[tokio::test]
    async fn set_core_pool_size_after_shutdown_only_records_core() {
        // no top-up after shutdown (spawning only when delta > 0 and not shut down)
        let ex = ConsumeExecutor::with_params(1, 8, Duration::from_secs(30), "rmq-test");
        ex.submit(task(async {})).expect("submit");
        ex.shutdown();
        ex.await_termination().await;
        ex.set_core_pool_size(5).expect("still records the value");
        assert_eq!(ex.get_core_pool_size(), 5);
        assert_eq!(ex.worker_count(), 0);
    }

    #[tokio::test]
    async fn shutdown_wakes_idle_workers_without_waiting_for_keep_alive() {
        // keep_alive so long that a natural timeout is impossible: workers can only exit after being woken by shutdown
        let ex = ConsumeExecutor::with_params(2, 4, Duration::from_secs(3600), "rmq-test");
        ex.submit(task(async {})).expect("submit 1");
        ex.submit(task(async {})).expect("submit 2");
        assert_eq!(ex.worker_count(), 2);
        ex.shutdown();
        ex.await_termination().await;
        assert_eq!(ex.worker_count(), 0);
    }

    #[test]
    fn submit_needs_a_tokio_runtime() {
        // difference #1: workers are tokio tasks and need a host runtime
        let ex = ConsumeExecutor::new(1, 2);
        let outsider = ex.clone();
        let result = std::thread::spawn(move || outsider.submit(task(async {})))
            .join()
            .expect("thread joined");
        let err = result.expect_err("no runtime outside a tokio context");
        assert!(
            err.to_string().contains("needs a tokio runtime"),
            "unexpected message: {err}"
        );
        assert_eq!(ex.queued_count(), 0, "a rejected submit never enqueues");
    }

    #[test]
    fn pinned_handle_lets_plain_threads_submit() {
        // the other half of difference #1: with a handle injected, sync contexts (standalone std threads) can submit too
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("dedicated runtime");
        let ex = ConsumeExecutor::with_params(1, 2, Duration::from_secs(30), "rmq-test")
            .with_handle(runtime.handle());
        let counter = Arc::new(AtomicI32::new(0));
        for _ in 0..3 {
            let counter = Arc::clone(&counter);
            ex.submit(task(async move {
                counter.fetch_add(1, Ordering::AcqRel);
            }))
            .expect("submit from a plain thread with a pinned handle");
        }
        assert_eq!(ex.worker_count(), 1);
        // the worker runs on the injected runtime, so the graceful shutdown must also wait inside that runtime
        runtime.block_on(ex.shutdown_gracefully());
        assert_eq!(counter.load(Ordering::Acquire), 3);
        assert_eq!(ex.worker_count(), 0);
        runtime.shutdown_timeout(Duration::from_secs(1));
    }

    #[tokio::test]
    async fn tasks_run_in_fifo_order_per_worker() {
        let ex = ConsumeExecutor::with_params(1, 4, Duration::from_secs(30), "rmq-test");
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        for i in 0..6 {
            let order = Arc::clone(&order);
            ex.submit(task(async move {
                lock(&order).push(i);
            }))
            .expect("submit");
        }
        ex.shutdown_gracefully().await;
        assert_eq!(
            *lock(&order),
            (0..6).collect::<Vec<_>>(),
            "a single worker must execute in submission order"
        );
    }

    #[tokio::test]
    async fn clones_share_one_pool() {
        let ex = ConsumeExecutor::with_params(1, 2, Duration::from_secs(30), "rmq-test");
        let other = ex.clone();
        let (done_tx, done_rx) = oneshot::channel();
        other
            .submit(task(async move {
                let _ = done_tx.send(());
            }))
            .expect("submit via clone");
        done_rx.await.expect("clone shares the same workers");
        assert_eq!(ex.worker_count(), 1);
        ex.shutdown_gracefully().await;
        assert!(other.submit(task(async {})).is_err(), "the shutdown applies to both sides");
    }
}
