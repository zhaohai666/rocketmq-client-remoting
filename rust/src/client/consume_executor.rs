//! * 队列**无界**，于是 `execute` 里「线程数超过 core」这条路径永远不会走
//!   （只有 `poolSize < corePoolSize` 才新建线程，否则入队），**真实并发度 == corePoolSize**；
//! * `AbstractConsumeMessageService#updateCorePoolSize` → `ThreadPoolExecutor#setCorePoolSize`
//!   —— 运行时改的就是这个并发度，`getCorePoolSize()` 能读回来，所以
//!   `update_core_pool_size` / `get_core_pool_size`（在 `consumer.rs` 一侧）有真实语义。
//!
//!
//!
//! # 给后续 `consumer.rs` 留的接缝（seam）
//! 本模块**不认识**消息、listener、`MQClientInstance`，也就无需任何消费者侧 trait：
//! 唯一接缝是 [`ConsumeTask`]（= `Pin<Box<dyn Future<Output = ()> + Send>>`，等价 Python
//! `submit(fn, *args, **kwargs)` 里的 `(fn, args, kwargs)` 三元组）。消费回调的接缝
//! 已在 [`super::result`] 里定义好 ——
//! [`MessageListenerConcurrently`](super::result::MessageListenerConcurrently) 与
//! [`MessageListenerOrderly`](super::result::MessageListenerOrderly)，都是 `Send + Sync`
//! 的对象安全 trait，本模块刻意不重复定义（复用规则）。`consumer.rs` 要写的就是
//! Python `consumer.py:1396` `self._pop_executor.submit(self._consume_pop_batch, batch, pq, mq)`
//! 的等价物：
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

/// 投递给执行器的一个任务，等价 Python `submit(fn, *args, **kwargs)` 的 `(fn, args, kwargs)`。
///
/// 这是本模块对外的**唯一接缝**：future 天生惰性，于是「构造」= Python 在 submit 处求实参，
/// 「执行」= worker 领到任务，两个时点天然分开。用法：`executor.submit(Box::pin(async move { .. }))`。
pub type ConsumeTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// 取锁（Python 的 `with self._lock`）。
///
/// 中毒时照用：池子状态只有计数和队列，panic 过的持有者不会留下半致的不变量，
/// 而让整个消费池彻底失联才是更大的问题。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// 池子状态（对应 Python `ConsumeExecutor.__init__` 里那批 `self._xxx` 字段）。
struct State {
    core: i32,
    max: i32,
    queue: VecDeque<ConsumeTask>,
    workers: i32,
    /// 线程名序号（`self._seq`），名字格式 `<prefix>-<seq>` 与 Python `%s-%d` 一致
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

    /// 对应 Python `_run` 里的 `self._workers -= 1`（原 `_retire_locked` 的名册摘除见差异 #6）。
    ///
    /// **调用方必须已持锁，且「是否该退出」的判断与本次扣账在同一临界区内**：
    /// 否则两个超编 worker 同时超时会让 core 内的 worker 也被扣掉。
    fn retire_locked(&mut self, name: &str) {
        self.workers -= 1;
        rmq_debug!(
            "consume executor worker {name} retired, alive={} queued={}",
            self.workers,
            self.queue.len()
        );
    }
}

/// 手工实现：队列里的 [`ConsumeTask`] 是 `dyn Future`，没有 `Debug`，所以只报可观测计数。
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
    /// Python 的 `threading.Condition(self._lock)`：有新任务 / 已关闭时叫醒空闲 worker。
    work_available: Notify,
    /// worker 走光时叫醒 [`ConsumeExecutor::await_termination`]（Python 的 `Thread.join`）。
    retired: Notify,
    /// `self._handler_exceptions`：被吞掉的任务异常计数。
    handler_exceptions: AtomicUsize,
    keep_alive: Duration,
    prefix: String,
    /// [`ConsumeExecutor::with_handle`] 注入的句柄；`None` 表示每次现取 `Handle::try_current()`。
    pinned: Mutex<Option<Handle>>,
}

/// `Clone` 共享同一个池（内部 `Arc`）：Python 里 consumer 持有的是引用，Rust 侧
/// `consumer.rs` 直接 clone 给各 POP 循环即可，不要再套一层 `Arc<ConsumeExecutor>`。
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
    /// 对应 Python `ConsumeExecutor(core, max)`：keep_alive 60s、线程名前缀 `rmq-consume`
    /// （Python 的两个默认参数）。
    pub fn new(core_pool_size: i32, maximum_pool_size: i32) -> ConsumeExecutor {
        ConsumeExecutor::with_params(
            core_pool_size,
            maximum_pool_size,
            DEFAULT_KEEP_ALIVE,
            DEFAULT_THREAD_NAME_PREFIX,
        )
    }

    /// 对应 Python `ConsumeExecutor(core, max, keep_alive_seconds=..., thread_name_prefix=...)`
    /// （POP 路径实际用的形态，见 Python `consumer.py:789`）。
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

    /// 固定用某个运行时建 worker（对齐 `ConsumerStatsManager::start_with_handle` 的口径）。
    ///
    /// 不设置时每次现取 `Handle::try_current()`；`MQClientInstance` 自带专用运行时时，
    /// 注入进来才能保证消费 worker 与传输层在同一个池里。
    #[must_use]
    pub fn with_handle(self, handle: &Handle) -> ConsumeExecutor {
        *lock(&self.inner.pinned) = Some(handle.clone());
        self
    }

    // ---------------- 对外 API ----------------

    /// 失败只有两种：池已关闭（Python 的 `RuntimeError`，文案一致）、拿不到 tokio
    /// 运行时（差异 #1，Python 无此约束）。任务本身没有结果，所以 `Ok(())` 只表示「入队了」。
    pub fn submit(&self, task: ConsumeTask) -> Result<()> {
        let handle = self.resolve_handle()?;
        self.submit_with(&handle, task)
    }

    /// [`ConsumeExecutor::submit`] 的显式句柄版本，供非异步上下文（独立 std 线程等）调用。
    pub fn submit_with(&self, handle: &Handle, task: ConsumeTask) -> Result<()> {
        let spawn = {
            let mut state = lock(&self.inner.state);
            if state.shutdown {
                // 文案与 Python `consume_executor.py:67` 逐字一致
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
        // 对应 Python `self._work_available.notify()`（这里是全员唤醒，见差异 #4）
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
    /// 即：core 变大时按 `min(delta, 队列长度)` **补齐** worker（不是「补到跟队列一样多」），
    /// core 变小时只让超编的空闲 worker 走 keep_alive 退出逻辑（不打断在跑的任务）。
    /// 记账（`workers` 抬升）在返回前就完成，所以 `worker_count()` 立刻可读。
    ///
    /// `n < 0` 返回 `Err`（Python 的 `ValueError`，文案一致）。
    pub fn set_core_pool_size(&self, n: i32) -> Result<()> {
        let handle = self.resolve_handle()?;
        self.set_core_pool_size_with(&handle, n)
    }

    /// [`ConsumeExecutor::set_core_pool_size`] 的显式句柄版本。
    pub fn set_core_pool_size_with(&self, handle: &Handle, n: i32) -> Result<()> {
        if n < 0 {
            // 文案与 Python `consume_executor.py:98` 逐字一致
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
        // Python 的 setCorePoolSize 不 notify（新起的线程自己会看队列）；这里补一次唤醒，
        // 让「core 抬高但被 max 卡住没起新线程」时的空闲 worker 也能立刻来领任务。
        self.inner.work_available.notify_waiters();
        Ok(())
    }

    pub fn get_core_pool_size(&self) -> i32 {
        lock(&self.inner.state).core
    }

    /// 无界队列下它只是「补线程」路径的上界，正常情况下够不到。
    pub fn get_max_pool_size(&self) -> i32 {
        lock(&self.inner.state).max
    }

    pub fn worker_count(&self) -> i32 {
        lock(&self.inner.state).workers
    }

    pub fn queued_count(&self) -> usize {
        lock(&self.inner.state).queue.len()
    }

    /// 被吞掉的任务异常计数（Python `handler_exception_count`，仅供观测/单测）。
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
        // 对应 Python `self._work_available.notify_all()`
        self.inner.work_available.notify_waiters();
        if !first_time {
            rmq_debug!("consume executor already shut down, nothing to do");
        }
    }

    /// 等所有 worker 退出（Python `shutdown(wait=True)` 里那段 `Thread.join()`）。
    pub async fn await_termination(&self) {
        loop {
            // 先登记唤醒凭据再看计数：「最后一个 worker 刚好在此刻退出」不会丢通知
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

    // ---------------- 内部 ----------------

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

/// 对应 Python `_spawn_locked` 的后半段（`threading.Thread(target=_run, daemon=True).start()`）。
fn spawn_worker(inner: &Arc<Inner>, handle: &Handle, name: String) {
    rmq_debug!("consume executor spawn worker {name}");
    handle.spawn(worker_loop(Arc::clone(inner), handle.clone(), name));
}

/// 对应 Python `_run`：领任务 → 跑 → 再领，直到该退出。
async fn worker_loop(inner: Arc<Inner>, handle: Handle, name: String) {
    rmq_debug!("consume executor worker {name} started");
    loop {
        let task = match take_task(&inner, &name).await {
            Some(task) => task,
            None => return, // 退出记账已在 `take_task` 内做完
        };
        run_task(&inner, &handle, task, &name).await;
    }
}

/// 对应 Python `_run` 里持锁等待 + `popleft()` 那一段。
///
/// 返回 `None` 表示本 worker 该退出了（等价 `workers -= 1` + `_retire_locked`）。
async fn take_task(inner: &Arc<Inner>, name: &str) -> Option<ConsumeTask> {
    loop {
        // 先登记唤醒凭据、再看队列：`notify_waiters` 只叫醒当时已在等的人
        let notified = inner.work_available.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        // ---- Python: `if self._shutdown and not self._queue: retire` / `task = popleft()` ----
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

        // ---- Python: `self._work_available.wait(timeout=self._keep_alive)` ----
        let keep_alive = inner.keep_alive;
        if tokio::time::timeout(keep_alive, notified).await.is_ok() {
            // 被新任务 / shutdown 叫醒：回到循环顶重看队列
            continue;
        }

        // ---- Python: 空闲超时，只有超编线程（> core）才退出 ----
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

/// 对应 Python `_run` 末尾的 `try: fn(*args, **kwargs) / except BaseException`。
async fn run_task(inner: &Arc<Inner>, handle: &Handle, task: ConsumeTask, name: &str) {
    match handle.spawn(task).await {
        Ok(()) => {}
        Err(err) if err.is_panic() => {
            inner.handler_exceptions.fetch_add(1, Ordering::AcqRel);
            rmq_error!("consume executor task raised, worker kept alive: {name} -> {err}");
        }
        Err(err) => {
            // 任务没跑完就被运行时回收（例如整个 runtime 正在退出），不算任务异常
            rmq_warn!("consume executor task dropped before completion: {name} -> {err}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicI32;
    use tokio::sync::oneshot;

    /// 把任意异步块变成 [`ConsumeTask`]（等价 Python 的 `submit(fn, *args)`）。
    fn task<F>(future: F) -> ConsumeTask
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Box::pin(future)
    }

    /// 只让出调度权、不睡时间窗地等条件成立（失败即断言，结果确定）。
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

    /// 「占住 worker 直到闸门放开」的任务 + 报到通道。
    ///
    /// 闸门用 [`tokio::sync::Mutex`]：测试持有时任务全部阻塞在里面（等价 Python 的
    /// `started.set(); release.wait(5)`），**报到发生在阻塞之前**，所以测试观察到
    /// `started` 就能确定该 worker 已被占住。
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

    // ---------------------------------------------------- TestConsumeExecutor（Python 同名 7 项）

    #[tokio::test]
    async fn spawns_up_to_core_then_queues() {
        // core=2：前两个任务各起一个 worker；第 3 个任务只入队，不再建 worker
        let ex = ConsumeExecutor::with_params(2, 8, Duration::from_secs(30), "rmq-test");
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let _hold = gate.lock().await;

        let (a, a_started) = blocked_task(&gate);
        ex.submit(a).expect("submit a");
        a_started.await.expect("worker a running");
        let (b, b_started) = blocked_task(&gate);
        ex.submit(b).expect("submit b");
        b_started.await.expect("worker b running");

        // 两个 worker 都被占住，第 3 个任务只能在队列里等
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

        // delta=3、队列长度=3 → min 之后补 3 个；记账在调用返回前就完成
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
        // 之后第一个任务把 workers 抬到 1（仍 <= core），不会一次建满
        ex.submit(task(async {})).expect("submit");
        assert_eq!(ex.worker_count(), 1);
        ex.shutdown_gracefully().await;
    }

    #[tokio::test]
    async fn extra_worker_retires_after_keep_alive_core_worker_does_not() {
        // > core 的 worker 空闲到 keep_alive 就退出；<= core 的永不退出
        // 时钟暂停 + 手动 advance：不靠 sleep 抢时间窗
        tokio::time::pause();
        let ex = ConsumeExecutor::with_params(1, 4, Duration::from_millis(200), "rmq-test");

        ex.submit(task(async {})).expect("submit 1");
        until("task 1 drained", || ex.queued_count() == 0).await;
        ex.set_core_pool_size(2).expect("raise core"); // 队列空 → 不补 worker，core 现在是 2
        assert_eq!(ex.get_core_pool_size(), 2);
        assert_eq!(ex.worker_count(), 1);

        // 再提交一个任务把 worker 抬到 2（仍 <= core，超时后应当存活）
        ex.submit(task(async {})).expect("submit 2");
        until("task 2 drained", || ex.queued_count() == 0).await;
        assert_eq!(ex.worker_count(), 2);

        // 多出的那个现在属于「超编」；逐步推进时钟：谁先 park 谁先超时，
        // 但结果确定 —— 只会剩 core 那一个
        ex.set_core_pool_size(1).expect("lower core");
        for _ in 0..5 {
            if ex.worker_count() == 1 {
                break;
            }
            tokio::time::advance(Duration::from_millis(300)).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(ex.worker_count(), 1, "超编 worker 应在 keep_alive 后退出");

        for _ in 0..10 {
            tokio::time::advance(Duration::from_millis(300)).await;
            tokio::task::yield_now().await;
            assert_eq!(ex.worker_count(), 1, "core 内的 worker 不能退出");
        }

        // 活下来的那个 worker 确实还能干活
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
        // 对应 Python `test_task_exception_does_not_kill_worker`
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

        // 同一个 worker（core=1，不可能有第二个）领到了下一个任务，说明它没被 panic 杀死；
        // 而它是先跑完/吞掉 boom 才来领任务的，所以计数已可读。
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
        // 重复 shutdown 是 no-op（Python 第二次也只是复制名册）
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
        // 跑完之后的提交依旧被拒
        assert!(ex.submit(task(async {})).is_err());
    }

    #[tokio::test]
    async fn zero_core_still_runs_tasks() {
        // core=0（不建常驻 worker）时任务仍然会被执行（提交时按需起一个）
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

    // ---------------------------------------------------- 参数夹取 / 守卫 / 边界

    #[test]
    fn sizes_are_clamped_like_python() {
        // core = max(0, core)；max = max(core, max)
        let ex = ConsumeExecutor::new(-3, -7);
        assert_eq!(ex.get_core_pool_size(), 0);
        assert_eq!(ex.get_max_pool_size(), 0);

        let ex = ConsumeExecutor::new(4, 2);
        assert_eq!(ex.get_core_pool_size(), 4);
        assert_eq!(ex.get_max_pool_size(), 4, "max 不能低于 core");

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
            .expect_err("negative core is a ValueError in Python");
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
            "Java 把 max 抬到 core"
        );
        assert_eq!(ex.worker_count(), 0, "队列空 → 一个 worker 都不建");
    }

    #[tokio::test]
    async fn set_core_pool_size_after_shutdown_only_records_core() {
        // Python: `if delta > 0 and not self._shutdown` —— 关闭后不再补线程
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
        // keep_alive 长到不可能自然超时：worker 只能被 shutdown 叫醒后退出
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
        // 差异 #1：Python 起的是 daemon 线程，无此约束
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
        assert_eq!(ex.queued_count(), 0, "被拒的提交不入队");
    }

    #[test]
    fn pinned_handle_lets_plain_threads_submit() {
        // 差异 #1 的另一半：注入句柄后，同步上下文（独立 std 线程）也能投递
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
        // worker 跑在注入的运行时上，所以「优雅关闭」也得在那个运行时里等
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
            "单 worker 必须按投递顺序执行"
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
        assert!(other.submit(task(async {})).is_err(), "关闭对两侧都生效");
    }
}
