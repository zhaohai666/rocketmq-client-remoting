//! 消费侧统计（对应 `python/rocketmq/client/consumer_stats.py`，即 Java
//! `org.apache.rocketmq.client.stat.ConsumerStatsManager` 与
//! `org.apache.rocketmq.common.stats.{StatsItem,StatsItemSet,StatsSnapshot}`）。
//!
//! Java 真实模型（5.5.1 源码逐条核对，**不是**按名字想象的"每分钟一个桶"）：
//!
//! * [`StatsItem`] 持有**累计值** value / times（只增不减），以及两条采样快照链：
//!   `minute`（每 10s 采一个累计点）与 `hour`（每 10 分钟采一个累计点）；
//! * 快照计算 [`compute_stats_data`]（StatsItem.java:53-79）：
//!
//!   ```text
//!   sum       = last.value - first.value             # 窗口内的增量
//!   tps       = sum * 1000.0 / (last.ts - first.ts)  # 每秒（注意不是每分钟！）
//!   timesDiff = last.times - first.times
//!   avgpt     = timesDiff > 0 ? sum / timesDiff : 0  # "每次调用的平均量"——RT 项即平均耗时
//!   ```
//!
//! * TPS 类计数（PULL_TPS 等）走 `add_value(key, msgs, 1)`：value 累加**消息数**、
//!   times 累加**调用次数** → tps = 调用次数/秒；
//! * RT 类计数（PULL_RT 等）同样 `add_value(key, rt, 1)`（Java 的 `addRTValue`）：
//!   value 累加耗时、times 累加次数 → avgpt = 平均耗时（毫秒）；
//! * [`ConsumerStatsManager::consume_status`] 全部取 **minute** 快照：pullRT/consumeRT 用
//!   avgpt，pullTPS/consumeOKTPS/consumeFailedTPS 用 tps，consumeFailedMsgs 取 failed 的
//!   **hour** 窗口 sum（Java 特意跨窗口取数，照抄）。
//!
//! 与 Java / Python 的**有意差异**（语义不变）：
//! 1. Java 给每个 StatsItem 单独排 10s/10min 的采样任务，Python/本模块由 manager 的
//!    **一个**采样任务统一巡采（10s 一轮，每 60 轮即 10 分钟做一次小时级）；
//! 2. Python 用 daemon 线程 + `Event.wait()`，这里用 `tokio::time::interval_at`
//!    （先等后采，与 `_stop.wait(10)` 一致）+ `Notify` 退出，采样周期与小时轮次都可注入，
//!    单测在毫秒级跑完；采样任务只持 `Weak<Inner>`，manager 被丢弃后自行退出
//!    （对应 Python 的 daemon 线程不会拖住进程退出）；
//! 3. `shutdown()` 不 join（Rust 同步上下文里不能 block_on），改为"置位 + 唤醒 + abort"，
//!    等价于 Python 的 `join(timeout=3)` 但永不阻塞；
//! 4. Java 的 tps 除法**没有**除零保护（span 为 0 时是 Infinity/NaN），Python 加了
//!    `span_ms > 0` 判断，这里跟随 Python；
//! 5. `StatsItemSet::keys()` 返回**字典序**（C++ 用 `std::map`、本模块用 `BTreeMap`），
//!    Python 是插入序 —— 巡采结果与顺序无关。

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::common::util_all::current_time_millis;
use crate::error::Result;
use crate::remoting::protocol::body::ConsumeStatus;
use crate::{bail, rmq_debug};

/// 采样参数（Java StatsItem.init 的 `scheduleAtFixedRate(..., 0, 10, SECONDS)`）。
pub const SAMPLING_INTERVAL_SECONDS: f64 = 10.0;
/// 小时级快照间隔（Java `scheduleAtFixedRate(..., 0, 10, MINUTES)` = 600s）。
pub const HOUR_SAMPLING_INTERVAL_SECONDS: f64 = 600.0;
/// 分钟快照链长度（Java csListMinute 最多约 60 个点 ≈ 10 分钟窗口）。
pub const MINUTE_LIST_MAX: usize = 60;
/// 小时快照链长度。
pub const HOUR_LIST_MAX: usize = 60;
/// 每多少轮分钟采样做一次小时采样：`600s / 10s = 60`（Python 里写死的 `% 60`）。
pub const HOUR_SAMPLING_ROUNDS: u64 =
    (HOUR_SAMPLING_INTERVAL_SECONDS / SAMPLING_INTERVAL_SECONDS) as u64;

/// 一个采样点：`(timestamp_ms, 累计 value, 累计 times)`（Java `CallSnapshot`）。
pub type CallSnapshot = (i64, i64, i64);

/// 锁中毒时照常取内值 —— 统计路径绝不允许 panic 把业务线程带下去。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// 对应 Java `StatsSnapshot`：sum / tps / avgpt / times。
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct StatsSnapshot {
    /// 窗口内的累计增量。
    pub sum: i64,
    /// 每秒量（`sum * 1000 / span_ms`）。
    pub tps: f64,
    /// 每次调用的平均量（RT 项即平均耗时，毫秒）。
    pub avgpt: f64,
    /// 窗口内的调用次数增量。
    pub times: i64,
}

impl fmt::Display for StatsSnapshot {
    /// 与 Python `StatsSnapshot.__repr__` 逐字对齐（tps/avgpt 保留两位小数）。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StatsSnapshot(sum={}, tps={:.2}, avgpt={:.2}, times={})",
            self.sum, self.tps, self.avgpt, self.times
        )
    }
}

/// Java `StatsItem.computeStatsData` 逐条照抄（StatsItem.java:53-79）。
pub fn compute_stats_data(cs_list: &[CallSnapshot]) -> StatsSnapshot {
    compute_stats_data_pair(cs_list.first(), cs_list.last())
}

/// 同上，但只喂首尾两点 —— 链在 Rust 侧是 `VecDeque`，而 Java/Python 也只用到 first/last。
fn compute_stats_data_pair(first: Option<&CallSnapshot>, last: Option<&CallSnapshot>) -> StatsSnapshot {
    let mut ss = StatsSnapshot::default();
    let (first, last) = match (first, last) {
        (Some(f), Some(l)) => (f, l),
        _ => return ss,
    };
    ss.sum = last.1 - first.1;
    let span_ms = last.0 - first.0;
    if span_ms > 0 {
        ss.tps = (ss.sum as f64 * 1000.0) / span_ms as f64;
    }
    let times_diff = last.2 - first.2;
    ss.times = times_diff;
    if times_diff > 0 {
        ss.avgpt = (ss.sum as f64 * 1.0) / times_diff as f64;
    }
    ss
}

/// 单项统计：累计 value/times + 分钟/小时两级采样链（Java `StatsItem`）。
#[derive(Debug)]
pub struct StatsItem {
    stats_name: String,
    stats_key: String,
    state: Mutex<ItemState>,
}

#[derive(Debug, Default)]
struct ItemState {
    value: i64,
    times: i64,
    /// 元素 = `(timestamp_ms, 累计 value, 累计 times)`。
    minute: VecDeque<CallSnapshot>,
    hour: VecDeque<CallSnapshot>,
}

impl StatsItem {
    /// 对应 Python `StatsItem(stats_name, stats_key)`，key 一般是 `topic@group`。
    pub fn new(stats_name: impl Into<String>, stats_key: impl Into<String>) -> StatsItem {
        StatsItem {
            stats_name: stats_name.into(),
            stats_key: stats_key.into(),
            state: Mutex::new(ItemState::default()),
        }
    }

    /// 统计名（`PULL_RT` / `PULL_TPS` / ...）。
    pub fn stats_name(&self) -> &str {
        &self.stats_name
    }

    /// 统计 key（`topic@group`）。
    pub fn stats_key(&self) -> &str {
        &self.stats_key
    }

    /// 对应 Python `add_value`（Java `addValue` / `addRTValue`）：累计值只增不减。
    pub fn add_value(&self, inc_value: i64, inc_times: i64) {
        let mut state = lock(&self.state);
        state.value += inc_value;
        state.times += inc_times;
    }

    /// 当前累计 value（Python 的 `value` property）。
    pub fn value(&self) -> i64 {
        lock(&self.state).value
    }

    /// 当前累计 times（Python 的 `times` property）。
    pub fn times(&self) -> i64 {
        lock(&self.state).times
    }

    /// 追加分钟级采样点（每 10s 由采样任务调用），时钟取 `current_time_millis()`。
    ///
    /// 与 Python 的细微差别：Python 在锁内读时钟，这里先读时钟再取锁 —— 采样点内容
    /// （累计 value/times）仍在锁内取，口径不变。
    pub fn sample(&self) {
        let ts = current_time_millis();
        self.sample_at(ts);
    }

    /// [`StatsItem::sample`] 的显式时钟版本（C++/C# 端的 `appendSampleForTest` 钩子）：
    /// 单测与"补采历史点"都靠它，无需睡墙钟。
    pub fn sample_at(&self, ts_ms: i64) {
        let mut state = lock(&self.state);
        let point = (ts_ms, state.value, state.times);
        state.minute.push_back(point);
        while state.minute.len() > MINUTE_LIST_MAX {
            state.minute.pop_front();
        }
    }

    /// 追加点小时级采样点（每 10 分钟由采样任务调用）。
    pub fn sample_hour(&self) {
        let ts = current_time_millis();
        self.sample_hour_at(ts);
    }

    /// [`StatsItem::sample_hour`] 的显式时钟版本。
    pub fn sample_hour_at(&self, ts_ms: i64) {
        let mut state = lock(&self.state);
        let point = (ts_ms, state.value, state.times);
        state.hour.push_back(point);
        while state.hour.len() > HOUR_LIST_MAX {
            state.hour.pop_front();
        }
    }

    /// 分钟窗口快照（Java `getStatsDataInMinute`）。
    pub fn get_stats_data_in_minute(&self) -> StatsSnapshot {
        let state = lock(&self.state);
        compute_stats_data_pair(state.minute.front(), state.minute.back())
    }

    /// 小时窗口快照（Java `getStatsDataInHour`）。
    pub fn get_stats_data_in_hour(&self) -> StatsSnapshot {
        let state = lock(&self.state);
        compute_stats_data_pair(state.hour.front(), state.hour.back())
    }

    /// 分钟链当前长度（Python 测试里直接看 `len(it._minute)`）。
    pub fn minute_len(&self) -> usize {
        lock(&self.state).minute.len()
    }

    /// 小时链当前长度。
    pub fn hour_len(&self) -> usize {
        lock(&self.state).hour.len()
    }
}

/// key -> StatsItem（对应 Java `StatsItemSet`；key = `topic@group`）。
///
/// `Clone` 是**同一张表**的另一个句柄（内部 `Arc<Mutex<BTreeMap>>`），
/// 所以 manager 可以把某个 set 交给调用方长期持有。
#[derive(Clone)]
pub struct StatsItemSet {
    stats_name: String,
    items: Arc<Mutex<BTreeMap<String, Arc<StatsItem>>>>,
}

impl fmt::Debug for StatsItemSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StatsItemSet")
            .field("stats_name", &self.stats_name)
            .field("keys", &self.keys())
            .finish()
    }
}

impl StatsItemSet {
    /// 对应 Python `StatsItemSet(stats_name)`。
    pub fn new(stats_name: impl Into<String>) -> StatsItemSet {
        StatsItemSet {
            stats_name: stats_name.into(),
            items: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// 统计名（Java 的 `statsName`，如 `CONSUME_OK_TPS`）。
    pub fn stats_name(&self) -> &str {
        &self.stats_name
    }

    /// 对应 Python `get_and_create`：不存在就建，存在就返回同一个 item。
    pub fn get_and_create(&self, key: &str) -> Arc<StatsItem> {
        let mut items = lock(&self.items);
        items
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(StatsItem::new(self.stats_name.clone(), key)))
            .clone()
    }

    /// 对应 Python `find`：不存在返回 None（**不**创建，consume_status 就靠这点跳过零值）。
    pub fn find(&self, key: &str) -> Option<Arc<StatsItem>> {
        lock(&self.items).get(key).cloned()
    }

    /// 对应 Python `add_value`：取或建，然后累加。
    pub fn add_value(&self, key: &str, inc_value: i64, inc_times: i64) {
        self.get_and_create(key).add_value(inc_value, inc_times);
    }

    /// 现有 key（字典序，见模块头的差异说明 5）。
    pub fn keys(&self) -> Vec<String> {
        lock(&self.items).keys().cloned().collect()
    }

    /// 巡采本 set 的所有 item（分钟级）。
    pub fn sample_all(&self) {
        for key in self.keys() {
            if let Some(item) = self.find(&key) {
                item.sample();
            }
        }
    }

    /// 巡采本 set 的所有 item（小时级）。
    pub fn sample_hour_all(&self) {
        for key in self.keys() {
            if let Some(item) = self.find(&key) {
                item.sample_hour();
            }
        }
    }
}

/// 五个 set 的集合，字段顺序与 Python `_sets` 元组一致（巡采顺序照抄）。
#[derive(Debug, Clone)]
struct Sets {
    topic_and_group_pull_rt: StatsItemSet,
    topic_and_group_pull_tps: StatsItemSet,
    topic_and_group_consume_rt: StatsItemSet,
    topic_and_group_consume_ok_tps: StatsItemSet,
    topic_and_group_consume_failed_tps: StatsItemSet,
}

impl Sets {
    fn as_slice(&self) -> [&StatsItemSet; 5] {
        [
            &self.topic_and_group_pull_rt,
            &self.topic_and_group_pull_tps,
            &self.topic_and_group_consume_rt,
            &self.topic_and_group_consume_ok_tps,
            &self.topic_and_group_consume_failed_tps,
        ]
    }
}

/// 采样线程的退出信号（对应 Python 的 `threading.Event`）。
#[derive(Debug, Default)]
struct StopSignal {
    stopped: AtomicBool,
    notify: Notify,
}

impl StopSignal {
    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn reset(&self) {
        self.stopped.store(false, Ordering::Release);
    }

    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    async fn wait(&self) {
        loop {
            if self.is_stopped() {
                return;
            }
            self.notify.notified().await;
        }
    }
}

struct Inner {
    sets: Sets,
    sampler: Mutex<Option<JoinHandle<()>>>,
    /// 单独放在 `Arc` 里：采样任务强引用它、弱引用 `Inner`，
    /// 这样 manager 被丢弃时任务能自行退出，而 shutdown 又能立刻唤醒它。
    stop: Arc<StopSignal>,
    sampling_interval: Duration,
    hour_every_rounds: u64,
}

/// 消费统计管理器（Java `ConsumerStatsManager`）。
///
/// 五个 [`StatsItemSet`]，key 一律是 `topic@group`：
/// PULL_RT / PULL_TPS / CONSUME_RT / CONSUME_OK_TPS / CONSUME_FAILED_TPS。
/// [`ConsumerStatsManager::start`] 起一个统一采样任务（10s 分钟级 + 每 60 轮即 10 分钟做小时级）。
///
/// `Clone` 共享同一个实例（内部 `Arc`），可以直接 `Arc::new` 给客户端在任务间传。
#[derive(Clone)]
pub struct ConsumerStatsManager {
    inner: Arc<Inner>,
}

impl fmt::Debug for ConsumerStatsManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumerStatsManager")
            .field("pull_rt", &self.inner.sets.topic_and_group_pull_rt.keys())
            .field("sampling_interval", &self.inner.sampling_interval)
            .field("running", &self.is_running())
            .finish()
    }
}

impl Default for ConsumerStatsManager {
    fn default() -> ConsumerStatsManager {
        ConsumerStatsManager::new()
    }
}

impl ConsumerStatsManager {
    /// 对应 Python `ConsumerStatsManager()`：五个 set + 默认 10s/600s 采样参数。
    pub fn new() -> ConsumerStatsManager {
        ConsumerStatsManager::with_sampling(
            Duration::from_secs_f64(SAMPLING_INTERVAL_SECONDS),
            HOUR_SAMPLING_ROUNDS,
        )
    }

    /// 注入采样周期与"每多少轮做一次小时采样"，测试用毫秒级周期跑完整闭环。
    ///
    /// `hour_every_rounds == 0` 表示关闭小时采样（避免取模除零）。
    pub fn with_sampling(
        sampling_interval: Duration,
        hour_every_rounds: u64,
    ) -> ConsumerStatsManager {
        ConsumerStatsManager {
            inner: Arc::new(Inner {
                sets: Sets {
                    topic_and_group_pull_rt: StatsItemSet::new("PULL_RT"),
                    topic_and_group_pull_tps: StatsItemSet::new("PULL_TPS"),
                    topic_and_group_consume_rt: StatsItemSet::new("CONSUME_RT"),
                    topic_and_group_consume_ok_tps: StatsItemSet::new("CONSUME_OK_TPS"),
                    topic_and_group_consume_failed_tps: StatsItemSet::new("CONSUME_FAILED_TPS"),
                },
                sampler: Mutex::new(None),
                stop: Arc::new(StopSignal::default()),
                sampling_interval,
                hour_every_rounds,
            }),
        }
    }

    // ---------------- 生命周期 ----------------

    /// 对应 Python `start()`：起统一采样任务。
    ///
    /// 需要当前线程在 tokio 运行时里（`MQClientInstance` 的 start 本来就跑在异步上下文）；
    /// 不在运行时里时**返回 Err**（Python 是 daemon 线程，无此约束）。
    pub fn start(&self) -> Result<()> {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                self.start_with_handle(&handle);
                Ok(())
            }
            Err(_) => bail!(
                "ConsumerStatsManager::start needs a tokio runtime; call it inside an async \
                 context or use start_with_handle(&handle)"
            ),
        }
    }

    /// 显式给运行时的 [`crate::remoting::client::RemotingClient::runtime_handle`]。
    /// 重复调用是 no-op（Python `start()` 见到已有线程就直接 return）。
    pub fn start_with_handle(&self, handle: &tokio::runtime::Handle) {
        let mut slot = lock(&self.inner.sampler);
        if slot.as_ref().is_some_and(|task| !task.is_finished()) {
            return;
        }
        self.inner.stop.reset();
        let inner = Arc::downgrade(&self.inner);
        let stop = Arc::clone(&self.inner.stop);
        let interval = self.inner.sampling_interval;
        *slot = Some(handle.spawn(sample_loop(inner, stop, interval)));
        drop(slot);
        rmq_debug!("consumer stats sampler started, interval={interval:?}");
    }

    /// 对应 Python `shutdown()`：置停止位、唤醒并 abort 采样任务。
    ///
    /// Python 是 `join(timeout=3)`；这里不阻塞（同步上下文不能 block_on），
    /// abort 后任务立即消失，之后再 `start()` 会重新起一个。
    pub fn shutdown(&self) {
        self.inner.stop.stop();
        let task = lock(&self.inner.sampler).take();
        if let Some(task) = task {
            task.abort();
        }
        rmq_debug!("consumer stats sampler shutdown");
    }

    /// 采样任务是否在跑（Python 测试里的 `_thread is not None`）。
    pub fn is_running(&self) -> bool {
        lock(&self.inner.sampler)
            .as_ref()
            .is_some_and(|task| !task.is_finished())
    }

    // ---------------- 巡采（Python `_sample_loop` 的循环体） ----------------

    /// 全量巡采一轮分钟点。
    pub fn sample_all(&self) {
        for set in self.inner.sets.as_slice() {
            set.sample_all();
        }
    }

    /// 全量巡采一轮小时点。
    pub fn sample_hour_all(&self) {
        for set in self.inner.sets.as_slice() {
            set.sample_hour_all();
        }
    }

    // ---------------- 记数（Java ConsumerStatsManager 同名方法） ----------------

    /// Python `_key(topic, group)`：`"%s@%s" % (topic, group)`，注意是 **topic 在前**。
    pub fn key(topic: &str, group: &str) -> String {
        format!("{topic}@{group}")
    }

    /// 对应 Java `incPullRT`：value 累加耗时、times 累加 1。
    pub fn inc_pull_rt(&self, group: &str, topic: &str, rt: i64) {
        self.inner
            .sets
            .topic_and_group_pull_rt
            .add_value(&Self::key(topic, group), rt, 1);
    }

    /// 对应 Java `incPullTPS`：value 累加**消息数**、times 累加 1。
    pub fn inc_pull_tps(&self, group: &str, topic: &str, msgs: i64) {
        self.inner
            .sets
            .topic_and_group_pull_tps
            .add_value(&Self::key(topic, group), msgs, 1);
    }

    /// 对应 Java `incConsumeRT`。
    pub fn inc_consume_rt(&self, group: &str, topic: &str, rt: i64) {
        self.inner
            .sets
            .topic_and_group_consume_rt
            .add_value(&Self::key(topic, group), rt, 1);
    }

    /// 对应 Java `incConsumeOKTPS`。
    pub fn inc_consume_ok_tps(&self, group: &str, topic: &str, msgs: i64) {
        self.inner
            .sets
            .topic_and_group_consume_ok_tps
            .add_value(&Self::key(topic, group), msgs, 1);
    }

    /// 对应 Java `incConsumeFailedTPS`。
    pub fn inc_consume_failed_tps(&self, group: &str, topic: &str, msgs: i64) {
        self.inner
            .sets
            .topic_and_group_consume_failed_tps
            .add_value(&Self::key(topic, group), msgs, 1);
    }

    // ---------------- 查询 ----------------

    /// 五个 set 的句柄（Python 的同名公开属性）。
    pub fn topic_and_group_pull_rt(&self) -> StatsItemSet {
        self.inner.sets.topic_and_group_pull_rt.clone()
    }

    /// 见 [`ConsumerStatsManager::topic_and_group_pull_rt`]。
    pub fn topic_and_group_pull_tps(&self) -> StatsItemSet {
        self.inner.sets.topic_and_group_pull_tps.clone()
    }

    /// 见 [`ConsumerStatsManager::topic_and_group_pull_rt`]。
    pub fn topic_and_group_consume_rt(&self) -> StatsItemSet {
        self.inner.sets.topic_and_group_consume_rt.clone()
    }

    /// 见 [`ConsumerStatsManager::topic_and_group_pull_rt`]。
    pub fn topic_and_group_consume_ok_tps(&self) -> StatsItemSet {
        self.inner.sets.topic_and_group_consume_ok_tps.clone()
    }

    /// 见 [`ConsumerStatsManager::topic_and_group_pull_rt`]。
    pub fn topic_and_group_consume_failed_tps(&self) -> StatsItemSet {
        self.inner.sets.topic_and_group_consume_failed_tps.clone()
    }

    /// Java `ConsumerStatsManager.consumeStatus`：全部取 minute 快照；
    /// `consumeFailedMsgs` 取 failed 的 **hour** 窗口 sum（Java 特意跨窗口，照抄）。
    ///
    /// 返回的是既有的 [`ConsumeStatus`]（`remoting::protocol::body`，即 Java 的
    /// `ConsumerRunningInfo.statusTable` 的 value），**没有**再造第二个 ConsumeStatus 类型。
    pub fn consume_status(&self, group: &str, topic: &str) -> ConsumeStatus {
        let key = Self::key(topic, group);
        let mut cs = ConsumeStatus::default();
        if let Some(item) = self.inner.sets.topic_and_group_pull_rt.find(&key) {
            cs.pull_rt = item.get_stats_data_in_minute().avgpt;
        }
        if let Some(item) = self.inner.sets.topic_and_group_pull_tps.find(&key) {
            cs.pull_tps = item.get_stats_data_in_minute().tps;
        }
        if let Some(item) = self.inner.sets.topic_and_group_consume_rt.find(&key) {
            cs.consume_rt = item.get_stats_data_in_minute().avgpt;
        }
        if let Some(item) = self.inner.sets.topic_and_group_consume_ok_tps.find(&key) {
            cs.consume_ok_tps = item.get_stats_data_in_minute().tps;
        }
        if let Some(item) = self.inner.sets.topic_and_group_consume_failed_tps.find(&key) {
            cs.consume_failed_tps = item.get_stats_data_in_minute().tps;
            cs.consume_failed_msgs = item.get_stats_data_in_hour().sum;
        }
        cs
    }
}

/// 一轮巡采：分钟点必采，小时点按 `rounds % hour_every_rounds == 0` 决定
/// （Python `_sample_loop` 的循环体，`rounds` 从 1 开始）。
fn sample_round(inner: &Inner, rounds: u64) {
    for set in inner.sets.as_slice() {
        set.sample_all();
    }
    if inner.hour_every_rounds != 0 && rounds.is_multiple_of(inner.hour_every_rounds) {
        for set in inner.sets.as_slice() {
            set.sample_hour_all();
        }
    }
}

/// 采样任务主体：先等一个周期再巡采（Python `_stop.wait(10)` 是"先等后采"的次序）。
///
/// 只强引用 `StopSignal`、弱引用 `Inner`：manager 被丢弃后任务自行退出，
/// 对应 Python 的 daemon 线程不会把进程拖住。
async fn sample_loop(
    inner: Weak<Inner>,
    stop: Arc<StopSignal>,
    sampling_interval: Duration,
) {
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + sampling_interval,
        sampling_interval,
    );
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut rounds: u64 = 0;
    loop {
        tokio::select! {
            biased;
            () = stop.wait() => break,
            _ = ticker.tick() => {}
        }
        if stop.is_stopped() {
            break;
        }
        let Some(inner) = inner.upgrade() else {
            // manager 已丢弃 —— 等价于 Python daemon 线程随进程退出
            rmq_debug!("consumer stats sampler exits: manager dropped");
            return;
        };
        rounds += 1;
        sample_round(&inner, rounds);
        drop(inner);
        if stop.is_stopped() {
            break;
        }
    }
    rmq_debug!("consumer stats sampler exits: stopped after {rounds} rounds");
}

#[cfg(test)]
mod tests {
    use super::*;

    const GROUP: &str = "GID_StatsUnit";
    const TOPIC: &str = "StatsTopic";
    const KEY: &str = "StatsTopic@GID_StatsUnit";

    fn almost(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-6
    }

    // ---------------- compute_stats_data ----------------

    #[test]
    fn empty_chain_gives_zero_snapshot() {
        let ss = compute_stats_data(&[]);
        assert_eq!(ss.sum, 0);
        assert_eq!(ss.times, 0);
        assert!(almost(ss.tps, 0.0));
        assert!(almost(ss.avgpt, 0.0));
    }

    #[test]
    fn java_formula_on_two_points() {
        // 两个累计点：10 秒内 value 增 30、times 增 3 -> tps 3/s、avgpt 10
        let ss = compute_stats_data(&[(0, 100, 10), (10_000, 130, 13)]);
        assert_eq!(ss.sum, 30);
        assert!(almost(ss.tps, 3.0));
        assert!(almost(ss.avgpt, 10.0));
        assert_eq!(ss.times, 3);
        // Python __repr__ 对拍
        assert_eq!(
            ss.to_string(),
            "StatsSnapshot(sum=30, tps=3.00, avgpt=10.00, times=3)"
        );
    }

    #[test]
    fn single_point_has_zero_span_and_zero_tps() {
        // 只有一个点：span=0 -> tps=0（Python 的 span_ms > 0 保护；Java 会给 Infinity）
        let ss = compute_stats_data(&[(1_000, 5, 1)]);
        assert_eq!(ss.sum, 0);
        assert!(almost(ss.tps, 0.0));
        assert!(almost(ss.avgpt, 0.0));
        assert_eq!(ss.times, 0);
    }

    #[test]
    fn reversed_chain_keeps_negative_sums_but_zero_rates() {
        // 对拍 python: compute_stats_data([(10000,130,13),(0,100,10)])
        // -> sum=-30 tps=0.00 avgpt=0.00 times=-3
        let ss = compute_stats_data(&[(10_000, 130, 13), (0, 100, 10)]);
        assert_eq!(ss.sum, -30);
        assert!(almost(ss.tps, 0.0));
        assert!(almost(ss.avgpt, 0.0));
        assert_eq!(ss.times, -3);
    }

    #[test]
    fn zero_times_diff_gives_zero_avgpt_even_with_positive_sum() {
        // 对拍 python: [(0,100,10),(1000,400,10)] -> sum=300 tps=300 avgpt=0 times=0
        let ss = compute_stats_data(&[(0, 100, 10), (1_000, 400, 10)]);
        assert_eq!(ss.sum, 300);
        assert!(almost(ss.tps, 300.0));
        assert!(almost(ss.avgpt, 0.0));
        assert_eq!(ss.times, 0);
    }

    #[test]
    fn fractional_rates_are_plain_float_division() {
        // 对拍 python: [(0,0,0),(3000,10,3)] -> tps=3.33.. avgpt=3.33..
        let ss = compute_stats_data(&[(0, 0, 0), (3_000, 10, 3)]);
        assert!(almost(ss.tps, 10.0 / 3.0), "{ss}");
        assert!(almost(ss.avgpt, 10.0 / 3.0), "{ss}");
        assert_eq!(ss.sum, 10);
        assert_eq!(ss.times, 3);
    }

    #[test]
    fn sampling_constants_match_reference() {
        assert!(almost(SAMPLING_INTERVAL_SECONDS, 10.0));
        assert!(almost(HOUR_SAMPLING_INTERVAL_SECONDS, 600.0));
        assert_eq!(MINUTE_LIST_MAX, 60);
        assert_eq!(HOUR_LIST_MAX, 60);
        assert_eq!(HOUR_SAMPLING_ROUNDS, 60);
        assert_eq!(
            Duration::from_secs_f64(SAMPLING_INTERVAL_SECONDS),
            Duration::from_secs(10)
        );
    }

    // ---------------- StatsItem ----------------

    #[test]
    fn item_accumulates_cumulative_values() {
        let it = StatsItem::new("PULL_TPS", KEY);
        it.add_value(5, 1);
        it.add_value(7, 1);
        assert_eq!(it.value(), 12);
        assert_eq!(it.times(), 2);
        assert_eq!(it.stats_name(), "PULL_TPS");
        assert_eq!(it.stats_key(), KEY);
    }

    #[test]
    fn minute_snapshot_diffs_two_cumulative_points() {
        let it = StatsItem::new("PULL_TPS", "T@G");
        it.add_value(10, 1);
        it.sample_at(1_000); // 第一个累计点
        it.add_value(15, 1);
        it.sample_at(2_000); // 第二个累计点
        let ss = it.get_stats_data_in_minute();
        assert_eq!(ss.sum, 15);
        assert_eq!(ss.times, 1);
        // 15 msgs / 1s = 15/s
        assert!(almost(ss.tps, 15.0));
        assert!(almost(ss.avgpt, 15.0));
    }

    #[test]
    fn hour_window_is_separate_from_minute() {
        // 累计差分：两点 (ts,100,2) -> (ts,400,4)：sum=300、timesDiff=2、avgpt=150
        let it = StatsItem::new("PULL_RT", "T@G");
        it.add_value(100, 2);
        it.sample_hour_at(0);
        it.add_value(300, 2);
        it.sample_hour_at(1_000);
        let ss = it.get_stats_data_in_hour();
        assert_eq!(ss.sum, 300);
        assert_eq!(ss.times, 2);
        assert!(almost(ss.avgpt, 150.0));
        // 分钟链没被小时采样污染
        assert_eq!(it.get_stats_data_in_minute(), StatsSnapshot::default());
    }

    #[test]
    fn both_chains_are_capped_and_oldest_points_are_dropped() {
        // 对拍 python（80 轮，每轮 add_value(1,1) 后按 ts=i 采样）：
        //   链封顶 60 个点，first=(20,21,21)、last=(79,80,80)
        //   -> sum=59 tps=1000.00 avgpt=1.00 times=59
        let it = StatsItem::new("PULL_TPS", "T@G");
        for i in 0..80_i64 {
            it.add_value(1, 1);
            it.sample_at(i);
            it.sample_hour_at(i);
        }
        assert_eq!(it.minute_len(), MINUTE_LIST_MAX);
        assert_eq!(it.hour_len(), HOUR_LIST_MAX);
        let ss = it.get_stats_data_in_minute();
        assert_eq!(ss.sum, 59);
        assert_eq!(ss.times, 59);
        assert!(almost(ss.tps, 1_000.0), "{ss}");
        assert!(almost(ss.avgpt, 1.0), "{ss}");
        assert!(almost(it.get_stats_data_in_hour().tps, 1_000.0));
    }

    #[test]
    fn sample_uses_wall_clock_chain() {
        let it = StatsItem::new("PULL_TPS", "T@G");
        it.add_value(3, 1);
        it.sample();
        it.add_value(4, 1);
        it.sample();
        let ss = it.get_stats_data_in_minute();
        assert_eq!(ss.sum, 4);
        assert_eq!(ss.times, 1);
        assert!(ss.tps >= 0.0);
    }

    // ---------------- StatsItemSet ----------------

    #[test]
    fn get_and_create_is_idempotent() {
        let s = StatsItemSet::new("X");
        let a = s.get_and_create("k");
        let b = s.get_and_create("k");
        assert!(Arc::ptr_eq(&a, &b));
        a.add_value(1, 1);
        assert_eq!(b.value(), 1);
        assert_eq!(a.stats_name(), "X");
        assert_eq!(s.keys(), vec!["k".to_string()]);
    }

    #[test]
    fn add_value_creates_and_accumulates() {
        let s = StatsItemSet::new("X");
        s.add_value(KEY, 3, 1);
        s.add_value(KEY, 4, 1);
        let it = s.find(KEY).expect("created");
        assert_eq!(it.value(), 7);
        assert_eq!(it.times(), 2);
        assert!(s.find("other").is_none());
    }

    #[test]
    fn sample_all_walks_every_item_in_both_granularities() {
        let s = StatsItemSet::new("PULL_TPS");
        s.add_value("a", 1, 1);
        s.add_value("b", 2, 1);
        s.sample_all();
        s.sample_hour_all();
        for key in ["a", "b"] {
            let it = s.find(key).expect("present");
            assert_eq!(it.minute_len(), 1);
            assert_eq!(it.hour_len(), 1);
        }
        assert_eq!(s.keys(), vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn cloned_set_shares_the_same_map() {
        let s = StatsItemSet::new("PULL_RT");
        let c = s.clone();
        c.add_value("k", 9, 1);
        assert_eq!(s.find("k").expect("shared").value(), 9);
        assert_eq!(c.stats_name(), "PULL_RT");
    }

    // ---------------- ConsumerStatsManager ----------------

    #[test]
    fn inc_semantics_match_java() {
        // TPS 类：value=消息数、times=调用次数；RT 类：value=耗时、times=调用次数
        let m = ConsumerStatsManager::new();
        m.inc_pull_tps(GROUP, TOPIC, 10);
        m.inc_pull_tps(GROUP, TOPIC, 20);
        let item = m
            .topic_and_group_pull_tps()
            .find(KEY)
            .expect("pull_tps item");
        assert_eq!(item.value(), 30);
        assert_eq!(item.times(), 2);

        m.inc_pull_rt(GROUP, TOPIC, 25);
        let rt = m
            .topic_and_group_pull_rt()
            .find(KEY)
            .expect("pull_rt item");
        assert_eq!(rt.value(), 25);
        assert_eq!(rt.times(), 1);
    }

    #[test]
    fn key_is_topic_at_group_and_set_names_follow_java() {
        let m = ConsumerStatsManager::new();
        assert_eq!(ConsumerStatsManager::key(TOPIC, GROUP), KEY);
        m.inc_consume_ok_tps("myGroup", "myTopic", 1);
        assert!(m
            .topic_and_group_consume_ok_tps()
            .find("myTopic@myGroup")
            .is_some());
        assert_eq!(m.topic_and_group_pull_rt().stats_name(), "PULL_RT");
        assert_eq!(m.topic_and_group_pull_tps().stats_name(), "PULL_TPS");
        assert_eq!(m.topic_and_group_consume_rt().stats_name(), "CONSUME_RT");
        assert_eq!(
            m.topic_and_group_consume_ok_tps().stats_name(),
            "CONSUME_OK_TPS"
        );
        assert_eq!(
            m.topic_and_group_consume_failed_tps().stats_name(),
            "CONSUME_FAILED_TPS"
        );
    }

    #[test]
    fn consume_status_is_all_zero_when_untouched() {
        let m = ConsumerStatsManager::new();
        let cs = m.consume_status(GROUP, TOPIC);
        assert_eq!(cs, ConsumeStatus::default());
        assert!(almost(cs.pull_rt, 0.0));
        assert!(almost(cs.pull_tps, 0.0));
        assert!(almost(cs.consume_rt, 0.0));
        assert!(almost(cs.consume_ok_tps, 0.0));
        assert!(almost(cs.consume_failed_tps, 0.0));
        assert_eq!(cs.consume_failed_msgs, 0);
    }

    /// 直接喂累计快照点（间隔 1s），绕开采样任务。
    fn feed(set: &StatsItemSet, now_ms: i64) {
        let it = set.find(KEY).expect("item created by inc_*");
        let points = vec![(now_ms - 1_000, 0, 0), (now_ms, it.value(), it.times())];
        {
            let mut state = lock(&it.state);
            state.minute.clear();
            state.minute.extend(points);
        }
    }

    #[test]
    fn consume_status_maps_rt_to_avgpt_and_tps_to_tps() {
        // 对拍 python/tests/test_consumer_stats.py 的同名用例：
        //   pull_rt value=200/times=4 -> avgpt=50ms
        //   consume_rt value=40/times=4 -> avgpt=10ms
        //   pull_tps value=20 -> tps=20/s（消息每秒）
        //   consume_ok_tps value=8 -> tps=8/s
        let m = ConsumerStatsManager::new();
        let now = current_time_millis();
        for _ in 0..4 {
            m.inc_pull_rt(GROUP, TOPIC, 50);
            m.inc_consume_rt(GROUP, TOPIC, 10);
            m.inc_pull_tps(GROUP, TOPIC, 5);
            m.inc_consume_ok_tps(GROUP, TOPIC, 2);
        }
        feed(&m.topic_and_group_pull_rt(), now);
        feed(&m.topic_and_group_consume_rt(), now);
        feed(&m.topic_and_group_pull_tps(), now);
        feed(&m.topic_and_group_consume_ok_tps(), now);
        let cs = m.consume_status(GROUP, TOPIC);
        assert!(almost(cs.pull_rt, 50.0), "{cs:?}");
        assert!(almost(cs.consume_rt, 10.0), "{cs:?}");
        assert!(almost(cs.pull_tps, 20.0), "{cs:?}");
        assert!(almost(cs.consume_ok_tps, 8.0), "{cs:?}");
        // 没碰过的 failed 维度保持 0
        assert!(almost(cs.consume_failed_tps, 0.0));
        assert_eq!(cs.consume_failed_msgs, 0);
    }

    #[test]
    fn consume_failed_msgs_reads_the_hour_window() {
        let m = ConsumerStatsManager::new();
        m.inc_consume_failed_tps(GROUP, TOPIC, 7);
        let it = m
            .topic_and_group_consume_failed_tps()
            .find(KEY)
            .expect("failed item");
        let now = current_time_millis();
        {
            let mut state = lock(&it.state);
            state.hour.push_back((now - 1_000, 0, 0));
            state.hour.push_back((now, 7, 1));
        }
        let cs = m.consume_status(GROUP, TOPIC);
        assert_eq!(cs.consume_failed_msgs, 7);
        // 分钟链里没点 -> tps 走 0，而不是 hour 的值
        assert!(almost(cs.consume_failed_tps, 0.0));
    }

    #[test]
    fn round_robin_follows_the_mod_60_rule() {
        // rounds % hour_every_rounds == 0 才采小时点（这里把 60 换成 3 便于断言）
        let m = ConsumerStatsManager::with_sampling(Duration::from_millis(1), 3);
        m.inc_pull_tps(GROUP, TOPIC, 1);
        for rounds in 1..=3 {
            sample_round(&m.inner, rounds);
        }
        let it = m
            .topic_and_group_pull_tps()
            .find(KEY)
            .expect("item");
        assert_eq!(it.minute_len(), 3);
        assert_eq!(it.hour_len(), 1);
        sample_round(&m.inner, 4);
        assert_eq!(it.minute_len(), 4);
        assert_eq!(it.hour_len(), 1);
        sample_round(&m.inner, 6);
        assert_eq!(it.hour_len(), 2);
    }

    #[test]
    fn round_robin_with_zero_hour_period_never_samples_hour() {
        // 除零保护：hour_every_rounds == 0 表示关闭小时采样
        let m = ConsumerStatsManager::with_sampling(Duration::from_millis(1), 0);
        m.inc_pull_rt(GROUP, TOPIC, 5);
        sample_round(&m.inner, 60);
        assert_eq!(
            m.topic_and_group_pull_rt()
                .find(KEY)
                .expect("item")
                .hour_len(),
            0
        );
    }

    #[test]
    fn manager_sample_helpers_walk_all_five_sets() {
        let m = ConsumerStatsManager::new();
        m.inc_pull_rt(GROUP, TOPIC, 1);
        m.inc_pull_tps(GROUP, TOPIC, 1);
        m.inc_consume_rt(GROUP, TOPIC, 1);
        m.inc_consume_ok_tps(GROUP, TOPIC, 1);
        m.inc_consume_failed_tps(GROUP, TOPIC, 1);
        m.sample_all();
        m.sample_hour_all();
        for set in [
            m.topic_and_group_pull_rt(),
            m.topic_and_group_pull_tps(),
            m.topic_and_group_consume_rt(),
            m.topic_and_group_consume_ok_tps(),
            m.topic_and_group_consume_failed_tps(),
        ] {
            let it = set.find(KEY).expect("item");
            assert_eq!(it.minute_len(), 1, "{}", set.stats_name());
            assert_eq!(it.hour_len(), 1, "{}", set.stats_name());
        }
    }

    #[test]
    fn cloned_manager_shares_the_same_counters() {
        let m = ConsumerStatsManager::new();
        let c = m.clone();
        m.inc_pull_rt(GROUP, TOPIC, 8);
        assert_eq!(
            c.topic_and_group_pull_rt()
                .find(KEY)
                .expect("shared")
                .value(),
            8
        );
        assert_eq!(c.topic_and_group_pull_rt().stats_name(), "PULL_RT");
    }

    /// 把采样任务从槽里取出来，好让测试自己观察它的退出（`take` 后 shutdown 就没有
    /// 可 abort 的句柄了，于是退出只能靠停止位 + Notify，正是想验证的路径）。
    fn take_sampler(m: &ConsumerStatsManager) -> JoinHandle<()> {
        lock(&m.inner.sampler).take().expect("sampler started")
    }

    #[tokio::test]
    async fn sampler_registers_stops_and_can_restart() {
        let m = ConsumerStatsManager::with_sampling(Duration::from_millis(2), 500);
        m.inc_pull_tps(GROUP, TOPIC, 4);
        assert!(!m.is_running());
        m.start().expect("start inside a tokio runtime");
        assert!(m.is_running());
        // 重复 start 是 no-op（Python: _thread 已存在就直接 return）
        m.start().expect("start inside a tokio runtime");
        assert!(m.is_running());

        wait_for(|| minute_points(&m) >= 2).await;

        let task = take_sampler(&m);
        m.shutdown();
        assert!(!m.is_running());
        assert!(m.inner.stop.is_stopped());
        // 没有 abort 可用，任务必须被停止位叫醒后自行退出
        task.await.expect("sampler exits without panic");
        let after_stop = minute_points(&m);

        // shutdown 之后可以重新 start（Python 里 _thread 被清空、_stop 被 clear）
        m.start().expect("start inside a tokio runtime");
        assert!(m.is_running());
        assert!(!m.inner.stop.is_stopped());
        wait_for(|| minute_points(&m) > after_stop).await;
        m.shutdown();
        assert!(!m.is_running());
    }

    fn minute_points(m: &ConsumerStatsManager) -> usize {
        m.topic_and_group_pull_tps()
            .find(KEY)
            .map(|i| i.minute_len())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn sampler_honours_the_injected_hour_round_period() {
        // 周期 1ms、每 2 轮一次小时采样：小时链涨 1 个点时分钟链至少涨 2 个
        let m = ConsumerStatsManager::with_sampling(Duration::from_millis(1), 2);
        m.inc_consume_ok_tps(GROUP, TOPIC, 3);
        m.start_with_handle(&tokio::runtime::Handle::current());
        wait_for(|| {
            m.topic_and_group_consume_ok_tps()
                .find(KEY)
                .map(|i| i.hour_len())
                .unwrap_or(0)
                >= 1
        })
        .await;
        let it = m
            .topic_and_group_consume_ok_tps()
            .find(KEY)
            .expect("item");
        assert!(it.minute_len() + 1 >= it.hour_len() * 2, "{} / {}", it.minute_len(), it.hour_len());
        m.shutdown();
    }

    #[tokio::test]
    async fn sampler_task_exits_when_manager_is_dropped() {
        // manager 被丢弃后任务自行退出（Python daemon 线程语义）：不 panic、不吊着内存
        let m = ConsumerStatsManager::with_sampling(Duration::from_millis(1), 60);
        m.inc_pull_tps(GROUP, TOPIC, 1);
        m.start_with_handle(&tokio::runtime::Handle::current());
        let task = take_sampler(&m);
        wait_for(|| minute_points(&m) >= 1).await;
        drop(m);
        let outcome = tokio::time::timeout(Duration::from_secs(5), task).await;
        assert!(outcome.is_ok(), "manager dropped 后采样任务没退出");
        outcome.expect("joined").expect("sampler exits without panic");
    }

    #[test]
    fn start_without_runtime_returns_error_instead_of_panicking() {
        // 非异步上下文里 start() 必须返回 Err（内部用 Handle::try_current，绝不 panic）
        let m = ConsumerStatsManager::new();
        let outcome = std::thread::spawn(move || m.start())
            .join()
            .expect("thread ran to completion");
        match outcome {
            Err(crate::error::Error::Client { message, .. }) => {
                assert!(message.contains("tokio runtime"), "{message}");
            }
            other => panic!("expected a MQClientException-style error, got {other:?}"),
        }
    }

    #[test]
    fn types_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StatsItem>();
        assert_send_sync::<StatsItemSet>();
        assert_send_sync::<ConsumerStatsManager>();
        assert_send_sync::<StatsSnapshot>();
    }

    #[test]
    fn stats_snapshot_display_matches_python_repr() {
        assert_eq!(
            StatsSnapshot::default().to_string(),
            "StatsSnapshot(sum=0, tps=0.00, avgpt=0.00, times=0)"
        );
        assert_eq!(
            compute_stats_data(&[(0, 0, 0), (3_000, 10, 3)]).to_string(),
            "StatsSnapshot(sum=10, tps=3.33, avgpt=3.33, times=3)"
        );
    }

    /// 轮询等待条件成立（有上限，避免睡墙钟带来的偶发失败）。
    async fn wait_for<F: FnMut() -> bool>(mut predicate: F) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !predicate() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "sampler did not produce samples in time"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}
