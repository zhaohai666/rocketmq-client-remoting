//! * [`StatsItem`] holds the **cumulative** value / times (only ever growing), plus two sampled snapshot chains:
//!   `minute` (one cumulative point every 10s) and `hour` (one cumulative point every 10 minutes);
//! * snapshot math in [`compute_stats_data`]:
//!
//!   ```text
//!   sum       = last.value - first.value             # the delta within the window
//!   tps       = sum * 1000.0 / (last.ts - first.ts)  # per second (mind: not per minute!)
//!   timesDiff = last.times - first.times
//!   avgpt     = timesDiff > 0 ? sum / timesDiff : 0  # "average per call" -- for the RT item that is the average latency
//!   ```

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

pub const SAMPLING_INTERVAL_SECONDS: f64 = 10.0;
pub const HOUR_SAMPLING_INTERVAL_SECONDS: f64 = 600.0;
pub const DAY_SAMPLING_INTERVAL_SECONDS: f64 = 3600.0;
/// Minute snapshot chain length (10s samples).
pub const MINUTE_LIST_MAX: usize = 7;
/// Hour snapshot chain length (10min samples).
pub const HOUR_LIST_MAX: usize = 7;
/// Day snapshot chain length (1h samples).
pub const DAY_LIST_MAX: usize = 25;
/// How many minute-sampling rounds make one hour sample: `600s / 10s = 60` (the hardcoded `% 60`).
pub const HOUR_SAMPLING_ROUNDS: u64 =
    (HOUR_SAMPLING_INTERVAL_SECONDS / SAMPLING_INTERVAL_SECONDS) as u64;
/// How many minute-sampling rounds make one day sample: `3600s / 10s = 360`.
pub const DAY_SAMPLING_ROUNDS: u64 =
    (DAY_SAMPLING_INTERVAL_SECONDS / SAMPLING_INTERVAL_SECONDS) as u64;

pub type CallSnapshot = (i64, i64, i64);

/// On lock poisoning, take the inner value anyway -- the stats path must never let a panic drag business threads down.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct StatsSnapshot {
    /// The cumulative delta within the window.
    pub sum: i64,
    /// Per-second rate (`sum * 1000 / span_ms`).
    pub tps: f64,
    /// Average per call (for the RT item, the average latency in milliseconds).
    pub avgpt: f64,
    /// The call-count delta within the window.
    pub times: i64,
}

impl fmt::Display for StatsSnapshot {
    /// Fixed display format, verified verbatim (tps/avgpt keep two decimals).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StatsSnapshot(sum={}, tps={:.2}, avgpt={:.2}, times={})",
            self.sum, self.tps, self.avgpt, self.times
        )
    }
}

pub fn compute_stats_data(cs_list: &[CallSnapshot]) -> StatsSnapshot {
    compute_stats_data_pair(cs_list.first(), cs_list.last())
}

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
    /// Element = `(timestamp_ms, cumulative value, cumulative times)`.
    minute: VecDeque<CallSnapshot>,
    hour: VecDeque<CallSnapshot>,
    day: VecDeque<CallSnapshot>,
}

impl StatsItem {
    /// Creates a stats item: `stats_name` + `stats_key` (the key is usually `topic@group`).
    pub fn new(stats_name: impl Into<String>, stats_key: impl Into<String>) -> StatsItem {
        StatsItem {
            stats_name: stats_name.into(),
            stats_key: stats_key.into(),
            state: Mutex::new(ItemState::default()),
        }
    }

    /// The stats name (`PULL_RT` / `PULL_TPS` / ...).
    pub fn stats_name(&self) -> &str {
        &self.stats_name
    }

    /// The stats key (`topic@group`).
    pub fn stats_key(&self) -> &str {
        &self.stats_key
    }

    pub fn add_value(&self, inc_value: i64, inc_times: i64) {
        let mut state = lock(&self.state);
        state.value += inc_value;
        state.times += inc_times;
    }

    /// The current cumulative value.
    pub fn value(&self) -> i64 {
        lock(&self.state).value
    }

    /// The current cumulative times.
    pub fn times(&self) -> i64 {
        lock(&self.state).times
    }

    /// Appends a minute-level sample point (called by the sampler every 10s); the clock is `current_time_millis()`.
    ///
    /// Subtle difference: the clock is read before taking the lock -- the sample point contents
    /// (cumulative value/times) are still read under the lock, same accounting as before.
    pub fn sample(&self) {
        let ts = current_time_millis();
        self.sample_at(ts);
    }

    /// Explicit-clock variant of [`StatsItem::sample`] (lets tests inject the sampling moment):
    /// both tests and "back-filling historical points" rely on it, with no wall-clock sleeping.
    pub fn sample_at(&self, ts_ms: i64) {
        let mut state = lock(&self.state);
        if state.minute.is_empty() {
            state.minute.push_back((
                ts_ms - (SAMPLING_INTERVAL_SECONDS as i64) * 1000,
                0,
                0,
            ));
        }
        let point = (ts_ms, state.value, state.times);
        state.minute.push_back(point);
        while state.minute.len() > MINUTE_LIST_MAX {
            state.minute.pop_front();
        }
    }

    /// Appends an hour-level sample point (called by the sampler every 10 minutes).
    pub fn sample_hour(&self) {
        let ts = current_time_millis();
        self.sample_hour_at(ts);
    }

    /// Explicit-clock variant of [`StatsItem::sample_hour`].
    pub fn sample_hour_at(&self, ts_ms: i64) {
        let mut state = lock(&self.state);
        if state.hour.is_empty() {
            state.hour.push_back((
                ts_ms - (HOUR_SAMPLING_INTERVAL_SECONDS as i64) * 1000,
                0,
                0,
            ));
        }
        let point = (ts_ms, state.value, state.times);
        state.hour.push_back(point);
        while state.hour.len() > HOUR_LIST_MAX {
            state.hour.pop_front();
        }
    }

    /// Appends a day-level sample point (called by the sampler every hour).
    pub fn sample_day(&self) {
        let ts = current_time_millis();
        self.sample_day_at(ts);
    }

    /// Explicit-clock variant of [`StatsItem::sample_day`].
    pub fn sample_day_at(&self, ts_ms: i64) {
        let mut state = lock(&self.state);
        if state.day.is_empty() {
            state.day.push_back((
                ts_ms - (DAY_SAMPLING_INTERVAL_SECONDS as i64) * 1000,
                0,
                0,
            ));
        }
        let point = (ts_ms, state.value, state.times);
        state.day.push_back(point);
        while state.day.len() > DAY_LIST_MAX {
            state.day.pop_front();
        }
    }

    pub fn get_stats_data_in_minute(&self) -> StatsSnapshot {
        let state = lock(&self.state);
        compute_stats_data_pair(state.minute.front(), state.minute.back())
    }

    pub fn get_stats_data_in_hour(&self) -> StatsSnapshot {
        let state = lock(&self.state);
        compute_stats_data_pair(state.hour.front(), state.hour.back())
    }

    pub fn get_stats_data_in_day(&self) -> StatsSnapshot {
        let state = lock(&self.state);
        compute_stats_data_pair(state.day.front(), state.day.back())
    }

    /// The current minute-chain length.
    pub fn minute_len(&self) -> usize {
        lock(&self.state).minute.len()
    }

    /// The current hour-chain length.
    pub fn hour_len(&self) -> usize {
        lock(&self.state).hour.len()
    }

    /// The current day-chain length.
    pub fn day_len(&self) -> usize {
        lock(&self.state).day.len()
    }
}

/// `Clone` is another handle onto **the same table** (internally `Arc<Mutex<BTreeMap>>`),
/// so the manager can hand a set to callers for long-term holding.
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
    /// Creates an item set by name.
    pub fn new(stats_name: impl Into<String>) -> StatsItemSet {
        StatsItemSet {
            stats_name: stats_name.into(),
            items: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn stats_name(&self) -> &str {
        &self.stats_name
    }

    /// Creates when absent; when present, returns the same item.
    pub fn get_and_create(&self, key: &str) -> Arc<StatsItem> {
        let mut items = lock(&self.items);
        items
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(StatsItem::new(self.stats_name.clone(), key)))
            .clone()
    }

    /// Lookup only, never creates: returns None when absent (**no** creation; `consume_status` relies on this to skip zero values).
    pub fn find(&self, key: &str) -> Option<Arc<StatsItem>> {
        lock(&self.items).get(key).cloned()
    }

    /// Gets or creates, then accumulates.
    pub fn add_value(&self, key: &str, inc_value: i64, inc_times: i64) {
        self.get_and_create(key).add_value(inc_value, inc_times);
    }

    /// Existing keys (lexicographic; see difference note 5 in the module header).
    pub fn keys(&self) -> Vec<String> {
        lock(&self.items).keys().cloned().collect()
    }

    /// Samples every item in this set (minute level).
    pub fn sample_all(&self) {
        for key in self.keys() {
            if let Some(item) = self.find(&key) {
                item.sample();
            }
        }
    }

    /// Samples every item in this set (hour level).
    pub fn sample_hour_all(&self) {
        for key in self.keys() {
            if let Some(item) = self.find(&key) {
                item.sample_hour();
            }
        }
    }

    /// Samples every item in this set (day level).
    pub fn sample_day_all(&self) {
        for key in self.keys() {
            if let Some(item) = self.find(&key) {
                item.sample_day();
            }
        }
    }
}

/// The bundle of five sets; field order is the sampling order.
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

/// Exit signal for the sampling task.
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
    /// Kept in its own `Arc`: the sampling task strongly references it and weakly references `Inner`,
    /// so the task exits by itself once the manager is dropped, while shutdown can still wake it instantly.
    stop: Arc<StopSignal>,
    sampling_interval: Duration,
    hour_every_rounds: u64,
}

/// Five [`StatsItemSet`]s, keys always `topic@group`:
/// PULL_RT / PULL_TPS / CONSUME_RT / CONSUME_OK_TPS / CONSUME_FAILED_TPS.
/// [`ConsumerStatsManager::start`] spawns one unified sampling task (10s minute-level + hour-level every 60 rounds, i.e. 10 minutes).
///
/// `Clone` shares the same instance (internally an `Arc`); wrap it in `Arc::new` and pass it between the client's tasks.
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
    /// The five sets + default 10s/600s sampling parameters.
    pub fn new() -> ConsumerStatsManager {
        ConsumerStatsManager::with_sampling(
            Duration::from_secs_f64(SAMPLING_INTERVAL_SECONDS),
            HOUR_SAMPLING_ROUNDS,
        )
    }

    /// Injects the sampling period and the "hour sample every N rounds"; tests run the full loop with millisecond periods.
    ///
    /// `hour_every_rounds == 0` disables hour sampling (avoiding a modulo division by zero).
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

    // ---------------- lifecycle ----------------

    /// Starts the unified sampling task.
    ///
    /// Requires the current thread to be inside a tokio runtime (`MQClientInstance`'s start already runs in an async context);
    /// outside a tokio runtime it **returns Err** (a background task must have a host runtime).
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

    /// Explicit-runtime variant for [`crate::remoting::client::RemotingClient::runtime_handle`].
    /// A repeated call is a no-op.
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

    /// Sets the stop flag, wakes, and aborts the sampling task.
    ///
    /// It does not block here (a sync context cannot block_on);
    /// after abort the task disappears at once, and a later `start()` spawns a fresh one.
    pub fn shutdown(&self) {
        self.inner.stop.stop();
        let task = lock(&self.inner.sampler).take();
        if let Some(task) = task {
            task.abort();
        }
        rmq_debug!("consumer stats sampler shutdown");
    }

    /// Whether the sampling task is running.
    pub fn is_running(&self) -> bool {
        lock(&self.inner.sampler)
            .as_ref()
            .is_some_and(|task| !task.is_finished())
    }

    // ---------------- sampling (loop body) ----------------

    /// Samples one full round of minute points.
    pub fn sample_all(&self) {
        for set in self.inner.sets.as_slice() {
            set.sample_all();
        }
    }

    /// Samples one full round of hour points.
    pub fn sample_hour_all(&self) {
        for set in self.inner.sets.as_slice() {
            set.sample_hour_all();
        }
    }

    /// Samples one full round of day points.
    pub fn sample_day_all(&self) {
        for set in self.inner.sets.as_slice() {
            set.sample_day_all();
        }
    }


    /// The stats key: `topic@group`; note **topic comes first**.
    pub fn key(topic: &str, group: &str) -> String {
        format!("{topic}@{group}")
    }

    pub fn inc_pull_rt(&self, group: &str, topic: &str, rt: i64) {
        self.inner
            .sets
            .topic_and_group_pull_rt
            .add_value(&Self::key(topic, group), rt, 1);
    }

    pub fn inc_pull_tps(&self, group: &str, topic: &str, msgs: i64) {
        self.inner
            .sets
            .topic_and_group_pull_tps
            .add_value(&Self::key(topic, group), msgs, 1);
    }

    pub fn inc_consume_rt(&self, group: &str, topic: &str, rt: i64) {
        self.inner
            .sets
            .topic_and_group_consume_rt
            .add_value(&Self::key(topic, group), rt, 1);
    }

    pub fn inc_consume_ok_tps(&self, group: &str, topic: &str, msgs: i64) {
        self.inner
            .sets
            .topic_and_group_consume_ok_tps
            .add_value(&Self::key(topic, group), msgs, 1);
    }

    pub fn inc_consume_failed_tps(&self, group: &str, topic: &str, msgs: i64) {
        self.inner
            .sets
            .topic_and_group_consume_failed_tps
            .add_value(&Self::key(topic, group), msgs, 1);
    }

    // ---------------- queries ----------------

    /// Handles to the five sets (public accessors).
    pub fn topic_and_group_pull_rt(&self) -> StatsItemSet {
        self.inner.sets.topic_and_group_pull_rt.clone()
    }

    /// See [`ConsumerStatsManager::topic_and_group_pull_rt`].
    pub fn topic_and_group_pull_tps(&self) -> StatsItemSet {
        self.inner.sets.topic_and_group_pull_tps.clone()
    }

    /// See [`ConsumerStatsManager::topic_and_group_pull_rt`].
    pub fn topic_and_group_consume_rt(&self) -> StatsItemSet {
        self.inner.sets.topic_and_group_consume_rt.clone()
    }

    /// See [`ConsumerStatsManager::topic_and_group_pull_rt`].
    pub fn topic_and_group_consume_ok_tps(&self) -> StatsItemSet {
        self.inner.sets.topic_and_group_consume_ok_tps.clone()
    }

    /// See [`ConsumerStatsManager::topic_and_group_pull_rt`].
    pub fn topic_and_group_consume_failed_tps(&self) -> StatsItemSet {
        self.inner.sets.topic_and_group_consume_failed_tps.clone()
    }

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
            let minute = item.get_stats_data_in_minute();
            // An idle minute window says nothing; fall back to the hour window.
            cs.consume_rt = if minute.sum == 0 {
                item.get_stats_data_in_hour().avgpt
            } else {
                minute.avgpt
            };
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

/// One sampling round: minute points always, hour points only when `rounds % hour_every_rounds == 0`
/// (`rounds` starts at 1), day points every six hour rounds.
fn sample_round(inner: &Inner, rounds: u64) {
    for set in inner.sets.as_slice() {
        set.sample_all();
    }
    if inner.hour_every_rounds != 0 && rounds % inner.hour_every_rounds == 0 {
        for set in inner.sets.as_slice() {
            set.sample_hour_all();
        }
        let day_every_rounds = inner.hour_every_rounds.saturating_mul(6);
        if day_every_rounds != 0 && rounds % day_every_rounds == 0 {
            for set in inner.sets.as_slice() {
                set.sample_day_all();
            }
        }
    }
}

/// Sampling task body: wait one period first, then sample (the "wait first, sample after" order).
///
/// It strongly references only `StopSignal` and weakly `Inner`: once the manager is dropped the task exits by itself,
/// without holding the process hostage.
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
            // manager already dropped -- the task exits by itself, no panic
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
    fn tps_formula_on_two_points() {
        // two cumulative points: value +30 and times +3 over 10 seconds -> tps 3/s, avgpt 10
        let ss = compute_stats_data(&[(0, 100, 10), (10_000, 130, 13)]);
        assert_eq!(ss.sum, 30);
        assert!(almost(ss.tps, 3.0));
        assert!(almost(ss.avgpt, 10.0));
        assert_eq!(ss.times, 3);
        // display-format golden vector
        assert_eq!(
            ss.to_string(),
            "StatsSnapshot(sum=30, tps=3.00, avgpt=10.00, times=3)"
        );
    }

    #[test]
    fn single_point_has_zero_span_and_zero_tps() {
        let ss = compute_stats_data(&[(1_000, 5, 1)]);
        assert_eq!(ss.sum, 0);
        assert!(almost(ss.tps, 0.0));
        assert!(almost(ss.avgpt, 0.0));
        assert_eq!(ss.times, 0);
    }

    #[test]
    fn reversed_chain_keeps_negative_sums_but_zero_rates() {
        // golden vector: compute_stats_data([(10000,130,13),(0,100,10)])
        // -> sum=-30 tps=0.00 avgpt=0.00 times=-3
        let ss = compute_stats_data(&[(10_000, 130, 13), (0, 100, 10)]);
        assert_eq!(ss.sum, -30);
        assert!(almost(ss.tps, 0.0));
        assert!(almost(ss.avgpt, 0.0));
        assert_eq!(ss.times, -3);
    }

    #[test]
    fn zero_times_diff_gives_zero_avgpt_even_with_positive_sum() {
        // golden vector: [(0,100,10),(1000,400,10)] -> sum=300 tps=300 avgpt=0 times=0
        let ss = compute_stats_data(&[(0, 100, 10), (1_000, 400, 10)]);
        assert_eq!(ss.sum, 300);
        assert!(almost(ss.tps, 300.0));
        assert!(almost(ss.avgpt, 0.0));
        assert_eq!(ss.times, 0);
    }

    #[test]
    fn fractional_rates_are_plain_float_division() {
        // golden vector: [(0,0,0),(3000,10,3)] -> tps=3.33.. avgpt=3.33..
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
        assert!(almost(DAY_SAMPLING_INTERVAL_SECONDS, 3600.0));
        assert_eq!(MINUTE_LIST_MAX, 7);
        assert_eq!(HOUR_LIST_MAX, 7);
        assert_eq!(DAY_LIST_MAX, 25);
        assert_eq!(HOUR_SAMPLING_ROUNDS, 60);
        assert_eq!(DAY_SAMPLING_ROUNDS, 360);
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
        it.sample_at(10_000); // first cumulative point
        // The first sample seeds a zero point one period earlier.
        assert_eq!(it.minute_len(), 2);
        it.add_value(15, 1);
        it.sample_at(20_000); // second cumulative point
        let ss = it.get_stats_data_in_minute();
        assert_eq!(ss.sum, 25);
        assert_eq!(ss.times, 2);
        // 25 msgs / 20s = 1.25/s
        assert!(almost(ss.tps, 1.25));
        assert!(almost(ss.avgpt, 12.5));
    }

    #[test]
    fn hour_window_is_separate_from_minute() {
        // The first hour sample seeds a zero point one sampling period earlier, so the two points
        // (ts0,0,0) -> (ts1,400,4) diff to sum=400 over timesDiff=4, avgpt=100.
        let it = StatsItem::new("PULL_RT", "T@G");
        it.add_value(100, 2);
        it.sample_hour_at(0);
        it.add_value(300, 2);
        it.sample_hour_at(1_000);
        let ss = it.get_stats_data_in_hour();
        assert_eq!(ss.sum, 400);
        assert_eq!(ss.times, 4);
        assert!(almost(ss.avgpt, 100.0));
        // the minute chain is untouched by the hour sampling
        assert_eq!(it.get_stats_data_in_minute(), StatsSnapshot::default());
    }

    #[test]
    fn both_chains_are_capped_and_oldest_points_are_dropped() {
        // 80 rounds, each round add_value(1,1) then sample at ts=i. The minute and hour chains cap
        // at 7, so only rounds 73..79 survive (values 74..80); the day chain caps at 25.
        let it = StatsItem::new("PULL_TPS", "T@G");
        for i in 0..80_i64 {
            it.add_value(1, 1);
            it.sample_at(i);
            it.sample_hour_at(i);
            it.sample_day_at(i);
        }
        assert_eq!(it.minute_len(), MINUTE_LIST_MAX);
        assert_eq!(it.hour_len(), HOUR_LIST_MAX);
        assert_eq!(it.day_len(), DAY_LIST_MAX);
        let ss = it.get_stats_data_in_minute();
        assert_eq!(ss.sum, 6);
        assert_eq!(ss.times, 6);
        assert!(almost(ss.tps, 1_000.0), "{ss}");
        assert!(almost(ss.avgpt, 1.0), "{ss}");
        assert!(almost(it.get_stats_data_in_hour().tps, 1_000.0));
        // the day chain keeps rounds 56..80: 24 messages over 24 sampled units
        assert_eq!(it.get_stats_data_in_day().sum, 24);
    }

    #[test]
    fn first_snapshot_is_seeded_one_period_earlier_with_zero() {
        let it = StatsItem::new("PULL_TPS", "T@G");
        it.add_value(5, 1);
        it.sample_at(10_000);
        it.sample_hour_at(10_000);
        it.sample_day_at(10_000);
        {
            let state = lock(&it.state);
            assert_eq!(state.minute.front(), Some(&(0, 0, 0)));
            assert_eq!(state.hour.front(), Some(&(-590_000, 0, 0)));
            assert_eq!(state.day.front(), Some(&(-3_590_000, 0, 0)));
        }
    }

    #[test]
    fn sample_uses_wall_clock_chain() {
        let it = StatsItem::new("PULL_TPS", "T@G");
        it.add_value(3, 1);
        it.sample();
        it.add_value(4, 1);
        it.sample();
        let ss = it.get_stats_data_in_minute();
        assert_eq!(ss.sum, 7);
        assert_eq!(ss.times, 2);
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
    fn sample_all_walks_every_item_in_all_granularities() {
        let s = StatsItemSet::new("PULL_TPS");
        s.add_value("a", 1, 1);
        s.add_value("b", 2, 1);
        s.sample_all();
        s.sample_hour_all();
        s.sample_day_all();
        for key in ["a", "b"] {
            let it = s.find(key).expect("present");
            // Every chain seeds one zero point before the first cumulative point.
            assert_eq!(it.minute_len(), 2);
            assert_eq!(it.hour_len(), 2);
            assert_eq!(it.day_len(), 2);
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
    fn inc_semantics_match_contract() {
        // TPS kinds: value=message count, times=call count; RT kinds: value=elapsed time, times=call count
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
    fn key_is_topic_at_group_and_set_names_follow_contract() {
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

    /// Feeds cumulative snapshot points directly (1s apart), bypassing the sampling task.
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
        // protocol-alignment case:
        //   pull_rt value=200/times=4 -> avgpt=50ms
        //   consume_rt value=40/times=4 -> avgpt=10ms
        //   pull_tps value=20 -> tps=20/s (messages per second)
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
        // the untouched failed dimension stays 0
        assert!(almost(cs.consume_failed_tps, 0.0));
        assert_eq!(cs.consume_failed_msgs, 0);
    }

    #[test]
    fn consume_rt_falls_back_to_the_hour_window_when_minute_sum_is_zero() {
        let m = ConsumerStatsManager::new();
        m.inc_consume_rt(GROUP, TOPIC, 10);
        let it = m
            .topic_and_group_consume_rt()
            .find(KEY)
            .expect("consume_rt item");
        let now = current_time_millis();
        {
            let mut state = lock(&it.state);
            state.minute.clear();
            state.minute.push_back((now - 1_000, 0, 0));
            state.minute.push_back((now, 0, 4));
            state.hour.clear();
            state.hour.push_back((now - 1_000, 0, 0));
            state.hour.push_back((now, 40, 4));
        }
        // no traffic in the minute window -> the hour window answers (40/4 = 10ms)
        assert!(almost(m.consume_status(GROUP, TOPIC).consume_rt, 10.0));
        {
            let mut state = lock(&it.state);
            state.minute.clear();
            state.minute.push_back((now - 1_000, 0, 0));
            state.minute.push_back((now, 20, 4));
        }
        // minute traffic wins over the hour window (20/4 = 5ms)
        assert!(almost(m.consume_status(GROUP, TOPIC).consume_rt, 5.0));
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
        // no points in the minute chain -> tps falls back to 0, not the hour value
        assert!(almost(cs.consume_failed_tps, 0.0));
    }

    #[test]
    fn round_robin_follows_the_mod_60_rule() {
        // hour points are taken only when rounds % hour_every_rounds == 0 (60 replaced by 3 here for easy asserts);
        // each first sample seeds a zero point
        let m = ConsumerStatsManager::with_sampling(Duration::from_millis(1), 3);
        m.inc_pull_tps(GROUP, TOPIC, 1);
        for rounds in 1..=3 {
            sample_round(&m.inner, rounds);
        }
        let it = m
            .topic_and_group_pull_tps()
            .find(KEY)
            .expect("item");
        assert_eq!(it.minute_len(), 4);
        assert_eq!(it.hour_len(), 2);
        assert_eq!(it.day_len(), 0);
        sample_round(&m.inner, 4);
        assert_eq!(it.minute_len(), 5);
        assert_eq!(it.hour_len(), 2);
        sample_round(&m.inner, 6);
        assert_eq!(it.hour_len(), 3);
        sample_round(&m.inner, 18);
        assert_eq!(it.hour_len(), 4);
        assert_eq!(it.day_len(), 2, "a day point needs six hour rounds");
    }

    #[test]
    fn round_robin_with_zero_hour_period_never_samples_hour() {
        // division-by-zero guard: hour_every_rounds == 0 disables hour and day sampling
        let m = ConsumerStatsManager::with_sampling(Duration::from_millis(1), 0);
        m.inc_pull_rt(GROUP, TOPIC, 5);
        sample_round(&m.inner, 600);
        let it = m.topic_and_group_pull_rt().find(KEY).expect("item");
        assert_eq!(it.hour_len(), 0);
        assert_eq!(it.day_len(), 0);
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
        m.sample_day_all();
        for set in [
            m.topic_and_group_pull_rt(),
            m.topic_and_group_pull_tps(),
            m.topic_and_group_consume_rt(),
            m.topic_and_group_consume_ok_tps(),
            m.topic_and_group_consume_failed_tps(),
        ] {
            let it = set.find(KEY).expect("item");
            assert_eq!(it.minute_len(), 2, "{}", set.stats_name());
            assert_eq!(it.hour_len(), 2, "{}", set.stats_name());
            assert_eq!(it.day_len(), 2, "{}", set.stats_name());
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

    /// Takes the sampling task out of the slot so the test can observe its exit itself (after `take`, shutdown has no
    /// handle left to abort, so exit can only happen via the stop flag + Notify -- exactly the path under test).
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
        // a repeated start is a no-op (returns directly when a task already exists)
        m.start().expect("start inside a tokio runtime");
        assert!(m.is_running());

        wait_for(|| minute_points(&m) >= 2).await;

        let task = take_sampler(&m);
        m.shutdown();
        assert!(!m.is_running());
        assert!(m.inner.stop.is_stopped());
        // no abort available: the task must wake on the stop flag and exit by itself
        task.await.expect("sampler exits without panic");
        let after_stop = minute_points(&m);

        // start again after shutdown (both the task handle and the stop flag were reset)
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
        // period 1ms, hour sample every 2 rounds: when the hour chain gains 1 point the minute chain gains at least 2
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
        // the task exits by itself after the manager is dropped: no panic, no dangling memory
        let m = ConsumerStatsManager::with_sampling(Duration::from_millis(1), 60);
        m.inc_pull_tps(GROUP, TOPIC, 1);
        m.start_with_handle(&tokio::runtime::Handle::current());
        let task = take_sampler(&m);
        wait_for(|| minute_points(&m) >= 1).await;
        drop(m);
        let outcome = tokio::time::timeout(Duration::from_secs(5), task).await;
        assert!(outcome.is_ok(), "the sampling task did not exit after the manager was dropped");
        outcome.expect("joined").expect("sampler exits without panic");
    }

    #[test]
    fn start_without_runtime_returns_error_instead_of_panicking() {
        // start() outside an async context must return Err (it uses Handle::try_current internally, never panics)
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
    fn stats_snapshot_display_matches_golden_repr() {
        assert_eq!(
            StatsSnapshot::default().to_string(),
            "StatsSnapshot(sum=0, tps=0.00, avgpt=0.00, times=0)"
        );
        assert_eq!(
            compute_stats_data(&[(0, 0, 0), (3_000, 10, 3)]).to_string(),
            "StatsSnapshot(sum=10, tps=3.33, avgpt=3.33, times=3)"
        );
    }

    /// Polls until the condition holds (with a deadline, avoiding flaky failures from wall-clock sleeps).
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
