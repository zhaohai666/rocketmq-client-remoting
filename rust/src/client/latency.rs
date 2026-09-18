//! 发送延迟故障容错（对应 Java `org.apache.rocketmq.client.latency.*`，
//! 逐条对齐 `python/rocketmq/client/latency.py`）。
//!
//! 实现 [`MQFaultStrategy`] + [`LatencyFaultToleranceImpl`]（带 [`FaultItem`]）：
//! 追踪每个 broker 的发送延迟，延迟过高或发生异常时**隔离**一段时间
//! （不分配给新消息），默认关闭。
//!
//! 与 Java 关键点逐条对齐（对拍向量取自 Python 参考实现，见各单测）：
//! - `latency_max` / `not_available_duration` 两套阈值表；
//! - [`MQFaultStrategy::update_fault_item`] 的隔离场景固定按 `10000ms` 查档位，
//!   但**记进表里的仍是真实延迟**（`isolation` 只影响隔离时长，不影响 `currentLatency`）；
//! - [`FaultItem::is_available`] = `now >= startTimestamp`（隔离期未过则不可用）；
//! - [`LatencyFaultToleranceImpl::is_available`] / [`LatencyFaultToleranceImpl::is_reachable`]
//!   在**没有记录**时返回 true，即「从未出过问题的 broker 默认可用/可达」；
//! - 隔离期只增不减：更短的 `notAvailableDuration` 不会缩短已有窗口（Java 同）。
//!
//! 默认 `send_latency_fault_enable = false`，与 Java / Python 一致；开启后才会记录与生效。
//!
//! 与 Python 的**有意差异**（均不改变判定结果）：
//! 1. 时间单位用 `i64` 毫秒（Java 就是 `long`），Python 的 `time.time() * 1000.0` 是 float，
//!    阈值表与窗口都是整毫秒，落到 `i64` 后判定完全一致（无 `round`/`int` 差异）；
//! 2. `FaultItem` 的方法显式收 `now_millis` 参数，`LatencyFaultToleranceImpl` 持有
//!    可注入时钟 [`NowFn`]（Python 直接调 `time.time()`）——单测要靠它 pin 隔离窗口，
//!    否则会随真实时钟漂移；
//! 3. [`LatencyFaultToleranceImpl::get_fault_item`] 返回**快照副本**：Python 返回表里的活对象
//!    （改它就改表），Rust 里表由锁持有所有权，改动只能走 `update_fault_item`；
//! 4. Java 的后台可达性探测线程（`startDetector` / `detectByOneRound` / `pickOneAtLeast` /
//!    `FaultItem#compareTo`）Python 已省略，这里同样省略；`check_stamp` 字段保留但无人写；
//!    Java `updateFaultItem` / `updateNotAvailableDuration` 里的两条 `log.info` 也未移植（Python 没有）。

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::common::message::MessageQueue;
use crate::common::util_all::current_time_millis;
use crate::error::{Error, Result};

/// 可注入时钟（见模块头差异 2）。签名固定为无参函数指针，便于 `Default` 与 `Clone`。
pub type NowFn = fn() -> i64;

/// 队列过滤器（对应 Java `TopicPublishInfo.QueueFilter#filter`、
/// Python 里传给 `select_one_message_queue(*filters)` 的可调用对象）。
pub type QueueFilter<'a> = dyn Fn(&MessageQueue) -> bool + 'a;

/// 发布信息的选队列接口面（对应 Python `TopicPublishInfo` 的
/// `reset_index` / `select_one_message_queue(*filters)` 两个方法）。
///
/// 队列列表与轮询游标的所有权在**生产者层**（`client::producer`，对应
/// `mq_client.TopicPublishInfo`），本模块只声明策略需要的这点接口，
/// 避免 latency 反向依赖 producer 造成循环。语义必须与 Python 一致：
/// - `filters` 为空 ⇒ 无条件轮询，**永不返回 `Ok(None)`**；
/// - `filters` 非空 ⇒ 从当前游标起最多试 `n` 个队列，全被过滤掉才 `Ok(None)`；
///   每次试探都会推进游标（所以失败一轮会让下次起点前移，这是 Java 的行为）；
/// - 队列列表为空 ⇒ `Err`（Python `MQClientException("no message queue for publish info")`）。
pub trait PublishInfo: Send + Sync {
    /// Python `TopicPublishInfo.reset_index`：把游标清零。
    fn reset_index(&self);

    /// Python `TopicPublishInfo.select_one_message_queue(*filters)`。
    fn select_one_message_queue(&self, filters: &[&QueueFilter<'_>]) -> Result<Option<MessageQueue>>;
}

// ---------------------------------------------------------------- FaultItem

/// 单个 broker 的故障项（对应 Java `LatencyFaultToleranceImpl.FaultItem`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultItem {
    /// broker 名字。
    pub name: String,
    /// 最近一次上报的发送延迟（毫秒）。
    pub current_latency: i64,
    /// 隔离结束时间点（毫秒）；`now >= start_timestamp` 才算可用。
    pub start_timestamp: i64,
    /// Java 探测线程的去重时间戳。Python 只留字段、不写它（见模块头差异 4）。
    pub check_stamp: i64,
    /// 可达性标志（Java `reachableFlag`）。
    pub reachable_flag: bool,
}

impl FaultItem {
    /// 对应 Python `FaultItem(name)`：延迟 0、隔离窗口 0、**可达 = true**。
    ///
    /// ⚠ Java 的 `reachableFlag` 是 `boolean` 字段，默认 `false`；Python 显式选了 `True`，
    /// 且两个实现都会在构造后立刻被 `update_fault_item` 覆写，所以这里跟随 Python。
    pub fn new(name: &str) -> FaultItem {
        FaultItem {
            name: name.to_string(),
            current_latency: 0,
            start_timestamp: 0,
            check_stamp: 0,
            reachable_flag: true,
        }
    }

    /// Java `updateNotAvailableDuration`：**仅当** `duration > 0` 且
    /// `now + duration > startTimestamp` 时才前移窗口（保持最长隔离期）。
    pub fn update_not_available_duration(&mut self, not_available_duration: i64, now_millis: i64) {
        if not_available_duration > 0 && now_millis + not_available_duration > self.start_timestamp {
            self.start_timestamp = now_millis + not_available_duration;
        }
    }

    /// Java `isAvailable`：隔离期已过（含边界相等）即可用。
    pub fn is_available(&self, now_millis: i64) -> bool {
        now_millis >= self.start_timestamp
    }

    /// Java `isReachable`。
    pub fn is_reachable(&self) -> bool {
        self.reachable_flag
    }

    /// Java `getName`。
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Java `getCurrentLatency`。
    pub fn get_current_latency(&self) -> i64 {
        self.current_latency
    }

    /// Java `getStartTimestamp`。
    pub fn get_start_timestamp(&self) -> i64 {
        self.start_timestamp
    }
}

impl fmt::Display for FaultItem {
    /// 对齐 Python `FaultItem.__repr__`
    /// （`FaultItem{name=broker-a, latency=123, startTs=1700000000123, reachable=true}`）。
    ///
    /// Python 用 `%.0f` 打印浮点、`%s` 打印 bool（于是输出 `True`）；这里数值是 `i64`、
    /// 布尔按 Java/Rust 口径输出 `true`/`false`（Java 的 `FaultItem` 根本没有 `toString`）。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FaultItem{{name={}, latency={}, startTs={}, reachable={}}}",
            self.name, self.current_latency, self.start_timestamp, self.reachable_flag,
        )
    }
}

// -------------------------------------------------- LatencyFaultToleranceImpl

/// 对应 Java `client.latency.LatencyFaultToleranceImpl`（与 Python 一样简化为纯内存版，
/// 无探测线程），线程安全：内部 `RwLock<HashMap<..>>` 承担 Python 的 `RLock`。
#[derive(Debug)]
pub struct LatencyFaultToleranceImpl {
    /// Python `_fault_item_table`。
    fault_item_table: RwLock<HashMap<String, FaultItem>>,
    /// 时钟注入点（单测用假时钟 pin 隔离窗口）。
    now_millis: NowFn,
}

impl Default for LatencyFaultToleranceImpl {
    fn default() -> Self {
        LatencyFaultToleranceImpl::new()
    }
}

impl LatencyFaultToleranceImpl {
    pub fn new() -> LatencyFaultToleranceImpl {
        LatencyFaultToleranceImpl::with_clock(current_time_millis)
    }

    /// 用指定时钟构造（对应模块头差异 2；生产走 [`LatencyFaultToleranceImpl::new`]）。
    pub fn with_clock(now_millis: NowFn) -> LatencyFaultToleranceImpl {
        LatencyFaultToleranceImpl {
            fault_item_table: RwLock::new(HashMap::new()),
            now_millis,
        }
    }

    /// 当前毫秒（暴露给上层，便于 `select` 之后复用同一时刻）。
    pub fn now_millis(&self) -> i64 {
        (self.now_millis)()
    }

    /// Java `updateFaultItem(name, currentLatency, notAvailableDuration, reachable)`。
    ///
    /// 建新项与更新旧项的三步赋值顺序一致（Python 写了两份分支只是为了插入），
    /// 所以这里用 entry -or-insert 后统一改字段。
    pub fn update_fault_item(
        &self,
        name: &str,
        current_latency: i64,
        not_available_duration: i64,
        reachable: bool,
    ) {
        let now = self.now_millis();
        let mut table = write_guard(&self.fault_item_table);
        let item = table
            .entry(name.to_string())
            .or_insert_with(|| FaultItem::new(name));
        item.current_latency = current_latency;
        item.update_not_available_duration(not_available_duration, now);
        item.reachable_flag = reachable;
    }

    /// Java `isAvailable`：没有记录 ⇒ `true`（从未出过问题的 broker 默认可用）。
    pub fn is_available(&self, name: &str) -> bool {
        let now = self.now_millis();
        read_guard(&self.fault_item_table)
            .get(name)
            .map(|item| item.is_available(now))
            .unwrap_or(true)
    }

    /// Java `isReachable`：没有记录 ⇒ `true`。
    pub fn is_reachable(&self, name: &str) -> bool {
        read_guard(&self.fault_item_table)
            .get(name)
            .map(FaultItem::is_reachable)
            .unwrap_or(true)
    }

    /// Java `remove`：路由里没有这个 broker 时由上层清理。
    pub fn remove(&self, name: &str) {
        write_guard(&self.fault_item_table).remove(name);
    }

    /// 对应 Python `get_fault_item`：返回**快照副本**（见模块头差异 3）。
    pub fn get_fault_item(&self, name: &str) -> Option<FaultItem> {
        read_guard(&self.fault_item_table).get(name).cloned()
    }

    /// 表里当前记录了哪些 broker（仅供排错；Python 无对应方法）。
    pub fn fault_item_names(&self) -> Vec<String> {
        let mut names: Vec<String> = read_guard(&self.fault_item_table)
            .keys()
            .cloned()
            .collect();
        names.sort();
        names
    }
}

// ------------------------------------------------------------ MQFaultStrategy

/// Java `MQFaultStrategy.latencyMax` 默认表。
pub const LATENCY_MAX: [i64; 7] = [50, 100, 550, 1800, 3000, 5000, 15000];
/// Java `MQFaultStrategy.notAvailableDuration` 默认表（与 `LATENCY_MAX` 一一对应）。
pub const NOT_AVAILABLE_DURATION: [i64; 7] = [0, 0, 2000, 5000, 6000, 10000, 30000];
/// Java `updateFaultItem` 里隔离场景查档位用的固定延迟。
pub const ISOLATION_LATENCY: i64 = 10000;

/// 对应 Java `client.latency.MQFaultStrategy`。
///
/// 仅当 `send_latency_fault_enable` 为 true 时，发送选队列阶段才会：
/// 1. 先选 available（隔离期已过）的 broker；
/// 2. 否则选 reachable 的 broker；
/// 3. 否则退化为普通轮询。
///
/// 发送结果/异常会回调 [`MQFaultStrategy::update_fault_item`] 写延迟与隔离信息。
/// 开关用 `AtomicBool`（Java 是 `volatile boolean`），这样 `&self` 也能 set，
/// 上层可以安全地把策略放进 `Arc`。
#[derive(Debug)]
pub struct MQFaultStrategy {
    send_latency_fault_enable: AtomicBool,
    latency_fault_tolerance: LatencyFaultToleranceImpl,
    /// 延迟档位表：Python 是实例属性 `latency_max`（可直接改），这里同样留 `pub`。
    pub latency_max: Vec<i64>,
    /// 隔离时长档位表：Python 的 `not_available_duration`。
    pub not_available_duration: Vec<i64>,
}

impl Default for MQFaultStrategy {
    /// Python `MQFaultStrategy(send_latency_fault_enable=False)` 的默认。
    fn default() -> MQFaultStrategy {
        MQFaultStrategy::new(false)
    }
}

impl MQFaultStrategy {
    pub fn new(send_latency_fault_enable: bool) -> MQFaultStrategy {
        MQFaultStrategy::with_clock(send_latency_fault_enable, current_time_millis)
    }

    /// 带假时钟的构造口（单测 pin 隔离窗口用；等价于 Python 里 monkeypatch `time.time`）。
    pub fn with_clock(
        send_latency_fault_enable: bool,
        now_millis: NowFn,
    ) -> MQFaultStrategy {
        MQFaultStrategy {
            send_latency_fault_enable: AtomicBool::new(send_latency_fault_enable),
            latency_fault_tolerance: LatencyFaultToleranceImpl::with_clock(now_millis),
            latency_max: LATENCY_MAX.to_vec(),
            not_available_duration: NOT_AVAILABLE_DURATION.to_vec(),
        }
    }

    /// Java `isSendLatencyFaultEnable`。
    pub fn is_send_latency_fault_enable(&self) -> bool {
        self.send_latency_fault_enable.load(Ordering::Relaxed)
    }

    /// Java `setSendLatencyFaultEnable`（`DefaultMQProducer.set_send_latency_fault_enable` 会同时调它）。
    pub fn set_send_latency_fault_enable(&self, enable: bool) {
        self.send_latency_fault_enable.store(enable, Ordering::Relaxed);
    }

    /// 故障表（对应 Java `getLatencyFaultTolerance` / Python 的 `latency_fault_tolerance` 属性）。
    pub fn latency_fault_tolerance(&self) -> &LatencyFaultToleranceImpl {
        &self.latency_fault_tolerance
    }

    /// Python `_available_filter` / Java `availableFilter`：按 broker 名查隔离窗口。
    pub fn available_filter(&self, mq: &MessageQueue) -> bool {
        self.latency_fault_tolerance.is_available(mq.get_broker_name())
    }

    /// Python `_reachable_filter` / Java `reachableFilter`。
    pub fn reachable_filter(&self, mq: &MessageQueue) -> bool {
        self.latency_fault_tolerance.is_reachable(mq.get_broker_name())
    }

    /// 对应 Python `MQFaultStrategy.select_one_message_queue(tp_info, last_broker_name, reset_index=False)`
    /// / Java `MQFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName, resetIndex)`。
    ///
    /// `broker_filter` 的语义：`last_broker_name` 为 `None` 时**全部通过**，
    /// 否则排除上一轮用过的那个 broker。退化路径（最后一档无过滤器）永不返回 `None`，
    /// 所以只有「路由里一个队列都没有」才会 `Err`。
    pub fn select_one_message_queue(
        &self,
        tp_info: &dyn PublishInfo,
        last_broker_name: Option<&str>,
        reset_index: bool,
    ) -> Result<MessageQueue> {
        let broker_filter = |mq: &MessageQueue| match last_broker_name {
            None => true,
            Some(last) => mq.get_broker_name() != last,
        };
        let available: &QueueFilter<'_> = &|mq: &MessageQueue| self.available_filter(mq);
        let reachable: &QueueFilter<'_> = &|mq: &MessageQueue| self.reachable_filter(mq);
        let broker: &QueueFilter<'_> = &broker_filter;

        if self.is_send_latency_fault_enable() {
            if reset_index {
                tp_info.reset_index();
            }
            // 注意档次的先后：available 档**不看** reachable，所以「可用但不可达」的 broker
            // 会被选中（Python/Java 皆如此，见单测 available_filter_beats_reachable_filter）。
            if let Some(mq) = tp_info.select_one_message_queue(&[available, broker])? {
                return Ok(mq);
            }
            if let Some(mq) = tp_info.select_one_message_queue(&[reachable, broker])? {
                return Ok(mq);
            }
            return select_unfiltered(tp_info);
        }

        if let Some(mq) = tp_info.select_one_message_queue(&[broker])? {
            return Ok(mq);
        }
        select_unfiltered(tp_info)
    }

    /// 对应 Python `update_fault_item(broker_name, current_latency, isolation, reachable)`
    /// / Java `updateFaultItem`。
    ///
    /// 开关关着时**什么都不记**（Python 直接 `return`）。`isolation = true`
    /// （发送抛异常）时固定按 10000ms 的延迟查档位 —— 即隔离 10s，
    /// 但表里写的 `current_latency` 仍是真实延迟（Java 的 `isolation ? 10000 : currentLatency`
    /// 只出现在档位实参里）。
    pub fn update_fault_item(
        &self,
        broker_name: &str,
        current_latency: i64,
        isolation: bool,
        reachable: bool,
    ) {
        if !self.is_send_latency_fault_enable() {
            return;
        }
        let latency = if isolation { ISOLATION_LATENCY } else { current_latency };
        let duration = self.compute_not_available_duration(latency);
        self.latency_fault_tolerance
            .update_fault_item(broker_name, current_latency, duration, reachable);
    }

    /// 对应 Python `_compute_not_available_duration` / Java `computeNotAvailableDuration`：
    /// 从高档往低档找**第一个 `latency >= latency_max[i]`**，命中即返回对应窗口。
    ///
    /// Python 直接 `not_available_duration[i]` 索引，两表长度不一致时会 `IndexError`；
    /// 这里按「越界的档位等于 0」处理，以免配置写歪时把发送线程搞崩（唯一的行为差别）。
    fn compute_not_available_duration(&self, current_latency: i64) -> i64 {
        let latency_max = &self.latency_max;
        for i in (0..latency_max.len()).rev() {
            if current_latency >= latency_max[i] {
                return self.not_available_duration.get(i).copied().unwrap_or(0);
            }
        }
        0
    }
}

/// Python 的最后一档 `tp_info.select_one_message_queue()`：**永不返回 None**
/// （队列为空时 Python 直接抛 MQClientException）。这里把「实现方违约返回 None」
/// 兜成同一句错误文案，绝不 panic。
fn select_unfiltered(tp_info: &dyn PublishInfo) -> Result<MessageQueue> {
    match tp_info.select_one_message_queue(&[])? {
        Some(mq) => Ok(mq),
        None => Err(Error::client("no message queue for publish info")),
    }
}

/// 锁毒化时继续用（故障表宁可带着旧数据跑，也不能让发送链路 panic）。
fn read_guard<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_guard<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// 假时钟：固定时刻，隔离窗口才可断言。
    const NOW: i64 = 1_700_000_000_000;

    fn fixed_now() -> i64 {
        NOW
    }

    /// Python `TopicPublishInfo` 的测试替身：语义逐条照抄
    /// （空表报错、无过滤器永不返回 None、带过滤器最多试 n 个且每次都推进游标）。
    #[derive(Debug, Default)]
    struct FakePublishInfo {
        queues: Vec<MessageQueue>,
        index: AtomicUsize,
    }

    impl FakePublishInfo {
        fn new(queues: Vec<MessageQueue>) -> FakePublishInfo {
            FakePublishInfo { queues, index: AtomicUsize::new(0) }
        }

        fn index(&self) -> usize {
            self.index.load(Ordering::Relaxed)
        }
    }

    impl PublishInfo for FakePublishInfo {
        fn reset_index(&self) {
            self.index.store(0, Ordering::Relaxed);
        }

        fn select_one_message_queue(
            &self,
            filters: &[&QueueFilter<'_>],
        ) -> Result<Option<MessageQueue>> {
            let n = self.queues.len();
            if n == 0 {
                crate::bail!("no message queue for publish info");
            }
            let next = |which: &FakePublishInfo| {
                let i = which.index.fetch_add(1, Ordering::Relaxed) % n;
                which.queues[i].clone()
            };
            if filters.is_empty() {
                return Ok(Some(next(self)));
            }
            for _ in 0..n {
                let mq = next(self);
                if filters.iter().all(|filter| filter(&mq)) {
                    return Ok(Some(mq));
                }
            }
            Ok(None)
        }
    }

    fn queues() -> Vec<MessageQueue> {
        vec![
            MessageQueue::new("T", "broker-a", 0),
            MessageQueue::new("T", "broker-a", 1),
            MessageQueue::new("T", "broker-b", 0),
        ]
    }

    fn short(mq: &MessageQueue) -> (String, i32) {
        (mq.get_broker_name().to_string(), mq.get_queue_id())
    }

    #[test]
    fn fault_item_defaults_and_reachable_true_follows_python() {
        let item = FaultItem::new("broker-a");
        assert_eq!(item.current_latency, 0);
        assert_eq!(item.start_timestamp, 0);
        assert_eq!(item.check_stamp, 0);
        assert!(item.reachable_flag);
        assert!(item.is_available(0));
        assert!(item.is_reachable());
        assert_eq!(item.get_name(), "broker-a");
    }

    #[test]
    fn fault_item_display_matches_python_repr() {
        assert_eq!(
            FaultItem::new("broker-a").to_string(),
            "FaultItem{name=broker-a, latency=0, startTs=0, reachable=true}"
        );
        let item = FaultItem {
            name: "broker-a".into(),
            current_latency: 123,
            start_timestamp: 1_700_000_000_123,
            check_stamp: 0,
            reachable_flag: false,
        };
        assert_eq!(
            item.to_string(),
            "FaultItem{name=broker-a, latency=123, startTs=1700000000123, reachable=false}"
        );
    }

    #[test]
    fn update_not_available_duration_keeps_longest_window_only() {
        let mut item = FaultItem::new("x");
        item.update_not_available_duration(30_000, NOW);
        assert_eq!(item.start_timestamp, NOW + 30_000);
        // 更短的窗口不缩短（Python/Java：only when now + dur > startTimestamp）
        item.update_not_available_duration(2_000, NOW);
        assert_eq!(item.start_timestamp, NOW + 30_000);
        // 0 与负数一律忽略
        item.update_not_available_duration(0, NOW);
        assert_eq!(item.start_timestamp, NOW + 30_000);
        item.update_not_available_duration(-5, NOW);
        assert_eq!(item.start_timestamp, NOW + 30_000);
        // 边界：now == start_timestamp 时仍然「可用」（>= 判定）
        assert!(item.is_available(NOW + 30_000));
        assert!(!item.is_available(NOW + 29_999));
    }

    #[test]
    fn tolerance_defaults_to_available_and_reachable_for_unknown_name() {
        let tolerance = LatencyFaultToleranceImpl::with_clock(fixed_now);
        assert!(tolerance.is_available("nope"));
        assert!(tolerance.is_reachable("nope"));
        assert!(tolerance.get_fault_item("nope").is_none());
        assert!(tolerance.fault_item_names().is_empty());

        tolerance.update_fault_item("broker-a", 20, 0, false);
        let item = tolerance.get_fault_item("broker-a").unwrap();
        assert_eq!(item.current_latency, 20);
        // duration 0 ⇒ 不建隔离窗口，仍然 available；但 reachable 被写成了 false
        assert!(tolerance.is_available("broker-a"));
        assert!(!tolerance.is_reachable("broker-a"));
        assert_eq!(item.start_timestamp, 0);

        tolerance.remove("broker-a");
        assert!(tolerance.get_fault_item("broker-a").is_none());
        assert!(tolerance.is_reachable("broker-a"));
    }

    #[test]
    fn tolerance_update_overwrites_latency_but_not_longer_window() {
        let tolerance = LatencyFaultToleranceImpl::with_clock(fixed_now);
        tolerance.update_fault_item("x", 5_000, 30_000, true);
        assert_eq!(
            tolerance.get_fault_item("x").unwrap().start_timestamp,
            NOW + 30_000
        );
        tolerance.update_fault_item("x", 600, 2_000, false);
        let item = tolerance.get_fault_item("x").unwrap();
        // 延迟被覆盖、窗口保持更长的那个、reachable 一定被覆盖
        assert_eq!(item.current_latency, 600);
        assert_eq!(item.start_timestamp, NOW + 30_000);
        assert!(!item.reachable_flag);
        assert!(!tolerance.is_available("x"));
        // 建新项与更新旧项的三步赋值等价（Python 两份分支）
        assert_eq!(item.get_current_latency(), 600);
        assert_eq!(item.get_start_timestamp(), NOW + 30_000);
    }

    #[test]
    fn compute_not_available_duration_threshold_table() {
        let strategy = MQFaultStrategy::new(true);
        // 向量由 `python3 /tmp/rmq_probe/probe_latency.py` 打印
        for (latency, duration) in [
            (0, 0),
            (49, 0),
            (50, 0),
            (51, 0),
            (99, 0),
            (100, 0),
            (101, 0),
            (549, 0),
            (550, 2_000),
            (1_799, 2_000),
            (1_800, 5_000),
            (2_999, 5_000),
            (3_000, 6_000),
            (4_999, 6_000),
            (5_000, 10_000),
            (9_999, 10_000),
            (10_000, 10_000),
            (14_999, 10_000),
            (15_000, 30_000),
            (20_000, 30_000),
            (999_999, 30_000),
            (-1, 0),
            (-5_000, 0),
        ] {
            assert_eq!(
                strategy.compute_not_available_duration(latency),
                duration,
                "latency={}",
                latency
            );
        }
    }

    #[test]
    fn update_fault_item_isolation_uses_ten_second_window_but_real_latency() {
        let strategy = MQFaultStrategy::with_clock(true, fixed_now);
        strategy.update_fault_item("broker-a", 123, true, false);
        let item = strategy.latency_fault_tolerance().get_fault_item("broker-a").unwrap();
        assert_eq!(item.current_latency, 123);
        assert_eq!(item.start_timestamp, NOW + 10_000);
        assert!(!item.reachable_flag);
        assert!(!strategy.latency_fault_tolerance().is_available("broker-a"));

        // 非隔离：按真实延迟查档位
        strategy.update_fault_item("broker-b", 7_000, false, true);
        assert_eq!(
            strategy.latency_fault_tolerance().get_fault_item("broker-b").unwrap().start_timestamp,
            NOW + 10_000
        );
        // 低延迟不建窗口
        strategy.update_fault_item("broker-c", 10, false, true);
        assert_eq!(
            strategy.latency_fault_tolerance().get_fault_item("broker-c").unwrap().start_timestamp,
            0
        );
    }

    #[test]
    fn disabled_strategy_records_nothing() {
        let strategy = MQFaultStrategy::default();
        assert!(!strategy.is_send_latency_fault_enable());
        strategy.update_fault_item("broker-z", 99_999, true, false);
        assert!(strategy.latency_fault_tolerance().get_fault_item("broker-z").is_none());
        assert!(strategy.latency_fault_tolerance().fault_item_names().is_empty());

        strategy.set_send_latency_fault_enable(true);
        assert!(strategy.is_send_latency_fault_enable());
        strategy.update_fault_item("broker-z", 99_999, true, false);
        assert!(strategy.latency_fault_tolerance().get_fault_item("broker-z").is_some());
    }

    #[test]
    fn select_disabled_falls_back_to_plain_round_robin() {
        let strategy = MQFaultStrategy::new(false);
        let tp = FakePublishInfo::new(queues());
        let seq: Vec<(String, i32)> = (0..5)
            .map(|_| short(&strategy.select_one_message_queue(&tp, None, false).unwrap()))
            .collect();
        assert_eq!(
            seq,
            vec![
                ("broker-a".into(), 0),
                ("broker-a".into(), 1),
                ("broker-b".into(), 0),
                ("broker-a".into(), 0),
                ("broker-a".into(), 1),
            ]
        );
        assert_eq!(tp.index(), 5);
    }

    #[test]
    fn select_disabled_with_last_broker_same_single_broker_degrades() {
        // 单 broker 多队列：broker_filter 一轮全灭 ⇒ 退化普通轮询（每次推进 2+1 步）
        let strategy = MQFaultStrategy::new(false);
        let tp = FakePublishInfo::new(vec![
            MessageQueue::new("T", "broker-a", 0),
            MessageQueue::new("T", "broker-a", 1),
        ]);
        let seq: Vec<(String, i32)> = (0..3)
            .map(|_| {
                short(&strategy.select_one_message_queue(&tp, Some("broker-a"), false).unwrap())
            })
            .collect();
        assert_eq!(
            seq,
            vec![("broker-a".into(), 0), ("broker-a".into(), 1), ("broker-a".into(), 0)]
        );
        assert_eq!(tp.index(), 9);
    }

    #[test]
    fn select_enabled_skips_isolated_broker() {
        let strategy = MQFaultStrategy::with_clock(true, fixed_now);
        strategy.update_fault_item("broker-a", 20_000, false, true);
        let tp = FakePublishInfo::new(queues());
        let seq: Vec<(String, i32)> = (0..3)
            .map(|_| short(&strategy.select_one_message_queue(&tp, None, false).unwrap()))
            .collect();
        // available 档：a 的两个队列被拒（各推进 1 步），第 3 步命中 b ⇒ 共 3 步/次
        assert_eq!(
            seq,
            vec![
                ("broker-b".into(), 0),
                ("broker-b".into(), 0),
                ("broker-b".into(), 0)
            ]
        );
        assert_eq!(tp.index(), 9);
    }

    #[test]
    fn available_filter_beats_reachable_filter() {
        // 奇点：available 档不看可达性 ⇒ 「可用但不可达」的 broker-a 会被选中，
        // 「可达但隔离」的 broker-b 反而落选。Python/Java 皆如此。
        let strategy = MQFaultStrategy::with_clock(true, fixed_now);
        strategy.update_fault_item("broker-a", 1, false, false);
        strategy.update_fault_item("broker-b", 20_000, false, true);
        assert!(strategy.latency_fault_tolerance().is_available("broker-a"));
        assert!(!strategy.latency_fault_tolerance().is_reachable("broker-a"));
        assert!(!strategy.latency_fault_tolerance().is_available("broker-b"));
        assert!(strategy.latency_fault_tolerance().is_reachable("broker-b"));

        let tp = FakePublishInfo::new(vec![
            MessageQueue::new("T", "broker-a", 0),
            MessageQueue::new("T", "broker-b", 0),
        ]);
        let picked = strategy.select_one_message_queue(&tp, None, false).unwrap();
        assert_eq!(short(&picked), ("broker-a".into(), 0));
        assert_eq!(tp.index(), 1);
    }

    #[test]
    fn select_enabled_with_last_broker_falls_to_next_available() {
        let strategy = MQFaultStrategy::with_clock(true, fixed_now);
        strategy.update_fault_item("broker-a", 1, false, false);
        strategy.update_fault_item("broker-b", 20_000, false, true);
        let tp = FakePublishInfo::new(vec![
            MessageQueue::new("T", "broker-a", 0),
            MessageQueue::new("T", "broker-b", 0),
        ]);
        let picked = strategy.select_one_message_queue(&tp, Some("broker-a"), false).unwrap();
        assert_eq!(short(&picked), ("broker-b".into(), 0));
        assert_eq!(tp.index(), 4);
    }

    #[test]
    fn select_all_brokers_faulted_degrades_to_round_robin() {
        let strategy = MQFaultStrategy::with_clock(true, fixed_now);
        strategy.update_fault_item("broker-a", 20_000, true, false);
        strategy.update_fault_item("broker-b", 20_000, true, false);
        let tp = FakePublishInfo::new(queues());
        let seq: Vec<(String, i32)> = (0..4)
            .map(|_| short(&strategy.select_one_message_queue(&tp, None, false).unwrap()))
            .collect();
        // available 档 3 步全灭 + reachable 档 3 步全灭 + 无过滤器 1 步 ⇒ 每次 7 步
        assert_eq!(
            seq,
            vec![
                ("broker-a".into(), 0),
                ("broker-a".into(), 1),
                ("broker-b".into(), 0),
                ("broker-a".into(), 0),
            ]
        );
        assert_eq!(tp.index(), 28);
    }

    #[test]
    fn select_with_single_isolated_queue_still_returns_it() {
        let strategy = MQFaultStrategy::with_clock(true, fixed_now);
        strategy.update_fault_item("broker-a", 20_000, true, false);
        let tp = FakePublishInfo::new(vec![MessageQueue::new("T", "broker-a", 0)]);
        let picked = strategy.select_one_message_queue(&tp, Some("broker-a"), false).unwrap();
        assert_eq!(short(&picked), ("broker-a".into(), 0));
        assert_eq!(tp.index(), 3);
    }

    #[test]
    fn reset_index_only_applies_when_fault_enabled() {
        let strategy = MQFaultStrategy::with_clock(true, fixed_now);
        let tp = FakePublishInfo::new(queues());
        strategy.select_one_message_queue(&tp, None, false).unwrap();
        assert_eq!(tp.index(), 1);
        assert_eq!(
            short(&strategy.select_one_message_queue(&tp, None, true).unwrap()),
            ("broker-a".into(), 0)
        );
        assert_eq!(tp.index(), 1);
        // 关闭时 Python 连 reset_index 都不执行（那个分支整体在 enable 判断内）
        let off = MQFaultStrategy::new(false);
        let tp2 = FakePublishInfo::new(queues());
        off.select_one_message_queue(&tp2, None, false).unwrap();
        off.select_one_message_queue(&tp2, None, true).unwrap();
        assert_eq!(tp2.index(), 2);
    }

    #[test]
    fn empty_publish_info_is_an_error() {
        let strategy = MQFaultStrategy::with_clock(true, fixed_now);
        let tp = FakePublishInfo::new(Vec::new());
        let err = strategy.select_one_message_queue(&tp, None, false).err().unwrap();
        assert_eq!(err.to_string(), "MQClientException: no message queue for publish info");
        // 关闭时同样报错
        let off = MQFaultStrategy::new(false);
        assert!(off.select_one_message_queue(&tp, None, false).is_err());
    }

    #[test]
    fn threshold_tables_are_per_instance_and_mutable() {
        let mut strategy = MQFaultStrategy::new(true);
        assert_eq!(strategy.latency_max, LATENCY_MAX.to_vec());
        assert_eq!(strategy.not_available_duration, NOT_AVAILABLE_DURATION.to_vec());
        strategy.not_available_duration = vec![0, 1, 2, 3, 4, 5, 6];
        assert_eq!(strategy.compute_not_available_duration(100), 1);
        assert_eq!(strategy.compute_not_available_duration(550), 2);
        // 另一实例仍是默认表（Python 的类常量 vs 实例副本）
        let other = MQFaultStrategy::new(true);
        assert_eq!(other.compute_not_available_duration(100), 0);
    }

    #[test]
    fn mismatched_table_length_yields_zero_instead_of_panicking() {
        // Python 这里会 IndexError；Rust 侧按「档位缺失 = 0」处理（见方法文档）
        let mut strategy = MQFaultStrategy::new(true);
        strategy.latency_max = vec![10, 20, 30];
        strategy.not_available_duration = vec![100];
        assert_eq!(strategy.compute_not_available_duration(25), 0);
        assert_eq!(strategy.compute_not_available_duration(5), 0);
        strategy.not_available_duration.push(200);
        assert_eq!(strategy.compute_not_available_duration(25), 200);
        assert_eq!(strategy.compute_not_available_duration(15), 100);
        assert_eq!(strategy.compute_not_available_duration(5), 0);
    }
}
