//! 设计要点（语义与线上基准一致）：
//! - `snapshot()` 返回强类型 [`MetricsSnapshot`]，
//!   [`MetricsSnapshot::to_json_value`] 的键名与顺序是固定的；
//! - 时钟只在**私有**的 `record_*_side(failed, rt)` 里可注入，单测因此能精确断言 RT，
//!   而不是睡墙钟。

use std::fmt;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// 取当前毫秒时间戳（浮点）。
///
/// 时钟回拨时 RT 可能为负 —— 同样不做防护（与线上基准口径一致优先）。
fn now_ms() -> f64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs_f64() * 1000.0,
        Err(e) => -(e.duration().as_secs_f64() * 1000.0),
    }
}

/// 保留三位小数：对二进制真值做"四舍六入五成双"，即 `{:.3}` 的打印结果。
/// 解析失败（inf/NaN）时原样返回，绝不 panic。
pub fn round3(value: f64) -> f64 {
    format!("{value:.3}").parse::<f64>().unwrap_or(value)
}

/// 指标快照。
///
/// 浮点字段均已 `round3` 舍入，整数字段是累计值。
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct MetricsSnapshot {
    /// `sendCount`：发送**成功**次数。
    pub send_count: i64,
    /// `sendFailureCount`：发送失败次数。
    pub send_failure_count: i64,
    /// `sendRTSum`：成功 + 失败的总耗时（毫秒）。
    pub send_rt_sum: f64,
    /// `sendRTMax`：单次最大耗时。
    pub send_rt_max: f64,
    /// `sendRTMin`：单次最小耗时。
    pub send_rt_min: f64,
    /// `sendRTAvg`：`rt_sum / send_count`（分母不含失败次数），count 为 0 时取 0.0。
    pub send_rt_avg: f64,
    /// `consumeCount`：消费成功次数。
    pub consume_count: i64,
    /// `consumeFailureCount`：消费失败次数。
    pub consume_failure_count: i64,
    /// `consumeRTSum`：成功 + 失败的总耗时（毫秒）。
    pub consume_rt_sum: f64,
    /// `consumeRTMax`：单次最大耗时。
    pub consume_rt_max: f64,
    /// `consumeRTMin`：单次最小耗时。
    pub consume_rt_min: f64,
    /// `consumeRTAvg`：`rt_sum / consume_count`，count 为 0 时取 0.0。
    pub consume_rt_avg: f64,
}

impl MetricsSnapshot {
    /// 转成**固定键名、固定顺序**的 JSON 对象
    /// （`sendCount/sendFailureCount/sendRTSum/sendRTMax/sendRTMin/sendRTAvg` + 消费侧同名）。
    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "sendCount": self.send_count,
            "sendFailureCount": self.send_failure_count,
            "sendRTSum": self.send_rt_sum,
            "sendRTMax": self.send_rt_max,
            "sendRTMin": self.send_rt_min,
            "sendRTAvg": self.send_rt_avg,
            "consumeCount": self.consume_count,
            "consumeFailureCount": self.consume_failure_count,
            "consumeRTSum": self.consume_rt_sum,
            "consumeRTMax": self.consume_rt_max,
            "consumeRTMin": self.consume_rt_min,
            "consumeRTAvg": self.consume_rt_avg,
        })
    }
}

impl fmt::Display for MetricsSnapshot {
    /// 打印 JSON 串，键名与顺序同快照字段，
    /// 采用 serde_json 的紧凑格式。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClientMetrics{}", self.to_json_value())
    }
}

/// 一组（发送或消费）的内部累计量：`count/failure_count/rt_*`。
#[derive(Debug, Default, Clone, Copy)]
struct Side {
    count: i64,
    failure_count: i64,
    rt_sum: f64,
    rt_max: f64,
    rt_min: f64,
    started: bool,
}

impl Side {
    /// `record_*_success/failure` 里共用的累加逻辑。
    fn record(&mut self, failed: bool, rt: f64) {
        if failed {
            self.failure_count += 1;
        } else {
            self.count += 1;
        }
        self.rt_sum += rt;
        if !self.started || rt > self.rt_max {
            self.rt_max = rt;
        }
        if !self.started || rt < self.rt_min {
            self.rt_min = rt;
        }
        self.started = true;
    }

    /// 快照的半边，返回 `(count, failure_count, sum, max, min, avg)`。
    /// avg 的分母只算成功次数，0 次时给 0.0。
    fn parts(&self) -> (i64, i64, f64, f64, f64, f64) {
        let avg = if self.count != 0 {
            round3(self.rt_sum / self.count as f64)
        } else {
            0.0
        };
        (
            self.count,
            self.failure_count,
            round3(self.rt_sum),
            round3(self.rt_max),
            round3(self.rt_min),
            avg,
        )
    }
}

/// 线程安全的发送/消费基本指标计数器。
///
/// 内部只有一把 `Mutex`，`Send + Sync`，可以被 `Arc` 包起来在 producer/consumer 的
/// 多个任务间共享。锁中毒时照常取内值（统计路径绝不允许 panic）。
#[derive(Debug, Default)]
pub struct ClientMetrics {
    inner: Mutex<MetricsState>,
}

#[derive(Debug, Default, Clone, Copy)]
struct MetricsState {
    send: Side,
    consume: Side,
}

impl Clone for ClientMetrics {
    fn clone(&self) -> Self {
        ClientMetrics { inner: Mutex::new(self.locked()) }
    }
}

impl ClientMetrics {
    /// 全零初始状态。
    pub fn new() -> Self {
        Self::default()
    }

    fn locked(&self) -> MetricsState {
        *self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    // ---------------- 发送 ----------------

    /// 发送开始：**只返回时间戳**，不改状态。
    pub fn record_send_start(&self) -> f64 {
        now_ms()
    }

    /// 记录一次发送成功（按开始时间算 RT）。
    pub fn record_send_success(&self, start_ms: f64) {
        self.record_send_side(false, now_ms() - start_ms);
    }

    /// 记录一次发送失败：失败同样累加 rt_sum。
    pub fn record_send_failure(&self, start_ms: f64) {
        self.record_send_side(true, now_ms() - start_ms);
    }

    fn record_send_side(&self, failed: bool, rt: f64) {
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        state.send.record(failed, rt);
    }

    // ---------------- 消费 ----------------

    /// 消费开始：**只返回时间戳**，不改状态。
    pub fn record_consume_start(&self) -> f64 {
        now_ms()
    }

    /// 记录一次消费成功（按开始时间算 RT）。
    pub fn record_consume_success(&self, start_ms: f64) {
        self.record_consume_side(false, now_ms() - start_ms);
    }

    /// 记录一次消费失败（按开始时间算 RT）。
    pub fn record_consume_failure(&self, start_ms: f64) {
        self.record_consume_side(true, now_ms() - start_ms);
    }

    fn record_consume_side(&self, failed: bool, rt: f64) {
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        state.consume.record(failed, rt);
    }

    // ---------------- 快照 ----------------

    /// 取一次锁、读出**舍入后**的全量指标。
    pub fn snapshot(&self) -> MetricsSnapshot {
        let state = self.locked();
        let (send_count, send_failure_count, send_rt_sum, send_rt_max, send_rt_min, send_rt_avg) =
            state.send.parts();
        let (consume_count, consume_failure_count, consume_rt_sum, consume_rt_max,
             consume_rt_min, consume_rt_avg) = state.consume.parts();
        MetricsSnapshot {
            send_count,
            send_failure_count,
            send_rt_sum,
            send_rt_max,
            send_rt_min,
            send_rt_avg,
            consume_count,
            consume_failure_count,
            consume_rt_sum,
            consume_rt_max,
            consume_rt_min,
            consume_rt_avg,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 用注入时钟走一遍公开路径的等价实现（`record_*` 只在墙钟上取 now_ms）。
    fn record(metrics: &ClientMetrics, consume: bool, failed: bool, start_ms: f64, now_ms: f64) {
        let mut state = metrics.inner.lock().unwrap_or_else(|e| e.into_inner());
        let side = if consume { &mut state.consume } else { &mut state.send };
        side.record(failed, now_ms - start_ms);
    }

    fn almost(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn fresh_snapshot_is_all_zero_and_matches_python_dict() {
        let snapshot = ClientMetrics::new().snapshot();
        assert_eq!(snapshot, MetricsSnapshot::default());
        // 空快照的键名与顺序是固定的
        assert_eq!(
            snapshot.to_json_value(),
            serde_json::json!({
                "sendCount": 0,
                "sendFailureCount": 0,
                "sendRTSum": 0.0,
                "sendRTMax": 0.0,
                "sendRTMin": 0.0,
                "sendRTAvg": 0.0,
                "consumeCount": 0,
                "consumeFailureCount": 0,
                "consumeRTSum": 0.0,
                "consumeRTMax": 0.0,
                "consumeRTMin": 0.0,
                "consumeRTAvg": 0.0,
            })
        );
        // 键序也要一致（preserve_order 生效）
        let value = snapshot.to_json_value();
        let keys: Vec<&String> = value
            .as_object()
            .map(|m| m.keys().collect())
            .unwrap_or_default();
        assert_eq!(
            keys,
            vec![
                "sendCount",
                "sendFailureCount",
                "sendRTSum",
                "sendRTMax",
                "sendRTMin",
                "sendRTAvg",
                "consumeCount",
                "consumeFailureCount",
                "consumeRTSum",
                "consumeRTMax",
                "consumeRTMin",
                "consumeRTAvg",
            ]
        );
    }

    #[test]
    fn start_hooks_only_return_a_timestamp() {
        let metrics = ClientMetrics::new();
        let before = metrics.record_send_start();
        let after = metrics.record_consume_start();
        assert!(before > 1_600_000_000_000.0, "毫秒时间戳量级: {before}");
        assert!(after >= before);
        // 两个 start 都不记账
        assert_eq!(metrics.snapshot(), MetricsSnapshot::default());
    }

    #[test]
    fn first_record_seeds_both_min_and_max() {
        let metrics = ClientMetrics::new();
        record(&metrics, false, false, 1000.0, 1050.5);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.send_count, 1);
        assert_eq!(snapshot.send_failure_count, 0);
        assert!(almost(snapshot.send_rt_sum, 50.5));
        assert!(almost(snapshot.send_rt_max, 50.5));
        assert!(almost(snapshot.send_rt_min, 50.5));
        assert!(almost(snapshot.send_rt_avg, 50.5));
    }

    #[test]
    fn failures_share_the_rt_chain_but_not_the_count() {
        // 口径：rt_sum 含失败耗时，avg 的分母只算成功次数。
        let metrics = ClientMetrics::new();
        record(&metrics, false, false, 0.0, 100.0); // rt 100
        record(&metrics, false, true, 0.0, 200.0); // rt 200（失败）
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.send_count, 1);
        assert_eq!(snapshot.send_failure_count, 1);
        assert!(almost(snapshot.send_rt_sum, 300.0));
        assert!(almost(snapshot.send_rt_max, 200.0));
        assert!(almost(snapshot.send_rt_min, 100.0));
        // 300 / 1 = 300，而不是 300 / 2
        assert!(almost(snapshot.send_rt_avg, 300.0));
    }

    #[test]
    fn zero_success_count_gives_zero_avg_not_nan() {
        let metrics = ClientMetrics::new();
        record(&metrics, true, true, 0.0, 7.0); // 只有失败
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.consume_count, 0);
        assert_eq!(snapshot.consume_failure_count, 1);
        assert!(almost(snapshot.consume_rt_sum, 7.0));
        assert!(almost(snapshot.consume_rt_avg, 0.0));
        assert!(!snapshot.consume_rt_avg.is_nan());
    }

    #[test]
    fn min_and_max_only_move_outwards_once_started() {
        // 场景：started 已置位、max=100、min=1，再记一条超大 rt
        let metrics = ClientMetrics::new();
        {
            let mut state = metrics.inner.lock().unwrap_or_else(|e| e.into_inner());
            state.send.started = true;
            state.send.rt_max = 100.0;
            state.send.rt_min = 1.0;
        }
        record(&metrics, false, false, 0.0, 1_789_714_979_881.618);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.send_count, 1);
        assert!(almost(snapshot.send_rt_max, 1_789_714_979_881.618));
        assert!(almost(snapshot.send_rt_min, 1.0));
    }

    #[test]
    fn consume_side_is_independent_of_send_side() {
        let metrics = ClientMetrics::new();
        record(&metrics, false, false, 0.0, 10.0);
        record(&metrics, true, true, 0.0, 25.0);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.send_count, 1);
        assert_eq!(snapshot.send_failure_count, 0);
        assert_eq!(snapshot.consume_count, 0);
        assert_eq!(snapshot.consume_failure_count, 1);
        assert!(almost(snapshot.consume_rt_sum, 25.0));
        assert!(almost(snapshot.consume_rt_max, 25.0));
        assert!(almost(snapshot.consume_rt_min, 25.0));
    }

    #[test]
    fn snapshot_values_are_rounded_to_three_decimals() {
        let metrics = ClientMetrics::new();
        record(&metrics, false, false, 0.0, 12.345_678_9); // rt 12.3456789
        record(&metrics, false, false, 12.345_678_9, 12.346_078_9); // rt 0.0004
        let snapshot = metrics.snapshot();
        // sum = 12.3460789 -> 12.346；max = 12.3456789 -> 12.346；min = 0.0004 -> 0.0
        assert!(almost(snapshot.send_rt_sum, 12.346), "{snapshot:?}");
        assert!(almost(snapshot.send_rt_max, 12.346), "{snapshot:?}");
        assert!(almost(snapshot.send_rt_min, 0.0), "{snapshot:?}");
        assert!(almost(snapshot.send_rt_avg, 6.173), "{snapshot:?}");
    }

    #[test]
    fn round3_matches_python_on_half_way_vectors() {
        // 固定向量：二进制真值 + 四舍六入五成双
        for (v, want) in [
            (1.0005_f64, 1.0_f64),
            (0.0625, 0.062),
            (2.6785, 2.679),
            (0.123456789, 0.123),
            (1234.567891, 1234.568),
            (-0.0625, -0.062),
            (0.0005, 0.001),
            (0.0015, 0.002),
            (1_789_714_979_881.618, 1_789_714_979_881.618),
        ] {
            assert!(almost(round3(v), want), "round3({v}) = {}", round3(v));
        }
        // inf/NaN 原样返回，不 panic
        assert!(round3(f64::INFINITY).is_infinite());
        assert!(round3(f64::NAN).is_nan());
    }

    #[test]
    fn real_clock_paths_accumulate_non_negative_rt() {
        // 走真实的 record_send_*：只做量级断言，不睡墙钟
        let metrics = ClientMetrics::new();
        let start = metrics.record_send_start();
        for _ in 0..64 {
            std::hint::black_box(&start);
        }
        metrics.record_send_success(start);
        metrics.record_send_failure(start);
        let c_start = metrics.record_consume_start();
        metrics.record_consume_success(c_start);
        metrics.record_consume_failure(c_start);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.send_count, 1);
        assert_eq!(snapshot.send_failure_count, 1);
        assert_eq!(snapshot.consume_count, 1);
        assert_eq!(snapshot.consume_failure_count, 1);
        assert!(snapshot.send_rt_sum >= 0.0);
        assert!(snapshot.consume_rt_sum >= 0.0);
        assert!(snapshot.send_rt_min <= snapshot.send_rt_max);
        assert!(snapshot.consume_rt_min <= snapshot.consume_rt_max);
        // 一次成功 + 一次失败的 rt_sum 都被 avg 的分子用到
        assert!(snapshot.send_rt_avg >= snapshot.send_rt_min);
    }

    #[test]
    fn display_and_clone_semantics() {
        let metrics = ClientMetrics::new();
        record(&metrics, false, false, 0.0, 12.265);
        let text = metrics.snapshot().to_string();
        assert!(text.starts_with("ClientMetrics{"), "{text}");
        assert!(text.contains("\"sendRTSum\":12.265"), "{text}");
        // Clone 是"当前值的深拷贝"，之后新记录不回灌到副本
        let copied = metrics.clone();
        record(&metrics, false, false, 0.0, 1000.0);
        assert_eq!(copied.snapshot().send_count, 1);
        assert_eq!(metrics.snapshot().send_count, 2);
    }

    #[test]
    fn is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ClientMetrics>();
        assert_send_sync::<MetricsSnapshot>();
    }
}
