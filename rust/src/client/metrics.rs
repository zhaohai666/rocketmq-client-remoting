//! Design notes (semantics match the online baseline):
//! - `snapshot()` returns the strongly typed [`MetricsSnapshot`],
//!   the key names and order of [`MetricsSnapshot::to_json_value`] are fixed;
//! - the clock is injectable only inside the **private** `record_*_side(failed, rt)`, so unit tests
//!   can assert RT precisely instead of sleeping on the wall clock.

use std::fmt;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Current millisecond timestamp (floating point).
///
/// RT can go negative when the clock rewinds -- deliberately unguarded (matching the online baseline takes priority).
fn now_ms() -> f64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs_f64() * 1000.0,
        Err(e) => -(e.duration().as_secs_f64() * 1000.0),
    }
}

/// Keep three decimals: round-half-to-even applied to the binary true value, i.e. the printed result of `{:.3}`.
/// On parse failure (inf/NaN) the value is returned unchanged, never a panic.
pub fn round3(value: f64) -> f64 {
    format!("{value:.3}").parse::<f64>().unwrap_or(value)
}

/// The metrics snapshot.
///
/// Float fields are all `round3`-rounded; integer fields are cumulative values.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct MetricsSnapshot {
    /// `sendCount`: number of **successful** sends.
    pub send_count: i64,
    /// `sendFailureCount`: number of failed sends.
    pub send_failure_count: i64,
    /// `sendRTSum`: total duration of success + failure (milliseconds).
    pub send_rt_sum: f64,
    /// `sendRTMax`: largest single duration.
    pub send_rt_max: f64,
    /// `sendRTMin`: smallest single duration.
    pub send_rt_min: f64,
    /// `sendRTAvg`: `rt_sum / send_count` (the denominator excludes failure counts), 0.0 when count is 0.
    pub send_rt_avg: f64,
    /// `consumeCount`: number of successful consumptions.
    pub consume_count: i64,
    /// `consumeFailureCount`: number of failed consumptions.
    pub consume_failure_count: i64,
    /// `consumeRTSum`: total duration of success + failure (milliseconds).
    pub consume_rt_sum: f64,
    /// `consumeRTMax`: largest single duration.
    pub consume_rt_max: f64,
    /// `consumeRTMin`: smallest single duration.
    pub consume_rt_min: f64,
    /// `consumeRTAvg`: `rt_sum / consume_count`, 0.0 when count is 0.
    pub consume_rt_avg: f64,
}

impl MetricsSnapshot {
    /// Convert to a JSON object with **fixed key names in fixed order**
    /// (`sendCount/sendFailureCount/sendRTSum/sendRTMax/sendRTMin/sendRTAvg` + the same names on the consume side).
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
    /// Print the JSON string, key names and order same as the snapshot fields,
    /// in serde_json's compact format.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClientMetrics{}", self.to_json_value())
    }
}

/// One side's (send or consume) internal accumulators: `count/failure_count/rt_*`.
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
    /// Accumulation logic shared by `record_*_success/failure`.
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

    /// The snapshot's half, returning `(count, failure_count, sum, max, min, avg)`.
    /// The avg denominator counts successes only, 0.0 when there are none.
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

/// Thread-safe basic send/consume metrics counter.
///
/// One `Mutex` inside, `Send + Sync`, can be wrapped in an `Arc` and shared between multiple
/// producer/consumer tasks. A poisoned lock still yields the inner value (the stats path must never panic).
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
    /// All-zero initial state.
    pub fn new() -> Self {
        Self::default()
    }

    fn locked(&self) -> MetricsState {
        *self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    // ---------------- send ----------------

    /// Send start: **only returns a timestamp**, does not mutate state.
    pub fn record_send_start(&self) -> f64 {
        now_ms()
    }

    /// Record one successful send (RT computed from the start time).
    pub fn record_send_success(&self, start_ms: f64) {
        self.record_send_side(false, now_ms() - start_ms);
    }

    /// Record one failed send: failures also add into rt_sum.
    pub fn record_send_failure(&self, start_ms: f64) {
        self.record_send_side(true, now_ms() - start_ms);
    }

    fn record_send_side(&self, failed: bool, rt: f64) {
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        state.send.record(failed, rt);
    }

    // ---------------- consume ----------------

    /// Consume start: **only returns a timestamp**, does not mutate state.
    pub fn record_consume_start(&self) -> f64 {
        now_ms()
    }

    /// Record one successful consumption (RT computed from the start time).
    pub fn record_consume_success(&self, start_ms: f64) {
        self.record_consume_side(false, now_ms() - start_ms);
    }

    /// Record one failed consumption (RT computed from the start time).
    pub fn record_consume_failure(&self, start_ms: f64) {
        self.record_consume_side(true, now_ms() - start_ms);
    }

    fn record_consume_side(&self, failed: bool, rt: f64) {
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        state.consume.record(failed, rt);
    }

    // ---------------- snapshot ----------------

    /// Take the lock once and read out the full **rounded** metrics.
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

    /// Equivalent implementation of the public path driven with an injected clock (`record_*` only takes now_ms from the wall clock).
    fn record(metrics: &ClientMetrics, consume: bool, failed: bool, start_ms: f64, now_ms: f64) {
        let mut state = metrics.inner.lock().unwrap_or_else(|e| e.into_inner());
        let side = if consume { &mut state.consume } else { &mut state.send };
        side.record(failed, now_ms - start_ms);
    }

    fn almost(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn fresh_snapshot_is_all_zero_and_matches_golden_dict() {
        let snapshot = ClientMetrics::new().snapshot();
        assert_eq!(snapshot, MetricsSnapshot::default());
        // Key names and order of an empty snapshot are fixed
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
        // Key order must match too (preserve_order in effect)
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
        assert!(before > 1_600_000_000_000.0, "millisecond timestamp magnitude: {before}");
        assert!(after >= before);
        // Neither start hook books anything
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
        // Rule: rt_sum includes failure durations, the avg denominator counts successes only.
        let metrics = ClientMetrics::new();
        record(&metrics, false, false, 0.0, 100.0); // rt 100
        record(&metrics, false, true, 0.0, 200.0); // rt 200 (failed)
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.send_count, 1);
        assert_eq!(snapshot.send_failure_count, 1);
        assert!(almost(snapshot.send_rt_sum, 300.0));
        assert!(almost(snapshot.send_rt_max, 200.0));
        assert!(almost(snapshot.send_rt_min, 100.0));
        // 300 / 1 = 300, not 300 / 2
        assert!(almost(snapshot.send_rt_avg, 300.0));
    }

    #[test]
    fn zero_success_count_gives_zero_avg_not_nan() {
        let metrics = ClientMetrics::new();
        record(&metrics, true, true, 0.0, 7.0); // failures only
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.consume_count, 0);
        assert_eq!(snapshot.consume_failure_count, 1);
        assert!(almost(snapshot.consume_rt_sum, 7.0));
        assert!(almost(snapshot.consume_rt_avg, 0.0));
        assert!(!snapshot.consume_rt_avg.is_nan());
    }

    #[test]
    fn min_and_max_only_move_outwards_once_started() {
        // Scenario: started already set, max=100, min=1, then record one huge rt
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
        // sum = 12.3460789 -> 12.346; max = 12.3456789 -> 12.346; min = 0.0004 -> 0.0
        assert!(almost(snapshot.send_rt_sum, 12.346), "{snapshot:?}");
        assert!(almost(snapshot.send_rt_max, 12.346), "{snapshot:?}");
        assert!(almost(snapshot.send_rt_min, 0.0), "{snapshot:?}");
        assert!(almost(snapshot.send_rt_avg, 6.173), "{snapshot:?}");
    }

    #[test]
    fn round3_matches_golden_on_half_way_vectors() {
        // Fixed vectors: binary true values + round-half-to-even
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
        // inf/NaN are returned unchanged, no panic
        assert!(round3(f64::INFINITY).is_infinite());
        assert!(round3(f64::NAN).is_nan());
    }

    #[test]
    fn real_clock_paths_accumulate_non_negative_rt() {
        // Drive the real record_send_* paths: only magnitude assertions, no sleeping on the wall clock
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
        // the rt_sum of one success + one failure is all used by avg's numerator
        assert!(snapshot.send_rt_avg >= snapshot.send_rt_min);
    }

    #[test]
    fn display_and_clone_semantics() {
        let metrics = ClientMetrics::new();
        record(&metrics, false, false, 0.0, 12.265);
        let text = metrics.snapshot().to_string();
        assert!(text.starts_with("ClientMetrics{"), "{text}");
        assert!(text.contains("\"sendRTSum\":12.265"), "{text}");
        // Clone is a "deep copy of the current values"; later records do not flow back into the copy
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
