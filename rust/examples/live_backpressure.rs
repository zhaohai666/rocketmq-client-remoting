//! Precondition: NameServer + Broker are up, with `autoCreateTopicEnable=true`.
//!
//!
//!
//! ⚠ Four script disciplines (all learned from real-machine runs):
//!   (1) topics always carry a stamp -- multiple rounds run **sequentially** on the same broker, and a fixed name would inherit the previous round's counts;
//!   (2) "a rejected send never even emitted the request" can only be judged from broker-side stored counts (the sum of maxOffset-minOffset over queues),
//!      client callbacks alone can be fooled by an implementation that "reports an error in the callback yet still sends the request";
//!   (3) a newly created topic needs time for the broker to register the topicConfig delta to the namesrv (seconds to tens of seconds), so every
//!      broker-side reconciliation polls **until timeout**; one failed route read is not a failure;
//!   (4) holding in-flight slots works via `std::thread::sleep` inside the hook, which pins a tokio worker thread inside a
//!      synchronous call -- measured locally: **as long as any task blocks, the whole runtime's time driver stops advancing**
//!      (while one task slept 1500ms, a `sleep(100ms)` in another task never woke once). Therefore
//!        · the "in-flight sends" and the "sends that cannot get a permit" must be fired back-to-back, with no await in between to observe in-flight state;
//!        · in-flight charging and "topping up the over-limit sends at the moment the gate fills" are both delegated to [`GateWatch`] (a plain OS thread),
//!          and "did it enter the send kernel" uses the hook's own recorded timestamps -- neither is affected by runtime scheduling;
//!          moreover such assertions can only be read after the whole chain has finished, since the hook may still be stuck on the new topic's route;
//!        · anything that must act while the hook "is still sleeping" (B3's runtime resize) also has to run on a plain thread.
//!
//! Usage:
//! ```text
//! cargo run --example live_backpressure -- 127.0.0.1:9876
//! ```

use std::collections::BTreeMap;
use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::backpressure::{
    MIN_ASYNC_SEND_NUM, MIN_ASYNC_SEND_SIZE,
};
use rocketmq_client_remoting::client::hook::{SendMessageContext, SendMessageHook};
use rocketmq_client_remoting::client::producer::{
    DefaultMQProducer, ProducerConfig, SendCallback,
};
use rocketmq_client_remoting::client::result::SendResult;
use rocketmq_client_remoting::common::message::{Message, MessageQueue};
use rocketmq_client_remoting::error::Result;

/// Default budget (seconds) for cluster liveness and "waiting for broker-side results to land".
const WAIT_SECONDS: u64 = 30;
const GATE_NUM: i64 = MIN_ASYNC_SEND_NUM;
/// B4's single 600KB body: under the 1M byte gate only 1 send can be in flight.
const BIG_BODY: usize = 600 * 1024;
/// B3's in-flight hook sleep duration (ms): leaves a judgement window for "the resize happens before the 10 held sends return".
const HOLD_MS: i64 = 2_500;
/// Runtime worker thread count.
///
/// Must be clearly larger than the concurrent in-flight count: holding in-flight slots relies on the **blocking** sleep
/// inside `send_message_before` (the hook is a synchronous interface, it cannot await), so one in-flight send eats one worker thread.
const WORKER_THREADS: usize = 32;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// Assertion accumulator: all scenarios run before the summary; the first failure does not exit early.
struct Checker {
    passed: u32,
    failed: Vec<String>,
}

impl Checker {
    fn new() -> Checker {
        Checker {
            passed: 0,
            failed: Vec::new(),
        }
    }

    fn check(&mut self, name: &str, cond: bool, detail: &str) {
        if cond {
            self.passed += 1;
            println!("  [PASS] {name}");
        } else {
            println!("  [FAIL] {name}: {detail}");
            self.failed.push(format!("{name}: {detail}"));
        }
    }

    fn abort(&mut self, name: &str, err: &str) {
        println!("  [FAIL] {name}: {err}");
        self.failed.push(format!("{name}: {err}"));
    }
}

/// Thread-safe callback recorder.
#[derive(Default)]
struct Recorder {
    done: AtomicUsize,
    ok: AtomicUsize,
    errors: Mutex<Vec<String>>,
}

impl Recorder {
    fn done(&self) -> usize {
        self.done.load(Ordering::SeqCst)
    }

    fn ok(&self) -> usize {
        self.ok.load(Ordering::SeqCst)
    }

    fn errors(&self) -> Vec<String> {
        lock(&self.errors).clone()
    }

    fn all_errors_contain(&self, needle: &str, expected: usize) -> bool {
        let errors = lock(&self.errors);
        errors.len() == expected && errors.iter().all(|e| e.contains(needle))
    }

    fn summary(&self) -> String {
        let errors = lock(&self.errors);
        let first = errors.first().map(String::as_str).unwrap_or("");
        format!(
            "done={} ok={} err={} {first}",
            self.done(),
            self.ok(),
            errors.len()
        )
    }
}

impl SendCallback for Recorder {
    fn on_success(&self, result: SendResult) {
        if matches!(result.status, rocketmq_client_remoting::client::result::SendStatus::SendOk) {
            self.ok.fetch_add(1, Ordering::SeqCst);
        }
        self.done.fetch_add(1, Ordering::SeqCst);
    }

    fn on_exception(&self, err: rocketmq_client_remoting::error::Error) {
        lock(&self.errors).push(err.to_string());
        self.done.fetch_add(1, Ordering::SeqCst);
    }
}

/// Sleeps a while inside `send_message_before` to hold the in-flight slot, and records when each send entered the hook.
///
/// Permits are taken **at the gate** and returned **at the end of the chain**, so the cleanest way to hold in-flight state
/// is to make the chain itself slow: blocking in the user callback does not hold the permit (the return happens one step
/// before handing the result to the user).
///
/// Timestamps are recorded by the hook itself (relative to [`SlowHook::begin`]), not the main task's `Instant`: the hook's
/// `std::thread::sleep` pins a tokio worker thread, and measured locally **as long as any task blocks, the whole runtime's time driver stops advancing**,
/// so every `sleep` in the main task oversleeps until the hook finishes. Hence "who passed the gate and when" can only be taken from the chain, not from the observer.
struct SlowHook {
    millis: AtomicI64,
    clock: Mutex<Option<Instant>>,
    /// `(label, ms since begin when the send entered the hook)`; the label is the one [`message`] writes into keys.
    entries: Mutex<Vec<(String, u128)>>,
}

impl SlowHook {
    fn new(millis: i64) -> SlowHook {
        SlowHook {
            millis: AtomicI64::new(millis),
            clock: Mutex::new(None),
            entries: Mutex::new(Vec::new()),
        }
    }

    fn set(&self, millis: i64) {
        self.millis.store(millis, Ordering::SeqCst);
    }

    /// Resets the clock and clears the records so each scenario's observations are independent.
    fn begin(&self) {
        *lock(&self.clock) = Some(Instant::now());
        lock(&self.entries).clear();
    }

    /// How many sends labelled `label` entered the hook.
    fn count_of(&self, label: &str) -> usize {
        lock(&self.entries).iter().filter(|(l, _)| l == label).count()
    }

    /// Earliest moment a send labelled `label` entered the hook (`None` if it never did).
    fn first_entry_of(&self, label: &str) -> Option<u128> {
        lock(&self.entries)
            .iter()
            .filter(|(l, _)| l == label)
            .map(|(_, at)| *at)
            .min()
    }

    /// All records, for debugging the labels themselves on failure.
    fn dump(&self) -> String {
        lock(&self.entries)
            .iter()
            .map(|(label, at)| format!("{label}@{at}ms"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Milliseconds since [`begin`]; returns 0 if the clock has not started.
    ///
    /// Lets the plain thread that acts also report its moment -- on a loaded machine `sleep(1s)` is late by itself, and
    /// "the actor was late" vs "the waker never woke" are two different causes that misdiagnose when mixed.
    fn elapsed_of_clock(&self) -> u128 {
        match *lock(&self.clock) {
            Some(began) => began.elapsed().as_millis(),
            None => 0,
        }
    }
}

impl SendMessageHook for SlowHook {
    fn hook_name(&self) -> &str {
        "slow-before"
    }

    fn send_message_before(&self, context: &mut SendMessageContext) -> Result<()> {
        let label = context
            .message
            .as_ref()
            .and_then(|msg| msg.get_keys().map(str::to_string))
            .unwrap_or_default();
        let began = { *lock(&self.clock) }.unwrap_or_else(Instant::now);
        lock(&self.entries).push((label, began.elapsed().as_millis()));
        let millis = self.millis.load(Ordering::SeqCst);
        if millis > 0 {
            std::thread::sleep(Duration::from_millis(millis as u64));
        }
        Ok(())
    }
}

/// Uses a **plain OS thread** to watch a gate's free permits: as soon as it is first filled down to `target`, run `then`
/// at that instant (= top up the sends that "cannot get a permit"), while recording the minimum seen in the window.
///
/// Why this cannot be done in the main task (module-header discipline (4)): the hook's blocking sleep freezes the whole
/// runtime's time driver, and from "gate filled" to "hook done sleeping" the main task is never scheduled -- if it did
/// the observing and the topping-up, it would in practice become "send only after every permit was returned", yielding
/// a fake all-success result. An independent thread is unaffected by runtime scheduling: it sees the charging happen and can slip its requests in exactly while the charge is held.
struct GateWatch {
    stop: Arc<AtomicBool>,
    min: Arc<AtomicI64>,
    hit: Arc<AtomicBool>,
    task: Option<std::thread::JoinHandle<()>>,
}

impl GateWatch {
    fn spawn<F, G>(read: F, target: i64, then: G) -> GateWatch
    where
        F: Fn() -> i64 + Send + 'static,
        G: FnOnce() + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let min = Arc::new(AtomicI64::new(i64::MAX));
        let hit = Arc::new(AtomicBool::new(false));
        let (t_stop, t_min, t_hit) = (Arc::clone(&stop), Arc::clone(&min), Arc::clone(&hit));
        let task = std::thread::spawn(move || {
            let mut then = Some(then);
            let mut fired = false;
            while !t_stop.load(Ordering::SeqCst) {
                let seen = read();
                t_min.fetch_min(seen, Ordering::SeqCst);
                if !fired && seen <= target {
                    fired = true;
                    t_hit.store(true, Ordering::SeqCst);
                    if let Some(then) = then.take() {
                        then();
                    }
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        GateWatch { stop, min, hit, task: Some(task) }
    }

    /// Stops the thread and returns `(minimum free permits in the window, whether target was ever hit)`.
    fn finish(mut self) -> (i64, bool) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
        (
            self.min.load(Ordering::SeqCst),
            self.hit.load(Ordering::SeqCst),
        )
    }
}

struct Env {
    namesrv: String,
    stamp: String,
    admin: DefaultMQAdminExt,
    broker_addr: Mutex<String>,
    topics: Mutex<Vec<String>>,
}

impl Env {
    fn new(namesrv: &str, stamp: &str) -> Env {
        let admin = DefaultMQAdminExt::with_config(AdminConfig {
            instance_name: format!("BPADMIN-{stamp}"),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        Env {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            admin,
            broker_addr: Mutex::new(String::new()),
            topics: Mutex::new(Vec::new()),
        }
    }

    fn topic(&self, kind: &str) -> String {
        let t = format!("Bp{kind}{}", self.stamp);
        lock(&self.topics).push(t.clone());
        t
    }

    /// Cluster liveness: an open port != the broker registered to the namesrv, so poll.
    async fn start(&self, ck: &mut Checker) -> bool {
        if let Err(e) = self.admin.start().await {
            ck.abort("admin start", &e.to_string());
            return false;
        }
        let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
        loop {
            if let Ok(info) = self.admin.fetch_broker_cluster_info().await {
                if let Some(first) = info.get_broker_addrs().first() {
                    *lock(&self.broker_addr) = first.clone();
                    ck.check("cluster liveness", true, &format!("broker={first}"));
                    return true;
                }
            }
            if Instant::now() >= deadline {
                ck.abort("cluster liveness", "the nameServer returned no broker within the budget");
                return false;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Starts a producer configured only with the backpressure knobs. The hook must be attached before `start()`.
    async fn producer(
        &self,
        kind: &str,
        enable: bool,
        num: Option<i64>,
        size: Option<i64>,
        hook: Option<Arc<SlowHook>>,
    ) -> std::result::Result<DefaultMQProducer, String> {
        let producer = DefaultMQProducer::with_config(ProducerConfig {
            producer_group: format!("PID_rmq_bp_rust_{}", self.stamp),
            instance_name: format!("bp-rust-{kind}-{}", self.stamp),
            name_server_addrs: vec![self.namesrv.clone()],
            enable_backpressure_for_async_mode: enable,
            ..Default::default()
        })
        .map_err(|e| e.to_string())?;
        if let Some(num) = num {
            producer.set_back_pressure_for_async_send_num(num);
        }
        if let Some(size) = size {
            producer.set_back_pressure_for_async_send_size(size);
        }
        if let Some(hook) = hook {
            producer.register_send_message_hook(hook);
        }
        producer.start().await.map_err(|e| e.to_string())?;
        Ok(producer)
    }
}

// ---------------------------------------------------------- common helpers

/// Sends with a label: the label goes on the keys property and the hook classifies by it (see [`SlowHook`]).
fn message(topic: &str, label: &str, body: &[u8]) -> Message {
    let mut msg = Message::new(topic, Some(body));
    msg.set_keys(label);
    msg
}

/// Polls until the condition holds (at most `secs` seconds).
async fn wait_until<F: Fn() -> bool>(cond: F, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= deadline {
            return cond();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// How many messages landed on the broker for this topic in total (sum of maxOffset-minOffset over queues); -1 if unreadable.
/// This is the only hard evidence for "a rejected send never even emitted the request".
async fn landed_count(env: &Env, topic: &str) -> i64 {
    let Ok(route) = env.admin.examine_topic_route(topic).await else {
        return -1; // route not registered yet
    };
    let mut addrs: BTreeMap<String, String> = BTreeMap::new();
    for broker in &route.broker_datas {
        if let Some(addr) = broker.select_broker_addr() {
            addrs.insert(broker.broker_name.clone(), addr);
        }
    }
    let mut total = 0_i64;
    for queue in &route.queue_datas {
        if !addrs.contains_key(&queue.broker_name) {
            continue;
        }
        for queue_id in 0..queue.read_queue_nums {
            let mq = MessageQueue::new(topic, &queue.broker_name, queue_id);
            // Both reads can fail right after the queue is created; treat as "not ready yet" instead of 0
            let (max, min) =
                match (env.admin.max_offset(&mq).await, env.admin.min_offset(&mq).await) {
                    (Ok(max), Ok(min)) => (max, min),
                    _ => return -1,
                };
            total += max - min;
        }
    }
    total
}

/// Waits until at least `expected` messages appear on the broker; returns the last reading.
async fn wait_landed(env: &Env, topic: &str, expected: i64) -> i64 {
    let mut landed = landed_count(env, topic).await;
    let mut reads = 0;
    while landed < expected && reads < 60 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        landed = landed_count(env, topic).await;
        reads += 1;
    }
    landed
}

// ------------------------------------------------------- B1 default capacity does not leak quota

async fn b1_default_capacity_no_leak(ck: &mut Checker, env: &Env) {
    let topic = env.topic("B1");
    let producer = match env.producer("b1", true, None, None, None).await {
        Ok(p) => p,
        Err(e) => {
            ck.abort("B1 producer", &e);
            return;
        }
    };
    ck.check(
        "B1 default capacity is 1024 messages / 100M bytes",
        producer.semaphore_async_send_num_available_permits() == 1024
            && producer.semaphore_async_send_size_available_permits() == 100 * 1024 * 1024,
        &format!(
            "num={} size={}",
            producer.semaphore_async_send_num_available_permits(),
            producer.semaphore_async_send_size_available_permits()
        ),
    );

    let rec = Arc::new(Recorder::default());
    let mut rejected = 0usize;
    for i in 0..40 {
        let body = format!("b1-{i}-{}", "x".repeat(120));
        if producer
            .send_async(
                message(&topic, "b1", body.as_bytes()),
                rec.clone(),
                Some(5_000),
                None,
            )
            .is_err()
        {
            rejected += 1;
        }
    }
    ck.check("B1 none of the 40 send_async calls were rejected at enqueue", rejected == 0, "rejected={rejected}");
    ck.check(
        "B1 all 40 async sends completed their callbacks",
        wait_until(|| rec.done() >= 40, 20).await,
        &rec.summary(),
    );
    ck.check(
        "B1 all SEND_OK",
        rec.ok() == 40 && rec.errors().is_empty(),
        &rec.summary(),
    );
    let landed = wait_landed(env, &topic, 40).await;
    ck.check("B1 exactly 40 messages landed on the broker", landed == 40, &format!("landed={landed}"));
    ck.check(
        "B1 count permits fully returned",
        producer.semaphore_async_send_num_available_permits() == 1024,
        &format!(
            "available={}",
            producer.semaphore_async_send_num_available_permits()
        ),
    );
    ck.check(
        "B1 byte permits fully returned",
        producer.semaphore_async_send_size_available_permits() == 100 * 1024 * 1024,
        &format!(
            "available={}",
            producer.semaphore_async_send_size_available_permits()
        ),
    );
    producer.shutdown();
}

// ------------------------------------- B2/B3 count gate: rejection, reconciliation, runtime resize

async fn b2_and_b3_num_gate(ck: &mut Checker, env: &Env) {
    let topic = env.topic("B2");
    let slow = Arc::new(SlowHook::new(600));
    let producer = match env
        .producer("b2", true, Some(GATE_NUM), None, Some(slow.clone()))
        .await
    {
        Ok(p) => p,
        Err(e) => {
            ck.abort("B2 producer", &e);
            return;
        }
    };

    // ---- B2: 10 sends fill the in-flight slots; two more are fired by the watcher thread at the "filled" instant and cannot get a permit
    slow.begin();
    let held = Arc::new(Recorder::default());
    let rejected = Arc::new(Recorder::default());
    let began = Instant::now();
    let watch = GateWatch::spawn(
        {
            let producer = producer.clone();
            move || producer.semaphore_async_send_num_available_permits()
        },
        0,
        {
            let producer = producer.clone();
            let topic = topic.clone();
            let rejected = Arc::clone(&rejected);
            move || {
                for _ in 0..2 {
                    let _ = producer.send_async(
                        message(&topic, "b2-rejected", b"b2-rejected"),
                        rejected.clone(),
                        Some(150),
                        None,
                    );
                }
            }
        },
    );
    for _ in 0..GATE_NUM {
        let _ = producer.send_async(
            message(&topic, "b2-held", b"b2-held"),
            held.clone(),
            Some(8_000),
            None,
        );
    }
    let holders_done = wait_until(|| held.done() >= GATE_NUM as usize, 30).await;
    let victims_done = wait_until(|| rejected.done() >= 2, 15).await;
    let (min_free, drained) = watch.finish();
    ck.check(
        "B2 free count is 0 once the in-flight slots are filled",
        drained && min_free == 0,
        &format!("filled-hit={drained} min free in sampling window={min_free}"),
    );
    let both_failed = victims_done
        && rejected
            .all_errors_contain("send message tryAcquire semaphoreAsyncNum timeout", 2);
    ck.check(
        "B2 the two over-limit sends callback with TooMuchRequest, wording matched verbatim",
        both_failed,
        &rejected.summary(),
    );
    let waited = began.elapsed();
    ck.check(
        "B2 the gate waits until the budget is exhausted before erroring (not a glance-and-reject)",
        waited >= Duration::from_millis(130),
        &format!("waited {}ms (the time driver stalls while the hook blocks; this is a lower bound)", waited.as_millis()),
    );
    let landed = wait_landed(env, &topic, GATE_NUM).await;
    ck.check(
        "B2 only those 10 landed on the broker (not one rejected send got in)",
        holders_done && landed == GATE_NUM,
        &format!("landed={landed} {}", held.summary()),
    );
    // The rejected sends should **never have entered the send kernel**: the hook sits after the gate and before the network, so a single query tells.
    ck.check(
        "B2 all 10 held sends entered the send kernel and none of the 2 rejected did",
        slow.count_of("b2-held") == GATE_NUM as usize && slow.count_of("b2-rejected") == 0,
        &format!(
            "held entered the hook {} times, rejected entered the hook {} times",
            slow.count_of("b2-held"),
            slow.count_of("b2-rejected")
        ),
    );
    ck.check(
        "B2 count permits fully returned after everything landed",
        producer.semaphore_async_send_num_available_permits() == GATE_NUM,
        &format!(
            "available={}",
            producer.semaphore_async_send_num_available_permits()
        ),
    );

    // ---- B3: sleep 2.5s; the 11th send sticks at the gate and is woken by a **resize from another thread** mid-sleep
    //
    // The resize must land while the hook is still asleep, to distinguish "capacity changed and let it through" from
    // "the earlier 10 sends returned permits and let it through"; in that window runtime tasks are all frozen, so only a plain thread can do it.
    const RESIZE_AT: u128 = 1_000;
    slow.set(HOLD_MS);
    slow.begin();
    let held2 = Arc::new(Recorder::default());
    // The 11th send must be handed over only **after the gate is really filled**: the pool's dispatch races with the
    // main task's loop and whoever reaches the gate first is not deterministic -- if it passed first, the stuck one becomes the 10th held send,
    // and "it was let through by the resize" no longer holds. Observing and topping up are both left to the watcher thread (discipline (4)).
    let woken = Arc::new(Recorder::default());
    let woken_sent_at = Arc::new(AtomicI64::new(-1));
    let watch = GateWatch::spawn(
        {
            let producer = producer.clone();
            move || producer.semaphore_async_send_num_available_permits()
        },
        0,
        {
            let producer = producer.clone();
            let topic = topic.clone();
            let woken = Arc::clone(&woken);
            let (slow, woken_sent_at) = (slow.clone(), Arc::clone(&woken_sent_at));
            move || {
                woken_sent_at.store(slow.elapsed_of_clock() as i64, Ordering::SeqCst);
                let _ = producer.send_async(
                    message(&topic, "b3-woken", b"b3-woken"),
                    woken,
                    Some(15_000),
                    None,
                );
            }
        },
    );
    for _ in 0..GATE_NUM {
        let _ = producer.send_async(
            message(&topic, "b3-held", b"b3-held"),
            held2.clone(),
            Some(15_000),
            None,
        );
    }
    // The moment the resize thread **actually runs**: on a loaded machine `sleep(1s)` is late by itself, and
    // "the resize was late" vs "the resize failed to wake the waiter" are two entirely different causes that must be kept apart.
    let resize_fired_at = Arc::new(AtomicI64::new(-1));
    let _ = std::thread::spawn({
        let producer = producer.clone();
        let resize_fired_at = Arc::clone(&resize_fired_at);
        let fired = slow.clone();
        move || {
            std::thread::sleep(Duration::from_millis(RESIZE_AT as u64));
            resize_fired_at
                .store(fired.elapsed_of_clock() as i64, Ordering::SeqCst);
            producer.set_back_pressure_for_async_send_num(GATE_NUM + 2);
        }
    })
    .join();
    let (_min_free, sent_while_gated) = watch.finish();
    let sent_at = woken_sent_at.load(Ordering::SeqCst);
    // This is the premise of everything above: the 11th send must be handed over **after** the gate has emptied and
    // **before** the resize, otherwise it may simply walk through on the new capacity and nothing gets tested.
    ck.check(
        "B3 the 11th send is held at the gate (handed over before the resize)",
        sent_while_gated && sent_at >= 0 && (sent_at as u128) < RESIZE_AT,
        &format!("gate-emptied={sent_while_gated} handed-over-at={sent_at}ms resize at {RESIZE_AT}ms"),
    );
    ck.check(
        "B3 the woken send really went out",
        wait_until(|| woken.done() > 0, 20).await
            && woken.ok() == 1
            && woken.errors().is_empty(),
        &format!("{} resize actually happened at {}ms", woken.summary(), resize_fired_at.load(Ordering::SeqCst)),
    );
    // The moment is recorded by the hook itself: its kernel entry is both after the resize and before the 10 in-flight
    // returns, so the only thing that could have let it through is the resize itself.
    let woken_entry = slow.first_entry_of("b3-woken");
    ck.check(
        "B3 the 11th send sticks at the gate and enters the send kernel only at the resize moment",
        woken_entry.is_some_and(|at| at >= RESIZE_AT && at < HOLD_MS as u128)
            && slow.count_of("b3-held") == GATE_NUM as usize,
        &format!(
            "it entered the kernel at {woken_entry:?}ms (resize at {RESIZE_AT}ms; the 10 in-flight only return after ≈{}ms)",
            HOLD_MS
        ),
    );
    let all_done = wait_until(|| held2.done() >= GATE_NUM as usize, 30).await;
    // B2 landed 10 messages, B3 lands another 10 + 1
    let landed = wait_landed(env, &topic, GATE_NUM * 2 + 1).await;
    ck.check(
        "B3 a total of 21 messages landed on the broker",
        all_done && landed == GATE_NUM * 2 + 1,
        &format!(
            "landed={landed} hook-records=[{}] {}",
            slow.dump(),
            held2.summary()
        ),
    );
    ck.check(
        "B3 free permits after everything landed = the new capacity 12",
        producer.semaphore_async_send_num_available_permits() == GATE_NUM + 2,
        &format!(
            "available={}",
            producer.semaphore_async_send_num_available_permits()
        ),
    );
    producer.shutdown();
}

// ------------------------------------------------------------- B4 byte gate

async fn b4_size_gate(ck: &mut Checker, env: &Env) {
    let topic = env.topic("B4");
    let slow = Arc::new(SlowHook::new(1_500));
    let producer = match env
        .producer(
            "b4",
            true,
            Some(GATE_NUM),
            Some(MIN_ASYNC_SEND_SIZE),
            Some(slow.clone()),
        )
        .await
    {
        Ok(p) => p,
        Err(e) => {
            ck.abort("B4 producer", &e);
            return;
        }
    };
    let body = vec![b'x'; BIG_BODY];
    let expected_free = MIN_ASYNC_SEND_SIZE - BIG_BODY as i64;

    // One 600KB send in flight; as soon as the watcher thread sees "the byte gate filled by it" it tops up two sends that cannot get a permit (same as B2).
    slow.begin();
    let first = Arc::new(Recorder::default());
    let second = Arc::new(Recorder::default());
    let watch = GateWatch::spawn(
        {
            let producer = producer.clone();
            move || producer.semaphore_async_send_size_available_permits()
        },
        expected_free,
        {
            let producer = producer.clone();
            let topic = topic.clone();
            let body = body.clone();
            let second = Arc::clone(&second);
            move || {
                for _ in 0..2 {
                    let _ = producer.send_async(
                        message(&topic, "b4-big", &body),
                        second.clone(),
                        Some(150),
                        None,
                    );
                }
            }
        },
    );
    let _ = producer.send_async(
        message(&topic, "b4-big", &body),
        first.clone(),
        Some(15_000),
        None,
    );
    let first_done = wait_until(|| first.done() >= 1, 30).await;
    let victims_done = wait_until(|| second.done() >= 2, 15).await;
    let (min_free, drained) = watch.finish();
    ck.check(
        "B4 one 600KB send in flight charges exactly 600K byte permits",
        drained && min_free == expected_free,
        &format!(
            "charge-hit={drained} min free in sampling window={min_free} (expected {expected_free})"
        ),
    );
    let rejected_ok = victims_done
        && second.all_errors_contain(
            "send message tryAcquire semaphoreAsyncSize timeout",
            2,
        );
    ck.check(
        "B4 the two over-limit sends callback with semaphoreAsyncSize timeout, wording matched verbatim",
        rejected_ok,
        &second.summary(),
    );
    let landed = wait_landed(env, &topic, 1).await;
    ck.check(
        "B4 only that 1 message landed on the broker",
        first_done && landed == 1,
        &format!("landed={landed} {}", first.summary()),
    );
    // The two sends blocked by the byte gate **never entered the send kernel** (the hook sits after the gate), so on the
    // real cluster only one send ever crossed the chain; this also proves "when the byte gate blocks, count permits already
    // taken are not leaked" -- a leak would make the full-return check below permanently short those two permits.
    ck.check(
        "B4 only one send entered the send kernel (neither of the rejected two did)",
        slow.count_of("b4-big") == 1,
        &format!(
            "entered the hook {} times, all records=[{}]",
            slow.count_of("b4-big"),
            slow.dump()
        ),
    );
    ck.check(
        "B4 count permits fully returned after everything landed (the two rejected sends returned their own share)",
        producer.semaphore_async_send_num_available_permits() == GATE_NUM,
        &format!(
            "available={}",
            producer.semaphore_async_send_num_available_permits()
        ),
    );
    ck.check(
        "B4 byte permits fully returned after everything landed",
        producer.semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE,
        &format!(
            "available={}",
            producer.semaphore_async_send_size_available_permits()
        ),
    );
    producer.shutdown();
}

// ------------------------------------------------------- B5 switch off means no limiting

async fn b5_disabled_never_limits(ck: &mut Checker, env: &Env) {
    let topic = env.topic("B5");
    // Config deliberately set to 1 message / 1KB -- it gets clamped to the floor values 10 / 1M, but the switch is off, so nothing is limited at all
    let producer = match env.producer("b5", false, Some(1), Some(1024), None).await {
        Ok(p) => p,
        Err(e) => {
            ck.abort("B5 producer", &e);
            return;
        }
    };
    ck.check(
        "B5 out-of-range config is clamped to the floor values",
        producer.semaphore_async_send_num_available_permits() == GATE_NUM
            && producer.semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE,
        &format!(
            "num={} size={}",
            producer.semaphore_async_send_num_available_permits(),
            producer.semaphore_async_send_size_available_permits()
        ),
    );

    let rec = Arc::new(Recorder::default());
    let big = vec![b'y'; 300 * 1024];
    for i in 0..30 {
        let body: &[u8] = if i % 10 == 9 { &big } else { b"b5-small" };
        let _ = producer.send_async(
            message(&topic, "b5", body),
            rec.clone(),
            Some(15_000),
            None,
        );
    }
    let done = wait_until(|| rec.done() >= 30, 30).await;
    ck.check(
        "B5 with backpressure off, all 30 concurrent sends (including 3 x 300KB) land",
        done && rec.ok() == 30 && rec.errors().is_empty(),
        &rec.summary(),
    );
    let landed = wait_landed(env, &topic, 30).await;
    ck.check(
        "B5 30 messages landed on the broker",
        landed == 30,
        &format!("landed={landed}"),
    );
    ck.check(
        "B5 with the switch off, neither gate moved at all",
        producer.semaphore_async_send_num_available_permits() == GATE_NUM
            && producer.semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE,
        &format!(
            "num={} size={}",
            producer.semaphore_async_send_num_available_permits(),
            producer.semaphore_async_send_size_available_permits()
        ),
    );
    producer.shutdown();
}

// ------------------------------------------------------------------ cleanup

async fn cleanup(ck: &mut Checker, env: &Env) {
    let topics = lock(&env.topics).clone();
    for topic in topics {
        if let Err(e) = env.admin.delete_topic(&topic, None).await {
            println!("  [WARN] delete_topic({topic}) failed: {e}");
        }
    }
    ck.check("cleanup of this run's topics", true, "");
}

// ------------------------------------------------------------------ driver

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let env = Env::new(namesrv, &stamp);
    println!("== live async-send back-pressure check, namesrv={namesrv} stamp={stamp} ==");
    if !env.start(&mut ck).await {
        return ck;
    }
    b1_default_capacity_no_leak(&mut ck, &env).await;
    b2_and_b3_num_gate(&mut ck, &env).await;
    b4_size_gate(&mut ck, &env).await;
    b5_disabled_never_limits(&mut ck, &env).await;
    cleanup(&mut ck, &env).await;
    env.admin.shutdown();
    ck
}

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// A blocking hook pins worker threads, so `#[tokio::main]` must **not** be used here (its default thread count may be
/// only a few cores); build a larger multi-thread runtime explicitly instead.
fn main() -> ExitCode {
    let argv: Vec<String> = env::args().collect();
    let namesrv = argv
        .get(1)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:9876".to_string());
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKER_THREADS)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("failed to build the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let ck = runtime.block_on(run(&namesrv));
    println!(
        "== summary: {} passed, {} failed ==",
        ck.passed,
        ck.failed.len()
    );
    for f in &ck.failed {
        println!("   FAILED {f}");
    }
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
