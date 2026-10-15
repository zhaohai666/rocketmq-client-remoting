//! Prerequisites: NameServer + Broker are up, `autoCreateTopicEnable=true`.
//!
//!
//!
//!
//! ⚠ Four scripting rules of discipline (following `live_backpressure.rs`):
//!   ① every topic carries a stamp —— multiple rounds run **sequentially** on the same broker, and
//!   a fixed name would inherit the previous round's message counts;
//!   ② "a rejected send never even put the request on the wire" can only be observed from the
//!      broker-side stored count (the sum of maxOffset-minOffset over all queues); looking only at
//!      the client callback would be fooled by an implementation where "the callback reports an
//!      error but the request still went out";
//!   ③ a newly created topic has to wait for the broker to incrementally register its topicConfig
//!      with the namesrv (seconds to tens of seconds), so every broker-side reconciliation is a
//!      **poll until timeout**; a single failed route read does not count as a failure;
//!   ④ a `std::thread::sleep` inside a hook occupies one tokio worker thread, and on this machine
//!      it was measured that "as soon as any task blocks, the whole runtime's time driver stops
//!      advancing", so the observed values are only read after the hook has finished sleeping, and
//!      the caller-side measurement in A1 itself contains no `await`.
//!
//! Usage:
//! ```text
//! cargo run --example live_async_send -- 127.0.0.1:9876
//! ```

use std::collections::{BTreeMap, HashSet};
use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::ThreadId;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::hook::{
    CheckForbiddenContext, CheckForbiddenHook, SendMessageContext, SendMessageHook,
};
use rocketmq_client_remoting::client::producer::{
    DefaultMQProducer, ProducerConfig, SendCallback,
};
use rocketmq_client_remoting::client::result::{SendResult, SendStatus};
use rocketmq_client_remoting::common::message::{Message, MessageQueue};
use rocketmq_client_remoting::common::message_decoder::decode_message_id;
use rocketmq_client_remoting::error::{Error, Result};

/// Default budget (seconds) for the cluster liveness probe and for "wait until the broker-side
/// result lands".
const WAIT_SECONDS: u64 = 30;
/// A2's concurrency count.
const BURST: usize = 30;
/// Number of runtime worker threads: it must be clearly larger than the concurrency count (the
/// hook that occupies an in-flight send is a synchronous interface and pins one thread).
const WORKER_THREADS: usize = 32;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// Assertion accumulator: run all scenarios first and then summarize; the first failure does not
/// exit early.
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

/// Thread-safe callback recording.
///
/// Besides the result itself it records two more things: **which thread** the terminal callback
/// ran on (A1's thread criterion), and at **what moment** (A6's "`shutdown()` does not wait for
/// in-flight sends").
#[derive(Default)]
struct Recorder {
    done: AtomicUsize,
    ok: AtomicUsize,
    results: Mutex<Vec<SendResult>>,
    errors: Mutex<Vec<String>>,
    threads: Mutex<Vec<ThreadId>>,
    at: Mutex<Vec<Instant>>,
}

impl Recorder {
    fn done(&self) -> usize {
        self.done.load(Ordering::SeqCst)
    }

    fn ok(&self) -> usize {
        self.ok.load(Ordering::SeqCst)
    }

    fn results(&self) -> Vec<SendResult> {
        lock(&self.results).clone()
    }

    fn errors(&self) -> Vec<String> {
        lock(&self.errors).clone()
    }

    /// Which thread the `i`-th terminal callback ran on (`None` if there is none).
    fn thread_at(&self, i: usize) -> Option<ThreadId> {
        lock(&self.threads).get(i).copied()
    }

    /// How many callbacks reached their terminal state after `moment` (`Instant` is comparable
    /// across threads and its scale is the monotonic clock).
    fn after(&self, moment: Instant) -> usize {
        lock(&self.at).iter().filter(|at| **at > moment).count()
    }

    fn summary(&self) -> String {
        let errors = self.errors();
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
        if result.status == SendStatus::SendOk {
            self.ok.fetch_add(1, Ordering::SeqCst);
        }
        lock(&self.results).push(result);
        self.note();
    }

    fn on_exception(&self, err: Error) {
        lock(&self.errors).push(err.to_string());
        self.note();
    }
}

impl Recorder {
    /// Record the moment and the thread name of the terminal state (the caller must already hold
    /// the respective locks; here we only take our own two).
    fn note(&self) {
        lock(&self.threads).push(std::thread::current().id());
        lock(&self.at).push(Instant::now());
        self.done.fetch_add(1, Ordering::SeqCst);
    }
}

/// Sleeps a while inside `send_message_before` and records the thread each chain ran on plus the
/// before/after counts.
///
/// Sleeping in the before hook ＝ slowing down the whole "preparation + going live" chain, which
/// is exactly what we need to measure "the caller does not have to wait for the preparation".
struct TracingHook {
    millis: AtomicUsize,
    before: AtomicUsize,
    after: AtomicUsize,
    before_threads: Mutex<Vec<ThreadId>>,
}

impl TracingHook {
    fn new(millis: usize) -> Arc<TracingHook> {
        Arc::new(TracingHook {
            millis: AtomicUsize::new(millis),
            before: AtomicUsize::new(0),
            after: AtomicUsize::new(0),
            before_threads: Mutex::new(Vec::new()),
        })
    }

    fn before_count(&self) -> usize {
        self.before.load(Ordering::SeqCst)
    }

    fn after_count(&self) -> usize {
        self.after.load(Ordering::SeqCst)
    }

    fn first_before_thread(&self) -> Option<ThreadId> {
        lock(&self.before_threads).first().copied()
    }
}

impl SendMessageHook for TracingHook {
    fn hook_name(&self) -> &str {
        "tracing-before"
    }

    fn send_message_before(&self, _context: &mut SendMessageContext) -> Result<()> {
        lock(&self.before_threads).push(std::thread::current().id());
        self.before.fetch_add(1, Ordering::SeqCst);
        let millis = self.millis.load(Ordering::SeqCst);
        if millis > 0 {
            std::thread::sleep(Duration::from_millis(millis as u64));
        }
        Ok(())
    }

    fn send_message_after(&self, _context: &mut SendMessageContext) -> Result<()> {
        self.after.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default)]
struct ForbiddenTagHook {
    calls: AtomicUsize,
    modes: Mutex<Vec<String>>,
}

impl ForbiddenTagHook {
    fn new() -> Arc<ForbiddenTagHook> {
        Arc::new(ForbiddenTagHook::default())
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn saw_mode(&self, mode: &str) -> bool {
        lock(&self.modes).iter().all(|m| m == mode)
    }

    fn dump_modes(&self) -> String {
        lock(&self.modes).join(",")
    }
}

impl CheckForbiddenHook for ForbiddenTagHook {
    fn hook_name(&self) -> &str {
        "forbidden-tag"
    }

    fn check_forbidden(&self, context: &mut CheckForbiddenContext) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mode = context
            .communication_mode
            .map(|m| m.name().to_string())
            .unwrap_or_default();
        lock(&self.modes).push(mode);
        let tags = context
            .message
            .as_ref()
            .and_then(|msg| msg.get_tags())
            .unwrap_or_default()
            .to_string();
        if tags == "forbidden" {
            return Err(Error::client("live test: tag forbidden is not allowed"));
        }
        Ok(())
    }
}

struct Env {
    namesrv: String,
    stamp: String,
    admin: DefaultMQAdminExt,
    topics: Mutex<Vec<String>>,
}

impl Env {
    fn new(namesrv: &str, stamp: &str) -> Env {
        let admin = DefaultMQAdminExt::with_config(AdminConfig {
            instance_name: format!("ASYNCADMIN-{}", stamp),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        Env {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            admin,
            topics: Mutex::new(Vec::new()),
        }
    }

    fn topic(&self, kind: &str) -> String {
        let t = format!("Async{}{}", kind, self.stamp);
        lock(&self.topics).push(t.clone());
        t
    }

    /// Cluster liveness probe: an open port != the broker has registered with the namesrv, so we
    /// poll.
    async fn start(&self, ck: &mut Checker) -> bool {
        if let Err(e) = self.admin.start().await {
            ck.abort("admin start", &e.to_string());
            return false;
        }
        let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
        loop {
            if let Ok(info) = self.admin.fetch_broker_cluster_info().await {
                if let Some(first) = info.get_broker_addrs().first() {
                    ck.check("cluster liveness probe", true, &format!("broker={first}"));
                    return true;
                }
            }
            if Instant::now() >= deadline {
                ck.abort("cluster liveness probe", "the nameServer returned no broker within the budget");
                return false;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Start a producer with only the send knobs configured. The hook must be attached before
    /// `start()`.
    async fn producer(
        &self,
        kind: &str,
        send_hook: Option<Arc<TracingHook>>,
        forbidden: Option<Arc<ForbiddenTagHook>>,
    ) -> std::result::Result<DefaultMQProducer, String> {
        let producer = DefaultMQProducer::with_config(ProducerConfig {
            producer_group: format!("PID_rmq_async_rust_{}", self.stamp),
            instance_name: format!("async-rust-{kind}-{}", self.stamp),
            name_server_addrs: vec![self.namesrv.clone()],
            ..Default::default()
        })
        .map_err(|e| e.to_string())?;
        if let Some(hook) = send_hook {
            producer.register_send_message_hook(hook);
        }
        if let Some(hook) = forbidden {
            producer.register_check_forbidden_hook(hook);
        }
        producer.start().await.map_err(|e| e.to_string())?;
        Ok(producer)
    }
}

// ---------------------------------------------------------- common helpers

fn message(topic: &str, body: &[u8], tags: &str) -> Message {
    let mut msg = Message::new(topic, Some(body));
    if !tags.is_empty() {
        msg.set_tags(tags);
    }
    msg
}

/// Poll until the condition holds (at most `secs` seconds).
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

/// How many messages of this topic are stored on the broker in total (the sum of
/// maxOffset-minOffset over all queues); returns -1 when it cannot be read.
async fn landed_count(env: &Env, topic: &str) -> i64 {
    let Ok(route) = env.admin.examine_topic_route(topic).await else {
        return -1; // the route has not been registered yet
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
            match queue_landed(env, &mq).await {
                Ok(n) => total += n,
                Err(_) => return -1,
            }
        }
    }
    total
}

/// How many messages are stored on one queue (reading a just-created queue fails; leave it to the
/// caller to decide "not ready yet").
async fn queue_landed(env: &Env, mq: &MessageQueue) -> Result<i64> {
    let max = env.admin.max_offset(mq).await?;
    let min = env.admin.min_offset(mq).await?;
    Ok(max - min)
}

/// Wait until at least `expected` messages show up on the broker; returns the last reading.
async fn wait_landed(env: &Env, topic: &str, expected: i64) -> i64 {
    let mut landed = landed_count(env, topic).await;
    let mut reads = 0;
    while landed < expected && reads < 80 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        landed = landed_count(env, topic).await;
        reads += 1;
    }
    landed
}

// ------------------------------------------------- A1 returns immediately + thread criterion + read-back

async fn a1_non_blocking_and_read_back(ck: &mut Checker, env: &Env) {
    let topic = env.topic("ReadBack");
    let hook = TracingHook::new(400);
    let producer = match env.producer("a1", Some(hook.clone()), None).await {
        Ok(p) => p,
        Err(e) => return ck.abort("A1 producer", &e),
    };
    let rec = Arc::new(Recorder::default());
    let body = b"async-a1-readback".to_vec();

    // The caller-side section runs on the blocking thread pool (deviation ① in the module header):
    // threads there do not belong to the tokio worker set, so "neither the preparation nor the
    // callback runs on the caller thread" is the hard conclusion. This section contains no `await`.
    let caller = {
        let (p, r, topic) = (producer.clone(), rec.clone(), topic.clone());
        let h = hook.clone();
        let body = body.clone();
        tokio::task::spawn_blocking(move || {
            let began = Instant::now();
            let rejected = match p.send_async(message(&topic, &body, "TagAsync"), r, Some(5_000), None)
            {
                Ok(()) => None,
                Err(e) => Some(e.to_string()),
            };
            (
                std::thread::current().id(),
                began.elapsed(),
                rejected,
                h.before_count(),
            )
        })
    }
    .await;
    let (caller_thread, took, rejected, before_at_return) = match caller {
        Ok(v) => v,
        Err(e) => return ck.abort("A1 the caller-side section", &e.to_string()),
    };
    ck.check(
        "A1 the caller returned before the preparation stage (the hook sleeps 400ms)",
        rejected.is_none() && took < Duration::from_millis(200),
        &format!(
            "caller took={}µs enqueue rejected={rejected:?} before already ran={before_at_return}",
            took.as_micros()
        ),
    );
    ck.check(
        "A1 the callback fired exactly once and was SEND_OK",
        wait_until(|| rec.done() >= 1, 20).await && rec.done() == 1 && rec.ok() == 1
            && rec.errors().is_empty(),
        &rec.summary(),
    );
    ck.check(
        "A1 the before hook does not run on the caller thread (async send thread pool)",
        hook.first_before_thread().is_some_and(|t| t != caller_thread),
        &format!(
            "hook thread={:?} caller thread={caller_thread:?}",
            hook.first_before_thread()
        ),
    );
    ck.check(
        "A1 the user callback does not run on the caller thread (shared callback thread pool)",
        rec.thread_at(0).is_some_and(|t| t != caller_thread),
        &format!(
            "callback thread={:?} caller thread={caller_thread:?}",
            rec.thread_at(0)
        ),
    );
    ck.check(
        "A1 before/after each ran exactly once",
        hook.before_count() == 1 && hook.after_count() == 1,
        &format!(
            "before={} after={}",
            hook.before_count(),
            hook.after_count()
        ),
    );
    let results = rec.results();
    let Some(first) = results.first() else {
        producer.shutdown();
        return ck.abort("A1 read-back", &rec.summary());
    };
    ck.check(
        "A1 msgId is the client-side UNIQ_KEY and differs from the broker-side offsetMsgId",
        first
            .msg_id
            .as_deref()
            .is_some_and(|id| id.len() == 32 && decode_message_id(id).is_ok())
            && first.offset_msg_id.is_some()
            && first.msg_id != first.offset_msg_id,
        &format!(
            "msgId={:?} offsetMsgId={:?}",
            first.msg_id, first.offset_msg_id
        ),
    );
    // The offsetMsgId is given by the broker, and only it can be decoded into a commitLog offset
    // ⇒ read the body back with it.
    let Some(offset_msg_id) = first.offset_msg_id.clone() else {
        producer.shutdown();
        return ck.abort("A1 read-back", "the callback carried no offsetMsgId");
    };
    let mut back = None;
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        match env.admin.view_message(&topic, &offset_msg_id).await {
            Ok(msg) => {
                back = Some(msg);
                break;
            }
            // the commitLog has not been flushed yet
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    let read_back = back.as_ref().is_some_and(|m| {
        m.body.as_deref() == Some(body.as_slice()) && m.topic == topic
    });
    ck.check(
        "A1 the offsetMsgId from the callback can read this message back",
        read_back,
        &format!(
            "offsetMsgId={offset_msg_id} body={:?}",
            back.as_ref().and_then(|m| m.body.as_ref()).map(|b| &b[..])
        ),
    );
    if let Some(mq) = first.message_queue.clone() {
        let max = env.admin.max_offset(&mq).await.unwrap_or(-1);
        ck.check(
            "A1 the queueOffset in the callback is exactly where it landed",
            first.queue_offset == max - 1,
            &format!("queueOffset={} maxOffset={}", first.queue_offset, max),
        );
    } else {
        ck.abort("A1 the queueOffset in the callback is exactly where it landed", "the callback carried no MessageQueue");
    }
    ck.check(
        "A1 the broker stored this one message",
        wait_landed(env, &topic, 1).await == 1,
        &format!("landed={}", landed_count(env, &topic).await),
    );
    producer.shutdown();
}

// ------------------------------------------------- A2 no cross-talk under concurrency

async fn a2_burst_exactly_once_each(ck: &mut Checker, env: &Env) {
    let topic = env.topic("Burst");
    let producer = match env.producer("a2", None, None).await {
        Ok(p) => p,
        Err(e) => return ck.abort("A2 producer", &e),
    };
    let each: Vec<Arc<Recorder>> = (0..BURST).map(|_| Arc::new(Recorder::default())).collect();
    for (i, rec) in each.iter().enumerate() {
        let body = format!("async-burst-{i}");
        if let Err(e) = producer.send_async(
            message(&topic, body.as_bytes(), "TagBurst"),
            rec.clone(),
            Some(8_000),
            None,
        ) {
            return ck.abort("A2 all 30 concurrent sends got their callbacks", &format!("send #{i} was rejected at enqueue: {e}"));
        }
    }
    let all_back = wait_until(|| each.iter().all(|r| r.done() >= 1), 20).await;
    let total_done: usize = each.iter().map(|r| r.done()).sum();
    ck.check(
        "A2 every one of the 30 concurrent async sends reached a terminal state",
        all_back && total_done == BURST,
        &format!("total callbacks={total_done} sent={BURST}"),
    );
    let mut multi = 0usize;
    let mut not_ok = 0usize;
    let mut slots: HashSet<String> = HashSet::new();
    let mut uniq: HashSet<String> = HashSet::new();
    let mut first_errors = String::new();
    for rec in &each {
        if rec.done() != 1 {
            multi += 1;
        }
        let results = rec.results();
        if results.len() != 1 || results[0].status != SendStatus::SendOk {
            not_ok += 1;
            if first_errors.is_empty() {
                first_errors = rec.summary();
            }
        }
        for r in results {
            let slot = match &r.message_queue {
                Some(mq) => format!("{}#{}@{}", mq.broker_name, mq.queue_id, r.queue_offset),
                None => "no-queue".to_string(),
            };
            slots.insert(slot);
            uniq.insert(r.msg_id.clone().unwrap_or_default());
        }
    }
    ck.check(
        "A2 each send has **exactly one** terminal state (no more, no less)",
        multi == 0,
        &format!("sends with extra callbacks={multi}"),
    );
    ck.check(
        "A2 everything is SEND_OK",
        not_ok == 0,
        &format!("{not_ok} sends were not OK, e.g.: {first_errors}"),
    );
    ck.check(
        "A2 exactly 30 messages are stored on the broker",
        wait_landed(env, &topic, BURST as i64).await == BURST as i64,
        &format!("landed={}", landed_count(env, &topic).await),
    );
    ck.check(
        "A2 the (broker, queueId, queueOffset) of each send do not overlap",
        slots.len() == BURST,
        &format!("after dedup={}", slots.len()),
    );
    ck.check(
        "A2 every send has a distinct UNIQ_KEY",
        uniq.len() == BURST,
        &format!("after dedup={}", uniq.len()),
    );
    producer.shutdown();
}

// ------------------------------------------------- A3 aimed send

async fn a3_pinned_queue(ck: &mut Checker, env: &Env) {
    let topic = env.topic("Pinned");
    let producer = match env.producer("a3", None, None).await {
        Ok(p) => p,
        Err(e) => return ck.abort("A3 producer", &e),
    };
    // First grow the topic into shape (send one message synchronously so the broker creates all
    // the queues), then pick one queue to aim at.
    // ⚠ Before taking the baseline we must wait until the warm-up send is **already readable** on
    // the broker side: a just-acked message lands in the consumeQueue with a delay, so a baseline
    // read as 0 turns into 1 at reconciliation time and conjures one extra message out of thin air.
    let mut warmup = message(&topic, b"async-a3-warmup", "TagA3");
    match producer.send(&mut warmup, Some(5_000), None).await {
        Ok(_) => {}
        Err(e) => return ck.abort("A3 the warm-up send", &e.to_string()),
    }
    let warmed = wait_landed(env, &topic, 1).await;
    ck.check(
        "A3 the warm-up send is already readable on the broker",
        warmed >= 1,
        &format!("landed={warmed}"),
    );
    let queues = match producer.fetch_publish_message_queues(&topic).await {
        Ok(q) => q,
        Err(e) => return ck.abort("A3 got the publish queue", &e.to_string()),
    };
    ck.check(
        "A3 got the publish queue",
        !queues.is_empty(),
        &format!("queues={}", queues.len()),
    );
    let Some(aimed) = queues.first().cloned() else {
        producer.shutdown();
        return;
    };
    let before = queue_landed(env, &aimed).await.unwrap_or(-1);
    let mut others_before = 0_i64;
    for mq in queues.iter().skip(1) {
        others_before += queue_landed(env, mq).await.unwrap_or(0).max(0);
    }

    let rec = Arc::new(Recorder::default());
    if let Err(e) = producer.send_async(
        message(&topic, b"async-a3-pinned", "TagA3"),
        rec.clone(),
        Some(5_000),
        Some(aimed.clone()),
    ) {
        return ck.abort("A3 aimed async send", &e.to_string());
    }
    ck.check(
        "A3 the aimed async send reached a terminal state and is SEND_OK",
        wait_until(|| rec.done() >= 1, 20).await && rec.done() == 1 && rec.ok() == 1,
        &rec.summary(),
    );
    let Some(r) = rec.results().first().cloned() else {
        producer.shutdown();
        return ck.abort("A3 aimed landing reconciliation", &rec.summary());
    };
    ck.check(
        "A3 the result landed on the designated queue",
        r.message_queue.as_ref() == Some(&aimed),
        &format!("{:?}", r.message_queue),
    );
    let mut after_aimed = queue_landed(env, &aimed).await.unwrap_or(-1);
    let deadline = Instant::now() + Duration::from_secs(20);
    while after_aimed != before + 1 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        after_aimed = queue_landed(env, &aimed).await.unwrap_or(-1);
    }
    ck.check(
        "A3 that queue has exactly one more message",
        after_aimed == before + 1,
        &format!("landed={after_aimed} before={before}"),
    );
    let mut others_after = 0_i64;
    for mq in queues.iter().skip(1) {
        others_after += queue_landed(env, mq).await.unwrap_or(0).max(0);
    }
    ck.check(
        "A3 no other queue gained a single message",
        others_after == others_before,
        &format!("other queues {others_before} -> {others_after}"),
    );
    producer.shutdown();
}

// ------------------------------------------------- A4 intercepting hook

async fn a4_forbidden_hook(ck: &mut Checker, env: &Env) {
    let topic = env.topic("Forbidden");
    let forbidden = ForbiddenTagHook::new();
    let producer = match env.producer("a4", None, Some(forbidden.clone())).await {
        Ok(p) => p,
        Err(e) => return ck.abort("A4 producer", &e),
    };

    let rejected = Arc::new(Recorder::default());
    let bad = message(&topic, b"async-a4-rejected", "forbidden");
    if let Err(e) = producer.send_async(bad, rejected.clone(), Some(5_000), None) {
        return ck.abort("A4 the exception rejected by the hook reached the callback verbatim", &e.to_string());
    }
    ck.check(
        "A4 the exception rejected by the hook reached the callback verbatim",
        wait_until(|| rejected.done() >= 1, 10).await
            && rejected
                .errors()
                .first()
                .is_some_and(|e| e.contains("tag forbidden is not allowed")),
        &rejected.summary(),
    );
    ck.check(
        "A4 the intercepting hook sees ASYNC",
        forbidden.calls() >= 1 && forbidden.saw_mode("ASYNC"),
        &format!("modes={}", forbidden.dump_modes()),
    );
    // This topic has nothing but the rejected send in it ⇒ either we read 0 messages, or even the
    // route is not registered yet (-1). The route is only created by autoCreate **once the first
    // message lands on the broker**, so "the route cannot be read" is itself evidence that "the
    // broker never received a request".
    let after_reject = landed_count(env, &topic).await;
    ck.check(
        "A4 the rejected send left no trace on the broker",
        after_reject <= 0,
        &format!("landed={after_reject} (-1 = the route has not been created, i.e. the broker received nothing at all)"),
    );

    let passed = Arc::new(Recorder::default());
    let ok = message(&topic, b"async-a4-ok", "TagA4");
    if let Err(e) = producer.send_async(ok, passed.clone(), Some(5_000), None) {
        return ck.abort("A4 the same producer with a different tag lands normally", &e.to_string());
    }
    ck.check(
        "A4 the same producer with a different tag lands normally (the rejection did not break the pool)",
        wait_until(|| passed.done() >= 1, 20).await && passed.ok() == 1,
        &passed.summary(),
    );
    let landed = wait_landed(env, &topic, 1).await;
    ck.check(
        "A4 exactly 1 message is stored on the broker",
        landed == 1,
        &format!("landed={landed}"),
    );
    ck.check(
        "A4 the hook was called twice in total (one send rejected, one let through)",
        forbidden.calls() == 2,
        &format!("calls={}", forbidden.calls()),
    );
    producer.shutdown();
}

/// The sum of the messages already stored on a set of queues (`.await` cannot live inside
/// `map(...).sum()`, so it has to be spelled out).
async fn queues_landed(env: &Env, queues: &[MessageQueue]) -> i64 {
    let mut total = 0_i64;
    for mq in queues {
        total += queue_landed(env, mq).await.unwrap_or(0).max(0);
    }
    total
}

// ------------------------------------------------- A5 batch async (send_batch_async)

/// Every assertion is deliberately placed on the side that "running only the offline unit tests
/// cannot reveal": whether the callback is delivered, whether the batch core's local validation was
/// bypassed by the async path, whether errors are silently swallowed, and whether a permit leaks.
async fn a5_batch_async(ck: &mut Checker, env: &Env) {
    let topic = env.topic("BatchAsync");
    let producer = match env.producer("a5", None, None).await {
        Ok(p) => p,
        Err(e) => return ck.abort("A5 producer", &e),
    };

    // ① One batch of 5 messages: the callback fires exactly once, SEND_OK, and really 5 messages
    //    land on the broker.
    //    Looking only at the callback would be fooled by "the callback reported OK but the request
    //    never went out" —— that one has to be reconciled on the broker side.
    let rec = Arc::new(Recorder::default());
    let batch: Vec<Message> = (0..5)
        .map(|i| message(&topic, format!("async-a5-{i}").as_bytes(), "TagA5"))
        .collect();
    if let Err(e) = producer.send_batch_async(batch, rec.clone(), Some(10_000), None) {
        return ck.abort("A5 batch async submit", &e.to_string());
    }
    ck.check(
        "A5 one batch of 5 messages: the callback fired exactly once and is SEND_OK",
        wait_until(|| rec.done() >= 1, 20).await
            && rec.done() == 1
            && rec.ok() == 1
            && rec.errors().is_empty()
            && rec.results().first().is_some_and(|r| {
                r.msg_id.as_ref().is_some_and(|id| !id.is_empty())
            }),
        &rec.summary(),
    );
    let landed = wait_landed(env, &topic, 5).await;
    ck.check(
        "A5 really 5 messages landed on the broker (not just what the callback claimed)",
        landed == 5,
        &format!("landed={landed} (-1 = the route has not been created yet)"),
    );

    let batch_offset = rec.results().first().and_then(|r| r.offset_msg_id.clone());
    // The offsetMsgId of a batch reply is a string the broker returns **per message** (comma
    // separated, one commitLog offset per sub-message), so it is itself evidence that "this batch
    // really was split and stored as 5 messages"; when reading back we take the first offset.
    let sub_offsets: Vec<&str> = batch_offset
        .as_deref()
        .map(|s| s.split(',').filter(|o| !o.is_empty()).collect())
        .unwrap_or_default();
    ck.check(
        "A5 the broker returned 5 commitLog offsets one by one (the batch really was split and landed)",
        sub_offsets.len() == 5 && sub_offsets.iter().all(|o| decode_message_id(o).is_ok()),
        &format!("offsetMsgId={batch_offset:?}"),
    );
    let mut stored_uniq = None;
    if let Some(&id) = sub_offsets.first() {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(m) = env.admin.view_message(&topic, id).await {
                // MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX
                stored_uniq = m.get_property("UNIQ_KEY").map(str::to_string);
                break;
            }
            if Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    ck.check(
        "A5 the sub-messages stored on the broker carry the client-side UNIQ_KEY (the per-message id is encoded in the body)",
        stored_uniq.as_deref().is_some_and(|u| u.len() == 32),
        &format!("stored UNIQ_KEY={stored_uniq:?} offsetMsgId={batch_offset:?}"),
    );
    ck.check(
        "A5 the batch SendResult.msgId is the batch's own 32-hex id, not the offsetMsgId",
        rec.results().first().is_some_and(|r| {
            r.msg_id.as_deref().is_some_and(|id| id.len() == 32 && !id.contains(','))
                && r.msg_id != r.offset_msg_id
        }),
        &rec
            .results()
            .first()
            .map(|r| format!("msgId={:?} offsetMsgId={:?}", r.msg_id, r.offset_msg_id))
            .unwrap_or_default(),
    );

    // ② Aimed batch: the `mq` argument really reaches the batch core instead of being dropped and
    //    having a queue picked on its own.
    let queues = match producer.fetch_publish_message_queues(&topic).await {
        Ok(q) if !q.is_empty() => q,
        Ok(_) => return ck.abort("A5 aimed batch", "the topic has no usable queue"),
        Err(e) => return ck.abort("A5 aimed batch fetch queues", &e.to_string()),
    };
    let aimed = queues[0].clone();
    let aimed_before = queue_landed(env, &aimed).await.unwrap_or(-1);
    let others_before = queues_landed(env, &queues[1..]).await;
    let pinned = Arc::new(Recorder::default());
    let pin_batch: Vec<Message> = (0..3)
        .map(|i| message(&topic, format!("async-a5-pin-{i}").as_bytes(), "TagA5"))
        .collect();
    if let Err(e) =
        producer.send_batch_async(pin_batch, pinned.clone(), Some(10_000), Some(aimed.clone()))
    {
        return ck.abort("A5 aimed batch submit", &e.to_string());
    }
    let pinned_ok = wait_until(|| pinned.done() >= 1, 20).await
        && pinned.done() == 1
        && pinned.ok() == 1;
    let landed_on_aimed = pinned
        .results()
        .first()
        .is_some_and(|r| r.message_queue.as_ref() == Some(&aimed));
    let mut aimed_after = queue_landed(env, &aimed).await.unwrap_or(-1);
    let deadline = Instant::now() + Duration::from_secs(20);
    while aimed_after != aimed_before + 3 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        aimed_after = queue_landed(env, &aimed).await.unwrap_or(-1);
    }
    let others_after = queues_landed(env, &queues[1..]).await;
    ck.check(
        "A5 the aimed batch lands on the designated queue and no other queue gained a single message",
        pinned_ok
            && landed_on_aimed
            && aimed_after == aimed_before + 3
            && others_after == others_before,
        &format!(
            "ok={} landed_on_aimed={landed_on_aimed} that queue {aimed_before}->{aimed_after} others {others_before}->{others_after}",
            pinned.ok()
        ),
    );

    let mixed = Arc::new(Recorder::default());
    let mixed_batch = vec![
        message(&topic, b"async-a5-mixed-1", "TagA5"),
        message(&env.topic("BatchOther"), b"async-a5-mixed-2", "TagA5"),
    ];
    if let Err(e) = producer.send_batch_async(mixed_batch, mixed.clone(), Some(5_000), None) {
        return ck.abort("A5 mixed-topic batch submit", &e.to_string());
    }
    ck.check(
        "A5 a batch mixing topics reports an error in the callback (local validation was not bypassed by the async path)",
        wait_until(|| mixed.done() >= 1, 10).await
            && mixed.done() == 1
            && mixed.ok() == 0
            && mixed
                .errors()
                .first()
                .is_some_and(|e| e.contains("should be the same")),
        &mixed.summary(),
    );

    // ④ Empty batch: the same criterion —— the error goes to the callback and not a single callback
    //    is owed.
    let empty = Arc::new(Recorder::default());
    if let Err(e) = producer.send_batch_async(Vec::new(), empty.clone(), Some(5_000), None) {
        return ck.abort("A5 empty batch submit", &e.to_string());
    }
    ck.check(
        "A5 an empty batch also yields exactly one failure callback and is not silently swallowed",
        wait_until(|| empty.done() >= 1, 10).await
            && empty.done() == 1
            && empty.ok() == 0
            && empty
                .errors()
                .first()
                .is_some_and(|e| e.contains("message list is empty")),
        &empty.summary(),
    );

    // ⑤ The backpressure charges the bytes of the **whole batch**, not of "one message": push the
    //    byte gate down to the floor (1 MiB), and a batch of 2×600 KiB must be blocked by the gate
    //    (the whole batch is 1.2 MiB > 1 MiB); a batch of 2×100 KiB must instead get through.
    //    If it only counted the first message (600 KiB) or did not charge at all, these two cases
    //    would both invert.
    producer.set_enable_backpressure_for_async_mode(true);
    producer.set_back_pressure_for_async_send_num(1);
    producer.set_back_pressure_for_async_send_size(1024 * 1024);
    let big_body = vec![b'x'; 600 * 1024];
    let gated = Arc::new(Recorder::default());
    let big_batch = vec![
        message(&topic, &big_body, "TagA5"),
        message(&topic, &big_body, "TagA5"),
    ];
    if let Err(e) = producer.send_batch_async(big_batch, gated.clone(), Some(2_000), None) {
        return ck.abort("A5 over-gate batch submit", &e.to_string());
    }
    ck.check(
        "A5 the whole 1.2MiB batch is blocked by the 1MiB byte gate (the callback gets TooMuchRequest and not a single request went out)",
        wait_until(|| gated.done() >= 1, 15).await
            && gated.done() == 1
            && gated.ok() == 0
            && gated
                .errors()
                .first()
                .is_some_and(|e| e.contains("semaphoreAsyncSize timeout")),
        &gated.summary(),
    );
    // A small batch under the very same gate must still land normally, proving that "being blocked"
    //    is because of the whole-batch size and not because batches are rejected outright
    let small_body = vec![b'y'; 100 * 1024];
    let passed = Arc::new(Recorder::default());
    let small_batch = vec![
        message(&topic, &small_body, "TagA5"),
        message(&topic, &small_body, "TagA5"),
    ];
    if let Err(e) = producer.send_batch_async(small_batch, passed.clone(), Some(10_000), None) {
        return ck.abort("A5 small batch submit", &e.to_string());
    }
    ck.check(
        "A5 a 200KiB batch under the very same gate still gets SEND_OK",
        wait_until(|| passed.done() >= 1, 20).await && passed.done() == 1 && passed.ok() == 1,
        &passed.summary(),
    );
    // The permits must be returned as they were: leaking even one means that "after a long run all
    // async sends time out together", which a single send cannot reveal (the count gate only let 1
    // through, so the blocked send has to give back the count permit it already took).
    let num_total = producer.get_back_pressure_for_async_send_num();
    let size_total = producer.get_back_pressure_for_async_send_size();
    ck.check(
        "A5 the batch path returns both permits unchanged (the idle amounts are back to the totals, including the send blocked by the gate)",
        wait_until(
            || producer.semaphore_async_send_num_available_permits() == num_total
                && producer.semaphore_async_send_size_available_permits() == size_total,
            10,
        )
        .await,
        &format!(
            "idle counts={} total={num_total} idle bytes={} total={size_total}",
            producer.semaphore_async_send_num_available_permits(),
            producer.semaphore_async_send_size_available_permits()
        ),
    );
    producer.shutdown();
}

// ------------------------------------------------- A6 shutdown does not wait for in-flight sends

async fn a6_shutdown_does_not_wait(ck: &mut Checker, env: &Env) {
    let topic = env.topic("Drain");
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let sends = (cores * 3).max(1);
    let sent = sends as i64;
    let hook = TracingHook::new(100);
    let producer = match env.producer("a6", Some(hook.clone()), None).await {
        Ok(p) => p,
        Err(e) => return ck.abort("A6 producer", &e),
    };
    let rec = Arc::new(Recorder::default());
    for i in 0..sends {
        let body = format!("async-a6-{i}");
        let msg = message(&topic, body.as_bytes(), "TagA6");
        if let Err(e) = producer.send_async(msg, rec.clone(), Some(8_000), None) {
            return ck.abort("A6 every send was accepted", &format!("send #{i}: {e}"));
        }
    }
    let began = Instant::now();
    producer.shutdown();
    let shut_took = began.elapsed();
    let returned_at = Instant::now();
    ck.check(
        "A6 every send got a terminal callback (not waiting does not mean callbacks vanish into thin air)",
        wait_until(|| rec.done() >= sends, 30).await && rec.done() == sends,
        &format!("done={} sent={sends} {}", rec.done(), rec.summary()),
    );
    let after_return = rec.after(returned_at);
    // The real evidence is "the return took far less than one preparation stage": each before hook
    // sleeps 100ms and the pool only has cores*2 threads, so a shutdown that waited for in-flight
    // sends would take at least a few hundred ms. A previous version also required "only 1 send is
    // delivered before the return", which was gambling on scheduling: the submit loop of 36 sends
    // itself takes tens of ms, so the first few to finish could easily reach their terminal state
    // before shutdown returns.
    ck.check(
        "A6 shutdown returns faster than a single preparation stage (it did not wait for in-flight sends)",
        shut_took < Duration::from_millis(100),
        &format!("return took={}ms, one preparation stage=100ms", shut_took.as_millis()),
    );
    ck.check(
        "A6 shutdown does not wait for in-flight sends: most callbacks land only after it returns",
        after_return * 2 >= sends,
        &format!(
            "only {after_return}/{sends} sends reached their terminal state after shutdown returned, which took={}ms",
            shut_took.as_millis()
        ),
    );
    let errors = rec.errors().len();
    ck.check(
        "A6 the price of not waiting: the client shuts down first and virtually every in-flight send errors out",
        errors * 2 >= sends,
        &format!("errors={errors}/{sends}, e.g.: {:?}", rec.errors().first()),
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    let landed = landed_count(env, &topic).await;
    ck.check(
        "A6 far fewer messages landed on the broker than were sent (not waiting really does lose messages)",
        sent > landed.max(0) * 2,
        &format!("landed={landed} sent={sends}"),
    );
    println!(
        "     A6 detail: landed={landed}/{sends} ok={} errors={errors} shutdown returned in={}ms,\
         callbacks reaching a terminal state only after the return={after_return}, first error={:?}",
        rec.ok(),
        shut_took.as_millis(),
        rec.errors().first()
    );

    let after_shutdown = producer
        .send_async(
            message(&topic, b"async-a6-after", "TagA6"),
            rec.clone(),
            Some(1_000),
            None,
        )
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    ck.check(
        "A6 the pool that was shut down accepts no new task (the sync send is rejected and no callback is appended)",
        after_shutdown.contains("not started") && rec.done() == sends,
        &format!("{after_shutdown} / done={}", rec.done()),
    );

    let p2 = match env.producer("a6-after", None, None).await {
        Ok(p) => p,
        Err(e) => return ck.abort("A6 the new producer", &e),
    };
    let fresh = Arc::new(Recorder::default());
    let again = message(&topic, b"async-a6-new-producer", "TagA6");
    if let Err(e) = p2.send_async(again, fresh.clone(), Some(8_000), None) {
        return ck.abort("A6 the pool that was shut down is not reused by another producer", &e.to_string());
    }
    ck.check(
        "A6 the pool that was shut down is not reused by another producer (the new producer can go on sending)",
        wait_until(|| fresh.done() >= 1, 20).await && fresh.ok() == 1,
        &fresh.summary(),
    );
    p2.shutdown();
}

// ---------------------------------------------------------- cleanup

async fn cleanup(ck: &mut Checker, env: &Env) {
    let topics = lock(&env.topics).clone();
    for topic in topics {
        if let Err(e) = env.admin.delete_topic(&topic, None).await {
            println!("  [WARN] delete_topic({topic}) failed: {e}");
        }
    }
    ck.check("clean up this round's topic", true, "");
}

// ------------------------------------------------------- driver

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let env = Env::new(namesrv, &stamp);
    println!("== live async-send kernel check, namesrv={namesrv} stamp={stamp} ==");
    if !env.start(&mut ck).await {
        return ck;
    }
    a1_non_blocking_and_read_back(&mut ck, &env).await;
    a2_burst_exactly_once_each(&mut ck, &env).await;
    a3_pinned_queue(&mut ck, &env).await;
    a4_forbidden_hook(&mut ck, &env).await;
    a5_batch_async(&mut ck, &env).await;
    a6_shutdown_does_not_wait(&mut ck, &env).await;
    cleanup(&mut ck, &env).await;
    ck
}

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().to_string(),
        Err(_) => "0".to_string(),
    }
}

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
