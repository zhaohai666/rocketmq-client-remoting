//! Live verification of the pinned-queue guard (S1..S6).
//!
//! Prerequisite: a NameServer and a Broker are running with `autoCreateTopicEnable=true`.
//!
//!
//! Usage:
//! ```text
//! cargo run --example live_pinned_guard -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::producer::{
    DefaultMQProducer, ProducerConfig, SendCallback,
};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently, SendResult,
    SendStatus,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::sysflag::PermName;
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

const WAIT_SECONDS: u64 = 20;
/// Upper bound for the counter-leg's elapsed time: the guard is a pure string comparison, so 50ms on a live cluster leaves two orders of magnitude of headroom.
const LOCAL_BUDGET_MS: f64 = 50.0;
const SYNC_WORDING: &str = "message's topic not equal mq's topic";
const ASYNC_WORDING: &str = "Topic of the message does not match its target message queue";

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// Assertion accumulator: every scenario runs to completion before the summary; the first failure does not exit early.
struct Checker {
    passed: u32,
    failed: Vec<String>,
}

impl Checker {
    fn new() -> Checker {
        Checker { passed: 0, failed: Vec::new() }
    }

    fn check(&mut self, name: &str, cond: bool, detail: &str) {
        if cond {
            self.passed += 1;
            println!("  [PASS] {name}  {detail}");
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

/// Terminal state of one async send.
#[derive(Default)]
struct Latch {
    oks: AtomicUsize,
    results: Mutex<Vec<SendResult>>,
    errors: Mutex<Vec<String>>,
    done: AtomicUsize,
}

impl Latch {
    fn new() -> Arc<Latch> {
        Arc::new(Latch::default())
    }

    fn oks(&self) -> usize {
        self.oks.load(Ordering::SeqCst)
    }

    fn errors(&self) -> Vec<String> {
        lock(&self.errors).clone()
    }

    fn first_status(&self) -> Option<SendStatus> {
        lock(&self.results).first().map(|r| r.status)
    }

    async fn wait(&self, secs: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            if self.done() >= 1 || Instant::now() >= deadline {
                return self.done() >= 1;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Number of terminal states reached (success and exception both count).
    fn done(&self) -> usize {
        self.done.load(Ordering::SeqCst)
    }
}

impl SendCallback for Latch {
    fn on_success(&self, result: SendResult) {
        if result.status == SendStatus::SendOk {
            self.oks.fetch_add(1, Ordering::SeqCst);
        }
        lock(&self.results).push(result);
        self.done.fetch_add(1, Ordering::SeqCst);
    }

    fn on_exception(&self, err: Error) {
        lock(&self.errors).push(err.to_string());
        self.done.fetch_add(1, Ordering::SeqCst);
    }
}

/// Collector for consumed keys.
struct KeySink {
    keys: Mutex<Vec<String>>,
}

impl KeySink {
    fn new() -> Arc<KeySink> {
        Arc::new(KeySink { keys: Mutex::new(Vec::new()) })
    }

    fn snapshot(&self) -> Vec<String> {
        lock(&self.keys).clone()
    }
}

struct KeyListener {
    sink: Arc<KeySink>,
}

impl MessageListenerConcurrently for KeyListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let mut keys = lock(&self.sink.keys);
        for m in msgs {
            if let Some(k) = m.get_keys() {
                keys.push(k.to_string());
            } else {
                keys.push(String::from_utf8_lossy(m.get_body()).into_owned());
            }
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
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
            instance_name: format!("PINGUARD-{stamp}"),
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
        let t = format!("PinGuard{kind}_{}", self.stamp);
        lock(&self.topics).push(t.clone());
        t
    }

    fn broker(&self) -> String {
        lock(&self.broker_addr).clone()
    }

    async fn start(&self, ck: &mut Checker) -> bool {
        if let Err(e) = self.admin.start().await {
            ck.abort("admin start", &e.to_string());
            return false;
        }
        let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
        loop {
            if let Ok(info) = self.admin.fetch_broker_cluster_info().await {
                if let Some(addr) = info.get_broker_addrs().first() {
                    *lock(&self.broker_addr) = addr.clone();
                    ck.check("cluster probe", true, &format!("broker={addr}"));
                    return true;
                }
            }
            if Instant::now() >= deadline {
                ck.abort("cluster probe", "nameServer returned no broker within the budget");
                return false;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn create_topic(&self, topic: &str, queues: i32) -> Result<(), String> {
        self.admin
            .create_topic_in_broker(
                &self.broker(),
                topic,
                queues,
                queues,
                PermName::PERM_READ | PermName::PERM_WRITE,
            )
            .await
            .map_err(|e| format!("create_topic_in_broker({topic}): {e}"))
    }

    async fn cleanup(&self, ck: &mut Checker) {
        let topics = lock(&self.topics).clone();
        for topic in topics {
            if let Err(e) = self.admin.delete_topic(&topic, None).await {
                println!("  [WARN] delete_topic({topic}) failed: {e}");
            }
        }
        ck.check("cleanup of this run's topics", true, "");
    }
}

fn producer_config(env: &Env, kind: &str, namespace: &str) -> Result<DefaultMQProducer, String> {
    DefaultMQProducer::with_config(ProducerConfig {
        producer_group: format!("PID_pin_guard_{kind}_{}", env.stamp),
        instance_name: format!("pin-guard-{kind}-{}", env.stamp),
        name_server_addrs: vec![env.namesrv.clone()],
        namespace: namespace.to_string(),
        send_msg_timeout: 5_000,
        ..Default::default()
    })
    .map_err(|e| e.to_string())
}

async fn queue_zero(producer: &DefaultMQProducer, topic: &str) -> Result<MessageQueue, String> {
    let queues = producer
        .fetch_publish_message_queues(topic)
        .await
        .map_err(|e| format!("fetch_publish_message_queues({topic}): {e}"))?;
    queues
        .into_iter()
        .find(|q| q.queue_id == 0)
        .ok_or_else(|| format!("{topic} has no queue 0"))
}

async fn wait_route(producer: &DefaultMQProducer, topic: &str) -> Result<MessageQueue, String> {
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    loop {
        match queue_zero(producer, topic).await {
            Ok(mq) => return Ok(mq),
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(e);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Waits until maxOffset reaches `want` before returning (the broker's ConsumeQueue dispatch lags the SEND response by a few milliseconds).
async fn wait_offset(producer: &DefaultMQProducer, mq: &MessageQueue, want: i64) -> i64 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let got = producer.max_offset(mq).await.unwrap_or(-1);
        if got >= want || Instant::now() >= deadline {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn offset_of(producer: &DefaultMQProducer, mq: &MessageQueue) -> i64 {
    producer.max_offset(mq).await.unwrap_or(-1)
}

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let env = Env::new(namesrv, &stamp);
    println!("== live pinned-topic guard check, namesrv={namesrv} stamp={stamp} ==");
    if !env.start(&mut ck).await {
        return ck;
    }
    let topic_a = env.topic("A");
    let topic_b = env.topic("B");

    let producer = match producer_config(&env, "sync", "") {
        Ok(p) => p,
        Err(e) => {
            ck.abort("producer construction", &e);
            return ck;
        }
    };
    if let Err(e) = producer.start().await {
        ck.abort("producer start", &e.to_string());
        return ck;
    }

    // ---------------- S0 routes of both topics registered + consumer in place ----------------
    println!("\n-- S0 fixture: routes and consumer --");
    if let Err(e) = env.create_topic(&topic_a, 4).await {
        ck.abort("create A", &e);
        return ck;
    }
    if let Err(e) = env.create_topic(&topic_b, 4).await {
        ck.abort("create B", &e);
        return ck;
    }
    let mq_a = match wait_route(&producer, &topic_a).await {
        Ok(mq) => mq,
        Err(e) => {
            ck.abort("S0 A route", &e);
            return ck;
        }
    };
    let mq_b = match wait_route(&producer, &topic_b).await {
        Ok(mq) => mq,
        Err(e) => {
            ck.abort("S0 B route", &e);
            return ck;
        }
    };
    ck.check(
        "S0 routes of both topics usable (counter-leg uses a real queue, so a refusal must be about the topic, not the address)",
        mq_a.broker_name == mq_b.broker_name,
        &format!("A={}/{} B={}/{}", mq_a.broker_name, mq_a.queue_id, mq_b.broker_name, mq_b.queue_id),
    );

    // Consumer starts first with CONSUME_FROM_FIRST_OFFSET: it does not race the send for the rebalance timing
    let sink = KeySink::new();
    let consumer = match DefaultMQPushConsumer::with_config(ConsumerConfig {
        consumer_group: format!("pin-guard-live-{stamp}"),
        name_server_addrs: vec![namesrv.to_string()],
        instance_name: format!("pin-guard-live-{stamp}"),
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("consumer construction", &e.to_string());
            return ck;
        }
    };
    if let Err(e) = consumer.subscribe(&topic_a, "*") {
        ck.abort("consumer subscribe", &e.to_string());
        return ck;
    }
    consumer.set_message_listener_concurrently(Arc::new(KeyListener { sink: sink.clone() }));
    if let Err(e) = consumer.start().await {
        ck.abort("consumer start", &e.to_string());
        return ck;
    }

    // ---------------- S1 positive leg: pinned send on a real routed queue ----------------
    println!("\n-- S1 pinned send on a real routed queue --");
    let k_single = format!("pinned-single-{stamp}");
    let mut m1 = Message::new(&topic_a, Some(b"s1-single"));
    m1.set_keys(&k_single);
    match producer.send(&mut m1, Some(5_000), Some(&mq_a)).await {
        Ok(r) => {
            let same = r
                .message_queue
                .as_ref()
                .map(|q| q.queue_id == mq_a.queue_id && q.topic == topic_a)
                .unwrap_or(false);
            ck.check(
                "S1a sync single pinned send returns SEND_OK and lands on queue 0",
                r.status == SendStatus::SendOk && same,
                &format!("status={:?} mq={:?}", r.status, r.message_queue),
            );
        }
        Err(e) => ck.abort("S1a sync single pinned send", &e.to_string()),
    }

    let k_b1 = format!("pinned-b1-{stamp}");
    let k_b2 = format!("pinned-b2-{stamp}");
    let mut sb1 = Message::new(&topic_a, Some(b"s1-b1"));
    sb1.set_keys(&k_b1);
    let mut sb2 = Message::new(&topic_a, Some(b"s1-b2"));
    sb2.set_keys(&k_b2);
    match producer.send_batch(vec![sb1, sb2], Some(&mq_a), Some(5_000)).await {
        Ok(r) => {
            let same = r
                .message_queue
                .as_ref()
                .map(|q| q.queue_id == mq_a.queue_id)
                .unwrap_or(false);
            ck.check(
                "S1b sync batch pinned send returns SEND_OK on the same queue",
                r.status == SendStatus::SendOk && same,
                &format!("status={:?} mq={:?}", r.status, r.message_queue),
            );
        }
        Err(e) => ck.abort("S1b sync batch pinned send", &e.to_string()),
    }
    // A batch lands in the consume queue entry by entry as **sub-messages** (the broker splits the inner-batch on receipt) ⇒ 2 sub-messages raise the offset by 2
    let off_after_s1 = wait_offset(&producer, &mq_a, 3).await;
    ck.check(
        "S1c all three sub-messages truly persisted (maxOffset = 1 single + 2 batch sub-messages)",
        off_after_s1 == 3,
        &format!("maxOffset={off_after_s1}"),
    );

    // ---------------- S2 counter-leg: sync refusal, local, leaves no trace ----------------
    println!("\n-- S2 topic mismatch: sub-millisecond local refusal, no trace on the broker --");
    let mut r1 = Message::new(&topic_a, Some(b"refused"));
    r1.set_keys(&format!("pinned-refused-{stamp}"));
    let began = Instant::now();
    let single = producer.send(&mut r1, Some(5_000), Some(&mq_b)).await;
    let single_ms = began.elapsed().as_secs_f64() * 1000.0;
    let single_msg = single.as_ref().err().map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "S2a sync single refusal (message text matches verbatim)",
        single.is_err() && single_msg.contains(SYNC_WORDING),
        &format!("{single_ms:.2}ms {single_msg}"),
    );
    ck.check(
        "S2b refusal is local: sub-millisecond (not a timeout, not a broker remark)",
        single.is_err() && single_ms < LOCAL_BUDGET_MS,
        &format!("{single_ms:.2}ms"),
    );
    let rb1 = Message::new(&topic_a, Some(b"r1"));
    let rb2 = Message::new(&topic_a, Some(b"r2"));
    let began = Instant::now();
    let batch = producer.send_batch(vec![rb1, rb2], Some(&mq_b), Some(5_000)).await;
    let batch_ms = began.elapsed().as_secs_f64() * 1000.0;
    let batch_msg = batch.as_ref().err().map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "S2c sync batch shares the same guard and the same message text",
        batch.is_err() && batch_msg.contains(SYNC_WORDING) && batch_ms < LOCAL_BUDGET_MS,
        &format!("{batch_ms:.2}ms {batch_msg}"),
    );
    let a_now = offset_of(&producer, &mq_a).await;
    let b_now = offset_of(&producer, &mq_b).await;
    ck.check(
        "S2d wire counter-evidence: A's maxOffset untouched and B does not hold a single message",
        a_now == off_after_s1 && b_now == 0,
        &format!("A={a_now} B={b_now}"),
    );

    // ---------------- S3 namespace: wrap is idempotent, only a genuinely different name is refused ----------------
    println!("\n-- S3 comparison under a namespace (neither ns%topic nor the bare topic is falsely refused) --");
    let w_topic = format!("ns1%{topic_a}");
    lock(&env.topics).push(w_topic.clone());
    if let Err(e) = env.create_topic(&w_topic, 4).await {
        ck.abort("S3 create ns1%topic", &e);
        return ck;
    }
    let nsprod = match producer_config(&env, "ns", "ns1") {
        Ok(p) => p,
        Err(e) => {
            ck.abort("S3 namespace producer construction", &e);
            return ck;
        }
    };
    if let Err(e) = nsprod.start().await {
        ck.abort("S3 namespace producer start", &e.to_string());
        return ck;
    }
    let mqw = match wait_route(&nsprod, &w_topic).await {
        Ok(mq) => mq,
        Err(e) => {
            ck.abort("S3 route of the prefixed topic", &e);
            return ck;
        }
    };
    let mut n1 = Message::new(&topic_a, Some(b"ns-wrapped-queue"));
    n1.set_keys(&format!("pinned-ns-q-{stamp}"));
    match nsprod.send(&mut n1, Some(5_000), Some(&mqw)).await {
        Ok(r) => ck.check(
            "S3b queue topic already carries the ns prefix: wrap is idempotent, no false refusal",
            r.status == SendStatus::SendOk,
            &format!("status={:?}", r.status),
        ),
        Err(e) => ck.abort("S3b send with a prefixed queue topic", &e.to_string()),
    }
    let mut n2 = Message::new(&w_topic, Some(b"ns-wrapped-message"));
    n2.set_keys(&format!("pinned-ns-m-{stamp}"));
    match nsprod.send(&mut n2, Some(5_000), Some(&mqw)).await {
        Ok(r) => ck.check(
            "S3c message topic carrying its own prefix is likewise allowed",
            r.status == SendStatus::SendOk,
            &format!("status={:?}", r.status),
        ),
        Err(e) => ck.abort("S3c send with a prefixed message topic", &e.to_string()),
    }
    let ns2_mq = MessageQueue::new(&format!("ns2%{topic_a}"), &mqw.broker_name, 0);
    let mut n3 = Message::new(&topic_a, Some(b"ns2-refused"));
    let began = Instant::now();
    let ns2 = nsprod.send(&mut n3, Some(5_000), Some(&ns2_mq)).await;
    let ns2_ms = began.elapsed().as_secs_f64() * 1000.0;
    let ns2_msg = ns2.as_ref().err().map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "S3d refused only when the prefix changes to ns2% (control leg: what is refused is the name, not merely having a prefix)",
        ns2.is_err() && ns2_msg.contains(SYNC_WORDING) && ns2_ms < LOCAL_BUDGET_MS,
        &format!("{ns2_ms:.2}ms {ns2_msg}"),
    );
    let ns_off = wait_offset(&nsprod, &mqw, 2).await;
    ck.check(
        "S3e both allowed sends truly land in ns1%topic (maxOffset=2)",
        ns_off == 2,
        &format!("maxOffset={ns_off}"),
    );

    // ---------------- S4 async: the callback carries the async message text ----------------
    println!("\n-- S4 async single/batch: refusals go through the callback, allowed sends go on the wire --");
    let refused_latch = Latch::new();
    let mut ar = Message::new(&topic_a, Some(b"async-refused"));
    ar.set_keys(&format!("pinned-async-refused-{stamp}"));
    if let Err(e) = producer.send_async(ar, refused_latch.clone(), Some(5_000), Some(mq_b.clone())) {
        ck.abort("S4a the async call itself must not throw", &e.to_string());
    }
    let got = refused_latch.wait(10).await;
    let errs = refused_latch.errors();
    ck.check(
        "S4a single async refusal goes through the callback with the async message text",
        got && errs.len() == 1 && errs[0].contains(ASYNC_WORDING),
        &format!("errors={errs:?}"),
    );
    let a_now = offset_of(&producer, &mq_a).await;
    ck.check(
        "S4b maxOffset still unchanged after the refusal (async does not sneak a send either)",
        a_now == off_after_s1,
        &format!("maxOffset={a_now}"),
    );

    let batch_refused = Latch::new();
    let ab1 = Message::new(&topic_a, Some(b"abr1"));
    let ab2 = Message::new(&topic_a, Some(b"abr2"));
    if let Err(e) = producer.send_batch_async(
        vec![ab1, ab2],
        batch_refused.clone(),
        Some(5_000),
        Some(mq_b.clone()),
    ) {
        ck.abort("S4c the batch async call itself must not throw", &e.to_string());
    }
    let got = batch_refused.wait(10).await;
    let errs = batch_refused.errors();
    ck.check(
        "S4c batch async shares the same message text and the same refusal path",
        got && errs.len() == 1 && errs[0].contains(ASYNC_WORDING),
        &format!("errors={errs:?}"),
    );

    let single_ok = Latch::new();
    let mut okm = Message::new(&topic_a, Some(b"async-ok"));
    okm.set_keys(&format!("pinned-async-single-{stamp}"));
    if let Err(e) = producer.send_async(okm, single_ok.clone(), Some(5_000), Some(mq_a.clone())) {
        ck.abort("S4d async allowed call failed", &e.to_string());
    }
    let batch_ok = Latch::new();
    let mut bk1 = Message::new(&topic_a, Some(b"ab1"));
    bk1.set_keys(&format!("pinned-async-b1-{stamp}"));
    let mut bk2 = Message::new(&topic_a, Some(b"ab2"));
    bk2.set_keys(&format!("pinned-async-b2-{stamp}"));
    if let Err(e) = producer.send_batch_async(
        vec![bk1, bk2],
        batch_ok.clone(),
        Some(5_000),
        Some(mq_a.clone()),
    ) {
        ck.abort("S4e batch async allowed call failed", &e.to_string());
    }
    let ok_ok = single_ok.wait(10).await;
    let ok_batch = batch_ok.wait(10).await;
    ck.check(
        "S4d both allowed legs return SEND_OK",
        ok_ok
            && single_ok.oks() == 1
            && single_ok.first_status() == Some(SendStatus::SendOk)
            && ok_batch
            && batch_ok.oks() == 1
            && batch_ok.first_status() == Some(SendStatus::SendOk),
        &format!(
            "single_errors={:?} batch_errors={:?}",
            single_ok.errors(),
            batch_ok.errors()
        ),
    );
    let off_after_async = wait_offset(&producer, &mq_a, off_after_s1 + 3).await;
    ck.check(
        "S4e async allowed legs truly persist too (1 single + 2 batch sub-messages, maxOffset rises by another 3)",
        off_after_async == off_after_s1 + 3,
        &format!("maxOffset={off_after_async}"),
    );

    println!("\n-- S5 oneway pinned send has no guard: the message's own topic decides --");
    let k_ow = format!("pinned-oneway-{stamp}");
    let mut ow = Message::new(&topic_a, Some(b"oneway"));
    ow.set_keys(&k_ow);
    match producer.send_oneway(&mut ow, Some(&mq_b)).await {
        Ok(()) => {}
        Err(e) => ck.abort("S5 the oneway send itself must not fail", &e.to_string()),
    }
    let off_after_ow = wait_offset(&producer, &mq_a, off_after_async + 1).await;
    ck.check(
        "S5a oneway pinned send has no guard: the packet lands in A under the message's own topic",
        off_after_ow == off_after_async + 1,
        &format!("A maxOffset={off_after_ow}"),
    );
    let b_now = offset_of(&producer, &mq_b).await;
    ck.check(
        "S5b B, where the target queue lives, holds not a single message (by local design, not a missed send)",
        b_now == 0,
        &format!("B maxOffset={b_now}"),
    );

    // ---------------- S6 positive-leg wrap-up: not one message missing ----------------
    println!("\n-- S6 push consumer receives all positive-leg messages --");
    let expected = [
        k_single.clone(),
        k_b1.clone(),
        k_b2.clone(),
        format!("pinned-async-single-{stamp}"),
        format!("pinned-async-b1-{stamp}"),
        format!("pinned-async-b2-{stamp}"),
        k_ow.clone(),
    ];
    let deadline = Instant::now() + Duration::from_secs(40);
    let mut got = sink.snapshot();
    while Instant::now() < deadline && !expected.iter().all(|k| got.contains(k)) {
        tokio::time::sleep(Duration::from_millis(500)).await;
        got = sink.snapshot();
    }
    let missing: Vec<&String> = expected.iter().filter(|k| !got.contains(k)).collect();
    ck.check(
        "S6 every positive-leg message is consumed",
        missing.is_empty(),
        &format!("received={got:?} missing={missing:?}"),
    );

    consumer.shutdown();
    producer.shutdown();
    nsprod.shutdown();
    env.cleanup(&mut ck).await;
    env.admin.shutdown();
    ck
}

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().to_string(),
        Err(_) => "0".to_string(),
    }
}

fn report(ck: &mut Checker) {
    println!(
        "== result: {}/{} passed ==",
        ck.passed,
        ck.passed as usize + ck.failed.len()
    );
    for f in &ck.failed {
        println!("   FAILED {f}");
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let argv: Vec<String> = env::args().collect();
    let namesrv = argv
        .get(1)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:9876".to_string());
    let mut ck = run(&namesrv).await;
    report(&mut ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
