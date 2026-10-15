//! Live verification of the three send-header fields (`defaultTopic` / `defaultTopicQueueNums` / `brokerName`).
//!
//! Covers the six scenarios H0..H5.
//!
//! Prerequisite: NameServer + Broker are running with `autoCreateTopicEnable=true`.
//!
//!
//! Usage:
//! ```text
//! cargo run --example live_send_header -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::producer::{
    DefaultMQProducer, ProducerConfig, SendCallback,
};
use rocketmq_client_remoting::client::result::{SendResult, SendStatus};
use rocketmq_client_remoting::common::message::{Message, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::sysflag::PermName;
use rocketmq_client_remoting::error::Error;

/// Default budget (seconds) for "waiting for the broker-side result to land". An auto-created topic needs time before its config reads back.
const WAIT_SECONDS: u64 = 20;

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
        Checker {
            passed: 0,
            failed: Vec::new(),
        }
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
}

impl Latch {
    fn oks(&self) -> usize {
        self.oks.load(Ordering::SeqCst)
    }

    fn first(&self) -> Option<SendResult> {
        lock(&self.results).first().cloned()
    }

    fn errors(&self) -> Vec<String> {
        lock(&self.errors).clone()
    }

    async fn wait(&self, secs: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            if self.done() > 0 || Instant::now() >= deadline {
                return self.done() > 0;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn done(&self) -> usize {
        self.oks() + lock(&self.errors).len()
    }
}

impl SendCallback for Latch {
    fn on_success(&self, result: SendResult) {
        if result.status == SendStatus::SendOk {
            self.oks.fetch_add(1, Ordering::SeqCst);
        }
        lock(&self.results).push(result);
    }

    fn on_exception(&self, err: Error) {
        lock(&self.errors).push(err.to_string());
    }
}

/// Environment: one name server address + a set of stamp-isolated topics, cleaned up by itself when the run ends.
struct Env {
    namesrv: String,
    stamp: String,
    admin: DefaultMQAdminExt,
    broker_addr: Mutex<String>,
    /// H5's criterion: the name of this broker in the route is exactly the send header's `n`.
    broker_name: Mutex<String>,
    topics: Mutex<Vec<String>>,
}

impl Env {
    fn new(namesrv: &str, stamp: &str) -> Env {
        let admin = DefaultMQAdminExt::with_config(AdminConfig {
            instance_name: format!("HDRADMIN-{stamp}"),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        Env {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            admin,
            broker_addr: Mutex::new(String::new()),
            broker_name: Mutex::new(String::new()),
            topics: Mutex::new(Vec::new()),
        }
    }

    fn topic(&self, kind: &str) -> String {
        let t = format!("Hdr{kind}_{}", self.stamp);
        lock(&self.topics).push(t.clone());
        t
    }

    fn producer_group(&self) -> String {
        format!("PID_send_header_{}", self.stamp)
    }

    fn broker(&self) -> String {
        lock(&self.broker_addr).clone()
    }

    fn broker_name(&self) -> String {
        lock(&self.broker_name).clone()
    }

    /// Cluster probe: an open port != the broker is registered with the namesrv, so poll;
    /// also look up this broker's name along the way.
    async fn start(&self, ck: &mut Checker) -> bool {
        if let Err(e) = self.admin.start().await {
            ck.abort("admin start", &e.to_string());
            return false;
        }
        let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
        loop {
            if let Ok(info) = self.admin.fetch_broker_cluster_info().await {
                if let Some(addr) = info.get_broker_addrs().first() {
                    let name = info
                        .broker_addr_table
                        .iter()
                        .find(|(_, ids)| ids.iter().any(|(_, a)| a == addr))
                        .map(|(n, _)| n.clone())
                        .unwrap_or_default();
                    *lock(&self.broker_addr) = addr.clone();
                    *lock(&self.broker_name) = name.clone();
                    ck.check(
                        "cluster probe",
                        true,
                        &format!("broker={addr} name={name}"),
                    );
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

    /// A producer with only the c/d knobs configured; `None` keeps the default.
    fn producer(&self, kind: &str, c: Option<&str>, d: Option<i32>) -> Result<DefaultMQProducer, String> {
        DefaultMQProducer::with_config(ProducerConfig {
            producer_group: self.producer_group(),
            instance_name: format!("hdr-{kind}-{}", self.stamp),
            name_server_addrs: vec![self.namesrv.clone()],
            create_topic_key: c.unwrap_or(MixAll::DEFAULT_TOPIC).to_string(),
            default_topic_queue_nums: d.unwrap_or(MixAll::DEFAULT_TOPIC_QUEUE_NUMS),
            send_msg_timeout: 5_000,
            ..Default::default()
        })
        .map_err(|e| e.to_string())
    }

    /// Starts a standalone producer to send one message, then shuts it down (H1..H3 each need a different c/d combination).
    async fn send_once(&self, kind: &str, c: Option<&str>, d: Option<i32>, topic: &str, body: &str) -> Result<SendResult, String> {
        let producer = self.producer(kind, c, d)?;
        producer.start().await.map_err(|e| e.to_string())?;
        let mut msg = Message::new(topic, Some(body.as_bytes()));
        let sent = producer.send(&mut msg, Some(5_000), None).await;
        producer.shutdown();
        sent.map_err(|e| e.to_string())
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

/// Reads back the (read, write) queue counts of the topic on the broker; None when it does not exist yet.
async fn queue_nums(env: &Env, topic: &str) -> Option<(i32, i32)> {
    env.admin
        .examine_topic_config(&env.broker(), topic)
        .await
        .ok()
        .map(|cfg| (cfg.read_queue_nums, cfg.write_queue_nums))
}

/// Waits for the topic to appear on the broker (auto topic creation happens synchronously in the send path; the config takes a moment to land).
async fn wait_topic(env: &Env, topic: &str) -> Option<(i32, i32)> {
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    loop {
        let got = queue_nums(env, topic).await;
        if got.is_some() || Instant::now() >= deadline {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// The persisted message count of the topic across all brokers (a new topic's minOffset is always 0); returns -1 when unreadable.
async fn total_messages(env: &Env, topic: &str) -> i64 {
    match env.admin.examine_topic_stats(topic).await {
        Ok(stats) => stats
            .offset_table
            .iter()
            .map(|(_, o)| o.max_offset - o.min_offset)
            .sum(),
        Err(_) => -1,
    }
}

// ------------------------------------------------------- H0/H1/H2/H3

async fn check_queue_nums(ck: &mut Checker, env: &Env) {
    // H0: TBW102 is the real "template topic" on this broker; H2/H3's criteria both depend on its queue counts.
    let (tbw_read, tbw_write) = match queue_nums(env, MixAll::DEFAULT_TOPIC).await {
        Some(pair) if pair.1 > 0 => pair,
        other => {
            ck.abort(
                "H0 TBW102 template readable",
                &format!("examine_topic_config(TBW102) = {other:?}"),
            );
            return;
        }
    };
    ck.check(
        "H0 TBW102 template readable",
        true,
        &format!("read={tbw_read} write={tbw_write}"),
    );

    // H1: nothing is set, so d uses MixAll::DEFAULT_TOPIC_QUEUE_NUMS(4).
    let t1 = env.topic("Def");
    match env.send_once("h1", None, None, &t1, "h1").await {
        Ok(r) => ck.check(
            "H1 default-config send succeeded",
            r.status == SendStatus::SendOk,
            &format!(
                "msgId={} broker={}",
                r.msg_id.unwrap_or_default(),
                r.message_queue
                    .map(|mq| mq.broker_name)
                    .unwrap_or_default()
            ),
        ),
        Err(e) => ck.abort("H1 default-config send succeeded", &e),
    }
    let (r1, w1) = wait_topic(env, &t1).await.unwrap_or((-1, -1));
    let expected1 = std::cmp::min(MixAll::DEFAULT_TOPIC_QUEUE_NUMS, tbw_write);
    ck.check(
        "H1 topic created with default d=4 has queue count min(4, TBW102)",
        w1 == expected1 && r1 == w1,
        &format!("read={r1} write={w1} (TBW102={tbw_write})"),
    );

    // H2: d=2 must carry the queue count down -- before the fix this hardcoded 4.
    let t2 = env.topic("Nums");
    if let Err(e) = env.send_once("h2", None, Some(2), &t2, "h2").await {
        ck.abort("H2 send", &e);
    }
    let (r2, w2) = wait_topic(env, &t2).await.unwrap_or((-1, -1));
    ck.check(
        "H2 a topic created with defaultTopicQueueNums=2 has only 2 queues",
        w2 == std::cmp::min(2, tbw_write) && r2 == w2,
        &format!("read={r2} write={w2} (the old hardcoded-4 behavior would give {w1})"),
    );

    // H3: c points at its own template topic. The template must carry PERM_INHERIT, otherwise the
    // broker's isInherited check fails and the broker rejects the auto-created topic outright.
    let src = env.topic("Src");
    if let Err(e) = env
        .admin
        .create_topic_in_broker(
            &env.broker(),
            &src,
            3,
            3,
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_INHERIT,
        )
        .await
    {
        ck.abort("H3 template topic creation", &e.to_string());
        return;
    }
    let (src_read, src_write) = wait_topic(env, &src).await.unwrap_or((-1, -1));
    ck.check(
        "H3 template topic created (3 queues, with INHERIT)",
        src_write == 3 && (src_read, src_write) != (tbw_read, tbw_write),
        &format!("src=({src_read},{src_write}) tbw102=({tbw_read},{tbw_write})"),
    );

    let t3 = env.topic("Inherit");
    if let Err(e) = env.send_once("h3", Some(&src), Some(8), &t3, "h3").await {
        ck.abort("H3 send", &e);
    }
    let (r3, w3) = wait_topic(env, &t3).await.unwrap_or((-1, -1));
    ck.check(
        &format!("H3 inherited when createTopicKey=template topic (min(8,3)=3 instead of TBW102's {tbw_write})"),
        w3 == 3 && r3 == 3,
        &format!("read={r3} write={w3}"),
    );
}

// ------------------------------------------------------------- H4

async fn check_send_entries(ck: &mut Checker, env: &Env) {
    let t4 = env.topic("Entries");
    let producer = match env.producer("h4", None, None) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("H4 construction", &e);
            return;
        }
    };
    if let Err(e) = producer.start().await {
        ck.abort("H4 producer start", &e.to_string());
        return;
    }

    let mut landed: Vec<(&str, bool, String)> = Vec::new();

    // (1) sync
    let mut m1 = Message::new(&t4, Some(b"h4-sync"));
    let pinned_mq: Option<MessageQueue> = match producer.send(&mut m1, Some(5_000), None).await {
        Ok(r) => {
            landed.push(("sync", r.status == SendStatus::SendOk, String::new()));
            r.message_queue.clone()
        }
        Err(e) => {
            landed.push(("sync", false, e.to_string()));
            None
        }
    };

    // (2) pinned: an explicit mq, taking a different call chain (the broker name is given by the caller)
    match pinned_mq.as_ref() {
        Some(mq) => {
            let mut m2 = Message::new(&t4, Some(b"h4-pinned"));
            match producer.send(&mut m2, Some(5_000), Some(mq)).await {
                Ok(r) => {
                    let same = r
                        .message_queue
                        .as_ref()
                        .map(|q| q.broker_name == mq.broker_name)
                        .unwrap_or(false);
                    landed.push((
                        "pinned",
                        r.status == SendStatus::SendOk && same,
                        format!("landing={}", mq.broker_name),
                    ));
                }
                Err(e) => landed.push(("pinned", false, e.to_string())),
            }
        }
        None => landed.push(("pinned", false, "the sync send returned no mq".to_string())),
    }

    // (3) oneway: no response; only the total count below can back it up
    let mut m3 = Message::new(&t4, Some(b"h4-oneway"));
    match producer.send_oneway(&mut m3, None).await {
        Ok(()) => landed.push(("oneway", true, String::new())),
        Err(e) => landed.push(("oneway", false, e.to_string())),
    }

    // (4) batch (320)
    let batch: Vec<Message> = (0..3)
        .map(|i| Message::new(&t4, Some(format!("h4-batch-{i}").as_bytes())))
        .collect();
    match producer.send_batch(batch, None, Some(5_000)).await {
        Ok(r) => landed.push(("batch", r.status == SendStatus::SendOk, String::new())),
        Err(e) => landed.push(("batch", false, e.to_string())),
    }

    // (5) async: takes the ASYNC branch and ends up on the same send kernel path
    let latch = Arc::new(Latch::default());
    match producer.send_async(
        Message::new(&t4, Some(b"h4-async")),
        latch.clone(),
        Some(5_000),
        None,
    ) {
        Ok(()) => {
            let done = latch.wait(10).await;
            let ok = done
                && latch.oks() == 1
                && latch
                    .first()
                    .map(|r| r.status == SendStatus::SendOk)
                    .unwrap_or(false);
            landed.push(("async", ok, latch.errors().join("; ")));
        }
        Err(e) => landed.push(("async", false, e.to_string())),
    }

    producer.shutdown();

    for (entry, ok, detail) in landed {
        ck.check(
            &format!("H4 {entry} entry send succeeded"),
            ok,
            if ok { "" } else { &detail },
        );
    }
    // 1 sync + 1 pinned + 1 oneway + 3 batch + 1 async = 7 messages
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    let mut total = total_messages(env, &t4).await;
    while total < 7 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(300)).await;
        total = total_messages(env, &t4).await;
    }
    ck.check(
        "H4 all seven messages persisted one by one (the batch counts as sub-messages)",
        total == 7,
        &format!("total={total}"),
    );
}

// ------------------------------------------------------------- H5

async fn check_broker_name(ck: &mut Checker, env: &Env) {
    let t5 = env.topic("BrokerName");
    match env.send_once("h5", None, None, &t5, "h5").await {
        Ok(r) => {
            let landed = r
                .message_queue
                .map(|mq| mq.broker_name)
                .unwrap_or_default();
            ck.check(
                "H5 the landing broker name matches the route (n is exactly it)",
                r.status == SendStatus::SendOk && landed == env.broker_name(),
                &format!("landing={landed} route={}", env.broker_name()),
            );
        }
        Err(e) => ck.abort("H5 send", &e),
    }
}

// ------------------------------------------------------------------ driver

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let env = Env::new(namesrv, &stamp);
    println!("== live send-header (c/d/n) check, namesrv={namesrv} stamp={stamp} ==");
    if !env.start(&mut ck).await {
        return ck;
    }
    println!("\n-- H0~H3: c/d decides the queue count of the auto-created topic --");
    check_queue_nums(&mut ck, &env).await;
    println!("\n-- H4: the five send entries, each verified to land --");
    check_send_entries(&mut ck, &env).await;
    println!("\n-- H5: n is exactly the broker the route selects --");
    check_broker_name(&mut ck, &env).await;
    env.cleanup(&mut ck).await;
    env.admin.shutdown();
    ck
}

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs().to_string(),
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
