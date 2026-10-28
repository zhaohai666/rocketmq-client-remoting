//! Integration verification of the [`DefaultLitePullConsumer`] topic queue-set change listener against a **real 5.5.1 broker**.
//!
//! Usage (start the local cluster per the runbook first):
//! ```text
//! cargo run --example live_lite_topic_queue_change -- 127.0.0.1:9876
//! ```
//!
//! Why a real cluster is required: every comparison round queries the name server afresh for the subscribed queue set, while the ordinary
//! route cache only refreshes every 30s. A fake nameserver can prove the comparison logic; only a real cluster can prove the "fresh lookup".
//! Here the check interval is pushed to its 1s floor, the route polling keeps the default 30s, and the topic is really scaled up:
//! in the stable period after the first-lookup delay, from the name server reporting the new queue count to the listener receiving the callback
//! only one or two check rounds should pass (≤5s); with a 30s cache read this window would stretch to half a minute or more.
//!
//! - L1 queues untouched ⇒ the listener is not disturbed
//! - L1b the first-lookup round really ran ⇒ still silent (the snapshot registered while running does not count as a change)
//! - L2 scale up 2→4: once the name server acknowledges, the callback follows within a few check rounds
//! - L3 after the callback the snapshot advances ⇒ the same queue set is not reported twice
//! - L4 scale down 4→2: also seen via the fresh lookup
//! - L5 never-created topic: failing to fetch queues counts as "not found", not disguised as scaled down to 0 queues

use std::collections::BTreeSet;
use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultLitePullConsumer, LitePullConsumerConfig, TopicMessageQueueChangeListener,
};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::message::MessageQueue;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;

const BASE_QUEUES: i32 = 2;
const SCALED_QUEUES: i32 = 4;
/// Check interval pushed to the floor, to separate "fresh lookup every round" from "reading the 30s cache" in time.
const CHECK_INTERVAL_MILLIS: i64 = 1000;
/// First-lookup delay of the background comparison (the default in the implementation) + some margin.
const FIRST_DELAY: Duration = Duration::from_millis(12_000);
/// Maximum allowed callback gap after the name server sees the change.
const FRESH_WINDOW: Duration = Duration::from_millis(5_000);

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
            println!("  [PASS] {name}");
        } else {
            println!("  [FAIL] {name}: {detail}");
            self.failed.push(format!("{name}: {detail}"));
        }
    }
}

/// Records callbacks only, changes no state; the background task also calls it, so it carries its own lock.
struct Recorder {
    events: Mutex<Vec<(String, Vec<i32>)>>,
}

impl Recorder {
    fn new() -> Arc<Recorder> {
        Arc::new(Recorder { events: Mutex::new(Vec::new()) })
    }

    fn snapshot(&self) -> Vec<(String, Vec<i32>)> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn count(&self) -> usize {
        self.snapshot().len()
    }
}

impl TopicMessageQueueChangeListener for Recorder {
    fn on_changed(&self, topic: &str, message_queues: &[MessageQueue]) {
        let ids: BTreeSet<i32> = message_queues.iter().map(|mq| mq.queue_id).collect();
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((topic.to_string(), ids.into_iter().collect()));
    }
}

fn stamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

fn seq(n: i32) -> Vec<i32> {
    (0..n).collect()
}

fn guard<T>(v: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    v.lock().unwrap_or_else(|e| e.into_inner())
}

/// Freshly queries the queue set until it reports `want` queues; returns the wait duration, or None on timeout.
async fn wait_queue_num(
    consumer: &DefaultLitePullConsumer,
    topic: &str,
    want: usize,
    timeout: Duration,
) -> Option<Duration> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(queues) = consumer.fetch_message_queues(topic).await {
            if queues.len() == want {
                return Some(start.elapsed());
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    None
}

/// Waits for the listener to record `want` callbacks; returns the wait duration, or None on timeout.
async fn wait_events(rec: &Recorder, want: usize, timeout: Duration) -> Option<Duration> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if rec.count() >= want {
            return Some(start.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

/// Issues another create-topic request for the same topic, changing the read/write queue counts to `queues`.
async fn scale_topic(admin: &MQClientInstance, broker_addr: &str, topic: &str, queues: i32) {
    let _ = admin
        .create_topic_in_broker(
            broker_addr,
            MixAll::DEFAULT_TOPIC,
            topic,
            queues,
            queues,
            DEFAULT_PERM,
            0,
            TopicFilterType::SINGLE_TAG,
            false,
            None,
            5000,
            2,
        )
        .await;
}

fn broker_addr_of(route: &TopicRouteData) -> Option<String> {
    route.broker_datas.first()?.select_broker_addr()
}

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let s = stamp();
    let topic = format!("RustLiteQcLive{s}");
    let group = format!("rust-lite-qc-{s}");
    let ghost = format!("RustLiteQcGhost{s}");
    println!("namesrv={namesrv} topic={topic} group={group}");

    let admin = MQClientInstance::new(&format!("rust-lite-qc-admin-{s}"), vec![namesrv.to_string()]);
    if let Err(e) = admin.start().await {
        ck.check("admin start", false, &e.to_string());
        return ck;
    }
    let route = match admin.get_topic_route_data(MixAll::DEFAULT_TOPIC).await {
        Some(r) => r,
        None => {
            ck.check("default topic route", false, "namesrv returned no route");
            return ck;
        }
    };
    let broker_addr = match broker_addr_of(&route) {
        Some(a) => a,
        None => {
            ck.check("broker address", false, "no broker address in the route");
            return ck;
        }
    };

    // ---- create topic (2 queues)
    scale_topic(&admin, &broker_addr, &topic, BASE_QUEUES).await;

    let cfg = LitePullConsumerConfig {
        consumer_group: group.clone(),
        name_server_addrs: vec![namesrv.to_string()],
        instance_name: format!("lite-qc-live-{s}"),
        poll_timeout_millis: 1000,
        ..Default::default()
    };
    let consumer = match DefaultLitePullConsumer::with_config(cfg) {
        Ok(c) => c,
        Err(e) => {
            ck.check("build lite consumer", false, &e.to_string());
            return ck;
        }
    };
    consumer.set_topic_metadata_check_interval_millis(CHECK_INTERVAL_MILLIS);
    ck.check(
        "L0 check interval pushed to the 1s floor",
        consumer.topic_metadata_check_interval_millis() == CHECK_INTERVAL_MILLIS,
        &consumer.topic_metadata_check_interval_millis().to_string(),
    );
    if let Err(e) = consumer.subscribe(&topic, "*") {
        ck.check("lite subscribe", false, &e.to_string());
        return ck;
    }
    if let Err(e) = consumer.start().await {
        ck.check("consumer start", false, &e.to_string());
        return ck;
    }
    let loop_start = Instant::now();

    let visible = wait_queue_num(&consumer, &topic, BASE_QUEUES as usize, Duration::from_secs(30)).await;
    ck.check("L0 route visible (2 queues)", visible.is_some(), "timeout");

    // Registered while running ⇒ the snapshot is recorded immediately ⇒ the first round should not call back
    let rec = Recorder::new();
    let listener: Arc<dyn TopicMessageQueueChangeListener> = rec.clone();
    if let Err(e) = consumer
        .register_topic_message_queue_change_listener(&topic, listener)
        .await
    {
        ck.check("register queue change listener", false, &e.to_string());
        consumer.shutdown();
        return ck;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    ck.check(
        "L1 queues untouched ⇒ silent",
        rec.count() == 0,
        &format!("{:?}", guard(&rec.events)),
    );

    // Wait out the first-lookup delay; after that only the stable 1s-per-round period remains
    let elapsed = loop_start.elapsed();
    if elapsed < FIRST_DELAY {
        tokio::time::sleep(FIRST_DELAY - elapsed).await;
    }
    ck.check(
        "L1b the first-lookup round really ran ⇒ still silent",
        rec.count() == 0,
        &format!("{:?}", guard(&rec.events)),
    );

    // ---- L2 scale up 2→4
    scale_topic(&admin, &broker_addr, &topic, SCALED_QUEUES).await;
    let ns = wait_queue_num(&consumer, &topic, SCALED_QUEUES as usize, Duration::from_secs(45)).await;
    ck.check("L2a nameserver reports 4 queues", ns.is_some(), "timeout");
    if ns.is_some() {
        let cb = wait_events(&rec, 1, FRESH_WINDOW).await;
        let got = cb.is_some()
            && rec
                .snapshot()
                .first()
                .map(|(_, ids)| *ids == seq(SCALED_QUEUES))
                .unwrap_or(false);
        ck.check(
            "L2b the comparison round queries the route afresh (the callback follows the nameserver instead of waiting for the 30s cache)",
            got,
            &format!("callback={cb:?}, events={:?}", rec.snapshot()),
        );
    }

    // ---- L3 snapshot advance
    tokio::time::sleep(Duration::from_secs(3)).await;
    ck.check(
        "L3 after the callback the snapshot advances ⇒ no repeated callback",
        rec.count() == 1,
        &format!("events={:?}", rec.snapshot()),
    );

    // ---- L4 scale down 4→2
    scale_topic(&admin, &broker_addr, &topic, BASE_QUEUES).await;
    let ns = wait_queue_num(&consumer, &topic, BASE_QUEUES as usize, Duration::from_secs(45)).await;
    ck.check("L4a nameserver reports back 2 queues", ns.is_some(), "timeout");
    if ns.is_some() {
        let cb = wait_events(&rec, 2, FRESH_WINDOW).await;
        let got = cb.is_some()
            && rec
                .snapshot()
                .get(1)
                .map(|(_, ids)| *ids == seq(BASE_QUEUES))
                .unwrap_or(false);
        ck.check(
            "L4b the scale-down is likewise seen via the fresh route lookup",
            got,
            &format!("callback={cb:?}, events={:?}", rec.snapshot()),
        );
    }

    // ---- L5 unknown topic: an empty queue set counts as "not found"
    let raised = match consumer.fetch_message_queues(&ghost).await {
        Ok(q) => format!("no error, queues={}", q.len()),
        Err(e) => e.to_string(),
    };
    ck.check(
        "L5 fetching queues of an unknown topic raises not-found instead of returning empty",
        raised.contains("Namesrv return empty") || raised.contains("Can not find"),
        &raised,
    );

    consumer.shutdown();
    admin.shutdown();
    ck
}

fn report(ck: &Checker) {
    println!("== summary: {} passed, {} failed ==", ck.passed, ck.failed.len());
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
    let ck = run(&namesrv).await;
    report(&ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
