//! Onewy sends reaching a **real-time** push consumer, verified against a real 5.5.1 cluster.
//!
//! Why this probe exists: every existing oneway assertion (live_producer P2, ACL A3/A12)
//! either reads the message back with a raw pull or starts the consumer **after** the sends
//! (history sweep, CONSUME_FROM_FIRST_OFFSET). Neither covers the plain user scenario:
//! consumer already running -> producer fires oneway -> push delivery in real time.
//!
//! What is asserted here:
//! - O1 three `send_oneway(msg, None)` (fault-strategy queue selection, the path no live
//!   test pins) plus one ordinary sync control message, all delivered to the running
//!   consumer within the window;
//! - O2 one `send_oneway(msg, Some(mq))` (pinned queue) is delivered too;
//! - the sync control arriving proves the consumer itself is healthy, so a missing oneway
//!   body cannot be blamed on the consumer.
//!
//! Usage (start the local cluster per the recorded runbook first):
//! ```text
//! cargo run --example live_oneway_push -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently, SendStatus,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

/// Queue count of the topic created by this example.
const QUEUE_NUMS: i32 = 4;
/// Oneway messages sent on the O1 leg.
const ONEWAY_N: usize = 3;
/// Fallback seconds to wait for real-time deliveries.
const WAIT_SECONDS: u64 = 30;

// ------------------------------------------------------------------ Skeleton

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

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// Polls until the condition holds or the timeout hits (real-cluster delivery is asynchronous; never bet on a fixed sleep).
async fn poll_until(mut pred: impl FnMut() -> bool, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if pred() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ---------------------------------------------------------- Delivery collection

#[derive(Default)]
struct Inbox {
    items: Mutex<Vec<String>>,
}

impl Inbox {
    fn bodies(&self) -> Vec<String> {
        let mut v: Vec<String> = lock(&self.items).clone();
        v.sort();
        v
    }

    fn count(&self) -> usize {
        lock(&self.items).len()
    }

    fn has(&self, body: &str) -> bool {
        lock(&self.items).iter().any(|b| b == body)
    }
}

struct CollectingListener {
    inbox: Arc<Inbox>,
}

impl MessageListenerConcurrently for CollectingListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        {
            let mut items = lock(&self.inbox.items);
            for msg in msgs {
                items.push(String::from_utf8_lossy(msg.get_body()).into_owned());
            }
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

// ------------------------------------------------------------------ Environment

struct Env {
    namesrv: String,
    stamp: String,
    topic: String,
    group: String,
    admin: DefaultMQAdminExt,
    producer: DefaultMQProducer,
}

impl Env {
    fn new(namesrv: &str, stamp: &str) -> Result<Env, String> {
        let producer = DefaultMQProducer::new(&format!("rust-live-oneway-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        let admin = DefaultMQAdminExt::with_config(AdminConfig {
            instance_name: format!("ONEWAY-ADMIN-{stamp}"),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        Ok(Env {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            topic: format!("RustLiveOneway{stamp}"),
            group: format!("rust-live-oneway-{stamp}"),
            admin,
            producer,
        })
    }

    async fn start(&mut self) -> Result<(), String> {
        self.admin
            .start()
            .await
            .map_err(|e| format!("admin start failed: {e}"))?;
        self.producer
            .start()
            .await
            .map_err(|e| format!("producer start failed: {e}"))?;
        self.admin
            .create_topic(MixAll::DEFAULT_TOPIC, &self.topic, QUEUE_NUMS, 0)
            .await
            .map_err(|e| format!("create topic failed: {e}"))?;
        Ok(())
    }

    /// All message queues of this topic (from the producer-side route).
    async fn message_queues(&self) -> Vec<MessageQueue> {
        match self.producer.fetch_publish_message_queues(&self.topic).await {
            Ok(queues) => queues,
            Err(e) => {
                println!("  [WARN] fetch_publish_message_queues failed: {e}");
                Vec::new()
            }
        }
    }

    /// Builds (but does not start) the push consumer; LAST_OFFSET so only messages sent
    /// after startup can arrive -- that is exactly what makes this a real-time test.
    fn consumer(&self, inbox: Arc<Inbox>) -> Result<DefaultMQPushConsumer, String> {
        self.consumer_on(&self.topic, "MAIN", inbox, ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET)
    }

    /// Generic variant: caller picks the topic, the group suffix and the initial-offset policy.
    /// `from_where` is the string constant from `ConsumeFromWhere` (`&'static str`).
    fn consumer_on(
        &self,
        topic: &str,
        suffix: &str,
        inbox: Arc<Inbox>,
        from_where: &str,
    ) -> Result<DefaultMQPushConsumer, String> {
        let consumer = DefaultMQPushConsumer::with_config(ConsumerConfig {
            consumer_group: format!("{}_{}", self.group, suffix),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("live-oneway-{suffix}-{}", self.stamp),
            consume_from_where: from_where.to_string(),
            ..Default::default()
        })
        .map_err(|e| format!("consumer build failed: {e}"))?;
        consumer
            .subscribe(topic, "*")
            .map_err(|e| format!("subscribe failed: {e}"))?;
        consumer.set_message_listener_concurrently(Arc::new(CollectingListener {
            inbox: Arc::clone(&inbox),
        }));
        Ok(consumer)
    }
}

// ------------------------------------------------------------------ Scenarios

async fn run(namesrv: &str) -> Checker {
    println!("oneway -> real-time push consumer live check on {namesrv}");
    let st = stamp();
    let mut env = match Env::new(namesrv, &st) {
        Ok(env) => env,
        Err(e) => {
            let mut ck = Checker::new();
            ck.check("env build", false, &e);
            return ck;
        }
    };
    let mut ck = Checker::new();
    if let Err(e) = env.start().await {
        ck.check("env start (admin/producer/createTopic)", false, &e);
        return ck;
    }
    let began = Instant::now();
    let mut route_ready = false;
    while began.elapsed() < Duration::from_secs(20) {
        if env.message_queues().await.len() >= QUEUE_NUMS as usize {
            route_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    ck.check(
        &format!("T0 topic route ready ({QUEUE_NUMS} queues)"),
        route_ready,
        "fetch_publish_message_queues never carried 4 queues",
    );
    if !route_ready {
        return ck;
    }

    // ---- consumer first, then sends: the real-time shape ----
    let inbox = Arc::new(Inbox::default());
    let consumer = match env.consumer(Arc::clone(&inbox)) {
        Ok(c) => c,
        Err(e) => {
            ck.check("consumer build", false, &e);
            return ck;
        }
    };
    if let Err(e) = consumer.start().await {
        ck.check("consumer start", false, &e.to_string());
        return ck;
    }
    // Wait for the queue assignment: without it the consumer holds no queue and nothing
    // can ever arrive (that would masquerade as a oneway defect).
    let assigned = poll_until(|| consumer.assigned_queue_count() >= 1, 20).await;
    ck.check("consumer got at least one assigned queue", assigned, "assigned=0 after 20s");

    // ---- O1: oneway via fault-strategy selection (mq=None) + a sync control ----
    println!("\n---------- O1 oneway send (auto queue selection) + sync control ----------");
    let mut oneway_bodies = Vec::new();
    for i in 0..ONEWAY_N {
        let body = format!("oneway-auto-{i}");
        let mut msg = Message::new(&env.topic, Some(body.as_bytes()));
        if let Err(e) = env.producer.send_oneway(&mut msg, None).await {
            ck.check(&format!("O1 send_oneway({body}) returned Ok"), false, &e.to_string());
        } else {
            oneway_bodies.push(body);
        }
    }
    let control_body = "sync-control".to_string();
    let mut control = Message::new(&env.topic, Some(control_body.as_bytes()));
    match env.producer.send(&mut control, Some(5000), None).await {
        Ok(r) if r.status == SendStatus::SendOk => {}
        Ok(r) => ck.check("O1 sync control SEND_OK", false, &format!("status={:?}", r.status)),
        Err(e) => ck.check("O1 sync control SEND_OK", false, &e.to_string()),
    }

    // Everything must arrive within the same window; the control proves the consumer is live.
    let want = ONEWAY_N + 1;
    let arrived = poll_until(|| inbox.count() >= want, WAIT_SECONDS).await;
    ck.check(
        &format!("O1 all {want} messages reached the running consumer (3 oneway + 1 sync control)"),
        arrived,
        &format!("got {} of {want}: {:?}", inbox.count(), inbox.bodies()),
    );
    for body in &oneway_bodies {
        ck.check(
            &format!("O1 oneway body '{body}' delivered in real time"),
            inbox.has(body),
            &format!("inbox={:?}", inbox.bodies()),
        );
    }
    ck.check(
        "O1 sync control delivered (consumer itself is healthy)",
        inbox.has(&control_body),
        &format!("inbox={:?}", inbox.bodies()),
    );

    // ---- O2: oneway with a pinned queue ----
    println!("\n---------- O2 oneway send (pinned queue) ----------");
    let queues = env.message_queues().await;
    let pinned = queues.first().cloned();
    match pinned {
        Some(mq) => {
            let body = "oneway-pinned".to_string();
            let mut msg = Message::new(&env.topic, Some(body.as_bytes()));
            if let Err(e) = env.producer.send_oneway(&mut msg, Some(&mq)).await {
                ck.check("O2 send_oneway(pinned) returned Ok", false, &e.to_string());
            } else {
                let got = poll_until(|| inbox.has(&body), WAIT_SECONDS).await;
                ck.check(
                    "O2 pinned oneway body delivered in real time",
                    got,
                    &format!("inbox={:?}", inbox.bodies()),
                );
            }
        }
        None => ck.check("O2 pinned queue available", false, "route carried no queue"),
    }

    // ---- O3: send-first + CONSUME_FROM_FIRST_OFFSET (the send-then-subscribe shape) ----
    // The SQL92 regression taught us that consumer-first probes can mask initial-offset
    // bugs (the queues are empty so max offset == 0). This leg pins the other ordering:
    // all messages exist BEFORE the consumer starts, which starts at 0 for FIRST_OFFSET
    // regardless of the subscription type (no stored offset -> 0).
    println!("\n---------- O3 send-first + CONSUME_FROM_FIRST_OFFSET ----------");
    let first_topic = format!("RustLiveOnewayFirst{}", env.stamp);
    if let Err(e) = env
        .admin
        .create_topic(MixAll::DEFAULT_TOPIC, &first_topic, QUEUE_NUMS, 0)
        .await
    {
        ck.check("O3 create topic", false, &e.to_string());
        return ck;
    }
    // poll the route (async fetch inside the loop body)
    let began = Instant::now();
    let mut first_route_ready = false;
    while began.elapsed() < Duration::from_secs(20) {
        match env.producer.fetch_publish_message_queues(&first_topic).await {
            Ok(q) if q.len() >= QUEUE_NUMS as usize => {
                first_route_ready = true;
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
    ck.check(
        &format!("O3 first-topic route ready ({QUEUE_NUMS} queues)"),
        first_route_ready,
        "fetch_publish_message_queues never carried the queues",
    );
    if !first_route_ready {
        return ck;
    }
    // Send FIRST: 3 oneway + 1 sync control, no consumer running yet.
    let mut first_oneway = Vec::new();
    for i in 0..ONEWAY_N {
        let body = format!("oneway-first-{i}");
        let mut msg = Message::new(&first_topic, Some(body.as_bytes()));
        match env.producer.send_oneway(&mut msg, None).await {
            Ok(()) => first_oneway.push(body),
            Err(e) => ck.check(&format!("O3 send_oneway({body}) returned Ok"), false, &e.to_string()),
        }
    }
    let first_control = "sync-control-first".to_string();
    let mut control_msg = Message::new(&first_topic, Some(first_control.as_bytes()));
    match env.producer.send(&mut control_msg, Some(5000), None).await {
        Ok(r) if r.status == SendStatus::SendOk => {}
        Ok(r) => ck.check("O3 sync control SEND_OK", false, &format!("status={:?}", r.status)),
        Err(e) => ck.check("O3 sync control SEND_OK", false, &e.to_string()),
    }
    ck.check(
        &format!("O3 {ONEWAY_N} oneway + 1 sync control sent before the consumer starts"),
        first_oneway.len() == ONEWAY_N,
        &format!("oneway sent={}", first_oneway.len()),
    );
    tokio::time::sleep(Duration::from_secs(1)).await; // oneway is fire-and-forget: give the broker a beat
    let first_inbox = Arc::new(Inbox::default());
    let first_consumer = match env.consumer_on(
        &first_topic,
        "FIRST",
        Arc::clone(&first_inbox),
        ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET,
    ) {
        Ok(c) => c,
        Err(e) => {
            ck.check("O3 consumer build", false, &e);
            return ck;
        }
    };
    if let Err(e) = first_consumer.start().await {
        ck.check("O3 consumer start", false, &e.to_string());
        return ck;
    }
    let want_first = ONEWAY_N + 1;
    let arrived_first = poll_until(|| first_inbox.count() >= want_first, WAIT_SECONDS).await;
    ck.check(
        &format!("O3 FIRST_OFFSET receives all {want_first} messages sent BEFORE start (3 oneway + 1 sync control)"),
        arrived_first && first_inbox.has(&first_control) && first_oneway.iter().all(|b| first_inbox.has(b)),
        &format!("got {} of {want_first}: {:?}", first_inbox.count(), first_inbox.bodies()),
    );
    first_consumer.shutdown();
    let first_retry = format!("{}{}_FIRST", MixAll::RETRY_GROUP_TOPIC_PREFIX, env.group);
    let _ = env.admin.delete_topic(&first_topic, None).await;
    let _ = env.admin.delete_topic(&first_retry, None).await;

    consumer.shutdown();
    let retry_topic = format!("{}{}", MixAll::RETRY_GROUP_TOPIC_PREFIX, env.group);
    let _ = env.admin.delete_topic(&env.topic, None).await;
    let _ = env.admin.delete_topic(&retry_topic, None).await;
    env.producer.shutdown();
    env.admin.shutdown();
    ck
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
    println!(
        "\n== summary: {} passed, {} failed ==",
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
