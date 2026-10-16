//! SQL92 filtering + `CHECK_CLIENT_CONFIG(46)` integration-verified against a **real 5.5.1 cluster**.
//!
//! Assertions item by item on a real cluster.
//!
//! Why this must run on a real cluster: the SQL92 path is the easiest one to fail silently. When the broker's
//! `ExpressionMessageFilter` cannot get the compiled filter data at the ConsumeQueue stage it **admits all
//! messages** (`return true`), so both errors show up as "the consumer starts normally and all messages arrive":
//! the broker has `enablePropertyFilter` off (the expression is never compiled), or the expression has a syntax error.
//! Offline unit tests (`mod tests` in `src/client/mq_client.rs`) can pin the protocol shape but cannot prove the broker
//! really filters by property. So all four sections verify here:
//!
//!
//! Usage (start the local cluster per the recorded runbook first; the broker needs `enablePropertyFilter=true`):
//! ```text
//! cargo run --example live_sql92 -- 127.0.0.1:9876
//! ```

use std::collections::BTreeSet;
use std::env;
use std::process::ExitCode;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::Value as JsonValue;

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{
    ConsumerConfig, DefaultMQPushConsumer, MessageSelector,
};
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently, SendStatus,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::protocol::codes::{request_code, response_code};
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;
use rocketmq_client_remoting::remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_client_remoting::remoting::rpchook::RPCHook;

/// Queue count of the topic created by this example (enough for the broker to spread across, and makes the route-ready check meaningful).
const QUEUE_NUMS: i32 = 4;
/// How many messages to send per color.
const PER_COLOR: usize = 3;
/// Fallback seconds to wait for deliveries to settle (includes rebalance + long polling + the offset period).
const WAIT_SECONDS: u64 = 25;

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

    fn abort(&mut self, name: &str, err: &str) {
        println!("  [FAIL] {name}: {err}");
        self.failed.push(format!("{name}: {err}"));
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

/// Polls until the condition holds or the timeout hits (everything on a real cluster lands asynchronously; never bet on a fixed sleep).
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

/// The response_code of `Error::Client`; other variants return None.
fn client_code(e: &Error) -> Option<i32> {
    match e {
        Error::Client { response_code, .. } => *response_code,
        _ => None,
    }
}

// -------------------------------------------- on-line evidence hook for request code 46

/// Hooks into the transport and captures the request-46 calls actually sent at startup (with body).
#[derive(Default)]
struct CheckConfigProbe {
    codes: Mutex<Vec<i32>>,
    bodies: Mutex<Vec<JsonValue>>,
    total: std::sync::atomic::AtomicUsize,
}

impl CheckConfigProbe {
    fn new() -> Arc<CheckConfigProbe> {
        Arc::new(CheckConfigProbe::default())
    }

    fn codes(&self) -> Vec<i32> {
        lock(&self.codes).clone()
    }

    fn check_count(&self) -> usize {
        lock(&self.codes)
            .iter()
            .filter(|c| **c == request_code::CHECK_CLIENT_CONFIG)
            .count()
    }

    fn first_body(&self) -> JsonValue {
        lock(&self.bodies).first().cloned().unwrap_or(JsonValue::Null)
    }
}

impl RPCHook for CheckConfigProbe {
    fn do_before_request(&self, _remote_addr: &str, request: &mut RemotingCommand) {
        self.total.fetch_add(1, Ordering::SeqCst);
        if request.code == request_code::CHECK_CLIENT_CONFIG {
            lock(&self.codes).push(request.code);
            if let Some(body) = request.body() {
                let parsed = serde_json::from_slice(body).unwrap_or(JsonValue::Null);
                lock(&self.bodies).push(parsed);
            }
        }
    }
}

// ---------------------------------------------------------------- Delivery collection

#[derive(Default)]
struct Inbox {
    /// (body, property color)
    items: Mutex<Vec<(String, Option<String>)>>,
}

impl Inbox {
    fn bodies(&self) -> Vec<String> {
        let mut v: Vec<String> = lock(&self.items).iter().map(|(b, _)| b.clone()).collect();
        v.sort();
        v
    }

    fn colors(&self) -> BTreeSet<String> {
        lock(&self.items)
            .iter()
            .filter_map(|(_, c)| c.clone())
            .collect()
    }

    fn count(&self) -> usize {
        lock(&self.items).len()
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
                items.push((
                    String::from_utf8_lossy(msg.get_body()).into_owned(),
                    msg.get_property("color").map(str::to_string),
                ));
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
        let producer = DefaultMQProducer::new(&format!("rust-live-sql92-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        let admin = DefaultMQAdminExt::with_config(AdminConfig {
            instance_name: format!("SQL92-ADMIN-{stamp}"),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        Ok(Env {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            topic: format!("RustLiveSql92{stamp}"),
            group: format!("rust-live-sql92-{stamp}"),
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

    /// How many queues this topic actually carries in the route (0 if the route cannot be fetched).
    async fn route_queue_count(&self) -> i32 {
        match self.admin.examine_topic_route(&self.topic).await {
            Ok(route) => route
                .queue_datas
                .iter()
                .map(|q| q.read_queue_nums)
                .sum::<i32>()
                .max(0),
            Err(_) => 0,
        }
    }

    /// Starts a push consumer subscribed to this topic (when `selector` is None, `expression` falls back to TAG).
    fn consumer(
        &self,
        suffix: &str,
        selector: Option<MessageSelector>,
        probe: Option<Arc<dyn RPCHook>>,
    ) -> Result<(DefaultMQPushConsumer, Arc<Inbox>), String> {
        let group = format!("{}_{}", self.group, suffix);
        let inbox = Arc::new(Inbox::default());
        let consumer = DefaultMQPushConsumer::with_config(ConsumerConfig {
            consumer_group: group,
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("live-sql92-{suffix}-{}", self.stamp),
            // Messages are sent only after startup, so keeping the default CONSUME_FROM_LAST_OFFSET here is what makes it meaningful:
            // if it were FIRST, S2/S3 would sweep in historical messages too and the assertions would lose discrimination.
            consume_from_where: ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET.to_string(),
            ..Default::default()
        })
        .map_err(|e| format!("consumer build failed: {e}"))?;
        let selector = selector.unwrap_or_else(|| MessageSelector::by_tag("*"));
        consumer
            .subscribe_with_selector(&self.topic, &selector)
            .map_err(|e| format!("subscribe failed: {e}"))?;
        consumer.set_message_listener_concurrently(Arc::new(CollectingListener {
            inbox: Arc::clone(&inbox),
        }));
        consumer.set_rpc_hook(probe);
        Ok((consumer, inbox))
    }

    /// Sends `PER_COLOR` messages carrying the `color=<color>` property.
    async fn produce(&self, color: &str) -> Vec<String> {
        let mut bodies = Vec::new();
        for i in 0..PER_COLOR {
            let text = format!("body-{color}-{i}");
            let mut msg = Message::new(&self.topic, Some(text.as_bytes()));
            msg.set_keys(&format!("sql92-{}-{color}-{i}", self.stamp));
            msg.put_property("color", color);
            match self.producer.send(&mut msg, Some(5000), None).await {
                Ok(r) if r.status == SendStatus::SendOk => bodies.push(text),
                Ok(r) => println!("  [WARN] send {text} status={:?}", r.status),
                Err(e) => println!("  [WARN] send {text} failed: {e}"),
            }
        }
        bodies
    }
}

// ------------------------------------------------------------------ Scenarios

async fn s1_wire_evidence(env: &Env, ck: &mut Checker) {
    println!("\n---------- S1 request code 46 sent at startup ----------");
    let probe = CheckConfigProbe::new();
    let (consumer, _inbox) = match env.consumer(
        "SQL",
        Some(MessageSelector::by_sql("color = 'red'")),
        Some(probe.clone()),
    ) {
        Ok(pair) => pair,
        Err(e) => return ck.abort("S1 build SQL92 consumer", &e),
    };
    match consumer.start().await {
        Ok(()) => {}
        Err(e) => {
            ck.abort("S1 SQL92 consumer start", &e.to_string());
            return;
        }
    }
    ck.check(
        "SQL92 subscription triggers exactly one CHECK_CLIENT_CONFIG(46)",
        probe.check_count() == 1,
        &format!("codes={:?}", probe.codes()),
    );
    let body = probe.first_body();
    let sd = body.get("subscriptionData").cloned().unwrap_or(JsonValue::Null);
    let want_group = format!("{}_SQL", env.group);
    ck.check(
        "body is CheckClientRequestBody (clientId / group / subscriptionData)",
        body.get("group").and_then(JsonValue::as_str) == Some(want_group.as_str())
            && body.get("clientId").and_then(JsonValue::as_str)
                == Some(consumer.client_id().as_str())
            && sd.get("expressionType").and_then(JsonValue::as_str) == Some("SQL92")
            && sd.get("subString").and_then(JsonValue::as_str) == Some("color = 'red'")
            && sd.get("topic").and_then(JsonValue::as_str) == Some(env.topic.as_str()),
        &body.to_string(),
    );
    consumer.shutdown();

    let tag_probe = CheckConfigProbe::new();
    let (tag_consumer, _inbox) = match env.consumer(
        "TAG",
        Some(MessageSelector::by_tag("tagA || tagB")),
        Some(tag_probe.clone()),
    ) {
        Ok(pair) => pair,
        Err(e) => return ck.abort("S1 build TAG consumer", &e),
    };
    if let Err(e) = tag_consumer.start().await {
        ck.abort("S1 TAG consumer start", &e.to_string());
        return;
    }
    ck.check(
        "a pure TAG subscription sends no 46 at all (isTagType short-circuit)",
        tag_probe.check_count() == 0,
        &format!("codes={:?}", tag_probe.codes()),
    );
    tag_consumer.shutdown();
}

async fn s2_s3_real_filtering(env: &Env, ck: &mut Checker) {
    println!("\n---------- S2 broker filters by property / S3 never-matching expression ----------");
    let built = [
        ("FILTER", Some(MessageSelector::by_sql("color = 'red'"))),
        ("NONE", Some(MessageSelector::by_sql("color = 'green'"))),
        ("ALL", None),
    ];
    let mut consumers = Vec::new();
    let mut inboxes = Vec::new();
    for (suffix, selector) in built {
        match env.consumer(suffix, selector, None) {
            Ok((c, inbox)) => {
                consumers.push(c);
                inboxes.push(inbox);
            }
            Err(e) => {
                ck.abort(&format!("S2 build consumer {suffix}"), &e);
                return;
            }
        }
    }
    // Ordering: **start all consumers first, then send messages**.
    for c in &consumers {
        if let Err(e) = c.start().await {
            ck.abort("S2 consumer start", &e.to_string());
            return;
        }
    }
    let red = env.produce("red").await;
    let blue = env.produce("blue").await;
    ck.check(
        "all 6 messages SEND_OK",
        red.len() == PER_COLOR && blue.len() == PER_COLOR,
        &format!("red={} blue={}", red.len(), blue.len()),
    );

    let all_inbox = Arc::clone(&inboxes[2]);
    poll_until(|| all_inbox.count() >= 2 * PER_COLOR, WAIT_SECONDS).await;
    // The green leg is the "should not receive" one and can only wait out the full window; wait a bit longer here to rule out "arriving late".
    tokio::time::sleep(Duration::from_secs(4)).await;

    let red_bodies = inboxes[0].bodies();
    let all_bodies = inboxes[2].bodies();
    let mut want_red = red.clone();
    want_red.sort();
    let mut want_all: Vec<String> = red.iter().chain(blue.iter()).cloned().collect();
    want_all.sort();
    ck.check(
        "SQL92(color=red) receives 3 messages, and exactly the 3 that were sent",
        red_bodies == want_red,
        &format!("{red_bodies:?}"),
    );
    ck.check(
        "the TAG '*' control group receives all 6 (red+blue all present)",
        all_bodies == want_all,
        &format!("{all_bodies:?}"),
    );
    ck.check(
        "none of the 3 blue messages leaks into the SQL92 consumer (proves the broker really filters)",
        blue.iter().all(|b| !red_bodies.contains(b)),
        &format!("{red_bodies:?}"),
    );
    ck.check(
        "the color property of received messages is readable and all red",
        inboxes[0].colors() == BTreeSet::from(["red".to_string()]),
        &format!("{:?}", inboxes[0].colors()),
    );
    ck.check(
        "subscribing to never-matching color='green' ⇒ zero messages received",
        inboxes[1].count() == 0,
        &format!("{:?}", inboxes[1].bodies()),
    );
    for c in consumers {
        c.shutdown();
    }
}

async fn s4_bad_expression_fails_start(env: &Env, ck: &mut Checker) {
    println!("\n---------- S4 illegal expression fails at startup ----------");
    let (consumer, _inbox) = match env.consumer("BAD", Some(MessageSelector::by_sql("color ==")), None)
    {
        Ok(pair) => pair,
        Err(e) => return ck.abort("S4 build consumer", &e),
    };
    let began = Instant::now();
    let err = consumer.start().await.err();
    let cost = began.elapsed().as_secs_f64();
    match &err {
        None => ck.check("start() returns an error", false, "the illegal expression actually started successfully"),
        Some(e) => {
            ck.check("start() returns a client error", client_code(e).is_some(), &e.to_string());
            ck.check(
                "the error code is the broker's SUBSCRIPTION_PARSE_FAILED(23)",
                client_code(e) == Some(response_code::SUBSCRIPTION_PARSE_FAILED),
                &format!("code={:?} err={e}", client_code(e)),
            );
        }
    }
    ck.check("the consumer is not left in the started state after the failure", !consumer.is_started(), "still left at started=true");
    ck.check(
        "the illegal expression fails immediately (<5s, not by waiting out the timeout fallback)",
        cost < 5.0,
        &format!("{cost:.2}s"),
    );
    // Rollback clean? The same object with a corrected expression should be able to start again.
    let retry = async {
        consumer
            .subscribe_with_selector(&env.topic, &MessageSelector::by_sql("color = 'blue'"))
            .map_err(|e| e.to_string())?;
        consumer.start().await.map_err(|e| e.to_string())
    }
    .await;
    ck.check(
        "after the failure a corrected expression can start again (startup was rolled back in place)",
        retry.is_ok(),
        retry.as_ref().err().map(String::as_str).unwrap_or(""),
    );
    if retry.is_ok() {
        consumer.shutdown();
    }
}

async fn s5_send_first_first_offset(env: &Env, ck: &mut Checker) {
    println!("\n---------- S5 send-first + CONSUME_FROM_FIRST_OFFSET ----------");
    // The regression this pins: an earlier draft resolved the initial offset of a SQL92
    // subscription to the max offset, silently skipping every message produced *before*
    // the consumer started (consumer-first passed only because the queues were empty and
    // the max offset was 0 anyway). Java (RebalancePushImpl#computePullFromWhere) starts
    // at 0 for FIRST_OFFSET regardless of the subscription type and lets the broker filter.
    let topic = format!("RustLiveSql92First{}", env.stamp);
    if let Err(e) = env
        .admin
        .create_topic(MixAll::DEFAULT_TOPIC, &topic, QUEUE_NUMS, 0)
        .await
    {
        return ck.abort("S5 create topic", &e.to_string());
    }
    let began = Instant::now();
    let mut ready = false;
    while began.elapsed() < Duration::from_secs(20) {
        if let Ok(route) = env.admin.examine_topic_route(&topic).await {
            if route.queue_datas.iter().map(|q| q.read_queue_nums).sum::<i32>() >= QUEUE_NUMS {
                ready = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    ck.check("S5 topic route ready (4 queues)", ready, "route never carried the queues");
    if !ready {
        return;
    }

    // Send FIRST: 2 red + 1 blue, no consumer running yet.
    let mut red = Vec::new();
    let mut blue = Vec::new();
    for (color, count, sink) in [("red", 2usize, &mut red), ("blue", 1usize, &mut blue)] {
        for i in 0..count {
            let text = format!("first-{color}-{i}");
            let mut msg = Message::new(&topic, Some(text.as_bytes()));
            msg.set_keys(&format!("sql92-first-{}-{color}-{i}", env.stamp));
            msg.put_property("color", color);
            match env.producer.send(&mut msg, Some(5000), None).await {
                Ok(r) if r.status == SendStatus::SendOk => sink.push(text),
                Ok(r) => println!("  [WARN] send {text} status={:?}", r.status),
                Err(e) => println!("  [WARN] send {text} failed: {e}"),
            }
        }
    }
    ck.check(
        "3 messages sent before the consumer starts (2 red + 1 blue)",
        red.len() == 2 && blue.len() == 1,
        &format!("red={} blue={}", red.len(), blue.len()),
    );

    let inbox = Arc::new(Inbox::default());
    let consumer = match DefaultMQPushConsumer::with_config(ConsumerConfig {
        consumer_group: format!("{}_FIRST", env.group),
        name_server_addrs: vec![env.namesrv.clone()],
        instance_name: format!("live-sql92-first-{}", env.stamp),
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..Default::default()
    })
    .map_err(|e| format!("consumer build failed: {e}"))
    {
        Ok(c) => c,
        Err(e) => return ck.abort("S5 build consumer", &e),
    };
    if let Err(e) = consumer.subscribe_with_selector(&topic, &MessageSelector::by_sql("color = 'red'")) {
        return ck.abort("S5 subscribe", &e.to_string());
    }
    consumer.set_message_listener_concurrently(Arc::new(CollectingListener {
        inbox: Arc::clone(&inbox),
    }));
    if let Err(e) = consumer.start().await {
        return ck.abort("S5 consumer start", &e.to_string());
    }
    poll_until(|| inbox.count() >= 2, WAIT_SECONDS).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let bodies = inbox.bodies();
    let mut want_red = red.clone();
    want_red.sort();
    ck.check(
        "SQL92 + FIRST_OFFSET receives the 2 red messages sent BEFORE start (was: skipped by the old max-offset short-circuit)",
        bodies == want_red,
        &format!("{bodies:?}"),
    );
    ck.check(
        "no blue leaks into the send-first SQL92 consumer",
        blue.iter().all(|b| !bodies.contains(b)),
        &format!("{bodies:?}"),
    );
    consumer.shutdown();
    let retry_topic = format!("{}{}_FIRST", MixAll::RETRY_GROUP_TOPIC_PREFIX, env.group);
    let _ = env.admin.delete_topic(&topic, None).await;
    let _ = env.admin.delete_topic(&retry_topic, None).await;
}

async fn run(namesrv: &str) -> Checker {
    println!("SQL92 / CHECK_CLIENT_CONFIG live check on {namesrv}");
    let stamp = stamp();
    let mut env = match Env::new(namesrv, &stamp) {
        Ok(env) => env,
        Err(e) => {
            let mut ck = Checker::new();
            ck.abort("env build", &e);
            return ck;
        }
    };
    let mut ck = Checker::new();
    if let Err(e) = env.start().await {
        ck.abort("env start", &e);
        return ck;
    }
    // The route must really carry 4 queues to count as ready (the broker refreshes the topic to the name server asynchronously after creating it).
    let began = Instant::now();
    let mut ready = false;
    while began.elapsed() < Duration::from_secs(20) {
        if env.route_queue_count().await >= QUEUE_NUMS {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    ck.check("T0 topic route ready (4 queues)", ready, "examine_topic_route cannot get the queues");

    // Preflight: does this broker accept SQL92 subscriptions at all? The broker answers
    // CHECK_CLIENT_CONFIG(46) with SYSTEM_ERROR ("The broker does not support consumer to
    // filter message by SQL92") unless it runs with enablePropertyFilter=true, and the Java
    // client fails start() the same way. Detect that up front and say what to do, instead of
    // reporting four confusing FAILs deeper in.
    {
        let (preflight, _inbox) = match env.consumer(
            "PREFLIGHT",
            Some(MessageSelector::by_sql("color = 'red'")),
            None,
        ) {
            Ok(pair) => pair,
            Err(e) => {
                ck.abort("preflight build consumer", &e);
                return ck;
            }
        };
        match preflight.start().await {
            Ok(()) => {
                preflight.shutdown();
                println!("preflight OK: the broker accepts SQL92 subscriptions (enablePropertyFilter=true)");
            }
            Err(e) => {
                let msg = e.to_string();
                preflight.shutdown();
                if msg.contains("does not support consumer to filter message by") {
                    println!("!! the broker refuses SQL92 subscriptions: {msg}");
                    println!("!! fix: set enablePropertyFilter=true in broker.conf and restart the broker,");
                    println!("!!      then re-run this probe (the same requirement applies to the Java client).");
                    println!("!! skipping end-to-end SQL92 verification: broker not configured for property filter");
                    let retry_topic = format!("{}{}", MixAll::RETRY_GROUP_TOPIC_PREFIX, env.group);
                    let _ = env.admin.delete_topic(&env.topic, None).await;
                    let _ = env.admin.delete_topic(&retry_topic, None).await;
                    env.producer.shutdown();
                    env.admin.shutdown();
                    // Environment skip, not a client defect: no FAIL recorded.
                    return ck;
                }
                ck.abort("preflight SQL92 consumer start", &msg);
                return ck;
            }
        }
    }

    s1_wire_evidence(&env, &mut ck).await;
    s2_s3_real_filtering(&env, &mut ck).await;
    s4_bad_expression_fails_start(&env, &mut ck).await;
    s5_send_first_first_offset(&env, &mut ck).await;

    // Cleanup: topic + retry group (the consumer group itself needs no explicit deletion).
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
