//! Live verification of [`DefaultLitePullConsumer`] against a **real 5.5.1 broker**.
//!
//! Covers the six basic scenarios S1..S6, plus three additions: pause/resume, manual heartbeat,
//! and "seek() drops buffered messages of that queue older than the target offset" (L7/L8/L4) --
//! only a real broker can verify these (background pull loop + broker-side offsets and subscriptions).
//!
//!
//! Usage (first start the local cluster per the runbook recorded in project memory):
//! ```text
//! cargo run --example live_lite_pull_consumer -- 127.0.0.1:9876
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::process::ExitCode;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Local, TimeZone, Utc};

use rocketmq_client_remoting::client::consumer::mq_key;
use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultLitePullConsumer, DefaultMQPullConsumer, LitePullConsumerConfig, MAX_POLL_BATCH_SIZE,
    PullConsumerConfig,
};
use rocketmq_client_remoting::client::result::SendStatus;
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;

/// Queue count and message count of the test topic (12 messages, TagA/TagB alternating -> 6 each).
const QUEUE_NUMS: i32 = 4;
const N_MSG: usize = 12;
/// Max seconds to wait for the background pull loop to fill the buffer.
const WAIT_SECONDS: u64 = 30;

// ------------------------------------------------------------------ skeleton

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// Assertion accumulator: run every scenario once, then summarize; the first failure does not exit early.
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

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn bodies(msgs: &[MessageExt]) -> BTreeSet<String> {
    msgs.iter()
        .map(|m| String::from_utf8_lossy(m.get_body()).into_owned())
        .collect()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn wall_clock(secs_offset: i64) -> String {
    let dt: DateTime<Local> = Local
        .timestamp_opt(Utc::now().timestamp() + secs_offset, 0)
        .single()
        .unwrap_or_else(Local::now);
    dt.format("%Y%m%d%H%M%S").to_string()
}

/// Wait for the first rebalance to hand out queues.
///
/// ⚠ lite's `start()` does **not** rebalance synchronously (once the background loop is up,
/// `_pull_service_loop` triggers one every >1s), so `assignment()` may be empty right after
/// start. Nudge an explicit `rebalance()` here; never test "eventually consistent" as "instantly consistent".
async fn wait_assignment(c: &DefaultLitePullConsumer, want: usize, secs: u64) -> Vec<MessageQueue> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let assigned = c.assignment();
        if assigned.len() >= want || Instant::now() >= deadline {
            return assigned;
        }
        c.rebalance().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Repeatedly `poll` until `want` messages accumulate or timeout (the background loop fills the buffer; never gamble on a sleep).
///
/// A refused poll is a broken scene, not a short drain: without a running consumer nothing is ever
/// delivered, so record the refusal and stop here -- the count checks of the caller then also fail.
async fn poll_bounded(
    ck: &mut Checker,
    c: &DefaultLitePullConsumer,
    want: usize,
    secs: u64,
) -> Vec<MessageExt> {
    let mut got: Vec<MessageExt> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(secs);
    while got.len() < want && Instant::now() < deadline {
        match c.poll(Some(1000)).await {
            Ok(batch) => got.extend(batch),
            Err(e) => {
                ck.abort("poll during the drain loop", &e.to_string());
                break;
            }
        }
    }
    got
}

// ------------------------------------------------------------------ fixture

struct Fixture {
    namesrv: String,
    stamp: String,
    producer: DefaultMQProducer,
    /// Admin instance used to create/delete topics.
    admin: MQClientInstance,
    broker_name: String,
    broker_addr: String,
    /// Topics created in this run; L10 deletes them all.
    topics: Mutex<Vec<String>>,
}

impl Fixture {
    fn new(namesrv: &str, stamp: &str) -> Result<Fixture, String> {
        let producer = DefaultMQProducer::new(&format!("rust-live-lite-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        let admin = MQClientInstance::new(
            &format!("rust-live-lite-admin-{stamp}"),
            vec![namesrv.to_string()],
        );
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            producer,
            admin,
            broker_name: String::new(),
            broker_addr: String::new(),
            topics: Mutex::new(Vec::new()),
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
        let route = self
            .admin
            .get_topic_route_data(MixAll::DEFAULT_TOPIC)
            .await
            .ok_or_else(|| format!("no route of {} from namesrv", MixAll::DEFAULT_TOPIC))?;
        let (broker_name, broker_addr) = broker_of(&route)?;
        self.broker_name = broker_name;
        self.broker_addr = broker_addr;
        Ok(())
    }

    fn topic_name(&self, kind: &str) -> String {
        format!("RustLiveLite{kind}{}", self.stamp)
    }

    fn group_name(&self, kind: &str) -> String {
        format!("rust-live-lite-{kind}-{}", self.stamp)
    }

    /// Create the topic explicitly (not relying on `autoCreateTopicEnable`, so the queue count is deterministic).
    async fn create_topic(&self, topic: &str, queues: i32) -> Result<(), String> {
        self.admin
            .create_topic_in_broker(
                &self.broker_addr,
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
            .await
            .map_err(|e| format!("create topic {topic} failed: {e}"))?;
        lock(&self.topics).push(topic.to_string());
        Ok(())
    }

    /// Build a lite consumer for a group name (not subscribed, not started; the caller sets the start point via `set_consume_from_where`).
    fn consumer(&self, kind: &str) -> Result<DefaultLitePullConsumer, String> {
        let cfg = LitePullConsumerConfig {
            consumer_group: self.group_name(kind),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("live-lite-{kind}-{}", self.stamp),
            poll_timeout_millis: 1000,
            ..Default::default()
        };
        DefaultLitePullConsumer::with_config(cfg).map_err(|e| format!("{kind} build failed: {e}"))
    }

    /// Built, subscribed, started consumer (subscribing must precede start:
    /// the synchronous heartbeat inside `start()` must carry the subscription set).
    async fn subscribed(
        &self,
        ck: &mut Checker,
        kind: &str,
        topic: &str,
        expression: &str,
        from: Option<&str>,
        timestamp: Option<String>,
    ) -> Option<DefaultLitePullConsumer> {
        let c = match self.consumer(kind) {
            Ok(c) => c,
            Err(e) => {
                ck.abort(&format!("{kind} build"), &e);
                return None;
            }
        };
        if let Err(e) = c.subscribe(topic, expression) {
            ck.abort(&format!("{kind} subscribe"), &e.to_string());
            return None;
        }
        if let Some(where_) = from {
            c.set_consume_from_where(where_);
        }
        if let Some(ts) = timestamp {
            c.set_consume_timestamp(&ts);
        }
        if let Err(e) = c.start().await {
            ck.abort(&format!("{kind} start"), &e.to_string());
            return None;
        }
        Some(c)
    }

    /// Send TagA/TagB alternately (even indexes are TagA); body = `<topic>-<i:02>`.
    async fn produce_alternating(&self, topic: &str) -> usize {
        let mut ok = 0;
        for i in 0..N_MSG {
            let body = format!("{topic}-{i:02}");
            let mut msg = Message::new(topic, Some(body.as_bytes()));
            msg.set_tags(if i % 2 == 0 { "TagA" } else { "TagB" });
            match self.producer.send(&mut msg, Some(5000), None).await {
                Ok(_) => ok += 1,
                Err(e) => println!("  [WARN] send {body} failed: {e}"),
            }
        }
        ok
    }
}

fn broker_of(route: &TopicRouteData) -> Result<(String, String), String> {
    let bd = route
        .broker_datas
        .first()
        .ok_or_else(|| "route has no brokerData".to_string())?;
    let addr = bd
        .select_broker_addr()
        .ok_or_else(|| format!("broker {} has no address", bd.broker_name))?;
    Ok((bd.broker_name.clone(), addr))
}

/// All queues of the business topic (routed via the admin instance, not the consumer).
async fn route_queues(fx: &Fixture, topic: &str) -> Vec<MessageQueue> {
    fx.admin
        .get_topic_publish_info(topic, false)
        .await
        .map(|info| info.msg_queue_list())
        .unwrap_or_default()
}

fn expected_bodies(topic: &str) -> BTreeSet<String> {
    (0..N_MSG).map(|i| format!("{topic}-{i:02}")).collect()
}

// ------------------------------------------------------------- L1 lifecycle

async fn l1_lifecycle(ck: &mut Checker, fx: &Fixture) {
    let c = match fx.consumer("bare") {
        Ok(c) => c,
        Err(e) => {
            ck.abort("L1 build", &e);
            return;
        }
    };
    match c.start().await {
        Ok(()) => ck.check("L1 start fails without a subscription", false, "start unexpectedly succeeded"),
        Err(e) => ck.check(
            "L1 start fails without a subscription",
            e.to_string().contains("subscription is not set"),
            &e.to_string(),
        ),
    }
    ck.check("L1 no started state left behind after failure", !c.is_started(), "");
    ck.check(
        "L1 poll refuses before start",
        c.poll(Some(100)).await.is_err(),
        "",
    );
    ck.check("L1 is_running is false before start", !c.is_running(), "");

    // consumeTimestamp only accepts 14-digit wall clock: epoch millis must be rejected (otherwise parsed
    // as an epoch, the start point jumps decades ahead -- silently degrading to "consume from max offset").
    if let Err(e) = c.subscribe(&fx.topic_name("Ts"), "*") {
        ck.abort("L1 subscribe for the timestamp check", &e.to_string());
        return;
    }
    c.set_consume_timestamp("1789000000000");
    match c.start().await {
        Ok(()) => ck.check("L1 invalid consumeTimestamp is rejected", false, "start unexpectedly succeeded"),
        Err(e) => ck.check(
            "L1 invalid consumeTimestamp is rejected",
            e.to_string().contains("consumeTimestamp is invalid"),
            &e.to_string(),
        ),
    }
    ck.check("L1 back to not-started after the validation failure", !c.is_started(), "");

    // Assign mode (no subscription table needed) + idempotency.
    let c2 = match fx.consumer("life") {
        Ok(c) => c,
        Err(e) => {
            ck.abort("L1 build", &e);
            return;
        }
    };
    if let Err(e) = c2.assign(&[MessageQueue::new(
        &fx.topic_name("Main"),
        &fx.broker_name,
        0,
    )]) {
        ck.abort("L1 assign in assign mode", &e.to_string());
        return;
    }
    ck.check("L1 is_assign_mode after assign", c2.is_assign_mode(), "");
    if let Err(e) = c2.start().await {
        ck.abort("L1 assign-mode start", &e.to_string());
        return;
    }
    ck.check("L1 is_started after start", c2.is_started(), "");
    ck.check("L1 repeated start is idempotent", c2.start().await.is_ok(), "");
    ck.check(
        "L1 client_id = local IP@instanceName (buildMQClientId semantics)",
        c2.client_id()
            .split_once('@')
            .is_some_and(|(ip, instance)| !ip.is_empty() && instance.starts_with("live-lite-life")),
        &c2.client_id(),
    );
    c2.shutdown();
    c2.shutdown();
    ck.check("L1 shutdown is idempotent and does not panic", !c2.is_started(), "");
    ck.check("L1 is_running resets after shutdown", !c2.is_running(), "");
}

// ------------------------------------------------- L2 subscribe + poll receives all

async fn l2_subscribe_poll(ck: &mut Checker, fx: &Fixture, topic: &str) {
    // The start point must be FIRST: the first rebalance is done by the background loop (`start()`
    // does not rebalance synchronously); with LAST, whether "offset resolution" or "message send" goes
    // first is indeterminate -- resolving after sending parks the cursor at the queue tail and the 12 messages are never received. LAST is covered by L4 (assign mode).
    let c = match fx
        .subscribed(ck, "sub", topic, "*", Some(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET), None)
        .await
    {
        Some(c) => c,
        None => return,
    };
    let sent = fx.produce_alternating(topic).await;
    ck.check(
        &format!("sent all {N_MSG} with SEND_OK"),
        sent == N_MSG,
        &format!("sent={sent}"),
    );
    let assigned = wait_assignment(&c, QUEUE_NUMS as usize, 20).await;
    ck.check(
        &format!("L2 assigned {QUEUE_NUMS} queues after the first rebalance"),
        assigned.len() == QUEUE_NUMS as usize,
        &format!("assigned={assigned:?}"),
    );
    let got = poll_bounded(ck, &c, N_MSG, WAIT_SECONDS).await;
    ck.check(
        &format!("L2 poll received all {N_MSG} (no dup, no gap)"),
        bodies(&got) == expected_bodies(topic),
        &format!("n={} got={:?}", got.len(), bodies(&got)),
    );
    let per_queue_ok = got.iter().all(|m| {
        m.topic == topic && m.queue_id >= 0 && m.queue_offset >= 0 && m.reconsume_times == 0
    });
    ck.check("L2 delivery fields come from the broker", per_queue_ok, "");
    let mut by_queue: std::collections::BTreeMap<i32, Vec<i64>> =
        std::collections::BTreeMap::new();
    for m in &got {
        by_queue.entry(m.queue_id).or_default().push(m.queue_offset);
    }
    ck.check(
        "L2 queue_offset strictly increases within a queue",
        by_queue.values().all(|v| v.windows(2).all(|w| w[0] < w[1])),
        &format!("{by_queue:?}"),
    );
    c.shutdown();
}

// ------------------------------------------------------------- L3 offset commit

async fn l3_commit(ck: &mut Checker, fx: &Fixture, topic: &str, mqs: &[MessageQueue]) {
    // auto_commit=true (default): **commits as soon as a pull lands**; no explicit commit() needed.
    let c = match fx
        .subscribed(
            ck,
            "ac",
            topic,
            "*",
            Some(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET),
            None,
        )
        .await
    {
        Some(c) => c,
        None => return,
    };
    let got = poll_bounded(ck, &c, N_MSG, WAIT_SECONDS).await;
    ck.check(
        &format!("L3 auto-commit group received all {N_MSG}"),
        got.len() == N_MSG,
        &format!("n={}", got.len()),
    );
    let interval = c.config().auto_commit_interval_millis.max(0) as u64;
    let spin_until = Instant::now() + Duration::from_millis(interval + 1_500);
    while Instant::now() < spin_until {
        let _ = c.poll(Some(500)).await;
    }
    let mut committed = Vec::new();
    let mut err = String::new();
    for mq in mqs {
        match c.committed(mq).await {
            Ok(v) => committed.push((mq.queue_id, v)),
            Err(e) => {
                if err.is_empty() {
                    err = e.to_string();
                }
            }
        }
    }
    if !err.is_empty() {
        ck.abort("L3 committed read-back", &err);
    } else {
        let advanced = committed
            .iter()
            .all(|(_, off)| off.unwrap_or(0) > 0);
        ck.check(
            "L3 with auto_commit=true the offset persists on its own after polling past the period (nobody called commit)",
            advanced,
            &format!("{committed:?}"),
        );
    }
    c.shutdown();

    // auto_commit=false: the offset does not land on the broker; it advances only after an explicit commit().
    let manual = match fx.consumer("manual") {
        Ok(c) => c,
        Err(e) => {
            ck.abort("L3 build", &e);
            return;
        }
    };
    manual.set_auto_commit(false);
    if let Err(e) = manual.subscribe(topic, "*") {
        ck.abort("L3 manual subscribe", &e.to_string());
        return;
    }
    manual.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    if let Err(e) = manual.start().await {
        ck.abort("L3 manual start", &e.to_string());
        return;
    }
    let got = poll_bounded(ck, &manual, N_MSG, WAIT_SECONDS).await;
    ck.check(
        "L3 auto_commit=false still delivers normally",
        got.len() == N_MSG,
        &format!("n={}", got.len()),
    );
    // A refused read must not look like "nobody has committed": record the failure and stop the scene.
    let before = match manual.committed(&mqs[0]).await {
        Ok(offset) => offset,
        Err(e) => {
            ck.abort("L3 committed read before commit()", &e.to_string());
            manual.shutdown();
            return;
        }
    };
    ck.check(
        "L3 with auto_commit=false the offset does not land on the broker",
        before.is_none(),
        &format!("{before:?}"),
    );
    match manual.commit().await {
        Ok(()) => {}
        Err(e) => ck.abort("L3 commit()", &e.to_string()),
    }
    let after = match manual.committed(&mqs[0]).await {
        Ok(offset) => offset,
        Err(e) => {
            ck.abort("L3 committed read after commit()", &e.to_string());
            manual.shutdown();
            return;
        }
    };
    ck.check(
        "L3 the offset lands on the broker only after commit()",
        after.unwrap_or(0) > 0,
        &format!("before={before:?} after={after:?}"),
    );
    manual.shutdown();
}

// --------------------------------------------------- L4 assign + seek replay

async fn l4_assign_seek(ck: &mut Checker, fx: &Fixture, topic: &str, mqs: &[MessageQueue]) {
    let c = match fx.consumer("assign") {
        Ok(c) => c,
        Err(e) => {
            ck.abort("L4 build", &e);
            return;
        }
    };
    if let Err(e) = c.assign(mqs) {
        ck.abort("L4 assign", &e.to_string());
        return;
    }
    ck.check("L4 is_assign_mode after assign", c.is_assign_mode(), "");
    if let Err(e) = c.start().await {
        ck.abort("L4 start", &e.to_string());
        return;
    }
    ck.check(
        "L4 assign mode: assignment() == the assigned queues",
        c.assignment().len() == mqs.len(),
        &format!("{:?}", c.assignment()),
    );
    // Assign mode defaults to LAST (a new group has no committed offset) -> not one backlog message is received.
    ck.check(
        "L4 poll finds no backlog when the start point is the queue tail",
        c.poll(Some(500)).await.is_ok_and(|msgs| msgs.is_empty()),
        "",
    );
    if let Err(e) = c.seek_to_begin(&mqs[0]).await {
        ck.abort("L4 seek_to_begin", &e.to_string());
        c.shutdown();
        return;
    }
    let per_queue = N_MSG / QUEUE_NUMS as usize;
    let got = poll_bounded(ck, &c, per_queue, 20).await;
    let got_bodies = bodies(&got);
    ck.check(
        &format!("L4 after seek_to_begin replays that queue's {per_queue} messages"),
        got.len() == per_queue
            && got.iter().all(|m| m.queue_id == mqs[0].queue_id)
            && got_bodies.iter().all(|b| expected_bodies(topic).contains(b)),
        &format!("n={} got={got_bodies:?}", got.len()),
    );
    if let Err(e) = c.seek_to_end(&mqs[0]).await {
        ck.abort("L4 seek_to_end", &e.to_string());
        c.shutdown();
        return;
    }
    ck.check(
        "L4 seek drops buffered messages of that queue older than the target offset",
        c.buffered_message_count() == 0,
        &format!("buffered={}", c.buffered_message_count()),
    );
    for mq in mqs {
        if let Err(e) = c.seek_to_begin(mq).await {
            ck.abort("L4 seek_to_begin on all queues", &e.to_string());
            c.shutdown();
            return;
        }
    }
    let got = poll_bounded(ck, &c, N_MSG, WAIT_SECONDS).await;
    ck.check(
        &format!("L4 received all {N_MSG} after seek_to_begin on all queues"),
        bodies(&got) == expected_bodies(topic),
        &format!("n={} got={:?}", got.len(), bodies(&got)),
    );
    // Seek to a concrete offset: queue 0 replays its remaining 2 messages from offset=1.
    if let Err(e) = c.seek(&mqs[0], 1).await {
        ck.abort("L4 seek(concrete offset)", &e.to_string());
        c.shutdown();
        return;
    }
    let replay = poll_bounded(ck, &c, per_queue - 1, 20).await;
    let from_q0: BTreeSet<String> = replay
        .iter()
        .filter(|m| m.queue_id == mqs[0].queue_id)
        .map(|m| String::from_utf8_lossy(m.get_body()).into_owned())
        .collect();
    ck.check(
        "L4 seek(concrete offset) replays only that queue's remaining messages",
        from_q0.len() == per_queue - 1,
        &format!("{from_q0:?}"),
    );
    c.shutdown();
}

// ------------------------------------------------------------ L5 subscription-level tag

async fn l5_tag_filter(ck: &mut Checker, fx: &Fixture, topic: &str) {
    let c = match fx
        .subscribed(
            ck,
            "tag",
            topic,
            "TagA",
            Some(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET),
            None,
        )
        .await
    {
        Some(c) => c,
        None => return,
    };
    let want: BTreeSet<String> = (0..N_MSG)
        .step_by(2)
        .map(|i| format!("{topic}-{i:02}"))
        .collect();
    let got = poll_bounded(ck, &c, N_MSG / 2, WAIT_SECONDS).await;
    let got_bodies = bodies(&got);
    ck.check(
        &format!("L5 subscribe(\"TagA\") receives only {} messages", N_MSG / 2),
        got_bodies == want,
        &format!("n={} got={got_bodies:?}", got.len()),
    );
    ck.check(
        "L5 every received message has tag TagA",
        got.iter().all(|m| m.get_tags() == Some("TagA")),
        &format!("{:?}", got.iter().map(|m| m.get_tags()).collect::<Vec<_>>()),
    );
    let subs = c.subscription();
    ck.check(
        "L5 subscription() reads the expression back",
        subs.iter().any(|(t, e)| t == topic && e == "TagA"),
        &format!("{subs:?}"),
    );
    let sub_data = c.subscriptions();
    ck.check(
        "L5 subscriptions() holds the parsed SubscriptionData (what the heartbeat carries)",
        sub_data.iter().any(|s| s.topic == topic && s.sub_string == "TagA"),
        &format!("{sub_data:?}"),
    );
    // After unsubscribe the subscription table is empty (the heartbeat carries one item fewer).
    c.unsubscribe(topic);
    ck.check(
        "L5 the subscription table is empty after unsubscribe",
        c.subscription().is_empty(),
        &format!("{:?}", c.subscription()),
    );
    c.shutdown();
}

// ---------------------------------------------- L6 CONSUME_FROM_TIMESTAMP

async fn l6_consume_from_timestamp(
    ck: &mut Checker,
    fx: &Fixture,
    topic: &str,
    mqs: &[MessageQueue],
) {
    // L6a: start point = local wall clock 30 minutes ago -> earlier than every message -> each queue receives all from its head.
    let past = wall_clock(-30 * 60);
    let c = match fx
        .subscribed(
            ck,
            "ts",
            topic,
            "*",
            Some(ConsumeFromWhere::CONSUME_FROM_TIMESTAMP),
            Some(past.clone()),
        )
        .await
    {
        Some(c) => c,
        None => return,
    };
    let got = poll_bounded(ck, &c, N_MSG, WAIT_SECONDS).await;
    ck.check(
        &format!("L6a wall-clock start={past} (30 minutes ago) -> received all {N_MSG}"),
        bodies(&got) == expected_bodies(topic),
        &format!("n={} got={:?}", got.len(), bodies(&got)),
    );
    c.shutdown();

    // L6b: what the wall clock really affects is the "timestamp -> queue position" mapping; measure that
    // mapping directly here. The known pitfall is parsing the wall-clock string as epoch millis -- that flips both directions at once.
    let c2 = match fx
        .subscribed(
            ck,
            "tsmap",
            topic,
            "*",
            Some(ConsumeFromWhere::CONSUME_FROM_TIMESTAMP),
            Some(past.clone()),
        )
        .await
    {
        Some(c) => c,
        None => return,
    };
    let assigned = wait_assignment(&c2, mqs.len(), 20).await;
    let wall = now_ms();
    let mut sum_past = 0;
    let mut sum_future = 0;
    let mut err = String::new();
    for mq in &assigned {
        match (
            c2.offset_for_timestamp(mq, wall - 30 * 60 * 1000).await,
            c2.offset_for_timestamp(mq, wall + 10 * 60 * 1000).await,
        ) {
            (Ok(a), Ok(b)) => {
                sum_past += a;
                sum_future += b;
            }
            (Err(e), _) | (_, Err(e)) => {
                if err.is_empty() {
                    err = e.to_string();
                }
            }
        }
    }
    if !err.is_empty() {
        ck.abort("L6b offset_for_timestamp", &err);
    }
    ck.check(
        "L6b 30 minutes ago -> head of each queue (Σ==0)",
        sum_past == 0,
        &format!("Σ={sum_past}"),
    );
    ck.check(
        &format!("L6b 10 minutes ahead -> past all {N_MSG} messages (Σ=={N_MSG})"),
        sum_future == N_MSG as i64,
        &format!("Σ={sum_future}"),
    );
    ck.check(
        "L6b the same route is used beyond assign mode (4 queues)",
        assigned.len() == mqs.len(),
        &format!("{assigned:?}"),
    );
    c2.shutdown();
}

// ------------------------------------------------------- L7 pause / resume

async fn l7_pause_resume(ck: &mut Checker, fx: &Fixture) {
    // Separate topic: this scenario sends 12 more messages; sharing the main topic would skew the other scenarios' counts.
    let topic = fx.topic_name("Paused");
    if let Err(e) = fx.create_topic(&topic, QUEUE_NUMS).await {
        ck.abort("L7 create topic", &e);
        return;
    }
    let c = match fx
        .subscribed(
            ck,
            "pause",
            &topic,
            "*",
            Some(ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET),
            None,
        )
        .await
    {
        Some(c) => c,
        None => return,
    };
    // Wait for the queues to be assigned first: `pause(&[])` pauses nothing, and every later assertion would be distorted.
    let mqs = wait_assignment(&c, QUEUE_NUMS as usize, 20).await;
    ck.check(
        &format!("L7 assigned {QUEUE_NUMS} queues before pause"),
        mqs.len() == QUEUE_NUMS as usize,
        &format!("assigned={mqs:?}"),
    );
    c.pause(&mqs);
    let sent = fx.produce_alternating(&topic).await;
    ck.check(
        &format!("L7 sent {N_MSG} messages while paused"),
        sent == N_MSG,
        &format!("sent={sent}"),
    );
    let during_pause = match c.poll(Some(1500)).await {
        Ok(msgs) => msgs,
        Err(e) => {
            ck.abort("L7 poll after pause", &e.to_string());
            Vec::new()
        }
    };
    ck.check(
        "L7 poll gets no messages after pause",
        during_pause.is_empty(),
        &format!(
            "still got {} messages while paused (assignment={}): {:?}",
            during_pause.len(),
            mqs.len(),
            bodies(&during_pause)
        ),
    );
    c.resume(&mqs);
    // The few messages that slipped in during the pause count as consumed too; do not drop them and wait all over again.
    let mut got = during_pause;
    let need = N_MSG.saturating_sub(got.len());
    got.extend(poll_bounded(ck, &c, need, WAIT_SECONDS).await);
    ck.check(
        &format!("L7 received all {N_MSG} new messages after resume"),
        bodies(&got) == expected_bodies(&topic),
        &format!("n={} got={:?}", got.len(), bodies(&got)),
    );
    c.shutdown();
}

// ------------------------------------------------ L8 heartbeat / L9 state & ops

async fn l8_heartbeat_and_state(ck: &mut Checker, fx: &Fixture, topic: &str, mqs: &[MessageQueue]) {
    let c = match fx
        .subscribed(
            ck,
            "state",
            topic,
            "*",
            Some(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET),
            None,
        )
        .await
    {
        Some(c) => c,
        None => return,
    };
    // The heartbeat targets "brokers known to the instance's route table". lite's `start()` already
    // refreshed the subscribed topic's route synchronously, so the first heartbeat can reach a broker;
    // still wait for the rebalance assignment here, then verify the heartbeat truly arrives at the
    // broker -- without a registered subscription, broker-side tag filtering and GET_CONSUMER_LIST_BY_GROUP both distort.
    let _ = wait_assignment(&c, QUEUE_NUMS as usize, 20).await;
    let _ = c.fetch_message_queues(topic).await;
    let ok = c.send_heartbeat_to_all_broker().await;
    ck.check(
        "L8 the manual heartbeat reaches brokers when routes are known",
        ok >= 1,
        &format!("ok={ok}"),
    );
    let got = poll_bounded(ck, &c, 1, WAIT_SECONDS).await;
    ck.check(
        "L9 the buffer is readable after poll receives a message (buffered_message_count)",
        !got.is_empty(),
        &format!("n={} buffered={}", got.len(), c.buffered_message_count()),
    );
    let mq = &mqs[0];
    let min = c.offset_for_timestamp(mq, 1).await.unwrap_or(-1);
    let at_now = c
        .offset_for_timestamp(mq, now_ms())
        .await
        .unwrap_or(-1);
    ck.check(
        "L9 offset_for_timestamp is monotonic in time",
        min >= 0 && at_now >= min,
        &format!("min={min} at_now={at_now}"),
    );
    let fetched = c
        .fetch_subscribe_message_queues(topic)
        .await
        .unwrap_or_default();
    ck.check(
        "L9 lite's fetch_subscribe_message_queues matches the route",
        fetched.len() == mqs.len(),
        &format!("got {}", fetched.len()),
    );
    // A manual rebalance (triggered once beyond the background loop) must not change the queue count.
    c.rebalance().await;
    ck.check(
        "L9 assignment still holds 4 queues after a manual rebalance",
        c.assignment().len() == mqs.len(),
        &format!("{:?}", c.assignment()),
    );
    c.shutdown();
}

// ------------------------ L11 three offset tables

/// Reads only that one offset cell on the broker; touches none of the tested instance's memory (independent group connection).
async fn broker_offset(probe: &DefaultMQPullConsumer, mq: &MessageQueue) -> i64 {
    probe
        .fetch_consume_offset(mq)
        .await
        .unwrap_or(None)
        .unwrap_or(-1)
}

/// Hand-builds a lite consumer in assign mode: commit timing is fully scenario-controlled (`auto_commit=false`),
/// 32 messages pulled per queue at a time, starting from FIRST.
///
/// Every cache gate is lifted above the scenario's backlog on purpose: L11 buffers 1200 messages in one
/// queue, and the default per-queue count threshold (1000) would stop the background pull at 1024 -- that
/// is the gate working (L12 measures it), not a defect. L11 is about the three offset tables.
async fn lite_off_consumer(
    ck: &mut Checker,
    fx: &Fixture,
    instance: &str,
    group: &str,
    mqs: &[MessageQueue],
) -> Option<DefaultLitePullConsumer> {
    let cfg = LitePullConsumerConfig {
        consumer_group: group.to_string(),
        name_server_addrs: vec![fx.namesrv.clone()],
        instance_name: format!("{instance}-{}", fx.stamp),
        poll_timeout_millis: 1000,
        pull_batch_size: 32,
        pull_threshold_for_all: 1200 * 32,
        pull_threshold_for_queue: 1300,
        pull_threshold_size_for_queue: 1024,
        consume_max_span: 2200,
        auto_commit: false,
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..Default::default()
    };
    let o = match DefaultLitePullConsumer::with_config(cfg) {
        Ok(o) => o,
        Err(e) => {
            ck.abort(&format!("L11 build {instance}"), &e.to_string());
            return None;
        }
    };
    if let Err(e) = o.assign(mqs) {
        ck.abort(&format!("L11 {instance} assign"), &e.to_string());
        return None;
    }
    if let Err(e) = o.start().await {
        ck.abort(&format!("L11 {instance} start"), &e.to_string());
        return None;
    }
    Some(o)
}

/// The three offset tables: `nextOffset` (pull cursor) / `consumeOffset` (consumed cursor) /
/// the in-memory offset table of `offsetStore` (where commits land).
///
/// Unit tests can pin the table shape but not "this path can really change broker-side delivery":
/// committing the wrong cell (using the pull cursor as the commit source) shows up on a real broker
/// as **silent message loss** -- the offset runs ahead of consumption and after the caller crashes
/// that stretch of messages is never redelivered, even across restarts; committing too conservatively
/// only duplicates, which is visible. So a fresh 1-queue topic is filled with 1200 messages (> the 1024 single-delivery cap), making each number countable: pull cursor 1200, consumed cursor 1024, broker cell 1024.
async fn l11_three_offset_tables(ck: &mut Checker, fx: &Fixture, mqs: &[MessageQueue]) {
    const N_BIG: i64 = 1200;
    const CHUNK: i64 = 300;
    let cap = MAX_POLL_BATCH_SIZE as i64;
    let topic = fx.topic_name("Off");
    if let Err(e) = fx.create_topic(&topic, 1).await {
        ck.abort("L11 create topic (1 queue)", &e);
        return;
    }
    let mut qs: Vec<MessageQueue> = Vec::new();
    let route_deadline = Instant::now() + Duration::from_secs(20);
    while qs.is_empty() && Instant::now() < route_deadline {
        qs = route_queues(fx, &topic).await;
        if qs.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    ck.check(
        "L11 prepared topic (1 queue)",
        qs.len() == 1,
        &format!("queues={}", qs.len()),
    );
    let Some(q0) = qs.first().cloned() else {
        return;
    };
    let key0 = mq_key(&q0);

    let mut landed = 0i64;
    let mut from = 0i64;
    while from < N_BIG {
        let to = (from + CHUNK).min(N_BIG);
        let mut chunk = Vec::new();
        for i in from..to {
            chunk.push(Message::new(&topic, Some(format!("off-{i:04}").as_bytes())));
        }
        match fx.producer.send_batch(chunk, Some(&q0), Some(5_000)).await {
            Ok(r) if r.status == SendStatus::SendOk => landed += to - from,
            Ok(r) => println!("  [WARN] send_batch status={:?}", r.status),
            Err(e) => println!("  [WARN] send_batch failed: {e}"),
        }
        from = to;
    }
    ck.check(
        &format!("L11 produced all {N_BIG} successfully"),
        landed == N_BIG,
        &format!("landed={landed}"),
    );

    let group = fx.group_name("off");
    let probe_cfg = PullConsumerConfig {
        consumer_group: group.clone(),
        name_server_addrs: vec![fx.namesrv.clone()],
        instance_name: format!("liteoff-probe-{}", fx.stamp),
        ..Default::default()
    };
    let probe = match DefaultMQPullConsumer::with_config(probe_cfg) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("L11 build probe", &e.to_string());
            return;
        }
    };
    if let Err(e) = probe.start().await {
        ck.abort("L11 probe start", &e.to_string());
        return;
    }
    let Some(o) = lite_off_consumer(ck, fx, "liteoff", &group, std::slice::from_ref(&q0)).await
    else {
        probe.shutdown();
        return;
    };

    // ---- L11a pull without delivering: the pull cursor runs to 1200, the consumed cursor must not move one cell
    let pull_deadline = Instant::now() + Duration::from_secs(30);
    while o.pull_cursor_of(&q0) < N_BIG && Instant::now() < pull_deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    ck.check(
        &format!(
            "L11a the background loop pulled all {N_BIG} into the local buffer (pull cursor={})",
            o.pull_cursor_of(&q0)
        ),
        o.pull_cursor_of(&q0) == N_BIG,
        &format!("pullCursor={}", o.pull_cursor_of(&q0)),
    );
    ck.check(
        "L11a nothing delivered => the consumed cursor stays at -1",
        o.consume_cursor_of(&q0) == -1,
        &format!("consumeCursor={}", o.consume_cursor_of(&q0)),
    );
    // This is the core of #68: the old implementation's commit() iterated the pull cursor and would have
    // sent 1200 here -- a caller crashing before that point never consumed a message, yet none is ever redelivered.
    if let Err(e) = o.commit().await {
        ck.abort("L11a commit()", &e.to_string());
    }
    let after_empty_commit = broker_offset(&probe, &q0).await;
    ck.check(
        "L11a nothing delivered => the broker has no committed offset (the old implementation committed 1200 here)",
        after_empty_commit == -1,
        &format!("brokerOffset={after_empty_commit}"),
    );

    // ---- L11b a single poll caps at 1024 < 1200 buffered => the tail 176 do not count as consumed
    let first = match o.poll(Some(3000)).await {
        Ok(msgs) => msgs,
        Err(e) => {
            ck.abort("L11b poll", &e.to_string());
            Vec::new()
        }
    };
    ck.check(
        &format!("L11b one poll hands out {cap} messages (single-delivery cap)"),
        first.len() as i64 == cap,
        &format!("got={}", first.len()),
    );
    ck.check(
        "L11b the consumed cursor = the cell handed out",
        o.consume_cursor_of(&q0) == cap,
        &format!("consumeCursor={}", o.consume_cursor_of(&q0)),
    );
    ck.check(
        &format!("L11b {} undelivered messages still buffered", N_BIG - cap),
        o.pull_cursor_of(&q0) == N_BIG && o.consume_cursor_of(&q0) < o.pull_cursor_of(&q0),
        &format!("pull={}", o.pull_cursor_of(&q0)),
    );
    if let Err(e) = o.commit().await {
        ck.abort("L11b commit()", &e.to_string());
    }
    let broker_after_commit = broker_offset(&probe, &q0).await;
    ck.check(
        &format!("L11b exactly {cap} committed to the broker (not {N_BIG})"),
        broker_after_commit == cap,
        &format!("brokerOffset={broker_after_commit}"),
    );

    // ---- L11c pin an earlier offset: only the commit target changes, neither cursor may move
    let pull_before = o.pull_cursor_of(&q0);
    let consume_before = o.consume_cursor_of(&q0);
    let mut rewind: BTreeMap<String, i64> = BTreeMap::new();
    rewind.insert(key0.clone(), 5);
    if let Err(e) = o.commit_offsets(&rewind, true).await {
        ck.abort("L11c commit(map, persist=true)", &e.to_string());
    }
    let broker_rewound = broker_offset(&probe, &q0).await;
    ck.check(
        "L11c commit(map) moves the broker offset to the caller-specified 5",
        broker_rewound == 5,
        &format!("brokerOffset={broker_rewound}"),
    );
    ck.check(
        "L11c committing an offset does not touch the pull cursor",
        o.pull_cursor_of(&q0) == pull_before,
        &format!("pullCursor={}", o.pull_cursor_of(&q0)),
    );
    ck.check(
        "L11c committing an offset does not touch the consumed cursor",
        o.consume_cursor_of(&q0) == consume_before,
        &format!("consumeCursor={}", o.consume_cursor_of(&q0)),
    );
    // After rewinding to 5, the tail 176 are still delivered (the local buffer is independent of the broker offset)
    let tail = match o.poll(Some(3000)).await {
        Ok(msgs) => msgs,
        Err(e) => {
            ck.abort("L11c poll of the buffer tail", &e.to_string());
            Vec::new()
        }
    };
    ck.check(
        &format!("L11c the {} messages left in the buffer are still delivered after rewinding to 5", N_BIG - cap),
        tail.len() as i64 == N_BIG - cap,
        &format!("got={}", tail.len()),
    );
    let mut seen = bodies(&first);
    seen.extend(bodies(&tail));
    ck.check(
        &format!("L11 received all {N_BIG} across the whole run, no dup and no gap"),
        seen.len() as i64 == N_BIG,
        &format!("distinct={}", seen.len()),
    );

    // ---- L11d persist=false: not one byte goes online; committed() sees it, the broker does not
    let mut memory_only: BTreeMap<String, i64> = BTreeMap::new();
    memory_only.insert(key0.clone(), 777);
    if let Err(e) = o.commit_offsets(&memory_only, false).await {
        ck.abort("L11d commit(map, persist=false)", &e.to_string());
    }
    let committed_memory = o.committed(&q0).await.unwrap_or(None);
    ck.check(
        "L11d persist=false: committed() reads 777 from the in-memory table",
        committed_memory == Some(777),
        &format!("committed={committed_memory:?}"),
    );
    let broker_still = broker_offset(&probe, &q0).await;
    ck.check(
        "L11d persist=false: the broker side still has the previous round's 5",
        broker_still == 5,
        &format!("brokerOffset={broker_still}"),
    );

    // ---- L11e a new instance (same group) resumes from the broker's cell: the in-memory table does not cross instances
    if let Some(o2) = lite_off_consumer(ck, fx, "liteoff2", &group, std::slice::from_ref(&q0)).await
    {
        let start = o2.pull_cursor_of(&q0);
        ck.check(
            "L11e the new instance starts from the broker's 5 (not the other instance's in-memory 777)",
            (5..777).contains(&start),
            &format!("pullCursor={start}"),
        );
        o2.shutdown();
    }

    // ---- L11f seek rewrites both cursors: the replayed segment must not be skipped over by the old offset
    // Pause this queue first: continued background pulls would push the pull cursor past 60; without that this assertion is a timing gamble.
    o.pause(std::slice::from_ref(&q0));
    // Wait for the in-flight pull's response to land (it writes the pull cursor); only then can the seek pin at 60.
    tokio::time::sleep(Duration::from_millis(300)).await;
    if let Err(e) = o.seek(&q0, 60).await {
        ck.abort("L11f seek", &e.to_string());
        o.shutdown();
        probe.shutdown();
        return;
    }
    ck.check(
        "L11f seek changes the pull cursor",
        o.pull_cursor_of(&q0) == 60,
        &format!("pullCursor={}", o.pull_cursor_of(&q0)),
    );
    ck.check(
        "L11f seek also changes the consumed cursor",
        o.consume_cursor_of(&q0) == 60,
        &format!("consumeCursor={}", o.consume_cursor_of(&q0)),
    );
    if let Err(e) = o.commit().await {
        ck.abort("L11f commit()", &e.to_string());
    }
    let broker_seeked = broker_offset(&probe, &q0).await;
    ck.check(
        "L11f the commit after seek lands at 60",
        broker_seeked == 60,
        &format!("brokerOffset={broker_seeked}"),
    );
    o.resume(std::slice::from_ref(&q0));

    let Some(foreign) = mqs.get(1).cloned() else {
        ck.abort("L11g get foreign queue", "main topic has fewer than 2 queues");
        o.shutdown();
        probe.shutdown();
        return;
    };
    if let Err(e) = o.assign(&[q0.clone(), foreign.clone()]) {
        ck.abort("L11g assign the foreign queue alongside q0", &e.to_string());
        o.shutdown();
        probe.shutdown();
        return;
    }
    let mut memory_only2: BTreeMap<String, i64> = BTreeMap::new();
    memory_only2.insert(key0.clone(), 300);
    if let Err(e) = o.commit_offsets(&memory_only2, false).await {
        ck.abort("L11g commit(map, persist=false)", &e.to_string());
    }
    ck.check(
        "L11g the unpersisted in-memory value is visible first",
        o.pending_commit_of(&q0) == 300,
        &format!("pending={}", o.pending_commit_of(&q0)),
    );
    if let Err(e) = o.commit_queues(&[], true).await {
        ck.abort("L11g commit(empty set)", &e.to_string());
    }
    ck.check(
        "L11g an empty set neither clears the table nor sends anything",
        o.pending_commit_of(&q0) == 300,
        &format!("pending={}", o.pending_commit_of(&q0)),
    );
    let broker_after_empty_set = broker_offset(&probe, &q0).await;
    ck.check(
        "L11g the empty set left the broker untouched",
        broker_after_empty_set == 60,
        &format!("brokerOffset={broker_after_empty_set}"),
    );
    // Name one foreign queue: it has no consume record (the -1 guard blocks the table write),
    // but persistAll's table sweep clears q0's unpersisted value -- that is the real cost of "committing a subset of queues".
    if let Err(e) = o.commit_queues(std::slice::from_ref(&foreign), true).await {
        ck.abort("L11g commit(subset of queues)", &e.to_string());
    }
    ck.check(
        "L11g a named commit clears the in-memory values of queues not named (remove unused mq)",
        o.pending_commit_of(&q0) == -1,
        &format!("pending={}", o.pending_commit_of(&q0)),
    );
    let after_prune = o.committed(&q0).await.unwrap_or(None);
    ck.check(
        "L11g after the clear, the read-back is the broker's cell (and it backfills the table)",
        after_prune == Some(60) && o.pending_commit_of(&q0) == 60,
        &format!("committed={after_prune:?} pending={}", o.pending_commit_of(&q0)),
    );
    let broker_after_prune = broker_offset(&probe, &q0).await;
    ck.check(
        "L11g the cleanup only drops the in-memory value; nothing wrote 300 to the broker",
        broker_after_prune == 60,
        &format!("brokerOffset={broker_after_prune}"),
    );
    // commit(Set) takes the current consumed cursor, not the in-memory cell
    if let Err(e) = o.commit_offsets(&memory_only2, false).await {
        ck.abort("L11g commit(map, persist=false) second round", &e.to_string());
    }
    if let Err(e) = o.commit_queues(std::slice::from_ref(&q0), true).await {
        ck.abort("L11g commit(set)", &e.to_string());
    }
    let broker_set = broker_offset(&probe, &q0).await;
    ck.check(
        "L11g commit(set) commits the consumed cursor (60), not the in-memory 300",
        broker_set == 60,
        &format!("brokerOffset={broker_set}"),
    );
    ck.check(
        "L11g after persistAll the in-memory table returns to the consumed cursor (updateConsumeOffset semantics)",
        o.pending_commit_of(&q0) == 60,
        &format!("pending={}", o.pending_commit_of(&q0)),
    );

    o.shutdown();
    probe.shutdown();
}

// ------------------------------------------------------------------ L12 cache gate

/// A lite consumer assigned to one queue with the count threshold the caller chooses; every other gate is
/// left wide open so only the gate under test can act.
async fn gate_consumer(
    ck: &mut Checker,
    fx: &Fixture,
    instance: &str,
    group: &str,
    count_threshold: i32,
    q0: &MessageQueue,
) -> Option<DefaultLitePullConsumer> {
    let cfg = LitePullConsumerConfig {
        consumer_group: group.to_string(),
        name_server_addrs: vec![fx.namesrv.clone()],
        instance_name: format!("{instance}-{}", fx.stamp),
        poll_timeout_millis: 1000,
        pull_batch_size: 32,
        pull_threshold_for_all: 200 * 32,
        pull_threshold_for_queue: count_threshold,
        auto_commit: false,
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..Default::default()
    };
    let o = match DefaultLitePullConsumer::with_config(cfg) {
        Ok(o) => o,
        Err(e) => {
            ck.abort(&format!("L12 build {instance}"), &e.to_string());
            return None;
        }
    };
    if let Err(e) = o.assign(std::slice::from_ref(q0)) {
        ck.abort(&format!("L12 {instance} assign"), &e.to_string());
        return None;
    }
    if let Err(e) = o.start().await {
        ck.abort(&format!("L12 start {instance}"), &e.to_string());
        return None;
    }
    Some(o)
}

/// The per-queue cached-count gate on a real broker: the background pull parks itself once this queue
/// holds more than `pull_threshold_for_queue` messages, and releases as soon as `poll()` drains it.
///
/// The gate is measured, not asserted from a unit fixture: one 1-queue topic gets 200 messages, the
/// threshold is 64 and a pull round takes 32, so the cursor must stop above the threshold and strictly
/// below the tail. A second consumer on the **same queue** with the threshold lifted above the backlog
/// is the paired control -- if the pull stopped for any other reason (route, switch, broker refusal) it
/// would stop for the control too.
async fn l12_cache_gate(ck: &mut Checker, fx: &Fixture) {
    const N_GATE: i64 = 200;
    const BATCH: i32 = 32;
    const THRESHOLD: i32 = 64;

    let topic = fx.topic_name("Gate");
    if let Err(e) = fx.create_topic(&topic, 1).await {
        ck.abort("L12 create topic (1 queue)", &e);
        return;
    }
    let mut qs: Vec<MessageQueue> = Vec::new();
    let route_deadline = Instant::now() + Duration::from_secs(20);
    while qs.is_empty() && Instant::now() < route_deadline {
        qs = route_queues(fx, &topic).await;
        if qs.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    ck.check(
        "L12 prepared topic (1 queue)",
        qs.len() == 1,
        &format!("queues={}", qs.len()),
    );
    let Some(q0) = qs.first().cloned() else {
        return;
    };

    let mut landed = 0i64;
    let mut from = 0i64;
    while from < N_GATE {
        let to = (from + 50).min(N_GATE);
        let mut chunk = Vec::new();
        for i in from..to {
            chunk.push(Message::new(&topic, Some(format!("gate-{i:04}").as_bytes())));
        }
        match fx.producer.send_batch(chunk, Some(&q0), Some(5_000)).await {
            Ok(r) if r.status == SendStatus::SendOk => landed += to - from,
            Ok(r) => println!("  [WARN] L12 send_batch status={:?}", r.status),
            Err(e) => println!("  [WARN] L12 send_batch failed: {e}"),
        }
        from = to;
    }
    ck.check(
        &format!("L12 produced all {N_GATE} successfully"),
        landed == N_GATE,
        &format!("landed={landed}"),
    );

    // Only the count gate is under test; the aggregate gate needs threshold*batch to stay clear of a
    // 200-message backlog, and the size (100 MiB) and span (2000) gates cannot trip on 200 tiny messages.
    let Some(gated) = gate_consumer(ck, fx, "litegate", &fx.group_name("gate"), THRESHOLD, &q0).await
    else {
        return;
    };
    // The gate has to be reached first: a cursor stuck *below* the threshold would prove nothing about it.
    let reached = Instant::now() + Duration::from_secs(20);
    while gated.pull_cursor_of(&q0) <= i64::from(THRESHOLD) && Instant::now() < reached {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let parked = gated.pull_cursor_of(&q0);
    ck.check(
        &format!("L12a the gate let the queue cache past its threshold ({THRESHOLD})"),
        parked > i64::from(THRESHOLD),
        &format!("pullCursor={parked}"),
    );
    ck.check(
        "L12a the count gate recorded at least one trip",
        gated.flow_control_queue_triggered() > 0,
        &format!("trips={}", gated.flow_control_queue_triggered()),
    );
    // Nothing is consumed, so the buffer never drains: the refusal must hold, and one round of 32 at
    // most can still be in flight when the gate trips.
    let held = Instant::now() + Duration::from_secs(5);
    while Instant::now() < held {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let still = gated.pull_cursor_of(&q0);
    ck.check(
        &format!("L12a while the buffer is over budget the cursor holds its ground (never reaches {N_GATE})"),
        still == parked && still < N_GATE,
        &format!("pullCursor={still}"),
    );
    ck.check(
        "L12a the refusal costs at most one more pull round",
        still - i64::from(THRESHOLD) <= 2 * i64::from(BATCH),
        &format!("pullCursor={still} threshold={THRESHOLD}"),
    );

    // Paired control: same topic, same queue, only the threshold is high enough to clear the backlog.
    let Some(control) = gate_consumer(
        ck,
        fx,
        "litegatectl",
        &fx.group_name("gatectl"),
        (N_GATE + 100) as i32,
        &q0,
    )
    .await else {
        gated.shutdown();
        return;
    };
    let run = Instant::now() + Duration::from_secs(25);
    while control.pull_cursor_of(&q0) < N_GATE && Instant::now() < run {
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    ck.check(
        &format!("L12-control the same queue pulls all {N_GATE} once the threshold clears"),
        control.pull_cursor_of(&q0) == N_GATE,
        &format!("pullCursor={}", control.pull_cursor_of(&q0)),
    );

    // Draining releases the gate: the rest arrives without restarting anything.
    let mut got: Vec<MessageExt> = Vec::new();
    let drain_deadline = Instant::now() + Duration::from_secs(60);
    while (got.len() as i64) < N_GATE && Instant::now() < drain_deadline {
        match gated.poll(Some(1000)).await {
            Ok(batch) => got.extend(batch),
            Err(e) => {
                ck.abort("L12 poll during the drain loop", &e.to_string());
                break;
            }
        }
    }
    let distinct: BTreeSet<String> = got
        .iter()
        .map(|m| String::from_utf8_lossy(m.get_body()).to_string())
        .collect();
    ck.check(
        &format!("L12b after the buffer drains the rest arrives ({N_GATE})"),
        got.len() as i64 == N_GATE && distinct.len() as i64 == N_GATE,
        &format!("got={} distinct={}", got.len(), distinct.len()),
    );
    ck.check(
        "L12b the cursor runs to the tail once the queue is consumed",
        gated.pull_cursor_of(&q0) == N_GATE,
        &format!("pullCursor={}", gated.pull_cursor_of(&q0)),
    );
    gated.shutdown();
    control.shutdown();
}

// ------------------------------------------------------------------ L10 cleanup
async fn l10_cleanup(ck: &mut Checker, fx: &Fixture) {
    let topics: Vec<String> = lock(&fx.topics).clone();
    let mut failed = Vec::new();
    for topic in &topics {
        if let Err(e) = fx
            .admin
            .delete_topic_in_broker(&fx.broker_addr, topic, 5000)
            .await
        {
            failed.push(format!("{topic}:{e}"));
        }
    }
    ck.check(
        "L10 deleted the topics created in this run",
        failed.is_empty(),
        &format!("{failed:?}"),
    );
}

// ------------------------------------------------------------------ driver

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let mut fx = match Fixture::new(namesrv, &stamp) {
        Ok(fx) => fx,
        Err(e) => {
            ck.abort("fixture build", &e);
            return ck;
        }
    };
    if let Err(e) = fx.start().await {
        ck.abort("fixture start", &e);
        return ck;
    }
    println!("== live lite-pull check, namesrv={namesrv} stamp={stamp} ==");

    l1_lifecycle(&mut ck, &fx).await;

    let topic = fx.topic_name("Main");
    if let Err(e) = fx.create_topic(&topic, QUEUE_NUMS).await {
        ck.abort("create topic", &e);
        return ck;
    }
    let mqs = route_queues(&fx, &topic).await;
    if mqs.len() != QUEUE_NUMS as usize {
        ck.abort("not enough route queues", &format!("got {}", mqs.len()));
        l10_cleanup(&mut ck, &fx).await;
        return ck;
    }
    l2_subscribe_poll(&mut ck, &fx, &topic).await;
    l3_commit(&mut ck, &fx, &topic, &mqs).await;
    l4_assign_seek(&mut ck, &fx, &topic, &mqs).await;
    l5_tag_filter(&mut ck, &fx, &topic).await;
    l6_consume_from_timestamp(&mut ck, &fx, &topic, &mqs).await;
    l7_pause_resume(&mut ck, &fx).await;
    l8_heartbeat_and_state(&mut ck, &fx, &topic, &mqs).await;
    l11_three_offset_tables(&mut ck, &fx, &mqs).await;
    l12_cache_gate(&mut ck, &fx).await;
    l10_cleanup(&mut ck, &fx).await;
    ck
}

fn report(ck: &mut Checker) {
    println!(
        "== summary: {} passed, {} failed ==",
        ck.passed,
        ck.failed.len()
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
