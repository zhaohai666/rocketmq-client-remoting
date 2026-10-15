//! Live verification of the illegal-offset (OFFSET_ILLEGAL) correction branch.
//!
//! This branch does four things: switch the offset to the corrected value given by the broker (`setNextOffset`) → drop the
//! messages pulled but not yet consumed on this queue (`ProcessQueue.setDropped(true)`) → persist the corrected offset **immediately**
//! (`updateAndFreezeOffset` + `persist`) → remove the queue so rebalance rebuilds it from the corrected offset
//! (`removeProcessQueue` + `rebalanceImmediately`). Offline unit tests can only pin down how local state is cleared and
//! which acks get invalidated; the two things below can only be proven on a real cluster:
//!
//!
//! - S2 "immediate persist" -- the corrected offset must be pushed to the broker right away, not left for the periodic persist. Approach: exploit the
//!   existing non-atomicity of the two `resetOffsetByQueueId` RPCs (the 1st commitOffset is stored first without range validation,
//!   the 2nd one returning 222 is rejected by `resetOffsetInner`, measured in `live_admin.rs`),
//!   plant the illegal committed offset 103 on the broker, then let a new consumer with
//!   `persist_consumer_offset_interval_millis = 60000` start pulling from 103. Within the window,
//!   the only path that can write 103 back to 3 (maxOffset) is the persist built into the correction branch, with zero deliveries throughout.
//!
//! Usage:
//! ```text
//! cargo run --example live_offset_illegal -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{
    mq_key, ConsumerConfig, DefaultMQPushConsumer,
};
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

const QUEUES: i32 = 1;
const MSGS: i64 = 3;
/// Illegal offset: far beyond maxOffset, the broker must reply `PULL_OFFSET_MOVED`.
const ILLEGAL_TARGET: i64 = 103;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

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
}

async fn poll_until(mut pred: impl FnMut() -> bool, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if pred() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Gated listener: records the arriving queueOffsets, then **holds** this batch (simulating "pulled but not yet acked").
///
/// The dispatch loop is a single task: holding the first batch ⇒ only 1 message is in flight on the single queue while the rest stay in the buffer,
/// which is exactly the window S1 needs. On a real run this job is carried by `spawn_blocking`, so blocking is safe.
struct GatedListener {
    state: Mutex<GateState>,
    cv: Condvar,
}

struct GateState {
    arrivals: Vec<i64>,
    released: bool,
}

impl GatedListener {
    fn new() -> Arc<GatedListener> {
        Arc::new(GatedListener {
            state: Mutex::new(GateState {
                arrivals: Vec::new(),
                released: false,
            }),
            cv: Condvar::new(),
        })
    }

    fn arrivals(&self) -> Vec<i64> {
        lock(&self.state).arrivals.clone()
    }

    fn release(&self) {
        lock(&self.state).released = true;
        self.cv.notify_all();
    }
}

impl MessageListenerConcurrently for GatedListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let mut st = lock(&self.state);
        for m in msgs {
            st.arrivals.push(m.queue_offset);
        }
        // The cap only prevents deadlock: the normal path is released explicitly by release, and must come after the correction
        let deadline = Instant::now() + Duration::from_secs(90);
        while !st.released {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let (guard, _) = self
                .cv
                .wait_timeout(st, deadline - now)
                .unwrap_or_else(|e| e.into_inner());
            st = guard;
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

/// Records only the arriving queueOffsets: this pass cares about "which messages should not arrive", not the message content.
struct RecordingListener {
    arrivals: Mutex<Vec<i64>>,
}

impl RecordingListener {
    fn new() -> Arc<RecordingListener> {
        Arc::new(RecordingListener {
            arrivals: Mutex::new(Vec::new()),
        })
    }

    fn arrivals(&self) -> Vec<i64> {
        lock(&self.arrivals).clone()
    }
}

impl MessageListenerConcurrently for RecordingListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let mut arrivals = lock(&self.arrivals);
        for m in msgs {
            arrivals.push(m.queue_offset);
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

struct Fixture {
    namesrv: String,
    stamp: u64,
    broker_addr: String,
    admin: DefaultMQAdminExt,
    producer: DefaultMQProducer,
}

impl Fixture {
    async fn new(namesrv: &str) -> Result<Fixture, String> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let admin = DefaultMQAdminExt::with_config(AdminConfig {
            instance_name: format!("rust-live-oil-admin-{stamp}"),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        admin
            .start()
            .await
            .map_err(|e| format!("admin start failed: {e}"))?;
        let producer = DefaultMQProducer::new(&format!("rust-live-oil-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        producer
            .start()
            .await
            .map_err(|e| format!("producer start failed: {e}"))?;

        // Port open != broker registered with the name server: poll and wait here
        let deadline = Instant::now() + Duration::from_secs(40);
        let broker_addr = loop {
            match admin.fetch_broker_cluster_info().await {
                Ok(info) if !info.broker_addr_table.is_empty() => {
                    break info
                        .get_broker_addrs()
                        .first()
                        .cloned()
                        .ok_or_else(|| "cluster info has no broker address".to_string())?;
                }
                _ => {}
            }
            if Instant::now() > deadline {
                return Err("nameServer returned no broker within the budget".to_string());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        };
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            stamp,
            broker_addr,
            admin,
            producer,
        })
    }

    fn topic(&self, tag: &str) -> String {
        format!("RustLiveOil{tag}{}", self.stamp)
    }

    async fn create_topic(&self, topic: &str) -> Result<(), String> {
        self.admin
            .create_topic(MixAll::DEFAULT_TOPIC, topic, QUEUES, 0)
            .await
            .map_err(|e| format!("create topic {topic} failed: {e}"))?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    /// The real queue list of this topic from the route (`%RETRY%<group>` has only 1 queue,
    /// so querying it with the main topic's queue count would give a false report).
    async fn queues(&self, topic: &str) -> Vec<MessageQueue> {
        match self.admin.examine_topic_route(topic).await {
            Ok(route) => route
                .get_all_message_queue(topic)
                .into_iter()
                .map(|k| MessageQueue::new(&k.topic, &k.broker_name, k.queue_id))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Queue key of the single-queue topic (matching the consumer-side `_mq_map` key convention).
    async fn queue_key(&self, topic: &str) -> Result<String, String> {
        self.queues(topic)
            .await
            .first()
            .map(mq_key)
            .ok_or_else(|| format!("route of {topic} has no queue"))
    }

    /// The committed offset of this queue on the broker; `None` = no record found (`QUERY_NOT_FOUND`).
    async fn committed(&self, group: &str, topic: &str) -> Vec<(MessageQueue, Option<i64>)> {
        let mut out = Vec::new();
        for mq in self.queues(topic).await {
            let off = self.admin.examine_consumer_offset(group, &mq).await.unwrap_or(None);
            out.push((mq, off));
        }
        out
    }

    fn consumer(
        &self,
        group: &str,
        role: &str,
        topic: &str,
        batch_size: i32,
        from_first: bool,
        persist_millis: Option<u64>,
    ) -> Result<DefaultMQPushConsumer, String> {
        let cfg = ConsumerConfig {
            consumer_group: group.to_string(),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("rust-live-oil-{role}-{}", self.stamp),
            consume_from_where: if from_first {
                ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string()
            } else {
                ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET.to_string()
            },
            consume_message_batch_max_size: batch_size,
            ..Default::default()
        };
        let consumer = DefaultMQPushConsumer::with_config(cfg)
            .map_err(|e| format!("build consumer failed: {e}"))?;
        if let Some(ms) = persist_millis {
            consumer.set_persist_consumer_offset_interval_millis(ms);
        }
        consumer
            .subscribe(topic, "*")
            .map_err(|e| format!("subscribe {topic} failed: {e}"))?;
        Ok(consumer)
    }

    fn shutdown(&self) {
        self.producer.shutdown();
        self.admin.shutdown();
    }
}

fn fmt(rows: &[(MessageQueue, Option<i64>)]) -> String {
    rows.iter()
        .map(|(mq, off)| {
            format!(
                "q{}:{}",
                mq.queue_id,
                off.map(|o| o.to_string()).unwrap_or_else(|| "None".to_string())
            )
        })
        .collect::<Vec<String>>()
        .join(" ")
}

async fn s1_drop_and_rebuild(fx: &Fixture, ck: &mut Checker) {
    let topic = fx.topic("Drop");
    let group = format!("GID_rust_live_oil_drop_{}", fx.stamp);
    if let Err(e) = fx.create_topic(&topic).await {
        ck.check("S1-create topic", false, &e);
        return;
    }
    let key = match fx.queue_key(&topic).await {
        Ok(k) => k,
        Err(e) => {
            ck.check("S1-queue key", false, &e);
            return;
        }
    };

    let sink = GatedListener::new();
    let c = match fx.consumer(&group, "drop", &topic, 1, true, None) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S1-consumer", false, &e);
            return;
        }
    };
    c.set_message_listener_concurrently(sink.clone());
    if let Err(e) = c.start().await {
        ck.check("S1-start", false, &format!("{e}"));
        c.shutdown();
        return;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    for i in 0..MSGS {
        let mut msg = Message::new(&topic, Some(format!("ilo-{i}").as_bytes()));
        if let Err(e) = fx.producer.send(&mut msg, Some(20000), None).await {
            ck.check("S1-send", false, &format!("{e}"));
            sink.release();
            c.shutdown();
            return;
        }
    }
    println!("S1: sent {MSGS} messages (single queue), waiting for the listener to hold the 1st one...");

    let ready = poll_until(
        || sink.arrivals().len() == 1 && c.cached_message_count(&key) == (MSGS - 1) as usize,
        30,
    )
    .await;
    ck.check(
        &format!(
            "S1-window ready: 1 message in flight (listener gated) + {} messages held in the buffer",
            MSGS - 1
        ),
        ready,
        &format!(
            "pending={} arrivals={:?}",
            c.cached_message_count(&key),
            sink.arrivals()
        ),
    );

    // Server-side reset to maxOffset: RPC2 writes resetOffsetTable, and the next pull hits it
    // and is short-circuited by the broker into OFFSET_RESET (nextBeginOffset=corrected value)
    if let Err(e) = fx
        .admin
        .reset_offset_by_queue_id(&fx.broker_addr, &group, &topic, 0, MSGS)
        .await
    {
        println!("  [WARN] resetOffsetByQueueId 2nd RPC rejected (expected in this scenario): {e}");
    }
    println!("S1: resetOffsetByQueueId(->{MSGS}) issued, waiting for the next pull to pick up the server-side reset...");

    let bumped = poll_until(|| c.queue_epoch(&key) >= 1, 45).await;
    ck.check(
        "S1-local side drops the queue after the broker rules the offset illegal (ProcessQueue.setDropped: epoch +1)",
        bumped,
        &format!("epoch={} arrivals={:?}", c.queue_epoch(&key), sink.arrivals()),
    );

    // The committed-offset wait must poll: the offset is pushed to the broker by the local persist, not local state
    let mut ok = false;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let rows = fx.committed(&group, &topic).await;
        if !rows.is_empty() && rows.iter().all(|(_, o)| *o == Some(MSGS)) {
            ok = true;
            break;
        }
        if Instant::now() >= deadline {
            println!("  [WARN] committed={}", fmt(&rows));
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        &format!("S1-committed offset on the broker stays at the corrected value {MSGS} (the two reset RPCs of this scenario already wrote it once; weak assertion)"),
        ok,
        &fmt(&fx.committed(&group, &topic).await),
    );

    sink.release();
    tokio::time::sleep(Duration::from_secs(6)).await;
    let arr = sink.arrivals();
    ck.check(
        &format!(
            "S1-the {} messages pulled into the buffer are invalidated as a batch (offsets 1 and 2 are never delivered)",
            MSGS - 1
        ),
        !arr.is_empty() && !arr.contains(&1) && !arr.contains(&2),
        &format!("arrivals={arr:?}"),
    );

    let mut msg = Message::new(&topic, Some(b"ilo-after"));
    if let Err(e) = fx.producer.send(&mut msg, Some(20000), None).await {
        ck.check("S1-send after", false, &format!("{e}"));
        c.shutdown();
        return;
    }
    let got4 = poll_until(|| sink.arrivals().contains(&MSGS), 20).await;
    let arr = sink.arrivals();
    ck.check(
        &format!(
            "S1-the rebuilt queue resumes pulling from the corrected offset (message {MSGS} is delivered normally, history already fetched is not redelivered)"
        ),
        got4 && !arr.contains(&1) && !arr.contains(&2),
        &format!("arrivals={arr:?}"),
    );

    let mut ok = false;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let rows = fx.committed(&group, &topic).await;
        if !rows.is_empty() && rows.iter().all(|(_, o)| *o == Some(MSGS + 1)) {
            ok = true;
            break;
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    ck.check(
        &format!("S1-freeze is lifted by the rebuild (the ack of new messages advances the broker offset to {})", MSGS + 1),
        ok,
        &format!("committed={}", fmt(&fx.committed(&group, &topic).await)),
    );
    c.shutdown();
    cleanup(fx, &topic, &group).await;
}

async fn s2_immediate_persist(fx: &Fixture, ck: &mut Checker) {
    let topic = fx.topic("Persist");
    let group = format!("GID_rust_live_oil_persist_{}", fx.stamp);
    if let Err(e) = fx.create_topic(&topic).await {
        ck.check("S2-create topic", false, &e);
        return;
    }
    let sink = RecordingListener::new();
    let c = match fx.consumer(&group, "persist", &topic, 1, true, None) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S2-consumer", false, &e);
            return;
        }
    };
    c.set_message_listener_concurrently(sink.clone());
    if let Err(e) = c.start().await {
        ck.check("S2-start", false, &format!("{e}"));
        c.shutdown();
        return;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    for i in 0..MSGS {
        let mut msg = Message::new(&topic, Some(format!("ilo2-{i}").as_bytes()));
        if let Err(e) = fx.producer.send(&mut msg, Some(20000), None).await {
            ck.check("S2-send", false, &format!("{e}"));
            c.shutdown();
            return;
        }
    }
    let consumed = poll_until(|| sink.arrivals().len() == MSGS as usize, 30).await;
    ck.check(
        &format!("S2-precondition: the consumer first consumes the {MSGS} messages normally"),
        consumed,
        &format!("arrivals={:?}", sink.arrivals()),
    );
    // The persist on shutdown is spawned (not waited on); wait until the broker shows 3 before planting the illegal value,
    // otherwise that late persist would overwrite the 103 planted below.
    c.shutdown();
    let persisted = {
        let mut ok = false;
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let rows = fx.committed(&group, &topic).await;
            if !rows.is_empty() && rows.iter().all(|(_, o)| *o == Some(MSGS)) {
                ok = true;
                break;
            }
            if Instant::now() >= deadline {
                println!("  [WARN] shutdown persist not landed: {}", fmt(&rows));
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        ok
    };
    ck.check(
        "S2-precondition: shutdown persist wrote the offset up to maxOffset",
        persisted,
        &fmt(&fx.committed(&group, &topic).await),
    );

    let mut rejected = false;
    let mut remark = String::new();
    match fx
        .admin
        .reset_offset_by_queue_id(&fx.broker_addr, &group, &topic, 0, ILLEGAL_TARGET)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            rejected = true;
            remark = format!("{e}").chars().take(140).collect();
        }
    }
    ck.check(
        "S2-precondition: out-of-range target rejected by resetOffsetInner (2nd RPC)",
        rejected,
        &remark,
    );
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        &format!("S2-precondition: the 1st commitOffset already stored the illegal offset {ILLEGAL_TARGET} (the two RPCs are not atomic)"),
        !rows.is_empty() && rows.iter().all(|(_, o)| *o == Some(ILLEGAL_TARGET)),
        &fmt(&rows),
    );

    // Periodic persist stretched to 60s: within the window the only path that can rewrite the broker offset is the immediate persist built into the correction branch
    let l2 = RecordingListener::new();
    let c2 = match fx.consumer(&group, "persist2", &topic, 1, true, Some(60_000)) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S2-consumer2", false, &e);
            return;
        }
    };
    c2.set_message_listener_concurrently(l2.clone());
    let t0 = Instant::now();
    if let Err(e) = c2.start().await {
        ck.check("S2-start2", false, &format!("{e}"));
        c2.shutdown();
        return;
    }
    println!(
        "S2: the new consumer starts pulling from the illegal offset {ILLEGAL_TARGET}, waiting for the correction to write the broker offset back to {MSGS} (periodic persist=60s)..."
    );
    let mut ok = false;
    let mut last;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        last = fx.committed(&group, &topic).await;
        if !last.is_empty() && last.iter().all(|(_, o)| *o == Some(MSGS)) {
            ok = true;
            break;
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let elapsed = t0.elapsed();
    ck.check(
        &format!(
            "S2-broker offset corrected from {ILLEGAL_TARGET} back to {MSGS} (the persist built into the correction branch)"
        ),
        ok,
        &format!("elapsed={:.1}s committed={}", elapsed.as_secs_f64(), fmt(&last)),
    );

    // Then wait one more silent window: after the correction the offset does not go back and redeliver 0..2, nor is it rewritten again
    tokio::time::sleep(Duration::from_secs(6)).await;
    ck.check(
        "S2-zero deliveries throughout (the corrected offset lands after the historical messages, none are dispatched)",
        l2.arrivals().is_empty(),
        &format!("arrivals={:?}", l2.arrivals()),
    );
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        &format!("S2-after the silent window the offset still stays at {MSGS}"),
        !rows.is_empty() && rows.iter().all(|(_, o)| *o == Some(MSGS)),
        &fmt(&rows),
    );
    c2.shutdown();
    cleanup(fx, &topic, &group).await;
}

async fn cleanup(fx: &Fixture, topic: &str, group: &str) {
    if let Err(e) = fx.admin.delete_topic(topic, None).await {
        println!("    (deleteTopic({topic}) failed: {e})");
    } else {
        println!("    (deleteTopic({topic}) OK)");
    }
    if let Err(e) = fx
        .admin
        .delete_subscription_group(&fx.broker_addr, group, true)
        .await
    {
        println!("    (deleteSubscriptionGroup({group}) failed: {e})");
    }
}

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let fx = match Fixture::new(namesrv).await {
        Ok(fx) => fx,
        Err(e) => {
            ck.check("cluster probe", false, &e);
            return ck;
        }
    };
    ck.check("cluster probe", true, &format!("broker={}", fx.broker_addr));
    s1_drop_and_rebuild(&fx, &mut ck).await;
    s2_immediate_persist(&fx, &mut ck).await;
    fx.shutdown();
    ck
}

fn report(ck: &mut Checker) {
    println!("\n== Result: {} PASS / {} FAIL ==", ck.passed, ck.failed.len());
    for f in &ck.failed {
        println!("  FAILED: {f}");
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
