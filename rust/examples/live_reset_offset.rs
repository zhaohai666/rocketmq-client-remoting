//! Live verification of offset reset (RESET_CONSUMER_OFFSET).
//!
//! 220 is the reset instruction the broker pushes to the **consumer**; the response of the
//! admin-side `INVOKE_BROKER_TO_RESET_OFFSET(222)` is only a table of "where each queue resets to",
//! and what actually changes the consumer's offset is the oneway 220 the broker pushes right after
//! (`Broker2Client.resetOffset:181-238`). The broker only takes this push path when `useServerSideResetOffset=false`
//! (the default true changes the offset server-side directly in `AdminBrokerProcessor:2255-2263`,
//! notifying no consumer at all), so this verification first hot-flips the switch to false (confirmed by a read-back), and restores it afterwards.
//!
//! The offline unit tests (`client::consumer`'s reset_offset_* and `client::mq_client`'s 220 handler cases)
//! pin down both request-body shapes and the local state of "drop the queue + bump the epoch + the new offset lands via the revoke tail"; only a real
//! cluster can prove the following:
//!
//! S1 "backward reset takes effect immediately + in-flight batches invalidated" -- the offset resets from 10 back to 3, with three discriminators. a) The
//! committed offset on the broker becomes 3 within ~2s: inside the window **only** the persist of the reset path writes it (the periodic flush has been stretched to
//! 60s), while a memory-table-only implementation stays put here (broker stuck at 10). b) The old batch stuck in the listener
//! (offset 10 fetched before the reset) must have its ack invalidated as a whole batch once released -- sampling point: after releasing the old batch, when the first batch of the
//! new queue has entered the listener and not yet acked, the local consumed offset must still be "no record / <=3"; an implementation without the epoch gate
//! would jump to 11 here. c) The queue is truly rebuilt: every batch of 3..14 is **redelivered once** (the unacked 11..14
//! in the old buffer are also invalidated and can only appear as part of the redelivery).
//!
//! S2 "forward skip + recovery" -- timestamp=-1 resets to maxOffset: the offset jumps straight to 10, none of 4..9 is delivered;
//! new messages are then consumed normally and the offset keeps advancing.
//!
//! Usage:
//! ```text
//! cargo run --example live_reset_offset -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{
    mq_key, ConsumerConfig, DefaultMQPushConsumer,
};
use rocketmq_client_remoting::client::mq_client::RegisteredConsumer;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::remoting::protocol::ext_fields::StringMap;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

const MSGS: i64 = 10;
const BACK_TARGET: i64 = 3;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Async version of `poll_until`: use this when the predicate itself needs to send an RPC (`committed`).
async fn poll_until_async<F, Fut>(mut pred: F, secs: u64) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if pred().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_before_async<F, Fut>(mut pred: F, seconds: f64) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs_f64(seconds);
    loop {
        if pred().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Batch-by-batch gated listener: every batch stops at the gate, and the test releases it batch by batch via `release()` -- so the local state
/// between batches can be sampled. S1's key sampling point (old batch released, first batch of the new queue not yet acked) only exists
/// when batches are individually controllable; a one-shot listener would mix the 11 (old batch ack) and the subsequent redelivery
/// into a single instant.
struct SteppingListener {
    state: Mutex<StepState>,
    cv: Condvar,
}

struct StepState {
    batches: Vec<Vec<i64>>,
    released: bool,
}

impl SteppingListener {
    fn new() -> Arc<SteppingListener> {
        Arc::new(SteppingListener {
            state: Mutex::new(StepState {
                batches: Vec::new(),
                released: false,
            }),
            cv: Condvar::new(),
        })
    }

    fn batches(&self) -> Vec<Vec<i64>> {
        lock(&self.state).batches.clone()
    }

    fn offsets(&self) -> Vec<i64> {
        lock(&self.state)
            .batches
            .iter()
            .flatten()
            .copied()
            .collect()
    }

    fn release(&self) {
        lock(&self.state).released = true;
        self.cv.notify_all();
    }

    async fn wait_batch(&self, n: usize, secs: u64) -> bool {
        poll_until(|| self.batches().len() >= n, secs).await
    }
}

impl MessageListenerConcurrently for SteppingListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let mut st = lock(&self.state);
        st.batches.push(msgs.iter().map(|m| m.queue_offset).collect());
        // The cap only guards against deadlock: the normal path is the test calling release() batch by batch; the one that wakes resets the gate (semantically the same as
        // a one-shot event's set() + clear()).
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
        st.released = false;
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

/// Only records arriving queueOffsets: this pass cares about "which ones should not have come", not the message content.
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
            instance_name: format!("rust-live-ro-admin-{stamp}"),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        admin
            .start()
            .await
            .map_err(|e| format!("admin start failed: {e}"))?;
        let producer = DefaultMQProducer::new(&format!("rust-live-ro-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        producer
            .start()
            .await
            .map_err(|e| format!("producer start failed: {e}"))?;

        // Port open != broker registered with the nameServer: poll and wait here
        let deadline = Instant::now() + Duration::from_secs(40);
        let broker_addr = loop {
            if let Ok(info) = admin.fetch_broker_cluster_info().await {
                if !info.broker_addr_table.is_empty() {
                    break info
                        .get_broker_addrs()
                        .first()
                        .cloned()
                        .ok_or_else(|| "cluster info has no broker address".to_string())?;
                }
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
        format!("RustLiveRo{tag}{}", self.stamp)
    }

    async fn create_topic(&self, topic: &str) -> Result<(), String> {
        self.admin
            .create_topic(MixAll::DEFAULT_TOPIC, topic, 1, 0)
            .await
            .map_err(|e| format!("create topic {topic} failed: {e}"))?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

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

    async fn queue_key(&self, topic: &str) -> Result<String, String> {
        self.queues(topic)
            .await
            .first()
            .map(mq_key)
            .ok_or_else(|| format!("route of {topic} has no queue"))
    }

    /// [(mq, maxOffset, committed offset or None)]; offset None means no record on the broker.
    async fn committed(&self, group: &str, topic: &str) -> Vec<(MessageQueue, i64, Option<i64>)> {
        let client = match self.admin.get_mq_client_instance() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        let mut out = Vec::new();
        for mq in self.queues(topic).await {
            let off = self.admin.examine_consumer_offset(group, &mq).await.unwrap_or(None);
            let max_off = client.get_max_offset(&mq, 10_000, None).await.unwrap_or(-1);
            out.push((mq, max_off, off));
        }
        out
    }

    fn consumer(
        &self,
        group: &str,
        role: &str,
        topic: &str,
        batch_size: i32,
        persist_millis: Option<u64>,
    ) -> Result<DefaultMQPushConsumer, String> {
        let cfg = ConsumerConfig {
            consumer_group: group.to_string(),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("rust-live-ro-{role}-{}", self.stamp),
            consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
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

    async fn send(&self, topic: &str, body: &str) -> Result<(), String> {
        let mut msg = Message::new(topic, Some(body.as_bytes()));
        self.producer
            .send(&mut msg, Some(20000), None)
            .await
            .map_err(|e| format!("send {body} failed: {e}"))?;
        Ok(())
    }

    /// Hot-flips the switch + reads it back (`useServerSideResetOffset` is the precondition for the 220 push path).
    async fn set_server_side_reset(&self, value: &str) -> bool {
        let mut props = StringMap::new();
        props.insert("useServerSideResetOffset".to_string(), value.to_string());
        if let Err(e) = self
            .admin
            .update_broker_config(&self.broker_addr, &props, None)
            .await
        {
            println!("  (updateBrokerConfig failed: {e})");
            return false;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        match self.admin.get_broker_config(&self.broker_addr, None).await {
            Ok(cfg) => cfg.get("useServerSideResetOffset") == Some(value),
            Err(e) => {
                println!("  (getBrokerConfig failed: {e})");
                false
            }
        }
    }

    fn shutdown(&self) {
        self.producer.shutdown();
        self.admin.shutdown();
    }
}

fn fmt(rows: &[(MessageQueue, i64, Option<i64>)]) -> String {
    rows.iter()
        .map(|(mq, max, off)| {
            format!(
                "q{}:{}/max{}",
                mq.queue_id,
                off.map(|o| o.to_string()).unwrap_or_else(|| "None".to_string()),
                max
            )
        })
        .collect::<Vec<String>>()
        .join(" ")
}

fn all_committed(rows: &[(MessageQueue, i64, Option<i64>)], want: i64) -> bool {
    !rows.is_empty() && rows.iter().all(|(_, _, o)| *o == Some(want))
}

/// The "consumed offset" in this client's memory; `None` = the queue has no record in the table.
fn local_offset(c: &DefaultMQPushConsumer, topic: &str) -> Option<i64> {
    c.get_consumer_status(Some(topic))
        .into_iter()
        .find(|(mq, _)| mq.queue_id == 0)
        .map(|(_, off)| off)
}

// ---------------------------------------------------------------- S1

async fn s1_backward_reset(fx: &Fixture, ck: &mut Checker) {
    let topic = fx.topic("Back");
    let group = format!("GID_rust_live_ro_back_{}", fx.stamp);
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

    // Precondition: first use a plain listener to make the broker's offset 10 (reset requires the group to have a record on the broker)
    let l0 = RecordingListener::new();
    let c0 = match fx.consumer(&group, "back0", &topic, 1, None) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S1-consumer0", false, &e);
            return;
        }
    };
    c0.set_message_listener_concurrently(l0.clone());
    if let Err(e) = c0.start().await {
        ck.check("S1-start0", false, &format!("{e}"));
        c0.shutdown();
        return;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut t_mid = 0i64;
    for i in 0..MSGS {
        if let Err(e) = fx.send(&topic, &format!("rb-{i}")).await {
            ck.check("S1-send", false, &e);
            c0.shutdown();
            return;
        }
        if i == 2 {
            t_mid = now_millis();
            // Separate the storage time of the 3rd message (offset 3) from the previous three, so the later timestamp reset can reliably hit 3
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    let consumed = poll_until(|| l0.arrivals().len() == MSGS as usize, 30).await;
    ck.check(
        &format!("S1-precondition: consumer consumed {MSGS} messages"),
        consumed,
        &format!("arrivals={:?}", l0.arrivals()),
    );

    let persisted = poll_until_async(
        || async {
            let rows = fx.committed(&group, &topic).await;
            all_committed(&rows, MSGS)
        },
        20,
    )
    .await;
    if !persisted {
        println!(
            "  [WARN] committed={}",
            fmt(&fx.committed(&group, &topic).await)
        );
    }
    ck.check(
        &format!("S1-precondition: broker offset periodically flushed to {MSGS} (reset requires the group to have a record on the broker)"),
        persisted,
        "",
    );
    c0.shutdown();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Main consumer: batch-by-batch gating + periodic flush 60s => inside the window the only path that can change the broker offset is the reset's own persist
    let sink = SteppingListener::new();
    let c = match fx.consumer(&group, "back", &topic, 1, Some(60_000)) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S1-consumer", false, &e);
            return;
        }
    };
    c.set_message_listener_concurrently(sink.clone());
    let t_start = Instant::now();
    if let Err(e) = c.start().await {
        ck.check("S1-start", false, &format!("{e}"));
        c.shutdown();
        return;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    for i in MSGS..MSGS + 5 {
        if let Err(e) = fx.send(&topic, &format!("rb-{i}")).await {
            ck.check("S1-send2", false, &e);
            sink.release();
            c.shutdown();
            return;
        }
    }

    let ready = poll_until(
        || !sink.batches().is_empty() && c.cached_message_count(&key) == 4,
        30,
    )
    .await;
    let arr0 = sink.offsets();
    ck.check(
        "S1-window ready: 1 in flight (offset 10 stuck in the listener) + 4 left in the buffer",
        ready && arr0.first() == Some(&10),
        &format!("arrivals={arr0:?} pending={}", c.cached_message_count(&key)),
    );
    if !ready {
        sink.release();
        c.shutdown();
        return;
    }

    let quiet = poll_until(|| t_start.elapsed() > Duration::from_secs(12), 15).await;
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        "S1-first periodic flush has passed (no periodic write within the next 60s)",
        quiet && all_committed(&rows, MSGS),
        &fmt(&rows),
    );

    // Reset to 3 by timestamp: t_mid falls between the 3rd and 4th messages => getOffsetInQueueByTime hits 3,
    // 3 < consumerOffset(10) and isForce=true => the broker pushes {mq: 3}
    let t0 = Instant::now();
    let table = match fx
        .admin
        .reset_offset_by_timestamp(&topic, &group, t_mid + 500, true, None, false)
        .await
    {
        Ok(t) => t,
        Err(e) => {
            ck.check("S1-reset", false, &format!("{e}"));
            sink.release();
            c.shutdown();
            return;
        }
    };
    let targets: Vec<i64> = table.iter().map(|(_, v)| *v).collect();
    ck.check(
        "S1-the target offset in the 222 response is exactly 3",
        targets == vec![BACK_TARGET],
        &format!("table={targets:?}"),
    );

    let bumped = wait_before_async(
        || async {
            let rows = fx.committed(&group, &topic).await;
            all_committed(&rows, BACK_TARGET)
        },
        2.0,
    )
    .await;
    let elapsed = t0.elapsed();
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        &format!(
            "S1-broker offset becomes {BACK_TARGET} within {:.1}s (the persist carried by the reset path, periodic flush=60s)",
            elapsed.as_secs_f64()
        ),
        bumped,
        &fmt(&rows),
    );

    let off = local_offset(&c, &topic);
    ck.check(
        "S1-after the reset the local table has no old offset (removeOffset; the new offset goes out via the revoke tail)",
        off.is_none(),
        &format!("local_offset={off:?} epoch={}", c.queue_epoch(&key)),
    );

    // Release the old batch: its ack belongs to the already-revoked ProcessQueue and must be invalidated
    sink.release();
    let second = sink.wait_batch(2, 20).await;
    let off = local_offset(&c, &topic);
    let two = sink.batches();
    ck.check(
        &format!(
            "S1-old batch ack invalidated (after release, while the new queue's first batch offset {} is in flight, the local offset has still not passed {BACK_TARGET})",
            if second && two.len() > 1 { two[1][0] } else { -1 }
        ),
        second && off.map_or(true, |o| o <= BACK_TARGET),
        &format!("local_offset={off:?} arrivals={:?}", sink.offsets()),
    );

    // Release batch by batch to walk through the redelivery: each of 3..14 is redelivered once (the 11..14 in the old buffer are also invalidated and can only appear as redelivery)
    let mut expected = vec![10i64];
    expected.extend(BACK_TARGET..MSGS + 5);
    for n in 2..=expected.len() {
        sink.release();
        if n < expected.len() {
            sink.wait_batch(n + 1, 15).await;
        }
    }
    let arr = sink.offsets();
    ck.check(
        &format!("S1-the queue is truly rebuilt: the redelivery sequence is {expected:?}"),
        arr == expected,
        &format!("arrivals={arr:?}"),
    );
    poll_until(|| local_offset(&c, &topic) == Some(MSGS + 5), 10).await;

    let rows = fx.committed(&group, &topic).await;
    ck.check(
        &format!("S1-inside the window only the reset writes to the broker (offset still at {BACK_TARGET}, periodic flush=60s)"),
        all_committed(&rows, BACK_TARGET),
        &fmt(&rows),
    );

    // Shutdown flushes once: the redelivery's ack is the final value (15 = the last one 14 plus 1)
    c.shutdown();
    let final_ok = poll_until_async(
        || async {
            let rows = fx.committed(&group, &topic).await;
            all_committed(&rows, MSGS + 5)
        },
        15,
    )
    .await;
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        &format!(
            "S1-shutdown flush writes the redelivery's ack back to the broker (offset advances to {})",
            MSGS + 5
        ),
        final_ok,
        &fmt(&rows),
    );
    cleanup(fx, &topic, &group).await;
}

// ---------------------------------------------------------------- S2

async fn s2_forward_skip(fx: &Fixture, ck: &mut Checker) {
    let topic = fx.topic("Skip");
    let group = format!("GID_rust_live_ro_skip_{}", fx.stamp);
    if let Err(e) = fx.create_topic(&topic).await {
        ck.check("S2-create topic", false, &e);
        return;
    }

    let sink = SteppingListener::new();
    // Periodic flush uses the default 5s: this scenario first relies on the first flush (10s after start) to create a record for the group on the broker
    // (Broker2Client.resetOffset directly replies SYSTEM_ERROR for groups with queryOffset==-1),
    // then waits for a >5s window to do the reset -- the reset's persist and the periodic write are at different moments, so the discriminator stays clean.
    let c = match fx.consumer(&group, "skip", &topic, 1, None) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S2-consumer", false, &e);
            return;
        }
    };
    c.set_message_listener_concurrently(sink.clone());
    let t_start = Instant::now();
    if let Err(e) = c.start().await {
        ck.check("S2-start", false, &format!("{e}"));
        c.shutdown();
        return;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    for i in 0..MSGS {
        if let Err(e) = fx.send(&topic, &format!("rs-{i}")).await {
            ck.check("S2-send", false, &e);
            sink.release();
            c.shutdown();
            return;
        }
    }

    // Release the first 3 batch by batch: the 4th (offset 3) stays in the listener as the "in-flight batch"
    let mut arranged = false;
    for n in 1..=4 {
        if !sink.wait_batch(n, 30).await {
            break;
        }
        if n < 4 {
            sink.release();
        }
        arranged = n == 4;
    }
    let arr = sink.offsets();
    ck.check(
        "S2-window ready: the first 3 are acked, the 4th is stuck in the listener",
        arranged && arr == vec![0, 1, 2, 3],
        &format!("arrivals={arr:?}"),
    );
    if !arranged {
        sink.release();
        c.shutdown();
        return;
    }

    // Wait for the first flush to create the group record (local consumed offset 3 = the ack of the first three), and leave a >5s quiet window
    let ready = poll_until_async(
        || async {
            if t_start.elapsed() <= Duration::from_secs(12) {
                return false;
            }
            let rows = fx.committed(&group, &topic).await;
            all_committed(&rows, 3)
        },
        20,
    )
    .await;
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        "S2-precondition: the group has a record on the broker (q0:3), and the next periodic write is still more than 5s away",
        ready,
        &fmt(&rows),
    );

    let t0 = Instant::now();
    let table = match fx
        .admin
        .reset_offset_by_timestamp(&topic, &group, -1, true, None, false)
        .await
    {
        Ok(t) => t,
        Err(e) => {
            ck.check("S2-reset", false, &format!("{e}"));
            sink.release();
            c.shutdown();
            return;
        }
    };
    let targets: Vec<i64> = table.iter().map(|(_, v)| *v).collect();
    ck.check(
        &format!("S2-the target offset in the 222 response is exactly maxOffset({MSGS})"),
        targets == vec![MSGS],
        &format!("table={targets:?}"),
    );

    let jumped = wait_before_async(
        || async {
            let rows = fx.committed(&group, &topic).await;
            all_committed(&rows, MSGS)
        },
        2.0,
    )
    .await;
    let elapsed = t0.elapsed();
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        &format!(
            "S2-broker offset jumps forward to {MSGS} within {:.1}s (the persist carried by the reset path)",
            elapsed.as_secs_f64()
        ),
        jumped,
        &fmt(&rows),
    );

    sink.release();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let arr = sink.offsets();
    ck.check(
        &format!("S2-none of the skipped 4..{} is delivered (the in-flight one's ack is also invalidated)", MSGS - 1),
        arr == vec![0, 1, 2, 3],
        &format!("arrivals={arr:?}"),
    );

    if let Err(e) = fx.send(&topic, "rs-new").await {
        ck.check("S2-send new", false, &e);
        c.shutdown();
        return;
    }
    let new_ok = sink.wait_batch(5, 20).await;
    sink.release();
    let arr = sink.offsets();
    ck.check(
        &format!("S2-the rebuilt queue continues from the tail (the new message offset {MSGS} is delivered normally)"),
        new_ok && arr == vec![0, 1, 2, 3, MSGS],
        &format!("arrivals={arr:?}"),
    );

    // release() only makes the listener return; the ack is landed by the consume thread afterwards. Shutting down without waiting for the local offset to really reach 11
    // may make that shutdown persist run before the ack (writing back 10 still) -- this is an assertion race, not a semantic issue.
    poll_until(|| local_offset(&c, &topic) == Some(MSGS + 1), 10).await;
    c.shutdown();
    let final_ok = poll_until_async(
        || async {
            let rows = fx.committed(&group, &topic).await;
            all_committed(&rows, MSGS + 1)
        },
        15,
    )
    .await;
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        &format!(
            "S2-shutdown flush writes the new message's ack back to the broker (offset advances to {})",
            MSGS + 1
        ),
        final_ok,
        &fmt(&rows),
    );
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
            ck.check("cluster liveness", false, &e);
            return ck;
        }
    };
    ck.check("cluster liveness", true, &format!("broker={}", fx.broker_addr));

    let off = fx.set_server_side_reset("false").await;
    let cfg = fx
        .admin
        .get_broker_config(&fx.broker_addr, None)
        .await
        .map(|c| {
            c.get("useServerSideResetOffset")
                .unwrap_or("<missing>")
                .to_string()
        })
        .unwrap_or_else(|_| "<read failed>".to_string());
    ck.check(
        "switch hot-flip: useServerSideResetOffset=false (precondition for the 220 push path)",
        off,
        &format!("read back useServerSideResetOffset={cfg}"),
    );

    s1_backward_reset(&fx, &mut ck).await;
    s2_forward_skip(&fx, &mut ck).await;

    let restored = fx.set_server_side_reset("true").await;
    ck.check("restore useServerSideResetOffset=true", restored, "");

    fx.shutdown();
    ck
}

fn report(ck: &mut Checker) {
    println!("\n== result: {} PASS / {} FAIL ==", ck.passed, ck.failed.len());
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
