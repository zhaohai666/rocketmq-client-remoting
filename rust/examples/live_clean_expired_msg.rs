//! Live verification of expired-message sweeping and send-back (A0-A5).
//!
//! Offline unit tests (`clean_expired_queue_*` and
//! `send_back_batch_skips_entries_swept_away` in `src/client/consumer.rs`) pin the **criteria** (entry selection / thresholds / caps / the sweep gate /
//! skipping send-back); here we pin two things only a real cluster can show:
//!   A. **the sweep really fires**: a listener must hang without returning past the consumeTimeout minutes, and the sweep thread
//!      sends that message back with sendMessageBack (delayLevel 3) -- along the whole chain the client must neither drop it nor deliver it twice.
//!   B. **the redelivery really reaches the broker**: the pull loop of %RETRY%<group> is independent of the hung consume thread, so the message must
//!      reappear in the local buffer, be delivered a second time, and carry reconsumeTimes=1 (broker-side count).
//!
//! When this path breaks it fails **silently**: the hung message pins the queue offset and the dispatch loop together, with no exception
//! or timeout visible; it can only be inferred from "the message was sent but never arrives a second time". So the full evidence chain
//! of "hang → sweep → redelivery arrives" must be produced on a real cluster; "the client reports no error" proves nothing.
//!
//!
//! Usage:
//! ```text
//! cargo run --example live_clean_expired_msg -- 127.0.0.1:9876
//! ```
//! (about 4 minutes, waiting out two sweep cycles)

use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::message_const::PROPERTY_CONSUME_START_TIMESTAMP;
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn body_of(m: &MessageExt) -> String {
    String::from_utf8_lossy(m.body.as_deref().unwrap_or(&[])).to_string()
}

fn contains_body(msgs: &[MessageExt], body: &str) -> bool {
    msgs.iter().any(|m| body_of(m) == body)
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
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Hangs without returning on the first delivery; after release, returns CONSUME_SUCCESS normally.
///
/// The listener runs on a `spawn_blocking` thread; this blocks with a pure std Condvar,
/// so no tokio runtime thread is occupied.
struct HungListener {
    state: Mutex<HungState>,
    first_entered: Condvar,
    released: Condvar,
}

struct HungState {
    calls: usize,
    released: bool,
    /// [(body, reconsumeTimes, arrival time)]
    arrivals: Vec<(String, i32, i64)>,
}

impl HungListener {
    fn new() -> Arc<HungListener> {
        Arc::new(HungListener {
            state: Mutex::new(HungState {
                calls: 0,
                released: false,
                arrivals: Vec::new(),
            }),
            first_entered: Condvar::new(),
            released: Condvar::new(),
        })
    }

    fn wait_first(&self, secs: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(secs);
        let mut s = lock(&self.state);
        while s.calls == 0 {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (guard, _) = self
                .first_entered
                .wait_timeout(s, deadline - now)
                .unwrap_or_else(|e| e.into_inner());
            s = guard;
        }
        true
    }

    fn release(&self) {
        lock(&self.state).released = true;
        self.released.notify_all();
    }

    fn calls(&self) -> usize {
        lock(&self.state).calls
    }

    /// Arrival records of a given body [(reconsumeTimes, time)].
    fn times(&self, body: &str) -> Vec<(i32, i64)> {
        lock(&self.state)
            .arrivals
            .iter()
            .filter(|(b, _, _)| b == body)
            .map(|(_, n, at)| (*n, *at))
            .collect()
    }
}

impl MessageListenerConcurrently for HungListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let first = {
            let mut s = lock(&self.state);
            s.calls += 1;
            let first = s.calls == 1;
            for m in msgs {
                s.arrivals.push((body_of(m), m.reconsume_times, now_ms()));
            }
            first
        };
        if first {
            let mut s = lock(&self.state);
            self.first_entered.notify_all();
            // Hang window: wait for the sweep to act + the script's release (300s cap as a fallback, in case the script crashes and would deadlock the thread)
            let deadline = Instant::now() + Duration::from_secs(300);
            while !s.released {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                let (guard, _) = self
                    .released
                    .wait_timeout(s, deadline - now)
                    .unwrap_or_else(|e| e.into_inner());
                s = guard;
            }
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

fn broker_route(route: &TopicRouteData) -> Result<(String, String), String> {
    let bd = route
        .broker_datas
        .first()
        .ok_or_else(|| "route has no brokerData".to_string())?;
    let addr = bd
        .select_broker_addr()
        .ok_or_else(|| format!("broker {} has no address", bd.broker_name))?;
    Ok((bd.broker_name.clone(), addr))
}

struct Fixture {
    namesrv: String,
    stamp: u64,
    broker_addr: String,
    producer: DefaultMQProducer,
    admin: MQClientInstance,
}

impl Fixture {
    async fn new(namesrv: &str) -> Result<Fixture, String> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let producer = DefaultMQProducer::new(&format!("rust-live-ce-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        let admin = MQClientInstance::new(
            &format!("rust-live-ce-admin-{stamp}"),
            vec![namesrv.to_string()],
        );
        admin
            .start()
            .await
            .map_err(|e| format!("admin start failed: {e}"))?;
        producer
            .start()
            .await
            .map_err(|e| format!("producer start failed: {e}"))?;
        let route = admin
            .get_topic_route_data(MixAll::DEFAULT_TOPIC)
            .await
            .ok_or_else(|| format!("no route of {} from namesrv", MixAll::DEFAULT_TOPIC))?;
        let (_, broker_addr) = broker_route(&route)?;
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            stamp,
            broker_addr,
            producer,
            admin,
        })
    }

    fn topic(&self) -> String {
        format!("RustLiveCe{}", self.stamp)
    }

    async fn queues(&self, topic: &str) -> Vec<MessageQueue> {
        match self.admin.get_topic_publish_info(topic, false).await {
            Ok(publish) => publish
                .msg_queue_list()
                .into_iter()
                .map(|q| MessageQueue::new(&q.topic, &q.broker_name, q.queue_id))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    async fn create_topic(&self, topic: &str) -> Result<(), String> {
        self.admin
            .create_topic_in_broker(
                &self.broker_addr,
                MixAll::DEFAULT_TOPIC,
                topic,
                1,
                1,
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
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    /// Sum of committed offsets (in this case the business topic has only 1 queue); a missing broker record counts as -1.
    async fn committed_offset(&self, group: &str, topic: &str) -> Result<i64, String> {
        let queues = self.queues(topic).await;
        if queues.is_empty() {
            return Err(format!("route of {topic} has no queue"));
        }
        let mut total = 0i64;
        for mq in queues {
            let off = self
                .admin
                .query_consumer_offset(group, &mq, 5000, Some(&self.broker_addr), false)
                .await
                .map_err(|e| format!("query offset q{} failed: {e}", mq.queue_id))?;
            total += off.unwrap_or(-1);
        }
        Ok(total)
    }

    fn shutdown(&self) {
        self.producer.shutdown();
        self.admin.shutdown();
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

    let topic = fx.topic();
    let group = format!("GID_rust_live_ce_{}", fx.stamp);
    let retry_topic = MixAll::get_retry_topic(&group);
    if let Err(e) = fx.create_topic(&topic).await {
        ck.check("create topic", false, &e);
        fx.shutdown();
        return ck;
    }
    println!("topic={topic} group={group}");

    let listener = HungListener::new();
    let cfg = ConsumerConfig {
        consumer_group: group.clone(),
        name_server_addrs: vec![fx.namesrv.clone()],
        instance_name: format!("rust-live-ce-{}", fx.stamp),
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        consume_timeout: 1,
        ..Default::default()
    };
    let consumer = match DefaultMQPushConsumer::with_config(cfg) {
        Ok(c) => c,
        Err(e) => {
            ck.check("build consumer", false, &format!("{e}"));
            fx.shutdown();
            return ck;
        }
    };
    consumer.set_message_listener_concurrently(listener.clone());
    if let Err(e) = consumer.subscribe(&topic, "*") {
        ck.check("subscribe", false, &format!("{e}"));
        fx.shutdown();
        return ck;
    }
    if let Err(e) = consumer.start().await {
        ck.check("start consumer", false, &format!("{e}"));
        fx.shutdown();
        return ck;
    }

    let body = format!("ce-rust-{}", fx.stamp);

    // ---------- A0 business queue (excluding %RETRY%) ----------
    let mut biz_key = String::new();
    let assigned = poll_until(
        || {
            for k in consumer.assigned_queue_keys() {
                if !k.starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX) {
                    biz_key = k;
                    return true;
                }
            }
            false
        },
        30,
    )
    .await;
    ck.check(
        "A0-business queue assigned (the %RETRY% queue does not count)",
        assigned,
        &format!("key={biz_key}"),
    );
    if !assigned {
        listener.release();
        consumer.shutdown();
        fx.shutdown();
        return ck;
    }

    // ---------- A1 baseline: the first delivery hangs ----------
    let mut msg = Message::new(&topic, Some(body.as_bytes()));
    if let Err(e) = fx.producer.send(&mut msg, Some(20000), None).await {
        ck.check("A1 send", false, &format!("{e}"));
        listener.release();
        consumer.shutdown();
        fx.shutdown();
        return ck;
    }
    let first_ok = listener.wait_first(30);
    ck.check(
        "A1-the first delivery arrives and hangs within 30s",
        first_ok,
        &format!("calls={}", listener.calls()),
    );
    let first = listener.times(&body);
    ck.check(
        "A1-first delivery has reconsumeTimes=0",
        first.first().is_some_and(|(n, _)| *n == 0),
        &format!(
            "times={}",
            first.first().map(|(n, _)| *n).unwrap_or(-1)
        ),
    );
    let entries = consumer.process_queue_entries(&biz_key);
    ck.check(
        "A1-while hung the message stays registered (held by the listener)",
        contains_body(&entries, &body),
        &format!("entries={}", entries.len()),
    );
    let stamp_seen = entries
        .iter()
        .find(|m| body_of(m) == body)
        .and_then(|m| m.get_property(PROPERTY_CONSUME_START_TIMESTAMP))
        .unwrap_or_default();
    ck.check(
        "A1-the registered copy carries this round's CONSUME_START_TIME (the sweep uses it to judge expiry)",
        !stamp_seen.is_empty(),
        &format!("stamp={stamp_seen}"),
    );

    // ---------- A2 sweep hits: the listener is still hung and the registered view no longer has it ----------
    let t0 = Instant::now();
    let swept = poll_until(
        || !contains_body(&consumer.process_queue_entries(&biz_key), &body),
        210,
    )
    .await;
    let elapsed = t0.elapsed().as_secs_f64();
    ck.check(
        "A2-the sweep takes the message away while the listener is still hung (around start+120s)",
        swept,
        &format!("elapsed={elapsed:.3}s calls={}", listener.calls()),
    );
    ck.check(
        "A2-the sweep time is later than one sweep threshold (>60s, ruling out another path acting)",
        elapsed > 60.0,
        &format!("elapsed={elapsed:.3}s"),
    );

    // ---------- A3 the send-back really reached the broker: it shows up in the %RETRY% buffer ----------
    // The hung listener occupies the dispatch loop, so the redelivered message can only sit in the local buffer of the %RETRY% queue.
    let mut retry_key_found = String::new();
    let back = poll_until(
        || {
            for k in consumer.assigned_queue_keys() {
                if !k.starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX) {
                    continue;
                }
                retry_key_found = k.clone();
                if contains_body(&consumer.pending_messages(&k), &body) {
                    return true;
                }
            }
            false
        },
        40,
    )
    .await;
    ck.check(
        &format!("A3-the sent-back message shows up in the local buffer of {retry_topic} (the broker really received sendMessageBack)"),
        back,
        &format!("retryKey={retry_key_found}"),
    );

    // ---------- A4 release: the redelivery is consumed again ----------
    listener.release();
    let second = poll_until(|| listener.times(&body).len() >= 2, 60).await;
    let times = listener.times(&body);
    ck.check(
        "A4-after release it is consumed again (reconsumeTimes=1)",
        second,
        &format!("times={}", times.len()),
    );
    ck.check(
        "A4-the second delivery has reconsumeTimes=1 (broker-side redelivery count)",
        times.len() >= 2 && times[1].0 == 1,
        &format!("times={}", times.get(1).map(|(n, _)| *n).unwrap_or(-1)),
    );
    ck.check(
        "A4-the same body arrives exactly twice in total (one settled send-back, no duplicate delivery)",
        times.len() == 2,
        &format!("times={}", times.len()),
    );
    let gap_secs = if times.len() >= 2 {
        (times[1].1 - times[0].1) / 1000
    } else {
        -1
    };
    ck.check(
        "A4-the second delivery is >60s after the first (not a redelivery caused by the listener itself)",
        gap_secs > 60,
        &format!("gap={gap_secs}s"),
    );

    // ---------- A5 offset wrap-up ----------
    let mut committed = -2i64;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(v) = fx.committed_offset(&group, &topic).await {
            committed = v;
            if v == 1 {
                break;
            }
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
    ck.check(
        "A5-after the hung listener returns the business queue offset advances to 1 (removeMessage includes the swept entry)",
        committed == 1,
        &format!("offset={committed}"),
    );

    // ---------------- Cleanup ----------------
    listener.release();
    consumer.shutdown();
    if let Err(e) = fx.admin.delete_topic_in_broker(&fx.broker_addr, &topic, 5000).await {
        println!("    (deleteTopic({topic}) failed: {e})");
    } else {
        println!("    (deleteTopic({topic}) OK)");
    }
    fx.shutdown();
    ck
}

fn report(ck: &mut Checker) {
    println!(
        "\nCleanExpiredMsg(rust): PASS={} FAIL={}",
        ck.passed,
        ck.failed.len()
    );
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
