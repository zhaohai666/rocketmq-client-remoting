//! Live verification of the subscription-tag offset correction branch.
//!
//! Offline unit tests (`correct_tags_offset_*` and
//! `dispatch_loop_keeps_the_correction_out_until_the_listener_returns` in `src/client/consumer.rs`) pin the **criteria**;
//! here we pin two things only a real cluster can show:
//! - **A the correction really reaches the broker**: the offset is eventually persisted via `UPDATE_CONSUMER_OFFSET`; only a real cluster can
//!   prove that the committed offset on the broker moved forward.
//! - **B it moves forward under zero deliveries**: when the subscription expression never matches, the broker filters by the group's subscription
//!   (the pull request carries the SubscriptionData registered by the heartbeat) → `PULL_RETRY_IMMEDIATELY`
//!   → the client gets `NO_MATCHED_MSG`. Without the correction the offset of this queue would stay at "not committed" forever.
//!
//! Scenarios:
//! - S1 control group: a `TagA` subscription consumes 5 messages normally -- proving the messages really are in the queue, and that
//!   "committed offset == each queue's maxOffset" is exactly where regular consumption lands (rules out a false green in S2).
//! - S2 NO_MATCHED_MSG: a `TagB` subscription (never matches). Assert 0 deliveries to the listener + every queue's committed offset
//!   == that queue's maxOffset.
//! - S3 NO_NEW_MSG: at startup the same consumer automatically picks up `%RETRY%<group>` (that queue is empty) →
//!   `PULL_NOT_FOUND` → `NO_NEW_MSG`. Assert a record with value == maxOffset(0) appears on the broker.
//! - S4 wrap-up: wait one more silent window; the listener still has 0 messages (the correction does not conjure deliveries).
//!
//! Usage:
//! ```text
//! cargo run --example live_correct_tags_offset -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;

const QUEUES: i32 = 4;
const MSGS: usize = 5;
/// First persist hop 10s + period 5s (defaults), plus one extra period of margin.
const PERSIST_WINDOW_SECS: u64 = 45;

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

/// Counts only the arriving messages and tags: this pass cares about "not a single one should arrive", not the message content.
struct Sink {
    arrivals: Mutex<Vec<String>>,
}

impl Sink {
    fn new() -> Arc<Sink> {
        Arc::new(Sink {
            arrivals: Mutex::new(Vec::new()),
        })
    }

    fn arrivals(&self) -> usize {
        lock(&self.arrivals).len()
    }
}

impl MessageListenerConcurrently for Sink {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let mut arrivals = lock(&self.arrivals);
        for m in msgs {
            arrivals.push(m.get_tags().unwrap_or("").to_string());
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
        let producer = DefaultMQProducer::new(&format!("rust-live-cto-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        let admin = MQClientInstance::new(
            &format!("rust-live-cto-admin-{stamp}"),
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
        format!("RustLiveCto{}", self.stamp)
    }

    /// The real queue list of this topic from the route.
    ///
    /// Never assume "however many queues the main topic has" as a general rule: `%RETRY%<group>` is created by the broker
    /// with only 1 queue (this client also queries it via the route). Querying 4 queues would misreport
    /// "only q0 has a record" as a failure.
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
                QUEUES,
                QUEUES,
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

    fn consumer(
        &self,
        group: &str,
        topic: &str,
        expression: &str,
        sink: Arc<Sink>,
        from_first: bool,
    ) -> Result<DefaultMQPushConsumer, String> {
        let cfg = ConsumerConfig {
            consumer_group: group.to_string(),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("live-{group}-{}", self.stamp),
            consume_from_where: if from_first {
                ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string()
            } else {
                ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET.to_string()
            },
            consume_message_batch_max_size: 3,
            ..Default::default()
        };
        let consumer = DefaultMQPushConsumer::with_config(cfg)
            .map_err(|e| format!("build consumer failed: {e}"))?;
        consumer
            .subscribe(topic, expression)
            .map_err(|e| format!("subscribe {topic}/{expression} failed: {e}"))?;
        consumer.set_message_listener_concurrently(sink);
        Ok(consumer)
    }

    /// [(mq, maxOffset, committed)]; committed=None means no record found on the broker.
    async fn offsets(
        &self,
        group: &str,
        topic: &str,
    ) -> Result<Vec<(MessageQueue, i64, Option<i64>)>, String> {
        let queues = self.queues(topic).await;
        if queues.is_empty() {
            return Err(format!("route of {topic} has no queue"));
        }
        let mut out = Vec::new();
        for mq in queues {
            let max_off = self
                .admin
                .get_max_offset(&mq, 5000, Some(&self.broker_addr))
                .await
                .unwrap_or(-1);
            let committed = self
                .admin
                .query_consumer_offset(group, &mq, 5000, Some(&self.broker_addr), false)
                .await
                .unwrap_or(None);
            out.push((mq, max_off, committed));
        }
        Ok(out)
    }

    fn shutdown(&self) {
        self.producer.shutdown();
        self.admin.shutdown();
    }
}

fn fmt(rows: &[(MessageQueue, i64, Option<i64>)]) -> String {
    rows.iter()
        .map(|(mq, max_off, off)| {
            format!(
                "q{}:{}/{}",
                mq.queue_id,
                off.map(|o| o.to_string()).unwrap_or_else(|| "None".to_string()),
                max_off
            )
        })
        .collect::<Vec<String>>()
        .join(" ")
}

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let fx = match Fixture::new(namesrv).await {
        Ok(fx) => fx,
        Err(e) => {
            ck.check("fixture", false, &e);
            return ck;
        }
    };
    let topic = fx.topic();
    let group_ctrl = format!("GID_rust_live_cto_ctrl_{}", fx.stamp);
    let group_test = format!("GID_rust_live_cto_test_{}", fx.stamp);
    if let Err(e) = fx.create_topic(&topic).await {
        ck.check("create topic", false, &e);
        fx.shutdown();
        return ck;
    }
    println!("topic={topic} queues={QUEUES}");

    // ---------- S1 control group ----------
    let ctrl_sink = Sink::new();
    let ctrl = match fx.consumer(&group_ctrl, &topic, "TagA", ctrl_sink.clone(), true) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S1 consumer", false, &e);
            fx.shutdown();
            return ck;
        }
    };
    if let Err(e) = ctrl.start().await {
        ck.check("S1 start", false, &format!("{e}"));
        ctrl.shutdown();
        fx.shutdown();
        return ck;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    for i in 0..MSGS {
        let mut msg = Message::new(&topic, Some(format!("cto-{i}").as_bytes()));
        msg.set_tags("TagA");
        if let Err(e) = fx.producer.send(&mut msg, Some(20000), None).await {
            ck.check("S1 send", false, &format!("{e}"));
            ctrl.shutdown();
            fx.shutdown();
            return ck;
        }
    }
    println!("S1: sent {MSGS} TagA messages, waiting for the control group to consume...");
    let got = poll_until(|| ctrl_sink.arrivals() >= MSGS, 30).await;
    ck.check(
        &format!("S1-the control group (TagA) received all {MSGS} messages -- the messages really are in the queue"),
        got && ctrl_sink.arrivals() == MSGS,
        &format!("arrivals={}", ctrl_sink.arrivals()),
    );
    // Poll the broker-side view (a closure cannot hold the lock across await here, so an explicit loop is used)
    let mut ctrl_rows = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        match fx.offsets(&group_ctrl, &topic).await {
            Ok(rows) => {
                ctrl_rows = rows;
                if !ctrl_rows.is_empty()
                    && ctrl_rows.iter().all(|(_, m, o)| o.is_some() && o == &Some(*m))
                {
                    break;
                }
            }
            Err(e) => println!("  [WARN] read offsets: {e}"),
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        "S1-the control group's committed offset == each queue's maxOffset (numeric convention)",
        !ctrl_rows.is_empty() && ctrl_rows.iter().all(|(_, m, o)| o.is_some() && o == &Some(*m)),
        &fmt(&ctrl_rows),
    );
    ck.check(
        "S1-the control group really pushed messages into the queue (sum of maxOffset > 0)",
        ctrl_rows.iter().map(|(_, m, _)| *m).sum::<i64>() > 0,
        &format!(
            "maxSum={}",
            ctrl_rows.iter().map(|(_, m, _)| *m).sum::<i64>()
        ),
    );
    ctrl.shutdown();

    // ---------- S2 NO_MATCHED_MSG: never-matching subscription, zero deliveries but the offset must move ----------
    let test_sink = Sink::new();
    let test = match fx.consumer(&group_test, &topic, "TagB", test_sink.clone(), false) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S2 consumer", false, &e);
            fx.shutdown();
            return ck;
        }
    };
    if let Err(e) = test.start().await {
        ck.check("S2 start", false, &format!("{e}"));
        fx.shutdown();
        return ck;
    }
    println!("S2: the consumer (TagB, never matches) is started, waiting for the empty-reply correction to persist (first hop 10s + period 5s)...");
    let mut test_rows = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(PERSIST_WINDOW_SECS);
    loop {
        match fx.offsets(&group_test, &topic).await {
            Ok(rows) => {
                test_rows = rows;
                if !test_rows.is_empty()
                    && test_rows.iter().all(|(_, m, o)| o.is_some() && o == &Some(*m))
                {
                    break;
                }
            }
            Err(e) => println!("  [WARN] read offsets: {e}"),
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        "S2-zero deliveries (the listener received nothing)",
        test_sink.arrivals() == 0,
        &format!("arrivals={}", test_sink.arrivals()),
    );
    ck.check(
        "S2-every queue's committed offset == that queue's maxOffset (the empty-reply correction took effect)",
        !test_rows.is_empty() && test_rows.iter().all(|(_, m, o)| o.is_some() && o == &Some(*m)),
        &fmt(&test_rows),
    );
    ck.check(
        "S2-the corrected offset sum == the control group's (the max offsets of the same queues)",
        test_rows.iter().map(|(_, _, o)| o.unwrap_or(-1)).sum::<i64>()
            == ctrl_rows.iter().map(|(_, m, _)| *m).sum::<i64>(),
        &format!(
            "test={} ctrl={}",
            test_rows.iter().map(|(_, _, o)| o.unwrap_or(-1)).sum::<i64>(),
            ctrl_rows.iter().map(|(_, m, _)| *m).sum::<i64>()
        ),
    );

    // ---------- S3 NO_NEW_MSG: even the empty %RETRY%<group> queue must leave an offset record ----------
    let retry_topic = format!("%RETRY%{group_test}");
    println!("S3: waiting for the offset record of {retry_topic} (empty queue NO_NEW_MSG)...");
    let mut retry_rows = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(PERSIST_WINDOW_SECS);
    loop {
        match fx.offsets(&group_test, &retry_topic).await {
            Ok(rows) => {
                retry_rows = rows;
                if !retry_rows.is_empty()
                    && retry_rows.iter().all(|(_, m, o)| o.is_some() && o == &Some(*m))
                {
                    break;
                }
            }
            Err(e) => println!("  [WARN] read retry offsets: {e}"),
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        &format!("S3-an offset record appears on {retry_topic} and equals maxOffset"),
        !retry_rows.is_empty() && retry_rows.iter().all(|(_, m, o)| o.is_some() && o == &Some(*m)),
        &fmt(&retry_rows),
    );
    ck.check(
        "S3-that offset is indeed 0 (nextBeginOffset of the empty queue)",
        !retry_rows.is_empty() && retry_rows.iter().all(|(_, _, o)| o == &Some(0)),
        &format!(
            "offsets={:?}",
            retry_rows.iter().map(|(_, _, o)| *o).collect::<Vec<_>>()
        ),
    );

    // ---------- S4 wrap-up: still zero deliveries within the silent window ----------
    tokio::time::sleep(Duration::from_secs(6)).await;
    let after = test_sink.arrivals();
    ck.check(
        "S4-across the whole run the listener still has 0 messages (the correction does not conjure deliveries)",
        after == 0,
        &format!("arrivals={after}"),
    );

    test.shutdown();
    fx.shutdown();
    ck
}

fn report(ck: &mut Checker) {
    println!(
        "\n== Result: {} PASS / {} FAIL ==",
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
