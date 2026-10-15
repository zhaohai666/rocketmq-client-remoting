//! Usage (start the local cluster per the recorded runbook first):
//! ```text
//! cargo run --example live_subscribe -- 127.0.0.1:9876
//! ```

use std::env;
use std::future::Future;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

const HEARTBEAT_PERIOD_MS: u128 = 30_000;
const QUEUES: i32 = 4;

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

struct Check {
    passed: u32,
    failed: Vec<String>,
}

impl Check {
    fn new() -> Check {
        Check { passed: 0, failed: Vec::new() }
    }

    fn check(&mut self, name: &str, ok: bool, detail: &str) {
        if ok {
            self.passed += 1;
            println!("  [PASS] {name}  {detail}");
        } else {
            self.failed.push(name.to_string());
            println!("  [FAIL] {name}  {detail}");
        }
    }
}

async fn wait_until_async<F, Fut>(mut pred: F, timeout: Duration, step: Duration) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if pred().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(step).await;
    }
}

struct BodySink {
    items: Mutex<Vec<String>>,
}

impl BodySink {
    fn new() -> Arc<BodySink> {
        Arc::new(BodySink { items: Mutex::new(Vec::new()) })
    }

    fn snapshot(&self) -> Vec<String> {
        lock(&self.items).clone()
    }
}

struct CollectListener {
    sink: Arc<BodySink>,
}

impl MessageListenerConcurrently for CollectListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let mut items = lock(&self.sink.items);
        for m in msgs {
            items.push(String::from_utf8_lossy(m.get_body()).into_owned());
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

/// Cluster probe: take the broker address from the default topic route (request 300 must be sent to an address).
async fn probe_broker(admin: &DefaultMQAdminExt) -> Result<String, String> {
    let route = admin
        .examine_topic_route(MixAll::DEFAULT_TOPIC)
        .await
        .map_err(|e| e.to_string())?;
    let bd = route.broker_datas.first().ok_or("no broker in the TBW102 route")?;
    bd.select_broker_addr().ok_or_else(|| "the TBW102 broker has no address".to_string())
}

async fn who(admin: &DefaultMQAdminExt, broker: &str, topic: &str) -> Vec<String> {
    match admin.query_topic_consume_by_who(broker, topic).await {
        Ok(list) => list,
        Err(e) => {
            println!("    (queryTopicConsumeByWho({topic}) failed: {e})");
            Vec::new()
        }
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let namesrv = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1:9876".to_string());
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    match rt.block_on(run(&namesrv)) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("fixture failed: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(namesrv: &str) -> Result<bool, String> {
    let stamp = now_ms().to_string();
    let t_base = format!("RustSubBaseTopic{stamp}");
    let t_late = format!("RustSubLateTopic{stamp}");
    let group = format!("rust-sub-after-start-{stamp}");
    let p_group = format!("rust-sub-producer-{stamp}");

    let admin = DefaultMQAdminExt::with_config(AdminConfig {
        instance_name: format!("ADMIN-{stamp}"),
        name_server_addrs: vec![namesrv.to_string()],
        timeout_millis: 10_000,
        ..Default::default()
    });
    admin.start().await.map_err(|e| format!("admin start failed: {e}"))?;

    let mut ck = Check::new();
    println!("namesrv = {namesrv}  stamp = {stamp}");

    let broker_addr = match probe_broker(&admin).await {
        Ok(addr) => addr,
        Err(e) => {
            admin.shutdown();
            return Err(format!("cluster probe failed: {e}"));
        }
    };
    ck.check("cluster probe", true, &format!("broker={broker_addr}"));

    for topic in [&t_base, &t_late] {
        admin
            .create_topic(MixAll::DEFAULT_TOPIC, topic, QUEUES, 0)
            .await
            .map_err(|e| format!("createTopic({topic}) failed: {e}"))?;
    }

    let outcome =
        run_scenarios(&mut ck, namesrv, &admin, &broker_addr, &t_base, &t_late, &group, &p_group)
            .await;

    for topic in [&t_base, &t_late] {
        match admin.delete_topic(topic, None).await {
            Ok(()) => println!("    (deleteTopic({topic}) OK)"),
            Err(e) => println!("    (deleteTopic({topic}) failed: {e})"),
        }
    }
    match admin.delete_subscription_group(&broker_addr, &group, true).await {
        Ok(()) => println!("    (deleteSubscriptionGroup({group}) OK)"),
        Err(e) => println!("    (deleteSubscriptionGroup({group}) failed: {e})"),
    }
    admin.shutdown();

    println!("\n== summary: {} passed, {} failed ==", ck.passed, ck.failed.len());
    for f in &ck.failed {
        println!("  FAILED: {f}");
    }
    match outcome {
        Err(e) => Err(e),
        Ok(()) => Ok(ck.failed.is_empty()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_scenarios(
    ck: &mut Check,
    namesrv: &str,
    admin: &DefaultMQAdminExt,
    broker_addr: &str,
    t_base: &str,
    t_late: &str,
    group: &str,
    p_group: &str,
) -> Result<(), String> {
    let sink = BodySink::new();
    let consumer = DefaultMQPushConsumer::with_config(ConsumerConfig {
        consumer_group: group.to_string(),
        name_server_addrs: vec![namesrv.to_string()],
        instance_name: format!("live-subscribe-{group}"),
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET.to_string(),
        ..Default::default()
    })
    .map_err(|e| format!("consumer build failed: {e}"))?;
    consumer.subscribe(t_base, "*").map_err(|e| format!("subscribe({t_base}) failed: {e}"))?;
    consumer.set_message_listener_concurrently(Arc::new(CollectListener { sink: sink.clone() }));
    consumer.start().await.map_err(|e| format!("consumer start failed: {e}"))?;

    // ---------------- S0 positive control ----------------
    let t0 = Instant::now();
    let ok = wait_until_async(
        || async { who(admin, broker_addr, t_base).await.iter().any(|g| g == group) },
        Duration::from_secs(35),
        Duration::from_millis(200),
    )
    .await;
    let got_b = who(admin, broker_addr, t_base).await;
    ck.check(
        "S0-the base topic B has this group registered (request 300 finds it)",
        ok,
        &format!("groupList={got_b:?} elapsed={:.2}s", t0.elapsed().as_secs_f64()),
    );

    // ---------------- S1 negative control ----------------
    let got_l = who(admin, broker_addr, t_late).await;
    ck.check(
        "S1-negative control: the unsubscribed L does not show this group",
        !got_l.iter().any(|g| g == group),
        &format!("groupList={got_l:?}"),
    );

    // ---------------- S2 late subscription takes effect immediately ----------------
    let t1 = Instant::now();
    consumer.subscribe(t_late, "*").map_err(|e| format!("late subscribe({t_late}) failed: {e}"))?;
    let subscribe_elapsed = t1.elapsed();
    // This hop is fire-and-forget (`subscribe` has a sync signature + async RPC, see the comment
    // on `notify_subscription_changed`), so a window far smaller than the heartbeat period is given for it to land.
    let registered = wait_until_async(
        || async { who(admin, broker_addr, t_late).await.iter().any(|g| g == group) },
        Duration::from_secs(5),
        Duration::from_millis(20),
    )
    .await;
    let got_l = who(admin, broker_addr, t_late).await;
    let elapsed = t1.elapsed();
    ck.check(
        "S2-after the late subscription the broker registers this group immediately (request 300 finds it)",
        registered,
        &format!("groupList={got_l:?}"),
    );
    // The heartbeat period defaults to 30s: only the subscription-path heartbeat round could make the registration appear this fast.
    ck.check(
        "S2-the registration takes far less than the 30s heartbeat period (it can only have been pushed by the subscription path)",
        registered && elapsed.as_millis() < HEARTBEAT_PERIOD_MS / 3,
        &format!(
            "subscribe return took={}ms, registration took={}ms",
            subscribe_elapsed.as_millis(),
            elapsed.as_millis()
        ),
    );

    // ---------------- S3 the late subscription really gets consumed ----------------
    let t2 = Instant::now();
    let assigned = wait_until_async(
        || async {
            consumer.assigned_queue_keys().iter().any(|k| k.starts_with(t_late))
        },
        Duration::from_secs(45),
        Duration::from_millis(200),
    )
    .await;
    let keys: Vec<String> = consumer
        .assigned_queue_keys()
        .into_iter()
        .filter(|k| k.starts_with(t_late))
        .collect();
    ck.check(
        "S3-the new topic L enters this instance's assignment set (rebalance took effect)",
        assigned,
        &format!("assigned={keys:?} elapsed={:.2}s", t2.elapsed().as_secs_f64()),
    );

    let producer = DefaultMQProducer::new(p_group).map_err(|e| e.to_string())?;
    producer.set_namesrv_addr(namesrv);
    producer.start().await.map_err(|e| format!("producer start failed: {e}"))?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut msg = Message::new(t_late, Some(b"late-subscribe-me"));
    producer.send(&mut msg, None, None).await.map_err(|e| format!("send failed: {e}"))?;

    let consumed = wait_until_async(
        || async { sink.snapshot().iter().any(|b| b == "late-subscribe-me") },
        Duration::from_secs(30),
        Duration::from_millis(200),
    )
    .await;
    ck.check(
        "S3-the message on the late-subscribed topic really gets consumed",
        consumed,
        &format!("seen={:?}", sink.snapshot()),
    );

    // ---------------- S4 live subscription table ----------------
    consumer.unsubscribe(t_late);
    let live: Vec<String> = consumer.subscriptions().into_iter().map(|s| s.topic).collect();
    ck.check(
        "S4-after unsubscribe this group's subscription set immediately loses L (only the table entry is removed, no heartbeat sent)",
        !live.iter().any(|t| t == t_late) && live.iter().any(|t| t == t_base),
        &format!("live={live:?}"),
    );

    consumer.shutdown();
    producer.shutdown();
    Ok(())
}
