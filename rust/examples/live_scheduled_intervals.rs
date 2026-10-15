//! Live verification of the route-refresh interval / offset-persist interval (covers the three scenarios I1-I3).
//!
//! The offline unit tests (`mod tests` in `src/client/mq_client.rs`) can only prove that the **first hop** lands on the
//! initialDelay; the interval itself can only be measured against a **real cluster**: route refreshing uses a topic that
//! "does not exist at first and is created later by the admin" as the probe -- it is fetched from the NameServer only when the interval
//! elapses, so the moment it appears in the cache equals the interval. Offset persisting likewise uses `QUERY_CONSUMER_OFFSET` to read the broker-side offset as the probe.
//!
//!
//! Usage (start the local cluster per the runbook recorded in project memory first):
//! ```text
//! cargo run --example live_scheduled_intervals -- 127.0.0.1:9876
//! ```

use std::env;
use std::future::Future;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::DefaultMQPushConsumer;
use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

const FAST_POLL_MS: u64 = 1_000;
const SLOW_POLL_MS: u64 = 30_000;
const FAST_PERSIST_MS: u64 = 1_000;
const SLOW_PERSIST_MS: u64 = 60_000;
const PERSIST_INITIAL_DELAY_MS: u64 = 10_000;

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn secs(d: Duration) -> String {
    format!("{:.2}s", d.as_secs_f64())
}

async fn wait_until(mut pred: impl FnMut() -> bool, timeout: Duration, step: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if pred() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(step).await;
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

// ------------------------------------------------------------------ scoreboard

struct Check {
    passed: usize,
    failed: Vec<String>,
    skipped: usize,
}

impl Check {
    fn new() -> Check {
        Check { passed: 0, failed: Vec::new(), skipped: 0 }
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

// ------------------------------------------------------------------ consume listener

struct BodySink {
    items: Mutex<Vec<String>>,
}

impl BodySink {
    fn new() -> Arc<BodySink> {
        Arc::new(BodySink { items: Mutex::new(Vec::new()) })
    }

    fn count(&self) -> usize {
        lock(&self.items).len()
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

// ------------------------------------------------------------------ fixture

struct Fixture {
    namesrv: String,
    stamp: String,
    admin: DefaultMQAdminExt,
    /// The broker address obtained from the cluster probe (used for deleting subscription groups and building MessageQueues).
    broker_addr: String,
}

impl Fixture {
    async fn read_offset(&self, group: &str, mq: &MessageQueue) -> Result<i64, String> {
        match self.admin.examine_consumer_offset(group, mq).await {
            Ok(Some(v)) => Ok(v),
            Ok(None) => Ok(0), // never committed = 0 (QUERY_NOT_FOUND)
            Err(e) => Err(e.to_string()),
        }
    }
}

async fn topic_route(admin: &DefaultMQAdminExt, topic: &str) -> Result<(String, String), String> {
    let route = admin.examine_topic_route(topic).await.map_err(|e| e.to_string())?;
    let bd = route.broker_datas.first().ok_or_else(|| format!("{topic} has no broker in its route"))?;
    let addr = bd.select_broker_addr().ok_or_else(|| format!("{topic}'s broker has no address"))?;
    Ok((bd.broker_name.clone(), addr))
}

/// Polls the broker-side offset until it is `>= want` or the timeout elapses (`true` = it got there).
async fn wait_offset_at_least(
    fx: &Fixture,
    group: &str,
    mq: &MessageQueue,
    want: i64,
    timeout_ms: u64,
) -> bool {
    wait_until_async(
        || async { fx.read_offset(group, mq).await.unwrap_or(-1) >= want },
        Duration::from_millis(timeout_ms),
        Duration::from_millis(200),
    )
    .await
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
    let t_poll = format!("RustIntervalPollTopic{stamp}");
    let t_persist = format!("RustIntervalPersistTopic{stamp}");
    let g_poll_fast = format!("rust-interval-poll-fast-{stamp}");
    let g_poll_slow = format!("rust-interval-poll-slow-{stamp}");
    let g_persist_fast = format!("rust-interval-persist-fast-{stamp}");
    let g_persist_slow = format!("rust-interval-persist-slow-{stamp}");
    let g_producer = format!("rust-interval-producer-{stamp}");

    let admin = DefaultMQAdminExt::with_config(AdminConfig {
        instance_name: format!("ADMIN-{stamp}"),
        name_server_addrs: vec![namesrv.to_string()],
        timeout_millis: 10_000,
        ..Default::default()
    });
    admin.start().await.map_err(|e| format!("admin start failed: {e}"))?;

    let mut ck = Check::new();
    println!("namesrv = {namesrv}  stamp = {stamp}");

    // Cluster probe (the broker address is needed later to delete subscription groups)
    let broker_addr = match topic_route(&admin, MixAll::DEFAULT_TOPIC).await {
        Ok((_, addr)) => addr,
        Err(e) => {
            admin.shutdown();
            return Err(format!("cluster probe failed: {e}"));
        }
    };
    ck.check("cluster probe", true, &format!("broker={broker_addr}"));

    let fx = Fixture {
        namesrv: namesrv.to_string(),
        stamp: stamp.clone(),
        admin,
        broker_addr,
    };
    let groups =
        [&g_poll_fast, &g_poll_slow, &g_persist_fast, &g_persist_slow, &g_producer];
    let outcome =
        run_all(&mut ck, &fx, &t_poll, &t_persist, &groups).await;

    // Wrap-up (runs even on failure): delete the topics and subscription groups cleanly to avoid polluting the next run
    for topic in [&t_poll, &t_persist] {
        match fx.admin.delete_topic(topic, None).await {
            Ok(()) => println!("    (deleteTopic({topic}) OK)"),
            Err(e) => println!("    (deleteTopic({topic}) failed: {e})"),
        }
    }
    for g in groups {
        match fx.admin.delete_subscription_group(&fx.broker_addr, g, true).await {
            Ok(()) => println!("    (deleteSubscriptionGroup({g}) OK)"),
            Err(e) => println!("    (deleteSubscriptionGroup({g}) failed: {e})"),
        }
    }
    fx.admin.shutdown();

    println!(
        "\n== summary: {} passed, {} failed, {} skipped ==",
        ck.passed,
        ck.failed.len(),
        ck.skipped
    );
    for f in &ck.failed {
        println!("  FAILED: {f}");
    }
    match outcome {
        Err(e) => Err(e),
        Ok(()) => Ok(ck.failed.is_empty()),
    }
}

async fn run_all(
    ck: &mut Check,
    fx: &Fixture,
    t_poll: &str,
    t_persist: &str,
    groups: &[&String],
) -> Result<(), String> {
    let g_poll_fast = groups[0].as_str();
    let g_poll_slow = groups[1].as_str();
    let g_persist_fast = groups[2].as_str();
    let g_persist_slow = groups[3].as_str();
    let g_producer = groups[4].as_str();

    // ---------------- I1 route-refresh interval ----------------
    let fast = DefaultMQProducer::new(g_poll_fast).map_err(|e| e.to_string())?;
    fast.set_namesrv_addr(&fx.namesrv);
    fast.set_instance_name(&format!("interval-poll-fast-{}", fx.stamp));
    fast.set_poll_name_server_interval_millis(FAST_POLL_MS);
    fast.start().await.map_err(|e| format!("fast producer start: {e}"))?;

    let slow = DefaultMQProducer::new(g_poll_slow).map_err(|e| e.to_string())?;
    slow.set_namesrv_addr(&fx.namesrv);
    slow.set_instance_name(&format!("interval-poll-slow-{}", fx.stamp));
    slow.start().await.map_err(|e| format!("slow producer start: {e}"))?;

    let fast_client: MQClientInstance = fast.client().ok_or("fast producer has no instance")?;
    let slow_client: MQClientInstance = slow.client().ok_or("slow producer has no instance")?;
    let fast_route_ms = fast_client.config().route_refresh_interval_millis;
    let slow_route_ms = slow_client.config().route_refresh_interval_millis;
    ck.check(
        "I3 the producer instance picked up the configured refresh interval (1s group)",
        fast_route_ms == FAST_POLL_MS,
        &format!("instance.route_refresh_interval_millis={fast_route_ms}"),
    );
    ck.check(
        "I3 the producer instance defaults to 30s (control group)",
        slow_route_ms == SLOW_POLL_MS,
        &format!("instance.route_refresh_interval_millis={slow_route_ms}"),
    );

    // A not-yet-created topic is registered into the periodic refresh set: neither producer sends messages to it,
    // so when it appears in each cache is decided only by that producer's refresh interval.
    fast_client.register_topic_in_use(t_poll);
    slow_client.register_topic_in_use(t_poll);
    ck.check(
        "I1 both producers registered tPoll into the in-use topic set (registering itself pulls nothing)",
        fast_client.find_broker_addr_by_topic(t_poll).is_none()
            && slow_client.find_broker_addr_by_topic(t_poll).is_none(),
        "both caches empty after registering",
    );

    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let t0 = Instant::now();
    fx.admin
        .create_topic(MixAll::DEFAULT_TOPIC, t_poll, 1, 0)
        .await
        .map_err(|e| format!("createTopic({t_poll}) failed: {e}"))?;
    ck.check("I1 admin created the topic", true, &format!("{t_poll} 1 queue"));

    let fast_client_1 = fast_client.clone();
    let t_poll_1 = t_poll.to_string();
    let fast_ok = wait_until(
        || fast_client_1.find_broker_addr_by_topic(&t_poll_1).is_some(),
        Duration::from_millis(6_000),
        Duration::from_millis(100),
    )
    .await;
    let dt_fast = t0.elapsed();
    ck.check(
        "I1 the 1s group pulls the new topic route from the NameServer within 6s",
        fast_ok,
        &format!("dt={} interval={FAST_POLL_MS}ms", secs(dt_fast)),
    );
    ck.check(
        "I1 at this moment the 30s group has **not** pulled it yet (control: the interval decides the timing)",
        slow_client.find_broker_addr_by_topic(t_poll).is_none(),
        &format!("dt={} interval={SLOW_POLL_MS}ms", secs(t0.elapsed())),
    );

    let slow_client_1 = slow_client.clone();
    let t_poll_2 = t_poll.to_string();
    let slow_ok = wait_until(
        || slow_client_1.find_broker_addr_by_topic(&t_poll_2).is_some(),
        Duration::from_millis(40_000),
        Duration::from_millis(500),
    )
    .await;
    let dt_slow = t0.elapsed();
    ck.check(
        "I1 the 30s group eventually pulls it too (a default value is only slow, not broken)",
        slow_ok,
        &format!("dt={} interval={SLOW_POLL_MS}ms", secs(dt_slow)),
    );
    ck.check(
        "I1 the two groups' gaps are of the same magnitude as the configs (the 30s group lags by at least 20s)",
        dt_slow.saturating_sub(dt_fast) >= Duration::from_secs(20),
        &format!("fast={} slow={}", secs(dt_fast), secs(dt_slow)),
    );
    fast.shutdown();
    slow.shutdown();

    // ---------------- I2 offset-persist interval ----------------
    fx.admin
        .create_topic(MixAll::DEFAULT_TOPIC, t_persist, 1, 0)
        .await
        .map_err(|e| format!("createTopic({t_persist}) failed: {e}"))?;
    let (persist_broker_name, _) = topic_route(&fx.admin, t_persist).await?;
    let mq = MessageQueue::new(t_persist, &persist_broker_name, 0);

    let prod = DefaultMQProducer::new(g_producer).map_err(|e| e.to_string())?;
    prod.set_namesrv_addr(&fx.namesrv);
    prod.start().await.map_err(|e| format!("producer start: {e}"))?;

    let fast_sink = BodySink::new();
    let fast_c = DefaultMQPushConsumer::new(g_persist_fast).map_err(|e| e.to_string())?;
    fast_c.set_namesrv_addr(&fx.namesrv);
    fast_c.set_instance_name(&format!("interval-persist-fast-{}", fx.stamp));
    fast_c.set_poll_name_server_interval_millis(2_000);
    fast_c.set_persist_consumer_offset_interval_millis(FAST_PERSIST_MS);
    fast_c.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    fast_c.subscribe(t_persist, "*").map_err(|e| e.to_string())?;
    fast_c.set_message_listener_concurrently(Arc::new(CollectListener { sink: fast_sink.clone() }));
    fast_c.start().await.map_err(|e| format!("fast consumer start: {e}"))?;

    let slow_sink = BodySink::new();
    let slow_c = DefaultMQPushConsumer::new(g_persist_slow).map_err(|e| e.to_string())?;
    slow_c.set_namesrv_addr(&fx.namesrv);
    slow_c.set_instance_name(&format!("interval-persist-slow-{}", fx.stamp));
    slow_c.set_persist_consumer_offset_interval_millis(SLOW_PERSIST_MS);
    slow_c.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    slow_c.subscribe(t_persist, "*").map_err(|e| e.to_string())?;
    slow_c.set_message_listener_concurrently(Arc::new(CollectListener { sink: slow_sink.clone() }));
    slow_c.start().await.map_err(|e| format!("slow consumer start: {e}"))?;

    let persist_ms = fast_c.config().persist_consumer_offset_interval_millis;
    let fast_persist_ok = fast_c
        .client()
        .map(|c| c.config().persist_offset_interval_millis == FAST_PERSIST_MS)
        .unwrap_or(false);
    ck.check(
        "I3 the consumer instance picked up the configured persist interval",
        fast_persist_ok && persist_ms == FAST_PERSIST_MS,
        &format!("config={persist_ms}ms(facade)"),
    );

    // First batch of 3
    for i in 1..=3 {
        send_body(&prod, t_persist, &format!("persist-batch1-{i}")).await?;
    }
    let got3 = wait_until(
        || fast_sink.count() >= 3 && slow_sink.count() >= 3,
        Duration::from_millis(30_000),
        Duration::from_millis(100),
    )
    .await;
    ck.check(
        "I2 both consumers consumed the 3 messages (no commit / shutdown yet)",
        got3,
        &format!("fast={} slow={}", fast_sink.count(), slow_sink.count()),
    );
    if !got3 {
        fast_c.shutdown();
        slow_c.shutdown();
        prod.shutdown();
        return Ok(());
    }

    let t_first = Instant::now();
    let off_fast = fx.read_offset(g_persist_fast, &mq).await?;
    let off_slow = fx.read_offset(g_persist_slow, &mq).await?;
    ck.check(
        "I2 after consuming, the broker offset has not been pushed up immediately",
        off_fast < 3 && off_slow < 3,
        &format!("fast={off_fast} slow={off_slow} elapsed={}", secs(t_first.elapsed())),
    );

    let fast_first = wait_offset_at_least(fx, g_persist_fast, &mq, 3, 20_000).await;
    let dt_first = t_first.elapsed();
    ck.check(
        "I2 the 1s group's first persist happens after the initialDelay(~10s)",
        fast_first && dt_first >= Duration::from_millis(9_500),
        &format!("dt={} interval={FAST_PERSIST_MS}ms", secs(dt_first)),
    );
    ck.check(
        "I2 the first persist is no earlier than the initialDelay 10s",
        dt_first >= Duration::from_millis(9_500),
        &format!("dt={} initialDelay={PERSIST_INITIAL_DELAY_MS}ms", secs(dt_first)),
    );

    let slow_first = wait_offset_at_least(fx, g_persist_slow, &mq, 3, 20_000).await;
    let dt_slow_first = t_first.elapsed();
    ck.check(
        "I2 the 60s group also completes its first persist at ~10s (its interval has not arrived, the initialDelay runs first)",
        slow_first && dt_slow_first < Duration::from_millis(20_000),
        &format!("dt={} interval={SLOW_PERSIST_MS}ms", secs(dt_slow_first)),
    );

    // Second batch of 3: the 1s group pushes 6 up within one interval; the 60s group is still far from its own interval
    for i in 1..=3 {
        send_body(&prod, t_persist, &format!("persist-batch2-{i}")).await?;
    }
    let got6 = wait_until(
        || fast_sink.count() >= 6 && slow_sink.count() >= 6,
        Duration::from_millis(30_000),
        Duration::from_millis(100),
    )
    .await;
    ck.check(
        "I2 both consumers consumed the second batch (6 messages)",
        got6,
        &format!("fast={} slow={}", fast_sink.count(), slow_sink.count()),
    );

    let t_second = Instant::now();
    let fast_six = wait_offset_at_least(fx, g_persist_fast, &mq, 6, 8_000).await;
    ck.check(
        "I2 the 1s group pushes the second batch's offset up within one interval",
        fast_six,
        &format!("dt={} interval={FAST_PERSIST_MS}ms", secs(t_second.elapsed())),
    );

    let off_slow_now = fx.read_offset(g_persist_slow, &mq).await?;
    ck.check(
        "I2 at this moment the 60s group is still 3 (its 60s interval is far off, and it did consume all 6)",
        slow_sink.count() >= 6 && off_slow_now == 3,
        &format!("broker={off_slow_now} consumed={} interval={SLOW_PERSIST_MS}ms", slow_sink.count()),
    );
    ck.check(
        "I2 the 60s group's next interval has not arrived yet (time since the first persist < the 60s interval)",
        dt_slow_first + t_second.elapsed() < Duration::from_secs(60),
        &format!("elapsed={}", secs(dt_slow_first + t_second.elapsed())),
    );

    slow_c.shutdown();
    let slow_flushed = wait_offset_at_least(fx, g_persist_slow, &mq, 6, 10_000).await;
    ck.check(
        "I2 the 60s group persists 6 on shutdown() (the persistConsumerOffset wrap-up)",
        slow_flushed,
        &format!("broker={}", fx.read_offset(g_persist_slow, &mq).await?),
    );

    fast_c.shutdown();
    prod.shutdown();
    Ok(())
}

async fn send_body(prod: &DefaultMQProducer, topic: &str, body: &str) -> Result<(), String> {
    let mut msg = Message::new(topic, Some(body.as_bytes()));
    prod.send(&mut msg, None, None).await.map_err(|e| format!("send({body}) failed: {e}"))?;
    Ok(())
}
