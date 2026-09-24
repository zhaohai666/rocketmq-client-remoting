//! 路由刷新周期 / 位点落盘周期 真机验证（Rust 对齐 `cpp/examples/live_scheduled_intervals.cpp`
//! 与 `dotnet/examples/RocketMQ.Examples/LiveScheduledIntervals.cs` 的 I1–I3）。
//!
//! 离线单测（`src/client/mq_client.rs` 的 `mod tests`）只能证明**首跳**落在 initialDelay；
//! 周期本身必须用**真集群**才量得出来：路由刷新要用一个"先不存在、后由 admin 建出来"的
//! topic 当探针 —— 只有周期到了才会去 NameServer 拉，缓存里何时出现它就等于周期。
//! 位点落盘同理，用 `QUERY_CONSUMER_OFFSET` 读 broker 侧的位点当探针。
//!
//! 场景：
//! - I1 路由刷新周期：两个生产者同时 start，`poll_name_server_interval` 分别设 1000ms 与
//!   Java 默认 30000ms；两者都把**尚未创建**的 topic 登记进在用集合（登记不拉取）。
//!   先等 1.5s 让两边首跳（initialDelay 10ms）各落空一次，再建 topic，然后看缓存：
//!   1s 组几秒内拿到新路由，30s 组在同一个时刻**还没有**，间隔与配置同量级（≥20s）。
//! - I2 位点落盘周期：两个消费者（1s / 60s）同订阅一个 1 队列 topic，`CONSUME_FROM_FIRST_OFFSET`，
//!   各消费 3 条后 broker 侧位点仍为 0；首个落盘不早于 Java 的 initialDelay(10s)；
//!   再消费 3 条后 1s 组在一个周期内把 6 落盘，60s 组仍是 3；`shutdown()` 收尾把 6 落盘。
//! - I3 实例级周期转发：facade 上配的周期真的落到了 `MQClientInstanceConfig`
//!   （`route_refresh_interval_millis` / `persist_offset_interval_millis`）。
//!
//! 用法（先按项目记忆里记的 runbook 起本地集群）：
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
const SLOW_POLL_MS: u64 = 30_000; // Java ClientConfig#pollNameServerInterval 默认
const FAST_PERSIST_MS: u64 = 1_000;
const SLOW_PERSIST_MS: u64 = 60_000;
/// Java `scheduleAtFixedRate(persistAllConsumerOffset, 1000 * 10, ...)` 的 initialDelay。
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

// ------------------------------------------------------------------ 计分板

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

// ------------------------------------------------------------------ 消费监听

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

// ------------------------------------------------------------------ 夹具

struct Fixture {
    namesrv: String,
    stamp: String,
    admin: DefaultMQAdminExt,
    /// 集群探活拿到的 broker 地址（删订阅组、拼 MessageQueue 都用它）。
    broker_addr: String,
}

impl Fixture {
    async fn read_offset(&self, group: &str, mq: &MessageQueue) -> Result<i64, String> {
        match self.admin.examine_consumer_offset(group, mq).await {
            Ok(Some(v)) => Ok(v),
            Ok(None) => Ok(0), // 没提交过 = 0（QUERY_NOT_FOUND）
            Err(e) => Err(e.to_string()),
        }
    }
}

async fn topic_route(admin: &DefaultMQAdminExt, topic: &str) -> Result<(String, String), String> {
    let route = admin.examine_topic_route(topic).await.map_err(|e| e.to_string())?;
    let bd = route.broker_datas.first().ok_or_else(|| format!("{topic} 路由里没有 broker"))?;
    let addr = bd.select_broker_addr().ok_or_else(|| format!("{topic} 的 broker 没有地址"))?;
    Ok((bd.broker_name.clone(), addr))
}

/// 轮询 broker 侧位点，直到 `>= want` 或超时（`true` = 等到了）。
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
            eprintln!("fixture 失败: {e}");
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

    // 集群探活（后面删订阅组要用这个 broker 地址）
    let broker_addr = match topic_route(&admin, MixAll::DEFAULT_TOPIC).await {
        Ok((_, addr)) => addr,
        Err(e) => {
            admin.shutdown();
            return Err(format!("集群探活失败: {e}"));
        }
    };
    ck.check("集群探活", true, &format!("broker={broker_addr}"));

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

    // 收尾（失败也要走到）：topic 与订阅组都删干净，避免污染下一轮
    for topic in [&t_poll, &t_persist] {
        match fx.admin.delete_topic(topic, None).await {
            Ok(()) => println!("    (deleteTopic({topic}) OK)"),
            Err(e) => println!("    (deleteTopic({topic}) 失败: {e})"),
        }
    }
    for g in groups {
        match fx.admin.delete_subscription_group(&fx.broker_addr, g, true).await {
            Ok(()) => println!("    (deleteSubscriptionGroup({g}) OK)"),
            Err(e) => println!("    (deleteSubscriptionGroup({g}) 失败: {e})"),
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

    // ---------------- I1 路由刷新周期 ----------------
    let fast = DefaultMQProducer::new(g_poll_fast).map_err(|e| e.to_string())?;
    fast.set_namesrv_addr(&fx.namesrv);
    fast.set_instance_name(&format!("interval-poll-fast-{}", fx.stamp));
    fast.set_poll_name_server_interval_millis(FAST_POLL_MS);
    fast.start().await.map_err(|e| format!("fast producer start: {e}"))?;

    let slow = DefaultMQProducer::new(g_poll_slow).map_err(|e| e.to_string())?;
    slow.set_namesrv_addr(&fx.namesrv);
    slow.set_instance_name(&format!("interval-poll-slow-{}", fx.stamp));
    slow.start().await.map_err(|e| format!("slow producer start: {e}"))?; // 不设周期 = Java 默认 30s

    let fast_client: MQClientInstance = fast.client().ok_or("fast producer 没有实例")?;
    let slow_client: MQClientInstance = slow.client().ok_or("slow producer 没有实例")?;
    let fast_route_ms = fast_client.config().route_refresh_interval_millis;
    let slow_route_ms = slow_client.config().route_refresh_interval_millis;
    ck.check(
        "I3 生产者实例拿到配置的刷新周期（1s 组）",
        fast_route_ms == FAST_POLL_MS,
        &format!("instance.route_refresh_interval_millis={fast_route_ms}"),
    );
    ck.check(
        "I3 生产者实例默认 30s（对照组）",
        slow_route_ms == SLOW_POLL_MS,
        &format!("instance.route_refresh_interval_millis={slow_route_ms}"),
    );

    // 把一个还没创建的 topic 登记进周期刷新集合：两个生产者都不会给它发消息，
    // 所以缓存里何时出现它，只由各自的刷新周期决定。
    fast_client.register_topic_in_use(t_poll);
    slow_client.register_topic_in_use(t_poll);
    ck.check(
        "I1 两个生产者都把 tPoll 登记进在用 topic 集合（登记本身不拉取）",
        fast_client.find_broker_addr_by_topic(t_poll).is_none()
            && slow_client.find_broker_addr_by_topic(t_poll).is_none(),
        "登记后两边缓存都为空",
    );

    // 先让两个实例各自的**首跳**（Java scheduleAtFixedRate 的 initialDelay=10ms）跑完并把
    // 这个还不存在的 topic 拉失败一次，再去建 topic。否则首跳可能落在建 topic 之后：
    // 那一跳对两组都是"第一次拉"，30s 组照样当场拿到路由，两组间隔就退化成一个传输 RTT，
    // 这条对照实验也就失去意义（真机上表现为 30s 组和 1s 组同时命中）。
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let t0 = Instant::now();
    fx.admin
        .create_topic(MixAll::DEFAULT_TOPIC, t_poll, 1, 0)
        .await
        .map_err(|e| format!("createTopic({t_poll}) 失败: {e}"))?;
    ck.check("I1 admin 建 topic 成功", true, &format!("{t_poll} 1 队列"));

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
        "I1 1s 周期组 ≤6s 从 NameServer 拉到新 topic 路由",
        fast_ok,
        &format!("dt={} 周期={FAST_POLL_MS}ms", secs(dt_fast)),
    );
    ck.check(
        "I1 此刻 30s 周期组**还**没拉到（对照：周期决定时机）",
        slow_client.find_broker_addr_by_topic(t_poll).is_none(),
        &format!("dt={} 周期={SLOW_POLL_MS}ms", secs(t0.elapsed())),
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
        "I1 30s 周期组最终也拉到（默认值只是慢，不是坏）",
        slow_ok,
        &format!("dt={} 周期={SLOW_POLL_MS}ms", secs(dt_slow)),
    );
    ck.check(
        "I1 两组间隔与配置同量级（30s 组至少晚 20s）",
        dt_slow.saturating_sub(dt_fast) >= Duration::from_secs(20),
        &format!("fast={} slow={}", secs(dt_fast), secs(dt_slow)),
    );
    fast.shutdown();
    slow.shutdown();

    // ---------------- I2 位点落盘周期 ----------------
    fx.admin
        .create_topic(MixAll::DEFAULT_TOPIC, t_persist, 1, 0)
        .await
        .map_err(|e| format!("createTopic({t_persist}) 失败: {e}"))?;
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
        "I3 消费者实例拿到配置的落盘周期",
        fast_persist_ok && persist_ms == FAST_PERSIST_MS,
        &format!("config={persist_ms}ms(facade)"),
    );

    // 第一批 3 条
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
        "I2 两个消费者都消费到 3 条（尚未 commit / shutdown）",
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
        "I2 消费后 broker 位点还没有立刻被推上去",
        off_fast < 3 && off_slow < 3,
        &format!("fast={off_fast} slow={off_slow} elapsed={}", secs(t_first.elapsed())),
    );

    // 首个落盘：Java 的 initialDelay=10s 到了才写第一笔（周期 1s/60s 此刻都还没到）
    let fast_first = wait_offset_at_least(fx, g_persist_fast, &mq, 3, 20_000).await;
    let dt_first = t_first.elapsed();
    ck.check(
        "I2 1s 组首次落盘发生在 initialDelay(~10s) 之后",
        fast_first && dt_first >= Duration::from_millis(9_500),
        &format!("dt={} 周期={FAST_PERSIST_MS}ms", secs(dt_first)),
    );
    ck.check(
        "I2 首笔落盘不早于 Java 的 initialDelay 10s",
        dt_first >= Duration::from_millis(9_500),
        &format!("dt={} initialDelay={PERSIST_INITIAL_DELAY_MS}ms", secs(dt_first)),
    );

    let slow_first = wait_offset_at_least(fx, g_persist_slow, &mq, 3, 20_000).await;
    let dt_slow_first = t_first.elapsed();
    ck.check(
        "I2 60s 组同样在 ~10s 完成首笔落盘（周期未到，先走 initialDelay）",
        slow_first && dt_slow_first < Duration::from_millis(20_000),
        &format!("dt={} 周期={SLOW_PERSIST_MS}ms", secs(dt_slow_first)),
    );

    // 第二批 3 条：1s 组会在一个周期内把 6 推上去，60s 组离自己的周期还远
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
        "I2 两个消费者都消费到第二批（6 条）",
        got6,
        &format!("fast={} slow={}", fast_sink.count(), slow_sink.count()),
    );

    let t_second = Instant::now();
    let fast_six = wait_offset_at_least(fx, g_persist_fast, &mq, 6, 8_000).await;
    ck.check(
        "I2 1s 组一个周期内把第二批位点推上去",
        fast_six,
        &format!("dt={} 周期={FAST_PERSIST_MS}ms", secs(t_second.elapsed())),
    );

    let off_slow_now = fx.read_offset(g_persist_slow, &mq).await?;
    ck.check(
        "I2 此刻 60s 组仍是 3（周期 60s 远未到，且它确实消费到了 6）",
        slow_sink.count() >= 6 && off_slow_now == 3,
        &format!("broker={off_slow_now} 已消费={} 周期={SLOW_PERSIST_MS}ms", slow_sink.count()),
    );
    ck.check(
        "I2 60s 组的下一次周期还没到（距首笔落盘 < 周期 60s）",
        dt_slow_first + t_second.elapsed() < Duration::from_secs(60),
        &format!("elapsed={}", secs(dt_slow_first + t_second.elapsed())),
    );

    // shutdown() 收尾：Java persistConsumerOffset 在关停时补一笔
    slow_c.shutdown();
    let slow_flushed = wait_offset_at_least(fx, g_persist_slow, &mq, 6, 10_000).await;
    ck.check(
        "I2 60s 组 shutdown() 时把 6 落盘（Java persistConsumerOffset 收尾）",
        slow_flushed,
        &format!("broker={}", fx.read_offset(g_persist_slow, &mq).await?),
    );

    fast_c.shutdown();
    prod.shutdown();
    Ok(())
}

async fn send_body(prod: &DefaultMQProducer, topic: &str, body: &str) -> Result<(), String> {
    let mut msg = Message::new(topic, Some(body.as_bytes()));
    prod.send(&mut msg, None, None).await.map_err(|e| format!("send({body}) 失败: {e}"))?;
    Ok(())
}
