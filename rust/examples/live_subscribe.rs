//! 后置订阅真机验证（对齐 Java `subscribe` 之后的「立即推一轮心跳」）。
//!
//! Java `DefaultMQPushConsumerImpl.subscribe:1265-1275` 只做两件事：
//! `subscriptionInner.put(...)` + `if (this.mQClientFactory != null)
//! this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();` —— 允许 `start()` 之后订阅，
//! 而且**同步**推一轮心跳。观测点是 broker 的 topic→group 表
//! （`ConsumerManager#registerConsumer` 维护，`QUERY_TOPIC_CONSUME_BY_WHO(300)` 读取）：
//! 订阅路径不推心跳的话，表里要等下一个 30s 心跳周期才出现本组。
//!
//! 场景（与 `python/verify_subscribe_live.py` / `cpp/examples/live_subscribe.cpp` /
//! `dotnet/examples/RocketMQ.Examples/LiveSubscribe.cs` 一一对应）：
//! - S0 正对照：`start()` 之后基础 topic B 已登记本组（心跳链路与 300 查询本身是通的）。
//! - S1 负对照：本轮**还没**订阅的 L，300 查不到本组。
//! - S2 后置订阅立即生效：`subscribe(L)` 之后直接查 300(L) → 本组已在表里，
//!   且耗时远小于心跳周期（默认 30s）⇒ 只可能来自订阅路径那一轮心跳
//!   （Python/C++/.NET 是同步推，毫秒级；Rust 是 fire-and-forget，窗口给 5s）。
//! - S3 后置订阅真会被消费：L 进分配集 → 发一条消息 → listener 收到。
//! - S4 活订阅表：`unsubscribe(L)` 后本组订阅集立刻少掉 L。
//!   （Java:1317-1319 只删表项、**不**推心跳，且 broker 的 topicGroupTable 只在整组
//!   无订阅时才清 —— 所以这里不拿 broker 的表当断言。）
//!
//! 用法（先按项目记忆里记的 runbook 起本地集群）：
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

/// Java `ClientConfig#heartbeatBrokerInterval` 默认 30s：登记必须远快于它。
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

/// 集群探活：拿默认 topic 路由里的 broker 地址（300 查询要按地址下发）。
async fn probe_broker(admin: &DefaultMQAdminExt) -> Result<String, String> {
    let route = admin
        .examine_topic_route(MixAll::DEFAULT_TOPIC)
        .await
        .map_err(|e| e.to_string())?;
    let bd = route.broker_datas.first().ok_or("TBW102 路由里没有 broker")?;
    bd.select_broker_addr().ok_or_else(|| "TBW102 的 broker 没有地址".to_string())
}

async fn who(admin: &DefaultMQAdminExt, broker: &str, topic: &str) -> Vec<String> {
    match admin.query_topic_consume_by_who(broker, topic).await {
        Ok(list) => list,
        Err(e) => {
            println!("    (queryTopicConsumeByWho({topic}) 失败: {e})");
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
            eprintln!("fixture 失败: {e}");
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
            return Err(format!("集群探活失败: {e}"));
        }
    };
    ck.check("集群探活", true, &format!("broker={broker_addr}"));

    // 先建 topic：消费者不做默认 topic 兜底（对齐 Java），topic 不存在就拿不到路由。
    for topic in [&t_base, &t_late] {
        admin
            .create_topic(MixAll::DEFAULT_TOPIC, topic, QUEUES, 0)
            .await
            .map_err(|e| format!("createTopic({topic}) 失败: {e}"))?;
    }

    let outcome =
        run_scenarios(&mut ck, namesrv, &admin, &broker_addr, &t_base, &t_late, &group, &p_group)
            .await;

    for topic in [&t_base, &t_late] {
        match admin.delete_topic(topic, None).await {
            Ok(()) => println!("    (deleteTopic({topic}) OK)"),
            Err(e) => println!("    (deleteTopic({topic}) 失败: {e})"),
        }
    }
    match admin.delete_subscription_group(&broker_addr, &group, true).await {
        Ok(()) => println!("    (deleteSubscriptionGroup({group}) OK)"),
        Err(e) => println!("    (deleteSubscriptionGroup({group}) 失败: {e})"),
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
    .map_err(|e| format!("消费者构造失败: {e}"))?;
    consumer.subscribe(t_base, "*").map_err(|e| format!("subscribe({t_base}) 失败: {e}"))?;
    consumer.set_message_listener_concurrently(Arc::new(CollectListener { sink: sink.clone() }));
    consumer.start().await.map_err(|e| format!("消费者 start 失败: {e}"))?;

    // ---------------- S0 正对照 ----------------
    let t0 = Instant::now();
    let ok = wait_until_async(
        || async { who(admin, broker_addr, t_base).await.iter().any(|g| g == group) },
        Duration::from_secs(35),
        Duration::from_millis(200),
    )
    .await;
    let got_b = who(admin, broker_addr, t_base).await;
    ck.check(
        "S0-基础 topic B 已登记本组（300 查得到）",
        ok,
        &format!("groupList={got_b:?} elapsed={:.2}s", t0.elapsed().as_secs_f64()),
    );

    // ---------------- S1 负对照 ----------------
    let got_l = who(admin, broker_addr, t_late).await;
    ck.check(
        "S1-负对照：未订阅的 L 查不到本组",
        !got_l.iter().any(|g| g == group),
        &format!("groupList={got_l:?}"),
    );

    // ---------------- S2 后置订阅立即生效 ----------------
    let t1 = Instant::now();
    consumer.subscribe(t_late, "*").map_err(|e| format!("后置 subscribe({t_late}) 失败: {e}"))?;
    let subscribe_elapsed = t1.elapsed();
    // Rust 这一跳是 fire-and-forget（`subscribe` 同步签名 + 异步 RPC，见
    // `notify_subscription_changed` 的注释），所以给一个远小于心跳周期的窗口等它落地。
    let registered = wait_until_async(
        || async { who(admin, broker_addr, t_late).await.iter().any(|g| g == group) },
        Duration::from_secs(5),
        Duration::from_millis(20),
    )
    .await;
    let got_l = who(admin, broker_addr, t_late).await;
    let elapsed = t1.elapsed();
    ck.check(
        "S2-后置订阅后 broker 立刻登记本组（300 查得到）",
        registered,
        &format!("groupList={got_l:?}"),
    );
    // 心跳周期默认 30s：只有订阅路径那一轮心跳才能让登记这么快出现。
    ck.check(
        "S2-登记耗时远小于 30s 心跳周期（只可能是订阅路径推的）",
        registered && elapsed.as_millis() < HEARTBEAT_PERIOD_MS / 3,
        &format!(
            "subscribe 返回耗时={}ms，登记耗时={}ms",
            subscribe_elapsed.as_millis(),
            elapsed.as_millis()
        ),
    );

    // ---------------- S3 后置订阅真会被消费 ----------------
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
        "S3-新 topic L 进入本实例分配集（rebalance 生效）",
        assigned,
        &format!("assigned={keys:?} elapsed={:.2}s", t2.elapsed().as_secs_f64()),
    );

    let producer = DefaultMQProducer::new(p_group).map_err(|e| e.to_string())?;
    producer.set_namesrv_addr(namesrv);
    producer.start().await.map_err(|e| format!("producer start 失败: {e}"))?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut msg = Message::new(t_late, Some(b"late-subscribe-me"));
    producer.send(&mut msg, None, None).await.map_err(|e| format!("send 失败: {e}"))?;

    let consumed = wait_until_async(
        || async { sink.snapshot().iter().any(|b| b == "late-subscribe-me") },
        Duration::from_secs(30),
        Duration::from_millis(200),
    )
    .await;
    ck.check(
        "S3-后置订阅的 topic 上的消息真的被消费",
        consumed,
        &format!("seen={:?}", sink.snapshot()),
    );

    // ---------------- S4 活订阅表 ----------------
    consumer.unsubscribe(t_late);
    let live: Vec<String> = consumer.subscriptions().into_iter().map(|s| s.topic).collect();
    ck.check(
        "S4-unsubscribe 后本组订阅集立刻少掉 L（只删表项，不发心跳）",
        !live.iter().any(|t| t == t_late) && live.iter().any(|t| t == t_base),
        &format!("live={live:?}"),
    );

    consumer.shutdown();
    producer.shutdown();
    Ok(())
}
