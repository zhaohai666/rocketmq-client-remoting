//! [`DefaultLitePullConsumer`] 的 topic 队列集合变更监听对**真实 5.5.1 broker** 的联调验证。
//!
//! 用法（先按 runbook 起本地集群）：
//! ```text
//! cargo run --example live_lite_topic_queue_change -- 127.0.0.1:9876
//! ```
//!
//! 为什么必须真机：比对趟次每趟都现问 nameserver 取订阅队列集合，而普通路由缓存是 30s
//! 才刷一次。假 nameserver 能证比对逻辑，只有真集群能证「现查」。这里把检查周期压到 1s
//! 下限、路由轮询保持默认 30s，再把 topic 真的扩容：过了首查延迟的稳定期里，从 nameserver
//! 报出新队列数到监听器收到回调只该隔一两趟检查（≤5s）；读 30s 缓存的话这个窗口会拖到半
//! 分钟以上。
//!
//! - L1 队列没动 ⇒ 监听器不被打扰
//! - L1b 首查那趟真的跑过 ⇒ 依旧静默（运行中注册的快照不算变化）
//! - L2 扩容 2→4：nameserver 认了之后回调紧跟几趟检查
//! - L3 回调后快照推进 ⇒ 同一套队列不重复回调
//! - L4 缩容 4→2：同样靠现查看到
//! - L5 没建过的 topic：取不到队列算「查不到」，不伪装成缩到 0 队列

use std::collections::BTreeSet;
use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultLitePullConsumer, LitePullConsumerConfig, TopicMessageQueueChangeListener,
};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::message::MessageQueue;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;

const BASE_QUEUES: i32 = 2;
const SCALED_QUEUES: i32 = 4;
/// 检查周期压到下限，好把「每趟现查」和「吃 30s 缓存」在时间上分开。
const CHECK_INTERVAL_MILLIS: i64 = 1000;
/// 后台比对的首查延迟（实现里的默认值）+ 一点余量。
const FIRST_DELAY: Duration = Duration::from_millis(12_000);
/// nameserver 见到变化后允许的最大回调间隔。
const FRESH_WINDOW: Duration = Duration::from_millis(5_000);

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
}

/// 只记回调，不改状态；后台任务也会调它，所以自带锁。
struct Recorder {
    events: Mutex<Vec<(String, Vec<i32>)>>,
}

impl Recorder {
    fn new() -> Arc<Recorder> {
        Arc::new(Recorder { events: Mutex::new(Vec::new()) })
    }

    fn snapshot(&self) -> Vec<(String, Vec<i32>)> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn count(&self) -> usize {
        self.snapshot().len()
    }
}

impl TopicMessageQueueChangeListener for Recorder {
    fn on_changed(&self, topic: &str, message_queues: &[MessageQueue]) {
        let ids: BTreeSet<i32> = message_queues.iter().map(|mq| mq.queue_id).collect();
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((topic.to_string(), ids.into_iter().collect()));
    }
}

fn stamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

fn seq(n: i32) -> Vec<i32> {
    (0..n).collect()
}

fn guard<T>(v: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    v.lock().unwrap_or_else(|e| e.into_inner())
}

/// 现查队列集合，直到报出 `want` 个为止；返回等待时长，超时返回 None。
async fn wait_queue_num(
    consumer: &DefaultLitePullConsumer,
    topic: &str,
    want: usize,
    timeout: Duration,
) -> Option<Duration> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(queues) = consumer.fetch_message_queues(topic).await {
            if queues.len() == want {
                return Some(start.elapsed());
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    None
}

/// 等监听器记到 `want` 条回调；返回等待时长，超时返回 None。
async fn wait_events(rec: &Recorder, want: usize, timeout: Duration) -> Option<Duration> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if rec.count() >= want {
            return Some(start.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

/// 对同一 topic 再下发一次建 topic 请求，把读写队列数改成 `queues`。
async fn scale_topic(admin: &MQClientInstance, broker_addr: &str, topic: &str, queues: i32) {
    let _ = admin
        .create_topic_in_broker(
            broker_addr,
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
        .await;
}

fn broker_addr_of(route: &TopicRouteData) -> Option<String> {
    route.broker_datas.first()?.select_broker_addr()
}

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let s = stamp();
    let topic = format!("RustLiteQcLive{s}");
    let group = format!("rust-lite-qc-{s}");
    let ghost = format!("RustLiteQcGhost{s}");
    println!("namesrv={namesrv} topic={topic} group={group}");

    let admin = MQClientInstance::new(&format!("rust-lite-qc-admin-{s}"), vec![namesrv.to_string()]);
    if let Err(e) = admin.start().await {
        ck.check("admin start", false, &e.to_string());
        return ck;
    }
    let route = match admin.get_topic_route_data(MixAll::DEFAULT_TOPIC).await {
        Some(r) => r,
        None => {
            ck.check("默认 topic 路由", false, "namesrv 没有返回路由");
            return ck;
        }
    };
    let broker_addr = match broker_addr_of(&route) {
        Some(a) => a,
        None => {
            ck.check("broker 地址", false, "路由里没有 broker 地址");
            return ck;
        }
    };

    // ---- 建 topic（2 队列）
    scale_topic(&admin, &broker_addr, &topic, BASE_QUEUES).await;

    let cfg = LitePullConsumerConfig {
        consumer_group: group.clone(),
        name_server_addrs: vec![namesrv.to_string()],
        instance_name: format!("lite-qc-live-{s}"),
        poll_timeout_millis: 1000,
        ..Default::default()
    };
    let consumer = match DefaultLitePullConsumer::with_config(cfg) {
        Ok(c) => c,
        Err(e) => {
            ck.check("构造 lite 消费者", false, &e.to_string());
            return ck;
        }
    };
    consumer.set_topic_metadata_check_interval_millis(CHECK_INTERVAL_MILLIS);
    ck.check(
        "L0 检查周期压到 1s 下限",
        consumer.topic_metadata_check_interval_millis() == CHECK_INTERVAL_MILLIS,
        &consumer.topic_metadata_check_interval_millis().to_string(),
    );
    consumer.subscribe(&topic, "*");
    if let Err(e) = consumer.start().await {
        ck.check("消费者启动", false, &e.to_string());
        return ck;
    }
    let loop_start = Instant::now();

    let visible = wait_queue_num(&consumer, &topic, BASE_QUEUES as usize, Duration::from_secs(30)).await;
    ck.check("L0 路由可见（2 个队列）", visible.is_some(), "超时");

    // 运行中注册 ⇒ 立刻记快照 ⇒ 首轮不该回调
    let rec = Recorder::new();
    let listener: Arc<dyn TopicMessageQueueChangeListener> = rec.clone();
    if let Err(e) = consumer
        .register_topic_message_queue_change_listener(&topic, listener)
        .await
    {
        ck.check("注册队列变更监听", false, &e.to_string());
        consumer.shutdown();
        return ck;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    ck.check(
        "L1 队列没动 ⇒ 静默",
        rec.count() == 0,
        &format!("{:?}", guard(&rec.events)),
    );

    // 等到首查延迟过去，此后只剩 1s 一趟的稳定期
    let elapsed = loop_start.elapsed();
    if elapsed < FIRST_DELAY {
        tokio::time::sleep(FIRST_DELAY - elapsed).await;
    }
    ck.check(
        "L1b 首查那趟真的跑过 ⇒ 依旧静默",
        rec.count() == 0,
        &format!("{:?}", guard(&rec.events)),
    );

    // ---- L2 扩容 2→4
    scale_topic(&admin, &broker_addr, &topic, SCALED_QUEUES).await;
    let ns = wait_queue_num(&consumer, &topic, SCALED_QUEUES as usize, Duration::from_secs(45)).await;
    ck.check("L2a nameserver 报出 4 个队列", ns.is_some(), "超时");
    if ns.is_some() {
        let cb = wait_events(&rec, 1, FRESH_WINDOW).await;
        let got = cb.is_some()
            && rec
                .snapshot()
                .first()
                .map(|(_, ids)| *ids == seq(SCALED_QUEUES))
                .unwrap_or(false);
        ck.check(
            "L2b 比对趟次现查路由（回调紧跟 nameserver，不等 30s 缓存）",
            got,
            &format!("callback={cb:?}, events={:?}", rec.snapshot()),
        );
    }

    // ---- L3 快照推进
    tokio::time::sleep(Duration::from_secs(3)).await;
    ck.check(
        "L3 回调后快照推进 ⇒ 不重复回调",
        rec.count() == 1,
        &format!("events={:?}", rec.snapshot()),
    );

    // ---- L4 缩容 4→2
    scale_topic(&admin, &broker_addr, &topic, BASE_QUEUES).await;
    let ns = wait_queue_num(&consumer, &topic, BASE_QUEUES as usize, Duration::from_secs(45)).await;
    ck.check("L4a nameserver 报回 2 个队列", ns.is_some(), "超时");
    if ns.is_some() {
        let cb = wait_events(&rec, 2, FRESH_WINDOW).await;
        let got = cb.is_some()
            && rec
                .snapshot()
                .get(1)
                .map(|(_, ids)| *ids == seq(BASE_QUEUES))
                .unwrap_or(false);
        ck.check(
            "L4b 缩容同样靠现查路由看到",
            got,
            &format!("callback={cb:?}, events={:?}", rec.snapshot()),
        );
    }

    // ---- L5 未知 topic：空队列集算「查不到」
    let raised = match consumer.fetch_message_queues(&ghost).await {
        Ok(q) => format!("no error, queues={}", q.len()),
        Err(e) => e.to_string(),
    };
    ck.check(
        "L5 未知 topic 取队列抛「查不到」而不是返回空",
        raised.contains("Namesrv return empty") || raised.contains("Can not find"),
        &raised,
    );

    consumer.shutdown();
    admin.shutdown();
    ck
}

fn report(ck: &Checker) {
    println!("== summary: {} passed, {} failed ==", ck.passed, ck.failed.len());
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
    let ck = run(&namesrv).await;
    report(&ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
