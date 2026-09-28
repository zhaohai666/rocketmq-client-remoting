//! lite-pull **请求码 / broker 开关**（#107）真机验证。
//! 与 `python/verify_lite_pull_code_live.py`、`cpp/examples/live_lite_pull_code.cpp`、
//! dotnet 的对应场景同题、逐条对应。
//!
//! 为什么必须真机：`FLAG_LITE_PULL_MESSAGE(0x10)` + `LITE_PULL_MESSAGE(361)` 这条链在离线
//! 假 broker 上永远是绿的 —— 少了位、码还是 11 时，报文依然是一个完全合法的 pull，
//! 假 broker（和真 broker 的普通 pull 分支）照常回消息。能把它区分出来的只有真 broker 的
//! `litePullMessageEnable` 开关（`PullMessageProcessor:325-331` **只拦 361**）：把开关在
//! 运行时翻成 false（UPDATE_BROKER_CONFIG，无需重启）：
//!
//!   S1 开关默认 true：lite pull 全链路正常（基线）。
//!   S2 开关 false：
//!      S2a 裸 361 请求 → NO_PERMISSION(16) + "…for lite pull consumer is forbidden"；
//!      S2b 同队列同一位点的裸 11 请求 → 照常 SUCCESS 且拿到消息（**对照**：开关只管
//!          lite，普通 pull 不受影响 —— 没有这条腿，S2a 的失败可能只是 broker 坏了）；
//!      S2c lite 消费者安静饿死：消息明明在，poll 一条不来、拉取游标纹丝不动
//!          （旧实现位不置/码为 11，这条腿会收到消息 → 判别器变红）；
//!      S2d push 消费者照常消费（**对照**：整条消费链路没坏）。
//!   S3 开关还原 true：lite pull 立即恢复。
//!
//! 退出前**无条件**把 `litePullMessageEnable` 改回原值（与 live_reset_offset.rs 同款）。
//!
//! 用法：
//! ```text
//! cargo run --example live_lite_pull_code -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultLitePullConsumer, LitePullConsumerConfig,
};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::sysflag::PullSysFlag;
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::protocol::ext_fields::StringMap;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

const CONFIG_KEY: &str = "litePullMessageEnable";
const DENY_REMARK: &str = "for lite pull consumer is forbidden";

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

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// 与 Java `DefaultLitePullConsumerImpl#pullSyncImpl:1058` 的
/// `buildSysFlag(false, block, true, false, /*litePull=*/true)` 对齐。
fn lite_flag() -> i32 {
    PullSysFlag::build_sys_flag(/*commit_offset=*/false, /*suspend=*/false,
                                /*subscription=*/true, /*class_filter=*/false,
                                /*lite_pull=*/true)
}

/// `DefaultMQPullConsumerImpl.pullSyncImpl:248` 的 4 参版本，lite 位必须为 0。
fn classic_flag() -> i32 {
    PullSysFlag::build_sys_flag_basic(/*commit_offset=*/false, /*suspend=*/false,
                                      /*subscription=*/true, /*class_filter=*/false)
}

struct BodySink {
    seen: Mutex<Vec<String>>,
}

impl BodySink {
    fn new() -> Arc<BodySink> {
        Arc::new(BodySink {
            seen: Mutex::new(Vec::new()),
        })
    }

    fn has(&self, body: &str) -> bool {
        lock(&self.seen).iter().any(|b| b == body)
    }

    fn snapshot(&self) -> Vec<String> {
        lock(&self.seen).clone()
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
        let mut seen = lock(&self.sink.seen);
        for m in msgs {
            seen.push(String::from_utf8_lossy(m.get_body()).to_string());
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
            instance_name: format!("rust-lite-code-admin-{stamp}"),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        admin
            .start()
            .await
            .map_err(|e| format!("admin start failed: {e}"))?;
        let producer = DefaultMQProducer::new(&format!("rust-lite-code-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        producer
            .start()
            .await
            .map_err(|e| format!("producer start failed: {e}"))?;

        // 端口开着 != broker 已注册到 nameServer：轮询等它注册。
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
                return Err("nameServer 在预算内没有返回任何 broker".to_string());
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

    async fn create_topic(&self, topic: &str) -> Result<(), String> {
        self.admin
            .create_topic(MixAll::DEFAULT_TOPIC, topic, 1, 0)
            .await
            .map_err(|e| format!("create topic {topic} failed: {e}"))?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    async fn queue0(&self, topic: &str) -> Result<MessageQueue, String> {
        let route = self
            .admin
            .examine_topic_route(topic)
            .await
            .map_err(|e| format!("examine topic route failed: {e}"))?;
        route
            .get_all_message_queue(topic)
            .into_iter()
            .next()
            .map(|k| MessageQueue::new(&k.topic, &k.broker_name, k.queue_id))
            .ok_or_else(|| format!("route of {topic} has no queue"))
    }

    async fn send(&self, topic: &str, body: &str, mq: &MessageQueue) -> Result<(), String> {
        let mut msg = Message::new(topic, Some(body.as_bytes()));
        self.producer
            .send(&mut msg, Some(20000), Some(mq))
            .await
            .map_err(|e| format!("send {body} failed: {e}"))?;
        Ok(())
    }

    async fn read_flag(&self) -> Option<String> {
        match self.admin.get_broker_config(&self.broker_addr, None).await {
            Ok(cfg) => cfg.get(CONFIG_KEY).map(str::to_string),
            Err(e) => {
                println!("  [diag] getBrokerConfig failed: {e}");
                None
            }
        }
    }

    async fn write_flag(&self, value: &str) -> bool {
        let mut props = StringMap::new();
        props.insert(CONFIG_KEY.to_string(), value.to_string());
        if let Err(e) = self
            .admin
            .update_broker_config(&self.broker_addr, &props, None)
            .await
        {
            println!("  [diag] updateBrokerConfig({CONFIG_KEY}={value}) failed: {e}");
            return false;
        }
        true
    }

    fn lite(&self, group: &str, queue: &MessageQueue) -> Result<DefaultLitePullConsumer, String> {
        let cfg = LitePullConsumerConfig {
            consumer_group: group.to_string(),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("rust-lite-code-{group}"),
            poll_timeout_millis: 500,
            // 位点由用例自己看：关掉自动提交，S2c 的「游标纹丝不动」才干净。
            auto_commit: false,
            pull_interval_millis: 200,
            consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
            ..Default::default()
        };
        let c = DefaultLitePullConsumer::with_config(cfg)
            .map_err(|e| format!("lite build failed: {e}"))?;
        c.assign(std::slice::from_ref(queue));
        Ok(c)
    }

    fn shutdown(&self) {
        self.producer.shutdown();
        self.admin.shutdown();
    }
}

/// 给定窗口里反复 poll，返回收到的 body。
async fn poll_for(c: &DefaultLitePullConsumer, window: Duration) -> Vec<String> {
    let mut out = Vec::new();
    let deadline = Instant::now() + window;
    while Instant::now() < deadline {
        out.extend(
            c.poll(Some(300))
                .await
                .iter()
                .map(|m| String::from_utf8_lossy(m.get_body()).to_string()),
        );
    }
    out
}

fn join(v: &[String]) -> String {
    format!("{v:?}")
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

    let topic = format!("RustLiteCodeLive{}", fx.stamp);
    let group1 = format!("GID_RustLiteCode_g1_{}", fx.stamp);
    let group2 = format!("GID_RustLiteCode_g2_{}", fx.stamp);
    let group3 = format!("GID_RustLiteCode_g3_{}", fx.stamp);
    println!("topic={topic} broker={}", fx.broker_addr);

    if let Err(e) = fx.create_topic(&topic).await {
        ck.check("create topic", false, &e);
        fx.shutdown();
        return ck;
    }
    let q0 = match fx.queue0(&topic).await {
        Ok(q) => q,
        Err(e) => {
            ck.check("路由可见：1 条队列", false, &e);
            fx.shutdown();
            return ck;
        }
    };
    ck.check("路由可见：1 条队列", true, &format!("queueId={}", q0.queue_id));

    let mut c1: Option<DefaultLitePullConsumer> = None;
    let mut c2: Option<DefaultLitePullConsumer> = None;
    let mut c3: Option<DefaultLitePullConsumer> = None;
    let mut push: Option<DefaultMQPushConsumer> = None;

    // ---------- R0 开关基线 ----------
    let original = fx.read_flag().await;
    ck.check(
        &format!("R0 能读到 broker 的 {CONFIG_KEY}"),
        original.is_some(),
        &format!("value={:?}", original),
    );
    if original.as_deref() != Some("true") {
        ck.check(
            &format!("R0 已临时打开 {CONFIG_KEY}"),
            fx.write_flag("true").await,
            "",
        );
    }
    if fx.read_flag().await.as_deref() != Some("true") {
        ck.check("R0 开关不在 true，后续检查无意义", false, "");
        fx.shutdown();
        return ck;
    }

    // ---------- S1 基线：开关 true，lite 正常 ----------
    println!("\nS1 开关 true：lite pull 基线");
    if let Err(e) = fx.send(&topic, "lite-code-s1", &q0).await {
        ck.check("S1 pinned send", false, &e);
    }
    match fx.lite(&group1, &q0) {
        Ok(c) => {
            if let Err(e) = c.start().await {
                ck.check("S1 lite start", false, &e.to_string());
            } else {
                let got: Vec<String> = {
                    let deadline = Instant::now() + Duration::from_secs(20);
                    let mut got = Vec::new();
                    while got.is_empty() && Instant::now() < deadline {
                        got.extend(
                            c.poll(Some(500))
                                .await
                                .iter()
                                .map(|m| String::from_utf8_lossy(m.get_body()).to_string()),
                        );
                    }
                    got
                };
                ck.check(
                    "S1 lite 消费者收到消息",
                    got.iter().any(|b| b == "lite-code-s1"),
                    &format!("got={}", join(&got)),
                );
                ck.check(
                    "S1 拉取游标已推进",
                    c.pull_cursor_of(&q0) >= 1,
                    &format!("cursor={}", c.pull_cursor_of(&q0)),
                );
            }
            c1 = Some(c);
        }
        Err(e) => ck.check("S1 lite build", false, &e),
    }

    // ---------- S2 开关 false ----------
    println!("\nS2 运行时关闭 {CONFIG_KEY}（UPDATE_BROKER_CONFIG，不重启 broker）");
    ck.check("S2 开关已改为 false", fx.write_flag("false").await, "");
    let read_back = fx.read_flag().await;
    ck.check(
        "S2 开关读回确认",
        read_back.as_deref() == Some("false"),
        &format!("value={:?}", read_back),
    );

    if let Err(e) = fx.send(&topic, "lite-code-s2", &q0).await {
        ck.check("S2 pinned send", false, &e);
    }

    // S2a：裸 361 → NO_PERMISSION + 固定 remark（走我们自己的客户端 API 选码）
    let client = match fx.admin.get_mq_client_instance() {
        Ok(c) => Some(c),
        Err(e) => {
            ck.check("S2a 取 admin 的 MQClientInstance", false, &e.to_string());
            None
        }
    };
    if let Some(client) = client.as_ref() {
        let lite_result = client
            .pull_message(
                &group1,
                &q0,
                0,
                32,
                lite_flag(),
                0,
                "*",
                0,
                "TAG",
                /*timeout_millis=*/30000,
                /*max_msg_bytes=*/-1,
                /*suspend_timeout_millis=*/15000,
                Some(&fx.broker_addr),
                0,
                None,
            )
            .await;
        match lite_result {
            Ok(_) => ck.check(
                "S2a 裸 361 被开关拒绝（NO_PERMISSION=16）",
                false,
                "竟然 SUCCESS —— lite 位/码没生效",
            ),
            Err(Error::Broker {
                response_code,
                message,
            }) => {
                ck.check(
                    "S2a 裸 361 被开关拒绝（NO_PERMISSION=16）",
                    response_code == 16,
                    &format!("code={response_code} remark={message}"),
                );
                ck.check(
                    "S2a 拒绝理由正是 lite 开关",
                    message.contains(DENY_REMARK),
                    &message,
                );
            }
            Err(e) => ck.check("S2a 裸 361 被开关拒绝（NO_PERMISSION=16）", false, &e.to_string()),
        }

        // S2b：同队列同一位点的裸 11 → 照常拿消息（对照组）
        let classic_result = client
            .pull_message(
                &group1,
                &q0,
                0,
                32,
                classic_flag(),
                0,
                "*",
                0,
                "TAG",
                /*timeout_millis=*/30000,
                /*max_msg_bytes=*/-1,
                /*suspend_timeout_millis=*/15000,
                Some(&fx.broker_addr),
                0,
                None,
            )
            .await;
        match classic_result {
            Ok(r) => {
                let got: Vec<String> = r
                    .msg_found_list
                    .iter()
                    .map(|m| String::from_utf8_lossy(m.get_body()).to_string())
                    .collect();
                ck.check(
                    "S2b 对照组：裸 11 不受开关影响，照常拿消息",
                    got.iter().any(|b| b == "lite-code-s1" || b == "lite-code-s2"),
                    &format!("status={:?} got={}", r.status, join(&got)),
                );
            }
            Err(e) => ck.check("S2b 对照组：裸 11 不受开关影响", false, &e.to_string()),
        }
    }

    // S2c：lite 消费者安静饿死（消息在，但一条不来；游标不动）
    match fx.lite(&group2, &q0) {
        Ok(c) => {
            if let Err(e) = c.start().await {
                ck.check("S2c lite start", false, &e.to_string());
            } else {
                let starved = poll_for(&c, Duration::from_secs(8)).await;
                ck.check(
                    "S2c 开关关闭期间 lite 消费者一条都收不到",
                    starved.is_empty(),
                    &format!("got={}", join(&starved)),
                );
                ck.check(
                    "S2c 拉取游标纹丝不动",
                    c.pull_cursor_of(&q0) == 0,
                    &format!("cursor={}", c.pull_cursor_of(&q0)),
                );
            }
            c2 = Some(c);
        }
        Err(e) => ck.check("S2c lite build", false, &e),
    }

    // S2d：push 消费者照常消费（对照组）
    let sink = BodySink::new();
    let cfg = ConsumerConfig {
        consumer_group: format!("GID_RustLiteCode_push_{}", fx.stamp),
        name_server_addrs: vec![namesrv.to_string()],
        instance_name: format!("rust-lite-code-push-{}", fx.stamp),
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..Default::default()
    };
    match DefaultMQPushConsumer::with_config(cfg) {
        Ok(p) => {
            p.set_message_listener_concurrently(Arc::new(CollectListener { sink: sink.clone() }));
            if let Err(e) = p.subscribe(&topic, "*") {
                ck.check("S2d push subscribe", false, &e.to_string());
            }
            match p.start().await {
                Ok(()) => {
                    if let Err(e) = fx.send(&topic, "lite-code-s2d", &q0).await {
                        ck.check("S2d pinned send", false, &e);
                    }
                    let deadline = Instant::now() + Duration::from_secs(20);
                    while !sink.has("lite-code-s2d") && Instant::now() < deadline {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                    }
                    ck.check(
                        "S2d 对照组：push 消费者照常收到消息（开关只管 lite）",
                        sink.has("lite-code-s2d"),
                        &format!("got={}", join(&sink.snapshot())),
                    );
                }
                Err(e) => ck.check("S2d push start", false, &e.to_string()),
            }
            push = Some(p);
        }
        Err(e) => ck.check("S2d push build", false, &e.to_string()),
    }

    // ---------- S3 还原 ----------
    println!("\nS3 开关还原 true：lite 恢复");
    ck.check("S3 开关已还原", fx.write_flag("true").await, "");
    if let Err(e) = fx.send(&topic, "lite-code-s3", &q0).await {
        ck.check("S3 pinned send", false, &e);
    }
    match fx.lite(&group3, &q0) {
        Ok(c) => {
            if let Err(e) = c.start().await {
                ck.check("S3 lite start", false, &e.to_string());
            } else {
                let got: Vec<String> = {
                    let deadline = Instant::now() + Duration::from_secs(20);
                    let mut got = Vec::new();
                    while got.is_empty() && Instant::now() < deadline {
                        got.extend(
                            c.poll(Some(500))
                                .await
                                .iter()
                                .map(|m| String::from_utf8_lossy(m.get_body()).to_string()),
                        );
                    }
                    got
                };
                ck.check(
                    "S3 还原后 lite 消费者立即恢复",
                    !got.is_empty(),
                    &format!("got={}", join(&got)),
                );
            }
            c3 = Some(c);
        }
        Err(e) => ck.check("S3 lite build", false, &e),
    }

    // ---------- 收尾：消费者 → 还原开关 → fixture ----------
    for c in [&c1, &c2, &c3].into_iter().flatten() {
        c.shutdown();
    }
    if let Some(p) = push.as_ref() {
        p.shutdown();
    }
    if let Some(original) = original.as_deref() {
        let ok = fx.write_flag(original).await;
        println!(
            "\n[restore] {CONFIG_KEY}={original} → {}",
            if ok { "OK" } else { "FAILED" }
        );
    }
    fx.shutdown();
    ck
}

#[tokio::main]
async fn main() -> ExitCode {
    let namesrv = env::args().nth(1).unwrap_or_else(|| "127.0.0.1:9876".to_string());
    let ck = run(&namesrv).await;
    println!("\nLitePullCode(rust): {} passed, {} failed", ck.passed, ck.failed.len());
    for f in &ck.failed {
        println!("  - {f}");
    }
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
