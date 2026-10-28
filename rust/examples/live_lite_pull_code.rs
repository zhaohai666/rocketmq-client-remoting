//! Live verification of the lite-pull **request code / broker switch** (#107).
//!
//! Why a real machine is required: the `FLAG_LITE_PULL_MESSAGE(0x10)` + `LITE_PULL_MESSAGE(361)` chain is always green on an
//! offline fake broker -- with the bit missing or the code still 11, the packet is still a perfectly legal pull,
//! and the fake broker (and a real broker's plain pull branch) replies as usual. The only thing that can tell them apart
//! is a real broker's `litePullMessageEnable` switch (the broker's pull processor **intercepts only 361**): flip the switch
//! to false at runtime (UPDATE_BROKER_CONFIG, no restart needed):
//!
//!   S1 switch defaults to true: the lite pull full chain works (baseline).
//!   S2 switch false:
//!      S2a bare 361 request → NO_PERMISSION(16) + "...for lite pull consumer is forbidden";
//!      S2b bare 11 request on the same queue at the same offset → SUCCESS as usual with messages (**control**: the switch
//!          only governs lite; the plain pull is unaffected -- without this leg, S2a's failure might just be a broken broker);
//!      S2c the lite consumer quietly starves: the messages are there, poll returns none and the pull cursor does not move
//!          (an old implementation leaving the bit unset / using code 11 would receive messages here → the discriminator goes red);
//!      S2d the push consumer keeps consuming (**control**: the whole consume chain is intact).
//!   S3 switch restored to true: lite pull recovers immediately.
//!
//! Before exiting, `litePullMessageEnable` is **unconditionally** restored to its original value (same pattern as live_reset_offset.rs).
//!
//! Usage:
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

fn lite_flag() -> i32 {
    PullSysFlag::build_sys_flag(/*commit_offset=*/false, /*suspend=*/false,
                                /*subscription=*/true, /*class_filter=*/false,
                                /*lite_pull=*/true)
}

/// The 4-argument variant used by the classic pull path; the lite bit must stay 0.
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

        // An open port != the broker is registered with the nameServer: poll until it registers.
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
            // The offset is watched by the case itself: auto-commit is off so S2c's "cursor does not move" stays clean.
            auto_commit: false,
            pull_interval_millis: 200,
            consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
            ..Default::default()
        };
        let c = DefaultLitePullConsumer::with_config(cfg)
            .map_err(|e| format!("lite build failed: {e}"))?;
        c.assign(std::slice::from_ref(queue))
            .map_err(|e| format!("lite assign failed: {e}"))?;
        Ok(c)
    }

    fn shutdown(&self) {
        self.producer.shutdown();
        self.admin.shutdown();
    }
}

/// Polls repeatedly within the given window and returns the received bodies.
///
/// `Err` means a poll refused to answer (the consumer is not running): that is not "received nothing",
/// so the caller must not read an empty list out of it.
async fn poll_for(c: &DefaultLitePullConsumer, window: Duration) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let deadline = Instant::now() + window;
    while Instant::now() < deadline {
        match c.poll(Some(300)).await {
            Ok(batch) => out.extend(
                batch
                    .iter()
                    .map(|m| String::from_utf8_lossy(m.get_body()).to_string()),
            ),
            Err(e) => return Err(format!("lite poll refused: {e}")),
        }
    }
    Ok(out)
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
            ck.check("route visible: 1 queue", false, &e);
            fx.shutdown();
            return ck;
        }
    };
    ck.check("route visible: 1 queue", true, &format!("queueId={}", q0.queue_id));

    let mut c1: Option<DefaultLitePullConsumer> = None;
    let mut c2: Option<DefaultLitePullConsumer> = None;
    let mut c3: Option<DefaultLitePullConsumer> = None;
    let mut push: Option<DefaultMQPushConsumer> = None;

    // ---------- R0 switch baseline ----------
    let original = fx.read_flag().await;
    ck.check(
        &format!("R0 can read the broker's {CONFIG_KEY}"),
        original.is_some(),
        &format!("value={:?}", original),
    );
    if original.as_deref() != Some("true") {
        ck.check(
            &format!("R0 temporarily turned {CONFIG_KEY} on"),
            fx.write_flag("true").await,
            "",
        );
    }
    if fx.read_flag().await.as_deref() != Some("true") {
        ck.check("R0 the switch is not true, later checks are meaningless", false, "");
        fx.shutdown();
        return ck;
    }

    // ---------- S1 baseline: switch true, lite works ----------
    println!("\nS1 switch true: lite pull baseline");
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
                        match c.poll(Some(500)).await {
                            Ok(batch) => got.extend(
                                batch
                                    .iter()
                                    .map(|m| String::from_utf8_lossy(m.get_body()).to_string()),
                            ),
                            Err(e) => {
                                ck.check("S1 lite poll during the drain window", false, &e.to_string());
                                break;
                            }
                        }
                    }
                    got
                };
                ck.check(
                    "S1 the lite consumer received the message",
                    got.iter().any(|b| b == "lite-code-s1"),
                    &format!("got={}", join(&got)),
                );
                ck.check(
                    "S1 the pull cursor has advanced",
                    c.pull_cursor_of(&q0) >= 1,
                    &format!("cursor={}", c.pull_cursor_of(&q0)),
                );
            }
            c1 = Some(c);
        }
        Err(e) => ck.check("S1 lite build", false, &e),
    }

    // ---------- S2 switch false ----------
    println!("\nS2 turning {CONFIG_KEY} off at runtime (UPDATE_BROKER_CONFIG, no broker restart)");
    ck.check("S2 switch set to false", fx.write_flag("false").await, "");
    let read_back = fx.read_flag().await;
    ck.check(
        "S2 switch read-back confirmed",
        read_back.as_deref() == Some("false"),
        &format!("value={:?}", read_back),
    );

    if let Err(e) = fx.send(&topic, "lite-code-s2", &q0).await {
        ck.check("S2 pinned send", false, &e);
    }

    // S2a: bare 361 => NO_PERMISSION + the fixed remark (the code is chosen by our own client API)
    let client = match fx.admin.get_mq_client_instance() {
        Ok(c) => Some(c),
        Err(e) => {
            ck.check("S2a fetch the admin's MQClientInstance", false, &e.to_string());
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
                "S2a bare 361 rejected by the switch (NO_PERMISSION=16)",
                false,
                "returned SUCCESS -- the lite bit/code had no effect",
            ),
            Err(Error::Broker {
                response_code,
                message,
            }) => {
                ck.check(
                    "S2a bare 361 rejected by the switch (NO_PERMISSION=16)",
                    response_code == 16,
                    &format!("code={response_code} remark={message}"),
                );
                ck.check(
                    "S2a the refusal reason is exactly the lite switch",
                    message.contains(DENY_REMARK),
                    &message,
                );
            }
            Err(e) => ck.check("S2a bare 361 rejected by the switch (NO_PERMISSION=16)", false, &e.to_string()),
        }

        // S2b: bare 11 on the same queue at the same offset => messages as usual (control leg)
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
                    "S2b control: bare 11 is unaffected by the switch and gets messages",
                    got.iter().any(|b| b == "lite-code-s1" || b == "lite-code-s2"),
                    &format!("status={:?} got={}", r.status, join(&got)),
                );
            }
            Err(e) => ck.check("S2b control: bare 11 is unaffected by the switch", false, &e.to_string()),
        }
    }

    // S2c: the lite consumer quietly starves (messages exist, none arrive; the cursor does not move)
    match fx.lite(&group2, &q0) {
        Ok(c) => {
            if let Err(e) = c.start().await {
                ck.check("S2c lite start", false, &e.to_string());
            } else {
                let starved = match poll_for(&c, Duration::from_secs(8)).await {
                    Ok(bodies) => Some(bodies),
                    Err(e) => {
                        ck.check("S2c lite poll window", false, &e);
                        None
                    }
                };
                if let Some(starved) = &starved {
                    ck.check(
                        "S2c while the switch is off the lite consumer receives nothing",
                        starved.is_empty(),
                        &format!("got={}", join(starved)),
                    );
                }
                ck.check(
                    "S2c the pull cursor does not move at all",
                    c.pull_cursor_of(&q0) == 0,
                    &format!("cursor={}", c.pull_cursor_of(&q0)),
                );
            }
            c2 = Some(c);
        }
        Err(e) => ck.check("S2c lite build", false, &e),
    }

    // S2d: the push consumer keeps consuming (control leg)
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
                        "S2d control: the push consumer receives messages as usual (the switch governs lite only)",
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

    // ---------- S3 restore ----------
    println!("\nS3 switch restored to true: lite recovers");
    ck.check("S3 switch restored", fx.write_flag("true").await, "");
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
                        match c.poll(Some(500)).await {
                            Ok(batch) => got.extend(
                                batch
                                    .iter()
                                    .map(|m| String::from_utf8_lossy(m.get_body()).to_string()),
                            ),
                            Err(e) => {
                                ck.check("S3 lite poll during the drain window", false, &e.to_string());
                                break;
                            }
                        }
                    }
                    got
                };
                ck.check(
                    "S3 after the restore the lite consumer recovers immediately",
                    !got.is_empty(),
                    &format!("got={}", join(&got)),
                );
            }
            c3 = Some(c);
        }
        Err(e) => ck.check("S3 lite build", false, &e),
    }

    // ---------- wrap-up: consumers -> restore switch -> fixture ----------
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
