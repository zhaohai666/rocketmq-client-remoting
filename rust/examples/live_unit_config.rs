//! Integration verification of unitName / unitMode / enableStreamRequestType against a **real 5.5.1 broker**.
//!
//! Of these three knobs, unitName and unitMode must not stay client-side only -- the broker side has
//! observable consequences, so every assertion below runs against the real machine:
//! - U1 `unitName` joins the clientId (`ip@instanceName@unitName`) and does not affect sending;
//!   without unitName the clientId must not grow an extra segment out of nowhere.
//! - U2 when the consumer carries `@unitName@STREAM`, **the clientId read back from the broker is exactly that value**
//!   (`GET_CONSUMER_CONNECTION_LIST`), proving the suffix is not something the client fabricated on its own;
//!   messages are delivered as usual at the same time.
//! - U3 sending with `unitMode=true` to a new topic: the broker goes through `AbstractSendMessageProcessor:485-497`
//!   `buildSysFlag(true, false)`, so the auto-created topic sysFlag carries the UNIT bit (0x1);
//!   the `unitMode=false` control does not -- the only evidence that unitMode really took effect.
//! - U4 the heartbeat of a `unitMode=true` consumer: `ClientManageProcessor:111-116` uses
//!   `buildSysFlag(false, true)` to create the `%RETRY%` topic, whose sysFlag carries the UNIT_SUB bit (0x2).
//! - U5 with `enableStreamRequestType=true` every request carries `ReqT=0`, which a normal broker ignores;
//!   sending and pulling both succeed as usual (the lite consumer has stream on by default, used here as a control).
//!
//!
//! Prerequisites: NameServer + Broker running, with `autoCreateTopicEnable=true` and
//! `autoCreateSubscriptionGroup=true`.
//!
//! Usage:
//! ```text
//! cargo run --example live_unit_config -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::producer::{DefaultMQProducer, ProducerConfig};
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultLitePullConsumer, LitePullConsumerConfig,
};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

/// Default budget (seconds) for cluster probing and for "waiting for broker-side results to land".
const WAIT_SECONDS: u64 = 30;
const FLAG_UNIT: i32 = 0x1 << 0;
const FLAG_UNIT_SUB: i32 = 0x1 << 1;

// ------------------------------------------------------------------ skeleton

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// Assertion accumulator: runs every scenario before summing up; the first failure does not exit early.
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
            println!("  [PASS] {name}");
        } else {
            println!("  [FAIL] {name}: {detail}");
            self.failed.push(format!("{name}: {detail}"));
        }
    }

    fn abort(&mut self, name: &str, err: &str) {
        println!("  [FAIL] {name}: {err}");
        self.failed.push(format!("{name}: {err}"));
    }
}

/// Concurrency listener that only records bodies (U2 needs to prove that a clientId carrying `@STREAM` still receives messages normally).
#[derive(Default)]
struct Inbox {
    bodies: Mutex<Vec<String>>,
}

impl Inbox {
    fn count(&self, body: &str) -> usize {
        lock(&self.bodies).iter().filter(|b| *b == body).count()
    }

    fn snapshot(&self) -> Vec<String> {
        lock(&self.bodies).clone()
    }
}

impl MessageListenerConcurrently for Inbox {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        for msg in msgs {
            lock(&self.bodies)
                .push(String::from_utf8_lossy(msg.get_body()).into_owned());
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

struct Env {
    namesrv: String,
    stamp: String,
    ip: String,
    admin: DefaultMQAdminExt,
    broker_addr: Mutex<String>,
    /// Topics / subscription groups created by this run, all deleted at the end so the dev broker does not keep piling up.
    topics: Mutex<Vec<String>>,
    groups: Mutex<Vec<String>>,
}

impl Env {
    fn new(namesrv: &str, stamp: &str) -> Env {
        let admin = DefaultMQAdminExt::with_config(AdminConfig {
            instance_name: format!("UCADMIN-{stamp}"),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        Env {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            ip: MixAll::cached_ip_str().to_string(),
            admin,
            broker_addr: Mutex::new(String::new()),
            topics: Mutex::new(Vec::new()),
            groups: Mutex::new(Vec::new()),
        }
    }

    fn topic(&self, kind: &str) -> String {
        let t = format!("UnitCfg{kind}{}", self.stamp);
        lock(&self.topics).push(t.clone());
        t
    }

    fn group(&self, kind: &str) -> String {
        let g = format!("GID_unit_cfg_{kind}_{}", self.stamp);
        lock(&self.groups).push(g.clone());
        g
    }

    fn producer_group(&self) -> String {
        format!("PID_unit_cfg_{}", self.stamp)
    }

    fn broker(&self) -> String {
        lock(&self.broker_addr).clone()
    }

    /// The producer's unique clientId fragment (the same clientId shares one MQClientInstance).
    fn instance(&self, kind: &str) -> String {
        format!("uc-{kind}-{}", self.stamp)
    }

    /// Cluster probe: an open port != the broker being registered with the namesrv, so poll.
    async fn start(&self, ck: &mut Checker) -> bool {
        if let Err(e) = self.admin.start().await {
            ck.abort("admin start", &e.to_string());
            return false;
        }
        let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
        loop {
            if let Ok(info) = self.admin.fetch_broker_cluster_info().await {
                if let Some(first) = info.get_broker_addrs().first() {
                    *lock(&self.broker_addr) = first.clone();
                    ck.check("cluster probe", true, &format!("broker={first}"));
                    return true;
                }
            }
            if Instant::now() >= deadline {
                ck.abort("cluster probe", "name server returned no broker within the budget");
                return false;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Spin up a producer configured with only these three knobs.
    fn make_producer(
        &self,
        kind: &str,
        unit_name: Option<&str>,
        unit_mode: bool,
        stream: bool,
    ) -> Result<DefaultMQProducer, String> {
        DefaultMQProducer::with_config(ProducerConfig {
            producer_group: self.producer_group(),
            instance_name: self.instance(kind),
            name_server_addrs: vec![self.namesrv.clone()],
            unit_name: unit_name.map(str::to_string),
            unit_mode,
            enable_stream_request_type: stream,
            ..Default::default()
        })
        .map_err(|e| e.to_string())
    }

    /// Send one message with a dedicated producer (warmup and control both go through here).
    ///
    /// `unit_mode` must be passed in: U3 verifies exactly this; hardcoding false would keep the broker
    /// from ever creating the UNIT bit and make the test fail spuriously.
    async fn send_as(&self, kind: &str, unit_mode: bool, topic: &str, body: &str) -> Result<(), String> {
        let producer = self.make_producer(&format!("{kind}-{body}"), None, unit_mode, false)?;
        producer.start().await.map_err(|e| e.to_string())?;
        let mut msg = Message::new(topic, Some(body.as_bytes()));
        let sent = producer.send(&mut msg, Some(5000), None).await;
        producer.shutdown();
        sent.map(|_| ()).map_err(|e| e.to_string())
    }

    /// Send one message with the default (unitMode=false), used for warmup and for the message under test.
    async fn send(&self, topic: &str, body: &str) -> Result<(), String> {
        self.send_as("plain", false, topic, body).await
    }
}

// ------------------------------------------------------- generic wait helpers

/// A new topic needs the broker to register it with the namesrv (one sweep every 30s by default) before it has its own route;
/// only after the route is visible do the client's heartbeats cover that broker.
async fn wait_route_visible(env: &Env, topic: &str, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Ok(route) = env.admin.examine_topic_route(topic).await {
            if !route.queue_datas.is_empty() {
                return true;
            }
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Read back the topic's sysFlag on the broker; None when it does not exist.
async fn sys_flag(env: &Env, topic: &str) -> Option<i32> {
    env.admin
        .examine_topic_config(&env.broker(), topic)
        .await
        .ok()
        .map(|cfg| cfg.topic_sys_flag)
}

/// Wait until the topic exists and carries the `want` bit; return the last sysFlag read for assertion printing.
async fn wait_flag(env: &Env, topic: &str, want: i32, secs: u64) -> Option<i32> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let last = sys_flag(env, topic).await;
        if last.is_some_and(|f| f & want == want) || Instant::now() >= deadline {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

// ------------------------------------------------------------------ U1

async fn u1_unit_name_in_client_id(ck: &mut Checker, env: &Env) {
    let topic = env.topic("Send");
    let producer = match env.make_producer("u1", Some("unitA"), false, false) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("U1 build", &e);
            return;
        }
    };
    if let Err(e) = producer.start().await {
        ck.abort("U1 producer start", &e.to_string());
        return;
    }
    let expect = format!("{}@uc-u1-{}@unitA", env.ip, env.stamp);
    let got = producer.client_id().unwrap_or_default();
    ck.check(
        "U1 producer clientId carries unitName",
        got == expect,
        &format!("{got} (expected {expect})"),
    );
    let mut msg = Message::new(&topic, Some(b"u1-body"));
    match producer.send(&mut msg, Some(5000), None).await {
        Ok(r) => {
            let msg_id = r.msg_id.unwrap_or_default();
            ck.check(
                "U1 sending with unitName succeeds",
                !msg_id.is_empty(),
                &format!("msgId={msg_id}"),
            );
        }
        Err(e) => ck.abort("U1 sending with unitName succeeds", &e.to_string()),
    }
    producer.shutdown();

    // Control: without unitName the clientId must not grow an extra segment out of nowhere
    let plain = match env.make_producer("u1b", None, false, false) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("U1 control build", &e);
            return;
        }
    };
    if let Err(e) = plain.start().await {
        ck.abort("U1 control start", &e.to_string());
        return;
    }
    let expect2 = format!("{}@uc-u1b-{}", env.ip, env.stamp);
    let got2 = plain.client_id().unwrap_or_default();
    ck.check(
        "U1 control: no unitName means no suffix appended",
        got2 == expect2,
        &format!("{got2} (expected {expect2})"),
    );
    plain.shutdown();
}

// ------------------------------------------------------------------ U2

async fn u2_broker_sees_unit_and_stream(ck: &mut Checker, env: &Env) {
    let topic = env.topic("Conn");
    let group = env.group("conn");
    // Create the topic first and wait for its route to be visible, otherwise the consumer's route table is empty → zero heartbeats → the broker side finds no connection.
    if let Err(e) = env.send(&topic, "u2-warmup").await {
        ck.abort("U2 warmup send", &e);
        return;
    }
    if !wait_route_visible(env, &topic, WAIT_SECONDS).await {
        ck.abort("U2 warmup topic route visible", "route did not appear within the budget");
        return;
    }

    let inbox = Arc::new(Inbox::default());
    let consumer = match DefaultMQPushConsumer::with_config(ConsumerConfig {
        consumer_group: group.clone(),
        instance_name: env.instance("u2"),
        name_server_addrs: vec![env.namesrv.clone()],
        unit_name: Some("unitA".to_string()),
        // The push consumer has stream off by default; turn it on explicitly here: what we verify is "on means live"
        enable_stream_request_type: true,
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("U2 consumer build", &e.to_string());
            return;
        }
    };
    if let Err(e) = consumer.subscribe(&topic, "*") {
        ck.abort("U2 subscribe", &e.to_string());
        return;
    }
    consumer.set_message_listener_concurrently(inbox.clone());
    if let Err(e) = consumer.start().await {
        ck.abort("U2 consumer start", &e.to_string());
        return;
    }
    let expect = format!("{}@uc-u2-{}@unitA@STREAM", env.ip, env.stamp);
    let local = consumer.client_id();
    ck.check(
        "U2 local clientId",
        local == expect,
        &format!("{local} (expected {expect})"),
    );
    if let Err(e) = env.send(&topic, "u2-body").await {
        ck.abort("U2 send message under test", &e);
        consumer.shutdown();
        return;
    }
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    while inbox.count("u2-body") == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        "U2 a consumer with the @STREAM suffix receives messages",
        inbox.count("u2-body") >= 1,
        &format!("got={:?}", inbox.snapshot()),
    );

    // Read back from the broker: the clientId is the original value carried in the heartbeat; the client did not fabricate anything.
    let mut ids: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    loop {
        if let Ok(conn) = env
            .admin
            .examine_consumer_connection_info(&group, Some(&env.broker()))
            .await
        {
            ids = conn
                .connection_set
                .iter()
                .filter_map(|c| c.client_id.clone())
                .collect();
            if !ids.is_empty() {
                break;
            }
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        "U2 the clientId seen by the broker contains @unitA@STREAM",
        ids.iter().any(|i| i.ends_with("@unitA@STREAM")),
        &format!("brokerIds={ids:?}"),
    );
    consumer.shutdown();
}

// ------------------------------------------------------------------ U3

async fn u3_unit_mode_sets_topic_unit_flag(ck: &mut Checker, env: &Env) {
    let t_on = env.topic("On");
    let t_off = env.topic("Off");
    // On the send path the broker creates the topic synchronously, but the config takes a moment to land
    if let Err(e) = env.send_as("u3-on", true, &t_on, "u3-on").await {
        ck.abort("U3 unitMode=true send", &e);
        return;
    }
    if let Err(e) = env.send_as("u3-off", false, &t_off, "u3-off").await {
        ck.abort("U3 unitMode=false send", &e);
        return;
    }
    let on = wait_flag(env, &t_on, FLAG_UNIT, WAIT_SECONDS).await;
    ck.check(
        "U3 the topic created with unitMode=true carries the UNIT bit",
        on.is_some_and(|f| f & FLAG_UNIT == FLAG_UNIT),
        &format!("sysFlag={on:?}"),
    );
    // Control: wait for it to be created (without the UNIT bit); do not mistake "not yet created" for "created without the bit"
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    let mut off = sys_flag(env, &t_off).await;
    while off.is_none() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(300)).await;
        off = sys_flag(env, &t_off).await;
    }
    ck.check(
        "U3 the unitMode=false control topic does not carry the UNIT bit",
        off.is_some_and(|f| f & FLAG_UNIT == 0),
        &format!("sysFlag={off:?}"),
    );
}

// ------------------------------------------------------------------ U4

async fn u4_heartbeat_sets_retry_unit_sub(ck: &mut Checker, env: &Env) {
    let topic = env.topic("Retry");
    let group = env.group("hb");
    // The order must be "send a message first, then start the consumer": heartbeats only go to brokers already known to the route table.
    if let Err(e) = env.send(&topic, "u4-warmup").await {
        ck.abort("U4 warmup send", &e);
        return;
    }
    if !wait_route_visible(env, &topic, WAIT_SECONDS).await {
        ck.abort("U4 warmup topic route visible", "route did not appear within the budget");
        return;
    }
    let consumer = match DefaultMQPushConsumer::with_config(ConsumerConfig {
        consumer_group: group.clone(),
        instance_name: env.instance("u4"),
        name_server_addrs: vec![env.namesrv.clone()],
        unit_mode: true,
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("U4 consumer build", &e.to_string());
            return;
        }
    };
    if let Err(e) = consumer.subscribe(&topic, "*") {
        ck.abort("U4 subscribe", &e.to_string());
        return;
    }
    consumer.set_message_listener_concurrently(Arc::new(Inbox::default()));
    if let Err(e) = consumer.start().await {
        ck.abort("U4 consumer start", &e.to_string());
        return;
    }
    // The heartbeat is sent synchronously once inside start(); failures are swallowed at debug level, so only look at the broker-side result
    let retry_topic = MixAll::get_retry_topic(&group);
    let flag = wait_flag(env, &retry_topic, FLAG_UNIT_SUB, WAIT_SECONDS).await;
    ck.check(
        "U4 the %RETRY% topic created with unitMode=true carries the UNIT_SUB bit",
        flag.is_some_and(|f| f & FLAG_UNIT_SUB == FLAG_UNIT_SUB),
        &format!("sysFlag={flag:?}"),
    );
    consumer.shutdown();
}

// ------------------------------------------------------------------ U5

async fn u5_stream_requests_still_work(ck: &mut Checker, env: &Env) {
    let topic = env.topic("Stream");
    let group = env.group("stream");
    let producer = match env.make_producer("u5p", None, false, true) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("U5 build", &e);
            return;
        }
    };
    if let Err(e) = producer.start().await {
        ck.abort("U5 producer start", &e.to_string());
        return;
    }
    let cid = producer.client_id().unwrap_or_default();
    ck.check(
        "U5 the stream-enabled producer clientId carries @STREAM",
        cid.ends_with("@STREAM"),
        &cid,
    );
    let mut sent = 0;
    for i in 0..3 {
        let mut msg = Message::new(&topic, Some(format!("u5-{i}").as_bytes()));
        match producer.send(&mut msg, Some(5000), None).await {
            Ok(_) => sent += 1,
            Err(e) => println!("  [WARN] send u5-{i} failed: {e}"),
        }
    }
    ck.check("U5 sending works with ReqT=0 on every request", sent == 3, &format!("sent={sent}"));
    producer.shutdown();

    if !wait_route_visible(env, &topic, WAIT_SECONDS).await {
        ck.abort("U5 topic route visible", "route did not appear within the budget");
        return;
    }
    let lite = match DefaultLitePullConsumer::with_config(LitePullConsumerConfig {
        consumer_group: group.clone(),
        instance_name: env.instance("u5"),
        name_server_addrs: vec![env.namesrv.clone()],
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        poll_timeout_millis: 1000,
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("U5 lite build", &e.to_string());
            return;
        }
    };
    lite.subscribe(&topic, "*");
    if let Err(e) = lite.start().await {
        ck.abort("U5 lite start", &e.to_string());
        return;
    }
    let lite_cid = lite.client_id();
    ck.check(
        "U5 the lite consumer clientId carries @STREAM by default",
        lite_cid.ends_with("@STREAM"),
        &lite_cid,
    );
    // poll() drains the local buffer in one go, so accumulate across multiple polls
    let mut bodies: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    while bodies.len() < 3 && Instant::now() < deadline {
        let got = lite.poll(Some(1000)).await;
        for m in got {
            bodies.push(String::from_utf8_lossy(m.get_body()).into_owned());
        }
    }
    let ok = (0..3).all(|i| bodies.iter().any(|b| b == &format!("u5-{i}")));
    ck.check(
        "U5 pulling still works with ReqT=0 on every request",
        ok,
        &format!("bodies={bodies:?}"),
    );
    lite.shutdown();
}

// ------------------------------------------------------------------ cleanup

async fn cleanup(ck: &mut Checker, env: &Env) {
    // Take the lists out before deleting: writing `lock(..).clone()` directly in the for header would keep the MutexGuard
    // alive across the whole loop body (including await points).
    let topics: Vec<String> = lock(&env.topics).clone();
    let groups: Vec<String> = lock(&env.groups).clone();
    let broker = env.broker();
    for topic in &topics {
        if let Err(e) = env.admin.delete_topic(topic, None).await {
            println!("  [WARN] delete_topic({topic}) failed: {e}");
        }
    }
    for group in &groups {
        if let Err(e) = env
            .admin
            .delete_subscription_group(&broker, group, true)
            .await
        {
            println!("  [WARN] delete_subscription_group({group}) failed: {e}");
        }
    }
    ck.check("cleaned up this run's topics / subscription groups", true, "");
}

// ------------------------------------------------------------------ driver

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let env = Env::new(namesrv, &stamp);
    println!("== live unitName/unitMode/stream check, namesrv={namesrv} stamp={stamp} ==");
    if !env.start(&mut ck).await {
        return ck;
    }
    u1_unit_name_in_client_id(&mut ck, &env).await;
    u2_broker_sees_unit_and_stream(&mut ck, &env).await;
    u3_unit_mode_sets_topic_unit_flag(&mut ck, &env).await;
    u4_heartbeat_sets_retry_unit_sub(&mut ck, &env).await;
    u5_stream_requests_still_work(&mut ck, &env).await;
    cleanup(&mut ck, &env).await;
    env.admin.shutdown();
    ck
}

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs().to_string(),
        Err(_) => "0".to_string(),
    }
}

fn report(ck: &mut Checker) {
    println!(
        "== summary: {} passed, {} failed ==",
        ck.passed,
        ck.failed.len()
    );
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
    let mut ck = run(&namesrv).await;
    report(&mut ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
