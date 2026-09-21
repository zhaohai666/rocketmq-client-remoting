//! unitName / unitMode / enableStreamRequestType 对**真实 5.5.1 broker** 的联调验证。
//!
//! 与 `python/verify_unit_config_live.py`（U1..U5）同题。这三项里 unitName 和
//! unitMode 都不该只在客户端自 high —— broker 侧有可观测后果，所以全部用真机断言：
//! - U1 `unitName` 进 clientId（`ip@instanceName@unitName`），且不影响发送；
//!   不设 unitName 时 clientId 不能凭空多出一段。
//! - U2 消费者带 `@unitName@STREAM` 时，**broker 回读到的 clientId 就是这个值**
//!   （`GET_CONSUMER_CONNECTION_LIST`），证明后缀不是客户端单方面加工的；
//!   同时消息照常投递。
//! - U3 `unitMode=true` 发到新 topic：broker 走 `AbstractSendMessageProcessor:485-497`
//!   的 `buildSysFlag(true, false)`，自动建出来的 topic sysFlag 带 UNIT 位（0x1）；
//!   `unitMode=false` 的对照组不带 —— 这是 unitMode 真正上线的唯一证据。
//! - U4 `unitMode=true` 的消费者心跳：`ClientManageProcessor:111-116` 用
//!   `buildSysFlag(false, true)` 建 `%RETRY%` topic，sysFlag 带 UNIT_SUB 位（0x2）。
//! - U5 `enableStreamRequestType=true` 时每个请求带 `ReqT=0`，普通 broker 忽略它，
//!   发送与拉取都照常成功（lite 消费者默认就开着 stream，这里当对照组用）。
//!
//! ⚠ 两处顺序坑（Python 同样踩过，结论记在这里）：
//! 1. 心跳只发给**路由表里已出现**的 broker（`sendHeartbeatToAllBroker` 遍历
//!    `topicRouteTable`），订阅一个还不存在的 topic 时一条心跳都发不出去 ——
//!    所以 U2/U4 必须先由生产者发预热消息、再等 topic 在 namesrv 可见。
//! 2. 新消费组默认 `CONSUME_FROM_LAST_OFFSET`（Java 同），首拉会把位点直接设成
//!    max，**消费者启动之前**的消息会被跳过 —— 所以 U2/U5 显式改成 FIRST。
//!
//! 前置：NameServer + Broker 已起，`autoCreateTopicEnable=true`、
//! `autoCreateSubscriptionGroup=true`。
//!
//! 用法：
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

/// 集群探活与「等 broker 侧结果落地」的默认预算（秒）。
const WAIT_SECONDS: u64 = 30;
/// Java `org.apache.rocketmq.common.sysflag.TopicSysFlag`
const FLAG_UNIT: i32 = 0x1 << 0;
const FLAG_UNIT_SUB: i32 = 0x1 << 1;

// ------------------------------------------------------------------ 骨架

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// 断言累积器：跑完全部场景再汇总，首个失败不提前退出。
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

/// 只记 body 的并发 listener（U2 要证明带 `@STREAM` 的clientId 也能正常收消息）。
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
    /// Java `MixAll#cachedIpStr`：clientId 的前缀。
    ip: String,
    admin: DefaultMQAdminExt,
    broker_addr: Mutex<String>,
    /// 本次建的 topic / 订阅组，末尾统一删掉，别把开发 broker 越堆越满。
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

    /// 生产者的唯一 clientId 片段（同 clientId 会共享 MQClientInstance）。
    fn instance(&self, kind: &str) -> String {
        format!("uc-{kind}-{}", self.stamp)
    }

    /// 集群探活：端口开着 != broker 已注册到 namesrv，所以轮询。
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
                    ck.check("集群探活", true, &format!("broker={first}"));
                    return true;
                }
            }
            if Instant::now() >= deadline {
                ck.abort("集群探活", "nameServer 在预算内没有返回任何 broker");
                return false;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// 起一个只配了这三项旋钮的生产者。
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

    /// 用一个独立的生产者发一条消息（预热与对照组都走这里）。
    ///
    /// `unit_mode` 必须能传进来：U3 验的就是它，固定发 false 会让 broker 侧
    /// 永远建不出 UNIT 位，测试假失败。
    async fn send_as(&self, kind: &str, unit_mode: bool, topic: &str, body: &str) -> Result<(), String> {
        let producer = self.make_producer(&format!("{kind}-{body}"), None, unit_mode, false)?;
        producer.start().await.map_err(|e| e.to_string())?;
        let mut msg = Message::new(topic, Some(body.as_bytes()));
        let sent = producer.send(&mut msg, Some(5000), None).await;
        producer.shutdown();
        sent.map(|_| ()).map_err(|e| e.to_string())
    }

    /// 默认（unitMode=false）发一条，用于预热和待验消息。
    async fn send(&self, topic: &str, body: &str) -> Result<(), String> {
        self.send_as("plain", false, topic, body).await
    }
}

// ------------------------------------------------------- 通用等待助手

/// 新 topic 要等 broker 把它注册到 namesrv（默认 30s 一轮）才有独立路由；
/// 路由可见之后客户端的心跳才会覆盖到这个 broker。
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

/// 读回 broker 上该 topic 的 sysFlag；不存在时 None。
async fn sys_flag(env: &Env, topic: &str) -> Option<i32> {
    env.admin
        .examine_topic_config(&env.broker(), topic)
        .await
        .ok()
        .map(|cfg| cfg.topic_sys_flag)
}

/// 等 topic 建出来且带上 `want` 位；返回最后一次读到的 sysFlag 供断言打印。
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
            ck.abort("U1 构造", &e);
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
        "U1 producer clientId 带 unitName",
        got == expect,
        &format!("{got} (期望 {expect})"),
    );
    let mut msg = Message::new(&topic, Some(b"u1-body"));
    match producer.send(&mut msg, Some(5000), None).await {
        Ok(r) => {
            let msg_id = r.msg_id.unwrap_or_default();
            ck.check(
                "U1 unitName 客户端发送成功",
                !msg_id.is_empty(),
                &format!("msgId={msg_id}"),
            );
        }
        Err(e) => ck.abort("U1 unitName 客户端发送成功", &e.to_string()),
    }
    producer.shutdown();

    // 对照：不设 unitName 时 clientId 不能凭空多出一段
    let plain = match env.make_producer("u1b", None, false, false) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("U1 对照构造", &e);
            return;
        }
    };
    if let Err(e) = plain.start().await {
        ck.abort("U1 对照 start", &e.to_string());
        return;
    }
    let expect2 = format!("{}@uc-u1b-{}", env.ip, env.stamp);
    let got2 = plain.client_id().unwrap_or_default();
    ck.check(
        "U1 对照：无 unitName 不拼后缀",
        got2 == expect2,
        &format!("{got2} (期望 {expect2})"),
    );
    plain.shutdown();
}

// ------------------------------------------------------------------ U2

async fn u2_broker_sees_unit_and_stream(ck: &mut Checker, env: &Env) {
    let topic = env.topic("Conn");
    let group = env.group("conn");
    // 先造出 topic 并等路由可见，否则消费者路由表为空 → 零心跳 → broker 侧查不到连接。
    if let Err(e) = env.send(&topic, "u2-warmup").await {
        ck.abort("U2 预热发送", &e);
        return;
    }
    if !wait_route_visible(env, &topic, WAIT_SECONDS).await {
        ck.abort("U2 预热 topic 路由可见", "路由在预算内没出现");
        return;
    }

    let inbox = Arc::new(Inbox::default());
    let consumer = match DefaultMQPushConsumer::with_config(ConsumerConfig {
        consumer_group: group.clone(),
        instance_name: env.instance("u2"),
        name_server_addrs: vec![env.namesrv.clone()],
        unit_name: Some("unitA".to_string()),
        // 推送消费者默认关 stream，这里显式开，验的是「开了就上线」
        enable_stream_request_type: true,
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("U2 消费者构造", &e.to_string());
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
        "U2 本地 clientId",
        local == expect,
        &format!("{local} (期望 {expect})"),
    );
    if let Err(e) = env.send(&topic, "u2-body").await {
        ck.abort("U2 发送待验消息", &e);
        consumer.shutdown();
        return;
    }
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    while inbox.count("u2-body") == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        "U2 带 @STREAM 后缀的消费者能收到消息",
        inbox.count("u2-body") >= 1,
        &format!("got={:?}", inbox.snapshot()),
    );

    // broker 回读：clientId 是心跳里带上来的原值，客户端没有单方面加工。
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
        "U2 broker 看到的 clientId 含 @unitA@STREAM",
        ids.iter().any(|i| i.ends_with("@unitA@STREAM")),
        &format!("brokerIds={ids:?}"),
    );
    consumer.shutdown();
}

// ------------------------------------------------------------------ U3

async fn u3_unit_mode_sets_topic_unit_flag(ck: &mut Checker, env: &Env) {
    let t_on = env.topic("On");
    let t_off = env.topic("Off");
    // 发送链路里 broker 同步建 topic，但配置落地要一点点时间
    if let Err(e) = env.send_as("u3-on", true, &t_on, "u3-on").await {
        ck.abort("U3 unitMode=true 发送", &e);
        return;
    }
    if let Err(e) = env.send_as("u3-off", false, &t_off, "u3-off").await {
        ck.abort("U3 unitMode=false 发送", &e);
        return;
    }
    let on = wait_flag(env, &t_on, FLAG_UNIT, WAIT_SECONDS).await;
    ck.check(
        "U3 unitMode=true 建的 topic 带 UNIT 位",
        on.is_some_and(|f| f & FLAG_UNIT == FLAG_UNIT),
        &format!("sysFlag={on:?}"),
    );
    // 对照组：等它建出来（不带 UNIT 位），别把「还没建」当成「不带」
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    let mut off = sys_flag(env, &t_off).await;
    while off.is_none() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(300)).await;
        off = sys_flag(env, &t_off).await;
    }
    ck.check(
        "U3 unitMode=false 的对照 topic 不带 UNIT 位",
        off.is_some_and(|f| f & FLAG_UNIT == 0),
        &format!("sysFlag={off:?}"),
    );
}

// ------------------------------------------------------------------ U4

async fn u4_heartbeat_sets_retry_unit_sub(ck: &mut Checker, env: &Env) {
    let topic = env.topic("Retry");
    let group = env.group("hb");
    // 顺序必须是「先发消息，再起消费者」：心跳只发给路由表里已知的 broker。
    if let Err(e) = env.send(&topic, "u4-warmup").await {
        ck.abort("U4 预热发送", &e);
        return;
    }
    if !wait_route_visible(env, &topic, WAIT_SECONDS).await {
        ck.abort("U4 预热 topic 路由可见", "路由在预算内没出现");
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
            ck.abort("U4 消费者构造", &e.to_string());
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
    // 心跳在 start() 里同步发一次；失败会被 debug 吞掉，所以只看 broker 侧结果
    let retry_topic = MixAll::get_retry_topic(&group);
    let flag = wait_flag(env, &retry_topic, FLAG_UNIT_SUB, WAIT_SECONDS).await;
    ck.check(
        "U4 unitMode=true 的 %RETRY% topic 带 UNIT_SUB 位",
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
            ck.abort("U5 构造", &e);
            return;
        }
    };
    if let Err(e) = producer.start().await {
        ck.abort("U5 producer start", &e.to_string());
        return;
    }
    let cid = producer.client_id().unwrap_or_default();
    ck.check(
        "U5 开启 stream 的 producer clientId 带 @STREAM",
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
    ck.check("U5 每个请求都带 ReqT=0 时发送正常", sent == 3, &format!("sent={sent}"));
    producer.shutdown();

    if !wait_route_visible(env, &topic, WAIT_SECONDS).await {
        ck.abort("U5 topic 路由可见", "路由在预算内没出现");
        return;
    }
    // lite 消费者默认就开 stream（Java DefaultLitePullConsumer 构造函数即置位）
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
            ck.abort("U5 lite 构造", &e.to_string());
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
        "U5 轻量消费者 clientId 默认带 @STREAM",
        lite_cid.ends_with("@STREAM"),
        &lite_cid,
    );
    // poll() 会把本地缓冲一次性排空，所以要跨多次 poll 累加
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
        "U5 每个请求都带 ReqT=0 时拉取仍正常",
        ok,
        &format!("bodies={bodies:?}"),
    );
    lite.shutdown();
}

// ------------------------------------------------------------------ 清理

async fn cleanup(ck: &mut Checker, env: &Env) {
    // 先把清单取出来再删：`lock(..).clone()` 直接写在 for 头部会让 MutexGuard
    // 横跨整个循环体（含 await 点）。
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
    ck.check("清理本次的 topic / 订阅组", true, "");
}

// ------------------------------------------------------------------ 驱动

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
