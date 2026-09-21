//! SQL92 过滤 + `CHECK_CLIENT_CONFIG(46)` 对**真实 5.5.1 集群**的联调验证。
//!
//! 与 `python/verify_sql92_live.py`、`cpp/examples/sql92_live.cpp`、
//! `dotnet/examples/RocketMQ.Examples/LiveSql92.cs` 同一套断言（四语言对拍）。
//!
//! 为什么必须在真集群上跑：SQL92 这条链路最容易「静默失效」。broker 的
//! `ExpressionMessageFilter` 在 ConsumeQueue 阶段拿不到编译好的过滤数据时**直接放行全部
//! 消息**（`return true`），于是两种错都表现为「消费者正常启动、消息也都收到了」：
//! broker 没开 `enablePropertyFilter`（表达式根本没被编译）、表达式语法错。
//! 离线单测（`src/client/mq_client.rs` 的 `mod tests`）锁得住协议形状，锁不住 broker
//! 真的按属性过滤了。所以这里四段都验：
//!
//! - S1 线上取证：SQL92 订阅 ⇒ 启动时正好一笔 46（body 是 `CheckClientRequestBody`）；
//!   纯 TAG 订阅 ⇒ 一笔都不发（Java `ExpressionType.isTagType` 短路）。
//! - S2 真过滤：消费者**先起来再发消息**（新消费组 + `CONSUME_FROM_LAST_OFFSET` 会跳过
//!   启动前的消息，先发消息这一段就是假绿）：SQL92 只订阅 red ⇒ 恰好那 3 条 red；
//!   TAG `'*'` 对照组 ⇒ 6 条全收。
//! - S3 空结果腿：订阅永不匹配的 `color = 'green'` ⇒ 一条都不收（排除「其实全放行了」）。
//! - S4 反证：语法错的表达式让 `start()` 抛 `SUBSCRIPTION_PARSE_FAILED(23)`，且启动就地
//!   回滚（同一个对象换成合法表达式能重新 start）。
//!
//! 用法（先按项目记忆里记的 runbook 起本地集群，broker 需 `enablePropertyFilter=true`）：
//! ```text
//! cargo run --example live_sql92 -- 127.0.0.1:9876
//! ```

use std::collections::BTreeSet;
use std::env;
use std::process::ExitCode;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::Value as JsonValue;

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{
    ConsumerConfig, DefaultMQPushConsumer, MessageSelector,
};
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently, SendStatus,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::protocol::codes::{request_code, response_code};
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;
use rocketmq_client_remoting::remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_client_remoting::remoting::rpchook::RPCHook;

/// 本例子建的 topic 队列数（够 broker 分摊，也让路由就绪判定有意义）。
const QUEUE_NUMS: i32 = 4;
/// 每种颜色发几条。
const PER_COLOR: usize = 3;
/// 等消息投递到位的兜底秒数（含重平衡 + 长轮询 + 位点周期）。
const WAIT_SECONDS: u64 = 25;

// ------------------------------------------------------------------ 骨架

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

    fn abort(&mut self, name: &str, err: &str) {
        println!("  [FAIL] {name}: {err}");
        self.failed.push(format!("{name}: {err}"));
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// 轮询到条件成立或超时（真机的一切都是异步到位的，不能睡固定值赌）。
async fn poll_until(mut pred: impl FnMut() -> bool, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if pred() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `Error::Client` 的 response_code；其它变体返回 None。
fn client_code(e: &Error) -> Option<i32> {
    match e {
        Error::Client { response_code, .. } => *response_code,
        _ => None,
    }
}

// ------------------------------------------------- 46 号请求的线上取证钩子

/// 钩在 transport 上，抓启动期真正发出去的 46 号请求（含 body）。
#[derive(Default)]
struct CheckConfigProbe {
    codes: Mutex<Vec<i32>>,
    bodies: Mutex<Vec<JsonValue>>,
    total: std::sync::atomic::AtomicUsize,
}

impl CheckConfigProbe {
    fn new() -> Arc<CheckConfigProbe> {
        Arc::new(CheckConfigProbe::default())
    }

    fn codes(&self) -> Vec<i32> {
        lock(&self.codes).clone()
    }

    fn check_count(&self) -> usize {
        lock(&self.codes)
            .iter()
            .filter(|c| **c == request_code::CHECK_CLIENT_CONFIG)
            .count()
    }

    fn first_body(&self) -> JsonValue {
        lock(&self.bodies).first().cloned().unwrap_or(JsonValue::Null)
    }
}

impl RPCHook for CheckConfigProbe {
    fn do_before_request(&self, _remote_addr: &str, request: &mut RemotingCommand) {
        self.total.fetch_add(1, Ordering::SeqCst);
        if request.code == request_code::CHECK_CLIENT_CONFIG {
            lock(&self.codes).push(request.code);
            if let Some(body) = request.body() {
                let parsed = serde_json::from_slice(body).unwrap_or(JsonValue::Null);
                lock(&self.bodies).push(parsed);
            }
        }
    }
}

// ---------------------------------------------------------------- 投递收集

#[derive(Default)]
struct Inbox {
    /// (body, 属性 color)
    items: Mutex<Vec<(String, Option<String>)>>,
}

impl Inbox {
    fn bodies(&self) -> Vec<String> {
        let mut v: Vec<String> = lock(&self.items).iter().map(|(b, _)| b.clone()).collect();
        v.sort();
        v
    }

    fn colors(&self) -> BTreeSet<String> {
        lock(&self.items)
            .iter()
            .filter_map(|(_, c)| c.clone())
            .collect()
    }

    fn count(&self) -> usize {
        lock(&self.items).len()
    }
}

struct CollectingListener {
    inbox: Arc<Inbox>,
}

impl MessageListenerConcurrently for CollectingListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        {
            let mut items = lock(&self.inbox.items);
            for msg in msgs {
                items.push((
                    String::from_utf8_lossy(msg.get_body()).into_owned(),
                    msg.get_property("color").map(str::to_string),
                ));
            }
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

// ------------------------------------------------------------------ 环境

struct Env {
    namesrv: String,
    stamp: String,
    topic: String,
    group: String,
    admin: DefaultMQAdminExt,
    producer: DefaultMQProducer,
}

impl Env {
    fn new(namesrv: &str, stamp: &str) -> Result<Env, String> {
        let producer = DefaultMQProducer::new(&format!("rust-live-sql92-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        let admin = DefaultMQAdminExt::with_config(AdminConfig {
            instance_name: format!("SQL92-ADMIN-{stamp}"),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        Ok(Env {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            topic: format!("RustLiveSql92{stamp}"),
            group: format!("rust-live-sql92-{stamp}"),
            admin,
            producer,
        })
    }

    async fn start(&mut self) -> Result<(), String> {
        self.admin
            .start()
            .await
            .map_err(|e| format!("admin start failed: {e}"))?;
        self.producer
            .start()
            .await
            .map_err(|e| format!("producer start failed: {e}"))?;
        self.admin
            .create_topic(MixAll::DEFAULT_TOPIC, &self.topic, QUEUE_NUMS, 0)
            .await
            .map_err(|e| format!("create topic failed: {e}"))?;
        Ok(())
    }

    /// 本 topic 在路由里实际带了多少个队列（拉不到路由算 0）。
    async fn route_queue_count(&self) -> i32 {
        match self.admin.examine_topic_route(&self.topic).await {
            Ok(route) => route
                .queue_datas
                .iter()
                .map(|q| q.read_queue_nums)
                .sum::<i32>()
                .max(0),
            Err(_) => 0,
        }
    }

    /// 起一个订阅本 topic 的 push 消费者（`selector` 为 None 时按 `expression` 走 TAG）。
    fn consumer(
        &self,
        suffix: &str,
        selector: Option<MessageSelector>,
        probe: Option<Arc<dyn RPCHook>>,
    ) -> Result<(DefaultMQPushConsumer, Arc<Inbox>), String> {
        let group = format!("{}_{}", self.group, suffix);
        let inbox = Arc::new(Inbox::default());
        let consumer = DefaultMQPushConsumer::with_config(ConsumerConfig {
            consumer_group: group,
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("live-sql92-{suffix}-{}", self.stamp),
            // 先起来再发消息，这里保持默认的 CONSUME_FROM_LAST_OFFSET 才有意义：
            // 若误成 FIRST，S2/S3 会把历史消息也捞进来，断言就失去区分度。
            consume_from_where: ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET.to_string(),
            ..Default::default()
        })
        .map_err(|e| format!("consumer build failed: {e}"))?;
        let selector = selector.unwrap_or_else(|| MessageSelector::by_tag("*"));
        consumer
            .subscribe_with_selector(&self.topic, &selector)
            .map_err(|e| format!("subscribe failed: {e}"))?;
        consumer.set_message_listener_concurrently(Arc::new(CollectingListener {
            inbox: Arc::clone(&inbox),
        }));
        consumer.set_rpc_hook(probe);
        Ok((consumer, inbox))
    }

    /// 发 `PER_COLOR` 条带 `color=<color>` 属性的消息。
    async fn produce(&self, color: &str) -> Vec<String> {
        let mut bodies = Vec::new();
        for i in 0..PER_COLOR {
            let text = format!("body-{color}-{i}");
            let mut msg = Message::new(&self.topic, Some(text.as_bytes()));
            msg.set_keys(&format!("sql92-{}-{color}-{i}", self.stamp));
            msg.put_property("color", color);
            match self.producer.send(&mut msg, Some(5000), None).await {
                Ok(r) if r.status == SendStatus::SendOk => bodies.push(text),
                Ok(r) => println!("  [WARN] send {text} status={:?}", r.status),
                Err(e) => println!("  [WARN] send {text} failed: {e}"),
            }
        }
        bodies
    }
}

// ------------------------------------------------------------------ 场景

async fn s1_wire_evidence(env: &Env, ck: &mut Checker) {
    println!("\n---------- S1 启动期的 46 号请求 ----------");
    let probe = CheckConfigProbe::new();
    let (consumer, _inbox) = match env.consumer(
        "SQL",
        Some(MessageSelector::by_sql("color = 'red'")),
        Some(probe.clone()),
    ) {
        Ok(pair) => pair,
        Err(e) => return ck.abort("S1 build SQL92 consumer", &e),
    };
    match consumer.start().await {
        Ok(()) => {}
        Err(e) => {
            ck.abort("S1 SQL92 consumer start", &e.to_string());
            return;
        }
    }
    ck.check(
        "SQL92 订阅触发恰好一笔 CHECK_CLIENT_CONFIG(46)",
        probe.check_count() == 1,
        &format!("codes={:?}", probe.codes()),
    );
    let body = probe.first_body();
    let sd = body.get("subscriptionData").cloned().unwrap_or(JsonValue::Null);
    let want_group = format!("{}_SQL", env.group);
    ck.check(
        "body 是 CheckClientRequestBody（clientId / group / subscriptionData）",
        body.get("group").and_then(JsonValue::as_str) == Some(want_group.as_str())
            && body.get("clientId").and_then(JsonValue::as_str)
                == Some(consumer.client_id().as_str())
            && sd.get("expressionType").and_then(JsonValue::as_str) == Some("SQL92")
            && sd.get("subString").and_then(JsonValue::as_str) == Some("color = 'red'")
            && sd.get("topic").and_then(JsonValue::as_str) == Some(env.topic.as_str()),
        &body.to_string(),
    );
    consumer.shutdown();

    let tag_probe = CheckConfigProbe::new();
    let (tag_consumer, _inbox) = match env.consumer(
        "TAG",
        Some(MessageSelector::by_tag("tagA || tagB")),
        Some(tag_probe.clone()),
    ) {
        Ok(pair) => pair,
        Err(e) => return ck.abort("S1 build TAG consumer", &e),
    };
    if let Err(e) = tag_consumer.start().await {
        ck.abort("S1 TAG consumer start", &e.to_string());
        return;
    }
    ck.check(
        "纯 TAG 订阅一笔 46 都不发（Java isTagType 短路）",
        tag_probe.check_count() == 0,
        &format!("codes={:?}", tag_probe.codes()),
    );
    tag_consumer.shutdown();
}

async fn s2_s3_real_filtering(env: &Env, ck: &mut Checker) {
    println!("\n---------- S2 broker 按属性过滤 / S3 永不匹配的表达式 ----------");
    let built = [
        ("FILTER", Some(MessageSelector::by_sql("color = 'red'"))),
        ("NONE", Some(MessageSelector::by_sql("color = 'green'"))),
        ("ALL", None),
    ];
    let mut consumers = Vec::new();
    let mut inboxes = Vec::new();
    for (suffix, selector) in built {
        match env.consumer(suffix, selector, None) {
            Ok((c, inbox)) => {
                consumers.push(c);
                inboxes.push(inbox);
            }
            Err(e) => {
                ck.abort(&format!("S2 build consumer {suffix}"), &e);
                return;
            }
        }
    }
    // 顺序照 Python：**先全部 start，再发消息**。
    for c in &consumers {
        if let Err(e) = c.start().await {
            ck.abort("S2 consumer start", &e.to_string());
            return;
        }
    }
    let red = env.produce("red").await;
    let blue = env.produce("blue").await;
    ck.check(
        "6 条消息全部 SEND_OK",
        red.len() == PER_COLOR && blue.len() == PER_COLOR,
        &format!("red={} blue={}", red.len(), blue.len()),
    );

    let all_inbox = Arc::clone(&inboxes[2]);
    poll_until(|| all_inbox.count() >= 2 * PER_COLOR, WAIT_SECONDS).await;
    // green 那一腿是「不该收到」，只能等满窗口；这里额外等一会，排除「来得晚」。
    tokio::time::sleep(Duration::from_secs(4)).await;

    let red_bodies = inboxes[0].bodies();
    let all_bodies = inboxes[2].bodies();
    let mut want_red = red.clone();
    want_red.sort();
    let mut want_all: Vec<String> = red.iter().chain(blue.iter()).cloned().collect();
    want_all.sort();
    ck.check(
        "SQL92(color=red) 收到 3 条，且正好是发出去的那 3 条",
        red_bodies == want_red,
        &format!("{red_bodies:?}"),
    );
    ck.check(
        "TAG '*' 对照组收到 6 条（红+蓝全在）",
        all_bodies == want_all,
        &format!("{all_bodies:?}"),
    );
    ck.check(
        "blue 的 3 条没漏进 SQL92 消费者（证明 broker 真在过滤）",
        blue.iter().all(|b| !red_bodies.contains(b)),
        &format!("{red_bodies:?}"),
    );
    ck.check(
        "收到的消息属性 color 可读且都是 red",
        inboxes[0].colors() == BTreeSet::from(["red".to_string()]),
        &format!("{:?}", inboxes[0].colors()),
    );
    ck.check(
        "订阅永不匹配的 color='green' ⇒ 一条都没收到",
        inboxes[1].count() == 0,
        &format!("{:?}", inboxes[1].bodies()),
    );
    for c in consumers {
        c.shutdown();
    }
}

async fn s4_bad_expression_fails_start(env: &Env, ck: &mut Checker) {
    println!("\n---------- S4 非法表达式在启动期失败 ----------");
    let (consumer, _inbox) = match env.consumer("BAD", Some(MessageSelector::by_sql("color ==")), None)
    {
        Ok(pair) => pair,
        Err(e) => return ck.abort("S4 build consumer", &e),
    };
    let began = Instant::now();
    let err = consumer.start().await.err();
    let cost = began.elapsed().as_secs_f64();
    match &err {
        None => ck.check("start() 抛出错误", false, "非法表达式竟然启动成功"),
        Some(e) => {
            ck.check("start() 抛出 MQClientException", client_code(e).is_some(), &e.to_string());
            ck.check(
                "错误码是 broker 的 SUBSCRIPTION_PARSE_FAILED(23)",
                client_code(e) == Some(response_code::SUBSCRIPTION_PARSE_FAILED),
                &format!("code={:?} err={e}", client_code(e)),
            );
        }
    }
    ck.check("失败后消费者没留在已启动状态", !consumer.is_started(), "仍留在 started=true");
    ck.check(
        "非法表达式立刻失败（<5s，不是等超时兜底）",
        cost < 5.0,
        &format!("{cost:.2}s"),
    );
    // 回滚干净了？同一个对象换个合法表达式应当能重新 start。
    let retry = async {
        consumer
            .subscribe_with_selector(&env.topic, &MessageSelector::by_sql("color = 'blue'"))
            .map_err(|e| e.to_string())?;
        consumer.start().await.map_err(|e| e.to_string())
    }
    .await;
    ck.check(
        "失败后修正表达式可重新 start（启动已就地回滚）",
        retry.is_ok(),
        retry.as_ref().err().map(String::as_str).unwrap_or(""),
    );
    if retry.is_ok() {
        consumer.shutdown();
    }
}

async fn run(namesrv: &str) -> Checker {
    println!("SQL92 / CHECK_CLIENT_CONFIG live check on {namesrv}");
    let stamp = stamp();
    let mut env = match Env::new(namesrv, &stamp) {
        Ok(env) => env,
        Err(e) => {
            let mut ck = Checker::new();
            ck.abort("env build", &e);
            return ck;
        }
    };
    let mut ck = Checker::new();
    if let Err(e) = env.start().await {
        ck.abort("env start", &e);
        return ck;
    }
    // 路由要真的带上 4 个队列才算就绪（broker 建 topic 后异步刷到 namesrv）。
    let began = Instant::now();
    let mut ready = false;
    while began.elapsed() < Duration::from_secs(20) {
        if env.route_queue_count().await >= QUEUE_NUMS {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    ck.check("T0 topic 路由就绪（4 队列）", ready, "examine_topic_route 拿不到队列");

    s1_wire_evidence(&env, &mut ck).await;
    s2_s3_real_filtering(&env, &mut ck).await;
    s4_bad_expression_fails_start(&env, &mut ck).await;

    // 清理：topic + 重试组（消费者组本身不需要显式删）。
    let retry_topic = format!("{}{}", MixAll::RETRY_GROUP_TOPIC_PREFIX, env.group);
    let _ = env.admin.delete_topic(&env.topic, None).await;
    let _ = env.admin.delete_topic(&retry_topic, None).await;
    env.producer.shutdown();
    env.admin.shutdown();
    ck
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
    println!(
        "== summary: {} passed, {} failed ==",
        ck.passed,
        ck.failed.len()
    );
    for f in &ck.failed {
        println!("   FAILED {f}");
    }
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
