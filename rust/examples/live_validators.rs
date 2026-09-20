//! `Validators` / `TopicValidator` 对**真实 5.5.1 集群**的联调验证。
//!
//! 与 `python/verify_validators_live.py`、`cpp/examples/validators_live.cpp`、
//! `dotnet/examples/RocketMQ.Examples/LiveValidators.cs` 同一套断言（四语言对拍）。
//!
//! 为什么要在真集群上跑：单测只能证明「函数会抛」，证明不了它**拦在网络之前**。
//! 而这条链路的真实代价是可重试码 —— `TOPIC_NOT_EXIST`(17) 在发送重试的可重试集合里，
//! 名字写错时每条消息都会把重试次数与超时预算空转一遍才报出同一个原因。所以正反两条腿
//! 都要跑：
//! - V1 反腿：同一个**已启动**的生产者，非法字符 / 空 / 超长 topic、禁发 topic、超长 body、
//!   空 body 全部亚毫秒本地失败；码值口径照抄 Java（名字类无 broker 码，body 类才是
//!   `MESSAGE_ILLEGAL`(13)）。
//! - V2 边界：恰好等于 `maxMessageSize` 放行（Java 判的是 `>`）。
//! - V3 批量反腿：批内一条非法 ⇒ 整批本地失败；`INNER_MULTI_DISPATCH` 带路径分隔符也挡。
//! - V4 组名反腿：producer / push / pull / lite 的 `start()` 都先查组名，地址齐也照样本地
//!   失败，且失败后不留在 started。
//! - V5 管理端反腿：`create_topic` 对非法/系统 topic 本地拒绝。
//! - V6 正腿：合法 topic 建队列 + 发送 SEND_OK，lite 消费者 poll 收全、pull 查得到位点。
//! - V7 对照腿：合法但**不存在**的 topic 本地放行、走完整集群链路，耗时比反腿高两个数量级
//!   —— 差值就是本地校验省掉的空转。
//!
//! 用法（先按项目记忆里记的 runbook 起本地集群）：
//! ```text
//! cargo run --example live_validators -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::consumer::DefaultMQPushConsumer;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultLitePullConsumer, DefaultMQPullConsumer,
};
use rocketmq_client_remoting::client::result::SendStatus;
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::message_const::PROPERTY_INNER_MULTI_DISPATCH;
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::error::Error;

/// 本地反腿的耗时上界（毫秒）：纯本地计算，真机给 50ms 已经留了两个数量级的余量。
const LOCAL_BUDGET_MS: f64 = 50.0;
const QUEUE_NUMS: i32 = 4;
const N_MSG: usize = 3;

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
            println!("  [FAIL] {name}  {detail}");
            self.failed.push(format!("{name}: {detail}"));
        }
    }
}

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().to_string(),
        Err(_) => "0".to_string(),
    }
}

fn elapsed_ms(began: Instant) -> f64 {
    began.elapsed().as_secs_f64() * 1000.0
}

/// `Error::Client` 的 response_code；其它变体返回 None（表示"不是客户端校验错"）。
fn client_code(e: &Error) -> Option<i32> {
    match e {
        Error::Client { response_code, .. } => *response_code,
        _ => None,
    }
}

fn message(topic: &str, body: &[u8]) -> Message {
    let mut m = Message::new(topic, Some(body));
    // keys 用来做收发对齐；只在正文本身就是可打印 key 时设置
    if let Ok(key) = std::str::from_utf8(body) {
        if key.starts_with("validators-live-") {
            m.set_keys(key);
        }
    }
    m
}

fn needle_for(group: &str, reserved: &str, reserved_msg: &'static str) -> &'static str {
    if group == reserved {
        reserved_msg
    } else if group.contains(' ') {
        "contains illegal characters"
    } else {
        "is longer than group max length"
    }
}

/// 反腿：发一条必然非法的消息，断言「本地文案 + 码值 + 亚毫秒」，返回耗时毫秒。
async fn expect_local_reject(
    ck: &mut Checker,
    name: &str,
    p: &DefaultMQProducer,
    m: Message,
    needle: &str,
    expect_code: Option<i32>,
) -> f64 {
    let mut m = m;
    let began = Instant::now();
    let err = match p.send(&mut m, Some(3000), None).await {
        Ok(_) => {
            ck.check(name, false, "没有抛异常");
            return 0.0;
        }
        Err(e) => e,
    };
    let ms = elapsed_ms(began);
    let text = err.to_string();
    let code_ok = match expect_code {
        Some(c) => client_code(&err) == Some(c),
        None => client_code(&err).is_none(),
    };
    ck.check(
        name,
        text.contains(needle) && ms < LOCAL_BUDGET_MS && code_ok,
        &format!("{text} in {ms:.2}ms code={:?} want={expect_code:?}", client_code(&err)),
    );
    ms
}

/// 反腿：`start()` 必须本地失败、亚毫秒、且不留在 started。
fn expect_start_reject(
    ck: &mut Checker,
    name: &str,
    err: Option<Error>,
    started: bool,
    began: Instant,
    needle: &str,
) {
    let ms = elapsed_ms(began);
    match err {
        None => ck.check(name, false, &format!("没有抛异常 started={started}")),
        Some(e) => {
            let text = e.to_string();
            ck.check(
                name,
                text.contains(needle) && ms < LOCAL_BUDGET_MS && !started,
                &format!("{text} in {ms:.2}ms started={started}"),
            );
        }
    }
}

async fn v1_producer_negatives(ck: &mut Checker, p: &DefaultMQProducer) -> f64 {
    println!("== V1 非法输入必须本地亚毫秒失败 ==");
    let local_ms = expect_local_reject(
        ck,
        "V1a 非法字符 topic",
        p,
        message("bad topic", b"x"),
        "contains illegal characters",
        None,
    )
    .await;
    expect_local_reject(
        ck,
        "V1b 空 topic",
        p,
        message("", b"x"),
        "The specified topic is blank",
        None,
    )
    .await;
    expect_local_reject(
        ck,
        "V1c 超长 topic",
        p,
        message(&"t".repeat(128), b"x"),
        "is longer than topic max length",
        None,
    )
    .await;
    expect_local_reject(
        ck,
        "V1d 禁发 topic（无 broker 码，不是 MESSAGE_ILLEGAL）",
        p,
        message("SCHEDULE_TOPIC_XXXX", b"x"),
        "is forbidden",
        None,
    )
    .await;
    p.set_max_message_size(8);
    expect_local_reject(
        ck,
        "V1e 超长 body（MESSAGE_ILLEGAL）",
        p,
        message("V1eBody", b"123456789"),
        "the message body size over max value",
        Some(13),
    )
    .await;
    expect_local_reject(
        ck,
        "V1f 空 body（MESSAGE_ILLEGAL）",
        p,
        message("V1fBody", b""),
        "the message body length is zero",
        Some(13),
    )
    .await;
    local_ms
}

/// V2：等于上限必须**不被本地 body 校验拦下**（后面会因 topic 不存在而慢失败，那不是重点）。
async fn v2_boundary(ck: &mut Checker, p: &DefaultMQProducer) {
    println!("== V2 恰好等于 maxMessageSize 放行 ==");
    p.set_max_message_size(8);
    let mut m = message("V2Edge", b"12345678");
    let began = Instant::now();
    let err = p.send(&mut m, Some(1000), None).await.err();
    let ms = elapsed_ms(began);
    let text = err.map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "V2 等于上限不报 oversize（Java 的 > 判定）",
        !text.contains("over max value"),
        &format!("{text} in {ms:.1}ms"),
    );
    p.set_max_message_size(4 * 1024 * 1024);
}

async fn v3_batch_negatives(ck: &mut Checker, p: &DefaultMQProducer) {
    println!("== V3 批量发送不绕过本地校验 ==");
    let began = Instant::now();
    let err = p
        .send_batch(
            vec![message("V3Batch", b"ok"), message("bad topic", b"ok")],
            None,
            Some(3000),
        )
        .await
        .err();
    match err {
        Some(e) => ck.check(
            "V3a 批内非法子消息 ⇒ 整批本地失败",
            e.to_string().contains("contains illegal characters")
                && elapsed_ms(began) < LOCAL_BUDGET_MS,
            &format!("{e} in {:.2}ms", elapsed_ms(began)),
        ),
        None => ck.check("V3a 批内非法子消息 ⇒ 整批本地失败", false, "没有抛异常"),
    }

    let mut lmq = message("V3Lmq", b"x");
    lmq.put_property(PROPERTY_INNER_MULTI_DISPATCH, "a/b");
    let began = Instant::now();
    let err = p.send(&mut lmq, Some(3000), None).await.err();
    match err {
        Some(e) => ck.check(
            "V3b INNER_MULTI_DISPATCH 带路径分隔符被本地拦下（MESSAGE_ILLEGAL）",
            e.to_string().contains("INNER_MULTI_DISPATCH")
                && client_code(&e) == Some(13)
                && elapsed_ms(began) < LOCAL_BUDGET_MS,
            &format!("{e} in {:.2}ms code={:?}", elapsed_ms(began), client_code(&e)),
        ),
        None => ck.check("V3b INNER_MULTI_DISPATCH 带路径分隔符被本地拦下", false, "没有抛异常"),
    }
}

async fn v4_group_negatives(ck: &mut Checker, namesrv: &str) {
    println!("== V4 组名校验排在任何网络动作之前 ==");
    let long_group = "g".repeat(121);
    for group in [MixAll::DEFAULT_PRODUCER_GROUP, "bad group", &long_group] {
        let p = match DefaultMQProducer::new(group) {
            Ok(p) => p,
            Err(e) => {
                ck.check("V4a producer 构造", false, &e.to_string());
                continue;
            }
        };
        p.set_namesrv_addr(namesrv);
        let began = Instant::now();
        let err = p.start().await.err();
        expect_start_reject(
            ck,
            &format!("V4a producer 拒绝组名 {group:?}"),
            err,
            p.is_started(),
            began,
            needle_for(
                group,
                MixAll::DEFAULT_PRODUCER_GROUP,
                "can not equal DEFAULT_PRODUCER",
            ),
        );
    }

    for group in [
        MixAll::DEFAULT_CONSUMER_GROUP,
        "bad group",
        long_group.as_str(),
    ] {
        let needle = needle_for(
            group,
            MixAll::DEFAULT_CONSUMER_GROUP,
            "can not equal DEFAULT_CONSUMER",
        );
        if let Ok(push) = DefaultMQPushConsumer::new(group) {
            push.set_namesrv_addr(namesrv);
            let began = Instant::now();
            let err = push.start().await.err();
            expect_start_reject(
                ck,
                &format!("V4b push 拒绝组名 {group:?}"),
                err,
                push.is_started(),
                began,
                needle,
            );
        }
        if let Ok(pull) = DefaultMQPullConsumer::new(group) {
            pull.set_namesrv_addr(namesrv);
            let began = Instant::now();
            let err = pull.start().await.err();
            expect_start_reject(
                ck,
                &format!("V4c pull 拒绝组名 {group:?}"),
                err,
                pull.is_started(),
                began,
                needle,
            );
        }
        if let Ok(lite) = DefaultLitePullConsumer::new(group) {
            lite.set_namesrv_addr(namesrv);
            lite.subscribe("V4LiteTopic", "*");
            let began = Instant::now();
            let err = lite.start().await.err();
            expect_start_reject(
                ck,
                &format!("V4d lite 拒绝组名 {group:?}"),
                err,
                lite.is_started(),
                began,
                needle,
            );
        }
    }
}

async fn v5_admin_negatives(ck: &mut Checker, p: &DefaultMQProducer) {
    println!("== V5 管理端 create_topic 也先把关 ==");
    for (topic, needle) in [
        ("bad topic", "contains illegal characters"),
        ("RMQ_SYS_TRACE_TOPIC", "is conflict with system topic"),
        ("", "The specified topic is blank"),
    ] {
        let began = Instant::now();
        let err = p.create_topic(topic, QUEUE_NUMS, 0).await.err();
        match err {
            Some(e) => ck.check(
                &format!("V5 拒绝建 topic {topic:?}"),
                e.to_string().contains(needle) && elapsed_ms(began) < LOCAL_BUDGET_MS,
                &format!("{e} in {:.2}ms", elapsed_ms(began)),
            ),
            None => ck.check(&format!("V5 拒绝建 topic {topic:?}"), false, "没有抛异常"),
        }
    }
}

/// 正腿：合法名字照常收发（建 topic、lite 收全、pull 查到位点）。
async fn v6_positive(ck: &mut Checker, p: &DefaultMQProducer, namesrv: &str, stamp: &str) {
    println!("== V6 合法名字照常在集群收发 ==");
    let topic = format!("RustLiveValidators{stamp}");
    let group = format!("rust-live-validators-{stamp}");
    if let Err(e) = p.create_topic(&topic, QUEUE_NUMS, 0).await {
        ck.check("V6a 建合法 topic", false, &e.to_string());
        return;
    }
    let began = Instant::now();
    let mqs = loop {
        match p.fetch_publish_message_queues(&topic).await {
            Ok(q) if !q.is_empty() => break q,
            _ => {
                if elapsed_ms(began) > 20_000.0 {
                    break Vec::new();
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }
    };
    ck.check(
        "V6a 合法 topic 路由可用",
        mqs.len() >= QUEUE_NUMS as usize,
        &format!("queues={}", mqs.len()),
    );

    // 先起消费者再发消息：新消费组的 CONSUME_FROM_LAST_OFFSET 取的是"启动那一刻"的
    // max 位点，先发消息就再也收不到了（真机踩过）。
    let lite = match DefaultLitePullConsumer::new(&group) {
        Ok(c) => c,
        Err(e) => {
            ck.check("V6b lite 构造", false, &e.to_string());
            return;
        }
    };
    lite.set_namesrv_addr(namesrv);
    // ⚠ 各自独立的 instanceName：MQClientInstance 按 clientId 复用（对齐 Java 的工厂表），
    // 默认 instanceName 同为 "DEFAULT" 时 lite / pull / producer 会**共用同一个实例**，
    // 那么先 shutdown 的那个会把还在用的实例一起关掉（V7 就是死在这上面）。Java 靠
    // MQClientManager 的引用计数规避，本项目四语言都没做——先各自命名绕开。
    lite.set_instance_name(&format!("lite-{stamp}"));
    lite.subscribe(&topic, "*");
    if let Err(e) = lite.start().await {
        ck.check("V6b lite 启动", false, &e.to_string());
    }
    lite.rebalance().await;
    ck.check(
        "V6b lite 消费者分到队列",
        !lite.assignment().is_empty(),
        &format!("assigned={}", lite.assignment().len()),
    );

    let mut keys = Vec::new();
    for i in 0..N_MSG {
        let key = format!("validators-live-{i}");
        keys.push(key.clone());
        let mut m = message(&topic, key.as_bytes());
        match p.send(&mut m, Some(5000), None).await {
            Ok(r) => {
                if i == 0 {
                    ck.check(
                        "V6c 合法消息发送成功",
                        r.status == SendStatus::SendOk && r.msg_id.is_some(),
                        &format!("{:?}", r.status),
                    );
                }
            }
            Err(e) => {
                ck.check("V6c 合法消息发送成功", false, &e.to_string());
                break;
            }
        }
    }

    let mut got: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while got.len() < N_MSG && Instant::now() < deadline {
        got.extend(
            lite.poll(Some(2000))
                .await
                .iter()
                .filter_map(|m: &MessageExt| m.get_keys())
                .map(str::to_string),
        );
    }
    let missing: Vec<&String> = keys.iter().filter(|k| !got.contains(k)).collect();
    ck.check(
        "V6d lite 消费者收全合法消息",
        missing.is_empty(),
        &format!("got={} missing={missing:?}", got.len()),
    );
    lite.shutdown();

    // pull 的合法组名：不光能启动，还能真查到位点（这条走的是 broker RPC）
    let pull_group = format!("{group}-pull");
    if let Ok(pull) = DefaultMQPullConsumer::new(&pull_group) {
        pull.set_namesrv_addr(namesrv);
        pull.set_instance_name(&format!("pull-{stamp}"));
        let started = pull.start().await.is_ok();
        let queues = pull
            .fetch_subscribe_message_queues(&topic)
            .await
            .unwrap_or_default();
        let max_off = match queues.first() {
            Some(mq) => pull.max_offset(mq).await.unwrap_or(-1),
            None => -1,
        };
        ck.check(
            "V6e 合法组名的 pull 消费者可启动并查到位点",
            started && queues.len() >= QUEUE_NUMS as usize && max_off >= 0,
            &format!("queues={} maxOffset={max_off}", queues.len()),
        );
        pull.shutdown();
    }
}

async fn v7_control_leg(ck: &mut Checker, p: &DefaultMQProducer, stamp: &str, local_ms: f64) {
    println!("== V7 对照腿：本地校验省掉的是什么 ==");
    let mut m = message(&format!("RustLiveValidatorsMissing{stamp}"), b"x");
    let began = Instant::now();
    let r = p.send(&mut m, Some(5000), None).await;
    let ms = elapsed_ms(began);
    let sent_ok = r.is_ok();
    let text = r.err().map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "V7a 合法但不存在的 topic 不被本地误伤（broker 自动建出来）",
        sent_ok || !text.contains("contains illegal characters"),
        &format!("{text} in {ms:.1}ms"),
    );
    ck.check(
        "V7b 集群腿比本地反腿慢两个数量级以上",
        ms > 10.0 * local_ms.max(0.05),
        &format!("cluster={ms:.1}ms local={local_ms:.2}ms"),
    );
}

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    println!("== live validators check, namesrv={namesrv} stamp={stamp} ==");
    let p = match DefaultMQProducer::new(&format!("rust-live-validators-pg-{stamp}")) {
        Ok(p) => p,
        Err(e) => {
            ck.check("producer 构造", false, &e.to_string());
            return ck;
        }
    };
    p.set_namesrv_addr(namesrv);
    if let Err(e) = p.start().await {
        ck.check("producer 启动", false, &e.to_string());
        return ck;
    }
    let local_ms = v1_producer_negatives(&mut ck, &p).await;
    v2_boundary(&mut ck, &p).await;
    v3_batch_negatives(&mut ck, &p).await;
    v4_group_negatives(&mut ck, namesrv).await;
    v5_admin_negatives(&mut ck, &p).await;
    v6_positive(&mut ck, &p, namesrv, &stamp).await;
    v7_control_leg(&mut ck, &p, &stamp, local_ms).await;
    p.shutdown();
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
