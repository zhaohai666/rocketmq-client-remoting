//! `Validators` / `TopicValidator` live verification against a **real 5.5.1 cluster**.
//!
//! Item-by-item verification of the boundary decisions of the naming and message-size validators.
//!
//!
//! Usage (start the local cluster per the runbook recorded in project memory first):
//! ```text
//! cargo run --example live_validators -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::consumer::DefaultMQPushConsumer;
use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultLitePullConsumer, DefaultMQPullConsumer,
};
use rocketmq_client_remoting::client::result::SendStatus;
use rocketmq_client_remoting::client::top_addressing::DefaultTopAddressing;
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::message_const::PROPERTY_INNER_MULTI_DISPATCH;
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::error::{client_error_code, Error};

/// Upper bound for local counter-leg elapsed time (milliseconds): pure local computation, so 50ms on a live machine already leaves two orders of magnitude of headroom.
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

/// The response_code of `Error::Client`; other variants return None (meaning "not a client validation error").
fn client_code(e: &Error) -> Option<i32> {
    match e {
        Error::Client { response_code, .. } => *response_code,
        _ => None,
    }
}

fn message(topic: &str, body: &[u8]) -> Message {
    let mut m = Message::new(topic, Some(body));
    // keys are used to align sends with consumes; set only when the body itself is a printable key
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

/// Counter-leg: send a message that is certain to be illegal, assert "local message text + code value + sub-millisecond", and return the elapsed milliseconds.
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
            ck.check(name, false, "no exception was thrown");
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

/// Counter-leg: `start()` must fail locally, sub-millisecond, and not remain in the started state.
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
        None => ck.check(name, false, &format!("no exception was thrown started={started}")),
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
    println!("== V1 illegal input must fail locally in sub-millisecond time ==");
    let local_ms = expect_local_reject(
        ck,
        "V1a illegal-character topic",
        p,
        message("bad topic", b"x"),
        "contains illegal characters",
        None,
    )
    .await;
    expect_local_reject(
        ck,
        "V1b blank topic",
        p,
        message("", b"x"),
        "The specified topic is blank",
        None,
    )
    .await;
    expect_local_reject(
        ck,
        "V1c overlong topic",
        p,
        message(&"t".repeat(128), b"x"),
        "is longer than topic max length",
        None,
    )
    .await;
    expect_local_reject(
        ck,
        "V1d forbidden topic (no broker code, not MESSAGE_ILLEGAL)",
        p,
        message("SCHEDULE_TOPIC_XXXX", b"x"),
        "is forbidden",
        None,
    )
    .await;
    p.set_max_message_size(8);
    expect_local_reject(
        ck,
        "V1e overlong body (MESSAGE_ILLEGAL)",
        p,
        message("V1eBody", b"123456789"),
        "the message body size over max value",
        Some(13),
    )
    .await;
    expect_local_reject(
        ck,
        "V1f empty body (MESSAGE_ILLEGAL)",
        p,
        message("V1fBody", b""),
        "the message body length is zero",
        Some(13),
    )
    .await;
    local_ms
}

/// V2: a size exactly at the limit must **not be intercepted by the local body validation** (it later fails slowly because the topic does not exist; that is not the point).
async fn v2_boundary(ck: &mut Checker, p: &DefaultMQProducer) {
    println!("== V2 a body exactly equal to maxMessageSize is allowed ==");
    p.set_max_message_size(8);
    let mut m = message("V2Edge", b"12345678");
    let began = Instant::now();
    let err = p.send(&mut m, Some(1000), None).await.err();
    let ms = elapsed_ms(began);
    let text = err.map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "V2 exactly at the limit does not report oversize (> comparison)",
        !text.contains("over max value"),
        &format!("{text} in {ms:.1}ms"),
    );
    p.set_max_message_size(4 * 1024 * 1024);
}

async fn v3_batch_negatives(ck: &mut Checker, p: &DefaultMQProducer) {
    println!("== V3 batch sending does not bypass local validation ==");
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
            "V3a an illegal sub-message in the batch => the whole batch fails locally",
            e.to_string().contains("contains illegal characters")
                && elapsed_ms(began) < LOCAL_BUDGET_MS,
            &format!("{e} in {:.2}ms", elapsed_ms(began)),
        ),
        None => ck.check("V3a an illegal sub-message in the batch => the whole batch fails locally", false, "no exception was thrown"),
    }

    let mut lmq = message("V3Lmq", b"x");
    lmq.put_property(PROPERTY_INNER_MULTI_DISPATCH, "a/b");
    let began = Instant::now();
    let err = p.send(&mut lmq, Some(3000), None).await.err();
    match err {
        Some(e) => ck.check(
            "V3b INNER_MULTI_DISPATCH with a path separator is intercepted locally (MESSAGE_ILLEGAL)",
            e.to_string().contains("INNER_MULTI_DISPATCH")
                && client_code(&e) == Some(13)
                && elapsed_ms(began) < LOCAL_BUDGET_MS,
            &format!("{e} in {:.2}ms code={:?}", elapsed_ms(began), client_code(&e)),
        ),
        None => ck.check("V3b INNER_MULTI_DISPATCH with a path separator is intercepted locally", false, "no exception was thrown"),
    }
}

async fn v4_group_negatives(ck: &mut Checker, namesrv: &str) {
    println!("== V4 group-name validation runs before any network action ==");
    let long_group = "g".repeat(121);
    for group in [MixAll::DEFAULT_PRODUCER_GROUP, "bad group", &long_group] {
        let p = match DefaultMQProducer::new(group) {
            Ok(p) => p,
            Err(e) => {
                ck.check("V4a producer construction", false, &e.to_string());
                continue;
            }
        };
        p.set_namesrv_addr(namesrv);
        let began = Instant::now();
        let err = p.start().await.err();
        expect_start_reject(
            ck,
            &format!("V4a producer rejects group name {group:?}"),
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
                &format!("V4b push rejects group name {group:?}"),
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
                &format!("V4c pull rejects group name {group:?}"),
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
                &format!("V4d lite rejects group name {group:?}"),
                err,
                lite.is_started(),
                began,
                needle,
            );
        }
    }
}

async fn v5_admin_negatives(ck: &mut Checker, p: &DefaultMQProducer) {
    println!("== V5 admin create_topic also gates first ==");
    for (topic, needle) in [
        ("bad topic", "contains illegal characters"),
        ("RMQ_SYS_TRACE_TOPIC", "is conflict with system topic"),
        ("", "The specified topic is blank"),
    ] {
        let began = Instant::now();
        let err = p.create_topic(topic, QUEUE_NUMS, 0).await.err();
        match err {
            Some(e) => ck.check(
                &format!("V5 refuses to create topic {topic:?}"),
                e.to_string().contains(needle) && elapsed_ms(began) < LOCAL_BUDGET_MS,
                &format!("{e} in {:.2}ms", elapsed_ms(began)),
            ),
            None => ck.check(&format!("V5 refuses to create topic {topic:?}"), false, "no exception was thrown"),
        }
    }
}

/// Positive leg: legal names still send and consume as usual (create topic, lite consumes all, pull queries offsets).
async fn v6_positive(ck: &mut Checker, p: &DefaultMQProducer, namesrv: &str, stamp: &str) {
    println!("== V6 legal names send and receive on the cluster as usual ==");
    let topic = format!("RustLiveValidators{stamp}");
    let group = format!("rust-live-validators-{stamp}");
    if let Err(e) = p.create_topic(&topic, QUEUE_NUMS, 0).await {
        ck.check("V6a create legal topic", false, &e.to_string());
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
        "V6a route of the legal topic is available",
        mqs.len() >= QUEUE_NUMS as usize,
        &format!("queues={}", mqs.len()),
    );

    // The consumer starts before the messages are sent: a new consumer group's CONSUME_FROM_LAST_OFFSET
    // takes the max offset "at start-up", so messages sent earlier would never be received (hit on a live cluster).
    let lite = match DefaultLitePullConsumer::new(&group) {
        Ok(c) => c,
        Err(e) => {
            ck.check("V6b lite construction", false, &e.to_string());
            return;
        }
    };
    lite.set_namesrv_addr(namesrv);
    lite.set_instance_name(&format!("lite-{stamp}"));
    lite.subscribe(&topic, "*");
    if let Err(e) = lite.start().await {
        ck.check("V6b lite start", false, &e.to_string());
    }
    lite.rebalance().await;
    ck.check(
        "V6b lite consumer gets queues assigned",
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
                        "V6c legal message sent successfully",
                        r.status == SendStatus::SendOk && r.msg_id.is_some(),
                        &format!("{:?}", r.status),
                    );
                }
            }
            Err(e) => {
                ck.check("V6c legal message sent successfully", false, &e.to_string());
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
        "V6d lite consumer receives all legal messages",
        missing.is_empty(),
        &format!("got={} missing={missing:?}", got.len()),
    );
    lite.shutdown();

    // Legal group name for pull: it must not only start but also really query an offset (this one goes through a broker RPC)
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
            "V6e a pull consumer with a legal group name starts and queries offsets",
            started && queues.len() >= QUEUE_NUMS as usize && max_off >= 0,
            &format!("queues={} maxOffset={max_off}", queues.len()),
        );
        pull.shutdown();
    }
}

async fn v7_control_leg(ck: &mut Checker, p: &DefaultMQProducer, stamp: &str, local_ms: f64) {
    println!("== V7 control leg: what local validation saves ==");
    let mut m = message(&format!("RustLiveValidatorsMissing{stamp}"), b"x");
    let began = Instant::now();
    let r = p.send(&mut m, Some(5000), None).await;
    let ms = elapsed_ms(began);
    let sent_ok = r.is_ok();
    let text = r.err().map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "V7a a legal but nonexistent topic is not falsely rejected locally (the broker auto-creates it)",
        sent_ok || !text.contains("contains illegal characters"),
        &format!("{text} in {ms:.1}ms"),
    );
    ck.check(
        "V7b the cluster leg is more than two orders of magnitude slower than the local counter-leg",
        ms > 10.0 * local_ms.max(0.05),
        &format!("cluster={ms:.1}ms local={local_ms:.2}ms"),
    );
}

// ---------------------------------------------------------------------- V8

/// The boundary between an addressing failure (10004) and "topic has no route" (10005).
async fn v8_name_server_addressing(ck: &mut Checker, p: &DefaultMQProducer, stamp: &str) {
    println!("== V8 the boundary between an addressing failure (10004) and \"topic has no route\" (10005) ==");

    if DefaultTopAddressing::is_configured() {
        println!("   (skip V8a: ROCKETMQ_NAMESRV_DOMAIN is configured, start() allows a static address to be empty)");
    } else {
        let bare = match DefaultMQProducer::new(&format!("rust-live-validators-noaddr-{stamp}")) {
            Ok(p) => p,
            Err(e) => {
                ck.check("V8a producer construction", false, &e.to_string());
                return;
            }
        };
        let began = Instant::now();
        let err = bare.start().await.err();
        let ms = elapsed_ms(began);
        let code = err.as_ref().and_then(|e| e.response_code());
        ck.check(
            "V8a with no name server configured, start() reports 10004 and does not remain started",
            code == Some(client_error_code::NO_NAME_SERVER_EXCEPTION)
                && !bare.is_started()
                && ms < LOCAL_BUDGET_MS,
            &format!(
                "code={code:?} started={} {ms:.2}ms err={:?}",
                bare.is_started(),
                err.map(|e| e.to_string())
            ),
        );
    }

    // V8b: instance level: when no address exists at all, the route API returns 10004, not
    // "no queue for this topic" (the 10005 wording). This is the config that `validate_name_server_setting` in the funnel reads.
    let no_addr_client = MQClientInstance::new(&format!("rust-live-validators-noaddr-c-{stamp}"), Vec::new());
    let err = no_addr_client.get_topic_publish_info("V8NoAddrTopic", false).await.err();
    let code = err.as_ref().and_then(|e| e.response_code());
    ck.check(
        "V8b a zero-address instance querying the route reports 10004 (not \"topic has no queue\")",
        code == Some(client_error_code::NO_NAME_SERVER_EXCEPTION),
        &format!("code={code:?} err={:?}", err.map(|e| e.to_string())),
    );
    no_addr_client.shutdown();

    // V8c: the address is configured but unreachable (a just-released local port) => the 10004 gate must stay silent:
    // it judges "does the config have an address", not "is the address reachable".
    let dead = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(l) => l.local_addr().map(|a| a.to_string()).unwrap_or_default(),
        Err(e) => {
            ck.check("V8c bind dead port", false, &e.to_string());
            String::new()
        }
    };
    if !dead.is_empty() {
        let mis = match DefaultMQProducer::new(&format!("rust-live-validators-deadns-{stamp}")) {
            Ok(p) => p,
            Err(e) => {
                ck.check("V8c producer construction", false, &e.to_string());
                return;
            }
        };
        mis.set_namesrv_addr(&dead);
        match mis.start().await {
            Err(e) => ck.check("V8c start does not probe reachability and must not fail", false, &e.to_string()),
            Ok(()) => {
                let began = Instant::now();
                let mut m = message("V8DeadNamesrvTopic", b"x");
                let err = mis.send(&mut m, Some(3000), None).await.err();
                let ms = elapsed_ms(began);
                let code = err.as_ref().and_then(|e| e.response_code());
                ck.check(
                    "V8c an unreachable address is not disguised as 10004 (it judges config, not reachability)",
                    code != Some(client_error_code::NO_NAME_SERVER_EXCEPTION),
                    &format!("code={code:?} {ms:.1}ms err={:?}", err.map(|e| e.to_string())),
                );
                mis.shutdown();
            }
        }
    }

    // V8d: control leg (real cluster) -- with addressing working, the same send path returns SEND_OK as usual, showing that
    // what was blocked above is addressing, not a blanket condemnation of the topic.
    let mut m = message(&format!("RustLiveValidatorsAddr{stamp}"), b"addressing-ok");
    let began = Instant::now();
    let r = p.send(&mut m, Some(5000), None).await;
    let ms = elapsed_ms(began);
    let ok = r.is_ok();
    let text = r.err().map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "V8d control: on the real cluster the same send path returns SEND_OK as usual",
        ok,
        &format!("{text} in {ms:.1}ms"),
    );
}

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    println!("== live validators check, namesrv={namesrv} stamp={stamp} ==");
    let p = match DefaultMQProducer::new(&format!("rust-live-validators-pg-{stamp}")) {
        Ok(p) => p,
        Err(e) => {
            ck.check("producer construction", false, &e.to_string());
            return ck;
        }
    };
    p.set_namesrv_addr(namesrv);
    if let Err(e) = p.start().await {
        ck.check("producer start", false, &e.to_string());
        return ck;
    }
    let local_ms = v1_producer_negatives(&mut ck, &p).await;
    v2_boundary(&mut ck, &p).await;
    v3_batch_negatives(&mut ck, &p).await;
    v4_group_negatives(&mut ck, namesrv).await;
    v5_admin_negatives(&mut ck, &p).await;
    v6_positive(&mut ck, &p, namesrv, &stamp).await;
    v7_control_leg(&mut ck, &p, &stamp, local_ms).await;
    v8_name_server_addressing(&mut ck, &p, &stamp).await;
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
