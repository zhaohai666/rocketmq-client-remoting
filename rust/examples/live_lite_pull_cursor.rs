//! Offline unit tests (the three `lite_cursor_*` tests in `src/client/pull_consumer.rs`) can only prove that "the
//! `nextBeginOffset` returned by the script is followed"; only a real broker can make both of the following true at once: that `nextBeginOffset`
//! is **computed by the broker itself**, and that after following it **messages really arrive**.
//!
//! Scenarios:
//! - S1 control group: pin 1 message per queue → assign + `seek(0)` + `*` → all 4 received (the chain must work,
//!   and the `maxOffset == 1` yardstick must also hold).
//! - S2 NO_MATCHED_MSG: switch the assign expression to a never-matching tag and `seek(0)` again. The `nextBeginOffset` the broker
//!   returns after filtering out the whole span by the expression **already skips past the entire span** (= maxOffset). Assert that every
//!   queue's pull cursor reaches maxOffset (the previous implementation only advanced the cursor on FOUND, so it would stay at 0 forever,
//!   rescanning the same span every round). Zero deliveries.
//! - S3 OFFSET_ILLEGAL self-healing (the decisive one): `seek(maxOffset + 1000)` on every queue. The broker returns the corrected value
//!   → the cursor must come back to maxOffset; then pin 1 more message per queue, and all 4 must **arrive**. In the previous implementation the cursor
//!   stayed stuck at the out-of-range value forever: every round received the same "out-of-range correction", and not a single new message was seen --
//!   after an out-of-range seek the consumer would **silently** never receive messages again.
//!
//! Usage:
//! ```text
//! cargo run --example live_lite_pull_cursor -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultLitePullConsumer, LitePullConsumerConfig,
};
use rocketmq_client_remoting::common::message::{Message, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;

const QUEUES: i32 = 4;
const NEVER_MATCH: &str = "TagLiteCursorNeverMatch";
const BIG_AHEAD: i64 = 1000;

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

async fn poll_until(mut pred: impl FnMut() -> bool, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if pred() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Polls repeatedly until `expect` messages are collected (or timeout), returning the received bodies.
///
/// `Err` means a poll refused to answer (the consumer is not running) -- that is not "not yet delivered".
async fn drain(c: &DefaultLitePullConsumer, expect: usize, secs: u64) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(secs);
    while out.len() < expect && Instant::now() < deadline {
        match c.poll(Some(500)).await {
            Ok(batch) => out.extend(
                batch
                    .iter()
                    .map(|m| String::from_utf8_lossy(m.get_body()).to_string()),
            ),
            Err(e) => return Err(format!("lite poll refused during the drain: {e}")),
        }
    }
    Ok(out)
}

/// Watches the buffer over a given window (used when asserting "no delivery should happen").
/// An `Ok(vec![])` is the answer that proves silence; a refusal must not be read as one.
async fn poll_quiet(c: &DefaultLitePullConsumer, secs: u64) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        match c.poll(Some(200)).await {
            Ok(batch) => out.extend(
                batch
                    .iter()
                    .map(|m| String::from_utf8_lossy(m.get_body()).to_string()),
            ),
            Err(e) => return Err(format!("lite poll refused during the quiet window: {e}")),
        }
    }
    Ok(out)
}

/// The assign-mode expression is a pre-start setting: a running consumer has already told the broker
/// which expression this topic has, so a leg that needs another expression stops the consumer,
/// sets it, and starts again. Returns false (recorded in `ck`) when either step fails.
async fn set_expr_and_restart(
    ck: &mut Checker,
    c: &DefaultLitePullConsumer,
    topic: &str,
    expression: &str,
) -> bool {
    c.shutdown();
    if let Err(e) = c.set_sub_expression_for_assign(topic, expression) {
        ck.check(
            &format!("expression {expression:?} accepted before start"),
            false,
            &e.to_string(),
        );
        return false;
    }
    if let Err(e) = c.start().await {
        ck.check(
            &format!("consumer restarted with expression {expression:?}"),
            false,
            &e.to_string(),
        );
        return false;
    }
    true
}

fn cursor_row(c: &DefaultLitePullConsumer, mqs: &[MessageQueue]) -> String {
    mqs.iter()
        .map(|mq| format!("q{}:{}", mq.queue_id, c.pull_cursor_of(mq)))
        .collect::<Vec<String>>()
        .join(" ")
}

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let admin = MQClientInstance::new(&format!("rust-lite-cursor-admin-{stamp}"), vec![namesrv.to_string()]);
    if let Err(e) = admin.start().await {
        ck.check("admin start", false, &e.to_string());
        return ck;
    }
    let producer = match DefaultMQProducer::new(&format!("rust-lite-cursor-pg-{stamp}")) {
        Ok(p) => p,
        Err(e) => {
            ck.check("producer build", false, &e.to_string());
            admin.shutdown();
            return ck;
        }
    };
    producer.set_namesrv_addr(namesrv);
    if let Err(e) = producer.start().await {
        ck.check("producer start", false, &e.to_string());
        admin.shutdown();
        return ck;
    }

    let broker_addr = match admin.get_topic_route_data(MixAll::DEFAULT_TOPIC).await {
        Some(route) => match broker_route(&route) {
            Ok((_, addr)) => addr,
            Err(e) => {
                ck.check("broker route", false, &e);
                return ck;
            }
        },
        None => {
            ck.check("broker route", false, "no route of DEFAULT_TOPIC");
            return ck;
        }
    };

    let topic = format!("RustLiteCursor{stamp}");
    let group = format!("GID_RustLiteCursorLive_{stamp}");
    if let Err(e) = admin
        .create_topic_in_broker(
            &broker_addr,
            MixAll::DEFAULT_TOPIC,
            &topic,
            QUEUES,
            QUEUES,
            DEFAULT_PERM,
            0,
            TopicFilterType::SINGLE_TAG,
            false,
            None,
            5000,
            2,
        )
        .await
    {
        ck.check("create topic", false, &format!("{e}"));
        return ck;
    }
    println!("topic={topic} queues={QUEUES} group={group}");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mqs: Vec<MessageQueue> = match admin.get_topic_publish_info(&topic, false).await {
        Ok(p) => p
            .msg_queue_list()
            .into_iter()
            .map(|q| MessageQueue::new(&q.topic, &q.broker_name, q.queue_id))
            .collect(),
        Err(_) => Vec::new(),
    };
    ck.check(
        "route visible: 4 queues",
        mqs.len() == QUEUES as usize,
        &format!("got={}", mqs.len()),
    );
    if mqs.len() != QUEUES as usize {
        producer.shutdown();
        admin.shutdown();
        return ck;
    }

    // ---------- S1 control group: * subscription + seek(0), one pinned message per queue, all received ----------
    for (i, mq) in mqs.iter().enumerate() {
        let mut msg = Message::new(&topic, Some(format!("lc-s1-{i}").as_bytes()));
        msg.set_tags("TagA");
        if let Err(e) = producer.send(&mut msg, None, Some(mq)).await {
            ck.check("S1 pinned send", false, &e.to_string());
        }
    }

    let cfg = LitePullConsumerConfig {
        consumer_group: group.clone(),
        name_server_addrs: vec![namesrv.to_string()],
        instance_name: format!("rust-lite-cursor-{stamp}"),
        poll_timeout_millis: 1000,
        // The out-of-range offset leg must never commit the out-of-range value: auto-commit is off, we only watch the two cursor legs.
        auto_commit: false,
        pull_interval_millis: 200,
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..Default::default()
    };
    let c = match DefaultLitePullConsumer::with_config(cfg) {
        Ok(c) => c,
        Err(e) => {
            ck.check("lite build", false, &e.to_string());
            return ck;
        }
    };
    if let Err(e) = c.assign(&mqs) {
        ck.check("lite assign", false, &e.to_string());
        producer.shutdown();
        admin.shutdown();
        return ck;
    }
    if let Err(e) = c.start().await {
        ck.check("lite start", false, &e.to_string());
        return ck;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    for mq in &mqs {
        c.seek(mq, 0);
    }

    let got1 = match drain(&c, QUEUES as usize, 30).await {
        Ok(bodies) => bodies,
        Err(e) => {
            ck.check("S1 drain window", false, &e);
            c.shutdown();
            producer.shutdown();
            admin.shutdown();
            return ck;
        }
    };
    ck.check(
        "S1-control group: all 4 pinned messages received under the * subscription",
        got1.len() == QUEUES as usize,
        &format!("got={} bodies={:?}", got1.len(), got1),
    );
    {
        let mut maxes = Vec::new();
        for mq in &mqs {
            maxes.push(
                admin
                    .get_max_offset(mq, 5000, Some(&broker_addr))
                    .await
                    .unwrap_or(-1),
            );
        }
        let ok = poll_until(|| maxes.iter().all(|m| *m == 1), 10).await;
        ck.check(
            "S1-every queue has maxOffset == 1 (the yardstick for the next two legs)",
            ok,
            &format!("{:?}", maxes),
        );
    }

    // ---------- S2 NO_MATCHED_MSG: the whole span is filtered out by the expression, the cursor must follow past it ----------
    // While the consumer runs, this expression is something it already reported to the broker, so the
    // change is refused there: the refusal itself is the first thing this leg checks.
    let mid_run = c
        .set_sub_expression_for_assign(&topic, NEVER_MATCH)
        .err()
        .map(|e| e.to_string());
    ck.check(
        "S2 the assign expression is pre-start: changing it while running is refused",
        mid_run
            .as_deref()
            .is_some_and(|m| m.contains("only can be called before start")),
        &mid_run.unwrap_or_else(|| "the call was accepted".to_string()),
    );
    // Then apply it the supported way (stopped, before start), so the leg still measures the span skip.
    if !set_expr_and_restart(&mut ck, &c, &topic, NEVER_MATCH).await {
        c.shutdown();
        producer.shutdown();
        admin.shutdown();
        return ck;
    }
    for mq in &mqs {
        c.seek(mq, 0);
    }
    let ok2 = poll_until(|| mqs.iter().all(|mq| c.pull_cursor_of(mq) == 1), 20).await;
    ck.check(
        "S2-after NO_MATCHED_MSG the pull cursor skips past the whole unmatched span (== maxOffset=1)",
        ok2,
        &cursor_row(&c, &mqs),
    );
    match poll_quiet(&c, 1).await {
        Ok(quiet) => ck.check(
            "S2-zero deliveries during the empty replies",
            quiet.is_empty(),
            &format!("{quiet:?}"),
        ),
        Err(e) => ck.check("S2 quiet window", false, &e),
    }

    // ---------- S3 OFFSET_ILLEGAL: the out-of-range offset is corrected by the broker + messages really come back ----------
    let mut ahead = Vec::new();
    for mq in &mqs {
        let max_off = admin
            .get_max_offset(mq, 5000, Some(&broker_addr))
            .await
            .unwrap_or(0);
        ahead.push(max_off);
        c.seek(mq, max_off + BIG_AHEAD);
    }
    let ok3 = poll_until(|| mqs.iter().all(|mq| c.pull_cursor_of(mq) == 1), 20).await;
    ck.check(
        "S3-after the broker corrects the out-of-range offset the cursor returns to maxOffset (out-of-range self-healing)",
        ok3,
        &format!("{} seekedTo={ahead:?}", cursor_row(&c, &mqs)),
    );

    // The never-matching expression set in S2 must be switched back, otherwise the 4 TagA messages below are filtered out by the broker as-is.
    // Again a pre-start change: stop, set, start.
    if !set_expr_and_restart(&mut ck, &c, &topic, "*").await {
        c.shutdown();
        producer.shutdown();
        admin.shutdown();
        return ck;
    }
    for (i, mq) in mqs.iter().enumerate() {
        let mut msg = Message::new(&topic, Some(format!("lc-s3-{i}").as_bytes()));
        msg.set_tags("TagA");
        if let Err(e) = producer.send(&mut msg, None, Some(mq)).await {
            ck.check("S3 pinned send", false, &e.to_string());
        }
    }

    let got3 = match drain(&c, QUEUES as usize, 40).await {
        Ok(bodies) => bodies,
        Err(e) => {
            ck.check("S3 drain window", false, &e);
            c.shutdown();
            producer.shutdown();
            admin.shutdown();
            return ck;
        }
    };
    ck.check(
        "S3-after self-healing all new messages arrive (previous implementation: cursor stuck at +1000, none seen)",
        got3.len() == QUEUES as usize,
        &format!("got={} bodies={:?}", got3.len(), got3),
    );
    let mut want: Vec<String> = (0..QUEUES).map(|i| format!("lc-s3-{i}")).collect();
    want.sort();
    let mut have = got3.clone();
    have.sort();
    ck.check(
        "S3-exactly the 4 messages pinned after the out-of-range seek are received",
        have == want,
        &format!("bodies={have:?}"),
    );

    c.shutdown();
    producer.shutdown();
    admin.shutdown();
    ck
}

fn broker_route(route: &TopicRouteData) -> Result<(String, String), String> {
    let bd = route
        .broker_datas
        .first()
        .ok_or_else(|| "route has no brokerData".to_string())?;
    let addr = bd
        .select_broker_addr()
        .ok_or_else(|| format!("broker {} has no address", bd.broker_name))?;
    Ok((bd.broker_name.clone(), addr))
}

#[tokio::main]
async fn main() -> ExitCode {
    let namesrv = env::args().nth(1).unwrap_or_else(|| "127.0.0.1:9876".to_string());
    let ck = run(&namesrv).await;
    println!(
        "\nlite pull cursor live: {} passed, {} failed",
        ck.passed,
        ck.failed.len()
    );
    for f in &ck.failed {
        println!("  - {f}");
    }
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
