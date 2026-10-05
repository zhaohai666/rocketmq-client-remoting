//! lite-pull **拉取游标**（#105，Java `DefaultLitePullConsumerImpl#PullTaskImpl.run:982-998`）
//! 真机验证。与 `python/verify_lite_pull_cursor_live.py`、`cpp/examples/live_lite_pull_cursor.cpp`、
//! csharp 的对应场景同题、逐条对应。
//!
//! 离线单测（`src/client/pull_consumer.rs` 的 `lite_cursor_*` 三条）只能证明「脚本回的
//! `nextBeginOffset` 被跟了」；真 broker 才能让下面两件事同时成立：那个 `nextBeginOffset`
//! 是 **broker 自己算的**，而且跟过去以后**真的能收到消息**。
//!
//! 场景：
//! - S1 对照组：每条队列钉 1 条 → assign + `seek(0)` + `*` → 4 条全收（链路要通，
//!   `maxOffset == 1` 这个标尺也要立住）。
//! - S2 NO_MATCHED_MSG：把 assign 表达式换成永不匹配的 Tag 再 `seek(0)`。broker 按表达式把
//!   整段滤掉后回的 `nextBeginOffset` **已经越过整段**（= maxOffset）。断言每条队列的拉取
//!   游标都到 maxOffset（旧实现只在 FOUND 时推游标，这里会永远停在 0，每轮重扫同一段）。
//!   零投递。
//! - S3 OFFSET_ILLEGAL 自愈（决定性一条）：每条队列 `seek(maxOffset + 1000)`。broker 回纠正值
//!   → 游标必须回到 maxOffset；随后每条队列再钉 1 条，4 条必须**全部收到**。旧实现的游标
//!   永远卡在越界值上：每轮收到同一个「越界纠正」，新消息一条也看不到 —— 越界之后消费者
//!   会**静默**地永远收不到消息。
//!
//! 用法：
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

/// 反复 poll 直到收齐 `expect` 条（或超时），返回收到的 body。
async fn drain(c: &DefaultLitePullConsumer, expect: usize, secs: u64) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(secs);
    while out.len() < expect && Instant::now() < deadline {
        out.extend(
            c.poll(Some(500))
                .await
                .iter()
                .map(|m| String::from_utf8_lossy(m.get_body()).to_string()),
        );
    }
    out
}

/// 给定窗口里盯住缓冲（断言「不该有交付」时用）。
async fn poll_quiet(c: &DefaultLitePullConsumer, secs: u64) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        out.extend(
            c.poll(Some(200))
                .await
                .iter()
                .map(|m| String::from_utf8_lossy(m.get_body()).to_string()),
        );
    }
    out
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
        "路由可见：4 条队列",
        mqs.len() == QUEUES as usize,
        &format!("got={}", mqs.len()),
    );
    if mqs.len() != QUEUES as usize {
        producer.shutdown();
        admin.shutdown();
        return ck;
    }

    // ---------- S1 对照组：* 订阅 + seek(0)，每条队列一条消息全收 ----------
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
        // 位点越界那一腿绝不能把越界值提交上去：关掉自动提交，只看两条游标。
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
    c.assign(&mqs);
    if let Err(e) = c.start().await {
        ck.check("lite start", false, &e.to_string());
        return ck;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    for mq in &mqs {
        c.seek(mq, 0);
    }

    let got1 = drain(&c, QUEUES as usize, 30).await;
    ck.check(
        "S1-对照组：* 订阅下 4 条钉到队列的消息全收",
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
            "S1-每条队列 maxOffset == 1（后面两条腿的标尺）",
            ok,
            &format!("{:?}", maxes),
        );
    }

    // ---------- S2 NO_MATCHED_MSG：整段被表达式滤掉，游标要跟过整段 ----------
    c.set_sub_expression_for_assign(&topic, NEVER_MATCH);
    for mq in &mqs {
        c.seek(mq, 0);
    }
    let ok2 = poll_until(|| mqs.iter().all(|mq| c.pull_cursor_of(mq) == 1), 20).await;
    ck.check(
        "S2-NO_MATCHED_MSG 后拉取游标越过整段不匹配区间（== maxOffset=1）",
        ok2,
        &cursor_row(&c, &mqs),
    );
    let quiet = poll_quiet(&c, 1).await;
    ck.check("S2-空应答期间零投递", quiet.is_empty(), &format!("{quiet:?}"));

    // ---------- S3 OFFSET_ILLEGAL：越界位点被 broker 纠正 + 消息真的回来 ----------
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
        "S3-越界位点被 broker 纠正后游标回到 maxOffset（越界自愈）",
        ok3,
        &format!("{} seekedTo={ahead:?}", cursor_row(&c, &mqs)),
    );

    // S2 换上的永不匹配表达式要换回来，否则下面 4 条 TagA 会被 broker 原样滤掉。
    c.set_sub_expression_for_assign(&topic, "*");
    for (i, mq) in mqs.iter().enumerate() {
        let mut msg = Message::new(&topic, Some(format!("lc-s3-{i}").as_bytes()));
        msg.set_tags("TagA");
        if let Err(e) = producer.send(&mut msg, None, Some(mq)).await {
            ck.check("S3 pinned send", false, &e.to_string());
        }
    }

    let got3 = drain(&c, QUEUES as usize, 40).await;
    ck.check(
        "S3-自愈后新消息全部送达（旧实现：游标卡在 +1000，一条都看不到）",
        got3.len() == QUEUES as usize,
        &format!("got={} bodies={:?}", got3.len(), got3),
    );
    let mut want: Vec<String> = (0..QUEUES).map(|i| format!("lc-s3-{i}")).collect();
    want.sort();
    let mut have = got3.clone();
    have.sort();
    ck.check(
        "S3-收到的正是越界之后钉进去的那 4 条",
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
