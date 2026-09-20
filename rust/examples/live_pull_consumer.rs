//! [`DefaultMQPullConsumer`] 对**真实 5.5.1 broker** 的联调验证。
//!
//! 场景与 `python/verify_pull_live.py`（S1..S7）对齐，另外补了两条只有真机才暴露的点：
//! 长轮询真的挂起（P7）、以及**未提交过的消费组读位点得到 `None` 而不是 0**（P5，
//! 对应 Java `QueryConsumerOffsetRequestHeader` 缺省 `setZeroIfNotFound=true` 被显式关掉）。
//!
//! 拉模式的核心是「调用方自己拉、自己管位点」，所以断言都围绕这一点：
//! - P1 生命周期：未配 name server 时 `start()` 失败；未 start 就拉取报错；
//!   `start()` 幂等、`shutdown()` 幂等；`shutdown()` 后再拉取又报错。
//! - P2 `fetch_subscribe_message_queues` → 显式建的 4 队列 topic 拿到 4 个队列、队列号不重不漏。
//! - P3 定向发 12 条（每队列 3 条）→ 逐队列 `max_offset - min_offset == 3`、总量 12。
//! - P4 手动 `pull`：逐队列从 min 拉到 max → 12 条不重不漏、body 与发送集合一致，
//!   且 `MessageExt.broker_name` 已回填（`send_message_back` 要靠它反查路由）。
//! - P5 手动提交位点：`update_consume_offset` → `fetch_consume_offset` 逐队列回读一致。
//! - P6 位点由调用方掌控：从已提交位点再拉 → NO_NEW_MSG；退回队首再拉 → FOUND
//!   （push 模式做不到，这正是 pull 模式的存在意义）；换 TagB 拉 → NO_MATCHED_MSG。
//! - P7 长轮询 `pull_block_if_not_found`：从队尾起拉，1.5s 后另一任务发消息 → FOUND，
//!   实测往返耗时明显大于 1s 且远小于 30s 请求超时（证明挂起发生在 broker 侧）。
//! - P8 `search_offset`（now / 1）与 `earliest_msg_store_time`。
//! - P9 `send_message_back` → 消息出现在 `%RETRY%<group>` 并可被拉取。
//! - P10 消费者自带的 `create_topic` 运维接口 → 新 topic 路由可见、offset 可查。
//! - P11 清理：删掉本次建的 topic。
//!
//! 用法（先按项目记忆里记的 runbook 起本地集群）：
//! ```text
//! cargo run --example live_pull_consumer -- 127.0.0.1:9876
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::process::ExitCode;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultMQPullConsumer, PullConsumerConfig,
};
use rocketmq_client_remoting::client::result::{PullResult, PullStatus};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;

/// 测试 topic 的队列数，与发送条数配套（12 / 4 = 每队列 3 条）。
const QUEUE_NUMS: i32 = 4;
const N_MSG: usize = 12;
const PER_QUEUE: usize = N_MSG / QUEUE_NUMS as usize;
/// 定向发送时用的 tag；P6 用另一个 tag 验证 broker 侧过滤。
const TAG: &str = "TagA";

// ------------------------------------------------------------------ 骨架

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// 断言累积器：一次跑完所有场景再汇总，首个失败不提前退出。
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

/// 锁中毒时照常取内值（一次 panic 不该让整轮验证连锁崩）。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn bodies(msgs: &[MessageExt]) -> BTreeSet<String> {
    msgs.iter()
        .map(|m| String::from_utf8_lossy(m.get_body()).into_owned())
        .collect()
}

/// 队列 → 该队列已拉到的最大 queue_offset + 1（即下一条该拉的位点）。
fn next_offsets(msgs: &[MessageExt]) -> BTreeMap<i32, i64> {
    let mut out: BTreeMap<i32, i64> = BTreeMap::new();
    for m in msgs {
        let next = out.entry(m.queue_id).or_insert(m.queue_offset + 1);
        *next = (*next).max(m.queue_offset + 1);
    }
    out
}

fn describe(result: &PullResult) -> String {
    format!(
        "status={:?} n={} next={} min={} max={}",
        result.status,
        result.msg_found_list.len(),
        result.next_begin_offset,
        result.min_offset,
        result.max_offset,
    )
}

// ------------------------------------------------------------------ 夹具

struct Fixture {
    namesrv: String,
    stamp: String,
    producer: DefaultMQProducer,
    /// 建/删 topic 用的运维实例。
    admin: MQClientInstance,
    broker_name: String,
    broker_addr: String,
    /// 本次建过的 topic，P11 统一删掉。
    topics: Mutex<Vec<String>>,
}

impl Fixture {
    fn new(namesrv: &str, stamp: &str) -> Result<Fixture, String> {
        let producer = DefaultMQProducer::new(&format!("rust-live-pull-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        let admin = MQClientInstance::new(
            &format!("rust-live-pull-admin-{stamp}"),
            vec![namesrv.to_string()],
        );
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            producer,
            admin,
            broker_name: String::new(),
            broker_addr: String::new(),
            topics: Mutex::new(Vec::new()),
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
        // 用 TBW102 的路由找一台可建 topic 的 broker（与 live_consumer.rs 同一做法）。
        let route = self
            .admin
            .get_topic_route_data(MixAll::DEFAULT_TOPIC)
            .await
            .ok_or_else(|| format!("no route of {} from namesrv", MixAll::DEFAULT_TOPIC))?;
        let (broker_name, broker_addr) = broker_of(&route)?;
        self.broker_name = broker_name;
        self.broker_addr = broker_addr;
        Ok(())
    }

    fn topic_name(&self, kind: &str) -> String {
        format!("RustLivePull{kind}{}", self.stamp)
    }

    fn group_name(&self, kind: &str) -> String {
        format!("rust-live-pull-{kind}-{}", self.stamp)
    }

    /// 显式建 topic（不靠 `autoCreateTopicEnable`，队列数才确定）。
    async fn create_topic(&self, topic: &str, queues: i32) -> Result<(), String> {
        self.admin
            .create_topic_in_broker(
                &self.broker_addr,
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
            .await
            .map_err(|e| format!("create topic {topic} failed: {e}"))?;
        lock(&self.topics).push(topic.to_string());
        Ok(())
    }

    /// 按组名建消费者（已配 name server 与实例名，未 start）。
    fn consumer(&self, kind: &str) -> Result<DefaultMQPullConsumer, String> {
        let group = self.group_name(kind);
        let cfg = PullConsumerConfig {
            consumer_group: group,
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("live-pull-{kind}-{}", self.stamp),
            ..Default::default()
        };
        DefaultMQPullConsumer::with_config(cfg).map_err(|e| format!("{kind} build failed: {e}"))
    }

    /// 起一个已 start 的消费者；失败时把错误写进 Checker。
    async fn started(&self, ck: &mut Checker, kind: &str) -> Option<DefaultMQPullConsumer> {
        let c = match self.consumer(kind) {
            Ok(c) => c,
            Err(e) => {
                ck.abort(&format!("{kind} 构造"), &e);
                return None;
            }
        };
        if let Err(e) = c.start().await {
            ck.abort(&format!("{kind} start"), &e.to_string());
            return None;
        }
        Some(c)
    }

    /// 定向发到某个队列，body = `<topic>-<queueId>-<i>`。
    async fn produce_to(&self, topic: &str, queue_id: i32, n: usize) -> usize {
        let mut ok = 0;
        for i in 0..n {
            let body = format!("{topic}-{queue_id}-{i}");
            let mut msg = Message::new(topic, Some(body.as_bytes()));
            msg.set_tags(TAG);
            let mq = MessageQueue::new(topic, &self.broker_name, queue_id);
            match self.producer.send(&mut msg, Some(5000), Some(&mq)).await {
                Ok(_) => ok += 1,
                Err(e) => println!("  [WARN] send {body} failed: {e}"),
            }
        }
        ok
    }
}

fn broker_of(route: &TopicRouteData) -> Result<(String, String), String> {
    let bd = route
        .broker_datas
        .first()
        .ok_or_else(|| "route has no brokerData".to_string())?;
    let addr = bd
        .select_broker_addr()
        .ok_or_else(|| format!("broker {} has no address", bd.broker_name))?;
    Ok((bd.broker_name.clone(), addr))
}

// ------------------------------------------------------------------ P1 生命周期

async fn p1_lifecycle(ck: &mut Checker, fx: &Fixture) {
    // 未配 name server（且没配地址服务器环境变量）→ start 必须硬失败。
    let bare = match DefaultMQPullConsumer::with_config(PullConsumerConfig {
        consumer_group: fx.group_name("bare"),
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("P1 构造", &e.to_string());
            return;
        }
    };
    match bare.start().await {
        Ok(()) => ck.check("P1 未配 name server 时 start 失败", false, "start 竟然成功了"),
        Err(e) => ck.check(
            "P1 未配 name server 时 start 失败",
            e.to_string().contains("name server address is not set"),
            &e.to_string(),
        ),
    }
    ck.check("P1 失败后不残留 started 状态", !bare.is_started(), "");

    let c = match fx.consumer("life") {
        Ok(c) => c,
        Err(e) => {
            ck.abort("P1 构造", &e);
            return;
        }
    };
    let mq = MessageQueue::new(MixAll::DEFAULT_TOPIC, &fx.broker_name, 0);
    match c.pull(&mq, "*", 0, 1, Some(100)).await {
        Ok(r) => ck.check(
            "P1 未 start 就拉取报错",
            false,
            &format!("竟然成功了 {}", describe(&r)),
        ),
        Err(e) => ck.check(
            "P1 未 start 就拉取报错",
            e.to_string().contains("consumer not started"),
            &e.to_string(),
        ),
    }
    ck.check("P1 start 前 is_started 为 false", !c.is_started(), "");
    if let Err(e) = c.start().await {
        ck.abort("P1 start", &e.to_string());
        return;
    }
    ck.check("P1 start 后 is_started", c.is_started(), "");
    ck.check("P1 重复 start 幂等", c.start().await.is_ok(), "");
    let client_id = c.client_id();
    ck.check(
        "P1 start 时现造 client_id（本机IP@instanceName，Java buildMQClientId）",
        client_id
            .split_once('@')
            .is_some_and(|(ip, instance)| !ip.is_empty() && instance.starts_with("live-pull-life")),
        &client_id,
    );
    ck.check(
        "P1 start 后可查默认 topic 路由",
        c.fetch_subscribe_message_queues(MixAll::DEFAULT_TOPIC).await.is_ok(),
        "",
    );
    c.shutdown();
    c.shutdown();
    ck.check("P1 shutdown 幂等且不 panic", !c.is_started(), "");
    match c.pull(&mq, "*", 0, 1, Some(100)).await {
        Ok(r) => ck.check("P1 shutdown 后拉取又报错", false, &describe(&r)),
        Err(e) => ck.check(
            "P1 shutdown 后拉取又报错",
            e.to_string().contains("consumer not started"),
            &e.to_string(),
        ),
    }
}

// ------------------------------------------------- P2 队列 / P3 每队列存量

async fn p2_queues(ck: &mut Checker, fx: &Fixture, topic: &str) -> Vec<MessageQueue> {
    let c = match fx.started(ck, "queue").await {
        Some(c) => c,
        None => return Vec::new(),
    };
    let mqs = match c.fetch_subscribe_message_queues(topic).await {
        Ok(mqs) => mqs,
        Err(e) => {
            ck.abort("P2 fetch_subscribe_message_queues", &e.to_string());
            c.shutdown();
            return Vec::new();
        }
    };
    ck.check(
        &format!("P2 拿到 {QUEUE_NUMS} 个队列"),
        mqs.len() == QUEUE_NUMS as usize,
        &format!("got {}", mqs.len()),
    );
    ck.check(
        "P2 队列 topic/broker 与建 topic 时一致",
        mqs.iter().all(|m| m.topic == topic)
            && mqs.iter().all(|m| m.broker_name == fx.broker_name),
        &format!("{mqs:?}"),
    );
    let ids: BTreeSet<i32> = mqs.iter().map(|m| m.queue_id).collect();
    ck.check(
        "P2 队列号 0..3 不重不漏",
        ids == (0..QUEUE_NUMS).collect::<BTreeSet<i32>>(),
        &format!("{ids:?}"),
    );
    c.shutdown();
    mqs
}

async fn p3_offsets(ck: &mut Checker, fx: &Fixture, mqs: &[MessageQueue]) {
    let c = match fx.started(ck, "offset").await {
        Some(c) => c,
        None => return,
    };
    let mut spans = Vec::new();
    let mut err = String::new();
    for mq in mqs {
        match (c.min_offset(mq).await, c.max_offset(mq).await) {
            (Ok(min), Ok(max)) => spans.push((mq.queue_id, min, max)),
            (Err(e), _) | (_, Err(e)) => {
                if err.is_empty() {
                    err = e.to_string();
                }
            }
        }
    }
    if !err.is_empty() {
        ck.abort("P3 min/max offset", &err);
        c.shutdown();
        return;
    }
    ck.check(
        &format!("P3 每队列 max-min == {PER_QUEUE}"),
        spans
            .iter()
            .all(|(_, min, max)| max - min == PER_QUEUE as i64),
        &format!("{spans:?}"),
    );
    let total: i64 = spans.iter().map(|(_, min, max)| max - min).sum();
    ck.check(
        &format!("P3 topic 总存量 == {N_MSG}"),
        total == N_MSG as i64,
        &format!("total={total} spans={spans:?}"),
    );
    c.shutdown();
}

// ------------------------------------------------------ P4/P5/P6 手动拉取

async fn p4_pull_all(ck: &mut Checker, fx: &Fixture, topic: &str, mqs: &[MessageQueue]) {
    let c = match fx.started(ck, "pull").await {
        Some(c) => c,
        None => return,
    };
    let mut got: Vec<MessageExt> = Vec::new();
    let mut err = String::new();
    for mq in mqs {
        let min = match c.min_offset(mq).await {
            Ok(v) => v,
            Err(e) => {
                if err.is_empty() {
                    err = e.to_string();
                }
                continue;
            }
        };
        match c.pull(mq, TAG, min, 32, Some(5000)).await {
            Ok(r) => {
                ck.check(
                    &format!("P4 队列 {} 首拉 FOUND", mq.queue_id),
                    r.status == PullStatus::Found && r.msg_found_list.len() == PER_QUEUE,
                    &describe(&r),
                );
                ck.check(
                    &format!("P4 队列 {} next_begin_offset == max", mq.queue_id),
                    r.next_begin_offset == r.max_offset,
                    &describe(&r),
                );
                got.extend(r.msg_found_list);
            }
            Err(e) => {
                if err.is_empty() {
                    err = e.to_string();
                }
            }
        }
    }
    if !err.is_empty() {
        ck.abort("P4 拉取", &err);
        c.shutdown();
        return;
    }
    let want: BTreeSet<String> = (0..QUEUE_NUMS)
        .flat_map(|q| (0..PER_QUEUE).map(move |i| format!("{topic}-{q}-{i}")))
        .collect();
    ck.check(
        &format!("P4 手动拉取收全 {N_MSG} 条"),
        got.len() == N_MSG,
        &format!("got {}", got.len()),
    );
    ck.check(
        "P4 body 与发送集合一致（不重不漏）",
        bodies(&got) == want,
        &format!("{:?}", bodies(&got)),
    );
    ck.check(
        "P4 broker_name 已回填（send_message_back 靠它反查路由）",
        got.iter()
            .all(|m| m.broker_name.as_deref() == Some(fx.broker_name.as_str())),
        &format!("{:?}", got.first().map(|m| m.broker_name.clone())),
    );
    ck.check(
        "P4 tag/queueId/reconsumeTimes 等字段来自 broker",
        got.iter().all(|m| m.get_tags() == Some(TAG))
            && got.iter().all(|m| m.reconsume_times == 0)
            && got.iter().all(|m| m.queue_offset >= 0),
        &format!("{:?}", got.first().map(|m| (m.get_tags(), m.reconsume_times))),
    );

    // ---------------- P5 提交位点并回读
    let next = next_offsets(&got);
    let mut readback: Vec<(i32, Option<i64>)> = Vec::new();
    for mq in mqs {
        let off = next.get(&mq.queue_id).copied().unwrap_or(0);
        if let Err(e) = c.update_consume_offset(mq, off).await {
            ck.abort("P5 update_consume_offset", &e.to_string());
            c.shutdown();
            return;
        }
        readback.push((mq.queue_id, c.fetch_consume_offset(mq).await.unwrap_or(None)));
    }
    let consistent = mqs.iter().all(|mq| {
        let want = next.get(&mq.queue_id).copied().unwrap_or(0);
        readback
            .iter()
            .any(|(q, got)| *q == mq.queue_id && *got == Some(want))
    });
    ck.check(
        "P5 提交位点能被 broker 读回（逐队列往返一致）",
        consistent,
        &format!("{readback:?} next={next:?}"),
    );
    c.shutdown();

    // 全新组从没提交过 → None（不是 0）。
    let fresh = match fx.started(ck, "fresh").await {
        Some(c) => c,
        None => return,
    };
    match fresh.fetch_consume_offset(&mqs[0]).await {
        Ok(v) => ck.check(
            "P5 未提交的消费组位点读回 None（不退化成 0）",
            v.is_none(),
            &format!("{v:?}"),
        ),
        Err(e) => ck.abort("P5 读空位点", &e.to_string()),
    }

    // ---------------- P6 位点由调用方掌控
    let mq = &mqs[0];
    let min = fresh.min_offset(mq).await.unwrap_or(0);
    let committed = next.get(&mq.queue_id).copied().unwrap_or(0);
    match fresh.pull(mq, TAG, committed, 32, Some(5000)).await {
        Ok(r) => ck.check(
            "P6 从已提交位点再拉 → NO_NEW_MSG",
            r.status == PullStatus::NoNewMsg && r.msg_found_list.is_empty(),
            &describe(&r),
        ),
        Err(e) => ck.abort("P6 已提交位点拉取", &e.to_string()),
    }
    match fresh.pull(mq, TAG, min, 32, Some(5000)).await {
        Ok(r) => ck.check(
            &format!("P6 位点退回队首再拉 → FOUND {PER_QUEUE} 条"),
            r.status == PullStatus::Found && r.msg_found_list.len() == PER_QUEUE,
            &describe(&r),
        ),
        Err(e) => ck.abort("P6 退回位点拉取", &e.to_string()),
    }
    // 位点非法（超过 max）→ OFFSET_ILLEGAL，并且 next_begin_offset 被 broker 纠正。
    match fresh.pull(mq, TAG, min + 10_000, 32, Some(5000)).await {
        Ok(r) => ck.check(
            "P6 越界位点 → OFFSET_ILLEGAL 且回带 min/max",
            r.status == PullStatus::OffsetIllegal && r.max_offset >= r.min_offset,
            &describe(&r),
        ),
        Err(e) => ck.abort("P6 越界位点拉取", &e.to_string()),
    }
    match fresh.pull(mq, "TagB", min, 32, Some(5000)).await {
        Ok(r) => ck.check(
            "P6 不匹配的 tag → 没有消息（NO_MATCHED_MSG 或 NO_NEW_MSG）",
            r.msg_found_list.is_empty()
                && matches!(r.status, PullStatus::NoMatchedMsg | PullStatus::NoNewMsg),
            &describe(&r),
        ),
        Err(e) => ck.abort("P6 tag 过滤拉取", &e.to_string()),
    }
    fresh.shutdown();
}

// ------------------------------------------------------------------ P7 长轮询

async fn p7_long_polling(ck: &mut Checker, fx: &Fixture, topic: &str, mqs: &[MessageQueue]) {
    let c = match fx.started(ck, "block").await {
        Some(c) => c,
        None => return,
    };
    let mq = &mqs[1];
    let tail = match c.max_offset(mq).await {
        Ok(v) => v,
        Err(e) => {
            ck.abort("P7 max_offset", &e.to_string());
            c.shutdown();
            return;
        }
    };
    let body = format!("{topic}-blocked-0");
    let sent_body = body.clone();
    let producer = fx.producer.clone();
    let send_topic = topic.to_string();
    let broker = fx.broker_name.clone();
    // 1.5s 后发一条 —— 期间这次长轮询必须一直挂在 broker 上。
    let sender = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let mut msg = Message::new(&send_topic, Some(sent_body.as_bytes()));
        msg.set_tags(TAG);
        let mq = MessageQueue::new(&send_topic, &broker, 1);
        producer.send(&mut msg, Some(5000), Some(&mq)).await.is_ok()
    });
    let began = Instant::now();
    let result = c.pull_block_if_not_found(mq, "*", tail, 16).await;
    let cost = began.elapsed();
    let sent = sender.await.unwrap_or(false);
    match result {
        Ok(r) => {
            ck.check(
                "P7 队尾起拉 → 挂起后消息到达返回 FOUND",
                sent && r.status == PullStatus::Found && bodies(&r.msg_found_list).contains(&body),
                &format!("sent={sent} {}", describe(&r)),
            );
            // broker 挂起上限 20s、请求超时 30s：1.5s 后到达 → 往返必然明显大于 1.5s，
            // 也远小于 30s（若退化成短轮询会立刻返回 NO_NEW_MSG 且耗时 ~0）。
            ck.check(
                "P7 真的挂起过（耗时 > 1s 且 < 25s）",
                cost > Duration::from_millis(1000) && cost < Duration::from_secs(25),
                &format!("cost={cost:?}"),
            );
        }
        Err(e) => ck.abort("P7 长轮询", &e.to_string()),
    }
    c.shutdown();
}

// ---------------------------------------------------------- P8 时间位点接口

async fn p8_time_apis(ck: &mut Checker, fx: &Fixture, mqs: &[MessageQueue]) {
    let c = match fx.started(ck, "time").await {
        Some(c) => c,
        None => return,
    };
    let mq = &mqs[0];
    let min = c.min_offset(mq).await.unwrap_or(-1);
    let max = c.max_offset(mq).await.unwrap_or(-1);
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    // 未来时间戳 → 越过全部消息（等于 max）；极早时间戳 → 队首。
    match c.search_offset(mq, now_ms).await {
        Ok(off) => ck.check(
            "P8 search_offset(now) 落在 [min, max]",
            (min..=max).contains(&off),
            &format!("off={off} min={min} max={max}"),
        ),
        Err(e) => ck.abort("P8 search_offset(now)", &e.to_string()),
    }
    match c.search_offset(mq, 1).await {
        Ok(off) => ck.check(
            "P8 search_offset(1) == min_offset",
            off == min,
            &format!("off={off} min={min}"),
        ),
        Err(e) => ck.abort("P8 search_offset(1)", &e.to_string()),
    }
    match c.earliest_msg_store_time(mq).await {
        Ok(t) => ck.check(
            "P8 earliest_msg_store_time 是合理毫秒时间戳",
            t > 1_600_000_000_000 && t <= now_ms,
            &format!("{t} vs now={now_ms}"),
        ),
        Err(e) => ck.abort("P8 earliest_msg_store_time", &e.to_string()),
    }
    c.shutdown();
}

// -------------------------------------------------------------- P9 消息回投

async fn p9_send_back(ck: &mut Checker, fx: &Fixture, mqs: &[MessageQueue]) {
    let group = fx.group_name("back");
    let c = match fx.started(ck, "back").await {
        Some(c) => c,
        None => return,
    };
    let mq = &mqs[2];
    let min = c.min_offset(mq).await.unwrap_or(0);
    let msg = match c.pull(mq, TAG, min, 1, Some(5000)).await {
        Ok(r) => match r.msg_found_list.into_iter().next() {
            Some(m) => m,
            None => {
                ck.abort("P9 取一条消息", "拉到了空结果");
                c.shutdown();
                return;
            }
        },
        Err(e) => {
            ck.abort("P9 拉取", &e.to_string());
            c.shutdown();
            return;
        }
    };
    let body = String::from_utf8_lossy(msg.get_body()).into_owned();
    if let Err(e) = c.send_message_back(&msg, 0).await {
        ck.abort("P9 send_message_back", &e.to_string());
        c.shutdown();
        return;
    }
    // delayLevel=0 → 立即可见；%RETRY%<group> 由 broker 在首次回投时自动建，
    // 路由要等 topic 建好，所以轮询而不是拉一次就下结论。
    let retry_topic = format!("{}{}", MixAll::RETRY_GROUP_TOPIC_PREFIX, group);
    let mut got: Vec<MessageExt> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        for rmq in c.fetch_subscribe_message_queues(&retry_topic).await.unwrap_or_default() {
            let rmin = c.min_offset(&rmq).await.unwrap_or(0);
            let rmax = c.max_offset(&rmq).await.unwrap_or(0);
            if rmax > rmin {
                if let Ok(r) = c.pull(&rmq, "*", rmin, 32, Some(5000)).await {
                    got.extend(r.msg_found_list);
                }
            }
        }
        if bodies(&got).contains(&body) || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        "P9 回投的消息出现在 %RETRY%<group> 且可被拉取",
        bodies(&got).contains(&body),
        &format!("retry_topic={retry_topic} got={:?}", bodies(&got)),
    );
    c.shutdown();
}

// ------------------------------------------------------- P10 消费者建 topic

async fn p10_consumer_create_topic(ck: &mut Checker, fx: &Fixture) {
    let topic = fx.topic_name("Admin");
    let c = match fx.started(ck, "admin").await {
        Some(c) => c,
        None => return,
    };
    if let Err(e) = c.create_topic(&topic, 3, 0).await {
        ck.abort("P10 create_topic", &e.to_string());
        c.shutdown();
        return;
    }
    lock(&fx.topics).push(topic.clone());
    match c.fetch_subscribe_message_queues(&topic).await {
        Ok(mqs) => ck.check(
            "P10 消费者自带 create_topic 后路由可见（3 队列）",
            mqs.len() == 3,
            &format!("got {}", mqs.len()),
        ),
        Err(e) => ck.abort("P10 查路由", &e.to_string()),
    }
    let mq = MessageQueue::new(&topic, &fx.broker_name, 0);
    match (c.min_offset(&mq).await, c.max_offset(&mq).await) {
        (Ok(min), Ok(max)) => ck.check(
            "P10 新 topic 队列可查 min/max offset",
            min == 0 && max == 0,
            &format!("min={min} max={max}"),
        ),
        _ => ck.check("P10 新 topic 队列可查 min/max offset", false, "RPC 失败"),
    }
    c.shutdown();
}

// ------------------------------------------------------------------ P11 清理

async fn p11_cleanup(ck: &mut Checker, fx: &Fixture) {
    let topics: Vec<String> = lock(&fx.topics).clone();
    let mut failed = Vec::new();
    for topic in &topics {
        if let Err(e) = fx
            .admin
            .delete_topic_in_broker(&fx.broker_addr, topic, 5000)
            .await
        {
            failed.push(format!("{topic}:{e}"));
        }
    }
    ck.check(
        "P11 删除本次建的 topic",
        failed.is_empty(),
        &format!("{failed:?}"),
    );
}

// ------------------------------------------------------------------ 驱动

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let mut fx = match Fixture::new(namesrv, &stamp) {
        Ok(fx) => fx,
        Err(e) => {
            ck.abort("夹具构造", &e);
            return ck;
        }
    };
    if let Err(e) = fx.start().await {
        ck.abort("夹具启动", &e);
        return ck;
    }
    println!("== live pull-consumer check, namesrv={namesrv} stamp={stamp} ==");

    p1_lifecycle(&mut ck, &fx).await;

    let topic = fx.topic_name("Main");
    if let Err(e) = fx.create_topic(&topic, QUEUE_NUMS).await {
        ck.abort("建 topic", &e);
        return ck;
    }
    let mut sent = 0;
    for q in 0..QUEUE_NUMS {
        sent += fx.produce_to(&topic, q, PER_QUEUE).await;
    }
    ck.check(
        &format!("发送 {N_MSG} 条全部 SEND_OK"),
        sent == N_MSG,
        &format!("sent={sent}"),
    );
    let mqs = p2_queues(&mut ck, &fx, &topic).await;
    if mqs.len() != QUEUE_NUMS as usize {
        ck.abort("队列不足", "后续场景跳过");
    } else {
        p3_offsets(&mut ck, &fx, &mqs).await;
        p4_pull_all(&mut ck, &fx, &topic, &mqs).await;
        p7_long_polling(&mut ck, &fx, &topic, &mqs).await;
        p8_time_apis(&mut ck, &fx, &mqs).await;
        p9_send_back(&mut ck, &fx, &mqs).await;
        p10_consumer_create_topic(&mut ck, &fx).await;
    }
    p11_cleanup(&mut ck, &fx).await;
    ck
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
