//! [`DefaultLitePullConsumer`] 对**真实 5.5.1 broker** 的联调验证。
//!
//! 场景与 `python/verify_lite_pull_live.py`（S1..S6）对齐，另外补了 pause/resume、
//! 手工心跳、以及「`seek()` 会丢掉缓冲里该队列早于目标位点的消息」这三条
//! （L7/L8/L4）——它们都只有真机能验（后台拉取循环 + broker 侧位点与订阅）。
//!
//! Lite 相对 [`DefaultMQPullConsumer`] 的本质区别：**调用方不管位点**，`poll()` 从
//! 后台灌好的本地缓冲里拿消息。所以断言都围绕这条链路：
//! - L1 生命周期：既没订阅也没 assign 时 `start()` 失败；`consumeTimestamp` 不是 14 位
//!   本地墙钟时 `start()` 硬失败（晚抛等于静默退化成「从 max offset 消费」）；
//!   重复 `start()`/`shutdown()` 幂等；未 start 时 `poll()` 只返回空。
//! - L2 subscribe 模式：后台首轮重平衡分到 4 个队列（`start()` 不同步重平衡，
//!   Python 同，所以要催一次 `rebalance()`），随后发的 12 条被 `poll()` 收全，不重不漏。
//! - L3 位点提交：`auto_commit=true` 时**拉到即提交**（`committed()` 逐队列 > 0）；
//!   `auto_commit=false` 时位点不落 broker，显式 `commit()` 之后才前进。
//! - L4 assign 模式：显式 `assign` + 默认 LAST → poll 不到存量；`seek_to_begin` 重放该
//!   队列；全部 `seek_to_begin` 后重放 12 条；`seek_to_end` 丢掉缓冲里的旧消息。
//! - L5 订阅级 tag：`subscribe(topic, "TagA")` 只收 6 条 TagA，且 `subscription()` /
//!   `subscriptions()` 回读到过滤后的订阅集（这是心跳里带给 broker 的东西）。
//! - L6 `CONSUME_FROM_TIMESTAMP`：起点=30 分钟前的**本地墙钟** → 收全 12 条；
//!   墙钟→队列位置的映射：30 分钟前 → Σ==0（各队列队首），10 分钟后 → Σ==12（越过
//!   全部消息）。把墙钟串当 epoch 毫秒解析会把这两个方向同时翻转。
//! - L7 `pause` / `resume`：暂停后 `poll()` 拿不到新消息，恢复后 12 条照常收全。
//! - L8 `send_heartbeat_to_all_broker()` >= 1：lite 的心跳报文只带自己那份
//!   `ConsumerData`；⚠ 自建实例在 `start()` 时路由表还是空的（那一轮发 0 份，
//!   Python 逐字相同），所以先把路由缓存进来再验到达数。
//! - L9 状态与运维接口：`assignment` / `buffered_message_count` /
//!   `offset_for_timestamp` 单调 / `fetch_subscribe_message_queues`。
//! - L10 清理：删掉本次建的 topic。
//!
//! 用法（先按项目记忆里记的 runbook 起本地集群）：
//! ```text
//! cargo run --example live_lite_pull_consumer -- 127.0.0.1:9876
//! ```

use std::collections::BTreeSet;
use std::env;
use std::process::ExitCode;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Local, TimeZone, Utc};

use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultLitePullConsumer, LitePullConsumerConfig,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;

/// 测试 topic 的队列数与消息条数（12 条，TagA/TagB 交替 → 各 6 条）。
const QUEUE_NUMS: i32 = 4;
const N_MSG: usize = 12;
/// 等后台拉取循环把消息灌进缓冲的最长秒数。
const WAIT_SECONDS: u64 = 30;

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

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn bodies(msgs: &[MessageExt]) -> BTreeSet<String> {
    msgs.iter()
        .map(|m| String::from_utf8_lossy(m.get_body()).into_owned())
        .collect()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Java `UtilAll.YYYYMMDDHHMMSS` 口径的**本地墙钟**串（相对 now 偏移若干秒）。
fn wall_clock(secs_offset: i64) -> String {
    let dt: DateTime<Local> = Local
        .timestamp_opt(Utc::now().timestamp() + secs_offset, 0)
        .single()
        .unwrap_or_else(Local::now);
    dt.format("%Y%m%d%H%M%S").to_string()
}

/// 等首轮重平衡把队列分下来。
///
/// ⚠ lite 的 `start()` **不**同步重平衡（Python 也是：起后台循环后由
/// `_pull_service_loop` 每 >1s 触发一次），所以刚 start 时 `assignment()` 可能是空的。
/// 这里显式 `rebalance()` 催一次，别把「最终一致」测成「瞬时一致」。
async fn wait_assignment(c: &DefaultLitePullConsumer, want: usize, secs: u64) -> Vec<MessageQueue> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let assigned = c.assignment();
        if assigned.len() >= want || Instant::now() >= deadline {
            return assigned;
        }
        c.rebalance().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 反复 `poll` 直到累计够 `want` 条或超时（消息是后台循环灌进缓冲的，不能 sleep 赌）。
async fn poll_bounded(c: &DefaultLitePullConsumer, want: usize, secs: u64) -> Vec<MessageExt> {
    let mut got: Vec<MessageExt> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(secs);
    while got.len() < want && Instant::now() < deadline {
        got.extend(c.poll(Some(1000)).await);
    }
    got
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
    /// 本次建过的 topic，L10 统一删掉。
    topics: Mutex<Vec<String>>,
}

impl Fixture {
    fn new(namesrv: &str, stamp: &str) -> Result<Fixture, String> {
        let producer = DefaultMQProducer::new(&format!("rust-live-lite-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        let admin = MQClientInstance::new(
            &format!("rust-live-lite-admin-{stamp}"),
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
        format!("RustLiveLite{kind}{}", self.stamp)
    }

    fn group_name(&self, kind: &str) -> String {
        format!("rust-live-lite-{kind}-{}", self.stamp)
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

    /// 按组名建 lite 消费者（未订阅、未 start；起点由调用方 `set_consume_from_where`）。
    fn consumer(&self, kind: &str) -> Result<DefaultLitePullConsumer, String> {
        let cfg = LitePullConsumerConfig {
            consumer_group: self.group_name(kind),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("live-lite-{kind}-{}", self.stamp),
            poll_timeout_millis: 1000,
            ..Default::default()
        };
        DefaultLitePullConsumer::with_config(cfg).map_err(|e| format!("{kind} build failed: {e}"))
    }

    /// 建好、订阅、已 start 的消费者（订阅必须早于 start：
    /// `start()` 里那次同步心跳要带上订阅集）。
    async fn subscribed(
        &self,
        ck: &mut Checker,
        kind: &str,
        topic: &str,
        expression: &str,
        from: Option<&str>,
        timestamp: Option<String>,
    ) -> Option<DefaultLitePullConsumer> {
        let c = match self.consumer(kind) {
            Ok(c) => c,
            Err(e) => {
                ck.abort(&format!("{kind} 构造"), &e);
                return None;
            }
        };
        c.subscribe(topic, expression);
        if let Some(where_) = from {
            c.set_consume_from_where(where_);
        }
        if let Some(ts) = timestamp {
            c.set_consume_timestamp(&ts);
        }
        if let Err(e) = c.start().await {
            ck.abort(&format!("{kind} start"), &e.to_string());
            return None;
        }
        Some(c)
    }

    /// 交替发 TagA/TagB（偶数下标 TagA），body = `<topic>-<i:02>`。
    async fn produce_alternating(&self, topic: &str) -> usize {
        let mut ok = 0;
        for i in 0..N_MSG {
            let body = format!("{topic}-{i:02}");
            let mut msg = Message::new(topic, Some(body.as_bytes()));
            msg.set_tags(if i % 2 == 0 { "TagA" } else { "TagB" });
            match self.producer.send(&mut msg, Some(5000), None).await {
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

/// 业务 topic 的全部队列（走运维实例的路由，不依赖消费者）。
async fn route_queues(fx: &Fixture, topic: &str) -> Vec<MessageQueue> {
    fx.admin
        .get_topic_publish_info(topic, false)
        .await
        .map(|info| info.msg_queue_list())
        .unwrap_or_default()
}

fn expected_bodies(topic: &str) -> BTreeSet<String> {
    (0..N_MSG).map(|i| format!("{topic}-{i:02}")).collect()
}

// ------------------------------------------------------------- L1 生命周期

async fn l1_lifecycle(ck: &mut Checker, fx: &Fixture) {
    let c = match fx.consumer("bare") {
        Ok(c) => c,
        Err(e) => {
            ck.abort("L1 构造", &e);
            return;
        }
    };
    // 未订阅、未 assign → start 必须失败（Java 的 subscribe 前置校验）。
    match c.start().await {
        Ok(()) => ck.check("L1 未订阅时 start 失败", false, "start 竟然成功了"),
        Err(e) => ck.check(
            "L1 未订阅时 start 失败",
            e.to_string().contains("subscription is not set"),
            &e.to_string(),
        ),
    }
    ck.check("L1 失败后不残留 started 状态", !c.is_started(), "");
    ck.check(
        "L1 未 start 时 poll 返回空",
        c.poll(Some(100)).await.is_empty(),
        "",
    );
    ck.check("L1 未 start 时 is_running 为 false", !c.is_running(), "");

    // consumeTimestamp 只认 14 位墙钟：epoch 毫秒必须被拒（否则会被当成 epoch 解析，
    // 起点直接跑到几十年后 —— 静默退化成「从 max offset 消费」）。
    c.subscribe(&fx.topic_name("Ts"), "*");
    c.set_consume_timestamp("1789000000000");
    match c.start().await {
        Ok(()) => ck.check("L1 非法 consumeTimestamp 被拒", false, "start 竟然成功了"),
        Err(e) => ck.check(
            "L1 非法 consumeTimestamp 被拒",
            e.to_string().contains("consumeTimestamp is invalid"),
            &e.to_string(),
        ),
    }
    ck.check("L1 校验失败后回到未启动", !c.is_started(), "");

    // assign 模式（不需要订阅表）+ 幂等性。
    let c2 = match fx.consumer("life") {
        Ok(c) => c,
        Err(e) => {
            ck.abort("L1 构造", &e);
            return;
        }
    };
    c2.assign(&[MessageQueue::new(
        &fx.topic_name("Main"),
        &fx.broker_name,
        0,
    )]);
    ck.check("L1 assign 后 is_assign_mode", c2.is_assign_mode(), "");
    if let Err(e) = c2.start().await {
        ck.abort("L1 assign 模式 start", &e.to_string());
        return;
    }
    ck.check("L1 start 后 is_started", c2.is_started(), "");
    ck.check("L1 重复 start 幂等", c2.start().await.is_ok(), "");
    ck.check(
        "L1 client_id = instanceName@时间戳",
        c2.client_id().starts_with("live-lite-life") && c2.client_id().contains('@'),
        &c2.client_id(),
    );
    c2.shutdown();
    c2.shutdown();
    ck.check("L1 shutdown 幂等且不 panic", !c2.is_started(), "");
    ck.check("L1 shutdown 后 is_running 归位", !c2.is_running(), "");
}

// ------------------------------------------------- L2 subscribe + poll 收全

async fn l2_subscribe_poll(ck: &mut Checker, fx: &Fixture, topic: &str) {
    // 起点必须是 FIRST：首轮重平衡由后台循环做（`start()` 不同步重平衡），
    // 用 LAST 的话「位点解析」与「消息发送」谁先谁后不确定 —— 解析晚于发送就会
    // 把游标放到队尾，12 条永远收不到。LAST 路径由 L4（assign 模式）覆盖。
    let c = match fx
        .subscribed(ck, "sub", topic, "*", Some(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET), None)
        .await
    {
        Some(c) => c,
        None => return,
    };
    let sent = fx.produce_alternating(topic).await;
    ck.check(
        &format!("发送 {N_MSG} 条全部 SEND_OK"),
        sent == N_MSG,
        &format!("sent={sent}"),
    );
    let assigned = wait_assignment(&c, QUEUE_NUMS as usize, 20).await;
    ck.check(
        &format!("L2 首轮重平衡后分到 {QUEUE_NUMS} 个队列"),
        assigned.len() == QUEUE_NUMS as usize,
        &format!("assigned={assigned:?}"),
    );
    let got = poll_bounded(&c, N_MSG, WAIT_SECONDS).await;
    ck.check(
        &format!("L2 poll 收全 {N_MSG} 条（不重不漏）"),
        bodies(&got) == expected_bodies(topic),
        &format!("n={} got={:?}", got.len(), bodies(&got)),
    );
    let per_queue_ok = got.iter().all(|m| {
        m.topic == topic && m.queue_id >= 0 && m.queue_offset >= 0 && m.reconsume_times == 0
    });
    ck.check("L2 投递字段来自 broker", per_queue_ok, "");
    let mut by_queue: std::collections::BTreeMap<i32, Vec<i64>> =
        std::collections::BTreeMap::new();
    for m in &got {
        by_queue.entry(m.queue_id).or_default().push(m.queue_offset);
    }
    ck.check(
        "L2 同队列内 queue_offset 严格递增",
        by_queue.values().all(|v| v.windows(2).all(|w| w[0] < w[1])),
        &format!("{by_queue:?}"),
    );
    c.shutdown();
}

// ------------------------------------------------------------- L3 位点提交

async fn l3_commit(ck: &mut Checker, fx: &Fixture, topic: &str, mqs: &[MessageQueue]) {
    // auto_commit=true（默认）：**拉到即提交**，不需要显式 commit()。
    let c = match fx
        .subscribed(
            ck,
            "ac",
            topic,
            "*",
            Some(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET),
            None,
        )
        .await
    {
        Some(c) => c,
        None => return,
    };
    let got = poll_bounded(&c, N_MSG, WAIT_SECONDS).await;
    ck.check(
        &format!("L3 auto-commit 组收全 {N_MSG} 条"),
        got.len() == N_MSG,
        &format!("n={}", got.len()),
    );
    let mut committed = Vec::new();
    let mut err = String::new();
    for mq in mqs {
        match c.committed(mq).await {
            Ok(v) => committed.push((mq.queue_id, v)),
            Err(e) => {
                if err.is_empty() {
                    err = e.to_string();
                }
            }
        }
    }
    if !err.is_empty() {
        ck.abort("L3 committed 读回", &err);
    } else {
        let advanced = committed
            .iter()
            .all(|(_, off)| off.unwrap_or(0) > 0);
        ck.check(
            "L3 auto_commit=true 时 committed 全部 > 0（拉到即提交）",
            advanced,
            &format!("{committed:?}"),
        );
    }
    c.shutdown();

    // auto_commit=false：位点不落 broker，显式 commit() 之后才前进。
    let manual = match fx.consumer("manual") {
        Ok(c) => c,
        Err(e) => {
            ck.abort("L3 构造", &e);
            return;
        }
    };
    manual.set_auto_commit(false);
    manual.subscribe(topic, "*");
    manual.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    if let Err(e) = manual.start().await {
        ck.abort("L3 manual start", &e.to_string());
        return;
    }
    let got = poll_bounded(&manual, N_MSG, WAIT_SECONDS).await;
    ck.check(
        "L3 auto_commit=false 也照常投递",
        got.len() == N_MSG,
        &format!("n={}", got.len()),
    );
    let before = manual.committed(&mqs[0]).await.unwrap_or(None);
    ck.check(
        "L3 auto_commit=false 时位点不落 broker",
        before.is_none(),
        &format!("{before:?}"),
    );
    match manual.commit().await {
        Ok(()) => {}
        Err(e) => ck.abort("L3 commit()", &e.to_string()),
    }
    let after = manual.committed(&mqs[0]).await.unwrap_or(None);
    ck.check(
        "L3 commit() 之后位点才落 broker",
        after.unwrap_or(0) > 0,
        &format!("before={before:?} after={after:?}"),
    );
    manual.shutdown();
}

// --------------------------------------------------- L4 assign + seek 重放

async fn l4_assign_seek(ck: &mut Checker, fx: &Fixture, topic: &str, mqs: &[MessageQueue]) {
    let c = match fx.consumer("assign") {
        Ok(c) => c,
        Err(e) => {
            ck.abort("L4 构造", &e);
            return;
        }
    };
    c.assign(mqs);
    ck.check("L4 assign 后 is_assign_mode", c.is_assign_mode(), "");
    if let Err(e) = c.start().await {
        ck.abort("L4 start", &e.to_string());
        return;
    }
    ck.check(
        "L4 assign 模式 assignment() == assign 的队列",
        c.assignment().len() == mqs.len(),
        &format!("{:?}", c.assignment()),
    );
    // assign 模式默认 LAST（新组无已提交位点）→ 存量一条不收。
    ck.check(
        "L4 起点在队尾时 poll 不到存量",
        c.poll(Some(500)).await.is_empty(),
        "",
    );
    if let Err(e) = c.seek_to_begin(&mqs[0]).await {
        ck.abort("L4 seek_to_begin", &e.to_string());
        c.shutdown();
        return;
    }
    let per_queue = N_MSG / QUEUE_NUMS as usize;
    let got = poll_bounded(&c, per_queue, 20).await;
    let got_bodies = bodies(&got);
    ck.check(
        &format!("L4 seek_to_begin 后重放该队列的 {per_queue} 条"),
        got.len() == per_queue
            && got.iter().all(|m| m.queue_id == mqs[0].queue_id)
            && got_bodies.iter().all(|b| expected_bodies(topic).contains(b)),
        &format!("n={} got={got_bodies:?}", got.len()),
    );
    if let Err(e) = c.seek_to_end(&mqs[0]).await {
        ck.abort("L4 seek_to_end", &e.to_string());
        c.shutdown();
        return;
    }
    ck.check(
        "L4 seek 丢掉缓冲里该队列早于目标位点的消息",
        c.buffered_message_count() == 0,
        &format!("buffered={}", c.buffered_message_count()),
    );
    for mq in mqs {
        if let Err(e) = c.seek_to_begin(mq).await {
            ck.abort("L4 全部队列 seek_to_begin", &e.to_string());
            c.shutdown();
            return;
        }
    }
    let got = poll_bounded(&c, N_MSG, WAIT_SECONDS).await;
    ck.check(
        &format!("L4 全部队列 seek_to_begin 后收全 {N_MSG} 条"),
        bodies(&got) == expected_bodies(topic),
        &format!("n={} got={:?}", got.len(), bodies(&got)),
    );
    // seek 到具体位点：队列 0 从 offset=1 起再放剩下 2 条。
    c.seek(&mqs[0], 1);
    let replay = poll_bounded(&c, per_queue - 1, 20).await;
    let from_q0: BTreeSet<String> = replay
        .iter()
        .filter(|m| m.queue_id == mqs[0].queue_id)
        .map(|m| String::from_utf8_lossy(m.get_body()).into_owned())
        .collect();
    ck.check(
        "L4 seek(具体位点) 只重放该队列剩余消息",
        from_q0.len() == per_queue - 1,
        &format!("{from_q0:?}"),
    );
    c.shutdown();
}

// ------------------------------------------------------------ L5 订阅级 tag

async fn l5_tag_filter(ck: &mut Checker, fx: &Fixture, topic: &str) {
    let c = match fx
        .subscribed(
            ck,
            "tag",
            topic,
            "TagA",
            Some(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET),
            None,
        )
        .await
    {
        Some(c) => c,
        None => return,
    };
    let want: BTreeSet<String> = (0..N_MSG)
        .step_by(2)
        .map(|i| format!("{topic}-{i:02}"))
        .collect();
    let got = poll_bounded(&c, N_MSG / 2, WAIT_SECONDS).await;
    let got_bodies = bodies(&got);
    ck.check(
        &format!("L5 subscribe(\"TagA\") 只收 {} 条", N_MSG / 2),
        got_bodies == want,
        &format!("n={} got={got_bodies:?}", got.len()),
    );
    ck.check(
        "L5 收到的消息 tag 全为 TagA",
        got.iter().all(|m| m.get_tags() == Some("TagA")),
        &format!("{:?}", got.iter().map(|m| m.get_tags()).collect::<Vec<_>>()),
    );
    let subs = c.subscription();
    ck.check(
        "L5 subscription() 回读表达式",
        subs.iter().any(|(t, e)| t == topic && e == "TagA"),
        &format!("{subs:?}"),
    );
    let sub_data = c.subscriptions();
    ck.check(
        "L5 subscriptions() 里是解析后的 SubscriptionData（心跳要带的）",
        sub_data.iter().any(|s| s.topic == topic && s.sub_string == "TagA"),
        &format!("{sub_data:?}"),
    );
    // unsubscribe 后订阅表清空（心跳也随之少一项）。
    c.unsubscribe(topic);
    ck.check(
        "L5 unsubscribe 后订阅表为空",
        c.subscription().is_empty(),
        &format!("{:?}", c.subscription()),
    );
    c.shutdown();
}

// ---------------------------------------------- L6 CONSUME_FROM_TIMESTAMP

async fn l6_consume_from_timestamp(
    ck: &mut Checker,
    fx: &Fixture,
    topic: &str,
    mqs: &[MessageQueue],
) {
    // L6a：起点 = 30 分钟前的本地墙钟 → 早于本次全部消息 → 各队列从队首起收全。
    let past = wall_clock(-30 * 60);
    let c = match fx
        .subscribed(
            ck,
            "ts",
            topic,
            "*",
            Some(ConsumeFromWhere::CONSUME_FROM_TIMESTAMP),
            Some(past.clone()),
        )
        .await
    {
        Some(c) => c,
        None => return,
    };
    let got = poll_bounded(&c, N_MSG, WAIT_SECONDS).await;
    ck.check(
        &format!("L6a 墙钟起点={past}（30 分钟前）→ 收全 {N_MSG} 条"),
        bodies(&got) == expected_bodies(topic),
        &format!("n={} got={:?}", got.len(), bodies(&got)),
    );
    c.shutdown();

    // L6b：墙钟真正影响的是「时间戳 → 队列位置」的映射。这里直接量这个映射：
    // 已知的坑是把墙钟串当 epoch 毫秒解析 —— 那样两个方向会同时翻转。
    let c2 = match fx
        .subscribed(
            ck,
            "tsmap",
            topic,
            "*",
            Some(ConsumeFromWhere::CONSUME_FROM_TIMESTAMP),
            Some(past.clone()),
        )
        .await
    {
        Some(c) => c,
        None => return,
    };
    let assigned = wait_assignment(&c2, mqs.len(), 20).await;
    let wall = now_ms();
    let mut sum_past = 0;
    let mut sum_future = 0;
    let mut err = String::new();
    for mq in &assigned {
        match (
            c2.offset_for_timestamp(mq, wall - 30 * 60 * 1000).await,
            c2.offset_for_timestamp(mq, wall + 10 * 60 * 1000).await,
        ) {
            (Ok(a), Ok(b)) => {
                sum_past += a;
                sum_future += b;
            }
            (Err(e), _) | (_, Err(e)) => {
                if err.is_empty() {
                    err = e.to_string();
                }
            }
        }
    }
    if !err.is_empty() {
        ck.abort("L6b offset_for_timestamp", &err);
    }
    ck.check(
        "L6b 30 分钟前 → 各队列队首（Σ==0）",
        sum_past == 0,
        &format!("Σ={sum_past}"),
    );
    ck.check(
        &format!("L6b 10 分钟后 → 越过全部 {N_MSG} 条（Σ=={N_MSG}）"),
        sum_future == N_MSG as i64,
        &format!("Σ={sum_future}"),
    );
    ck.check(
        "L6b assign 之外也用同一路由（4 个队列）",
        assigned.len() == mqs.len(),
        &format!("{assigned:?}"),
    );
    c2.shutdown();
}

// ------------------------------------------------------- L7 pause / resume

async fn l7_pause_resume(ck: &mut Checker, fx: &Fixture) {
    // 独立 topic：本场景要再发 12 条，共用主 topic 会让别的场景计数失配。
    let topic = fx.topic_name("Paused");
    if let Err(e) = fx.create_topic(&topic, QUEUE_NUMS).await {
        ck.abort("L7 建 topic", &e);
        return;
    }
    let c = match fx
        .subscribed(
            ck,
            "pause",
            &topic,
            "*",
            Some(ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET),
            None,
        )
        .await
    {
        Some(c) => c,
        None => return,
    };
    // 先等队列分下来：`pause(&[])` 是什么都没暂停，后面所有断言都会失真。
    let mqs = wait_assignment(&c, QUEUE_NUMS as usize, 20).await;
    ck.check(
        &format!("L7 pause 前已分到 {QUEUE_NUMS} 个队列"),
        mqs.len() == QUEUE_NUMS as usize,
        &format!("assigned={mqs:?}"),
    );
    c.pause(&mqs);
    let sent = fx.produce_alternating(&topic).await;
    ck.check(
        &format!("L7 暂停期间发了 {N_MSG} 条"),
        sent == N_MSG,
        &format!("sent={sent}"),
    );
    let during_pause = c.poll(Some(1500)).await;
    ck.check(
        "L7 pause 后 poll 拿不到消息",
        during_pause.is_empty(),
        &format!(
            "暂停期间仍拿到 {} 条（assignment={}）: {:?}",
            during_pause.len(),
            mqs.len(),
            bodies(&during_pause)
        ),
    );
    c.resume(&mqs);
    // 暂停期间漏进来的那几条也算已消费，别丢掉再等一遍。
    let mut got = during_pause;
    let need = N_MSG.saturating_sub(got.len());
    got.extend(poll_bounded(&c, need, WAIT_SECONDS).await);
    ck.check(
        &format!("L7 resume 后收全新增 {N_MSG} 条"),
        bodies(&got) == expected_bodies(&topic),
        &format!("n={} got={:?}", got.len(), bodies(&got)),
    );
    c.shutdown();
}

// ------------------------------------------------ L8 心跳 / L9 状态与运维

async fn l8_heartbeat_and_state(ck: &mut Checker, fx: &Fixture, topic: &str, mqs: &[MessageQueue]) {
    let c = match fx
        .subscribed(
            ck,
            "state",
            topic,
            "*",
            Some(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET),
            None,
        )
        .await
    {
        Some(c) => c,
        None => return,
    };
    // 心跳的目标是「实例路由表里已知的 broker」。lite 自建实例在 start() 时路由表
    // 还是空的（所以那次心跳发 0 份，与 Python 同序），得等重平衡把订阅 topic 的
    // 路由缓存进来。先拉一次路由（后台循环做的同一件事），再验心跳真的到得了 broker
    // —— 没注册上订阅，broker 侧的 tag 过滤与 GET_CONSUMER_LIST_BY_GROUP 都会失真。
    let _ = wait_assignment(&c, QUEUE_NUMS as usize, 20).await;
    let _ = c.fetch_message_queues(topic).await;
    let ok = c.send_heartbeat_to_all_broker().await;
    ck.check(
        "L8 路由已知时手工心跳发给 broker",
        ok >= 1,
        &format!("ok={ok}"),
    );
    let got = poll_bounded(&c, 1, WAIT_SECONDS).await;
    ck.check(
        "L9 poll 拿到消息后缓冲可读（buffered_message_count）",
        !got.is_empty(),
        &format!("n={} buffered={}", got.len(), c.buffered_message_count()),
    );
    let mq = &mqs[0];
    let min = c.offset_for_timestamp(mq, 1).await.unwrap_or(-1);
    let at_now = c
        .offset_for_timestamp(mq, now_ms())
        .await
        .unwrap_or(-1);
    ck.check(
        "L9 offset_for_timestamp 随时间单调",
        min >= 0 && at_now >= min,
        &format!("min={min} at_now={at_now}"),
    );
    let fetched = c
        .fetch_subscribe_message_queues(topic)
        .await
        .unwrap_or_default();
    ck.check(
        "L9 lite 的 fetch_subscribe_message_queues 与路由一致",
        fetched.len() == mqs.len(),
        &format!("got {}", fetched.len()),
    );
    // 手工重平衡（后台循环之外再触发一次）不该改变队列总数。
    c.rebalance().await;
    ck.check(
        "L9 手工 rebalance 后 assignment 仍是 4 个队列",
        c.assignment().len() == mqs.len(),
        &format!("{:?}", c.assignment()),
    );
    c.shutdown();
}

// ------------------------------------------------------------------ L10 清理

async fn l10_cleanup(ck: &mut Checker, fx: &Fixture) {
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
        "L10 删除本次建的 topic",
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
    println!("== live lite-pull check, namesrv={namesrv} stamp={stamp} ==");

    l1_lifecycle(&mut ck, &fx).await;

    let topic = fx.topic_name("Main");
    if let Err(e) = fx.create_topic(&topic, QUEUE_NUMS).await {
        ck.abort("建 topic", &e);
        return ck;
    }
    let mqs = route_queues(&fx, &topic).await;
    if mqs.len() != QUEUE_NUMS as usize {
        ck.abort("路由队列不足", &format!("got {}", mqs.len()));
        l10_cleanup(&mut ck, &fx).await;
        return ck;
    }
    l2_subscribe_poll(&mut ck, &fx, &topic).await;
    l3_commit(&mut ck, &fx, &topic, &mqs).await;
    l4_assign_seek(&mut ck, &fx, &topic, &mqs).await;
    l5_tag_filter(&mut ck, &fx, &topic).await;
    l6_consume_from_timestamp(&mut ck, &fx, &topic, &mqs).await;
    l7_pause_resume(&mut ck, &fx).await;
    l8_heartbeat_and_state(&mut ck, &fx, &topic, &mqs).await;
    l10_cleanup(&mut ck, &fx).await;
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
