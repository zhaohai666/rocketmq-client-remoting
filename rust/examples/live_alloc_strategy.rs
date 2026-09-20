//! 可插拔队列分配策略的真机验证（对应 Java `setAllocateMessageQueueStrategy`）。
//!
//! 单元测试（`src/client/allocate_strategy.rs`）只能证明「给定 mqAll/cidAll 的切分结果
//! 与 Java 一致」，证明不了**这条链路真的通**：换掉策略对象之后，rebalance 是否用它、
//! broker 是否看得见同组第二个实例、分配结果是否真的驱动了收发。所以这里跑真集群。
//!
//! 场景与 cpp/examples/live_lite_pull.cpp、`verify_lite_pull_live.py`、
//! .NET `LiveLitePull` 的 S7 同口径（同一套断言，跨语言对拍）：
//! - A1 默认策略：`allocate_message_queue_strategy()` 回读 = AVG（Java 字段初值），
//!   setter 换掉的策略能读回 AVG_BY_CIRCLE。
//! - A2 同组两实例 + AVG_BY_CIRCLE：两边各自心跳注册，broker 的
//!   `GET_CONSUMER_LIST_BY_GROUP` 才认到两个 clientId → 4 个队列按下标取模交叉切开，
//!   不重不漏，且形状是「步长 2」的交叉（AVG 会给连续两段）；两边合起来收全 12 条。
//! - A3 两个 CONFIG 消费者各配一半队列：`assignment()` 恰为配置进去的那一半
//!   （CONFIG 无视 mqAll/cidAll），poll 到的消息也只来自这些队列；两半合起来
//!   恰好覆盖全部 12 条且互不重叠 → 策略确实驱动了收发，而不只是改了个字段。
//! - A4 CONSISTENT_HASH：用**真实 clientId** 建哈希环，等两边把队列分完之后，
//!   线上 `assignment()` 必须与「拿真实 mqAll/cidAll 离线跑同一策略」的预测逐条相等。
//!   注意这里**不要求两边都非空**：环按队列 key 落点，真实 clientId 的哈希完全可能把
//!   4 个队列全分给一个实例（Java 同款），所以判定只看「不重不漏」。
//! - A5 MACHINE_ROOM_NEARBY：包着 A4 的同一个环，外加一个「全员同机房」的 resolver。
//!   单机房时 NEARBY 在内层分完后必定走同一个机房分支 → 结果应与内层策略**完全一致**，
//!   这条断言证明装饰器没有偷偷改切分；resolver 的调用记录还证明 rebalance 真的问过
//!   每个队列的机房（`broker-a`）和每个客户端的机房。
//! - A6 MACHINE_ROOM：真实 brokerName 是 `broker-a`，不含 `@`，Java 的
//!   `split("@").length == 2` 判定不通过 → 任何机房白名单都筛不出队列。这里验证
//!   「配错机房」是**安静地饿死**（分配空、收不到消息、不抛异常），而不是把
//!   rebalance 打崩。
//!
//! ⚠ A2 依赖「订阅在首个心跳前就已注册 broker」：lite 的心跳只发给实例路由表里
//! 已知的 broker，所以 `start()` 会先把订阅 topic 的路由拉进来（见
//! `pull_consumer.rs` 的 `refresh_route_for_heartbeat`）。少了那一步，同组对端要等
//! 一个心跳周期（5s）才互相看得见，首轮 rebalance 会各自独占全部队列。
//!
//! 用法（先按项目记忆里记的 runbook 起本地集群）：
//! ```text
//! cargo run --example live_alloc_strategy -- 127.0.0.1:9876
//! ```

use std::collections::BTreeSet;
use std::env;
use std::process::ExitCode;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::allocate_strategy::{
    AllocateMachineRoomNearby, AllocateMessageQueueAveragely, AllocateMessageQueueAveragelyByCircle,
    AllocateMessageQueueByConfig, AllocateMessageQueueByMachineRoom,
    AllocateMessageQueueConsistentHash, AllocateMessageQueueStrategy, MachineRoomResolver,
};
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

const QUEUE_NUMS: i32 = 4;
const N_MSG: usize = 12;
/// 等分配收敛的最长秒数。两边要各跑一轮心跳（5s）+ 一轮重平衡（20s），留足一倍余量。
const WAIT_SECONDS: u64 = 45;

// ------------------------------------------------------------------ 骨架

fn stamp() -> String {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("{ms}")
}

struct Checker {
    passed: usize,
    failed: Vec<String>,
}

impl Checker {
    fn new() -> Checker {
        Checker { passed: 0, failed: Vec::new() }
    }

    fn check(&mut self, name: &str, cond: bool, detail: &str) {
        if cond {
            self.passed += 1;
            println!("  [PASS] {name}{}", if detail.is_empty() { String::new() } else { format!("  {detail}") });
        } else {
            let label = format!("{name}{}", if detail.is_empty() { String::new() } else { format!("  {detail}") });
            self.failed.push(label.clone());
            println!("  [FAIL] {label}");
        }
    }

    fn abort(&mut self, name: &str, detail: &str) {
        self.failed.push(format!("{name}（前置失败）: {detail}"));
        println!("  [ABORT] {name}: {detail}");
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 队列 key 集合（`brokerName#queueId`），与其余三门语言的 queueKeys 同口径。
fn queue_keys(mqs: &[MessageQueue]) -> BTreeSet<String> {
    mqs.iter()
        .map(|mq| format!("{}#{}", mq.get_broker_name(), mq.get_queue_id()))
        .collect()
}

fn key_set_text(keys: &BTreeSet<String>) -> String {
    keys.iter().cloned().collect::<Vec<_>>().join(" ")
}

fn bodies(msgs: &[MessageExt]) -> BTreeSet<String> {
    msgs.iter()
        .map(|m| String::from_utf8_lossy(m.get_body()).into_owned())
        .collect()
}

/// 只统计来自 `on_queues`（`brokerName#queueId` 集合）那部分消息的 body。
fn bodies_on(msgs: &[MessageExt], on_queues: &BTreeSet<String>) -> BTreeSet<String> {
    msgs.iter()
        .filter(|m| {
            on_queues.contains(&format!("{}#{}", m.get_broker_name().unwrap_or_default(), m.queue_id))
        })
        .map(|m| String::from_utf8_lossy(m.get_body()).into_owned())
        .collect()
}

/// 按**时间**排空缓冲：策略只分到部分队列时收条数未知，不能按条数等。
async fn drain_for(c: &DefaultLitePullConsumer, secs: u64) -> Vec<MessageExt> {
    let mut got = Vec::new();
    let deadline = now_ms() + (secs as i64) * 1000;
    while now_ms() < deadline {
        got.extend(c.poll(Some(1000)).await);
    }
    got
}

fn sorted_queues(mut mqs: Vec<MessageQueue>) -> Vec<MessageQueue> {
    mqs.sort_by(|a, b| {
        a.broker_name
            .cmp(&b.broker_name)
            .then(a.queue_id.cmp(&b.queue_id))
    });
    mqs
}

/// 等两个实例把队列**分完**（分配收敛要两边各跑一轮心跳 + rebalance）。
async fn wait_split_assignment(
    a: &DefaultLitePullConsumer,
    b: &DefaultLitePullConsumer,
    total: usize,
    secs: u64,
) -> (Vec<MessageQueue>, Vec<MessageQueue>) {
    let deadline = now_ms() + (secs as i64) * 1000;
    loop {
        let qa = a.assignment();
        let qb = b.assignment();
        let ka = queue_keys(&qa);
        let kb = queue_keys(&qb);
        let overlap: BTreeSet<String> = ka.intersection(&kb).cloned().collect();
        let union: BTreeSet<String> = ka.union(&kb).cloned().collect();
        if !ka.is_empty() && !kb.is_empty() && overlap.is_empty() && union.len() == total {
            return (qa, qb);
        }
        if now_ms() >= deadline {
            return (qa, qb);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// 等首轮重平衡把队列分下来（lite 的 `start()` 不同步重平衡，由后台循环触发）。
async fn wait_assignment(c: &DefaultLitePullConsumer, secs: u64) -> Vec<MessageQueue> {
    let deadline = now_ms() + (secs as i64) * 1000;
    loop {
        let a = c.assignment();
        if !a.is_empty() || now_ms() >= deadline {
            return a;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// ------------------------------------------------------------------ 夹具

struct Fixture {
    namesrv: String,
    stamp: String,
    producer: DefaultMQProducer,
    admin: MQClientInstance,
    broker_name: String,
    broker_addr: String,
    topics: Mutex<Vec<String>>,
}

impl Fixture {
    fn new(namesrv: &str, stamp: &str) -> Result<Fixture, String> {
        let producer = DefaultMQProducer::new(&format!("rust-live-alloc-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        let admin = MQClientInstance::new(
            &format!("rust-live-alloc-admin-{stamp}"),
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

    fn topic_name(&self) -> String {
        format!("RustLiveAlloc{}", self.stamp)
    }

    fn group_name(&self, kind: &str) -> String {
        format!("rust-live-alloc-{kind}-{}", self.stamp)
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

    /// 按组名 + 实例名建 lite 消费者（未订阅、未 start）。
    fn consumer(&self, group_kind: &str, instance: &str) -> Result<DefaultLitePullConsumer, String> {
        let cfg = LitePullConsumerConfig {
            consumer_group: self.group_name(group_kind),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("live-alloc-{instance}-{}", self.stamp),
            poll_timeout_millis: 1000,
            ..Default::default()
        };
        DefaultLitePullConsumer::with_config(cfg).map_err(|e| format!("{instance} build failed: {e}"))
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

    async fn cleanup(&mut self, ck: &mut Checker) {
        let topics: Vec<String> = lock(&self.topics).clone();
        let mut failed = Vec::new();
        for topic in &topics {
            if let Err(e) = self
                .admin
                .delete_topic_in_broker(&self.broker_addr, topic, 5000)
                .await
            {
                failed.push(format!("{topic}:{e}"));
            }
        }
        ck.check(
            "A9 删除本次建的 topic",
            failed.is_empty(),
            &format!("{failed:?}"),
        );
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

// ------------------------------------------------------------------ 共用骨架

/// 造同组两实例、各装一个策略并订阅（未 start）。
///
/// 两个策略分开传：A4/A5 两边同策略，A6 要故意配成「一个正常 + 一个配错机房」，
/// 看 Java 那种「各组各算、互不误吃」的语义在真机上成不成立。
fn pair_with_strategy(
    fx: &Fixture,
    kind: &str,
    strategies: &[std::sync::Arc<dyn AllocateMessageQueueStrategy>; 2],
    topic: &str,
) -> Result<(DefaultLitePullConsumer, DefaultLitePullConsumer), String> {
    let ca = fx.consumer(kind, &format!("{kind}-a"))?;
    let cb = fx.consumer(kind, &format!("{kind}-b"))?;
    for (c, strategy) in [(&ca, &strategies[0]), (&cb, &strategies[1])] {
        c.set_allocate_message_queue_strategy(strategy.clone());
        // 存量消息在 A0 就发完了，新组默认 LAST 会跳过它们 → 收不到任何一条。
        // 这里要验的是「策略真的驱动了收发」，所以从队首起消费。
        c.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        c.subscribe(topic, "*");
    }
    Ok((ca, cb))
}

/// 用**真实**输入（路由给的 mqAll + cid_members 的真实 clientId 当 cidAll）离线跑策略，
/// 并**等线上 `assignment()` 收敛到这份预测**。
///
/// 为什么以预测为收敛条件，而不是「先等不重不漏、再比预测」：哈希环完全可能把 4 个
/// 队列全分给一个实例，另一边在首轮重平衡之前 `assignment()` 本来就是空 —— 那种初始态
/// 同样满足「不重不漏」，比出来的其实是「一边还没算」的快照。（第一版就在这里翻车。）
///
/// Java `RebalanceImpl#rebalanceByTopic` 调策略前会把 mqAll、cidAll 都 `Collections.sort`，
/// 所以这里也得自己排：`all` 由 `sorted_queues` 排好，clientId 按字典序（同 Java
/// `String#compareTo`）。`strategies` 与 `asserted` 一一对应 —— A6 要故意让两边配不同策略。
async fn wait_until_prediction_converged(
    group: &str,
    all: &[MessageQueue],
    cid_members: &[&DefaultLitePullConsumer],
    asserted: &[&DefaultLitePullConsumer],
    strategies: &[&dyn AllocateMessageQueueStrategy],
    secs: u64,
) -> (bool, String) {
    let mut cid_all: Vec<String> = cid_members.iter().map(|c| c.client_id()).collect();
    cid_all.sort();
    let mut expected: Vec<BTreeSet<String>> = Vec::with_capacity(asserted.len());
    for (c, strategy) in asserted.iter().zip(strategies) {
        match strategy.allocate(group, &c.client_id(), all, &cid_all) {
            Ok(v) => expected.push(queue_keys(&v)),
            Err(e) => return (false, format!("离线预测失败: {e}")),
        }
    }
    let deadline = now_ms() + (secs as i64) * 1000;
    loop {
        let live: Vec<BTreeSet<String>> = asserted.iter().map(|c| queue_keys(&c.assignment())).collect();
        let matched = live.len() == expected.len()
            && live.iter().zip(expected.iter()).all(|(l, e)| l == e);
        if matched || now_ms() >= deadline {
            let mut parts: Vec<String> = Vec::new();
            for (c, (l, e)) in asserted
                .iter()
                .zip(live.iter().zip(expected.iter()))
            {
                parts.push(format!(
                    "cid={} live=[{}] predict=[{}]",
                    c.client_id(),
                    key_set_text(l),
                    key_set_text(e)
                ));
            }
            parts.push(format!("cidAll={cid_all:?}"));
            return (matched, parts.join(" | "));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// NEARBY 的落点：真实集群只有一个 broker，把队列和客户端都记成同一个机房，
/// 于是 NEARBY 必定走「自己机房」那条分支、等价于内层策略。
///
/// 同时留调用记录 —— 用来证明 rebalance 真的逐个问过队列和客户端的机房，
/// 而不是策略对象换了个名字却没参与分配。
const ROOM: &str = "room1";

struct OneRoom {
    broker_calls: Mutex<Vec<String>>,
    consumer_calls: Mutex<Vec<String>>,
}

impl OneRoom {
    fn new() -> OneRoom {
        OneRoom {
            broker_calls: Mutex::new(Vec::new()),
            consumer_calls: Mutex::new(Vec::new()),
        }
    }
}

impl MachineRoomResolver for OneRoom {
    fn broker_deploy_in(&self, message_queue: &MessageQueue) -> String {
        lock(&self.broker_calls).push(message_queue.get_broker_name().to_string());
        ROOM.to_string()
    }

    fn consumer_deploy_in(&self, client_id: &str) -> String {
        lock(&self.consumer_calls).push(client_id.to_string());
        ROOM.to_string()
    }
}

// ------------------------------------------------------------------ A1 配置面

async fn a1_default_strategy(ck: &mut Checker, fx: &Fixture) {
    let c = match fx.consumer("probe", "probe") {
        Ok(c) => c,
        Err(e) => {
            ck.abort("A1 构造", &e);
            return;
        }
    };
    ck.check(
        "A1 默认策略名 = AVG（Java 字段初值）",
        c.allocate_message_queue_strategy().get_name() == "AVG",
        c.allocate_message_queue_strategy().get_name(),
    );
    let circle: std::sync::Arc<dyn AllocateMessageQueueStrategy> =
        std::sync::Arc::new(AllocateMessageQueueAveragelyByCircle);
    c.set_allocate_message_queue_strategy(circle);
    ck.check(
        "A1 setter 换掉的策略能读回 AVG_BY_CIRCLE",
        c.allocate_message_queue_strategy().get_name() == "AVG_BY_CIRCLE",
        c.allocate_message_queue_strategy().get_name(),
    );
}

// ------------------------------------------------------------------ A2 同组交叉切分

async fn a2_circle_split(ck: &mut Checker, fx: &Fixture, topic: &str, all: &[MessageQueue]) {
    let (ca, cb) = match (
        fx.consumer("circle", "circle-a"),
        fx.consumer("circle", "circle-b"),
    ) {
        (Ok(a), Ok(b)) => (a, b),
        (a, b) => {
            let e = a.err().or_else(|| b.err());
            ck.abort("A2 构造", &e.unwrap_or_else(|| "unknown".to_string()));
            return;
        }
    };
    let circle: std::sync::Arc<dyn AllocateMessageQueueStrategy> =
        std::sync::Arc::new(AllocateMessageQueueAveragelyByCircle);
    for c in [&ca, &cb] {
        c.set_allocate_message_queue_strategy(circle.clone());
        // 存量消息在 A0 就发完了，新组默认 LAST 会跳过它们 → 收不到任何一条。
        // 这里要验的是「交叉切分真的驱动了收发」，所以从队首起消费。
        c.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        c.subscribe(topic, "*");
    }
    if let Err(e) = ca.start().await {
        ck.abort("A2 ca start", &e.to_string());
        return;
    }
    if let Err(e) = cb.start().await {
        ca.shutdown();
        ck.abort("A2 cb start", &e.to_string());
        return;
    }
    let (qa, qb) = wait_split_assignment(&ca, &cb, all.len(), WAIT_SECONDS).await;
    let ka = queue_keys(&qa);
    let kb = queue_keys(&qb);
    let overlap: BTreeSet<String> = ka.intersection(&kb).cloned().collect();
    let union: BTreeSet<String> = ka.union(&kb).cloned().collect();
    ck.check(
        "A2 两实例分配无交集（broker 认到两个 clientId）",
        overlap.is_empty(),
        &format!("overlap=[{}]", key_set_text(&overlap)),
    );
    ck.check(
        &format!("A2 并集覆盖全部 {} 个队列", all.len()),
        union == queue_keys(all),
        &format!("a={} b={}", ka.len(), kb.len()),
    );
    // 环形分配的签名：拿到的是「按下标取模」的交叉队列而非连续段
    // （4 队列 / 2 实例 → 各 2 条且下标步长为 2；AVG 会给连续两段）。
    let pos_a: Vec<usize> = all
        .iter()
        .enumerate()
        .filter(|(_, mq)| ka.contains(&format!("{}#{}", mq.broker_name, mq.queue_id)))
        .map(|(i, _)| i)
        .collect();
    let circle_shape = pos_a.len() == 2
        && pos_a
            .windows(2)
            .all(|w| (w[1] - w[0]) % 2 == 0);
    ck.check(
        "A2 分配形状是交叉（步长 2），不是 AVG 的连续段",
        all.len() != 4 || circle_shape,
        &format!("posA={pos_a:?}"),
    );
    // 交叉切分后两边各自收自己那半边，合起来覆盖全部 12 条。
    //
    // ⚠ 不断言「互不重叠」：对端心跳落地前 cidAll 只有自己，第一轮 AVG_BY_CIRCLE 会把
    // **全部**队列分给自己（Java 同款行为），撤队只清 next_offset / last_commit，
    // 已经进了本地缓冲的消息留在 FIFO 里（与 Python 同口径，见 pull_consumer.rs:1647）。
    // 所以这里只要求「两边都真收到、合起来不漏」。
    let ga = drain_for(&ca, 8).await;
    let gb = drain_for(&cb, 8).await;
    let ba = bodies(&ga);
    let bb = bodies(&gb);
    let joined: BTreeSet<String> = ba.union(&bb).cloned().collect();
    let shared: BTreeSet<String> = ba.intersection(&bb).cloned().collect();
    ck.check(
        &format!("A2 两实例合起来收到全部 {N_MSG} 条"),
        joined == expected_bodies(topic) && !ba.is_empty() && !bb.is_empty(),
        &format!(
            "a={} b={} union={} 撤队前抢到的重叠={}",
            ba.len(),
            bb.len(),
            joined.len(),
            shared.len()
        ),
    );
    ca.shutdown();
    cb.shutdown();
}

// ------------------------------------------------------------------ A3 CONFIG 驱动收发

async fn a3_config_halves(ck: &mut Checker, fx: &Fixture, topic: &str, all: &[MessageQueue]) {
    let half = all.len() / 2;
    let (half_a, half_b) = (all[..half].to_vec(), all[half..].to_vec());
    let (ca, cb) = match (
        fx.consumer("config-a", "config-a"),
        fx.consumer("config-b", "config-b"),
    ) {
        (Ok(a), Ok(b)) => (a, b),
        (a, b) => {
            let e = a.err().or_else(|| b.err());
            ck.abort("A3 构造", &e.unwrap_or_else(|| "unknown".to_string()));
            return;
        }
    };
    ca.set_allocate_message_queue_strategy(std::sync::Arc::new(AllocateMessageQueueByConfig::new(half_a.clone())));
    cb.set_allocate_message_queue_strategy(std::sync::Arc::new(AllocateMessageQueueByConfig::new(half_b.clone())));
    for c in [&ca, &cb] {
        c.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        c.subscribe(topic, "*");
    }
    if let Err(e) = ca.start().await {
        ck.abort("A3 ca start", &e.to_string());
        return;
    }
    if let Err(e) = cb.start().await {
        ca.shutdown();
        ck.abort("A3 cb start", &e.to_string());
        return;
    }
    let a_assigned = wait_assignment(&ca, WAIT_SECONDS).await;
    let b_assigned = wait_assignment(&cb, WAIT_SECONDS).await;
    ck.check(
        "A3 CONFIG 只给配置进去的队列（无视 mqAll/cidAll）",
        queue_keys(&a_assigned) == queue_keys(&half_a)
            && queue_keys(&b_assigned) == queue_keys(&half_b),
        &format!(
            "a=[{}] b=[{}]",
            key_set_text(&queue_keys(&a_assigned)),
            key_set_text(&queue_keys(&b_assigned))
        ),
    );
    let ga = drain_for(&ca, 8).await;
    let gb = drain_for(&cb, 8).await;
    let keys_a = queue_keys(&half_a);
    let keys_b = queue_keys(&half_b);
    let all_a = bodies(&ga);
    let all_b = bodies(&gb);
    let on_a = bodies_on(&ga, &keys_a);
    let on_b = bodies_on(&gb, &keys_b);
    ck.check(
        "A3 CONFIG 消费者只收到自己配置队列里的消息",
        all_a == on_a && all_b == on_b,
        &format!(
            "a={} aOnCfg={} b={} bOnCfg={}",
            all_a.len(),
            on_a.len(),
            all_b.len(),
            on_b.len()
        ),
    );
    let union: BTreeSet<String> = all_a.union(&all_b).cloned().collect();
    let inter: BTreeSet<String> = all_a.intersection(&all_b).cloned().collect();
    ck.check(
        &format!("A3 两半合起来恰好覆盖全部 {N_MSG} 条且互不重叠"),
        union == expected_bodies(topic) && inter.is_empty(),
        &format!("union={} inter={}", union.len(), inter.len()),
    );
    ca.shutdown();
    cb.shutdown();
}

// ------------------------------------------------------------------ A4 一致性哈希环

async fn a4_consistent_hash(ck: &mut Checker, fx: &Fixture, topic: &str, all: &[MessageQueue]) {
    let ch = std::sync::Arc::new(AllocateMessageQueueConsistentHash::new());
    let strategy: std::sync::Arc<dyn AllocateMessageQueueStrategy> = ch.clone();
    let (ca, cb) = match pair_with_strategy(fx, "chash", &[strategy.clone(), strategy.clone()], topic)
    {
        Ok(p) => p,
        Err(e) => {
            ck.abort("A4 构造", &e);
            return;
        }
    };
    let got = ca.allocate_message_queue_strategy();
    ck.check(
        "A4 替换后策略名 = CONSISTENT_HASH",
        got.get_name() == "CONSISTENT_HASH",
        got.get_name(),
    );
    if let Err(e) = ca.start().await {
        ck.abort("A4 ca start", &e.to_string());
        return;
    }
    if let Err(e) = cb.start().await {
        ca.shutdown();
        ck.abort("A4 cb start", &e.to_string());
        return;
    }
    // 以「线上分配 == 真实输入下的离线预测」为收敛条件（详见 helper 的注释）。
    let (matched, detail) = wait_until_prediction_converged(
        &fx.group_name("chash"),
        all,
        &[&ca, &cb],
        &[&ca, &cb],
        &[strategy.as_ref(), strategy.as_ref()],
        WAIT_SECONDS,
    )
    .await;
    ck.check("A4 线上分配收敛到一致性环的离线预测", matched, &detail);
    let qa = ca.assignment();
    let qb = cb.assignment();
    let ka = queue_keys(&qa);
    let kb = queue_keys(&qb);
    let overlap: BTreeSet<String> = ka.intersection(&kb).cloned().collect();
    let union: BTreeSet<String> = ka.union(&kb).cloned().collect();
    ck.check(
        "A4 两实例分配无交集",
        overlap.is_empty(),
        &format!("overlap=[{}]", key_set_text(&overlap)),
    );
    ck.check(
        &format!("A4 并集覆盖全部 {} 个队列", all.len()),
        union == queue_keys(all),
        &format!("a={} b={}", ka.len(), kb.len()),
    );
    // 端到端：不管环怎么偏，两边合起来要把 12 条收全。
    let ga = drain_for(&ca, 8).await;
    let gb = drain_for(&cb, 8).await;
    let joined: BTreeSet<String> = bodies(&ga).union(&bodies(&gb)).cloned().collect();
    ck.check(
        &format!("A4 两实例合起来收到全部 {N_MSG} 条（环真的在驱动收发）"),
        joined == expected_bodies(topic),
        &format!("union={}", joined.len()),
    );
    ca.shutdown();
    cb.shutdown();
}

// ------------------------------------------------------------------ A5 NEARBY 包环

async fn a5_nearby(ck: &mut Checker, fx: &Fixture, topic: &str, all: &[MessageQueue]) {
    let inner: std::sync::Arc<dyn AllocateMessageQueueStrategy> =
        std::sync::Arc::new(AllocateMessageQueueConsistentHash::new());
    let resolver = std::sync::Arc::new(OneRoom::new());
    let nearby: std::sync::Arc<dyn AllocateMessageQueueStrategy> =
        std::sync::Arc::new(AllocateMachineRoomNearby::new(inner.clone(), resolver.clone()));
    let (ca, cb) = match pair_with_strategy(fx, "nearby", &[nearby.clone(), nearby.clone()], topic) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("A5 构造", &e);
            return;
        }
    };
    let got = ca.allocate_message_queue_strategy();
    ck.check(
        "A5 装饰后的策略名 = MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
        got.get_name() == "MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
        got.get_name(),
    );
    if let Err(e) = ca.start().await {
        ck.abort("A5 ca start", &e.to_string());
        return;
    }
    if let Err(e) = cb.start().await {
        ca.shutdown();
        ck.abort("A5 cb start", &e.to_string());
        return;
    }
    // 单机房 ⇒ NEARBY 与内层环的结果**一字不差**，所以直接以内层策略的离线预测当收敛条件：
    // 收敛了就等于「装饰器原样透传了内层切分，且真的驱动了 rebalance」。
    let (matched, detail) = wait_until_prediction_converged(
        &fx.group_name("nearby"),
        all,
        &[&ca, &cb],
        &[&ca, &cb],
        &[inner.as_ref(), inner.as_ref()],
        WAIT_SECONDS,
    )
    .await;
    ck.check("A5 NEARBY 的线上分配 == 内层环的离线预测", matched, &detail);
    let ka = queue_keys(&ca.assignment());
    let kb = queue_keys(&cb.assignment());
    let overlap: BTreeSet<String> = ka.intersection(&kb).cloned().collect();
    let union: BTreeSet<String> = ka.union(&kb).cloned().collect();
    ck.check(
        "A5 NEARBY 两实例分配无交集且不漏",
        overlap.is_empty() && union == queue_keys(all),
        &format!("a={} b={} overlap={}", ka.len(), kb.len(), overlap.len()),
    );
    // resolver 真的被 rebalance 调用过，且看到的是真实 brokerName + 两个真实 clientId。
    let broker_calls = lock(&resolver.broker_calls).clone();
    let consumer_calls = lock(&resolver.consumer_calls).clone();
    let seen: BTreeSet<String> = broker_calls.iter().cloned().collect();
    let real_brokers: BTreeSet<String> = all.iter().map(|m| m.broker_name.clone()).collect();
    ck.check(
        &format!("A5 resolver 被逐个队列问过机房（{} 次）", broker_calls.len()),
        !broker_calls.is_empty() && seen == real_brokers,
        &format!("brokers={seen:?}"),
    );
    let both_cids = consumer_calls.contains(&ca.client_id())
        && consumer_calls.contains(&cb.client_id());
    ck.check(
        "A5 resolver 被问过两个真实 clientId",
        both_cids,
        &format!("calls={consumer_calls:?}"),
    );
    ca.shutdown();
    cb.shutdown();
}

// ------------------------------------------------------------------ A6 机房配错

async fn a6_machine_room_starved(ck: &mut Checker, fx: &Fixture, topic: &str, all: &[MessageQueue]) {
    // 真实 brokerName 是 `broker-a`，Java 的 `split("@")` 只切出 1 段 → 白名单再怎么写都筛不出队列。
    let room_strategy = AllocateMessageQueueByMachineRoom::new([ROOM]);
    ck.check(
        "A6 策略名 = MACHINE_ROOM 且白名单能读回",
        room_strategy.get_name() == "MACHINE_ROOM" && room_strategy.get_consumeridcs().contains(ROOM),
        format!("idcs={:?}", room_strategy.get_consumeridcs()).as_str(),
    );
    let room: std::sync::Arc<dyn AllocateMessageQueueStrategy> = std::sync::Arc::new(room_strategy);
    // 对照组：同组另一个消费者用默认 AVG。两边各自算策略（Java 就是各算各的），
    // 对照组能分到队列 ⇒ 这一组的心跳注册 + 重平衡确实跑起来了，
    // 于是「A6 一边为空」只能归因于机房筛选，而不是链路没通。
    let avg: std::sync::Arc<dyn AllocateMessageQueueStrategy> =
        std::sync::Arc::new(AllocateMessageQueueAveragely);
    let (ca, cb) = match pair_with_strategy(fx, "room", &[room.clone(), avg.clone()], topic) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("A6 构造", &e);
            return;
        }
    };
    if let Err(e) = ca.start().await {
        ck.abort("A6 ca start", &e.to_string());
        return;
    }
    if let Err(e) = cb.start().await {
        ca.shutdown();
        ck.abort("A6 cb start", &e.to_string());
        return;
    }
    let ctrl = wait_assignment(&cb, WAIT_SECONDS).await;
    ck.check(
        "A6 同组对照组（AVG）正常分到队列",
        !ctrl.is_empty(),
        &format!("ctrl=[{}]", key_set_text(&queue_keys(&ctrl))),
    );
    let starved = ca.assignment();
    ck.check(
        "A6 机房不匹配真实 brokerName → 一条都不分（不报错也不误吃）",
        starved.is_empty(),
        &format!("assignment=[{}]", key_set_text(&queue_keys(&starved))),
    );
    // 两边各自算策略（Java 就是各算各的）：配错机房的一方算出空，对照组按**两个** cid
    // 算 AVG 只拿到自己那半边 —— 它没有替配错的那位兜底，这才是 Java 的语义。
    let (matched, detail) = wait_until_prediction_converged(
        &fx.group_name("room"),
        all,
        &[&ca, &cb],
        &[&ca, &cb],
        &[room.as_ref(), avg.as_ref()],
        WAIT_SECONDS,
    )
    .await;
    ck.check("A6 两边线上分配各自收敛到自己策略的离线预测", matched, &detail);
    let ga = drain_for(&ca, 5).await;
    ck.check(
        "A6 被饿死的一方 poll 不到消息也不抛错",
        bodies(&ga).is_empty(),
        &format!("got={}", ga.len()),
    );
    ca.shutdown();
    cb.shutdown();
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
    println!("== live allocation-strategy check, namesrv={namesrv} stamp={stamp} ==");

    a1_default_strategy(&mut ck, &fx).await;

    let topic = fx.topic_name();
    if let Err(e) = fx.create_topic(&topic, QUEUE_NUMS).await {
        ck.abort("建 topic", &e);
        return ck;
    }
    let all = sorted_queues(route_queues(&fx, &topic).await);
    if all.len() != QUEUE_NUMS as usize {
        ck.abort("路由队列不足", &format!("got {}", all.len()));
        fx.cleanup(&mut ck).await;
        return ck;
    }
    let sent = fx.produce_alternating(&topic).await;
    ck.check(
        &format!("A0 生产 {N_MSG} 条成功"),
        sent == N_MSG,
        &format!("sentOk={sent}"),
    );

    a2_circle_split(&mut ck, &fx, &topic, &all).await;
    a3_config_halves(&mut ck, &fx, &topic, &all).await;
    a4_consistent_hash(&mut ck, &fx, &topic, &all).await;
    a5_nearby(&mut ck, &fx, &topic, &all).await;
    a6_machine_room_starved(&mut ck, &fx, &topic, &all).await;
    fx.cleanup(&mut ck).await;
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
