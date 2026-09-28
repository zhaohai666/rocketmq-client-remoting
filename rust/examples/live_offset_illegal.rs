//! `OFFSET_ILLEGAL` 纠错分支（Java `DefaultMQPushConsumerImpl:402-427`）真机验证。
//!
//! 与 `python/verify_offset_illegal_live.py`、`cpp/examples/live_offset_illegal.cpp`、
//! dotnet 的对应场景同题、逐条对应。
//!
//! 这条分支做四件事：位点改用 broker 给的修正值（`setNextOffset`）→ 丢掉这条队列上
//! 已取回未消费的消息（`ProcessQueue.setDropped(true)`）→ 把修正位点**立刻**落盘
//! （`updateAndFreezeOffset` + `persist`）→ 撤掉队列让 rebalance 按修正位点重建
//! （`removeProcessQueue` + `rebalanceImmediately`）。离线单测只能锁住本地状态怎么清、
//! 哪个 ack 被作废，下面两件事只有真集群能证明：
//!
//! - S1 「丢队列」——broker 判定位点非法时，队列上**已取回但还没消费/没 ack** 的消息必须
//!   整批作废。做法：让 listener 卡住第一条（在途 1 条、缓冲里 2 条），再用
//!   `resetOffsetByQueueId` 把位点重置到 3（服务端重置 ⇒ 下一笔 pull 被
//!   `PullMessageProcessor:539-545` 短路成 OFFSET_RESET ⇒ 客户端 OFFSET_ILLEGAL）。
//!   修复前：缓冲里的第 1、2 条照常投递（listener 实收 3 条）；修复后：只剩在途的
//!   第 0 条，且它的 ack 因队列已被丢（Java `ConsumeMessageConcurrentlyService:267`）
//!   而作废。最后再发第 4 条，验证重建后的队列从修正位点续跑、冻结已随重建解除
//!   （新消息的 ack 让 broker 上的位点继续前进到 4）。
//!
//! - S2 「立刻落盘」——纠错后的位点必须马上推给 broker，不能等周期落盘。做法：利用
//!   `resetOffsetByQueueId` 两笔 RPC 非原子（第 1 笔 commitOffset 无区间校验先落库、
//!   第 2 笔 222 被 `resetOffsetInner` 拒绝，见 `live_admin.rs` 的实测）的既有行为，
//!   把 broker 上的已提交位点做成非法值 103，再让一个
//!   `persist_consumer_offset_interval_millis = 60000` 的新消费者从 103 起拉。窗口内
//!   唯一能把 103 写回 3（maxOffset）的路径就是纠错分支自带的那次 persist，且全程零投递。
//!
//! 用法：
//! ```text
//! cargo run --example live_offset_illegal -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{
    mq_key, ConsumerConfig, DefaultMQPushConsumer,
};
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

const QUEUES: i32 = 1;
const MSGS: i64 = 3;
/// 非法位点：远超 maxOffset，broker 必回 `PULL_OFFSET_MOVED`。
const ILLEGAL_TARGET: i64 = 103;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

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
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// 闸门 listener：把到达的 queueOffset 记下来，然后**卡住**本批（模拟"已取回还没 ack"）。
///
/// 分发循环是单任务的：卡住第一批 ⇒ 单队列上只有 1 条在途，其余留在缓冲里，
/// 正是 S1 要的窗口。真机上这活由 `spawn_blocking` 承载，阻塞安全。
struct GatedListener {
    state: Mutex<GateState>,
    cv: Condvar,
}

struct GateState {
    arrivals: Vec<i64>,
    released: bool,
}

impl GatedListener {
    fn new() -> Arc<GatedListener> {
        Arc::new(GatedListener {
            state: Mutex::new(GateState {
                arrivals: Vec::new(),
                released: false,
            }),
            cv: Condvar::new(),
        })
    }

    fn arrivals(&self) -> Vec<i64> {
        lock(&self.state).arrivals.clone()
    }

    fn release(&self) {
        lock(&self.state).released = true;
        self.cv.notify_all();
    }
}

impl MessageListenerConcurrently for GatedListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let mut st = lock(&self.state);
        for m in msgs {
            st.arrivals.push(m.queue_offset);
        }
        // 上限只防死锁：正常路径下由 release 显式放行，且必须晚于纠错
        let deadline = Instant::now() + Duration::from_secs(90);
        while !st.released {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let (guard, _) = self
                .cv
                .wait_timeout(st, deadline - now)
                .unwrap_or_else(|e| e.into_inner());
            st = guard;
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

/// 只记到达的 queueOffset：这一趟关心的是"哪些不该来"，不是消息内容。
struct RecordingListener {
    arrivals: Mutex<Vec<i64>>,
}

impl RecordingListener {
    fn new() -> Arc<RecordingListener> {
        Arc::new(RecordingListener {
            arrivals: Mutex::new(Vec::new()),
        })
    }

    fn arrivals(&self) -> Vec<i64> {
        lock(&self.arrivals).clone()
    }
}

impl MessageListenerConcurrently for RecordingListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let mut arrivals = lock(&self.arrivals);
        for m in msgs {
            arrivals.push(m.queue_offset);
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

struct Fixture {
    namesrv: String,
    stamp: u64,
    broker_addr: String,
    admin: DefaultMQAdminExt,
    producer: DefaultMQProducer,
}

impl Fixture {
    async fn new(namesrv: &str) -> Result<Fixture, String> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let admin = DefaultMQAdminExt::with_config(AdminConfig {
            instance_name: format!("rust-live-oil-admin-{stamp}"),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        admin
            .start()
            .await
            .map_err(|e| format!("admin start failed: {e}"))?;
        let producer = DefaultMQProducer::new(&format!("rust-live-oil-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        producer
            .start()
            .await
            .map_err(|e| format!("producer start failed: {e}"))?;

        // 端口开着 != broker 已注册到 nameServer：按 Python 那样轮询
        let deadline = Instant::now() + Duration::from_secs(40);
        let broker_addr = loop {
            match admin.fetch_broker_cluster_info().await {
                Ok(info) if !info.broker_addr_table.is_empty() => {
                    break info
                        .get_broker_addrs()
                        .first()
                        .cloned()
                        .ok_or_else(|| "cluster info has no broker address".to_string())?;
                }
                _ => {}
            }
            if Instant::now() > deadline {
                return Err("nameServer 在预算内没有返回任何 broker".to_string());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        };
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            stamp,
            broker_addr,
            admin,
            producer,
        })
    }

    fn topic(&self, tag: &str) -> String {
        format!("RustLiveOil{tag}{}", self.stamp)
    }

    async fn create_topic(&self, topic: &str) -> Result<(), String> {
        self.admin
            .create_topic(MixAll::DEFAULT_TOPIC, topic, QUEUES, 0)
            .await
            .map_err(|e| format!("create topic {topic} failed: {e}"))?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    /// 该 topic 在路由上的真实队列列表（`%RETRY%<group>` 只有 1 个队列，
    /// 照主 topic 的队列数去查会误报）。
    async fn queues(&self, topic: &str) -> Vec<MessageQueue> {
        match self.admin.examine_topic_route(topic).await {
            Ok(route) => route
                .get_all_message_queue(topic)
                .into_iter()
                .map(|k| MessageQueue::new(&k.topic, &k.broker_name, k.queue_id))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// 单队列 topic 的队列 key（消费者侧 `_mq_map` 的键口径）。
    async fn queue_key(&self, topic: &str) -> Result<String, String> {
        self.queues(topic)
            .await
            .first()
            .map(mq_key)
            .ok_or_else(|| format!("route of {topic} has no queue"))
    }

    /// broker 上该队列的已提交位点；`None` = 查无记录（`QUERY_NOT_FOUND`）。
    async fn committed(&self, group: &str, topic: &str) -> Vec<(MessageQueue, Option<i64>)> {
        let mut out = Vec::new();
        for mq in self.queues(topic).await {
            let off = self.admin.examine_consumer_offset(group, &mq).await.unwrap_or(None);
            out.push((mq, off));
        }
        out
    }

    fn consumer(
        &self,
        group: &str,
        role: &str,
        topic: &str,
        batch_size: i32,
        from_first: bool,
        persist_millis: Option<u64>,
    ) -> Result<DefaultMQPushConsumer, String> {
        let cfg = ConsumerConfig {
            consumer_group: group.to_string(),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("rust-live-oil-{role}-{}", self.stamp),
            consume_from_where: if from_first {
                ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string()
            } else {
                ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET.to_string()
            },
            consume_message_batch_max_size: batch_size,
            ..Default::default()
        };
        let consumer = DefaultMQPushConsumer::with_config(cfg)
            .map_err(|e| format!("build consumer failed: {e}"))?;
        if let Some(ms) = persist_millis {
            consumer.set_persist_consumer_offset_interval_millis(ms);
        }
        consumer
            .subscribe(topic, "*")
            .map_err(|e| format!("subscribe {topic} failed: {e}"))?;
        Ok(consumer)
    }

    fn shutdown(&self) {
        self.producer.shutdown();
        self.admin.shutdown();
    }
}

fn fmt(rows: &[(MessageQueue, Option<i64>)]) -> String {
    rows.iter()
        .map(|(mq, off)| {
            format!(
                "q{}:{}",
                mq.queue_id,
                off.map(|o| o.to_string()).unwrap_or_else(|| "None".to_string())
            )
        })
        .collect::<Vec<String>>()
        .join(" ")
}

async fn s1_drop_and_rebuild(fx: &Fixture, ck: &mut Checker) {
    let topic = fx.topic("Drop");
    let group = format!("GID_rust_live_oil_drop_{}", fx.stamp);
    if let Err(e) = fx.create_topic(&topic).await {
        ck.check("S1-create topic", false, &e);
        return;
    }
    let key = match fx.queue_key(&topic).await {
        Ok(k) => k,
        Err(e) => {
            ck.check("S1-queue key", false, &e);
            return;
        }
    };

    let sink = GatedListener::new();
    let c = match fx.consumer(&group, "drop", &topic, 1, true, None) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S1-consumer", false, &e);
            return;
        }
    };
    c.set_message_listener_concurrently(sink.clone());
    if let Err(e) = c.start().await {
        ck.check("S1-start", false, &format!("{e}"));
        c.shutdown();
        return;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    for i in 0..MSGS {
        let mut msg = Message::new(&topic, Some(format!("ilo-{i}").as_bytes()));
        if let Err(e) = fx.producer.send(&mut msg, Some(20000), None).await {
            ck.check("S1-send", false, &format!("{e}"));
            sink.release();
            c.shutdown();
            return;
        }
    }
    println!("S1: 已发送 {MSGS} 条（单队列），等 listener 卡住第 1 条...");

    let ready = poll_until(
        || sink.arrivals().len() == 1 && c.cached_message_count(&key) == (MSGS - 1) as usize,
        30,
    )
    .await;
    ck.check(
        &format!(
            "S1-窗口就绪：1 条在途（listener 被闸住）+ {} 条留在缓冲",
            MSGS - 1
        ),
        ready,
        &format!(
            "pending={} arrivals={:?}",
            c.cached_message_count(&key),
            sink.arrivals()
        ),
    );

    // 服务端重置到 maxOffset：RPC2 写 resetOffsetTable，下一笔 pull 命中
    // PullMessageProcessor:539-545 被短路成 OFFSET_RESET（nextBeginOffset=修正值）
    if let Err(e) = fx
        .admin
        .reset_offset_by_queue_id(&fx.broker_addr, &group, &topic, 0, MSGS)
        .await
    {
        println!("  [WARN] resetOffsetByQueueId 第 2 笔 RPC 被拒（本场景预期）: {e}");
    }
    println!("S1: 已发出 resetOffsetByQueueId(->{MSGS})，等下一笔 pull 取走服务端重置...");

    // 发现延迟 = 客户端在途长轮询的返回时间 + broker 侧巡检周期：本端口按下发
    // suspendTimeoutMillis=20000（Java PullAPIWrapper.brokerSuspendMaxTimeMillis 默认值
    // 20s）请求挂起，broker 的 PullRequestHoldService 每 5s 巡检一次到期请求，
    // 命中前那笔 pull 不会重读 resetOffsetTable。实测 24.3s（Python），与 Java 同构
    // （长轮询语义如此，不是缺陷）——这里给 45s 余量。
    let bumped = poll_until(|| c.queue_epoch(&key) >= 1, 45).await;
    ck.check(
        "S1-broker 判定位点非法后本端丢弃该队列（ProcessQueue.setDropped：代号 +1）",
        bumped,
        &format!("epoch={} arrivals={:?}", c.queue_epoch(&key), sink.arrivals()),
    );

    // committed 的等待要点轮询：位点由本端的 persist 推给 broker，不是本地状态
    let mut ok = false;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let rows = fx.committed(&group, &topic).await;
        if !rows.is_empty() && rows.iter().all(|(_, o)| *o == Some(MSGS)) {
            ok = true;
            break;
        }
        if Instant::now() >= deadline {
            println!("  [WARN] committed={}", fmt(&rows));
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        &format!("S1-broker 上的位点停在修正值 {MSGS}（本场景两笔重置 RPC 已先写过一次，弱断言）"),
        ok,
        &fmt(&fx.committed(&group, &topic).await),
    );

    sink.release();
    tokio::time::sleep(Duration::from_secs(6)).await;
    let arr = sink.arrivals();
    ck.check(
        &format!(
            "S1-缓冲里已取回的 {} 条被整批作废（第 1、2 条永不投递）",
            MSGS - 1
        ),
        !arr.is_empty() && !arr.contains(&1) && !arr.contains(&2),
        &format!("arrivals={arr:?}"),
    );

    let mut msg = Message::new(&topic, Some(b"ilo-after"));
    if let Err(e) = fx.producer.send(&mut msg, Some(20000), None).await {
        ck.check("S1-send after", false, &format!("{e}"));
        c.shutdown();
        return;
    }
    let got4 = poll_until(|| sink.arrivals().contains(&MSGS), 20).await;
    let arr = sink.arrivals();
    ck.check(
        &format!(
            "S1-重建后的队列从修正位点续拉（第 {MSGS} 条新消息正常投递，历史拿过的不重投）"
        ),
        got4 && !arr.contains(&1) && !arr.contains(&2),
        &format!("arrivals={arr:?}"),
    );

    let mut ok = false;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let rows = fx.committed(&group, &topic).await;
        if !rows.is_empty() && rows.iter().all(|(_, o)| *o == Some(MSGS + 1)) {
            ok = true;
            break;
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    ck.check(
        &format!("S1-冻结随重建解除（新消息的 ack 让 broker 位点继续前进到 {}）", MSGS + 1),
        ok,
        &format!("committed={}", fmt(&fx.committed(&group, &topic).await)),
    );
    c.shutdown();
    cleanup(fx, &topic, &group).await;
}

async fn s2_immediate_persist(fx: &Fixture, ck: &mut Checker) {
    let topic = fx.topic("Persist");
    let group = format!("GID_rust_live_oil_persist_{}", fx.stamp);
    if let Err(e) = fx.create_topic(&topic).await {
        ck.check("S2-create topic", false, &e);
        return;
    }
    let sink = RecordingListener::new();
    let c = match fx.consumer(&group, "persist", &topic, 1, true, None) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S2-consumer", false, &e);
            return;
        }
    };
    c.set_message_listener_concurrently(sink.clone());
    if let Err(e) = c.start().await {
        ck.check("S2-start", false, &format!("{e}"));
        c.shutdown();
        return;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    for i in 0..MSGS {
        let mut msg = Message::new(&topic, Some(format!("ilo2-{i}").as_bytes()));
        if let Err(e) = fx.producer.send(&mut msg, Some(20000), None).await {
            ck.check("S2-send", false, &format!("{e}"));
            c.shutdown();
            return;
        }
    }
    let consumed = poll_until(|| sink.arrivals().len() == MSGS as usize, 30).await;
    ck.check(
        &format!("S2-前置：消费者先正常消费掉 {MSGS} 条"),
        consumed,
        &format!("arrivals={:?}", sink.arrivals()),
    );
    // shutdown 的落盘是 spawn 出去的（不等它），必须先等 broker 上出现 3 再种非法值，
    // 否则那次迟到落盘会把下面种进去的 103 覆盖回去。
    c.shutdown();
    let persisted = {
        let mut ok = false;
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let rows = fx.committed(&group, &topic).await;
            if !rows.is_empty() && rows.iter().all(|(_, o)| *o == Some(MSGS)) {
                ok = true;
                break;
            }
            if Instant::now() >= deadline {
                println!("  [WARN] shutdown persist 未落盘: {}", fmt(&rows));
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        ok
    };
    ck.check(
        "S2-前置：关停落盘把位点写到 maxOffset",
        persisted,
        &fmt(&fx.committed(&group, &topic).await),
    );

    let mut rejected = false;
    let mut remark = String::new();
    match fx
        .admin
        .reset_offset_by_queue_id(&fx.broker_addr, &group, &topic, 0, ILLEGAL_TARGET)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            rejected = true;
            remark = format!("{e}").chars().take(140).collect();
        }
    }
    ck.check(
        "S2-前置：越界目标被 resetOffsetInner 拒绝（第 2 笔 RPC）",
        rejected,
        &remark,
    );
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        &format!("S2-前置：第 1 笔 commitOffset 已把非法位点 {ILLEGAL_TARGET} 落库（两笔 RPC 非原子）"),
        !rows.is_empty() && rows.iter().all(|(_, o)| *o == Some(ILLEGAL_TARGET)),
        &fmt(&rows),
    );

    // 周期落盘拉长到 60s：窗口内唯一能改写 broker 位点的路径是纠错分支自带的立即 persist
    let l2 = RecordingListener::new();
    let c2 = match fx.consumer(&group, "persist2", &topic, 1, true, Some(60_000)) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S2-consumer2", false, &e);
            return;
        }
    };
    c2.set_message_listener_concurrently(l2.clone());
    let t0 = Instant::now();
    if let Err(e) = c2.start().await {
        ck.check("S2-start2", false, &format!("{e}"));
        c2.shutdown();
        return;
    }
    println!(
        "S2: 新消费者从非法位点 {ILLEGAL_TARGET} 起拉，等纠错把 broker 位点写回 {MSGS}（周期落盘=60s）..."
    );
    let mut ok = false;
    let mut last;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        last = fx.committed(&group, &topic).await;
        if !last.is_empty() && last.iter().all(|(_, o)| *o == Some(MSGS)) {
            ok = true;
            break;
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let elapsed = t0.elapsed();
    ck.check(
        &format!(
            "S2-broker 位点由 {ILLEGAL_TARGET} 纠回 {MSGS}（纠错分支自带的那次 persist）"
        ),
        ok,
        &format!("elapsed={:.1}s committed={}", elapsed.as_secs_f64(), fmt(&last)),
    );

    // 再等一个静默窗口：位点被纠回后不会回头重投 0..2，也不会再被改写
    tokio::time::sleep(Duration::from_secs(6)).await;
    ck.check(
        "S2-全程零投递（修正位点落在历史消息之后，一条都不下发）",
        l2.arrivals().is_empty(),
        &format!("arrivals={:?}", l2.arrivals()),
    );
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        &format!("S2-静默窗口后位点仍停在 {MSGS}"),
        !rows.is_empty() && rows.iter().all(|(_, o)| *o == Some(MSGS)),
        &fmt(&rows),
    );
    c2.shutdown();
    cleanup(fx, &topic, &group).await;
}

async fn cleanup(fx: &Fixture, topic: &str, group: &str) {
    if let Err(e) = fx.admin.delete_topic(topic, None).await {
        println!("    (deleteTopic({topic}) 失败: {e})");
    } else {
        println!("    (deleteTopic({topic}) OK)");
    }
    if let Err(e) = fx
        .admin
        .delete_subscription_group(&fx.broker_addr, group, true)
        .await
    {
        println!("    (deleteSubscriptionGroup({group}) 失败: {e})");
    }
}

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let fx = match Fixture::new(namesrv).await {
        Ok(fx) => fx,
        Err(e) => {
            ck.check("集群探活", false, &e);
            return ck;
        }
    };
    ck.check("集群探活", true, &format!("broker={}", fx.broker_addr));
    s1_drop_and_rebuild(&fx, &mut ck).await;
    s2_immediate_persist(&fx, &mut ck).await;
    fx.shutdown();
    ck
}

fn report(ck: &mut Checker) {
    println!("\n== 结果：{} PASS / {} FAIL ==", ck.passed, ck.failed.len());
    for f in &ck.failed {
        println!("  FAILED: {f}");
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
