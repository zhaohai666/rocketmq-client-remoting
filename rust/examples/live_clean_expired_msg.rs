//! cleanExpiredMsg 挂起逃生口真机验证（Java `ConsumeMessageConcurrentlyService:68-88/192-200`
//! ＋ `ProcessQueue.cleanExpiredMsg:75-127`）。
//!
//! 与 `python/verify_clean_expired_msg_live.py`、`cpp/examples/live_clean_expired_msg.cpp`、
//! C# 的对应场景同题、逐条对应（A0–A5）。
//!
//! 离线单测（`src/client/consumer.rs` 的 `clean_expired_queue_*` 与
//! `send_back_batch_skips_entries_swept_away`）锁的是**判据**（选条/阈值/上限/摘除闸门/
//! 跳过回投）；这里锁真机上两件离线锁不住的事：
//!   A. **清扫真的会开火**：需要一个 listener 挂着不返回超过 consumeTimeout 分钟，清扫线程
//!      把这条消息 sendMessageBack（delayLevel 3）——整条链路上客户端不能丢它、也不能投两次。
//!   B. **重投真的回到 broker**：%RETRY%<group> 的拉取循环独立于挂住的消费线程，消息必须
//!      重新出现在本地缓冲、被第二次投递且 reconsumeTimes=1（broker 侧计数）。
//!
//! 这条路径坏掉的样子是**静默**的：卡住的消息把该队列位点与分发循环一起钉死，没有任何异常
//! 或超时可见，只能从「消息发了却永远不来第二次」反推。所以必须真机跑出「挂起 → 清扫 →
//! 重投到达」的完整证据链，光靠「客户端不报错」什么都证明不了。
//!
//! 场景（`consume_timeout = 1`，清扫周期与阈值都是 1 分钟；Java 的过期判据是**严格大于**，
//! 所以清扫在第二个 tick 命中，约 start+120s）：
//!   A0 业务队列已分配（排除自动订阅的 %RETRY%<group>）。
//!   A1 首投在 30s 内到达并挂住；reconsumeTimes=0；期间在册视图里能看到这条消息
//!      且带 CONSUME_START_TIME（本轮盖章）。
//!   A2 清扫命中（**核心判据**）：listener 仍挂着时轮询在册视图 —— 消息从里面消失即清扫
//!      回投并摘除；距今必须 >60s（排除别的路径动手）。
//!   A3 重投真的到了 broker 侧：%RETRY% 队列的本地缓冲里出现这条消息（≤40s）。
//!   A4 放行后重新消费：reconsumeTimes=1、同一条 body 全程只到两次、与首投相隔 >60s。
//!   A5 位点收尾：挂住的 listener 返回后业务队列已提交位点走到 1
//!      （Java removeMessage 的列表仍含这条已被清扫的消息）。
//!
//! 用法：
//! ```text
//! cargo run --example live_clean_expired_msg -- 127.0.0.1:9876
//! ```
//! （约 4 分钟，等两个清扫周期）

use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::message_const::PROPERTY_CONSUME_START_TIMESTAMP;
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn body_of(m: &MessageExt) -> String {
    String::from_utf8_lossy(m.body.as_deref().unwrap_or(&[])).to_string()
}

fn contains_body(msgs: &[MessageExt], body: &str) -> bool {
    msgs.iter().any(|m| body_of(m) == body)
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
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// 第一次投递就挂住不返回；放行后恢复正常返回 CONSUME_SUCCESS。
///
/// listener 在 `spawn_blocking` 线程上执行，这里用纯 std 的 Condvar 阻塞，
/// 不会占用 tokio 运行时线程。
struct HungListener {
    state: Mutex<HungState>,
    first_entered: Condvar,
    released: Condvar,
}

struct HungState {
    calls: usize,
    released: bool,
    /// [(body, reconsumeTimes, 到达时刻)]
    arrivals: Vec<(String, i32, i64)>,
}

impl HungListener {
    fn new() -> Arc<HungListener> {
        Arc::new(HungListener {
            state: Mutex::new(HungState {
                calls: 0,
                released: false,
                arrivals: Vec::new(),
            }),
            first_entered: Condvar::new(),
            released: Condvar::new(),
        })
    }

    fn wait_first(&self, secs: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(secs);
        let mut s = lock(&self.state);
        while s.calls == 0 {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (guard, _) = self
                .first_entered
                .wait_timeout(s, deadline - now)
                .unwrap_or_else(|e| e.into_inner());
            s = guard;
        }
        true
    }

    fn release(&self) {
        lock(&self.state).released = true;
        self.released.notify_all();
    }

    fn calls(&self) -> usize {
        lock(&self.state).calls
    }

    /// 某 body 的到达记录 [(reconsumeTimes, 时刻)]。
    fn times(&self, body: &str) -> Vec<(i32, i64)> {
        lock(&self.state)
            .arrivals
            .iter()
            .filter(|(b, _, _)| b == body)
            .map(|(_, n, at)| (*n, *at))
            .collect()
    }
}

impl MessageListenerConcurrently for HungListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let first = {
            let mut s = lock(&self.state);
            s.calls += 1;
            let first = s.calls == 1;
            for m in msgs {
                s.arrivals.push((body_of(m), m.reconsume_times, now_ms()));
            }
            first
        };
        if first {
            let mut s = lock(&self.state);
            self.first_entered.notify_all();
            // 挂起窗口：等清扫动手 + 脚本放行（300s 上限兜底，防止脚本崩了卡死线程）
            let deadline = Instant::now() + Duration::from_secs(300);
            while !s.released {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                let (guard, _) = self
                    .released
                    .wait_timeout(s, deadline - now)
                    .unwrap_or_else(|e| e.into_inner());
                s = guard;
            }
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
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

struct Fixture {
    namesrv: String,
    stamp: u64,
    broker_addr: String,
    producer: DefaultMQProducer,
    admin: MQClientInstance,
}

impl Fixture {
    async fn new(namesrv: &str) -> Result<Fixture, String> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let producer = DefaultMQProducer::new(&format!("rust-live-ce-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        let admin = MQClientInstance::new(
            &format!("rust-live-ce-admin-{stamp}"),
            vec![namesrv.to_string()],
        );
        admin
            .start()
            .await
            .map_err(|e| format!("admin start failed: {e}"))?;
        producer
            .start()
            .await
            .map_err(|e| format!("producer start failed: {e}"))?;
        let route = admin
            .get_topic_route_data(MixAll::DEFAULT_TOPIC)
            .await
            .ok_or_else(|| format!("no route of {} from namesrv", MixAll::DEFAULT_TOPIC))?;
        let (_, broker_addr) = broker_route(&route)?;
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            stamp,
            broker_addr,
            producer,
            admin,
        })
    }

    fn topic(&self) -> String {
        format!("RustLiveCe{}", self.stamp)
    }

    async fn queues(&self, topic: &str) -> Vec<MessageQueue> {
        match self.admin.get_topic_publish_info(topic, false).await {
            Ok(publish) => publish
                .msg_queue_list()
                .into_iter()
                .map(|q| MessageQueue::new(&q.topic, &q.broker_name, q.queue_id))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    async fn create_topic(&self, topic: &str) -> Result<(), String> {
        self.admin
            .create_topic_in_broker(
                &self.broker_addr,
                MixAll::DEFAULT_TOPIC,
                topic,
                1,
                1,
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
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    /// 已提交位点之和（本用例业务 topic 只有 1 个队列）；broker 查无记录按 -1 计。
    async fn committed_offset(&self, group: &str, topic: &str) -> Result<i64, String> {
        let queues = self.queues(topic).await;
        if queues.is_empty() {
            return Err(format!("route of {topic} has no queue"));
        }
        let mut total = 0i64;
        for mq in queues {
            let off = self
                .admin
                .query_consumer_offset(group, &mq, 5000, Some(&self.broker_addr), false)
                .await
                .map_err(|e| format!("query offset q{} failed: {e}", mq.queue_id))?;
            total += off.unwrap_or(-1);
        }
        Ok(total)
    }

    fn shutdown(&self) {
        self.producer.shutdown();
        self.admin.shutdown();
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

    let topic = fx.topic();
    let group = format!("GID_rust_live_ce_{}", fx.stamp);
    let retry_topic = MixAll::get_retry_topic(&group);
    if let Err(e) = fx.create_topic(&topic).await {
        ck.check("create topic", false, &e);
        fx.shutdown();
        return ck;
    }
    println!("topic={topic} group={group}");

    let listener = HungListener::new();
    let cfg = ConsumerConfig {
        consumer_group: group.clone(),
        name_server_addrs: vec![fx.namesrv.clone()],
        instance_name: format!("rust-live-ce-{}", fx.stamp),
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        // 清扫周期与阈值都变成 1 分钟（Java 同字段同语义）
        consume_timeout: 1,
        ..Default::default()
    };
    let consumer = match DefaultMQPushConsumer::with_config(cfg) {
        Ok(c) => c,
        Err(e) => {
            ck.check("build consumer", false, &format!("{e}"));
            fx.shutdown();
            return ck;
        }
    };
    consumer.set_message_listener_concurrently(listener.clone());
    if let Err(e) = consumer.subscribe(&topic, "*") {
        ck.check("subscribe", false, &format!("{e}"));
        fx.shutdown();
        return ck;
    }
    if let Err(e) = consumer.start().await {
        ck.check("start consumer", false, &format!("{e}"));
        fx.shutdown();
        return ck;
    }

    let body = format!("ce-rust-{}", fx.stamp);

    // ---------- A0 业务队列（排除 %RETRY%） ----------
    let mut biz_key = String::new();
    let assigned = poll_until(
        || {
            for k in consumer.assigned_queue_keys() {
                if !k.starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX) {
                    biz_key = k;
                    return true;
                }
            }
            false
        },
        30,
    )
    .await;
    ck.check(
        "A0-业务队列已分配（%RETRY% 队列不算）",
        assigned,
        &format!("key={biz_key}"),
    );
    if !assigned {
        listener.release();
        consumer.shutdown();
        fx.shutdown();
        return ck;
    }

    // ---------- A1 基线：第一投挂住 ----------
    let mut msg = Message::new(&topic, Some(body.as_bytes()));
    if let Err(e) = fx.producer.send(&mut msg, Some(20000), None).await {
        ck.check("A1 send", false, &format!("{e}"));
        listener.release();
        consumer.shutdown();
        fx.shutdown();
        return ck;
    }
    let first_ok = listener.wait_first(30);
    ck.check(
        "A1-首投在 30s 内到达并挂住",
        first_ok,
        &format!("calls={}", listener.calls()),
    );
    let first = listener.times(&body);
    ck.check(
        "A1-首投 reconsumeTimes=0",
        first.first().is_some_and(|(n, _)| *n == 0),
        &format!(
            "times={}",
            first.first().map(|(n, _)| *n).unwrap_or(-1)
        ),
    );
    let entries = consumer.process_queue_entries(&biz_key);
    ck.check(
        "A1-挂住期间消息登记在册（在 listener 手里）",
        contains_body(&entries, &body),
        &format!("entries={}", entries.len()),
    );
    let stamp_seen = entries
        .iter()
        .find(|m| body_of(m) == body)
        .and_then(|m| m.get_property(PROPERTY_CONSUME_START_TIMESTAMP))
        .unwrap_or_default();
    ck.check(
        "A1-在册副本带本轮 CONSUME_START_TIME（清扫靠它判过期）",
        !stamp_seen.is_empty(),
        &format!("stamp={stamp_seen}"),
    );

    // ---------- A2 清扫命中：listener 还挂着，在册视图里已经没了 ----------
    let t0 = Instant::now();
    let swept = poll_until(
        || !contains_body(&consumer.process_queue_entries(&biz_key), &body),
        210,
    )
    .await;
    let elapsed = t0.elapsed().as_secs_f64();
    ck.check(
        "A2-清扫在 listener 仍挂起时收走了这条消息（约 start+120s）",
        swept,
        &format!("elapsed={elapsed:.3}s calls={}", listener.calls()),
    );
    ck.check(
        "A2-收走时间晚于一个清扫阈值（>60s，排除别的路径动手）",
        elapsed > 60.0,
        &format!("elapsed={elapsed:.3}s"),
    );

    // ---------- A3 回投真的到了 broker：%RETRY% 缓冲里出现 ----------
    // 挂起的 listener 把分发循环占住，重投消息只能停在 %RETRY% 队列的本地缓冲里。
    let mut retry_key_found = String::new();
    let back = poll_until(
        || {
            for k in consumer.assigned_queue_keys() {
                if !k.starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX) {
                    continue;
                }
                retry_key_found = k.clone();
                if contains_body(&consumer.pending_messages(&k), &body) {
                    return true;
                }
            }
            false
        },
        40,
    )
    .await;
    ck.check(
        &format!("A3-回投消息出现在 {retry_topic} 的本地缓冲（broker 真收到了 sendMessageBack）"),
        back,
        &format!("retryKey={retry_key_found}"),
    );

    // ---------- A4 放行：重投被重新消费 ----------
    listener.release();
    let second = poll_until(|| listener.times(&body).len() >= 2, 60).await;
    let times = listener.times(&body);
    ck.check(
        "A4-放行后重新消费到（reconsumeTimes=1）",
        second,
        &format!("times={}", times.len()),
    );
    ck.check(
        "A4-第二次投递 reconsumeTimes=1（broker 侧重投计数）",
        times.len() >= 2 && times[1].0 == 1,
        &format!("times={}", times.get(1).map(|(n, _)| *n).unwrap_or(-1)),
    );
    ck.check(
        "A4-同一条 body 全程只到两次（清算一次回投，无重复投递）",
        times.len() == 2,
        &format!("times={}", times.len()),
    );
    let gap_secs = if times.len() >= 2 {
        (times[1].1 - times[0].1) / 1000
    } else {
        -1
    };
    ck.check(
        "A4-第二次投递与首投相隔 >60s（不是 listener 自己造成的重投）",
        gap_secs > 60,
        &format!("gap={gap_secs}s"),
    );

    // ---------- A5 位点收尾 ----------
    let mut committed = -2i64;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(v) = fx.committed_offset(&group, &topic).await {
            committed = v;
            if v == 1 {
                break;
            }
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
    ck.check(
        "A5-挂住的 listener 返回后业务队列位点走到 1（Java removeMessage 含已清扫条目）",
        committed == 1,
        &format!("offset={committed}"),
    );

    // ---------------- 清理 ----------------
    listener.release();
    consumer.shutdown();
    if let Err(e) = fx.admin.delete_topic_in_broker(&fx.broker_addr, &topic, 5000).await {
        println!("    (deleteTopic({topic}) 失败: {e})");
    } else {
        println!("    (deleteTopic({topic}) OK)");
    }
    fx.shutdown();
    ck
}

fn report(ck: &mut Checker) {
    println!(
        "\nCleanExpiredMsg(rust): PASS={} FAIL={}",
        ck.passed,
        ck.failed.len()
    );
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
