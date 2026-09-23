//! 发送头三个字段（`defaultTopic` / `defaultTopicQueueNums` / `brokerName`）真机验证。
//!
//! 与 `python/verify_send_header_live.py`（H0..H5）、`cpp/examples/live_send_header.cpp`、
//! `dotnet/examples/RocketMQ.Examples/LiveSendHeader.cs` 同题。
//!
//! 前置：NameServer + Broker 已起，`autoCreateTopicEnable=true`。
//!
//! 离线单测（`src/client/producer/send_retry_tests.rs`）锁的是**上线形状**：V2 头的单字母
//! 键 `c` / `d` / `n` 有没有真的写进 extFields、五种发送入口是不是带同一份值。但「字段上了
//! 线」和「broker 真拿它做了决定」是两件事，后者只有真集群能证：
//! - H1 默认值：不带任何配置发到新 topic，broker 按 `min(d=4, TBW102.writeQueueNums)` 建
//!   队列（`TopicConfigManager.java:289`），与 Java 客户端默认行为一致。
//! - H2 `default_topic_queue_nums=2` 真的生效：建出来的 topic 只有 2 条队列。修之前这里
//!   写死 4，这条必然变 4 —— 那是那个假 setter 唯一可观测的后果。
//! - H3 `create_topic_key=src` 真的生效：先建一个带 PERM_INHERIT、3 条队列的模板 topic，
//!   再以它为 `c` 发送 → 新 topic 继承模板的 3 条队列，而不是 TBW102 的 8 条。
//! - H4 补上这三个字段之后，五种入口（同步 / 定点 / 单向 / 批量 320 / 异步）在真 broker
//!   上仍逐条落地，条数一条不差。
//! - H5 `n`（brokerName）：落点就是路由选中的那台 broker 名。⚠ 经典 broker 的发送链路里
//!   **没有** `requestHeader.getBrokerName()` 的读者（5.5.1 源码 grep 过），所以 `n` 在线上
//!   的存在只能由离线抓帧证明，这里不假装能观测到它。
//!
//! 用法：
//! ```text
//! cargo run --example live_send_header -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::producer::{
    DefaultMQProducer, ProducerConfig, SendCallback,
};
use rocketmq_client_remoting::client::result::{SendResult, SendStatus};
use rocketmq_client_remoting::common::message::{Message, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::sysflag::PermName;
use rocketmq_client_remoting::error::Error;

/// 「等 broker 侧结果落地」的默认预算（秒）。自动建出来的 topic 要等配置回读得到。
const WAIT_SECONDS: u64 = 20;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// 断言累积器：跑完全部场景再汇总，首个失败不提前退出。
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

    fn abort(&mut self, name: &str, err: &str) {
        println!("  [FAIL] {name}: {err}");
        self.failed.push(format!("{name}: {err}"));
    }
}

/// 一次异步发送的终态。
#[derive(Default)]
struct Latch {
    oks: AtomicUsize,
    results: Mutex<Vec<SendResult>>,
    errors: Mutex<Vec<String>>,
}

impl Latch {
    fn oks(&self) -> usize {
        self.oks.load(Ordering::SeqCst)
    }

    fn first(&self) -> Option<SendResult> {
        lock(&self.results).first().cloned()
    }

    fn errors(&self) -> Vec<String> {
        lock(&self.errors).clone()
    }

    async fn wait(&self, secs: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            if self.done() > 0 || Instant::now() >= deadline {
                return self.done() > 0;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn done(&self) -> usize {
        self.oks() + lock(&self.errors).len()
    }
}

impl SendCallback for Latch {
    fn on_success(&self, result: SendResult) {
        if result.status == SendStatus::SendOk {
            self.oks.fetch_add(1, Ordering::SeqCst);
        }
        lock(&self.results).push(result);
    }

    fn on_exception(&self, err: Error) {
        lock(&self.errors).push(err.to_string());
    }
}

/// 环境：一个名字服务地址 + 一组按 stamp 隔离的 topic，跑完自己删干净。
struct Env {
    namesrv: String,
    stamp: String,
    admin: DefaultMQAdminExt,
    broker_addr: Mutex<String>,
    /// H5 的判据：路由里这台 broker 的名字，就是发送头里的 `n`。
    broker_name: Mutex<String>,
    topics: Mutex<Vec<String>>,
}

impl Env {
    fn new(namesrv: &str, stamp: &str) -> Env {
        let admin = DefaultMQAdminExt::with_config(AdminConfig {
            instance_name: format!("HDRADMIN-{stamp}"),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        Env {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            admin,
            broker_addr: Mutex::new(String::new()),
            broker_name: Mutex::new(String::new()),
            topics: Mutex::new(Vec::new()),
        }
    }

    fn topic(&self, kind: &str) -> String {
        let t = format!("Hdr{kind}_{}", self.stamp);
        lock(&self.topics).push(t.clone());
        t
    }

    fn producer_group(&self) -> String {
        format!("PID_send_header_{}", self.stamp)
    }

    fn broker(&self) -> String {
        lock(&self.broker_addr).clone()
    }

    fn broker_name(&self) -> String {
        lock(&self.broker_name).clone()
    }

    /// 集群探活：端口开着 != broker 已注册到 namesrv，所以轮询；
    /// 顺带反查这台 broker 的名字。
    async fn start(&self, ck: &mut Checker) -> bool {
        if let Err(e) = self.admin.start().await {
            ck.abort("admin start", &e.to_string());
            return false;
        }
        let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
        loop {
            if let Ok(info) = self.admin.fetch_broker_cluster_info().await {
                if let Some(addr) = info.get_broker_addrs().first() {
                    let name = info
                        .broker_addr_table
                        .iter()
                        .find(|(_, ids)| ids.iter().any(|(_, a)| a == addr))
                        .map(|(n, _)| n.clone())
                        .unwrap_or_default();
                    *lock(&self.broker_addr) = addr.clone();
                    *lock(&self.broker_name) = name.clone();
                    ck.check(
                        "集群探活",
                        true,
                        &format!("broker={addr} name={name}"),
                    );
                    return true;
                }
            }
            if Instant::now() >= deadline {
                ck.abort("集群探活", "nameServer 在预算内没有返回任何 broker");
                return false;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// 只配了 c/d 两个旋钮的生产者；`None` 表示保持默认。
    fn producer(&self, kind: &str, c: Option<&str>, d: Option<i32>) -> Result<DefaultMQProducer, String> {
        DefaultMQProducer::with_config(ProducerConfig {
            producer_group: self.producer_group(),
            instance_name: format!("hdr-{kind}-{}", self.stamp),
            name_server_addrs: vec![self.namesrv.clone()],
            create_topic_key: c.unwrap_or(MixAll::DEFAULT_TOPIC).to_string(),
            default_topic_queue_nums: d.unwrap_or(MixAll::DEFAULT_TOPIC_QUEUE_NUMS),
            send_msg_timeout: 5_000,
            ..Default::default()
        })
        .map_err(|e| e.to_string())
    }

    /// 起一个独立生产者发一条消息，发完就关（H1..H3 每笔都要用不同的 c/d 组合）。
    async fn send_once(&self, kind: &str, c: Option<&str>, d: Option<i32>, topic: &str, body: &str) -> Result<SendResult, String> {
        let producer = self.producer(kind, c, d)?;
        producer.start().await.map_err(|e| e.to_string())?;
        let mut msg = Message::new(topic, Some(body.as_bytes()));
        let sent = producer.send(&mut msg, Some(5_000), None).await;
        producer.shutdown();
        sent.map_err(|e| e.to_string())
    }

    async fn cleanup(&self, ck: &mut Checker) {
        let topics = lock(&self.topics).clone();
        for topic in topics {
            if let Err(e) = self.admin.delete_topic(&topic, None).await {
                println!("  [WARN] delete_topic({topic}) failed: {e}");
            }
        }
        ck.check("清理本次的 topic", true, "");
    }
}

/// 读回 broker 上该 topic 的 (read, write) 队列数；还不存在时 None。
async fn queue_nums(env: &Env, topic: &str) -> Option<(i32, i32)> {
    env.admin
        .examine_topic_config(&env.broker(), topic)
        .await
        .ok()
        .map(|cfg| (cfg.read_queue_nums, cfg.write_queue_nums))
}

/// 等 topic 在 broker 上出现（自动建 topic 在发送链路里同步做，配置落地要一点时间）。
async fn wait_topic(env: &Env, topic: &str) -> Option<(i32, i32)> {
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    loop {
        let got = queue_nums(env, topic).await;
        if got.is_some() || Instant::now() >= deadline {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// 该 topic 全 broker 的落库条数（新 topic 的 minOffset 恒为 0）；读不到返回 -1。
async fn total_messages(env: &Env, topic: &str) -> i64 {
    match env.admin.examine_topic_stats(topic).await {
        Ok(stats) => stats
            .offset_table
            .iter()
            .map(|(_, o)| o.max_offset - o.min_offset)
            .sum(),
        Err(_) => -1,
    }
}

// ------------------------------------------------------- H0/H1/H2/H3

async fn check_queue_nums(ck: &mut Checker, env: &Env) {
    // H0：TBW102 是这个 broker 上真正的「模板 topic」，H2/H3 的判据都依赖它的队列数。
    let (tbw_read, tbw_write) = match queue_nums(env, MixAll::DEFAULT_TOPIC).await {
        Some(pair) if pair.1 > 0 => pair,
        other => {
            ck.abort(
                "H0 TBW102 模板可读",
                &format!("examine_topic_config(TBW102) = {other:?}"),
            );
            return;
        }
    };
    ck.check(
        "H0 TBW102 模板可读",
        true,
        &format!("read={tbw_read} write={tbw_write}"),
    );

    // H1：什么都不设，d 走 MixAll::DEFAULT_TOPIC_QUEUE_NUMS(4)。
    let t1 = env.topic("Def");
    match env.send_once("h1", None, None, &t1, "h1").await {
        Ok(r) => ck.check(
            "H1 默认配置发送成功",
            r.status == SendStatus::SendOk,
            &format!(
                "msgId={} broker={}",
                r.msg_id.unwrap_or_default(),
                r.message_queue
                    .map(|mq| mq.broker_name)
                    .unwrap_or_default()
            ),
        ),
        Err(e) => ck.abort("H1 默认配置发送成功", &e),
    }
    let (r1, w1) = wait_topic(env, &t1).await.unwrap_or((-1, -1));
    let expected1 = std::cmp::min(MixAll::DEFAULT_TOPIC_QUEUE_NUMS, tbw_write);
    ck.check(
        "H1 默认 d=4 建的 topic 队列数=min(4, TBW102)",
        w1 == expected1 && r1 == w1,
        &format!("read={r1} write={w1} (TBW102={tbw_write})"),
    );

    // H2：d=2 必须把队列数带下去 —— 修之前这里写死 4。
    let t2 = env.topic("Nums");
    if let Err(e) = env.send_once("h2", None, Some(2), &t2, "h2").await {
        ck.abort("H2 发送", &e);
    }
    let (r2, w2) = wait_topic(env, &t2).await.unwrap_or((-1, -1));
    ck.check(
        "H2 defaultTopicQueueNums=2 建的 topic 只有 2 条队列",
        w2 == std::cmp::min(2, tbw_write) && r2 == w2,
        &format!("read={r2} write={w2}（写死 4 的旧行为会是 {w1}）"),
    );

    // H3：c 指向自己的模板 topic。模板必须带 PERM_INHERIT，否则 TopicConfigManager:286
    // 的 isInherited 不通过，broker 直接拒绝自动建 topic。
    let src = env.topic("Src");
    if let Err(e) = env
        .admin
        .create_topic_in_broker(
            &env.broker(),
            &src,
            3,
            3,
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_INHERIT,
        )
        .await
    {
        ck.abort("H3 模板 topic 创建", &e.to_string());
        return;
    }
    let (src_read, src_write) = wait_topic(env, &src).await.unwrap_or((-1, -1));
    ck.check(
        "H3 模板 topic 建好（3 条队列、带 INHERIT）",
        src_write == 3 && (src_read, src_write) != (tbw_read, tbw_write),
        &format!("src=({src_read},{src_write}) tbw102=({tbw_read},{tbw_write})"),
    );

    let t3 = env.topic("Inherit");
    if let Err(e) = env.send_once("h3", Some(&src), Some(8), &t3, "h3").await {
        ck.abort("H3 发送", &e);
    }
    let (r3, w3) = wait_topic(env, &t3).await.unwrap_or((-1, -1));
    ck.check(
        &format!("H3 createTopicKey=模板 topic 时被继承（min(8,3)=3 而不是 TBW102 的 {tbw_write}）"),
        w3 == 3 && r3 == 3,
        &format!("read={r3} write={w3}"),
    );
}

// ------------------------------------------------------------- H4

async fn check_send_entries(ck: &mut Checker, env: &Env) {
    let t4 = env.topic("Entries");
    let producer = match env.producer("h4", None, None) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("H4 构造", &e);
            return;
        }
    };
    if let Err(e) = producer.start().await {
        ck.abort("H4 producer start", &e.to_string());
        return;
    }

    let mut landed: Vec<(&str, bool, String)> = Vec::new();

    // ① 同步
    let mut m1 = Message::new(&t4, Some(b"h4-sync"));
    let pinned_mq: Option<MessageQueue> = match producer.send(&mut m1, Some(5_000), None).await {
        Ok(r) => {
            landed.push(("sync", r.status == SendStatus::SendOk, String::new()));
            r.message_queue.clone()
        }
        Err(e) => {
            landed.push(("sync", false, e.to_string()));
            None
        }
    };

    // ② 定点：显式给 mq，落在另一条调用链上（broker 名由调用方给）
    match pinned_mq.as_ref() {
        Some(mq) => {
            let mut m2 = Message::new(&t4, Some(b"h4-pinned"));
            match producer.send(&mut m2, Some(5_000), Some(mq)).await {
                Ok(r) => {
                    let same = r
                        .message_queue
                        .as_ref()
                        .map(|q| q.broker_name == mq.broker_name)
                        .unwrap_or(false);
                    landed.push((
                        "pinned",
                        r.status == SendStatus::SendOk && same,
                        format!("落点={}", mq.broker_name),
                    ));
                }
                Err(e) => landed.push(("pinned", false, e.to_string())),
            }
        }
        None => landed.push(("pinned", false, "同步发送没拿到 mq".to_string())),
    }

    // ③ 单向：没有应答，只能靠后面的总数兜底
    let mut m3 = Message::new(&t4, Some(b"h4-oneway"));
    match producer.send_oneway(&mut m3, None).await {
        Ok(()) => landed.push(("oneway", true, String::new())),
        Err(e) => landed.push(("oneway", false, e.to_string())),
    }

    // ④ 批量（320）
    let batch: Vec<Message> = (0..3)
        .map(|i| Message::new(&t4, Some(format!("h4-batch-{i}").as_bytes())))
        .collect();
    match producer.send_batch(batch, None, Some(5_000)).await {
        Ok(r) => landed.push(("batch", r.status == SendStatus::SendOk, String::new())),
        Err(e) => landed.push(("batch", false, e.to_string())),
    }

    // ⑤ 异步：走 ASYNC 分支，最终落到同一个 sendKernelImpl
    let latch = Arc::new(Latch::default());
    match producer.send_async(
        Message::new(&t4, Some(b"h4-async")),
        latch.clone(),
        Some(5_000),
        None,
    ) {
        Ok(()) => {
            let done = latch.wait(10).await;
            let ok = done
                && latch.oks() == 1
                && latch
                    .first()
                    .map(|r| r.status == SendStatus::SendOk)
                    .unwrap_or(false);
            landed.push(("async", ok, latch.errors().join("; ")));
        }
        Err(e) => landed.push(("async", false, e.to_string())),
    }

    producer.shutdown();

    for (entry, ok, detail) in landed {
        ck.check(
            &format!("H4 {entry} 入口发送成功"),
            ok,
            if ok { "" } else { &detail },
        );
    }
    // 1 同步 + 1 定点 + 1 单向 + 3 批量 + 1 异步 = 7 条
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    let mut total = total_messages(env, &t4).await;
    while total < 7 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(300)).await;
        total = total_messages(env, &t4).await;
    }
    ck.check(
        "H4 七条消息逐条落库（批量按子消息计）",
        total == 7,
        &format!("total={total}"),
    );
}

// ------------------------------------------------------------- H5

async fn check_broker_name(ck: &mut Checker, env: &Env) {
    let t5 = env.topic("BrokerName");
    match env.send_once("h5", None, None, &t5, "h5").await {
        Ok(r) => {
            let landed = r
                .message_queue
                .map(|mq| mq.broker_name)
                .unwrap_or_default();
            ck.check(
                "H5 落点 broker 名与路由一致（n 就是它）",
                r.status == SendStatus::SendOk && landed == env.broker_name(),
                &format!("落点={landed} 路由={}", env.broker_name()),
            );
        }
        Err(e) => ck.abort("H5 发送", &e),
    }
}

// ------------------------------------------------------------------ 驱动

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let env = Env::new(namesrv, &stamp);
    println!("== live send-header (c/d/n) check, namesrv={namesrv} stamp={stamp} ==");
    if !env.start(&mut ck).await {
        return ck;
    }
    println!("\n-- H0~H3: c/d 决定自动建 topic 的队列数 --");
    check_queue_nums(&mut ck, &env).await;
    println!("\n-- H4: 五种发送入口逐条落地 --");
    check_send_entries(&mut ck, &env).await;
    println!("\n-- H5: n 就是路由选中的那台 broker --");
    check_broker_name(&mut ck, &env).await;
    env.cleanup(&mut ck).await;
    env.admin.shutdown();
    ck
}

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs().to_string(),
        Err(_) => "0".to_string(),
    }
}

fn report(ck: &mut Checker) {
    println!(
        "== 结果: {}/{} 通过 ==",
        ck.passed,
        ck.passed as usize + ck.failed.len()
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
