//! 定点发送 topic 一致性守卫真机验证（Java `DefaultMQProducerImpl:1234-1236` / `:1277-1278`）。
//!
//! 与 `python/verify_pinned_guard_live.py`（S1..S6）、`cpp/examples/live_pinned_guard.cpp`、
//! `dotnet/examples/RocketMQ.Examples/LivePinnedGuard.cs` 同题。
//!
//! 前置：NameServer + Broker 已起，`autoCreateTopicEnable=true`。
//!
//! 离线单测（`src/client/producer/send_retry_tests.rs`）用假集群证明的是「拒了、且报文没上线」；
//! 真机这一趟证明**另一面**：
//! - S1/S6 守卫不误伤真业务：从真路由取到的队列在同步单条/同步批量/异步单条/异步批量四条
//!   入口上照常 `SEND_OK`，消息按 keys 一条不少地被消费到；
//! - S2/S4 拒的时候守在本端：亚毫秒、无 broker 码，而且 **broker 上的 maxOffset 一动不动**
//!   （真机版的 wire 反证：拒绝不留痕，也不会事后偷发）；
//! - S3 命名空间的比较用 Java `queueWithNamespace` 的幂等：`ns%topic` 与裸 topic 都不误拒，
//!   `ns2%topic` 才拒；
//! - S5 单向定点**没有**守卫（Java `:1303-1310` 有意留的口子）：报文按 msg 自己的 topic
//!   落库 —— 用「A 收到、B 的 maxOffset 还是 0」把这条语义钉死。
//!
//! 用法：
//! ```text
//! cargo run --example live_pinned_guard -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::producer::{
    DefaultMQProducer, ProducerConfig, SendCallback,
};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently, SendResult,
    SendStatus,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::sysflag::PermName;
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

const WAIT_SECONDS: u64 = 20;
/// 反腿的耗时上界：守卫是纯字符串比较，真机给 50ms 已留两个数量级余量。
const LOCAL_BUDGET_MS: f64 = 50.0;
const SYNC_WORDING: &str = "message's topic not equal mq's topic";
const ASYNC_WORDING: &str = "Topic of the message does not match its target message queue";

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
        Checker { passed: 0, failed: Vec::new() }
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
    done: AtomicUsize,
}

impl Latch {
    fn new() -> Arc<Latch> {
        Arc::new(Latch::default())
    }

    fn oks(&self) -> usize {
        self.oks.load(Ordering::SeqCst)
    }

    fn errors(&self) -> Vec<String> {
        lock(&self.errors).clone()
    }

    fn first_status(&self) -> Option<SendStatus> {
        lock(&self.results).first().map(|r| r.status)
    }

    async fn wait(&self, secs: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            if self.done() >= 1 || Instant::now() >= deadline {
                return self.done() >= 1;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// 终态到达数（成功 + 异常都算）。
    fn done(&self) -> usize {
        self.done.load(Ordering::SeqCst)
    }
}

impl SendCallback for Latch {
    fn on_success(&self, result: SendResult) {
        if result.status == SendStatus::SendOk {
            self.oks.fetch_add(1, Ordering::SeqCst);
        }
        lock(&self.results).push(result);
        self.done.fetch_add(1, Ordering::SeqCst);
    }

    fn on_exception(&self, err: Error) {
        lock(&self.errors).push(err.to_string());
        self.done.fetch_add(1, Ordering::SeqCst);
    }
}

/// 消费到的 keys 收集器。
struct KeySink {
    keys: Mutex<Vec<String>>,
}

impl KeySink {
    fn new() -> Arc<KeySink> {
        Arc::new(KeySink { keys: Mutex::new(Vec::new()) })
    }

    fn snapshot(&self) -> Vec<String> {
        lock(&self.keys).clone()
    }
}

struct KeyListener {
    sink: Arc<KeySink>,
}

impl MessageListenerConcurrently for KeyListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let mut keys = lock(&self.sink.keys);
        for m in msgs {
            if let Some(k) = m.get_keys() {
                keys.push(k.to_string());
            } else {
                keys.push(String::from_utf8_lossy(m.get_body()).into_owned());
            }
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

struct Env {
    namesrv: String,
    stamp: String,
    admin: DefaultMQAdminExt,
    broker_addr: Mutex<String>,
    topics: Mutex<Vec<String>>,
}

impl Env {
    fn new(namesrv: &str, stamp: &str) -> Env {
        let admin = DefaultMQAdminExt::with_config(AdminConfig {
            instance_name: format!("PINGUARD-{stamp}"),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        Env {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            admin,
            broker_addr: Mutex::new(String::new()),
            topics: Mutex::new(Vec::new()),
        }
    }

    fn topic(&self, kind: &str) -> String {
        let t = format!("PinGuard{kind}_{}", self.stamp);
        lock(&self.topics).push(t.clone());
        t
    }

    fn broker(&self) -> String {
        lock(&self.broker_addr).clone()
    }

    async fn start(&self, ck: &mut Checker) -> bool {
        if let Err(e) = self.admin.start().await {
            ck.abort("admin start", &e.to_string());
            return false;
        }
        let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
        loop {
            if let Ok(info) = self.admin.fetch_broker_cluster_info().await {
                if let Some(addr) = info.get_broker_addrs().first() {
                    *lock(&self.broker_addr) = addr.clone();
                    ck.check("集群探活", true, &format!("broker={addr}"));
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

    async fn create_topic(&self, topic: &str, queues: i32) -> Result<(), String> {
        self.admin
            .create_topic_in_broker(
                &self.broker(),
                topic,
                queues,
                queues,
                PermName::PERM_READ | PermName::PERM_WRITE,
            )
            .await
            .map_err(|e| format!("create_topic_in_broker({topic}): {e}"))
    }

    async fn cleanup(&self, ck: &mut Checker) {
        for topic in lock(&self.topics).clone() {
            if let Err(e) = self.admin.delete_topic(&topic, None).await {
                println!("  [WARN] delete_topic({topic}) failed: {e}");
            }
        }
        ck.check("清理本次的 topic", true, "");
    }
}

fn producer_config(env: &Env, kind: &str, namespace: &str) -> Result<DefaultMQProducer, String> {
    DefaultMQProducer::with_config(ProducerConfig {
        producer_group: format!("PID_pin_guard_{kind}_{}", env.stamp),
        instance_name: format!("pin-guard-{kind}-{}", env.stamp),
        name_server_addrs: vec![env.namesrv.clone()],
        namespace: namespace.to_string(),
        send_msg_timeout: 5_000,
        ..Default::default()
    })
    .map_err(|e| e.to_string())
}

async fn queue_zero(producer: &DefaultMQProducer, topic: &str) -> Result<MessageQueue, String> {
    let queues = producer
        .fetch_publish_message_queues(topic)
        .await
        .map_err(|e| format!("fetch_publish_message_queues({topic}): {e}"))?;
    queues
        .into_iter()
        .find(|q| q.queue_id == 0)
        .ok_or_else(|| format!("{topic} 没有 queue 0"))
}

async fn wait_route(producer: &DefaultMQProducer, topic: &str) -> Result<MessageQueue, String> {
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    loop {
        match queue_zero(producer, topic).await {
            Ok(mq) => return Ok(mq),
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(e);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// 等 maxOffset 涨到 want 再返回（broker 的 ConsumeQueue 派发比 SEND 回包慢几毫秒）。
async fn wait_offset(producer: &DefaultMQProducer, mq: &MessageQueue, want: i64) -> i64 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let got = producer.max_offset(mq).await.unwrap_or(-1);
        if got >= want || Instant::now() >= deadline {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn offset_of(producer: &DefaultMQProducer, mq: &MessageQueue) -> i64 {
    producer.max_offset(mq).await.unwrap_or(-1)
}

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let env = Env::new(namesrv, &stamp);
    println!("== live pinned-topic guard check, namesrv={namesrv} stamp={stamp} ==");
    if !env.start(&mut ck).await {
        return ck;
    }
    let topic_a = env.topic("A");
    let topic_b = env.topic("B");

    let producer = match producer_config(&env, "sync", "") {
        Ok(p) => p,
        Err(e) => {
            ck.abort("生产者构造", &e);
            return ck;
        }
    };
    if let Err(e) = producer.start().await {
        ck.abort("生产者 start", &e.to_string());
        return ck;
    }

    // ---------------- S0 两条 topic 的路由都注册好 + 消费者就位 ----------------
    println!("\n-- S0 fixture: 路由与消费者 --");
    if let Err(e) = env.create_topic(&topic_a, 4).await {
        ck.abort("创建 A", &e);
        return ck;
    }
    if let Err(e) = env.create_topic(&topic_b, 4).await {
        ck.abort("创建 B", &e);
        return ck;
    }
    let mq_a = match wait_route(&producer, &topic_a).await {
        Ok(mq) => mq,
        Err(e) => {
            ck.abort("S0 A 路由", &e);
            return ck;
        }
    };
    let mq_b = match wait_route(&producer, &topic_b).await {
        Ok(mq) => mq,
        Err(e) => {
            ck.abort("S0 B 路由", &e);
            return ck;
        }
    };
    ck.check(
        "S0 两条 topic 的路由都可用（反腿用真队列，拒的才一定是 topic 而不是地址）",
        mq_a.broker_name == mq_b.broker_name,
        &format!("A={}/{} B={}/{}", mq_a.broker_name, mq_a.queue_id, mq_b.broker_name, mq_b.queue_id),
    );

    // 消费者先起、CONSUME_FROM_FIRST_OFFSET：不和发送抢 rebalance 的时间点
    let sink = KeySink::new();
    let consumer = match DefaultMQPushConsumer::with_config(ConsumerConfig {
        consumer_group: format!("pin-guard-live-{stamp}"),
        name_server_addrs: vec![namesrv.to_string()],
        instance_name: format!("pin-guard-live-{stamp}"),
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("消费者构造", &e.to_string());
            return ck;
        }
    };
    if let Err(e) = consumer.subscribe(&topic_a, "*") {
        ck.abort("消费者 subscribe", &e.to_string());
        return ck;
    }
    consumer.set_message_listener_concurrently(Arc::new(KeyListener { sink: sink.clone() }));
    if let Err(e) = consumer.start().await {
        ck.abort("消费者 start", &e.to_string());
        return ck;
    }

    // ---------------- S1 正腿：真路由队列上的定点发送 ----------------
    println!("\n-- S1 真实路由队列上的定点发送 --");
    let k_single = format!("pinned-single-{stamp}");
    let mut m1 = Message::new(&topic_a, Some(b"s1-single"));
    m1.set_keys(&k_single);
    match producer.send(&mut m1, Some(5_000), Some(&mq_a)).await {
        Ok(r) => {
            let same = r
                .message_queue
                .as_ref()
                .map(|q| q.queue_id == mq_a.queue_id && q.topic == topic_a)
                .unwrap_or(false);
            ck.check(
                "S1a 同步单条定点 SEND_OK 且落在 queue 0",
                r.status == SendStatus::SendOk && same,
                &format!("status={:?} mq={:?}", r.status, r.message_queue),
            );
        }
        Err(e) => ck.abort("S1a 同步单条定点发送", &e.to_string()),
    }

    let k_b1 = format!("pinned-b1-{stamp}");
    let k_b2 = format!("pinned-b2-{stamp}");
    let mut sb1 = Message::new(&topic_a, Some(b"s1-b1"));
    sb1.set_keys(&k_b1);
    let mut sb2 = Message::new(&topic_a, Some(b"s1-b2"));
    sb2.set_keys(&k_b2);
    match producer.send_batch(vec![sb1, sb2], Some(&mq_a), Some(5_000)).await {
        Ok(r) => {
            let same = r
                .message_queue
                .as_ref()
                .map(|q| q.queue_id == mq_a.queue_id)
                .unwrap_or(false);
            ck.check(
                "S1b 同步批量定点 SEND_OK 且同一队列",
                r.status == SendStatus::SendOk && same,
                &format!("status={:?} mq={:?}", r.status, r.message_queue),
            );
        }
        Err(e) => ck.abort("S1b 同步批量定点发送", &e.to_string()),
    }
    // 批量在消费队列上按**子消息**逐条落位（broker 收到 inner-batch 后拆开写）⇒ 2 条子消息涨 2
    let off_after_s1 = wait_offset(&producer, &mq_a, 3).await;
    ck.check(
        "S1c 三笔子消息都真落库（maxOffset = 单条 1 + 批量子消息 2）",
        off_after_s1 == 3,
        &format!("maxOffset={off_after_s1}"),
    );

    // ---------------- S2 反腿：同步拒绝，本端、无痕 ----------------
    println!("\n-- S2 topic 不符：本端亚毫秒拒，broker 上无痕 --");
    let mut r1 = Message::new(&topic_a, Some(b"refused"));
    r1.set_keys(&format!("pinned-refused-{stamp}"));
    let began = Instant::now();
    let single = producer.send(&mut r1, Some(5_000), Some(&mq_b)).await;
    let single_ms = began.elapsed().as_secs_f64() * 1000.0;
    let single_msg = single.as_ref().err().map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "S2a 同步单条拒（Java 原文案）",
        single.is_err() && single_msg.contains(SYNC_WORDING),
        &format!("{single_ms:.2}ms {single_msg}"),
    );
    ck.check(
        "S2b 拒在本端：亚毫秒（不是超时、不是 broker remark）",
        single.is_err() && single_ms < LOCAL_BUDGET_MS,
        &format!("{single_ms:.2}ms"),
    );
    let rb1 = Message::new(&topic_a, Some(b"r1"));
    let rb2 = Message::new(&topic_a, Some(b"r2"));
    let began = Instant::now();
    let batch = producer.send_batch(vec![rb1, rb2], Some(&mq_b), Some(5_000)).await;
    let batch_ms = began.elapsed().as_secs_f64() * 1000.0;
    let batch_msg = batch.as_ref().err().map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "S2c 同步批量共用同一处守卫与同一句文案",
        batch.is_err() && batch_msg.contains(SYNC_WORDING) && batch_ms < LOCAL_BUDGET_MS,
        &format!("{batch_ms:.2}ms {batch_msg}"),
    );
    let a_now = offset_of(&producer, &mq_a).await;
    let b_now = offset_of(&producer, &mq_b).await;
    ck.check(
        "S2d wire 反证：A 的 maxOffset 一动没动，B 上一条都没有",
        a_now == off_after_s1 && b_now == 0,
        &format!("A={a_now} B={b_now}"),
    );

    // ---------------- S3 命名空间：wrap 幂等，只拒真的不同名 ----------------
    println!("\n-- S3 命名空间下的比较（ns%topic 与裸 topic 都不误拒）--");
    let w_topic = format!("ns1%{topic_a}");
    lock(&env.topics).push(w_topic.clone());
    if let Err(e) = env.create_topic(&w_topic, 4).await {
        ck.abort("S3 创建 ns1%topic", &e);
        return ck;
    }
    let nsprod = match producer_config(&env, "ns", "ns1") {
        Ok(p) => p,
        Err(e) => {
            ck.abort("S3 命名空间生产者构造", &e);
            return ck;
        }
    };
    if let Err(e) = nsprod.start().await {
        ck.abort("S3 命名空间生产者 start", &e.to_string());
        return ck;
    }
    let mqw = match wait_route(&nsprod, &w_topic).await {
        Ok(mq) => mq,
        Err(e) => {
            ck.abort("S3 带前缀 topic 的路由", &e);
            return ck;
        }
    };
    let mut n1 = Message::new(&topic_a, Some(b"ns-wrapped-queue"));
    n1.set_keys(&format!("pinned-ns-q-{stamp}"));
    match nsprod.send(&mut n1, Some(5_000), Some(&mqw)).await {
        Ok(r) => ck.check(
            "S3b 队列 topic 已带 ns 前缀：wrap 幂等，不误拒",
            r.status == SendStatus::SendOk,
            &format!("status={:?}", r.status),
        ),
        Err(e) => ck.abort("S3b 队列 topic 已带前缀的发送", &e.to_string()),
    }
    let mut n2 = Message::new(&w_topic, Some(b"ns-wrapped-message"));
    n2.set_keys(&format!("pinned-ns-m-{stamp}"));
    match nsprod.send(&mut n2, Some(5_000), Some(&mqw)).await {
        Ok(r) => ck.check(
            "S3c 消息 topic 自己已带前缀同样放行",
            r.status == SendStatus::SendOk,
            &format!("status={:?}", r.status),
        ),
        Err(e) => ck.abort("S3c 消息 topic 已带前缀的发送", &e.to_string()),
    }
    let ns2_mq = MessageQueue::new(&format!("ns2%{topic_a}"), &mqw.broker_name, 0);
    let mut n3 = Message::new(&topic_a, Some(b"ns2-refused"));
    let began = Instant::now();
    let ns2 = nsprod.send(&mut n3, Some(5_000), Some(&ns2_mq)).await;
    let ns2_ms = began.elapsed().as_secs_f64() * 1000.0;
    let ns2_msg = ns2.as_ref().err().map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "S3d 换成 ns2% 前缀才拒（对照腿：拒的是名字，不是「有前缀」）",
        ns2.is_err() && ns2_msg.contains(SYNC_WORDING) && ns2_ms < LOCAL_BUDGET_MS,
        &format!("{ns2_ms:.2}ms {ns2_msg}"),
    );
    let ns_off = wait_offset(&nsprod, &mqw, 2).await;
    ck.check(
        "S3e 两条放行腿真落进 ns1%topic（maxOffset=2）",
        ns_off == 2,
        &format!("maxOffset={ns_off}"),
    );

    // ---------------- S4 异步：回调里是异步那处文案 ----------------
    println!("\n-- S4 异步单条/批量：拒绝走回调，放行走内核 --");
    let refused_latch = Latch::new();
    let mut ar = Message::new(&topic_a, Some(b"async-refused"));
    ar.set_keys(&format!("pinned-async-refused-{stamp}"));
    if let Err(e) = producer.send_async(ar, refused_latch.clone(), Some(5_000), Some(mq_b.clone())) {
        ck.abort("S4a 异步调用本身不该抛", &e.to_string());
    }
    let got = refused_latch.wait(10).await;
    let errs = refused_latch.errors();
    ck.check(
        "S4a 单条异步拒绝走回调、文案是异步那处",
        got && errs.len() == 1 && errs[0].contains(ASYNC_WORDING),
        &format!("errors={errs:?}"),
    );
    let a_now = offset_of(&producer, &mq_a).await;
    ck.check(
        "S4b 拒后 maxOffset 仍不动（异步也没偷发）",
        a_now == off_after_s1,
        &format!("maxOffset={a_now}"),
    );

    let batch_refused = Latch::new();
    let ab1 = Message::new(&topic_a, Some(b"abr1"));
    let ab2 = Message::new(&topic_a, Some(b"abr2"));
    if let Err(e) = producer.send_batch_async(
        vec![ab1, ab2],
        batch_refused.clone(),
        Some(5_000),
        Some(mq_b.clone()),
    ) {
        ck.abort("S4c 批量异步调用本身不该抛", &e.to_string());
    }
    let got = batch_refused.wait(10).await;
    let errs = batch_refused.errors();
    ck.check(
        "S4c 批量异步共用同一处文案与同一条拒绝路径",
        got && errs.len() == 1 && errs[0].contains(ASYNC_WORDING),
        &format!("errors={errs:?}"),
    );

    let single_ok = Latch::new();
    let mut okm = Message::new(&topic_a, Some(b"async-ok"));
    okm.set_keys(&format!("pinned-async-single-{stamp}"));
    if let Err(e) = producer.send_async(okm, single_ok.clone(), Some(5_000), Some(mq_a.clone())) {
        ck.abort("S4d 异步放行调用失败", &e.to_string());
    }
    let batch_ok = Latch::new();
    let mut bk1 = Message::new(&topic_a, Some(b"ab1"));
    bk1.set_keys(&format!("pinned-async-b1-{stamp}"));
    let mut bk2 = Message::new(&topic_a, Some(b"ab2"));
    bk2.set_keys(&format!("pinned-async-b2-{stamp}"));
    if let Err(e) = producer.send_batch_async(
        vec![bk1, bk2],
        batch_ok.clone(),
        Some(5_000),
        Some(mq_a.clone()),
    ) {
        ck.abort("S4e 批量异步放行调用失败", &e.to_string());
    }
    let ok_ok = single_ok.wait(10).await;
    let ok_batch = batch_ok.wait(10).await;
    ck.check(
        "S4d 两条放行腿都 SEND_OK",
        ok_ok
            && single_ok.oks() == 1
            && single_ok.first_status() == Some(SendStatus::SendOk)
            && ok_batch
            && batch_ok.oks() == 1
            && batch_ok.first_status() == Some(SendStatus::SendOk),
        &format!(
            "single_errors={:?} batch_errors={:?}",
            single_ok.errors(),
            batch_ok.errors()
        ),
    );
    let off_after_async = wait_offset(&producer, &mq_a, off_after_s1 + 3).await;
    ck.check(
        "S4e 异步放行腿同样真落库（单条 1 + 批量子消息 2，maxOffset 再涨 3）",
        off_after_async == off_after_s1 + 3,
        &format!("maxOffset={off_after_async}"),
    );

    // ---------------- S5 单向：Java 有意没有守卫 ----------------
    println!("\n-- S5 单向定点没有守卫：msg 自己的 topic 说了算 --");
    let k_ow = format!("pinned-oneway-{stamp}");
    let mut ow = Message::new(&topic_a, Some(b"oneway"));
    ow.set_keys(&k_ow);
    match producer.send_oneway(&mut ow, Some(&mq_b)).await {
        Ok(()) => {}
        Err(e) => ck.abort("S5 单向发送本身不该失败", &e.to_string()),
    }
    let off_after_ow = wait_offset(&producer, &mq_a, off_after_async + 1).await;
    ck.check(
        "S5a 单向定点没有守卫：报文按 msg 自己的 topic 落进 A",
        off_after_ow == off_after_async + 1,
        &format!("A maxOffset={off_after_ow}"),
    );
    let b_now = offset_of(&producer, &mq_b).await;
    ck.check(
        "S5b 目标队列所在的 B 一条都没有（是 Java 的口子，不是漏发）",
        b_now == 0,
        &format!("B maxOffset={b_now}"),
    );

    // ---------------- S6 正腿收尾：消息一条不少 ----------------
    println!("\n-- S6 push 消费者收齐正腿消息 --");
    let expected = vec![
        k_single.clone(),
        k_b1.clone(),
        k_b2.clone(),
        format!("pinned-async-single-{stamp}"),
        format!("pinned-async-b1-{stamp}"),
        format!("pinned-async-b2-{stamp}"),
        k_ow.clone(),
    ];
    let deadline = Instant::now() + Duration::from_secs(40);
    let mut got = sink.snapshot();
    while Instant::now() < deadline && !expected.iter().all(|k| got.contains(k)) {
        tokio::time::sleep(Duration::from_millis(500)).await;
        got = sink.snapshot();
    }
    let missing: Vec<&String> = expected.iter().filter(|k| !got.contains(k)).collect();
    ck.check(
        "S6 正腿消息一条不少地被消费到",
        missing.is_empty(),
        &format!("received={got:?} missing={missing:?}"),
    );

    consumer.shutdown();
    producer.shutdown();
    nsprod.shutdown();
    env.cleanup(&mut ck).await;
    env.admin.shutdown();
    ck
}

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().to_string(),
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
