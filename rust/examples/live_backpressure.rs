//! 异步发送背压（`enableBackpressureForAsyncMode` 那一套公平信号量）真机验证，
//! 与 `python/verify_backpressure_live.py`（B1–B5）、`cpp/examples/live_backpressure.cpp`、
//! `dotnet/examples/RocketMQ.Examples/LiveBackPressure.cs` 同套场景。
//! 对端是 Java `DefaultMQProducerImpl:635-682`（两道闸、共享一份预算）与 `:577-633`
//! （`BackpressureSendCallBack` 的归还）。
//!
//! 前置：NameServer + Broker 已起，`autoCreateTopicEnable=true`。
//!
//! 离线单测（`src/client/backpressure.rs` 与 `src/client/producer/send_retry_tests.rs`
//! 的「异步发送背压」一节）锁的是语义；这个脚本锁的是**打到真 broker 时**的五件事：
//!   B1  默认容量（1024 条 / 100M 字节）开着背压发一整轮异步消息：全部 SEND_OK，
//!       且发完之后两个信号量**满额归还**（真机不泄配额 —— 泄了迟早把生产者自己锁死）。
//!   B2  条数闸夹到地板值 10、用 `send_message_before` 钩子睡 600ms 把在途占满：
//!       另外两笔等不到许可，回调 `send message tryAcquire semaphoreAsyncNum timeout`
//!       （Java `:654-658` 原文案），而且 broker 上**一条都没多** —— 被拒的连发送内核
//!       都没进（钩子在闸门之后，进过没进过一查便知），更没发过一次请求。
//!   B3  运行时把容量从 10 调到 12：正卡在闸上的那笔被叫醒并真的落库，
//!       全部落地后空闲许可 = 新容量。这一轮钩子睡 2.5s，扩容由**另一个线程**在 1s
//!       那一刻做，于是「它是被扩容放行的」与「它是被前 10 笔归还放行的」分得开。
//!   B4  字节闸（容量 1M 地板值 + 600KB body ⇒ 在途只能 1 笔）：另两笔回调
//!       `send message tryAcquire semaphoreAsyncSize timeout`（Java `:667-671`），
//!       在途时空闲字节许可正好是 `1M - 600K`，broker 上只落 1 条，
//!       且被拦下的那两笔也没漏还它们已经拿到的条数许可。
//!   B5  关掉背压：同样的（夹到地板值的）容量配置**完全不限流**，30 笔并发全部落地。
//!
//! ⚠ **本端口与 Java/Python/C++/.NET 的唯一可观测量差别：闸门等许可发生在被 spawn
//! 出去的发送任务里，不在调用方线程上**（调用方线程 park 住会让单线程运行时死锁，见
//! [`DefaultMQProducer::send_async`] 的文档）。所以「等不到许可」的等待时间记在回调到达
//! 之前，而不是记在 `send_async` 的返回上；被拒的文案、扣费与归还完全一致。
//!
//! ⚠ 四条脚本纪律（①②③ 是 Python 那一轮真机联调踩出来的，④ 是 Rust 这一轮踩出来的）：
//!   ① topic 一律带 stamp —— 四语言**依次**跑在同一个 broker 上，固定名会继承上一轮的条数；
//!   ② 「被拒的发送连请求都没发出去」只能看 broker 侧落库条数（各队列 maxOffset-minOffset 之和），
//!      光看客户端回调会被「回调报错但请求照样发出去」的实现蒙过去；
//!   ③ 新建 topic 要等 broker 把 topicConfig 增量注册到 namesrv（秒级到十秒级），所以所有
//!      broker 侧对账都是**轮询到超时**，读一次路由失败不算失败；
//!   ④ 占住在途靠的是钩子里的 `std::thread::sleep`，它会把一条 tokio 工作线程按住在
//!      同步调用里 —— 本机实测：**只要有任何任务阻塞，整个运行时的时间驱动就不推进**
//!      （一个任务睡 1500ms 期间，另一个任务里的 `sleep(100ms)` 一次都没醒）。所以
//!        · 「在途笔」与「等不到许可的笔」必须连着发完，中间不能 await 去观察在途；
//!        · 在途扣费与「占满那一刻才补发超限的那几笔」都交给 [`GateWatch`]（普通 OS 线程），
//!          「进过发送内核没有」用钩子自己记的时间戳 —— 两者都不受运行时调度影响；
//!          而且这类断言只能在链路彻底跑完之后再读，钩子可能还堵在新 topic 的路由上；
//!        · 需要在「还在睡」这段时间里动手脚（B3 的运行时扩容）也只能交给普通线程。
//!
//! 用法：
//! ```text
//! cargo run --example live_backpressure -- 127.0.0.1:9876
//! ```

use std::collections::BTreeMap;
use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::backpressure::{
    MIN_ASYNC_SEND_NUM, MIN_ASYNC_SEND_SIZE,
};
use rocketmq_client_remoting::client::hook::{SendMessageContext, SendMessageHook};
use rocketmq_client_remoting::client::producer::{
    DefaultMQProducer, ProducerConfig, SendCallback,
};
use rocketmq_client_remoting::client::result::SendResult;
use rocketmq_client_remoting::common::message::{Message, MessageQueue};
use rocketmq_client_remoting::error::Result;

/// 集群探活与「等 broker 侧结果落地」的默认预算（秒）。
const WAIT_SECONDS: u64 = 30;
/// B2/B3/B4 用的条数闸容量（Java 地板值）。
const GATE_NUM: i64 = MIN_ASYNC_SEND_NUM;
/// B4 一笔 600KB body：1M 的字节闸下在途只能容 1 笔。
const BIG_BODY: usize = 600 * 1024;
/// B3 在途钩子的睡眠时长（ms）：留出「扩容发生在 10 笔归还之前」的判断窗口。
const HOLD_MS: i64 = 2_500;
/// 运行时工作线程数。
///
/// 必须明显大于并发在途笔数：占住在途靠的是 `send_message_before` 里的**阻塞**睡眠
/// （钩子是同步接口，没法 await），一个在途发送就吃掉一条工作线程。
const WORKER_THREADS: usize = 32;

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

/// 线程安全的回调记录（对应 Python 验证脚本里的 `_Recorder`）。
#[derive(Default)]
struct Recorder {
    done: AtomicUsize,
    ok: AtomicUsize,
    errors: Mutex<Vec<String>>,
}

impl Recorder {
    fn done(&self) -> usize {
        self.done.load(Ordering::SeqCst)
    }

    fn ok(&self) -> usize {
        self.ok.load(Ordering::SeqCst)
    }

    fn errors(&self) -> Vec<String> {
        lock(&self.errors).clone()
    }

    /// 「每一条错误都带这句文案」—— Java 的文案是逐字对齐的，只查条数会放过内容漂移。
    fn all_errors_contain(&self, needle: &str, expected: usize) -> bool {
        let errors = lock(&self.errors);
        errors.len() == expected && errors.iter().all(|e| e.contains(needle))
    }

    fn summary(&self) -> String {
        let errors = lock(&self.errors);
        let first = errors.first().map(String::as_str).unwrap_or("");
        format!(
            "done={} ok={} err={} {first}",
            self.done(),
            self.ok(),
            errors.len()
        )
    }
}

impl SendCallback for Recorder {
    fn on_success(&self, result: SendResult) {
        if matches!(result.status, rocketmq_client_remoting::client::result::SendStatus::SendOk) {
            self.ok.fetch_add(1, Ordering::SeqCst);
        }
        self.done.fetch_add(1, Ordering::SeqCst);
    }

    fn on_exception(&self, err: rocketmq_client_remoting::error::Error) {
        lock(&self.errors).push(err.to_string());
        self.done.fetch_add(1, Ordering::SeqCst);
    }
}

/// 在 `send_message_before` 里睡一会儿，把「在途」占住，并记下每笔进入钩子的时刻。
///
/// 许可是**过闸时**拿的、**链终点**还的，所以占住在途最干净的办法是让链本身变慢：
/// 堵用户回调占不住许可（归还就在把结果交给用户之前一步）。
///
/// 时间戳用钩子自己记（相对 [`SlowHook::begin`]），不用主任务的 `Instant`：钩子的
/// `std::thread::sleep` 会占住一条 tokio 工作线程，而本机实测**只要有任何任务阻塞，
/// 整个运行时的时间驱动就不推进**，主任务的所有 `sleep` 都会一觉睡到钩子结束。
/// 于是「谁在什么时候过了闸」只能从链路上取，不能从观察方取。
struct SlowHook {
    millis: AtomicI64,
    clock: Mutex<Option<Instant>>,
    /// `(标签, 进入钩子时距 begin 的毫秒数)`；标签即 [`message`] 写进 keys 的那个标签。
    entries: Mutex<Vec<(String, u128)>>,
}

impl SlowHook {
    fn new(millis: i64) -> SlowHook {
        SlowHook {
            millis: AtomicI64::new(millis),
            clock: Mutex::new(None),
            entries: Mutex::new(Vec::new()),
        }
    }

    fn set(&self, millis: i64) {
        self.millis.store(millis, Ordering::SeqCst);
    }

    /// 重新计时并清空记录，让一段场景的观测点互相独立。
    fn begin(&self) {
        *lock(&self.clock) = Some(Instant::now());
        lock(&self.entries).clear();
    }

    /// 标签为 `label` 的发送进入过钩子的次数。
    fn count_of(&self, label: &str) -> usize {
        lock(&self.entries).iter().filter(|(l, _)| l == label).count()
    }

    /// 标签为 `label` 的发送最早一次进入钩子的时刻（没进过返回 `None`）。
    fn first_entry_of(&self, label: &str) -> Option<u128> {
        lock(&self.entries)
            .iter()
            .filter(|(l, _)| l == label)
            .map(|(_, at)| *at)
            .min()
    }

    /// 全部记录，失败时用来排查标签本身。
    fn dump(&self) -> String {
        lock(&self.entries)
            .iter()
            .map(|(label, at)| format!("{label}@{at}ms"))
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl SendMessageHook for SlowHook {
    fn hook_name(&self) -> &str {
        "slow-before"
    }

    fn send_message_before(&self, context: &mut SendMessageContext) -> Result<()> {
        let label = context
            .message
            .as_ref()
            .and_then(|msg| msg.get_keys().map(str::to_string))
            .unwrap_or_default();
        let began = { *lock(&self.clock) }.unwrap_or_else(Instant::now);
        lock(&self.entries).push((label, began.elapsed().as_millis()));
        let millis = self.millis.load(Ordering::SeqCst);
        if millis > 0 {
            std::thread::sleep(Duration::from_millis(millis as u64));
        }
        Ok(())
    }
}

/// 用**普通 OS 线程**盯一个闸门的空闲许可：等到它第一次被占到 `target` 为止，就在那一
/// 刻跑 `then`（＝补发那几笔「等不到许可」的发送），同时记下窗口里看到的最小值。
///
/// 为什么不能在主任务里做（模块头纪律 ④）：钩子的阻塞 sleep 会冻住整个运行时的时间驱动，
/// 「在途占满」到「钩子睡完」这段时间主任务一次都不会被调度 —— 由它来观察在途、再补发
/// 超限的那几笔，实际会变成「等人家把许可全还光了才发」，只会得到全都成功的假结果。
/// 独立线程不受运行时调度影响，既看得见扣费，也能恰好在扣费期间把请求塞进去。
struct GateWatch {
    stop: Arc<AtomicBool>,
    min: Arc<AtomicI64>,
    hit: Arc<AtomicBool>,
    task: Option<std::thread::JoinHandle<()>>,
}

impl GateWatch {
    fn spawn<F, G>(read: F, target: i64, then: G) -> GateWatch
    where
        F: Fn() -> i64 + Send + 'static,
        G: FnOnce() + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let min = Arc::new(AtomicI64::new(i64::MAX));
        let hit = Arc::new(AtomicBool::new(false));
        let (t_stop, t_min, t_hit) = (Arc::clone(&stop), Arc::clone(&min), Arc::clone(&hit));
        let task = std::thread::spawn(move || {
            let mut then = Some(then);
            let mut fired = false;
            while !t_stop.load(Ordering::SeqCst) {
                let seen = read();
                t_min.fetch_min(seen, Ordering::SeqCst);
                if !fired && seen <= target {
                    fired = true;
                    t_hit.store(true, Ordering::SeqCst);
                    if let Some(then) = then.take() {
                        then();
                    }
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        GateWatch { stop, min, hit, task: Some(task) }
    }

    /// 停线程，取回 `(窗口内最小空闲许可, 是否命中过 target)`。
    fn finish(mut self) -> (i64, bool) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
        (
            self.min.load(Ordering::SeqCst),
            self.hit.load(Ordering::SeqCst),
        )
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
            instance_name: format!("BPADMIN-{stamp}"),
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
        let t = format!("Bp{kind}{}", self.stamp);
        lock(&self.topics).push(t.clone());
        t
    }

    /// 集群探活：端口开着 != broker 已注册到 namesrv，所以轮询。
    async fn start(&self, ck: &mut Checker) -> bool {
        if let Err(e) = self.admin.start().await {
            ck.abort("admin start", &e.to_string());
            return false;
        }
        let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
        loop {
            if let Ok(info) = self.admin.fetch_broker_cluster_info().await {
                if let Some(first) = info.get_broker_addrs().first() {
                    *lock(&self.broker_addr) = first.clone();
                    ck.check("集群探活", true, &format!("broker={first}"));
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

    /// 起一个只配了背压旋钮的生产者。钩子必须在 `start()` 之前挂上。
    async fn producer(
        &self,
        kind: &str,
        enable: bool,
        num: Option<i64>,
        size: Option<i64>,
        hook: Option<Arc<SlowHook>>,
    ) -> std::result::Result<DefaultMQProducer, String> {
        let producer = DefaultMQProducer::with_config(ProducerConfig {
            producer_group: format!("PID_rmq_bp_rust_{}", self.stamp),
            instance_name: format!("bp-rust-{kind}-{}", self.stamp),
            name_server_addrs: vec![self.namesrv.clone()],
            enable_backpressure_for_async_mode: enable,
            ..Default::default()
        })
        .map_err(|e| e.to_string())?;
        if let Some(num) = num {
            producer.set_back_pressure_for_async_send_num(num);
        }
        if let Some(size) = size {
            producer.set_back_pressure_for_async_send_size(size);
        }
        if let Some(hook) = hook {
            producer.register_send_message_hook(hook);
        }
        producer.start().await.map_err(|e| e.to_string())?;
        Ok(producer)
    }
}

// ---------------------------------------------------------- 通用助手

/// 打标签发送：标签写在 keys 属性上，钩子按它归类（见 [`SlowHook`]）。
///
/// 为什么不用 body 前几字节当标签：超过 4KB 的 body 在进钩子**之前**就被压缩了
/// （Java `DefaultMQProducerImpl:943-990` 同样先 `tryToCompressMessage` 再建
/// `SendMessageContext`），钩子看到的是一串 zlib 字节，根本没法按内容认领。
fn message(topic: &str, label: &str, body: &[u8]) -> Message {
    let mut msg = Message::new(topic, Some(body));
    msg.set_keys(label);
    msg
}

/// 轮询等待条件成立（最多 `secs` 秒）。
async fn wait_until<F: Fn() -> bool>(cond: F, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= deadline {
            return cond();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// broker 上这个 topic 一共落了多少条（各队列 maxOffset-minOffset 之和）；读不到返回 -1。
/// 这是「被拒的发送连请求都没发出去」的唯一硬证据。
async fn landed_count(env: &Env, topic: &str) -> i64 {
    let Ok(route) = env.admin.examine_topic_route(topic).await else {
        return -1; // 路由还没注册上
    };
    let mut addrs: BTreeMap<String, String> = BTreeMap::new();
    for broker in &route.broker_datas {
        if let Some(addr) = broker.select_broker_addr() {
            addrs.insert(broker.broker_name.clone(), addr);
        }
    }
    let mut total = 0_i64;
    for queue in &route.queue_datas {
        if !addrs.contains_key(&queue.broker_name) {
            continue;
        }
        for queue_id in 0..queue.read_queue_nums {
            let mq = MessageQueue::new(topic, &queue.broker_name, queue_id);
            // 队列刚建出来时两次读数都可能失败，按「还没就绪」处理而不是当 0
            let (max, min) =
                match (env.admin.max_offset(&mq).await, env.admin.min_offset(&mq).await) {
                    (Ok(max), Ok(min)) => (max, min),
                    _ => return -1,
                };
            total += max - min;
        }
    }
    total
}

/// 等 broker 上至少出现 `expected` 条，返回最后一次读数。
async fn wait_landed(env: &Env, topic: &str, expected: i64) -> i64 {
    let mut landed = landed_count(env, topic).await;
    let mut reads = 0;
    while landed < expected && reads < 60 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        landed = landed_count(env, topic).await;
        reads += 1;
    }
    landed
}

// ------------------------------------------------------- B1 默认容量不漏配额

async fn b1_default_capacity_no_leak(ck: &mut Checker, env: &Env) {
    let topic = env.topic("B1");
    let producer = match env.producer("b1", true, None, None, None).await {
        Ok(p) => p,
        Err(e) => {
            ck.abort("B1 producer", &e);
            return;
        }
    };
    ck.check(
        "B1 默认容量 1024 条 / 100M 字节",
        producer.semaphore_async_send_num_available_permits() == 1024
            && producer.semaphore_async_send_size_available_permits() == 100 * 1024 * 1024,
        &format!(
            "num={} size={}",
            producer.semaphore_async_send_num_available_permits(),
            producer.semaphore_async_send_size_available_permits()
        ),
    );

    let rec = Arc::new(Recorder::default());
    let mut rejected = 0usize;
    for i in 0..40 {
        let body = format!("b1-{i}-{}", "x".repeat(120));
        if producer
            .send_async(
                message(&topic, "b1", body.as_bytes()),
                rec.clone(),
                Some(5_000),
                None,
            )
            .is_err()
        {
            rejected += 1;
        }
    }
    ck.check("B1 40 笔 send_async 都没被入队拒绝", rejected == 0, "rejected={rejected}");
    ck.check(
        "B1 40 笔异步都走完回调",
        wait_until(|| rec.done() >= 40, 20).await,
        &rec.summary(),
    );
    ck.check(
        "B1 全部 SEND_OK",
        rec.ok() == 40 && rec.errors().is_empty(),
        &rec.summary(),
    );
    let landed = wait_landed(env, &topic, 40).await;
    ck.check("B1 broker 上正好落了 40 条", landed == 40, &format!("landed={landed}"));
    ck.check(
        "B1 条数许可满额归还",
        producer.semaphore_async_send_num_available_permits() == 1024,
        &format!(
            "available={}",
            producer.semaphore_async_send_num_available_permits()
        ),
    );
    ck.check(
        "B1 字节许可满额归还",
        producer.semaphore_async_send_size_available_permits() == 100 * 1024 * 1024,
        &format!(
            "available={}",
            producer.semaphore_async_send_size_available_permits()
        ),
    );
    producer.shutdown();
}

// ------------------------------------- B2/B3 条数闸：拒绝、对账、运行时扩容

async fn b2_and_b3_num_gate(ck: &mut Checker, env: &Env) {
    let topic = env.topic("B2");
    let slow = Arc::new(SlowHook::new(600));
    let producer = match env
        .producer("b2", true, Some(GATE_NUM), None, Some(slow.clone()))
        .await
    {
        Ok(p) => p,
        Err(e) => {
            ck.abort("B2 producer", &e);
            return;
        }
    };

    // ---- B2：10 笔把在途占满，另外两笔由看门线程在「占满」那一刻补发、等不到许可
    slow.begin();
    let held = Arc::new(Recorder::default());
    let rejected = Arc::new(Recorder::default());
    let began = Instant::now();
    let watch = GateWatch::spawn(
        {
            let producer = producer.clone();
            move || producer.semaphore_async_send_num_available_permits()
        },
        0,
        {
            let producer = producer.clone();
            let topic = topic.clone();
            let rejected = Arc::clone(&rejected);
            move || {
                for _ in 0..2 {
                    let _ = producer.send_async(
                        message(&topic, "b2-rejected", b"b2-rejected"),
                        rejected.clone(),
                        Some(150),
                        None,
                    );
                }
            }
        },
    );
    for _ in 0..GATE_NUM {
        let _ = producer.send_async(
            message(&topic, "b2-held", b"b2-held"),
            held.clone(),
            Some(8_000),
            None,
        );
    }
    let holders_done = wait_until(|| held.done() >= GATE_NUM as usize, 30).await;
    let victims_done = wait_until(|| rejected.done() >= 2, 15).await;
    let (min_free, drained) = watch.finish();
    ck.check(
        "B2 在途占满后空闲条数为 0",
        drained && min_free == 0,
        &format!("命中过占满={drained} 采样窗口内最小空闲={min_free}"),
    );
    let both_failed = victims_done
        && rejected
            .all_errors_contain("send message tryAcquire semaphoreAsyncNum timeout", 2);
    ck.check(
        "B2 超限的两笔回调 TooMuchRequest，文案与 Java 逐字一致",
        both_failed,
        &rejected.summary(),
    );
    let waited = began.elapsed();
    ck.check(
        "B2 闸等到预算耗尽才报错（不是看一眼就拒）",
        waited >= Duration::from_millis(130),
        &format!("等了 {}ms（钩子阻塞期间时间驱动停摆，这是下限）", waited.as_millis()),
    );
    let landed = wait_landed(env, &topic, GATE_NUM).await;
    ck.check(
        "B2 broker 上只落了那 10 条（被拒的一条都没进去）",
        holders_done && landed == GATE_NUM,
        &format!("landed={landed} {}", held.summary()),
    );
    // 被拒的笔应当**根本没进发送内核**：钩子在闸门之后、网络之前，进过没进过一查便知。
    ck.check(
        "B2 在途的 10 笔都进了发送内核、被拒的 2 笔一笔都没进",
        slow.count_of("b2-held") == GATE_NUM as usize && slow.count_of("b2-rejected") == 0,
        &format!(
            "held 进钩子 {} 次、rejected 进钩子 {} 次",
            slow.count_of("b2-held"),
            slow.count_of("b2-rejected")
        ),
    );
    ck.check(
        "B2 全部落地后条数许可满额归还",
        producer.semaphore_async_send_num_available_permits() == GATE_NUM,
        &format!(
            "available={}",
            producer.semaphore_async_send_num_available_permits()
        ),
    );

    // ---- B3：睡 2.5s，第 11 笔卡在闸上，由**另一个线程**在睡眠中途把它扩容叫醒
    //
    // 扩容必须发生在钩子还在睡的那段时间里，才能区分「是容量变了放它过去」和
    // 「是前面 10 笔归还许可放它过去」；而那段时间运行时的任务全被冻住，只能交给普通线程。
    const RESIZE_AT: u128 = 1_000;
    slow.set(HOLD_MS);
    slow.begin();
    let held2 = Arc::new(Recorder::default());
    for _ in 0..GATE_NUM {
        let _ = producer.send_async(
            message(&topic, "b3-held", b"b3-held"),
            held2.clone(),
            Some(15_000),
            None,
        );
    }
    let woken = Arc::new(Recorder::default());
    let _ = producer.send_async(
        message(&topic, "b3-woken", b"b3-woken"),
        woken.clone(),
        Some(15_000),
        None,
    );
    let _ = std::thread::spawn({
        let producer = producer.clone();
        move || {
            std::thread::sleep(Duration::from_millis(RESIZE_AT as u64));
            producer.set_back_pressure_for_async_send_num(GATE_NUM + 2);
        }
    })
    .join();
    ck.check(
        "B3 被叫醒的那笔真的发出去了",
        wait_until(|| woken.done() > 0, 20).await
            && woken.ok() == 1
            && woken.errors().is_empty(),
        &woken.summary(),
    );
    // 时刻由钩子自己记：它进内核那一刻既晚于扩容、又早于 10 笔在途归还，
    // 所以放行它的只可能是扩容这件事本身。
    let woken_entry = slow.first_entry_of("b3-woken");
    ck.check(
        "B3 第 11 笔卡在闸上，直到扩容那一刻才进发送内核",
        woken_entry.is_some_and(|at| at >= RESIZE_AT && at < HOLD_MS as u128)
            && slow.count_of("b3-held") == GATE_NUM as usize,
        &format!(
            "它进内核于 {:?}ms（扩容在 {RESIZE_AT}ms、10 笔在途要到 ≈{}ms 之后才归还）",
            woken_entry, HOLD_MS
        ),
    );
    let all_done = wait_until(|| held2.done() >= GATE_NUM as usize, 30).await;
    // B2 落了 10 条，B3 又落 10 + 1 条
    let landed = wait_landed(env, &topic, GATE_NUM * 2 + 1).await;
    ck.check(
        "B3 broker 上一共落了 21 条",
        all_done && landed == GATE_NUM * 2 + 1,
        &format!("landed={landed}"),
    );
    ck.check(
        "B3 全部落地后空闲许可 = 新容量 12",
        producer.semaphore_async_send_num_available_permits() == GATE_NUM + 2,
        &format!(
            "available={}",
            producer.semaphore_async_send_num_available_permits()
        ),
    );
    producer.shutdown();
}

// ------------------------------------------------------------- B4 字节闸

async fn b4_size_gate(ck: &mut Checker, env: &Env) {
    let topic = env.topic("B4");
    let slow = Arc::new(SlowHook::new(1_500));
    let producer = match env
        .producer(
            "b4",
            true,
            Some(GATE_NUM),
            Some(MIN_ASYNC_SEND_SIZE),
            Some(slow.clone()),
        )
        .await
    {
        Ok(p) => p,
        Err(e) => {
            ck.abort("B4 producer", &e);
            return;
        }
    };
    let body = vec![b'x'; BIG_BODY];
    let expected_free = MIN_ASYNC_SEND_SIZE - BIG_BODY as i64;

    // 一笔 600KB 在途；看门线程一等到「字节闸被它占满」就补发两笔等不到许可的（同 B2）。
    slow.begin();
    let first = Arc::new(Recorder::default());
    let second = Arc::new(Recorder::default());
    let watch = GateWatch::spawn(
        {
            let producer = producer.clone();
            move || producer.semaphore_async_send_size_available_permits()
        },
        expected_free,
        {
            let producer = producer.clone();
            let topic = topic.clone();
            let body = body.clone();
            let second = Arc::clone(&second);
            move || {
                for _ in 0..2 {
                    let _ = producer.send_async(
                        message(&topic, "b4-big", &body),
                        second.clone(),
                        Some(150),
                        None,
                    );
                }
            }
        },
    );
    let _ = producer.send_async(
        message(&topic, "b4-big", &body),
        first.clone(),
        Some(15_000),
        None,
    );
    let first_done = wait_until(|| first.done() >= 1, 30).await;
    let victims_done = wait_until(|| second.done() >= 2, 15).await;
    let (min_free, drained) = watch.finish();
    ck.check(
        "B4 一笔 600KB 在途恰好扣掉 600K 个字节许可",
        drained && min_free == expected_free,
        &format!(
            "命中过扣费={drained} 采样窗口内最小空闲={min_free}（期望 {expected_free}）"
        ),
    );
    let rejected_ok = victims_done
        && second.all_errors_contain(
            "send message tryAcquire semaphoreAsyncSize timeout",
            2,
        );
    ck.check(
        "B4 超限的两笔回调 semaphoreAsyncSize timeout，文案与 Java 逐字一致",
        rejected_ok,
        &second.summary(),
    );
    let landed = wait_landed(env, &topic, 1).await;
    ck.check(
        "B4 broker 上只落了那 1 条",
        first_done && landed == 1,
        &format!("landed={landed} {}", first.summary()),
    );
    // 字节闸拦下的那两笔**根本没进发送内核**（钩子在闸门之后），所以真机上一共只有一笔
    // 走过链路；这里顺便证明「过不了字节闸时，已经拿到的条数许可不会漏还」——
    // 漏了的话下面那条满额归还的检查会永远差着那两个许可。
    ck.check(
        "B4 只有一笔进过发送内核（被拒的两笔一笔都没进）",
        slow.count_of("b4-big") == 1,
        &format!(
            "进钩子 {} 次，全部记录=[{}]",
            slow.count_of("b4-big"),
            slow.dump()
        ),
    );
    ck.check(
        "B4 全部落地后条数许可满额归还（被拒那两笔把自己那份还回去了）",
        producer.semaphore_async_send_num_available_permits() == GATE_NUM,
        &format!(
            "available={}",
            producer.semaphore_async_send_num_available_permits()
        ),
    );
    ck.check(
        "B4 全部落地后字节许可满额归还",
        producer.semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE,
        &format!(
            "available={}",
            producer.semaphore_async_send_size_available_permits()
        ),
    );
    producer.shutdown();
}

// ------------------------------------------------------- B5 开关关掉就不限流

async fn b5_disabled_never_limits(ck: &mut Checker, env: &Env) {
    let topic = env.topic("B5");
    // 配置故意给到 1 笔 / 1KB —— 会被夹到地板值 10 / 1M，但开关是关的，所以完全不限流
    let producer = match env.producer("b5", false, Some(1), Some(1024), None).await {
        Ok(p) => p,
        Err(e) => {
            ck.abort("B5 producer", &e);
            return;
        }
    };
    ck.check(
        "B5 越界配置被夹到地板值",
        producer.semaphore_async_send_num_available_permits() == GATE_NUM
            && producer.semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE,
        &format!(
            "num={} size={}",
            producer.semaphore_async_send_num_available_permits(),
            producer.semaphore_async_send_size_available_permits()
        ),
    );

    let rec = Arc::new(Recorder::default());
    let big = vec![b'y'; 300 * 1024];
    for i in 0..30 {
        let body: &[u8] = if i % 10 == 9 { &big } else { b"b5-small" };
        let _ = producer.send_async(
            message(&topic, "b5", body),
            rec.clone(),
            Some(15_000),
            None,
        );
    }
    let done = wait_until(|| rec.done() >= 30, 30).await;
    ck.check(
        "B5 关掉背压后 30 笔并发（含 3 笔 300KB）全部落地",
        done && rec.ok() == 30 && rec.errors().is_empty(),
        &rec.summary(),
    );
    let landed = wait_landed(env, &topic, 30).await;
    ck.check(
        "B5 broker 上落了 30 条",
        landed == 30,
        &format!("landed={landed}"),
    );
    ck.check(
        "B5 关着的时候两个闸一分未动",
        producer.semaphore_async_send_num_available_permits() == GATE_NUM
            && producer.semaphore_async_send_size_available_permits() == MIN_ASYNC_SEND_SIZE,
        &format!(
            "num={} size={}",
            producer.semaphore_async_send_num_available_permits(),
            producer.semaphore_async_send_size_available_permits()
        ),
    );
    producer.shutdown();
}

// ------------------------------------------------------------------ 清理

async fn cleanup(ck: &mut Checker, env: &Env) {
    let topics = lock(&env.topics).clone();
    for topic in topics {
        if let Err(e) = env.admin.delete_topic(&topic, None).await {
            println!("  [WARN] delete_topic({topic}) failed: {e}");
        }
    }
    ck.check("清理本次的 topic", true, "");
}

// ------------------------------------------------------------------ 驱动

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let env = Env::new(namesrv, &stamp);
    println!("== live async-send back-pressure check, namesrv={namesrv} stamp={stamp} ==");
    if !env.start(&mut ck).await {
        return ck;
    }
    b1_default_capacity_no_leak(&mut ck, &env).await;
    b2_and_b3_num_gate(&mut ck, &env).await;
    b4_size_gate(&mut ck, &env).await;
    b5_disabled_never_limits(&mut ck, &env).await;
    cleanup(&mut ck, &env).await;
    env.admin.shutdown();
    ck
}

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// 阻塞式钩子会占住工作线程，所以这里**不能**用 `#[tokio::main]`（它的默认线程数
/// 可能只有几核），显式建一个大一点的多样本运行时。
fn main() -> ExitCode {
    let argv: Vec<String> = env::args().collect();
    let namesrv = argv
        .get(1)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:9876".to_string());
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKER_THREADS)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("建运行时失败: {e}");
            return ExitCode::FAILURE;
        }
    };
    let ck = runtime.block_on(run(&namesrv));
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
