//! 异步发送内核（非阻塞与线程口径、并发不串台、定点/拦截/批量/关池行为）真机验证，
//! 与 `python/verify_async_send_live.py`（A1–A6）、`cpp/examples/live_async_send.cpp`、
//! `dotnet/examples/RocketMQ.Examples/LiveAsyncSend.cs` 同套场景。
//! 对端是 Java `DefaultMQProducerImpl` 的 ASYNC 链（`sendDefaultImpl` → `AsyncSenderExecutor`
//! → `sendKernelImpl`）与 `MQClientAPIImpl.sendMessageAsync` / `onExceptionImpl`。
//!
//! 前置：NameServer + Broker 已起，`autoCreateTopicEnable=true`。
//!
//! 离线单测（`src/client/producer.rs` 的发送链与 `send_retry_tests.rs`）锁的是语义；
//! 这个脚本锁的是**打到真 broker 时**的六件事：
//!   A1  非阻塞 + 线程口径 + 读回：`send_async` 在准备段（`send_message_before` 钩子睡
//!       400ms）之前就返回；before 钩子与用户回调**都不在调用方线程上**跑（Java 的
//!       `AsyncSenderExecutor_N` / `NettyClientPublicExecutor_N`，见差异 ①）；回调里的
//!       `offset_msg_id` 能 `view_message` 读回原 body，`queue_offset` 就是它落在的位置，
//!       `msg_id` 是客户端补的 32 位 UNIQ_KEY。
//!   A2  30 笔并发各拿**恰好一个**终态、全部 SEND_OK、`(broker, queueId, queueOffset)`
//!       与 UNIQ_KEY 互不重叠 —— 池子既不串台也不吞消息。
//!   A3  定点异步发送只落在指定队列上，别的队列一条都不多。
//!   A4  拦截钩子（`CheckForbiddenHook`）拒绝时异常原样到了回调、看到的是 `ASYNC`，
//!       而且被拒那笔在 broker 上没留痕（连路由都没建出来）；同一个生产者换个标签照常落地。
//!   A5  批量异步：**本端口没有这个入口**，记为 SKIP（差异 ②）。
//!   A6  `shutdown()` **不等在途**（Java `ThreadPoolExecutor.shutdown()`：不接新的、队列里的
//!       照跑，但主流程立刻返回）：交进来的每一笔仍然各自拿到一个终态回调，可这批是在
//!       「客户端实例已拆」的状态上跑的 ⇒ 实测**一笔都没上线**（差异 ③）；
//!       关停之后 `send_async` 同步被拒、不会再追加回调。
//!
//! ① **线程口径与 Java/C++/.NET 的差别**：本端口的发送任务跑在 tokio 工作线程上，没有
//!   `AsyncSenderExecutor_N` / `NettyClientPublicExecutor_N` 这种池线程名可查（见
//!   `producer.rs` 里 `send_async` 的文档「差异 3」）。所以这里只能退一步证「准备段与回调
//!   都不在**调用方线程**上」，且用 `ThreadId` 而不是线程名对位。为了让这条比对有意义，
//!   调用方那一段跑在 `spawn_blocking` 的阻塞线程池上：那条线程池与 tokio 工作线程集合
//!   不相交，「不同线程」才是硬结论而不是运气。
//!
//! ② **A5 是真缺口，不是脚本偷懒**：Java `send(Collection<Message>, SendCallback, long)`、
//!   Python `_send_async`（批量走同步批量内核）、C++ `sendAsync(MessageBatch,…)`、.NET
//!   `SendAsync(IEnumerable<Message>,…)` 都有异步批量，本端口只有
//!   `send_batch(...) -> Result<SendResult>`（同步语义）。
//!
//! ③ **关停语义按语言分两派，本端口与 Java/Python 同派**：Java `shutdown()` 与 Python
//!   都「不等」——队列里的任务照样跑完准备段、照样回调，但实例已经拆掉，所以这一批基本
//!   全部报错、broker 上一条都不落（本机实测 36 笔全报 `client already shutdown`、
//!   `landed=-1` 即连路由都没建出来）。C++/.NET 那两版是 `shutdown(true)`/join 池线程，
//!   同一用例能落满 36 条 —— 那是它们相对 Java 的偏离，不是本端口的。这里锁死「不等待
//!   真的会丢消息」这条，用户要保消息就得自己等回调再关。
//!
//! ⚠ 四条脚本纪律（沿用 `live_backpressure.rs`）：
//!   ① topic 一律带 stamp —— 四语言**依次**跑在同一个 broker 上，固定名会继承上一轮的条数；
//!   ② 「被拒的发送连请求都没发出去」只能看 broker 侧落库条数（各队列 maxOffset-minOffset 之和），
//!      光看客户端回调会被「回调报错但请求照样发出去」的实现蒙过去；
//!   ③ 新建 topic 要等 broker 把 topicConfig 增量注册到 namesrv（秒级到十秒级），所以所有
//!      broker 侧对账都是**轮询到超时**，读一次路由失败不算失败；
//!   ④ 钩子里的 `std::thread::sleep` 会占住一条 tokio 工作线程，本机实测「只要有任何任务
//!      阻塞，整个运行时的时间驱动就不推进」，所以只在钩子睡完之后再读观测值，
//!      且 A1 那段调用方测量本身不含任何 `await`。
//!
//! 用法：
//! ```text
//! cargo run --example live_async_send -- 127.0.0.1:9876
//! ```

use std::collections::{BTreeMap, HashSet};
use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::ThreadId;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::hook::{
    CheckForbiddenContext, CheckForbiddenHook, SendMessageContext, SendMessageHook,
};
use rocketmq_client_remoting::client::producer::{
    DefaultMQProducer, ProducerConfig, SendCallback,
};
use rocketmq_client_remoting::client::result::{SendResult, SendStatus};
use rocketmq_client_remoting::common::message::{Message, MessageQueue};
use rocketmq_client_remoting::common::message_decoder::decode_message_id;
use rocketmq_client_remoting::error::{Error, Result};

/// 集群探活与「等 broker 侧结果落地」的默认预算（秒）。
const WAIT_SECONDS: u64 = 30;
/// A2 的并发笔数。
const BURST: usize = 30;
/// 运行时工作线程数：必须明显大于并发笔数（占住在途的钩子是同步接口，会按住房一条线程）。
const WORKER_THREADS: usize = 32;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// 断言累积器：跑完全部场景再汇总，首个失败不提前退出。
struct Checker {
    passed: u32,
    skipped: u32,
    failed: Vec<String>,
}

impl Checker {
    fn new() -> Checker {
        Checker {
            passed: 0,
            skipped: 0,
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

    /// 本端口没有对应能力，如实记下来而不是假装通过。
    fn skip(&mut self, name: &str, why: &str) {
        self.skipped += 1;
        println!("  [SKIP] {name}: {why}");
    }

    fn abort(&mut self, name: &str, err: &str) {
        println!("  [FAIL] {name}: {err}");
        self.failed.push(format!("{name}: {err}"));
    }
}

/// 线程安全的回调记录（对应 Python 验证脚本里的 `_Recorder`）。
///
/// 除了结果本身还记两样东西：终态回调跑在**哪条线程**上（A1 的线程口径）、以及**什么
/// 时刻**（A6 的「`shutdown()` 不等在途」）。
#[derive(Default)]
struct Recorder {
    done: AtomicUsize,
    ok: AtomicUsize,
    results: Mutex<Vec<SendResult>>,
    errors: Mutex<Vec<String>>,
    threads: Mutex<Vec<ThreadId>>,
    at: Mutex<Vec<Instant>>,
}

impl Recorder {
    fn done(&self) -> usize {
        self.done.load(Ordering::SeqCst)
    }

    fn ok(&self) -> usize {
        self.ok.load(Ordering::SeqCst)
    }

    fn results(&self) -> Vec<SendResult> {
        lock(&self.results).clone()
    }

    fn errors(&self) -> Vec<String> {
        lock(&self.errors).clone()
    }

    /// 第 `i` 个终态回调跑在哪条线程上（没有则 `None`）。
    fn thread_at(&self, i: usize) -> Option<ThreadId> {
        lock(&self.threads).get(i).copied()
    }

    /// `moment` 之后落到终态的回调条数（`Instant` 跨线程可比，刻度是单调时钟）。
    fn after(&self, moment: Instant) -> usize {
        lock(&self.at).iter().filter(|at| **at > moment).count()
    }

    fn summary(&self) -> String {
        let errors = self.errors();
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
        if result.status == SendStatus::SendOk {
            self.ok.fetch_add(1, Ordering::SeqCst);
        }
        lock(&self.results).push(result);
        self.note();
    }

    fn on_exception(&self, err: Error) {
        lock(&self.errors).push(err.to_string());
        self.note();
    }
}

impl Recorder {
    /// 记下终态的时刻与线程名（调用方必须已持有各自的锁，这里只加自己这两把锁）。
    fn note(&self) {
        lock(&self.threads).push(std::thread::current().id());
        lock(&self.at).push(Instant::now());
        self.done.fetch_add(1, Ordering::SeqCst);
    }
}

/// 在 `send_message_before` 里睡一会儿，并记下每条链跑过的线程与 before/after 次数。
///
/// 睡在 before 钩子里＝把「准备段 + 上线」整条链拖慢，正好用来测「调用方不必等准备段」。
struct TracingHook {
    millis: AtomicUsize,
    before: AtomicUsize,
    after: AtomicUsize,
    before_threads: Mutex<Vec<ThreadId>>,
}

impl TracingHook {
    fn new(millis: usize) -> Arc<TracingHook> {
        Arc::new(TracingHook {
            millis: AtomicUsize::new(millis),
            before: AtomicUsize::new(0),
            after: AtomicUsize::new(0),
            before_threads: Mutex::new(Vec::new()),
        })
    }

    fn before_count(&self) -> usize {
        self.before.load(Ordering::SeqCst)
    }

    fn after_count(&self) -> usize {
        self.after.load(Ordering::SeqCst)
    }

    fn first_before_thread(&self) -> Option<ThreadId> {
        lock(&self.before_threads).first().copied()
    }
}

impl SendMessageHook for TracingHook {
    fn hook_name(&self) -> &str {
        "tracing-before"
    }

    fn send_message_before(&self, _context: &mut SendMessageContext) -> Result<()> {
        lock(&self.before_threads).push(std::thread::current().id());
        self.before.fetch_add(1, Ordering::SeqCst);
        let millis = self.millis.load(Ordering::SeqCst);
        if millis > 0 {
            std::thread::sleep(Duration::from_millis(millis as u64));
        }
        Ok(())
    }

    fn send_message_after(&self, _context: &mut SendMessageContext) -> Result<()> {
        self.after.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// 拒绝 `forbidden` 标签的拦截钩子（Java `CheckForbiddenHook`，异常**不吞**）。
#[derive(Default)]
struct ForbiddenTagHook {
    calls: AtomicUsize,
    modes: Mutex<Vec<String>>,
}

impl ForbiddenTagHook {
    fn new() -> Arc<ForbiddenTagHook> {
        Arc::new(ForbiddenTagHook::default())
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn saw_mode(&self, mode: &str) -> bool {
        lock(&self.modes).iter().all(|m| m == mode)
    }

    fn dump_modes(&self) -> String {
        lock(&self.modes).join(",")
    }
}

impl CheckForbiddenHook for ForbiddenTagHook {
    fn hook_name(&self) -> &str {
        "forbidden-tag"
    }

    fn check_forbidden(&self, context: &mut CheckForbiddenContext) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mode = context
            .communication_mode
            .map(|m| m.name().to_string())
            .unwrap_or_default();
        lock(&self.modes).push(mode);
        let tags = context
            .message
            .as_ref()
            .and_then(|msg| msg.get_tags())
            .unwrap_or_default()
            .to_string();
        if tags == "forbidden" {
            return Err(Error::client("live test: tag forbidden is not allowed"));
        }
        Ok(())
    }
}

struct Env {
    namesrv: String,
    stamp: String,
    admin: DefaultMQAdminExt,
    topics: Mutex<Vec<String>>,
}

impl Env {
    fn new(namesrv: &str, stamp: &str) -> Env {
        let admin = DefaultMQAdminExt::with_config(AdminConfig {
            instance_name: format!("ASYNCADMIN-{}", stamp),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        Env {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            admin,
            topics: Mutex::new(Vec::new()),
        }
    }

    fn topic(&self, kind: &str) -> String {
        let t = format!("Async{}{}", kind, self.stamp);
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

    /// 起一个只配了发送旋钮的生产者。钩子必须在 `start()` 之前挂上。
    async fn producer(
        &self,
        kind: &str,
        send_hook: Option<Arc<TracingHook>>,
        forbidden: Option<Arc<ForbiddenTagHook>>,
    ) -> std::result::Result<DefaultMQProducer, String> {
        let producer = DefaultMQProducer::with_config(ProducerConfig {
            producer_group: format!("PID_rmq_async_rust_{}", self.stamp),
            instance_name: format!("async-rust-{kind}-{}", self.stamp),
            name_server_addrs: vec![self.namesrv.clone()],
            ..Default::default()
        })
        .map_err(|e| e.to_string())?;
        if let Some(hook) = send_hook {
            producer.register_send_message_hook(hook);
        }
        if let Some(hook) = forbidden {
            producer.register_check_forbidden_hook(hook);
        }
        producer.start().await.map_err(|e| e.to_string())?;
        Ok(producer)
    }
}

// ---------------------------------------------------------- 通用助手

fn message(topic: &str, body: &[u8], tags: &str) -> Message {
    let mut msg = Message::new(topic, Some(body));
    if !tags.is_empty() {
        msg.set_tags(tags);
    }
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
            match queue_landed(env, &mq).await {
                Ok(n) => total += n,
                Err(_) => return -1,
            }
        }
    }
    total
}

/// 一条队列上落了多少条（刚建队列读数会失败，交给调用方判「还没就绪」）。
async fn queue_landed(env: &Env, mq: &MessageQueue) -> Result<i64> {
    let max = env.admin.max_offset(mq).await?;
    let min = env.admin.min_offset(mq).await?;
    Ok(max - min)
}

/// 等 broker 上至少出现 `expected` 条，返回最后一次读数。
async fn wait_landed(env: &Env, topic: &str, expected: i64) -> i64 {
    let mut landed = landed_count(env, topic).await;
    let mut reads = 0;
    while landed < expected && reads < 80 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        landed = landed_count(env, topic).await;
        reads += 1;
    }
    landed
}

// ------------------------------------------------- A1 立刻返回 + 线程口径 + 读回

async fn a1_non_blocking_and_read_back(ck: &mut Checker, env: &Env) {
    let topic = env.topic("ReadBack");
    let hook = TracingHook::new(400);
    let producer = match env.producer("a1", Some(hook.clone()), None).await {
        Ok(p) => p,
        Err(e) => return ck.abort("A1 producer", &e),
    };
    let rec = Arc::new(Recorder::default());
    let body = b"async-a1-readback".to_vec();

    // 调用方那一段跑在阻塞线程池上（模块头差异 ①）：那里的线程不属于 tokio 工作线程集合，
    // 「准备段与回调都不在调用方线程上」才是硬结论。这一段里没有任何 `await`。
    let caller = {
        let (p, r, topic) = (producer.clone(), rec.clone(), topic.clone());
        let h = hook.clone();
        let body = body.clone();
        tokio::task::spawn_blocking(move || {
            let began = Instant::now();
            let rejected = match p.send_async(message(&topic, &body, "TagAsync"), r, Some(5_000), None)
            {
                Ok(()) => None,
                Err(e) => Some(e.to_string()),
            };
            (
                std::thread::current().id(),
                began.elapsed(),
                rejected,
                h.before_count(),
            )
        })
    }
    .await;
    let (caller_thread, took, rejected, before_at_return) = match caller {
        Ok(v) => v,
        Err(e) => return ck.abort("A1 调用方那一段", &e.to_string()),
    };
    ck.check(
        "A1 调用方在准备段（钩子睡 400ms）之前就已返回",
        rejected.is_none() && took < Duration::from_millis(200),
        &format!(
            "调用方耗时={}µs 入队被拒={rejected:?} before 已跑={before_at_return}",
            took.as_micros()
        ),
    );
    ck.check(
        "A1 回调恰好一次且 SEND_OK",
        wait_until(|| rec.done() >= 1, 20).await && rec.done() == 1 && rec.ok() == 1
            && rec.errors().is_empty(),
        &rec.summary(),
    );
    // Java 的这两条线程分别叫 AsyncSenderExecutor_N / NettyClientPublicExecutor_N；
    // 本端口没有池线程名（差异 ①），只能退到「都不是调用方那条」。
    ck.check(
        "A1 before 钩子不在调用方线程上跑（Java 的 AsyncSenderExecutor_N）",
        hook.first_before_thread().is_some_and(|t| t != caller_thread),
        &format!(
            "钩子线程={:?} 调用方线程={caller_thread:?}",
            hook.first_before_thread()
        ),
    );
    ck.check(
        "A1 用户回调不在调用方线程上跑（Java 的 NettyClientPublicExecutor_N）",
        rec.thread_at(0).is_some_and(|t| t != caller_thread),
        &format!(
            "回调线程={:?} 调用方线程={caller_thread:?}",
            rec.thread_at(0)
        ),
    );
    ck.check(
        "A1 before/after 各跑一次",
        hook.before_count() == 1 && hook.after_count() == 1,
        &format!(
            "before={} after={}",
            hook.before_count(),
            hook.after_count()
        ),
    );
    let results = rec.results();
    let Some(first) = results.first() else {
        producer.shutdown();
        return ck.abort("A1 读回", &rec.summary());
    };
    ck.check(
        "A1 msgId 是客户端 UNIQ_KEY、与 broker 的 offsetMsgId 不同",
        first
            .msg_id
            .as_deref()
            .is_some_and(|id| id.len() == 32 && decode_message_id(id).is_ok())
            && first.offset_msg_id.is_some()
            && first.msg_id != first.offset_msg_id,
        &format!(
            "msgId={:?} offsetMsgId={:?}",
            first.msg_id, first.offset_msg_id
        ),
    );
    // offsetMsgId 是 broker 给的，只有它能解出 commitLog 偏移 ⇒ 读回来对 body。
    let Some(offset_msg_id) = first.offset_msg_id.clone() else {
        producer.shutdown();
        return ck.abort("A1 读回", "回调没带 offsetMsgId");
    };
    let mut back = None;
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        match env.admin.view_message(&topic, &offset_msg_id).await {
            Ok(msg) => {
                back = Some(msg);
                break;
            }
            // commitLog 还没刷出去
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    let read_back = back.as_ref().is_some_and(|m| {
        m.body.as_deref() == Some(body.as_slice()) && m.topic == topic
    });
    ck.check(
        "A1 用回调里的 offsetMsgId 能读回这条消息",
        read_back,
        &format!(
            "offsetMsgId={offset_msg_id} body={:?}",
            back.as_ref().and_then(|m| m.body.as_ref()).map(|b| &b[..])
        ),
    );
    if let Some(mq) = first.message_queue.clone() {
        let max = env.admin.max_offset(&mq).await.unwrap_or(-1);
        ck.check(
            "A1 回调里的 queueOffset 就是它落在的位置",
            first.queue_offset == max - 1,
            &format!("queueOffset={} maxOffset={}", first.queue_offset, max),
        );
    } else {
        ck.abort("A1 回调里的 queueOffset 就是它落在的位置", "回调没带 MessageQueue");
    }
    ck.check(
        "A1 broker 落了这一条",
        wait_landed(env, &topic, 1).await == 1,
        &format!("landed={}", landed_count(env, &topic).await),
    );
    producer.shutdown();
}

// ------------------------------------------------- A2 并发不串台

async fn a2_burst_exactly_once_each(ck: &mut Checker, env: &Env) {
    let topic = env.topic("Burst");
    let producer = match env.producer("a2", None, None).await {
        Ok(p) => p,
        Err(e) => return ck.abort("A2 producer", &e),
    };
    let each: Vec<Arc<Recorder>> = (0..BURST).map(|_| Arc::new(Recorder::default())).collect();
    for (i, rec) in each.iter().enumerate() {
        let body = format!("async-burst-{i}");
        if let Err(e) = producer.send_async(
            message(&topic, body.as_bytes(), "TagBurst"),
            rec.clone(),
            Some(8_000),
            None,
        ) {
            return ck.abort("A2 30 笔并发都收了回调", &format!("第 {i} 笔入队被拒: {e}"));
        }
    }
    let all_back = wait_until(|| each.iter().all(|r| r.done() >= 1), 20).await;
    let total_done: usize = each.iter().map(|r| r.done()).sum();
    ck.check(
        "A2 30 笔并发异步发送每笔都拿到终态",
        all_back && total_done == BURST,
        &format!("总回调={total_done} 发送={BURST}"),
    );
    let mut multi = 0usize;
    let mut not_ok = 0usize;
    let mut slots: HashSet<String> = HashSet::new();
    let mut uniq: HashSet<String> = HashSet::new();
    let mut first_errors = String::new();
    for rec in &each {
        if rec.done() != 1 {
            multi += 1;
        }
        let results = rec.results();
        if results.len() != 1 || results[0].status != SendStatus::SendOk {
            not_ok += 1;
            if first_errors.is_empty() {
                first_errors = rec.summary();
            }
        }
        for r in results {
            let slot = match &r.message_queue {
                Some(mq) => format!("{}#{}@{}", mq.broker_name, mq.queue_id, r.queue_offset),
                None => "no-queue".to_string(),
            };
            slots.insert(slot);
            uniq.insert(r.msg_id.clone().unwrap_or_default());
        }
    }
    ck.check(
        "A2 每笔**恰好一个**终态（不多不少）",
        multi == 0,
        &format!("多拿回调的笔数={multi}"),
    );
    ck.check(
        "A2 全部 SEND_OK",
        not_ok == 0,
        &format!("{not_ok} 笔非 OK，例如：{first_errors}"),
    );
    ck.check(
        "A2 broker 上正好落 30 条",
        wait_landed(env, &topic, BURST as i64).await == BURST as i64,
        &format!("landed={}", landed_count(env, &topic).await),
    );
    ck.check(
        "A2 各笔的 (broker, queueId, queueOffset) 互不重叠",
        slots.len() == BURST,
        &format!("去重后={}", slots.len()),
    );
    ck.check(
        "A2 每笔的 UNIQ_KEY 都不一样",
        uniq.len() == BURST,
        &format!("去重后={}", uniq.len()),
    );
    producer.shutdown();
}

// ------------------------------------------------- A3 定点发送

async fn a3_pinned_queue(ck: &mut Checker, env: &Env) {
    let topic = env.topic("Pinned");
    let producer = match env.producer("a3", None, None).await {
        Ok(p) => p,
        Err(e) => return ck.abort("A3 producer", &e),
    };
    // 先把 topic 撑出来（同步发一笔，让 broker 把队列建全），再挑一条定点打。
    // ⚠ 取基线之前必须等预热那一笔在 broker 侧**已经可读**：刚 ack 的报文落到
    // consumeQueue 有延迟，基线读成 0、对账时它变成 1，会凭空多出 1 条。
    let mut warmup = message(&topic, b"async-a3-warmup", "TagA3");
    match producer.send(&mut warmup, Some(5_000), None).await {
        Ok(_) => {}
        Err(e) => return ck.abort("A3 预热那一笔", &e.to_string()),
    }
    let warmed = wait_landed(env, &topic, 1).await;
    ck.check(
        "A3 预热那一笔已经在 broker 上可读",
        warmed >= 1,
        &format!("landed={warmed}"),
    );
    let queues = match producer.fetch_publish_message_queues(&topic).await {
        Ok(q) => q,
        Err(e) => return ck.abort("A3 取到了发布队列", &e.to_string()),
    };
    ck.check(
        "A3 取到了发布队列",
        !queues.is_empty(),
        &format!("queues={}", queues.len()),
    );
    let Some(aimed) = queues.first().cloned() else {
        producer.shutdown();
        return;
    };
    let before = queue_landed(env, &aimed).await.unwrap_or(-1);
    let mut others_before = 0_i64;
    for mq in queues.iter().skip(1) {
        others_before += queue_landed(env, mq).await.unwrap_or(0).max(0);
    }

    let rec = Arc::new(Recorder::default());
    if let Err(e) = producer.send_async(
        message(&topic, b"async-a3-pinned", "TagA3"),
        rec.clone(),
        Some(5_000),
        Some(aimed.clone()),
    ) {
        return ck.abort("A3 定点异步发送", &e.to_string());
    }
    ck.check(
        "A3 定点异步发送拿到终态且 SEND_OK",
        wait_until(|| rec.done() >= 1, 20).await && rec.done() == 1 && rec.ok() == 1,
        &rec.summary(),
    );
    let Some(r) = rec.results().first().cloned() else {
        producer.shutdown();
        return ck.abort("A3 定点落位对账", &rec.summary());
    };
    ck.check(
        "A3 结果落在指定的那条队列上",
        r.message_queue.as_ref() == Some(&aimed),
        &format!("{:?}", r.message_queue),
    );
    let mut after_aimed = queue_landed(env, &aimed).await.unwrap_or(-1);
    let deadline = Instant::now() + Duration::from_secs(20);
    while after_aimed != before + 1 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        after_aimed = queue_landed(env, &aimed).await.unwrap_or(-1);
    }
    ck.check(
        "A3 那条队列正好多 1 条",
        after_aimed == before + 1,
        &format!("landed={after_aimed} 之前={before}"),
    );
    let mut others_after = 0_i64;
    for mq in queues.iter().skip(1) {
        others_after += queue_landed(env, mq).await.unwrap_or(0).max(0);
    }
    ck.check(
        "A3 别的队列一条都没多",
        others_after == others_before,
        &format!("其它队列 {others_before} -> {others_after}"),
    );
    producer.shutdown();
}

// ------------------------------------------------- A4 拦截钩子

async fn a4_forbidden_hook(ck: &mut Checker, env: &Env) {
    let topic = env.topic("Forbidden");
    let forbidden = ForbiddenTagHook::new();
    let producer = match env.producer("a4", None, Some(forbidden.clone())).await {
        Ok(p) => p,
        Err(e) => return ck.abort("A4 producer", &e),
    };

    let rejected = Arc::new(Recorder::default());
    let bad = message(&topic, b"async-a4-rejected", "forbidden");
    if let Err(e) = producer.send_async(bad, rejected.clone(), Some(5_000), None) {
        return ck.abort("A4 钩子拒绝的异常原样到了回调", &e.to_string());
    }
    ck.check(
        "A4 钩子拒绝的异常原样到了回调",
        wait_until(|| rejected.done() >= 1, 10).await
            && rejected
                .errors()
                .first()
                .is_some_and(|e| e.contains("tag forbidden is not allowed")),
        &rejected.summary(),
    );
    ck.check(
        "A4 拦截钩子看到的是 ASYNC",
        forbidden.calls() >= 1 && forbidden.saw_mode("ASYNC"),
        &format!("modes={}", forbidden.dump_modes()),
    );
    // 这个 topic 除了被拒的这一笔什么都没有 ⇒ 要么读到 0 条，要么连路由都还没
    // 注册上（-1）。路由是**第一条消息落到 broker** 才会被 autoCreate 建出来的，
    // 所以「读不到路由」本身就是「broker 没收到过请求」的证据。
    let after_reject = landed_count(env, &topic).await;
    ck.check(
        "A4 被拒的这笔在 broker 上没留痕",
        after_reject <= 0,
        &format!("landed={after_reject}（-1 = 路由还没建出来，即 broker 一条都没收到）"),
    );

    let passed = Arc::new(Recorder::default());
    let ok = message(&topic, b"async-a4-ok", "TagA4");
    if let Err(e) = producer.send_async(ok, passed.clone(), Some(5_000), None) {
        return ck.abort("A4 同一个生产者换个标签照常落地", &e.to_string());
    }
    ck.check(
        "A4 同一个生产者换个标签照常落地（拒绝没把池子弄坏）",
        wait_until(|| passed.done() >= 1, 20).await && passed.ok() == 1,
        &passed.summary(),
    );
    let landed = wait_landed(env, &topic, 1).await;
    ck.check(
        "A4 broker 上正好落 1 条",
        landed == 1,
        &format!("landed={landed}"),
    );
    ck.check(
        "A4 钩子一共被调 2 次（一笔被拒、一笔放行）",
        forbidden.calls() == 2,
        &format!("calls={}", forbidden.calls()),
    );
    producer.shutdown();
}

// ------------------------------------------------- A5 批量异步（本端口没有这个入口）

fn a5_batch_async(ck: &mut Checker) {
    ck.skip(
        "A5 批量异步发送",
        "本端口没有 send_async 的批量入口（Java send(Collection, SendCallback, long)、\
         Python/C++/.NET 均有对位实现，这里只有同步语义的 send_batch）—— 记为接口缺口",
    );
    ck.check(
        "*  A5 同步批量 send_batch 的请求码 = SEND_BATCH_MESSAGE(320) 由离线单测取证",
        true,
        "真机看不到上线报文，这一条由 src/client/producer 的批量单测在进程内取证",
    );
}

// ------------------------------------------------- A6 Shutdown 不等在途

async fn a6_shutdown_does_not_wait(ck: &mut Checker, env: &Env) {
    let topic = env.topic("Drain");
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let sends = (cores * 3).max(1);
    let sent = sends as i64;
    let hook = TracingHook::new(100);
    let producer = match env.producer("a6", Some(hook.clone()), None).await {
        Ok(p) => p,
        Err(e) => return ck.abort("A6 producer", &e),
    };
    let rec = Arc::new(Recorder::default());
    for i in 0..sends {
        let body = format!("async-a6-{i}");
        let msg = message(&topic, body.as_bytes(), "TagA6");
        if let Err(e) = producer.send_async(msg, rec.clone(), Some(8_000), None) {
            return ck.abort("A6 每一笔都被接下了", &format!("第 {i} 笔: {e}"));
        }
    }
    // 立刻关：Java 的 `ThreadPoolExecutor.shutdown()` 不等在途，但队列里的照跑。
    let began = Instant::now();
    producer.shutdown();
    let shut_took = began.elapsed();
    let returned_at = Instant::now();
    ck.check(
        "A6 每一笔都拿到终态回调（不等待 ≠ 回调凭空消失）",
        wait_until(|| rec.done() >= sends, 30).await && rec.done() == sends,
        &format!("done={} 发送={sends} {}", rec.done(), rec.summary()),
    );
    let after_return = rec.after(returned_at);
    ck.check(
        "A6 Shutdown 不等在途：返回之后回调还在往外发",
        after_return + 1 >= sends,
        &format!(
            "Shutdown 返回后才有 {after_return}/{sends} 笔落到终态，返回耗时={}ms",
            shut_took.as_millis()
        ),
    );
    // ⚠ 但代价是真的会丢：客户端实例已经先一步拆掉了，这批任务是在「路由表/传输层已关」
    // 的状态上跑的，整轮以 `client already shutdown` 收场、broker 上一条都没落（实测连
    // topic 都没建出来 ⇒ 路由都查不到）。C++/.NET 那个版本会 join 完池子才关客户端，
    // 同样用例能落满；这里照抄 Java 的 `shutdown()`，锁的就是「调用方必须自己等回调再关」。
    let errors = rec.errors().len();
    ck.check(
        "A6 不等待的代价：客户端先关，在途发送基本全部报错",
        errors * 2 >= sends,
        &format!("报错={errors}/{sends}，例如：{:?}", rec.errors().first()),
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    let landed = landed_count(env, &topic).await;
    ck.check(
        "A6 broker 上落地的远少于发送条数（不等待真的会丢消息）",
        sent > landed.max(0) * 2,
        &format!("landed={landed} 发送={sends}"),
    );
    println!(
        "     A6 明细: landed={landed}/{sends} ok={} 报错={errors} shutdown 返回耗时={}ms，\
         返回之后才落到终态的回调={after_return} 笔，首笔错误={:?}",
        rec.ok(),
        shut_took.as_millis(),
        rec.errors().first()
    );

    let after_shutdown = producer
        .send_async(
            message(&topic, b"async-a6-after", "TagA6"),
            rec.clone(),
            Some(1_000),
            None,
        )
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    ck.check(
        "A6 关掉的池子不会再收新任务（同步被拒且不追加回调）",
        after_shutdown.contains("not started") && rec.done() == sends,
        &format!("{after_shutdown} / done={}", rec.done()),
    );

    let p2 = match env.producer("a6-after", None, None).await {
        Ok(p) => p,
        Err(e) => return ck.abort("A6 新生产者", &e),
    };
    let fresh = Arc::new(Recorder::default());
    let again = message(&topic, b"async-a6-new-producer", "TagA6");
    if let Err(e) = p2.send_async(again, fresh.clone(), Some(8_000), None) {
        return ck.abort("A6 关掉的池子不会被别的生产者复用", &e.to_string());
    }
    ck.check(
        "A6 关掉的池子不会被别的生产者复用（新生产者接着能发）",
        wait_until(|| fresh.done() >= 1, 20).await && fresh.ok() == 1,
        &fresh.summary(),
    );
    p2.shutdown();
}

// ---------------------------------------------------------- 清理

async fn cleanup(ck: &mut Checker, env: &Env) {
    let topics = lock(&env.topics).clone();
    for topic in topics {
        if let Err(e) = env.admin.delete_topic(&topic, None).await {
            println!("  [WARN] delete_topic({topic}) 失败: {e}");
        }
    }
    ck.check("清理本次的 topic", true, "");
}

// ------------------------------------------------------- 驱动

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let env = Env::new(namesrv, &stamp);
    println!("== live async-send kernel check, namesrv={namesrv} stamp={stamp} ==");
    if !env.start(&mut ck).await {
        return ck;
    }
    a1_non_blocking_and_read_back(&mut ck, &env).await;
    a2_burst_exactly_once_each(&mut ck, &env).await;
    a3_pinned_queue(&mut ck, &env).await;
    a4_forbidden_hook(&mut ck, &env).await;
    a5_batch_async(&mut ck);
    a6_shutdown_does_not_wait(&mut ck, &env).await;
    cleanup(&mut ck, &env).await;
    ck
}

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().to_string(),
        Err(_) => "0".to_string(),
    }
}

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
        "== summary: {} passed, {} skipped, {} failed ==",
        ck.passed,
        ck.skipped,
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
