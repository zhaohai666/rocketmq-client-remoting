//! 拉取前流控（Java `ProcessQueue` 五个阈值）真机验证。
//!
//! 与 `python/verify_flow_control_live.py`（S0..S5）、`cpp/examples/live_flow_control.cpp`、
//! `dotnet/examples/RocketMQ.Examples/LiveFlowControl.cs` 同题、逐条对应。
//!
//! 离线单测（`src/client/consumer.rs` 的 `flow_control_hits_each_threshold`）锁的是**判据
//! 本身**；这里锁真机上两件离线永远锁不住的事：
//! - **A** 闸门在真实 broker 上**确实会命中**。单位错一位、阈值读错一个字段，离线拿 mock
//!   缓冲照样"能命中"，真机上却永远不命中（或永远命中）。Rust 就曾在
//!   `pull_threshold_size_for_topic` 那道闸门上误用了队列级开关，离线全绿。
//! - **B** 命中之后**一条消息都不许丢**。流控只是"暂停拉取"，不是"丢弃/跳过"：暂停期间
//!   位点不许越过还没消费完的消息，恢复后同一个队列必须继续消费到末尾。实现写成"命中就
//!   丢批 / 退出循环"在十几秒窗口里完全看不出来，只有把全部消息数完才暴露。
//!
//! 场景：
//! - S0 默认闸门 + 快消费：**不该**命中（triggered==0），消息全部到达 —— 防闸门误伤正常流量。
//! - S1 队列级字节闸门：条数闸门放到不可能命中，`size=1MiB` + 400KB **不可压缩**大消息 + 慢消费。
//! - S2 位点跨度闸门：条数/字节都关掉，只剩 `consumeConcurrentlyMaxSpan=2`。
//! - S3 topic 级条数闸门：队列级三条全关掉，只剩 `pullThresholdForTopic=4` —— 必须跨队列累计才可能命中。
//! - S4 命中之后恢复：同一组再来一批大消息，闸门仍会命中且新消息照单全收。
//! - S5 启动期数值闸门（Java `checkConfig` :1099-1209）：区间**边界**配置在真集群上能
//!   启动并收全消息；越界配置本地被拒（文案逐字对 Java）且**没打到 broker** ——
//!   broker 侧查不到那个组，说明校验排在注册之前，没留下僵尸 clientId。
//!
//! ⚠ 大消息必须是**不可压缩**的伪随机字节：生产者对超过压缩阈值的 body 先试压，全同字节
//!   的 payload 会被压到几百字节，broker 落盘的 `storeSize` 跟着变几百字节 —— "size 闸门
//!   永不命中"就成了夹具问题（Python 侧第一次跑正是这么踩到的）。
//! ⚠ S1/S4 的 topic 必须只有 **1 条队列**：8 条 400KB 摊到 4 条队列上每条才 800KB，
//!   永远够不到 1MiB 这道**队列级**闸门。
//! ⚠ 大消息必须**并发投递**（见 `Fixture::produce`）：闸门看的是"进比出快"堆出来的缓冲，
//!   串行 send 每条 ~190ms 与 300ms/批的慢消费几乎同步，缓冲只到 1~2 条，字节闸门会在
//!   真机上"永不命中" —— 那是夹具节奏，不是判据。
//!
//! 用法：
//! ```text
//! cargo run --example live_flow_control -- 127.0.0.1:9876
//! ```

use std::collections::BTreeSet;
use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::consumer::{
    ConsumerConfig, DefaultMQPushConsumer, MAX_POP_INVISIBLE_TIME, MIN_POP_INVISIBLE_TIME,
};
use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;
use tokio::task::JoinSet;

/// 一条"远大于任何真机缓冲"的阈值，等价于把那道闸门关掉。
///
/// 不能写 `INT_MAX`（加法溢出），也不能写 `0`：`0` 曾经就是"关闭"的写法，但
/// Java `DefaultMQPushConsumerImpl.checkConfig`（:1099-1209）把
/// `pullThresholdForQueue` / `consumeConcurrentlyMaxSpan` 的下界定在 **1**，
/// 我们在 `start()` 里照抄了这道闸门（见 S5），于是 0 会在启动时被拒。
/// 65535 是 Java 给这几条字段的上界，语义上仍是"实际不可能命中"。
const GATE_OFF: i32 = 65535;
/// 字节闸门"关闭"的写法同上：`pullThresholdSizeForQueue` 合法域 [1, 1024]（单位 MiB），
/// 0 已非法，用 1024 MiB 表示不拦。
const GATE_OFF_SIZE_MIB: i32 = 1024;
/// 字节闸门配 1 = **1 MiB**（Java pullThresholdSizeForQueue 的单位是 MiB，不是字节）。
const GATE_ONE_MIB: i32 = 1;
const BIG: usize = 400 * 1024;

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

/// 收消息用：记录 body，可选地每批睡 `batch_cost`（制造"已拉未消费"的堆积）。
struct Sink {
    bodies: Mutex<BTreeSet<String>>,
    count: Mutex<usize>,
    /// 见过的最大 `MessageExt.store_size`：字节闸门的输入就是它，为 0 就说明解码没带上
    /// broker 的 TOTALSIZE（那时 size 闸门必然永不命中，与判据实现无关）。
    max_store_size: Mutex<i32>,
    batch_cost: Duration,
}

impl Sink {
    fn new(batch_cost: Duration) -> Arc<Sink> {
        Arc::new(Sink {
            bodies: Mutex::new(BTreeSet::new()),
            count: Mutex::new(0),
            max_store_size: Mutex::new(0),
            batch_cost,
        })
    }

    fn count(&self) -> usize {
        *lock(&self.count)
    }

    fn distinct(&self) -> usize {
        lock(&self.bodies).len()
    }

    fn max_store_size(&self) -> i32 {
        *lock(&self.max_store_size)
    }
}

impl MessageListenerConcurrently for Sink {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        if !self.batch_cost.is_zero() {
            std::thread::sleep(self.batch_cost);
        }
        let mut bodies = lock(&self.bodies);
        let mut count = lock(&self.count);
        {
            let mut mx = lock(&self.max_store_size);
            for m in msgs {
                *mx = (*mx).max(m.store_size);
            }
        }
        for m in msgs {
            bodies.insert(String::from_utf8_lossy(m.get_body()).into_owned());
            *count += 1;
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

/// 400KB **不可压缩**消息体：前缀 + xorshift64* 伪随机尾巴。
/// 不用 `rand`（不给 crate 加依赖），也不需要可复现 —— 只要压不动。
fn big_body(tag: usize) -> Vec<u8> {
    let mut out = format!("FCBIG-{tag:03}").into_bytes();
    let mut state: u64 = 0x9E3779B97F4A7C15 ^ (tag as u64).wrapping_mul(0xD1B54A32D192ED03);
    if state == 0 {
        state = 0x2545F4914F6CDD1D;
    }
    while out.len() < BIG {
        // xorshift64*：每次出一个 8 字节块
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        out.extend_from_slice(&(state.wrapping_mul(0x2545F4914F6CDD1D)).to_le_bytes());
    }
    out.truncate(BIG);
    out
}

struct Fixture {
    namesrv: String,
    stamp: u64,
    producer: DefaultMQProducer,
    admin: MQClientInstance,
    broker_addr: String,
}

impl Fixture {
    fn new(namesrv: &str) -> Result<Fixture, String> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let producer = DefaultMQProducer::new(&format!("rust-live-fc-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            stamp,
            producer,
            admin: MQClientInstance::new(&format!("rust-live-fc-admin-{stamp}"), vec![namesrv.to_string()]),
            broker_addr: String::new(),
        })
    }

    async fn start(&mut self) -> Result<(), String> {
        self.admin
            .start()
            .await
            .map_err(|e| format!("admin instance start failed: {e}"))?;
        self.producer
            .start()
            .await
            .map_err(|e| format!("producer start failed: {e}"))?;
        let route = self
            .admin
            .get_topic_route_data(MixAll::DEFAULT_TOPIC)
            .await
            .ok_or_else(|| format!("no route of {} from namesrv", MixAll::DEFAULT_TOPIC))?;
        self.broker_addr = broker_of(&route)?;
        Ok(())
    }

    fn topic_name(&self, kind: &str) -> String {
        format!("RustLiveFlow{kind}{}", self.stamp)
    }

    fn group_name(&self, kind: &str) -> String {
        format!("rust-live-flow-{kind}-{}", self.stamp)
    }

    /// 显式建 topic（不靠 autoCreateTopicEnable，队列数才确定 —— S1 依赖"就是 1 条队列"）。
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
        // 等 NameServer 路由传播，否则消费者首轮 rebalance 仍查不到
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    /// 起一个已订阅、已设 listener 的**未启动**消费者（闸门由调用方 update_config 调）。
    fn consumer(
        &self,
        group: &str,
        topic: &str,
        sink: Arc<Sink>,
    ) -> Result<DefaultMQPushConsumer, String> {
        let cfg = ConsumerConfig {
            consumer_group: group.to_string(),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("live-{group}-{}", self.stamp),
            ..Default::default()
        };
        let consumer = DefaultMQPushConsumer::with_config(cfg)
            .map_err(|e| format!("build consumer failed: {e}"))?;
        consumer
            .subscribe(topic, "*")
            .map_err(|e| format!("subscribe {topic} failed: {e}"))?;
        consumer.set_message_listener_concurrently(sink);
        Ok(consumer)
    }

    /// 并发地把一批消息一次打进 broker —— 流控夹具能不能成立，关键在这一步的节奏。
    ///
    /// 字节闸门的输入是"已拉未消费"缓冲，而缓冲堆不堆得起来取决于**进比出快**。
    /// 串行 send 每条 400KB 实测要 ~190ms，和 300ms/批的慢 listener 差不多同步，
    /// 缓冲只堆到 1~2 条（≈0.8MiB）就停下来等下一条：闸门"真机上永不命中"其实是
    /// 投递节奏的问题，不是判据错（同一份判据在 Python 里命中 15 次，因为它的 send
    /// 只有几十毫秒）。并发投递后 8 条在 ~100ms 内全部落盘，缓冲必然越过 1MiB，
    /// 判据于是与线程/网络节奏解耦，四语言跑的是同一条断言。
    async fn produce(&self, topic: &str, bodies: Vec<Vec<u8>>) -> Result<(), String> {
        let mut set: JoinSet<Result<(), String>> = JoinSet::new();
        for body in bodies {
            let producer = self.producer.clone();
            let topic = topic.to_string();
            set.spawn(async move {
                let mut msg = Message::new(&topic, Some(body.as_slice()));
                producer
                    .send(&mut msg, Some(20000), None)
                    .await
                    .map_err(|e| format!("send failed: {e}"))?;
                Ok(())
            });
        }
        while let Some(joined) = set.join_next().await {
            joined.map_err(|e| format!("send task failed: {e}"))??;
        }
        Ok(())
    }

    fn shutdown(&self) {
        self.producer.shutdown();
        self.admin.shutdown();
    }
}

fn broker_of(route: &TopicRouteData) -> Result<String, String> {
    let bd = route
        .broker_datas
        .first()
        .ok_or_else(|| "route has no brokerData".to_string())?;
    bd.select_broker_addr()
        .ok_or_else(|| format!("broker {} has no address", bd.broker_name))
}

/// 闸门参数：`count`/`size`/`span`/`topic_count`，None 表示用默认值。
#[derive(Default, Clone, Copy)]
struct Gates {
    count: Option<i32>,
    size: Option<i32>,
    span: Option<i64>,
    topic_count: Option<i32>,
    /// true 时把消费线程池钉成 1 线程。
    ///
    /// 慢 listener 是为了制造"已拉未消费"的堆积，而 Rust 的池默认 **20 线程**
    /// （`consumeThreadMin=20`，对齐 Java）：8 条消息一轮就抽干了，缓冲永远堆不到
    /// 1MiB —— 字节闸门于是"在真机上不命中"，纯属夹具与线程池大小的耦合。Python/C++
    /// 的分发是单线程，同一条夹具天然就能堆积；这里显式钉成 1，让四语言用同一条判据。
    single_consumer: bool,
}

fn apply_gates(c: &DefaultMQPushConsumer, g: Gates) {
    c.update_config(|x| {
        if g.single_consumer {
            x.consume_thread_min = 1;
            x.consume_thread_max = 1;
        }
        if let Some(v) = g.count {
            x.pull_threshold_for_queue = v;
        }
        if let Some(v) = g.size {
            x.pull_threshold_size_for_queue = v;
        }
        if let Some(v) = g.span {
            x.consume_concurrently_max_span = v;
        }
        if let Some(v) = g.topic_count {
            x.pull_threshold_for_topic = v;
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn run_gate_case(
    ck: &mut Checker,
    fx: &Fixture,
    label: &str,
    kind: &str,
    queues: i32,
    bodies: Vec<Vec<u8>>,
    slow_ms: u64,
    gates: Gates,
    group: Option<String>,
) -> Option<(Arc<Sink>, DefaultMQPushConsumer)> {
    let topic = fx.topic_name(kind);
    if group.is_none() {
        if let Err(e) = fx.create_topic(&topic, queues).await {
            ck.abort(&format!("{label} create topic"), &e);
            return None;
        }
    }
    let group = group.unwrap_or_else(|| fx.group_name(&format!("{}-lower", label.to_lowercase())));
    let sink = Sink::new(Duration::from_millis(slow_ms));
    let consumer = match fx.consumer(&group, &topic, sink.clone()) {
        Ok(c) => c,
        Err(e) => {
            ck.abort(&format!("{label} build consumer"), &e);
            return None;
        }
    };
    apply_gates(&consumer, gates);
    if let Err(e) = consumer.start().await {
        ck.abort(&format!("{label} start"), &format!("{e}"));
        return None;
    }
    // 等分配稳定：消费者要先被 broker 登记（30s 心跳，启动期缩短），rebalance 才有分配
    tokio::time::sleep(Duration::from_secs(3)).await;
    if let Err(e) = fx.produce(&topic, bodies).await {
        ck.abort(&format!("{label} produce"), &e);
        return None;
    }
    Some((sink, consumer))
}

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    println!("== 流控真机验证：{namesrv} ==");
    let mut fx = match Fixture::new(namesrv) {
        Ok(v) => v,
        Err(e) => {
            ck.abort("fixture build", &e);
            return ck;
        }
    };
    if let Err(e) = fx.start().await {
        ck.abort("fixture start", &e);
        return ck;
    }

    let small: Vec<Vec<u8>> = (0..12).map(|i| format!("ok-{i:03}").into_bytes()).collect();

    // ---------------- S0 默认闸门 + 快消费：不该命中 ----------------
    if let Some((sink, c)) = run_gate_case(
        &mut ck,
        &fx,
        "S0",
        "Defaults",
        4,
        small.clone(),
        0,
        Gates::default(),
        None,
    )
    .await
    {
        let all = poll_until(|| sink.count() >= 12, 40).await;
        let fc = c.flow_control_triggered();
        c.shutdown();
        // 闸门误伤正常流量是最难查的事故（生产上表现为吞吐莫名腰斩），锁 triggered==0
        ck.check(
            "S0 default gates do not fire on ordinary traffic",
            fc == 0,
            &format!("triggered={fc}"),
        );
        ck.check(
            "S0 everything still arrives under the default gates",
            all && sink.count() == 12 && sink.distinct() == 12,
            &format!("count={} distinct={}", sink.count(), sink.distinct()),
        );
    }

    // ---------------- S1 队列级字节闸门（必须单队列）----------------
    let big8: Vec<Vec<u8>> = (0..8).map(big_body).collect();
    let size_gates = Gates {
        count: Some(GATE_OFF),
        size: Some(GATE_ONE_MIB),
        span: Some(GATE_OFF as i64),
        topic_count: None,
        single_consumer: true,
    };
    if let Some((sink, c)) = run_gate_case(
        &mut ck,
        &fx,
        "S1",
        "Size",
        1,
        big8,
        300,
        size_gates,
        None,
    )
    .await
    {
        // 缓冲条数峰值一起打出来：字节闸门的输入就是这份缓冲，峰值 <3 条（3×400KB>1MiB）
        // 说明堆积没形成，命中与否都无从谈起 —— 命中判据失败时先看这个数字，能一眼分清
        // "闸门没接上"和"夹具没堆起来"（后者曾是串行投递造成的）。
        let mut peak = 0usize;
        let hit = poll_until(
            || {
                peak = peak.max(c.buffered_message_count());
                c.flow_control_triggered() > 0
            },
            30,
        )
        .await;
        let all = poll_until(|| sink.count() >= 8, 40).await;
        let fc = c.flow_control_triggered();
        ck.check(
            "S1 a 400KB message reaches the listener with broker storeSize >= 400KB",
            sink.max_store_size() >= 400 * 1024,
            &format!("max_store_size={}", sink.max_store_size()),
        );
        ck.check(
            "S1 the per-queue byte gate fires on a real broker",
            hit,
            &format!("triggered={fc} buffered_peak_count={peak}"),
        );
        ck.check(
            "S1 no 400KB message is lost while the gate is engaged",
            all && sink.count() == 8 && sink.distinct() == 8,
            &format!("count={} distinct={}", sink.count(), sink.distinct()),
        );
        c.shutdown();
    }

    // ---------------- S2 位点跨度闸门 ----------------
    let span_gates = Gates {
        count: Some(GATE_OFF),
        size: Some(GATE_OFF_SIZE_MIB),
        span: Some(2),
        topic_count: None,
        single_consumer: true,
    };
    let small14: Vec<Vec<u8>> = (0..14).map(|i| format!("s-{i:03}").into_bytes()).collect();
    if let Some((sink, c)) = run_gate_case(
        &mut ck,
        &fx,
        "S2",
        "Span",
        4,
        small14,
        300,
        span_gates,
        None,
    )
    .await
    {
        let hit = poll_until(|| c.flow_control_triggered() > 0, 30).await;
        let all = poll_until(|| sink.count() >= 14, 40).await;
        let fc = c.flow_control_triggered();
        c.shutdown();
        ck.check(
            "S2 the offset-span gate fires on a real broker",
            hit,
            &format!("triggered={fc}"),
        );
        ck.check(
            "S2 an over-span buffer is still drained completely",
            all && sink.count() == 14,
            &format!("count={}", sink.count()),
        );
    }

    // ---------------- S3 topic 级条数闸门（跨队列累计）----------------
    let topic_gates = Gates {
        count: Some(GATE_OFF),
        size: Some(GATE_OFF_SIZE_MIB),
        span: Some(GATE_OFF as i64),
        topic_count: Some(4),
        single_consumer: true,
    };
    let small16: Vec<Vec<u8>> = (0..16).map(|i| format!("t-{i:03}").into_bytes()).collect();
    if let Some((sink, c)) = run_gate_case(
        &mut ck,
        &fx,
        "S3",
        "Topic",
        4,
        small16,
        300,
        topic_gates,
        None,
    )
    .await
    {
        let hit = poll_until(|| c.flow_control_triggered() > 0, 30).await;
        let all = poll_until(|| sink.count() >= 16, 40).await;
        let fc = c.flow_control_triggered();
        c.shutdown();
        ck.check(
            "S3 the topic-level gate needs cross-queue accumulation and does fire",
            hit,
            &format!("triggered={fc}"),
        );
        ck.check(
            "S3 every queue of the throttled topic is still consumed to the end",
            all && sink.count() == 16,
            &format!("count={}", sink.count()),
        );
    }

    // ---------------- S4 命中过流控的队列恢复后继续消费 ----------------
    // 复用 S1 的组与 topic（位点已由 S1 提交到 broker 末尾）。这一条锁的是"暂停 100ms"
    // 被写成"退出拉取循环"的错误 —— 那条队列会永久停摆，而 S1 已消费完的消息看不出差别。
    let big6: Vec<Vec<u8>> = (100..106).map(big_body).collect();
    if let Some((sink, c)) = run_gate_case(
        &mut ck,
        &fx,
        "S4",
        "Size",
        1,
        big6,
        300,
        size_gates,
        Some(fx.group_name("s1-lower")),
    )
    .await
    {
        let all = poll_until(|| sink.count() >= 6, 40).await;
        let hit = poll_until(|| c.flow_control_triggered() > 0, 10).await;
        let fc = c.flow_control_triggered();
        c.shutdown();
        ck.check(
            "S4 the queue that hit the gate keeps consuming after it restarts",
            all && sink.count() == 6 && sink.distinct() == 6,
            &format!("count={} distinct={}", sink.count(), sink.distinct()),
        );
        ck.check(
            "S4 the gate is still armed for a second round (not one-shot)",
            hit,
            &format!("triggered={fc}"),
        );
    }

    // ---------------- S5 启动期数值闸门（Java checkConfig :1099-1209）----------------
    // 离线单测（`src/client/consumer.rs` 的 `each_range_gate_*`）锁的是区间与文案；这里
    // 补两件只有真集群能锁死的事：
    //   1. 落在 Java 区间**边界**上的配置真能把消费者跑起来并收全消息 —— 闸门写歪最常
    //      见的方式是"比 Java 还严"，把合法配置也拒了，用户直接起不来；
    //   2. 越界的配置**没有打到 broker**。写成"先注册再校验"会在 broker 的 ConsumerManager
    //      里留下一堆永不心跳的僵尸 clientId，把 rebalance 用的 cidAll 撑歪（表现为队列
    //      分配不均），而客户端日志里只有启动失败那一条。
    let s5_topic = fx.topic_name("Boundary");
    let s5_group = fx.group_name("s5");
    let bad_group = fx.group_name("s5-rejected");
    if let Err(e) = fx.create_topic(&s5_topic, 4).await {
        ck.abort("S5 create topic", &e);
    } else {
        let sink5 = Sink::new(Duration::ZERO);
        match fx.consumer(&s5_group, &s5_topic, sink5.clone()) {
            Err(e) => ck.abort("S5 build consumer", &e),
            Ok(c5) => {
                // 每条闸门取端点：`pullBatchSize=1024` 这类"贴着上限=实际不拦"的写法在
                // 生产里很常见，误拒等于把用户挡在门外（区间写成开区间的后果）。
                c5.update_config(|x| {
                    x.consume_thread_min = 1;
                    x.consume_thread_max = 2;
                    x.consume_concurrently_max_span = i64::from(GATE_OFF);
                    x.pull_threshold_for_queue = GATE_OFF;
                    x.pull_threshold_for_topic = -1;
                    x.pull_threshold_size_for_queue = GATE_OFF_SIZE_MIB;
                    x.pull_threshold_size_for_topic = -1;
                    x.pull_interval = 0;
                    x.consume_message_batch_max_size = 1;
                    x.pull_batch_size = 1024;
                    x.pop_invisible_time = MAX_POP_INVISIBLE_TIME;
                    x.pop_batch_nums = 32;
                });
                if let Err(e) = c5.start().await {
                    ck.abort("S5 boundary config must start", &format!("{e}"));
                } else {
                    ck.check("S5 boundary config starts", true, "is_started=true");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    let small10: Vec<Vec<u8>> =
                        (0..10).map(|i| format!("b-{i:03}").into_bytes()).collect();
                    if let Err(e) = fx.produce(&s5_topic, small10).await {
                        ck.abort("S5 produce", &e);
                    }
                    let all = poll_until(|| sink5.count() >= 10, 40).await;
                    ck.check(
                        "S5 every message arrives under the boundary config",
                        all && sink5.count() == 10 && sink5.distinct() == 10,
                        &format!("count={} distinct={}", sink5.count(), sink5.distinct()),
                    );

                    // 越界配置：本地拒 + 不回滚 started 标志（失败的 start 必须留干净对象）
                    type GateBreaker = fn(&mut ConsumerConfig);
                    let illegal: Vec<(GateBreaker, &str)> = vec![
                        (
                            |c| c.pull_threshold_size_for_queue = 0,
                            "MQClientException: pullThresholdSizeForQueue Out of range [1, 1024]",
                        ),
                        (
                            |c| c.pull_batch_size = 1025,
                            "MQClientException: pullBatchSize Out of range [1, 1024]",
                        ),
                        (
                            |c| c.pop_invisible_time = MIN_POP_INVISIBLE_TIME - 1,
                            "MQClientException: popInvisibleTime Out of range [5000, 300000]",
                        ),
                        (
                            |c| c.pop_batch_nums = 33,
                            "MQClientException: popBatchNums Out of range [1, 32]",
                        ),
                        (
                            |c| {
                                c.consume_thread_min = 8;
                                c.consume_thread_max = 4;
                            },
                            "MQClientException: consumeThreadMin (8) is larger than consumeThreadMax (4)",
                        ),
                    ];
                    for (break_it, want) in illegal {
                        let c6 = match fx.consumer(&bad_group, &s5_topic, Sink::new(Duration::ZERO))
                        {
                            Ok(v) => v,
                            Err(e) => {
                                ck.abort("S5 build rejected consumer", &e);
                                continue;
                            }
                        };
                        c6.update_config(break_it);
                        match c6.start().await {
                            Err(e) => ck.check(
                                "S5 out-of-range config is rejected",
                                e.to_string() == want,
                                &format!("want={want} actual={e}"),
                            ),
                            Ok(()) => {
                                c6.shutdown();
                                ck.abort("S5 out-of-range config is rejected", "start() succeeded");
                            }
                        }
                        ck.check(
                            "S5 a rejected start leaves nothing half-started",
                            !c6.is_started(),
                            "is_started() must be false after a rejected start",
                        );
                    }

                    // broker 侧的反证
                    match fx.admin.get_topic_route_data(&s5_topic).await {
                        None => ck.abort("S5 route of the boundary topic", "no route from namesrv"),
                        Some(route) => match broker_of(&route) {
                            Err(e) => ck.abort("S5 broker address", &e),
                            Ok(addr) => {
                                // 从未成功注册的组：broker 的 GET_CONSUMER_LIST_BY_GROUP 回的
                                // 不是空列表而是 "no consumer for this group"（code=1）。两种
                                // 形态都算"查无此组"，但**绝不能**带任何 clientId。
                                let bad = fx
                                    .admin
                                    .get_consumer_list_by_group(&bad_group, 5000, Some(&addr))
                                    .await;
                                let (absent, detail) = match bad {
                                    Ok(body) => (
                                        body.consumer_id_list.is_empty(),
                                        format!("ids={:?}", body.consumer_id_list),
                                    ),
                                    // 只有 broker 亲口回的错才算"查无此组"。连不上/超时必须
                                    // 判失败，否则反证退化成"什么都没查到"的空话。
                                    Err(e @ Error::Broker { .. }) => {
                                        (true, format!("broker 拒绝: {e}"))
                                    }
                                    Err(e) => (false, format!("探测请求失败: {e}")),
                                };
                                ck.check("S5 broker never heard of the rejected group", absent, &detail);
                                let ok_ids = fx
                                    .admin
                                    .get_consumer_list_by_group(&s5_group, 5000, Some(&addr))
                                    .await
                                    .map(|b| b.consumer_id_list)
                                    .unwrap_or_default();
                                ck.check(
                                    "S5 broker knows the boundary-value consumer",
                                    ok_ids.len() == 1,
                                    &format!("ids={ok_ids:?}"),
                                );
                            }
                        },
                    }
                }
                // 反证必须发生在 shutdown 之前：早退会撤掉 broker 侧那条注册
                c5.shutdown();
            }
        }
    }

    fx.shutdown();
    ck
}

fn report(ck: &mut Checker) {
    println!(
        "\n== 结果：{} PASS / {} FAIL ==",
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
