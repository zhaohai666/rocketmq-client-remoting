//! 220 `RESET_CONSUMER_CLIENT_OFFSET`（Java `MQClientInstance.resetOffset:1403-1450`）真机验证。
//!
//! 与 `python/verify_reset_offset_live.py` 同题逐条对应，另三端（cpp/csharp）各有同名脚本。
//!
//! 220 是 broker 推给**消费端**的重置指令；管理端那笔 `INVOKE_BROKER_TO_RESET_OFFSET(222)`
//! 的响应只是一张「每个队列重置到哪」的表，真正让消费端改位点的是 broker 随后 oneway 推的
//! 220（`Broker2Client.resetOffset:181-238`）。broker 只在 `useServerSideResetOffset=false`
//! 时才走这条推送路径（默认 true 在 `AdminBrokerProcessor:2255-2263` 直接服务端改位点、
//! 一台消费端都不通知），所以本验证先把该开关热改成 false（回读确认），跑完还原。
//!
//! 离线单测（`client::consumer` 的 reset_offset_* 与 `client::mq_client` 的 220 处理器用例）
//! 锁死了请求体两种形状与「撤队列 + 代号 +1 + 新位点经撤销尾巴落盘」的本地状态；下面这些
//! 事只有真集群能证明：
//!
//! S1「回退重置立刻生效 + 在途批次作废」——位点从 10 往回重置到 3，三段判据。a) broker 上的
//! 已提交位点在 ~2s 内变成 3：窗口内**只有**重置路径那次 persist 会写它（周期落盘已拉长到
//! 60s），只写内存表的实现在这里原地不动（broker 停在 10）。b) listener 里卡着的旧批次
//! （重置前取回的 offset 10）放行后其 ack 必须整批作废 —— 采样点：放行旧批次、新队列的第一批
//! 已进 listener 且还没 ack 时，本地已消费位点必须还是「没有记录 / ≤3」；没有代号闸门的实现
//! 这时会跳到 11。c) 队列被真正重建：3..14 每一批都**重投一次**（旧缓冲里没 ack 的 11..14
//! 也随之作废，只能作为重投的一部分出现）。
//!
//! S2「前跳 + 恢复」——timestamp=-1 重置到 maxOffset：位点直接跳到 10，中间 4..9 一条都不投；
//! 随后新消息照常消费、位点继续前进。
//!
//! 用法：
//! ```text
//! cargo run --example live_reset_offset -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{
    mq_key, ConsumerConfig, DefaultMQPushConsumer,
};
use rocketmq_client_remoting::client::mq_client::RegisteredConsumer;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::remoting::protocol::ext_fields::StringMap;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

const MSGS: i64 = 10;
const BACK_TARGET: i64 = 3;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `poll_until` 的 async 版本：谓词本身要发 RPC（`committed`）时用这个。
async fn poll_until_async<F, Fut>(mut pred: F, secs: u64) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if pred().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_before_async<F, Fut>(mut pred: F, seconds: f64) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs_f64(seconds);
    loop {
        if pred().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 逐批闸住的 listener：每批停在闸门上，由测试逐批 `release()` —— 批与批之间的本地状态
/// 因此可以被采样。S1 的关键采样点（旧批次已放行、新队列第一批还没 ack）只有在
/// 「逐批可控」时才存在；一次性放行的 listener 会把 11（旧批次 ack）与随后的重投混在
/// 一个瞬间里。
struct SteppingListener {
    state: Mutex<StepState>,
    cv: Condvar,
}

struct StepState {
    batches: Vec<Vec<i64>>,
    released: bool,
}

impl SteppingListener {
    fn new() -> Arc<SteppingListener> {
        Arc::new(SteppingListener {
            state: Mutex::new(StepState {
                batches: Vec::new(),
                released: false,
            }),
            cv: Condvar::new(),
        })
    }

    fn batches(&self) -> Vec<Vec<i64>> {
        lock(&self.state).batches.clone()
    }

    fn offsets(&self) -> Vec<i64> {
        lock(&self.state)
            .batches
            .iter()
            .flatten()
            .copied()
            .collect()
    }

    fn release(&self) {
        lock(&self.state).released = true;
        self.cv.notify_all();
    }

    async fn wait_batch(&self, n: usize, secs: u64) -> bool {
        poll_until(|| self.batches().len() >= n, secs).await
    }
}

impl MessageListenerConcurrently for SteppingListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let mut st = lock(&self.state);
        st.batches.push(msgs.iter().map(|m| m.queue_offset).collect());
        // 上限只防死锁：正常路径由测试逐批 release()；醒来的那个把闸门复位（语义同
        // Python `Event.set()` + `clear()`）。
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
        st.released = false;
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

/// 只记到达的 queueOffset：这一趟关心的是「哪些不该来」，不是消息内容。
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
            instance_name: format!("rust-live-ro-admin-{stamp}"),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        admin
            .start()
            .await
            .map_err(|e| format!("admin start failed: {e}"))?;
        let producer = DefaultMQProducer::new(&format!("rust-live-ro-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        producer
            .start()
            .await
            .map_err(|e| format!("producer start failed: {e}"))?;

        // 端口开着 != broker 已注册到 nameServer：按 Python 那样轮询
        let deadline = Instant::now() + Duration::from_secs(40);
        let broker_addr = loop {
            if let Ok(info) = admin.fetch_broker_cluster_info().await {
                if !info.broker_addr_table.is_empty() {
                    break info
                        .get_broker_addrs()
                        .first()
                        .cloned()
                        .ok_or_else(|| "cluster info has no broker address".to_string())?;
                }
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
        format!("RustLiveRo{tag}{}", self.stamp)
    }

    async fn create_topic(&self, topic: &str) -> Result<(), String> {
        self.admin
            .create_topic(MixAll::DEFAULT_TOPIC, topic, 1, 0)
            .await
            .map_err(|e| format!("create topic {topic} failed: {e}"))?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

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

    async fn queue_key(&self, topic: &str) -> Result<String, String> {
        self.queues(topic)
            .await
            .first()
            .map(mq_key)
            .ok_or_else(|| format!("route of {topic} has no queue"))
    }

    /// [(mq, maxOffset, 已提交位点 or None)]，位点 None 表示 broker 上查无记录。
    async fn committed(&self, group: &str, topic: &str) -> Vec<(MessageQueue, i64, Option<i64>)> {
        let client = match self.admin.get_mq_client_instance() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        let mut out = Vec::new();
        for mq in self.queues(topic).await {
            let off = self.admin.examine_consumer_offset(group, &mq).await.unwrap_or(None);
            let max_off = client.get_max_offset(&mq, 10_000, None).await.unwrap_or(-1);
            out.push((mq, max_off, off));
        }
        out
    }

    fn consumer(
        &self,
        group: &str,
        role: &str,
        topic: &str,
        batch_size: i32,
        persist_millis: Option<u64>,
    ) -> Result<DefaultMQPushConsumer, String> {
        let cfg = ConsumerConfig {
            consumer_group: group.to_string(),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("rust-live-ro-{role}-{}", self.stamp),
            consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
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

    async fn send(&self, topic: &str, body: &str) -> Result<(), String> {
        let mut msg = Message::new(topic, Some(body.as_bytes()));
        self.producer
            .send(&mut msg, Some(20000), None)
            .await
            .map_err(|e| format!("send {body} failed: {e}"))?;
        Ok(())
    }

    /// 开关热改 + 回读（`useServerSideResetOffset` 是 220 推送路径的前提）。
    async fn set_server_side_reset(&self, value: &str) -> bool {
        let mut props = StringMap::new();
        props.insert("useServerSideResetOffset".to_string(), value.to_string());
        if let Err(e) = self
            .admin
            .update_broker_config(&self.broker_addr, &props, None)
            .await
        {
            println!("  (updateBrokerConfig 失败: {e})");
            return false;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        match self.admin.get_broker_config(&self.broker_addr, None).await {
            Ok(cfg) => cfg.get("useServerSideResetOffset") == Some(value),
            Err(e) => {
                println!("  (getBrokerConfig 失败: {e})");
                false
            }
        }
    }

    fn shutdown(&self) {
        self.producer.shutdown();
        self.admin.shutdown();
    }
}

fn fmt(rows: &[(MessageQueue, i64, Option<i64>)]) -> String {
    rows.iter()
        .map(|(mq, max, off)| {
            format!(
                "q{}:{}/max{}",
                mq.queue_id,
                off.map(|o| o.to_string()).unwrap_or_else(|| "None".to_string()),
                max
            )
        })
        .collect::<Vec<String>>()
        .join(" ")
}

fn all_committed(rows: &[(MessageQueue, i64, Option<i64>)], want: i64) -> bool {
    !rows.is_empty() && rows.iter().all(|(_, _, o)| *o == Some(want))
}

/// 本端口内存里的「已消费位点」；`None` = 该队列在表里没有记录。
///
/// 220 的重置语义（Java `removeOffset`）就是「重置后表里没有旧位点」，所以 None 与 0 必须
/// 能分开：只有 None 才能证明旧批次的 ack 没有把位点推回去。
fn local_offset(c: &DefaultMQPushConsumer, topic: &str) -> Option<i64> {
    c.get_consumer_status(Some(topic))
        .into_iter()
        .find(|(mq, _)| mq.queue_id == 0)
        .map(|(_, off)| off)
}

// ---------------------------------------------------------------- S1

async fn s1_backward_reset(fx: &Fixture, ck: &mut Checker) {
    let topic = fx.topic("Back");
    let group = format!("GID_rust_live_ro_back_{}", fx.stamp);
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

    // 前置：先用普通 listener 把 broker 上的位点做成 10（reset 要求该组在 broker 上有记录）
    let l0 = RecordingListener::new();
    let c0 = match fx.consumer(&group, "back0", &topic, 1, None) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S1-consumer0", false, &e);
            return;
        }
    };
    c0.set_message_listener_concurrently(l0.clone());
    if let Err(e) = c0.start().await {
        ck.check("S1-start0", false, &format!("{e}"));
        c0.shutdown();
        return;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut t_mid = 0i64;
    for i in 0..MSGS {
        if let Err(e) = fx.send(&topic, &format!("rb-{i}")).await {
            ck.check("S1-send", false, &e);
            c0.shutdown();
            return;
        }
        if i == 2 {
            t_mid = now_millis();
            // 让第 3 条（offset 3）与前面三条拉开存储时间，后面按时间戳重置才能稳定命中 3
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    let consumed = poll_until(|| l0.arrivals().len() == MSGS as usize, 30).await;
    ck.check(
        &format!("S1-前置：消费者消费掉 {MSGS} 条"),
        consumed,
        &format!("arrivals={:?}", l0.arrivals()),
    );

    let persisted = poll_until_async(
        || async {
            let rows = fx.committed(&group, &topic).await;
            all_committed(&rows, MSGS)
        },
        20,
    )
    .await;
    if !persisted {
        println!(
            "  [WARN] committed={}",
            fmt(&fx.committed(&group, &topic).await)
        );
    }
    ck.check(
        &format!("S1-前置：broker 位点周期落盘到 {MSGS}（reset 要求组在 broker 上有记录）"),
        persisted,
        "",
    );
    c0.shutdown();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 主力消费者：逐批闸住 + 周期落盘 60s ⇒ 窗口内唯一能改 broker 位点的路径是重置自带的那次
    let sink = SteppingListener::new();
    let c = match fx.consumer(&group, "back", &topic, 1, Some(60_000)) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S1-consumer", false, &e);
            return;
        }
    };
    c.set_message_listener_concurrently(sink.clone());
    let t_start = Instant::now();
    if let Err(e) = c.start().await {
        ck.check("S1-start", false, &format!("{e}"));
        c.shutdown();
        return;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    for i in MSGS..MSGS + 5 {
        if let Err(e) = fx.send(&topic, &format!("rb-{i}")).await {
            ck.check("S1-send2", false, &e);
            sink.release();
            c.shutdown();
            return;
        }
    }

    let ready = poll_until(
        || !sink.batches().is_empty() && c.cached_message_count(&key) == 4,
        30,
    )
    .await;
    let arr0 = sink.offsets();
    ck.check(
        "S1-窗口就绪：1 条在途（offset 10 卡在 listener）+ 4 条留在缓冲",
        ready && arr0.first() == Some(&10),
        &format!("arrivals={arr0:?} pending={}", c.cached_message_count(&key)),
    );
    if !ready {
        sink.release();
        c.shutdown();
        return;
    }

    // 等周期落盘的**首跳**过去（Java initialDelay = start 后 10s，之后才是 60s 周期）。
    // 不等它，重置后那次 persist 会与首跳混在一起，「broker 位点之所以是 3」就说不清。
    let quiet = poll_until(|| t_start.elapsed() > Duration::from_secs(12), 15).await;
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        "S1-首跳周期落盘已过（此后 60s 内不再有周期写）",
        quiet && all_committed(&rows, MSGS),
        &fmt(&rows),
    );

    // 按时间戳重置到 3：t_mid 落在第 3 条与第 4 条之间 ⇒ getOffsetInQueueByTime 命中 3，
    // 3 < consumerOffset(10) 且 isForce=true ⇒ broker 推 {mq: 3}
    let t0 = Instant::now();
    let table = match fx
        .admin
        .reset_offset_by_timestamp(&topic, &group, t_mid + 500, true, None, false)
        .await
    {
        Ok(t) => t,
        Err(e) => {
            ck.check("S1-reset", false, &format!("{e}"));
            sink.release();
            c.shutdown();
            return;
        }
    };
    let targets: Vec<i64> = table.iter().map(|(_, v)| *v).collect();
    ck.check(
        "S1-222 响应里的目标位点就是 3",
        targets == vec![BACK_TARGET],
        &format!("table={targets:?}"),
    );

    let bumped = wait_before_async(
        || async {
            let rows = fx.committed(&group, &topic).await;
            all_committed(&rows, BACK_TARGET)
        },
        2.0,
    )
    .await;
    let elapsed = t0.elapsed();
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        &format!(
            "S1-broker 位点 {:.1}s 内变成 {BACK_TARGET}（重置路径自带的那次 persist，周期落盘=60s）",
            elapsed.as_secs_f64()
        ),
        bumped,
        &fmt(&rows),
    );

    let off = local_offset(&c, &topic);
    ck.check(
        "S1-重置后本地表里没有旧位点（Java removeOffset；新位点经撤销尾巴出去）",
        off.is_none(),
        &format!("local_offset={off:?} epoch={}", c.queue_epoch(&key)),
    );

    // 放行旧批次：它的 ack 属于已被撤销的 ProcessQueue，必须作废
    sink.release();
    let second = sink.wait_batch(2, 20).await;
    let off = local_offset(&c, &topic);
    let two = sink.batches();
    ck.check(
        &format!(
            "S1-旧批次 ack 作废（放行后新队列第一批 offset {} 已在途时，本地位点仍未越过 {BACK_TARGET}）",
            if second && two.len() > 1 { two[1][0] } else { -1 }
        ),
        second && off.is_none_or(|o| o <= BACK_TARGET),
        &format!("local_offset={off:?} arrivals={:?}", sink.offsets()),
    );

    // 逐批放行走完重投：3..14 每条重投一次（旧缓冲里 11..14 也已作废，只能作为重投出现）
    let mut expected = vec![10i64];
    expected.extend(BACK_TARGET..MSGS + 5);
    for n in 2..=expected.len() {
        sink.release();
        if n < expected.len() {
            sink.wait_batch(n + 1, 15).await;
        }
    }
    let arr = sink.offsets();
    ck.check(
        &format!("S1-队列被真正重建：重投序列是 {expected:?}"),
        arr == expected,
        &format!("arrivals={arr:?}"),
    );
    poll_until(|| local_offset(&c, &topic) == Some(MSGS + 5), 10).await;

    let rows = fx.committed(&group, &topic).await;
    ck.check(
        &format!("S1-窗口内只有重置那次写 broker（位点仍停在 {BACK_TARGET}，周期落盘=60s）"),
        all_committed(&rows, BACK_TARGET),
        &fmt(&rows),
    );

    // 关停会落盘一次：重投的 ack 才是最终值（15 = 最后一条 14 的 +1）
    c.shutdown();
    let final_ok = poll_until_async(
        || async {
            let rows = fx.committed(&group, &topic).await;
            all_committed(&rows, MSGS + 5)
        },
        15,
    )
    .await;
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        &format!(
            "S1-关停落盘把重投的 ack 写回 broker（位点前进到 {}）",
            MSGS + 5
        ),
        final_ok,
        &fmt(&rows),
    );
    cleanup(fx, &topic, &group).await;
}

// ---------------------------------------------------------------- S2

async fn s2_forward_skip(fx: &Fixture, ck: &mut Checker) {
    let topic = fx.topic("Skip");
    let group = format!("GID_rust_live_ro_skip_{}", fx.stamp);
    if let Err(e) = fx.create_topic(&topic).await {
        ck.check("S2-create topic", false, &e);
        return;
    }

    let sink = SteppingListener::new();
    // 周期落盘用默认 5s：本场景要先靠首跳（start 后 10s）在 broker 上给这个组建记录
    //（Broker2Client.resetOffset 对 queryOffset==-1 的组直接回 SYSTEM_ERROR），
    // 再等一个 >5s 的窗口做重置 —— 重置那笔 persist 与周期写不同刻，判据仍然干净。
    let c = match fx.consumer(&group, "skip", &topic, 1, None) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S2-consumer", false, &e);
            return;
        }
    };
    c.set_message_listener_concurrently(sink.clone());
    let t_start = Instant::now();
    if let Err(e) = c.start().await {
        ck.check("S2-start", false, &format!("{e}"));
        c.shutdown();
        return;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    for i in 0..MSGS {
        if let Err(e) = fx.send(&topic, &format!("rs-{i}")).await {
            ck.check("S2-send", false, &e);
            sink.release();
            c.shutdown();
            return;
        }
    }

    // 逐批放行前 3 条：第 4 条（offset 3）留在 listener 里当「在途批次」
    let mut arranged = false;
    for n in 1..=4 {
        if !sink.wait_batch(n, 30).await {
            break;
        }
        if n < 4 {
            sink.release();
        }
        arranged = n == 4;
    }
    let arr = sink.offsets();
    ck.check(
        "S2-窗口就绪：前 3 条已 ack、第 4 条卡在 listener",
        arranged && arr == vec![0, 1, 2, 3],
        &format!("arrivals={arr:?}"),
    );
    if !arranged {
        sink.release();
        c.shutdown();
        return;
    }

    // 等首跳落盘把组建出来（本地已消费位点 3 = 前三条的 ack），并留出 >5s 的静默窗口
    let ready = poll_until_async(
        || async {
            if t_start.elapsed() <= Duration::from_secs(12) {
                return false;
            }
            let rows = fx.committed(&group, &topic).await;
            all_committed(&rows, 3)
        },
        20,
    )
    .await;
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        "S2-前置：broker 上该组有记录（q0:3），且下一笔周期写还在 5s 之外",
        ready,
        &fmt(&rows),
    );

    let t0 = Instant::now();
    let table = match fx
        .admin
        .reset_offset_by_timestamp(&topic, &group, -1, true, None, false)
        .await
    {
        Ok(t) => t,
        Err(e) => {
            ck.check("S2-reset", false, &format!("{e}"));
            sink.release();
            c.shutdown();
            return;
        }
    };
    let targets: Vec<i64> = table.iter().map(|(_, v)| *v).collect();
    ck.check(
        &format!("S2-222 响应里的目标位点就是 maxOffset({MSGS})"),
        targets == vec![MSGS],
        &format!("table={targets:?}"),
    );

    let jumped = wait_before_async(
        || async {
            let rows = fx.committed(&group, &topic).await;
            all_committed(&rows, MSGS)
        },
        2.0,
    )
    .await;
    let elapsed = t0.elapsed();
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        &format!(
            "S2-broker 位点 {:.1}s 内前跳到 {MSGS}（重置路径自带的那次 persist）",
            elapsed.as_secs_f64()
        ),
        jumped,
        &fmt(&rows),
    );

    sink.release();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let arr = sink.offsets();
    ck.check(
        &format!("S2-被跳过的 4..{} 一条都不投（在途那条的 ack 也作废）", MSGS - 1),
        arr == vec![0, 1, 2, 3],
        &format!("arrivals={arr:?}"),
    );

    if let Err(e) = fx.send(&topic, "rs-new").await {
        ck.check("S2-send new", false, &e);
        c.shutdown();
        return;
    }
    let new_ok = sink.wait_batch(5, 20).await;
    sink.release();
    let arr = sink.offsets();
    ck.check(
        &format!("S2-重建后的队列从队尾续跑（新消息 offset {MSGS} 正常投递）"),
        new_ok && arr == vec![0, 1, 2, 3, MSGS],
        &format!("arrivals={arr:?}"),
    );

    // release() 只让 listener 返回；ack 是消费线程随后落的。不等本地位点真的推到 11 就
    // 关停，关停那次 persist 可能跑在 ack 之前（写回的还是 10）—— 这是断言竞态，不是语义问题。
    poll_until(|| local_offset(&c, &topic) == Some(MSGS + 1), 10).await;
    c.shutdown();
    let final_ok = poll_until_async(
        || async {
            let rows = fx.committed(&group, &topic).await;
            all_committed(&rows, MSGS + 1)
        },
        15,
    )
    .await;
    let rows = fx.committed(&group, &topic).await;
    ck.check(
        &format!(
            "S2-关停落盘把新消息的 ack 写回 broker（位点前进到 {}）",
            MSGS + 1
        ),
        final_ok,
        &fmt(&rows),
    );
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

    let off = fx.set_server_side_reset("false").await;
    let cfg = fx
        .admin
        .get_broker_config(&fx.broker_addr, None)
        .await
        .map(|c| {
            c.get("useServerSideResetOffset")
                .unwrap_or("<missing>")
                .to_string()
        })
        .unwrap_or_else(|_| "<read failed>".to_string());
    ck.check(
        "开关热改：useServerSideResetOffset=false（220 推送路径的前提）",
        off,
        &format!("回读 useServerSideResetOffset={cfg}"),
    );

    s1_backward_reset(&fx, &mut ck).await;
    s2_forward_skip(&fx, &mut ck).await;

    let restored = fx.set_server_side_reset("true").await;
    ck.check("还原 useServerSideResetOffset=true", restored, "");

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
