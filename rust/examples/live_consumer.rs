//! `DefaultMQPushConsumer` 对**真实 5.5.1 broker** 的联调验证。
//!
//! 离线单测（`src/client/consumer.rs` 的 `mod tests`）只能覆盖纯逻辑：阈值算术、
//! CK 解析、返回类型判定。这里补上必须真收发才能证明的部分：长轮询投递、tag 过滤、
//! 失败重投与 `%RETRY%` 主题还原、POP + ack、广播位点落盘、顺序消费的 broker 锁、
//! 多实例分摊与撤位、位点持久化到 broker、以及运维接口读到的运行时状态。
//!
//! 场景：
//! - C1 构造与 `start()` 校验：空白组名、三道启动校验（名字服务/订阅/listener）、
//!   幂等 start、启动后禁改订阅、`shutdown()` 幂等；并核对首轮**同步**心跳与
//!   首轮重平衡在 `start()` 返回前就已完成。
//! - C2 长轮询全量消费：24 条不重不丢、字段齐全，位点推进并**刷到 broker**
//!   （`QUERY_CONSUMER_OFFSET` 读回来求和 == 24），runningInfo 与之一致。
//! - C3 tag 过滤：broker 侧确实存了 20 条（`GET_MAX_OFFSET` 求和），
//!   客户端只投递 10 条 TagA。
//! - C4 消费失败重投：`RECONSUME_LATER` 后同一条 body 再次到达，`reconsumeTimes`
//!   递增，且 listener 看到的 topic 已被还原成原始 topic（不是 `%RETRY%<group>`）
//!   —— 这条只有真 broker 能验。
//! - C4b 死信终态：`maxReconsumeTimes=2` ⇒ 恰好投递 3 次（0/1/2），第 3 次回投后
//!   broker 把消息改投 `%DLQ%<group>`（自动建 topic 并注册路由），死信里
//!   `reconsumeTimes=3`、`RETRY_TOPIC` 保留业务 topic，原组不再有第 4 次投递。
//! - C4c 部分 ack：listener 在首批（`consumeMessageBatchMaxSize=3`）上设
//!   `context.ack_index = 0` ⇒ 只有第一条被认可，后两条经 `%RETRY%` 二次到达
//!   （`reconsumeTimes>=1`、listener 看到的仍是业务 topic），被 ack 的那条整个窗口
//!   只投一次，3 条最终一条不丢，业务队列位点仍整批前进到 3；对照腿（不碰
//!   `ack_index`，Java 默认 `Integer.MAX_VALUE`）一条都不回投。
//! - C5 POP 模式：弹出即带 `POP_CK`，消费成功后 `waitAckCounter` 归零，
//!   等过一个 invisibleTime 窗口**不再重投**（证明 ack 真的写到了 broker）；
//!   且 POP 路径完全不写消费位点。
//! - C5b POP 循环的拉取统计：持续 26s 有流量后，用**独立 admin 实例**走真实 307
//!   （admin → broker → 目标客户端）读回 `statusTable`，`pullRT`/`pullTPS` 必须非 0
//!   —— 漏记是静默故障：消息照弹照 ack、消费完全正常，只有运维看板一片 0。
//! - C6 广播模式：同组两个实例各拿到全量（不分摊）；位点**不落 broker**，
//!   而是按 `$HOME/.rocketmq_offsets/<clientId>/<group>/offsets.json` 落盘并可解回。
//! - C7 顺序消费：`LOCK_BATCH_MQ` 被 5.5.1 接受（runningInfo 的 `locked`），
//!   同队列内 `queueOffset` 严格递增投递。
//! - C8 多实例：同组两实例队列**不重不漏**；撤掉一个后另一个在 40 通知/定时
//!   重平衡下接管全部队列并继续消费，全量消息一条不丢。
//! - C9 运维接口与背压：流控只暂停拉取不丢消息、核心线程数守卫、积压统计、
//!   手工心跳/重平衡/订阅队列查询、经实例注册表调 `persist_consumer_offset()`。
//! - C11 拉取停摆自愈（Java `ProcessQueue.isPullExpired` / 120s）：1 队列 topic 先消费 3 条，
//!   把该队列的拉取盖章倒拨 125s（等价于"这条循环已经两分钟没动静"），走**生产** rebalance
//!   确认它被撤掉重建（同一趟里换新属主、重新盖章），随后再发 3 条照样消费、
//!   broker 位点从 3 前进到 6 且**不回退**，6 条各只投一次（撤走前持久化了位点）。
//! - C10 清理：删掉本次建的 topic 与广播位点目录。
//! - C12 顺序消费毒消息：listener 一直 SUSPEND + `max_reconsume_times=2` ⇒ 本地恰好投 3 次
//!   （`reconsumeTimes` 0/1/2，每次自己 +1），第 3 次交 broker 后业务队列继续前进，
//!   消息因 rebalance 锁未过期被 broker 立刻改投 `%DLQ%<group>`（`reconsumeTimes=3`、
//!   `RETRY_TOPIC` 保留业务 topic）。
//! - C12b 顺序侧的 `-1` 是**不设上限**（投过 >=18 次、`%DLQ%` 空），不是并发侧的 16。
//!
//!
//! 用法（先按项目记忆里记的 runbook 起本地集群）：
//! ```text
//! cargo run --example live_consumer -- 127.0.0.1:9876
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::Value as JsonValue;

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{
    ConsumerConfig, DefaultMQPushConsumer, PULL_MAX_IDLE_TIME,
};
use rocketmq_client_remoting::client::mq_client::{MQClientInstance, RegisteredConsumer};
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultLitePullConsumer, LitePullConsumerConfig,
};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, ConsumeOrderlyContext,
    ConsumeOrderlyStatus, MessageListenerConcurrently, MessageListenerOrderly,
};
use rocketmq_client_remoting::client::top_addressing::DefaultTopAddressing;
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::message_const::{
    PROPERTY_MAX_OFFSET, PROPERTY_POP_CK, PROPERTY_RETRY_TOPIC,
    PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::remoting::protocol::admin_body::MessageQueueKey;
use rocketmq_client_remoting::remoting::protocol::body::ConsumerRunningInfo;
use rocketmq_client_remoting::remoting::protocol::heartbeat::{
    ConsumeFromWhere, MessageModel, SubscriptionData,
};
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;

/// 每个测试 topic 的默认队列数（够 C8 做「4 队列分给 2 实例」）。
const QUEUE_NUMS: i32 = 4;
/// 等消息投递到位的最长秒数（含重平衡、长轮询挂起、5s 位点刷新周期）。
const WAIT_SECONDS: u64 = 30;
/// 位点持久化周期是 5s，这里留两个周期的余量。
const PERSIST_SECONDS: u64 = 12;

// ------------------------------------------------------------------ 骨架

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// 毫秒墙钟：`lastPullAt` / `lastPullTimestamp` 用的就是这个口径（Java `System.currentTimeMillis`）。
fn now_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(_) => 0,
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

    fn skip(&mut self, name: &str, why: &str) {
        println!("  [SKIP] {name}: {why}");
    }
}

/// 锁中毒时照常取内值（listener 里的一次 panic 不该让整轮验证连锁崩）。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// 轮询到条件成立或超时（真机的一切都是异步到位的，不能 sleep 一个固定值赌）。
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

/// 本实例收到过多少次 broker 的 `NOTIFY_CONSUMER_IDS_CHANGED(40)`；实例已被回收
/// 时返回 0（40 是 broker 推给长连接的，本端只能靠计数证明它落地了）。
fn ids_changed_count(client_id: &str) -> usize {
    MQClientInstance::find_instance(client_id)
        .map(|i| i.consumer_ids_changed_count())
        .unwrap_or_default()
}

/// 分配结果里剔除 `%RETRY%<group>` 那条队列：`start()` 一定把它和业务 topic 一起/// 订阅（Java `copySubscription`），比较「分摊是否不重不漏」时只该看业务队列。
fn main_queues(keys: &[String]) -> Vec<String> {
    keys.iter()
        .filter(|k| !k.starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX))
        .cloned()
        .collect()
}

// ------------------------------------------------------ 投递记录与 listener

/// 一条投递到 listener 的消息的观测值（全部取自 broker 真实写回的字段）。
#[derive(Debug, Clone)]
struct Delivered {
    body: String,
    tags: String,
    topic: String,
    queue_id: i32,
    queue_offset: i64,
    reconsume_times: i32,
    msg_id: String,
    /// 生产者 `SendResult.msgId` 就是这个客户端唯一 ID；`msg_id` 不是它。
    uniq_key: String,
    offset_msg_id: String,
    retry_topic_prop: Option<String>,
    max_offset_prop: Option<String>,
    has_pop_ck: bool,
}

impl Delivered {
    fn from(msg: &MessageExt) -> Delivered {
        Delivered {
            body: String::from_utf8_lossy(msg.get_body()).into_owned(),
            tags: msg.get_tags().unwrap_or_default().to_string(),
            topic: msg.topic.clone(),
            queue_id: msg.queue_id,
            queue_offset: msg.queue_offset,
            reconsume_times: msg.reconsume_times,
            msg_id: msg.msg_id.clone().unwrap_or_default(),
            uniq_key: msg
                .get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                .unwrap_or_default()
                .to_string(),
            offset_msg_id: msg.get_offset_msg_id().unwrap_or_default().to_string(),
            retry_topic_prop: msg.get_property(PROPERTY_RETRY_TOPIC).map(str::to_string),
            max_offset_prop: msg.get_property(PROPERTY_MAX_OFFSET).map(str::to_string),
            has_pop_ck: msg.get_property(PROPERTY_POP_CK).is_some(),
        }
    }
}

/// listener 侧的收集器。
#[derive(Default)]
struct Inbox {
    items: Mutex<Vec<Delivered>>,
    batches: AtomicUsize,
}

impl Inbox {
    fn snapshot(&self) -> Vec<Delivered> {
        lock(&self.items).clone()
    }

    fn count(&self) -> usize {
        lock(&self.items).len()
    }

    fn bodies(&self) -> BTreeSet<String> {
        self.snapshot().into_iter().map(|d| d.body).collect()
    }

    fn matching(&self, body: &str) -> Vec<Delivered> {
        self.snapshot()
            .into_iter()
            .filter(|d| d.body == body)
            .collect()
    }
}

/// 并发 listener：记录投递，可选「指定 body 第一次到达时判失败」、可选拖慢每批。
struct LiveListener {
    inbox: Arc<Inbox>,
    /// 命中该 body 的**第一次**投递返回 RECONSUME_LATER（C4 重投验证）。
    fail_once: Option<String>,
    /// true 时每次命中都判失败（C4b 要把重试次数耗尽到死信）。
    fail_always: bool,
    /// 每批固定耗时（C9 用来制造待消费积压以触发流控）。
    batch_cost: Duration,
    /// 顺序 listener 命中该 body 时返回 SUSPEND_CURRENT_QUEUE_A_MOMENT（C12 毒消息）。
    /// 与 `fail_once` 分开：并发侧的「失败」在顺序侧必须是**挂起**，两者走的
    /// 是 Java 里两套完全不同的处理分支（回投 broker vs 本地原地重试）。
    suspend_body: Option<String>,
    orderly: Arc<OrderlyState>,
}

/// 顺序消费的违序计数器（listener 内自持，便于外部读取）。
#[derive(Default)]
struct OrderlyState {
    last_offset: Mutex<BTreeMap<i32, i64>>,
    violations: AtomicUsize,
}

impl LiveListener {
    fn collecting(inbox: Arc<Inbox>) -> Arc<LiveListener> {
        Arc::new(LiveListener {
            inbox,
            fail_once: None,
            fail_always: false,
            batch_cost: Duration::ZERO,
            suspend_body: None,
            orderly: Arc::new(OrderlyState::default()),
        })
    }

    fn failing_once(inbox: Arc<Inbox>, body: &str) -> Arc<LiveListener> {
        Arc::new(LiveListener {
            inbox,
            fail_once: Some(body.to_string()),
            fail_always: false,
            batch_cost: Duration::ZERO,
            suspend_body: None,
            orderly: Arc::new(OrderlyState::default()),
        })
    }

    /// 命中该 body 的**每一次**投递都判失败：用来把重试次数跑到上限。
    fn failing_always(inbox: Arc<Inbox>, body: &str) -> Arc<LiveListener> {
        Arc::new(LiveListener {
            inbox,
            fail_once: Some(body.to_string()),
            fail_always: true,
            batch_cost: Duration::ZERO,
            suspend_body: None,
            orderly: Arc::new(OrderlyState::default()),
        })
    }

    fn slow(inbox: Arc<Inbox>, cost: Duration) -> Arc<LiveListener> {
        Arc::new(LiveListener {
            inbox,
            fail_once: None,
            fail_always: false,
            batch_cost: cost,
            suspend_body: None,
            orderly: Arc::new(OrderlyState::default()),
        })
    }

    /// 命中该 body 的**每一次**顺序投递都挂起当前队列（C12 的毒消息）。
    fn orderly_suspending_always(inbox: Arc<Inbox>, body: &str) -> Arc<LiveListener> {
        Arc::new(LiveListener {
            inbox,
            fail_once: None,
            fail_always: false,
            batch_cost: Duration::ZERO,
            suspend_body: Some(body.to_string()),
            orderly: Arc::new(OrderlyState::default()),
        })
    }

    fn record(&self, msgs: &[MessageExt]) -> bool {
        let mut out_of_order = 0usize;
        {
            let mut last = lock(&self.orderly.last_offset);
            let mut items = lock(&self.inbox.items);
            for msg in msgs {
                let d = Delivered::from(msg);
                if let Some(prev) = last.get(&d.queue_id) {
                    if d.queue_offset <= *prev {
                        out_of_order += 1;
                    }
                }
                last.insert(d.queue_id, d.queue_offset);
                items.push(d);
            }
        }
        if out_of_order > 0 {
            self.orderly
                .violations
                .fetch_add(out_of_order, Ordering::SeqCst);
        }
        self.inbox.batches.fetch_add(1, Ordering::SeqCst);
        if !self.batch_cost.is_zero() {
            std::thread::sleep(self.batch_cost);
        }
        out_of_order == 0
    }

    /// 指定 body 是否第一次出现（出现即记失败一次的依据）。
    fn is_first_seen(&self, body: &str) -> bool {
        lock(&self.inbox.items)
            .iter()
            .filter(|d| d.body == body)
            .count()
            <= 1
    }
}

impl MessageListenerConcurrently for LiveListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        self.record(msgs);
        match &self.fail_once {
            Some(body)
                if msgs.iter().any(|m| is_body(m, body))
                    && (self.fail_always || self.is_first_seen(body)) =>
            {
                ConsumeConcurrentlyStatus::ReconsumeLater
            }
            _ => ConsumeConcurrentlyStatus::ConsumeSuccess,
        }
    }
}

impl MessageListenerOrderly for LiveListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeOrderlyContext,
    ) -> ConsumeOrderlyStatus {
        self.record(msgs);
        match &self.suspend_body {
            Some(body) if msgs.iter().any(|m| is_body(m, body)) => {
                ConsumeOrderlyStatus::SuspendCurrentQueueAMoment
            }
            _ => ConsumeOrderlyStatus::Success,
        }
    }
}

fn is_body(msg: &MessageExt, want: &str) -> bool {
    String::from_utf8_lossy(msg.get_body()) == want
}

// ------------------------------------------------------------------ 夹具

struct Fixture {
    namesrv: String,
    stamp: String,
    producer_group: String,
    producer: DefaultMQProducer,
    /// 本例子自建的「运维」实例：建/删 topic、读 broker 位点与存量。
    admin: MQClientInstance,
    broker_name: String,
    broker_addr: String,
    /// 本次建过的 topic，C10 统一删掉。
    topics: Mutex<Vec<String>>,
    /// 广播模式写到 $HOME 下的位点目录，C10 统一删掉。
    offset_dirs: Mutex<Vec<PathBuf>>,
}

impl Fixture {
    fn new(namesrv: &str, stamp: &str) -> Result<Fixture, String> {
        let producer_group = format!("rust-live-consumer-pg-{stamp}");
        let mut producer = DefaultMQProducer::new(&producer_group)
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        let _ = &mut producer;
        let admin_id = format!("rust-live-consumer-admin-{stamp}");
        let admin = MQClientInstance::new(&admin_id, vec![namesrv.to_string()]);
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            producer_group,
            producer,
            admin,
            broker_name: String::new(),
            broker_addr: String::new(),
            topics: Mutex::new(Vec::new()),
            offset_dirs: Mutex::new(Vec::new()),
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
        // 用 TBW102 的路由找一台可建 topic 的 broker（与 live_protocol.rs 同一做法）
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

    /// 显式建 topic（不靠 `autoCreateTopicEnable`，这样队列数确定，C8 才能分摊）。
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

    fn topic_name(&self, kind: &str) -> String {
        format!("RustLiveConsumer{kind}{}", self.stamp)
    }

    fn group_name(&self, kind: &str) -> String {
        format!("rust-live-consumer-{kind}-{}", self.stamp)
    }

    fn base_config(&self, group: &str) -> ConsumerConfig {
        ConsumerConfig {
            consumer_group: group.to_string(),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("live-{group}-{}", self.stamp),
            consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
            ..Default::default()
        }
    }

    /// 起一个已订阅、已设并发 listener 的消费者（广播位点目录顺手登记待清理）。
    fn consumer(
        &self,
        group: &str,
        topic: &str,
        expression: &str,
        listener: Arc<LiveListener>,
    ) -> Result<DefaultMQPushConsumer, String> {
        let consumer = self.plain_consumer(group)?;
        consumer
            .subscribe(topic, expression)
            .map_err(|e| format!("subscribe {topic} failed: {e}"))?;
        consumer.set_message_listener_concurrently(listener);
        Ok(consumer)
    }

    /// 只按组名建消费者（未订阅、未设 listener），C1 的校验分支要用。
    fn plain_consumer(&self, group: &str) -> Result<DefaultMQPushConsumer, String> {
        let cfg = self.base_config(group);
        DefaultMQPushConsumer::with_config(cfg).map_err(|e| format!("build failed: {e}"))
    }

    /// 发到指定 topic（可钉队列）。SendResult 的 status 必须是 SEND_OK。
    async fn produce(&self, topic: &str, tag: &str, n: usize, queue: Option<i32>) -> Vec<(String, i32, i64)> {
        let mut out = Vec::new();
        for i in 0..n {
            let body = format!("{topic}-{i:03}");
            let mut msg = Message::new(topic, Some(body.as_bytes()));
            msg.set_tags(tag);
            msg.set_keys(&format!("{topic}-key-{i}"));
            let target = queue.map(|q| MessageQueue::new(topic, &self.broker_name, q));
            match self
                .producer
                .send(&mut msg, Some(5000), target.as_ref())
                .await
            {
                Ok(r) => out.push((
                    r.msg_id.clone().unwrap_or_default(),
                    r.message_queue.as_ref().map(|m| m.queue_id).unwrap_or(-1),
                    r.queue_offset,
                )),
                Err(e) => {
                    println!("  [WARN] send {body} failed: {e}");
                    out.push((String::new(), -1, -1));
                }
            }
        }
        out
    }

    fn queues(&self, topic: &str, nums: i32) -> Vec<MessageQueue> {
        (0..nums)
            .map(|q| MessageQueue::new(topic, &self.broker_name, q))
            .collect()
    }

    /// 该 topic 全部队列的 maxOffset 之和 == broker 侧真实存量。
    async fn broker_total(&self, topic: &str, nums: i32) -> i64 {
        let mut total = 0;
        for mq in self.queues(topic, nums) {
            match self
                .admin
                .get_max_offset(&mq, 5000, Some(&self.broker_addr))
                .await
            {
                Ok(off) => total += off,
                Err(e) => println!("  [WARN] get_max_offset {mq:?} failed: {e}"),
            }
        }
        total
    }

    /// 读 broker 上该组对该 topic 的已提交位点之和（`QUERY_CONSUMER_OFFSET`）。
    async fn committed_total(&self, group: &str, topic: &str, nums: i32) -> i64 {
        let mut total = 0;
        for mq in self.queues(topic, nums) {
            match self
                .admin
                .query_consumer_offset(group, &mq, 5000, Some(&self.broker_addr), true)
                .await
            {
                Ok(Some(off)) => total += off,
                Ok(None) => {}
                Err(e) => println!("  [WARN] query_consumer_offset {mq:?} failed: {e}"),
            }
        }
        total
    }

    /// 等位点刷到 broker（5s 周期，最长 PERSIST_SECONDS）。
    async fn wait_committed(&self, group: &str, topic: &str, nums: i32, want: i64) -> i64 {
        let deadline = Instant::now() + Duration::from_secs(PERSIST_SECONDS);
        loop {
            let t = self.committed_total(group, topic, nums).await;
            if t >= want || Instant::now() >= deadline {
                return t;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// broker 侧该组的消费者列表（经共享实例查，重平衡的输入就是它）。
    async fn consumer_ids(&self, client_id: &str, topic: &str, group: &str) -> Vec<String> {
        match MQClientInstance::find_instance(client_id) {
            Some(instance) => instance
                .get_consumer_id_list_by_group(topic, group, 5000)
                .await
                .unwrap_or_default(),
            None => Vec::new(),
        }
    }

    fn register_offset_dir(&self, client_id: &str) {
        let home = env::var("HOME").unwrap_or_default();
        if home.is_empty() {
            return;
        }
        lock(&self.offset_dirs).push(
            PathBuf::from(home)
                .join(".rocketmq_offsets")
                .join(client_id),
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

/// 把 runningInfo 的队列表（mqTable / mqPopTable）摊平成
/// `topic+broker+queueId -> ProcessQueueInfo JSON`。
fn table_of(entries: &[(MessageQueueKey, JsonValue)]) -> BTreeMap<String, JsonValue> {
    entries
        .iter()
        .map(|(k, v)| {
            (
                format!("{}{}{}", k.topic, k.broker_name, k.queue_id),
                v.clone(),
            )
        })
        .collect()
}

fn running_table(info: &ConsumerRunningInfo) -> BTreeMap<String, JsonValue> {
    table_of(&info.mq_table)
}

fn table_sum(table: &BTreeMap<String, JsonValue>, field: &str) -> i64 {
    table
        .values()
        .map(|v| v.get(field).and_then(JsonValue::as_i64).unwrap_or(0))
        .sum()
}

/// 运行信息里业务那一路的拉取时钟距今多少毫秒（没有记录时 `None`，调用方直接判失败）。
///
/// 必须排除 `%RETRY%` 那条：Java 的 processQueueTable 按**分配队列**逐条填，自动订阅的重试
/// 队列也在表里、也在被拉，取 `.max()` 会命中它的心跳，业务队列停摆就被掩盖掉了。
/// C11 整段锁在单队列上，所以业务条目就是剩下的唯一一条。
fn pull_clock_age_ms(info: &ConsumerRunningInfo) -> Option<i64> {
    let table = running_table(info);
    let stamp = table
        .iter()
        .filter(|(k, _)| !k.starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX))
        .filter_map(|(_, v)| v.get("lastPullTimestamp").and_then(JsonValue::as_i64))
        .max()?;
    if stamp <= 0 {
        return None;
    }
    Some(now_ms() - stamp)
}

// --------------------------------------------------------------- C1 启动校验

async fn c1_lifecycle(ck: &mut Checker, fx: &Fixture) {
    println!("-- C1 构造与 start()/shutdown() 校验");

    for group in ["", "   "] {
        ck.check(
            "C1 a blank consumerGroup is rejected",
            DefaultMQPushConsumer::new(group).is_err(),
            "a blank group was accepted",
        );
    }

    let topic = fx.topic_name("Lifecycle");
    let group = fx.group_name("lifecycle");
    if let Err(e) = fx.create_topic(&topic, 1).await {
        return ck.abort("C1 create topic", &e);
    }

    // 三道校验按 Java/Python 的顺序逐个命中
    if DefaultTopAddressing::is_configured() {
        ck.skip("C1 missing name server is rejected", "domain addressing is configured in this environment");
    } else {
        let cfg = ConsumerConfig {
            consumer_group: fx.group_name("no-namesrv"),
            ..Default::default()
        };
        let bare = DefaultMQPushConsumer::with_config(cfg).unwrap();
        let err = bare
            .start()
            .await
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        ck.check(
            "C1 start() with neither name server nor domain addressing is rejected",
            err.contains("name server"),
            &err,
        );
        ck.check(
            "C1 a rejected start leaves the consumer not started",
            !bare.is_started(),
            "is_started() was true after a failed start",
        );
    }

    let c = fx.plain_consumer(&group).unwrap();
    let err = c.start().await.err().map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "C1 start() without any subscription is rejected",
        err.contains("subscription"),
        &err,
    );
    c.subscribe(&topic, "*").unwrap();
    let err = c.start().await.err().map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "C1 start() without a message listener is rejected",
        err.contains("listener"),
        &err,
    );
    ck.check(
        "C1 every validation failure keeps started=false so it can be retried",
        !c.is_started(),
        "is_started() was true",
    );

    // 正常起：首轮心跳 + 首轮重平衡都在 start() 返回前完成
    let inbox = Arc::new(Inbox::default());
    c.set_message_listener_concurrently(LiveListener::collecting(inbox.clone()));
    if let Err(e) = c.start().await {
        return ck.abort("C1 start", &format!("{e}"));
    }
    ck.check("C1 start() succeeded", c.is_started(), "started flag false");
    ck.check(
        "C1 start() stamped a clientId (<clientIP>@<instanceName>, Java buildMQClientId)",
        c.client_id()
            .split_once('@')
            .is_some_and(|(ip, instance)| !ip.is_empty() && instance.contains("live-")),
        &format!("clientId={:?}", c.client_id()),
    );
    ck.check(
        "C1 the first heartbeat went out synchronously during start()",
        c.heartbeat_count() >= 1,
        &format!("heartbeat_count={}", c.heartbeat_count()),
    );
    let keys = c.assigned_queue_keys();
    ck.check(
        "C1 the first rebalance ran before start() returned (business queue + the auto-added %RETRY% queue)",
        main_queues(&keys).len() == 1 && keys.len() == 2,
        &format!("assigned={keys:?}"),
    );
    let subs: Vec<SubscriptionData> = c.subscriptions();
    let sub_topics: BTreeSet<String> = subs.iter().map(|s| s.topic.clone()).collect();
    ck.check(
        "C1 start() subscribes the business topic and %RETRY%<group> (Java copySubscription)",
        sub_topics.contains(&topic) && sub_topics.contains(&MixAll::get_retry_topic(&group)),
        &format!("{subs:?}"),
    );
    ck.check(
        "C1 the consumer is registered in its MQClientInstance under its group",
        MQClientInstance::find_instance(&c.client_id())
            .and_then(|i| i.find_consumer(&group))
            .is_some(),
        "find_consumer(group) returned None",
    );

    let again = c.start().await;
    ck.check(
        "C1 start() is idempotent",
        again.is_ok() && c.is_started(),
        &format!("{:?}", again.err()),
    );
    // Java `DefaultMQPushConsumerImpl#subscribe:1265-1275` 只把订阅 put 进表再推一轮心跳，
    // **没有 already-started 守卫**：活着的消费者必须能接新 topic。这里曾经断言「后置订阅
    // 必须被拒」（那是四个移植版早期的自造规矩），5d5ebe7 按 Java 拆掉守卫后这条就成了假失败
    // —— 别再把它改回去。broker 侧「登记真的发生了」由 examples/live_subscribe.rs 的
    // S1 负对照 / S2 后置订阅（300 查询）负责，这里只锁本地语义：接受 + 进表 + unsubscribe 摘掉。
    let late = fx.topic_name("Late");
    let accepted = c.subscribe(&late, "*").is_ok();
    ck.check(
        "C1 subscribe() after start() is accepted (Java has no already-started guard)",
        accepted && c.subscriptions().iter().any(|s| s.topic == late),
        &format!("{accepted:?} subs={:?}", c.subscriptions()),
    );
    c.unsubscribe(&late);
    ck.check(
        "C1 unsubscribe() removes the topic from the live subscription table",
        !c.subscriptions().iter().any(|s| s.topic == late),
        &format!("{:?}", c.subscriptions()),
    );
    // 与 Java/Python 的显式 setter 不同：本项目 update_config 是裸写字段（已在模块头记为差异）
    let before = c.config().message_model.clone();
    c.update_config(|x| x.message_model = MessageModel::BROADCASTING.to_string());
    ck.check(
        "C1 (documented deviation) update_config() is a plain setter, not a guarded setter",
        c.config().message_model != before,
        "update_config did nothing",
    );
    c.update_config(|x| x.message_model = MessageModel::CLUSTERING.to_string());

    let client_id = c.client_id();
    c.shutdown();
    ck.check("C1 shutdown() clears the started flag", !c.is_started(), "still started");
    ck.check(
        "C1 shutdown() removes the consumer from its MQClientInstance registry (Java unregisterConsumer)",
        MQClientInstance::find_instance(&client_id)
            .and_then(|i| i.find_consumer(&group))
            .is_none(),
        "still registered in the instance",
    );
    ck.check(
        "C1 (parity with the reference ports) shutdown() keeps the last allocation snapshot and only stops the loops",
        c.assigned_queue_keys() == keys,
        &format!("{:?}", c.assigned_queue_keys()),
    );
    let before = inbox.count();
    let _ = fx.produce(&topic, "TagA", 1, None).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    ck.check(
        "C1 a shut-down consumer stops pulling: nothing is delivered after shutdown()",
        inbox.count() == before,
        &format!("delivered={} before={}", inbox.count(), before),
    );
    c.shutdown();
    ck.check(
        "C1 shutdown() is idempotent",
        !c.is_started(),
        "the second shutdown changed state",
    );
}

// --------------------------------------------------- C2 长轮询消费 + 位点持久化

async fn c2_pull_consume_and_offset_persist(ck: &mut Checker, fx: &Fixture) {
    println!("-- C2 长轮询消费 24 条 + 位点刷到 broker");
    let topic = fx.topic_name("Pull");
    let group = fx.group_name("pull");
    if let Err(e) = fx.create_topic(&topic, QUEUE_NUMS).await {
        return ck.abort("C2 create topic", &e);
    }
    let inbox = Arc::new(Inbox::default());
    let listener = LiveListener::collecting(inbox.clone());
    let c = match fx.consumer(&group, &topic, "*", listener) {
        Ok(v) => v,
        Err(e) => return ck.abort("C2 build consumer", &e),
    };
    if let Err(e) = c.start().await {
        return ck.abort("C2 start", &format!("{e}"));
    }
    let sent = fx.produce(&topic, "TagA", 24, None).await;
    let sent_ids: BTreeSet<String> = sent.iter().map(|(id, _, _)| id.clone()).collect();

    let got_all = poll_until(|| inbox.count() >= 24, WAIT_SECONDS).await;
    let got = inbox.snapshot();
    ck.check(
        "C2 every message sent after start() is delivered exactly once to a single consumer",
        got_all && got.len() == 24 && inbox.bodies().len() == 24,
        &format!("delivered={} distinct={}", got.len(), inbox.bodies().len()),
    );
    ck.check(
        "C2 the producer's SendResult msgId is the delivered message's UNIQ_KEY property",
        got.iter().all(|d| sent_ids.contains(&d.uniq_key)),
        &format!(
            "first delivered uniqKey={:?}",
            got.first().map(|d| d.uniq_key.clone())
        ),
    );
    ck.check(
        "C2 the delivered msgId is the broker's offset-based ID, not the client uniq key",
        got.iter().all(|d| {
            d.msg_id.len() == 32
                && d.msg_id == d.offset_msg_id
                && d.msg_id != d.uniq_key
                && d.msg_id.get(16..).and_then(|o| i64::from_str_radix(o, 16).ok()).is_some_and(|commit_log_offset| commit_log_offset > 0)
        })
            && got
                .iter()
                .map(|d| d.msg_id.get(..16).unwrap_or_default())
                .collect::<BTreeSet<&str>>()
                .len()
                == 1,
        &format!(
            "first msgId={:?} uniqKey={:?}",
            got.first().map(|d| d.msg_id.clone()),
            got.first().map(|d| d.uniq_key.clone())
        ),
    );
    ck.check(
        "C2 tags, queueId and queueOffset survive the round trip",
        got.iter().all(|d| {
            d.tags == "TagA" && d.queue_id >= 0 && d.queue_id < QUEUE_NUMS && d.queue_offset >= 0
        }),
        &format!("{:?}", got.first()),
    );
    ck.check(
        "C2 (documented gap vs Java PullAPIWrapper:133-140) the pull path stamps no MIN/MAX_OFFSET, so msgAccCnt stays 0",
        got.iter().all(|d| d.max_offset_prop.is_none()) && c.msg_acc_cnt(None) == 0,
        &format!(
            "maxOffsetProp={:?} msgAccCnt={}",
            got.first().and_then(|d| d.max_offset_prop.clone()),
            c.msg_acc_cnt(None)
        ),
    );
    ck.check(
        "C2 the pull path carries no POP_CK (that handle belongs to the pop path)",
        got.iter().all(|d| !d.has_pop_ck),
        "a pulled message had POP_CK",
    );
    ck.check(
        "C2 nothing was retried: reconsumeTimes stays 0 and no RETRY_TOPIC property appears",
        got.iter().all(|d| d.reconsume_times == 0 && d.retry_topic_prop.is_none()),
        &format!("{:?}", got.iter().find(|d| d.reconsume_times != 0)),
    );
    ck.check(
        "C2 consumeMessageBatchMaxSize=1 means one message per listener call",
        inbox.batches.load(Ordering::SeqCst) == 24,
        &format!(
            "batches={} delivered={}",
            inbox.batches.load(Ordering::SeqCst),
            got.len()
        ),
    );

    // 进程内已消费位点求和 == 24（Java offsetStore.readOffset 的口径）
    let status = c.get_consumer_status(Some(&topic));
    ck.check(
        "C2 consume offsets are tracked per assigned queue and cover all 24",
        status.len() == QUEUE_NUMS as usize
            && status.iter().map(|(_, o)| *o).sum::<i64>() == 24,
        &format!("{status:?}"),
    );
    let committed = fx.wait_committed(&group, &topic, QUEUE_NUMS, 24).await;
    ck.check(
        "C2 the 5s persist loop committed the offsets to the broker (QUERY_CONSUMER_OFFSET sums to 24)",
        committed == 24,
        &format!("committed_total={committed}"),
    );
    ck.check(
        "C2 nothing is left buffered once the last batch is consumed",
        c.buffered_message_count() == 0,
        &format!("buffered={}", c.buffered_message_count()),
    );

    // 运维接口：runningInfo 的 mqTable 每队列一条，且 commitOffset 与进程内一致。
    // Java 的 processQueueTable 以「已分配的 MessageQueue」为键，而 `%RETRY%<group>` 那个
    // 队列同样会被分配并建 ProcessQueue（Java consumerRunningInfo 直接遍历这张表），
    // 所以这里必须数出 4 把业务队列 + 1 把重试队列；只数业务队列会漏掉自愈/位点视图。
    let info = c.consumer_running_info();
    let table = running_table(&info);
    let business: Vec<String> = main_queues(&table.keys().cloned().collect::<Vec<_>>());
    let retry_entries: Vec<&String> = table
        .keys()
        .filter(|k| k.starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX))
        .collect();
    ck.check(
        "C2 consumerRunningInfo reports one ProcessQueueInfo per assigned queue (business + the %RETRY% queue, Java processQueueTable)",
        business.len() == QUEUE_NUMS as usize && retry_entries.len() == 1,
        &format!(
            "business={} retry={:?}",
            business.len(),
            retry_entries
        ),
    );
    ck.check(
        "C2 runningInfo commitOffset equals the in-process consume offsets",
        table_sum(&table, "commitOffset") == 24,
        &format!("sum={}", table_sum(&table, "commitOffset")),
    );
    ck.check(
        "C2 runningInfo carries the CONSUME_PASSIVELY identity used by the heartbeat",
        info.properties
            .get("PROP_CONSUME_TYPE")
            .unwrap_or_default()
            .contains("CONSUME_PASSIVELY"),
        &format!("{:?}", info.properties.get("PROP_CONSUME_TYPE")),
    );
    ck.check(
        "C2 runningInfo is serialisable for the broker's examineConsumerRuntimeInfo reply",
        serde_json::to_string(&info.to_json_value()).is_ok(),
        "to_json_value produced unserialisable output",
    );
    c.shutdown();
}

// ------------------------------------------------------------- C3 表达式过滤

async fn c3_tag_filter(ck: &mut Checker, fx: &Fixture) {
    println!("-- C3 只消费订阅的 tag");
    let topic = fx.topic_name("Tag");
    let group = fx.group_name("tag");
    if let Err(e) = fx.create_topic(&topic, QUEUE_NUMS).await {
        return ck.abort("C3 create topic", &e);
    }
    let inbox = Arc::new(Inbox::default());
    let c = match fx.consumer(&group, &topic, "TagA", LiveListener::collecting(inbox.clone())) {
        Ok(v) => v,
        Err(e) => return ck.abort("C3 build consumer", &e),
    };
    if let Err(e) = c.start().await {
        return ck.abort("C3 start", &format!("{e}"));
    }
    let _ = fx.produce(&topic, "TagA", 10, None).await;
    let _ = fx.produce(&topic, "TagB", 10, None).await;

    let settled = poll_until(|| inbox.count() >= 10, WAIT_SECONDS).await;
    // 再等一会儿，确认没有「多余」的 TagB 漏进来
    tokio::time::sleep(Duration::from_secs(3)).await;
    let got = inbox.snapshot();
    ck.check(
        "C3 all 10 subscribed-tag messages are delivered",
        settled && got.len() == 10,
        &format!("delivered={}", got.len()),
    );
    let foreign: Vec<String> = got.iter().filter(|d| d.tags != "TagA").map(|d| d.body.clone()).collect();
    ck.check(
        "C3 no message of the other tag leaks through",
        foreign.is_empty(),
        &format!("{foreign:?}"),
    );
    let total = fx.broker_total(&topic, QUEUE_NUMS).await;
    ck.check(
        "C3 the broker really stored 20 messages, so filtering happened on the client side",
        total == 20,
        &format!("broker_total={total}"),
    );
    c.shutdown();
}

// --------------------------------------------------------- C4 失败重投与还原

async fn c4_retry_and_topic_reset(ck: &mut Checker, fx: &Fixture) {
    println!("-- C4 RECONSUME_LATER → broker 重投 + %RETRY% topic 还原");
    let topic = fx.topic_name("Retry");
    let group = fx.group_name("retry");
    if let Err(e) = fx.create_topic(&topic, 1).await {
        return ck.abort("C4 create topic", &e);
    }
    let inbox = Arc::new(Inbox::default());
    let victim = format!("{topic}-000");
    let c = match fx.consumer(
        &group,
        &topic,
        "*",
        LiveListener::failing_once(inbox.clone(), &victim),
    ) {
        Ok(v) => v,
        Err(e) => return ck.abort("C4 build consumer", &e),
    };
    if let Err(e) = c.start().await {
        return ck.abort("C4 start", &format!("{e}"));
    }
    let _ = fx.produce(&topic, "TagA", 3, None).await;

    let retried = poll_until(|| inbox.matching(&victim).len() >= 2, WAIT_SECONDS).await;
    let seen = inbox.matching(&victim);
    ck.check(
        "C4 a RECONSUME_LATER message comes back from the broker's retry topic",
        retried && seen.len() >= 2,
        &format!("deliveries of {victim} = {}", seen.len()),
    );
    if seen.len() >= 2 {
        let (first, second) = (&seen[0], &seen[1]);
        ck.check(
            "C4 the redelivery carries an increased reconsumeTimes",
            second.reconsume_times >= 1 && second.reconsume_times > first.reconsume_times,
            &format!("first={} second={}", first.reconsume_times, second.reconsume_times),
        );
        ck.check(
            "C4 the listener sees the ORIGINAL topic, not %RETRY%<group> (resetRetryAndNamespace)",
            second.topic == topic,
            &format!("topic={}", second.topic),
        );
        ck.check(
            "C4 the physical retry topic is kept as a property for the user",
            second.retry_topic_prop.as_deref() == Some(topic.as_str())
                || second.retry_topic_prop.is_some(),
            &format!("{:?}", second.retry_topic_prop),
        );
        ck.check(
            "C4 the retried message keeps body and queue identity",
            second.body == first.body && second.queue_id == first.queue_id,
            &format!("first={first:?} second={second:?}"),
        );
    }
    // 判成功的那两条不该被重投
    let distinct = inbox.bodies().len();
    ck.check(
        "C4 only the failed message is redelivered; the succeeded ones are not",
        distinct == 3 && inbox.count() >= 4,
        &format!("distinct={distinct} deliveries={}", inbox.count()),
    );
    let committed = fx.committed_total(&group, &topic, 1).await;
    ck.check(
        "C4 the retry is driven by the broker, not by rewinding the committed offset",
        committed >= 3,
        &format!("committed={committed}"),
    );
    c.shutdown();
}

// ------------------------------- C4c 部分 ack（ConsumeConcurrentlyContext.ackIndex）

/// 只在**首批**把 ackIndex 收窄的并发 listener。
///
/// 后续批次必须整批认可，否则尾巴会永远回投不完，收敛不了。
struct PartialAckListener {
    inbox: Arc<Inbox>,
    /// 首批要 ack 到的下标（含自身）；`None` = 完全不碰 ackIndex（对照腿）。
    ack_index_first_batch: Option<i32>,
    first_batch: Mutex<Vec<String>>,
}

impl MessageListenerConcurrently for PartialAckListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        {
            let mut items = lock(&self.inbox.items);
            for msg in msgs {
                items.push(Delivered::from(msg));
            }
        }
        let batch = self.inbox.batches.fetch_add(1, Ordering::SeqCst);
        if batch == 0 {
            let bodies: Vec<String> = msgs
                .iter()
                .map(|m| String::from_utf8_lossy(m.get_body()).into_owned())
                .collect();
            *lock(&self.first_batch) = bodies;
            if let Some(index) = self.ack_index_first_batch {
                context.ack_index = index;
            }
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

/// `ackIndex` 的部分 ack：为什么只能真机验。
///
/// 离线单测（`consumer.rs` 的 `mod tests`）能锁住「尾巴回投**失败**时位点不越过它」，
/// 但回投**成功**时的语义 —— 未认可的条目真的被 broker 收下并重新投递、已 ack 的那条
/// 整个窗口只投一次、业务队列位点仍然整批前进 —— 只有真 broker 说得了算得了。
/// 写错的两种形态在离线都看不出差别：忘记回投（尾巴静默丢失，收到的条数照样对）、
/// 或者把已 ack 的前缀也回投（消息重复投递，看起来"没丢"）。
/// 造一个「一批 3 条 + 可选收窄 ackIndex」的 push consumer。
fn partial_ack_consumer(
    fx: &Fixture,
    topic: &str,
    group: &str,
    listener: Arc<PartialAckListener>,
) -> Result<DefaultMQPushConsumer, String> {
    // consumeMessageBatchMaxSize 默认 1，不收窄到 3 就根本没有「部分」可言。
    // 用例是「先把 3 条放上去、再起消费者」，所以新组必须从 FIRST_OFFSET 起消：
    // LAST_OFFSET 下新组会从分配时刻的最新位点开始，先发的那 3 条会被直接跳过。
    let cfg = ConsumerConfig {
        consume_message_batch_max_size: 3,
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..fx.base_config(group)
    };
    let c = DefaultMQPushConsumer::with_config(cfg).map_err(|e| format!("build failed: {e}"))?;
    c.subscribe(topic, "*")
        .map_err(|e| format!("subscribe failed: {e}"))?;
    c.set_message_listener_concurrently(listener);
    Ok(c)
}

async fn c4c_partial_ack(ck: &mut Checker, fx: &Fixture) {
    println!("-- C4c CONSUME_SUCCESS + ackIndex 部分 ack → 尾巴经 %RETRY% 重投");
    let topic = fx.topic_name("PartialAck");
    let group = fx.group_name("partial-ack");
    let control_group = fx.group_name("full-ack-control");
    if let Err(e) = fx.create_topic(&topic, 1).await {
        return ck.abort("C4c create topic", &e);
    }
    let inbox = Arc::new(Inbox::default());
    let control_inbox = Arc::new(Inbox::default());
    let listener = Arc::new(PartialAckListener {
        inbox: inbox.clone(),
        ack_index_first_batch: Some(0),
        first_batch: Mutex::new(Vec::new()),
    });
    let control_listener = Arc::new(PartialAckListener {
        inbox: control_inbox.clone(),
        // 对照腿：完全不碰 ackIndex（Java 默认 Integer.MAX_VALUE = 整批认可）
        ack_index_first_batch: None,
        first_batch: Mutex::new(Vec::new()),
    });
    let mut pairs = Vec::new();
    for (g, l) in [(&group, listener.clone()), (&control_group, control_listener.clone())] {
        match partial_ack_consumer(fx, &topic, g, l) {
            Ok(c) => pairs.push(c),
            Err(e) => return ck.abort("C4c build consumer", &e),
        }
    }
    let (c, control) = (pairs.remove(0), pairs.remove(0));
    // 先发再起消费者：批次怎么切由拉取时机决定，队列里已经躺着 3 条时第一次拉取才会
    // 正好是「一整批 3 条」，否则首批可能是 1~2 条，ackIndex=0 划出的前缀/后缀就不确定了。
    fx.produce(&topic, "TagA", 3, None).await;
    if let Err(e) = c.start().await {
        return ck.abort("C4c start", &format!("{e}"));
    }
    if let Err(e) = control.start().await {
        return ck.abort("C4c control start", &format!("{e}"));
    }

    // 首批必须正好 3 条，否则 ackIndex=0 划出来的「前缀/后缀」根本不确定
    let first = poll_until(|| !lock(&listener.first_batch).is_empty(), WAIT_SECONDS).await;
    let first_batch = lock(&listener.first_batch).clone();
    ck.check(
        "C4c the listener really got one batch of 3 messages",
        first && first_batch.len() == 3,
        &format!("firstBatch={first_batch:?}"),
    );
    let tail: Vec<String> = first_batch[1..].to_vec();
    let redelivered = {
        let tail = tail.clone();
        poll_until(
            || {
                tail.iter().all(|b| {
                    inbox
                        .matching(b)
                        .iter()
                        .any(|d| d.reconsume_times >= 1 && d.topic == topic)
                })
            },
            WAIT_SECONDS * 3,
        )
        .await
    };
    let acked = first_batch.first().cloned().unwrap_or_default();
    let seen = inbox.snapshot();
    ck.check(
        "C4c the unacked tail comes back from the broker's retry topic (reconsumeTimes>=1, original topic)",
        redelivered,
        &format!("tail={tail:?} deliveries={}", seen.len()),
    );
    ck.check(
        "C4c the acked prefix message is delivered exactly once (no over-redelivery)",
        !acked.is_empty() && inbox.matching(&acked).len() == 1,
        &format!("acked={acked} times={}", inbox.matching(&acked).len()),
    );
    ck.check(
        "C4c nothing is lost: all 3 bodies were consumed",
        inbox.bodies().len() == 3,
        &format!("distinct={}", inbox.bodies().len()),
    );
    // 尾巴已经交给 broker 重投，业务队列的位点仍要整批前进（Java:266 removeMessage(整批)）
    let committed = fx.wait_committed(&group, &topic, 1, 3).await;
    ck.check(
        "C4c the business queue offset still advances past the whole batch",
        committed == 3,
        &format!("committed={committed}"),
    );
    // 对照腿：不碰 ackIndex ⇒ 一条都不该回投
    let control_seen = poll_until(|| control_inbox.count() >= 3, WAIT_SECONDS).await;
    let control_deliveries = control_inbox.snapshot();
    ck.check(
        "C4c control: default ackIndex (MAX_VALUE) sends NOTHING back",
        control_seen
            && control_inbox.count() == 3
            && control_deliveries.iter().all(|d| d.reconsume_times == 0),
        &format!(
            "deliveries={} times={:?}",
            control_inbox.count(),
            control_deliveries
                .iter()
                .map(|d| d.reconsume_times)
                .collect::<Vec<i32>>()
        ),
    );
    let control_committed = fx.wait_committed(&control_group, &topic, 1, 3).await;
    ck.check(
        "C4c control: the untouched group commits all 3 offsets",
        control_committed == 3,
        &format!("committed={control_committed}"),
    );
    c.shutdown();
    control.shutdown();
}

// --------------------------------------------- C4b 重试耗尽 → %DLQ% 终态

/// `%DLQ%<group>` 现场取证：等 broker 把死信 topic 建出来并注册进路由，再用**独立消费组
/// + `seek_to_begin`** 把已有的死信读出来（新组从队尾开始会把那条死信直接跳过 ⇒ 假失败）。
///
/// 返回 `(是否有路由, 死信消息)`：负向用例里「broker 压根没建死信 topic」本身就是正确
/// 结论，不能和「等不到路由」混成同一个空结果。
async fn read_dlq(fx: &Fixture, group: &str, reader_kind: &str, wait_secs: u64) -> Result<(bool, Vec<Delivered>), String> {
    let dlq_topic = MixAll::get_dlq_topic(group);
    let mut route = None;
    let deadline = Instant::now() + Duration::from_secs(wait_secs);
    while Instant::now() < deadline {
        if let Some(r) = fx.admin.get_topic_route_data(&dlq_topic).await {
            if !r.broker_datas.is_empty() {
                route = Some(r);
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let Some(r) = route else {
        return Ok((false, Vec::new()));
    };
    // 队列数取 broker 建出来的那份路由（`DLQ_NUMS_PER_GROUP` 在 5.5.1 是 1，
    // 但这是 broker 侧常量，别在测试里复述它）
    let nums = r
        .queue_datas
        .iter()
        .map(|q| q.read_queue_nums)
        .sum::<i32>()
        .max(1);
    let mqs = fx.queues(&dlq_topic, nums);
    let lite = DefaultLitePullConsumer::with_config(LitePullConsumerConfig {
        consumer_group: fx.group_name(reader_kind),
        name_server_addrs: vec![fx.namesrv.clone()],
        instance_name: format!("live-{reader_kind}-{}", fx.stamp),
        poll_timeout_millis: 1000,
        ..Default::default()
    })
    .map_err(|e| format!("build lite pull failed: {e}"))?;
    // assign 必须在 start 之前：start 之后那次 rebalance 只认订阅，会把分配算空
    lite.assign(&mqs);
    lite.start()
        .await
        .map_err(|e| format!("lite start failed: {e}"))?;
    for mq in &mqs {
        lite.seek_to_begin(mq)
            .await
            .map_err(|e| format!("seek_to_begin failed: {e}"))?;
    }
    let mut msgs: Vec<Delivered> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(wait_secs);
    while Instant::now() < deadline {
        for m in lite.poll(Some(1000)).await {
            msgs.push(Delivered::from(&m));
        }
        if msgs.is_empty() {
            continue;
        }
        // 收到后再排空几趟：断言「死信里只有这一条」要求把后面的也看见，
        // 但总窗口必须有界，否则正向用例每次都要白等满 wait_secs。
        let drain_to = Instant::now() + Duration::from_secs(3);
        while Instant::now() < drain_to {
            for m in lite.poll(Some(500)).await {
                msgs.push(Delivered::from(&m));
            }
        }
        break;
    }
    lite.shutdown();
    Ok((true, msgs))
}

/// 死信终态：为什么只能真机验。
///
/// 「重试到第几次算用尽」这件事**两端各写一半**：客户端只负责把 `maxReconsumeTimes`
/// 塞进 `CONSUMER_SEND_MSG_BACK` 的请求头（Java `DefaultMQPushConsumerImpl`
/// `#sendMessageBack:773`，`-1` 时按 16 传，见 `#getMaxReconsumeTimes:890`），
/// 而**判定和改投 `%DLQ%<group>` 全在 broker**
/// （`AbstractSendMessageProcessor#consumerSendMsgBack:183`：
/// `msgExt.getReconsumeTimes() >= maxReconsumeTimes || delayLevel < 0`，注意是 `>=`
/// 而不是 `>`；转死信时 topic 换成 `MixAll.getDLQTopic(group)`、队列数取
/// `DLQ_NUMS_PER_GROUP`（5.5.1 = 1）并顺手建 topic，`#226` 又给 reconsumeTimes +1）。
/// 于是两种写反都会「看起来正常」：客户端漏传 header ⇒ broker 用订阅组默认值（16 次），
/// 测试等到天荒地老；把 `>=` 写成 `>` ⇒ 多投一次才进死信。离线单测锁不住任何一个。
///
/// 这里用 `maxReconsumeTimes=2` 把终态压到几十秒内，逐条钉住：
/// 投递恰为 3 次（reconsumeTimes 0/1/2）、第 3 次回投后原组不再有第 4 次、
/// 死信 topic 由 broker 自动建并注册路由、`%DLQ%` 里那条的 reconsumeTimes=3、
/// `RETRY_TOPIC` 仍是业务原始 topic、且只有 1 条。
async fn c4b_dlq_terminal(ck: &mut Checker, fx: &Fixture) {
    println!("-- C4b maxReconsumeTimes 用尽 → %DLQ%<group> 终态");
    const MAX_RECONSUME: i32 = 2;
    let topic = fx.topic_name("Dlq");
    let group = fx.group_name("dlq");
    if let Err(e) = fx.create_topic(&topic, 1).await {
        return ck.abort("C4b create topic", &e);
    }
    let inbox = Arc::new(Inbox::default());
    let victim = format!("{topic}-000");
    let mut cfg = fx.base_config(&group);
    cfg.max_reconsume_times = MAX_RECONSUME;
    let c = match DefaultMQPushConsumer::with_config(cfg) {
        Ok(v) => v,
        Err(e) => return ck.abort("C4b build consumer", &format!("{e}")),
    };
    if let Err(e) = c.subscribe(&topic, "*") {
        return ck.abort("C4b subscribe", &format!("{e}"));
    }
    c.set_message_listener_concurrently(LiveListener::failing_always(inbox.clone(), &victim));
    if let Err(e) = c.start().await {
        return ck.abort("C4b start", &format!("{e}"));
    }
    let sent = fx.produce(&topic, "TagA", 1, None).await;
    // 回投档位 = 3 + reconsumeTimes ⇒ level3(10s) + level4(30s)，再加投递余量。
    // 150s 而不是 100s：整机并发跑其它套件时 broker 的定时服务会拖档，实测第三次
    // 投递能晚到 60s+，卡 100s 是假失败。
    let arrived = poll_until(|| inbox.matching(&victim).len() >= 3, 150).await;
    let seen = inbox.matching(&victim);
    let times: Vec<i32> = seen.iter().map(|d| d.reconsume_times).collect();
    ck.check(
        "C4b maxReconsumeTimes=2 ⇒ 投递 3 次，reconsumeTimes 依次是 0/1/2",
        arrived && times.starts_with(&[0, 1, 2]),
        &format!("times={times:?}"),
    );
    // 反证：死信之后原组不会再收到第 4 次投递（`>=` 写成 `>` 会在这里露馅）
    tokio::time::sleep(Duration::from_secs(15)).await;
    let final_times: Vec<i32> = inbox.matching(&victim).iter().map(|d| d.reconsume_times).collect();
    ck.check(
        "C4b 用尽之后不再投递（观察窗口内只有 3 次）",
        final_times.len() == 3,
        &format!("deliveries={} times={final_times:?}", final_times.len()),
    );
    c.shutdown();

    // %DLQ%<group> 由 broker 在转死信那一刻才建出来并注册到 namesrv
    let dlq_topic = MixAll::get_dlq_topic(&group);
    let (has_route, dlq_msgs) = match read_dlq(fx, &group, "dlqread", 30).await {
        Ok(v) => v,
        Err(e) => return ck.abort("C4b 读 %DLQ%", &e),
    };
    ck.check(
        "C4b broker 自动创建并注册了 %DLQ%<group> 的路由",
        has_route,
        &format!("dlq={dlq_topic}"),
    );
    if has_route {
        ck.check(
            "C4b %DLQ%<group> 只有那一条死信",
            dlq_msgs.len() == 1 && dlq_msgs[0].body == victim,
            &format!("{dlq_msgs:?}"),
        );
    }
    if let Some(d) = dlq_msgs.first() {
        // broker 存储时 +1（AbstractSendMessageProcessor:226）⇒ 2 次重投后为 3
        ck.check(
            "C4b 死信消息的 reconsumeTimes = maxReconsumeTimes + 1",
            d.reconsume_times == MAX_RECONSUME + 1,
            &format!("reconsumeTimes={}", d.reconsume_times),
        );
        ck.check(
            "C4b 死信消息保留 RETRY_TOPIC=业务原始 topic，且 topic 已是 %DLQ%<group>",
            d.retry_topic_prop.as_deref() == Some(topic.as_str()) && d.topic == dlq_topic,
            &format!("topic={} retryTopic={:?}", d.topic, d.retry_topic_prop),
        );
        // 重投过程中 properties 是整份搬过去的 ⇒ 唯一 ID 不会被改写
        let origin = sent.first().map(|s| s.0.clone()).unwrap_or_default();
        ck.check(
            "C4b 死信消息的 UNIQ_KEY 仍是最初那条的 msgId",
            !origin.is_empty() && d.uniq_key == origin,
            &format!("sent={origin} dlqUniq={}", d.uniq_key),
        );
    }
    if has_route {
        // 交给 C10 一起删掉，别在 broker 上留 %DLQ%/%RETRY% 垃圾
        lock(&fx.topics).push(dlq_topic.clone());
        lock(&fx.topics).push(MixAll::get_retry_topic(&group));
    }
}

// ------------------------------------------------------------- C5 POP + ack

async fn c5_pop_mode(ck: &mut Checker, fx: &Fixture) {
    println!("-- C5 POP 模式：弹出带 CK、ack 后不再复活");
    let topic = fx.topic_name("Pop");
    let group = fx.group_name("pop");
    if let Err(e) = fx.create_topic(&topic, 2).await {
        return ck.abort("C5 create topic", &e);
    }
    let inbox = Arc::new(Inbox::default());
    let c = match fx.consumer(&group, &topic, "*", LiveListener::collecting(inbox.clone())) {
        Ok(v) => v,
        Err(e) => return ck.abort("C5 build consumer", &e),
    };
    c.update_config(|x| {
        x.pop_mode = true;
        // Java 钳制区间 [5s, 300s] 的下界：复活观察窗口够短
        x.pop_invisible_time = 5_000;
        x.pop_batch_nums = 32;
    });
    if let Err(e) = c.start().await {
        return ck.abort("C5 start", &format!("{e}"));
    }
    let _ = fx.produce(&topic, "TagA", 12, None).await;
    let got_all = poll_until(|| inbox.count() >= 12, WAIT_SECONDS).await;
    let got = inbox.snapshot();
    ck.check(
        "C5 POP delivers every message of the topic",
        got_all && inbox.count() >= 12,
        &format!("delivered={}", inbox.count()),
    );
    ck.check(
        "C5 popped messages carry POP_CK (the handle used to ack)",
        got.iter().all(|d| d.has_pop_ck),
        "some popped message had no POP_CK",
    );
    let drained = poll_until(|| c.wait_ack_count() == 0, 10).await;
    ck.check(
        "C5 waitAckCounter drops to 0 once the batches are acked",
        drained,
        &format!("wait_ack_count={}", c.wait_ack_count()),
    );
    // ack 生效的直接证据：等过一个 invisibleTime 窗口后 broker 没把消息复活重投
    tokio::time::sleep(Duration::from_secs(8)).await;
    let after = inbox.count();
    ck.check(
        "C5 acked messages are not re-populated after invisibleTime elapsed",
        after == 12,
        &format!("deliveries after the invisible window = {after}"),
    );
    let status = c.get_consumer_status(Some(&topic));
    ck.check(
        "C5 the pop path never touches consume offsets (progress lives in the broker checkpoint)",
        status.is_empty(),
        &format!("{status:?}"),
    );
    let info = c.consumer_running_info();
    let pop_table = table_of(&info.mq_pop_table);
    // pop 模式下每个分配队列（含 `%RETRY%<group>`）都有 PopProcessQueue，
    // 与 Java `rebalanceByTopic` 给 processQueueTablePop 建队列的口径一致。
    let want: BTreeSet<String> = c.assigned_queue_keys().into_iter().collect();
    let reported: BTreeSet<String> = pop_table.keys().cloned().collect();
    ck.check(
        "C5 runningInfo fills mqPopTable (one entry per popped queue) and leaves mqTable empty for the pop path",
        !pop_table.is_empty()
            && info.mq_table.is_empty()
            && reported == want
            && table_sum(&pop_table, "cachedMsgCount") == 0,
        &format!(
            "pop_table={:?} want={:?} table={} cachedMsgCount={}",
            reported,
            want,
            info.mq_table.len(),
            table_sum(&pop_table, "cachedMsgCount")
        ),
    );
    c.shutdown();
}

// ------------------------------------------- C5b POP 路径的拉取统计（307 statusTable）

/// Java `popMessage` 的 `PopCallback.onSuccess:556-563`：POP 循环和 pull 循环一样要把
/// `incPullRT` / `incPullTPS` 记进状态表，307 应答的 `statusTable` 全靠这两格。漏记是
/// **静默**故障 —— 消息照弹照 ack、消费完全正常，只有运维看板上一片 0，而看板上
/// "这个消费者没在拉取"和"这个消费者压根没起来"是两种完全不同的处置。
///
/// ⚠ 采样器每 10s 落一个 minute 点、快照取首尾差分，所以流量必须**持续**跨过两个采样点
///   （这里 13 轮 × 2s ≈ 26s），否则 `pullTPS` 仍是 0 —— 那是夹具不够长，不是判据错。
/// ⚠ 读的是 admin 侧的 307（经 broker 转发回本实例），不是进程内自查：序列化与转发那两段
///   也在这条链路上，本进程读表看不到。
async fn c5b_pop_pull_stats(ck: &mut Checker, fx: &Fixture) {
    println!("-- C5b POP 循环把 pullRT/pullTPS 写进 307 状态表");
    let topic = fx.topic_name("PopStats");
    let group = fx.group_name("popstats");
    if let Err(e) = fx.create_topic(&topic, 2).await {
        return ck.abort("C5b create topic", &e);
    }
    let inbox = Arc::new(Inbox::default());
    let c = match fx.consumer(&group, &topic, "*", LiveListener::collecting(inbox.clone())) {
        Ok(v) => v,
        Err(e) => return ck.abort("C5b build consumer", &e),
    };
    c.update_config(|x| {
        x.pop_mode = true;
        x.pop_invisible_time = 10_000;
    });
    if let Err(e) = c.start().await {
        return ck.abort("C5b start", &format!("{e}"));
    }
    let mut sent = 0usize;
    for _ in 0..13 {
        sent += fx.produce(&topic, "TagA", 4, None).await.len();
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let client_id = c.client_id();
    // 独立的 admin 实例：307 是「admin → broker → 目标客户端」的转发链，
    // 用消费者自己的实例查自己等于跳过整条链路。
    let admin = DefaultMQAdminExt::with_config(AdminConfig {
        instance_name: format!("ADMIN-C5B-{}", fx.stamp),
        name_server_addrs: vec![fx.namesrv.clone()],
        timeout_millis: 10_000,
        ..Default::default()
    });
    if let Err(e) = admin.start().await {
        c.shutdown();
        return ck.abort("C5b admin start", &e.to_string());
    }
    let info = match admin
        .examine_consumer_running_info(&group, &client_id, false, None)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            admin.shutdown();
            c.shutdown();
            return ck.abort("C5b 307 examineConsumerRunningInfo", &e.to_string());
        }
    };
    let cs = match info.consume_status(&topic) {
        Ok(Some(v)) => v,
        Ok(None) => {
            admin.shutdown();
            c.shutdown();
            return ck.abort("C5b statusTable", "statusTable has no entry for the topic");
        }
        Err(e) => {
            admin.shutdown();
            c.shutdown();
            return ck.abort("C5b statusTable decode", &e.to_string());
        }
    };
    println!(
        "   sent={sent} pullRT={:.2} pullTPS={:.4} consumeOKTPS={:.4}",
        cs.pull_rt, cs.pull_tps, cs.consume_ok_tps
    );
    ck.check(
        "C5b pullRT is non-zero (every FOUND pop records its elapsed time)",
        cs.pull_rt > 0.0,
        &format!("pullRT={}", cs.pull_rt),
    );
    ck.check(
        "C5b pullTPS is non-zero (counted by the messages actually popped)",
        cs.pull_tps > 0.0,
        &format!("pullTPS={}", cs.pull_tps),
    );
    // 两格各自独立：只有 consumeOKTPS 有值而 pull* 全 0，正是 POP 循环漏记拉取统计的形状。
    ck.check(
        "C5b consumeOKTPS is non-zero as well (pull side and consume side report separately)",
        cs.consume_ok_tps > 0.0,
        &format!("consumeOKTPS={}", cs.consume_ok_tps),
    );
    // 状态表非 0 只说明"计数被调过"；还得确认这背后的流量真被消费掉 —— 否则 pullTPS
    // 可以靠一直接触到从未 ack 完的消息刷高，看板上好看、实际在打转。
    let consumed = poll_until(|| inbox.count() >= sent, 20).await;
    let got = inbox.count();
    ck.check(
        "C5b the traffic behind those stats was really consumed (not just counted)",
        consumed,
        &format!("delivered={got} sent={sent}"),
    );
    admin.shutdown();
    c.shutdown();
}

// ------------------------------------------------------- C6 广播 + 本地位点文件

async fn c6_broadcasting_and_local_offsets(ck: &mut Checker, fx: &Fixture) {
    println!("-- C6 BROADCASTING：各自全量消费 + 位点落本地文件");
    let topic = fx.topic_name("Bcast");
    let group = fx.group_name("bcast");
    if let Err(e) = fx.create_topic(&topic, 2).await {
        return ck.abort("C6 create topic", &e);
    }
    let _ = fx.produce(&topic, "TagA", 8, None).await;

    let inbox_a = Arc::new(Inbox::default());
    let client_a = format!("live-bcast-a-{}", fx.stamp);
    let mut cfg_a = fx.base_config(&group);
    cfg_a.instance_name = client_a.clone();
    cfg_a.client_id = Some(client_a.clone());
    cfg_a.message_model = MessageModel::BROADCASTING.to_string();
    fx.register_offset_dir(&client_a);
    let a = match DefaultMQPushConsumer::with_config(cfg_a.clone()) {
        Ok(c) => c,
        Err(e) => return ck.abort("C6 build A", &e.to_string()),
    };
    if let Err(e) = a.subscribe(&topic, "*") {
        return ck.abort("C6 subscribe A", &e.to_string());
    }
    a.set_message_listener_concurrently(LiveListener::collecting(inbox_a.clone()));
    if let Err(e) = a.start().await {
        return ck.abort("C6 start A", &format!("{e}"));
    }
    let got_a = poll_until(|| inbox_a.count() >= 8, WAIT_SECONDS).await;
    ck.check(
        "C6 a BROADCASTING consumer reads the whole topic from the first offset",
        got_a && inbox_a.bodies().len() == 8,
        &format!("distinct={}", inbox_a.bodies().len()),
    );
    ck.check(
        "C6 BROADCASTING never commits to the broker",
        fx.committed_total(&group, &topic, 2).await == 0,
        &format!(
            "committed_total={}",
            fx.committed_total(&group, &topic, 2).await
        ),
    );
    ck.check(
        "C6 a broadcasting consumer still registers itself so the group is visible",
        a.message_model() == MessageModel::BROADCASTING,
        &a.message_model(),
    );

    // 同组第二个广播实例必须拿到全量（广播不分摊队列）
    let inbox_b = Arc::new(Inbox::default());
    let client_b = format!("live-bcast-b-{}", fx.stamp);
    let mut cfg_b = cfg_a.clone();
    cfg_b.instance_name = client_b.clone();
    cfg_b.client_id = Some(client_b.clone());
    fx.register_offset_dir(&client_b);
    let b = match DefaultMQPushConsumer::with_config(cfg_b) {
        Ok(c) => c,
        Err(e) => {
            a.shutdown();
            return ck.abort("C6 build B", &e.to_string());
        }
    };
    if let Err(e) = b.subscribe(&topic, "*") {
        a.shutdown();
        return ck.abort("C6 subscribe B", &e.to_string());
    }
    b.set_message_listener_concurrently(LiveListener::collecting(inbox_b.clone()));
    if let Err(e) = b.start().await {
        a.shutdown();
        return ck.abort("C6 start B", &format!("{e}"));
    }
    let got_b = poll_until(|| inbox_b.count() >= 8, WAIT_SECONDS).await;
    ck.check(
        "C6 a second BROADCASTING instance in the same group gets every queue, not a share",
        got_b && b.assigned_queue_count() == 2 && inbox_b.bodies().len() == 8,
        &format!(
            "assigned={} distinct={}",
            b.assigned_queue_count(),
            inbox_b.bodies().len()
        ),
    );

    // 本地位点文件：shutdown 时 save_local_offsets 先写 .tmp 再 rename
    a.shutdown();
    b.shutdown();
    let home = env::var("HOME").unwrap_or_default();
    if home.is_empty() {
        ck.skip("C6 local offset file", "HOME is not set");
        return;
    }
    let file_a = PathBuf::from(&home)
        .join(".rocketmq_offsets")
        .join(&client_a)
        .join(&group)
        .join("offsets.json");
    let written = poll_until(|| file_a.exists(), 5).await;
    ck.check(
        "C6 the broadcasting offset file lands on .rocketmq_offsets/<clientId>/<group>/offsets.json",
        written,
        &format!("{}", file_a.display()),
    );
    if written {
        match std::fs::read_to_string(&file_a) {
            Ok(text) => {
                let parsed: BTreeMap<String, i64> = serde_json::from_str(&text).unwrap_or_default();
                ck.check(
                    "C6 the local offset file is a flat queueKey->offset JSON object summing to 8",
                    parsed.len() == 2 && parsed.values().sum::<i64>() == 8,
                    &format!("{parsed:?} raw={text}"),
                );
                ck.check(
                    "C6 no half-written offset file is left behind (tmp is renamed away)",
                    !file_a.with_file_name("offsets.json.tmp").exists(),
                    "offsets.json.tmp survived",
                );
            }
            Err(e) => ck.abort("C6 read the local offset file", &e.to_string()),
        }
    }
}

// --------------------------------------------------------------- C7 顺序消费

async fn c7_orderly_and_lock(ck: &mut Checker, fx: &Fixture) {
    println!("-- C7 Orderly listener：LOCK_BATCH_MQ + 队列内严格递增");
    let topic = fx.topic_name("Order");
    let group = fx.group_name("order");
    if let Err(e) = fx.create_topic(&topic, 2).await {
        return ck.abort("C7 create topic", &e);
    }
    let inbox = Arc::new(Inbox::default());
    let listener = LiveListener::collecting(inbox.clone());
    let c = match fx.plain_consumer(&group) {
        Ok(c) => c,
        Err(e) => {
            return ck.abort("C7 build consumer", &e);
        }
    };
    if let Err(e) = c.subscribe(&topic, "*") {
        return ck.abort("C7 subscribe", &e.to_string());
    }
    c.set_message_listener_orderly(listener.clone());
    if let Err(e) = c.start().await {
        return ck.abort("C7 start", &format!("{e}"));
    }
    // 钉死两个队列各 6 条，这样「队列内递增」才有对照（并发模式不保证顺序）
    let _ = fx.produce(&topic, "TagA", 6, Some(0)).await;
    let _ = fx.produce(&topic, "TagA", 6, Some(1)).await;

    let got_all = poll_until(|| inbox.count() >= 12, WAIT_SECONDS).await;
    ck.check(
        "C7 an orderly listener receives the whole topic",
        got_all && inbox.count() == 12,
        &format!("delivered={}", inbox.count()),
    );
    ck.check(
        "C7 deliveries inside one queue are strictly increasing by queueOffset",
        listener.orderly.violations.load(Ordering::SeqCst) == 0,
        &format!(
            "violations={}",
            listener.orderly.violations.load(Ordering::SeqCst)
        ),
    );
    // 顺序拉取遇到没锁的队列直接跳过（`queue_pull_loop` 查 `lock_ok`），所以上面能
    // 收满 12 条已经说明锁拿到了；这里再直读一次锁状态。
    // runningInfo 的 ProcessQueueInfo.locked 四个移植版都不回填，观测点只有锁表。
    let locked = poll_until(|| main_queues(&c.locked_queue_keys()).len() == 2, 25).await;
    ck.check(
        "C7 the lock loop holds every assigned queue (LOCK_BATCH_MQ accepted by the 5.5.1 broker)",
        locked,
        &format!("locked={:?}", c.locked_queue_keys()),
    );

    c.shutdown();
    // 撤位验证：换一个 clientId 的实例接手同组同 topic。broker 的 RebalanceLockManager
    // 只在「同一个 clientId」或「锁过期（60s）」时放行，所以 B 能立刻锁上并消费，
    // 就是 A 退出时 UNLOCK_BATCH_MQ 真把锁还了的直接证据。
    let client_b = format!("live-order-b-{}", fx.stamp);
    let mut cfg_b = fx.base_config(&group);
    cfg_b.instance_name = client_b.clone();
    cfg_b.client_id = Some(client_b.clone());
    let inbox_b = Arc::new(Inbox::default());
    let b = match DefaultMQPushConsumer::with_config(cfg_b) {
        Ok(c) => c,
        Err(e) => return ck.abort("C7 build the takeover consumer", &e.to_string()),
    };
    if let Err(e) = b.subscribe(&topic, "*") {
        return ck.abort("C7 takeover subscribe", &e.to_string());
    }
    b.set_message_listener_orderly(LiveListener::collecting(inbox_b.clone()));
    if let Err(e) = b.start().await {
        return ck.abort("C7 takeover start", &format!("{e}"));
    }
    let took_over = poll_until(|| main_queues(&b.locked_queue_keys()).len() == 2, WAIT_SECONDS).await;
    let _ = fx.produce(&topic, "TagA", 2, Some(0)).await;
    let _ = fx.produce(&topic, "TagA", 2, Some(1)).await;
    let got_new = poll_until(|| inbox_b.count() >= 4, WAIT_SECONDS).await;
    ck.check(
        "C7 unlock on shutdown lets a new instance of the same group lock every queue at once",
        took_over && got_new,
        &format!(
            "locked={:?} delivered={}",
            b.locked_queue_keys(),
            inbox_b.count()
        ),
    );
    let seen: BTreeSet<(i32, i64)> = inbox_b
        .snapshot()
        .into_iter()
        .map(|d| (d.queue_id, d.queue_offset))
        .collect();
    ck.check(
        "C7 the takeover instance resumes from the committed offsets instead of replaying the queue",
        seen == BTreeSet::from([(0, 6), (0, 7), (1, 6), (1, 7)]),
        &format!("{seen:?}"),
    );
    b.shutdown();
}

// ----------------------------------------------------- C8 多实例分摊与撤位

async fn c8_scale_in_and_takeover(ck: &mut Checker, fx: &Fixture) {
    println!("-- C8 同组两实例分摊 4 队列，撤一个后另一个接管全部");
    let topic = fx.topic_name("Scale");
    let group = fx.group_name("scale");
    if let Err(e) = fx.create_topic(&topic, QUEUE_NUMS).await {
        return ck.abort("C8 create topic", &e);
    }
    let inbox_a = Arc::new(Inbox::default());
    let inbox_b = Arc::new(Inbox::default());
    let a = match fx.consumer(&group, &topic, "*", LiveListener::collecting(inbox_a.clone())) {
        Ok(v) => v,
        Err(e) => return ck.abort("C8 build A", &e),
    };
    let mut cfg_b = a.config();
    cfg_b.instance_name = format!("live-{group}-b-{}", fx.stamp);
    cfg_b.client_id = Some(cfg_b.instance_name.clone());
    let b = match DefaultMQPushConsumer::with_config(cfg_b) {
        Ok(c) => c,
        Err(e) => return ck.abort("C8 build B", &e.to_string()),
    };
    if let Err(e) = b.subscribe(&topic, "*") {
        return ck.abort("C8 subscribe B", &e.to_string());
    }
    b.set_message_listener_concurrently(LiveListener::collecting(inbox_b.clone()));
    if let Err(e) = a.start().await {
        return ck.abort("C8 start A", &format!("{e}"));
    }
    if let Err(e) = b.start().await {
        a.shutdown();
        return ck.abort("C8 start B", &format!("{e}"));
    }
    // 双方都要在 broker 的成员列表里看到对方，否则重平衡各拿全部 → 重复消费。
    // 这条断言要打网络，不能用同步的 `poll_until`。
    let ids = {
        let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
        loop {
            let found = fx.consumer_ids(&a.client_id(), &topic, &group).await;
            if found.len() >= 2 || Instant::now() >= deadline {
                break found;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };
    ck.check(
        "C8 the broker-side member list of the group contains both instances",
        ids.len() >= 2,
        &format!("clientIdList={ids:?}"),
    );
    let split = poll_until(
        || {
            main_queues(&a.assigned_queue_keys()).len()
                + main_queues(&b.assigned_queue_keys()).len()
                == QUEUE_NUMS as usize
        },
        WAIT_SECONDS,
    )
    .await;
    let (ka, kb) = (a.assigned_queue_keys(), b.assigned_queue_keys());
    let (ma, mb) = (main_queues(&ka), main_queues(&kb));
    let union: BTreeSet<String> = ma.iter().chain(mb.iter()).cloned().collect();
    ck.check(
        "C8 the two instances split the 4 business queues without overlap",
        split && ma.len() + mb.len() == union.len() && union.len() == QUEUE_NUMS as usize,
        &format!("A={ma:?} B={mb:?}"),
    );
    ck.check(
        "C8 each instance got at least one queue (averagely allocation)",
        !ma.is_empty() && !mb.is_empty(),
        &format!("A={ma:?} B={mb:?}"),
    );

    // 反向推送只有 broker 发得出来，测试无法注入，所以这里是全链路唯一的观测点：
    // B 注册进组后，broker 沿 A 的长连接推 `NOTIFY_CONSUMER_IDS_CHANGED(40)`，
    // 实例级处理器把它记成一个计数并叫醒 A 的重平衡。
    // 断言的不是数值（通知可能来 0~N 次），而是「本端确实收到并处理过」——
    // 处理器没注册时这里会一直超时，而 remoting 层会打 WARN。
    let id_a = a.client_id();
    let notified = poll_until(|| ids_changed_count(&id_a) > 0, WAIT_SECONDS).await;
    ck.check(
        "C8 the broker's NOTIFY_CONSUMER_IDS_CHANGED(40) reached the earlier member",
        notified,
        &format!(
            "clientId={id_a} count={} (0 means the instance-level processor never ran)",
            ids_changed_count(&id_a)
        ),
    );

    let _ = fx.produce(&topic, "TagA", 16, None).await;
    let got_all = poll_until(|| inbox_a.count() + inbox_b.count() >= 16, WAIT_SECONDS).await;
    let mut bodies = inbox_a.bodies();
    bodies.extend(inbox_b.bodies());
    ck.check(
        "C8 a split group still receives all 16 messages exactly once",
        got_all && bodies.len() == 16,
        &format!(
            "A={} B={} distinct={}",
            inbox_a.count(),
            inbox_b.count(),
            bodies.len()
        ),
    );

    // 撤掉 A：broker 的成员列表变化后经 40 通知（或 20s 定时）让 B 接管
    let id_b = b.client_id();
    let b_before = ids_changed_count(&id_b);
    a.shutdown();
    let took_over = poll_until(
        || main_queues(&b.assigned_queue_keys()).len() == QUEUE_NUMS as usize,
        WAIT_SECONDS + 10,
    )
    .await;
    ck.check(
        "C8 after one instance shuts down the survivor takes over every queue",
        took_over,
        &format!("B assigned={:?}", main_queues(&b.assigned_queue_keys())),
    );
    // 接管不该只能等 20s 定时重平衡：A 的连接断开时 broker 会给 B 长连接推 40。
    let survivor_notified =
        poll_until(|| ids_changed_count(&id_b) > b_before, WAIT_SECONDS).await;
    ck.check(
        "C8 the survivor is pushed NOTIFY_CONSUMER_IDS_CHANGED(40) when a member leaves",
        survivor_notified,
        &format!(
            "clientId={id_b} count {} -> {} (timed rebalance alone would leave it unchanged)",
            b_before,
            ids_changed_count(&id_b)
        ),
    );
    let before_b = inbox_b.count();
    let _ = fx.produce(&topic, "TagA", 4, None).await;
    let still_consuming =
        poll_until(|| inbox_b.count() > before_b, WAIT_SECONDS).await;
    ck.check(
        "C8 the survivor keeps consuming after the takeover (its pull loops were re-spawned)",
        still_consuming,
        &format!("B delivered={} before={}", inbox_b.count(), before_b),
    );
    let broker_total = fx.broker_total(&topic, QUEUE_NUMS).await;
    let committed = fx.wait_committed(&group, &topic, QUEUE_NUMS, broker_total).await;
    ck.check(
        "C8 the surviving member's committed offsets cover every message on the broker",
        committed == broker_total,
        &format!("committed_total={committed} broker_total={broker_total}"),
    );
    b.shutdown();
}

// ----------------------------------------------------- C9 运维接口与背压

async fn c9_admin_and_flow_control(ck: &mut Checker, fx: &Fixture) {
    println!("-- C9 运维接口 / 流控背压 / 手工触发");
    let topic = fx.topic_name("Admin");
    let group = fx.group_name("admin");
    if let Err(e) = fx.create_topic(&topic, QUEUE_NUMS).await {
        return ck.abort("C9 create topic", &e);
    }
    let inbox = Arc::new(Inbox::default());
    let listener = LiveListener::slow(inbox.clone(), Duration::from_millis(50));
    let c = match fx.consumer(&group, &topic, "*", listener) {
        Ok(v) => v,
        Err(e) => return ck.abort("C9 build consumer", &e),
    };
    // 队列内缓冲 1 条就流控 —— 消费慢于拉取时才会真的命中。
    // 另外两道闸门这里要"关掉"，但写法不能是 0：Java checkConfig（:1099-1209）把
    // 它们的合法下界都定在 1，`start()` 会直接拒（见 S5/C13），所以用各自的上界。
    c.update_config(|x| {
        x.pull_threshold_for_queue = 1;
        x.pull_threshold_size_for_queue = 1024;
        x.consume_concurrently_max_span = 65535;
    });
    if let Err(e) = c.start().await {
        return ck.abort("C9 start", &format!("{e}"));
    }
    let _ = fx.produce(&topic, "TagA", 30, None).await;
    let got_all = poll_until(|| inbox.count() >= 30, WAIT_SECONDS + 20).await;
    ck.check(
        "C9 flow control pauses pulling but never drops a message",
        got_all && inbox.bodies().len() == 30,
        &format!("delivered={} distinct={}", inbox.count(), inbox.bodies().len()),
    );
    ck.check(
        "C9 the flow-control counter fired (pullThresholdForQueue=1)",
        c.flow_control_triggered() > 0,
        &format!("triggered={}", c.flow_control_triggered()),
    );
    ck.check(
        "C9 the per-queue buffer stayed within the threshold plus one in-flight batch",
        c.buffered_message_count() <= 2,
        &format!("buffered={}", c.buffered_message_count()),
    );

    // 积压统计（Java ProcessQueue.msgAccCnt，取自 broker 的 MAX_OFFSET 属性）
    let key = c.assigned_queue_keys().first().cloned().unwrap_or_default();
    let acc = c.msg_acc_cnt(Some(&key));
    let total_acc = c.compute_accumulation_total();
    ck.check(
        "C9 msgAccCnt per queue and the accumulated total are readable",
        acc >= 0 && total_acc >= 0,
        &format!("key={key} acc={acc} total={total_acc}"),
    );

    // 线程池守卫（Java DefaultMQPushConsumerImpl.updateCorePoolSize 的三道守卫）
    ck.check(
        "C9 updateCorePoolSize rejects 0",
        !c.update_core_pool_size(0),
        "accepted 0",
    );
    ck.check(
        "C9 updateCorePoolSize rejects anything above Short.MAX_VALUE",
        !c.update_core_pool_size(32768),
        "accepted 32768",
    );
    ck.check(
        "C9 updateCorePoolSize rejects values >= consumeThreadMax",
        !c.update_core_pool_size(c.config().consume_thread_max),
        "accepted consumeThreadMax",
    );
    ck.check(
        "C9 updateCorePoolSize accepts a smaller value and getCorePoolSize reads it back",
        c.update_core_pool_size(7) && c.get_core_pool_size() == 7,
        &format!("core={}", c.get_core_pool_size()),
    );

    // 手工触发：心跳 / 订阅队列查询 / 立即重平衡
    let hb = c.send_heartbeat_to_all_broker().await;
    ck.check(
        "C9 a manual heartbeat reaches at least one broker",
        hb >= 1,
        &format!("brokers={hb} count={}", c.heartbeat_count()),
    );
    let queues = c
        .fetch_subscribe_message_queues(&topic)
        .await
        .unwrap_or_default();
    ck.check(
        "C9 fetchSubscribeMessage_queues returns every read queue of the topic",
        queues.len() == QUEUE_NUMS as usize && queues.iter().all(|q| q.topic == topic),
        &format!("{queues:?}"),
    );
    c.rebalance_immediately();
    let rebalanced = c.do_rebalance().await.is_ok();
    let after = main_queues(&c.assigned_queue_keys());
    ck.check(
        "C9 a forced rebalance keeps the same assignment",
        rebalanced && after.len() == QUEUE_NUMS as usize,
        &format!("{:?}", c.assigned_queue_keys()),
    );

    // 经实例注册表拿回消费者并主动刷位点（Java MQClientInstance.persistAllConsumerOffset）
    let reg = MQClientInstance::find_instance(&c.client_id()).and_then(|i| i.find_consumer(&group));
    match reg {
        Some(rc) => match rc.persist_consumer_offset().await {
            Ok(()) => {
                let committed = fx.wait_committed(&group, &topic, QUEUE_NUMS, 30).await;
                ck.check(
                    "C9 persist_consumer_offset() through the registry commits every consumed message",
                    committed == 30,
                    &format!("committed_total={committed}"),
                );
            }
            Err(e) => ck.abort("C9 persist_consumer_offset", &e.to_string()),
        },
        None => ck.abort(
            "C9 persist_consumer_offset",
            "the consumer is not registered in its MQClientInstance",
        ),
    }
    c.shutdown();
}

// ------------------------------------------------- C11 拉取停摆自愈（isPullExpired）

/// 用调用方给定的 body 发送，绕开 `produce` 的 `topic-00i` 编号。
///
/// 自愈场景必须"每一批 body 都不重名"，否则"撤走重建有没有把老消息重投一遍"这件事
/// 会被编号撞车掩盖掉 —— `produce` 每轮都从 000 开始，两批之间天然重叠。
async fn send_bodies(fx: &Fixture, topic: &str, bodies: &[String]) -> usize {
    let mut sent = 0usize;
    for body in bodies {
        let mut msg = Message::new(topic, Some(body.as_bytes()));
        msg.set_tags("TagA");
        match fx.producer.send(&mut msg, Some(5000), None).await {
            Ok(_) => sent += 1,
            Err(e) => println!("  [WARN] send {body} failed: {e}"),
        }
    }
    sent
}

fn batch(prefix: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{prefix}-{i}")).collect()
}

/// 注入一次停摆并走**生产路径**自愈，返回是否成功自愈。
///
/// 为什么允许重试：盖章只发生在**发起**拉取的那一刻，而真循环多半正挂在 30s 长轮询上，
/// 所以倒拨之后基本第一次就能被判停摆。极少数情况下旧的拉取请求刚好回来并重新盖章，
/// 这一趟判据就不成立了（不是 bug），重试比 sleep 满 120s 赌一次可靠。
async fn heal_once(c: &DefaultMQPushConsumer, key: &str) -> bool {
    let injected = now_ms() - PULL_MAX_IDLE_TIME - 5_000;
    for _ in 0..20 {
        c.set_last_pull_at(key, injected);
        if !c.pull_stalled(key) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        // 撤走 + 重建都在这一次同步里完成，重建时会立刻把章盖成"现在"。
        c.sync_pull_threads().await;
        if c.last_pull_at(key).is_some_and(|t| t > injected + 60_000) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

async fn c11_pull_stall_self_heal(ck: &mut Checker, fx: &Fixture) {
    println!("-- C11 拉取循环停摆 → 同一趟 rebalance 撤掉重建，消息不重不丢");
    let topic = fx.topic_name("Heal");
    let group = fx.group_name("heal");
    if let Err(e) = fx.create_topic(&topic, 1).await {
        return ck.abort("C11 create topic", &e);
    }
    let inbox = Arc::new(Inbox::default());
    let c = match fx.consumer(&group, &topic, "*", LiveListener::collecting(inbox.clone())) {
        Ok(v) => v,
        Err(e) => return ck.abort("C11 build consumer", &e),
    };
    if let Err(e) = c.start().await {
        return ck.abort("C11 start", &format!("{e}"));
    }
    // 1 队列：循环只有一条，注入点唯一，位点判据也唯一（多队列会被分摊到别的实例）
    let assigned = poll_until(|| main_queues(&c.assigned_queue_keys()).len() == 1, WAIT_SECONDS).await;
    let key = main_queues(&c.assigned_queue_keys())
        .first()
        .cloned()
        .unwrap_or_default();
    ck.check("C11 the single queue is assigned", assigned && !key.is_empty(), &key);
    if !assigned {
        c.shutdown();
        return ck.abort("C11 assign", "no queue was assigned to this consumer");
    }

    // ---- 基线：3 条被消费，位点提交到 3，运行信息里的拉取时钟是真值 ----
    let b1 = batch("heal-base", 3);
    let sent = send_bodies(fx, &topic, &b1).await;
    let got = poll_until(|| inbox.count() >= 3, WAIT_SECONDS).await;
    let committed = fx.wait_committed(&group, &topic, 1, 3).await;
    ck.check(
        "C11 baseline: 3 messages sent and consumed, offset committed to the broker",
        sent == 3 && got && committed == 3,
        &format!("sent={sent} arrivals={} committed={committed}", inbox.count()),
    );
    ck.check(
        "C11 consumerRunningInfo reports the real lastPullTimestamp (Java ProcessQueue#fillOutRunningInfo:456)",
        pull_clock_age_ms(&c.consumer_running_info())
            .is_some_and(|age| (0..60_000).contains(&age)),
        &format!("ageMs={:?}", pull_clock_age_ms(&c.consumer_running_info())),
    );

    // ---- 注入停摆 → 自愈（Java updateProcessQueueTableInRebalance 的 [BUG] 分支）----
    let healed = heal_once(&c, &key).await;
    let stalled_after = c.pull_stalled(&key);
    let fresh = c.last_pull_at(&key).unwrap_or_default();
    ck.check(
        "C11 a stalled queue is retired and rebuilt inside one rebalance (the stamp went back to now)",
        healed && !stalled_after,
        &format!("key={key} lastPullAt={fresh}"),
    );

    // ---- 自愈之后这把队列必须照常消费，而且不能把老消息重投 ----
    let b2 = batch("heal-post", 3);
    let sent2 = send_bodies(fx, &topic, &b2).await;
    let got2 = poll_until(|| inbox.count() >= 6, WAIT_SECONDS).await;
    let committed2 = fx.wait_committed(&group, &topic, 1, 6).await;
    let got_bodies: BTreeSet<String> = inbox.bodies();
    let want: BTreeSet<String> = b1.iter().chain(b2.iter()).cloned().collect();
    let retried: Vec<String> = inbox
        .snapshot()
        .iter()
        .filter(|d| d.reconsume_times != 0)
        .map(|d| d.body.clone())
        .collect();
    ck.check(
        "C11 the rebuilt loop keeps consuming the same queue (self-heal is not a no-op)",
        sent2 == 3 && got2 && committed2 == 6,
        &format!("arrivals={} committed={committed2}", inbox.count()),
    );
    ck.check(
        "C11 the retired queue's offset was persisted before the rebuild, so nothing is redelivered",
        got_bodies == want && retried.is_empty(),
        &format!(
            "distinct={} want={} redelivered={:?}",
            got_bodies.len(),
            want.len(),
            retried
        ),
    );
    // 留一个窗口给"新循环用陈旧游标回退位点"这类错误显形
    tokio::time::sleep(Duration::from_secs(6)).await;
    let committed3 = fx.committed_total(&group, &topic, 1).await;
    let dup = inbox.count();
    ck.check(
        "C11 the offset never rolls back after the heal (no duplicate burst shows up later)",
        committed3 >= 6 && dup == 6,
        &format!("committed={committed3} arrivals={dup}"),
    );
    c.shutdown();
}

// ---------------------------------------------------------------- C12 顺序消费毒消息

/// 顺序消费的毒消息终态：为什么只能真机验。
///
/// Java `ConsumeMessageOrderlyService#processConsumeResult:236-307` 的 SUSPEND 分支先过
/// `checkReconsumeTimes:322-339`：次数没用尽就**本地** `reconsumeTimes + 1` 并原地挂起
/// （broker 那边压根没记这次失败）；用尽了才 `sendMessageBack:341-362` 把整条投给
/// `%RETRY%<group>`，**投成功就不再挂起**、commit 位点让路（所以下一条必须被消费）。
/// 而「投给 `%RETRY%` 之后进不进 `%DLQ%`」全在 broker：`SendMessageProcessor#handleRetryAndDLQ:185-234`
/// 读 SEND_MESSAGE_V2 的 `j`/`l`（`AbstractSendMessageProcessor:427` 把 `j` 直接写成存储消息的
/// reconsumeTimes），且只有本组的 rebalance 锁还没过期（`:202-207` isLockAllExpired=false，
/// 也就是这个实例真的握着 `LOCK_BATCH_MQ`）才「立刻改投死信」。
///
/// 三种写错在客户端本地都表现为「看起来正常」：少 +1 ⇒ 毒消息原地转到天荒地老且永远不进死信；
/// 把 `-1` 读成并发侧的 16 ⇒ 顺序消费凭空多出死信；回投成功后仍挂起 ⇒ 队列永久卡死，
/// 与消费者进程死掉一模一样。离线单测只能锁「回投失败」那一半（未 `start()` 的内部生产者必败），
/// 「回投成功 ⇒ 位点前进、`%DLQ%` 出现、锁真的握着」只有真 broker 说得了。
async fn c12_orderly_dlq(ck: &mut Checker, fx: &Fixture) {
    println!("-- C12 顺序消费毒消息：本地计数 → 交 broker → %DLQ%<group>");
    const MAX_RECONSUME: i32 = 2;
    let topic = fx.topic_name("OrdDlq");
    let group = fx.group_name("orddlq");
    if let Err(e) = fx.create_topic(&topic, 1).await {
        return ck.abort("C12 create topic", &e);
    }
    // 1 队列 + 每批 1 条：毒消息后面那条必须排在它后面，且挂起不会连带别的路径
    let poison = format!("{topic}-000");
    let inbox = Arc::new(Inbox::default());
    let mut cfg = fx.base_config(&group);
    cfg.max_reconsume_times = MAX_RECONSUME;
    cfg.consume_message_batch_max_size = 1;
    cfg.suspend_current_queue_time_millis = 500;
    let c = match DefaultMQPushConsumer::with_config(cfg) {
        Ok(v) => v,
        Err(e) => return ck.abort("C12 build consumer", &format!("{e}")),
    };
    if let Err(e) = c.subscribe(&topic, "*") {
        return ck.abort("C12 subscribe", &format!("{e}"));
    }
    c.set_message_listener_orderly(LiveListener::orderly_suspending_always(
        inbox.clone(),
        &poison,
    ));
    if let Err(e) = c.start().await {
        return ck.abort("C12 start", &format!("{e}"));
    }
    fx.produce(&topic, "TagA", 2, Some(0)).await;

    let three = poll_until(|| inbox.matching(&poison).len() >= 3, 60).await;
    let times: Vec<i32> = inbox.matching(&poison).iter().map(|d| d.reconsume_times).collect();
    ck.check(
        "C12 毒消息恰好投 3 次（每次由客户端自己 +1：reconsumeTimes 0/1/2）",
        three && times.starts_with(&[0, 1, 2]),
        &format!("times={times:?}"),
    );
    // 交棒判据：回投成功后 Java commit 位点，队列必须往前走。少了这一步就是「毒消息把
    // 整个队列钉住」，与消费者死掉无法区分；多了（回投还没成功就前进）则是静默丢消息。
    let after = format!("{topic}-001");
    let handed = poll_until(|| !inbox.matching(&after).is_empty(), 60).await;
    ck.check(
        "C12 交给 broker 后业务队列继续前进（后一条被消费）",
        handed,
        &format!("after={after} arrivals={}", inbox.matching(&after).len()),
    );
    tokio::time::sleep(Duration::from_secs(15)).await;
    let seen = inbox.matching(&poison);
    ck.check(
        "C12 用尽后不再原地挂起（观察窗口内毒消息只投了 3 次）",
        seen.len() == 3,
        &format!("arrivals={}", seen.len()),
    );
    ck.check(
        "C12 挂起期间 listener 始终看到业务 topic（本地重投不换 topic）",
        seen.iter().all(|d| d.topic == topic),
        &format!("topics={:?}", seen.iter().map(|d| d.topic.clone()).collect::<Vec<_>>()),
    );
    c.shutdown();

    let dlq_topic = MixAll::get_dlq_topic(&group);
    let (has_route, dlq_msgs) = match read_dlq(fx, &group, "orddlqread", 40).await {
        Ok(v) => v,
        Err(e) => return ck.abort("C12 读 %DLQ%", &e),
    };
    ck.check(
        "C12 broker 自动创建并注册了 %DLQ%<group> 的路由",
        has_route,
        &format!("dlq={dlq_topic}"),
    );
    // 顺序回投落进死信而不是退回 `%RETRY%` 重投，本身就是 broker 认定「本组 rebalance 锁
    // 还没过期」⇒ 这个实例真的握着 LOCK_BATCH_MQ（handleRetryAndDLQ:202-207）。
    if has_route {
        ck.check(
            "C12 毒消息落在 %DLQ%<group>（回投走 rebalance 锁，立刻进死信）",
            dlq_msgs.len() == 1 && dlq_msgs[0].body == poison,
            &format!("{dlq_msgs:?}"),
        );
    }
    if let Some(d) = dlq_msgs.first() {
        // 3 = 客户端在 RECONSUME_TIME 上写的 +1，经 V2 头 j 落成存储值；漏填 j 的话
        // broker 按订阅组默认 16 判，这条永远进不了死信。
        ck.check(
            "C12 死信 reconsumeTimes = maxReconsumeTimes + 1",
            d.reconsume_times == MAX_RECONSUME + 1,
            &format!("reconsumeTimes={}", d.reconsume_times),
        );
        ck.check(
            "C12 死信保留 RETRY_TOPIC=业务 topic，且 topic 已是 %DLQ%<group>",
            d.retry_topic_prop.as_deref() == Some(topic.as_str()) && d.topic == dlq_topic,
            &format!("topic={} retryTopic={:?}", d.topic, d.retry_topic_prop),
        );
    }
    // 交给 C10 一起删掉，别在 broker 上留 %DLQ%/%RETRY% 垃圾
    lock(&fx.topics).push(dlq_topic.clone());
    lock(&fx.topics).push(MixAll::get_retry_topic(&group));
}

/// 顺序侧的 `-1` 是**不设上限**，不是并发侧的 16。
///
/// Java 两处 `getMaxReconsumeTimes` 故意不同：`ConsumeMessageOrderlyService:313-320` 把 `-1`
/// 读成 `Integer.MAX_VALUE`（顺序消费一直在本地原地重试，broker 侧没有计数，默认就该重试到
/// 成功为止）；`DefaultMQPushConsumerImpl:890` 把 `-1` 读成 16（那边每轮都过一遍 broker，
/// 16 是 broker 默认的 retryMaxTimes）。合成一个常量的两种坏法都得分别挡住：顺序侧读成 16
/// ⇒ 第 17 次投给 broker，而锁还没过期 ⇒ 直接造出一条死信；并发侧读成 MAX ⇒ 毒消息永远不进
/// `%DLQ%`（C4b 用 `maxReconsumeTimes=2` 覆盖了「阈值生效」，这条覆盖「默认值绝不生效」）。
async fn c12b_orderly_no_cap(ck: &mut Checker, fx: &Fixture) {
    println!("-- C12b maxReconsumeTimes=-1 时顺序消费不设上限（不是并发侧的 16）");
    let topic = fx.topic_name("OrdNoCap");
    let group = fx.group_name("ordnocap");
    if let Err(e) = fx.create_topic(&topic, 1).await {
        return ck.abort("C12b create topic", &e);
    }
    let poison = format!("{topic}-000");
    let inbox = Arc::new(Inbox::default());
    let mut cfg = fx.base_config(&group);
    // 显式不设上限（base_config 的默认就是 -1，写出来是为了让「默认」这条断言有出处）
    cfg.max_reconsume_times = -1;
    cfg.consume_message_batch_max_size = 1;
    cfg.suspend_current_queue_time_millis = 200;
    let c = match DefaultMQPushConsumer::with_config(cfg) {
        Ok(v) => v,
        Err(e) => return ck.abort("C12b build consumer", &format!("{e}")),
    };
    if let Err(e) = c.subscribe(&topic, "*") {
        return ck.abort("C12b subscribe", &format!("{e}"));
    }
    c.set_message_listener_orderly(LiveListener::orderly_suspending_always(
        inbox.clone(),
        &poison,
    ));
    if let Err(e) = c.start().await {
        return ck.abort("C12b start", &format!("{e}"));
    }
    fx.produce(&topic, "TagA", 1, Some(0)).await;
    // 只要越过并发侧的 16 就能证明没用错常量。窗口给到 90s：走错的话第 17 次的延迟档位
    // 是 level20（2h），一旦投出去就再也回不来，只能靠「本地投了多少次 + %DLQ% 空」两头夹住。
    let uncapped = poll_until(|| inbox.matching(&poison).len() >= 18, 90).await;
    let times: Vec<i32> = inbox.matching(&poison).iter().map(|d| d.reconsume_times).collect();
    let max_times = times.iter().copied().max().unwrap_or(-1);
    ck.check(
        "C12b -1 时顺序消费不设上限（投过 >=18 次，16 不生效）",
        uncapped && max_times >= 17,
        &format!("arrivals={} maxTimes={max_times}", times.len()),
    );
    c.shutdown();
    let (has_route, dlq_msgs) = match read_dlq(fx, &group, "ordnocapread", 20).await {
        Ok(v) => v,
        Err(e) => return ck.abort("C12b 读 %DLQ%", &e),
    };
    ck.check(
        "C12b 没到阈值就不该有死信（broker 侧连 %DLQ% topic 都不必建）",
        dlq_msgs.is_empty(),
        &format!("routeFound={has_route} n={}", dlq_msgs.len()),
    );
    if has_route {
        lock(&fx.topics).push(MixAll::get_dlq_topic(&group));
    }
}

/// 观测顺序挂起时长的 listener：命中 `poison` 时把 context 的挂起时长设成 `asked_ms`，
/// 并记下每次投递的时刻（相邻两次的间隔 = 挂起时长 + 分发循环的 50ms 节拍）。
struct SuspendTimingListener {
    poison: String,
    asked_ms: i64,
    times: Mutex<Vec<Instant>>,
}

impl SuspendTimingListener {
    fn new(poison: &str, asked_ms: i64) -> Arc<SuspendTimingListener> {
        Arc::new(SuspendTimingListener {
            poison: poison.to_string(),
            asked_ms,
            times: Mutex::new(Vec::new()),
        })
    }

    fn gaps(&self) -> Vec<f64> {
        let times = lock(&self.times);
        times
            .windows(2)
            .map(|w| (w[1] - w[0]).as_secs_f64())
            .collect()
    }
}

impl MessageListenerOrderly for SuspendTimingListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        context: &mut ConsumeOrderlyContext,
    ) -> ConsumeOrderlyStatus {
        if msgs.iter().any(|m| is_body(m, &self.poison)) {
            context.suspend_current_queue_time_millis = self.asked_ms;
            lock(&self.times).push(Instant::now());
            return ConsumeOrderlyStatus::SuspendCurrentQueueAMoment;
        }
        ConsumeOrderlyStatus::Success
    }
}

fn median(xs: &[f64]) -> f64 {
    let mut s = xs.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    match s.len() {
        0 => 0.0,
        n if n % 2 == 1 => s[n / 2],
        n => (s[n / 2 - 1] + s[n / 2]) / 2.0,
    }
}

/// #74：`suspendCurrentQueueTimeMillis` 在 context 上优先于消费者配置。
///
/// Java `ConsumeMessageOrderlyService#submitConsumeRequestLater:211-234`：`-1`（默认）→ 回落
/// 消费者配置，再钳到 `[10, 30000]`。真机这一条证的是「listener 在 context 上设的值真的决定
/// 等待时长」：配置故意设 900ms、context 要 70ms ⇒ 相邻投递的中位间隔必须贴着 70ms ——
/// 把 context 读丢或读成配置，间隔会落到 0.9s 以上。
///
/// 钳位的两个端点用同一条链路的间隔只能兜「不忙等」（`>= 10ms`）：分发循环本身有 50ms 固定
/// 节拍，间隔法分不出 10ms 与 1ms —— 精确值由 `orderly_suspend_millis` 的离线矩阵锁死
/// （Python 侧另有拦 `time.sleep` 的真机判据可以直接读到请求值）。
async fn c12c_orderly_suspend_millis(ck: &mut Checker, fx: &Fixture) {
    println!("-- C12c context 上的挂起时长优先于消费者配置（Java submitConsumeRequestLater）");
    let topic = fx.topic_name("OrdSuspend");
    let group = fx.group_name("ordsusp");
    if let Err(e) = fx.create_topic(&topic, 1).await {
        return ck.abort("C12c create topic", &e);
    }
    let poison = format!("{topic}-000");
    let listener = SuspendTimingListener::new(&poison, 70);
    let mut cfg = fx.base_config(&group);
    cfg.consume_message_batch_max_size = 1;
    cfg.max_reconsume_times = -1;
    cfg.suspend_current_queue_time_millis = 900;
    let c = match DefaultMQPushConsumer::with_config(cfg) {
        Ok(v) => v,
        Err(e) => return ck.abort("C12c build consumer", &format!("{e}")),
    };
    if let Err(e) = c.subscribe(&topic, "*") {
        return ck.abort("C12c subscribe", &format!("{e}"));
    }
    c.set_message_listener_orderly(listener.clone());
    if let Err(e) = c.start().await {
        return ck.abort("C12c start", &format!("{e}"));
    }
    // 等首轮 LOCK_BATCH_MQ：没有队列锁时 broker 会把顺序重投直接改投死信，间隔就没意义了
    tokio::time::sleep(Duration::from_secs(4)).await;
    fx.produce(&topic, "TagA", 1, Some(0)).await;
    let _ = poll_until(|| lock(&listener.times).len() >= 9, 25).await;
    let gaps = listener.gaps();
    let med = median(&gaps);
    ck.check(
        "C12c context 的 70ms 生效（不是消费者配置的 900ms）",
        gaps.len() >= 6 && (0.03..=0.4).contains(&med),
        &format!(
            "n={} median={med:.3}s gaps={:?}",
            gaps.len(),
            gaps.iter()
                .take(5)
                .map(|g| (g * 1000.0).round() / 1000.0)
                .collect::<Vec<_>>()
        ),
    );
    c.shutdown();

    // 钳位下限：context 1ms + 配置 0（两个非法值）都必须按 10ms 走，不能变成忙等
    let topic_b = fx.topic_name("OrdSuspendFloor");
    let group_b = fx.group_name("ordsuspfloor");
    if let Err(e) = fx.create_topic(&topic_b, 1).await {
        return ck.abort("C12c create floor topic", &e);
    }
    let poison_b = format!("{topic_b}-000");
    let listener_b = SuspendTimingListener::new(&poison_b, 1);
    let mut cfg_b = fx.base_config(&group_b);
    cfg_b.consume_message_batch_max_size = 1;
    cfg_b.max_reconsume_times = -1;
    cfg_b.suspend_current_queue_time_millis = 0;
    let cb = match DefaultMQPushConsumer::with_config(cfg_b) {
        Ok(v) => v,
        Err(e) => return ck.abort("C12c build floor consumer", &format!("{e}")),
    };
    if let Err(e) = cb.subscribe(&topic_b, "*") {
        return ck.abort("C12c subscribe floor", &format!("{e}"));
    }
    cb.set_message_listener_orderly(listener_b.clone());
    if let Err(e) = cb.start().await {
        return ck.abort("C12c start floor", &format!("{e}"));
    }
    tokio::time::sleep(Duration::from_secs(4)).await;
    fx.produce(&topic_b, "TagA", 1, Some(0)).await;
    let _ = poll_until(|| lock(&listener_b.times).len() >= 9, 20).await;
    let gaps_b = listener_b.gaps();
    ck.check(
        "C12c 钳位下限：context 1ms / 配置 0 时不忙等（间隔 ≥ 10ms）",
        gaps_b.len() >= 6 && median(&gaps_b) >= 0.009,
        &format!("n={} median={:.4}s", gaps_b.len(), median(&gaps_b)),
    );
    cb.shutdown();
}

// ---------------------------------------------------------------- C10 清理
async fn c10_cleanup(ck: &mut Checker, fx: &mut Fixture) {
    println!("-- C10 清理");
    let topics: Vec<String> = lock(&fx.topics).clone();
    let mut ok = 0usize;
    for topic in &topics {
        if fx
            .admin
            .delete_topic_in_broker(&fx.broker_addr, topic, 5000)
            .await
            .is_ok()
        {
            ok += 1;
        }
        let _ = fx.admin.delete_topic_in_namesrv(topic, 5000).await;
    }
    ck.check(
        "C10 every topic created by this run is deleted from the broker",
        ok == topics.len(),
        &format!("deleted {ok}/{}", topics.len()),
    );
    let dirs: Vec<PathBuf> = lock(&fx.offset_dirs).clone();
    let mut gone = 0usize;
    for dir in &dirs {
        if !dir.exists() {
            gone += 1;
            continue;
        }
        match std::fs::remove_dir_all(dir) {
            Ok(()) => gone += 1,
            Err(e) => println!("  [WARN] remove {} failed: {e}", dir.display()),
        }
    }
    ck.check(
        "C10 the local offset directories written by C6 are cleaned up",
        gone == dirs.len(),
        &format!("removed {gone}/{}", dirs.len()),
    );
    fx.producer.shutdown();
    fx.admin.shutdown();
    ck.check(
        "C10 the admin instance is shut down",
        !fx.admin.is_started(),
        "still started",
    );
}

// ------------------------------------------------------------------ driver

async fn run(namesrv: &str) -> Checker {
    let stamp_s = stamp();
    println!("== rust live_consumer ==");
    println!("   namesrv   = {namesrv}");
    println!("   stamp     = {stamp_s}");

    let mut fx = match Fixture::new(namesrv, &stamp_s) {
        Ok(f) => f,
        Err(e) => {
            let mut ck = Checker::new();
            ck.abort("C0 fixture build", &e);
            return ck;
        }
    };
    if let Err(e) = fx.start().await {
        let mut ck = Checker::new();
        ck.abort("C0 fixture start", &e);
        return ck;
    }
    println!("   broker    = {} @ {}", fx.broker_name, fx.broker_addr);
    println!("   producer  = {}", fx.producer_group);

    let mut ck = Checker::new();
    c1_lifecycle(&mut ck, &fx).await;
    c2_pull_consume_and_offset_persist(&mut ck, &fx).await;
    c3_tag_filter(&mut ck, &fx).await;
    c4_retry_and_topic_reset(&mut ck, &fx).await;
    c4b_dlq_terminal(&mut ck, &fx).await;
    c4c_partial_ack(&mut ck, &fx).await;
    c5_pop_mode(&mut ck, &fx).await;
    c5b_pop_pull_stats(&mut ck, &fx).await;
    c6_broadcasting_and_local_offsets(&mut ck, &fx).await;
    c7_orderly_and_lock(&mut ck, &fx).await;
    c8_scale_in_and_takeover(&mut ck, &fx).await;
    c9_admin_and_flow_control(&mut ck, &fx).await;
    c11_pull_stall_self_heal(&mut ck, &fx).await;
    c12_orderly_dlq(&mut ck, &fx).await;
    c12b_orderly_no_cap(&mut ck, &fx).await;
    c12c_orderly_suspend_millis(&mut ck, &fx).await;
    c10_cleanup(&mut ck, &mut fx).await;
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
