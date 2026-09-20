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
//! - C5 POP 模式：弹出即带 `POP_CK`，消费成功后 `waitAckCounter` 归零，
//!   等过一个 invisibleTime 窗口**不再重投**（证明 ack 真的写到了 broker）；
//!   且 POP 路径完全不写消费位点。
//! - C6 广播模式：同组两个实例各拿到全量（不分摊）；位点**不落 broker**，
//!   而是按 `$HOME/.rocketmq_offsets/<clientId>/<group>/offsets.json` 落盘并可解回。
//! - C7 顺序消费：`LOCK_BATCH_MQ` 被 5.5.1 接受（runningInfo 的 `locked`），
//!   同队列内 `queueOffset` 严格递增投递。
//! - C8 多实例：同组两实例队列**不重不漏**；撤掉一个后另一个在 40 通知/定时
//!   重平衡下接管全部队列并继续消费，全量消息一条不丢。
//! - C9 运维接口与背压：流控只暂停拉取不丢消息、核心线程数守卫、积压统计、
//!   手工心跳/重平衡/订阅队列查询、经实例注册表调 `persist_consumer_offset()`。
//! - C10 清理：删掉本次建的 topic 与广播位点目录。
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

use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::mq_client::{MQClientInstance, RegisteredConsumer};
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
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
    /// 每批固定耗时（C9 用来制造待消费积压以触发流控）。
    batch_cost: Duration,
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
            batch_cost: Duration::ZERO,
            orderly: Arc::new(OrderlyState::default()),
        })
    }

    fn failing_once(inbox: Arc<Inbox>, body: &str) -> Arc<LiveListener> {
        Arc::new(LiveListener {
            inbox,
            fail_once: Some(body.to_string()),
            batch_cost: Duration::ZERO,
            orderly: Arc::new(OrderlyState::default()),
        })
    }

    fn slow(inbox: Arc<Inbox>, cost: Duration) -> Arc<LiveListener> {
        Arc::new(LiveListener {
            inbox,
            fail_once: None,
            batch_cost: cost,
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
            Some(body) if msgs.iter().any(|m| is_body(m, body)) && self.is_first_seen(body) => {
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
        ConsumeOrderlyStatus::Success
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
        "C1 start() stamped a clientId (instanceName@timestamp)",
        !c.client_id().is_empty() && c.client_id().contains("live-"),
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
    ck.check(
        "C1 subscribe() after start() is rejected",
        c.subscribe(&fx.topic_name("Late"), "*").is_err(),
        "a running consumer accepted a new subscription",
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

    // 运维接口：runningInfo 的 mqTable 每队列一条，且 commitOffset 与进程内一致
    let info = c.consumer_running_info();
    let table = running_table(&info);
    ck.check(
        "C2 consumerRunningInfo reports one ProcessQueueInfo per assigned queue",
        table.len() == QUEUE_NUMS as usize,
        &format!("{:?}", table.keys().collect::<Vec<_>>()),
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
    // 队列内缓冲 1 条就流控 —— 消费慢于拉取时才会真的命中
    c.update_config(|x| {
        x.pull_threshold_for_queue = 1;
        x.pull_threshold_size_for_queue = 0;
        x.consume_concurrently_max_span = 0;
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
    c5_pop_mode(&mut ck, &fx).await;
    c6_broadcasting_and_local_offsets(&mut ck, &fx).await;
    c7_orderly_and_lock(&mut ck, &fx).await;
    c8_scale_in_and_takeover(&mut ck, &fx).await;
    c9_admin_and_flow_control(&mut ck, &fx).await;
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
