//! **带 ACL 凭据**的「全消息类型」真机验证：一类消息一条断言，每条都以「从 broker 读回来」收尾。
//!
//! 这个示例的存在理由：普通集群上「发出去 + 收得到」只能证明链路通，证明不了签名链路。
//! 开了鉴权（broker `authenticationEnabled=true`）之后，每一类消息的发送与消费都要靠
//! 请求上的 ACL 签名过 broker 那道闸，于是「SEND_OK」的含义变成了「broker 认了这份签名」。
//! 覆盖的类型（一条不落）：
//! - A0 反向对照：**不带凭据**的发送必须被拒成 NO_PERMISSION(16)。
//!   没有这条，「带凭据全绿」在鉴权其实没开起来的集群上和「签名被接受」长得一模一样，
//!   是一次彻头彻尾的假通过。断言写死成码 16，而不是「报了个错」：
//!   只有 16 才说明请求真的原样发到了 broker 并被鉴权闸拦下，而不是本地校验或连不上。
//! - A1 同步普通消息：SEND_OK 且按回调位点读回正文。
//! - A2 异步发送：回调拿到完整 SendResult，且回调报的 `(queue, offset)` 上就是这条。
//! - A3 单向（oneway）发送：无应答，只能靠读回条数兜底。
//! - A4 批量发送（SEND_BATCH_MESSAGE）：一次物理写入，子消息位点连续且都能读回。
//! - A5 延迟消息（`DELAY` 级别）：`store_timestamp - born_timestamp` 证明真的到点才投递。
//! - A6 定时消息（用户属性 `TIMER_DELIVER_MS` 指到 ~5s 后）：同一口径证明。
//! - A7 事务消息：半消息 → 本地事务 COMMIT → END_TRANSACTION，读回证明它转成了可消费。
//! - A8 顺序消息：selector 按同一个 key 钉死同一队列，顺序消费按位点保序收到。
//! - A9 Tag 过滤消费：服务端按订阅表达式过滤，只投 TagA。
//! - A10 Keys + 服务端按 key 查询（QUERY_MSG_BY_KEY）：查回那条带 key 的消息。
//! - A11 用户属性透传：生产侧 put 的自定义属性，读回与消费侧都原样拿到。
//! - A12 带签名的 push 消费者：心跳 / 长轮询拉取 / 位点提交三条 RPC 全程签名，
//!   任一漏签都收不到消息。
//!
//! 每个角色都用**自己的** `instance_name`（→ 独立 clientId → 独立 [`MQClientInstance`] →
//! 独立 remoting 通道）：ACL 钩子是**注册到通道上**的，两个角色若复用同一实例，
//! 「无凭据被拒」会被另一个角色的签名带过去 —— 要么变成假 PASS，要么让无凭据那条
//! 探针根本发不出去。所以 A0 一定跑在所有客户端**之前**，跑完立刻关停并等一拍。
//!
//! ⚠ A12 与 A5/A6 必须**先起消费者再发送**：push 消费者新消费组的初始位点会解析成
//! 该队列当时的 maxOffset，「先发再收」的结果并不稳定。这里显式用
//! `CONSUME_FROM_FIRST_OFFSET`，两道保险都留着。
//!
//! 用法（集群需按 ACL 配置起 nameServer + broker；不带后两个参数则退回普通集群口径，
//! 所有钩子都不装，行为与从前一致）：
//! ```text
//! cargo run --example live_acl_message_types -- 127.0.0.1:9876 [accessKey] [secretKey]
//! ```

use std::collections::{BTreeSet, HashSet};
use std::env;
use std::future::Future;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::hook::AnyHolder;
use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::{
    DefaultMQProducer, SelectMessageQueueByHash, SendCallback, TransactionListener,
};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, ConsumeOrderlyContext,
    ConsumeOrderlyStatus, LocalTransactionState, MessageListenerConcurrently,
    MessageListenerOrderly, PullResult, SendResult, SendStatus, TransactionSendResult,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::message_const::{
    PROPERTY_DELAY_TIME_LEVEL, PROPERTY_TIMER_DELIVER_MS, PROPERTY_TRANSACTION_PREPARED,
};
use rocketmq_client_remoting::common::sysflag::PullSysFlag;
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::protocol::heartbeat::{ConsumeFromWhere, ExpressionType};
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;
use rocketmq_client_remoting::remoting::rpchook::{AclClientRPCHook, RPCHook, SessionCredentials};

/// 每个 topic 建 4 个队列（A8 的「同 key 同队列」与读回快照都依赖它）。
const QUEUE_NUMS: i32 = 4;
/// broker 侧所有鉴权失败都收敛成 NO_PERMISSION(16)。
const NO_PERMISSION: i32 = 16;
/// 默认延迟级别表 `1s 5s 10s 30s …` 里的第 3 级。
const DELAY_LEVEL: i32 = 3;
/// 第 3 级对应的投递延时。
const DELAY_MILLIS: i64 = 10_000;
/// 定时消息往后指的毫秒数。
const TIMER_AHEAD_MILLIS: i64 = 5_000;
/// 同机时间戳，只给极小的余量：判定「真的延迟了」不能被这个余量放水。
const CLOCK_SLACK: i64 = 500;
/// 读回快照用的消费组：只做 pull RPC，不建消费者。
const PULL_GROUP: &str = "CID_rust_acl_message_types";
/// 等一条消息从 broker 读回来的预算。
const READ_BACK_BUDGET: Duration = Duration::from_secs(30);
/// 延迟/定时消息额外等的预算。
const DELIVER_BUDGET: Duration = Duration::from_secs(60);
/// 先起消费者后，等首轮重平衡把队列分下去。
const REBALANCE_WAIT: Duration = Duration::from_secs(5);
/// 无凭据探针关停后等一拍，确保它的实例彻底释放。
const PROBE_RELEASE_WAIT: Duration = Duration::from_secs(1);

// ------------------------------------------------------------------ 骨架

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs().to_string(),
        Err(_) => "0".to_string(),
    }
}

fn now_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(_) => 0,
    }
}

/// 断言累积器：跑完所有类型再汇总，首个失败不提前退出。
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

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// 轮询异步条件直到成立，超时返回 `false` 让调用方打出现场。
async fn wait_until<F, Fut>(mut cond: F, budget: Duration) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + budget;
    loop {
        if cond().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

// ------------------------------------------------------------------ 凭据

/// 可选的 ACL 凭据：两个都非空才算「带凭据」。
///
/// 空的时候 [`Self::hook`] 恒回 `None`，而各客户端拿到 `None` 就是不装钩子 ——
/// 与普通集群上的行为逐字节一致。
#[derive(Clone)]
struct Creds {
    access_key: String,
    secret_key: String,
}

impl Creds {
    fn from_args(access_key: &str, secret_key: &str) -> Creds {
        Creds {
            access_key: access_key.trim().to_string(),
            secret_key: secret_key.trim().to_string(),
        }
    }

    fn enabled(&self) -> bool {
        !self.access_key.is_empty() && !self.secret_key.is_empty()
    }

    fn hook(&self) -> Option<Arc<dyn RPCHook>> {
        if self.enabled() {
            Some(acl_hook(&self.access_key, &self.secret_key))
        } else {
            None
        }
    }
}

fn acl_hook(access_key: &str, secret_key: &str) -> Arc<dyn RPCHook> {
    Arc::new(AclClientRPCHook::new(SessionCredentials::new(
        access_key,
        secret_key,
    )))
}

/// 拒绝类断言：必须是 broker 回的 NO_PERMISSION，不能是连不上 / 超时 / 本地校验失败。
fn denied(err: &Error) -> bool {
    err.response_code() == Some(NO_PERMISSION)
}

fn code_of(err: &Error) -> String {
    match err.response_code() {
        Some(code) => format!("code={code}"),
        None => "code=?".to_string(),
    }
}

// ------------------------------------------------------------------ 消息工具

fn msg(topic: &str, body: &[u8], tags: &str, keys: &str) -> Message {
    let mut m = Message::new(topic, Some(body));
    if !tags.is_empty() {
        m.set_tags(tags);
    }
    if !keys.is_empty() {
        m.set_keys(keys);
    }
    m
}

fn body_of(m: &MessageExt) -> String {
    String::from_utf8_lossy(m.get_body()).into_owned()
}

fn bodies(msgs: &[MessageExt]) -> Vec<String> {
    msgs.iter().map(body_of).collect()
}

/// 消费侧记下的一条投递：正文 + tag/key/自定义属性 + 位点 + 两个时间戳。
#[derive(Clone, Debug)]
struct Seen {
    body: String,
    tags: String,
    keys: String,
    city: String,
    env: String,
    queue_id: i32,
    queue_offset: i64,
    born_timestamp: i64,
    store_timestamp: i64,
}

impl Seen {
    fn of(m: &MessageExt) -> Seen {
        Seen {
            body: body_of(m),
            tags: m.get_tags().unwrap_or_default().to_string(),
            keys: m.get_keys().unwrap_or_default().to_string(),
            city: m.get_property("city").unwrap_or_default().to_string(),
            env: m.get_property("env").unwrap_or_default().to_string(),
            queue_id: m.queue_id,
            queue_offset: m.queue_offset,
            born_timestamp: m.born_timestamp,
            store_timestamp: m.store_timestamp,
        }
    }
}

/// 一个 listener 的收件箱：并发与顺序两种投递都往这儿记。
#[derive(Default)]
struct Inbox {
    seen: Mutex<Vec<Seen>>,
    batches: AtomicUsize,
}

impl Inbox {
    fn snapshot(&self) -> Vec<Seen> {
        lock(&self.seen).clone()
    }

    fn bodies(&self) -> BTreeSet<String> {
        self.snapshot().iter().map(|s| s.body.clone()).collect()
    }

    fn find(&self, body: &str) -> Option<Seen> {
        self.snapshot().into_iter().find(|s| s.body == body)
    }

    fn count_of(&self, body: &str) -> usize {
        self.snapshot().iter().filter(|s| s.body == body).count()
    }
}

/// 正文照单全收的 listener（两种消费模式共用一份实现）。
struct Recorder(Arc<Inbox>);

impl MessageListenerConcurrently for Recorder {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        self.record(msgs);
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

impl MessageListenerOrderly for Recorder {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeOrderlyContext,
    ) -> ConsumeOrderlyStatus {
        self.record(msgs);
        ConsumeOrderlyStatus::Success
    }
}

impl Recorder {
    fn record(&self, msgs: &[MessageExt]) {
        let mut sink = lock(&self.0.seen);
        for m in msgs {
            sink.push(Seen::of(m));
        }
        drop(sink);
        self.0.batches.fetch_add(1, Ordering::SeqCst);
    }
}

/// 异步发送的观测点：要么成功要么失败，终态只来一次。
#[derive(Default)]
struct AsyncProbe {
    done: AtomicUsize,
    ok: AtomicUsize,
    result: Mutex<Option<SendResult>>,
    errors: Mutex<Vec<String>>,
}

impl AsyncProbe {
    fn done(&self) -> usize {
        self.done.load(Ordering::SeqCst)
    }

    fn result(&self) -> Option<SendResult> {
        lock(&self.result).clone()
    }

    fn summary(&self) -> String {
        let errors = lock(&self.errors).clone();
        format!(
            "done={} ok={} err={:?} result={:?}",
            self.done(),
            self.ok.load(Ordering::SeqCst),
            errors.first(),
            self.result()
        )
    }
}

impl SendCallback for AsyncProbe {
    fn on_success(&self, result: SendResult) {
        if result.status == SendStatus::SendOk {
            self.ok.fetch_add(1, Ordering::SeqCst);
        }
        *lock(&self.result) = Some(result);
        self.done.fetch_add(1, Ordering::SeqCst);
    }

    fn on_exception(&self, err: Error) {
        lock(&self.errors).push(format!("{} {err}", code_of(&err)));
        self.done.fetch_add(1, Ordering::SeqCst);
    }
}

/// 本地事务一律提交，回查也一律提交（正常路径不会走到回查）。
#[derive(Default)]
struct CommitAlways;

impl TransactionListener for CommitAlways {
    fn execute_local_transaction(&self, _msg: &Message, _arg: Option<&AnyHolder>) -> LocalTransactionState {
        LocalTransactionState::CommitMessage
    }

    fn check_local_transaction(&self, _msg: &MessageExt) -> LocalTransactionState {
        LocalTransactionState::CommitMessage
    }
}

// ------------------------------------------------------------------ 读回

/// 集群定位结果：一台 broker 的名字与地址。
struct Cluster {
    broker_name: String,
    broker_addr: String,
}

fn cluster_of(route: &TopicRouteData) -> Result<Cluster, String> {
    let bd = route
        .broker_datas
        .first()
        .ok_or_else(|| "route has no brokerData".to_string())?;
    let addr = bd
        .select_broker_addr()
        .ok_or_else(|| format!("broker {} has no address", bd.broker_name))?;
    Ok(Cluster {
        broker_name: bd.broker_name.clone(),
        broker_addr: addr,
    })
}

/// 一条 `*` 表达式、不挂起的拉取：读回只用来问 broker 要存量，不动位点。
async fn pull(
    instance: &MQClientInstance,
    mq: &MessageQueue,
    from_offset: i64,
    broker_addr: &str,
) -> Result<PullResult, String> {
    instance
        .pull_message(
            PULL_GROUP,
            mq,
            from_offset,
            128,
            PullSysFlag::build_sys_flag_basic(false, false, true, false),
            0,
            "*",
            0,
            ExpressionType::TAG,
            30_000,
            -1,
            0,
            Some(broker_addr),
            0,
            None,
        )
        .await
        .map_err(|e| format!("pull {} queue {} failed: {e}", mq.topic, mq.queue_id))
}

/// 该 topic 全部队列的一次快照（从位点 0 起）。
async fn snapshot(instance: &MQClientInstance, topic: &str, cluster: &Cluster) -> Vec<MessageExt> {
    let mut out = Vec::new();
    for q in 0..QUEUE_NUMS {
        let mq = MessageQueue::new(topic, &cluster.broker_name, q);
        // 某个队列拉不动不影响全局快照：这里要的是「读到了什么」。
        if let Ok(r) = pull(instance, &mq, 0, &cluster.broker_addr).await {
            out.extend(r.msg_found_list);
        }
    }
    out
}

/// 等到某 topic 的快照里出现指定正文，返回其中一份读到的消息。
async fn wait_bodies(
    instance: &MQClientInstance,
    topic: &str,
    cluster: &Cluster,
    want: &[&str],
    budget: Duration,
) -> Vec<MessageExt> {
    let mut found: Vec<MessageExt>;
    let deadline = Instant::now() + budget;
    loop {
        let got = snapshot(instance, topic, cluster).await;
        found = want
            .iter()
            .filter_map(|w| got.iter().find(|m| body_of(m) == *w).cloned())
            .collect();
        if found.len() == want.len() || Instant::now() >= deadline {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// 按 `(队列, 位点)` 精确读回那一条（异步回调的位点断言用）。
async fn body_at(
    instance: &MQClientInstance,
    mq: &MessageQueue,
    offset: i64,
    broker_addr: &str,
    budget: Duration,
) -> Option<Vec<u8>> {
    let deadline = Instant::now() + budget;
    loop {
        if let Ok(pulled) = pull(instance, mq, offset, broker_addr).await {
            if let Some(found) = pulled
                .msg_found_list
                .iter()
                .find(|m| m.queue_offset == offset)
            {
                return Some(found.body.clone().unwrap_or_default());
            }
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// 「到点才投递」的量化口径：broker 落库时间减去客户端生成时间。
fn drift_ms(m: &MessageExt) -> i64 {
    m.store_timestamp - m.born_timestamp
}

// ------------------------------------------------------------------ A0 反向对照

/// 不带凭据的探针必须是**第一个**跑起来的客户端。
///
/// 这个端口按 clientId 在进程内共享 [`MQClientInstance`]，钩子注册在实例的 remoting
/// 通道上：探针若与后面的签名客户端同名，就会被那条通道顺手签掉，「被拒」当场消失。
/// 所以这里给它独立的 instanceName，跑完立即关停并等一拍。
async fn a0_unsigned_probe(namesrv: &str, probe_topic: &str, run: &str, ck: &mut Checker) {
    println!("\nA0 不带凭据的发送必须被 broker 拒绝（NO_PERMISSION=16）");
    let group = format!("PID_rust_acl_types_anon_{run}");
    let anon = match DefaultMQProducer::new(&group) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("A0 构造匿名生产者", &e.to_string());
            return;
        }
    };
    anon.set_namesrv_addr(namesrv);
    anon.set_instance_name(&format!("{run}-anon"));
    if let Err(e) = anon.start().await {
        ck.abort("A0 匿名生产者 start", &format!("{} {e}", code_of(&e)));
        return;
    }
    let mut m = Message::new(probe_topic, Some(b"should-not-be-accepted"));
    m.set_tags("TagAnon");
    match anon.send(&mut m, Some(5000), None).await {
        Ok(r) => ck.check(
            "A0 无凭据发送被 broker 拒绝",
            false,
            &format!("broker 竟然收了匿名请求：{r:?}（多半是 authenticationEnabled 没开）"),
        ),
        Err(e) => {
            let remark = e.to_string();
            ck.check(
                "A0 无凭据发送被 broker 拒绝",
                denied(&e),
                &format!("{} remark={remark}", code_of(&e)),
            );
            // 探针真的没带凭据，才会拿到「username cannot be null.」这一类理由；
            // 打印出来便于区分「签名算错」和「压根没签」。
            println!("  [INFO] A0 拒绝理由：{remark}");
        }
    }
    anon.shutdown();
    drop(anon);
    tokio::time::sleep(PROBE_RELEASE_WAIT).await;
}

// ------------------------------------------------------------------ 主流程

/// 起一个带签名的 push 消费者（未订阅）：每个角色自己的 instanceName。
#[allow(clippy::too_many_arguments)]
fn push_consumer(
    namesrv: &str,
    creds: &Creds,
    group: &str,
    instance_tag: &str,
    first_offset: bool,
) -> Result<DefaultMQPushConsumer, String> {
    let mut cfg = ConsumerConfig {
        consumer_group: group.to_string(),
        name_server_addrs: vec![namesrv.to_string()],
        instance_name: instance_tag.to_string(),
        ..Default::default()
    };
    if first_offset {
        cfg.consume_from_where = ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string();
    }
    let consumer =
        DefaultMQPushConsumer::with_config(cfg).map_err(|e| format!("build {group} failed: {e}"))?;
    consumer.set_rpc_hook(creds.hook());
    Ok(consumer)
}

async fn run(namesrv: &str, creds: Creds) -> Checker {
    let mut ck = Checker::new();
    let run = stamp();
    let topic = format!("RustAclTypes{run}");
    let tag_topic = format!("RustAclTypesTag{run}");
    let order_topic = format!("RustAclTypesOrder{run}");
    let anon_topic = format!("RustAclTypesAnon{run}");
    let group = format!("rust-acl-types-{run}");
    let producer_group = format!("PID_rust_acl_types_{run}");

    println!("== rocketmq rust live ACL message-type coverage ==");
    println!("   namesrv = {namesrv}");
    println!(
        "   acl     = {}",
        if creds.enabled() {
            format!("ak={}", creds.access_key)
        } else {
            "未带凭据（普通集群口径，不装钩子）".to_string()
        }
    );
    println!("   topic   = {topic} / {tag_topic} / {order_topic}");

    // A0 反向对照：必须抢在任何客户端之前跑。
    if creds.enabled() {
        a0_unsigned_probe(namesrv, &anon_topic, &run, &mut ck).await;
    }

    // ---- 带凭据的生产者：建 topic、发送、读回、按 key 查询都走它
    let p = match DefaultMQProducer::with_rpc_hook(&producer_group, creds.hook()) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("生产者构造", &e.to_string());
            return ck;
        }
    };
    p.set_namesrv_addr(namesrv);
    p.set_instance_name(&format!("{run}-p"));
    if let Err(e) = p.start().await {
        ck.abort("生产者 start（签名注册）", &format!("{} {e}", code_of(&e)));
        return ck;
    }
    let instance = match p.client() {
        Some(c) => c,
        None => {
            ck.abort("生产者实例", "started producer has no instance");
            return ck;
        }
    };

    for t in [&topic, &tag_topic, &order_topic] {
        match p.create_topic(t, QUEUE_NUMS, 0).await {
            Ok(()) => ck.check(&format!("带凭据建 topic {t}"), true, ""),
            Err(e) => ck.check(
                &format!("带凭据建 topic {t}"),
                false,
                &format!("{} {e}", code_of(&e)),
            ),
        }
    }
    let cluster = match instance.get_topic_route_data(&topic).await {
        Some(route) => match cluster_of(&route) {
            Ok(c) => c,
            Err(e) => {
                ck.abort("路由解析", &e);
                p.shutdown();
                return ck;
            }
        },
        None => {
            ck.abort("路由", "namesrv 上没有这个 topic 的路由");
            p.shutdown();
            return ck;
        }
    };
    println!(
        "   broker  = {} @ {}",
        cluster.broker_name, cluster.broker_addr
    );

    // ---- 消费者先起：A12 并发消费主 topic、A9 只订阅 TagA、A8 顺序消费
    let main_inbox = Arc::new(Inbox::default());
    let tag_inbox = Arc::new(Inbox::default());
    let order_inbox = Arc::new(Inbox::default());
    let main_c = match push_consumer(
        namesrv,
        &creds,
        &format!("{group}-main"),
        &format!("{run}-c-main"),
        true,
    ) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("A12 消费者构造", &e);
            p.shutdown();
            return ck;
        }
    };
    let tag_c = match push_consumer(
        namesrv,
        &creds,
        &format!("{group}-tag"),
        &format!("{run}-c-tag"),
        true,
    ) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("A9 消费者构造", &e);
            main_c.shutdown();
            p.shutdown();
            return ck;
        }
    };
    let order_c = match push_consumer(
        namesrv,
        &creds,
        &format!("{group}-order"),
        &format!("{run}-c-order"),
        true,
    ) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("A8 消费者构造", &e);
            main_c.shutdown();
            tag_c.shutdown();
            p.shutdown();
            return ck;
        }
    };
    // 一个 listener 槽位只装一种：并发/顺序由注册口决定（后写的覆盖先写的）。
    // A8 必须走 orderly 那个口，否则保序断言测的就不是顺序消费内核。
    for (c, t, expr, inbox, orderly) in [
        (&main_c, topic.as_str(), "*", main_inbox.clone(), false),
        (&tag_c, tag_topic.as_str(), "TagA", tag_inbox.clone(), false),
        (
            &order_c,
            order_topic.as_str(),
            "*",
            order_inbox.clone(),
            true,
        ),
    ] {
        if let Err(e) = c.subscribe(t, expr) {
            ck.abort("subscribe", &format!("{t}: {e}"));
        }
        // 同一个 Recorder 同时实现两种 listener，按模式注册到各自的那个口。
        let recorder = Arc::new(Recorder(inbox));
        if orderly {
            c.set_message_listener_orderly(recorder);
        } else {
            c.set_message_listener_concurrently(recorder);
        }
    }
    let started = {
        let a = main_c.start().await;
        let b = tag_c.start().await;
        let c = order_c.start().await;
        a.map_err(|e| e.to_string())
            .and(b.map_err(|e| e.to_string()))
            .and(c.map_err(|e| e.to_string()))
    };
    if let Err(e) = started {
        ck.abort(
            "消费者 start（心跳/注册都要签名）",
            &e,
        );
        main_c.shutdown();
        tag_c.shutdown();
        order_c.shutdown();
        p.shutdown();
        return ck;
    }
    // 等首轮重平衡把队列分下来：初始位点必须在 topic 还空着的时候解析。
    tokio::time::sleep(REBALANCE_WAIT).await;

    // ---- A1 同步普通消息 + A11 用户属性 + A10 keys
    let key_sync = format!("aclKeySync-{run}");
    println!("\nA1/A10/A11 同步普通消息（带 keys 与用户属性）");
    let mut m1 = msg(&topic, b"acl-sync", "TagSync", &key_sync);
    m1.put_property("city", "Hangzhou");
    m1.put_property("env", "prod");
    let r1 = match p.send(&mut m1, Some(5000), None).await {
        Ok(r) => r,
        Err(e) => {
            ck.abort("A1 同步发送", &format!("{} {e}", code_of(&e)));
            finish(&main_c, &tag_c, &order_c, &p).await;
            return ck;
        }
    };
    let sync_mq = r1.message_queue.clone().unwrap_or_default();
    let back_sync = body_at(&instance, &sync_mq, r1.queue_offset, &cluster.broker_addr, READ_BACK_BUDGET).await;
    let sync_read = wait_bodies(&instance, &topic, &cluster, &["acl-sync"], READ_BACK_BUDGET).await;
    let sync_pulled = sync_read.first().cloned().unwrap_or_default();
    ck.check(
        "A1 同步普通消息 SEND_OK 并从 broker 读回同一条",
        r1.status == SendStatus::SendOk
            && back_sync.as_deref() == Some(b"acl-sync".as_slice())
            && body_of(&sync_pulled) == "acl-sync"
            && sync_pulled.get_tags() == Some("TagSync"),
        &format!("{r1:?} 读回={:?} 队列位点={}", back_sync, sync_mq),
    );
    ck.check(
        "A11 用户属性 city/env 在 broker 侧原样读到",
        sync_pulled.get_property("city") == Some("Hangzhou")
            && sync_pulled.get_property("env") == Some("prod"),
        &format!("{:?}", sync_pulled.properties),
    );

    // ---- A2 异步发送
    println!("A2 异步发送（回调）");
    let probe = Arc::new(AsyncProbe::default());
    if let Err(e) = p.send_async(msg(&topic, b"acl-async", "TagAsync", ""), probe.clone(), Some(5000), None) {
        ck.abort("A2 send_async 投递", &format!("{} {e}", code_of(&e)));
    }
    let called = wait_until(|| async { probe.done() >= 1 }, Duration::from_secs(15)).await;
    let async_result = probe.result();
    let async_ok = async_result
        .as_ref()
        .is_some_and(|r| r.status == SendStatus::SendOk);
    let back_async = match async_result.as_ref() {
        Some(r) => {
            let mq = r.message_queue.clone().unwrap_or_default();
            body_at(&instance, &mq, r.queue_offset, &cluster.broker_addr, READ_BACK_BUDGET).await
        }
        None => None,
    };
    ck.check(
        "A2 异步回调报的 (queue, offset) 上就是那条消息",
        called && async_ok && back_async.as_deref() == Some(b"acl-async".as_slice()),
        &probe.summary(),
    );

    // ---- A3 单向发送：没有应答，只能靠读回
    println!("A3 单向发送（oneway，无应答）");
    let oneway_mq = MessageQueue::new(&topic, &cluster.broker_name, 3);
    let mut m3 = msg(&topic, b"acl-oneway", "TagOneway", "");
    if let Err(e) = p.send_oneway(&mut m3, Some(&oneway_mq)).await {
        ck.abort("A3 oneway 发送", &format!("{} {e}", code_of(&e)));
    }
    let oneway_back =
        wait_bodies(&instance, &topic, &cluster, &["acl-oneway"], READ_BACK_BUDGET).await;
    let oneway_count = snapshot(&instance, &topic, &cluster)
        .await
        .iter()
        .filter(|m| body_of(m) == "acl-oneway")
        .count();
    ck.check(
        "A3 oneway 无应答但消息确实落到 broker（读回条数）",
        oneway_back.len() == 1 && oneway_count == 1,
        &format!("back={} count={oneway_count}", oneway_back.len()),
    );

    // ---- A4 批量发送
    println!("A4 批量发送（SEND_BATCH_MESSAGE）");
    let batch_bodies: Vec<String> = (0..3).map(|i| format!("acl-batch-{i}")).collect();
    let batch_msgs = batch_bodies
        .iter()
        .map(|b| msg(&topic, b.as_bytes(), "TagBatch", ""))
        .collect();
    let batch_mq = MessageQueue::new(&topic, &cluster.broker_name, 1);
    let batch_result = p
        .send_batch(batch_msgs, Some(&batch_mq), Some(5000))
        .await
        .map_err(|e| format!("{} {e}", code_of(&e)));
    let batch_want: Vec<&str> = batch_bodies.iter().map(|s| s.as_str()).collect();
    let back_batch = match batch_result.as_ref() {
        Ok(_) => wait_bodies(&instance, &topic, &cluster, &batch_want, READ_BACK_BUDGET).await,
        Err(e) => {
            ck.abort("A4 批量发送", e);
            Vec::new()
        }
    };
    let offsets: Vec<i64> = back_batch.iter().map(|m| m.queue_offset).collect();
    let mut sorted_offsets = offsets.clone();
    sorted_offsets.sort_unstable();
    let contiguous = sorted_offsets.len() == 3
        && sorted_offsets
            .windows(2)
            .all(|w| w[1] - w[0] == 1);
    ck.check(
        "A4 批量三条一次写入，子消息位点连续且都能读回",
        batch_result.is_ok() && back_batch.len() == 3 && contiguous,
        &format!("{batch_result:?} offsets={offsets:?}"),
    );

    // ---- A7 事务消息：半消息 → 本地事务 COMMIT → END_TRANSACTION
    println!("A7 事务消息（半消息 → COMMIT → END_TRANSACTION → 可消费）");
    let mut m7 = msg(&topic, b"acl-tx-commit", "TagTxn", "");
    let tx_result = p
        .send_message_in_transaction(&mut m7, Some(Arc::new(CommitAlways::default())), None)
        .await;
    let tx_prepared = m7
        .get_property(PROPERTY_TRANSACTION_PREPARED)
        .unwrap_or_default()
        .to_string();
    let tx_detail = match tx_result.as_ref() {
        Ok(r) => format!(
            "status={:?} state={:?} halfProp={tx_prepared}",
            r.inner.status, r.local_transaction_state
        ),
        Err(e) => format!("{} {e}", code_of(&e)),
    };
    let tx_send_ok = match tx_result.as_ref() {
        Ok(r) => {
            r.inner.status == SendStatus::SendOk
                && r.local_transaction_state == Some(LocalTransactionState::CommitMessage)
        }
        Err(_) => false,
    };
    // 读回正是「END_TRANSACTION 真的生效」的证据：半消息没转普通消息前 broker 不会投它。
    let back_tx = wait_bodies(&instance, &topic, &cluster, &["acl-tx-commit"], READ_BACK_BUDGET).await;
    ck.check(
        "A7 事务提交后消息转为可消费（读回证明 END_TRANSACTION 生效）",
        tx_send_ok && back_tx.len() == 1,
        &tx_detail,
    );
    let tx_is_half = tx_result
        .as_ref()
        .ok()
        .and_then(|r: &TransactionSendResult| r.inner.transaction_id.clone())
        .is_some_and(|id| !id.is_empty());
    ck.check(
        "A7 半消息阶段 broker 已给事务消息赋 transactionId",
        tx_is_half,
        &tx_detail,
    );

    // ---- A8 顺序消息：同一 key 必须钉在同一个队列
    println!("A8 顺序消息（selector 同 key 同队列 + 顺序消费保序）");
    let mut picked: Vec<i32> = Vec::new();
    let mut order_send_failed: Vec<String> = Vec::new();
    for i in 0..6 {
        let mut m = msg(&order_topic, format!("acl-order-{i}").as_bytes(), "TagOrder", "");
        match p
            .send_by_selector(&mut m, &SelectMessageQueueByHash, "acl-shard-key", Some(5000))
            .await
        {
            Ok(r) => picked.push(r.message_queue.clone().map(|q| q.queue_id).unwrap_or(-1)),
            Err(e) => order_send_failed.push(format!("{} {e}", code_of(&e))),
        }
    }
    let distinct: HashSet<i32> = picked.iter().copied().collect();
    ck.check(
        "A8 同一个 key 的 6 条顺序消息全部落到同一队列",
        order_send_failed.is_empty() && picked.len() == 6 && distinct.len() == 1,
        &format!("queues={picked:?} err={order_send_failed:?}"),
    );
    let order_bodies: Vec<String> = (0..6).map(|i| format!("acl-order-{i}")).collect();
    let order_want: Vec<&str> = order_bodies.iter().map(|s| s.as_str()).collect();
    let back_order = wait_bodies(&instance, &order_topic, &cluster, &order_want, READ_BACK_BUDGET).await;
    let mut by_offset = back_order
        .iter()
        .map(|m| (m.queue_offset, body_of(m)))
        .collect::<Vec<(i64, String)>>();
    by_offset.sort();
    let broker_order = by_offset
        .iter()
        .map(|(_, b)| b.clone())
        .collect::<Vec<String>>();
    let orderly_delivered = wait_until(
        || async { order_inbox.bodies().len() >= 6 },
        Duration::from_secs(30),
    )
    .await;
    let seen_order = order_inbox.snapshot();
    let seen_bodies: Vec<String> = seen_order.iter().map(|s| s.body.clone()).collect();
    let seen_offsets: Vec<i64> = seen_order.iter().map(|s| s.queue_offset).collect();
    let offsets_increasing = seen_offsets.windows(2).all(|w| w[1] > w[0]);
    let same_queue = seen_order
        .iter()
        .map(|s| s.queue_id)
        .collect::<HashSet<i32>>()
        .len()
        <= 1;
    ck.check(
        "A8 顺序消费按 broker 上的位点顺序收到这 6 条",
        orderly_delivered
            && broker_order.len() == 6
            && seen_bodies.len() == 6
            && same_queue
            && offsets_increasing
            && seen_bodies == broker_order,
        &format!(
            "broker={broker_order:?} 消费={seen_bodies:?} 位点={seen_offsets:?} 队列={:?}",
            seen_order.iter().map(|s| s.queue_id).collect::<Vec<i32>>()
        ),
    );

    // ---- A9 Tag 过滤：服务端只投 TagA
    println!("A9 Tag 过滤消费（服务端表达式过滤）");
    for i in 0..3 {
        let mut a = msg(&tag_topic, format!("acl-tagA-{i}").as_bytes(), "TagA", "");
        if let Err(e) = p.send(&mut a, Some(5000), None).await {
            ck.abort("A9 发 TagA", &format!("{} {e}", code_of(&e)));
        }
        let mut b = msg(&tag_topic, format!("acl-tagB-{i}").as_bytes(), "TagB", "");
        if let Err(e) = p.send(&mut b, Some(5000), None).await {
            ck.abort("A9 发 TagB", &format!("{} {e}", code_of(&e)));
        }
    }
    let tag_a: Vec<String> = (0..3).map(|i| format!("acl-tagA-{i}")).collect();
    let tag_b: Vec<String> = (0..3).map(|i| format!("acl-tagB-{i}")).collect();
    let tag_want_a: Vec<&str> = tag_a.iter().map(|s| s.as_str()).collect();
    let tag_want_b: Vec<&str> = tag_b.iter().map(|s| s.as_str()).collect();
    let back_tag_a = wait_bodies(&instance, &tag_topic, &cluster, &tag_want_a, READ_BACK_BUDGET).await;
    // TagB 也必须在 broker 上：否则「只收到 TagA」可能只是 TagB 压根没发出去。
    let back_tag_b = wait_bodies(&instance, &tag_topic, &cluster, &tag_want_b, READ_BACK_BUDGET).await;
    let filtered = wait_until(
        || async { tag_inbox.bodies().len() >= 3 },
        Duration::from_secs(20),
    )
    .await;
    let got_tag = tag_inbox.bodies();
    let leaked_tag_b = tag_b.iter().any(|b| got_tag.contains(b.as_str()));
    ck.check(
        "A9 订阅 TagA 的消费者只收到 TagA（服务端过滤，broker 上两类都在）",
        back_tag_a.len() == 3
            && back_tag_b.len() == 3
            && filtered
            && got_tag.len() == 3
            && !leaked_tag_b,
        &format!(
            "broker 上 TagA={} TagB={} 消费到={got_tag:?}",
            back_tag_a.len(),
            back_tag_b.len()
        ),
    );

    // ---- A10 服务端按 key 查询
    println!("A10 按 key 的服务端查询（QUERY_MSG_BY_KEY）");
    let found_by_key = wait_until(
        || async {
            match p.query_message(&topic, &key_sync, 32, now_ms() - 60_000, now_ms() + 5_000).await {
                Ok(list) => list.iter().any(|m| body_of(m) == "acl-sync"),
                Err(_) => false,
            }
        },
        Duration::from_secs(30),
    )
    .await;
    let queried = p
        .query_message(&topic, &key_sync, 32, now_ms() - 60_000, now_ms() + 5_000)
        .await
        .unwrap_or_default();
    let missing_key = p
        .query_message(&topic, "no-such-key-at-all", 32, now_ms() - 60_000, now_ms() + 5_000)
        .await
        .map(|v| v.len())
        .unwrap_or(999);
    ck.check(
        "A10 带 key 发的消息能由服务端按 key 查回来，未知 key 是空表不是错",
        found_by_key
            && !queried.is_empty()
            && queried.iter().all(|m| m.topic == topic)
            && queried.iter().any(|m| m.get_keys() == Some(key_sync.as_str()))
            && missing_key == 0,
        &format!("key={key_sync} 查到={} 未知 key={missing_key}", queried.len()),
    );

    // ---- A5 延迟消息 + A6 定时消息：先读回，再拿时间戳证明「到点才投」
    println!("A5/A6 延迟级别与定时消息（等投递时刻，用时间戳证明）");
    let delay_born = now_ms();
    let mut m5 = msg(&topic, b"acl-delay", "TagDelay", "");
    m5.set_delay_time_level(DELAY_LEVEL);
    let delay_send = p.send(&mut m5, Some(5000), None).await;
    let mut m6 = msg(&topic, b"acl-timer", "TagTimer", "");
    let deliver_at = now_ms() + TIMER_AHEAD_MILLIS;
    m6.put_property(PROPERTY_TIMER_DELIVER_MS, &deliver_at.to_string());
    let timer_send = p.send(&mut m6, Some(5000), None).await;
    let delay_detail = match &delay_send {
        Ok(r) => format!("{:?}", r.status),
        Err(e) => format!("{} {e}", code_of(&e)),
    };
    let timer_detail = match &timer_send {
        Ok(r) => format!("{:?}", r.status),
        Err(e) => format!("{} {e}", code_of(&e)),
    };
    ck.check(
        "A5 延迟消息（DELAY 级别）发送 SEND_OK",
        delay_send.as_ref().ok().map(|r| r.status) == Some(SendStatus::SendOk),
        &delay_detail,
    );
    ck.check(
        "A6 定时消息（TIMER_DELIVER_MS）发送 SEND_OK",
        timer_send.as_ref().ok().map(|r| r.status) == Some(SendStatus::SendOk),
        &timer_detail,
    );
    // 属性名口径自查：延迟用 DELAY，定时用 TIMER_DELIVER_MS，两者不能串。
    let delay_level_text = DELAY_LEVEL.to_string();
    ck.check(
        "A5/A6 两类消息用的正是各自的属性键",
        m5.get_property(PROPERTY_DELAY_TIME_LEVEL) == Some(delay_level_text.as_str())
            && m6.get_property(PROPERTY_TIMER_DELIVER_MS).is_some()
            && m6.get_property(PROPERTY_DELAY_TIME_LEVEL).is_none(),
        &format!("{:?} {:?}", m5.properties, m6.properties),
    );
    // 定时与延迟消息在投递时刻之前不该被读回。
    let early = snapshot(&instance, &topic, &cluster).await;
    ck.check(
        "A5/A6 到点之前 broker 不投递（快照里还没有这两条）",
        !early.iter().any(|m| body_of(m) == "acl-delay") && !early.iter().any(|m| body_of(m) == "acl-timer"),
        &format!("此刻快照里已有 {} 条正文", bodies(&early).len()),
    );
    let back_delayed = wait_bodies(
        &instance,
        &topic,
        &cluster,
        &["acl-delay", "acl-timer"],
        DELIVER_BUDGET,
    )
    .await;
    let delayed_map: Vec<(String, MessageExt)> = back_delayed
        .iter()
        .map(|m| (body_of(m), m.clone()))
        .collect();
    let got_delay = delayed_map.iter().find(|(b, _)| b == "acl-delay").map(|(_, m)| m.clone());
    let got_timer = delayed_map.iter().find(|(b, _)| b == "acl-timer").map(|(_, m)| m.clone());
    let delay_drift = got_delay.as_ref().map(drift_ms).unwrap_or_default();
    let timer_drift = got_timer.as_ref().map(drift_ms).unwrap_or_default();
    ck.check(
        "A5 延迟消息最终读得到，且 store_ts-born_ts 证明延迟生效",
        got_delay.is_some() && delay_drift >= DELAY_MILLIS - CLOCK_SLACK,
        &format!("drift={delay_drift}ms got={} born={delay_born}", got_delay.is_some()),
    );
    ck.check(
        "A6 定时消息最终读得到，且 store_ts-born_ts 证明到点投递",
        got_timer.is_some() && timer_drift >= TIMER_AHEAD_MILLIS - CLOCK_SLACK,
        &format!("drift={timer_drift}ms deliverAt={deliver_at}"),
    );

    // ---- A12 带签名 push 消费者的读回：正文/属性/时间戳都从消费侧再对一遍
    println!("A12 带签名 push 消费者收满（心跳/拉取/位点提交全签名）");
    let expect: [&str; 9] = [
        "acl-sync",
        "acl-async",
        "acl-oneway",
        "acl-batch-0",
        "acl-batch-1",
        "acl-batch-2",
        "acl-tx-commit",
        "acl-delay",
        "acl-timer",
    ];
    let consumed_all = wait_until(
        || async { expect.iter().all(|b| main_inbox.count_of(b) >= 1) },
        DELIVER_BUDGET,
    )
    .await;
    let missing: Vec<&str> = expect
        .iter()
        .copied()
        .filter(|b| main_inbox.count_of(b) == 0)
        .collect();
    ck.check(
        "A12 消费侧收满全部 9 条（每类消息都真被投出过）",
        consumed_all && missing.is_empty(),
        &format!("缺 {missing:?} 已收={}", main_inbox.bodies().len()),
    );
    let consumed_sync = main_inbox.find("acl-sync");
    ck.check(
        "A11 用户属性透传到消费侧（city/env 原样读到）",
        consumed_sync
            .as_ref()
            .is_some_and(|s| s.city == "Hangzhou" && s.env == "prod"),
        &format!("{:?}", consumed_sync),
    );
    ck.check(
        "A10 keys 透传到消费侧",
        consumed_sync
            .as_ref()
            .is_some_and(|s| s.keys == key_sync),
        &format!("{:?}", consumed_sync.as_ref().map(|s| s.keys.clone())),
    );
    ck.check(
        "A1 tag 透传到消费侧",
        consumed_sync
            .as_ref()
            .is_some_and(|s| s.tags == "TagSync"),
        &format!("{:?}", consumed_sync.as_ref().map(|s| s.tags.clone())),
    );
    let consumed_delay = main_inbox.find("acl-delay");
    let consumed_timer = main_inbox.find("acl-timer");
    ck.check(
        "A5/A6 消费侧看到的投递延迟与读回口径一致",
        consumed_delay.as_ref().is_some_and(|s| s.store_timestamp - s.born_timestamp >= DELAY_MILLIS - CLOCK_SLACK)
            && consumed_timer.as_ref().is_some_and(|s| s.store_timestamp - s.born_timestamp >= TIMER_AHEAD_MILLIS - CLOCK_SLACK),
        &format!(
            "delay={:?} timer={:?}",
            consumed_delay.map(|s| s.store_timestamp - s.born_timestamp),
            consumed_timer.map(|s| s.store_timestamp - s.born_timestamp)
        ),
    );
    // 位点确实提交到了 broker：这是消费侧的第三条签名 RPC（提交用
    // UPDATE_CONSUMER_OFFSET，读回用 QUERY_CONSUMER_OFFSET）。
    // 提交是周期性的（默认 5s 一轮），所以要轮询读，而不是发完就判。
    let group_main = format!("{group}-main");
    let mut committed: std::result::Result<Option<i64>, Error>;
    let commit_deadline = Instant::now() + Duration::from_secs(20);
    loop {
        committed = instance
            .query_consumer_offset(
                &group_main,
                &sync_mq,
                5000,
                Some(cluster.broker_addr.as_str()),
                false,
            )
            .await;
        if matches!(committed, Ok(Some(offset)) if offset > 0)
            || Instant::now() >= commit_deadline
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let committed_ok = matches!(committed, Ok(Some(offset)) if offset > 0);
    ck.check(
        "A12 消费位点已提交到 broker（签名 RPC，读得回）",
        committed_ok,
        &match committed {
            Ok(v) => format!("group={group_main} offset={v:?} mq={sync_mq:?}"),
            Err(e) => format!("{} {e}", code_of(&e)),
        },
    );

    // ---- 清理：删掉本次建的三个 topic（带凭据的管理 RPC）
    println!("清理");
    for t in [&topic, &tag_topic, &order_topic] {
        let on_broker = instance
            .delete_topic_in_broker(&cluster.broker_addr, t, 5000)
            .await;
        ck.check(
            &format!("清理：删 broker 上的 {t}"),
            on_broker.is_ok(),
            &match on_broker {
                Ok(()) => String::new(),
                Err(e) => format!("{} {e}", code_of(&e)),
            },
        );
        let on_namesrv = instance.delete_topic_in_namesrv(t, 5000).await;
        ck.check(
            &format!("清理：删 namesrv 路由 {t}"),
            on_namesrv.is_ok(),
            &match on_namesrv {
                Ok(()) => String::new(),
                Err(e) => format!("{} {e}", code_of(&e)),
            },
        );
    }
    finish(&main_c, &tag_c, &order_c, &p).await;
    ck
}

/// 关停顺序：先消费者（撤位点、发注销）再生产者，避免关停竞态。
async fn finish(
    main_c: &DefaultMQPushConsumer,
    tag_c: &DefaultMQPushConsumer,
    order_c: &DefaultMQPushConsumer,
    p: &DefaultMQProducer,
) {
    main_c.shutdown();
    tag_c.shutdown();
    order_c.shutdown();
    p.shutdown();
}

fn report(ck: &mut Checker) {
    println!(
        "\n== summary: {} passed, {} failed ==",
        ck.passed,
        ck.failed.len()
    );
    for f in &ck.failed {
        println!("   FAILED {f}");
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let argv: Vec<String> = env::args().skip(1).collect();
    let namesrv = argv
        .first()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:9876".to_string());
    let creds = Creds::from_args(
        argv.get(1).map(|s| s.as_str()).unwrap_or_default(),
        argv.get(2).map(|s| s.as_str()).unwrap_or_default(),
    );
    let mut ck = run(&namesrv, creds).await;
    report(&mut ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
