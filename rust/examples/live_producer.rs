//! `DefaultMQProducer` 对**真实 5.5.1 broker** 的联调验证。
//!
//! 与另几个 live 示例的分工：`live_protocol.rs` 打协议编解码，
//! `live_rebalance_and_trace.rs` / `live_client_modules.rs` 打客户端模块，
//! `live_mq_client.rs` 打实例本身。这里打**生产者门面** ——
//! producer 是 `MQClientInstance` 上面那一层，所以真集群独有的性质是
//! 「broker 到底认不认这份请求」。锁死的条目：
//!
//! - P1 **生命周期**：`start()` 生成 clientId 并复用进程内实例、启动前所有发送入口
//!   一律快速失败、`shutdown()` 幂等且能重启、心跳真的把 ProducerData 注册到 broker
//!   （用半消息回查能找上门来反证，见 P6）；关停时**已经排队的异步发送照样跑完**
//!   （Java `ThreadPoolExecutor.shutdown()`：不再收新的，队列里的不吞、不重），
//!   关停之后再投异步一律同步报错。
//! - P2 **六条发送路径**：同步 / 定点 / 批量 / oneway / 选择器 / 异步，逐条对
//!   `SendResult` 字段与 broker 上实际落库的结果；超阈值消息在 broker 端被透明解压
//!   （消费者读到原正文，且 sysFlag 的 COMPRESSED 位已被清）。异步那一栏锁的是
//!   **发送内核**本身：回调里的 `(queue, offset)` 能从 broker 原样读回那一条、
//!   30 笔并发一笔不漏也不重复回调、定点异步真的落在指定队列、池内校验失败按 Java
//!   文案回调一次且生产者之后照常能用。
//! - P3 **钩子**：CheckForbidden 的异常原样抛给调用方且消息不落地；
//!   SendMessageHook 的 before/after 每轮各一次、after 看得见 sendResult。
//! - P4 **轨迹接缝**：注入的 dispatcher 在 `start()` 里被拉起并注册两个轨迹钩子，
//!   发送/事务收尾真的产出 `TraceContext`（Pub + EndTransaction，字段取自 broker 响应）。
//! - P5 **Request-Reply**：请求消息带上 CORRELATION_ID / REPLY_TO_CLIENT / TTL 三个
//!   属性、等待槽登记又清理、没人应答时按 Python 语义回 `RequestTimeout`。
//! - P6 **事务消息**：半消息对消费者不可见 → COMMIT 后可见 / ROLLBACK 后永不可见；
//!   UNKNOW 被 broker 经心跳登记的连接**回查**，回查后提交的最终状态生效。
//! - P7 **管理便捷方法**：`create_topic` 经 TBW102 真建出 topic、四个 offset RPC、
//!   按 key 查消息、`view_message` 按 Python 的行为明确报错。
//! - P9 **定时消息撤回**：SEND 响应带回 `recallHandle`（普通消息没有）、handle 本地解码
//!   对得上 brokerName/uniqKey、`recall_message` 真把定时消息摘掉（到点不投递，同延迟的
//!   对照组照投），`%RETRY%` topic 与坏 handle 在任何 IO 之前按 Java 文案失败。
//! - P8 **发送重试内核**：可重试码集合与 Java 对齐、单次超时上限与「非 SEND_OK 换
//!   broker」开关不影响真集群上的正常发送、没有路由时按错误码定性而非空转重试。
//! - P11 **退出注销**：`shutdown()` 逐台 broker 发 `UNREGISTER_CLIENT`(35)。同 `instanceName`
//!   的两个生产者共用一条连接，先退的那个组照样从 broker 消失、另一个的连接仍在 ——
//!   这条判据把「35 生效」和「TCP 断了才被清掉」分开（Python/C++/.NET 一生产者一实例，
//!   只能用抓帧证明，见那边的 `verify_producer_unregister_live.py` / `live_producer_unregister.cpp`
//!   / `ProducerUnregisterLive.cs`）。
//!
//! 用法（先按项目记忆里的 runbook 起本地集群）：
//! ```text
//! cargo run --example live_producer -- 127.0.0.1:9876
//! ```

use std::collections::HashSet;
use std::env;
use std::future::Future;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::DefaultMQAdminExt;
use rocketmq_client_remoting::client::hook::{
    CheckForbiddenContext, CheckForbiddenHook, EndTransactionContext, EndTransactionHook,
    SendMessageContext, SendMessageHook,
};
use rocketmq_client_remoting::client::mq_client::{MQClientInstance, TraceDispatcher};
use rocketmq_client_remoting::client::producer::{
    DefaultMQProducer, SelectMessageQueueByHash, SendCallback, TransactionListener,
    TransactionMQProducer, DEFAULT_RETRY_RESPONSE_CODES,
};
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::client::request_reply::request_future_holder;
use rocketmq_client_remoting::client::result::{
    LocalTransactionState, SendResult, SendStatus, TransactionSendResult,
};
use rocketmq_client_remoting::client::trace::{TraceContext, TraceType};
use rocketmq_client_remoting::client::trace_hook::TraceReportSink;
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::message_client_id_setter::get_uniq_id;
use rocketmq_client_remoting::common::message_const::{
    PROPERTY_CORRELATION_ID, PROPERTY_MESSAGE_REPLY_TO_CLIENT, PROPERTY_MESSAGE_TTL,
    PROPERTY_TRANSACTION_PREPARED,
};
use rocketmq_client_remoting::common::message_decoder::decode_message_id;
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::recall_message_handle;
use rocketmq_client_remoting::common::sysflag::{MessageSysFlag, PullSysFlag};
use rocketmq_client_remoting::common::topic_config::{self, TopicFilterType};
use rocketmq_client_remoting::common::util_all::current_time_millis;
use rocketmq_client_remoting::remoting::client::RemotingClient;
use rocketmq_client_remoting::remoting::protocol::codes::{request_code, response_code};
use rocketmq_client_remoting::remoting::protocol::ext_fields::StringMap;
use rocketmq_client_remoting::remoting::protocol::headers::{
    CreateTopicRequestHeader, GetRouteInfoRequestHeader,
};
use rocketmq_client_remoting::remoting::protocol::heartbeat::ExpressionType;
use rocketmq_client_remoting::remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;
use rocketmq_client_remoting::remoting::protocol::serialize::RemotingSerializable;

/// 主 topic 的队列数（P2 的定点/选择器断言依赖它）。
const QUEUE_NUMS: i32 = 4;
/// 压缩阈值：4 KiB（Java/Python 默认），正文取 8 KiB 可压缩数据必然触发压缩。
const COMPRESSIBLE_BODY_LEN: usize = 8 * 1024;
/// P5 等应答的预算：远短于 broker 的处理时间，保证一定走到超时分支。
const REQUEST_TIMEOUT_MS: i64 = 1_500;
/// P6 等 broker 回查的预算：half message 的免疫期默认 6s，扫描线程每轮再补几秒。
const CHECK_BACK_BUDGET: Duration = Duration::from_secs(90);
/// 消费组：只用于 pull/offset RPC，本示例不建消费者。
const PULL_GROUP: &str = "CID_rust_live_producer";

// ------------------------------------------------------------------ 骨架

fn stamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

/// 断言累积器：跑完所有场景再汇总，首个失败不提前退出。
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

type Live = Result<(), String>;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// 轮询异步条件直到成立，超时返回 `false` 让调用方打出现场。
async fn wait_async<F, Fut>(mut cond: F, budget: Duration) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + budget;
    loop {
        if cond().await {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

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

fn bodies(msgs: &[MessageExt]) -> Vec<String> {
    msgs.iter()
        .map(|m| String::from_utf8_lossy(&m.body.clone().unwrap_or_default()).into_owned())
        .collect()
}

fn queue_ids(mqs: &[MessageQueue]) -> Vec<i32> {
    mqs.iter().map(|q| q.queue_id).collect()
}

/// 异步发送回调的观测点：每一笔要么成功要么失败，且**终态只来一次**。
///
/// 一次性的 `ClosureSendCallback` 撑不起这类场景（它的闭包取走即空），而「有没有把
/// 某一笔悄悄吞掉」正是异步内核最容易出事的地方。
#[derive(Default)]
struct AsyncProbe {
    done: AtomicUsize,
    ok: AtomicUsize,
    results: Mutex<Vec<SendResult>>,
    errors: Mutex<Vec<String>>,
}

impl AsyncProbe {
    fn done(&self) -> usize {
        self.done.load(Ordering::SeqCst)
    }

    fn ok(&self) -> usize {
        self.ok.load(Ordering::SeqCst)
    }

    fn results(&self) -> Vec<SendResult> {
        lock(&self.results).clone()
    }

    fn first(&self) -> Option<SendResult> {
        lock(&self.results).first().cloned()
    }

    fn errors(&self) -> Vec<String> {
        lock(&self.errors).clone()
    }

    fn summary(&self) -> String {
        let errors = self.errors();
        format!(
            "done={} ok={} err={} {:?}",
            self.done(),
            self.ok(),
            errors.len(),
            errors.first()
        )
    }
}

impl SendCallback for AsyncProbe {
    fn on_success(&self, result: SendResult) {
        if result.status == SendStatus::SendOk {
            self.ok.fetch_add(1, Ordering::SeqCst);
        }
        lock(&self.results).push(result);
        self.done.fetch_add(1, Ordering::SeqCst);
    }

    fn on_exception(&self, err: Error) {
        lock(&self.errors).push(err.to_string());
        self.done.fetch_add(1, Ordering::SeqCst);
    }
}

/// 按异步回调报的 `(queue, offset)` 从 broker 读回那一条的正文。
///
/// 「回调说 SEND_OK」只证明客户端这么认为；位点上真躺着这条消息才证明异步链走完了
/// 网络、broker 也认了这份请求。落库有毫秒级延迟，所以重试到预算用完。
async fn body_at(
    instance: &MQClientInstance,
    mq: &MessageQueue,
    offset: i64,
    broker_addr: &str,
    budget: Duration,
) -> Option<Vec<u8>> {
    let deadline = Instant::now() + budget;
    loop {
        // 队列刚建好时拉取本身会失败，那不算「没落库」，继续轮到预算用完。
        if let Ok(pulled) = pull_from(instance, mq, offset, broker_addr).await {
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

async fn get_route(
    client: &RemotingClient,
    namesrv: &str,
    topic: &str,
) -> Result<RemotingCommand, String> {
    let req = RemotingCommand::create_request_command(
        request_code::GET_ROUTEINFO_BY_TOPIC,
        Some(Box::new(GetRouteInfoRequestHeader {
            topic: Some(topic.to_string()),
            accept_standard_json_only: None,
        })),
    );
    let mut cmd = req;
    client
        .invoke_sync(namesrv, &mut cmd, Some(5000))
        .await
        .map_err(|e| format!("get route of {topic} failed: {e}"))
}

/// 解码 namesrv 的 fastjson 风格路由（数字 map 键不带引号）。
fn decode_route(resp: &RemotingCommand, what: &str) -> Result<TopicRouteData, String> {
    let raw = match resp.body() {
        Some(b) => b.to_vec(),
        None => return Err(format!("{what} response has empty body")),
    };
    let value = RemotingSerializable::decode(&raw)
        .map_err(|e| format!("{what} body is not parsable: {e}"))?;
    TopicRouteData::from_json_value(&value).map_err(|e| format!("{what} route invalid: {e}"))
}

/// 预建主 topic（走裸 RPC，与 `live_mq_client` 同一做法）；P7 再单独验
/// 生产者自己的 `create_topic`。
async fn bootstrap_topic(client: &RemotingClient, namesrv: &str, topic: &str) -> Live {
    let resp = get_route(client, namesrv, MixAll::DEFAULT_TOPIC).await?;
    if resp.code != response_code::SUCCESS {
        return Err(format!(
            "no route of default topic {}: code={} remark={:?}",
            MixAll::DEFAULT_TOPIC,
            resp.code,
            resp.remark
        ));
    }
    let route = decode_route(&resp, "TBW102")?;
    let addr = match route.broker_datas.first().and_then(|b| b.select_broker_addr()) {
        Some(a) => a,
        None => return Err("default route has no usable broker address".to_string()),
    };
    let mut create = RemotingCommand::create_request_command(
        request_code::UPDATE_AND_CREATE_TOPIC,
        Some(Box::new(CreateTopicRequestHeader {
            topic: Some(topic.to_string()),
            default_topic: Some(MixAll::DEFAULT_TOPIC.to_string()),
            read_queue_nums: Some(QUEUE_NUMS),
            write_queue_nums: Some(QUEUE_NUMS),
            perm: Some(topic_config::DEFAULT_PERM),
            topic_filter_type: Some(TopicFilterType::SINGLE_TAG.to_string()),
            topic_sys_flag: Some(0),
            order: Some(false),
            attributes: Some(String::new()),
            force: Some(false),
        })),
    );
    let resp = client
        .invoke_sync(&addr, &mut create, Some(5000))
        .await
        .map_err(|e| format!("create topic {topic} on {addr} failed: {e}"))?;
    if resp.code != response_code::SUCCESS {
        return Err(format!(
            "create topic {topic} rejected: code={} remark={:?}",
            resp.code, resp.remark
        ));
    }
    Ok(())
}

/// 实例侧 `pull_message` 的常用形状：订阅表达式 `*`、无悬挂等待。
async fn pull_from(
    instance: &MQClientInstance,
    mq: &MessageQueue,
    from_offset: i64,
    broker_addr: &str,
) -> Result<rocketmq_client_remoting::client::result::PullResult, String> {
    instance
        .pull_message(
            PULL_GROUP,
            mq,
            from_offset,
            32,
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
        )
        .await
        .map_err(|e| format!("pull {} queue {} failed: {e}", mq.topic, mq.queue_id))
}

/// 某 topic 全部队列上读到的正文（一次快照）。
async fn read_all_bodies(
    instance: &MQClientInstance,
    topic: &str,
    broker_name: &str,
    broker_addr: &str,
    queues: i32,
) -> Vec<String> {
    let mut out = Vec::new();
    for q in 0..queues {
        let mq = MessageQueue::new(topic, broker_name, q);
        // 某个队列拉不动不影响全局快照：这里要的是「读到了什么」，不是错误细节。
        if let Ok(r) = pull_from(instance, &mq, 0, broker_addr).await {
            out.extend(bodies(&r.msg_found_list));
        }
    }
    out
}

// ---------------------------------------------------------- P4 用的假分发器

/// 满足生产者轨迹接缝（`TraceDispatcher` + `TraceReportSink`）的最小分发器：
/// 只把 `TraceContext` 收起来供断言 —— 证明「注入 → 注册钩子 → 启动 → 收到记录」
/// 这条链在真发送上成立，不必依赖真轨迹 topic。
#[derive(Default)]
struct FakeDispatcher {
    inner: Arc<FakeDispatcherState>,
}

#[derive(Default)]
struct FakeDispatcherState {
    reported: Mutex<Vec<TraceContext>>,
    started: AtomicUsize,
    shutdowns: AtomicUsize,
}

impl FakeDispatcher {
    fn new() -> Arc<FakeDispatcher> {
        Arc::new(FakeDispatcher { inner: Arc::new(FakeDispatcherState::default()) })
    }

    fn reported(&self) -> Vec<TraceContext> {
        lock(&self.inner.reported).clone()
    }
}

impl TraceDispatcher for FakeDispatcher {
    fn start(&self, _name_server_addr: &str) -> rocketmq_client_remoting::error::Result<()> {
        self.inner.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn shutdown(&self) {
        self.inner.shutdowns.fetch_add(1, Ordering::SeqCst);
    }
}

impl TraceReportSink for FakeDispatcher {
    fn trace_topic_name(&self) -> String {
        MixAll::TRACE_TOPIC.to_string()
    }

    fn report(&self, context: TraceContext) -> bool {
        lock(&self.inner.reported).push(context);
        true
    }

    fn client_id(&self) -> String {
        "fake-trace-dispatcher".to_string()
    }
}

// -------------------------------------------------------- P3 用的记录钩子

/// 记录 before/after 次序与 after 所见内容的钩子。
#[derive(Default)]
struct RecordingHook {
    state: Mutex<Vec<String>>,
}

impl RecordingHook {
    fn new() -> Arc<RecordingHook> {
        Arc::new(RecordingHook { state: Mutex::new(Vec::new()) })
    }

    fn log(&self, line: String) {
        lock(&self.state).push(line);
    }

    fn lines(&self) -> Vec<String> {
        lock(&self.state).clone()
    }
}

impl SendMessageHook for RecordingHook {
    fn hook_name(&self) -> &str {
        "LiveRecordingHook"
    }

    fn send_message_before(&self, ctx: &mut SendMessageContext) -> rocketmq_client_remoting::error::Result<()> {
        let topic = ctx
            .message
            .as_ref()
            .map(|m| m.topic.clone())
            .unwrap_or_default();
        self.log(format!("before {topic}"));
        Ok(())
    }

    fn send_message_after(&self, ctx: &mut SendMessageContext) -> rocketmq_client_remoting::error::Result<()> {
        match &ctx.send_result {
            Some(r) => self.log(format!("after ok {:?}", r.status)),
            None => self.log(format!("after err {:?}", ctx.exception.as_ref().map(|e| e.to_string()))),
        }
        Ok(())
    }
}

/// 拒绝所有发送的拦截钩子（P3 断言消息不落地）。
struct ForbidTagA;

impl CheckForbiddenHook for ForbidTagA {
    fn hook_name(&self) -> &str {
        "ForbidTagA"
    }

    fn check_forbidden(&self, ctx: &mut CheckForbiddenContext) -> rocketmq_client_remoting::error::Result<()> {
        let tags = ctx
            .message
            .as_ref()
            .and_then(|m| m.get_tags())
            .unwrap_or_default()
            .to_string();
        if tags == "Forbidden" {
            return Err(rocketmq_client_remoting::error::Error::client(
                "live example: tag Forbidden is not allowed",
            ));
        }
        Ok(())
    }
}

/// 记录事务收尾钩子被调到（P4 的 EndTransaction 轨迹一并验证这条链）。
#[derive(Default)]
struct EndTxnHook {
    called: AtomicUsize,
    last: Mutex<Option<String>>,
}

impl EndTransactionHook for EndTxnHook {
    fn hook_name(&self) -> &str {
        "LiveEndTxnHook"
    }

    fn end_transaction(&self, ctx: &mut EndTransactionContext) -> rocketmq_client_remoting::error::Result<()> {
        self.called.fetch_add(1, Ordering::SeqCst);
        *lock(&self.last) = Some(format!(
            "{:?} fromCheck={}",
            ctx.transaction_state, ctx.from_transaction_check
        ));
        Ok(())
    }
}

// -------------------------------------------------------- P6 用的事务监听器

/// 本地事务返回预设状态；回查返回 `check_state`，并记下被回查到的消息。
struct LiveListener {
    local_state: LocalTransactionState,
    check_state: LocalTransactionState,
    checks: AtomicUsize,
    checked: Mutex<Vec<String>>,
}

impl LiveListener {
    fn new(local: LocalTransactionState, check: LocalTransactionState) -> Arc<LiveListener> {
        Arc::new(LiveListener {
            local_state: local,
            check_state: check,
            checks: AtomicUsize::new(0),
            checked: Mutex::new(Vec::new()),
        })
    }

    fn check_count(&self) -> usize {
        self.checks.load(Ordering::SeqCst)
    }

    fn checked_bodies(&self) -> Vec<String> {
        lock(&self.checked).clone()
    }
}

impl TransactionListener for LiveListener {
    fn execute_local_transaction(
        &self,
        _msg: &Message,
        _arg: Option<&rocketmq_client_remoting::client::hook::AnyHolder>,
    ) -> LocalTransactionState {
        self.local_state
    }

    fn check_local_transaction(&self, msg: &MessageExt) -> LocalTransactionState {
        self.checks.fetch_add(1, Ordering::SeqCst);
        lock(&self.checked)
            .push(String::from_utf8_lossy(&msg.body.clone().unwrap_or_default()).into_owned());
        self.check_state
    }
}

// ---------------------------------------------------------------------- P1

/// 生命周期：未启动即失败、start 生成身份、shutdown 幂等且可重启。
async fn p1_lifecycle(namesrv: &str, topic: &str, run: &str, ck: &mut Checker) -> Live {
    let group = format!("PID_rust_producer_{run}_p1");
    let p = DefaultMQProducer::new(&group).map_err(|e| format!("build failed: {e}"))?;
    p.set_namesrv_addr(namesrv);

    let mut probe = msg(topic, b"before-start", "", "");
    let err = p
        .send(&mut probe, Some(1000), None)
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    ck.check(
        "P1 every send entry fails fast before start()",
        err.contains("not started"),
        &err,
    );
    p.shutdown();
    ck.check(
        "P1 shutdown() on a never-started producer is a no-op",
        !p.is_started() && p.client().is_none(),
        &format!("{:?}", p.client_id()),
    );

    p.start().await.map_err(|e| format!("start failed: {e}"))?;
    let client_id = p.client_id().unwrap_or_default();
    ck.check(
        "P1 start() mints a clientId and hands out the shared instance",
        p.is_started() && client_id.contains('@') && p.client().is_some(),
        &format!("clientId={client_id}"),
    );
    ck.check(
        "P1 the instance is registered under that clientId (Java's INSTANCE_MAP)",
        MQClientInstance::find_instance(&client_id).is_some(),
        &client_id,
    );
    // 幂等：第二次 start 不重建实例、不报错
    let before = p.client().map(|c| c.client_id().to_string());
    p.start().await.map_err(|e| format!("second start failed: {e}"))?;
    let after = p.client().map(|c| c.client_id().to_string());
    ck.check(
        "P1 start() is idempotent, like Python's _started guard",
        before == after && after.as_deref() == Some(client_id.as_str()),
        &format!("{before:?} {after:?}"),
    );
    p.shutdown();
    p.shutdown();
    ck.check(
        "P1 shutdown() twice is still fine and clears the started flag",
        !p.is_started(),
        "still started",
    );
    // 允许重启（Python 会重新起实例与心跳线程）
    p.start().await.map_err(|e| format!("restart failed: {e}"))?;
    let mut m = msg(topic, b"after-restart", "", "");
    let sent = p.send(&mut m, Some(5000), None).await;
    let err = sent.as_ref().err().map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "P1 a producer can be started again after shutdown and still sends",
        sent.map(|r| r.status == SendStatus::SendOk).unwrap_or(false),
        &err,
    );
    // ---- 关闭时把已经排队的异步发送跑完（Java `ThreadPoolExecutor.shutdown()`：不再收
    // 新的，队列里那些照样执行）。实例是同时被拆掉的，所以这一批的终态**可能**是错误，
    // 但必须「一笔不漏、一笔不重」—— 吞掉一笔的实现在生产上就是丢消息。
    const DRAIN: usize = 10;
    let draining = Arc::new(AsyncProbe::default());
    for i in 0..DRAIN {
        p.send_async(
            msg(topic, format!("drain-{i}").as_bytes(), "TagDrain", ""),
            draining.clone(),
            Some(10_000),
            None,
        )
        .map_err(|e| format!("send {i} before shutdown was rejected: {e}"))?;
    }
    let began = Instant::now();
    p.shutdown();
    let drained = wait_async(
        || async { draining.done() >= DRAIN },
        Duration::from_secs(20),
    )
    .await;
    let shut = draining.errors();
    ck.check(
        "P1 shutdown() drains the async send queue: every queued send gets exactly one terminal callback",
        drained && draining.done() == DRAIN,
        &format!(
            "{} 关停+跑完用时 {:?} 首笔错误 {:?}",
            draining.summary(),
            began.elapsed(),
            shut.first()
        ),
    );
    let after_shutdown = p
        .send_async(
            msg(topic, b"after-shutdown", "TagDrain", ""),
            draining.clone(),
            Some(1_000),
            None,
        )
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    ck.check(
        "P1 send_async after shutdown() is rejected at the door (and adds no callback)",
        after_shutdown.contains("not started") && draining.done() == DRAIN,
        &format!("{after_shutdown} / {}", draining.summary()),
    );
    Ok(())
}

// ---------------------------------------------------------------------- P2

/// 六条发送路径 + 压缩，逐条对 broker 侧的实际结果。
async fn p2_send_paths(
    p: &DefaultMQProducer,
    instance: &MQClientInstance,
    topic: &str,
    broker_name: &str,
    broker_addr: &str,
    ck: &mut Checker,
) -> Live {
    let queues = p
        .fetch_publish_message_queues(topic)
        .await
        .map_err(|e| format!("fetch_publish_message_queues failed: {e}"))?;
    ck.check(
        "P2 the producer resolves the topic's writable queues itself",
        queue_ids(&queues) == (0..QUEUE_NUMS).collect::<Vec<i32>>(),
        &format!("{:?}", queue_ids(&queues)),
    );

    // ---- 同步发送：SendResult 逐字段
    let mut m1 = msg(topic, b"sync-1", "TagSync", "keySync1");
    let r1 = p.send(&mut m1, Some(5000), None).await
        .map_err(|e| format!("sync send failed: {e}"))?;
    let uniq = get_uniq_id(&m1).unwrap_or_default();
    let offset_ok = match (&r1.message_queue, r1.offset_msg_id.as_deref()) {
        (Some(mq), Some(id)) => mq.topic == topic && decode_message_id(id).is_ok(),
        _ => false,
    };
    ck.check(
        "P2 a plain send round-trips SendResult and stamps UNIQ_KEY on the caller's message",
        r1.status == SendStatus::SendOk
            && r1.msg_id.as_deref() == Some(uniq.as_str())
            && offset_ok
            && r1.region_id.as_deref() == Some(MixAll::DEFAULT_TRACE_REGION_ID)
            && r1.trace_on,
        &format!("uniq={uniq} {r1:?}"),
    );

    // ---- 定点发送：必须落在指定队列
    let target = MessageQueue::new(topic, broker_name, 2);
    let mut m2 = msg(topic, b"fixed-queue", "TagFixed", "");
    let r2 = p.send(&mut m2, Some(5000), Some(&target)).await
        .map_err(|e| format!("fixed send failed: {e}"))?;
    ck.check(
        "P2 sending to an explicit MessageQueue lands exactly there",
        r2.message_queue.as_ref() == Some(&target),
        &format!("{:?}", r2.message_queue),
    );

    // ---- 批量
    let batch_bodies: Vec<String> = (0..3).map(|i| format!("batch-{i}")).collect();
    let batch_msgs = batch_bodies
        .iter()
        .map(|b| msg(topic, b.as_bytes(), "TagBatch", ""))
        .collect();
    let rb = p
        .send_batch(batch_msgs, Some(&MessageQueue::new(topic, broker_name, 1)), Some(5000))
        .await
        .map_err(|e| format!("batch send failed: {e}"))?;
    let pulled = wait_async(
        || async {
            let r = pull_from(instance, &MessageQueue::new(topic, broker_name, 1), rb.queue_offset, broker_addr)
                .await
                .unwrap_or_default();
            let got = bodies(&r.msg_found_list);
            batch_bodies.iter().all(|b| got.contains(b))
        },
        Duration::from_secs(15),
    )
    .await;
    ck.check(
        "P2 a batch is one physical write whose sub-messages pull back with contiguous offsets",
        rb.status == SendStatus::SendOk && pulled,
        &format!("parentOffset={} pulled={pulled}", rb.queue_offset),
    );

    // ---- oneway：无响应，但必须能读回
    let mut m3 = msg(topic, b"sent-oneway", "TagOneway", "");
    let oneway_mq = MessageQueue::new(topic, broker_name, 3);
    p.send_oneway(&mut m3, Some(&oneway_mq))
        .await
        .map_err(|e| format!("oneway send failed: {e}"))?;
    let landed = wait_async(
        || async {
            let r = pull_from(instance, &oneway_mq, 0, broker_addr)
                .await
                .unwrap_or_default();
            bodies(&r.msg_found_list).iter().any(|b| b == "sent-oneway")
        },
        Duration::from_secs(15),
    )
    .await;
    ck.check(
        "P2 a oneway send returns nothing yet still reaches the commitlog",
        landed,
        "the oneway body never showed up",
    );

    // ---- 选择器（顺序消息）：同一 arg 必须同一队列
    let mut picked = Vec::new();
    for i in 0..6 {
        let mut m = msg(topic, format!("order-{i}").as_bytes(), "TagOrder", "");
        let r = p
            .send_by_selector(&mut m, &SelectMessageQueueByHash, "same-shard-key", Some(5000))
            .await
            .map_err(|e| format!("selector send failed: {e}"))?;
        picked.push(r.message_queue.clone().map(|q| q.queue_id).unwrap_or(-1));
    }
    let distinct: HashSet<i32> = picked.iter().copied().collect();
    ck.check(
        "P2 the hash selector pins every message of one key to one queue",
        distinct.len() == 1 && picked.len() == 6,
        &format!("{picked:?}"),
    );

    // ---- 异步发送内核：回调里的 SendResult 与同步同形，而且消息真在 broker 上
    let probe = Arc::new(AsyncProbe::default());
    p.send_async(
        msg(topic, b"async-1", "TagAsync", ""),
        probe.clone(),
        Some(5_000),
        None,
    )
    .map_err(|e| format!("async send failed: {e}"))?;
    let called = wait_async(
        || async { probe.done() >= 1 },
        Duration::from_secs(10),
    )
    .await;
    let first = probe.first();
    let shaped = first.as_ref().is_some_and(|r| {
        r.status == SendStatus::SendOk
            && r.msg_id
                .as_deref()
                .is_some_and(|id| decode_message_id(id).is_ok())
            && r.message_queue.as_ref().is_some_and(|q| {
                q.topic == topic && q.broker_name == *broker_name && q.queue_id >= 0
            })
            && r.queue_offset >= 0
    });
    ck.check(
        "P2 send_async drives the success callback with a Java-shaped SendResult",
        called && shaped && probe.errors().is_empty(),
        &probe.summary(),
    );
    let landed = match first
        .as_ref()
        .and_then(|r| r.message_queue.as_ref().map(|mq| (r, mq)))
    {
        Some((r, mq)) => {
            body_at(instance, mq, r.queue_offset, broker_addr, Duration::from_secs(15)).await
                == Some(b"async-1".to_vec())
        }
        None => false,
    };
    ck.check(
        "P2 the async callback's queue+offset reads that very message back off the broker",
        landed,
        &probe.summary(),
    );

    // ---- 池子不吞消息：并发 30 笔，每笔恰好一个终态回调，且每一笔都在回调报的位点上
    const BURST: usize = 30;
    let burst = Arc::new(AsyncProbe::default());
    for i in 0..BURST {
        p.send_async(
            msg(topic, format!("async-burst-{i}").as_bytes(), "TagBurst", ""),
            burst.clone(),
            Some(10_000),
            None,
        )
        .map_err(|e| format!("burst send {i} was rejected: {e}"))?;
    }
    let all_back = wait_async(
        || async { burst.done() >= BURST },
        Duration::from_secs(30),
    )
    .await;
    ck.check(
        "P2 30 concurrent async sends each get exactly one terminal callback, all SEND_OK",
        all_back && burst.done() == BURST && burst.ok() == BURST,
        &burst.summary(),
    );
    let results = burst.results();
    let mut slots = HashSet::new();
    let mut read_back: HashSet<Vec<u8>> = HashSet::new();
    for r in &results {
        let Some(mq) = r.message_queue.as_ref() else {
            continue;
        };
        slots.insert((mq.broker_name.clone(), mq.queue_id, r.queue_offset));
        if let Some(body) = body_at(instance, mq, r.queue_offset, broker_addr, Duration::from_secs(15)).await {
            read_back.insert(body);
        }
    }
    let want: HashSet<Vec<u8>> = (0..BURST)
        .map(|i| format!("async-burst-{i}").into_bytes())
        .collect();
    ck.check(
        "P2 every burst message is readable at exactly the offset its own callback reported",
        read_back == want && slots.len() == BURST,
        &format!(
            "读回 {} 条、位点 {} 个、缺 {:?}",
            read_back.len(),
            slots.len(),
            want.difference(&read_back).count()
        ),
    );

    // ---- 定点异步发送：交给它的队列就是它落下去的队列
    let target = MessageQueue::new(topic, broker_name, 2);
    let aimed = Arc::new(AsyncProbe::default());
    p.send_async(
        msg(topic, b"async-on-mq", "TagAsyncMq", ""),
        aimed.clone(),
        Some(5_000),
        Some(target.clone()),
    )
    .map_err(|e| format!("targeted async send failed: {e}"))?;
    let aimed_back = wait_async(
        || async { aimed.done() >= 1 },
        Duration::from_secs(10),
    )
    .await;
    let aimed_result = aimed.first();
    let on_target = aimed_result
        .as_ref()
        .is_some_and(|r| r.message_queue.as_ref() == Some(&target));
    let aimed_landed = match aimed_result.as_ref() {
        Some(r) => {
            body_at(instance, &target, r.queue_offset, broker_addr, Duration::from_secs(15)).await
                == Some(b"async-on-mq".to_vec())
        }
        None => false,
    };
    ck.check(
        "P2 send_async honours an explicit target queue",
        aimed_back && on_target && aimed_landed,
        &format!("onTarget={on_target} {}", aimed.summary()),
    );

    // ---- 本地校验在池子里报错：`Validators.checkMessage` 的超长 body 走的是
    //      `async_send_inner` 的失败分支（Java `sendKernelImpl` 同位置）：既不落地，也
    //      只回调一次，之后这个生产者照常能用（许可/队列没被这条断链漏掉）。
    let too_big = vec![b'q'; 5 * 1024 * 1024];
    let rejected = Arc::new(AsyncProbe::default());
    p.send_async(
        msg(topic, &too_big, "TagTooBig", ""),
        rejected.clone(),
        Some(10_000),
        None,
    )
    .map_err(|e| format!("oversized async send was rejected at the door: {e}"))?;
    let rejected_back = wait_async(
        || async { rejected.done() >= 1 },
        Duration::from_secs(20),
    )
    .await;
    let rejected_errors = rejected.errors();
    let java_wording = rejected_errors
        .first()
        .is_some_and(|e| e.contains("the message body size over max value, MAX: "));
    let after_reject = Arc::new(AsyncProbe::default());
    let still_works = p
        .send_async(
            msg(topic, b"async-after-reject", "TagAfterReject", ""),
            after_reject.clone(),
            Some(5_000),
            None,
        )
        .is_ok()
        && wait_async(
            || async { after_reject.done() >= 1 },
            Duration::from_secs(10),
        )
        .await
        && after_reject.ok() == 1;
    ck.check(
        "P2 an in-pool validator failure callbacks once with Java's text and leaves the producer usable",
        rejected_back
            && rejected.done() == 1
            && rejected.ok() == 0
            && java_wording
            && still_works,
        &format!("{} / 之后一笔 {}", rejected.summary(), after_reject.summary()),
    );

    // ---- 压缩：8 KiB 可压缩正文 → 消费者读到原始正文
    let raw: Vec<u8> = vec![b'z'; COMPRESSIBLE_BODY_LEN];
    let mut big = msg(topic, &raw, "TagCompressed", "keyCompressed");
    let big_mq = MessageQueue::new(topic, broker_name, 0);
    let rb2 = p
        .send(&mut big, Some(5000), Some(&big_mq))
        .await
        .map_err(|e| format!("compressed send failed: {e}"))?;
    // tryToCompressMessage 就地改写调用方的 body：压完必然更短，且仍是 zlib 流
    // （zlib 头 0x78）。这是「客户端侧压缩过一次」的直接证据。
    let client_body = big.get_body().to_vec();
    let compressed_in_client =
        client_body.len() < COMPRESSIBLE_BODY_LEN && client_body.starts_with(&[0x78]);
    let back = wait_async(
        || async {
            let r = pull_from(instance, &big_mq, rb2.queue_offset, broker_addr)
                .await
                .unwrap_or_default();
            r.msg_found_list
                .iter()
                .any(|m| m.body.clone().unwrap_or_default() == raw)
        },
        Duration::from_secs(15),
    )
    .await;
    let on_broker = pull_from(instance, &big_mq, rb2.queue_offset, broker_addr)
        .await
        .unwrap_or_default();
    let stored = on_broker
        .msg_found_list
        .iter()
        .find(|m| m.queue_offset == rb2.queue_offset);
    let decoded = stored.map(|m| m.body.clone().unwrap_or_default()).unwrap_or_default();
    // broker 存的是我们发出去的压缩流，所以整条消息的 storeSize 远小于原始正文。
    let plain_len = i32::try_from(COMPRESSIBLE_BODY_LEN).unwrap_or(i32::MAX);
    let went_compressed = stored.map(|m| m.store_size < plain_len).unwrap_or(false);
    // 解完压的 MessageExt：COMPRESSED 位被清（否则重复消费会再解一次），
    // 但算法 id 留在 sysFlag 里 —— Java/Python 的 clearCompressedFlag 只清最低位。
    let flag_ok = stored
        .map(|m| {
            !MessageSysFlag::is_compressed(m.sys_flag)
                && MessageSysFlag::get_compression_type(m.sys_flag) == MessageSysFlag::ZLIB_TYPE
        })
        .unwrap_or(false);
    ck.check(
        "P2 an over-threshold body is zlib'd on the wire and transparently restored for the reader",
        compressed_in_client
            && back
            && went_compressed
            && flag_ok
            && rb2.status == SendStatus::SendOk
            && decoded == raw,
        &format!(
            "clientBody={} storeSize={:?} sysFlag={:?} restored={}",
            client_body.len(),
            stored.map(|m| m.store_size),
            stored.map(|m| m.sys_flag),
            decoded.len()
        ),
    );
    Ok(())
}

// ---------------------------------------------------------------------- P3

/// 钩子：拦截异常传播 + before/after 次序与可见性。
async fn p3_hooks(
    p: &DefaultMQProducer,
    instance: &MQClientInstance,
    topic: &str,
    broker_name: &str,
    broker_addr: &str,
    ck: &mut Checker,
) -> Live {
    let hook = RecordingHook::new();
    p.register_send_message_hook(hook.clone());
    p.register_check_forbidden_hook(Arc::new(ForbidTagA));

    // 被拦截：错误原样抛出，且消息没进 broker
    let mut forbidden = msg(topic, b"must-not-land", "Forbidden", "");
    let err = p
        .send(&mut forbidden, Some(5000), Some(&MessageQueue::new(topic, broker_name, 3)))
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    let lines = hook.lines();
    let absent = !read_all_bodies(instance, topic, broker_name, broker_addr, QUEUE_NUMS)
        .await
        .iter()
        .any(|b| b == "must-not-land");
    ck.check(
        "P3 a rejecting CheckForbiddenHook surfaces its error and the send hooks never run",
        err.contains("tag Forbidden is not allowed")
            && lines.iter().all(|l| !l.contains("must-not-land"))
            && absent,
        &format!("err={err} lines={lines:?}"),
    );

    // 正常放行：before -> after ok 各一次
    let before_len = hook.lines().len();
    let mut ok_msg = msg(topic, b"hook-passed", "TagAllowed", "");
    p.send(&mut ok_msg, Some(5000), Some(&MessageQueue::new(topic, broker_name, 2)))
        .await
        .map_err(|e| format!("hook send failed: {e}"))?;
    let new_lines = hook.lines()[before_len..].to_vec();
    ck.check(
        "P3 SendMessageHook runs before then after, and after sees the SendResult",
        new_lines.len() == 2
            && new_lines[0].starts_with("before ")
            && new_lines[1] == "after ok SendOk",
        &format!("{new_lines:?}"),
    );

    // 失败也要跑 after（Java sendKernelImpl 的 finally 语义）。
    // 注意不能拿「不存在的 topic」当失败手段：本集群开着 autoCreateTopicEnable，
    // 新 topic 会经 TBW102 兜底真发成功。这里改成把消息定点到一个路由里
    // 根本不存在的 brokerName —— send_message 必然找不到地址。
    let before_len = hook.lines().len();
    let mut bad = msg(topic, b"hook-fails", "TagAllowed", "");
    let bogus = MessageQueue::new(topic, "RustNoSuchBroker_zzz", 0);
    let err = p
        .send(&mut bad, Some(5000), Some(&bogus))
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    let tail = hook.lines()[before_len..].to_vec();
    ck.check(
        "P3 a failing send still runs the after hook, with the exception on the context",
        !err.is_empty()
            && err.contains("RustNoSuchBroker_zzz")
            && tail.len() == 2
            && tail[0].starts_with("before ")
            && tail[1].starts_with("after err "),
        &format!("err={err} tail={tail:?}"),
    );
    Ok(())
}

// ---------------------------------------------------------------------- P4

/// 轨迹接缝：注入 → start 注册钩子并拉起 → 真发送产出 TraceContext。
async fn p4_trace_seam(namesrv: &str, topic: &str, run: &str, ck: &mut Checker) -> Live {
    let group = format!("PID_rust_producer_{run}_p4");
    let p = DefaultMQProducer::new(&group).map_err(|e| format!("build failed: {e}"))?;
    p.set_namesrv_addr(namesrv);
    let dispatcher = FakeDispatcher::new();
    // FakeDispatcher 同时实现 TraceDispatcher + TraceReportSink，靠 producer 模块里的
    // blanket impl 直接当成 Arc<dyn TraceDispatcherChannel> 用。
    p.set_trace_dispatcher(Some(dispatcher.clone()));
    p.set_enable_trace(true);
    p.start().await.map_err(|e| format!("start failed: {e}"))?;

    ck.check(
        "P4 start() boots the injected dispatcher with the producer's namesrv",
        dispatcher.inner.started.load(Ordering::SeqCst) == 1,
        &format!("started={}", dispatcher.inner.started.load(Ordering::SeqCst)),
    );

    let mut m = msg(topic, b"traced-send", "TagTrace", "keyTrace");
    p.send(&mut m, Some(5000), None)
        .await
        .map_err(|e| format!("traced send failed: {e}"))?;
    let got = wait_async(
        || async { !dispatcher.reported().is_empty() },
        Duration::from_secs(10),
    )
    .await;
    let contexts = dispatcher.reported();
    let pub_ctx = contexts.iter().find(|c| c.trace_type == Some(TraceType::Pub));
    let bean_ok = pub_ctx
        .map(|c| {
            c.is_success
                && c.cost_time >= 0
                && c.region_id == MixAll::DEFAULT_TRACE_REGION_ID
                && c.group_name == group
                && c.trace_beans.len() == 1
                && c.trace_beans[0].topic == topic
                && c.trace_beans[0].tags == "TagTrace"
                && c.trace_beans[0].keys == "keyTrace"
                && !c.trace_beans[0].msg_id.is_empty()
                && c.trace_beans[0].body_length == b"traced-send".len() as i32
        })
        .unwrap_or(false);
    ck.check(
        "P4 the send trace hook reports a Pub context filled from the broker response",
        got && bean_ok,
        &format!("{contexts:?}"),
    );

    // 轨迹消息自己不产轨迹（避免回环）：topic 以轨迹 topic 开头的消息应被跳过
    let mut traced = msg(
        &format!("{}%x", MixAll::TRACE_TOPIC),
        b"trace-of-trace",
        "",
        "",
    );
    let _ = p.send(&mut traced, Some(3000), None).await;
    let count_after = dispatcher.reported().len();
    ck.check(
        "P4 a message on the trace topic itself is never traced, like Java's guard",
        count_after == contexts.len(),
        &format!("before={} after={count_after}", contexts.len()),
    );

    p.shutdown();
    ck.check(
        "P4 shutdown() also shuts the injected dispatcher down",
        dispatcher.inner.shutdowns.load(Ordering::SeqCst) == 1,
        &format!("shutdowns={}", dispatcher.inner.shutdowns.load(Ordering::SeqCst)),
    );
    Ok(())
}

// ---------------------------------------------------------------------- P5

/// Request-Reply：三个请求属性、等待槽生命周期、无人应答时的超时语义。
async fn p5_request_reply(
    p: &DefaultMQProducer,
    topic: &str,
    broker_name: &str,
    ck: &mut Checker,
) -> Live {
    let mut m = msg(topic, b"ping", "TagRpc", "keyRpc");
    let correlation_before = m
        .get_property(PROPERTY_CORRELATION_ID)
        .map(str::to_string);
    let result = p
        .send(
            &mut m,
            Some(5000),
            Some(&MessageQueue::new(topic, broker_name, 0)),
        )
        .await
        .map_err(|e| format!("warm-up send failed: {e}"))?;
    let _ = result;
    let mut req = msg(topic, b"ping-reply", "TagRpc", "keyRpc2");
    let err = p
        .request(&mut req, Some(REQUEST_TIMEOUT_MS), Some(&MessageQueue::new(topic, broker_name, 0)))
        .await
        .err();
    let code = err.as_ref().and_then(|e| e.response_code());
    let err = err.map(|e| e.to_string()).unwrap_or_default();
    let cid = req.get_property(PROPERTY_CORRELATION_ID).unwrap_or_default().to_string();
    let reply_to = req
        .get_property(PROPERTY_MESSAGE_REPLY_TO_CLIENT)
        .unwrap_or_default()
        .to_string();
    let ttl = req.get_property(PROPERTY_MESSAGE_TTL).unwrap_or_default().to_string();
    ck.check(
        "P5 the request message carries CORRELATION_ID, REPLY_TO_CLIENT and TTL",
        correlation_before.is_none()
            && !cid.is_empty()
            && reply_to == p.client_id().unwrap_or_default()
            && ttl == REQUEST_TIMEOUT_MS.to_string(),
        &format!("cid={cid} replyTo={reply_to} ttl={ttl}"),
    );
    ck.check(
        "P5 with nobody answering, request() reports RequestTimeout (the request really was sent)",
        err.contains("wait reply message timeout"),
        &err,
    );
    // 光有类型和文案不够：Java 抛的是
    // `RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, ...)`，
    // 10006 这个码也要在 —— 调用方按码分流时「没等到应答」（对方可能只是慢，
    // 消息其实已投出去）和别的客户端故障不是一类处置。
    ck.check(
        "P5 the timeout carries 10006 REQUEST_TIMEOUT_EXCEPTION",
        code == Some(10006),
        &format!("code={code:?} err={err}"),
    );
    ck.check(
        "P5 the wait slot is removed again, so the table does not leak",
        request_future_holder().is_empty(),
        &format!("pending={}", request_future_holder().len()),
    );
    Ok(())
}

// ---------------------------------------------------------------------- P6

/// 事务：半消息不可见 → COMMIT 可见 / ROLLBACK 永不可见 / UNKNOW 被回查后提交。
#[allow(clippy::too_many_arguments)]
async fn p6_transactions(
    p: &DefaultMQProducer,
    instance: &MQClientInstance,
    namesrv: &str,
    topic: &str,
    broker_name: &str,
    broker_addr: &str,
    run: &str,
    ck: &mut Checker,
) -> Live {
    let end_hook = Arc::new(EndTxnHook::default());
    p.register_end_transaction_hook(end_hook.clone());

    // ---- COMMIT：半消息提交后消费者读得到
    let commit_listener = LiveListener::new(LocalTransactionState::CommitMessage, LocalTransactionState::Unknow);
    let mut commit_msg = msg(topic, b"txn-commit", "TagTxn", "keyTxnCommit");
    let r = p
        .send_message_in_transaction(&mut commit_msg, Some(commit_listener.clone()), None)
        .await
        .map_err(|e| format!("txn commit failed: {e}"))?;
    let half_props = commit_msg
        .get_property(PROPERTY_TRANSACTION_PREPARED)
        .unwrap_or_default()
        .to_string();
    let committed = wait_async(
        || async {
            read_all_bodies(instance, topic, broker_name, broker_addr, QUEUE_NUMS)
                .await
                .iter()
                .any(|b| b == "txn-commit")
        },
        Duration::from_secs(20),
    )
    .await;
    ck.check(
        "P6 a committed transaction reports SEND_OK, carries TRAN_MSG and becomes visible",
        r.inner.status == SendStatus::SendOk
            && half_props == "true"
            && r.local_transaction_state == Some(LocalTransactionState::CommitMessage)
            && committed,
        &format!("{r:?} visible={committed}"),
    );

    // ---- ROLLBACK：永远读不到
    let rollback_listener = LiveListener::new(LocalTransactionState::RollbackMessage, LocalTransactionState::Unknow);
    let mut rb = msg(topic, b"txn-rollback", "TagTxn", "keyTxnRollback");
    let rr = p
        .send_message_in_transaction(&mut rb, Some(rollback_listener), None)
        .await
        .map_err(|e| format!("txn rollback failed: {e}"))?;
    tokio::time::sleep(Duration::from_secs(8)).await;
    let bodies = read_all_bodies(instance, topic, broker_name, broker_addr, QUEUE_NUMS).await;
    ck.check(
        "P6 a rolled-back transaction leaves nothing on the topic but still SEND_OKs the half message",
        rr.local_transaction_state == Some(LocalTransactionState::RollbackMessage)
            && !bodies.iter().any(|b| b == "txn-rollback"),
        &format!("state={:?} bodies={:?}", rr.local_transaction_state, bodies.len()),
    );

    // ---- UNKNOW → broker 回查（走心跳登记的连接）→ 这次返回 COMMIT
    let check_listener = LiveListener::new(LocalTransactionState::Unknow, LocalTransactionState::CommitMessage);
    let mut unknown = msg(topic, b"txn-checked", "TagTxn", "keyTxnCheck");
    let ru = p
        .send_message_in_transaction(&mut unknown, Some(check_listener.clone()), None)
        .await
        .map_err(|e| format!("txn unknown failed: {e}"))?;
    let immediately_visible = read_all_bodies(instance, topic, broker_name, broker_addr, QUEUE_NUMS)
        .await
        .iter()
        .any(|b| b == "txn-checked");
    let checked = wait_async(
        || async { check_listener.check_count() >= 1 },
        CHECK_BACK_BUDGET,
    )
    .await;
    let finally_visible = wait_async(
        || async {
            read_all_bodies(instance, topic, broker_name, broker_addr, QUEUE_NUMS)
                .await
                .iter()
                .any(|b| b == "txn-checked")
        },
        Duration::from_secs(20),
    )
    .await;
    ck.check(
        "P6 an UNKNOW transaction is invisible until the broker checks it back on our heartbeat channel",
        ru.local_transaction_state == Some(LocalTransactionState::Unknow)
            && !immediately_visible
            && checked
            && finally_visible,
        &format!(
            "state={:?} checks={} visible={finally_visible} bodies={:?}",
            ru.local_transaction_state,
            check_listener.check_count(),
            check_listener.checked_bodies()
        ),
    );
    ck.check(
        "P6 the broker's check-back reaches our listener with the original message body",
        check_listener.checked_bodies().iter().any(|b| b == "txn-checked"),
        &format!("{:?}", check_listener.checked_bodies()),
    );
    ck.check(
        "P4/P3 shared seam: the end-transaction hook fired for every two-phase commit",
        end_hook.called.load(Ordering::SeqCst) >= 3,
        &format!("called={}", end_hook.called.load(Ordering::SeqCst)),
    );
    // 回查后提交的那次收尾必须标着 fromCheck=true（Java 的
    // checkTransactionState → endTransaction(fromTransactionCheck=true)）
    let last_hook = lock(&end_hook.last).clone().unwrap_or_default();
    ck.check(
        "P6 the last end-transaction hook call is the broker check-back, marked as such",
        last_hook.contains("CommitMessage") && last_hook.contains("fromCheck=true"),
        &last_hook,
    );

    // ---- TransactionMQProducer：预置监听器之后，发送时不必再传
    // （Python/C++ 里这个子类唯一多做的事就是这段兜底，这里要把它打实。）
    let txn_group = format!("PID_rust_producer_{run}_p6txn");
    let txn = TransactionMQProducer::new(&txn_group).map_err(|e| format!("txn build failed: {e}"))?;
    txn.set_namesrv_addr(namesrv);
    txn.set_instance_name(&format!("{run}-txn"));
    let preset = LiveListener::new(LocalTransactionState::CommitMessage, LocalTransactionState::Unknow);
    txn.set_transaction_listener(Some(preset.clone()));
    txn.start().await.map_err(|e| format!("txn start failed: {e}"))?;
    let fallback_ok = txn.transaction_listener().is_some();
    let mut txn_msg = msg(topic, b"txn-preset-listener", "TagTxn", "keyTxnPreset");
    let rt: TransactionSendResult = txn
        .send_message_in_transaction(&mut txn_msg, None, None)
        .await
        .map_err(|e| format!("txn preset-listener send failed: {e}"))?;
    let preset_visible = wait_async(
        || async {
            read_all_bodies(instance, topic, broker_name, broker_addr, QUEUE_NUMS)
                .await
                .iter()
                .any(|b| b == "txn-preset-listener")
        },
        Duration::from_secs(20),
    )
    .await;
    ck.check(
        "P6 TransactionMQProducer falls back to its preset listener when the call passes none",
        fallback_ok
            && rt.local_transaction_state == Some(LocalTransactionState::CommitMessage)
            && rt.inner.status == SendStatus::SendOk
            && preset_visible,
        &format!("{rt:?} visible={preset_visible}"),
    );
    // Deref 到基础生产者：管理方法照常可用
    let via_deref = txn
        .fetch_publish_message_queues(topic)
        .await
        .map(|v| v.len())
        .unwrap_or(0);
    ck.check(
        "P6 TransactionMQProducer derefs to the base producer, like the C++ subclass does",
        via_deref == QUEUE_NUMS as usize,
        &format!("queues={via_deref}"),
    );
    txn.shutdown();
    Ok(())
}

// ---------------------------------------------------------------------- P7

/// 管理便捷方法：建 topic、四个 offset RPC、按 key 查询、view_message 报错。
#[allow(clippy::too_many_arguments)]
async fn p7_admin(
    p: &DefaultMQProducer,
    instance: &MQClientInstance,
    namesrv: &str,
    topic: &str,
    broker_name: &str,
    broker_addr: &str,
    run: &str,
    ck: &mut Checker,
) -> Live {
    // ---- create_topic：只给名字，队列由 broker 按默认值建
    let created = format!("RustLiveProducerCreated_{run}");
    p.create_topic(&created, QUEUE_NUMS, 0)
        .await
        .map_err(|e| format!("create_topic failed: {e}"))?;
    let route = get_route(instance.remoting_client(), namesrv, &created)
        .await
        .and_then(|r| decode_route(&r, "created"))?;
    let qd = route.queue_datas.first();
    ck.check(
        "P7 create_topic publishes a real route with R|W perms on the broker",
        qd.map(|q| q.broker_name == broker_name && q.perm == 6 && q.write_queue_nums == QUEUE_NUMS)
            .unwrap_or(false),
        &format!("{:?}", route.queue_datas),
    );
    let mut into_new = msg(&created, b"into-created-topic", "", "");
    let sent_new = p.send(&mut into_new, Some(5000), None).await;
    let new_err = sent_new.as_ref().err().map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "P7 the freshly created topic accepts sends through the same producer",
        sent_new.map(|r| r.status == SendStatus::SendOk).unwrap_or(false),
        &new_err,
    );

    // ---- offset 四件套
    let mq = MessageQueue::new(topic, broker_name, 0);
    let max_offset = p.max_offset(&mq).await.map_err(|e| format!("max_offset: {e}"))?;
    let min_offset = p.min_offset(&mq).await.map_err(|e| format!("min_offset: {e}"))?;
    // 基准不能用「现在减一分钟」：跑到这里时本次运行可能已经过了好几分钟（真机 TLS 上
    // 等事务回查就吃掉 90s），那样 searchOffset 会跳过早先的消息，判据与时长耦合。
    // topic 名带本次运行的秒级时间戳、只属于本次运行，所以开跑时刻就是第一条消息之前。
    let run_started_ms = run.parse::<i64>().unwrap_or_default() * 1000;
    let past = p.search_offset(&mq, run_started_ms).await
        .map_err(|e| format!("search_offset(run start): {e}"))?;
    let future = p.search_offset(&mq, current_time_millis() + 600_000).await
        .map_err(|e| format!("search_offset(future): {e}"))?;
    let earliest = p.earliest_msg_store_time(&mq).await
        .map_err(|e| format!("earliest_msg_store_time: {e}"))?;
    ck.check(
        "P7 the offset helpers agree with each other and with the queue contents",
        max_offset > min_offset
            && past == min_offset
            && future == max_offset
            && earliest > 0
            && earliest <= current_time_millis(),
        &format!("min={min_offset} max={max_offset} past={past} future={future} earliest={earliest}"),
    );

    // ---- 按 key 查询（broker 建索引是异步的，拉到为止）
    let key = format!("queryKey-{run}");
    let mut qm = msg(topic, b"queried-by-key", "TagQuery", &key);
    p.send(&mut qm, Some(5000), Some(&mq))
        .await
        .map_err(|e| format!("send for query failed: {e}"))?;
    let found = wait_async(
        || async {
            match p.query_message(topic, &key, 32, 0, current_time_millis() + 5000).await {
                Ok(list) => list.iter().any(|m| m.body.clone().unwrap_or_default() == b"queried-by-key"),
                Err(_) => false,
            }
        },
        Duration::from_secs(30),
    )
    .await;
    let got = p
        .query_message(topic, &key, 32, 0, current_time_millis() + 5000)
        .await
        .unwrap_or_default();
    ck.check(
        "P7 query_message finds the message by its business key",
        found && !got.is_empty() && got.iter().all(|m| m.topic == topic),
        &format!("found={found} got={}", bodies(&got).len()),
    );
    let missing = p
        .query_message(topic, "no-such-key-at-all", 32, 0, current_time_millis())
        .await
        .map(|v| v.len())
        .unwrap_or(999);
    ck.check(
        "P7 an unknown key is an empty result, not an error",
        missing == 0,
        &format!("len={missing}"),
    );

    // ---- view_message：与 Python 一致地显式不支持
    let err = p
        .view_message(topic, "0102030405")
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    ck.check(
        "P7 view_message refuses instead of pretending, as the Python edition does",
        err.contains("not supported"),
        &err,
    );

    // ---- 清理本例建的 topic
    for t in [&created] {
        match instance.delete_topic_in_broker(broker_addr, t, 5000).await {
            Ok(()) => ck.check(&format!("P7 delete_topic_in_broker {t}"), true, ""),
            Err(e) => ck.check(&format!("P7 delete_topic_in_broker {t}"), false, &e.to_string()),
        }
        match instance.delete_topic_in_namesrv(t, 5000).await {
            Ok(()) => ck.check(&format!("P7 delete_topic_in_namesrv {t}"), true, ""),
            Err(e) => ck.check(&format!("P7 delete_topic_in_namesrv {t}"), false, &e.to_string()),
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- P8 重试内核

/// 发送重试内核（`sendDefaultImpl`）里**能上真集群**的那几面：可重试码集合、
/// 单次超时上限、非 SEND_OK 换 broker 开关，以及彻底拿不到路由时的定性。
///
/// 「broker 回可重试码就换一台」「慢 broker 吃光预算报 call timeout」「端口拒绝报
/// 10001」这三条要的是 broker 主动回错误码 / 慢到吃掉预算 / 端口拒连 —— 真集群一台
/// 健康的单 broker 都给不了，留在 `src/client/producer/send_retry_tests.rs` 的
/// 进程内假集群里对拍（与 python/cpp 的同题用例一一对应）。
async fn p8_send_retry_kernel(
    p: &DefaultMQProducer,
    topic: &str,
    run: &str,
    ck: &mut Checker,
) -> Live {
    let defaults: Vec<i32> = DEFAULT_RETRY_RESPONSE_CODES.to_vec();
    ck.check(
        "P8 the default retryable broker codes are exactly the Java eight",
        defaults == [1, 2, 14, 16, 17, 204, 205, 1500]
            && p.get_retry_response_codes().len() == 8
            && p.is_retry_response_code(Some(response_code::SERVICE_NOT_AVAILABLE))
            // 没等到响应码（压根没连上）等于不可重试；不在集合里的码也不可重试
            && !p.is_retry_response_code(None)
            && !p.is_retry_response_code(Some(response_code::MESSAGE_ILLEGAL)),
        &format!("{defaults:?}"),
    );

    p.add_retry_response_code(response_code::MESSAGE_ILLEGAL);
    p.set_send_msg_max_timeout_per_request(2000);
    p.set_retry_another_broker_when_not_store_ok(true);
    let mut capped = 0usize;
    for i in 0..10 {
        let mut m = msg(topic, format!("retry-kernel-{i}").as_bytes(), "", "");
        if matches!(p.send(&mut m, Some(5000), None).await, Ok(r) if r.status == SendStatus::SendOk)
        {
            capped += 1;
        }
    }
    ck.check(
        "P8 a per-request cap plus the not-store-ok switch leaves healthy sends SEND_OK",
        capped == 10
            && p.get_send_msg_max_timeout_per_request() == 2000
            && p.is_retry_another_broker_when_not_store_ok()
            && p.is_retry_response_code(Some(response_code::MESSAGE_ILLEGAL)),
        &format!("send_ok={capped}/10"),
    );
    p.set_retry_another_broker_when_not_store_ok(false);
    p.set_send_msg_max_timeout_per_request(-1);

    // 路由彻底拉不到：这次连 namesrv 都拒了。Python 参考实现刻意**不**把传输层失败
    // 粉饰成「没有路由」（`mq_client.py` 的 `_fetch` 只在 broker 回了错误码时才往下传），
    // 所以这里断言的是：原样报连接失败，且一次 broker 都没联系、重试次数没被空转掉。
    let dead_addr = std::net::TcpListener::bind("127.0.0.1:0")
        .ok()
        .and_then(|l| l.local_addr().ok())
        .map(|a| a.to_string())
        .unwrap_or_else(|| "127.0.0.1:1".to_string());
    let orphan = DefaultMQProducer::new(&format!("PID_rust_orphan_{run}"))
        .map_err(|e| format!("orphan producer: {e}"))?;
    orphan.set_namesrv_addr(&dead_addr);
    orphan.set_instance_name(&format!("{run}-orphan"));
    if let Err(e) = orphan.start().await {
        orphan.shutdown();
        return Err(format!("orphan start against {dead_addr}: {e}"));
    }
    let mut m = msg(&format!("RustLiveNoRoute_{run}"), b"no-route", "", "");
    let began = Instant::now();
    let outcome = orphan.send(&mut m, Some(2000), None).await;
    let elapsed = began.elapsed();
    let detail = match &outcome {
        Ok(r) => format!("unexpectedly sent: {:?}", r.status),
        Err(e) => format!("{e}"),
    };
    let burned_retries = matches!(&outcome, Err(e) if e.to_string().contains("Send ["));
    ck.check(
        "P8 a dead name server surfaces the transport failure once, without burning retries",
        matches!(outcome.err(), Some(Error::Connect { .. }))
            && !burned_retries
            && elapsed < Duration::from_millis(500),
        &format!("elapsed={}ms {detail}", elapsed.as_millis()),
    );
    orphan.shutdown();
    Ok(())
}

// ------------------------------------------------------------------- P9

/// 读 broker 的一个配置项（`GET_BROKER_CONFIG` 的响应体是 properties 文本）。
async fn read_broker_flag(instance: &MQClientInstance, addr: &str, key: &str) -> Option<String> {
    let mut request = RemotingCommand::create_request_command(request_code::GET_BROKER_CONFIG, None);
    let response = instance
        .remoting_client()
        .invoke_sync(addr, &mut request, Some(5000))
        .await
        .ok()?;
    if response.code != response_code::SUCCESS {
        return None;
    }
    let text = String::from_utf8_lossy(response.body().unwrap_or_default()).into_owned();
    MixAll::string_to_properties(&text).get(key).map(str::to_string)
}

/// 只改一个配置项（Java `updateBrokerConfig` 按 Properties 逐键反射赋值，
/// 所以单键请求不会碰到其它设置）。
async fn write_broker_flag(
    instance: &MQClientInstance,
    addr: &str,
    key: &str,
    value: &str,
) -> Result<(), String> {
    let mut props = StringMap::new();
    props.insert(key, value);
    let mut request = RemotingCommand::create_request_command(request_code::UPDATE_BROKER_CONFIG, None);
    request.set_body(Some(MixAll::properties_to_string(&props, false).into_bytes()));
    let response = instance
        .remoting_client()
        .invoke_sync(addr, &mut request, Some(5000))
        .await
        .map_err(|e| e.to_string())?;
    if response.code == response_code::SUCCESS {
        return Ok(());
    }
    Err(format!(
        "code={} remark={:?}",
        response.code, response.remark
    ))
}

/// 定时消息的 `recallMessage`(370)：handle 由 SEND 响应下发，撤回后消息**不会**投递。
///
/// ⚠ broker 默认 `recallMessageEnable=false`（Java `BrokerConfig:546`），本机集群也是关的，
/// 所以这里临时打开、跑完还原；还原失败记 FAILED，不静默改共享集群的配置。
async fn p9_recall(
    p: &DefaultMQProducer,
    instance: &MQClientInstance,
    topic: &str,
    broker_name: &str,
    broker_addr: &str,
    ck: &mut Checker,
) -> Live {
    const ENABLE_KEY: &str = "recallMessageEnable";
    let prior = read_broker_flag(instance, broker_addr, ENABLE_KEY).await;
    if prior.as_deref() != Some("true") {
        write_broker_flag(instance, broker_addr, ENABLE_KEY, "true")
            .await
            .map_err(|e| format!("cannot enable {ENABLE_KEY} on the broker: {e}"))?;
    }
    // 定时消息的投递时刻：留够「先撤回、再确认没投递」的窗口。
    let delay_secs = 12;

    let mut to_recall = msg(topic, b"timer-recalled", "TagTimer", "keyTimerRecalled");
    to_recall.put_property("TIMER_DELAY_SEC", &delay_secs.to_string());
    let recalled = p
        .send(&mut to_recall, Some(5000), None)
        .await
        .map_err(|e| format!("timer send failed: {e}"))?;
    let mut control = msg(topic, b"timer-kept", "TagTimer", "keyTimerKept");
    control.put_property("TIMER_DELAY_SEC", &delay_secs.to_string());
    let kept = p
        .send(&mut control, Some(5000), None)
        .await
        .map_err(|e| format!("control timer send failed: {e}"))?;
    let mut plain = msg(topic, b"timer-none", "TagTimer", "keyTimerNone");
    let plain = p
        .send(&mut plain, Some(5000), None)
        .await
        .map_err(|e| format!("plain send failed: {e}"))?;

    ck.check(
        "P9 a timer send carries the broker's recallHandle",
        recalled.recall_handle.as_deref().is_some_and(|h| !h.is_empty())
            && kept.recall_handle.as_deref().is_some_and(|h| !h.is_empty()),
        &format!("{:?}", recalled.recall_handle),
    );
    ck.check(
        "P9 a normal send carries no recallHandle",
        plain.recall_handle.is_none(),
        &format!("{:?}", plain.recall_handle),
    );

    // handle 里就带着 brokerName，解出来必须是我们发出去的那台 broker。
    let handle = recalled.recall_handle.clone().unwrap_or_default();
    let decoded = recall_message_handle::decode_handle(&handle);
    ck.check(
        "P9 the handle the broker sent back decodes with our codec",
        decoded.as_ref().is_ok_and(|h| {
            h.topic == topic && h.broker_name == broker_name && h.message_id == recalled.msg_id.clone().unwrap_or_default()
        }),
        &format!("{decoded:?} topic={topic} broker={broker_name} msgId={:?}", recalled.msg_id),
    );

    let outcome = p.recall_message(topic, &handle).await;
    let want = recalled.msg_id.clone().unwrap_or_default();
    ck.check(
        "P9 recall_message answers with the recalled message's uniqKey",
        outcome.as_ref().ok().map(String::as_str) == Some(want.as_str()),
        &format!("{outcome:?} want={want}"),
    );

    // 本地校验：两类错误都不该产生任何 IO（Java 的顺序：状态 → topic 名 → %RETRY%/%DLQ%
    // → handle 解码 → 才谈路由）。
    let retry_err = p
        .recall_message(
            &format!("{}{PULL_GROUP}", MixAll::RETRY_GROUP_TOPIC_PREFIX),
            &handle,
        )
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    ck.check(
        "P9 recalling on a %RETRY% topic is refused with Java's message",
        retry_err.contains("topic is not supported"),
        &retry_err,
    );
    let started = Instant::now();
    let bad_err = p
        .recall_message(topic, "not-a-handle")
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    ck.check(
        "P9 a corrupt handle fails locally, before any IO",
        bad_err.contains("recall handle is invalid") && started.elapsed() < Duration::from_millis(200),
        &format!("{}ms {bad_err}", started.elapsed().as_millis()),
    );

    // 到点之后：被撤回的那条不该出现，对照组必须出现。
    let delivered = wait_async(
        || async {
            read_all_bodies(instance, topic, broker_name, broker_addr, QUEUE_NUMS)
                .await
                .iter()
                .any(|b| b == "timer-kept")
        },
        Duration::from_secs(delay_secs as u64 + 20),
    )
    .await;
    let bodies = read_all_bodies(instance, topic, broker_name, broker_addr, QUEUE_NUMS).await;
    ck.check(
        "P9 the control timer message is still delivered on time",
        delivered && bodies.iter().any(|b| b == "timer-kept"),
        &format!("bodies={}", bodies.len()),
    );
    ck.check(
        "P9 the recalled timer message is never delivered",
        !bodies.iter().any(|b| b == "timer-recalled"),
        &format!("bodies={}", bodies.len()),
    );

    let restore = match prior.as_deref() {
        Some(value) => write_broker_flag(instance, broker_addr, ENABLE_KEY, value).await,
        // broker 没这个键（更老的版本）：留 true 会改变 broker 行为，报出来让人知道。
        None => Err("broker reported no recallMessageEnable to restore".to_string()),
    };
    let restore_detail = restore.clone().err().unwrap_or_default();
    ck.check(
        "P9 recallMessageEnable is restored on the broker",
        restore.is_ok(),
        &restore_detail,
    );
    Ok(())
}

// ------------------------------------------------- P11 退出时的 broker 侧注销

/// 204 现在能看到这个生产组的 clientId 列表；broker 说「组不存在」时回空表。
async fn producer_clients(admin: &DefaultMQAdminExt, group: &str) -> Vec<String> {
    match admin.examine_producer_connection_info(group, None).await {
        Ok(conn) => conn
            .connection_set
            .iter()
            .map(|c| c.client_id.clone().unwrap_or_default())
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// P11：生产者退出必须逐台 broker 发 `UNREGISTER_CLIENT`(35)。
///
/// Java 链路：`DefaultMQProducerImpl#shutdown:313` → `MQClientInstance#unregisterProducer:1198-1201`
/// → 私有 `unregisterClient(group, null):1158-1182`。判别式在**同一条连接**上：
/// 同一个 `instanceName` ⇒ 同一个 clientId ⇒ 同一个 `MQClientInstance` ⇒ 每台 broker 一条
/// TCP 连接（Python/C++/.NET 每个生产者各自一份实例，做不了这条判据，那边改用抓帧证明）。
/// 于是先退场的 A 并不会把连接关掉（`MQClientInstance#shutdown` 的租户守卫让实例活着，
/// broker 那条 channel 依旧为 B 服务），这种情况下 A 的组能从 `ProducerManager.groupChannelTable`
/// 里消失，只有一发被 broker 接受的 35 解释得通 —— 不发的话只能等通道断开或 120s 扫描。
async fn p11_producer_unregister(namesrv: &str, topic: &str, run: &str, ck: &mut Checker) -> Live {
    let shared = format!("{run}-unreg");
    let group_a = format!("PID_rust_unreg_a_{run}");
    let group_b = format!("PID_rust_unreg_b_{run}");

    let a = DefaultMQProducer::new(&group_a).map_err(|e| format!("producer a: {e}"))?;
    a.set_namesrv_addr(namesrv);
    a.set_instance_name(&shared);
    a.start().await.map_err(|e| format!("a start: {e}"))?;
    let b = DefaultMQProducer::new(&group_b).map_err(|e| format!("producer b: {e}"))?;
    b.set_namesrv_addr(namesrv);
    b.set_instance_name(&shared);
    b.start().await.map_err(|e| format!("b start: {e}"))?;
    let id_a = a.client_id().unwrap_or_default();
    let id_b = b.client_id().unwrap_or_default();
    ck.check(
        "P11 same instanceName means one clientId and one shared instance",
        !id_a.is_empty() && id_a == id_b && MQClientInstance::find_instance(&id_a).is_some(),
        &format!("a={id_a} b={id_b}"),
    );

    let mut ma = msg(topic, b"unreg-a", "", "");
    let sent_a = a
        .send(&mut ma, Some(5000), None)
        .await
        .map(|r| r.status == SendStatus::SendOk)
        .unwrap_or(false);
    let mut mb = msg(topic, b"unreg-b", "", "");
    let sent_b = b
        .send(&mut mb, Some(5000), None)
        .await
        .map(|r| r.status == SendStatus::SendOk)
        .unwrap_or(false);
    ck.check(
        "P11 both producers on the shared channel send",
        sent_a && sent_b,
        &format!("a={sent_a} b={sent_b}"),
    );

    let admin = DefaultMQAdminExt::new();
    admin.set_namesrv_addr(namesrv);
    admin
        .start()
        .await
        .map_err(|e| format!("admin start: {e}"))?;

    // 注册靠心跳上线（30s 一轮），必须轮询等
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut seen_a = producer_clients(&admin, &group_a).await;
    while !seen_a.contains(&id_a) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(1)).await;
        seen_a = producer_clients(&admin, &group_a).await;
    }
    ck.check(
        "P11 the broker sees producer A's registration (204)",
        seen_a.contains(&id_a),
        &format!("{seen_a:?}"),
    );
    let seen_b = producer_clients(&admin, &group_b).await;
    ck.check(
        "P11 the broker also sees producer B on that same channel",
        seen_b.contains(&id_b),
        &format!("{seen_b:?}"),
    );

    a.shutdown();
    // 注销挂在运行时任务上（`shutdown()` 是同步的），给足一拍再判定
    let budget = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let now = producer_clients(&admin, &group_a).await;
            if !now.contains(&id_a) {
                break now;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
    .await;
    ck.check(
        "P11 A's group is gone right after shutdown (not after a heartbeat timeout)",
        budget.is_ok(),
        &format!("last={:?}", budget.unwrap_or_default()),
    );
    ck.check(
        "P11 A's shutdown did not take the shared channel down (B still registered)",
        producer_clients(&admin, &group_b).await.contains(&id_b),
        "B 的连接也跟着没了 ⇒ 这条判据不成立",
    );
    let mut mb2 = msg(topic, b"unreg-b-after-a", "", "");
    let b_still_sends = b
        .send(&mut mb2, Some(5000), None)
        .await
        .map(|r| r.status == SendStatus::SendOk)
        .unwrap_or(false);
    ck.check(
        "P11 B still sends through that channel after A left",
        b_still_sends && b.is_started(),
        "",
    );
    ck.check(
        "P11 A itself is gone locally (producerTable entry removed)",
        !a.is_started() && a.client().is_none(),
        "",
    );

    b.shutdown();
    let released = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if MQClientInstance::find_instance(&id_a).is_none() {
                break true;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .unwrap_or(false);
    ck.check(
        "P11 the last tenant tears the shared instance down",
        released,
        &id_a,
    );
    admin.shutdown();
    Ok(())
}

// ------------------------------------------------------------------- main

async fn run(namesrv: &str) -> Checker {
    let run = stamp();
    let topic = format!("RustLiveProducer_{run}");
    let group = format!("PID_rust_producer_{run}");

    println!("== rocketmq rust live producer test ==");
    println!("   namesrv = {namesrv}");
    println!("   topic   = {topic}");
    println!("   group   = {group}");

    let mut ck = Checker::new();
    let probe = RemotingClient::new();
    // 先用裸 RPC 建 topic（P1 之前的生产者还没启动），再拿生产者跑各场景
    let created = bootstrap_topic(&probe, namesrv, &topic).await;
    probe.shutdown();
    if let Err(e) = created {
        ck.abort("P0 bootstrap", &e);
        return ck;
    }

    if let Err(e) = p1_lifecycle(namesrv, &topic, &run, &mut ck).await {
        ck.abort("P1 lifecycle", &e);
    }

    let p = match DefaultMQProducer::new(&group) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("P0 producer", &e.to_string());
            return ck;
        }
    };
    p.set_namesrv_addr(namesrv);
    p.set_instance_name(&format!("{run}-main"));
    // 让压缩阈值用默认 4 KiB，但显式打开故障规避开关的关闭态（Python 默认）
    p.set_send_latency_fault_enable(false);
    if let Err(e) = p.start().await {
        ck.abort("P0 start", &e.to_string());
        return ck;
    }
    let instance = match p.client() {
        Some(c) => c,
        None => {
            ck.abort("P0 client", "started producer has no instance");
            return ck;
        }
    };
    let route = match instance.get_topic_route_data(&topic).await {
        Some(r) => r,
        None => {
            ck.abort("P0 route", "the topic route is still missing");
            p.shutdown();
            return ck;
        }
    };
    let broker_name = route.broker_datas[0].broker_name.clone();
    let broker_addr = match route.broker_datas[0].select_broker_addr() {
        Some(a) => a,
        None => {
            ck.abort("P0 route", "route has no usable broker address");
            p.shutdown();
            return ck;
        }
    };
    println!("   broker  = {broker_name} @ {broker_addr}");

    let scenarios: Vec<(&str, Live)> = vec![
        (
            "P2 send paths",
            p2_send_paths(&p, &instance, &topic, &broker_name, &broker_addr, &mut ck).await,
        ),
        (
            "P3 hooks",
            p3_hooks(&p, &instance, &topic, &broker_name, &broker_addr, &mut ck).await,
        ),
        ("P4 trace seam", p4_trace_seam(namesrv, &topic, &run, &mut ck).await),
        (
            "P5 request-reply",
            p5_request_reply(&p, &topic, &broker_name, &mut ck).await,
        ),
        (
            "P6 transactions",
            p6_transactions(
                &p,
                &instance,
                namesrv,
                &topic,
                &broker_name,
                &broker_addr,
                &run,
                &mut ck,
            )
            .await,
        ),
        (
            "P7 admin",
            p7_admin(&p, &instance, namesrv, &topic, &broker_name, &broker_addr, &run, &mut ck).await,
        ),
        (
            "P8 send retry kernel",
            p8_send_retry_kernel(&p, &topic, &run, &mut ck).await,
        ),
        (
            "P9 recall",
            p9_recall(&p, &instance, &topic, &broker_name, &broker_addr, &mut ck).await,
        ),
        (
            "P11 producer unregister",
            p11_producer_unregister(namesrv, &topic, &run, &mut ck).await,
        ),
    ];
    for (name, outcome) in scenarios {
        if let Err(e) = outcome {
            ck.abort(name, &e);
        }
    }

    // 收尾：删主 topic（broker 配置 + namesrv 路由）
    match instance.delete_topic_in_broker(&broker_addr, &topic, 5000).await {
        Ok(()) => ck.check("P10 delete_topic_in_broker", true, ""),
        Err(e) => ck.check("P10 delete_topic_in_broker", false, &e.to_string()),
    }
    match instance.delete_topic_in_namesrv(&topic, 5000).await {
        Ok(()) => ck.check("P10 delete_topic_in_namesrv", true, ""),
        Err(e) => ck.check("P10 delete_topic_in_namesrv", false, &e.to_string()),
    }
    p.shutdown();
    ck
}

fn report(ck: &mut Checker) {
    println!("== summary: {} passed, {} failed ==", ck.passed, ck.failed.len());
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
