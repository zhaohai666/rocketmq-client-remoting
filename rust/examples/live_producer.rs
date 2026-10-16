//! Integration verification of `DefaultMQProducer` against a **real 5.5.1 broker**.
//!
//! Division of labour with the other live examples: `live_protocol.rs` covers protocol codec,
//! `live_rebalance_and_trace.rs` / `live_client_modules.rs` cover client modules, and
//! `live_mq_client.rs` covers the instance itself. This one covers the **producer facade** --
//! the producer is the layer above `MQClientInstance`, so the property unique to a real
//! cluster is "does the broker actually accept this request". The pinned-down items:
//!
//!
//! Usage (first start a local cluster per the runbook in project knowledge):
//! ```text
//! cargo run --example live_producer -- 127.0.0.1:9876
//! ```
//!
//! With two optional arguments it instead targets a cluster with **authentication enabled** (`authenticationEnabled=true`):
//! ```text
//! cargo run --example live_producer -- 127.0.0.1:9876 <accessKey> <secretKey>
//! ```
//! Extra rules then apply (behaviour is byte-identical to the first usage without credentials):
//! - every client installs the same ACL signature hook before starting, otherwise the broker will not accept its RPCs;
//! - run the P0 counter-proof first: a send **without** credentials must be rejected, and rejected with broker code 16
//!   (`NO_PERMISSION`), not "any error whatsoever" -- only a request that really reached the broker can even speak of signing;
//! - the signature hook is registered on the `MQClientInstance` remoting channel, which is reused per clientId, so the
//!   counter-proof probe uses its own exclusive instanceName and must be the **first** client started in this run;
//!   it shuts down right after and waits for the instance to leave the global table before the credentialed client starts;
//! - P9 only runs after writing `recallMessageEnable` to the broker (a broker config change);
//!   the authenticated run leaves it alone and records it as skipped.

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
use rocketmq_client_remoting::remoting::rpchook::{
    AclClientRPCHook, RPCHook, SessionCredentials,
};

/// Queue count of the main topic (P2's fixed-queue/selector assertions depend on it).
const QUEUE_NUMS: i32 = 4;
const COMPRESSIBLE_BODY_LEN: usize = 8 * 1024;
/// P5's reply-wait budget: far shorter than the broker's processing time, so the timeout branch is guaranteed to be hit.
const REQUEST_TIMEOUT_MS: i64 = 1_500;
/// P6's budget for waiting out the broker check-back: half messages are immune for a default 6s, plus a few seconds per scan round.
const CHECK_BACK_BUDGET: Duration = Duration::from_secs(90);
/// Consumer group: only used for pull/offset RPCs; this example creates no consumer.
const PULL_GROUP: &str = "CID_rust_live_producer";

// ------------------------------------------------------------------ skeleton

fn stamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

/// Assertion accumulator: all scenarios run before the summary; the first failure does not exit early.
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

/// Code the broker returns on signature rejection: every auth failure (missing username, bad signature, insufficient permission) collapses into this one.
const NO_PERMISSION: i32 = 16;

/// ACL credentials. Both empty = authentication disabled; every client then gets `None`,
/// i.e. no hook installed, byte-identical to the no-argument behaviour.
#[derive(Clone)]
struct Creds {
    access_key: String,
    secret_key: String,
}

impl Creds {
    fn from_args(access_key: Option<&String>, secret_key: Option<&String>) -> Creds {
        Creds {
            access_key: access_key.map(|s| s.trim().to_string()).unwrap_or_default(),
            secret_key: secret_key.map(|s| s.trim().to_string()).unwrap_or_default(),
        }
    }

    fn enabled(&self) -> bool {
        !self.access_key.is_empty() && !self.secret_key.is_empty()
    }

    /// Returns the hook for clusters that install one; `None` for clusters without credentials, which the caller passes straight through.
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
        access_key, secret_key,
    )))
}

fn denied(err: &Error) -> bool {
    err.response_code() == Some(NO_PERMISSION)
}

fn code_of(err: &Error) -> String {
    match err.response_code() {
        Some(code) => format!("code={code}"),
        None => "code=?".to_string(),
    }
}

/// P0 counter-proof: a producer carrying **no credentials at all** must have its send rejected by the broker with 16.
///
/// The probe must run as the first client and use an exclusive instanceName -- the signature hook hangs on channels
/// reused per clientId, and the first client sets that channel's tone. After use: shutdown + drop, then wait until the
/// instance has really disappeared from the global table before the credentialed client may start.
async fn anon_probe(namesrv: &str, run: &str, ck: &mut Checker) {
    let group = format!("PID_rust_producer_anon_{run}");
    let topic = format!("RustLiveProducerAnon_{run}");
    let anon = match DefaultMQProducer::new(&group) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("P0 build the anonymous probe", &e.to_string());
            return;
        }
    };
    anon.set_namesrv_addr(namesrv);
    anon.set_instance_name(&format!("{run}-anon"));
    // start() already does RPCs (instance registration, heartbeat): a rejection there also satisfies the code-16 assertion.
    if let Err(e) = anon.start().await {
        ck.check(
            "P0 the anonymous client is rejected by the broker",
            denied(&e),
            &format!("start stage {}", code_of(&e)),
        );
        return;
    }
    let probe_id = anon.client_id().unwrap_or_default();
    let mut m = msg(&topic, b"should-not-be-accepted", "", "");
    match anon.send(&mut m, Some(5000), None).await {
        Ok(r) => ck.check(
            "P0 the anonymous client is rejected by the broker",
            false,
            &format!("the broker actually accepted an anonymous request: {r:?} (most likely authenticationEnabled is off)"),
        ),
        Err(e) => {
            let remark = e.to_string();
            ck.check(
                "P0 the anonymous client is rejected by the broker",
                denied(&e),
                &format!("{} remark={remark}", code_of(&e)),
            );
            // Only reasons like "username cannot be null." prove the probe was truly bare;
            // printing it distinguishes "never signed" from "signed wrongly".
            println!("  [INFO] P0 rejection reason: {remark}");
        }
    }
    anon.shutdown();
    drop(anon);
    let released = {
        let id = probe_id.clone();
        tokio::time::timeout(Duration::from_secs(20), async move {
            loop {
                if MQClientInstance::find_instance(&id).is_none() {
                    break true;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or(false)
    };
    ck.check(
        "P0 the anonymous probe's instance has left the table (leaving no channel behind for later clients)",
        released,
        &format!("clientId={probe_id} still in the global table"),
    );
    // No sync point exists between channel teardown and the broker side closing the connection; allow one more beat.
    tokio::time::sleep(Duration::from_secs(1)).await;
}

/// Polls an async condition until it holds; on timeout returns `false` so the caller can print the scene.
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

/// Observation point for async send callbacks: every send ends in success or failure, and the **terminal state arrives exactly once**.
///
/// A one-shot `ClosureSendCallback` cannot support this kind of scenario (its closure is consumed on take), and "did
/// some send get quietly swallowed" is exactly where the async kernel most easily goes wrong.
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

/// Reads back the body of the message sitting at the `(queue, offset)` the async callback reported.
///
/// "The callback said SEND_OK" only proves the client believes it; the message really lying at that offset proves the
/// async chain crossed the network and the broker accepted the request. Commitlog visibility lags by milliseconds, so retry until the budget runs out.
async fn body_at(
    instance: &MQClientInstance,
    mq: &MessageQueue,
    offset: i64,
    broker_addr: &str,
    budget: Duration,
) -> Option<Vec<u8>> {
    let deadline = Instant::now() + budget;
    loop {
        // Pulls fail outright while the queue has just been created; that does not mean "not committed yet", so keep polling until the budget runs out.
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

/// Decodes the name server's fastjson-style route (numeric map keys are unquoted).
fn decode_route(resp: &RemotingCommand, what: &str) -> Result<TopicRouteData, String> {
    let raw = match resp.body() {
        Some(b) => b.to_vec(),
        None => return Err(format!("{what} response has empty body")),
    };
    let value = RemotingSerializable::decode(&raw)
        .map_err(|e| format!("{what} body is not parsable: {e}"))?;
    TopicRouteData::from_json_value(&value).map_err(|e| format!("{what} route invalid: {e}"))
}

/// Pre-creates the main topic (via bare RPC, the same approach as `live_mq_client`); P7 then separately
/// verifies the producer's own `create_topic`.
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

/// The common shape of the instance-side `pull_message`: subscription expression `*`, no suspended wait.
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
            // An explicit addr: bypasses pullFromWhichNodeTable routing (the pre-P5 path)
            None,
        )
        .await
        .map_err(|e| format!("pull {} queue {} failed: {e}", mq.topic, mq.queue_id))
}

/// Bodies read from every queue of one topic (a single snapshot).
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
        // A queue that cannot be pulled does not break the global snapshot: what matters here is "what was read", not error details.
        if let Ok(r) = pull_from(instance, &mq, 0, broker_addr).await {
            out.extend(bodies(&r.msg_found_list));
        }
    }
    out
}

// ---------------------------------------------------------- fake dispatcher for P4

/// Minimal dispatcher satisfying the producer trace seam (`TraceDispatcher` +
/// `TraceReportSink`): it only collects `TraceContext`s for the assertions -- proving
/// the chain "inject → hook registration → start → record received" holds on real sends, without the real trace topic.
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

// ---------------------------------------------------------- recording hook for P3

/// Hook recording the before/after order and what after observed.
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

/// Interception hook that rejects sends (P3 asserts the message never lands).
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

/// Records that the end-transaction hook was invoked (P4's EndTransaction trace verifies this chain too).
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

// ------------------------------------------------------ transaction listener for P6

/// Local transactions return the preset state; check-backs return `check_state` and record the checked message.
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

/// Lifecycle: fail before start, start mints an identity, shutdown is idempotent and restart is allowed.
async fn p1_lifecycle(
    namesrv: &str,
    topic: &str,
    run: &str,
    creds: &Creds,
    ck: &mut Checker,
) -> Live {
    let group = format!("PID_rust_producer_{run}_p1");
    let p = DefaultMQProducer::with_rpc_hook(&group, creds.hook())
        .map_err(|e| format!("build failed: {e}"))?;
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
        "P1 the instance is registered under that clientId",
        MQClientInstance::find_instance(&client_id).is_some(),
        &client_id,
    );
    // Idempotent: a second start neither rebuilds the instance nor errors
    let before = p.client().map(|c| c.client_id().to_string());
    p.start().await.map_err(|e| format!("second start failed: {e}"))?;
    let after = p.client().map(|c| c.client_id().to_string());
    ck.check(
        "P1 start() is idempotent",
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
    // Restart allowed (the instance and heartbeat threads are started anew)
    p.start().await.map_err(|e| format!("restart failed: {e}"))?;
    let mut m = msg(topic, b"after-restart", "", "");
    let sent = p.send(&mut m, Some(5000), None).await;
    let err = sent.as_ref().err().map(|e| e.to_string()).unwrap_or_default();
    ck.check(
        "P1 a producer can be started again after shutdown and still sends",
        sent.map(|r| r.status == SendStatus::SendOk).unwrap_or(false),
        &err,
    );
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
            "{} shutdown+drain took {:?} first error {:?}",
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

/// Six send paths + compression, each checked against the actual broker-side outcome.
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

    // ---- sync send: SendResult field by field
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

    // ---- fixed-queue send: must land in exactly the given queue
    let target = MessageQueue::new(topic, broker_name, 2);
    let mut m2 = msg(topic, b"fixed-queue", "TagFixed", "");
    let r2 = p.send(&mut m2, Some(5000), Some(&target)).await
        .map_err(|e| format!("fixed send failed: {e}"))?;
    ck.check(
        "P2 sending to an explicit MessageQueue lands exactly there",
        r2.message_queue.as_ref() == Some(&target),
        &format!("{:?}", r2.message_queue),
    );

    // ---- batch
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

    // ---- oneway: no response, but it must still be readable back
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

    // ---- selector (ordered messages): one arg must pin one queue
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

    // ---- async send kernel: the callback's SendResult matches the sync shape, and the message really is on the broker
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
        "P2 send_async drives the success callback with a fully-shaped SendResult",
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

    // ---- the pool swallows nothing: 30 concurrent sends, each with exactly one terminal callback, each readable at the offset its callback reported
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
            "read back {} messages, {} offsets, missing {:?}",
            read_back.len(),
            slots.len(),
            want.difference(&read_back).count()
        ),
    );

    // ---- targeted async send: the queue handed in is the queue it lands in
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
    let canonical_wording = rejected_errors
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
        "P2 an in-pool validator failure callbacks once with the exact text and leaves the producer usable",
        rejected_back
            && rejected.done() == 1
            && rejected.ok() == 0
            && canonical_wording
            && still_works,
        &format!("{} / one follow-up send {}", rejected.summary(), after_reject.summary()),
    );

    // ---- compression: an 8 KiB compressible body → the reader gets the original body back
    let raw: Vec<u8> = vec![b'z'; COMPRESSIBLE_BODY_LEN];
    let mut big = msg(topic, &raw, "TagCompressed", "keyCompressed");
    let big_mq = MessageQueue::new(topic, broker_name, 0);
    let rb2 = p
        .send(&mut big, Some(5000), Some(&big_mq))
        .await
        .map_err(|e| format!("compressed send failed: {e}"))?;
    let client_body = big.get_body().to_vec();
    let body_restored = client_body == raw;
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
    // The broker stores the compressed stream we sent, so the whole message's storeSize is far below the raw body length.
    let plain_len = i32::try_from(COMPRESSIBLE_BODY_LEN).unwrap_or(i32::MAX);
    let went_compressed = stored.map(|m| m.store_size < plain_len).unwrap_or(false);
    let flag_ok = stored
        .map(|m| {
            !MessageSysFlag::is_compressed(m.sys_flag)
                && MessageSysFlag::get_compression_type(m.sys_flag) == MessageSysFlag::ZLIB_TYPE
        })
        .unwrap_or(false);
    ck.check(
        "P2 an over-threshold body is zlib'd on the wire and transparently restored for the reader",
        body_restored
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

/// Hooks: interception error propagation + before/after ordering and visibility.
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

    // Intercepted: the error surfaces as-is and the message never enters the broker
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

    // Passed through normally: one "before" then one "after ok"
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

    let before_len = hook.lines().len();
    let mut bad = msg(topic, b"hook-fails", "TagAllowed", "");
    let bad_mq = MessageQueue::new(topic, broker_name, QUEUE_NUMS * 256);
    let bad_err = p.send(&mut bad, Some(5000), Some(&bad_mq)).await.err();
    let err = bad_err.as_ref().map(|e| e.to_string()).unwrap_or_default();
    let tail = hook.lines()[before_len..].to_vec();
    let absent = !read_all_bodies(instance, topic, broker_name, broker_addr, QUEUE_NUMS)
        .await
        .iter()
        .any(|b| b == "hook-fails");
    ck.check(
        "P3 a failing send still runs the after hook, with the exception on the context",
        bad_err
            .as_ref()
            .and_then(|e| e.response_code())
            == Some(response_code::INVALID_PARAMETER)
            && err.contains("is illegal")
            && tail.len() == 2
            && tail[0].starts_with("before ")
            && tail[1].starts_with("after err ")
            && absent,
        &format!("err={err} tail={tail:?}"),
    );

    let before_len = hook.lines().len();
    let mut bogus_msg = msg(topic, b"must-not-land-bogus", "TagAllowed", "");
    let bogus_mq = MessageQueue::new(topic, "RustNoSuchBroker_zzz", 0);
    let bogus = p
        .send(&mut bogus_msg, Some(5000), Some(&bogus_mq))
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    let tail = hook.lines()[before_len..].to_vec();
    ck.check(
        "P3b send to a nonexistent broker: this side reports not exist, no hook runs at all (validation happens outside the try)",
        bogus.contains("The broker[RustNoSuchBroker_zzz] not exist") && tail.is_empty(),
        &format!("err={bogus} tail={tail:?}"),
    );
    Ok(())
}

// ---------------------------------------------------------------------- P4

/// Trace seam: inject → start registers the hook and boots it → a real send produces a TraceContext.
async fn p4_trace_seam(
    namesrv: &str,
    topic: &str,
    run: &str,
    creds: &Creds,
    ck: &mut Checker,
) -> Live {
    let group = format!("PID_rust_producer_{run}_p4");
    let p = DefaultMQProducer::with_rpc_hook(&group, creds.hook())
        .map_err(|e| format!("build failed: {e}"))?;
    p.set_namesrv_addr(namesrv);
    let dispatcher = FakeDispatcher::new();
    // FakeDispatcher implements both TraceDispatcher + TraceReportSink; via the blanket impl in the producer
    // module it can be used directly as Arc<dyn TraceDispatcherChannel>.
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

    // Trace messages themselves must not produce traces (avoiding loops): messages whose topic starts with the trace topic are skipped
    let mut traced = msg(
        &format!("{}%x", MixAll::TRACE_TOPIC),
        b"trace-of-trace",
        "",
        "",
    );
    let _ = p.send(&mut traced, Some(3000), None).await;
    let count_after = dispatcher.reported().len();
    ck.check(
        "P4 a message on the trace topic itself is never traced",
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

/// Request-Reply: the three request properties, the wait-slot lifecycle, and the timeout semantics when nobody answers.
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

/// Transactions: half message invisible → COMMIT makes it visible / ROLLBACK never visible / UNKNOW committed after a check-back.
#[allow(clippy::too_many_arguments)]
async fn p6_transactions(
    p: &DefaultMQProducer,
    instance: &MQClientInstance,
    namesrv: &str,
    topic: &str,
    broker_name: &str,
    broker_addr: &str,
    run: &str,
    creds: &Creds,
    ck: &mut Checker,
) -> Live {
    let end_hook = Arc::new(EndTxnHook::default());
    p.register_end_transaction_hook(end_hook.clone());

    // ---- COMMIT: once the half message is committed, the consumer can read it
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

    // ---- ROLLBACK: never readable
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

    // ---- UNKNOW → broker check-back (over the heartbeat-registered connection) → this time return COMMIT
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
    let last_hook = lock(&end_hook.last).clone().unwrap_or_default();
    ck.check(
        "P6 the last end-transaction hook call is the broker check-back, marked as such",
        last_hook.contains("CommitMessage") && last_hook.contains("fromCheck=true"),
        &last_hook,
    );

    // ---- TransactionMQProducer: once a listener is preset, the send call need not pass one
    // (this fallback is the only extra thing this subclass does here; pin it down.)
    let txn_group = format!("PID_rust_producer_{run}_p6txn");
    let txn = TransactionMQProducer::new(&txn_group).map_err(|e| format!("txn build failed: {e}"))?;
    txn.set_rpc_hook(creds.hook());
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
    // Deref to the base producer: the admin methods keep working
    let via_deref = txn
        .fetch_publish_message_queues(topic)
        .await
        .map(|v| v.len())
        .unwrap_or(0);
    ck.check(
        "P6 TransactionMQProducer derefs to the base producer",
        via_deref == QUEUE_NUMS as usize,
        &format!("queues={via_deref}"),
    );
    txn.shutdown();
    Ok(())
}

// ---------------------------------------------------------------------- P7

/// Admin conveniences: create a topic, the four offset RPCs, query by key, and view_message reporting an error.
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
    // ---- create_topic: name only; the broker creates queues from its defaults
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

    // ---- the four offset RPCs
    let mq = MessageQueue::new(topic, broker_name, 0);
    let max_offset = p.max_offset(&mq).await.map_err(|e| format!("max_offset: {e}"))?;
    let min_offset = p.min_offset(&mq).await.map_err(|e| format!("min_offset: {e}"))?;
    // The baseline cannot be "now minus one minute": by here this run may already be minutes old (on a real TLS machine
    // waiting for the transaction check-back alone eats 90s), and searchOffset would then jump past earlier messages -- the criterion is coupled to duration.
    // The topic name carries this run's second-level timestamp and belongs to this run alone, so the run start instant precedes the first message.
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

    // ---- query by key (the broker builds its index asynchronously, so poll until found)
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

    // ---- view_message: explicitly unsupported
    let err = p
        .view_message(topic, "0102030405")
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    ck.check(
        "P7 view_message refuses instead of pretending",
        err.contains("not supported"),
        &err,
    );

    // ---- clean up the topics this example created
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

// ---------------------------------------------------------------- P8 retry kernel

/// The faces of the send retry kernel (`sendDefaultImpl`) that **can run on a real cluster**: the retryable-code set,
/// the per-request timeout cap, the switch that hops brokers on non-SEND_OK, and the characterisation when no route can be fetched at all.
///
/// "Broker answers a retryable code, so hop to another", "a slow broker eats the whole budget and reports call timeout",
/// "a refused port reports 10001" -- these need a broker actively returning an error code / slowness that eats the budget
/// / a port refusing connections. No healthy single-broker real cluster provides any of them; they are pinned in the
/// in-process fake cluster in `src/client/producer/send_retry_tests.rs`.
async fn p8_send_retry_kernel(
    p: &DefaultMQProducer,
    topic: &str,
    run: &str,
    creds: &Creds,
    ck: &mut Checker,
) -> Live {
    let defaults: Vec<i32> = DEFAULT_RETRY_RESPONSE_CODES.to_vec();
    ck.check(
        "P8 the default retryable broker codes are exactly the eight expected ones",
        defaults == [1, 2, 14, 16, 17, 204, 205, 1500]
            && p.get_retry_response_codes().len() == 8
            && p.is_retry_response_code(Some(response_code::SERVICE_NOT_AVAILABLE))
            // no response code at all (never even connected) counts as not retryable; codes outside the set are not retryable either
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

    // No route whatsoever: this time even the name server refuses. This port deliberately does **not** whitewash a transport failure into "no route"
    // (route-missing propagates upward only when the broker itself returned an error code), so what is asserted here: the connection failure is reported as-is,
    // not a single broker is ever contacted, and the retry budget is not idled away.
    let dead_addr = std::net::TcpListener::bind("127.0.0.1:0")
        .ok()
        .and_then(|l| l.local_addr().ok())
        .map(|a| a.to_string())
        .unwrap_or_else(|| "127.0.0.1:1".to_string());
    let orphan = DefaultMQProducer::with_rpc_hook(
        &format!("PID_rust_orphan_{run}"),
        creds.hook(),
    )
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

/// Reads one broker config item (the `GET_BROKER_CONFIG` response body is properties text).
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

/// The timer message's `recallMessage` (370): the handle comes down with the SEND response; after recall the message is **never** delivered.
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
    // Timer delivery instant: leave enough window to recall first and then confirm nothing was delivered.
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

    // The handle itself carries the brokerName; decoding it must yield the broker we sent to.
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
        "P9 recalling on a %RETRY% topic is refused with the expected message",
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

    // Once due: the recalled message must not appear, the control message must appear.
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
        // The broker lacks this key (an older version): leaving true behind would change broker behaviour, so surface it.
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

// --------------------------------------------- P11 broker-side deregistration on exit

/// 204 now shows the clientId list of this producer group; an empty list comes back when the broker says "group does not exist".
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

/// P11: a producer exiting must send `UNREGISTER_CLIENT` (35) to every broker.
async fn p11_producer_unregister(
    namesrv: &str,
    topic: &str,
    run: &str,
    creds: &Creds,
    ck: &mut Checker,
) -> Live {
    let shared = format!("{run}-unreg");
    let group_a = format!("PID_rust_unreg_a_{run}");
    let group_b = format!("PID_rust_unreg_b_{run}");

    let a =
        DefaultMQProducer::with_rpc_hook(&group_a, creds.hook()).map_err(|e| format!("producer a: {e}"))?;
    a.set_namesrv_addr(namesrv);
    a.set_instance_name(&shared);
    a.start().await.map_err(|e| format!("a start: {e}"))?;
    let b =
        DefaultMQProducer::with_rpc_hook(&group_b, creds.hook()).map_err(|e| format!("producer b: {e}"))?;
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
    admin.set_rpc_hook(creds.hook());
    admin
        .start()
        .await
        .map_err(|e| format!("admin start: {e}"))?;

    // Registration rides on the first heartbeat after the route lands, not on the periodic round,
    // so keep the elapsed time: it is what separates a working beat from a 30s rescue.
    let begin = Instant::now();
    let deadline = begin + Duration::from_secs(15);
    let mut seen_a = producer_clients(&admin, &group_a).await;
    while !seen_a.contains(&id_a) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        seen_a = producer_clients(&admin, &group_a).await;
    }
    let waited = begin.elapsed();
    ck.check(
        "P11 the broker sees producer A's registration (204)",
        seen_a.contains(&id_a),
        &format!("{seen_a:?} waited={waited:?}"),
    );
    ck.check(
        "P11 registration lands with the route, not on the next 30s round",
        waited < Duration::from_secs(10),
        &format!("only listed after {waited:?}"),
    );
    let seen_b = producer_clients(&admin, &group_b).await;
    ck.check(
        "P11 the broker also sees producer B on that same channel",
        seen_b.contains(&id_b),
        &format!("{seen_b:?}"),
    );

    a.shutdown();
    // Deregistration rides on a runtime task (`shutdown()` itself is synchronous); give it a full beat before judging
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
        "B's connection would have vanished with it ⇒ this criterion does not hold",
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

async fn run(namesrv: &str, creds: Creds) -> Checker {
    let run = stamp();
    let topic = format!("RustLiveProducer_{run}");
    let group = format!("PID_rust_producer_{run}");

    println!("== rocketmq rust live producer test ==");
    println!("   namesrv = {namesrv}");
    println!("   topic   = {topic}");
    println!("   group   = {group}");
    println!(
        "   acl     = {}",
        if creds.enabled() {
            format!("on (accessKey={})", creds.access_key)
        } else {
            "off".to_string()
        }
    );

    let mut ck = Checker::new();

    // With credentials, run the counter-proof first: at this moment no signed client exists yet, so the probe gets its own independent channel.
    if creds.enabled() {
        anon_probe(namesrv, &run, &mut ck).await;
    }

    let probe = RemotingClient::new();
    if let Some(hook) = creds.hook() {
        probe.register_rpc_hook(hook);
    }
    // Create the topic via bare RPC first (the P1 producer is not started yet), then run the scenarios through the producer
    let created = bootstrap_topic(&probe, namesrv, &topic).await;
    probe.shutdown();
    if let Err(e) = created {
        ck.abort("P0 bootstrap", &e);
        return ck;
    }

    if let Err(e) = p1_lifecycle(namesrv, &topic, &run, &creds, &mut ck).await {
        ck.abort("P1 lifecycle", &e);
    }

    let p = match DefaultMQProducer::with_rpc_hook(&group, creds.hook()) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("P0 producer", &e.to_string());
            return ck;
        }
    };
    p.set_namesrv_addr(namesrv);
    p.set_instance_name(&format!("{run}-main"));
    // Keep the compression threshold at the default 4 KiB, but explicitly set the fault-avoidance switch to its off state
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

    let mut scenarios: Vec<(&str, Live)> = vec![
        (
            "P2 send paths",
            p2_send_paths(&p, &instance, &topic, &broker_name, &broker_addr, &mut ck).await,
        ),
        (
            "P3 hooks",
            p3_hooks(&p, &instance, &topic, &broker_name, &broker_addr, &mut ck).await,
        ),
        ("P4 trace seam", p4_trace_seam(namesrv, &topic, &run, &creds, &mut ck).await),
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
                &creds,
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
            p8_send_retry_kernel(&p, &topic, &run, &creds, &mut ck).await,
        ),
        (
            "P11 producer unregister",
            p11_producer_unregister(namesrv, &topic, &run, &creds, &mut ck).await,
        ),
    ];

    // P9 only runs after writing `recallMessageEnable` to the broker (change first, then restore).
    // The authenticated run leaves cluster config untouched: skip the whole section and say so in the summary; without credentials everything still runs.
    if creds.enabled() {
        println!("  [SKIP] P9 recall: needs a broker config change (recallMessageEnable) first; the authenticated run leaves cluster config untouched");
    } else {
        scenarios.push((
            "P9 recall",
            p9_recall(&p, &instance, &topic, &broker_name, &broker_addr, &mut ck).await,
        ));
    }
    for (name, outcome) in scenarios {
        if let Err(e) = outcome {
            ck.abort(name, &e);
        }
    }

    // Wrap-up: delete the main topic (broker config + name server route)
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
    let mut ck = run(&namesrv, Creds::from_args(argv.get(2), argv.get(3))).await;
    report(&mut ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
