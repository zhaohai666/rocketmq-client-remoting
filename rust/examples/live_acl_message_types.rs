//! Live verification of "all message types" **with ACL credentials**: one assertion per type, each ending with "read back from the broker".
//!
//! Why this example exists: on a plain cluster "sent + received" only proves the link works, not the signing path.
//! With authentication on (broker `authenticationEnabled=true`), sending and consuming every message type must pass
//! the broker's gate on the request's ACL signature, so "SEND_OK" comes to mean "the broker accepted this signature".
//! Covered types (none skipped):
//! - A0 negative control: a send **without credentials** must be rejected as NO_PERMISSION(16).
//!   Without it, "all green with credentials" on a cluster where auth was never actually enabled looks identical to
//!   "signature accepted" -- a total false pass. The assertion pins code 16, not merely "some error":
//!   only 16 proves the request truly reached the broker as-is and was stopped by the auth gate, not local validation or a dead link.
//! - A1 sync normal message: SEND_OK and the body read back at the callback's offset.
//! - A2 async send: the callback gets the full SendResult, and the message sits exactly at the reported `(queue, offset)`.
//! - A3 oneway send: no response; only the read-back count can back it up.
//! - A4 batch send (SEND_BATCH_MESSAGE): one physical write, sub-message offsets contiguous and all readable back.
//! - A5 delay message (`DELAY` level): `store_timestamp - born_timestamp` proves delivery really waited for the due time.
//! - A6 timer message (user property `TIMER_DELIVER_MS` set ~5s ahead): proven by the same metric.
//! - A7 transactional message: half message -> local transaction COMMIT -> END_TRANSACTION; read back proves it became consumable.
//! - A8 ordered message: the selector pins the same queue for one key; orderly consumption receives them in offset order.
//! - A9 tag-filtered consumption: the server filters by the subscription expression and delivers only TagA.
//! - A10 Keys + server-side query by key (QUERY_MSG_BY_KEY): that keyed message is queried back.
//! - A11 user property passthrough: custom properties put on the producer side arrive untouched in read-back and on the consumer side.
//! - A12 signed push consumer: heartbeat / long-polling pull / offset commit, all three RPCs signed end to end;
//!   missing any one signature means no messages arrive.
//!
//! Every role uses **its own** `instance_name` (-> distinct clientId -> distinct [`MQClientInstance`] ->
//! distinct remoting channel): the ACL hook is **registered on the channel**, so if two roles shared an instance,
//! "unsigned rejected" would be signed away by the other role's hook -- either a fake PASS or the unsigned
//! probe could not even be sent. So A0 always runs **before** every client, shuts down right after, and waits a beat.
//!
//! ⚠ A12 and A5/A6 must **start the consumer before sending**: a push consumer's fresh group resolves its initial
//! offset to the queue's maxOffset at that moment, so "send first, receive after" is not stable. Here
//! `CONSUME_FROM_FIRST_OFFSET` is explicit; both safeguards stay in place.
//!
//! Usage (the cluster must bring up nameServer + broker configured for ACL; without the last two arguments it
//! falls back to plain-cluster behavior: no hooks installed, behavior unchanged from before):
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

/// 4 queues per topic (A8's "same key same queue" and the read-back snapshot both rely on it).
const QUEUE_NUMS: i32 = 4;
/// All broker-side auth failures collapse into NO_PERMISSION(16).
const NO_PERMISSION: i32 = 16;
/// Level 3 of the default delay-level table `1s 5s 10s 30s ...`.
const DELAY_LEVEL: i32 = 3;
/// Delivery delay corresponding to level 3.
const DELAY_MILLIS: i64 = 10_000;
/// Milliseconds the timer message is set ahead.
const TIMER_AHEAD_MILLIS: i64 = 5_000;
/// Same-machine timestamps, so only a tiny slack: the "really delayed" verdict must not be loosened by it.
const CLOCK_SLACK: i64 = 500;
/// Consumer group for the read-back snapshot: pull RPC only, no consumer built.
const PULL_GROUP: &str = "CID_rust_acl_message_types";
/// Budget for waiting to read one message back from the broker.
const READ_BACK_BUDGET: Duration = Duration::from_secs(30);
/// Extra budget for waiting on delay/timer messages.
const DELIVER_BUDGET: Duration = Duration::from_secs(60);
/// After starting the consumers, time for the first rebalance to hand out queues.
const REBALANCE_WAIT: Duration = Duration::from_secs(5);
/// Wait after the unsigned probe shuts down so its instance is fully released.
const PROBE_RELEASE_WAIT: Duration = Duration::from_secs(1);

// ------------------------------------------------------------------ skeleton

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

/// Assertion accumulator: run all types, then summarize; the first failure does not exit early.
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

/// Polls an async condition until it holds; on timeout returns `false` so the caller can print the scene.
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

// ------------------------------------------------------------------ credentials

/// Optional ACL credentials: "with credentials" only when both are non-empty.
///
/// When empty, [`Self::hook`] always returns `None`, and a client given `None` installs no hook --
/// byte-for-byte the same behavior as on a plain cluster.
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

/// Rejection assertion: must be the broker's NO_PERMISSION, not a dead link / timeout / local validation failure.
fn denied(err: &Error) -> bool {
    err.response_code() == Some(NO_PERMISSION)
}

fn code_of(err: &Error) -> String {
    match err.response_code() {
        Some(code) => format!("code={code}"),
        None => "code=?".to_string(),
    }
}

// ------------------------------------------------------------------ message helpers

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

/// One delivery recorded on the consumer side: body + tag/key/custom properties + offset + two timestamps.
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

/// One listener's inbox: both concurrent and orderly deliveries are recorded here.
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

/// Listener that records every body (one implementation shared by both consume modes).
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

/// Observation point for async sends: success or failure, the terminal state comes exactly once.
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

/// Local transactions always commit; the check-back commits too (the normal path never reaches it).
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

// ------------------------------------------------------------------ read-back

/// Cluster location result: one broker's name and address.
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

/// A pull with a `*` expression, no suspension: read-back only asks the broker for existing messages, never moves offsets.
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

/// One snapshot of all queues of the topic (from offset 0).
async fn snapshot(instance: &MQClientInstance, topic: &str, cluster: &Cluster) -> Vec<MessageExt> {
    let mut out = Vec::new();
    for q in 0..QUEUE_NUMS {
        let mq = MessageQueue::new(topic, &cluster.broker_name, q);
        // A queue that fails to pull does not affect the global snapshot: what matters here is "what was read".
        if let Ok(r) = pull(instance, &mq, 0, &cluster.broker_addr).await {
            out.extend(r.msg_found_list);
        }
    }
    out
}

/// Waits until the given bodies appear in a topic's snapshot and returns the matching messages found.
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

/// Reads back exactly the message at `(queue, offset)` (used by the async callback's offset assertion).
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

/// Quantified metric for "delivered only at the due time": broker store time minus client-born time.
fn drift_ms(m: &MessageExt) -> i64 {
    m.store_timestamp - m.born_timestamp
}

// ------------------------------------------------------------------ A0 negative control

/// The unsigned probe must be the **first** client to run.
///
/// This crate shares [`MQClientInstance`] in-process by clientId, and hooks register on the instance's
/// remoting channel: if the probe shared a name with the later signed clients, that channel would sign
/// it away and "rejected" would vanish. So it gets its own instanceName, shuts down right after, and waits a beat.
async fn a0_unsigned_probe(namesrv: &str, probe_topic: &str, run: &str, ck: &mut Checker) {
    println!("\nA0 a send without credentials must be rejected by the broker (NO_PERMISSION=16)");
    let group = format!("PID_rust_acl_types_anon_{run}");
    let anon = match DefaultMQProducer::new(&group) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("A0 build anonymous producer", &e.to_string());
            return;
        }
    };
    anon.set_namesrv_addr(namesrv);
    anon.set_instance_name(&format!("{run}-anon"));
    if let Err(e) = anon.start().await {
        ck.abort("A0 anonymous producer start", &format!("{} {e}", code_of(&e)));
        return;
    }
    let mut m = Message::new(probe_topic, Some(b"should-not-be-accepted"));
    m.set_tags("TagAnon");
    match anon.send(&mut m, Some(5000), None).await {
        Ok(r) => ck.check(
            "A0 the unsigned send was rejected by the broker",
            false,
            &format!("the broker actually accepted an anonymous request: {r:?} (most likely authenticationEnabled is off)"),
        ),
        Err(e) => {
            let remark = e.to_string();
            ck.check(
                "A0 the unsigned send was rejected by the broker",
                denied(&e),
                &format!("{} remark={remark}", code_of(&e)),
            );
            // Only a probe truly without credentials gets a reason like "username cannot be null.";
            // printing it helps tell "signature computed wrong" from "never signed at all".
            println!("  [INFO] A0 rejection reason: {remark}");
        }
    }
    anon.shutdown();
    drop(anon);
    tokio::time::sleep(PROBE_RELEASE_WAIT).await;
}

// ------------------------------------------------------------------ main flow

/// Builds a signed push consumer (not subscribed): each role's own instanceName.
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
            "no credentials (plain-cluster mode, no hooks installed)".to_string()
        }
    );
    println!("   topic   = {topic} / {tag_topic} / {order_topic}");

    // A0 negative control: must run before any other client.
    if creds.enabled() {
        a0_unsigned_probe(namesrv, &anon_topic, &run, &mut ck).await;
    }

    // ---- Signed producer: topic creation, sends, read-back, and query-by-key all go through it
    let p = match DefaultMQProducer::with_rpc_hook(&producer_group, creds.hook()) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("producer build", &e.to_string());
            return ck;
        }
    };
    p.set_namesrv_addr(namesrv);
    p.set_instance_name(&format!("{run}-p"));
    if let Err(e) = p.start().await {
        ck.abort("producer start (signature registration)", &format!("{} {e}", code_of(&e)));
        return ck;
    }
    let instance = match p.client() {
        Some(c) => c,
        None => {
            ck.abort("producer instance", "started producer has no instance");
            return ck;
        }
    };

    for t in [&topic, &tag_topic, &order_topic] {
        match p.create_topic(t, QUEUE_NUMS, 0).await {
            Ok(()) => ck.check(&format!("created topic {t} with credentials"), true, ""),
            Err(e) => ck.check(
                &format!("created topic {t} with credentials"),
                false,
                &format!("{} {e}", code_of(&e)),
            ),
        }
    }
    let cluster = match instance.get_topic_route_data(&topic).await {
        Some(route) => match cluster_of(&route) {
            Ok(c) => c,
            Err(e) => {
                ck.abort("route resolution", &e);
                p.shutdown();
                return ck;
            }
        },
        None => {
            ck.abort("route", "namesrv has no route for this topic");
            p.shutdown();
            return ck;
        }
    };
    println!(
        "   broker  = {} @ {}",
        cluster.broker_name, cluster.broker_addr
    );

    // ---- Consumers start first: A12 consumes the main topic concurrently, A9 subscribes to TagA only, A8 orderly consumption
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
            ck.abort("A12 consumer build", &e);
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
            ck.abort("A9 consumer build", &e);
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
            ck.abort("A8 consumer build", &e);
            main_c.shutdown();
            tag_c.shutdown();
            p.shutdown();
            return ck;
        }
    };
    // One listener slot holds one kind: concurrent/orderly is decided by the registration entry (later registrations overwrite earlier ones).
    // A8 must go through the orderly entry, otherwise the ordering assertion would not exercise the orderly-consume core.
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
        // The same Recorder implements both listener traits; register it on the entry matching the mode.
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
            "consumer start (heartbeat/registration must be signed)",
            &e,
        );
        main_c.shutdown();
        tag_c.shutdown();
        order_c.shutdown();
        p.shutdown();
        return ck;
    }
    // Wait for the first rebalance to hand out queues: the initial offset must resolve while the topic is still empty.
    tokio::time::sleep(REBALANCE_WAIT).await;

    // ---- A1 sync normal message + A11 user properties + A10 keys
    let key_sync = format!("aclKeySync-{run}");
    println!("\nA1/A10/A11 sync normal message (with keys and user properties)");
    let mut m1 = msg(&topic, b"acl-sync", "TagSync", &key_sync);
    m1.put_property("city", "Hangzhou");
    m1.put_property("env", "prod");
    let r1 = match p.send(&mut m1, Some(5000), None).await {
        Ok(r) => r,
        Err(e) => {
            ck.abort("A1 sync send", &format!("{} {e}", code_of(&e)));
            finish(&main_c, &tag_c, &order_c, &p).await;
            return ck;
        }
    };
    let sync_mq = r1.message_queue.clone().unwrap_or_default();
    let back_sync = body_at(&instance, &sync_mq, r1.queue_offset, &cluster.broker_addr, READ_BACK_BUDGET).await;
    let sync_read = wait_bodies(&instance, &topic, &cluster, &["acl-sync"], READ_BACK_BUDGET).await;
    let sync_pulled = sync_read.first().cloned().unwrap_or_default();
    ck.check(
        "A1 sync normal message SEND_OK and read back the very same message from the broker",
        r1.status == SendStatus::SendOk
            && back_sync.as_deref() == Some(b"acl-sync".as_slice())
            && body_of(&sync_pulled) == "acl-sync"
            && sync_pulled.get_tags() == Some("TagSync"),
        &format!("{r1:?} read_back={:?} mq={}", back_sync, sync_mq),
    );
    ck.check(
        "A11 user properties city/env read back verbatim on the broker side",
        sync_pulled.get_property("city") == Some("Hangzhou")
            && sync_pulled.get_property("env") == Some("prod"),
        &format!("{:?}", sync_pulled.properties),
    );

    // ---- A2 async send
    println!("A2 async send (callback)");
    let probe = Arc::new(AsyncProbe::default());
    if let Err(e) = p.send_async(msg(&topic, b"acl-async", "TagAsync", ""), probe.clone(), Some(5000), None) {
        ck.abort("A2 send_async dispatch", &format!("{} {e}", code_of(&e)));
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
        "A2 the (queue, offset) reported by the async callback holds exactly that message",
        called && async_ok && back_async.as_deref() == Some(b"acl-async".as_slice()),
        &probe.summary(),
    );

    // ---- A3 oneway send: no response, so we can only rely on the read-back
    println!("A3 oneway send (oneway, no response)");
    let oneway_mq = MessageQueue::new(&topic, &cluster.broker_name, 3);
    let mut m3 = msg(&topic, b"acl-oneway", "TagOneway", "");
    if let Err(e) = p.send_oneway(&mut m3, Some(&oneway_mq)).await {
        ck.abort("A3 oneway send", &format!("{} {e}", code_of(&e)));
    }
    let oneway_back =
        wait_bodies(&instance, &topic, &cluster, &["acl-oneway"], READ_BACK_BUDGET).await;
    let oneway_count = snapshot(&instance, &topic, &cluster)
        .await
        .iter()
        .filter(|m| body_of(m) == "acl-oneway")
        .count();
    ck.check(
        "A3 oneway gets no response but the message really landed on the broker (read-back count)",
        oneway_back.len() == 1 && oneway_count == 1,
        &format!("back={} count={oneway_count}", oneway_back.len()),
    );

    // ---- A4 batch send
    println!("A4 batch send (SEND_BATCH_MESSAGE)");
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
            ck.abort("A4 batch send", e);
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
        "A4 three messages written in one batch, sub-message offsets contiguous and all readable back",
        batch_result.is_ok() && back_batch.len() == 3 && contiguous,
        &format!("{batch_result:?} offsets={offsets:?}"),
    );

    // ---- A7 transactional message: half message → local transaction COMMIT → END_TRANSACTION
    println!("A7 transactional message (half message → COMMIT → END_TRANSACTION → consumable)");
    let mut m7 = msg(&topic, b"acl-tx-commit", "TagTxn", "");
    let tx_result = p
        .send_message_in_transaction(&mut m7, Some(Arc::new(CommitAlways)), None)
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
        Err(e) => format!("{} {e}", code_of(e)),
    };
    let tx_send_ok = match tx_result.as_ref() {
        Ok(r) => {
            r.inner.status == SendStatus::SendOk
                && r.local_transaction_state == Some(LocalTransactionState::CommitMessage)
        }
        Err(_) => false,
    };
    // The read-back is exactly the proof that "END_TRANSACTION really took effect": the broker will
    // not deliver a half message before it is turned into a normal message.
    let back_tx = wait_bodies(&instance, &topic, &cluster, &["acl-tx-commit"], READ_BACK_BUDGET).await;
    ck.check(
        "A7 after the transaction commits the message becomes consumable (read-back proves END_TRANSACTION took effect)",
        tx_send_ok && back_tx.len() == 1,
        &tx_detail,
    );
    let tx_is_half = tx_result
        .as_ref()
        .ok()
        .and_then(|r: &TransactionSendResult| r.inner.transaction_id.clone())
        .is_some_and(|id| !id.is_empty());
    ck.check(
        "A7 the broker already assigned a transactionId to the transactional message during the half-message phase",
        tx_is_half,
        &tx_detail,
    );

    // ---- A8 ordered message: the same key must be pinned to one queue
    println!("A8 ordered message (selector maps the same key to the same queue + orderly consume keeps the order)");
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
        "A8 all 6 ordered messages with the same key landed in the same queue",
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
        "A8 the orderly consumer received these 6 messages in the offset order they have on the broker",
        orderly_delivered
            && broker_order.len() == 6
            && seen_bodies.len() == 6
            && same_queue
            && offsets_increasing
            && seen_bodies == broker_order,
        &format!(
            "broker={broker_order:?} consumed={seen_bodies:?} offsets={seen_offsets:?} queues={:?}",
            seen_order.iter().map(|s| s.queue_id).collect::<Vec<i32>>()
        ),
    );

    // ---- A9 Tag filtering: the server delivers only TagA
    println!("A9 Tag-filtered consume (server-side expression filtering)");
    for i in 0..3 {
        let mut a = msg(&tag_topic, format!("acl-tagA-{i}").as_bytes(), "TagA", "");
        if let Err(e) = p.send(&mut a, Some(5000), None).await {
            ck.abort("A9 send TagA", &format!("{} {e}", code_of(&e)));
        }
        let mut b = msg(&tag_topic, format!("acl-tagB-{i}").as_bytes(), "TagB", "");
        if let Err(e) = p.send(&mut b, Some(5000), None).await {
            ck.abort("A9 send TagB", &format!("{} {e}", code_of(&e)));
        }
    }
    let tag_a: Vec<String> = (0..3).map(|i| format!("acl-tagA-{i}")).collect();
    let tag_b: Vec<String> = (0..3).map(|i| format!("acl-tagB-{i}")).collect();
    let tag_want_a: Vec<&str> = tag_a.iter().map(|s| s.as_str()).collect();
    let tag_want_b: Vec<&str> = tag_b.iter().map(|s| s.as_str()).collect();
    let back_tag_a = wait_bodies(&instance, &tag_topic, &cluster, &tag_want_a, READ_BACK_BUDGET).await;
    // TagB must also be on the broker: otherwise "only TagA arrived" might just mean TagB was never
    // sent in the first place.
    let back_tag_b = wait_bodies(&instance, &tag_topic, &cluster, &tag_want_b, READ_BACK_BUDGET).await;
    let filtered = wait_until(
        || async { tag_inbox.bodies().len() >= 3 },
        Duration::from_secs(20),
    )
    .await;
    let got_tag = tag_inbox.bodies();
    let leaked_tag_b = tag_b.iter().any(|b| got_tag.contains(b.as_str()));
    ck.check(
        "A9 a consumer subscribed to TagA receives only TagA (server-side filtering, both kinds exist on the broker)",
        back_tag_a.len() == 3
            && back_tag_b.len() == 3
            && filtered
            && got_tag.len() == 3
            && !leaked_tag_b,
        &format!(
            "on broker TagA={} TagB={} consumed={got_tag:?}",
            back_tag_a.len(),
            back_tag_b.len()
        ),
    );

    // ---- A10 server-side query by key
    println!("A10 server-side query by key (QUERY_MSG_BY_KEY)");
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
        "A10 a message sent with a key can be queried back by key on the server; an unknown key yields an empty table, not an error",
        found_by_key
            && !queried.is_empty()
            && queried.iter().all(|m| m.topic == topic)
            && queried.iter().any(|m| m.get_keys() == Some(key_sync.as_str()))
            && missing_key == 0,
        &format!("key={key_sync} found={} unknown_key={missing_key}", queried.len()),
    );

    // ---- A5 delayed message + A6 timer message: read back first, then use the timestamps to prove
    // "delivered only at the due time"
    println!("A5/A6 delay level and timer messages (wait for the delivery moment, proved by timestamps)");
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
        Err(e) => format!("{} {e}", code_of(e)),
    };
    let timer_detail = match &timer_send {
        Ok(r) => format!("{:?}", r.status),
        Err(e) => format!("{} {e}", code_of(e)),
    };
    ck.check(
        "A5 delayed message (DELAY level) sent SEND_OK",
        delay_send.as_ref().ok().map(|r| r.status) == Some(SendStatus::SendOk),
        &delay_detail,
    );
    ck.check(
        "A6 timer message (TIMER_DELIVER_MS) sent SEND_OK",
        timer_send.as_ref().ok().map(|r| r.status) == Some(SendStatus::SendOk),
        &timer_detail,
    );
    // Sanity check on the property names: delay uses DELAY, timer uses TIMER_DELIVER_MS, the two must
    // not be mixed up.
    let delay_level_text = DELAY_LEVEL.to_string();
    ck.check(
        "A5/A6 the two kinds of messages use exactly their own property keys",
        m5.get_property(PROPERTY_DELAY_TIME_LEVEL) == Some(delay_level_text.as_str())
            && m6.get_property(PROPERTY_TIMER_DELIVER_MS).is_some()
            && m6.get_property(PROPERTY_DELAY_TIME_LEVEL).is_none(),
        &format!("{:?} {:?}", m5.properties, m6.properties),
    );
    // Timer and delayed messages must not be readable back before their delivery moment.
    let early = snapshot(&instance, &topic, &cluster).await;
    ck.check(
        "A5/A6 the broker does not deliver them before the due moment (the snapshot does not contain these two yet)",
        !early.iter().any(|m| body_of(m) == "acl-delay") && !early.iter().any(|m| body_of(m) == "acl-timer"),
        &format!("the current snapshot already holds {} bodies", bodies(&early).len()),
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
        "A5 the delayed message is eventually readable, and store_ts-born_ts proves the delay took effect",
        got_delay.is_some() && delay_drift >= DELAY_MILLIS - CLOCK_SLACK,
        &format!("drift={delay_drift}ms got={} born={delay_born}", got_delay.is_some()),
    );
    ck.check(
        "A6 the timer message is eventually readable, and store_ts-born_ts proves it was delivered at the due time",
        got_timer.is_some() && timer_drift >= TIMER_AHEAD_MILLIS - CLOCK_SLACK,
        &format!("drift={timer_drift}ms deliverAt={deliver_at}"),
    );

    // ---- A12 read-back by a signed push consumer: body/properties/timestamps all re-checked from the
    // consuming side
    println!("A12 signed push consumer received everything (heartbeat/pull/offset commit all signed)");
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
        "A12 the consuming side received all 9 messages (every kind really got delivered)",
        consumed_all && missing.is_empty(),
        &format!("missing {missing:?} received={}", main_inbox.bodies().len()),
    );
    let consumed_sync = main_inbox.find("acl-sync");
    ck.check(
        "A11 user properties passed through to the consuming side (city/env read back verbatim)",
        consumed_sync
            .as_ref()
            .is_some_and(|s| s.city == "Hangzhou" && s.env == "prod"),
        &format!("{:?}", consumed_sync),
    );
    ck.check(
        "A10 keys passed through to the consuming side",
        consumed_sync
            .as_ref()
            .is_some_and(|s| s.keys == key_sync),
        &format!("{:?}", consumed_sync.as_ref().map(|s| s.keys.clone())),
    );
    ck.check(
        "A1 tag passed through to the consuming side",
        consumed_sync
            .as_ref()
            .is_some_and(|s| s.tags == "TagSync"),
        &format!("{:?}", consumed_sync.as_ref().map(|s| s.tags.clone())),
    );
    let consumed_delay = main_inbox.find("acl-delay");
    let consumed_timer = main_inbox.find("acl-timer");
    ck.check(
        "A5/A6 the delivery delay seen on the consuming side matches what the read-back shows",
        consumed_delay.as_ref().is_some_and(|s| s.store_timestamp - s.born_timestamp >= DELAY_MILLIS - CLOCK_SLACK)
            && consumed_timer.as_ref().is_some_and(|s| s.store_timestamp - s.born_timestamp >= TIMER_AHEAD_MILLIS - CLOCK_SLACK),
        &format!(
            "delay={:?} timer={:?}",
            consumed_delay.map(|s| s.store_timestamp - s.born_timestamp),
            consumed_timer.map(|s| s.store_timestamp - s.born_timestamp)
        ),
    );
    // The offset really reached the broker: this is the third signed RPC on the consuming side (the
    // commit uses UPDATE_CONSUMER_OFFSET, the read-back uses QUERY_CONSUMER_OFFSET).
    // The commit is periodic (one round every 5s by default), so we poll the read instead of
    // judging right after the send.
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
        "A12 the consume offset was committed to the broker (signed RPC, readable back)",
        committed_ok,
        &match committed {
            Ok(v) => format!("group={group_main} offset={v:?} mq={sync_mq:?}"),
            Err(e) => format!("{} {e}", code_of(&e)),
        },
    );

    // ---- cleanup: delete the three topics created this round (admin RPCs with credentials)
    println!("cleanup");
    for t in [&topic, &tag_topic, &order_topic] {
        let on_broker = instance
            .delete_topic_in_broker(&cluster.broker_addr, t, 5000)
            .await;
        ck.check(
            &format!("cleanup: delete {t} on the broker"),
            on_broker.is_ok(),
            &match on_broker {
                Ok(()) => String::new(),
                Err(e) => format!("{} {e}", code_of(&e)),
            },
        );
        let on_namesrv = instance.delete_topic_in_namesrv(t, 5000).await;
        ck.check(
            &format!("cleanup: delete the namesrv route {t}"),
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

/// Shutdown order: consumers first (withdraw offsets, send the deregistration), then the producer,
/// to avoid a shutdown race.
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
