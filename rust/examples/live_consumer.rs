//! Live validation of [`DefaultMQPushConsumer`] against a **real 5.5.1 broker**.
//!
//! Offline unit tests (the `mod tests` in `src/client/consumer.rs`) can only cover pure logic: threshold
//! arithmetic, CK parsing, return-type decisions. What only real send/receive can prove lives here:
//! long-poll delivery, tag filtering, failed-message redelivery with `%RETRY%` topic reset, POP + ack,
//! broadcast offsets written to disk, orderly consumption's broker lock, multi-instance queue sharing and
//! revocation, offsets persisted to the broker, and the runtime state read back through the admin interfaces.
//!
//!
//! Usage (start a local cluster first, per the runbook recorded in the project knowledge):
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
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::protocol::admin_body::MessageQueueKey;
use rocketmq_client_remoting::remoting::protocol::body::ConsumerRunningInfo;
use rocketmq_client_remoting::remoting::protocol::heartbeat::{
    ConsumeFromWhere, MessageModel, SubscriptionData,
};
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;
use rocketmq_client_remoting::remoting::rpchook::{AclClientRPCHook, RPCHook, SessionCredentials};

/// Default queue count per test topic (enough for C8 to hand "4 queues to 2 instances").
const QUEUE_NUMS: i32 = 4;
/// Max seconds to wait for delivery (covers rebalance, long-poll suspension, and the 5s offset persist cycle).
const WAIT_SECONDS: u64 = 30;
/// The offset persist cycle is 5s; this leaves a two-cycle margin.
const PERSIST_SECONDS: u64 = 12;

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

/// Assertion accumulator: run all scenarios once, then summarize; the first failure does not exit early.
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

/// On a poisoned lock, still take the inner value (one panic inside a listener must not cascade into a whole-run failure).
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

// ------------------------------------------------------------------ ACL credentials

/// Every auth failure on the broker side collapses into NO_PERMISSION(16).
const NO_PERMISSION: i32 = 16;

/// Optional ACL credentials: "with credentials" only when both trailing CLI arguments are non-empty.
///
/// When empty, [`Creds::hook`] always returns `None`, and a client handed `None` installs no hook --
/// byte-identical to behaviour on a plain cluster (no `authenticationEnabled`), so the harness runs as usual.
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

    /// The signing hook installed on each client; `None` when no credentials are given.
    fn hook(&self) -> Option<Arc<dyn RPCHook>> {
        if self.enabled() {
            Some(Arc::new(AclClientRPCHook::new(SessionCredentials::new(
                &self.access_key,
                &self.secret_key,
            ))))
        } else {
            None
        }
    }
}

/// Rejection assertion: must be a broker-returned NO_PERMISSION, not a connect failure / timeout.
fn denied(err: &Error) -> bool {
    err.response_code() == Some(NO_PERMISSION)
}

fn code_of(err: &Error) -> String {
    match err.response_code() {
        Some(code) => format!("code={code}"),
        None => "code=?".to_string(),
    }
}

/// Reverse control: a consumer **without credentials** must fail to start or be rejected by the broker with code 16.
///
/// Without it, "all green with credentials" on a cluster where auth was never enabled looks exactly
/// like "the signature was accepted" -- a false pass. It must run **before** any other client in this
/// example: this port shares one [`MQClientInstance`] per clientId in-process and the hook lives on
/// the instance's remoting channel, so a probe sharing the signed clients' channel gets signed along and the rejection disappears.
async fn anon_probe(namesrv: &str, run: &str, ck: &mut Checker) {
    println!("-- reverse control: register/pull without credentials must be rejected by the broker (NO_PERMISSION=16)");
    let group = format!("rust-live-consumer-anon-{run}");
    let anon = match DefaultMQProducer::with_rpc_hook(&group, None) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("probe build", &format!("{e}"));
            return;
        }
    };
    anon.set_namesrv_addr(namesrv);
    // Independent instanceName ⇒ independent clientId ⇒ independent channel: the hook cannot intercept anyone else.
    anon.set_instance_name(&format!("live-anon-{run}"));
    if let Err(e) = anon.start().await {
        ck.check(
            "probe start without credentials is rejected",
            denied(&e),
            &format!("{} {e}", code_of(&e)),
        );
        return;
    }
    // The probe sends a new topic unique to this run: routing falls back via autoCreateTopicEnable and
    // reaches the broker, where the auth gate blocks it -- that is the proof the request went out unchanged.
    let probe_topic = format!("RustLiveConsumerAnon{run}");
    let mut m = Message::new(&probe_topic, Some(b"should-not-be-accepted"));
    match anon.send(&mut m, Some(5000), None).await {
        Ok(r) => ck.check(
            "probe send without credentials is rejected by the broker",
            false,
            &format!("the broker actually accepted an anonymous request: {r:?} (most likely authenticationEnabled is off)"),
        ),
        Err(e) => ck.check(
            "probe send without credentials is rejected by the broker",
            denied(&e),
            &format!("{} {e}", code_of(&e)),
        ),
    }
    anon.shutdown();
    drop(anon);
    // Wait for its instance to be fully released before the signed clients build their own channels.
    tokio::time::sleep(Duration::from_secs(1)).await;
}

/// Poll until the condition holds or time runs out (everything on a live broker lands asynchronously; never bet on a fixed sleep).
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

/// How many times this instance received the broker's `NOTIFY_CONSUMER_IDS_CHANGED(40)`; once the
/// instance has been reclaimed it returns 0 (40 is pushed by the broker over the long connection, so counting is the only way to prove it landed).
fn ids_changed_count(client_id: &str) -> usize {
    MQClientInstance::find_instance(client_id)
        .map(|i| i.consumer_ids_changed_count())
        .unwrap_or_default()
}

fn main_queues(keys: &[String]) -> Vec<String> {
    keys.iter()
        .filter(|k| !k.starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX))
        .cloned()
        .collect()
}

// ------------------------------------------------------ delivery records and listener

/// Observed values of one message delivered to the listener (all taken from fields the broker actually wrote back).
#[derive(Debug, Clone)]
struct Delivered {
    body: String,
    tags: String,
    topic: String,
    queue_id: i32,
    queue_offset: i64,
    reconsume_times: i32,
    msg_id: String,
    /// The producer's `SendResult.msgId` is this client-side unique ID; `msg_id` is not it.
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

/// Collector on the listener side.
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

/// Concurrent listener: records deliveries, optionally fails the first arrival of a given body, optionally slows each batch.
struct LiveListener {
    inbox: Arc<Inbox>,
    /// The **first** delivery matching this body returns RECONSUME_LATER (C4 redelivery check).
    fail_once: Option<String>,
    /// When true, every match fails (C4b needs retries exhausted down to the dead letter).
    fail_always: bool,
    /// Fixed cost per batch (C9 uses it to build a backlog that triggers flow control).
    batch_cost: Duration,
    suspend_body: Option<String>,
    orderly: Arc<OrderlyState>,
}

/// Out-of-order counter for orderly consumption (kept inside the listener for easy external reads).
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

    /// **Every** delivery matching this body fails: used to drive retries to the cap.
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

    /// **Every** orderly delivery matching this body suspends the current queue (C12's poison message).
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

    /// Whether the given body has been seen at most once (the basis for failing it exactly once).
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

// ------------------------------------------------------------------ fixture

struct Fixture {
    namesrv: String,
    stamp: String,
    /// ACL credentials: when non-empty every client this fixture starts gets the signing hook; when empty none do.
    creds: Creds,
    producer_group: String,
    producer: DefaultMQProducer,
    /// The dedicated "ops" instance built by this example: create/delete topics, read broker offsets and backlog.
    admin: MQClientInstance,
    broker_name: String,
    broker_addr: String,
    /// Topics created during this run; C10 deletes them all.
    topics: Mutex<Vec<String>>,
    /// Broadcasting writes offset dirs under $HOME; C10 removes them all.
    offset_dirs: Mutex<Vec<PathBuf>>,
}

impl Fixture {
    fn new(namesrv: &str, stamp: &str, creds: Creds) -> Result<Fixture, String> {
        let producer_group = format!("rust-live-consumer-pg-{stamp}");
        let mut producer = DefaultMQProducer::new(&producer_group)
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        // The hook is registered onto the instance's remoting channel inside start(): this fixture's producer,
        // admin instance, and every consumer each pass through it once; with no credentials this is None (= no hook).
        producer.set_rpc_hook(creds.hook());
        let _ = &mut producer;
        let admin_id = format!("rust-live-consumer-admin-{stamp}");
        let admin = MQClientInstance::new(&admin_id, vec![namesrv.to_string()]);
        if let Some(hook) = creds.hook() {
            admin.remoting_client().register_rpc_hook(hook);
        }
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            creds,
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
        // Use the TBW102 route to find a broker that can host topics (same approach as live_protocol.rs)
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

    /// Create the topic explicitly (not via `autoCreateTopicEnable`, so the queue count is deterministic and C8 can split it).
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

    /// Start a consumer that is subscribed and has a concurrent listener set (also registers the broadcast offset dir for cleanup).
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

    /// Build a consumer from the group name only (no subscription, no listener); used by C1's validation branches.
    fn plain_consumer(&self, group: &str) -> Result<DefaultMQPushConsumer, String> {
        let cfg = self.base_config(group);
        self.push(cfg)
    }

    /// Build a push consumer and install the hook: every consumer in this example comes from here,
    /// so none is missed when running against an auth-enabled cluster.
    fn push(&self, cfg: ConsumerConfig) -> Result<DefaultMQPushConsumer, String> {
        let consumer =
            DefaultMQPushConsumer::with_config(cfg).map_err(|e| format!("build failed: {e}"))?;
        consumer.set_rpc_hook(self.creds.hook());
        Ok(consumer)
    }

    /// Build a lite pull consumer and install the hook.
    fn lite(&self, cfg: LitePullConsumerConfig) -> Result<DefaultLitePullConsumer, String> {
        let consumer = DefaultLitePullConsumer::with_config(cfg)
            .map_err(|e| format!("build lite pull failed: {e}"))?;
        consumer.set_rpc_hook(self.creds.hook());
        Ok(consumer)
    }

    /// Build a standalone admin and install the hook (the admin's RPCs must be signed too: 307 forwarding, offset queries, topic creation).
    fn admin_ext(&self, cfg: AdminConfig) -> DefaultMQAdminExt {
        let admin = DefaultMQAdminExt::with_config(cfg);
        admin.set_rpc_hook(self.creds.hook());
        admin
    }

    /// Send to the given topic (optionally pinned to a queue). SendResult's status must be SEND_OK.
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

    /// Sum of maxOffset over all queues of the topic == the broker's real backlog.
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

    /// Sum of the offsets this group committed on the broker for the topic (`QUERY_CONSUMER_OFFSET`).
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

    /// Wait for offsets to be flushed to the broker (5s cycle, at most PERSIST_SECONDS).
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

    /// The broker-side consumer list of the group (queried through the shared instance; it is the rebalance input).
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

/// Flatten the runningInfo queue tables (mqTable / mqPopTable) into
/// `topic+broker+queueId -> ProcessQueueInfo JSON`.
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

/// Age in milliseconds of the business queue's pull clock in the running info (`None` when nothing recorded; the caller treats that as failure).
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

// --------------------------------------------------------------- C1 lifecycle checks

async fn c1_lifecycle(ck: &mut Checker, fx: &Fixture) {
    println!("-- C1 construction and start()/shutdown() checks");

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

    if DefaultTopAddressing::is_configured() {
        ck.skip("C1 missing name server is rejected", "domain addressing is configured in this environment");
    } else {
        let cfg = ConsumerConfig {
            consumer_group: fx.group_name("no-namesrv"),
            ..Default::default()
        };
        let bare = fx.push(cfg).unwrap();
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

    // Normal start: the first heartbeat + the first rebalance both complete before start() returns
    let inbox = Arc::new(Inbox::default());
    c.set_message_listener_concurrently(LiveListener::collecting(inbox.clone()));
    if let Err(e) = c.start().await {
        return ck.abort("C1 start", &format!("{e}"));
    }
    ck.check("C1 start() succeeded", c.is_started(), "started flag false");
    ck.check(
        "C1 start() stamped a clientId (<clientIP>@<instanceName>)",
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
        "C1 start() subscribes the business topic and %RETRY%<group>",
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
    let late = fx.topic_name("Late");
    let accepted = c.subscribe(&late, "*").is_ok();
    ck.check(
        "C1 subscribe() after start() is accepted (no already-started guard)",
        accepted && c.subscriptions().iter().any(|s| s.topic == late),
        &format!("{accepted:?} subs={:?}", c.subscriptions()),
    );
    c.unsubscribe(&late);
    ck.check(
        "C1 unsubscribe() removes the topic from the live subscription table",
        !c.subscriptions().iter().any(|s| s.topic == late),
        &format!("{:?}", c.subscriptions()),
    );
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
        "C1 shutdown() removes the consumer from its MQClientInstance registry",
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

// --------------------------------------------------- C2 long-poll consumption + offset persistence

async fn c2_pull_consume_and_offset_persist(ck: &mut Checker, fx: &Fixture) {
    println!("-- C2 long-poll consumption of 24 messages + offsets flushed to the broker");
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
        "C2 the pull path stamps no MIN/MAX_OFFSET, so msgAccCnt stays 0",
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

    let info = c.consumer_running_info();
    let table = running_table(&info);
    let business: Vec<String> = main_queues(&table.keys().cloned().collect::<Vec<_>>());
    let retry_entries: Vec<&String> = table
        .keys()
        .filter(|k| k.starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX))
        .collect();
    ck.check(
        "C2 consumerRunningInfo reports one ProcessQueueInfo per assigned queue (business + the %RETRY% queue)",
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

// ------------------------------------------------------------- C3 expression filtering

async fn c3_tag_filter(ck: &mut Checker, fx: &Fixture) {
    println!("-- C3 consume only the subscribed tag");
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
    // Wait a bit longer to confirm no extra TagB leaks in
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

// --------------------------------------------------------- C4 failed redelivery and topic reset

async fn c4_retry_and_topic_reset(ck: &mut Checker, fx: &Fixture) {
    println!("-- C4 RECONSUME_LATER → broker redelivery + %RETRY% topic reset");
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
    // The two messages judged successful must not be redelivered
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

// ------------------------------- C4c partial ack (ConsumeConcurrentlyContext.ackIndex)

/// Concurrent listener that narrows ackIndex on the **first batch** only.
///
/// Later batches must accept the whole batch, otherwise the tail never finishes redelivering and never converges.
struct PartialAckListener {
    inbox: Arc<Inbox>,
    /// Index to ack up to in the first batch (inclusive); `None` = leave ackIndex untouched (control leg).
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

/// Partial ack via `ackIndex`: why it can only be verified against a live broker.
///
/// Offline unit tests (the `mod tests` in `consumer.rs`) can pin down "the offset never passes the tail when
/// the tail's send-back **fails**", but the semantics when the send-back **succeeds** -- the unacked entries really
/// are taken by the broker and redelivered, the acked entry is delivered exactly once across the whole window,
/// and the business queue offset still advances past the whole batch -- only a real broker can rule on that.
/// Both wrong shapes look identical offline: forgetting the send-back (the tail is silently lost, the received count is still right),
/// or sending back the acked prefix too (duplicate deliveries that look like "nothing was lost").
/// Builds a push consumer with "one batch of 3 + optionally narrowed ackIndex".
fn partial_ack_consumer(
    fx: &Fixture,
    topic: &str,
    group: &str,
    listener: Arc<PartialAckListener>,
) -> Result<DefaultMQPushConsumer, String> {
    // consumeMessageBatchMaxSize defaults to 1; without narrowing to 3 there is no "partial" at all.
    // The scenario sends the 3 messages first and starts the consumer after, so the new group must consume from FIRST_OFFSET:
    // with LAST_OFFSET a new group starts at the latest offset at assignment time and the 3 pre-sent messages are skipped outright.
    let cfg = ConsumerConfig {
        consume_message_batch_max_size: 3,
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..fx.base_config(group)
    };
    let c = fx.push(cfg)?;
    c.subscribe(topic, "*")
        .map_err(|e| format!("subscribe failed: {e}"))?;
    c.set_message_listener_concurrently(listener);
    Ok(c)
}

async fn c4c_partial_ack(ck: &mut Checker, fx: &Fixture) {
    println!("-- C4c CONSUME_SUCCESS + ackIndex partial ack → tail redelivered via %RETRY%");
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
    // Produce first, start the consumer after: how the batch is cut depends on pull timing, and only when
    // 3 messages already sit in the queue is the first pull exactly "one full batch of 3"; otherwise the first batch may be 1-2 messages and the prefix/suffix carved by ackIndex=0 is undefined.
    fx.produce(&topic, "TagA", 3, None).await;
    if let Err(e) = c.start().await {
        return ck.abort("C4c start", &format!("{e}"));
    }
    if let Err(e) = control.start().await {
        return ck.abort("C4c control start", &format!("{e}"));
    }

    // The first batch must be exactly 3 messages, otherwise the prefix/suffix carved by ackIndex=0 is undefined
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
    let committed = fx.wait_committed(&group, &topic, 1, 3).await;
    ck.check(
        "C4c the business queue offset still advances past the whole batch",
        committed == 3,
        &format!("committed={committed}"),
    );
    // Control leg: ackIndex untouched ⇒ nothing may be sent back
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

// --------------------------------------------- C4b retries exhausted → %DLQ% terminal state

/// `%DLQ%<group>` forensics: wait for the broker to create the dead-letter topic and register it in routing, then read
/// the existing dead letters with an **independent consumer group + `seek_to_begin`** (a new group starting at the queue tail would skip that dead letter outright ⇒ false failure).
///
/// Returns `(has route, dead letters)`: in the negative case "the broker never created the DLQ topic" is itself the
/// correct conclusion, so it must not be merged into the same empty result as "route never showed up".
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
    // Queue count comes from the route the broker actually created (`DLQ_NUMS_PER_GROUP` is 1 in 5.5.1,
    // but that is a broker-side constant; do not restate it in the test)
    let nums = r
        .queue_datas
        .iter()
        .map(|q| q.read_queue_nums)
        .sum::<i32>()
        .max(1);
    let mqs = fx.queues(&dlq_topic, nums);
    let lite = fx.lite(LitePullConsumerConfig {
        consumer_group: fx.group_name(reader_kind),
        name_server_addrs: vec![fx.namesrv.clone()],
        instance_name: format!("live-{reader_kind}-{}", fx.stamp),
        poll_timeout_millis: 1000,
        ..Default::default()
    })
    .map_err(|e| format!("build lite pull failed: {e}"))?;
    // assign must happen before start: the rebalance right after start only honors subscriptions and would compute an empty assignment
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
    // seek_to_begin must run after start (it errors otherwise) and rewinds the pull cursor to the minimum offset;
    // if the pull loop has already buffered the same batch locally, seek only drops copies "before the offset",
    // so the same message is delivered twice with the same (queue_id, queue_offset). Dedup by storage position:
    // if the message truly entered the DLQ twice, the copies sit at different queue_offsets and are not eaten.
    let mut seen_at: BTreeSet<(i32, i64)> = BTreeSet::new();
    let deadline = Instant::now() + Duration::from_secs(wait_secs);
    while Instant::now() < deadline {
        for m in lite.poll(Some(1000)).await {
            let d = Delivered::from(&m);
            if seen_at.insert((d.queue_id, d.queue_offset)) {
                msgs.push(d);
            }
        }
        if msgs.is_empty() {
            continue;
        }
        // After the first arrival, drain a few more rounds: the assertion "the DLQ holds only this one" requires
        // seeing what comes after, but the total window must stay bounded or every positive case waits out wait_secs for nothing.
        let drain_to = Instant::now() + Duration::from_secs(3);
        while Instant::now() < drain_to {
            for m in lite.poll(Some(500)).await {
                let d = Delivered::from(&m);
                if seen_at.insert((d.queue_id, d.queue_offset)) {
                    msgs.push(d);
                }
            }
        }
        break;
    }
    lite.shutdown();
    Ok((true, msgs))
}

/// Dead-letter terminal state: why it can only be verified against a live broker.
///
///
/// `maxReconsumeTimes=2` compresses the terminal state into a few tens of seconds and pins each item:
/// exactly 3 deliveries (reconsumeTimes 0/1/2), no 4th delivery to the original group after the 3rd send-back,
/// the DLQ topic auto-created by the broker and registered in routing, the `%DLQ%` copy's reconsumeTimes=3,
/// `RETRY_TOPIC` still the original business topic, and exactly 1 message.
async fn c4b_dlq_terminal(ck: &mut Checker, fx: &Fixture) {
    println!("-- C4b maxReconsumeTimes exhausted → %DLQ%<group> terminal state");
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
    let c = match fx.push(cfg) {
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
    // Send-back delay level = 3 + reconsumeTimes ⇒ level3(10s) + level4(30s), plus delivery margin.
    // 150s instead of 100s: when other suites run concurrently on the same machine the broker's timer service slips;
    // the 3rd delivery was observed arriving 60s+ late, and a hard 100s cap is a false failure.
    let arrived = poll_until(|| inbox.matching(&victim).len() >= 3, 150).await;
    let seen = inbox.matching(&victim);
    let times: Vec<i32> = seen.iter().map(|d| d.reconsume_times).collect();
    ck.check(
        "C4b maxReconsumeTimes=2 ⇒ exactly 3 deliveries, reconsumeTimes 0/1/2 in order",
        arrived && times.starts_with(&[0, 1, 2]),
        &format!("times={times:?}"),
    );
    // Counter-proof: after the DLQ the original group gets no 4th delivery (writing `>` instead of `>=` would show here)
    tokio::time::sleep(Duration::from_secs(15)).await;
    let final_times: Vec<i32> = inbox.matching(&victim).iter().map(|d| d.reconsume_times).collect();
    ck.check(
        "C4b no delivery after exhaustion (only 3 within the observation window)",
        final_times.len() == 3,
        &format!("deliveries={} times={final_times:?}", final_times.len()),
    );
    c.shutdown();

    // %DLQ%<group> is created by the broker and registered to the name server only at the moment of dead-lettering
    let dlq_topic = MixAll::get_dlq_topic(&group);
    let (has_route, dlq_msgs) = match read_dlq(fx, &group, "dlqread", 30).await {
        Ok(v) => v,
        Err(e) => return ck.abort("C4b read %DLQ%", &e),
    };
    ck.check(
        "C4b broker auto-created and registered the %DLQ%<group> route",
        has_route,
        &format!("dlq={dlq_topic}"),
    );
    if has_route {
        ck.check(
            "C4b %DLQ%<group> holds exactly that one dead letter",
            dlq_msgs.len() == 1 && dlq_msgs[0].body == victim,
            &format!("{dlq_msgs:?}"),
        );
    }
    if let Some(d) = dlq_msgs.first() {
        // the broker adds +1 at storage time (AbstractSendMessageProcessor:226) ⇒ 3 after 2 redeliveries
        ck.check(
            "C4b the dead letter's reconsumeTimes = maxReconsumeTimes + 1",
            d.reconsume_times == MAX_RECONSUME + 1,
            &format!("reconsumeTimes={}", d.reconsume_times),
        );
        ck.check(
            "C4b the dead letter keeps RETRY_TOPIC=the original business topic, and topic is now %DLQ%<group>",
            d.retry_topic_prop.as_deref() == Some(topic.as_str()) && d.topic == dlq_topic,
            &format!("topic={} retryTopic={:?}", d.topic, d.retry_topic_prop),
        );
        // properties are carried over wholesale during redelivery ⇒ the unique ID is not rewritten
        let origin = sent.first().map(|s| s.0.clone()).unwrap_or_default();
        ck.check(
            "C4b the dead letter's UNIQ_KEY is still the original message's msgId",
            !origin.is_empty() && d.uniq_key == origin,
            &format!("sent={origin} dlqUniq={}", d.uniq_key),
        );
    }
    if has_route {
        // Hand to C10 for deletion; do not leave %DLQ%/%RETRY% litter on the broker
        lock(&fx.topics).push(dlq_topic.clone());
        lock(&fx.topics).push(MixAll::get_retry_topic(&group));
    }
}

// ------------------------------------------------------------- C5 POP + ack

async fn c5_pop_mode(ck: &mut Checker, fx: &Fixture) {
    println!("-- C5 POP mode: popped messages carry CK and do not revive after ack");
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
    // Direct evidence the ack worked: after one invisibleTime window the broker did not revive and redeliver the message
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

// ------------------------------------------- C5b pull stats of the POP path (307 statusTable)

/// ⚠ The sampler drops a minute point every 10s and the snapshot takes a first-to-last delta, so traffic must
///   **persistently** span two sample points (here 13 rounds × 2s ≈ 26s), or `pullTPS` stays 0 -- a too-short fixture, not a wrong criterion.
/// ⚠ Read from the admin side (307 forwarded by the broker back to this instance), not by in-process self-inspection:
///   serialization and forwarding are on that path too, invisible when the process reads its own table.
async fn c5b_pop_pull_stats(ck: &mut Checker, fx: &Fixture) {
    println!("-- C5b the POP loop writes pullRT/pullTPS into the 307 status table");
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
    // A separate admin instance: 307 is the "admin → broker → target client" forwarding chain,
    // and querying yourself with the consumer's own instance skips the entire chain.
    let admin = fx.admin_ext(AdminConfig {
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
    // The two counters are independent: consumeOKTPS non-zero while all pull* are 0 is exactly the shape of a POP loop that fails to record pull stats.
    ck.check(
        "C5b consumeOKTPS is non-zero as well (pull side and consume side report separately)",
        cs.consume_ok_tps > 0.0,
        &format!("consumeOKTPS={}", cs.consume_ok_tps),
    );
    // A non-zero status table only proves "the counter was called"; the traffic behind it must also really be
    // consumed -- otherwise pullTPS can be inflated by repeatedly touching messages that never fully ack: good on the dashboard, going in circles in reality.
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

// ------------------------------------------------------- C6 broadcasting + local offset files

async fn c6_broadcasting_and_local_offsets(ck: &mut Checker, fx: &Fixture) {
    println!("-- C6 BROADCASTING: each instance consumes everything + offsets land in a local file");
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
    let a = match fx.push(cfg_a.clone()) {
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

    // A second broadcasting instance of the same group must get everything (broadcasting never splits queues)
    let inbox_b = Arc::new(Inbox::default());
    let client_b = format!("live-bcast-b-{}", fx.stamp);
    let mut cfg_b = cfg_a.clone();
    cfg_b.instance_name = client_b.clone();
    cfg_b.client_id = Some(client_b.clone());
    fx.register_offset_dir(&client_b);
    let b = match fx.push(cfg_b) {
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

    // Local offset file: on shutdown save_local_offsets writes .tmp first, then renames
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
                let offsets = canonical_format_offsets(&text);
                ck.check(
                    "C6 the local offset file uses the object-as-key offsetTable format with 2 entries summing to 8",
                    matches!(&offsets, Some(v) if v.len() == 2 && v.iter().sum::<i64>() == 8),
                    &format!("{offsets:?} raw={text}"),
                );
                ck.check(
                    "C6 each offsetTable key carries the real topic/broker/queueId",
                    text.matches(&format!("\"topic\":\"{topic}\"")).count() == 2
                        && text.matches("\"brokerName\":").count() == 2,
                    &format!("raw={text}"),
                );
                ck.check(
                    "C6 no half-written offset file is left behind (tmp is renamed away)",
                    !file_a.with_file_name("offsets.json.tmp").exists(),
                    "offsets.json.tmp survived",
                );

                let inbox_r = Arc::new(Inbox::default());
                let mut cfg_r = cfg_a.clone();
                cfg_r.instance_name = client_a.clone();
                cfg_r.client_id = Some(client_a.clone());
                let r = match fx.push(cfg_r) {
                    Ok(c) => c,
                    Err(e) => return ck.abort("C6 build restart", &e.to_string()),
                };
                if let Err(e) = r.subscribe(&topic, "*") {
                    return ck.abort("C6 subscribe restart", &e.to_string());
                }
                r.set_message_listener_concurrently(LiveListener::collecting(inbox_r.clone()));
                if let Err(e) = r.start().await {
                    return ck.abort("C6 start restart", &format!("{e}"));
                }
                tokio::time::sleep(Duration::from_secs(10)).await;
                ck.check(
                    "C6 restarting with the same clientId resumes from the local offsets (no redelivery)",
                    inbox_r.count() == 0,
                    &format!("redelivered={}", inbox_r.count()),
                );
                r.shutdown();
            }
            Err(e) => ck.abort("C6 read the local offset file", &e.to_string()),
        }
    }
}

/// Extract the offset list from `{"offsetTable":{{...}:7,{...}:9}}`; returns `None` on a malformed format.
fn canonical_format_offsets(text: &str) -> Option<Vec<i64>> {
    let body = text.strip_prefix("{\"offsetTable\":{")?.strip_suffix("}}")?;
    let mut offs = Vec::new();
    for (i, seg) in body.split("}:").enumerate() {
        if i == 0 {
            continue;
        }
        let num = if seg.contains(',') {
            seg.split(',').next()?
        } else {
            seg
        };
        offs.push(num.trim().parse().ok()?);
    }
    Some(offs)
}

// --------------------------------------------------------------- C7 orderly consumption

async fn c7_orderly_and_lock(ck: &mut Checker, fx: &Fixture) {
    println!("-- C7 Orderly listener: LOCK_BATCH_MQ + strictly increasing offsets inside a queue");
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
    // Pin 6 messages into each of the two queues so "in-queue ordering" has something to check (concurrent mode guarantees no order)
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
    // Orderly pulls skip queues that are not locked (`queue_pull_loop` checks `lock_ok`), so collecting all
    // 12 above already proves the lock was taken; here the lock state is read directly once more.
    // ProcessQueueInfo.locked in runningInfo is not backfilled on any port, so the lock table is the only observation point.
    let locked = poll_until(|| main_queues(&c.locked_queue_keys()).len() == 2, 25).await;
    ck.check(
        "C7 the lock loop holds every assigned queue (LOCK_BATCH_MQ accepted by the 5.5.1 broker)",
        locked,
        &format!("locked={:?}", c.locked_queue_keys()),
    );

    c.shutdown();
    // Lock-release check: a different instance (new clientId) takes over the same group and topic. The broker's
    // RebalanceLockManager only lets a lock through for "the same clientId" or "lock expired (60s)", so B locking
    // and consuming immediately is direct evidence that A's shutdown really returned the locks via UNLOCK_BATCH_MQ.
    let client_b = format!("live-order-b-{}", fx.stamp);
    let mut cfg_b = fx.base_config(&group);
    cfg_b.instance_name = client_b.clone();
    cfg_b.client_id = Some(client_b.clone());
    let inbox_b = Arc::new(Inbox::default());
    let b = match fx.push(cfg_b) {
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

// ----------------------------------------------------- C8 multi-instance sharing and scale-in

async fn c8_scale_in_and_takeover(ck: &mut Checker, fx: &Fixture) {
    println!("-- C8 two instances of one group split 4 queues; after one leaves the other takes over everything");
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
    let b = match fx.push(cfg_b) {
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
    // Both sides must see each other in the broker's member list, or rebalance gives each everything → duplicate consumption.
    // This assertion hits the network, so the synchronous `poll_until` cannot be used.
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

    // Only the broker can originate the push notification and the test cannot inject it, so this is the sole observation
    // point on the whole path: after B joins the group, the broker pushes `NOTIFY_CONSUMER_IDS_CHANGED(40)` down
    // A's long connection, and the instance-level handler records a count and wakes A's rebalance.
    // What is asserted is not the number (the notification may arrive 0..N times) but "this side really received and
    // processed it" -- with the handler unregistered this keeps timing out while the remoting layer logs a WARN.
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

    // Remove A: once the broker's member list changes it notifies via 40 (or the 20s timer) so B takes over
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
    // The takeover must not depend only on the 20s timed rebalance: when A's connection drops the broker pushes 40 down B's long connection.
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

// ----------------------------------------------------- C9 admin interfaces and backpressure

async fn c9_admin_and_flow_control(ck: &mut Checker, fx: &Fixture) {
    println!("-- C9 admin interfaces / flow-control backpressure / manual triggers");
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

    let key = c.assigned_queue_keys().first().cloned().unwrap_or_default();
    let acc = c.msg_acc_cnt(Some(&key));
    let total_acc = c.compute_accumulation_total();
    ck.check(
        "C9 msgAccCnt per queue and the accumulated total are readable",
        acc >= 0 && total_acc >= 0,
        &format!("key={key} acc={acc} total={total_acc}"),
    );

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

    // Manual triggers: heartbeat / subscribed queue query / immediate rebalance
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

// ------------------------------------------------- C11 pull-stall self-heal (isPullExpired)

/// Send with caller-supplied bodies, bypassing `produce`'s `topic-00i` numbering.
///
/// The self-heal scenario needs "every batch's bodies uniquely named", otherwise "did the retire-and-rebuild
/// redeliver old messages" is masked by numbering collisions -- `produce` restarts at 000 every round, so batches overlap by construction.
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

/// Inject one stall and self-heal through the **production path**; returns whether the heal succeeded.
///
/// Why retries are allowed: the stamp is only taken at the moment a pull is **initiated**, and the real loop is
/// usually parked inside the 30s long poll, so after rewinding the clock the first pass almost always reads as stalled.
/// In rare cases the old pull request returns right away and re-stamps, invalidating this pass (not a bug); retrying beats one gamble after a full 120s sleep.
async fn heal_once(c: &DefaultMQPushConsumer, key: &str) -> bool {
    let injected = now_ms() - PULL_MAX_IDLE_TIME - 5_000;
    for _ in 0..20 {
        c.set_last_pull_at(key, injected);
        if !c.pull_stalled(key) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        // Retire and rebuild both complete inside this one sync; the rebuild immediately re-stamps to "now".
        c.sync_pull_threads().await;
        if c.last_pull_at(key).is_some_and(|t| t > injected + 60_000) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

async fn c11_pull_stall_self_heal(ck: &mut Checker, fx: &Fixture) {
    println!("-- C11 pull loop stalls → one rebalance retires and rebuilds it, no duplicates no losses");
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
    // 1 queue: exactly one loop, a unique injection point, and a unique offset criterion (multiple queues would be shared out to other instances)
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

    // ---- baseline: 3 messages consumed, offset committed to 3, the running info's pull clock is real ----
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
        "C11 consumerRunningInfo reports the real lastPullTimestamp",
        pull_clock_age_ms(&c.consumer_running_info())
            .is_some_and(|age| (0..60_000).contains(&age)),
        &format!("ageMs={:?}", pull_clock_age_ms(&c.consumer_running_info())),
    );

    let healed = heal_once(&c, &key).await;
    let stalled_after = c.pull_stalled(&key);
    let fresh = c.last_pull_at(&key).unwrap_or_default();
    ck.check(
        "C11 a stalled queue is retired and rebuilt inside one rebalance (the stamp went back to now)",
        healed && !stalled_after,
        &format!("key={key} lastPullAt={fresh}"),
    );

    // ---- after the heal this queue must keep consuming normally and must not redeliver old messages ----
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
    // Leave a window for errors like "the new loop rewinds the offset with a stale cursor" to surface
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

// ---------------------------------------------------------------- C12 orderly poison message

/// Orderly-consumption poison-message terminal state: why it can only be verified against a live broker.
///
///
/// All three wrong shapes look "normal" locally: missing the +1 ⇒ the poison message spins forever in place and never reaches the DLQ;
/// reading `-1` as the concurrent side's 16 ⇒ orderly consumption grows phantom dead letters; still suspending after a successful send-back ⇒
/// the queue is stuck forever, indistinguishable from a dead consumer process. Offline tests can only pin the "send-back failed" half (the internal producer that never `start()`ed must fail);
/// "send-back succeeded ⇒ offset advances, `%DLQ%` appears, the lock is really held" only a real broker can rule on.
async fn c12_orderly_dlq(ck: &mut Checker, fx: &Fixture) {
    println!("-- C12 orderly poison message: local counting → handed to the broker → %DLQ%<group>");
    const MAX_RECONSUME: i32 = 2;
    let topic = fx.topic_name("OrdDlq");
    let group = fx.group_name("orddlq");
    if let Err(e) = fx.create_topic(&topic, 1).await {
        return ck.abort("C12 create topic", &e);
    }
    // 1 queue + 1 message per batch: the message after the poison must queue behind it, and the suspend must not drag in other paths
    let poison = format!("{topic}-000");
    let inbox = Arc::new(Inbox::default());
    let mut cfg = fx.base_config(&group);
    cfg.max_reconsume_times = MAX_RECONSUME;
    cfg.consume_message_batch_max_size = 1;
    cfg.suspend_current_queue_time_millis = 500;
    let c = match fx.push(cfg) {
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
        "C12 the poison message is delivered exactly 3 times (each +1 done client-side: reconsumeTimes 0/1/2)",
        three && times.starts_with(&[0, 1, 2]),
        &format!("times={times:?}"),
    );
    let after = format!("{topic}-001");
    let handed = poll_until(|| !inbox.matching(&after).is_empty(), 60).await;
    ck.check(
        "C12 after handing over to the broker the business queue keeps advancing (the next message is consumed)",
        handed,
        &format!("after={after} arrivals={}", inbox.matching(&after).len()),
    );
    tokio::time::sleep(Duration::from_secs(15)).await;
    let seen = inbox.matching(&poison);
    ck.check(
        "C12 no more in-place suspension after exhaustion (the poison message was delivered only 3 times in the observation window)",
        seen.len() == 3,
        &format!("arrivals={}", seen.len()),
    );
    ck.check(
        "C12 during suspension the listener always sees the business topic (local redelivery never changes the topic)",
        seen.iter().all(|d| d.topic == topic),
        &format!("topics={:?}", seen.iter().map(|d| d.topic.clone()).collect::<Vec<_>>()),
    );
    c.shutdown();

    let dlq_topic = MixAll::get_dlq_topic(&group);
    let (has_route, dlq_msgs) = match read_dlq(fx, &group, "orddlqread", 40).await {
        Ok(v) => v,
        Err(e) => return ck.abort("C12 read %DLQ%", &e),
    };
    ck.check(
        "C12 broker auto-created and registered the %DLQ%<group> route",
        has_route,
        &format!("dlq={dlq_topic}"),
    );
    // An orderly send-back landing in the DLQ instead of returning to `%RETRY%` for redelivery is itself the broker
    // deciding "this group's rebalance lock has not expired" ⇒ this instance really holds LOCK_BATCH_MQ (handleRetryAndDLQ:202-207).
    if has_route {
        ck.check(
            "C12 the poison message lands in %DLQ%<group> (the send-back takes the rebalance lock and dead-letters immediately)",
            dlq_msgs.len() == 1 && dlq_msgs[0].body == poison,
            &format!("{dlq_msgs:?}"),
        );
    }
    if let Some(d) = dlq_msgs.first() {
        // 3 = the client's +1 written on RECONSUME_TIME, landing as the stored value via the V2 header flag j; if j is omitted
        // the broker judges by the subscription group's default of 16 and this message never reaches the DLQ.
        ck.check(
            "C12 dead letter reconsumeTimes = maxReconsumeTimes + 1",
            d.reconsume_times == MAX_RECONSUME + 1,
            &format!("reconsumeTimes={}", d.reconsume_times),
        );
        ck.check(
            "C12 the dead letter keeps RETRY_TOPIC=the business topic, and topic is now %DLQ%<group>",
            d.retry_topic_prop.as_deref() == Some(topic.as_str()) && d.topic == dlq_topic,
            &format!("topic={} retryTopic={:?}", d.topic, d.retry_topic_prop),
        );
    }
    // Hand to C10 for deletion; do not leave %DLQ%/%RETRY% litter on the broker
    lock(&fx.topics).push(dlq_topic.clone());
    lock(&fx.topics).push(MixAll::get_retry_topic(&group));
}

/// On the orderly side `-1` means **no cap**, not the concurrent side's 16.
async fn c12b_orderly_no_cap(ck: &mut Checker, fx: &Fixture) {
    println!("-- C12b maxReconsumeTimes=-1 means no cap for orderly consumption (not the concurrent side's 16)");
    let topic = fx.topic_name("OrdNoCap");
    let group = fx.group_name("ordnocap");
    if let Err(e) = fx.create_topic(&topic, 1).await {
        return ck.abort("C12b create topic", &e);
    }
    let poison = format!("{topic}-000");
    let inbox = Arc::new(Inbox::default());
    let mut cfg = fx.base_config(&group);
    // Explicitly no cap (base_config's default is already -1; spelling it out gives the "default" assertion a source)
    cfg.max_reconsume_times = -1;
    cfg.consume_message_batch_max_size = 1;
    cfg.suspend_current_queue_time_millis = 200;
    let c = match fx.push(cfg) {
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
    // Surpassing the concurrent side's 16 is enough to prove the right constant is used. The window is 90s: on the wrong
    // path the 17th attempt's delay level is level20 (2h) and once sent it never comes back, so the only grip is "local delivery count + empty %DLQ%" clamping from both ends.
    let uncapped = poll_until(|| inbox.matching(&poison).len() >= 18, 90).await;
    let times: Vec<i32> = inbox.matching(&poison).iter().map(|d| d.reconsume_times).collect();
    let max_times = times.iter().copied().max().unwrap_or(-1);
    ck.check(
        "C12b -1 means no cap for orderly consumption (delivered >=18 times, the 16 cap did not apply)",
        uncapped && max_times >= 17,
        &format!("arrivals={} maxTimes={max_times}", times.len()),
    );
    c.shutdown();
    let (has_route, dlq_msgs) = match read_dlq(fx, &group, "ordnocapread", 20).await {
        Ok(v) => v,
        Err(e) => return ck.abort("C12b read %DLQ%", &e),
    };
    ck.check(
        "C12b no dead letter before the threshold (the broker need not even create the %DLQ% topic)",
        dlq_msgs.is_empty(),
        &format!("routeFound={has_route} n={}", dlq_msgs.len()),
    );
    if has_route {
        lock(&fx.topics).push(MixAll::get_dlq_topic(&group));
    }
}

/// Listener that observes the orderly suspend duration: on a `poison` hit it sets the context's suspend duration to
/// `asked_ms` and records each delivery time (the gap between neighbours = suspend duration + the dispatch loop's 50ms beat).
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

/// The orderly listener used by C13: explicit commit / explicit rollback / illegal rollback, three postures.
///
/// Kept separate from `LiveListener` because the criterion here is **the interaction between the return value and autoCommit itself**,
/// not the message content; all three modes share one delivery log, comparing "how many times this batch was delivered, at what intervals, whether the topic changed".
struct OrderlyManualListener {
    mode: ManualMode,
    head: String,
    rollback_calls: usize,
    head_calls: AtomicUsize,
    /// [(body, reconsumeTimes, topic, at)], accumulated in delivery order.
    records: Mutex<Vec<(String, i32, String, Instant)>>,
    /// One entry per listener call (element = the body list of that batch).
    batches: Mutex<Vec<Vec<String>>>,
}

#[derive(PartialEq, Clone, Copy)]
enum ManualMode {
    CommitOnce,
    RollbackThenCommit,
    IllegalRollback,
}

impl OrderlyManualListener {
    fn new(mode: ManualMode, head: &str, rollback_calls: usize) -> Arc<OrderlyManualListener> {
        Arc::new(OrderlyManualListener {
            mode,
            head: head.to_string(),
            rollback_calls,
            head_calls: AtomicUsize::new(0),
            records: Mutex::new(Vec::new()),
            batches: Mutex::new(Vec::new()),
        })
    }

    fn count(&self, body: &str) -> usize {
        lock(&self.records).iter().filter(|r| r.0 == body).count()
    }

    fn head_records(&self) -> Vec<(String, i32, String, Instant)> {
        lock(&self.records)
            .iter()
            .filter(|r| r.0 == self.head)
            .cloned()
            .collect()
    }

    fn first_index(&self, body: &str) -> i64 {
        lock(&self.records)
            .iter()
            .position(|r| r.0 == body)
            .map(|i| i as i64)
            .unwrap_or(-1)
    }

    fn nth_index(&self, body: &str, n: usize) -> i64 {
        lock(&self.records)
            .iter()
            .enumerate()
            .filter(|(_, r)| r.0 == body)
            .map(|(i, _)| i as i64)
            .nth(n)
            .unwrap_or(-1)
    }

    fn batches(&self) -> Vec<Vec<String>> {
        lock(&self.batches).clone()
    }

    fn gaps(&self) -> Vec<f64> {
        let head = self.head_records();
        head.windows(2)
            .map(|w| (w[1].3 - w[0].3).as_secs_f64())
            .collect()
    }
}

impl MessageListenerOrderly for OrderlyManualListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        context: &mut ConsumeOrderlyContext,
    ) -> ConsumeOrderlyStatus {
        let now = Instant::now();
        let mut hit = false;
        {
            let mut recs = lock(&self.records);
            let mut batch = Vec::with_capacity(msgs.len());
            for m in msgs {
                let d = Delivered::from(m);
                if d.body == self.head {
                    hit = true;
                }
                batch.push(d.body.clone());
                recs.push((d.body, d.reconsume_times, d.topic, now));
            }
            lock(&self.batches).push(batch);
        }
        match self.mode {
            ManualMode::CommitOnce => {
                context.auto_commit = false;
                ConsumeOrderlyStatus::Commit
            }
            ManualMode::RollbackThenCommit => {
                context.auto_commit = false;
                if hit {
                    let n = self.head_calls.fetch_add(1, Ordering::SeqCst) + 1;
                    if n <= self.rollback_calls {
                        return ConsumeOrderlyStatus::Rollback;
                    }
                }
                ConsumeOrderlyStatus::Commit
            }
            ManualMode::IllegalRollback => ConsumeOrderlyStatus::Rollback,
        }
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

/// #74: `suspendCurrentQueueTimeMillis` on the context takes precedence over the consumer config.
///
///
/// For the two clamping endpoints, gap-based timing on the same chain can only cover "no busy-wait" (`>= 10ms`):
/// the dispatch loop itself has a fixed 50ms beat and gaps cannot tell 10ms from 1ms -- the exact values are pinned by the `orderly_suspend_millis` offline matrix.
async fn c12c_orderly_suspend_millis(ck: &mut Checker, fx: &Fixture) {
    println!("-- C12c the context's suspend duration takes precedence over the consumer config (submitConsumeRequestLater semantics)");
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
    let c = match fx.push(cfg) {
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
    // Wait for the first LOCK_BATCH_MQ: without a queue lock the broker diverts orderly redeliveries straight to the DLQ, making the gaps meaningless
    tokio::time::sleep(Duration::from_secs(4)).await;
    fx.produce(&topic, "TagA", 1, Some(0)).await;
    let _ = poll_until(|| lock(&listener.times).len() >= 9, 25).await;
    let gaps = listener.gaps();
    let med = median(&gaps);
    ck.check(
        "C12c the context's 70ms takes effect (not the consumer config's 900ms)",
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

    // Clamp floor: context 1ms + config 0 (both illegal) must both behave as 10ms, never a busy-wait
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
    let cb = match fx.push(cfg_b) {
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
        "C12c clamp floor: no busy-wait with context 1ms / config 0 (gap >= 10ms)",
        gaps_b.len() >= 6 && median(&gaps_b) >= 0.009,
        &format!("n={} median={:.4}s", gaps_b.len(), median(&gaps_b)),
    );
    cb.shutdown();
}

/// The three legs differ only in the **return value** and the single **autoCommit** boolean, yet live behaviour separates them completely:
async fn c13_orderly_explicit_commit_rollback(ck: &mut Checker, fx: &Fixture) {
    println!("-- C13 orderly explicit batch ack / explicit rollback (immediate redelivery) / illegal usage");

    // ---- C13a autoCommit=false + COMMIT: explicit batch ack ----
    // Three messages **sent before the consumer starts** (a new group starts explicitly at the queue head, same convention as C4c's partial ack) + batch cap 3:
    // the first batch is then necessarily the complete three, giving "accept the whole batch at once" a deterministic observation surface.
    let topic = fx.topic_name("OrdCommit");
    let group = fx.group_name("ordcommit");
    if let Err(e) = fx.create_topic(&topic, 1).await {
        return ck.abort("C13a create topic", &e);
    }
    let listener = OrderlyManualListener::new(ManualMode::CommitOnce, "", 0);
    let mut cfg = fx.base_config(&group);
    cfg.consume_message_batch_max_size = 3;
    cfg.suspend_current_queue_time_millis = 200;
    let c = match fx.push(cfg) {
        Ok(v) => v,
        Err(e) => return ck.abort("C13a build consumer", &format!("{e}")),
    };
    if let Err(e) = c.subscribe(&topic, "*") {
        return ck.abort("C13a subscribe", &format!("{e}"));
    }
    c.set_message_listener_orderly(listener.clone());
    let sent = fx.produce(&topic, "T13a", 3, Some(0)).await;
    let want: Vec<String> = (0..3).map(|i| format!("{topic}-{i:03}")).collect();
    if let Err(e) = c.start().await {
        return ck.abort("C13a start", &format!("{e}"));
    }
    let arrived = poll_until(|| !listener.batches().is_empty(), 30).await;
    tokio::time::sleep(Duration::from_secs(6)).await; // counter-proof window: treated as "not committed", it would be redelivered ~30 times
    c.shutdown();
    let batches = listener.batches();
    ck.check(
        "C13a the first batch is a complete 3-element batch (send first, start later + batch cap 3)",
        arrived && batches.len() == 1 && batches[0].len() == 3,
        &format!("batches={batches:?} sent={}", sent.len()),
    );
    ck.check(
        "C13a explicit COMMIT accepts the whole batch: delivered once, none of the three redelivered",
        batches == vec![want.clone()],
        &format!("batches={batches:?}"),
    );
    let committed = fx.wait_committed(&group, &topic, 1, 3).await;
    ck.check(
        "C13a the broker offset advances straight to 3 (whole-batch ack, not stuck at 0)",
        committed == 3,
        &format!("committed={committed}"),
    );

    // ---- C13b autoCommit=false + ROLLBACK: explicit rollback, immediate local redelivery ----
    // Roll back 6 times, then commit explicitly: the first 6 passes each return this message, and **the broker is not involved**.
    let topic = fx.topic_name("OrdRollback");
    let group = fx.group_name("ordrollback");
    if let Err(e) = fx.create_topic(&topic, 1).await {
        return ck.abort("C13b create topic", &e);
    }
    let head = format!("{topic}-000");
    let next = format!("{topic}-001");
    let listener = OrderlyManualListener::new(ManualMode::RollbackThenCommit, &head, 6);
    let mut cfg = fx.base_config(&group);
    cfg.consume_message_batch_max_size = 1;
    cfg.suspend_current_queue_time_millis = 200;
    let c = match fx.push(cfg) {
        Ok(v) => v,
        Err(e) => return ck.abort("C13b build consumer", &format!("{e}")),
    };
    if let Err(e) = c.subscribe(&topic, "*") {
        return ck.abort("C13b subscribe", &format!("{e}"));
    }
    c.set_message_listener_orderly(listener.clone());
    if let Err(e) = c.start().await {
        return ck.abort("C13b start", &format!("{e}"));
    }
    tokio::time::sleep(Duration::from_secs(4)).await; // wait for the first LOCK_BATCH_MQ
    fx.produce(&topic, "T13b", 2, Some(0)).await;
    let ok = poll_until(
        || listener.first_index(&next) >= 0 && listener.head_calls.load(Ordering::SeqCst) >= 7,
        45,
    )
    .await;
    let head_at = listener.head_records();
    let gaps = listener.gaps();
    let med = median(&gaps);
    let worst = gaps.iter().cloned().fold(0.0f64, f64::max);
    let idx7 = listener.nth_index(&head, 6);
    let idx_next = listener.first_index(&next);
    ck.check(
        "C13b explicit rollback returns the same batch for redelivery (head delivered 7 times = 6 rollbacks + 1 commit)",
        ok && head_at.len() >= 7,
        &format!("deliveries={}", head_at.len()),
    );
    ck.check(
        "C13b local redelivery bypasses the broker: adjacent gaps hug the 200ms suspend (the fastest %RETRY% level is 10s)",
        gaps.len() >= 5 && med < 1.0 && worst < 5.0,
        &format!(
            "n={} median={med:.3}s max={worst:.3}s gaps={:?}",
            gaps.len(),
            gaps.iter().take(6).map(|g| (g * 1000.0).round() / 1000.0).collect::<Vec<_>>()
        ),
    );
    ck.check(
        "C13b local redelivery leaves reconsumeTimes untouched (all 0) and keeps the topic",
        !head_at.is_empty()
            && head_at.iter().all(|r| r.1 == 0)
            && head_at.iter().all(|r| r.2 == topic),
        &format!(
            "times={:?} topics={:?}",
            head_at.iter().take(8).map(|r| r.1).collect::<Vec<_>>(),
            head_at.iter().map(|r| r.2.clone()).collect::<BTreeSet<_>>()
        ),
    );
    ck.check(
        "C13b during rollbacks the following message never jumps the queue (next first appears after the 7th delivery)",
        idx7 >= 0 && idx_next > idx7,
        &format!("idx7={idx7} idxNext={idx_next}"),
    );
    tokio::time::sleep(Duration::from_secs(2)).await; // counter-proof window: after the commit head must not come back
    ck.check(
        "C13b one delivery each after the explicit commit (the commit really released head)",
        listener.count(&head) == 7 && listener.count(&next) == 1,
        &format!(
            "head={} next={}",
            listener.count(&head),
            listener.count(&next)
        ),
    );
    c.shutdown();
    let committed = fx.wait_committed(&group, &topic, 1, 2).await;
    ck.check(
        "C13b the broker offset advances to 2 (both messages fully acked)",
        committed == 2,
        &format!("committed={committed}"),
    );

    // ---- C13c autoCommit=true (default) + ROLLBACK: illegal usage, treated as ack ----
    let topic = fx.topic_name("OrdIllegal");
    let group = fx.group_name("ordillegal");
    if let Err(e) = fx.create_topic(&topic, 1).await {
        return ck.abort("C13c create topic", &e);
    }
    let head = format!("{topic}-000");
    let next = format!("{topic}-001");
    let listener = OrderlyManualListener::new(ManualMode::IllegalRollback, &head, 0);
    let mut cfg = fx.base_config(&group);
    cfg.consume_message_batch_max_size = 1;
    cfg.suspend_current_queue_time_millis = 200;
    let c = match fx.push(cfg) {
        Ok(v) => v,
        Err(e) => return ck.abort("C13c build consumer", &format!("{e}")),
    };
    if let Err(e) = c.subscribe(&topic, "*") {
        return ck.abort("C13c subscribe", &format!("{e}"));
    }
    c.set_message_listener_orderly(listener.clone());
    if let Err(e) = c.start().await {
        return ck.abort("C13c start", &format!("{e}"));
    }
    tokio::time::sleep(Duration::from_secs(4)).await;
    fx.produce(&topic, "T13c", 2, Some(0)).await;
    let ok = poll_until(|| listener.first_index(&next) >= 0, 30).await;
    tokio::time::sleep(Duration::from_millis(2500)).await; // counter-proof window: a real rollback would redeliver ~10 times
    ck.check(
        "C13c with autoCommit=true ROLLBACK is treated as ack: head is delivered only once",
        listener.count(&head) == 1,
        &format!("head={}", listener.count(&head)),
    );
    ck.check(
        "C13c the illegal ROLLBACK does not block the queue: next is consumed",
        ok && listener.count(&next) == 1,
        &format!("next={}", listener.count(&next)),
    );
    c.shutdown();
    let committed = fx.wait_committed(&group, &topic, 1, 2).await;
    ck.check(
        "C13c the broker offset advances to 2 (illegal usage lands as a successful ack)",
        committed == 2,
        &format!("committed={committed}"),
    );
}

// ---------------------------------------------------------------- C10 cleanup
async fn c10_cleanup(ck: &mut Checker, fx: &mut Fixture) {
    println!("-- C10 cleanup");
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

async fn run(namesrv: &str, creds: Creds) -> Checker {
    let stamp_s = stamp();
    println!("== rust live_consumer ==");
    println!("   namesrv   = {namesrv}");
    println!("   stamp     = {stamp_s}");
    println!(
        "   acl       = {}",
        if creds.enabled() {
            format!("ak={}", creds.access_key)
        } else {
            "no credentials (plain cluster, no hook installed)".to_string()
        }
    );

    let mut ck = Checker::new();
    // The reverse control must run before any other client: see the [`anon_probe`] notes.
    if creds.enabled() {
        anon_probe(namesrv, &stamp_s, &mut ck).await;
    }

    let mut fx = match Fixture::new(namesrv, &stamp_s, creds) {
        Ok(f) => f,
        Err(e) => {
            ck.abort("C0 fixture build", &e);
            return ck;
        }
    };
    if let Err(e) = fx.start().await {
        ck.abort("C0 fixture start", &e);
        return ck;
    }
    println!("   broker    = {} @ {}", fx.broker_name, fx.broker_addr);
    println!("   producer  = {}", fx.producer_group);

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
    c13_orderly_explicit_commit_rollback(&mut ck, &fx).await;
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
    let argv: Vec<String> = env::args().skip(1).collect();
    let namesrv = argv
        .first()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:9876".to_string());
    // The two trailing arguments are optional: `[access_key] [secret_key]`; the ACL signing hook is installed only when both are non-empty.
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
