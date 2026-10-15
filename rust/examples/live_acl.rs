//! Integration verification of ACL authentication against a **real 5.5.1 broker**
//! (covers S1..S8, where S8 is the pull-mode signature chain).
//!
//!
//! Every assertion checks whether the broker really accepts/rejects the signature,
//! not local encoding/decoding:
//! - S1: an instance carrying credentials creates the topic successfully (the admin path signature is accepted).
//! - S2: creating the topic **without credentials** → the broker replies NO_PERMISSION(16).
//! - S3: send from a producer whose **secretKey is wrong** → 16. A mis-computed signature and no
//!   signature at all are rejected the same way on the broker side, so this case also proves the
//!   reverse: that the signature *content* participates in validation rather than "any request that
//!   carries an AccessKey is let through".
//! - S4: a producer with credentials sends 3 messages, all SEND_OK, and the msgIds are assigned by
//!   the broker (non-empty msgId proves the signed request was adopted).
//! - S5: a push consumer with credentials receives all 3 messages —— the heartbeat / long-polling
//!   pull / offset commit RPCs all carry the signature end to end; a missing signature on any one of
//!   them means no message arrives.
//! - S6: a raw broker RPC **without credentials** (GET_CONSUMER_LIST_BY_GROUP) → 16.
//!   Note it must go through [`get_consumer_list_by_group`] with an explicit addr, not
//!   [`get_consumer_id_list_by_group`]: the latter swallows the exception and returns `None`,
//!   which makes "rejected" and "succeeded" look identical to the caller.
//! - S7: querying **NameServer** routes without credentials still succeeds —— authentication only
//!   happens on the broker side, which proves the hook did not break the namesrv path.
//! - S8: a pull consumer with credentials: the four signed RPCs `fetch_subscribe_message_queues` /
//!   `pull` / `update_consume_offset` / `fetch_consume_offset` close the loop.
//!   Pull-mode offset RPCs use a different request code, so they are covered separately here.
//! - S9 cleanup: delete the topic created this round, with credentials.
//!
//! ⚠ S4/S5 must **start the consumer before sending**: a push consumer defaults to
//! `CONSUME_FROM_LAST_OFFSET`, which resolves the initial offset of a brand-new consumer group to the
//! queue's maxOffset **at that moment**, and the broker's consumequeue is dispatched asynchronously,
//! so "send first, receive later" is not deterministic.
//! Here we both start the consumer first *and* explicitly use `CONSUME_FROM_FIRST_OFFSET`, keeping
//! both safeguards.
//!
//! ⚠ Every role uses its own `instance_name` (→ its own clientId → its own [`MQClientInstance`]
//! → its own remoting channel). Credentials are **registered on the channel**, so if two roles share
//! one instance, "rejected without credentials" would be carried through by the other role's
//! signature and turn into a false PASS.
//!
//! Usage:
//! ```text
//! cargo run --example live_acl -- 127.0.0.1:9876 [accessKey] [secretKey]
//! ```
//!
//! [`get_consumer_list_by_group`]: rocketmq_client_remoting::client::mq_client::MQClientInstance::get_consumer_list_by_group
//! [`get_consumer_id_list_by_group`]: rocketmq_client_remoting::client::mq_client::MQClientInstance::get_consumer_id_list_by_group

use std::collections::BTreeSet;
use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultMQPullConsumer, PullConsumerConfig,
};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently, PullStatus,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;
use rocketmq_client_remoting::remoting::rpchook::{AclClientRPCHook, RPCHook, SessionCredentials};

/// Number of queues for the created topic (4 by default).
const QUEUE_NUMS: i32 = 4;
/// How many messages S4 sends.
const N_MSG: usize = 3;
const NO_PERMISSION: i32 = 16;
/// How long we wait for the push consumer to receive everything.
const RECV_WAIT: Duration = Duration::from_secs(30);

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// Assertion accumulator: run all scenarios first and then summarize; the first failure does not
/// exit early.
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

/// Read the inner value even when the lock is poisoned (one panic should not bring down the whole
/// verification run).
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// Rejection assertion: it must be a NO_PERMISSION returned by the broker, not an
/// unreachable/timeout.
///
/// Both "no 16 at all" and "a 16 for some other reason" count as failures —— if the broker happens
/// to be configured without authentication, all the positive scenarios still pass green; only the
/// negative scenarios can expose that false pass.
fn denied(err: &Error) -> bool {
    err.response_code() == Some(NO_PERMISSION)
}

fn code_of(err: &Error) -> String {
    match err.response_code() {
        Some(code) => format!("code={code}"),
        None => "code=?".to_string(),
    }
}

fn acl_hook(access_key: &str, secret_key: &str) -> Arc<dyn RPCHook> {
    Arc::new(AclClientRPCHook::new(SessionCredentials::new(
        access_key,
        secret_key,
    )))
}

// ------------------------------------------------------------------ listener

#[derive(Default)]
struct Inbox {
    bodies: Mutex<BTreeSet<String>>,
}

impl Inbox {
    fn len(&self) -> usize {
        lock(&self.bodies).len()
    }

    fn snapshot(&self) -> Vec<String> {
        lock(&self.bodies).iter().cloned().collect()
    }
}

struct AclListener(Arc<Inbox>);

impl MessageListenerConcurrently for AclListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        for m in msgs {
            lock(&self.0.bodies)
                .insert(String::from_utf8_lossy(m.get_body()).into_owned());
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

// ------------------------------------------------------------------ fixtures

struct Fixture {
    namesrv: String,
    stamp: String,
    access_key: String,
    secret_key: String,
    topic: String,
    group: String,
    broker_name: String,
    broker_addr: String,
    /// A credential-free instance used only to locate a broker address (reused by both the
    /// negative and the compatibility scenarios S6/S7).
    probe: MQClientInstance,
}

impl Fixture {
    fn new(namesrv: &str, access_key: &str, secret_key: &str) -> Result<Fixture, String> {
        let stamp = stamp();
        let probe = MQClientInstance::new(
            &format!("rust-live-acl-probe-{stamp}"),
            vec![namesrv.to_string()],
        );
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            topic: format!("RustLiveAcl{stamp}"),
            group: format!("rust-live-acl-{stamp}"),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            stamp,
            broker_name: String::new(),
            broker_addr: String::new(),
            probe,
        })
    }

    async fn start(&mut self) -> Result<(), String> {
        self.probe
            .start()
            .await
            .map_err(|e| format!("probe start failed: {e}"))?;
        // Route lookup is not authenticated (that is exactly what S7 proves), so the route of
        // TBW102 is enough to locate a broker.
        let route = self
            .probe
            .get_topic_route_data(MixAll::DEFAULT_TOPIC)
            .await
            .ok_or_else(|| format!("no route of {} from namesrv", MixAll::DEFAULT_TOPIC))?;
        let (broker_name, broker_addr) = broker_of(&route)?;
        self.broker_name = broker_name;
        self.broker_addr = broker_addr;
        Ok(())
    }

    fn instance_name(&self, kind: &str) -> String {
        format!("live-acl-{kind}-{}", self.stamp)
    }

    /// Start a bare **credential-carrying** instance (used as the admin: create/delete topic).
    ///
    /// The Rust admin layer has not been ported yet,
    /// and what a producer does internally is precisely "create the instance → register the hook
    /// on the remoting channel",
    /// so we do those two steps directly here, which is semantically equivalent.
    async fn signed_admin(&self, kind: &str) -> Result<MQClientInstance, String> {
        let instance = MQClientInstance::new(
            &format!("rust-live-acl-{kind}-{}", self.stamp),
            vec![self.namesrv.clone()],
        );
        instance
            .remoting_client()
            .register_rpc_hook(acl_hook(&self.access_key, &self.secret_key));
        instance
            .start()
            .await
            .map_err(|e| format!("{kind} instance start failed: {e}"))?;
        Ok(instance)
    }

    /// Create a topic with credentials (not relying on `autoCreateTopicEnable`: once authentication
    /// is on, anonymous clients cannot connect to the broker, and falling back to the default topic
    /// yields no deterministic queue count either).
    async fn create_topic(&self, admin: &MQClientInstance, topic: &str) -> Result<(), String> {
        admin
            .create_topic_in_broker(
                &self.broker_addr,
                MixAll::DEFAULT_TOPIC,
                topic,
                QUEUE_NUMS,
                QUEUE_NUMS,
                DEFAULT_PERM,
                0,
                TopicFilterType::SINGLE_TAG,
                false,
                None,
                5000,
                2,
            )
            .await
            .map_err(|e| format!("create topic {topic} failed ({e}): {}", code_of(&e)))
    }

    /// A producer with credentials (an empty `sk` means "use the secretKey held by the fixture").
    fn producer(&self, kind: &str, secret_key: Option<&str>) -> Result<DefaultMQProducer, String> {
        let sk = secret_key.unwrap_or(&self.secret_key);
        let producer = DefaultMQProducer::with_rpc_hook(
            &format!("rust-live-acl-{kind}-{}", self.stamp),
            Some(acl_hook(&self.access_key, sk)),
        )
        .map_err(|e| format!("{kind} producer build failed: {e}"))?;
        producer.set_namesrv_addr(&self.namesrv);
        producer.set_instance_name(&self.instance_name(kind));
        Ok(producer)
    }

    async fn send_bodies(&self, producer: &DefaultMQProducer, bodies: &[String]) -> Vec<String> {
        let mut evidence = Vec::new();
        for body in bodies {
            let mut msg = Message::new(&self.topic, Some(body.as_bytes()));
            msg.set_keys("acl");
            evidence.push(match producer.send(&mut msg, Some(5000), None).await {
                Ok(r) => format!("{}:{}", r.status, r.msg_id.clone().unwrap_or_default()),
                Err(e) => format!("EXC({}):{}", e, code_of(&e)),
            });
        }
        evidence
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

// ------------------------------------------------------------------ S1/S2 admin

/// S1: creating the topic with credentials succeeds; S2: creating it without credentials is
/// rejected.
async fn s1_s2_admin(ck: &mut Checker, fx: &Fixture, denied_topic: &str) -> bool {
    println!("\nS1/S2 admin create topic: with credentials vs without credentials");
    let admin = match fx.signed_admin("admin").await {
        Ok(admin) => admin,
        Err(e) => {
            ck.abort("S1 start instance with credentials", &e);
            return false;
        }
    };
    let created = match fx.create_topic(&admin, &fx.topic).await {
        Ok(()) => {
            ck.check("S1 create topic with credentials succeeded", true, "");
            true
        }
        Err(e) => {
            ck.abort("S1 create topic with credentials", &e);
            false
        }
    };
    admin.shutdown();

    // S2: the very same RPC against the very same broker, just without registering the hook.
    match fx
        .probe
        .create_topic_in_broker(
            &fx.broker_addr,
            MixAll::DEFAULT_TOPIC,
            denied_topic,
            QUEUE_NUMS,
            QUEUE_NUMS,
            DEFAULT_PERM,
            0,
            TopicFilterType::SINGLE_TAG,
            false,
            None,
            5000,
            0,
        )
        .await
    {
        Ok(()) => ck.check(
            "S2 create topic without credentials rejected by broker",
            false,
            "the broker accepted an anonymous request (most likely authenticationEnabled is off)",
        ),
        Err(e) => ck.check(
            "S2 create topic without credentials rejected by broker",
            denied(&e),
            &format!("{} {e}", code_of(&e)),
        ),
    }
    created
}

// ------------------------------------------------------------------ S3 wrong secretKey

async fn s3_bad_secret(ck: &mut Checker, fx: &Fixture) {
    println!("\nS3 a producer with a wrong secretKey must be rejected");
    let producer = match fx.producer("badsk", Some("WRONG_SECRET_KEY")) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("S3 build", &e);
            return;
        }
    };
    if let Err(e) = producer.start().await {
        ck.abort("S3 start", &e.to_string());
        return;
    }
    let evidence = fx.send_bodies(&producer, &["should-not-send".to_string()]).await;
    // send_bodies squashes errors into an evidence string: here success is decided by whether a
    // "SEND_OK" shows up.
    let accepted = evidence.iter().any(|s| s.contains("SendOk") || s.starts_with("SEND_OK"));
    ck.check(
        "S3 wrong secretKey rejected by broker",
        !accepted,
        &format!("evidence={evidence:?}"),
    );
    producer.shutdown();
}

// ------------------------------------------------------------------ S4/S5 positive path

/// Start the push consumer with credentials first, then send, then confirm everything arrived.
async fn s4_s5_signed_roundtrip(ck: &mut Checker, fx: &Fixture) -> Vec<String> {
    println!("\nS4/S5 producer/consumer with correct credentials (start the consumer before sending)");
    let bodies: Vec<String> = (0..N_MSG).map(|i| format!("acl-ok-{i}")).collect();

    let inbox = Arc::new(Inbox::default());
    let consumer = match DefaultMQPushConsumer::with_config(ConsumerConfig {
        consumer_group: fx.group.clone(),
        name_server_addrs: vec![fx.namesrv.clone()],
        instance_name: fx.instance_name("push"),
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("S5 build consumer", &e.to_string());
            return Vec::new();
        }
    };
    consumer.set_rpc_hook(Some(acl_hook(&fx.access_key, &fx.secret_key)));
    if let Err(e) = consumer.subscribe(&fx.topic, "*") {
        ck.abort("S5 subscribe", &e.to_string());
        return Vec::new();
    }
    consumer.set_message_listener_concurrently(Arc::new(AclListener(inbox.clone())));
    if let Err(e) = consumer.start().await {
        ck.abort("S5 start consumer (heartbeat/registration needs a signature)", &e.to_string());
        return Vec::new();
    }

    // Wait for the first rebalance to assign the queues: the initial offset must be resolved while
    // the topic is still empty.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let producer = match fx.producer("ok", None) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("S4 build", &e);
            consumer.shutdown();
            return Vec::new();
        }
    };
    if let Err(e) = producer.start().await {
        ck.abort("S4 start producer (registration needs a signature)", &e.to_string());
        consumer.shutdown();
        return Vec::new();
    }
    let evidence = fx.send_bodies(&producer, &bodies).await;
    producer.shutdown();
    let sent = evidence
        .iter()
        .filter(|s| s.starts_with("SEND_OK") || s.contains("SendOk"))
        .count();
    ck.check(
        &format!("S4 producer with credentials sends {N_MSG} messages"),
        sent == N_MSG,
        &format!("sent={sent} evidence={evidence:?}"),
    );
    // The msgId is assigned by the broker: a non-empty one proves the broker really adopted this
    // signed request.
    ck.check(
        "S4 broker assigned a msgId to the messages",
        evidence.iter().all(|s| s.split(':').nth(1).is_some_and(|id| !id.is_empty())),
        &format!("evidence={evidence:?}"),
    );

    let deadline = Instant::now() + RECV_WAIT;
    while Instant::now() < deadline && inbox.len() < sent {
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    ck.check(
        &format!("S5 push consumer with credentials received all {sent} messages (heartbeat/pull/offset commit all signed)"),
        sent == N_MSG && inbox.len() == sent,
        &format!("got={} bodies={:?}", inbox.len(), inbox.snapshot()),
    );
    consumer.shutdown();
    if sent == N_MSG { bodies } else { Vec::new() }
}

// ------------------------------------------------------------------ S6/S7 boundary

async fn s6_unsigned_broker_rpc(ck: &mut Checker, fx: &Fixture) {
    println!("\nS6 a direct broker RPC without credentials must be rejected");
    match fx
        .probe
        .get_consumer_list_by_group(&fx.group, 5000, Some(&fx.broker_addr))
        .await
    {
        Ok(list) => ck.check(
            "S6 broker RPC without credentials rejected",
            false,
            &format!("the broker actually replied {:?} (most likely authentication is off)", list),
        ),
        Err(e) => ck.check(
            "S6 broker RPC without credentials rejected",
            denied(&e),
            &format!("{} {e}", code_of(&e)),
        ),
    }
}

async fn s7_namesrv_without_creds(ck: &mut Checker, fx: &Fixture) {
    println!("\nS7 a NameServer route query without credentials should still succeed");
    match fx
        .probe
        .update_topic_route_info_from_name_server(&fx.topic, 5000, false)
        .await
    {
        Ok(ok) => ck.check(
            "S7 namesrv route query without credentials succeeded",
            ok,
            &format!("update returned {ok}"),
        ),
        Err(e) => ck.check(
            "S7 namesrv route query without credentials succeeded",
            false,
            &format!("{} {e}", code_of(&e)),
        ),
    }
}

// ------------------------------------------------------------------ S8 pull-mode signature chain

async fn s8_signed_pull(ck: &mut Checker, fx: &Fixture, bodies: &[String]) {
    println!("\nS8 pull consumer with credentials: fetch queues / pull / commit offsets, all signed");
    if bodies.is_empty() {
        ck.abort("S8 pull", "S4/S5 did not send any message, skipping");
        return;
    }
    let consumer = match DefaultMQPullConsumer::with_config(PullConsumerConfig {
        consumer_group: format!("{}-pull", fx.group),
        name_server_addrs: vec![fx.namesrv.clone()],
        instance_name: fx.instance_name("pull"),
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("S8 build", &e.to_string());
            return;
        }
    };
    consumer.set_rpc_hook(Some(acl_hook(&fx.access_key, &fx.secret_key)));
    if let Err(e) = consumer.start().await {
        ck.abort("S8 start", &e.to_string());
        return;
    }
    let mqs = match consumer.fetch_subscribe_message_queues(&fx.topic).await {
        Ok(mqs) => mqs,
        Err(e) => {
            ck.abort(
                "S8 fetch_subscribe_message_queues (signed route lookup)",
                &format!("{} {e}", code_of(&e)),
            );
            consumer.shutdown();
            return;
        }
    };
    let mut got: BTreeSet<String> = BTreeSet::new();
    let mut commit_failed: Vec<String> = Vec::new();
    let mut readback_failed: Vec<String> = Vec::new();
    for mq in &mqs {
        // The group offset of a never-committed group is None (with setZeroIfNotFound=false semantics),
        // so we start pulling from 0.
        let offset = consumer
            .fetch_consume_offset(mq)
            .await
            .unwrap_or_default()
            .unwrap_or(0);
        match consumer.pull(mq, "*", offset, 32, Some(5000)).await {
            Ok(r) if r.status == PullStatus::Found => {
                for m in &r.msg_found_list {
                    got.insert(String::from_utf8_lossy(m.get_body()).into_owned());
                }
                // Offset commit is a different RPC (UPDATE_CONSUMER_OFFSET); a missing signature makes it fail
                // silently as an exception.
                if let Err(e) =
                    consumer.update_consume_offset(mq, r.next_begin_offset).await
                {
                    commit_failed.push(format!("{}:{e}", code_of(&e)));
                }
                match consumer.fetch_consume_offset(mq).await {
                    Ok(Some(v)) if v == r.next_begin_offset => {}
                    other => readback_failed.push(format!("{mq:?}=>{other:?}")),
                }
            }
            Ok(r) => {
                // An empty queue / no match is fine, as long as the signature was not rejected.
                println!("  [INFO] {} pulled {:?}", mq.queue_id, r.status);
            }
            Err(e) => commit_failed.push(format!("pull:{} {e}", code_of(&e))),
        }
    }
    let want: BTreeSet<String> = bodies.iter().cloned().collect();
    ck.check(
        &format!("S8 pull with credentials fetched all {} messages", bodies.len()),
        got == want,
        &format!("got={got:?}"),
    );
    ck.check(
        "S8 offsets committed with credentials and read back consistently",
        commit_failed.is_empty() && readback_failed.is_empty(),
        &format!("commit={commit_failed:?} readback={readback_failed:?}"),
    );
    consumer.shutdown();
}

// ------------------------------------------------------------------ S9 cleanup

async fn s9_cleanup(ck: &mut Checker, fx: &Fixture) {
    println!("\nS9 cleanup: delete the topic created this round, with credentials");
    match fx.signed_admin("cleaner").await {
        Ok(admin) => {
            let r = admin.delete_topic_in_broker(&fx.broker_addr, &fx.topic, 5000).await;
            ck.check(
                "S9 delete the topic created this round",
                r.is_ok(),
                &match r {
                    Err(e) => format!("{} {e}", code_of(&e)),
                    Ok(()) => String::new(),
                },
            );
            admin.shutdown();
        }
        Err(e) => ck.abort("S9 start instance", &e),
    }
}

// ------------------------------------------------------------------ driver

async fn run(namesrv: &str, access_key: &str, secret_key: &str) -> Checker {
    let mut ck = Checker::new();
    let mut fx = match Fixture::new(namesrv, access_key, secret_key) {
        Ok(fx) => fx,
        Err(e) => {
            ck.abort("build fixture", &e);
            return ck;
        }
    };
    println!("== live ACL check, namesrv={namesrv} ak={access_key} ==");
    if let Err(e) = fx.start().await {
        ck.abort("start fixture (query namesrv route)", &e);
        return ck;
    }
    println!(
        "   broker={}@{} topic={} group={}",
        fx.broker_name, fx.broker_addr, fx.topic, fx.group
    );

    let denied_topic = format!("{}__DENIED", fx.topic);
    if s1_s2_admin(&mut ck, &fx, &denied_topic).await {
        s3_bad_secret(&mut ck, &fx).await;
        let bodies = s4_s5_signed_roundtrip(&mut ck, &fx).await;
        s6_unsigned_broker_rpc(&mut ck, &fx).await;
        s7_namesrv_without_creds(&mut ck, &fx).await;
        s8_signed_pull(&mut ck, &fx, &bodies).await;
        s9_cleanup(&mut ck, &fx).await;
    } else {
        ck.abort("remaining scenarios", "the topic was not created, skipping");
    }
    fx.probe.shutdown();
    ck
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
    let access_key = argv
        .get(1)
        .cloned()
        .unwrap_or_else(|| "AK_TEST".to_string());
    let secret_key = argv
        .get(2)
        .cloned()
        .unwrap_or_else(|| "SK_TEST_SECRET_12345678".to_string());
    let mut ck = run(&namesrv, &access_key, &secret_key).await;
    report(&mut ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
