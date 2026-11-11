//! Live verification of the 2026-11-11 capability closeout (Java 5.x parity) on Rust.
//!
//! Counterpart of `python/verify_capability_closeout_live.py`: the three capabilities added in
//! that round are only worth a real broker if they are either wire-visible or change **which
//! messages a client sees**, so each one is asserted against an actual 5.5.1 cluster.
//!
//! L1  ConsumeFromWhere's three "deprecated" values.
//!     Java folds `CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST` / `CONSUME_FROM_MIN_OFFSET` /
//!     `CONSUME_FROM_MAX_OFFSET` into the `CONSUME_FROM_LAST_OFFSET` branch of
//!     `RebalancePushImpl#computePullFromWhereWithException` (lines 171-173). **That folding is not
//!     asserted on delivery counts here**, because the broker makes those counts non-deterministic: for
//!     a never-committed group on a fresh topic it answers QUERY_CONSUMER_OFFSET with SUCCESS offset=0
//!     on its V3_0_6 path (minOffset<=0 && offset 0 still in memory) or with QUERY_NOT_FOUND, and that
//!     answer flips between legs (the Java reference client measured FIRST=8, LAST=0, MIN=8, MAX=8,
//!     LAST_AND_MIN=0 in one run of this very scenario).
//!     What pins the folding is pure logic: `plan_initial_offset` (src/client/consumer.rs:2500-2531)
//!     gives only `CONSUME_FROM_FIRST_OFFSET` and `CONSUME_FROM_TIMESTAMP` a branch of their own, so
//!     LAST_OFFSET and the three deprecated values reach the same `QueueTail` by construction. The
//!     offline suite pins those branches against the resolved plan (no broker involved):
//!     `initial_offset_plan_never_takes_the_subscription_into_account` asserts LAST_OFFSET -> QueueTail,
//!     and `consume_from_where_carries_all_six_java_values` pins the six wire strings.
//!     The live legs therefore assert acceptance, never an oracle: one backlog, five fresh groups,
//!     FIRST_OFFSET delivers the whole backlog (deterministic positive control) and every LAST-family
//!     value must start, take a real queue assignment and consume a sane count (0..=8) without an
//!     exception. The five counts are printed as the parity record.
//!     Per-end baseline note: this port always sends `setZeroIfNotFound=false`
//!     (`resolve_initial_offset` -> `query_consumer_offset(..., false)`), so 5.5.1 answers
//!     QUERY_NOT_FOUND and LAST_OFFSET starts at the tail -- unlike Java, which sends no such field and
//!     inherits the broker's offset=0 answer (whole backlog replayed).
//!
//! L2  LitePull pull-from-which-node pin (`connectBrokerByUser` / `defaultBrokerId`).
//!     Java `PullAPIWrapper#recalculatePullFromWhichNode` pins the id and ignores the broker's
//!     suggestion table -- the only way to read from a slave. This lite path keeps no suggestion table
//!     (its rounds go straight to `MQClientInstance::pull_message`), and the round reads the two fields
//!     exactly once per request (`pull_one`: `connect_broker_by_user.then_some(default_broker_id)`),
//!     so reading them back after `start()` reads what the next request would carry:
//!         pin off          -> no broker id on the request at all, backlog delivered
//!         pin 0 (master)   -> the request carries brokerId=0, backlog delivered
//!         pin 1 (no slave) -> the request carries brokerId=1, and the backlog is **still** delivered:
//!                            Java resolves with `findBrokerAddressInSubscribe(..., onlyThisBroker=false)`,
//!                            which falls back to the first broker in the table when the id is absent.
//!                            That fallback is parity, not a hole -- the cluster premise (no slave) is
//!                            printed so the leg can never be read as "reads the slave".
//!
//! L3  adminExtGroup.
//!     A pure identity bit with no wire effect (this port's admin sends no synthetic consumer
//!     heartbeat), so the live leg proves admin still boots and serves an RPC with a custom group,
//!     plus the default/override values themselves.
//!
//! ```text
//! cargo run --quiet --example live_capability_closeout -- 127.0.0.1:9876
//! ```
//!
//! Every run creates a fresh topic and fresh groups (a group that already committed offsets reads
//! them back whatever `consumeFromWhere` says -- the exact false green this scenario must avoid).

use std::collections::BTreeSet;
use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{DefaultMQAdminExt, DEFAULT_ADMIN_EXT_GROUP};
use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultLitePullConsumer, LitePullConsumerConfig,
};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently, SendStatus,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

/// Backlog of every leg. One queue per topic so every number is countable.
const N_MSG: usize = 8;
/// Observation window of one push leg (the first rebalance and the first long poll must both fit).
const PUSH_WINDOW_SECS: u64 = 12;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// Assertion accumulator: every leg runs, then one summary; the first failure does not exit early.
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
        let line = if detail.is_empty() {
            String::new()
        } else {
            format!("  {detail}")
        };
        if cond {
            self.passed += 1;
            println!("  [PASS] {name}{line}");
        } else {
            println!("  [FAIL] {name}{line}");
            self.failed.push(format!("{name}: {detail}"));
        }
    }

    fn abort(&mut self, name: &str, err: &str) {
        println!("  [FAIL] {name}: {err}");
        self.failed.push(format!("{name}: {err}"));
    }
}

// ------------------------------------------------------------------ fixture

struct Fixture {
    namesrv: String,
    prefix: String,
    /// Setup instance: creates topics on the broker and reads routes back from the name server.
    setup: MQClientInstance,
    producer: DefaultMQProducer,
    broker_name: String,
    broker_addr: String,
}

impl Fixture {
    async fn new(namesrv: &str, prefix: &str) -> Result<Fixture, String> {
        let setup = MQClientInstance::new(&format!("{prefix}-setup"), vec![namesrv.to_string()]);
        setup
            .start()
            .await
            .map_err(|e| format!("setup instance start failed: {e}"))?;
        let route = setup
            .get_topic_route_data(MixAll::DEFAULT_TOPIC)
            .await
            .ok_or_else(|| format!("no route of {} from namesrv", MixAll::DEFAULT_TOPIC))?;
        let broker = route
            .get_broker_datas()
            .first()
            .ok_or_else(|| "default topic route has no brokerData".to_string())?;
        let broker_name = broker.broker_name.clone();
        let broker_addr = broker
            .select_broker_addr()
            .ok_or_else(|| format!("broker {broker_name} has no address"))?;
        let producer = DefaultMQProducer::new(&format!("{prefix}_pg"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        producer
            .start()
            .await
            .map_err(|e| format!("producer start failed: {e}"))?;
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            prefix: prefix.to_string(),
            setup,
            producer,
            broker_name,
            broker_addr,
        })
    }

    fn topic(&self, kind: &str) -> String {
        format!("{}{kind}", self.prefix)
    }

    fn group(&self, kind: &str) -> String {
        format!("{}-{}", self.prefix, kind)
    }

    /// Create the topic on the broker, then wait until the **name server** publishes its route.
    ///
    /// A consumer gets no route for an uncreated topic (TBW102 is a producer-side fallback only), and
    /// `CREATE_TOPIC_IN_BROKER` reaches the name server on the broker's own registration round, so a
    /// send/consume that starts immediately can legitimately see nothing. Waiting here keeps that
    /// fixture latency out of the delivery assertions below.
    async fn create_topic(&self, topic: &str, queues: i32) -> Result<(), String> {
        self.setup
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
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(route) = self.setup.get_topic_route_data(topic).await {
                let read_queues: i32 = route.queue_datas.iter().map(|q| q.read_queue_nums).sum();
                if read_queues >= queues {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "route of {topic} not visible on the name server in 60s"
                ));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn seed(&self, topic: &str) -> usize {
        let mut ok = 0;
        for i in 0..N_MSG {
            let body = format!("cap-{i:02}");
            let mut msg = Message::new(topic, Some(body.as_bytes()));
            match self.producer.send(&mut msg, Some(5000), None).await {
                Ok(r) if r.status == SendStatus::SendOk => ok += 1,
                Ok(r) => println!("  [WARN] send {body} status={:?}", r.status),
                Err(e) => println!("  [WARN] send {body} failed: {e}"),
            }
        }
        ok
    }
}

// ------------------------------------------------------------------ L1 push legs

#[derive(Default)]
struct Recording {
    bodies: Mutex<Vec<String>>,
}

impl Recording {
    fn count(&self) -> usize {
        lock(&self.bodies).len()
    }

    fn distinct(&self) -> BTreeSet<String> {
        lock(&self.bodies).iter().cloned().collect()
    }
}

struct CountingListener {
    rec: Arc<Recording>,
}

impl MessageListenerConcurrently for CountingListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let mut bodies = lock(&self.rec.bodies);
        for m in msgs {
            bodies.push(String::from_utf8_lossy(m.get_body()).into_owned());
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

struct PushOutcome {
    got: usize,
    /// Queue keys this consumer owned during the window (the business topic only).
    assigned: Vec<String>,
}

/// Start one push consumer at `where_` and record what arrives for the observation window.
///
/// Every leg gets a **fresh group**: a group that has already committed offsets would read them back
/// regardless of `consumeFromWhere`, which is exactly the false green this scenario must avoid.
///
/// The leg also waits for a real queue assignment first: without one the consumer holds no queue,
/// nothing can arrive, and a rebalance bug would be indistinguishable from a `consumeFromWhere` bug --
/// decisive here, because four of the five legs expect **zero** messages.
async fn push_leg(
    ck: &mut Checker,
    fx: &Fixture,
    topic: &str,
    group: &str,
    where_: &str,
    window_secs: u64,
) -> PushOutcome {
    let cfg = ConsumerConfig {
        consumer_group: group.to_string(),
        name_server_addrs: vec![fx.namesrv.clone()],
        instance_name: format!("cap-closeout-{group}"),
        consume_from_where: where_.to_string(),
        consume_thread_min: 1,
        consume_thread_max: 1,
        ..Default::default()
    };
    let c = match DefaultMQPushConsumer::with_config(cfg) {
        Ok(c) => c,
        Err(e) => {
            ck.abort(&format!("start a consumer at {where_}"), &e.to_string());
            return PushOutcome {
                got: 0,
                assigned: Vec::new(),
            };
        }
    };
    if let Err(e) = c.subscribe(topic, "*") {
        ck.abort(&format!("subscribe at {where_}"), &e.to_string());
        return PushOutcome {
            got: 0,
            assigned: Vec::new(),
        };
    }
    let rec = Arc::new(Recording::default());
    c.set_message_listener_concurrently(Arc::new(CountingListener { rec: rec.clone() }));
    if let Err(e) = c.start().await {
        ck.abort(&format!("start a consumer at {where_}"), &e.to_string());
        return PushOutcome {
            got: 0,
            assigned: Vec::new(),
        };
    }
    let assign_deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < assign_deadline {
        if !c.assigned_queue_keys().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let assigned: Vec<String> = c
        .assigned_queue_keys()
        .into_iter()
        .filter(|k| k.contains(topic))
        .collect();
    let deadline = Instant::now() + Duration::from_secs(window_secs);
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    c.shutdown();
    let got = rec.count();
    println!(
        "  INFO {where_}: assigned={} received={got} distinct={}",
        assigned.len(),
        rec.distinct().len()
    );
    PushOutcome { got, assigned }
}

// ------------------------------------------------------------------ L2 lite legs

struct LiteOutcome {
    got: usize,
    pin: bool,
    default_broker_id: i64,
}

/// One LitePull subscribe-mode leg with the given pin, starting at FIRST_OFFSET so the whole backlog
/// is reachable in every leg and the pin is the only variable.
///
/// `pin` / `default_broker_id` are read back **after** `start()`: the pull round reads the very same
/// two fields, once per request, right before it calls `pull_message` -- so these values are what the
/// request carries, not a re-derivation of the intent. (Pin off means the `broker_id` argument is
/// `None`; there is no suggestion table on this path to fall back to.)
async fn lite_leg(
    ck: &mut Checker,
    fx: &Fixture,
    topic: &str,
    group: &str,
    pin: bool,
    default_broker_id: i64,
    want: usize,
    window_secs: u64,
) -> LiteOutcome {
    let cfg = LitePullConsumerConfig {
        consumer_group: group.to_string(),
        name_server_addrs: vec![fx.namesrv.clone()],
        instance_name: format!("cap-closeout-{group}"),
        poll_timeout_millis: 1000,
        pull_batch_size: 32,
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..Default::default()
    };
    let c = match DefaultLitePullConsumer::with_config(cfg) {
        Ok(c) => c,
        Err(e) => {
            ck.abort(&format!("build the lite leg {group}"), &e.to_string());
            return LiteOutcome {
                got: 0,
                pin: false,
                default_broker_id: -1,
            };
        }
    };
    if let Err(e) = c.subscribe(topic, "*") {
        ck.abort(&format!("lite subscribe {group}"), &e.to_string());
        return LiteOutcome {
            got: 0,
            pin: false,
            default_broker_id: -1,
        };
    }
    c.set_connect_broker_by_user(pin);
    c.set_default_broker_id(default_broker_id);
    if let Err(e) = c.start().await {
        ck.abort(&format!("lite start {group}"), &e.to_string());
        return LiteOutcome {
            got: 0,
            pin: false,
            default_broker_id: -1,
        };
    }
    let observed = LiteOutcome {
        got: 0,
        pin: c.is_connect_broker_by_user(),
        default_broker_id: c.get_default_broker_id(),
    };
    let mut got = 0usize;
    let deadline = Instant::now() + Duration::from_secs(window_secs);
    while got < want && Instant::now() < deadline {
        match c.poll(Some(1000)).await {
            Ok(batch) => got += batch.len(),
            Err(e) => {
                ck.abort(&format!("poll in the lite leg {group}"), &e.to_string());
                break;
            }
        }
    }
    c.shutdown();
    LiteOutcome { got, ..observed }
}

// ------------------------------------------------------------------ driver

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let prefix = format!("CapLive{}", stamp());
    let fx = match Fixture::new(namesrv, &prefix).await {
        Ok(fx) => fx,
        Err(e) => {
            ck.abort("fixture", &e);
            return ck;
        }
    };
    println!("== capability closeout live (rust): ns={namesrv} prefix={prefix} ==");
    println!("  INFO broker={} addr={}", fx.broker_name, fx.broker_addr);

    // ---------------------------------------------------------------- L1
    println!(
        "\nL1 ConsumeFromWhere: LAST-family values are accepted live (folding is pinned offline)"
    );
    let t1 = fx.topic("FromWhere");
    if let Err(e) = fx.create_topic(&t1, 1).await {
        ck.abort("L1 create topic", &e);
        return ck;
    }
    let sent = fx.seed(&t1).await;
    if sent != N_MSG {
        ck.abort(
            "L1 seed the backlog",
            &format!("sent={sent} expect={N_MSG}"),
        );
        return ck;
    }
    println!("  sent {N_MSG} messages, no consumer has ever joined this topic");

    let n_first = push_leg(
        &mut ck,
        &fx,
        &t1,
        &fx.group("gFirst"),
        ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET,
        PUSH_WINDOW_SECS,
    )
    .await;
    ck.check(
        "L1-positive control: FIRST_OFFSET delivers the whole backlog",
        !n_first.assigned.is_empty() && n_first.got == N_MSG,
        &format!(
            "received={} expect={N_MSG} assigned={:?}",
            n_first.got, n_first.assigned
        ),
    );

    // The LAST-family legs are an acceptance test plus a parity record, NOT an oracle: the broker's
    // answer to "where does this never-committed group start" flips between legs (see the module doc),
    // which is exactly why the Java reference cannot hold an equality across the four values either.
    // What the folding means is pinned offline by `plan_initial_offset`; here each value only has to be
    // accepted by a live consumer that then consumes a sane count.
    let mut last_family: Vec<(&'static str, PushOutcome)> = Vec::new();
    for (label, where_) in [
        ("LAST_OFFSET", ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET),
        ("MIN_OFFSET", ConsumeFromWhere::CONSUME_FROM_MIN_OFFSET),
        ("MAX_OFFSET", ConsumeFromWhere::CONSUME_FROM_MAX_OFFSET),
        (
            "LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST",
            ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
        ),
    ] {
        let n = push_leg(
            &mut ck,
            &fx,
            &t1,
            &fx.group(&format!("g{label}")),
            where_,
            PUSH_WINDOW_SECS,
        )
        .await;
        last_family.push((label, n));
    }

    println!(
        "  INFO LAST_OFFSET baseline on this end: received={} of {N_MSG} \
         (0 = tail start, {N_MSG} = whole backlog replayed)",
        last_family[0].1.got
    );
    println!(
        "  INFO delivery counts on this backlog -- parity record, NOT an oracle (the broker's \
         V3_0_6 answer for a never-committed group is timing-dependent): FIRST_OFFSET={} {}",
        n_first.got,
        last_family
            .iter()
            .map(|(label, n)| format!("{label}={}", n.got))
            .collect::<Vec<_>>()
            .join(" ")
    );

    for (label, n) in &last_family {
        ck.check(
            &format!(
                "L1-{label} is accepted: consumer started, got a queue, consumed a sane count"
            ),
            !n.assigned.is_empty() && n.got <= N_MSG,
            &format!(
                "received={} (sane range 0..={N_MSG}) assigned={:?}",
                n.got, n.assigned
            ),
        );
    }

    // ---------------------------------------------------------------- L2
    println!("\nL2 LitePull pin (connectBrokerByUser / defaultBrokerId)");
    let t2 = fx.topic("Pin");
    if let Err(e) = fx.create_topic(&t2, 1).await {
        ck.abort("L2 create topic", &e);
        return ck;
    }
    let sent = fx.seed(&t2).await;
    if sent != N_MSG {
        ck.abort(
            "L2 seed the backlog",
            &format!("sent={sent} expect={N_MSG}"),
        );
        return ck;
    }
    println!("  sent {N_MSG} messages to {t2}");
    // The premise of the third leg, printed so "pin to a missing slave still delivers" can never be
    // read as "reads the slave": this cluster really has no slave for that broker.
    if let Some(route) = fx.setup.get_topic_route_data(&t2).await {
        if let Some(bd) = route
            .get_broker_datas()
            .iter()
            .find(|b| b.broker_name == fx.broker_name)
        {
            println!(
                "  INFO broker {} ids present in the cluster: {:?}",
                fx.broker_name,
                bd.broker_addrs
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>()
            );
        }
    }

    let off = lite_leg(&mut ck, &fx, &t2, &fx.group("pinOff"), false, 0, N_MSG, 20).await;
    ck.check(
        "L2-pin off: the request carries no broker id (route resolution as before)",
        !off.pin,
        &format!("connectBrokerByUser={}", off.pin),
    );
    ck.check(
        "L2-pin off: delivers the backlog",
        off.got == N_MSG,
        &format!("got={} expect={N_MSG}", off.got),
    );

    let master = lite_leg(
        &mut ck,
        &fx,
        &t2,
        &fx.group("pinMaster"),
        true,
        0,
        N_MSG,
        20,
    )
    .await;
    ck.check(
        "L2-pin to the master (0): the request carries brokerId=0",
        master.pin && master.default_broker_id == 0,
        &format!(
            "connectBrokerByUser={} defaultBrokerId={}",
            master.pin, master.default_broker_id
        ),
    );
    ck.check(
        "L2-pin to the master (0): delivers the backlog",
        master.got == N_MSG,
        &format!("got={} expect={N_MSG}", master.got),
    );

    let slave = lite_leg(&mut ck, &fx, &t2, &fx.group("pinSlave"), true, 1, N_MSG, 12).await;
    ck.check(
        "L2-pin to a non-existent slave (1): the request carries brokerId=1",
        slave.pin && slave.default_broker_id == 1,
        &format!(
            "connectBrokerByUser={} defaultBrokerId={}",
            slave.pin, slave.default_broker_id
        ),
    );
    ck.check(
        "L2-pin to a non-existent slave (1): the onlyThisBroker=false fallback still delivers \
         (Java parity, not a hole)",
        slave.got == N_MSG,
        &format!("got={} expect={N_MSG}", slave.got),
    );

    // ---------------------------------------------------------------- L3
    println!("\nL3 adminExtGroup (identity only, no wire effect)");
    let admin = DefaultMQAdminExt::new();
    admin.set_namesrv_addr(namesrv);
    ck.check(
        "L3-DEFAULT_ADMIN_EXT_GROUP matches the Java constant",
        DEFAULT_ADMIN_EXT_GROUP == "admin_ext_group",
        &format!("value={DEFAULT_ADMIN_EXT_GROUP:?}"),
    );
    ck.check(
        "L3-default adminExtGroup is 'admin_ext_group'",
        admin.admin_ext_group() == "admin_ext_group",
        &format!("value={:?}", admin.admin_ext_group()),
    );
    let custom = format!("cap_closeout_admin_{prefix}");
    admin.set_admin_ext_group(&custom);
    ck.check(
        "L3-override takes",
        admin.admin_ext_group() == custom,
        &format!("value={:?}", admin.admin_ext_group()),
    );
    if let Err(e) = admin.start().await {
        ck.abort("L3 custom-group admin start", &e.to_string());
        return ck;
    }
    let list = match admin.fetch_all_topic_list().await {
        Ok(list) => Some(list),
        Err(e) => {
            ck.abort(
                "L3 fetch_all_topic_list with a custom group",
                &e.to_string(),
            );
            None
        }
    };
    let (found, topic_count) = match list {
        Some(list) => (
            list.topic_list.iter().any(|t| t == &t2),
            list.topic_list.len(),
        ),
        None => (false, 0),
    };
    admin.shutdown();
    ck.check(
        "L3-a custom-group admin still boots and serves an RPC on the live cluster",
        found,
        &format!("topics={topic_count}"),
    );

    fx.producer.shutdown();
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
