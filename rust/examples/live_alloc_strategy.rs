//! ⚠ A2 depends on "the subscription already being registered on the broker before the first heartbeat": the lite heartbeat only
//! goes to brokers known to the instance's route table, so `start()` first pulls in the route of the subscribed topic
//! (see `refresh_route_for_heartbeat` in `pull_consumer.rs`). Without that step the same-group peers would wait a
//! full heartbeat period (5s) to see each other, and the first rebalance would hand each side all queues.
//!
//! Usage (start the local cluster first, per the runbook recorded in the project knowledge):
//! ```text
//! cargo run --example live_alloc_strategy -- 127.0.0.1:9876
//! ```

use std::collections::BTreeSet;
use std::env;
use std::process::ExitCode;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::allocate_strategy::{
    AllocateMachineRoomNearby, AllocateMessageQueueAveragely, AllocateMessageQueueAveragelyByCircle,
    AllocateMessageQueueByConfig, AllocateMessageQueueByMachineRoom,
    AllocateMessageQueueConsistentHash, AllocateMessageQueueStrategy, MachineRoomResolver,
};
use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultLitePullConsumer, LitePullConsumerConfig,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;

const QUEUE_NUMS: i32 = 4;
const N_MSG: usize = 12;
/// Max seconds to wait for allocation convergence. Each side needs one heartbeat round (5s) + one rebalance round (20s), with a full margin to spare.
const WAIT_SECONDS: u64 = 45;

// ------------------------------------------------------------------ skeleton

fn stamp() -> String {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("{ms}")
}

struct Checker {
    passed: usize,
    failed: Vec<String>,
}

impl Checker {
    fn new() -> Checker {
        Checker { passed: 0, failed: Vec::new() }
    }

    fn check(&mut self, name: &str, cond: bool, detail: &str) {
        if cond {
            self.passed += 1;
            println!("  [PASS] {name}{}", if detail.is_empty() { String::new() } else { format!("  {detail}") });
        } else {
            let label = format!("{name}{}", if detail.is_empty() { String::new() } else { format!("  {detail}") });
            self.failed.push(label.clone());
            println!("  [FAIL] {label}");
        }
    }

    fn abort(&mut self, name: &str, detail: &str) {
        self.failed.push(format!("{name} (prerequisite failed): {detail}"));
        println!("  [ABORT] {name}: {detail}");
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Set of queue keys (`brokerName#queueId`); allocation results are compared by it.
fn queue_keys(mqs: &[MessageQueue]) -> BTreeSet<String> {
    mqs.iter()
        .map(|mq| format!("{}#{}", mq.get_broker_name(), mq.get_queue_id()))
        .collect()
}

fn key_set_text(keys: &BTreeSet<String>) -> String {
    keys.iter().cloned().collect::<Vec<_>>().join(" ")
}

fn bodies(msgs: &[MessageExt]) -> BTreeSet<String> {
    msgs.iter()
        .map(|m| String::from_utf8_lossy(m.get_body()).into_owned())
        .collect()
}

/// Counts only the bodies of messages that came from `on_queues` (a set of `brokerName#queueId`).
fn bodies_on(msgs: &[MessageExt], on_queues: &BTreeSet<String>) -> BTreeSet<String> {
    msgs.iter()
        .filter(|m| {
            on_queues.contains(&format!("{}#{}", m.get_broker_name().unwrap_or_default(), m.queue_id))
        })
        .map(|m| String::from_utf8_lossy(m.get_body()).into_owned())
        .collect()
}

/// Drains the buffer by **time**: when a strategy owns only part of the queues the received count is unknown, so waiting by count is impossible.
///
/// A refused poll means this consumer is not pulling at all, so record it and stop draining -- the
/// strategy checks below then fail instead of reporting an empty hand as a legitimate split.
async fn drain_for(ck: &mut Checker, c: &DefaultLitePullConsumer, secs: u64) -> Vec<MessageExt> {
    let mut got = Vec::new();
    let deadline = now_ms() + (secs as i64) * 1000;
    while now_ms() < deadline {
        match c.poll(Some(1000)).await {
            Ok(batch) => got.extend(batch),
            Err(e) => {
                ck.abort("poll during the drain loop", &e.to_string());
                break;
            }
        }
    }
    got
}

fn sorted_queues(mut mqs: Vec<MessageQueue>) -> Vec<MessageQueue> {
    mqs.sort_by(|a, b| {
        a.broker_name
            .cmp(&b.broker_name)
            .then(a.queue_id.cmp(&b.queue_id))
    });
    mqs
}

/// Waits until both instances have **split up all** queues (allocation convergence needs one heartbeat + rebalance round on each side).
async fn wait_split_assignment(
    a: &DefaultLitePullConsumer,
    b: &DefaultLitePullConsumer,
    total: usize,
    secs: u64,
) -> (Vec<MessageQueue>, Vec<MessageQueue>) {
    let deadline = now_ms() + (secs as i64) * 1000;
    loop {
        let qa = a.assignment();
        let qb = b.assignment();
        let ka = queue_keys(&qa);
        let kb = queue_keys(&qb);
        let overlap: BTreeSet<String> = ka.intersection(&kb).cloned().collect();
        let union: BTreeSet<String> = ka.union(&kb).cloned().collect();
        if !ka.is_empty() && !kb.is_empty() && overlap.is_empty() && union.len() == total {
            return (qa, qb);
        }
        if now_ms() >= deadline {
            return (qa, qb);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Waits for the first rebalance to hand out queues (the lite `start()` does not rebalance synchronously; a background loop triggers it).
async fn wait_assignment(c: &DefaultLitePullConsumer, secs: u64) -> Vec<MessageQueue> {
    let deadline = now_ms() + (secs as i64) * 1000;
    loop {
        let a = c.assignment();
        if !a.is_empty() || now_ms() >= deadline {
            return a;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// ------------------------------------------------------------------ fixture

struct Fixture {
    namesrv: String,
    stamp: String,
    producer: DefaultMQProducer,
    admin: MQClientInstance,
    broker_name: String,
    broker_addr: String,
    topics: Mutex<Vec<String>>,
}

impl Fixture {
    fn new(namesrv: &str, stamp: &str) -> Result<Fixture, String> {
        let producer = DefaultMQProducer::new(&format!("rust-live-alloc-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        let admin = MQClientInstance::new(
            &format!("rust-live-alloc-admin-{stamp}"),
            vec![namesrv.to_string()],
        );
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            producer,
            admin,
            broker_name: String::new(),
            broker_addr: String::new(),
            topics: Mutex::new(Vec::new()),
        })
    }

    async fn start(&mut self) -> Result<(), String> {
        self.admin
            .start()
            .await
            .map_err(|e| format!("admin start failed: {e}"))?;
        self.producer
            .start()
            .await
            .map_err(|e| format!("producer start failed: {e}"))?;
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

    fn topic_name(&self) -> String {
        format!("RustLiveAlloc{}", self.stamp)
    }

    fn group_name(&self, kind: &str) -> String {
        format!("rust-live-alloc-{kind}-{}", self.stamp)
    }

    /// Creates the topic explicitly (not relying on `autoCreateTopicEnable`, so the queue count is deterministic).
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

    /// Builds a lite consumer from group name + instance name (not subscribed, not started).
    fn consumer(&self, group_kind: &str, instance: &str) -> Result<DefaultLitePullConsumer, String> {
        let cfg = LitePullConsumerConfig {
            consumer_group: self.group_name(group_kind),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("live-alloc-{instance}-{}", self.stamp),
            poll_timeout_millis: 1000,
            ..Default::default()
        };
        DefaultLitePullConsumer::with_config(cfg).map_err(|e| format!("{instance} build failed: {e}"))
    }

    /// Sends TagA/TagB alternately (even indices TagA), body = `<topic>-<i:02>`.
    async fn produce_alternating(&self, topic: &str) -> usize {
        let mut ok = 0;
        for i in 0..N_MSG {
            let body = format!("{topic}-{i:02}");
            let mut msg = Message::new(topic, Some(body.as_bytes()));
            msg.set_tags(if i % 2 == 0 { "TagA" } else { "TagB" });
            match self.producer.send(&mut msg, Some(5000), None).await {
                Ok(_) => ok += 1,
                Err(e) => println!("  [WARN] send {body} failed: {e}"),
            }
        }
        ok
    }

    async fn cleanup(&mut self, ck: &mut Checker) {
        let topics: Vec<String> = lock(&self.topics).clone();
        let mut failed = Vec::new();
        for topic in &topics {
            if let Err(e) = self
                .admin
                .delete_topic_in_broker(&self.broker_addr, topic, 5000)
                .await
            {
                failed.push(format!("{topic}:{e}"));
            }
        }
        ck.check(
            "A9 delete the topics created by this run",
            failed.is_empty(),
            &format!("{failed:?}"),
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

/// All queues of the business topic (via the admin instance's route, not depending on the consumers).
async fn route_queues(fx: &Fixture, topic: &str) -> Vec<MessageQueue> {
    fx.admin
        .get_topic_publish_info(topic, false)
        .await
        .map(|info| info.msg_queue_list())
        .unwrap_or_default()
}

fn expected_bodies(topic: &str) -> BTreeSet<String> {
    (0..N_MSG).map(|i| format!("{topic}-{i:02}")).collect()
}

// ------------------------------------------------------------------ shared skeleton

/// Builds two instances of the same group, each with a strategy installed, and subscribes them (not started).
fn pair_with_strategy(
    fx: &Fixture,
    kind: &str,
    strategies: &[std::sync::Arc<dyn AllocateMessageQueueStrategy>; 2],
    topic: &str,
) -> Result<(DefaultLitePullConsumer, DefaultLitePullConsumer), String> {
    let ca = fx.consumer(kind, &format!("{kind}-a"))?;
    let cb = fx.consumer(kind, &format!("{kind}-b"))?;
    for (c, strategy) in [(&ca, &strategies[0]), (&cb, &strategies[1])] {
        c.set_allocate_message_queue_strategy(strategy.clone());
        // The pre-existing messages were all sent during A0; a new group defaulting to LAST would skip them → nothing received.
        // What is verified here is "the strategy really drives send/receive", so consumption starts from the queue head.
        c.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        c.subscribe(topic, "*").map_err(|e| e.to_string())?;
    }
    Ok((ca, cb))
}

/// Runs the strategy offline with **real** inputs (mqAll from the route + the real clientIds of cid_members as cidAll),
/// and **waits for the live `assignment()` to converge to that prediction**.
///
/// Why measure convergence against the prediction instead of "first wait for no overlap/no gap, then compare the
/// prediction": the hash ring can easily give all 4 queues to one instance, and the other side's `assignment()` is
/// empty by design before the first rebalance -- that initial state also satisfies "no overlap no gap", so the comparison would really be a snapshot of "one side has not computed yet". (The first version tripped exactly here.)
async fn wait_until_prediction_converged(
    group: &str,
    all: &[MessageQueue],
    cid_members: &[&DefaultLitePullConsumer],
    asserted: &[&DefaultLitePullConsumer],
    strategies: &[&dyn AllocateMessageQueueStrategy],
    secs: u64,
) -> (bool, String) {
    let mut cid_all: Vec<String> = cid_members.iter().map(|c| c.client_id()).collect();
    cid_all.sort();
    let mut expected: Vec<BTreeSet<String>> = Vec::with_capacity(asserted.len());
    for (c, strategy) in asserted.iter().zip(strategies) {
        match strategy.allocate(group, &c.client_id(), all, &cid_all) {
            Ok(v) => expected.push(queue_keys(&v)),
            Err(e) => return (false, format!("offline prediction failed: {e}")),
        }
    }
    let deadline = now_ms() + (secs as i64) * 1000;
    loop {
        let live: Vec<BTreeSet<String>> = asserted.iter().map(|c| queue_keys(&c.assignment())).collect();
        let matched = live.len() == expected.len()
            && live.iter().zip(expected.iter()).all(|(l, e)| l == e);
        if matched || now_ms() >= deadline {
            let mut parts: Vec<String> = Vec::new();
            for (c, (l, e)) in asserted
                .iter()
                .zip(live.iter().zip(expected.iter()))
            {
                parts.push(format!(
                    "cid={} live=[{}] predict=[{}]",
                    c.client_id(),
                    key_set_text(l),
                    key_set_text(e)
                ));
            }
            parts.push(format!("cidAll={cid_all:?}"));
            return (matched, parts.join(" | "));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Where NEARBY lands: the real cluster has a single broker, so queues and clients are all recorded in the same
/// machine room; NEARBY then always takes the "own machine room" branch, equivalent to the inner strategy.
///
/// It also keeps a call log -- proving that rebalance really asked the machine room of each queue and each client
/// in turn, rather than the strategy object being renamed without participating in the allocation.
const ROOM: &str = "room1";

struct OneRoom {
    broker_calls: Mutex<Vec<String>>,
    consumer_calls: Mutex<Vec<String>>,
}

impl OneRoom {
    fn new() -> OneRoom {
        OneRoom {
            broker_calls: Mutex::new(Vec::new()),
            consumer_calls: Mutex::new(Vec::new()),
        }
    }
}

impl MachineRoomResolver for OneRoom {
    fn broker_deploy_in(&self, message_queue: &MessageQueue) -> String {
        lock(&self.broker_calls).push(message_queue.get_broker_name().to_string());
        ROOM.to_string()
    }

    fn consumer_deploy_in(&self, client_id: &str) -> String {
        lock(&self.consumer_calls).push(client_id.to_string());
        ROOM.to_string()
    }
}

// ------------------------------------------------------------------ A1 config surface

async fn a1_default_strategy(ck: &mut Checker, fx: &Fixture) {
    let c = match fx.consumer("probe", "probe") {
        Ok(c) => c,
        Err(e) => {
            ck.abort("A1 build", &e);
            return;
        }
    };
    ck.check(
        "A1 default strategy name = AVG (field initial value)",
        c.allocate_message_queue_strategy().get_name() == "AVG",
        c.allocate_message_queue_strategy().get_name(),
    );
    let circle: std::sync::Arc<dyn AllocateMessageQueueStrategy> =
        std::sync::Arc::new(AllocateMessageQueueAveragelyByCircle);
    c.set_allocate_message_queue_strategy(circle);
    ck.check(
        "A1 the strategy swapped in by the setter reads back AVG_BY_CIRCLE",
        c.allocate_message_queue_strategy().get_name() == "AVG_BY_CIRCLE",
        c.allocate_message_queue_strategy().get_name(),
    );
}

// ------------------------------------------------------------------ A2 same-group circle split

async fn a2_circle_split(ck: &mut Checker, fx: &Fixture, topic: &str, all: &[MessageQueue]) {
    let (ca, cb) = match (
        fx.consumer("circle", "circle-a"),
        fx.consumer("circle", "circle-b"),
    ) {
        (Ok(a), Ok(b)) => (a, b),
        (a, b) => {
            let e = a.err().or_else(|| b.err());
            ck.abort("A2 build", &e.unwrap_or_else(|| "unknown".to_string()));
            return;
        }
    };
    let circle: std::sync::Arc<dyn AllocateMessageQueueStrategy> =
        std::sync::Arc::new(AllocateMessageQueueAveragelyByCircle);
    for c in [&ca, &cb] {
        c.set_allocate_message_queue_strategy(circle.clone());
        // The pre-existing messages were all sent during A0; a new group defaulting to LAST would skip them → nothing received.
        // What is verified here is "the circle split really drives send/receive", so consumption starts from the queue head.
        c.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        if let Err(e) = c.subscribe(topic, "*") {
            ck.abort("A2 subscribe", &e.to_string());
            return;
        }
    }
    if let Err(e) = ca.start().await {
        ck.abort("A2 ca start", &e.to_string());
        return;
    }
    if let Err(e) = cb.start().await {
        ca.shutdown();
        ck.abort("A2 cb start", &e.to_string());
        return;
    }
    let (qa, qb) = wait_split_assignment(&ca, &cb, all.len(), WAIT_SECONDS).await;
    let ka = queue_keys(&qa);
    let kb = queue_keys(&qb);
    let overlap: BTreeSet<String> = ka.intersection(&kb).cloned().collect();
    let union: BTreeSet<String> = ka.union(&kb).cloned().collect();
    ck.check(
        "A2 the two instances' allocations do not overlap (the broker sees two clientIds)",
        overlap.is_empty(),
        &format!("overlap=[{}]", key_set_text(&overlap)),
    );
    ck.check(
        &format!("A2 union covers all {} queues", all.len()),
        union == queue_keys(all),
        &format!("a={} b={}", ka.len(), kb.len()),
    );
    // Signature of circle allocation: what each side gets is the "index modulo" alternating
    // queues, not contiguous segments (4 queues / 2 instances → 2 each with index stride 2; AVG would give two contiguous segments).
    let pos_a: Vec<usize> = all
        .iter()
        .enumerate()
        .filter(|(_, mq)| ka.contains(&format!("{}#{}", mq.broker_name, mq.queue_id)))
        .map(|(i, _)| i)
        .collect();
    let circle_shape = pos_a.len() == 2
        && pos_a
            .windows(2)
            .all(|w| (w[1] - w[0]) % 2 == 0);
    ck.check(
        "A2 allocation shape is alternating (stride 2), not AVG's contiguous segments",
        all.len() != 4 || circle_shape,
        &format!("posA={pos_a:?}"),
    );
    // After the circle split each side receives its own half; together they cover all 12 messages.
    let ga = drain_for(ck, &ca, 8).await;
    let gb = drain_for(ck, &cb, 8).await;
    let ba = bodies(&ga);
    let bb = bodies(&gb);
    let joined: BTreeSet<String> = ba.union(&bb).cloned().collect();
    let shared: BTreeSet<String> = ba.intersection(&bb).cloned().collect();
    ck.check(
        &format!("A2 the two instances together receive all {N_MSG} messages"),
        joined == expected_bodies(topic) && !ba.is_empty() && !bb.is_empty(),
        &format!(
            "a={} b={} union={} overlap grabbed before queue revocation={}",
            ba.len(),
            bb.len(),
            joined.len(),
            shared.len()
        ),
    );
    ca.shutdown();
    cb.shutdown();
}

// ------------------------------------------------------------------ A3 CONFIG-driven send/receive

async fn a3_config_halves(ck: &mut Checker, fx: &Fixture, topic: &str, all: &[MessageQueue]) {
    let half = all.len() / 2;
    let (half_a, half_b) = (all[..half].to_vec(), all[half..].to_vec());
    let (ca, cb) = match (
        fx.consumer("config-a", "config-a"),
        fx.consumer("config-b", "config-b"),
    ) {
        (Ok(a), Ok(b)) => (a, b),
        (a, b) => {
            let e = a.err().or_else(|| b.err());
            ck.abort("A3 build", &e.unwrap_or_else(|| "unknown".to_string()));
            return;
        }
    };
    ca.set_allocate_message_queue_strategy(std::sync::Arc::new(AllocateMessageQueueByConfig::new(half_a.clone())));
    cb.set_allocate_message_queue_strategy(std::sync::Arc::new(AllocateMessageQueueByConfig::new(half_b.clone())));
    for c in [&ca, &cb] {
        c.set_consume_from_where(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
        if let Err(e) = c.subscribe(topic, "*") {
            ck.abort("A3 subscribe", &e.to_string());
            return;
        }
    }
    if let Err(e) = ca.start().await {
        ck.abort("A3 ca start", &e.to_string());
        return;
    }
    if let Err(e) = cb.start().await {
        ca.shutdown();
        ck.abort("A3 cb start", &e.to_string());
        return;
    }
    let a_assigned = wait_assignment(&ca, WAIT_SECONDS).await;
    let b_assigned = wait_assignment(&cb, WAIT_SECONDS).await;
    ck.check(
        "A3 CONFIG hands out only the configured queues (ignoring mqAll/cidAll)",
        queue_keys(&a_assigned) == queue_keys(&half_a)
            && queue_keys(&b_assigned) == queue_keys(&half_b),
        &format!(
            "a=[{}] b=[{}]",
            key_set_text(&queue_keys(&a_assigned)),
            key_set_text(&queue_keys(&b_assigned))
        ),
    );
    let ga = drain_for(ck, &ca, 8).await;
    let gb = drain_for(ck, &cb, 8).await;
    let keys_a = queue_keys(&half_a);
    let keys_b = queue_keys(&half_b);
    let all_a = bodies(&ga);
    let all_b = bodies(&gb);
    let on_a = bodies_on(&ga, &keys_a);
    let on_b = bodies_on(&gb, &keys_b);
    ck.check(
        "A3 CONFIG consumer receives only messages from its configured queues",
        all_a == on_a && all_b == on_b,
        &format!(
            "a={} aOnCfg={} b={} bOnCfg={}",
            all_a.len(),
            on_a.len(),
            all_b.len(),
            on_b.len()
        ),
    );
    let union: BTreeSet<String> = all_a.union(&all_b).cloned().collect();
    let inter: BTreeSet<String> = all_a.intersection(&all_b).cloned().collect();
    ck.check(
        &format!("A3 the two halves together cover exactly all {N_MSG} messages with no overlap"),
        union == expected_bodies(topic) && inter.is_empty(),
        &format!("union={} inter={}", union.len(), inter.len()),
    );
    ca.shutdown();
    cb.shutdown();
}

// ------------------------------------------------------------------ A4 consistent hash ring

async fn a4_consistent_hash(ck: &mut Checker, fx: &Fixture, topic: &str, all: &[MessageQueue]) {
    let ch = std::sync::Arc::new(AllocateMessageQueueConsistentHash::new());
    let strategy: std::sync::Arc<dyn AllocateMessageQueueStrategy> = ch.clone();
    let (ca, cb) = match pair_with_strategy(fx, "chash", &[strategy.clone(), strategy.clone()], topic)
    {
        Ok(p) => p,
        Err(e) => {
            ck.abort("A4 build", &e);
            return;
        }
    };
    let got = ca.allocate_message_queue_strategy();
    ck.check(
        "A4 strategy name after replacement = CONSISTENT_HASH",
        got.get_name() == "CONSISTENT_HASH",
        got.get_name(),
    );
    if let Err(e) = ca.start().await {
        ck.abort("A4 ca start", &e.to_string());
        return;
    }
    if let Err(e) = cb.start().await {
        ca.shutdown();
        ck.abort("A4 cb start", &e.to_string());
        return;
    }
    // Convergence condition: "live allocation == offline prediction under the real inputs" (see the helper's comment).
    let (matched, detail) = wait_until_prediction_converged(
        &fx.group_name("chash"),
        all,
        &[&ca, &cb],
        &[&ca, &cb],
        &[strategy.as_ref(), strategy.as_ref()],
        WAIT_SECONDS,
    )
    .await;
    ck.check("A4 live allocation converges to the consistent-hash ring's offline prediction", matched, &detail);
    let qa = ca.assignment();
    let qb = cb.assignment();
    let ka = queue_keys(&qa);
    let kb = queue_keys(&qb);
    let overlap: BTreeSet<String> = ka.intersection(&kb).cloned().collect();
    let union: BTreeSet<String> = ka.union(&kb).cloned().collect();
    ck.check(
        "A4 the two instances' allocations do not overlap",
        overlap.is_empty(),
        &format!("overlap=[{}]", key_set_text(&overlap)),
    );
    ck.check(
        &format!("A4 union covers all {} queues", all.len()),
        union == queue_keys(all),
        &format!("a={} b={}", ka.len(), kb.len()),
    );
    // End to end: no matter how the ring skews, the two sides together must receive all 12 messages.
    let ga = drain_for(ck, &ca, 8).await;
    let gb = drain_for(ck, &cb, 8).await;
    let joined: BTreeSet<String> = bodies(&ga).union(&bodies(&gb)).cloned().collect();
    ck.check(
        &format!("A4 the two instances together receive all {N_MSG} messages (the ring really drives send/receive)"),
        joined == expected_bodies(topic),
        &format!("union={}", joined.len()),
    );
    ca.shutdown();
    cb.shutdown();
}

// ------------------------------------------------------------------ A5 NEARBY wrapping the ring

async fn a5_nearby(ck: &mut Checker, fx: &Fixture, topic: &str, all: &[MessageQueue]) {
    let inner: std::sync::Arc<dyn AllocateMessageQueueStrategy> =
        std::sync::Arc::new(AllocateMessageQueueConsistentHash::new());
    let resolver = std::sync::Arc::new(OneRoom::new());
    let nearby: std::sync::Arc<dyn AllocateMessageQueueStrategy> =
        std::sync::Arc::new(AllocateMachineRoomNearby::new(inner.clone(), resolver.clone()));
    let (ca, cb) = match pair_with_strategy(fx, "nearby", &[nearby.clone(), nearby.clone()], topic) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("A5 build", &e);
            return;
        }
    };
    let got = ca.allocate_message_queue_strategy();
    ck.check(
        "A5 strategy name after decoration = MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
        got.get_name() == "MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
        got.get_name(),
    );
    if let Err(e) = ca.start().await {
        ck.abort("A5 ca start", &e.to_string());
        return;
    }
    if let Err(e) = cb.start().await {
        ca.shutdown();
        ck.abort("A5 cb start", &e.to_string());
        return;
    }
    // Single machine room ⇒ NEARBY's result matches the inner ring **byte for byte**, so the inner strategy's offline prediction serves directly as the convergence condition:
    // convergence means "the decorator passes the inner split through unchanged and it really drives the rebalance".
    let (matched, detail) = wait_until_prediction_converged(
        &fx.group_name("nearby"),
        all,
        &[&ca, &cb],
        &[&ca, &cb],
        &[inner.as_ref(), inner.as_ref()],
        WAIT_SECONDS,
    )
    .await;
    ck.check("A5 NEARBY's live allocation == the inner ring's offline prediction", matched, &detail);
    let ka = queue_keys(&ca.assignment());
    let kb = queue_keys(&cb.assignment());
    let overlap: BTreeSet<String> = ka.intersection(&kb).cloned().collect();
    let union: BTreeSet<String> = ka.union(&kb).cloned().collect();
    ck.check(
        "A5 NEARBY: the two instances' allocations do not overlap and miss nothing",
        overlap.is_empty() && union == queue_keys(all),
        &format!("a={} b={} overlap={}", ka.len(), kb.len(), overlap.len()),
    );
    // The resolver was really called by the rebalance, and it saw real brokerNames plus the two real clientIds.
    let broker_calls = lock(&resolver.broker_calls).clone();
    let consumer_calls = lock(&resolver.consumer_calls).clone();
    let seen: BTreeSet<String> = broker_calls.iter().cloned().collect();
    let real_brokers: BTreeSet<String> = all.iter().map(|m| m.broker_name.clone()).collect();
    ck.check(
        &format!("A5 resolver was asked for the machine room per queue ({} calls)", broker_calls.len()),
        !broker_calls.is_empty() && seen == real_brokers,
        &format!("brokers={seen:?}"),
    );
    let both_cids = consumer_calls.contains(&ca.client_id())
        && consumer_calls.contains(&cb.client_id());
    ck.check(
        "A5 resolver was asked about both real clientIds",
        both_cids,
        &format!("calls={consumer_calls:?}"),
    );
    ca.shutdown();
    cb.shutdown();
}

// ------------------------------------------------------------------ A6 machine room mismatch

async fn a6_machine_room_starved(ck: &mut Checker, fx: &Fixture, topic: &str, all: &[MessageQueue]) {
    let room_strategy = AllocateMessageQueueByMachineRoom::new([ROOM]);
    ck.check(
        "A6 strategy name = MACHINE_ROOM and the whitelist reads back",
        room_strategy.get_name() == "MACHINE_ROOM" && room_strategy.get_consumeridcs().contains(ROOM),
        format!("idcs={:?}", room_strategy.get_consumeridcs()).as_str(),
    );
    let room: std::sync::Arc<dyn AllocateMessageQueueStrategy> = std::sync::Arc::new(room_strategy);
    let avg: std::sync::Arc<dyn AllocateMessageQueueStrategy> =
        std::sync::Arc::new(AllocateMessageQueueAveragely);
    let (ca, cb) = match pair_with_strategy(fx, "room", &[room.clone(), avg.clone()], topic) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("A6 build", &e);
            return;
        }
    };
    if let Err(e) = ca.start().await {
        ck.abort("A6 ca start", &e.to_string());
        return;
    }
    if let Err(e) = cb.start().await {
        ca.shutdown();
        ck.abort("A6 cb start", &e.to_string());
        return;
    }
    let ctrl = wait_assignment(&cb, WAIT_SECONDS).await;
    ck.check(
        "A6 the same-group control (AVG) gets queues as normal",
        !ctrl.is_empty(),
        &format!("ctrl=[{}]", key_set_text(&queue_keys(&ctrl))),
    );
    let starved = ca.assignment();
    ck.check(
        "A6 machine room not matching real brokerName → zero queues allocated (no error, no wrongful consumption)",
        starved.is_empty(),
        &format!("assignment=[{}]", key_set_text(&queue_keys(&starved))),
    );
    let (matched, detail) = wait_until_prediction_converged(
        &fx.group_name("room"),
        all,
        &[&ca, &cb],
        &[&ca, &cb],
        &[room.as_ref(), avg.as_ref()],
        WAIT_SECONDS,
    )
    .await;
    ck.check("A6 both sides' live allocations each converge to their own strategy's offline prediction", matched, &detail);
    let ga = drain_for(ck, &ca, 5).await;
    ck.check(
        "A6 the starved side polls no messages and raises no error",
        bodies(&ga).is_empty(),
        &format!("got={}", ga.len()),
    );
    ca.shutdown();
    cb.shutdown();
}

// ------------------------------------------------------------------ driver

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let mut fx = match Fixture::new(namesrv, &stamp) {
        Ok(fx) => fx,
        Err(e) => {
            ck.abort("fixture build", &e);
            return ck;
        }
    };
    if let Err(e) = fx.start().await {
        ck.abort("fixture start", &e);
        return ck;
    }
    println!("== live allocation-strategy check, namesrv={namesrv} stamp={stamp} ==");

    a1_default_strategy(&mut ck, &fx).await;

    let topic = fx.topic_name();
    if let Err(e) = fx.create_topic(&topic, QUEUE_NUMS).await {
        ck.abort("create topic", &e);
        return ck;
    }
    let all = sorted_queues(route_queues(&fx, &topic).await);
    if all.len() != QUEUE_NUMS as usize {
        ck.abort("route has fewer queues than expected", &format!("got {}", all.len()));
        fx.cleanup(&mut ck).await;
        return ck;
    }
    let sent = fx.produce_alternating(&topic).await;
    ck.check(
        &format!("A0 produced all {N_MSG} messages successfully"),
        sent == N_MSG,
        &format!("sentOk={sent}"),
    );

    a2_circle_split(&mut ck, &fx, &topic, &all).await;
    a3_config_halves(&mut ck, &fx, &topic, &all).await;
    a4_consistent_hash(&mut ck, &fx, &topic, &all).await;
    a5_nearby(&mut ck, &fx, &topic, &all).await;
    a6_machine_room_starved(&mut ck, &fx, &topic, &all).await;
    fx.cleanup(&mut ck).await;
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
