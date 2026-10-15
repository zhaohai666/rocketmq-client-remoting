//! Integration verification of [`DefaultMQPullConsumer`] against a **real 5.5.1 broker**.
//!
//!
//! The core of pull mode is "the caller pulls by itself and manages its own offsets", so every assertion circles that:
//! - P1 lifecycle: `start()` fails when no name server is configured; pulling before start errors;
//!   `start()` idempotent, `shutdown()` idempotent; pulling after `shutdown()` errors again.
//! - P2 `fetch_subscribe_message_queues` → the explicitly created 4-queue topic yields 4 queues, ids neither duplicated nor missing.
//! - P2b `fetch_message_queues_in_balance` → an instance that has **never pulled any queue** works out its share from
//!   "the real route + GET_CONSUMER_LIST_BY_GROUP(38)" (which requires `registerTopic` to have run, so the heartbeat carries the subscription set).
//! - P3 sends 12 messages to fixed queues (3 per queue) → per queue `max_offset - min_offset == 3`, total 12.
//! - P4 manual `pull`: per queue from min to max → all 12, no duplicates or gaps, bodies match the sent set,
//!   and `MessageExt.broker_name` is filled back in (`send_message_back` needs it to look the route up).
//! - P5 manual offset commit: `update_consume_offset` → `fetch_consume_offset` reads back consistently per queue.
//! - P6 offsets owned by the caller: pull again from the committed offset → NO_NEW_MSG; rewind to the queue head → FOUND
//!   (push mode cannot do this, which is exactly why pull mode exists); pull with TagB → NO_MATCHED_MSG.
//! - P7 long polling `pull_block_if_not_found`: start from the queue tail, another task sends after 1.5s → FOUND,
//!   and the measured round-trip is clearly above 1s and far below the 30s request timeout (proving the hang happens broker-side).
//! - P8 `search_offset` (now / 1) and `earliest_msg_store_time`.
//! - P9 `send_message_back` → the message shows up in `%RETRY%<group>` and is pullable.
//! - P10 the consumer's own `create_topic` admin API → the new topic's route is visible and its offsets queryable.
//! - P11 cleanup: delete the topics this run created.
//!
//! Usage (first start a local cluster per the runbook recorded in project knowledge):
//! ```text
//! cargo run --example live_pull_consumer -- 127.0.0.1:9876
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::process::ExitCode;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultMQPullConsumer, PullConsumerConfig,
};
use rocketmq_client_remoting::client::result::{PullResult, PullStatus};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;

/// Queue count of the test topic, paired with the send count (12 / 4 = 3 per queue).
const QUEUE_NUMS: i32 = 4;
const N_MSG: usize = 12;
const PER_QUEUE: usize = N_MSG / QUEUE_NUMS as usize;
/// Tag used for the fixed-queue sends; P6 uses another tag to verify broker-side filtering.
const TAG: &str = "TagA";

// ------------------------------------------------------------------ skeleton

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// Assertion accumulator: all scenarios run once before the summary; the first failure does not exit early.
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
}

/// Takes the inner value even when the lock is poisoned (one panic must not cascade through the whole run).
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn bodies(msgs: &[MessageExt]) -> BTreeSet<String> {
    msgs.iter()
        .map(|m| String::from_utf8_lossy(m.get_body()).into_owned())
        .collect()
}

/// Queue → that queue's max pulled queue_offset + 1 (i.e. the offset to pull next).
fn next_offsets(msgs: &[MessageExt]) -> BTreeMap<i32, i64> {
    let mut out: BTreeMap<i32, i64> = BTreeMap::new();
    for m in msgs {
        let next = out.entry(m.queue_id).or_insert(m.queue_offset + 1);
        *next = (*next).max(m.queue_offset + 1);
    }
    out
}

fn describe(result: &PullResult) -> String {
    format!(
        "status={:?} n={} next={} min={} max={}",
        result.status,
        result.msg_found_list.len(),
        result.next_begin_offset,
        result.min_offset,
        result.max_offset,
    )
}

// ------------------------------------------------------------------ fixture

struct Fixture {
    namesrv: String,
    stamp: String,
    producer: DefaultMQProducer,
    /// Admin instance used to create/delete topics.
    admin: MQClientInstance,
    broker_name: String,
    broker_addr: String,
    /// Topics created during this run, all deleted in P11.
    topics: Mutex<Vec<String>>,
}

impl Fixture {
    fn new(namesrv: &str, stamp: &str) -> Result<Fixture, String> {
        let producer = DefaultMQProducer::new(&format!("rust-live-pull-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        let admin = MQClientInstance::new(
            &format!("rust-live-pull-admin-{stamp}"),
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
        // Use the TBW102 route to find a broker that can create topics (same approach as live_consumer.rs).
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

    fn topic_name(&self, kind: &str) -> String {
        format!("RustLivePull{kind}{}", self.stamp)
    }

    fn group_name(&self, kind: &str) -> String {
        format!("rust-live-pull-{kind}-{}", self.stamp)
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

    /// Builds a consumer for the group (name server and instance name configured, not started).
    fn consumer(&self, kind: &str) -> Result<DefaultMQPullConsumer, String> {
        let group = self.group_name(kind);
        let cfg = PullConsumerConfig {
            consumer_group: group,
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("live-pull-{kind}-{}", self.stamp),
            ..Default::default()
        };
        DefaultMQPullConsumer::with_config(cfg).map_err(|e| format!("{kind} build failed: {e}"))
    }

    /// Returns a started consumer; on failure the error goes into the Checker.
    async fn started(&self, ck: &mut Checker, kind: &str) -> Option<DefaultMQPullConsumer> {
        let c = match self.consumer(kind) {
            Ok(c) => c,
            Err(e) => {
                ck.abort(&format!("{kind} build"), &e);
                return None;
            }
        };
        if let Err(e) = c.start().await {
            ck.abort(&format!("{kind} start"), &e.to_string());
            return None;
        }
        Some(c)
    }

    /// Sends to a fixed queue, body = `<topic>-<queueId>-<i>`.
    async fn produce_to(&self, topic: &str, queue_id: i32, n: usize) -> usize {
        let mut ok = 0;
        for i in 0..n {
            let body = format!("{topic}-{queue_id}-{i}");
            let mut msg = Message::new(topic, Some(body.as_bytes()));
            msg.set_tags(TAG);
            let mq = MessageQueue::new(topic, &self.broker_name, queue_id);
            match self.producer.send(&mut msg, Some(5000), Some(&mq)).await {
                Ok(_) => ok += 1,
                Err(e) => println!("  [WARN] send {body} failed: {e}"),
            }
        }
        ok
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

// ------------------------------------------------------------------ P1 lifecycle

async fn p1_lifecycle(ck: &mut Checker, fx: &Fixture) {
    // No name server configured (and no address-server env var either) → start must fail hard.
    let bare = match DefaultMQPullConsumer::with_config(PullConsumerConfig {
        consumer_group: fx.group_name("bare"),
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("P1 build", &e.to_string());
            return;
        }
    };
    match bare.start().await {
        Ok(()) => ck.check("P1 start fails when no name server is configured", false, "start unexpectedly succeeded"),
        Err(e) => ck.check(
            "P1 start fails when no name server is configured",
            e.to_string().contains("name server address is not set"),
            &e.to_string(),
        ),
    }
    ck.check("P1 no started state left over after the failure", !bare.is_started(), "");

    let c = match fx.consumer("life") {
        Ok(c) => c,
        Err(e) => {
            ck.abort("P1 build", &e);
            return;
        }
    };
    let mq = MessageQueue::new(MixAll::DEFAULT_TOPIC, &fx.broker_name, 0);
    match c.pull(&mq, "*", 0, 1, Some(100)).await {
        Ok(r) => ck.check(
            "P1 pulling before start errors",
            false,
            &format!("unexpectedly succeeded {}", describe(&r)),
        ),
        Err(e) => ck.check(
            "P1 pulling before start errors",
            e.to_string().contains("consumer not started"),
            &e.to_string(),
        ),
    }
    ck.check("P1 is_started is false before start", !c.is_started(), "");
    if let Err(e) = c.start().await {
        ck.abort("P1 start", &e.to_string());
        return;
    }
    ck.check("P1 is_started after start", c.is_started(), "");
    ck.check("P1 a repeated start is idempotent", c.start().await.is_ok(), "");
    let client_id = c.client_id();
    ck.check(
        "P1 start mints the client_id on the spot (localIP@instanceName, buildMQClientId semantics)",
        client_id
            .split_once('@')
            .is_some_and(|(ip, instance)| !ip.is_empty() && instance.starts_with("live-pull-life")),
        &client_id,
    );
    ck.check(
        "P1 the default topic route is fetchable after start",
        c.fetch_subscribe_message_queues(MixAll::DEFAULT_TOPIC).await.is_ok(),
        "",
    );
    c.shutdown();
    c.shutdown();
    ck.check("P1 shutdown is idempotent and does not panic", !c.is_started(), "");
    match c.pull(&mq, "*", 0, 1, Some(100)).await {
        Ok(r) => ck.check("P1 pulling after shutdown errors again", false, &describe(&r)),
        Err(e) => ck.check(
            "P1 pulling after shutdown errors again",
            e.to_string().contains("consumer not started"),
            &e.to_string(),
        ),
    }
}

// ------------------------------------------------- P2 queues / P3 per-queue stock

async fn p2_queues(ck: &mut Checker, fx: &Fixture, topic: &str) -> Vec<MessageQueue> {
    let c = match fx.started(ck, "queue").await {
        Some(c) => c,
        None => return Vec::new(),
    };
    let mqs = match c.fetch_subscribe_message_queues(topic).await {
        Ok(mqs) => mqs,
        Err(e) => {
            ck.abort("P2 fetch_subscribe_message_queues", &e.to_string());
            c.shutdown();
            return Vec::new();
        }
    };
    ck.check(
        &format!("P2 got {QUEUE_NUMS} queues"),
        mqs.len() == QUEUE_NUMS as usize,
        &format!("got {}", mqs.len()),
    );
    ck.check(
        "P2 queue topic/broker match the created topic",
        mqs.iter().all(|m| m.topic == topic)
            && mqs.iter().all(|m| m.broker_name == fx.broker_name),
        &format!("{mqs:?}"),
    );
    let ids: BTreeSet<i32> = mqs.iter().map(|m| m.queue_id).collect();
    ck.check(
        "P2 queue ids 0..3 with no duplicates or gaps",
        ids == (0..QUEUE_NUMS).collect::<BTreeSet<i32>>(),
        &format!("{ids:?}"),
    );

    c.register_topic(topic);
    let hb_ok = c.send_heartbeat_to_all_broker().await;
    ck.check("P2b the heartbeat went out after registering the topic (the precondition for 38's visibility)", hb_ok > 0, &format!("heartbeat_ok={hb_ok}"));
    let mut mine: Vec<MessageQueue> = Vec::new();
    let balance_deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        match c.fetch_message_queues_in_balance(topic).await {
            Ok(v) if v.len() == QUEUE_NUMS as usize => {
                mine = v;
                break;
            }
            Ok(v) => mine = v,
            Err(_) => {}
        }
        if std::time::Instant::now() >= balance_deadline {
            break;
        }
        // 38's visibility depends on the broker-side registration, so re-send the heartbeat on each retry.
        c.send_heartbeat_to_all_broker().await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    ck.check(
        &format!("P2b balance view: the exclusive group gets all {QUEUE_NUMS} queues (never pulled ⇒ it can only be computed from route+38)"),
        mine.len() == QUEUE_NUMS as usize,
        &format!("got {} of {}", mine.len(), QUEUE_NUMS),
    );
    ck.check(
        "P2b the balance view is a subset of this topic's subscription view",
        mine
            .iter()
            .all(|m| m.topic == topic && m.broker_name == fx.broker_name),
        &format!("{mine:?}"),
    );
    c.shutdown();
    mqs
}

async fn p3_offsets(ck: &mut Checker, fx: &Fixture, mqs: &[MessageQueue]) {
    let c = match fx.started(ck, "offset").await {
        Some(c) => c,
        None => return,
    };
    let mut spans = Vec::new();
    let mut err = String::new();
    for mq in mqs {
        match (c.min_offset(mq).await, c.max_offset(mq).await) {
            (Ok(min), Ok(max)) => spans.push((mq.queue_id, min, max)),
            (Err(e), _) | (_, Err(e)) => {
                if err.is_empty() {
                    err = e.to_string();
                }
            }
        }
    }
    if !err.is_empty() {
        ck.abort("P3 min/max offset", &err);
        c.shutdown();
        return;
    }
    ck.check(
        &format!("P3 per-queue max-min == {PER_QUEUE}"),
        spans
            .iter()
            .all(|(_, min, max)| max - min == PER_QUEUE as i64),
        &format!("{spans:?}"),
    );
    let total: i64 = spans.iter().map(|(_, min, max)| max - min).sum();
    ck.check(
        &format!("P3 total stock on the topic == {N_MSG}"),
        total == N_MSG as i64,
        &format!("total={total} spans={spans:?}"),
    );
    c.shutdown();
}

// ------------------------------------------------------ P4/P5/P6 manual pulling

async fn p4_pull_all(ck: &mut Checker, fx: &Fixture, topic: &str, mqs: &[MessageQueue]) {
    let c = match fx.started(ck, "pull").await {
        Some(c) => c,
        None => return,
    };
    let mut got: Vec<MessageExt> = Vec::new();
    let mut err = String::new();
    for mq in mqs {
        let min = match c.min_offset(mq).await {
            Ok(v) => v,
            Err(e) => {
                if err.is_empty() {
                    err = e.to_string();
                }
                continue;
            }
        };
        match c.pull(mq, TAG, min, 32, Some(5000)).await {
            Ok(r) => {
                ck.check(
                    &format!("P4 queue {} first pull FOUND", mq.queue_id),
                    r.status == PullStatus::Found && r.msg_found_list.len() == PER_QUEUE,
                    &describe(&r),
                );
                ck.check(
                    &format!("P4 queue {} next_begin_offset == max", mq.queue_id),
                    r.next_begin_offset == r.max_offset,
                    &describe(&r),
                );
                got.extend(r.msg_found_list);
            }
            Err(e) => {
                if err.is_empty() {
                    err = e.to_string();
                }
            }
        }
    }
    if !err.is_empty() {
        ck.abort("P4 pull", &err);
        c.shutdown();
        return;
    }
    let want: BTreeSet<String> = (0..QUEUE_NUMS)
        .flat_map(|q| (0..PER_QUEUE).map(move |i| format!("{topic}-{q}-{i}")))
        .collect();
    ck.check(
        &format!("P4 the manual pull gathered all {N_MSG} messages"),
        got.len() == N_MSG,
        &format!("got {}", got.len()),
    );
    ck.check(
        "P4 bodies match the sent set (no duplicates, no gaps)",
        bodies(&got) == want,
        &format!("{:?}", bodies(&got)),
    );
    ck.check(
        "P4 broker_name is filled back in (send_message_back needs it to look the route up)",
        got.iter()
            .all(|m| m.broker_name.as_deref() == Some(fx.broker_name.as_str())),
        &format!("{:?}", got.first().map(|m| m.broker_name.clone())),
    );
    ck.check(
        "P4 tag/queueId/reconsumeTimes and friends come from the broker",
        got.iter().all(|m| m.get_tags() == Some(TAG))
            && got.iter().all(|m| m.reconsume_times == 0)
            && got.iter().all(|m| m.queue_offset >= 0),
        &format!("{:?}", got.first().map(|m| (m.get_tags(), m.reconsume_times))),
    );

    // ---------------- P5 commit offsets and read them back
    let next = next_offsets(&got);
    let mut readback: Vec<(i32, Option<i64>)> = Vec::new();
    for mq in mqs {
        let off = next.get(&mq.queue_id).copied().unwrap_or(0);
        if let Err(e) = c.update_consume_offset(mq, off).await {
            ck.abort("P5 update_consume_offset", &e.to_string());
            c.shutdown();
            return;
        }
        readback.push((mq.queue_id, c.fetch_consume_offset(mq).await.unwrap_or(None)));
    }
    let consistent = mqs.iter().all(|mq| {
        let want = next.get(&mq.queue_id).copied().unwrap_or(0);
        readback
            .iter()
            .any(|(q, got)| *q == mq.queue_id && *got == Some(want))
    });
    ck.check(
        "P5 committed offsets are readable back from the broker (per-queue round-trip consistent)",
        consistent,
        &format!("{readback:?} next={next:?}"),
    );
    c.shutdown();

    // A brand-new group never committed → None (not 0).
    let fresh = match fx.started(ck, "fresh").await {
        Some(c) => c,
        None => return,
    };
    match fresh.fetch_consume_offset(&mqs[0]).await {
        Ok(v) => ck.check(
            "P5 a never-committed group's offset reads back None (it does not degrade to 0)",
            v.is_none(),
            &format!("{v:?}"),
        ),
        Err(e) => ck.abort("P5 read empty offset", &e.to_string()),
    }

    // ---------------- P6 offsets owned by the caller
    let mq = &mqs[0];
    let min = fresh.min_offset(mq).await.unwrap_or(0);
    let committed = next.get(&mq.queue_id).copied().unwrap_or(0);
    match fresh.pull(mq, TAG, committed, 32, Some(5000)).await {
        Ok(r) => ck.check(
            "P6 pull from the committed offset → NO_NEW_MSG",
            r.status == PullStatus::NoNewMsg && r.msg_found_list.is_empty(),
            &describe(&r),
        ),
        Err(e) => ck.abort("P6 pull at committed offset", &e.to_string()),
    }
    match fresh.pull(mq, TAG, min, 32, Some(5000)).await {
        Ok(r) => ck.check(
            &format!("P6 rewind to the queue head and pull → FOUND {PER_QUEUE} messages"),
            r.status == PullStatus::Found && r.msg_found_list.len() == PER_QUEUE,
            &describe(&r),
        ),
        Err(e) => ck.abort("P6 pull at rewound offset", &e.to_string()),
    }
    // An illegal offset (beyond max) → OFFSET_ILLEGAL, and next_begin_offset is corrected by the broker.
    match fresh.pull(mq, TAG, min + 10_000, 32, Some(5000)).await {
        Ok(r) => ck.check(
            "P6 out-of-range offset → OFFSET_ILLEGAL, with min/max carried back",
            r.status == PullStatus::OffsetIllegal && r.max_offset >= r.min_offset,
            &describe(&r),
        ),
        Err(e) => ck.abort("P6 out-of-range pull", &e.to_string()),
    }
    match fresh.pull(mq, "TagB", min, 32, Some(5000)).await {
        Ok(r) => ck.check(
            "P6 a non-matching tag → no messages (NO_MATCHED_MSG or NO_NEW_MSG)",
            r.msg_found_list.is_empty()
                && matches!(r.status, PullStatus::NoMatchedMsg | PullStatus::NoNewMsg),
            &describe(&r),
        ),
        Err(e) => ck.abort("P6 tag-filtered pull", &e.to_string()),
    }
    fresh.shutdown();
}

// ------------------------------------------------------------------ P7 long polling

async fn p7_long_polling(ck: &mut Checker, fx: &Fixture, topic: &str, mqs: &[MessageQueue]) {
    let c = match fx.started(ck, "block").await {
        Some(c) => c,
        None => return,
    };
    let mq = &mqs[1];
    let tail = match c.max_offset(mq).await {
        Ok(v) => v,
        Err(e) => {
            ck.abort("P7 max_offset", &e.to_string());
            c.shutdown();
            return;
        }
    };
    let body = format!("{topic}-blocked-0");
    let sent_body = body.clone();
    let producer = fx.producer.clone();
    let send_topic = topic.to_string();
    let broker = fx.broker_name.clone();
    // Send one message after 1.5s -- the long poll must stay hung on the broker until then.
    let sender = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let mut msg = Message::new(&send_topic, Some(sent_body.as_bytes()));
        msg.set_tags(TAG);
        let mq = MessageQueue::new(&send_topic, &broker, 1);
        producer.send(&mut msg, Some(5000), Some(&mq)).await.is_ok()
    });
    let began = Instant::now();
    let result = c.pull_block_if_not_found(mq, "*", tail, 16).await;
    let cost = began.elapsed();
    let sent = sender.await.unwrap_or(false);
    match result {
        Ok(r) => {
            ck.check(
                "P7 pull from the tail → the message arrives during the hang and returns FOUND",
                sent && r.status == PullStatus::Found && bodies(&r.msg_found_list).contains(&body),
                &format!("sent={sent} {}", describe(&r)),
            );
            // broker hang cap 20s, request timeout 30s: arriving after 1.5s → the round-trip is necessarily well above 1.5s,
            // and far below 30s (a degraded short poll would return NO_NEW_MSG immediately at ~0 cost).
            ck.check(
                "P7 it really hung (cost > 1s and < 25s)",
                cost > Duration::from_millis(1000) && cost < Duration::from_secs(25),
                &format!("cost={cost:?}"),
            );
        }
        Err(e) => ck.abort("P7 long polling", &e.to_string()),
    }
    c.shutdown();
}

// ---------------------------------------------------------- P8 time-based offset APIs

async fn p8_time_apis(ck: &mut Checker, fx: &Fixture, mqs: &[MessageQueue]) {
    let c = match fx.started(ck, "time").await {
        Some(c) => c,
        None => return,
    };
    let mq = &mqs[0];
    let min = c.min_offset(mq).await.unwrap_or(-1);
    let max = c.max_offset(mq).await.unwrap_or(-1);
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    // A future timestamp → past all messages (equals max); a very early timestamp → the queue head.
    match c.search_offset(mq, now_ms).await {
        Ok(off) => ck.check(
            "P8 search_offset(now) lands in [min, max]",
            (min..=max).contains(&off),
            &format!("off={off} min={min} max={max}"),
        ),
        Err(e) => ck.abort("P8 search_offset(now)", &e.to_string()),
    }
    match c.search_offset(mq, 1).await {
        Ok(off) => ck.check(
            "P8 search_offset(1) == min_offset",
            off == min,
            &format!("off={off} min={min}"),
        ),
        Err(e) => ck.abort("P8 search_offset(1)", &e.to_string()),
    }
    match c.earliest_msg_store_time(mq).await {
        Ok(t) => ck.check(
            "P8 earliest_msg_store_time is a plausible millisecond timestamp",
            t > 1_600_000_000_000 && t <= now_ms,
            &format!("{t} vs now={now_ms}"),
        ),
        Err(e) => ck.abort("P8 earliest_msg_store_time", &e.to_string()),
    }
    c.shutdown();
}

// -------------------------------------------------------------- P9 send message back

async fn p9_send_back(ck: &mut Checker, fx: &Fixture, mqs: &[MessageQueue]) {
    let group = fx.group_name("back");
    let c = match fx.started(ck, "back").await {
        Some(c) => c,
        None => return,
    };
    let mq = &mqs[2];
    let min = c.min_offset(mq).await.unwrap_or(0);
    let msg = match c.pull(mq, TAG, min, 1, Some(5000)).await {
        Ok(r) => match r.msg_found_list.into_iter().next() {
            Some(m) => m,
            None => {
                ck.abort("P9 take one message", "the pull came back empty");
                c.shutdown();
                return;
            }
        },
        Err(e) => {
            ck.abort("P9 pull", &e.to_string());
            c.shutdown();
            return;
        }
    };
    let body = String::from_utf8_lossy(msg.get_body()).into_owned();
    if let Err(e) = c.send_message_back(&msg, 0).await {
        ck.abort("P9 send_message_back", &e.to_string());
        c.shutdown();
        return;
    }
    // delayLevel=0 → visible immediately; the broker auto-creates %RETRY%<group> on the first send-back,
    // and the route needs the topic to exist, so poll instead of concluding from one pull.
    let retry_topic = format!("{}{}", MixAll::RETRY_GROUP_TOPIC_PREFIX, group);
    let mut got: Vec<MessageExt> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        for rmq in c.fetch_subscribe_message_queues(&retry_topic).await.unwrap_or_default() {
            let rmin = c.min_offset(&rmq).await.unwrap_or(0);
            let rmax = c.max_offset(&rmq).await.unwrap_or(0);
            if rmax > rmin {
                if let Ok(r) = c.pull(&rmq, "*", rmin, 32, Some(5000)).await {
                    got.extend(r.msg_found_list);
                }
            }
        }
        if bodies(&got).contains(&body) || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        "P9 the sent-back message shows up in %RETRY%<group> and is pullable",
        bodies(&got).contains(&body),
        &format!("retry_topic={retry_topic} got={:?}", bodies(&got)),
    );
    c.shutdown();
}

// ------------------------------------------------------- P10 consumer creates a topic

async fn p10_consumer_create_topic(ck: &mut Checker, fx: &Fixture) {
    let topic = fx.topic_name("Admin");
    let c = match fx.started(ck, "admin").await {
        Some(c) => c,
        None => return,
    };
    if let Err(e) = c.create_topic(&topic, 3, 0).await {
        ck.abort("P10 create_topic", &e.to_string());
        c.shutdown();
        return;
    }
    lock(&fx.topics).push(topic.clone());
    match c.fetch_subscribe_message_queues(&topic).await {
        Ok(mqs) => ck.check(
            "P10 after the consumer's own create_topic the route is visible (3 queues)",
            mqs.len() == 3,
            &format!("got {}", mqs.len()),
        ),
        Err(e) => ck.abort("P10 fetch route", &e.to_string()),
    }
    let mq = MessageQueue::new(&topic, &fx.broker_name, 0);
    match (c.min_offset(&mq).await, c.max_offset(&mq).await) {
        (Ok(min), Ok(max)) => ck.check(
            "P10 the new topic's queues answer min/max offset queries",
            min == 0 && max == 0,
            &format!("min={min} max={max}"),
        ),
        _ => ck.check("P10 the new topic's queues answer min/max offset queries", false, "the RPC failed"),
    }
    c.shutdown();
}

// ------------------------------------------------------------------ P11 cleanup

async fn p11_cleanup(ck: &mut Checker, fx: &Fixture) {
    let topics: Vec<String> = lock(&fx.topics).clone();
    let mut failed = Vec::new();
    for topic in &topics {
        if let Err(e) = fx
            .admin
            .delete_topic_in_broker(&fx.broker_addr, topic, 5000)
            .await
        {
            failed.push(format!("{topic}:{e}"));
        }
    }
    ck.check(
        "P11 delete the topics created this run",
        failed.is_empty(),
        &format!("{failed:?}"),
    );
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
    println!("== live pull-consumer check, namesrv={namesrv} stamp={stamp} ==");

    p1_lifecycle(&mut ck, &fx).await;

    let topic = fx.topic_name("Main");
    if let Err(e) = fx.create_topic(&topic, QUEUE_NUMS).await {
        ck.abort("create topic", &e);
        return ck;
    }
    let mut sent = 0;
    for q in 0..QUEUE_NUMS {
        sent += fx.produce_to(&topic, q, PER_QUEUE).await;
    }
    ck.check(
        &format!("all {N_MSG} sends are SEND_OK"),
        sent == N_MSG,
        &format!("sent={sent}"),
    );
    let mqs = p2_queues(&mut ck, &fx, &topic).await;
    if mqs.len() != QUEUE_NUMS as usize {
        ck.abort("not enough queues", "skipping the later scenarios");
    } else {
        p3_offsets(&mut ck, &fx, &mqs).await;
        p4_pull_all(&mut ck, &fx, &topic, &mqs).await;
        p7_long_polling(&mut ck, &fx, &topic, &mqs).await;
        p8_time_apis(&mut ck, &fx, &mqs).await;
        p9_send_back(&mut ck, &fx, &mqs).await;
        p10_consumer_create_topic(&mut ck, &fx).await;
    }
    p11_cleanup(&mut ck, &fx).await;
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
