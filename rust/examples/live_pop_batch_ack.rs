//! Live proof of BATCH_ACK_MESSAGE(200151) against a real Apache RocketMQ 5.5.1 broker.
//!
//! Usage (cluster up: namesrv 9876 + broker 10911, `autoCreateTopicEnable=true`):
//! ```text
//! cargo run --offline --example live_pop_batch_ack -- 127.0.0.1:9876
//! ```
//!
//! Why this leg has to exist on its own: the POP consumption path acknowledges one message at a
//! time through ACK_MESSAGE(200051), so no runtime path ever builds a batch. The merge key, the bit
//! index and the base64 BitSet of 200151 are reachable only through
//! `MQClientInstance::batch_ack_message` -- and a primitive nobody has pointed at a broker is a
//! primitive that may not work.
//!
//! The batch body carries no per-message offsets, only a start offset plus a bit set, so a return
//! code cannot answer "which messages did the broker really clear". The criterion used here is
//! observable broker state inside a timed window:
//!
//!   L1 ONE REQUEST, WHOLE BATCH. Pop the batch, clear it with a single 200151, then keep the queue
//!      silent for the whole window. Wrong offsets or wrong bits leave checkpoints alive, and the
//!      revive service puts those messages back inside that window.
//!   L2 PAIRED CONTROL, ONE CHECKPOINT LEFT OUT. Same batch with exactly one checkpoint missing:
//!      only that message may come back, and it comes back off the POP retry queue. The follow-up
//!      pins the retry direction -- the revived checkpoints carry the retry marker, and the batch
//!      body that clears them names the BUSINESS topic together with that marker, because the broker
//!      re-derives the retry topic from marker + group.
//!   L3 NO OVER-MERGING. Two pop rounds of one queue cleared by one request. `retry@queueId@
//!      ckQueueOffset@popTime` is the merge key, so the two rounds must stay in two entries;
//!      collapsing them would acknowledge offsets nobody popped.
//!
//! Three timing rules this leg obeys, because breaking any of them makes the run red for the wrong
//! reason:
//!
//!   * 200151 needs no broker switch and no cluster restart: the request code is registered
//!     unconditionally, so the standard POP broker config already serves it.
//!   * Every window that keeps popping stays SHORTER than the checkpoint invisibility
//!     (`INVISIBLE_MS`). The revival wait stops at the FIRST sighting plus a short tail, so the
//!     follow-up ack lands while its own checkpoint is still alive; only after the ack does the long
//!     silence window run. A window that keeps popping past the invisibility watches a message
//!     revive, re-pops it, lets it revive again -- and the last check would then fail on timing
//!     instead of on the wire path. Every wait here is gated on recorded state, never on a fixed sleep.
//!   * No assertion ever claims *which* RPC answered a message: a POP of the business topic also
//!     consults the retry queue, and the reply topic is rewritten on the client side, so "it came
//!     from the retry topic" is not evidence. Durable broker state is the evidence: the retry marker
//!     segment of the checkpoint, and the `%RETRY%<group>_<topic>` route reaching the nameserver --
//!     and that route is probed only AFTER the ack, since route registration can wait for a broker
//!     heartbeat.
//!
//! Each leg creates a single-queue topic, sends its bodies and waits until the queue really holds
//! what was sent (polling the depth) before popping: a partially dispatched batch would make "one
//! request cleared the batch" unprovable. Topic and group names are stamped with the unix seconds,
//! so concurrent runs of other clients on the same cluster never share a queue.
//!
//! The last line is `== summary: N passed, M failed ==`; the process exits non-zero on any failure.

use std::collections::BTreeSet;
use std::env;
use std::future::Future;
use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde_json::Value;

use rocketmq_client_remoting::client::mq_client::{MQClientInstance, PublishMessage};
use rocketmq_client_remoting::client::result::{PopResult, PopStatus, SendStatus};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::message_const::PROPERTY_POP_CK;
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::sysflag::ConsumeInitMode;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::client::RemotingClient;
use rocketmq_client_remoting::remoting::protocol::body::{BatchAck, BatchAckMessageRequestBody};
use rocketmq_client_remoting::remoting::protocol::codes::{request_code, response_code};
use rocketmq_client_remoting::remoting::protocol::extra_info;
use rocketmq_client_remoting::remoting::protocol::headers::{
    CreateTopicRequestHeader, GetRouteInfoRequestHeader,
};
use rocketmq_client_remoting::remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;
use rocketmq_client_remoting::remoting::protocol::serialize::RemotingSerializable;

/// Invisible time of every checkpoint of this run: 10s. Every window that keeps popping stays
/// below it, so an ack always lands on a live checkpoint.
const INVISIBLE_MS: i64 = 10_000;
/// The silence window that follows an ack: long enough for the revive service to put back any
/// checkpoint the request failed to clear.
const SILENCE: Duration = Duration::from_secs(25);
/// Extra watching after the first sighting, so the whole revived batch is recorded.
const REVIVE_TAIL: Duration = Duration::from_secs(3);
/// Upper bound of the wait for the first sighting (it ends at the sighting, not at the bound).
const PROBE_BUDGET: Duration = Duration::from_secs(30);
/// Upper bound of the waits for the queue depth and for a route.
const DEPTH_BUDGET: Duration = Duration::from_secs(30);
const ROUTE_BUDGET: Duration = Duration::from_secs(30);
/// Upper bound for the POP retry topic's route: that topic only exists once the revive service has
/// written to it, and the nameserver learns about it on a broker heartbeat, so the wait is longer.
const RETRY_ROUTE_BUDGET: Duration = Duration::from_secs(90);
/// Pause between two pop rounds of an observation window.
const ROUND_PAUSE: Duration = Duration::from_millis(1_000);

/// Batch size of one POP: always ask for more than the queue holds, so "the batch" is one request.
const POP_MAX_MSGS: i32 = 32;
const POP_TIMEOUT_MS: i64 = 30_000;
/// RPC timeout of an ack / an offset query / a route query.
const RPC_TIMEOUT_MS: i64 = 5_000;

/// The compact field spellings of one batch-ack entry, in wire order. Everything the broker needs
/// rides the body: the request itself carries no custom header.
const ACK_FIELDS: [&str; 9] = ["c", "t", "r", "so", "q", "rq", "pt", "it", "b"];
/// Top-level field spellings of the batch body, in wire order.
const BODY_FIELDS: [&str; 2] = ["brokerName", "acks"];

// ------------------------------------------------------------------ skeleton

fn stamp() -> String {
    let secs = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => 0,
    };
    format!("{secs}")
}

/// Assertion accumulator: every leg runs to the end before the total is printed; one failed check
/// does not hide the rest of the evidence.
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

    /// A check that cannot be answered without the run having broken down: counted as a failure and
    /// the leg stops, because there is no batch left to assert about.
    fn abort(&mut self, name: &str, err: &str) {
        println!("  [FAIL] {name}: {err}");
        self.failed.push(format!("{name}: {err}"));
    }
}

/// Poll an async condition until it holds; on timeout return `false` so the caller can print the scene.
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

async fn route_response(
    client: &RemotingClient,
    namesrv: &str,
    topic: &str,
) -> Result<RemotingCommand, String> {
    let mut request = RemotingCommand::create_request_command(
        request_code::GET_ROUTEINFO_BY_TOPIC,
        Some(Box::new(GetRouteInfoRequestHeader {
            topic: Some(topic.to_string()),
            accept_standard_json_only: None,
        })),
    );
    client
        .invoke_sync(namesrv, &mut request, Some(RPC_TIMEOUT_MS))
        .await
        .map_err(|e| format!("route query of {topic} failed: {e}"))
}

fn decode_route(resp: &RemotingCommand, what: &str) -> Result<TopicRouteData, String> {
    let raw = match resp.body() {
        Some(b) => b.to_vec(),
        None => return Err(format!("{what} route response has an empty body")),
    };
    // The nameserver's route is fastjson-style (numeric map keys without quotes): it must go
    // through the compatible decoder.
    let value = RemotingSerializable::decode(&raw)
        .map_err(|e| format!("{what} body is not parsable: {e}"))?;
    TopicRouteData::from_json_value(&value).map_err(|e| format!("{what} route is invalid: {e}"))
}

/// The one broker of this cluster, found through the TBW102 default-topic route.
async fn cluster_broker(client: &RemotingClient, namesrv: &str) -> Result<(String, String), String> {
    let resp = route_response(client, namesrv, MixAll::DEFAULT_TOPIC).await?;
    if resp.code != response_code::SUCCESS {
        return Err(format!(
            "no route of default topic {}: code={} remark={:?}",
            MixAll::DEFAULT_TOPIC,
            resp.code,
            resp.remark
        ));
    }
    let route = decode_route(&resp, MixAll::DEFAULT_TOPIC)?;
    let broker_data = route
        .broker_datas
        .first()
        .ok_or_else(|| "the default route has no brokerData".to_string())?;
    let addr = broker_data
        .select_broker_addr()
        .ok_or_else(|| format!("broker {} has no usable address", broker_data.broker_name))?;
    Ok((broker_data.broker_name.clone(), addr))
}

/// The broker-side code of an error, when the call failed before the code could be returned.
fn error_code(err: &Error) -> i32 {
    match err {
        Error::Broker { response_code, .. } | Error::Server { response_code, .. } => *response_code,
        _ => -1,
    }
}

// ------------------------------------------------------------------ checkpoints

/// One decoded POP_CK: the 8 space-separated segments the popped message carries.
#[derive(Debug, Clone)]
struct Ck {
    raw: String,
    body: String,
    /// Segment 0: the queue start offset of the pop that produced this checkpoint.
    ck_offset: i64,
    /// Segment 7: this message's own offset in the consume queue -- the bit index of the batch body
    /// is `queue_offset - ck_offset`.
    queue_offset: i64,
    pop_time: i64,
    invisible_time: i64,
    retry: String,
    broker_name: String,
    queue_id: i32,
    revive_qid: i32,
}

fn parse_ck(raw: &str, body: &str) -> Result<Ck, String> {
    let segments = extra_info::split(raw).map_err(|e| format!("{raw:?}: {e}"))?;
    if segments.len() != 8 {
        return Err(format!("{raw:?} has {} segments, not 8", segments.len()));
    }
    Ok(Ck {
        raw: raw.to_string(),
        body: body.to_string(),
        ck_offset: extra_info::get_ck_queue_offset(&segments)
            .map_err(|e| format!("{raw:?}: {e}"))?,
        queue_offset: extra_info::get_queue_offset(&segments)
            .map_err(|e| format!("{raw:?}: {e}"))?,
        pop_time: extra_info::get_pop_time(&segments).map_err(|e| format!("{raw:?}: {e}"))?,
        invisible_time: extra_info::get_invisible_time(&segments)
            .map_err(|e| format!("{raw:?}: {e}"))?,
        retry: extra_info::get_retry(&segments).map_err(|e| format!("{raw:?}: {e}"))?,
        broker_name: extra_info::get_broker_name(&segments)
            .map_err(|e| format!("{raw:?}: {e}"))?,
        queue_id: extra_info::get_queue_id(&segments).map_err(|e| format!("{raw:?}: {e}"))?,
        revive_qid: extra_info::get_revive_qid(&segments).map_err(|e| format!("{raw:?}: {e}"))?,
    })
}

fn msg_body(msg: &MessageExt) -> String {
    String::from_utf8_lossy(msg.body.as_deref().unwrap_or_default()).into_owned()
}

fn msg_ck(msg: &MessageExt) -> String {
    msg.properties
        .get(PROPERTY_POP_CK)
        .map(|ck| ck.to_string())
        .unwrap_or_default()
}

/// Decode every popped message; anything unreadable is collected as a problem text instead of
/// silently skipped, so a red check says what is wrong.
fn popped_cks(msgs: &[MessageExt], problems: &mut Vec<String>) -> Vec<Ck> {
    let mut cks = Vec::with_capacity(msgs.len());
    for msg in msgs {
        match parse_ck(&msg_ck(msg), &msg_body(msg)) {
            Ok(ck) => cks.push(ck),
            Err(problem) => problems.push(problem),
        }
    }
    cks
}

fn sightings_to_cks(sightings: &[Sighting], problems: &mut Vec<String>) -> Vec<Ck> {
    let mut cks = Vec::with_capacity(sightings.len());
    for s in sightings {
        match parse_ck(&s.ck, &s.body) {
            Ok(ck) => cks.push(ck),
            Err(problem) => problems.push(problem),
        }
    }
    cks
}

/// The merge key of a checkpoint: entries of one key collapse into one batch-ack entry.
fn merge_key(ck: &Ck) -> String {
    format!("{}@{}@{}@{}", ck.retry, ck.queue_id, ck.ck_offset, ck.pop_time)
}

/// The same key read off a built entry.
fn merge_key_of(ack: &BatchAck) -> String {
    format!(
        "{}@{}@{}@{}",
        ack.retry, ack.queue_id, ack.start_offset, ack.pop_time
    )
}

/// The consume-queue offsets one entry clears: its start offset plus every set bit.
fn acked_offsets(ack: &BatchAck) -> Vec<i64> {
    let mut offsets: Vec<i64> = ack
        .bit_set
        .iter()
        .map(|bit| ack.start_offset + i64::from(*bit))
        .collect();
    offsets.sort_unstable();
    offsets
}

fn sorted_unique(values: &[String]) -> Vec<String> {
    BTreeSet::from_iter(values.iter().cloned()).into_iter().collect()
}

/// Order-insensitive comparison of two offset sets, with duplicates on either side reported as
/// different: "exactly these offsets, once each" is the claim.
fn same_int_set(got: &[i64], want: &[i64]) -> bool {
    let mut g = got.to_vec();
    let mut w = want.to_vec();
    g.sort_unstable();
    w.sort_unstable();
    g == w
}

/// Distinct values, first-seen order kept -- the order the wire array follows.
fn distinct(values: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(values.len());
    for value in values {
        if !out.iter().any(|seen| seen == value) {
            out.push(value.clone());
        }
    }
    out
}

/// The field names of a JSON object, in the order they were written.
fn keys_of(value: &Value) -> Vec<&str> {
    match value {
        Value::Object(map) => map.keys().map(|k| k.as_str()).collect(),
        _ => Vec::new(),
    }
}

// ------------------------------------------------------------------ observation

/// One message seen by a pop round: its body, the topic the reply reported it under, and its CK.
#[derive(Debug, Clone)]
struct Sighting {
    body: String,
    /// Diagnostic only: the reply topic is rewritten on the client side, so this is never evidence.
    reported_topic: String,
    ck: String,
}

#[derive(Debug, Default)]
struct Eyes {
    sightings: Vec<Sighting>,
}

impl Eyes {
    fn came_back(&self) -> bool {
        !self.sightings.is_empty()
    }

    /// The distinct bodies that were seen, sorted -- what "only that message came back" counts.
    fn bodies(&self) -> Vec<String> {
        sorted_unique(
            &self
                .sightings
                .iter()
                .map(|s| s.body.clone())
                .collect::<Vec<String>>(),
        )
    }

    fn detail(&self) -> String {
        format!(
            "sightings={} bodies={:?} reportedTopics={:?}",
            self.sightings.len(),
            self.bodies(),
            sorted_unique(
                &self
                    .sightings
                    .iter()
                    .map(|s| s.reported_topic.clone())
                    .collect::<Vec<String>>()
            )
        )
    }
}

// ------------------------------------------------------------------ the live side

/// One started instance plus the broker it talks to: the POP / batch-ACK / offset / route surface
/// the three legs are written against.
struct Harness {
    client: RemotingClient,
    instance: MQClientInstance,
    namesrv: String,
    broker_name: String,
    broker_addr: String,
    producer_group: String,
}

impl Harness {
    /// Create a **single-queue** topic through the default-topic route, wait for its route, then send
    /// one message per body.
    async fn make_topic(&self, topic: &str, bodies: &[String]) -> Result<(), String> {
        let mut request = RemotingCommand::create_request_command(
            request_code::UPDATE_AND_CREATE_TOPIC,
            Some(Box::new(CreateTopicRequestHeader {
                topic: Some(topic.to_string()),
                default_topic: Some(MixAll::DEFAULT_TOPIC.to_string()),
                read_queue_nums: Some(1),
                write_queue_nums: Some(1),
                perm: Some(DEFAULT_PERM),
                topic_filter_type: Some(TopicFilterType::SINGLE_TAG.to_string()),
                topic_sys_flag: Some(0),
                order: Some(false),
                attributes: Some(String::new()),
                force: Some(false),
            })),
        );
        let resp = self
            .client
            .invoke_sync(&self.broker_addr, &mut request, Some(RPC_TIMEOUT_MS))
            .await
            .map_err(|e| format!("create topic {topic} failed: {e}"))?;
        if resp.code != response_code::SUCCESS {
            return Err(format!(
                "create topic {topic} on {} failed: code={} remark={:?}",
                self.broker_addr, resp.code, resp.remark
            ));
        }
        if !self.wait_route(topic, ROUTE_BUDGET).await {
            return Err(format!("the route of {topic} never reached {srv}", srv = self.namesrv));
        }
        self.instance.register_topic_in_use(topic);

        let mq = MessageQueue::new(topic, &self.broker_name, 0);
        for body in bodies {
            let mut sent = false;
            let mut last = String::new();
            for _ in 0..5 {
                let mut msg = Message::new(topic, Some(body.as_bytes()));
                msg.set_tags("TagBatchAck");
                msg.set_keys(body);
                let mut publish = PublishMessage::Single(&mut msg);
                match self
                    .instance
                    .send_message(
                        &self.producer_group,
                        &mut publish,
                        &mq,
                        10_000,
                        0,
                        false,
                        MixAll::DEFAULT_TOPIC,
                        MixAll::DEFAULT_TOPIC_QUEUE_NUMS,
                    )
                    .await
                {
                    Ok(result) if result.status == SendStatus::SendOk => {
                        sent = true;
                        break;
                    }
                    Ok(result) => last = format!("{body}: send status {:?}", result.status),
                    Err(e) => last = format!("{body}: {e}"),
                }
                // The route can still be propagating right after createTopic.
                tokio::time::sleep(Duration::from_millis(2_000)).await;
            }
            if !sent {
                return Err(if last.is_empty() {
                    format!("{body}: never sent to {topic} queue 0")
                } else {
                    last
                });
            }
        }
        Ok(())
    }

    /// Messages are dispatched to the consume queue asynchronously: wait until the queue really
    /// holds what was sent, or "one request cleared the batch" proves nothing.
    async fn depth(&self, topic: &str) -> i64 {
        let mq = MessageQueue::new(topic, &self.broker_name, 0);
        let min = self
            .instance
            .get_min_offset(&mq, RPC_TIMEOUT_MS, Some(&self.broker_addr))
            .await
            .unwrap_or(0);
        let max = self
            .instance
            .get_max_offset(&mq, RPC_TIMEOUT_MS, Some(&self.broker_addr))
            .await
            .unwrap_or(0);
        max - min
    }

    async fn wait_depth(&self, topic: &str, want: i64) -> i64 {
        let deadline = Instant::now() + DEPTH_BUDGET;
        loop {
            let depth = self.depth(topic).await;
            if depth >= want || Instant::now() > deadline {
                return depth;
            }
            tokio::time::sleep(ROUND_PAUSE).await;
        }
    }

    /// A route the nameserver can serve for this topic: queueDatas plus the broker behind them.
    async fn route_exists(&self, topic: &str) -> bool {
        match route_response(&self.client, &self.namesrv, topic).await {
            Ok(resp) if resp.code == response_code::SUCCESS => {
                match decode_route(&resp, topic) {
                    Ok(route) => {
                        !route.queue_datas.is_empty()
                            && route.broker_datas.iter().any(|b| b.broker_name == self.broker_name)
                    }
                    Err(_) => false,
                }
            }
            _ => false,
        }
    }

    async fn wait_route(&self, topic: &str, budget: Duration) -> bool {
        wait_async(|| async { self.route_exists(topic).await }, budget).await
    }

    async fn pop(&self, group: &str, topic: &str, queue_id: i32, max_msgs: i32) -> Result<PopResult, String> {
        self.instance
            .pop_message(
                group,
                topic,
                queue_id,
                max_msgs,
                INVISIBLE_MS,
                0,
                ConsumeInitMode::MIN,
                None,
                None,
                false,
                Some(&self.broker_name),
                POP_TIMEOUT_MS,
                Some(&self.broker_addr),
            )
            .await
            .map_err(|e| format!("pop {topic} failed: {e}"))
    }

    /// One 200151 for a list of checkpoints; reports the broker's code without turning a non-SUCCESS
    /// into a panic, so the check says what the wire answered.
    async fn batch_ack(&self, group: &str, topic: &str, raws: &[&str]) -> (i32, String) {
        match self
            .instance
            .batch_ack_message(group, topic, raws, RPC_TIMEOUT_MS, Some(&self.broker_addr))
            .await
        {
            Ok(code) => (code, String::new()),
            Err(e) => (error_code(&e), e.to_string()),
        }
    }

    /// Pop the business topic and the group's POP retry topic once, and record what came back.
    async fn pop_round(&self, group: &str, topic: &str, eyes: &mut Eyes) {
        let retry_topic = extra_info::build_pop_retry_topic_v1(topic, group);
        for target in [topic, retry_topic.as_str()] {
            let Ok(pop) = self.pop(group, target, 0, POP_MAX_MSGS).await else {
                // A retry topic the group has never revived into is simply absent.
                continue;
            };
            if pop.status != PopStatus::Found {
                continue;
            }
            for msg in pop.msg_found_list.iter() {
                eyes.sightings.push(Sighting {
                    body: msg_body(msg),
                    reported_topic: msg.topic.clone(),
                    ck: msg_ck(msg),
                });
            }
        }
    }

    /// Watch a fixed window: used where the expected answer is silence.
    async fn watch(&self, group: &str, topic: &str, window: Duration, eyes: &mut Eyes) {
        let until = Instant::now() + window;
        while Instant::now() < until {
            self.pop_round(group, topic, eyes).await;
            tokio::time::sleep(ROUND_PAUSE).await;
        }
    }

    /// Watch until the FIRST sighting, then keep watching a short tail so the whole revived batch is
    /// recorded -- and stop. A sighted checkpoint left to expire is revived again before the ack can
    /// land, so a longer pre-ack window would fail on timing instead of on the wire path.
    async fn probe(&self, group: &str, topic: &str, eyes: &mut Eyes) -> bool {
        let until = Instant::now() + PROBE_BUDGET;
        while Instant::now() < until {
            self.pop_round(group, topic, eyes).await;
            if eyes.came_back() {
                self.watch(group, topic, REVIVE_TAIL, eyes).await;
                return true;
            }
            tokio::time::sleep(ROUND_PAUSE).await;
        }
        false
    }
}

/// A batch body plus everything a leg wants to say about it, with the wire JSON already parsed.
struct Built {
    body: BatchAckMessageRequestBody,
    json: Value,
}

impl Built {
    fn entry(&self, index: usize) -> Option<&Value> {
        self.json.get("acks")?.get(index)
    }
}

fn build_body(
    ck: &mut Checker,
    name: &str,
    group: &str,
    topic: &str,
    raws: &[String],
) -> Result<Built, String> {
    let refs: Vec<&str> = raws.iter().map(|r| r.as_str()).collect();
    let body =
        BatchAckMessageRequestBody::from_extra_info_list(group, topic, &refs).map_err(|e| e.to_string())?;
    let encoded = body.encode();
    let json: Value =
        serde_json::from_slice(&encoded).map_err(|e| format!("the encoded body is not JSON: {e}"))?;
    let round = BatchAckMessageRequestBody::decode(&encoded);
    ck.check(
        &format!("{name} the body round-trips through the wire codec"),
        matches!(&round, Ok(back) if *back == body),
        &format!(
            "decoded={} {:?}",
            match &round {
                Ok(back) => format!("{:?} acks", back.acks.len()),
                Err(e) => e.to_string(),
            },
            String::from_utf8_lossy(&encoded)
        ),
    );
    Ok(Built { body, json })
}

// ------------------------------------------------------------------ L1

/// One request clears a whole batch, and the body shape is checked field by field before it goes out.
async fn leg_one_request_whole_batch(h: &Harness, stamp: &str, ck: &mut Checker) {
    let topic = format!("RbaAll_{stamp}");
    let group = format!("CID_RbaAll_{stamp}");
    let bodies: Vec<String> = (0..8).map(|i| format!("ba-all-{i}")).collect();

    println!("=== L1 one request clears a whole batch ({topic} / {group}) ===");
    if let Err(e) = h.make_topic(&topic, &bodies).await {
        ck.check("L1 sent 8 messages to a single-queue topic", false, &e);
        return;
    }
    ck.check("L1 sent 8 messages to a single-queue topic", true, "");

    let depth = h.wait_depth(&topic, 8).await;
    ck.check(
        "L1 all 8 messages are on the queue before popping",
        depth >= 8,
        &format!("depth={depth}"),
    );
    if depth < 8 {
        return;
    }

    let pop = match h.pop(&group, &topic, 0, POP_MAX_MSGS).await {
        Ok(pop) => pop,
        Err(e) => {
            ck.check("L1 POP_MESSAGE answered the whole batch", false, &e);
            return;
        }
    };
    ck.check(
        "L1 POP_MESSAGE answered the whole batch",
        pop.status == PopStatus::Found && pop.msg_found_list.len() == 8,
        &format!("{pop}"),
    );
    if pop.msg_found_list.len() != 8 {
        return;
    }

    let mut problems = Vec::new();
    let cks = popped_cks(&pop.msg_found_list, &mut problems);
    ck.check(
        "L1 every message carries a parsable 8-segment checkpoint",
        problems.is_empty(),
        &problems.join("; "),
    );
    ck.check(
        "L1 the eight checkpoints are all there",
        cks.len() == 8,
        &format!("{} parsed", cks.len()),
    );
    if cks.len() != 8 {
        return;
    }

    let keys = sorted_unique(&cks.iter().map(merge_key).collect::<Vec<String>>());
    let ck_offsets = sorted_unique(
        &cks.iter()
            .map(|c| c.ck_offset.to_string())
            .collect::<Vec<String>>(),
    );
    let uniform = ck_offsets.len() == 1
        && cks.iter().all(|c| {
            c.broker_name == h.broker_name
                && c.queue_id == 0
                && c.invisible_time == INVISIBLE_MS
                && c.retry == extra_info::NORMAL_TOPIC
        });
    ck.check(
        "L1 the checkpoint fields the batch body depends on are uniform",
        uniform,
        &format!(
            "ckOffsets={ck_offsets:?} broker={:?} queueId={} retry={} invisible={} reviveQid={}",
            cks[0].broker_name, cks[0].queue_id, cks[0].retry, cks[0].invisible_time, cks[0].revive_qid
        ),
    );

    let want_offsets: Vec<i64> = cks.iter().map(|c| c.queue_offset).collect();
    let raws: Vec<String> = cks.iter().map(|c| c.raw.clone()).collect();
    let built = match build_body(ck, "L1", &group, &topic, &raws) {
        Ok(built) => built,
        Err(e) => {
            ck.abort("L1 the batch body builds", &e);
            return;
        }
    };
    let body = &built.body;
    if body.acks.is_empty() {
        ck.abort("L1 the batch body produced at least one entry", "no entries at all");
        return;
    }

    ck.check(
        "L1 the body acknowledges exactly the popped offsets, once each",
        body.acks.len() == 1 && same_int_set(&acked_offsets(&body.acks[0]), &want_offsets),
        &format!(
            "acked={:?} want={:?}",
            body.acks.iter().map(acked_offsets).collect::<Vec<Vec<i64>>>(),
            want_offsets
        ),
    );
    ck.check(
        "L1 the body has exactly one entry per merge key",
        body.acks.len() == keys.len()
            && sorted_unique(&body.acks.iter().map(merge_key_of).collect::<Vec<String>>()) == keys,
        &format!("{} entries for {} keys {keys:?}", body.acks.len(), keys.len()),
    );

    let ack = &body.acks[0];
    let echoes: [(&str, bool); 8] = [
        ("consumerGroup", ack.consumer_group == group),
        ("topic", ack.topic == topic),
        ("retry marker", ack.retry == extra_info::NORMAL_TOPIC),
        ("startOffset", ack.start_offset == cks[0].ck_offset),
        ("queueId", ack.queue_id == 0),
        ("reviveQueueId", ack.revive_queue_id == cks[0].revive_qid),
        ("popTime", ack.pop_time == cks[0].pop_time),
        ("invisibleTime", ack.invisible_time == INVISIBLE_MS),
    ];
    let wrong: Vec<&str> = echoes
        .iter()
        .filter(|(_, ok)| !*ok)
        .map(|(field, _)| *field)
        .collect();
    ck.check(
        "L1 every entry echoes its checkpoint fields and names the group and the business topic",
        wrong.is_empty(),
        &format!(
            "failed={wrong:?} entry={} ck={{retry={} ckOffset={} queueId={} popTime={} invisible={} reviveQid={}}}",
            ack.to_json_value(),
            cks[0].retry,
            cks[0].ck_offset,
            cks[0].queue_id,
            cks[0].pop_time,
            cks[0].invisible_time,
            cks[0].revive_qid
        ),
    );
    ck.check(
        "L1 the body broker name is the broker name of the first checkpoint",
        body.broker_name.as_deref() == Some(cks[0].broker_name.as_str())
            && body.broker_name.as_deref() == Some(h.broker_name.as_str()),
        &format!(
            "bodyBroker={:?} firstCkBroker={} topicBroker={}",
            body.broker_name, cks[0].broker_name, h.broker_name
        ),
    );

    let names_ok = keys_of(&built.json) == BODY_FIELDS.as_slice()
        && built
            .entry(0)
            .map(|entry| keys_of(entry) == ACK_FIELDS.as_slice())
            .unwrap_or(false);
    ck.check(
        "L1 the request carries nothing but the body: compact field spellings, all nine present",
        names_ok,
        &format!(
            "body={:?} entry={:?}",
            keys_of(&built.json),
            built.entry(0).map(keys_of).unwrap_or_default()
        ),
    );

    let bit_text = built
        .entry(0)
        .and_then(|entry| entry.get("b"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let bit_bytes = BASE64.decode(&bit_text).unwrap_or_default();
    ck.check(
        "L1 the bit set is the byte-minimal little-endian base64 of the whole batch",
        bit_bytes == vec![0xff_u8],
        &format!("b={bit_text:?} decoded={bit_bytes:?} (8 bits set => one byte 0xff)"),
    );
    let decoded_offsets: Result<Vec<i64>, String> = match built.entry(0) {
        Some(entry) => BatchAck::from_json_value(entry)
            .map(|ack| acked_offsets(&ack))
            .map_err(|e| e.to_string()),
        None => Err("the encoded body has no acks array".to_string()),
    };
    ck.check(
        "L1 the bit set decodes back to exactly the batch offsets",
        decoded_offsets
            .as_ref()
            .map(|offsets| same_int_set(offsets, &want_offsets))
            .unwrap_or(false),
        &format!("decoded={decoded_offsets:?} want={want_offsets:?}"),
    );

    let refs: Vec<&str> = raws.iter().map(|r| r.as_str()).collect();
    let (code, remark) = h.batch_ack(&group, &topic, &refs).await;
    ck.check(
        "L1 one BATCH_ACK_MESSAGE answered SUCCESS",
        code == response_code::SUCCESS,
        &format!("code={code} {remark}"),
    );

    let mut eyes = Eyes::default();
    h.watch(&group, &topic, SILENCE, &mut eyes).await;
    ck.check(
        "L1 nothing revived inside the silence window (the batch really landed)",
        !eyes.came_back(),
        &eyes.detail(),
    );
}

// ------------------------------------------------------------------ L2

/// The paired control: one checkpoint missing from the batch, and the retry ack path that clears it.
async fn leg_one_checkpoint_left_out(h: &Harness, stamp: &str, ck: &mut Checker) {
    let topic = format!("RbaPart_{stamp}");
    let group = format!("CID_RbaPart_{stamp}");
    let bodies: Vec<String> = (0..6).map(|i| format!("ba-part-{i}")).collect();

    println!("=== L2 one checkpoint left out of the batch ({topic} / {group}) ===");
    if let Err(e) = h.make_topic(&topic, &bodies).await {
        ck.check("L2 sent 6 messages to a single-queue topic", false, &e);
        return;
    }
    ck.check("L2 sent 6 messages to a single-queue topic", true, "");

    let depth = h.wait_depth(&topic, 6).await;
    ck.check(
        "L2 all 6 messages are on the queue before popping",
        depth >= 6,
        &format!("depth={depth}"),
    );
    if depth < 6 {
        return;
    }

    let pop = match h.pop(&group, &topic, 0, POP_MAX_MSGS).await {
        Ok(pop) => pop,
        Err(e) => {
            ck.check("L2 POP_MESSAGE answered the batch", false, &e);
            return;
        }
    };
    ck.check(
        "L2 POP_MESSAGE answered the batch",
        pop.status == PopStatus::Found && pop.msg_found_list.len() == 6,
        &format!("{pop}"),
    );
    if pop.msg_found_list.len() != 6 {
        return;
    }
    let mut problems = Vec::new();
    let mut cks = popped_cks(&pop.msg_found_list, &mut problems);
    ck.check(
        "L2 every message carries a parsable 8-segment checkpoint",
        problems.is_empty() && cks.len() == 6,
        &format!("{} parsed, problems={}", cks.len(), problems.join("; ")),
    );
    if cks.len() != 6 {
        return;
    }
    cks.sort_by_key(|c| c.queue_offset);

    // The dropped checkpoint is the highest offset: the last bit of the batch.
    let dropped = cks[5].clone();
    let kept: Vec<Ck> = cks[..5].to_vec();
    let kept_raws: Vec<String> = kept.iter().map(|c| c.raw.clone()).collect();
    let kept_offsets: Vec<i64> = kept.iter().map(|c| c.queue_offset).collect();
    let kept_bodies: Vec<String> = kept.iter().map(|c| c.body.clone()).collect();

    let built = match build_body(ck, "L2", &group, &topic, &kept_raws) {
        Ok(built) => built,
        Err(e) => {
            ck.abort("L2 the partial batch body builds", &e);
            return;
        }
    };
    let partial_offsets: Vec<i64> = built
        .body
        .acks
        .iter()
        .flat_map(acked_offsets)
        .collect();
    ck.check(
        "L2 the body acknowledges 5 of the 6 offsets and not the dropped one",
        built.body.acks.len() == 1
            && same_int_set(&partial_offsets, &kept_offsets)
            && !partial_offsets.contains(&dropped.queue_offset),
        &format!(
            "acked={partial_offsets:?} kept={kept_offsets:?} dropped={}",
            dropped.queue_offset
        ),
    );

    let refs: Vec<&str> = kept_raws.iter().map(|r| r.as_str()).collect();
    let (code, remark) = h.batch_ack(&group, &topic, &refs).await;
    ck.check(
        "L2 the partial BATCH_ACK_MESSAGE answered SUCCESS",
        code == response_code::SUCCESS,
        &format!("code={code} {remark}"),
    );

    let mut eyes = Eyes::default();
    let brought_back = h.probe(&group, &topic, &mut eyes).await;
    ck.check(
        "L2 the message left out of the batch really came back",
        brought_back,
        &eyes.detail(),
    );
    let sighted = eyes.bodies();
    ck.check(
        "L2 the five acked messages stayed gone while the dropped one came back",
        sighted.len() == 1 && sighted[0] == dropped.body && !sighted.iter().any(|b| kept_bodies.contains(b)),
        &format!("sighted={sighted:?} kept={kept_bodies:?} dropped={}", dropped.body),
    );
    if !brought_back {
        return;
    }

    let mut revive_problems = Vec::new();
    let revived = sightings_to_cks(&eyes.sightings, &mut revive_problems);
    ck.check(
        "L2 the revived checkpoints are parsable",
        revive_problems.is_empty() && !revived.is_empty(),
        &format!("{} parsed, problems={}", revived.len(), revive_problems.join("; ")),
    );
    if revived.is_empty() {
        return;
    }
    ck.check(
        "L2 the revived checkpoints carry the retry marker (they live in the retry queue)",
        revived
            .iter()
            .all(|c| c.retry == extra_info::RETRY_TOPIC),
        &format!(
            "markers={:?} offsets={:?}",
            sorted_unique(&revived.iter().map(|c| c.retry.clone()).collect::<Vec<String>>()),
            revived.iter().map(|c| c.queue_offset).collect::<Vec<i64>>()
        ),
    );

    // Every checkpoint the revival produced, oldest first: the broker has one record per pop, and a
    // stale one that is never acknowledged would revive the message again.
    let retry_raws = distinct(
        &eyes
            .sightings
            .iter()
            .map(|s| s.ck.clone())
            .collect::<Vec<String>>(),
    );
    let retry_built = match build_body(ck, "L2", &group, &topic, &retry_raws) {
        Ok(built) => built,
        Err(e) => {
            ck.abort("L2 the retry batch body builds", &e);
            return;
        }
    };
    let marker_ok = !retry_built.body.acks.is_empty()
        && retry_built.body.acks.iter().all(|a| {
            a.topic == topic
                && a.retry == extra_info::RETRY_TOPIC
                && a.consumer_group == group
        });
    ck.check(
        "L2 the retry body keeps the business topic and carries the retry marker on every entry",
        marker_ok,
        &format!(
            "entries={:?}",
            retry_built
                .body
                .acks
                .iter()
                .map(|a| a.to_json_value())
                .collect::<Vec<Value>>()
        ),
    );

    let refs: Vec<&str> = retry_raws.iter().map(|r| r.as_str()).collect();
    let (code, remark) = h.batch_ack(&group, &topic, &refs).await;
    ck.check(
        "L2 the retry-checkpoint BATCH_ACK_MESSAGE answered SUCCESS",
        code == response_code::SUCCESS,
        &format!("code={code} {remark}"),
    );

    // Durable broker state, read only AFTER the ack: the POP retry topic now has a route.
    let retry_topic = extra_info::build_pop_retry_topic_v1(&topic, &group);
    ck.check(
        "L2 the POP retry topic exists on the nameserver (the revive service really moved it there)",
        h.wait_route(&retry_topic, RETRY_ROUTE_BUDGET).await,
        &retry_topic,
    );

    let mut after = Eyes::default();
    h.watch(&group, &topic, SILENCE, &mut after).await;
    ck.check(
        "L2 the marker-driven retry ack silenced it (nothing revives again)",
        !after.came_back(),
        &after.detail(),
    );
}

// ------------------------------------------------------------------ L3

/// Two pop rounds of one queue cleared by one request: the rounds must stay two entries.
async fn leg_two_rounds_one_request(h: &Harness, stamp: &str, ck: &mut Checker) {
    let topic = format!("RbaRounds_{stamp}");
    let group = format!("CID_RbaRounds_{stamp}");
    let bodies: Vec<String> = (0..4).map(|i| format!("ba-rounds-{i}")).collect();

    println!("=== L3 two pop rounds of one queue, one batch request ({topic} / {group}) ===");
    if let Err(e) = h.make_topic(&topic, &bodies).await {
        ck.check("L3 sent 4 messages to a single-queue topic", false, &e);
        return;
    }
    ck.check("L3 sent 4 messages to a single-queue topic", true, "");

    let depth = h.wait_depth(&topic, 4).await;
    ck.check(
        "L3 all 4 messages are on the queue before popping",
        depth >= 4,
        &format!("depth={depth}"),
    );
    if depth < 4 {
        return;
    }

    let mut rounds: Vec<Vec<Ck>> = Vec::new();
    for round in 1..=2 {
        let popped = match h.pop(&group, &topic, 0, 2).await {
            Ok(popped) => popped,
            Err(e) => {
                ck.check(&format!("L3 POP round {round} answers 2 messages"), false, &e);
                return;
            }
        };
        let mut problems = Vec::new();
        let cks = popped_cks(&popped.msg_found_list, &mut problems);
        let ok = popped.status == PopStatus::Found
            && popped.msg_found_list.len() == 2
            && cks.len() == 2
            && problems.is_empty();
        ck.check(
            &format!("L3 POP round {round} answers 2 messages with parsable checkpoints"),
            ok,
            &format!(
                "{popped} problems={}",
                problems.join("; ")
            ),
        );
        if !ok {
            return;
        }
        rounds.push(cks);
    }

    let flat: Vec<&Ck> = rounds.iter().flat_map(|r| r.iter()).collect();
    let offsets: Vec<i64> = flat.iter().map(|c| c.queue_offset).collect();
    ck.check(
        "L3 the two rounds are disjoint batches of one queue",
        sorted_unique(
            &offsets
                .iter()
                .map(|o| o.to_string())
                .collect::<Vec<String>>(),
        )
        .len() == 4
            && flat.iter().all(|c| c.queue_id == 0),
        &format!("offsets={offsets:?}"),
    );

    let keys = sorted_unique(&flat.iter().map(|c| merge_key(c)).collect::<Vec<String>>());
    ck.check(
        "L3 the two rounds produce different merge keys, so the body cannot collapse them",
        keys.len() == 2,
        &format!("{keys:?}"),
    );

    let raws: Vec<String> = flat.iter().map(|c| c.raw.clone()).collect();
    let built = match build_body(ck, "L3", &group, &topic, &raws) {
        Ok(built) => built,
        Err(e) => {
            ck.abort("L3 the two-round batch body builds", &e);
            return;
        }
    };
    let covered: Vec<i64> = built.body.acks.iter().flat_map(acked_offsets).collect();
    ck.check(
        "L3 one body covers all 4 offsets exactly once, in two entries",
        built.body.acks.len() == 2 && same_int_set(&covered, &offsets),
        &format!(
            "entries={} each={:?}",
            built.body.acks.len(),
            built.body.acks.iter().map(acked_offsets).collect::<Vec<Vec<i64>>>()
        ),
    );

    let refs: Vec<&str> = raws.iter().map(|r| r.as_str()).collect();
    let (code, remark) = h.batch_ack(&group, &topic, &refs).await;
    ck.check(
        "L3 one BATCH_ACK_MESSAGE for two rounds answered SUCCESS",
        code == response_code::SUCCESS,
        &format!("code={code} {remark}"),
    );

    let mut eyes = Eyes::default();
    h.watch(&group, &topic, SILENCE, &mut eyes).await;
    ck.check(
        "L3 no message of either round revived inside the silence window",
        !eyes.came_back(),
        &eyes.detail(),
    );
}

// ------------------------------------------------------------------- main

async fn run(namesrv: &str) -> Checker {
    let run = stamp();
    println!("== rocketmq rust live BATCH_ACK_MESSAGE(200151) test ==");
    println!("   namesrv        = {namesrv}");
    println!("   stamp          = {run}");
    println!("   invisibleTime  = {INVISIBLE_MS} ms");
    println!("   silence window = {} s, revive tail = {} s", SILENCE.as_secs(), REVIVE_TAIL.as_secs());

    let client = RemotingClient::new();
    let mut ck = Checker::new();

    let (broker_name, broker_addr) = match cluster_broker(&client, namesrv).await {
        Ok(pair) => pair,
        Err(e) => {
            ck.abort("L0 broker bootstrap", &e);
            client.shutdown();
            return ck;
        }
    };
    println!("   broker         = {broker_name} @ {broker_addr}");

    let instance = MQClientInstance::new(
        &format!("rust-live-batch-ack-{run}@main"),
        vec![namesrv.to_string()],
    );
    if let Err(e) = instance.start().await {
        ck.abort("L0 instance start", &e.to_string());
        client.shutdown();
        return ck;
    }

    let producer_group = format!("PID_rust_batch_ack_{run}");
    let harness = Harness {
        client: RemotingClient::new(),
        instance,
        namesrv: namesrv.to_string(),
        broker_name,
        broker_addr,
        producer_group,
    };

    leg_one_request_whole_batch(&harness, &run, &mut ck).await;
    leg_one_checkpoint_left_out(&harness, &run, &mut ck).await;
    leg_two_rounds_one_request(&harness, &run, &mut ck).await;

    harness.instance.shutdown();
    harness.client.shutdown();
    client.shutdown();
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
