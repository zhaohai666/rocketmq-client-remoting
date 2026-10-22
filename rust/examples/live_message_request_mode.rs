//! Live proof of SET_MESSAGE_REQUEST_MODE(401) + GET_ALL_MESSAGE_REQUEST_MODE(402) against a real
//! Apache RocketMQ 5.5.1 broker, driven through the production admin method.
//!
//! Usage (cluster up: namesrv 9876 + broker 10911, `autoCreateTopicEnable=true`):
//! `cargo run --quiet --example live_message_request_mode -- 127.0.0.1:9876`
//!
//! Wire contract under test: a 401 carries **no custom header**. The broker decodes the body as ONE
//! JSON object `{"topic":..,"consumerGroup":..,"mode":..,"popShareQueueNum":..}` and reads all four
//! values out of it alone (`mode` is "POP"/"PULL"; `popShareQueueNum` is a JSON number, present even
//! at 0). The production `set_message_request_mode` builds exactly that body with an empty header.
//! Measured fact: a 401 whose values instead ride the header with an empty body leaves the broker
//! with nothing to decode -- it answers a system error whose remark reports the decoded body as null,
//! and the mode does NOT switch. The header form is *rejected*, not silently accepted.
//!
//! The only in-protocol way to see a switch is 402 (also no body/header); its RESPONSE body is
//! `{"messageRequestModeMap": {"<topic>": {"<group>": {topic,consumerGroup,mode,popShareQueueNum}}}}`.
//! A broker that never had a mode set answers with an empty body, read back here as an empty map.
//!
//! Three legs against ONE freshly created topic:
//!   * L1: the production POP write must land -- 402 has an entry keyed by exactly this topic+group,
//!     mode POP, popShareQueueNum the number 4.
//!   * L2: a second production write (PULL, 0) is what 402 then reports, proving the readback shows
//!     the latest write, not what L1 left.
//!   * L3 control: a RAW 401, four values in the header, no body, a second group. Its broker code is
//!     printed and required not-SUCCESS, and the control group required absent from the 402 map. If
//!     the header form also switched the mode, L1/L2 would prove nothing -- this is what makes them
//!     meaningful.
//!
//! Names are stamped with unix seconds so a stale entry can never satisfy an assertion. Reads are
//! polled to readiness, never slept on. Cleanup deletes the topic best effort (a failed delete is a
//! note, never a red run). Final line: `############ PASS=<n> FAIL=<n ############`; any failure exits
//! non-zero.

use std::env;
use std::future::Future;
use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::Value;

use rocketmq_client_remoting::client::admin::DefaultMQAdminExt;
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::remoting::client::RemotingClient;
use rocketmq_client_remoting::remoting::protocol::codes::{request_code, response_code};
use rocketmq_client_remoting::remoting::protocol::headers::{
    CreateTopicRequestHeader, GetRouteInfoRequestHeader,
};
use rocketmq_client_remoting::remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;
use rocketmq_client_remoting::remoting::protocol::serialize::RemotingSerializable;

/// Timeout for every raw RPC (route, create, 401 control, 402 readback).
const RPC_TIMEOUT_MS: i64 = 5_000;
/// Upper bound for the wait until the created topic's route is readable.
const ROUTE_BUDGET: Duration = Duration::from_secs(30);
/// Upper bound for the wait until a 402 readback shows the expected state.
const MODE_BUDGET: Duration = Duration::from_secs(15);
/// Pause between two polls of a readiness loop.
const POLL: Duration = Duration::from_millis(250);

// ------------------------------------------------------------------ helpers

fn stamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
        .to_string()
}

/// Accumulates assertions. Every leg runs to the end and the total is printed once, so one failed
/// check never hides the rest of the evidence.
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
}

/// Poll an async condition until it holds; on timeout return `false` so the caller prints the scene.
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
        tokio::time::sleep(POLL).await;
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
    // The nameserver's route is fastjson-style (numeric map keys): it must go through the
    // compatible decoder, which also handles ordinary string-keyed JSON.
    let value = RemotingSerializable::decode(&raw)
        .map_err(|e| format!("{what} body is not parsable: {e}"))?;
    TopicRouteData::from_json_value(&value).map_err(|e| format!("{what} route is invalid: {e}"))
}

/// Any broker of the cluster, found through the TBW102 default-topic route: the address to create
/// the fresh topic on.
async fn cluster_broker(client: &RemotingClient, namesrv: &str) -> Result<String, String> {
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
    route
        .broker_datas
        .first()
        .and_then(|b| b.select_broker_addr())
        .ok_or_else(|| "the default route has no usable broker address".to_string())
}

/// The MASTER broker address of a created topic, read off its own route: the broker 401 and 402 must
/// be sent to. Never a hardcoded port -- it is whatever the route reports for the topic's queue broker.
async fn topic_master_addr(
    client: &RemotingClient,
    namesrv: &str,
    topic: &str,
) -> Result<String, String> {
    let resp = route_response(client, namesrv, topic).await?;
    if resp.code != response_code::SUCCESS {
        return Err(format!(
            "route of {topic} not ready: code={} remark={:?}",
            resp.code, resp.remark
        ));
    }
    let route = decode_route(&resp, topic)?;
    let broker_name = route
        .queue_datas
        .first()
        .map(|q| q.broker_name.clone())
        .ok_or_else(|| "the topic route has no queueData".to_string())?;
    let data = route
        .broker_datas
        .iter()
        .find(|b| b.broker_name == broker_name)
        .ok_or_else(|| format!("no brokerData for {broker_name}"))?;
    let master_id = i64::from(MixAll::MASTER_ID);
    data.broker_addrs
        .iter()
        .find(|(id, _)| *id == master_id)
        .map(|(_, addr)| addr.clone())
        .or_else(|| data.select_broker_addr())
        .ok_or_else(|| format!("broker {broker_name} has no master address"))
}

/// Create a single-queue topic with a raw UPDATE_AND_CREATE_TOPIC and wait until the nameserver can
/// serve its route. The topic MUST exist before any 401 is sent: a broker refuses a 401 for a topic
/// it has no config for, and that refusal would otherwise be mistaken for a broken request shape.
async fn make_topic(
    client: &RemotingClient,
    namesrv: &str,
    broker_addr: &str,
    topic: &str,
) -> Result<(), String> {
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
    let resp = client
        .invoke_sync(broker_addr, &mut request, Some(RPC_TIMEOUT_MS))
        .await
        .map_err(|e| format!("create topic {topic} failed: {e}"))?;
    if resp.code != response_code::SUCCESS {
        return Err(format!(
            "create topic {topic} on {broker_addr} failed: code={} remark={:?}",
            resp.code, resp.remark
        ));
    }
    let present = wait_async(
        || async {
            match route_response(client, namesrv, topic).await {
                Ok(resp) if resp.code == response_code::SUCCESS => {
                    decode_route(&resp, topic).map(|r| !r.queue_datas.is_empty()).unwrap_or(false)
                }
                _ => false,
            }
        },
        ROUTE_BUDGET,
    )
    .await;
    if !present {
        return Err(format!("the route of {topic} never reached {namesrv}"));
    }
    Ok(())
}

// ------------------------------------------------------------------ the 402 readback

/// Send a raw 402 GET_ALL_MESSAGE_REQUEST_MODE (no body, no header) and return the broker's
/// `messageRequestModeMap`: `{ "<topic>": { "<group>": { mode, popShareQueueNum, .. } } }`. Anything
/// that is not a usable map -- a never-set broker (system error / empty body), a SUCCESS answer with
/// no map, even a transport error -- reads back empty, so an absent entry is the single signal to assert.
async fn read_mode_map(client: &RemotingClient, broker_addr: &str) -> Value {
    let mut request =
        RemotingCommand::create_request_command(request_code::GET_ALL_MESSAGE_REQUEST_MODE, None);
    let empty = Value::Object(serde_json::Map::new());
    let Ok(resp) = client.invoke_sync(broker_addr, &mut request, Some(RPC_TIMEOUT_MS)).await else {
        return empty;
    };
    if resp.code != response_code::SUCCESS {
        return empty;
    }
    let Some(raw) = resp.body().filter(|b| !b.is_empty()).map(|b| b.to_vec()) else {
        return empty;
    };
    match RemotingSerializable::decode(&raw)
        .ok()
        .and_then(|v| v.get("messageRequestModeMap").cloned())
    {
        Some(map @ Value::Object(_)) => map,
        _ => empty,
    }
}

/// The one 402 map entry keyed by exactly this topic and this group, if present.
fn entry<'a>(map: &'a Value, topic: &str, group: &str) -> Option<&'a Value> {
    map.get(topic)?.get(group)
}

// ------------------------------------------------------------------ L1

/// The production POP write must land in the table: an entry keyed by exactly this topic+group, with
/// mode POP and popShareQueueNum the number 4.
async fn leg_positive(h: &Harness, topic: &str, group: &str, ck: &mut Checker) {
    println!("=== L1 production POP write switches the mode ({topic} / {group}) ===");
    let write = h
        .admin
        .set_message_request_mode(&h.broker_addr, topic, group, "POP", 4)
        .await;
    ck.check(
        "L1 the production POP write was accepted by the broker (SUCCESS on 401)",
        write.is_ok(),
        &write.as_ref().err().map(|e| e.to_string()).unwrap_or_default(),
    );
    if write.is_err() {
        return;
    }

    let found = wait_async(
        || async { entry(&read_mode_map(&h.client, &h.broker_addr).await, topic, group).is_some() },
        MODE_BUDGET,
    )
    .await;
    let map = read_mode_map(&h.client, &h.broker_addr).await;
    let item = entry(&map, topic, group);
    ck.check(
        "L1 the 402 map has an entry keyed by exactly this topic and this group",
        found && item.is_some(),
        &format!("map={map}"),
    );
    if let Some(item) = item {
        ck.check(
            "L1 the entry mode is POP and its popShareQueueNum is the JSON number 4 (the JSON-body form really switched the mode)",
            item.get("mode").and_then(Value::as_str) == Some("POP")
                && item.get("popShareQueueNum").and_then(Value::as_i64) == Some(4),
            &format!("entry={item}"),
        );
    }
}

// ------------------------------------------------------------------ L2

/// The second write must replace the first: 402 then reports PULL with popShareQueueNum 0, proving
/// the readback shows the latest write rather than whatever L1 left behind.
async fn leg_replacement(h: &Harness, topic: &str, group: &str, ck: &mut Checker) {
    println!("=== L2 production PULL write replaces the POP entry ({topic} / {group}) ===");
    let write = h
        .admin
        .set_message_request_mode(&h.broker_addr, topic, group, "PULL", 0)
        .await;
    ck.check(
        "L2 the production PULL write was accepted by the broker (SUCCESS on 401)",
        write.is_ok(),
        &write.as_ref().err().map(|e| e.to_string()).unwrap_or_default(),
    );
    if write.is_err() {
        return;
    }

    // Wait for the PULL to become visible; on timeout the scene is printed by the assertions below.
    wait_async(
        || async {
            entry(&read_mode_map(&h.client, &h.broker_addr).await, topic, group)
                .and_then(|i| i.get("mode"))
                .and_then(Value::as_str)
                == Some("PULL")
        },
        MODE_BUDGET,
    )
    .await;

    let map = read_mode_map(&h.client, &h.broker_addr).await;
    let item = entry(&map, topic, group);
    ck.check(
        "L2 the same topic+group entry is still there after the replacement",
        item.is_some(),
        &format!("map={map}"),
    );
    if let Some(item) = item {
        ck.check(
            "L2 the entry mode is now PULL and its popShareQueueNum is the JSON number 0 (the second write, not L1's POP, is what 402 reports)",
            item.get("mode").and_then(Value::as_str) == Some("PULL")
                && item.get("popShareQueueNum").and_then(Value::as_i64) == Some(0),
            &format!("entry={item}"),
        );
    }
}

// ------------------------------------------------------------------ L3

/// The paired control: a RAW 401 carrying the four values in the ext-fields header and NO body. The
/// broker must reject it (code not SUCCESS) and the control group must stay absent from the 402 map.
/// Without this control, a switched mode in L1/L2 would prove nothing: it would be open that the
/// header form also switches, making the readback meaningless.
async fn leg_header_control(h: &Harness, topic: &str, ctrl_group: &str, ck: &mut Checker) {
    println!("=== L3 paired control: header-form 401 with no body ({topic} / {ctrl_group}) ===");

    let mut request =
        RemotingCommand::create_request_command(request_code::SET_MESSAGE_REQUEST_MODE, None);
    request.add_ext_field("topic", topic);
    request.add_ext_field("consumerGroup", ctrl_group);
    request.add_ext_field("mode", "POP");
    request.add_ext_field("popShareQueueNum", "4");
    // No body: the four values ride the header only.

    match h.client.invoke_sync(&h.broker_addr, &mut request, Some(RPC_TIMEOUT_MS)).await {
        Ok(resp) => {
            let code = resp.code;
            let remark = resp.remark.unwrap_or_default();
            println!("  [L3] header-form 401 answered code={code} remark={remark:?}");
            ck.check(
                "L3 the header-form control was NOT accepted (broker code is not SUCCESS)",
                code != response_code::SUCCESS,
                &format!("code={code} remark={remark:?}"),
            );
        }
        Err(e) => {
            ck.check("L3 the header-form control answered (no transport error)", false, &e.to_string());
            return;
        }
    }

    // "the control group is absent" only means something once the readback is live for this topic:
    // prove the original group's topic is still in the map, then prove the control group is not.
    let map = read_mode_map(&h.client, &h.broker_addr).await;
    ck.check(
        "L3 the 402 map still reports this topic (the readback itself is live)",
        map.get(topic).map(Value::is_object).unwrap_or(false),
        &format!("map={map}"),
    );
    ck.check(
        "L3 the header-form control left no entry for its group under this topic (the mode did NOT switch)",
        entry(&map, topic, ctrl_group).is_none(),
        &format!("control entry={:?}", entry(&map, topic, ctrl_group)),
    );
}

// ------------------------------------------------------------------ harness / main

/// The raw-RPC client (route, create, 401 control, 402 readback) and the production admin (the 401
/// writes under test and the best-effort cleanup), plus the master broker address the topic route
/// reported.
struct Harness {
    client: RemotingClient,
    admin: DefaultMQAdminExt,
    broker_addr: String,
}

async fn run(namesrv: &str) -> Checker {
    let run = stamp();
    let topic = format!("PopModeRustTopic_{run}");
    let group = format!("GID_PopModeRust_{run}");
    let ctrl_group = format!("GID_PopModeRustCtrl_{run}");

    println!("== rocketmq rust live SET_MESSAGE_REQUEST_MODE(401) / GET_ALL_MESSAGE_REQUEST_MODE(402) test ==");
    println!("   namesrv     = {namesrv}");
    println!("   stamp       = {run}");
    println!("   topic       = {topic}");
    println!("   group       = {group}");
    println!("   controlGroup= {ctrl_group}");

    let mut ck = Checker::new();
    let client = RemotingClient::new();

    let bootstrap_addr = match cluster_broker(&client, namesrv).await {
        Ok(addr) => addr,
        Err(e) => {
            ck.check("L0 broker bootstrap", false, &e);
            client.shutdown();
            return ck;
        }
    };
    println!("   bootstrap broker = {bootstrap_addr}");

    let admin = DefaultMQAdminExt::new();
    admin.set_namesrv_addr(namesrv);
    admin.set_instance_name(&format!("RS_MODE_{run}"));
    if let Err(e) = admin.start().await {
        ck.check("L0 admin start", false, &e.to_string());
        client.shutdown();
        return ck;
    }

    // The topic must exist before 401: a broker refuses a 401 for a topic it has no config for, and
    // the subscription group is left to the broker's autoCreate.
    if let Err(e) = make_topic(&client, namesrv, &bootstrap_addr, &topic).await {
        ck.check("L0 created the single-queue topic and its route is visible", false, &e);
        admin.shutdown();
        client.shutdown();
        return ck;
    }
    ck.check("L0 created the single-queue topic and its route is visible", true, "");

    // 401 / 402 are sent to the master broker the TOPIC route names -- never a hardcoded port.
    let broker_addr = match topic_master_addr(&client, namesrv, &topic).await {
        Ok(addr) => addr,
        Err(e) => {
            ck.check("L0 the topic route names a master broker", false, &e);
            admin.shutdown();
            client.shutdown();
            return ck;
        }
    };
    println!("   master broker (from topic route) = {broker_addr}");

    let harness = Harness { client, admin, broker_addr };

    leg_positive(&harness, &topic, &group, &mut ck).await;
    leg_replacement(&harness, &topic, &group, &mut ck).await;
    leg_header_control(&harness, &topic, &ctrl_group, &mut ck).await;

    // Best-effort cleanup: a failed delete is a note, never a red run.
    if let Err(e) = harness.admin.delete_topic(&topic, None).await {
        println!("  [NOTE] cleanup: deleting topic {topic} failed ({e}); leaving it for the broker to expire");
    } else {
        println!("  [NOTE] cleanup: deleted topic {topic}");
    }

    harness.admin.shutdown();
    harness.client.shutdown();
    ck
}

fn report(ck: &mut Checker) {
    println!("############ PASS={} FAIL={} ############", ck.passed, ck.failed.len());
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
