//! Live protocol-layer smoke test: connect directly to a local NameServer + Broker and
//! verify the remoting frame codec plus the round-trip of 7 core request codes on a
//! **real broker**.
//!
//! Usage (the first argument is the namesrv address, default `127.0.0.1:9876`):
//!
//! ```text
//! cargo run --example live_protocol -- 127.0.0.1:9876
//! ```
//!
//! All assertions accumulate through [`Checker::check`]; any failure makes the process
//! exit with a non-zero code.
//!
//! ## Covered scenarios
//!
//! * S0 `GET_ROUTEINFO_BY_TOPIC(TBW102)` + `UPDATE_AND_CREATE_TOPIC(17)` -- bootstrap
//! * S1 `GET_ROUTEINFO_BY_TOPIC(105)` -- namesrv route query + `TopicRouteData` decoding
//! * S2 `GET_BROKER_CLUSTER_INFO(106)` -- cluster info JSON body decoding
//! * S3 `SEND_MESSAGE_V2(310)` -- `SendMessageRequestHeaderV2` single-letter short keys a..n
//! * S4 `PULL_MESSAGE(11)` -- `PullSysFlag` + 17-section store format decode round-trip
//! * S5 `GET_MIN/MAX_OFFSET(31/30)` + `UPDATE/QUERY_CONSUMER_OFFSET(15/14)`
//! * S6 `HEART_BEAT(34)` + `GET_CONSUMER_LIST_BY_GROUP(38)` + `UNREGISTER_CLIENT(35)`
//! * S7 RocketMQ binary header (`serializeType=ROCKETMQ`) round-trip on a real broker
//! * S8 `DELETE_TOPIC_IN_BROKER(215)` -- cleanup, so the same cluster does not accumulate test topics
//!
//! ## Preconditions (the only dependency; nothing needs to be downloaded)
//!
//! The NameServer must already have a route for the default topic `TBW102`, i.e. the broker runs with
//! `autoCreateTopicEnable=true` (already enabled in this repo's `broker.conf`) -- S0 uses it to locate the broker.
//! The test topic itself is created explicitly by S0: 5.x NameServer no longer falls back to
//! TBW102 for unknown topics; querying a new topic directly returns `TOPIC_NOT_EXIST(17)`.
//!
//! The run log may contain one line
//! `no processor for request code 40 ... from <broker>`: that is the broker's reverse push of
//! `NOTIFY_CONSUMER_IDS_CHANGED` on membership change; this example only verifies protocol round-trips and registers no push handler,
//! so it is expected noise (real broker->client pushes are covered in the push consumer example).

use std::env;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use rocketmq_client_remoting::common::message::Message;
use rocketmq_client_remoting::common::message_client_id_setter;
use rocketmq_client_remoting::common::message_const as message_const;
use rocketmq_client_remoting::common::message_decoder;
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::sysflag::PullSysFlag;
use rocketmq_client_remoting::common::topic_config::{self, TopicFilterType};
use rocketmq_client_remoting::common::util_all;
use rocketmq_client_remoting::remoting::client::RemotingClient;
use rocketmq_client_remoting::remoting::protocol::body::GetConsumerListByGroupResponseBody;
use rocketmq_client_remoting::remoting::protocol::codes::{request_code, response_code, serialize_type};
use rocketmq_client_remoting::remoting::protocol::heartbeat::{
    ConsumeFromWhere, ConsumeType, ConsumerData, ExpressionType, HeartbeatData, MessageModel,
    ProducerData, SubscriptionData,
};
use rocketmq_client_remoting::remoting::protocol::headers::{
    CreateTopicRequestHeader, DeleteTopicRequestHeader, GetConsumerListByGroupRequestHeader,
    GetMaxOffsetRequestHeader, GetMinOffsetRequestHeader, GetRouteInfoRequestHeader,
    HeartbeatRequestHeader, PullMessageRequestHeader, PullMessageResponseHeader,
    QueryConsumerOffsetRequestHeader, QueryConsumerOffsetResponseHeader, SendMessageRequestHeaderV2,
    SendMessageResponseHeader, UnregisterClientRequestHeader, UpdateConsumerOffsetRequestHeader,
};
use rocketmq_client_remoting::remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;
use rocketmq_client_remoting::remoting::protocol::serialize::{
    header_length_of, protocol_type_of, RemotingSerializable,
};

/// Number of queues for the topic created in this run (default 4).
const QUEUE_NUMS: i32 = 4;

/// Timestamp log prefix for one run.
///
/// topic / group carry a timestamp suffix, so multiple runs on the same cluster do not read each
/// other's previous messages -- the broker's `storePathRootDir` is persistent and accumulates history unless reset.
fn stamp() -> String {
    let secs = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => 0,
    };
    format!("{secs}")
}

/// Assertion accumulator: one run finishes every scenario and reports together, instead of exiting on the first failure.
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

    /// Also records a failure when a scenario cannot complete (cannot connect, wrong response code).
    fn abort(&mut self, name: &str, err: &str) {
        println!("  [FAIL] {name}: {err}");
        self.failed.push(format!("{name}: {err}"));
    }
}

/// Scenario return value: the `String` is a human-readable failure reason.
type Live = Result<(), String>;

async fn rpc(
    client: &RemotingClient,
    addr: &str,
    mut cmd: RemotingCommand,
    timeout_millis: i64,
) -> Result<RemotingCommand, String> {
    client
        .invoke_sync(addr, &mut cmd, Some(timeout_millis))
        .await
        .map_err(|e| format!("invoke {addr} failed: {e}"))
}

/// Takes the response body (most body-class requests must have a body).
fn body_of(resp: &RemotingCommand, what: &str) -> Result<Vec<u8>, String> {
    match resp.body() {
        Some(b) => Ok(b.to_vec()),
        None => Err(format!("{what} response has empty body")),
    }
}

/// Reads the `(totalLength, headerLengthAndProtocolMark)` of an encoded frame header.
fn frame_prefix(wire: &[u8]) -> Option<(i32, i32)> {
    let head = wire.get(0..8)?;
    let total = i32::from_be_bytes(head[0..4].try_into().ok()?);
    let marked = i32::from_be_bytes(head[4..8].try_into().ok()?);
    Some((total, marked))
}

/// Raw round-trip of `GET_ROUTEINFO_BY_TOPIC(105)`, reused by S0/S1/S7.
async fn get_route(
    client: &RemotingClient,
    namesrv: &str,
    topic: &str,
) -> Result<RemotingCommand, String> {
    let req = RemotingCommand::create_request_command(
        request_code::GET_ROUTEINFO_BY_TOPIC,
        Some(Box::new(GetRouteInfoRequestHeader {
            topic: Some(topic.to_string()),
            // Leave this field None (None does not enter extFields).
            accept_standard_json_only: None,
        })),
    );
    rpc(client, namesrv, req, 5000).await
}

/// Picks the first broker's (brokerName, masterAddr) from the route.
fn first_broker(route: &TopicRouteData) -> Option<(String, String)> {
    let bd = route.broker_datas.first()?;
    let addr = bd.select_broker_addr()?;
    Some((bd.broker_name.clone(), addr))
}

/// Decodes `TopicRouteData`.
fn decode_route(resp: &RemotingCommand, what: &str) -> Result<TopicRouteData, String> {
    let raw = body_of(resp, what)?;
    TopicRouteData::decode(&raw).map_err(|e| format!("decode route failed: {e}"))
}

/// S0: use the default topic's (TBW102) route to locate the broker, then explicitly create the topic for this run.
///
/// WARNING: RocketMQ 5.x NameServer **no longer** falls back to TBW102 for unknown topics -- querying
/// a new topic directly returns `TOPIC_NOT_EXIST(17)`, which is where the console's
/// "No route info of this topic" comes from. The local bootstrap takes the same path: first query
/// the route of `DEFAULT_TOPIC`, then send `UPDATE_AND_CREATE_TOPIC(17)` to each broker.
/// The broker side `AdminBrokerProcessor.createTopic` synchronously
/// calls `registerIncrementBrokerData` to push the new topic to all nameservers, so S1 needs no wait.
async fn s0_bootstrap(
    client: &RemotingClient,
    namesrv: &str,
    topic: &str,
    ck: &mut Checker,
) -> Result<(), String> {
    let resp = get_route(client, namesrv, MixAll::DEFAULT_TOPIC).await?;
    ck.check(
        "S0 default topic (TBW102) route SUCCESS",
        resp.code == response_code::SUCCESS,
        &format!("code={} remark={:?}", resp.code, resp.remark),
    );
    if resp.code != response_code::SUCCESS {
        return Err(format!(
            "no route of default topic {}: code={} remark={:?}",
            MixAll::DEFAULT_TOPIC,
            resp.code,
            resp.remark
        ));
    }
    let route = decode_route(&resp, "TBW102")?;
    let (broker_name, broker_addr) =
        first_broker(&route).ok_or_else(|| "default route has no usable broker address".to_string())?;
    println!("        default topic broker = {broker_name} @ {broker_addr}");

    let req = RemotingCommand::create_request_command(
        request_code::UPDATE_AND_CREATE_TOPIC,
        Some(Box::new(CreateTopicRequestHeader {
            topic: Some(topic.to_string()),
            default_topic: Some(MixAll::DEFAULT_TOPIC.to_string()),
            read_queue_nums: Some(QUEUE_NUMS),
            write_queue_nums: Some(QUEUE_NUMS),
            perm: Some(topic_config::DEFAULT_PERM),
            // The broker's CreateTopicRequestHeader.checkFields() converts it to an enum;
            // if empty it directly replies "topicFilterType = [null] value invalid".
            topic_filter_type: Some(TopicFilterType::SINGLE_TAG.to_string()),
            topic_sys_flag: Some(0),
            order: Some(false),
            attributes: Some(String::new()),
            force: Some(false),
        })),
    );
    let resp = rpc(client, &broker_addr, req, 5000).await?;
    ck.check(
        "S0 UPDATE_AND_CREATE_TOPIC SUCCESS",
        resp.code == response_code::SUCCESS,
        &format!("code={} remark={:?}", resp.code, resp.remark),
    );
    if resp.code != response_code::SUCCESS {
        return Err(format!(
            "create topic {topic} on {broker_addr} failed: code={} remark={:?}",
            resp.code, resp.remark
        ));
    }
    Ok(())
}

/// S1: query the real topic's route and produce the broker address the later scenarios need.
async fn s1_route(
    client: &RemotingClient,
    namesrv: &str,
    topic: &str,
    ck: &mut Checker,
) -> Result<(String, String), String> {
    let resp = get_route(client, namesrv, topic).await?;
    ck.check(
        "S1 route responseCode == SUCCESS",
        resp.code == response_code::SUCCESS,
        &format!("code={} remark={:?} opaque={}", resp.code, resp.remark, resp.opaque),
    );
    if resp.code != response_code::SUCCESS {
        return Err(format!(
            "no route info of topic {topic}: code={} remark={:?}",
            resp.code, resp.remark
        ));
    }
    let route = decode_route(&resp, "GET_ROUTEINFO_BY_TOPIC")?;

    ck.check(
        "S1 queueDatas not empty",
        !route.queue_datas.is_empty(),
        "queueDatas is empty",
    );
    ck.check(
        "S1 brokerDatas not empty",
        !route.broker_datas.is_empty(),
        "brokerDatas is empty",
    );
    let qd = match route.queue_datas.first() {
        Some(q) => q,
        None => return Err("queueDatas empty, cannot continue".to_string()),
    };
    println!(
        "        queueData: brokerName={} read={} write={} perm={} sysFlag={}",
        qd.broker_name, qd.read_queue_nums, qd.write_queue_nums, qd.perm, qd.topic_sys_flag
    );
    // The read/write queue numbers and perm sent when creating the topic must be read back as-is,
    // otherwise S0's admin write did not land on this broker.
    ck.check(
        "S1 route reflects the created topic config",
        qd.read_queue_nums == QUEUE_NUMS
            && qd.write_queue_nums == QUEUE_NUMS
            && qd.perm == topic_config::DEFAULT_PERM,
        &format!(
            "read={} write={} perm={} want {} / {} / {}",
            qd.read_queue_nums,
            qd.write_queue_nums,
            qd.perm,
            QUEUE_NUMS,
            QUEUE_NUMS,
            topic_config::DEFAULT_PERM
        ),
    );
    let bd = match route.broker_datas.first() {
        Some(b) => b,
        None => return Err("brokerDatas empty, cannot continue".to_string()),
    };
    let addr = match bd.select_broker_addr() {
        Some(a) => a,
        None => return Err(format!("broker {} has no address", bd.broker_name)),
    };
    println!("        brokerData: cluster={} brokerName={} addr={}", bd.cluster, bd.broker_name, addr);
    ck.check(
        "S1 broker name and addr not blank",
        util_all::is_not_blank_str(&bd.broker_name) && util_all::is_not_blank_str(&addr),
        &format!("brokerName={:?} addr={:?}", bd.broker_name, addr),
    );
    // At least one writable queue must be selectable from the route, otherwise the producer has nothing to deliver to.
    let mqs = route.get_all_message_queue(topic);
    ck.check(
        "S1 writable message queues not empty",
        !mqs.is_empty(),
        "get_all_message_queue returned no writable queue",
    );
    Ok((bd.broker_name.clone(), addr))
}

/// S2: `GET_BROKER_CLUSTER_INFO(106)` -- the `ClusterInfo` JSON returned by namesrv.
async fn s2_cluster_info(client: &RemotingClient, namesrv: &str, ck: &mut Checker) -> Live {
    let resp = rpc(
        client,
        namesrv,
        RemotingCommand::create_request_command(request_code::GET_BROKER_CLUSTER_INFO, None),
        5000,
    )
    .await?;
    ck.check(
        "S2 clusterInfo responseCode == SUCCESS",
        resp.code == response_code::SUCCESS,
        &format!("code={} remark={:?}", resp.code, resp.remark),
    );
    if resp.code != response_code::SUCCESS {
        return Ok(());
    }
    let raw = body_of(&resp, "GET_BROKER_CLUSTER_INFO")?;
    // WARNING: the real machine returns fastjson-style **non-string map keys**:
    // `"brokerAddrs":{0:"127.0.0.1:10911"}` -- `serde_json` would directly report
    // "key must be a string", so the fastjson2-compatible decoder in the crate must be used.
    let value: Value = RemotingSerializable::decode(&raw)
        .map_err(|e| format!("clusterInfo body is not parsable: {e}"))?;
    let table = value.get("brokerAddrTable").and_then(Value::as_object);
    match table {
        Some(t) => {
            ck.check("S2 brokerAddrTable not empty", !t.is_empty(), "brokerAddrTable is empty");
            for (name, bd) in t {
                let cluster = bd
                    .get("cluster")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let addrs = bd.get("brokerAddrs").cloned().unwrap_or(Value::Null);
                println!("        broker: name={name} cluster={cluster} addrs={addrs}");
                ck.check(
                    "S2 broker has cluster + brokerAddrs",
                    !cluster.is_empty() && matches!(addrs, Value::Object(_)),
                    &format!("name={name} cluster={cluster:?} addrs={addrs}"),
                );
            }
        }
        None => {
            ck.check("S2 brokerAddrTable exists", false, &format!("body={value}"));
        }
    }
    Ok(())
}

/// Result of one send, for S4/S5 to compare against.
struct Sent {
    uniq_id: String,
    msg_id: String,
    queue_offset: i64,
    body: Vec<u8>,
}

/// S3: `SEND_MESSAGE_V2(310)` -- hand-assembled V2 short-key header + raw body.
async fn s3_send(
    client: &RemotingClient,
    broker_addr: &str,
    broker_name: &str,
    topic: &str,
    producer_group: &str,
    ck: &mut Checker,
) -> Result<Sent, String> {
    let body = format!("rust-live-protocol-{topic}").into_bytes();
    let mut msg = Message::new(topic, Some(&body));
    msg.set_tags("TagRustLive");
    msg.set_keys(&format!("{topic}-key"));
    message_client_id_setter::set_uniq_id(&mut msg);
    msg.put_property(message_const::PROPERTY_WAIT_STORE_MSG_OK, "true");
    let uniq_id = match message_client_id_setter::get_uniq_id(&msg) {
        Some(id) => id,
        None => return Err("set_uniq_id did not write UNIQ_KEY".to_string()),
    };

    let header = SendMessageRequestHeaderV2 {
        producer_group: Some(producer_group.to_string()),
        topic: Some(msg.get_topic().to_string()),
        default_topic: Some(MixAll::DEFAULT_TOPIC.to_string()),
        default_topic_queue_nums: Some(MixAll::DEFAULT_TOPIC_QUEUE_NUMS),
        queue_id: Some(0),
        // Uncompressed, non-transactional, IPv4 bornHost -> sysFlag all 0.
        sys_flag: Some(0),
        born_timestamp: Some(util_all::current_time_millis()),
        flag: Some(msg.get_flag()),
        properties: Some(message_decoder::message_properties_2_string(msg.get_properties())),
        reconsume_times: Some(0),
        unit_mode: Some(false),
        max_reconsume_times: Some(0),
        batch: Some(false),
        broker_name: Some(broker_name.to_string()),
    };
    let mut req = RemotingCommand::create_request_command(
        request_code::SEND_MESSAGE_V2,
        Some(Box::new(header)),
    );
    req.set_body(Some(msg.get_body().to_vec()));

    let resp = rpc(client, broker_addr, req, 10_000).await?;
    ck.check(
        "S3 send responseCode == SUCCESS",
        resp.code == response_code::SUCCESS,
        &format!("code={} remark={:?}", resp.code, resp.remark),
    );
    if resp.code != response_code::SUCCESS {
        return Err(format!(
            "send failed: code={} remark={:?}",
            resp.code, resp.remark
        ));
    }
    let resp_header: SendMessageResponseHeader = resp
        .decode_command_custom_header()
        .map_err(|e| format!("decode SendMessageResponseHeader failed: {e}"))?;
    let wire_msg_id = resp_header.msg_id.clone().unwrap_or_default();
    let queue_offset = resp_header.queue_offset.unwrap_or(-1);
    let queue_id = resp_header.queue_id.unwrap_or(-1);
    println!(
        "        sendResult: msgId={wire_msg_id} uniqId={uniq_id} queueId={queue_id} offset={queue_offset}"
    );
    // The msgId the broker returns is the store-side 16-byte offset form (32 hex chars),
    // an independent identifier from the client-side UNIQ_KEY (UUID form, with hyphens).
    ck.check(
        "S3 wire msgId is 32 hex chars",
        wire_msg_id.len() == 32 && wire_msg_id.chars().all(|c| c.is_ascii_hexdigit()),
        &format!("msgId={wire_msg_id:?}"),
    );
    ck.check(
        "S3 queueId == 0 and queueOffset >= 0",
        queue_id == 0 && queue_offset >= 0,
        &format!("queueId={queue_id} queueOffset={queue_offset}"),
    );
    ck.check(
        "S3 UNIQ_KEY is 32 uppercase hex chars",
        uniq_id.len() == 32
            && uniq_id.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase()),
        &format!("uniqId={uniq_id:?}"),
    );
    // store msgId and UNIQ_KEY are two independent identifiers; the broker only returns the former.
    ck.check(
        "S3 store msgId differs from UNIQ_KEY",
        wire_msg_id != uniq_id,
        &format!("msgId={wire_msg_id} uniqId={uniq_id}"),
    );
    Ok(Sent { uniq_id, msg_id: wire_msg_id, queue_offset, body })
}

/// S4: `PULL_MESSAGE(11)` -- verify the 17-section store format can decode back the message just sent.
async fn s4_pull(
    client: &RemotingClient,
    broker_addr: &str,
    topic: &str,
    consumer_group: &str,
    sent: &Sent,
    ck: &mut Checker,
) -> Live {
    // The suspend bit must be set: commitLog->consumeQueue dispatch is asynchronous, and without a
    // long-poll window a just-sent message would come back as PULL_NOT_FOUND(19).
    let sys_flag = PullSysFlag::build_sys_flag_basic(false, true, true, false);
    let req = RemotingCommand::create_request_command(
        request_code::PULL_MESSAGE,
        Some(Box::new(PullMessageRequestHeader {
            consumer_group: Some(consumer_group.to_string()),
            topic: Some(topic.to_string()),
            lite_topic: None,
            queue_id: Some(0),
            queue_offset: Some(sent.queue_offset),
            max_msg_nums: Some(32),
            sys_flag: Some(sys_flag),
            commit_offset: Some(0),
            suspend_timeout_millis: Some(15_000),
            // When FLAG_SUBSCRIPTION is set the broker builds subscription data on the fly via
            // `FilterAPI.build(topic, subscription, expressionType)`,
            // so the raw expression (`*`) is passed here instead of subscription JSON.
            subscription: Some("*".to_string()),
            sub_version: Some(0),
            expression_type: Some(ExpressionType::TAG.to_string()),
            max_msg_bytes: Some(-1),
            request_source: Some(0),
            proxy_froward_client_id: None,
        })),
    );
    let resp = rpc(client, broker_addr, req, 30_000).await?;
    ck.check(
        "S4 pull responseCode == SUCCESS",
        resp.code == response_code::SUCCESS,
        &format!("code={} remark={:?} (19=PULL_NOT_FOUND)", resp.code, resp.remark),
    );
    if resp.code != response_code::SUCCESS {
        return Ok(());
    }
    let resp_header: PullMessageResponseHeader = resp
        .decode_command_custom_header()
        .map_err(|e| format!("decode PullMessageResponseHeader failed: {e}"))?;
    let raw = body_of(&resp, "PULL_MESSAGE")?;
    let found = message_decoder::decode_messages(&raw);
    println!(
        "        pullResult: status=FOUND count={} min={} max={} next={}",
        found.len(),
        resp_header.min_offset.unwrap_or(-1),
        resp_header.max_offset.unwrap_or(-1),
        resp_header.next_begin_offset.unwrap_or(-1)
    );
    ck.check("S4 pulled at least 1 message", !found.is_empty(), "body decoded 0 message");
    let first = match found.first() {
        Some(m) => m,
        None => return Ok(()),
    };
    ck.check(
        "S4 body round-trips byte for byte",
        first.get_body() == sent.body.as_slice(),
        &format!(
            "sent={} got={}",
            util_all::bytes_2_string(&sent.body),
            util_all::bytes_2_string(first.get_body())
        ),
    );
    ck.check(
        "S4 UNIQ_KEY matches the sent message",
        first.get_property(message_const::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            == Some(sent.uniq_id.as_str()),
        &format!(
            "sent={} got={:?}",
            sent.uniq_id,
            first.get_property(message_const::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        ),
    );
    ck.check(
        "S4 TAGS/KEYS survive the store round-trip",
        first.get_tags() == Some("TagRustLive") && first.get_keys().is_some(),
        &format!("tags={:?} keys={:?}", first.get_tags(), first.get_keys()),
    );
    ck.check(
        "S4 topic + queueId + queueOffset echo back",
        first.get_topic() == topic
            && first.queue_id == 0
            && first.queue_offset == sent.queue_offset,
        &format!(
            "topic={} queueId={} offset={} (want {})",
            first.get_topic(),
            first.queue_id,
            first.queue_offset,
            sent.queue_offset
        ),
    );
    // The msgId echoed back by the broker must also be the store-side one (same commitlog offset).
    ck.check(
        "S4 store msgId equals SendResult.msgId",
        first.msg_id.as_deref() == Some(sent.msg_id.as_str()),
        &format!("pull={:?} send={}", first.msg_id, sent.msg_id),
    );
    let min_offset = resp_header.min_offset.unwrap_or(-1);
    let max_offset = resp_header.max_offset.unwrap_or(-1);
    let next = resp_header.next_begin_offset.unwrap_or(-1);
    ck.check(
        "S4 offsets are consistent",
        min_offset >= 0 && max_offset >= min_offset && next > sent.queue_offset,
        &format!("min={min_offset} max={max_offset} next={next} pulled_at={}", sent.queue_offset),
    );
    Ok(())
}

/// S5: read/write round-trip of min/max offset and the consumer offset.
async fn s5_offsets(
    client: &RemotingClient,
    broker_addr: &str,
    topic: &str,
    consumer_group: &str,
    sent: &Sent,
    ck: &mut Checker,
) -> Live {
    let offset_of = |resp: &RemotingCommand| -> i64 {
        resp.ext_fields().get("offset").and_then(|v| v.parse::<i64>().ok()).unwrap_or(-1)
    };

    let min = rpc(
        client,
        broker_addr,
        RemotingCommand::create_request_command(
            request_code::GET_MIN_OFFSET,
            Some(Box::new(GetMinOffsetRequestHeader {
                topic: Some(topic.to_string()),
                queue_id: Some(0),
            })),
        ),
        5000,
    )
    .await?;
    ck.check(
        "S5 GET_MIN_OFFSET SUCCESS",
        min.code == response_code::SUCCESS,
        &format!("code={} remark={:?}", min.code, min.remark),
    );
    let min_offset = offset_of(&min);

    let max = rpc(
        client,
        broker_addr,
        RemotingCommand::create_request_command(
            request_code::GET_MAX_OFFSET,
            Some(Box::new(GetMaxOffsetRequestHeader {
                topic: Some(topic.to_string()),
                queue_id: Some(0),
            })),
        ),
        5000,
    )
    .await?;
    ck.check(
        "S5 GET_MAX_OFFSET SUCCESS",
        max.code == response_code::SUCCESS,
        &format!("code={} remark={:?}", max.code, max.remark),
    );
    let max_offset = offset_of(&max);
    println!("        queue 0: min={min_offset} max={max_offset} sent_at={}", sent.queue_offset);
    ck.check(
        "S5 maxOffset == sentOffset + 1",
        max_offset == sent.queue_offset + 1,
        &format!("max={max_offset} sent={}", sent.queue_offset),
    );
    ck.check(
        "S5 minOffset <= sentOffset < maxOffset",
        min_offset >= 0 && min_offset <= sent.queue_offset && sent.queue_offset < max_offset,
        &format!("min={min_offset} sent={} max={max_offset}", sent.queue_offset),
    );

    let update = rpc(
        client,
        broker_addr,
        RemotingCommand::create_request_command(
            request_code::UPDATE_CONSUMER_OFFSET,
            Some(Box::new(UpdateConsumerOffsetRequestHeader {
                consumer_group: Some(consumer_group.to_string()),
                topic: Some(topic.to_string()),
                queue_id: Some(0),
                commit_offset: Some(max_offset),
            })),
        ),
        5000,
    )
    .await?;
    ck.check(
        "S5 UPDATE_CONSUMER_OFFSET SUCCESS",
        update.code == response_code::SUCCESS,
        &format!("code={} remark={:?}", update.code, update.remark),
    );

    let query = rpc(
        client,
        broker_addr,
        RemotingCommand::create_request_command(
            request_code::QUERY_CONSUMER_OFFSET,
            Some(Box::new(QueryConsumerOffsetRequestHeader {
                consumer_group: Some(consumer_group.to_string()),
                topic: Some(topic.to_string()),
                queue_id: Some(0),
                set_zero_if_not_found: None,
            })),
        ),
        5000,
    )
    .await?;
    let got = offset_of(&query);
    ck.check(
        "S5 QUERY_CONSUMER_OFFSET returns the committed offset",
        query.code == response_code::SUCCESS && got == max_offset,
        &format!("code={} offset={} want={}", query.code, got, max_offset),
    );
    let resp_header: QueryConsumerOffsetResponseHeader = query
        .decode_command_custom_header()
        .map_err(|e| format!("decode QueryConsumerOffsetResponseHeader failed: {e}"))?;
    ck.check(
        "S5 response header parses through from_ext_fields",
        resp_header.offset == Some(got),
        &format!("header={:?} extField={got}", resp_header.offset),
    );

    let missing = rpc(
        client,
        broker_addr,
        RemotingCommand::create_request_command(
            request_code::QUERY_CONSUMER_OFFSET,
            Some(Box::new(QueryConsumerOffsetRequestHeader {
                consumer_group: Some(format!("{consumer_group}_never")),
                topic: Some(topic.to_string()),
                queue_id: Some(0),
                set_zero_if_not_found: Some(false),
            })),
        ),
        5000,
    )
    .await?;
    ck.check(
        "S5 never-committed group => QUERY_NOT_FOUND or 0",
        missing.code == response_code::QUERY_NOT_FOUND || offset_of(&missing) == 0,
        &format!("code={} offset={}", missing.code, offset_of(&missing)),
    );
    Ok(())
}

/// S6: heartbeat registration -> query online members by group -> unregister.
async fn s6_heartbeat(
    client: &RemotingClient,
    broker_addr: &str,
    topic: &str,
    client_id: &str,
    producer_group: &str,
    consumer_group: &str,
    ck: &mut Checker,
) -> Live {
    let mut hb = HeartbeatData::new(client_id);
    hb.add_producer_data(ProducerData::new(producer_group));
    let mut cd = ConsumerData::new(
        consumer_group,
        ConsumeType::CONSUME_ACTIVELY,
        MessageModel::CLUSTERING,
        ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET,
    );
    cd.add_subscription_data(SubscriptionData::new(topic, "*"));
    hb.add_consumer_data(cd);
    let mut req = RemotingCommand::create_request_command(
        request_code::HEART_BEAT,
        Some(Box::new(HeartbeatRequestHeader { client_id: Some(client_id.to_string()) })),
    );
    req.set_body(Some(hb.encode()));
    let resp = rpc(client, broker_addr, req, 5000).await?;
    ck.check(
        "S6 HEART_BEAT SUCCESS",
        resp.code == response_code::SUCCESS,
        &format!("code={} remark={:?}", resp.code, resp.remark),
    );

    let list_ids = |resp: &RemotingCommand| -> Result<Vec<String>, String> {
        if resp.code != response_code::SUCCESS {
            return Ok(Vec::new());
        }
        let raw = body_of(resp, "GET_CONSUMER_LIST_BY_GROUP")?;
        let body = GetConsumerListByGroupResponseBody::decode(&raw)
            .map_err(|e| format!("decode consumer list failed: {e}"))?;
        Ok(body.consumer_id_list)
    };

    let online = rpc(
        client,
        broker_addr,
        RemotingCommand::create_request_command(
            request_code::GET_CONSUMER_LIST_BY_GROUP,
            Some(Box::new(GetConsumerListByGroupRequestHeader {
                consumer_group: Some(consumer_group.to_string()),
            })),
        ),
        5000,
    )
    .await?;
    let ids = list_ids(&online)?;
    println!("        online consumers of {consumer_group}: {ids:?}");
    ck.check(
        "S6 heartbeat registered this client",
        ids.iter().any(|id| id == client_id),
        &format!("clientID={client_id} not in {ids:?} (code={})", online.code),
    );

    let unregister = rpc(
        client,
        broker_addr,
        RemotingCommand::create_request_command(
            request_code::UNREGISTER_CLIENT,
            Some(Box::new(UnregisterClientRequestHeader {
                client_id: Some(client_id.to_string()),
                producer_group: Some(producer_group.to_string()),
                consumer_group: Some(consumer_group.to_string()),
            })),
        ),
        5000,
    )
    .await?;
    ck.check(
        "S6 UNREGISTER_CLIENT SUCCESS",
        unregister.code == response_code::SUCCESS,
        &format!("code={} remark={:?}", unregister.code, unregister.remark),
    );

    let after = rpc(
        client,
        broker_addr,
        RemotingCommand::create_request_command(
            request_code::GET_CONSUMER_LIST_BY_GROUP,
            Some(Box::new(GetConsumerListByGroupRequestHeader {
                consumer_group: Some(consumer_group.to_string()),
            })),
        ),
        5000,
    )
    .await?;
    let ids = list_ids(&after)?;
    // After unregistering, either the group is empty (broker returns CONSUMER_NOT_ONLINE=206) or you are no longer in the list.
    ck.check(
        "S6 client gone after unregister",
        !ids.iter().any(|id| id == client_id),
        &format!("code={} ids={ids:?}", after.code),
    );
    Ok(())
}

/// S7: RocketMQ binary header (`serializeType=ROCKETMQ`) against the real namesrv.
async fn s7_rocketmq_serialize(client: &RemotingClient, namesrv: &str, topic: &str, ck: &mut Checker) -> Live {
    let mut req = RemotingCommand::create_request_command(
        request_code::GET_ROUTEINFO_BY_TOPIC,
        Some(Box::new(GetRouteInfoRequestHeader {
            topic: Some(topic.to_string()),
            accept_standard_json_only: Some(false),
        })),
    );
    req.serialize_type_current_rpc = serialize_type::ROCKETMQ;

    let wire = req.encode();
    let (total_len, marked) = frame_prefix(&wire).ok_or_else(|| "frame shorter than 8 bytes".to_string())?;
    let header_len = header_length_of(marked);
    ck.check(
        "S7 frame carries the RMQ protocol mark",
        protocol_type_of(marked) == serialize_type::ROCKETMQ
            && total_len as usize == wire.len() - 4
            && header_len as usize == wire.len() - 8,
        &format!(
            "protocolType={} totalLen={total_len} headerLen={header_len} frameLen={}",
            protocol_type_of(marked),
            wire.len()
        ),
    );
    let parsed = RemotingCommand::decode(&wire)
        .map_err(|e| format!("decode our own binary-header frame failed: {e}"))?;
    ck.check(
        "S7 binary header round-trips through our codec",
        parsed.serialize_type_current_rpc == serialize_type::ROCKETMQ
            && parsed.code == request_code::GET_ROUTEINFO_BY_TOPIC
            && parsed.opaque == req.opaque
            && parsed.ext_fields().get("topic") == Some(topic)
            && parsed.ext_fields().get("acceptStandardJsonOnly") == Some("false"),
        &format!(
            "serializeType={} code={} opaque={} ext={:?}",
            parsed.serialize_type_current_rpc,
            parsed.code,
            parsed.opaque,
            parsed.ext_fields().sorted()
        ),
    );

    let resp = rpc(client, namesrv, parsed, 5000).await?;
    ck.check(
        "S7 namesrv accepts the binary header",
        resp.code == response_code::SUCCESS,
        &format!("code={} remark={:?}", resp.code, resp.remark),
    );
    if resp.code != response_code::SUCCESS {
        return Ok(());
    }
    // Real-machine behavior: the 5.x namesrv **still replies with the JSON serialize type** (it does not follow the request),
    // so this only prints and does not assert equality -- the client must pick the decoder by the response's own mark.
    println!(
        "        response serializeType={} (request used ROCKETMQ={})",
        resp.serialize_type_current_rpc, serialize_type::ROCKETMQ
    );
    let route = decode_route(&resp, "S7 route");
    let usable = route.as_ref().map(|r| !r.broker_datas.is_empty()).unwrap_or(false);
    ck.check(
        "S7 route decodes through the binary header path",
        usable,
        &format!("err={:?} ok={}", route.as_ref().err().map(|e| e.as_str()), route.is_ok()),
    );
    Ok(())
}

/// S8: cleanup -- first delete the broker-side topic config, then explicitly delete the namesrv route.
async fn s8_cleanup(
    client: &RemotingClient,
    namesrv: &str,
    broker_addr: &str,
    topic: &str,
    ck: &mut Checker,
) -> Live {
    let resp = rpc(
        client,
        broker_addr,
        RemotingCommand::create_request_command(
            request_code::DELETE_TOPIC_IN_BROKER,
            Some(Box::new(DeleteTopicRequestHeader { topic: Some(topic.to_string()) })),
        ),
        5000,
    )
    .await?;
    ck.check(
        "S8 DELETE_TOPIC_IN_BROKER SUCCESS",
        resp.code == response_code::SUCCESS,
        &format!("code={} remark={:?}", resp.code, resp.remark),
    );

    let mut del = RemotingCommand::create_request_command(request_code::DELETE_TOPIC_IN_NAMESRV, None);
    del.add_ext_field("topic", topic);
    let resp = rpc(client, namesrv, del, 5000).await?;
    ck.check(
        "S8 DELETE_TOPIC_IN_NAMESRV SUCCESS",
        resp.code == response_code::SUCCESS,
        &format!("code={} remark={:?}", resp.code, resp.remark),
    );

    let after = get_route(client, namesrv, topic).await?;
    ck.check(
        "S8 route gone after delete",
        after.code == response_code::TOPIC_NOT_EXIST,
        &format!("code={} remark={:?}", after.code, after.remark),
    );
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let argv: Vec<String> = env::args().collect();
    let namesrv = argv
        .get(1)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:9876".to_string());

    let stamp = stamp();
    let topic = format!("RustLiveProtocol_{stamp}");
    let producer_group = format!("PID_rust_live_{stamp}");
    let consumer_group = format!("CID_rust_live_{stamp}");
    let client_id = format!("rust-live-{}@{stamp}", util_all::get_pid());

    println!("== rocketmq rust client live protocol test ==");
    println!("   namesrv   = {namesrv}");
    println!("   topic     = {topic}");
    println!("   groups    = {producer_group} / {consumer_group}");
    println!("   clientId  = {client_id}");

    let client = RemotingClient::new();
    let mut ck = Checker::new();

    // S0 gets the broker address and creates the topic; only then can S1 query the real topic's route.
    if let Err(e) = s0_bootstrap(&client, &namesrv, &topic, &mut ck).await {
        ck.abort("S0 bootstrap", &e);
        client.shutdown();
        report(&mut ck);
        return ExitCode::FAILURE;
    }
    let (broker_name, broker_addr) = match s1_route(&client, &namesrv, &topic, &mut ck).await {
        Ok(b) => b,
        Err(e) => {
            ck.abort("S1 route lookup", &e);
            client.shutdown();
            report(&mut ck);
            return ExitCode::FAILURE;
        }
    };
    println!("   broker    = {broker_name} @ {broker_addr}");

    if let Err(e) = s2_cluster_info(&client, &namesrv, &mut ck).await {
        ck.abort("S2 cluster info", &e);
    }
    let sent = match s3_send(&client, &broker_addr, &broker_name, &topic, &producer_group, &mut ck).await {
        Ok(s) => Some(s),
        Err(e) => {
            ck.abort("S3 send message", &e);
            None
        }
    };
    match &sent {
        Some(s) => {
            if let Err(e) = s4_pull(&client, &broker_addr, &topic, &consumer_group, s, &mut ck).await {
                ck.abort("S4 pull message", &e);
            }
            if let Err(e) = s5_offsets(&client, &broker_addr, &topic, &consumer_group, s, &mut ck).await {
                ck.abort("S5 offsets", &e);
            }
        }
        None => println!("  [SKIP] S4/S5 need a successful send"),
    }
    if let Err(e) = s6_heartbeat(
        &client,
        &broker_addr,
        &topic,
        &client_id,
        &producer_group,
        &consumer_group,
        &mut ck,
    )
    .await
    {
        ck.abort("S6 heartbeat", &e);
    }
    if let Err(e) = s7_rocketmq_serialize(&client, &namesrv, &topic, &mut ck).await {
        ck.abort("S7 rocketmq serialize type", &e);
    }
    if let Err(e) = s8_cleanup(&client, &namesrv, &broker_addr, &topic, &mut ck).await {
        ck.abort("S8 cleanup", &e);
    }

    client.shutdown();
    report(&mut ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn report(ck: &mut Checker) {
    println!("== summary: {} passed, {} failed ==", ck.passed, ck.failed.len());
    for f in &ck.failed {
        println!("   FAILED {f}");
    }
}
