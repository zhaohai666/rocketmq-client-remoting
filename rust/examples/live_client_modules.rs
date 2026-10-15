//! Live verification of the client layer (`src/client/`) against a **real 5.5.1 broker**.
//!
//! Complements `live_protocol.rs` (S0~S8 covers the protocol layer only): this file runs the pure
//! client layer's seven modules -- address fetching / fault-tolerant queue selection / stats /
//! hooks / trace / metrics / request-reply -- and every input fed to them comes from real
//! broker round trips (real msgId, storeHost, commitlog offset, measured RPC latency), not hand-made fakes.
//!
//!
//! ⚠ The only non-real data: the local cluster has a single broker, and T2 needs a "second
//! broker" to observe the avoidance effect, so one group of `broker-b-sim` queues is added by
//! hand under the real topic, used only for queue-selection filtering (no network requests).
//!
//! Usage (start the local cluster per the project README first):
//! ```text
//! cargo run --example live_client_modules -- 127.0.0.1:9876
//! ```

use std::collections::HashMap;
use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use rocketmq_client_remoting::client::consumer_stats::{
    compute_stats_data, ConsumerStatsManager, StatsItem, StatsSnapshot,
};
use rocketmq_client_remoting::client::hook::{
    execute_check_forbidden_hook, execute_consume_hook_after, execute_consume_hook_before,
    execute_filter_hooks, execute_send_message_hook_after, execute_send_message_hook_before,
    CheckForbiddenContext, CheckForbiddenHookList, ConsumeMessageContext, ConsumeMessageHookList,
    CommunicationMode, FilterMessageContext, FilterMessageHookList, HookList, SendMessageContext,
    SendMessageHookList,
};
use rocketmq_client_remoting::client::latency::{
    MQFaultStrategy, PublishInfo, QueueFilter, ISOLATION_LATENCY,
};
use rocketmq_client_remoting::client::metrics::{round3, ClientMetrics};
use rocketmq_client_remoting::client::request_reply::{
    create_correlation_id, create_reply_message, is_reply_message, request_future_holder,
    RequestResponseFuture,
};
use rocketmq_client_remoting::client::result::{SendResult, SendStatus};
use rocketmq_client_remoting::client::top_addressing::DefaultTopAddressing;
use rocketmq_client_remoting::client::trace::{
    local_address, TraceBean, TraceConstants, TraceContext, TraceDataEncoder, TraceType, AccessChannel,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::message_client_id_setter;
use rocketmq_client_remoting::common::message_const as message_const;
use rocketmq_client_remoting::common::message_decoder;
use rocketmq_client_remoting::common::message_type::MessageType;
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::sysflag::PullSysFlag;
use rocketmq_client_remoting::common::topic_config::{self, TopicFilterType};
use rocketmq_client_remoting::common::util_all;
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::client::RemotingClient;
use rocketmq_client_remoting::remoting::protocol::codes::{request_code, response_code};
use rocketmq_client_remoting::remoting::protocol::heartbeat::ExpressionType;
use rocketmq_client_remoting::remoting::protocol::headers::{
    CreateTopicRequestHeader, DeleteTopicRequestHeader, GetRouteInfoRequestHeader,
    PullMessageRequestHeader, SendMessageRequestHeaderV2, SendMessageResponseHeader,
};
use rocketmq_client_remoting::remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;
use rocketmq_client_remoting::remoting::protocol::serialize::RemotingSerializable;

/// Number of queues created for the topic (default 4).
const QUEUE_NUMS: i32 = 4;
/// Starting point of the fake clock in T2 (milliseconds); a fixed value keeps the isolation-window assertions reproducible.
const FAKE_NOW_BASE: i64 = 1_700_000_000_000;
/// A port on this machine that certainly has no listener, used to produce a real "connection refused".
const DEAD_ADDR: &str = "127.0.0.1:1";

// ------------------------------------------------------------------ skeleton

fn stamp() -> String {
    let secs = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => 0,
    };
    format!("{secs}")
}

/// Assertion accumulator: all scenarios run in one pass before the summary; the first failure does not exit early.
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

/// Returns the inner value even when the lock is poisoned (the stats/hook paths must not cascade-fail because of a panic elsewhere).
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

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

/// RPC that also returns the measured elapsed time (T2/T3/T6 all feed real latencies into their modules).
async fn rpc_timed(
    client: &RemotingClient,
    addr: &str,
    cmd: RemotingCommand,
    timeout_millis: i64,
) -> (Result<RemotingCommand, String>, i64) {
    let started = Instant::now();
    let resp = rpc(client, addr, cmd, timeout_millis).await;
    (resp, started.elapsed().as_millis() as i64)
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
    rpc(client, namesrv, req, 5000).await
}

fn decode_route(resp: &RemotingCommand, what: &str) -> Result<TopicRouteData, String> {
    let raw = match resp.body() {
        Some(b) => b.to_vec(),
        None => return Err(format!("{what} response has empty body")),
    };
    // The namesrv clusterInfo/route payload is fastjson-style (numeric map keys are unquoted),
    // so it must go through the crate's compatibility decoder; see the measured notes in live_protocol.rs S2.
    let value = RemotingSerializable::decode(&raw)
        .map_err(|e| format!("{what} body is not parsable: {e}"))?;
    TopicRouteData::from_json_value(&value).map_err(|e| format!("{what} route invalid: {e}"))
}

/// Creates a topic (walks the TBW102 route to find a broker), the same approach as live_protocol.rs S0.
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
    let broker_addr = match route.broker_datas.first().and_then(|b| b.select_broker_addr()) {
        Some(a) => a,
        None => return Err("default route has no usable broker address".to_string()),
    };
    let resp = rpc(
        client,
        &broker_addr,
        RemotingCommand::create_request_command(
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
        ),
        5000,
    )
    .await?;
    if resp.code != response_code::SUCCESS {
        return Err(format!(
            "create topic {topic} on {broker_addr} failed: code={} remark={:?}",
            resp.code, resp.remark
        ));
    }
    Ok(())
}

// ------------------------------------------------------ real send/pull (shared)

/// Result of one sent message + measured elapsed time.
///
/// The `SEND_MESSAGE_V2` response header carries only msgId/queueId/queueOffset; offsetMsgId and
/// storeHost must wait for the pull side, so they are not captured here (see the measured notes in live_protocol.rs S3).
struct Sent {
    msg_id: String,
    queue_id: i32,
    queue_offset: i64,
    body: Vec<u8>,
    rt_millis: i64,
    broker_name: String,
}

/// `SEND_MESSAGE_V2(310)`: same header layout as live_protocol.rs S3.
async fn send_message(
    client: &RemotingClient,
    broker_addr: &str,
    broker_name: &str,
    topic: &str,
    producer_group: &str,
    body: &[u8],
    tags: &str,
) -> Result<Sent, String> {
    let mut msg = Message::new(topic, Some(body));
    msg.set_tags(tags);
    msg.set_keys(&format!("{topic}-key"));
    message_client_id_setter::set_uniq_id(&mut msg);
    msg.put_property(message_const::PROPERTY_WAIT_STORE_MSG_OK, "true");

    let header = SendMessageRequestHeaderV2 {
        producer_group: Some(producer_group.to_string()),
        topic: Some(msg.get_topic().to_string()),
        default_topic: Some(MixAll::DEFAULT_TOPIC.to_string()),
        default_topic_queue_nums: Some(MixAll::DEFAULT_TOPIC_QUEUE_NUMS),
        queue_id: Some(0),
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
    let mut req =
        RemotingCommand::create_request_command(request_code::SEND_MESSAGE_V2, Some(Box::new(header)));
    req.set_body(Some(msg.get_body().to_vec()));

    let (resp, rt) = rpc_timed(client, broker_addr, req, 10_000).await;
    let resp = resp?;
    if resp.code != response_code::SUCCESS {
        return Err(format!(
            "send failed: code={} remark={:?}",
            resp.code, resp.remark
        ));
    }
    let resp_header: SendMessageResponseHeader = resp
        .decode_command_custom_header()
        .map_err(|e| format!("decode SendMessageResponseHeader failed: {e}"))?;
    Ok(Sent {
        msg_id: resp_header.msg_id.clone().unwrap_or_default(),
        queue_id: resp_header.queue_id.unwrap_or(-1),
        queue_offset: resp_header.queue_offset.unwrap_or(-1),
        body: msg.get_body().to_vec(),
        rt_millis: rt,
        broker_name: broker_name.to_string(),
    })
}

/// `PULL_MESSAGE(11)`: returns the decoded messages + measured elapsed time.
async fn pull_messages(
    client: &RemotingClient,
    broker_addr: &str,
    topic: &str,
    consumer_group: &str,
    queue_id: i32,
    from_offset: i64,
    max_msgs: i32,
) -> Result<(Vec<MessageExt>, i64), String> {
    let sys_flag = PullSysFlag::build_sys_flag_basic(false, false, true, false);
    let req = RemotingCommand::create_request_command(
        request_code::PULL_MESSAGE,
        Some(Box::new(PullMessageRequestHeader {
            consumer_group: Some(consumer_group.to_string()),
            topic: Some(topic.to_string()),
            lite_topic: None,
            queue_id: Some(queue_id),
            queue_offset: Some(from_offset),
            max_msg_nums: Some(max_msgs),
            sys_flag: Some(sys_flag),
            commit_offset: Some(0),
            suspend_timeout_millis: Some(15_000),
            subscription: Some("*".to_string()),
            sub_version: Some(0),
            expression_type: Some(ExpressionType::TAG.to_string()),
            max_msg_bytes: Some(-1),
            request_source: Some(0),
            proxy_froward_client_id: None,
        })),
    );
    let (resp, rt) = rpc_timed(client, broker_addr, req, 30_000).await;
    let resp = resp?;
    if resp.code == response_code::PULL_NOT_FOUND {
        return Ok((Vec::new(), rt));
    }
    if resp.code != response_code::SUCCESS {
        return Err(format!(
            "pull failed: code={} remark={:?}",
            resp.code, resp.remark
        ));
    }
    let raw = match resp.body() {
        Some(b) => b.to_vec(),
        None => return Ok((Vec::new(), rt)),
    };
    Ok((message_decoder::decode_messages(&raw), rt))
}

/// Polls until at least `want` messages have been pulled (the broker's group/commit bookkeeping is asynchronously persisted; the offset is readable immediately).
async fn pull_until(
    client: &RemotingClient,
    broker_addr: &str,
    topic: &str,
    group: &str,
    from_offset: i64,
    want: usize,
) -> Result<(Vec<MessageExt>, i64), String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (msgs, rt) = pull_messages(client, broker_addr, topic, group, 0, from_offset, 32).await?;
        if msgs.len() >= want || Instant::now() > deadline {
            return Ok((msgs, rt));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ------------------------------------------------------------------ T1

async fn t1_top_addressing(
    client: &RemotingClient,
    namesrv: &str,
    topic: &str,
    ck: &mut Checker,
) -> Live {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("bind address-server stub failed: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("stub local_addr unavailable: {e}"))?
        .port();
    // (status, body) is re-read on every connection so a scenario can change the response mid-run.
    let served = Arc::new(Mutex::new((200_u16, format!("{namesrv}\n10.255.255.1:9876"))));
    let paths: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let task = {
        let served = Arc::clone(&served);
        let paths = Arc::clone(&paths);
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };
                let served = Arc::clone(&served);
                let paths = Arc::clone(&paths);
                tokio::spawn(async move {
                    let mut buf = [0_u8; 1024];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let first = String::from_utf8_lossy(&buf[..n])
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .to_string();
                    lock(&paths).push(first);
                    let (status, body) = lock(&served).clone();
                    let text = format!(
                        "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(text.as_bytes()).await;
                });
            }
        })
    };

    // The domain carries its own port => MixAll.getWSAddr must not append :8080; the path received by the stub is the counter-evidence.
    let ta = DefaultTopAddressing::from_domain(format!("127.0.0.1:{port}"));
    ck.check(
        "T1 ws_addr keeps the explicit port",
        ta.ws_addr() == format!("http://127.0.0.1:{port}/rocketmq/nsaddr"),
        &format!("ws_addr={}", ta.ws_addr()),
    );
    ck.check(
        "T1 timeout default is 3000ms",
        ta.timeout_millis() == 3000,
        &format!("timeout={}", ta.timeout_millis()),
    );

    let fetched = ta.fetch_ns_addr(true).await;
    ck.check(
        "T1 fetch_ns_addr cuts the body at the first newline",
        fetched.as_deref() == Some(namesrv),
        &format!("fetched={fetched:?} want={namesrv:?}"),
    );
    let first_get = { lock(&paths).first().cloned() };
    ck.check(
        "T1 stub received GET /rocketmq/nsaddr",
        first_get.as_deref() == Some("GET /rocketmq/nsaddr HTTP/1.1"),
        &format!("got line={first_get:?}"),
    );

    // The unitName + para concatenation rules: proven by the request line the stub actually receives, not just by inspecting the string.
    let unit_ta = ta
        .clone()
        .with_unit_name("unitA")
        .with_para(vec![("nofix".to_string(), "2".to_string())]);
    ck.check(
        "T1 build_url appends -unit?nofix=1&para",
        unit_ta.build_url()
            == format!("http://127.0.0.1:{port}/rocketmq/nsaddr-unitA?nofix=1&nofix=2"),
        &format!("url={}", unit_ta.build_url()),
    );
    let _ = unit_ta.fetch_ns_addr(false).await;
    let second_get = {
        let g = lock(&paths);
        g.get(1).cloned()
    };
    ck.check(
        "T1 unit+para URL reaches the server",
        second_get.as_deref()
            == Some("GET /rocketmq/nsaddr-unitA?nofix=1&nofix=2 HTTP/1.1"),
        &format!("got line={second_get:?}"),
    );

    let mut applying = ta.clone();
    ck.check(
        "T1 first fetch is applied",
        applying.fetch_and_apply().await.as_deref() == Some(namesrv),
        &format!("ns_addr={:?}", applying.ns_addr()),
    );
    ck.check(
        "T1 unchanged address is not applied again",
        applying.fetch_and_apply().await.is_none(),
        &format!("ns_addr={:?}", applying.ns_addr()),
    );
    *lock(&served) = (200, "10.255.255.2:9876".to_string());
    ck.check(
        "T1 changed address is applied",
        applying.fetch_and_apply().await.as_deref() == Some("10.255.255.2:9876"),
        &format!("ns_addr={:?}", applying.ns_addr()),
    );
    ck.check(
        "T1 ns_addr caches the applied value",
        applying.ns_addr() == Some("10.255.255.2:9876"),
        &format!("ns_addr={:?}", applying.ns_addr()),
    );

    // Non-200 / blank body / unreachable: all three failure paths must yield None and must not panic.
    *lock(&served) = (500, "boom".to_string());
    ck.check(
        "T1 non-200 yields None",
        ta.fetch_ns_addr(true).await.is_none(),
        "status 500 should not be applied",
    );
    *lock(&served) = (200, "   ".to_string());
    let mut blank = ta.clone();
    ck.check(
        "T1 blank body is not applied",
        blank.fetch_and_apply().await.is_none() && blank.ns_addr().is_none(),
        &format!("ns_addr={:?}", blank.ns_addr()),
    );
    let started = Instant::now();
    let dead =
        DefaultTopAddressing::from_domain(DEAD_ADDR).with_timeout_millis(300);
    ck.check(
        "T1 unreachable server yields None",
        dead.fetch_ns_addr(true).await.is_none(),
        "connection refused should yield None",
    );
    ck.check(
        "T1 unreachable server respects the timeout",
        started.elapsed() < Duration::from_secs(3),
        &format!("elapsed={:?}", started.elapsed()),
    );

    // Wrap-up: serve the real namesrv address again and do one real route lookup through the **fetched** address.
    *lock(&served) = (200, namesrv.to_string());
    let mut live_ta = ta.clone();
    let applied = match live_ta.fetch_and_apply().await {
        Some(addr) => addr,
        None => {
            task.abort();
            return Err("address server did not return the real namesrv address".to_string());
        }
    };
    let resp = get_route(client, &applied, topic).await;
    match resp {
        Ok(resp) => ck.check(
            "T1 route lookup works through the dynamically fetched address",
            resp.code == response_code::SUCCESS,
            &format!("addr={applied} code={} remark={:?}", resp.code, resp.remark),
        ),
        Err(e) => ck.abort("T1 route lookup via fetched address", &e),
    }
    task.abort();
    Ok(())
}

// ------------------------------------------------------------------ T2

/// Fake clock: T2 advances `CLOCK` manually throughout, so the isolation-window assertions are independent of real elapsed time.
static CLOCK: AtomicI64 = AtomicI64::new(FAKE_NOW_BASE);

fn fake_now() -> i64 {
    CLOCK.load(Ordering::Relaxed)
}

/// Minimal [`PublishInfo`] implementation: queues from the real route + one group of simulated second-broker queues.
///
/// Round-robin semantics:
/// with no filter it always advances; with filters it tries at most `n` entries and the cursor advances on every probe.
struct LivePublishInfo {
    queues: Vec<MessageQueue>,
    next: AtomicUsize,
}

impl LivePublishInfo {
    fn new(queues: Vec<MessageQueue>) -> LivePublishInfo {
        LivePublishInfo { queues, next: AtomicUsize::new(0) }
    }

    fn len(&self) -> usize {
        self.queues.len()
    }
}

impl PublishInfo for LivePublishInfo {
    fn reset_index(&self) {
        self.next.store(0, Ordering::Relaxed);
    }

    fn select_one_message_queue(
        &self,
        filters: &[&QueueFilter<'_>],
    ) -> Result<Option<MessageQueue>, Error> {
        let n = self.queues.len();
        if n == 0 {
            return Err(Error::client("no message queue for publish info"));
        }
        let tries = if filters.is_empty() { 1 } else { n };
        for _ in 0..tries {
            let i = self.next.fetch_add(1, Ordering::Relaxed) % n;
            let mq = &self.queues[i];
            if filters.iter().all(|f| f(mq)) {
                return Ok(Some(mq.clone()));
            }
        }
        Ok(None)
    }
}

async fn t2_latency(
    client: &RemotingClient,
    namesrv: &str,
    topic: &str,
    broker_name: &str,
    broker_addr: &str,
    sent: &Sent,
    ck: &mut Checker,
) -> Live {
    // Real latencies: one successful send (sent) + one request that certainly cannot connect (measured just below).
    let (dead_resp, dead_rt) = rpc_timed(
        client,
        DEAD_ADDR,
        RemotingCommand::create_request_command(
            request_code::GET_ROUTEINFO_BY_TOPIC,
            Some(Box::new(GetRouteInfoRequestHeader {
                topic: Some(topic.to_string()),
                accept_standard_json_only: None,
            })),
        ),
        1000,
    )
    .await;
    ck.check(
        "T2 dead address really fails",
        dead_resp.is_err(),
        &format!("unexpected success: {:?}", dead_resp.map(|r| r.code)),
    );
    println!(
        "        measured latency: send={}ms connect-fail={dead_rt}ms (real broker {broker_name}@{broker_addr})",
        sent.rt_millis
    );

    // The route side hands out MessageQueueKey (a hashable key type); only the producer side uses MessageQueue.
    let route = match get_route(client, namesrv, topic)
        .await
        .and_then(|r| decode_route(&r, "T2 route"))
    {
        Ok(route) => route,
        Err(e) => return Err(e),
    };
    let mut queues: Vec<MessageQueue> = route
        .get_all_message_queue(topic)
        .iter()
        .map(|k| MessageQueue::new(&k.topic, &k.broker_name, k.queue_id))
        .collect();
    let real_len = queues.len();
    ck.check(
        "T2 real route exposes writable queues",
        real_len > 0 && queues.iter().all(|q| q.get_broker_name() == broker_name),
        &format!("real queues={} broker={broker_name}", real_len),
    );
    // A single-broker cluster cannot produce the "avoid" effect, so one simulated broker is added by hand under the same topic.
    for q in 0..QUEUE_NUMS {
        queues.push(MessageQueue::new(topic, "broker-b-sim", q));
    }
    let info = LivePublishInfo::new(queues);
    ck.check(
        "T2 publish info spans the real plus the simulated broker",
        info.len() as i32 == real_len as i32 + QUEUE_NUMS,
        &format!("len={} real={real_len}", info.len()),
    );

    CLOCK.store(FAKE_NOW_BASE, Ordering::Relaxed);
    let strategy = MQFaultStrategy::with_clock(true, fake_now);

    // 1) The real successful latency is recorded: a local RT is usually < 50ms => the lowest tier, isolation window 0.
    strategy.update_fault_item(broker_name, sent.rt_millis, false, true);
    let item = strategy.latency_fault_tolerance().get_fault_item(broker_name);
    ck.check(
        "T2 real send latency recorded under the broker name",
        item.as_ref().map(|i| i.current_latency) == Some(sent.rt_millis),
        &format!("item={item:?} want latency={}", sent.rt_millis),
    );
    ck.check(
        "T2 a fast broker stays available",
        strategy.latency_fault_tolerance().is_available(broker_name),
        "a healthy real send must not be isolated",
    );

    // 2) A real connection failure => isolation: the tier looks up ISOLATION_LATENCY(10s) and the item is marked unreachable.
    let dead_broker = "broker-dead-sim";
    strategy.update_fault_item(dead_broker, dead_rt, true, false);
    let dead_item = strategy
        .latency_fault_tolerance()
        .get_fault_item(dead_broker);
    ck.check(
        "T2 isolation uses the real latency but the 10s tier",
        dead_item.as_ref().map(|i| i.current_latency) == Some(dead_rt)
            && dead_item.as_ref().map(|i| i.start_timestamp)
                == Some(FAKE_NOW_BASE + ISOLATION_LATENCY),
        &format!("item={dead_item:?}"),
    );
    ck.check(
        "T2 isolated broker is unavailable and unreachable",
        !strategy.latency_fault_tolerance().is_available(dead_broker)
            && !strategy.latency_fault_tolerance().is_reachable(dead_broker),
        "isolation should have set a 10s window",
    );
    let known = strategy.latency_fault_tolerance().fault_item_names();
    ck.check(
        "T2 fault table holds both brokers",
        known.len() == 2 && known.iter().any(|n| n == broker_name),
        &format!("names={known:?}"),
    );

    // 3) With the real broker isolated => queue selection must avoid it and land on the simulated broker.
    strategy.update_fault_item(broker_name, sent.rt_millis, true, false);
    let picked = strategy
        .select_one_message_queue(&info, None, true)
        .map_err(|e| format!("select_one_message_queue failed: {e}"))?;
    ck.check(
        "T2 selection avoids the isolated real broker",
        picked.get_broker_name() == "broker-b-sim",
        &format!("picked={}", picked.get_broker_name()),
    );
    ck.check(
        "T2 picked queue still belongs to the live topic",
        picked.get_topic() == topic && picked.get_queue_id() >= 0,
        &format!("picked={picked:?}"),
    );

    // 4) Advancing the fake clock past the 10s window => the real broker becomes available again and gets its turn after reset_index.
    CLOCK.store(FAKE_NOW_BASE + ISOLATION_LATENCY + 1, Ordering::Relaxed);
    let recovered = strategy
        .select_one_message_queue(&info, None, true)
        .map_err(|e| format!("select after recovery failed: {e}"))?;
    ck.check(
        "T2 the isolated real broker recovers after its window",
        recovered.get_broker_name() == broker_name,
        &format!("picked={}", recovered.get_broker_name()),
    );

    // 5) lastBrokerName exclusion: a send retry never picks the broker used last time.
    let mut all_other = true;
    for _ in 0..8 {
        let mq = strategy
            .select_one_message_queue(&info, Some(broker_name), false)
            .map_err(|e| format!("select with lastBroker failed: {e}"))?;
        all_other &= mq.get_broker_name() != broker_name;
    }
    ck.check(
        "T2 lastBrokerName is excluded from selection",
        all_other,
        "some selection returned the previous broker",
    );

    // 6) With the switch off: nothing is recorded, and selection still succeeds even with no filter.
    CLOCK.store(FAKE_NOW_BASE, Ordering::Relaxed);
    let off = MQFaultStrategy::with_clock(false, fake_now);
    off.update_fault_item(broker_name, sent.rt_millis, true, false);
    ck.check(
        "T2 a disabled strategy records nothing",
        off.latency_fault_tolerance().fault_item_names().is_empty()
            && off.latency_fault_tolerance().is_available(broker_name),
        &format!("names={:?}", off.latency_fault_tolerance().fault_item_names()),
    );
    let plain = off
        .select_one_message_queue(&info, None, true)
        .map_err(|e| format!("plain selection failed: {e}"))?;
    ck.check(
        "T2 disabled strategy still round-robins",
        plain.get_broker_name() == broker_name,
        &format!("picked={}", plain.get_broker_name()),
    );
    Ok(())
}

// ------------------------------------------------------------------ T3

async fn t3_consumer_stats(
    client: &RemotingClient,
    broker_addr: &str,
    topic: &str,
    group: &str,
    pulled: &[MessageExt],
    send_rt: i64,
    ck: &mut Checker,
) -> Live {
    // One real pull to obtain the real RT and message count.
    let from = match pulled.first() {
        Some(m) => m.queue_offset,
        None => return Err("no pulled message to build stats from".to_string()),
    };
    let (msgs, pull_rt) = pull_messages(client, broker_addr, topic, group, 0, from, 32).await?;
    ck.check(
        "T3 a real pull returns the sent messages",
        !msgs.is_empty(),
        "live pull found nothing",
    );
    println!(
        "        measured latency: pull={pull_rt}ms msgs={} send={send_rt}ms",
        msgs.len()
    );

    let mgr = ConsumerStatsManager::new();
    ck.check(
        "T3 key is topic@group",
        ConsumerStatsManager::key(topic, group) == format!("{topic}@{group}"),
        &format!("key={}", ConsumerStatsManager::key(topic, group)),
    );
    for m in &msgs {
        mgr.inc_pull_rt(group, topic, pull_rt);
        mgr.inc_pull_tps(group, topic, 1);
        // Consume side: a non-empty body counts as success; bookkeeping uses the real body length.
        let len = m.get_body().len() as i64;
        mgr.inc_consume_rt(group, topic, m.store_timestamp.max(1));
        mgr.inc_consume_ok_tps(group, topic, len);
    }

    let handle = client
        .runtime_handle()
        .ok_or("no tokio runtime handle bound to the remoting client")?;
    mgr.start_with_handle(&handle);
    ck.check(
        "T3 sampler is running after start_with_handle",
        mgr.is_running(),
        "start_with_handle did not spawn the sampler",
    );
    // A repeated start must be a no-op (it returns directly when an existing thread is found).
    mgr.start_with_handle(&handle);
    ck.check("T3 start is idempotent", mgr.is_running(), "second start killed the sampler");

    // Sampling with fixed timestamps is what makes the StatsSnapshot arithmetic exactly comparable.
    let key = ConsumerStatsManager::key(topic, group);
    let tps_item: Arc<StatsItem> = mgr.topic_and_group_pull_tps().get_and_create(&key);
    let rt_item: Arc<StatsItem> = mgr.topic_and_group_pull_rt().get_and_create(&key);
    let total_value = tps_item.value();
    let total_times = tps_item.times();
    let rt_value = rt_item.value();
    ck.check(
        "T3 pull TPS accumulated one per real message",
        total_value == msgs.len() as i64 && total_times == msgs.len() as i64,
        &format!("value={total_value} times={total_times} msgs={}", msgs.len()),
    );
    ck.check(
        "T3 pull RT accumulated the real RTs",
        rt_value == pull_rt * msgs.len() as i64,
        &format!("value={rt_value} want={}", pull_rt * msgs.len() as i64),
    );

    tps_item.sample_at(1_000);
    rt_item.sample_at(1_000);
    let (more_msgs, more_rt) = pull_messages(client, broker_addr, topic, group, 0, from, 32).await?;
    for _ in &more_msgs {
        mgr.inc_pull_rt(group, topic, more_rt);
        mgr.inc_pull_tps(group, topic, 1);
    }
    tps_item.sample_at(3_000);
    rt_item.sample_at(3_000);

    let tps_snap = tps_item.get_stats_data_in_minute();
    ck.check(
        "T3 minute TPS is msgs per second over the pinned window",
        tps_snap.sum == more_msgs.len() as i64
            && tps_snap.times == more_msgs.len() as i64
            && (tps_snap.tps - (more_msgs.len() as f64 * 1000.0 / 2000.0)).abs() < 1e-9,
        &format!("snap={tps_snap} more={}", more_msgs.len()),
    );
    let rt_snap = rt_item.get_stats_data_in_minute();
    let want_avg = if more_msgs.is_empty() {
        0.0
    } else {
        (more_rt * more_msgs.len() as i64) as f64 / more_msgs.len() as f64
    };
    ck.check(
        "T3 minute RT avgpt is the mean real RT",
        rt_snap.sum == more_rt * more_msgs.len() as i64
            && (rt_snap.avgpt - want_avg).abs() < 1e-9,
        &format!("snap={rt_snap} wantAvg={want_avg}"),
    );
    // Feeding compute_stats_data directly matches the item's accounting (the same first/last differencing algorithm).
    let direct: StatsSnapshot = compute_stats_data(&[(1_000, 0, 0), (3_000, 4, 2)]);
    ck.check(
        "T3 compute_stats_data matches the first/last diff",
        direct.sum == 4 && direct.times == 2 && (direct.tps - 2.0).abs() < 1e-9
            && (direct.avgpt - 2.0).abs() < 1e-9,
        &format!("direct={direct}"),
    );
    ck.check(
        "T3 StatsSnapshot Display keeps two decimals",
        format!("{direct}") == "StatsSnapshot(sum=4, tps=2.00, avgpt=2.00, times=2)",
        &format!("display={direct}"),
    );
    ck.check(
        "T3 hour window is empty before the first hour sample",
        rt_item.hour_len() == 0 && rt_item.get_stats_data_in_hour() == StatsSnapshot::default(),
        &format!("hourLen={} snap={}", rt_item.hour_len(), rt_item.get_stats_data_in_hour()),
    );

    // One sampling sweep: the minute chain grows, the hour chain is still empty (the hour boundary has not arrived).
    let minute_before = rt_item.minute_len();
    mgr.sample_all();
    ck.check(
        "T3 sample_all appends a minute point",
        rt_item.minute_len() > minute_before,
        &format!("before={minute_before} after={}", rt_item.minute_len()),
    );
    let keys = mgr.topic_and_group_pull_tps().keys();
    ck.check(
        "T3 the stats key shows up in its set",
        keys.iter().any(|k| k == &key),
        &format!("keys={keys:?}"),
    );

    mgr.shutdown();
    ck.check(
        "T3 shutdown stops the sampler",
        !mgr.is_running(),
        "sampler still running after shutdown",
    );
    // After shutdown a restart is possible (the thread handle is reset to None).
    mgr.start_with_handle(&handle);
    ck.check("T3 restart after shutdown works", mgr.is_running(), "restart failed");
    mgr.shutdown();
    Ok(())
}

// ------------------------------------------------------------------ T4

/// Hook that records the call order; with `poison=true` the before hook errors out, to verify that the error is swallowed.
struct Recorder {
    events: Arc<Mutex<Vec<String>>>,
    poison: bool,
}

impl rocketmq_client_remoting::client::hook::SendMessageHook for Recorder {
    fn hook_name(&self) -> &str {
        if self.poison { "poison-send-hook" } else { "record-send-hook" }
    }

    fn send_message_before(&self, context: &mut SendMessageContext) -> Result<(), Error> {
        lock(&self.events).push(format!(
            "send_before:{}",
            context.message.as_ref().map(|m| m.get_topic().to_string()).unwrap_or_default()
        ));
        if self.poison {
            return Err(Error::client("intentional before-hook failure"));
        }
        Ok(())
    }

    fn send_message_after(&self, context: &mut SendMessageContext) -> Result<(), Error> {
        lock(&self.events).push(format!(
            "send_after:ok={} uniq={}",
            context.send_result.as_ref().map(|r| r.status == SendStatus::SendOk).unwrap_or(false),
            context
                .send_result
                .as_ref()
                .and_then(|r| r.msg_id.clone())
                .unwrap_or_default()
        ));
        Ok(())
    }
}

impl rocketmq_client_remoting::client::hook::ConsumeMessageHook for Recorder {
    fn hook_name(&self) -> &str {
        "record-consume-hook"
    }

    fn consume_message_before(&self, context: &mut ConsumeMessageContext) -> Result<(), Error> {
        lock(&self.events).push(format!("consume_before:{}", context.msg_list.len()));
        Ok(())
    }

    fn consume_message_after(&self, context: &mut ConsumeMessageContext) -> Result<(), Error> {
        lock(&self.events).push(format!(
            "consume_after:success={} status={:?}",
            context.success, context.status
        ));
        Ok(())
    }
}

impl rocketmq_client_remoting::client::hook::CheckForbiddenHook for Recorder {
    fn hook_name(&self) -> &str {
        if self.poison { "deny-hook" } else { "allow-hook" }
    }

    fn check_forbidden(&self, context: &mut CheckForbiddenContext) -> Result<(), Error> {
        lock(&self.events).push(format!("check:{}", context.group));
        if self.poison {
            return Err(Error::client("forbidden by hook"));
        }
        Ok(())
    }
}

impl rocketmq_client_remoting::client::hook::FilterMessageHook for Recorder {
    fn hook_name(&self) -> &str {
        "drop-everything-hook"
    }

    fn filter_message(&self, context: &mut FilterMessageContext) -> Result<(), Error> {
        lock(&self.events).push(format!("filter:{}", context.msg_list.len()));
        // Real semantics: the msg_list rewritten by the hook is discarded outright by the client.
        context.msg_list.clear();
        Ok(())
    }
}

async fn t4_hooks(
    topic: &str,
    producer_group: &str,
    consumer_group: &str,
    broker_addr: &str,
    sent: &Sent,
    pulled: &[MessageExt],
    ck: &mut Checker,
) -> Live {
    let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let mq = MessageQueue::new(topic, &sent.broker_name, sent.queue_id);
    let send_result = SendResult {
        status: SendStatus::SendOk,
        msg_id: Some(sent.msg_id.clone()),
        message_queue: Some(mq.clone()),
        queue_offset: sent.queue_offset,
        offset_msg_id: Some(sent.msg_id.clone()),
        trace_on: true,
        ..Default::default()
    };

    let send_hooks: SendMessageHookList = HookList::new();
    ck.check(
        "T4 an empty hook list reports no hooks",
        send_hooks.is_empty() && !send_hooks.has_hooks(),
        &format!("len={}", send_hooks.len()),
    );
    send_hooks.register(Arc::new(Recorder { events: Arc::clone(&events), poison: false })
        as Arc<dyn rocketmq_client_remoting::client::hook::SendMessageHook>);
    send_hooks.register(Arc::new(Recorder { events: Arc::clone(&events), poison: true })
        as Arc<dyn rocketmq_client_remoting::client::hook::SendMessageHook>);
    ck.check(
        "T4 hooks register in order",
        send_hooks.len() == 2 && send_hooks.has_hooks(),
        &format!("len={}", send_hooks.len()),
    );

    let mut ctx = SendMessageContext {
        producer_group: producer_group.to_string(),
        message: Some({
            let mut m = Message::new(topic, Some(&sent.body));
            m.set_tags("TagRustLiveClient");
            m
        }),
        mq: Some(mq.clone()),
        broker_addr: broker_addr.to_string(),
        born_host: local_address().to_string(),
        communication_mode: Some(CommunicationMode::Sync),
        msg_type: MessageType::NormalMsg,
        ..Default::default()
    };
    execute_send_message_hook_before(&send_hooks, &mut ctx);
    let after_first = lock(&events).len();
    ck.check(
        "T4 both before hooks ran and the error was swallowed",
        after_first == 2,
        &format!("events={:?}", lock(&events)),
    );
    ctx.send_result = Some(send_result.clone());
    execute_send_message_hook_after(&send_hooks, &mut ctx);
    let snapshot = lock(&events).clone();
    ck.check(
        "T4 after hook sees the real SendResult",
        snapshot.len() == 4
            && snapshot[0] == format!("send_before:{topic}")
            && snapshot[2] == format!("send_after:ok=true uniq={}", sent.msg_id),
        &format!("events={snapshot:?}"),
    );
    ck.check(
        "T4 CommunicationMode prints the expected constant",
        CommunicationMode::Sync.name() == "SYNC"
            && CommunicationMode::from_name("ASYNC") == CommunicationMode::Async,
        &format!("mode={}", CommunicationMode::Sync),
    );

    // Consume hooks: the messages really pulled back + the real queue.
    let consume_events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let consume_hooks: ConsumeMessageHookList = HookList::new();
    consume_hooks.register(Arc::new(Recorder {
        events: Arc::clone(&consume_events),
        poison: false,
    }) as Arc<dyn rocketmq_client_remoting::client::hook::ConsumeMessageHook>);
    let mut cctx = ConsumeMessageContext::new(consumer_group, Some(pulled.to_vec()), Some(mq.clone()));
    ck.check(
        "T4 ConsumeMessageContext defaults success to True",
        cctx.success && cctx.msg_list.len() == pulled.len(),
        &format!("success={} len={}", cctx.success, cctx.msg_list.len()),
    );
    cctx.status = Some("CONSUME_SUCCESS".to_string());
    cctx.access_channel = Some("LOCAL".to_string());
    execute_consume_hook_before(&consume_hooks, &mut cctx);
    execute_consume_hook_after(&consume_hooks, &mut cctx);
    let ce = lock(&consume_events).clone();
    ck.check(
        "T4 consume hooks see the real message batch",
        ce.len() == 2
            && ce[0] == format!("consume_before:{}", pulled.len())
            && ce[1] == "consume_after:success=true status=Some(\"CONSUME_SUCCESS\")",
        &format!("events={ce:?}"),
    );

    // CheckForbidden is the opposite of the above: the Err must propagate up and later hooks must not run.
    let forbidden_events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let deny: CheckForbiddenHookList = HookList::new();
    deny.register(Arc::new(Recorder { events: Arc::clone(&forbidden_events), poison: true })
        as Arc<dyn rocketmq_client_remoting::client::hook::CheckForbiddenHook>);
    deny.register(Arc::new(Recorder { events: Arc::clone(&forbidden_events), poison: false })
        as Arc<dyn rocketmq_client_remoting::client::hook::CheckForbiddenHook>);
    let mut fctx = CheckForbiddenContext {
        name_srv_addr: String::new(),
        group: producer_group.to_string(),
        message: Some(Message::new(topic, Some(&sent.body))),
        mq: Some(mq.clone()),
        broker_addr: broker_addr.to_string(),
        communication_mode: Some(CommunicationMode::Sync),
        ..Default::default()
    };
    let denied = execute_check_forbidden_hook(&deny, &mut fctx);
    let fe = lock(&forbidden_events).clone();
    ck.check(
        "T4 a denying checkForbidden hook propagates and short-circuits",
        denied.is_err() && fe.len() == 1,
        &format!("result={:?} events={fe:?}", denied.is_ok()),
    );
    let allow: CheckForbiddenHookList = HookList::new();
    allow.register(Arc::new(Recorder { events: Arc::clone(&forbidden_events), poison: false })
        as Arc<dyn rocketmq_client_remoting::client::hook::CheckForbiddenHook>);
    ck.check(
        "T4 an allowing checkForbidden hook passes",
        execute_check_forbidden_hook(&allow, &mut fctx).is_ok(),
        "allowing hook returned Err",
    );

    // FilterMessage: the list after the hook rewrites it is exactly the list the client sees.
    let filter_events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let filters: FilterMessageHookList = HookList::new();
    filters.register(Arc::new(Recorder { events: Arc::clone(&filter_events), poison: false })
        as Arc<dyn rocketmq_client_remoting::client::hook::FilterMessageHook>);
    let mut gctx = FilterMessageContext::new(consumer_group, Some(pulled.to_vec()), Some(mq));
    execute_filter_hooks(&filters, &mut gctx);
    ck.check(
        "T4 a filter hook can drop the whole real batch",
        gctx.msg_list.is_empty() && lock(&filter_events).len() == 1,
        &format!("len={} events={:?}", gctx.msg_list.len(), lock(&filter_events)),
    );
    let empty_ctx = FilterMessageContext::new(consumer_group, None, None);
    let no_hooks: FilterMessageHookList = HookList::new();
    execute_filter_hooks(&no_hooks, &mut FilterMessageContext::default());
    ck.check(
        "T4 empty msg_list and no hooks are no-ops",
        empty_ctx.msg_list.is_empty() && !no_hooks.has_hooks() && no_hooks.is_empty(),
        &format!("len={}", empty_ctx.msg_list.len()),
    );
    Ok(())
}

// ------------------------------------------------------------------ T5

async fn t5_trace(
    client: &RemotingClient,
    broker_addr: &str,
    broker_name: &str,
    topic: &str,
    producer_group: &str,
    pulled: &[MessageExt],
    ck: &mut Checker,
) -> Live {
    let first = match pulled.first() {
        Some(m) => m,
        None => return Err("no real message for the trace bean".to_string()),
    };
    let uniq_id = first
        .get_property(message_const::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        .unwrap_or_default()
        .to_string();
    let tags = first.get_tags().unwrap_or_default().to_string();
    let keys = first.get_keys().unwrap_or_default().to_string();
    let store_host = first.store_host.clone().unwrap_or_default();

    let mut bean = TraceBean::new();
    bean.topic = topic.to_string();
    bean.msg_id = uniq_id.clone();
    bean.offset_msg_id = first.msg_id.clone().unwrap_or_default();
    bean.tags = tags.clone();
    bean.keys = format!("{keys}{}{topic}-key2", message_const::KEY_SEPARATOR);
    bean.store_host = store_host.clone();
    bean.client_host = local_address().to_string();
    bean.store_time = first.store_timestamp;
    bean.retry_times = first.reconsume_times;
    bean.body_length = first.get_body().len() as i32;
    bean.msg_type = MessageType::NormalMsg;

    let mut ctx = TraceContext::new();
    ctx.trace_type = Some(TraceType::Pub);
    ctx.group_name = producer_group.to_string();
    ctx.cost_time = 12;
    ctx.is_success = true;
    ctx.region_id = "namespace-region".to_string();
    ctx.region_name = "cn-hangzhou".to_string();
    ctx.access_channel = Some(AccessChannel::Local);
    ctx.trace_beans.push(bean.clone());

    let transfer = match TraceDataEncoder::encoder_from_context_bean(Some(&ctx)) {
        Some(t) => t,
        None => return Err("encoder_from_context_bean returned None".to_string()),
    };
    ck.check(
        "T5 encoded record is field-terminated by STX",
        transfer.trans_data.ends_with(TraceConstants::FIELD_SPLITOR)
            && transfer.trans_data.contains(TraceConstants::CONTENT_SPLITOR),
        &format!("data={:?}", transfer.trans_data),
    );
    ck.check(
        "T5 first field of a Pub record is the TraceType name",
        transfer
            .trans_data
            .split(TraceConstants::CONTENT_SPLITOR)
            .next()
            .unwrap_or_default()
            == TraceType::Pub.name(),
        &format!("data={:?}", transfer.trans_data),
    );
    ck.check(
        "T5 transKey carries the real msgId and the split business keys",
        transfer.trans_key.contains(&uniq_id)
            && transfer.trans_key.contains(&format!("{topic}-key2")),
        &format!("keys={:?}", transfer.trans_key),
    );

    // Decode round trip (within the same process).
    let decoded = TraceDataEncoder::decoder_from_trace_data_string(Some(&transfer.trans_data));
    if decoded.len() != 1 {
        return Err(format!("expected 1 decoded context, got {}", decoded.len()));
    }
    let back = &decoded[0];
    ck.check(
        "T5 Pub decode restores traceType/group/costTime/success",
        back.trace_type == Some(TraceType::Pub)
            && back.group_name == producer_group
            && back.cost_time == 12
            && back.is_success,
        &format!("ctx={back:?}"),
    );
    let got = match back.trace_beans.first() {
        Some(b) => b,
        None => return Err("decoded context has no bean".to_string()),
    };
    ck.check(
        "T5 Pub decode restores the real msgId/offsetMsgId/storeHost/tags/bodyLength",
        got.msg_id == uniq_id
            && got.offset_msg_id == first.msg_id.clone().unwrap_or_default()
            && got.store_host == store_host
            && got.body_length == first.get_body().len() as i32
            && got.tags == tags
            && got.retry_times == first.reconsume_times
            && got.msg_type == MessageType::NormalMsg,
        &format!("got={got:?}"),
    );
    ck.check(
        "T5 a Pub record carries no storeTime (14-field layout)",
        got.store_time == 0 && got.client_host == local_address(),
        &format!("storeTime={} clientHost={}", got.store_time, got.client_host),
    );
    let mut comparable = bean.clone();
    comparable.store_time = 0;
    comparable.client_host = local_address().to_string();
    ck.check(
        "T5 decoded bean equals the original modulo those two absent fields",
        got == &comparable,
        &format!("original={comparable:?}\n           decoded={got:?}"),
    );
    // clientHost only exists from the 15th field on: append one segment by hand to verify the old-version compatibility branch.
    let mut fifteen = transfer
        .trans_data
        .trim_end_matches(TraceConstants::FIELD_SPLITOR)
        .to_string();
    fifteen.push(TraceConstants::CONTENT_SPLITOR);
    fifteen.push_str("10.9.8.7:10911");
    fifteen.push(TraceConstants::FIELD_SPLITOR);
    let old = TraceDataEncoder::decoder_from_trace_data_string(Some(&fifteen));
    ck.check(
        "T5 the >=15-field Pub branch restores clientHost",
        old.first().and_then(|c| c.trace_beans.first()).map(|b| b.client_host.as_str())
            == Some("10.9.8.7:10911"),
        &format!("text={fifteen:?}"),
    );
    ck.check(
        "T5 empty input decodes to nothing",
        TraceDataEncoder::decoder_from_trace_data_string(None).is_empty()
            && TraceDataEncoder::decoder_from_trace_data_string(Some("")).is_empty()
            && TraceDataEncoder::encoder_from_context_bean(None).is_none(),
        "None/empty handling differs",
    );
    println!("        trace text: {:?}", transfer.trans_data);

    // The critical hop over a real broker: send the trace text as a message body and pull it back.
    // In production the trace is sent to RT_TOPIC exactly like this; SOH/STX are control characters
    // and must pass through the broker's commitlog unchanged.
    let body = transfer.trans_data.clone().into_bytes();
    let trace_sent = match send_message(
        client,
        broker_addr,
        broker_name,
        topic,
        producer_group,
        &body,
        "TagTrace",
    )
    .await
    {
        Ok(s) => s,
        Err(e) => return Err(format!("send trace record failed: {e}")),
    };
    let (backed, _) = pull_messages(client, broker_addr, topic, &format!("CID_unused_{topic}"), 0, trace_sent.queue_offset, 32)
        .await?;
    let pulled_trace = match backed.first() {
        Some(m) => m,
        None => return Err("trace record message was not pulled back".to_string()),
    };
    let text = String::from_utf8_lossy(pulled_trace.get_body()).into_owned();
    ck.check(
        "T5 the SOH/STX trace text survives a real broker round trip",
        text == transfer.trans_data,
        &format!("before={:?} after={:?}", transfer.trans_data, text),
    );
    let online = TraceDataEncoder::decoder_from_trace_data_string(Some(&text));
    ck.check(
        "T5 a record read back from the broker still decodes identically",
        online.first().and_then(|c| c.trace_beans.first()).map(|b| b.msg_id.as_str())
            == Some(uniq_id.as_str()),
        &format!("contexts={}", online.len()),
    );
    Ok(())
}

// ------------------------------------------------------------------ T6

async fn t6_metrics(
    client: &RemotingClient,
    broker_addr: &str,
    broker_name: &str,
    topic: &str,
    producer_group: &str,
    sent: &Sent,
    ck: &mut Checker,
) -> Live {
    let metrics = ClientMetrics::new();
    let snap = metrics.snapshot();
    ck.check(
        "T6 a fresh snapshot is all zeroes",
        snap.send_count == 0
            && snap.send_failure_count == 0
            && snap.consume_count == 0
            && snap.send_rt_avg == 0.0
            && snap.send_rt_min == 0.0,
        &format!("{snap}"),
    );

    // (1) A real send to the broker: one successful bookkeeping entry.
    let a_start = metrics.record_send_start();
    let live = send_message(
        client,
        broker_addr,
        broker_name,
        topic,
        producer_group,
        b"rust-live-client-metrics",
        "TagRustLiveClient",
    )
    .await?;
    metrics.record_send_success(a_start);
    // (2) The second success is deliberately slower (60ms) so min/max have a decidable order.
    let b_start = metrics.record_send_start();
    tokio::time::sleep(Duration::from_millis(60)).await;
    metrics.record_send_success(b_start);
    // (3) One real failure: an address that cannot be connected to.
    let c_start = metrics.record_send_start();
    let _ = rpc_timed(
        client,
        DEAD_ADDR,
        RemotingCommand::create_request_command(
            request_code::GET_ROUTEINFO_BY_TOPIC,
            Some(Box::new(GetRouteInfoRequestHeader {
                topic: Some(topic.to_string()),
                accept_standard_json_only: None,
            })),
        ),
        500,
    )
    .await;
    metrics.record_send_failure(c_start);

    let snap = metrics.snapshot();
    ck.check(
        "T6 successes and failures are counted apart",
        snap.send_count == 2 && snap.send_failure_count == 1,
        &format!("{snap}"),
    );
    ck.check(
        "T6 sendRTAvg is rtSum / successCount",
        (snap.send_rt_avg * 2.0 - snap.send_rt_sum).abs() <= 0.002
            && snap.send_rt_sum > snap.send_rt_avg,
        &format!("{snap}"),
    );
    ck.check(
        "T6 rtSum also carries the failed attempt",
        snap.send_rt_sum >= 60.0,
        &format!("{snap}"),
    );
    ck.check(
        "T6 min is the real broker send, max is the deliberately slow one",
        snap.send_rt_min < 60.0 && snap.send_rt_max >= 55.0 && snap.send_rt_min <= snap.send_rt_max,
        &format!("min={} max={}", snap.send_rt_min, snap.send_rt_max),
    );

    // Consume side: bookkeeping with the real message count.
    let c_start = metrics.record_consume_start();
    metrics.record_consume_success(c_start);
    let c_fail = metrics.record_consume_start();
    metrics.record_consume_failure(c_fail);
    let snap = metrics.snapshot();
    ck.check(
        "T6 consume counters mirror the send side",
        snap.consume_count == 1 && snap.consume_failure_count == 1,
        &format!("{snap}"),
    );
    let json = snap.to_json_value();
    ck.check(
        "T6 snapshot JSON keeps the camelCase key names",
        json.get("sendCount").is_some()
            && json.get("sendFailureCount").is_some()
            && json.get("consumeRTAvg").is_some()
            && json.get("sendRTSum").is_some(),
        &format!("{json}"),
    );
    ck.check(
        "T6 snapshot Display prints the ClientMetrics{...} form",
        snap.to_string().starts_with("ClientMetrics{"),
        &format!("{snap}"),
    );
    ck.check(
        "T6 round3 trims to three decimals",
        round3(1.23456) == 1.235 && round3(2.0) == 2.0,
        &format!("round3(1.23456)={}", round3(1.23456)),
    );
    // The recorded metrics are of the same magnitude as the real send latency (local loopback is necessarily < 1s).
    ck.check(
        "T6 the recorded real RT is plausible",
        sent.rt_millis >= 0 && sent.rt_millis < 1000 && live.rt_millis >= 0,
        &format!("send rt={}ms again={}ms", sent.rt_millis, live.rt_millis),
    );
    Ok(())
}

// ------------------------------------------------------------------ T7

async fn t7_request_reply(
    topic: &str,
    cluster_expected: &str,
    pulled: &[MessageExt],
    ck: &mut Checker,
) -> Live {
    let mut ids = HashMap::new();
    for _ in 0..2000 {
        let id = create_correlation_id();
        let chars: Vec<char> = id.chars().collect();
        if chars.len() != 36
            || chars[8] != '-'
            || chars[13] != '-'
            || chars[18] != '-'
            || chars[23] != '-'
            || chars[14] != '4'
            || !matches!(chars[19], '8' | '9' | 'a' | 'b')
        {
            return Err(format!("correlationId {id:?} is not UUID-v4 shaped"));
        }
        ids.insert(id, ());
    }
    ck.check(
        "T7 2000 correlationIds are unique and UUID-v4 shaped",
        ids.len() == 2000,
        &format!("unique={}", ids.len()),
    );

    let holder = request_future_holder();
    let before = holder.len();
    let cid = create_correlation_id();
    let future = Arc::new(RequestResponseFuture::new(&cid, 3000));
    holder.put_request(&cid, Arc::clone(&future));
    ck.check(
        "T7 the holder registers the pending request",
        holder.len() == before + 1 && holder.get_request(&cid).is_some(),
        &format!("len={} before={before}", holder.len()),
    );

    let response = match pulled.first() {
        Some(m) => m.clone(),
        None => return Err("no real message to use as a reply".to_string()),
    };
    let delivered = holder.put_response(Some(&cid), response.clone());
    ck.check(
        "T7 delivering the reply pops the slot",
        delivered.is_some() && holder.len() == before && holder.get_request(&cid).is_none(),
        &format!("len={} before={before}", holder.len()),
    );
    let got = future.wait_response_message(0).await;
    ck.check(
        "T7 the waiter receives the real message",
        got.as_ref().and_then(|m| m.msg_id.clone()) == response.msg_id,
        &format!("got msgId={:?}", got.and_then(|m| m.msg_id)),
    );
    ck.check(
        "T7 a duplicate reply finds no slot",
        holder.put_response(Some(&cid), response.clone()).is_none(),
        "second put_response should return None",
    );

    // Timeout: with no reply, wait must return None on schedule and must not panic.
    let cid2 = create_correlation_id();
    let pending = Arc::new(RequestResponseFuture::new(&cid2, 30));
    holder.put_request(&cid2, Arc::clone(&pending));
    let started = Instant::now();
    ck.check(
        "T7 an unanswered request times out to None",
        pending.wait_response_message(30).await.is_none(),
        "wait_response_message returned a message",
    );
    ck.check(
        "T7 the timeout is honoured",
        started.elapsed() < Duration::from_millis(2000),
        &format!("elapsed={:?}", started.elapsed()),
    );
    ck.check(
        "T7 removeRequest is idempotent",
        holder.remove_request(&cid2).is_some() && holder.remove_request(&cid2).is_none(),
        "double remove should yield None the second time",
    );
    let orphan = Arc::new(RequestResponseFuture::new("orphan", 1000));
    orphan.set_failed(Error::client("send failed before the broker answered"));
    ck.check(
        "T7 a failed send marks the future and still wakes waiters",
        !orphan.is_send_request_ok() && orphan.wait_response_message(0).await.is_none(),
        "set_failed did not mark the future",
    );

    // The real reply topic can only come from the CLUSTER property written by the broker.
    let request_msg = response.to_message();
    let cluster = request_msg
        .get_property(message_const::PROPERTY_CLUSTER)
        .unwrap_or_default()
        .to_string();
    ck.check(
        "T7 the broker stamped CLUSTER on the stored message",
        cluster == cluster_expected,
        &format!("cluster={cluster:?} want={cluster_expected:?}"),
    );
    let reply = match create_reply_message(&request_msg, b"rust-reply-body") {
        Ok(m) => m,
        Err(e) => return Err(format!("create_reply_message failed: {e}")),
    };
    let expected_reply_topic = format!("{cluster}_REPLY_TOPIC");
    ck.check(
        "T7 reply topic is <cluster>_REPLY_TOPIC and MSG_TYPE is reply",
        reply.get_topic() == expected_reply_topic && is_reply_message(&reply),
        &format!("topic={} want={expected_reply_topic} type={:?}", reply.get_topic(), reply.get_property(message_const::PROPERTY_MESSAGE_TYPE)),
    );
    ck.check(
        "T7 the original request is not itself a reply message",
        !is_reply_message(&request_msg),
        &format!("msgType={:?}", request_msg.get_property(message_const::PROPERTY_MESSAGE_TYPE)),
    );
    let mut no_cluster = Message::new(topic, Some(b"x"));
    no_cluster.set_topic(topic);
    match create_reply_message(&no_cluster, b"y") {
        Ok(_) => ck.check(
            "T7 create_reply_message refuses a request without CLUSTER",
            false,
            "missing CLUSTER must be an error",
        ),
        Err(e) => {
            let code = e.response_code();
            ck.check(
                "T7 create_reply_message refuses a request without CLUSTER (10007)",
                code == Some(10007)
                    && e.to_string().contains("property[CLUSTER] is null."),
                &format!("code={code:?} err={e}"),
            );
        }
    }
    let _ = no_cluster.get_topic();
    Ok(())
}

// ------------------------------------------------------------------ T8

async fn t8_cleanup(
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
        "T8 DELETE_TOPIC_IN_BROKER SUCCESS",
        resp.code == response_code::SUCCESS,
        &format!("code={} remark={:?}", resp.code, resp.remark),
    );
    // 5.x defaults deleteTopicWithBrokerRegistration=false => the namesrv route must also be deleted explicitly.
    let mut del = RemotingCommand::create_request_command(request_code::DELETE_TOPIC_IN_NAMESRV, None);
    del.add_ext_field("topic", topic);
    let resp = rpc(client, namesrv, del, 5000).await?;
    ck.check(
        "T8 DELETE_TOPIC_IN_NAMESRV SUCCESS",
        resp.code == response_code::SUCCESS,
        &format!("code={} remark={:?}", resp.code, resp.remark),
    );
    let after = get_route(client, namesrv, topic).await?;
    ck.check(
        "T8 route gone after cleanup",
        after.code == response_code::TOPIC_NOT_EXIST,
        &format!("code={} remark={:?}", after.code, after.remark),
    );
    Ok(())
}

// ----------------------------------------------------------------- main

#[tokio::main]
async fn main() -> ExitCode {
    let argv: Vec<String> = env::args().collect();
    let namesrv = argv
        .get(1)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:9876".to_string());

    let stamp = stamp();
    let topic = format!("RustLiveClient_{stamp}");
    let producer_group = format!("PID_rust_client_{stamp}");
    let consumer_group = format!("CID_rust_client_{stamp}");

    println!("== rocketmq rust client live client-module test ==");
    println!("   namesrv   = {namesrv}");
    println!("   topic     = {topic}");
    println!("   groups    = {producer_group} / {consumer_group}");

    let client = RemotingClient::new();
    let mut ck = Checker::new();

    if let Err(e) = bootstrap_topic(&client, &namesrv, &topic).await {
        ck.abort("T0 bootstrap topic", &e);
        client.shutdown();
        report(&mut ck);
        return ExitCode::FAILURE;
    }
    let route = match get_route(&client, &namesrv, &topic)
        .await
        .and_then(|r| decode_route(&r, "route"))
    {
        Ok(r) => r,
        Err(e) => {
            ck.abort("T0 route lookup", &e);
            client.shutdown();
            report(&mut ck);
            return ExitCode::FAILURE;
        }
    };
    let (broker_name, broker_addr) = match route.broker_datas.first() {
        Some(bd) => match bd.select_broker_addr() {
            Some(addr) => (bd.broker_name.clone(), addr),
            None => {
                ck.abort("T0 broker address", &format!("broker {} has no address", bd.broker_name));
                client.shutdown();
                report(&mut ck);
                return ExitCode::FAILURE;
            }
        },
        None => {
            ck.abort("T0 broker address", "route has no brokerData");
            client.shutdown();
            report(&mut ck);
            return ExitCode::FAILURE;
        }
    };
    println!("   broker    = {broker_name} @ {broker_addr}");

    // Three real messages: the TPS for T3, the batch for T4, and the field source for T5/T7.
    let mut sent = Vec::new();
    for i in 0..3 {
        let body = format!("rust-live-client-{topic}-{i}").into_bytes();
        match send_message(&client, &broker_addr, &broker_name, &topic, &producer_group, &body, "TagRustLiveClient").await {
            Ok(s) => sent.push(s),
            Err(e) => {
                ck.abort("T0 send warm-up messages", &e);
                break;
            }
        }
    }
    let first_offset = match sent.first() {
        Some(s) => s.queue_offset,
        None => {
            ck.abort("T0 no warm-up message sent", "skipping the whole run");
            client.shutdown();
            report(&mut ck);
            return ExitCode::FAILURE;
        }
    };
    let (pulled, _) = match pull_until(&client, &broker_addr, &topic, &consumer_group, first_offset, sent.len()).await {
        Ok(v) => v,
        Err(e) => {
            ck.abort("T0 pull warm-up messages", &e);
            (Vec::new(), 0)
        }
    };
    println!(
        "        live data: sent={} pulled={} offsets={}",
        sent.len(),
        pulled.len(),
        sent.iter().map(|s| s.queue_offset.to_string()).collect::<Vec<_>>().join(",")
    );
    let Some(primary) = sent.first() else {
        client.shutdown();
        report(&mut ck);
        return ExitCode::FAILURE;
    };

    let scenarios: Vec<(&str, Live)> = vec![
        ("T1 top_addressing", t1_top_addressing(&client, &namesrv, &topic, &mut ck).await),
        ("T2 latency", t2_latency(&client, &namesrv, &topic, &broker_name, &broker_addr, primary, &mut ck).await),
        (
            "T3 consumer_stats",
            t3_consumer_stats(&client, &broker_addr, &topic, &consumer_group, &pulled, primary.rt_millis, &mut ck).await,
        ),
        (
            "T4 hook",
            t4_hooks(&topic, &producer_group, &consumer_group, &broker_addr, primary, &pulled, &mut ck).await,
        ),
        (
            "T5 trace",
            t5_trace(&client, &broker_addr, &broker_name, &topic, &producer_group, &pulled, &mut ck).await,
        ),
        (
            "T6 metrics",
            t6_metrics(&client, &broker_addr, &broker_name, &topic, &producer_group, primary, &mut ck).await,
        ),
        ("T7 request_reply", t7_request_reply(&topic, "DefaultCluster", &pulled, &mut ck).await),
    ];
    for (name, outcome) in scenarios {
        if let Err(e) = outcome {
            ck.abort(name, &e);
        }
    }

    if let Err(e) = t8_cleanup(&client, &namesrv, &broker_addr, &topic, &mut ck).await {
        ck.abort("T8 cleanup", &e);
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
