//! A real cluster cannot produce `SYSTEM_BUSY`, nor a slow broker eating the
//! total budget -- and these two are exactly the whole difficulty of this kernel,
//! so this starts an in-process **mock cluster** (1 name server + N brokers, remoting protocol only) that scripts each broker's response code and response delay.
//!
//! A later "unitMode / enableStreamRequestType" section uses the same mock cluster for
//! **wire-message** assertions (`unitMode` lands on the V2 header's single-letter key `k`; `ReqT` is written by the stream hook before encode).

#![cfg(test)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use super::*;
use crate::client::latency::ISOLATION_LATENCY;
use crate::common::sysflag::PermName;
use crate::remoting::protocol::codes::{request_code, response_code};
use crate::remoting::protocol::route::{BrokerData, QueueData, TopicRouteData};

const MAX_FRAME: i32 = 20 * 1024 * 1024;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------- mock cluster

/// Response code in the script meaning "read the request, snapshot it, then **close the connection without answering**".
const CLOSE_WITHOUT_ANSWER: i32 = -1;

/// Per-broker script: pops `(response code, sleep millis before answering)` in order; once exhausted, keeps using `tail`.
#[derive(Debug)]
struct BrokerScript {
    steps: VecDeque<(i32, u64)>,
    tail: (i32, u64),
    requests: usize,
    /// extFields snapshot of each SEND request **as it goes on the wire** (hooks already ran; equivalent to the real message).
    sends: Vec<Vec<(String, String)>>,
    /// Request code aligned 1:1 with `sends` (310 single / 320 batch / 325 reply).
    send_codes: Vec<i32>,
    /// `opaque` aligned 1:1 with `sends`: async retry must reuse the same request with a **fresh opaque**,
    /// the only place where "the opaque was rotated" is observable.
    send_opaques: Vec<i32>,
    /// Body aligned 1:1 with `sends`: batch sending verifies against it that "each sub-message's UNIQ_KEY is written before encoding".
    send_bodies: Vec<Vec<u8>>,
}

impl BrokerScript {
    fn new() -> BrokerScript {
        BrokerScript {
            steps: VecDeque::new(),
            tail: (response_code::SUCCESS, 0),
            requests: 0,
            sends: Vec::new(),
            send_codes: Vec::new(),
            send_opaques: Vec::new(),
            send_bodies: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
struct ClusterState {
    /// `false` ⇒ name server answers `TOPIC_NOT_EXIST` for every topic (no route obtainable)
    route_ok: bool,
    brokers: Vec<BrokerScript>,
}

/// In-process mock cluster. Broker names are fixed to `broker-0..N`, and the queue order in
/// the route matches them, so the first send necessarily lands on `broker-0` (queue selection is round-robin, cursor starts at 0).
struct MockCluster {
    namesrv_addr: String,
    /// Listen addresses of `broker-0..N` from the route, in route order. Under `with_addrs` these are exactly the ones passed in.
    broker_addrs: Vec<String>,
    state: Arc<Mutex<ClusterState>>,
    /// Held only, never awaited: aborted when the test runtime finishes.
    tasks: Vec<JoinHandle<()>>,
}

impl MockCluster {
    /// Start `broker_count` genuinely listening brokers + a name server.
    async fn start(broker_count: usize, route_ok: bool) -> MockCluster {
        let mut listeners = Vec::new();
        let mut addrs = Vec::new();
        for _ in 0..broker_count {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock broker");
            addrs.push(listener.local_addr().expect("mock broker addr").to_string());
            listeners.push(listener);
        }
        let mut cluster = MockCluster::with_addrs(addrs, route_ok).await;
        for (index, listener) in listeners.into_iter().enumerate() {
            let state = Arc::clone(&cluster.state);
            cluster.tasks.push(spawn_broker(listener, index, state));
        }
        cluster
    }

    /// Start only the name server; the route points at the given addresses (an already-closed port can produce a connect failure).
    async fn with_addrs(broker_addrs: Vec<String>, route_ok: bool) -> MockCluster {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock namesrv");
        let namesrv_addr = listener.local_addr().expect("mock namesrv addr").to_string();
        let count = broker_addrs.len();
        let state = Arc::new(Mutex::new(ClusterState {
            route_ok,
            brokers: (0..count).map(|_| BrokerScript::new()).collect(),
        }));
        let tasks = vec![spawn_namesrv(listener, Arc::clone(&state), broker_addrs.clone())];
        MockCluster { namesrv_addr, broker_addrs, state, tasks }
    }

    /// Script the `index`-th broker: answer per `steps` in order first, then keep using `tail`.
    fn script(&self, index: usize, steps: Vec<(i32, u64)>, tail: (i32, u64)) {
        let mut state = lock(&self.state);
        let broker = &mut state.brokers[index];
        broker.steps = steps.into();
        broker.tail = tail;
        broker.requests = 0;
        broker.sends.clear();
        broker.send_codes.clear();
        broker.send_opaques.clear();
        broker.send_bodies.clear();
    }

    /// Number of SEND requests received by the `index`-th broker.
    fn requests(&self, index: usize) -> usize {
        lock(&self.state).brokers[index].requests
    }

    /// extFields snapshot of the `n`-th SEND received by the `index`-th broker.
    fn send_ext(&self, index: usize, n: usize) -> Vec<(String, String)> {
        lock(&self.state).brokers[index].sends[n].clone()
    }

    /// Body snapshot of the `n`-th SEND received by the `index`-th broker.
    fn send_body(&self, index: usize, n: usize) -> Vec<u8> {
        lock(&self.state).brokers[index].send_bodies[n].clone()
    }

    /// Request code of the `n`-th SEND received by the `index`-th broker.
    fn send_code(&self, index: usize, n: usize) -> i32 {
        lock(&self.state).brokers[index].send_codes[n]
    }

    /// `opaque` of the `n`-th SEND received by the `index`-th broker.
    fn send_opaque(&self, index: usize, n: usize) -> i32 {
        lock(&self.state).brokers[index].send_opaques[n]
    }
}

/// An address with **no listener**: connecting to it is refused at once (`Error::Connect`).
///
/// Binding then closing immediately is more reliable than inventing a port out of thin air (that port might be taken over by someone at any moment).
async fn dead_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind then close");
    let addr = listener.local_addr().expect("dead addr").to_string();
    drop(listener);
    addr
}

fn is_send_code(code: i32) -> bool {
    matches!(
        code,
        request_code::SEND_MESSAGE
            | request_code::SEND_MESSAGE_V2
            | request_code::SEND_BATCH_MESSAGE
            | request_code::SEND_REPLY_MESSAGE
            | request_code::SEND_REPLY_MESSAGE_V2
    )
}

async fn read_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut len_buf = [0_u8; 4];
    stream.read_exact(&mut len_buf).await.ok()?;
    let total = i32::from_be_bytes(len_buf);
    if total <= 0 || total > MAX_FRAME {
        return None;
    }
    let mut frame = vec![0_u8; 4 + total as usize];
    frame[..4].copy_from_slice(&len_buf);
    stream.read_exact(&mut frame[4..]).await.ok()?;
    Some(frame)
}

async fn write_frame(stream: &mut TcpStream, response: &mut RemotingCommand) {
    let bytes = response.encode();
    let _ = stream.write_all(&bytes).await;
    let _ = stream.flush().await;
}

/// Response carrying the request's `opaque` (the client pairs by opaque; mismatched ones are dropped as noise).
fn response_for(request: &RemotingCommand, code: i32) -> RemotingCommand {
    let mut response = RemotingCommand::create_response(code, Some("mock failure".to_string()));
    response.opaque = request.opaque;
    response.serialize_type_current_rpc = request.serialize_type_current_rpc;
    response
}

fn route_json(broker_addrs: &[String]) -> Vec<u8> {
    let route = TopicRouteData {
        queue_datas: broker_addrs
            .iter()
            .enumerate()
            .map(|(i, _)| {
                QueueData::new(
                    format!("broker-{i}"),
                    1,
                    1,
                    PermName::PERM_READ | PermName::PERM_WRITE,
                    0,
                )
            })
            .collect(),
        broker_datas: broker_addrs
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                BrokerData::new(
                    "MockCluster",
                    format!("broker-{i}"),
                    vec![(i64::from(MixAll::MASTER_ID), addr.clone())],
                    "",
                )
            })
            .collect(),
        ..Default::default()
    };
    serde_json::to_vec(&route.to_json_value()).expect("route must serialize")
}

/// namesrv: answers only `GET_ROUTEINFO_BY_TOPIC`, everything else gets SUCCESS.
fn spawn_namesrv(
    listener: TcpListener,
    state: Arc<Mutex<ClusterState>>,
    broker_addrs: Vec<String>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else { return };
            let state = Arc::clone(&state);
            let addrs = broker_addrs.clone();
            tokio::spawn(async move {
                while let Some(frame) = read_frame(&mut stream).await {
                    let Ok(request) = RemotingCommand::decode(&frame) else { return };
                    let mut response = if request.code == request_code::GET_ROUTEINFO_BY_TOPIC {
                        let ok = lock(&state).route_ok;
                        if !ok {
                            response_for(&request, response_code::TOPIC_NOT_EXIST)
                        } else {
                            let mut r = response_for(&request, response_code::SUCCESS);
                            r.set_body(Some(route_json(&addrs)));
                            r
                        }
                    } else {
                        response_for(&request, response_code::SUCCESS)
                    };
                    write_frame(&mut stream, &mut response).await;
                }
            });
        }
    })
}

/// broker: SENDs answer per the script, other requests (heartbeat etc.) get SUCCESS.
fn spawn_broker(
    listener: TcpListener,
    index: usize,
    state: Arc<Mutex<ClusterState>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else { return };
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                while let Some(frame) = read_frame(&mut stream).await {
                    let Ok(request) = RemotingCommand::decode(&frame) else { return };
                    if !is_send_code(request.code) {
                        if !request.is_oneway_rpc() {
                            let mut response = response_for(&request, response_code::SUCCESS);
                            write_frame(&mut stream, &mut response).await;
                        }
                        continue;
                    }
                    let (code, delay, seq) = {
                        let mut state = lock(&state);
                        let broker = &mut state.brokers[index];
                        let step = broker.steps.pop_front().unwrap_or(broker.tail);
                        broker.requests += 1;
                        // The snapshot must come before answering: what is captured here is the header after the hooks ran, exactly the one that goes on the wire
                        broker.sends.push(
                            request
                                .ext_fields()
                                .iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect(),
                        );
                        broker.send_codes.push(request.code);
                        broker.send_opaques.push(request.opaque);
                        broker.send_bodies.push(request.body().unwrap_or_default().to_vec());
                        (step.0, step.1, broker.requests)
                    };
                    if code == CLOSE_WITHOUT_ANSWER {
                        // Drop the connection: the client sees `Error::SendRequest` (connection closed)
                        return;
                    }
                    if delay > 0 {
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                    }
                    if request.is_oneway_rpc() {
                        continue;
                    }
                    let mut response = response_for(&request, code);
                    if code == response_code::SUCCESS {
                        response.add_ext_field("msgId", &format!("MOCK-{index}-{seq}"));
                        response.add_ext_field("queueId", "0");
                        response.add_ext_field("queueOffset", "7");
                    } else {
                        response.remark = Some(format!("mock broker-{index} says no"));
                    }
                    write_frame(&mut stream, &mut response).await;
                }
            });
        }
    })
}

// ------------------------------------------------------------- producer fixture

/// Start a producer pointed at the mock cluster. `instance` must be unique per test case: the same clientId shares one
/// [`MQClientInstance`] (together with its route table and name server address).
async fn started(instance: &str, cluster: &MockCluster) -> DefaultMQProducer {
    let producer = DefaultMQProducer::new("GID_send_retry").expect("group name is valid");
    producer.set_instance_name(instance);
    producer.set_namesrv_addr(&cluster.namesrv_addr);
    producer.start().await.expect("start should succeed against the mock cluster");
    producer
}

/// Print the error on failure so assertions stay readable.
fn expect_client_code(err: Error) -> (Option<i32>, String) {
    match err {
        Error::Client { response_code, message } => (response_code, message),
        other => panic!("expected an MQClientException, got {other}"),
    }
}

// ---------------------------------------------------------------- test cases

#[test]
fn default_retry_response_codes_match_protocol() {
    let cfg = ProducerConfig::default();
    let expected: BTreeSet<i32> = [
        response_code::SYSTEM_ERROR,
        response_code::SYSTEM_BUSY,
        response_code::SERVICE_NOT_AVAILABLE,
        response_code::NO_PERMISSION,
        response_code::TOPIC_NOT_EXIST,
        response_code::NO_BUYER_ID,
        response_code::NOT_IN_CURRENT_UNIT,
        response_code::GO_AWAY,
    ]
    .into_iter()
    .collect();
    assert_eq!(cfg.retry_response_codes, expected);
    assert_eq!(cfg.send_msg_max_timeout_per_request, -1);
}

/// The three error tiers differ in both the fault table and the retry decision; they cannot be collapsed into one `retryable`.
#[test]
fn send_errors_classify_by_variant_arms() {
    let cases: [(&Error, Option<SendErrorKind>); 9] = [
        (
            &Error::Broker { response_code: 2, message: "busy".into() },
            Some(SendErrorKind::Broker),
        ),
        (&Error::Connect { addr: "a:1".into() }, Some(SendErrorKind::Remoting)),
        (
            &Error::SendRequest { addr: "a:1".into(), message: "io".into() },
            Some(SendErrorKind::Remoting),
        ),
        (
            &Error::Timeout { addr: "a:1".into(), timeout_millis: 3000 },
            Some(SendErrorKind::Remoting),
        ),
        (
            &Error::TooMuchRequest("full".into()),
            Some(SendErrorKind::Remoting),
        ),
        (
            &Error::RemotingCommand("bad".into()),
            Some(SendErrorKind::Remoting),
        ),
        (&Error::client("no queue"), Some(SendErrorKind::Client)),
        (
            &Error::request_timeout("T1", 100),
            Some(SendErrorKind::Client),
        ),
        (&Error::Decode("bad frame".into()), None),
    ];
    for (err, expected) in cases {
        assert_eq!(classify_send_error(err), expected, "{err}");
    }
}

/// The wall clock can roll back, so the retry budget may only use the monotonic clock (a negative latency would record a slow broker as a fast one).
#[test]
fn latency_since_never_goes_backwards() {
    let began = monotonic_millis();
    std::thread::sleep(Duration::from_millis(15));
    let first = latency_since(began);
    let second = latency_since(began);
    assert!(first >= 10, "slept 15ms, measured {first}ms");
    assert!(second >= first, "monotonic clock must not go backwards: {first} -> {second}");
}

#[tokio::test]
async fn retryable_broker_code_switches_to_another_broker() {
    let cluster = MockCluster::start(2, true).await;
    cluster.script(0, vec![(response_code::SYSTEM_BUSY, 0)], (response_code::SUCCESS, 0));
    cluster.script(1, vec![], (response_code::SUCCESS, 0));
    let producer = started("retry_switch", &cluster).await;

    let mut msg = Message::new("T1", Some(b"body"));
    let result = producer.send(&mut msg, None, None).await.expect("a retryable code should switch brokers and succeed");
    assert_eq!(result.status, SendStatus::SendOk);
    assert_eq!(cluster.requests(0), 1, "the first broker answered SYSTEM_BUSY");
    assert_eq!(cluster.requests(1), 1, "the second broker was polled once");
    producer.shutdown();
}

#[tokio::test]
async fn non_retryable_broker_code_throws_at_once() {
    let cluster = MockCluster::start(2, true).await;
    cluster.script(0, vec![], (response_code::MESSAGE_ILLEGAL, 0));
    cluster.script(1, vec![], (response_code::SUCCESS, 0));
    let producer = started("retry_illegal", &cluster).await;

    let mut msg = Message::new("T1", Some(b"body"));
    let err = producer.send(&mut msg, None, None).await.expect_err("MESSAGE_ILLEGAL must be thrown verbatim");
    assert_eq!(err.response_code(), Some(response_code::MESSAGE_ILLEGAL));
    assert!(matches!(err, Error::Broker { .. }), "the thrown error should be an MQBrokerException: {err}");
    assert_eq!(cluster.requests(0), 1);
    assert_eq!(cluster.requests(1), 0, "a deterministic error should not try the second broker");
    producer.shutdown();
}

#[tokio::test]
async fn added_retry_response_code_makes_a_code_retryable() {
    let cluster = MockCluster::start(2, true).await;
    cluster.script(0, vec![], (response_code::MESSAGE_ILLEGAL, 0));
    cluster.script(1, vec![], (response_code::SUCCESS, 0));
    let producer = started("retry_added", &cluster).await;
    producer.add_retry_response_code(response_code::MESSAGE_ILLEGAL);
    assert!(producer.is_retry_response_code(Some(response_code::MESSAGE_ILLEGAL)));
    // Never got a response code at all (the connection never came up) equals non-retryable: an empty code is not in the retryable set
    assert!(!producer.is_retry_response_code(None));

    let mut msg = Message::new("T1", Some(b"body"));
    let result = producer.send(&mut msg, None, None).await.expect("adding the code should make it switch brokers");
    assert_eq!(result.status, SendStatus::SendOk);
    assert_eq!(cluster.requests(1), 1);
    producer.shutdown();
}

#[tokio::test]
async fn exhausted_retries_report_brokers_sent_and_code() {
    let cluster = MockCluster::start(2, true).await;
    cluster.script(0, vec![], (response_code::SYSTEM_BUSY, 0));
    cluster.script(1, vec![], (response_code::SYSTEM_BUSY, 0));
    let producer = started("retry_exhausted", &cluster).await;
    producer.set_retry_times_when_send_failed(2);

    let mut msg = Message::new("T1", Some(b"body"));
    let err = producer.send(&mut msg, None, None).await.expect_err("all attempts failing must throw");
    let (code, message) = expect_client_code(err);
    // The last failure carries a broker code ⇒ that code is reused
    assert_eq!(code, Some(response_code::SYSTEM_BUSY));
    assert!(message.contains("Send [3] times"), "{message}");
    assert!(message.contains("Topic: T1"), "{message}");
    assert!(
        message.contains("BrokersSent: [broker-0, broker-1, broker-0]"),
        "{message}"
    );
    assert!(message.contains("last error: MQBrokerException"), "{message}");
    assert_eq!(cluster.requests(0) + cluster.requests(1), 3);
    producer.shutdown();
}

#[tokio::test]
async fn missing_route_fails_fast_with_not_found_topic_code() {
    let cluster = MockCluster::start(1, false).await;
    let producer = started("retry_no_route", &cluster).await;

    let mut msg = Message::new("T1", Some(b"body"));
    let err = producer.send(&mut msg, None, None).await.expect_err("a missing route must fail");
    let (code, message) = expect_client_code(err);
    assert_eq!(code, Some(client_error_code::NOT_FOUND_TOPIC_EXCEPTION));
    assert!(message.contains("Message Queue"), "{message}");
    assert_eq!(cluster.requests(0), 0, "with no route at all, not even one request should be sent");
    producer.shutdown();
}

#[tokio::test]
async fn connect_failure_is_qualified_with_10001() {
    // Occupy a port first and release it at once; the route points at it ⇒ the connect must be refused
    let dead_addr = {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind dead port");
        listener.local_addr().expect("dead addr").to_string()
    };
    let cluster = MockCluster::with_addrs(vec![dead_addr], true).await;
    let producer = started("retry_connect", &cluster).await;

    let mut msg = Message::new("T1", Some(b"body"));
    // A loopback refusal for a "just-closed port" can be delayed to ~2s by the firewall: the default 3s
    // budget cannot fit 3 retries, so it is judged callTimeout and throws TooMuchRequest; widen it so the retries run to the end and land on 10001.
    let err = producer
        .send(&mut msg, Some(30_000), None)
        .await
        .expect_err("an unreachable target must fail");
    let (code, message) = expect_client_code(err);
    assert_eq!(code, Some(client_error_code::CONNECT_BROKER_EXCEPTION), "{message}");
    producer.shutdown();
}

// ---------------- 6b. an addressing failure (10004) must not be phrased as "this topic has no route" (10005)

/// Without this step, someone who misconfigures the address server (or who gets an empty list back from it)
/// would see "this topic has no route", go check topic creation / permissions, while what is actually broken is addressing -- troubleshooting in the wrong direction can burn half an hour.
#[tokio::test]
async fn no_name_server_address_reports_10004_not_missing_route() {
    let producer = DefaultMQProducer::new("GID_send_retry").expect("group name is valid");
    let client = MQClientInstance::new("retry_no_namesrv", Vec::new());

    let err = producer
        .topic_publish_info(&client, "T1")
        .await
        .expect_err("with not a single name server address, it must fail");
    let (code, message) = expect_client_code(err);
    assert_eq!(code, Some(client_error_code::NO_NAME_SERVER_EXCEPTION));
    assert_eq!(message, "No name server address, please set it.");
}

/// Control leg: the address is configured, only unreachable (a port nobody listens on) ⇒ the 10004 gate must stay silent.
#[tokio::test]
async fn unreachable_name_server_is_never_reported_as_10004() {
    let addr = dead_addr().await;
    let producer = DefaultMQProducer::new("GID_send_retry").expect("group name is valid");
    producer.set_instance_name("retry_dead_namesrv");
    producer.set_namesrv_addr(&addr);
    producer.start().await.expect("a non-empty configuration should pass the gate");

    let mut msg = Message::new("T1", Some(b"body"));
    let err = producer
        .send(&mut msg, Some(3000), None)
        .await
        .expect_err("an unreachable target must fail");
    assert_ne!(
        err.response_code(),
        Some(client_error_code::NO_NAME_SERVER_EXCEPTION),
        "an address in the configuration means 10004 must not be reported: {err}"
    );
    producer.shutdown();
}

#[tokio::test]
async fn producer_start_reports_10004_when_addressing_is_missing() {
    let producer = DefaultMQProducer::new("GID_send_retry").expect("group name is valid");
    producer.set_instance_name("retry_start_noaddr");
    if DefaultTopAddressing::is_configured() {
        return;
    }
    let err = producer.start().await.expect_err("without addressing it must not start");
    assert_eq!(err.response_code(), Some(client_error_code::NO_NAME_SERVER_EXCEPTION), "{err}");
    let (_, message) = expect_client_code(err);
    assert!(message.contains("name server address is not set"), "{message}");
}

/// The entire point of `sendMsgMaxTimeoutPerRequest`: a slow broker may only eat the clamped slice,
/// and the remaining budget is left for the next one. Without the cap the first broker monopolizes 700ms and the whole send is necessarily slower.
#[tokio::test]
async fn per_request_timeout_caps_a_slow_broker_and_retries() {
    let cluster = MockCluster::start(2, true).await;
    cluster.script(0, vec![], (response_code::SYSTEM_BUSY, 700));
    cluster.script(1, vec![], (response_code::SUCCESS, 0));
    let producer = started("retry_clamp", &cluster).await;
    producer.set_send_msg_max_timeout_per_request(150);
    assert_eq!(producer.get_send_msg_max_timeout_per_request(), 150);

    let mut msg = Message::new("T1", Some(b"body"));
    let began = Instant::now();
    let result = producer
        .send(&mut msg, Some(3000), None)
        .await
        .expect("after the clamped per-request timeout it should switch to the healthy broker");
    let elapsed = began.elapsed();
    assert_eq!(result.status, SendStatus::SendOk);
    assert!(
        elapsed < Duration::from_millis(500),
        "per-request timeout was not clamped to 150ms: the whole send took {}ms",
        elapsed.as_millis()
    );
    assert_eq!(cluster.requests(0), 1);
    assert_eq!(cluster.requests(1), 1);
    producer.shutdown();
}

/// Once the first broker eats the whole budget, the remaining brokers never get a single try -- what is
/// thrown then is `RemotingTooMuchRequestException("sendDefaultImpl call timeout")`.
#[tokio::test]
async fn exhausted_budget_reports_call_timeout() {
    let cluster = MockCluster::start(2, true).await;
    cluster.script(0, vec![], (response_code::SYSTEM_BUSY, 500));
    cluster.script(1, vec![], (response_code::SUCCESS, 500));
    let producer = started("retry_budget", &cluster).await;

    let mut msg = Message::new("T1", Some(b"body"));
    let err = producer
        .send(&mut msg, Some(50), None)
        .await
        .expect_err("a 50ms budget meeting 500ms brokers must time out");
    assert!(
        matches!(err, Error::TooMuchRequest(_)),
        "expected sendDefaultImpl call timeout, got {err}"
    );
    assert_eq!(err.to_string(), "too much request: sendDefaultImpl call timeout");
    producer.shutdown();
}

#[tokio::test]
async fn not_store_ok_only_switches_broker_when_configured() {
    // FLUSH_DISK_TIMEOUT is not an exception, it is a normal "stored, but not durably" response
    let cluster = MockCluster::start(2, true).await;
    cluster.script(0, vec![], (response_code::FLUSH_DISK_TIMEOUT, 0));
    cluster.script(1, vec![], (response_code::SUCCESS, 0));
    let producer = started("retry_not_store", &cluster).await;

    let mut msg = Message::new("T1", Some(b"body"));
    let result = producer.send(&mut msg, None, None).await.expect("by default the result is returned as-is");
    assert_eq!(result.status, SendStatus::FlushDiskTimeout);
    assert_eq!(cluster.requests(1), 0, "without the switch on, no broker switch should happen");

    producer.set_retry_another_broker_when_not_store_ok(true);
    assert!(producer.is_retry_another_broker_when_not_store_ok());
    let mut msg2 = Message::new("T1", Some(b"body"));
    let result = producer.send(&mut msg2, None, None).await.expect("with the switch on it should switch brokers");
    assert_eq!(result.status, SendStatus::SendOk);
    assert_eq!(cluster.requests(1), 1);
    producer.shutdown();
}

/// With fault avoidance on: the failed broker must be isolated (unselectable), and the fault table still records the **real latency**;
/// the 10000ms sentinel is substituted only when computing the isolation window.
#[tokio::test]
async fn failed_broker_is_isolated_and_latency_is_recorded() {
    let cluster = MockCluster::start(2, true).await;
    cluster.script(0, vec![], (response_code::SERVICE_NOT_AVAILABLE, 30));
    cluster.script(1, vec![], (response_code::SUCCESS, 0));
    let producer = started("retry_fault", &cluster).await;
    producer.set_send_latency_fault_enable(true);
    assert!(producer.is_retry_response_code(Some(response_code::SERVICE_NOT_AVAILABLE)));

    let mut msg = Message::new("T1", Some(b"body"));
    let result = producer.send(&mut msg, None, None).await.expect("after isolation it should switch to the healthy broker");
    assert_eq!(result.status, SendStatus::SendOk);

    let tolerance = producer.inner.fault_strategy.latency_fault_tolerance();
    assert!(!tolerance.is_available("broker-0"), "a broker that answered an error code must be isolated");
    let failed = tolerance.get_fault_item("broker-0").expect("the failed broker must be in the table");
    assert!(
        failed.get_current_latency() >= 10 && failed.get_current_latency() < ISOLATION_LATENCY,
        "the fault table should record the real latency (around 30ms), got {}",
        failed.get_current_latency()
    );
    assert!(!failed.is_reachable(), "the MQBrokerException tier passes reachable=False");
    let ok = tolerance.get_fault_item("broker-1").expect("the successful broker must be in the table");
    assert!(ok.is_available(tolerance.now_millis()), "a healthy broker should not be isolated");
    assert!(ok.is_reachable(), "the success tier passes reachable=True");
    producer.shutdown();
}

// ---------------------------------------------------------------- unitMode / stream

/// Read a value out of an extFields snapshot (order-independent, looked up by key only).
fn ext_value<'a>(ext: &'a [(String, String)], key: &str) -> Option<&'a str> {
    ext.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

#[tokio::test]
async fn unit_mode_is_carried_in_the_send_header() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));

    let on = DefaultMQProducer::new("GID_send_retry").expect("group name is valid");
    on.set_instance_name("unit_mode_on");
    on.set_namesrv_addr(&cluster.namesrv_addr);
    on.set_unit_mode(true);
    on.start().await.expect("start should succeed against the mock cluster");
    let mut msg = Message::new("T1", Some(b"unit-on"));
    on.send(&mut msg, None, None).await.expect("send should succeed");

    let off = DefaultMQProducer::new("GID_send_retry").expect("group name is valid");
    off.set_instance_name("unit_mode_off");
    off.set_namesrv_addr(&cluster.namesrv_addr);
    off.start().await.expect("default unitMode=false");
    let mut msg = Message::new("T1", Some(b"unit-off"));
    off.send(&mut msg, None, None).await.expect("send should succeed");

    assert_eq!(ext_value(&cluster.send_ext(0, 0), "k"), Some("true"));
    assert_eq!(ext_value(&cluster.send_ext(0, 1), "k"), Some("false"));
    assert_eq!(ext_value(&cluster.send_ext(0, 1), "a"), Some("GID_send_retry"));
    on.shutdown();
    off.shutdown();
}

/// `c`/`d` are a functional bug: the broker's auto topic creation decides the queue count
/// from these two values (the broker's auto-create path reads them off the request), so hardcoding
/// `TBW102`/4 turns `set_create_topic_key` / `set_default_topic_queue_nums` into fake
/// setters. `n` is wire-message parity: the classic broker addresses by connection and does not read it, while the proxy and audit side do.
///
/// The five entry points (sync / config override / batch 320 / oneway / async) must carry the same values, and `d=0`
/// must go on the wire verbatim -- 0 is "the caller explicitly said 0", not "nothing was configured".
#[tokio::test]
async fn send_header_carries_broker_name_and_topic_keys() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = started("send_header_fields", &cluster).await;

    let mut msg = Message::new("T1", Some(b"single"));
    producer.send(&mut msg, None, None).await.expect("send should succeed");
    let ext = cluster.send_ext(0, 0);
    assert_eq!(cluster.send_code(0, 0), request_code::SEND_MESSAGE_V2);
    assert_eq!(
        ext_value(&ext, "n"),
        Some("broker-0"),
        "brokerName must go on the wire under the single-letter key n"
    );
    assert_eq!(ext_value(&ext, "c"), Some(MixAll::DEFAULT_TOPIC));
    assert_eq!(ext_value(&ext, "d"), Some("4"));

    // (2) c/d follow the producer configuration
    producer.set_create_topic_key("CreatedTopicKey");
    producer.set_default_topic_queue_nums(9);
    let mut msg = Message::new("T1", Some(b"configured"));
    producer.send(&mut msg, None, None).await.expect("send should succeed");
    let ext = cluster.send_ext(0, 1);
    assert_eq!(ext_value(&ext, "c"), Some("CreatedTopicKey"));
    assert_eq!(ext_value(&ext, "d"), Some("9"));
    assert_eq!(ext_value(&ext, "n"), Some("broker-0"));

    // (3) batch goes through the same header-building function (code 320, `m=true` is covered by the send_request_code section)
    producer
        .send_batch(
            vec![
                Message::new("T1", Some(b"b-0")),
                Message::new("T1", Some(b"b-1")),
            ],
            None,
            None,
        )
        .await
        .expect("batch send should succeed");
    let ext = cluster.send_ext(0, 2);
    assert_eq!(cluster.send_code(0, 2), request_code::SEND_BATCH_MESSAGE);
    assert_eq!(ext_value(&ext, "n"), Some("broker-0"));
    assert_eq!(ext_value(&ext, "c"), Some("CreatedTopicKey"));
    assert_eq!(ext_value(&ext, "d"), Some("9"));

    // (4)(5) oneway and async must not be missed either; incidentally verify d=0 does not float up to the default
    producer.set_default_topic_queue_nums(0);
    let mut msg = Message::new("T1", Some(b"oneway"));
    producer
        .send_oneway(&mut msg, None)
        .await
        .expect("oneway send should succeed");
    assert!(wait_until(|| cluster.requests(0) >= 4).await, "the oneway request must be captured too");
    let ext = cluster.send_ext(0, 3);
    assert_eq!(ext_value(&ext, "n"), Some("broker-0"));
    assert_eq!(ext_value(&ext, "c"), Some("CreatedTopicKey"));
    assert_eq!(ext_value(&ext, "d"), Some("0"), "0 is the caller's explicit 0");

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(10), cb.clone(), Some(3_000), None)
        .expect("async is dispatchable");
    assert!(wait_until(|| cluster.requests(0) >= 5).await);
    let ext = cluster.send_ext(0, 4);
    assert_eq!(ext_value(&ext, "n"), Some("broker-0"));
    assert_eq!(ext_value(&ext, "c"), Some("CreatedTopicKey"));
    assert_eq!(ext_value(&ext, "d"), Some("0"));
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(cb.ok.load(Ordering::SeqCst), 1);

    producer.shutdown();
}

#[tokio::test]
async fn broker_name_header_follows_the_selected_queue() {
    let cluster = MockCluster::start(2, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    cluster.script(1, vec![], (response_code::SUCCESS, 0));
    let producer = started("send_header_broker_name", &cluster).await;

    let mut first = Message::new("T1", Some(b"to-0"));
    producer.send(&mut first, None, None).await.expect("send should succeed");
    let mut second = Message::new("T1", Some(b"to-1"));
    producer.send(&mut second, None, None).await.expect("send should succeed");
    assert_eq!(ext_value(&cluster.send_ext(0, 0), "n"), Some("broker-0"));
    assert_eq!(ext_value(&cluster.send_ext(1, 0), "n"), Some("broker-1"));

    // When the queue has no broker name (e.g. a manually specified MessageQueue), the key must vanish entirely rather than be written as `n=`
    let client = producer.require_client().expect("client is started");
    let mut msg = Message::new("T1", Some(b"no-broker-name"));
    let mut publish = PublishMessage::Single(&mut msg);
    let mq = MessageQueue::new("T1", "", 0);
    client
        .send_message_to_addr(
            "GID_send_retry",
            &mut publish,
            &mq,
            &cluster.broker_addrs[0],
            3_000,
            0,
            false,
            MixAll::DEFAULT_TOPIC,
            MixAll::DEFAULT_TOPIC_QUEUE_NUMS,
        )
        .await
        .expect("an empty brokerName must not affect the send");
    let ext = cluster.send_ext(0, 1);
    assert!(ext_value(&ext, "n").is_none(), "an empty brokerName must not go on the wire: {ext:?}");
    assert_eq!(ext_value(&ext, "c"), Some(MixAll::DEFAULT_TOPIC));
    assert_eq!(ext_value(&ext, "d"), Some("4"));

    producer.shutdown();
}

/// Why this must be nailed down at the **wire-message** layer: the broker's dead-letter decision reads
/// `requestHeader.reconsumeTimes` and `maxReconsumeTimes` from the send request's header,
/// **not the message properties**. Lifting them wrongly turns the consumer's configured
/// `maxReconsumeTimes` into decoration -- it falls back to the subscription group's default retryMaxTimes(16), so a poison message burns a dozen extra rounds
/// before entering `%DLQ%`. On a real cluster this only shows up as "dead letters arrive late"; only an offline packet capture reveals it.
#[test]
fn retry_topic_properties_are_lifted_into_the_send_header() {
    let mut msg = Message::new("%RETRY%GID_lift", Some(b"poison"));
    msg.put_property(
        crate::common::message_const::PROPERTY_RECONSUME_TIME,
        "4",
    );
    msg.put_property(
        crate::common::message_const::PROPERTY_MAX_RECONSUME_TIMES,
        "6",
    );
    let mq = MessageQueue::new("%RETRY%GID_lift", "broker-0", 0);
    let mut publish = PublishMessage::Single(&mut msg);
    let mut request = MQClientInstance::build_send_request(
        "PID_inner_lift",
        &mut publish,
        &mq,
        0,
        false,
        MixAll::DEFAULT_TOPIC,
        MixAll::DEFAULT_TOPIC_QUEUE_NUMS,
    );
    request.make_custom_header_to_net();
    let ext = request.ext_fields();
    assert_eq!(ext.get("j"), Some("4"), "reconsumeTimes is lifted into j");
    assert_eq!(ext.get("l"), Some("6"), "maxReconsumeTimes is lifted into l");
    let props = ext.get("i").unwrap_or_default();
    assert!(
        props.contains(crate::common::message_const::PROPERTY_RECONSUME_TIME),
        "the property must still go on the wire after being lifted into the header: {props}"
    );
}

/// Reverse control: an ordinary topic must **not** lift these two properties even when it carries them.
#[test]
fn ordinary_topic_never_lifts_reconsume_times() {
    let mut msg = Message::new("T1", Some(b"normal"));
    msg.put_property(
        crate::common::message_const::PROPERTY_RECONSUME_TIME,
        "4",
    );
    msg.put_property(
        crate::common::message_const::PROPERTY_MAX_RECONSUME_TIMES,
        "6",
    );
    let mq = MessageQueue::new("T1", "broker-0", 0);
    let mut publish = PublishMessage::Single(&mut msg);
    let mut request = MQClientInstance::build_send_request(
        "PID_normal",
        &mut publish,
        &mq,
        0,
        false,
        MixAll::DEFAULT_TOPIC,
        MixAll::DEFAULT_TOPIC_QUEUE_NUMS,
    );
    request.make_custom_header_to_net();
    let ext = request.ext_fields();
    assert_eq!(ext.get("j"), Some("0"));
    assert!(!ext.contains_key("l"), "unconfigured means it must not go on the wire: {ext:?}");
}

/// The unit name must appear in both the clientId and the wire message, without affecting the send.
#[tokio::test]
async fn unit_name_only_changes_the_client_id() {
    let cluster = MockCluster::start(1, true).await;
    let producer = DefaultMQProducer::new("GID_send_retry").expect("group name is valid");
    producer.set_instance_name("unit_name_case");
    producer.set_namesrv_addr(&cluster.namesrv_addr);
    producer.set_unit_name(Some("unitA"));
    producer.start().await.expect("start");
    assert_eq!(
        producer.client_id(),
        Some(format!("{}@unit_name_case@unitA", MixAll::cached_ip_str()))
    );
    let mut msg = Message::new("T1", Some(b"body"));
    producer.send(&mut msg, None, None).await.expect("send should succeed");
    assert!(cluster.send_ext(0, 0).iter().all(|(k, _)| k != "unitName"));
    producer.shutdown();
}

/// ⚠ The request code and the V2 header's `m` (batch) are **two different things**: the broker really picks
/// its batch path or a single write by `m` (the send request's batch flag is
/// what the broker reads); the code only affects server-side classification by code (proxy/auth list 310/320 in
/// the same case, both keyed on the request code plus
/// the batch flag). So both must be asserted --
/// aligning the code but not `m` would make a batch body get parsed as a single message.
#[tokio::test]
async fn send_request_code_follows_protocol_three_way_branch() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = started("send_code_branch", &cluster).await;

    let mut single = Message::new("T1", Some(b"single"));
    producer.send(&mut single, None, None).await.expect("send should succeed");
    assert_eq!(cluster.send_code(0, 0), request_code::SEND_MESSAGE_V2);
    assert_eq!(ext_value(&cluster.send_ext(0, 0), "m"), Some("false"));

    let batch = producer
        .send_batch(
            vec![
                Message::new("T1", Some(b"b-0")),
                Message::new("T1", Some(b"b-1")),
            ],
            None,
            None,
        )
        .await;
    batch.expect("batch send should succeed");
    assert_eq!(cluster.send_code(0, 1), request_code::SEND_BATCH_MESSAGE);
    assert_eq!(ext_value(&cluster.send_ext(0, 1), "m"), Some("true"));

    let mut reply_batch = MessageBatch::generate_from_list(vec![
        Message::new("T1", Some(b"r-0")),
        Message::new("T1", Some(b"r-1")),
    ])
    .expect("same topic, not delayed, not a retry: should be legal");
    reply_batch
        .message
        .put_property(
            crate::common::message_const::PROPERTY_MESSAGE_TYPE,
            MixAll::REPLY_MESSAGE_FLAG,
        );
    let mut publish = PublishMessage::Batch(&mut reply_batch);
    let mq = MessageQueue::new("T1", "broker-0", 0);
    let client = producer.require_client().expect("client is started");
    client
        .send_message(
            "GID_send_retry",
            &mut publish,
            &mq,
            3_000,
            0,
            false,
            MixAll::DEFAULT_TOPIC,
            MixAll::DEFAULT_TOPIC_QUEUE_NUMS,
        )
        .await
        .expect("the reply batch send should succeed");
    producer.shutdown();
}

#[tokio::test]
async fn send_batch_writes_client_ids_before_encoding_the_body() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = started("batch_client_id", &cluster).await;

    let result = producer
        .send_batch(
            vec![
                Message::new("T1", Some(b"sub-0")),
                Message::new("T1", Some(b"sub-1")),
                Message::new("T1", Some(b"sub-2")),
            ],
            None,
            None,
        )
        .await
        .expect("batch send should succeed");

    // (1) in the body that went on the wire, every sub-message carries a distinct 32-char client ID
    let wire = cluster.send_body(0, 0);
    let subs = crate::common::message_decoder::decode_batch_messages(&wire);
    assert_eq!(subs.len(), 3, "the batch body must carry the 3 sub-messages verbatim");
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for sub in &subs {
        let id = crate::common::message_client_id_setter::get_uniq_id(sub)
            .unwrap_or_else(|| panic!("sub-message lacks UNIQ_KEY: the ID must be written before encoding"));
        assert_eq!(id.len(), 32, "{id}");
        assert!(seen.insert(id.clone()), "each sub-message's UNIQ_KEY must be distinct");
        // The whole-batch ID must not collide with any sub-message's, otherwise the two were never told apart
        assert_ne!(result.msg_id.as_deref(), Some(id.as_str()));
    }

    // (2) msg_id is the batch's own UNIQ_KEY written into the message properties; offsetMsgId is the broker's copy
    let msg_id = result.msg_id.clone().expect("the batch must also report its client ID");
    assert_eq!(msg_id.len(), 32, "{msg_id}");
    assert!(!msg_id.contains(','), "the batch msgId uses a single handle, not a comma-joined string: {msg_id}");
    let wire_ext = cluster.send_ext(0, 0);
    let props = ext_value(&wire_ext, "i").unwrap_or_default();
    assert!(props.contains(msg_id.as_str()), "the batch's own ID must really go on the wire: {props}");
    assert_ne!(result.msg_id, result.offset_msg_id);

    producer.shutdown();
}

#[tokio::test]
async fn stream_request_type_tags_requests_before_user_hooks_run() {
    #[derive(Default)]
    struct OrderProbe {
        req_t_seen_by_user_hook: AtomicBool,
    }
    impl RPCHook for OrderProbe {
        fn do_before_request(&self, _addr: &str, request: &mut RemotingCommand) {
            self.req_t_seen_by_user_hook.store(
                request.get_ext_field(MixAll::REQ_T).is_some(),
                Ordering::SeqCst,
            );
        }
    }

    let cluster = MockCluster::start(1, true).await;
    let probe = Arc::new(OrderProbe::default());
    let producer = DefaultMQProducer::with_rpc_hook(
        "GID_send_retry",
        Some(probe.clone() as Arc<dyn RPCHook>),
    )
    .expect("group name is valid");
    producer.set_instance_name("stream_on");
    producer.set_namesrv_addr(&cluster.namesrv_addr);
    producer.set_enable_stream_request_type(true);
    producer.start().await.expect("start");
    assert!(
        producer.client_id().unwrap_or_default().ends_with("@STREAM"),
        "with stream enabled the clientId must carry the @STREAM suffix"
    );

    let mut msg = Message::new("T1", Some(b"body"));
    producer.send(&mut msg, None, None).await.expect("send should succeed");
    let ext = cluster.send_ext(0, 0);
    assert_eq!(
        ext_value(&ext, MixAll::REQ_T),
        Some("0"),
        "ReqT must appear in the wire message (the value is the RequestType.STREAM code)"
    );
    assert!(
        probe.req_t_seen_by_user_hook.load(Ordering::SeqCst),
        "ReqT must already be written when the user hook runs"
    );
    producer.shutdown();
}

/// Off by default: an ordinary producer's requests must not contain ReqT.
#[tokio::test]
async fn stream_request_type_is_off_by_default_for_producers() {
    let cluster = MockCluster::start(1, true).await;
    let producer = started("stream_off", &cluster).await;
    assert!(!producer.config().enable_stream_request_type);
    let mut msg = Message::new("T1", Some(b"body"));
    producer.send(&mut msg, None, None).await.expect("send should succeed");
    let ext = cluster.send_ext(0, 0);
    assert!(ext_value(&ext, MixAll::REQ_T).is_none(), "no ReqT by default: {ext:?}");
    assert!(
        !producer.client_id().unwrap_or_default().ends_with("@STREAM"),
        "the default clientId must not carry @STREAM"
    );
    producer.shutdown();
}

// --------------------------------------------------------- async send backpressure

use std::sync::atomic::AtomicUsize;

/// A send callback that records the invocations.
#[derive(Default)]
struct Recorder {
    done: AtomicUsize,
    ok: AtomicUsize,
    errors: Mutex<Vec<String>>,
}

impl Recorder {
    fn errors(&self) -> Vec<String> {
        lock(&self.errors).clone()
    }
}

impl SendCallback for Recorder {
    fn on_success(&self, _result: SendResult) {
        self.ok.fetch_add(1, Ordering::SeqCst);
        self.done.fetch_add(1, Ordering::SeqCst);
    }

    fn on_exception(&self, err: Error) {
        lock(&self.errors).push(err.to_string());
        self.done.fetch_add(1, Ordering::SeqCst);
    }
}

/// Counts how often the before/after hooks run and records whether after saw a result/exception.
#[derive(Default)]
struct CountingSendHook {
    before: AtomicUsize,
    after: AtomicUsize,
    saw_result: AtomicUsize,
    saw_exception: AtomicUsize,
}

impl CountingSendHook {
    fn before(&self) -> usize {
        self.before.load(Ordering::SeqCst)
    }

    fn after(&self) -> usize {
        self.after.load(Ordering::SeqCst)
    }

    fn saw_result(&self) -> bool {
        self.saw_result.load(Ordering::SeqCst) > 0
    }

    fn saw_exception(&self) -> bool {
        self.saw_exception.load(Ordering::SeqCst) > 0
    }
}

impl SendMessageHook for CountingSendHook {
    fn hook_name(&self) -> &str {
        "counting"
    }

    fn send_message_before(&self, _context: &mut SendMessageContext) -> Result<()> {
        self.before.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn send_message_after(&self, context: &mut SendMessageContext) -> Result<()> {
        self.after.fetch_add(1, Ordering::SeqCst);
        if context.send_result.is_some() {
            self.saw_result.fetch_add(1, Ordering::SeqCst);
        }
        if context.exception.is_some() {
            self.saw_exception.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

/// A **slow** forbidden hook: used to burn the async kernel's prep-stage budget (a real cluster cannot produce this duration).
struct SleepingForbiddenHook {
    millis: u64,
}

impl CheckForbiddenHook for SleepingForbiddenHook {
    fn hook_name(&self) -> &str {
        "sleeping"
    }

    fn check_forbidden(&self, _context: &mut CheckForbiddenContext) -> Result<()> {
        std::thread::sleep(Duration::from_millis(self.millis));
        Ok(())
    }
}

/// A **blocking** before hook: parks every send inside the hook until the test releases it, while recording
/// each entry's relative timestamp by label -- isomorphic to the `SlowHook` in the live-verification script.
///
/// A parked send returns no permit, so "a send passed the gate" can only mean the gate was raised -- the verdict needs no timing guesses.
/// What it occupies is **the thread the pool consumer runs on**, so this hook also exercises the "pool saturated by in-flight sends" dimension.
struct ParkingSendHook {
    clock: Mutex<Option<Instant>>,
    entries: Mutex<Vec<(String, u128)>>,
    gate: Mutex<bool>,
    opened: std::sync::Condvar,
}

impl ParkingSendHook {
    fn new() -> ParkingSendHook {
        ParkingSendHook {
            clock: Mutex::new(None),
            entries: Mutex::new(Vec::new()),
            gate: Mutex::new(false),
            opened: std::sync::Condvar::new(),
        }
    }

    /// Restart the clock and clear the records so adjacent test cases do not interfere.
    fn begin(&self) {
        *lock(&self.clock) = Some(Instant::now());
        lock(&self.entries).clear();
    }

    /// Release every send parked in the hook.
    fn open(&self) {
        *lock(&self.gate) = true;
        self.opened.notify_all();
    }

    fn count_of(&self, label: &str) -> usize {
        lock(&self.entries).iter().filter(|(l, _)| l == label).count()
    }

    fn dump(&self) -> String {
        lock(&self.entries)
            .iter()
            .map(|(label, at)| format!("{label}@{at}ms"))
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl SendMessageHook for ParkingSendHook {
    fn hook_name(&self) -> &str {
        "parking"
    }

    fn send_message_before(&self, context: &mut SendMessageContext) -> Result<()> {
        let label = context
            .message
            .as_ref()
            .and_then(|msg| msg.get_keys())
            .unwrap_or_default()
            .to_string();
        let began = lock(&self.clock).unwrap_or_else(Instant::now);
        lock(&self.entries).push((label, began.elapsed().as_millis()));
        // 30s fallback: a mid-case panic must not leave a pool consumer parked forever, or the test process hangs.
        // The WaitTimeoutResult must be read: discarding it means waiting without a timeout again, making the fallback useless.
        let mut guard = self
            .gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        while !*guard {
            match self.opened.wait_timeout(guard, Duration::from_secs(30)) {
                Ok((next, timed_out)) => {
                    guard = next;
                    if timed_out.timed_out() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        Ok(())
    }
}

/// A labelled message (the hook claims each send by its keys).
fn labelled(topic_len: usize, label: &str) -> Message {
    let mut msg = body_of(topic_len);
    msg.set_keys(label);
    msg
}

/// A producer with backpressure enabled: count gate set to `num`, byte gate clamped to the floor value 1M.
///
/// The byte gate can only be configured down to 1M -- smaller values are clamped back up, and 1M is already enough to compute
/// "how many bytes one send deducts" precisely (the bodies are only a few hundred bytes).
async fn backpressure_producer(
    instance: &str,
    cluster: &MockCluster,
    num: i64,
) -> DefaultMQProducer {
    let producer = started(instance, cluster).await;
    producer.set_enable_backpressure_for_async_mode(true);
    producer.set_back_pressure_for_async_send_num(num);
    producer.set_back_pressure_for_async_send_size(MIN_ASYNC_SEND_SIZE);
    producer
}

/// Same as above, plus a blocking before hook attached (to occupy the in-flight slots).
async fn backpressure_producer_with_hook(
    instance: &str,
    cluster: &MockCluster,
    num: i64,
    hook: Arc<ParkingSendHook>,
) -> DefaultMQProducer {
    let producer = backpressure_producer(instance, cluster, num).await;
    producer.register_send_message_hook(hook);
    producer
}

/// Number of in-flight sends parked in the before hook: one less than the count gate, the missing one held by the test itself.
///
/// Why the test has to hold that one: the count gate's floor and **the pool concurrency floor** are the same number
/// ([`MIN_ASYNC_SEND_NUM`], see [`async_sender_worker_count`]), 10 even on a two-core machine.
/// If all 10 sends park in the hook, the 11th can only wait in the pool queue for a worker slot and never reaches the gate -- the case
/// becomes a bet on how many cores the host has. With one held, only 9 park in the hook, the slot count is always ≥ 10, and the 10th
/// necessarily runs in the pool and stops at the gate, making the shape independent of the core count.
const PARKED_IN_HOOK: usize = MIN_ASYNC_SEND_NUM as usize - 1;

/// Offline counterpart of live check B3 (`examples/live_backpressure.rs`): **while every pool consumer is held by a synchronous
/// hook, a runtime resize must still wake the send parked at the gate and really dispatch it**.
///
/// A single-threaded runtime cannot measure this -- there pool tasks only run when the caller awaits, so the gate and the pool never entangle,
/// hence this case needs an explicit multi-thread runtime (the same shape as the live script). If lost wakeups regress, this goes red first.
///
/// The verdict does not look at durations: 9 sends are parked in the before hook and none can return a permit until the test releases them, and the one permit
/// missing at the gate is held by the test itself, so "the 10th appears in the hook" can only mean the resize raised the gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
async fn resize_wakes_the_gate_waiter_while_pool_workers_are_parked() {
    let workers = async_sender_worker_count();
    assert!(
        workers > PARKED_IN_HOOK,
        "pool concurrency {workers} cannot fit {} sends parked in the hook plus a {}-th stopped at the gate",
        PARKED_IN_HOOK,
        PARKED_IN_HOOK + 1
    );
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let hook = Arc::new(ParkingSendHook::new());
    let producer =
        backpressure_producer_with_hook("resize_in_pool", &cluster, MIN_ASYNC_SEND_NUM, hook.clone())
            .await;
    hook.begin();

    // The gate is one permit short of full and the test holds that one, so the number of hook-parked sends becomes core-count-independent.
    assert!(
        producer
            .inner
            .semaphore_async_send_num
            .try_acquire(1, 0)
            .await,
        "at start the count gate should have a free permit to take"
    );

    let held = Arc::new(Recorder::default());
    for _ in 0..PARKED_IN_HOOK {
        producer
            .send_async(labelled(8, "held"), held.clone(), Some(15_000), None)
            .expect("all 9 sends should queue into the pool");
    }
    // Must wait until all 9 really park in the hook before adding the 10th: pool tasks run concurrently with the main task, and who reaches the gate first is not fixed.
    // The first send through the gate takes a permit, so "the stuck 10th" becomes "the 10th stuck held",
    // and the verdict below would be luck (live B3 watches the same thing with a watchdog thread).
    assert!(
        wait_until(|| hook.count_of("held") == PARKED_IN_HOOK).await,
        "not all {} sends parked in the hook (recorded: {})",
        PARKED_IN_HOOK,
        hook.dump()
    );
    assert_eq!(
        producer.semaphore_async_send_num_available_permits(),
        0,
        "{} in-flight sends + the 1 permit held by the test must press the count gate to exactly empty",
        PARKED_IN_HOOK
    );

    let woken = Arc::new(Recorder::default());
    producer
        .send_async(labelled(8, "woken"), woken.clone(), Some(15_000), None)
        .expect("the 10th send should also queue into the pool");

    // That it stops at the gate (rather than still waiting in the pool queue) has evidence: the fair queue holds one more ticket.
    assert!(
        wait_until(|| producer
            .inner
            .semaphore_async_send_num
            .waiting_count()
            >= 1)
            .await,
        "the 10th send never reached the gate (hook record: {})",
        hook.dump()
    );
    assert_eq!(
        hook.count_of("woken"),
        0,
        "before the resize the 10th send must not pass the gate (hook record: {})",
        hook.dump()
    );
    producer.set_back_pressure_for_async_send_num(MIN_ASYNC_SEND_NUM + 2);

    // Only the resize can have woken it: at this moment all 9 in-flight sends are still parked in the hook and no permit has been returned.
    assert!(
        wait_until(|| hook.count_of("woken") == 1).await,
        "the resize did not wake the send parked at the gate: {:?}",
        woken.errors()
    );
    assert_eq!(
        hook.count_of("held"),
        PARKED_IN_HOOK,
        "not all {} sends are parked in the hook, so the 10th passing the gate would not evidence a raised gate",
        PARKED_IN_HOOK
    );

    hook.open();
    assert!(
        wait_until(|| woken.done.load(Ordering::SeqCst) == 1).await,
        "after release the 10th send never landed: {:?}",
        woken.errors()
    );
    assert_eq!(woken.ok.load(Ordering::SeqCst), 1, "{:?}", woken.errors());

    assert!(wait_until(|| held.done.load(Ordering::SeqCst)
        == PARKED_IN_HOOK)
        .await);
    assert_eq!(cluster.requests(0), PARKED_IN_HOOK + 1);
    producer.inner.semaphore_async_send_num.release(1);
    assert_eq!(
        producer.semaphore_async_send_num_available_permits(),
        MIN_ASYNC_SEND_NUM + 2,
        "after everything lands and the test-held permit is returned too, free permits = the new capacity"
    );
    producer.shutdown();
}

fn body_of(len: usize) -> Message {
    let body = vec![b'x'; len];
    Message::new("T1", Some(&body))
}

/// Poll until the condition holds (about 15s at most) instead of guessing durations with a fixed sleep.
///
/// The budget is 15s rather than 3s: a CI runner has only two or four cores, yet must run a runtime with 32 worker threads and
/// the whole concurrency suite -- merely "getting scheduled" is on the order of hundreds of milliseconds. It returns the instant the condition holds, so a wide budget only slows the reporting of
/// real failures, never green runs; conversely betting on 3s would report "slow scheduling" as a "regression".
///
/// ⚠ Conversely, do not use durations as the verdict either: what really happened once on this path was "the pool slots are fully
/// occupied by hook-parked in-flight sends, and send N+1 is never dispatched"; widening the timing window never fixes that, only a state-based verdict does (see
/// `resize_wakes_the_gate_waiter_while_pool_workers_are_parked`).
async fn wait_until<F: Fn() -> bool>(cond: F) -> bool {
    for _ in 0..1500 {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cond()
}

#[test]
fn backpressure_defaults_match_contract() {
    let producer = DefaultMQProducer::new("GID_bp_default").expect("group name is valid");
    assert!(!producer.is_enable_backpressure_for_async_mode());
    assert_eq!(producer.get_back_pressure_for_async_send_num(), 1024);
    assert_eq!(
        producer.get_back_pressure_for_async_send_size(),
        100 * 1024 * 1024
    );
    assert_eq!(producer.semaphore_async_send_num_available_permits(), 1024);
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        100 * 1024 * 1024
    );
}

#[test]
fn constructor_floors_undersized_capacities() {
    let cfg = ProducerConfig {
        producer_group: "GID_bp_floor".to_string(),
        back_pressure_for_async_send_num: 1,
        back_pressure_for_async_send_size: 1024,
        ..Default::default()
    };
    let producer = DefaultMQProducer::with_config(cfg).expect("config is valid");
    assert_eq!(
        producer.get_back_pressure_for_async_send_num(),
        1,
        "the configured value itself stays unchanged"
    );
    assert_eq!(
        producer.semaphore_async_send_num_available_permits(),
        MIN_ASYNC_SEND_NUM,
        "but the semaphore is built at the floor value"
    );
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        MIN_ASYNC_SEND_SIZE
    );
}

#[test]
fn setter_floors_both_gates() {
    let producer = DefaultMQProducer::new("GID_bp_setter").expect("group name is valid");
    producer.set_back_pressure_for_async_send_num(1);
    producer.set_back_pressure_for_async_send_size(1024);
    assert_eq!(
        producer.get_back_pressure_for_async_send_num(),
        MIN_ASYNC_SEND_NUM
    );
    assert_eq!(
        producer.get_back_pressure_for_async_send_size(),
        MIN_ASYNC_SEND_SIZE
    );
    assert_eq!(
        producer.semaphore_async_send_num_available_permits(),
        MIN_ASYNC_SEND_NUM
    );
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        MIN_ASYNC_SEND_SIZE
    );
}

/// While the gate is off, a send neither deducts nor returns permits.
#[tokio::test]
async fn disabled_gate_stays_out_of_the_way() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 300));
    let producer = started("bp_off", &cluster).await;
    assert!(!producer.is_enable_backpressure_for_async_mode());

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(400), cb.clone(), Some(3_000), None)
        .expect("dispatchable within the runtime");
    // while in flight the capacity is untouched
    assert!(wait_until(|| cluster.requests(0) >= 1).await);
    assert_eq!(producer.semaphore_async_send_num_available_permits(), 1024);
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        100 * 1024 * 1024
    );
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(cb.ok.load(Ordering::SeqCst), 1);
    assert_eq!(producer.semaphore_async_send_num_available_permits(), 1024);
    producer.shutdown();
}

#[tokio::test]
async fn num_gate_rejects_with_contract_message_and_sends_nothing() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = backpressure_producer("bp_num_gate", &cluster, MIN_ASYNC_SEND_NUM).await;
    assert!(producer
        .inner
        .semaphore_async_send_num
        .try_acquire(MIN_ASYNC_SEND_NUM, 0)
        .await);

    let began = Instant::now();
    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(1), cb.clone(), Some(300), None)
        .expect("dispatchable within the runtime");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    let errors = cb.errors();
    assert_eq!(errors.len(), 1);
    assert!(
        errors[0].contains("send message tryAcquire semaphoreAsyncNum timeout"),
        "error text checked verbatim: {}",
        errors[0]
    );
    assert!(
        began.elapsed() >= Duration::from_millis(250),
        "the failure was handed back before the timeout elapsed: {:?}",
        began.elapsed()
    );
    assert_eq!(cluster.requests(0), 0, "when the gate rejects, not a single request should go out");
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        MIN_ASYNC_SEND_SIZE,
        "when the count gate fails, the byte gate must not be deducted"
    );
    producer
        .inner
        .semaphore_async_send_num
        .release(MIN_ASYNC_SEND_NUM);
    producer.shutdown();
}

#[tokio::test]
async fn size_gate_rejects_and_gives_the_num_permit_back() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = backpressure_producer("bp_size_gate", &cluster, MIN_ASYNC_SEND_NUM).await;
    assert!(producer
        .inner
        .semaphore_async_send_size
        .try_acquire(MIN_ASYNC_SEND_SIZE, 0)
        .await);

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(10), cb.clone(), Some(200), None)
        .expect("dispatchable within the runtime");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    let errors = cb.errors();
    assert!(
        errors[0].contains("send message tryAcquire semaphoreAsyncSize timeout"),
        "error text checked verbatim: {}",
        errors[0]
    );
    assert_eq!(
        producer.semaphore_async_send_num_available_permits(),
        MIN_ASYNC_SEND_NUM,
        "the count permit was leaked"
    );
    assert_eq!(cluster.requests(0), 0);
    producer
        .inner
        .semaphore_async_send_size
        .release(MIN_ASYNC_SEND_SIZE);
    producer.shutdown();
}

/// One in-flight send = 1 count permit + body.length byte permits; both come back after the callback.
#[tokio::test]
async fn permits_are_borrowed_per_in_flight_send_and_given_back() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 500));
    let producer = backpressure_producer("bp_borrow", &cluster, MIN_ASYNC_SEND_NUM).await;

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(400), cb.clone(), Some(3_000), None)
        .expect("dispatchable within the runtime");
    assert!(wait_until(|| cluster.requests(0) >= 1).await);
    assert_eq!(producer.semaphore_async_send_num_available_permits(), 9);
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        MIN_ASYNC_SEND_SIZE - 400
    );
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(cb.ok.load(Ordering::SeqCst), 1);
    assert_eq!(producer.semaphore_async_send_num_available_permits(), 10);
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        MIN_ASYNC_SEND_SIZE
    );
    producer.shutdown();
}

#[tokio::test]
async fn failure_also_gives_the_permits_back() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SYSTEM_ERROR, 0));
    let producer = backpressure_producer("bp_failure", &cluster, MIN_ASYNC_SEND_NUM).await;

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(300), cb.clone(), Some(3_000), None)
        .expect("dispatchable within the runtime");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert!(!cb.errors().is_empty());
    assert_eq!(producer.semaphore_async_send_num_available_permits(), 10);
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        MIN_ASYNC_SEND_SIZE
    );
    producer.shutdown();
}

/// The retry chain holds **one** pair of permits (one per send, not one per attempt); returning them multiple times would inflate the capacity.
///
/// The retry is triggered by "the broker closes the connection without answering" (the async chain only switches brokers for failures with no response),
/// and the second round lands on the slow-answering `broker-1`, leaving an observation window.
#[tokio::test]
async fn retry_chain_holds_one_pair_not_one_per_attempt() {
    let cluster = MockCluster::start(2, true).await;
    cluster.script(
        0,
        vec![(CLOSE_WITHOUT_ANSWER, 0)],
        (response_code::SUCCESS, 0),
    );
    cluster.script(1, vec![], (response_code::SUCCESS, 400));
    let producer = backpressure_producer("bp_retry_pair", &cluster, MIN_ASYNC_SEND_NUM).await;

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(300), cb.clone(), Some(9_000), None)
        .expect("dispatchable within the runtime");
    // the second attempt is already on its way, and what is deducted is still just one send's worth
    assert!(wait_until(|| cluster.requests(1) >= 1).await);
    assert_eq!(producer.semaphore_async_send_num_available_permits(), 9);
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        MIN_ASYNC_SEND_SIZE - 300
    );
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(cluster.requests(0), 1, "the first attempt landed on the dropped broker");
    assert_eq!(cluster.requests(1), 1);
    assert!(cb.errors().is_empty(), "after switching brokers the send should succeed: {:?}", cb.errors());
    assert_eq!(producer.semaphore_async_send_num_available_permits(), 10);
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        MIN_ASYNC_SEND_SIZE
    );
    producer.shutdown();
}

#[tokio::test]
async fn gate_waits_for_a_permit_instead_of_failing_early() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = backpressure_producer("bp_wait", &cluster, MIN_ASYNC_SEND_NUM).await;
    assert!(producer
        .inner
        .semaphore_async_send_num
        .try_acquire(MIN_ASYNC_SEND_NUM, 0)
        .await);

    let cb = Arc::new(Recorder::default());
    let began = Instant::now();
    producer
        .send_async(body_of(1), cb.clone(), Some(5_000), None)
        .expect("dispatchable within the runtime");
    // Let it queue at the gate, then confirm it is **still alive** (neither failed early nor sent out)
    assert!(wait_until(|| producer
        .inner
        .semaphore_async_send_num
        .waiting_count()
        >= 1)
    .await);
    assert_eq!(cb.done.load(Ordering::SeqCst), 0);
    assert_eq!(cluster.requests(0), 0);

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(cb.done.load(Ordering::SeqCst), 0, "it must not fail early before the timeout");
    producer
        .inner
        .semaphore_async_send_num
        .release(MIN_ASYNC_SEND_NUM);
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert!(
        began.elapsed() >= Duration::from_millis(100),
        "it did not wait for a permit, failing the moment one look showed there was none: {:?}",
        began.elapsed()
    );
    assert!(cb.errors().is_empty(), "once the permit arrives, this send should go out");
    assert_eq!(cb.ok.load(Ordering::SeqCst), 1);
    assert_eq!(cluster.requests(0), 1);
    producer.shutdown();
}

#[tokio::test]
async fn runtime_resize_keeps_the_in_flight_share() {
    let cluster = MockCluster::start(1, true).await;
    let producer = backpressure_producer("bp_resize", &cluster, MIN_ASYNC_SEND_NUM).await;
    assert!(producer
        .inner
        .semaphore_async_send_num
        .try_acquire(5, 0)
        .await);
    assert_eq!(producer.semaphore_async_send_num_available_permits(), 5);
    producer.set_back_pressure_for_async_send_num(15);
    assert_eq!(
        producer.semaphore_async_send_num_available_permits() + 5,
        15
    );
    assert_eq!(producer.get_back_pressure_for_async_send_num(), 15);
    producer
        .inner
        .semaphore_async_send_num
        .release(5);
    assert_eq!(producer.semaphore_async_send_num_available_permits(), 15);
    producer.shutdown();
}

#[tokio::test]
async fn growing_capacity_wakes_a_blocked_sender() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = backpressure_producer("bp_grow", &cluster, MIN_ASYNC_SEND_NUM).await;
    assert!(producer
        .inner
        .semaphore_async_send_num
        .try_acquire(MIN_ASYNC_SEND_NUM, 0)
        .await);

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(1), cb.clone(), Some(5_000), None)
        .expect("dispatchable within the runtime");
    assert!(wait_until(|| producer
        .inner
        .semaphore_async_send_num
        .waiting_count()
        >= 1)
    .await);
    producer.set_back_pressure_for_async_send_num(MIN_ASYNC_SEND_NUM + 1); // one extra unit of capacity out of thin air
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert!(cb.errors().is_empty(), "after the resize this send should go out: {:?}", cb.errors());
    assert_eq!(cluster.requests(0), 1);
    producer.shutdown();
}

#[test]
fn empty_body_still_costs_one_size_permit() {
    assert_eq!(back_pressure_msg_len(&body_of(5)), 5);
    assert_eq!(back_pressure_msg_len(&body_of(0)), 1);
    let no_body = Message::new("T1", None);
    assert_eq!(back_pressure_msg_len(&no_body), 1);
}

/// "Queueing" here is the gap between a task being enqueued and a pool consumer running it; a real cluster cannot produce a stable duration for it,
/// so `began` is placed directly in the past, equivalent to that wait having spent the whole budget.
#[tokio::test]
async fn queue_wait_beyond_budget_reports_async_send_call_timeout() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = started("bp_stale_budget", &cluster).await;

    let cb = Arc::new(Recorder::default());
    let stale = monotonic_millis() - 5_000.0;
    producer
        .run_async_send(body_of(1), 1, cb.clone(), 3_000, stale, None)
        .await;
    assert_eq!(cb.done.load(Ordering::SeqCst), 1);
    let errors = cb.errors();
    assert_eq!(errors.len(), 1);
    assert!(
        errors[0].contains("DEFAULT ASYNC send call timeout"),
        "error text checked verbatim: {}",
        errors[0]
    );
    assert_eq!(cluster.requests(0), 0, "with no budget left, no request should be sent");
    producer.shutdown();
}

// ================================================================ async send kernel
//
// Bounded queue, address resolution, reused requests with a fresh opaque, `retryTimesWhenSendAsyncFailed`, async ignoring
// `retryResponseCodes` -- these only exist for the real async kernel; all are checked against the mock cluster.

/// Queue-full can be produced deterministically here: `#[tokio::test]` uses a **single-threaded** runtime, there is no `await` between the two
/// `send_async` calls, so the pool consumer never gets scheduled and a capacity-1 queue is necessarily full at the second send.
/// Capacity is read when the pool is **built** (`start()`), so it must be configured in `ProducerConfig`.
#[tokio::test]
async fn queue_full_rejects_the_caller_without_sending() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = DefaultMQProducer::with_config(ProducerConfig {
        producer_group: "GID_send_retry".to_string(),
        instance_name: "async_queue_full".to_string(),
        name_server_addrs: vec![cluster.namesrv_addr.clone()],
        async_sender_queue_capacity: 1,
        ..Default::default()
    })
    .expect("config is valid");
    producer.start().await.expect("start should succeed against the mock cluster");
    assert_eq!(producer.get_async_sender_queue_capacity(), 1);

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(10), cb.clone(), Some(3_000), None)
        .expect("the first send always enters the queue");
    let err = producer
        .send_async(body_of(10), cb.clone(), Some(3_000), None)
        .expect_err("when the queue is full the send must be handed back to the caller");
    assert!(
        err.to_string().contains("executor rejected"),
        "rejection text checked verbatim (no extra trailing space): {err}"
    );
    assert_eq!(cb.done.load(Ordering::SeqCst), 0, "a rejected send must not call back");
    assert_eq!(cluster.requests(0), 0, "before the rejection, no request should have been sent");

    // after letting the queue drain, the first send goes out as usual and its callback still runs
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(cb.ok.load(Ordering::SeqCst), 1);
    producer.shutdown();
}

#[tokio::test]
async fn queue_full_with_backpressure_dispatches_off_the_queue() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = DefaultMQProducer::with_config(ProducerConfig {
        producer_group: "GID_send_retry".to_string(),
        instance_name: "async_queue_full_bp".to_string(),
        name_server_addrs: vec![cluster.namesrv_addr.clone()],
        async_sender_queue_capacity: 1,
        enable_backpressure_for_async_mode: true,
        back_pressure_for_async_send_num: MIN_ASYNC_SEND_NUM,
        back_pressure_for_async_send_size: MIN_ASYNC_SEND_SIZE,
        ..Default::default()
    })
    .expect("config is valid");
    producer.start().await.expect("start should succeed against the mock cluster");

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(10), cb.clone(), Some(3_000), None)
        .expect("the first send enters the queue");
    producer
        .send_async(body_of(10), cb.clone(), Some(3_000), None)
        .expect("with backpressure on, a full queue must not hand the send back to the caller");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 2).await);
    assert_eq!(cb.ok.load(Ordering::SeqCst), 2, "both sends must go out");
    assert_eq!(cluster.requests(0), 2);
    assert_eq!(
        producer.semaphore_async_send_num_available_permits(),
        MIN_ASYNC_SEND_NUM,
        "the return must exactly equal what was deducted"
    );
    producer.shutdown();
}

/// The pool is closed with `shutdown()` ⇒ the caller gets a synchronous error, instead of a task being posted to a
/// queue no one will consume again. The callback never runs.
#[tokio::test]
async fn send_async_after_shutdown_is_rejected() {
    let cluster = MockCluster::start(1, true).await;
    let producer = started("async_after_shutdown", &cluster).await;
    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(10), cb.clone(), Some(3_000), None)
        .expect("async sends are accepted after start");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    producer.shutdown();
    let err = producer
        .send_async(body_of(10), cb.clone(), Some(1_000), None)
        .expect_err("async sends must not be accepted after shutdown");
    assert!(err.to_string().contains("producer not started"), "{err}");
    assert_eq!(cb.done.load(Ordering::SeqCst), 1, "a rejected send must not call back");
}

#[tokio::test]
async fn prep_stage_beyond_budget_reports_send_kernel_timeout() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = backpressure_producer("async_kernel_timeout", &cluster, MIN_ASYNC_SEND_NUM)
        .await;
    producer.register_check_forbidden_hook(Arc::new(SleepingForbiddenHook {
        millis: 200,
    }));
    let hooks = Arc::new(CountingSendHook::default());
    producer.register_send_message_hook(hooks.clone());

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(300), cb.clone(), Some(50), None)
        .expect("dispatchable within the runtime");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    let errors = cb.errors();
    assert!(
        errors[0].contains("sendKernelImpl call timeout"),
        "error text checked verbatim: {}",
        errors[0]
    );
    assert_eq!(cluster.requests(0), 0, "with no budget left, the request must not be handed out");
    assert_eq!(hooks.before(), 1, "the before hook runs before the budget check");
    assert_eq!(hooks.after(), 1, "the after hook must run once before the abort");
    assert_eq!(producer.semaphore_async_send_num_available_permits(), MIN_ASYNC_SEND_NUM);
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        MIN_ASYNC_SEND_SIZE
    );
    producer.shutdown();
}

#[tokio::test]
async fn connect_failure_retries_on_another_broker_with_a_fresh_opaque() {
    let live = MockCluster::start(1, true).await;
    live.script(0, vec![], (response_code::SUCCESS, 0));
    let dead = dead_addr().await;
    // In the route, broker-0 is a dead address and broker-1 is a real answering broker (queue selection starts at 0, retry avoids 0)
    let cluster = MockCluster::with_addrs(vec![dead, live.broker_addrs[0].clone()], true).await;
    let producer = started("async_retry_broker", &cluster).await;

    let cb = Arc::new(Recorder::default());
    // The budget has to be roomy: a connect to a dead loopback port costs ~2s here and
    // serialises behind the heartbeat connects to the same address, so the first attempt
    // alone can burn seconds that would otherwise leave `remaining <= 0` and no retry.
    producer
        .send_async(body_of(10), cb.clone(), Some(30_000), None)
        .expect("dispatchable within the runtime");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert!(cb.errors().is_empty(), "after switching brokers the send should succeed: {:?}", cb.errors());
    assert_eq!(cb.ok.load(Ordering::SeqCst), 1);
    assert_eq!(live.requests(0), 1, "the retry landed on the other broker");
    producer.shutdown();
}

#[tokio::test]
async fn async_retry_honours_retry_times_when_send_async_failed() {
    let cluster = MockCluster::start(2, true).await;
    for index in 0..2 {
        cluster.script(index, vec![], (CLOSE_WITHOUT_ANSWER, 0));
    }
    let producer = started("async_retry_limit", &cluster).await;
    assert_eq!(producer.get_retry_times_when_send_async_failed(), 2);

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(10), cb.clone(), Some(5_000), None)
        .expect("dispatchable within the runtime");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(
        cluster.requests(0) + cluster.requests(1),
        3,
        "retry_times_when_send_async_failed=2 ⇒ 3 attempts in total"
    );
    let errors = cb.errors();
    assert_eq!(errors.len(), 1);
    assert!(
        errors[0].contains("send request failed"),
        "RemotingSendRequestException branch: {}",
        errors[0]
    );
    producer.shutdown();
}

#[tokio::test]
async fn retry_times_when_send_async_failed_zero_means_one_attempt() {
    let cluster = MockCluster::start(2, true).await;
    for index in 0..2 {
        cluster.script(index, vec![], (CLOSE_WITHOUT_ANSWER, 0));
    }
    let producer = started("async_no_retry", &cluster).await;
    producer.set_retry_times_when_send_async_failed(0);

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(10), cb.clone(), Some(5_000), None)
        .expect("dispatchable within the runtime");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(cluster.requests(0), 1);
    assert_eq!(cluster.requests(1), 0, "with 0 configured, no broker switch should happen");
    producer.shutdown();
}

/// **Async sending ignores `retryResponseCodes`**: when the broker explicitly answers with an error (here `SYSTEM_ERROR`),
/// it is handed to the callback as-is, ending after one attempt. This **differs** from the sync send's semantics; do not copy it over.
#[tokio::test]
async fn broker_rejected_response_ends_the_chain_after_one_attempt() {
    let cluster = MockCluster::start(2, true).await;
    cluster.script(0, vec![], (response_code::SYSTEM_ERROR, 0));
    cluster.script(1, vec![], (response_code::SUCCESS, 0));
    let producer = started("async_broker_code", &cluster).await;
    assert!(
        producer.is_retry_response_code(Some(response_code::SYSTEM_ERROR)),
        "this code is retryable on the sync path"
    );

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(10), cb.clone(), Some(5_000), None)
        .expect("dispatchable within the runtime");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(cluster.requests(0), 1);
    assert_eq!(cluster.requests(1), 0, "the async chain does not switch brokers (the needRetry=false branch)");
    let errors = cb.errors();
    assert!(
        errors[0].contains("MQBrokerException"),
        "the broker's error code must be delivered **verbatim**, not wrapped as unknown reason: {}",
        errors[0]
    );
    producer.shutdown();
}

#[tokio::test]
async fn send_hooks_run_once_per_async_send() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = started("async_hooks_ok", &cluster).await;
    let hooks = Arc::new(CountingSendHook::default());
    producer.register_send_message_hook(hooks.clone());

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(10), cb.clone(), Some(3_000), None)
        .expect("dispatchable within the runtime");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(cb.ok.load(Ordering::SeqCst), 1);
    assert_eq!(hooks.before(), 1);
    assert_eq!(hooks.after(), 1);
    producer.shutdown();
}

/// The after hook must see `sendResult` (success) or `exception` (failure), consistent with the sync kernel.
#[tokio::test]
async fn after_hook_sees_the_async_outcome() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(
        0,
        vec![(response_code::SYSTEM_ERROR, 0), (response_code::SUCCESS, 0)],
        (response_code::SUCCESS, 0),
    );
    let producer = started("async_hook_ctx", &cluster).await;
    let hooks = Arc::new(CountingSendHook::default());
    producer.register_send_message_hook(hooks.clone());

    let cb = Arc::new(Recorder::default());
    // the first send fails (the broker explicitly answered an error), the second succeeds: both terminal paths must carry their own outcome
    for i in 1..=2 {
        producer
            .send_async(body_of(10), cb.clone(), Some(3_000), None)
            .expect("dispatchable within the runtime");
        assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == i).await);
    }
    assert_eq!(cb.ok.load(Ordering::SeqCst), 1);
    assert_eq!(cb.errors().len(), 1);
    assert!(hooks.saw_result(), "the success branch must carry sendResult");
    assert!(hooks.saw_exception(), "the failure branch must carry exception");
    assert_eq!(hooks.before(), 2);
    assert_eq!(hooks.after(), 2, "each send runs after once at its terminal point");
    producer.shutdown();
}

#[tokio::test]
async fn pinned_mq_retries_on_the_same_broker_with_new_opaques() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(
        0,
        vec![(CLOSE_WITHOUT_ANSWER, 0), (CLOSE_WITHOUT_ANSWER, 0)],
        (response_code::SUCCESS, 0),
    );
    let producer = started("async_pinned", &cluster).await;
    let mq = MessageQueue::new("T1", "broker-0", 0);

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(10), cb.clone(), Some(5_000), Some(mq))
        .expect("dispatchable within the runtime");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert!(cb.errors().is_empty(), "the third attempt should succeed: {:?}", cb.errors());
    assert_eq!(cluster.requests(0), 3, "all three attempts stayed on the pinned broker");
    assert_ne!(
        cluster.send_opaque(0, 0),
        cluster.send_opaque(0, 1),
        "retry must use a fresh opaque, otherwise the two attempts' responses would cross-talk"
    );
    assert_ne!(
        cluster.send_opaque(0, 1),
        cluster.send_opaque(0, 2),
        "retry must use a fresh opaque, otherwise the two attempts' responses would cross-talk"
    );
    producer.shutdown();
}

#[test]
fn send_async_before_start_raises_without_calling_back() {
    let producer = DefaultMQProducer::new("GID_async_unstarted").expect("group name is valid");
    let cb = Arc::new(Recorder::default());
    let err = producer
        .send_async(body_of(10), cb.clone(), Some(1_000), None)
        .expect_err("before start it must error synchronously");
    assert!(
        err.to_string().contains("producer not started"),
        "not-started guard text checked verbatim: {err}"
    );
    assert_eq!(cb.done.load(Ordering::SeqCst), 0);
}

// ------------------------------------- topic-consistency guard for pinned sends

/// Pinned sync sends compare the topic **before** `sendKernelImpl` and refuse on mismatch -- the mock broker receives not a single request.
#[tokio::test]
async fn pinned_sync_send_refuses_a_mismatched_topic_before_any_request() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = started("pinned_guard_sync", &cluster).await;
    let mq = MessageQueue::new("T2", "broker-0", 0);
    let mut msg = body_of(8);

    let err = producer
        .send(&mut msg, Some(3_000), Some(&mq))
        .await
        .expect_err("a mismatched topic must be refused");
    let (code, message) = expect_client_code(err);
    assert_eq!(code, None);
    assert_eq!(message, PINNED_TOPIC_MISMATCH_SYNC);
    assert_eq!(cluster.requests(0), 0, "the refusal must happen before any request");
    assert_eq!(msg.topic, "T1", "the guard runs before compression; the caller's message must be untouched");
    producer.shutdown();
}

/// A pinned send with a matching topic goes on the wire as usual, carrying exactly that topic (the guard must not be so loose it misfires).
#[tokio::test]
async fn pinned_sync_send_with_a_matching_topic_reaches_the_broker() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = started("pinned_guard_ok", &cluster).await;
    let mq = MessageQueue::new("T1", "broker-0", 0);
    let mut msg = body_of(8);

    let result = producer
        .send(&mut msg, Some(3_000), Some(&mq))
        .await
        .expect("a same-topic pinned send should succeed");
    assert_eq!(result.status, SendStatus::SendOk);
    assert_eq!(cluster.requests(0), 1);
    assert_eq!(ext_value(&cluster.send_ext(0, 0), "b"), Some("T1"));
    assert_eq!(ext_value(&cluster.send_ext(0, 0), "n"), Some("broker-0"));
    producer.shutdown();
}

#[tokio::test]
async fn pinned_sync_send_compares_topics_after_namespace_wrapping() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = started("pinned_guard_ns", &cluster).await;
    producer.set_namespace("ns1");

    // (1) the queue topic already carries the prefix (that is exactly the shape the route table returns)
    let mq = MessageQueue::new("ns1%T1", "broker-0", 0);
    let mut msg = body_of(8);
    producer
        .send(&mut msg, Some(3_000), Some(&mq))
        .await
        .expect("a prefixed queue topic must not be refused by mistake");
    assert_eq!(
        ext_value(&cluster.send_ext(0, 0), "b"),
        Some("ns1%T1"),
        "what goes on the wire is the prefixed resource name"
    );

    // (2) the queue topic is a bare name: both sides wrapped become equal, also admitted
    let mq = MessageQueue::new("T1", "broker-0", 0);
    let mut msg = body_of(8);
    producer
        .send(&mut msg, Some(3_000), Some(&mq))
        .await
        .expect("a bare-name queue should also be admitted");
    assert_eq!(ext_value(&cluster.send_ext(0, 1), "b"), Some("ns1%T1"));

    // (3) same topic name under another namespace -- a different resource, must be refused
    let mq = MessageQueue::new("ns2%T1", "broker-0", 0);
    let mut msg = body_of(8);
    let err = producer
        .send(&mut msg, Some(3_000), Some(&mq))
        .await
        .expect_err("across namespaces it must say no");
    assert_eq!(expect_client_code(err).1, PINNED_TOPIC_MISMATCH_SYNC);
    assert_eq!(cluster.requests(0), 2, "the first two go on the wire, the third does not");
    producer.shutdown();
}

#[tokio::test]
async fn pinned_batch_send_shares_the_sync_guard() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = started("pinned_guard_batch", &cluster).await;

    let mq = MessageQueue::new("T2", "broker-0", 0);
    let err = producer
        .send_batch(vec![body_of(8), body_of(9)], Some(&mq), Some(3_000))
        .await
        .expect_err("a mismatched batch topic must be refused just the same");
    assert_eq!(expect_client_code(err).1, PINNED_TOPIC_MISMATCH_SYNC);
    assert_eq!(cluster.requests(0), 0, "the refusal must happen before any request");

    let mq = MessageQueue::new("T1", "broker-0", 0);
    producer
        .send_batch(vec![body_of(8), body_of(9)], Some(&mq), Some(3_000))
        .await
        .expect("a same-topic pinned batch should succeed");
    assert_eq!(cluster.requests(0), 1);
    assert_eq!(cluster.send_code(0, 0), request_code::SEND_BATCH_MESSAGE);
    assert_eq!(ext_value(&cluster.send_ext(0, 0), "b"), Some("T1"));
    producer.shutdown();
}

#[tokio::test]
async fn pinned_async_send_refuses_with_the_async_wording_via_the_callback() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = started("pinned_guard_async", &cluster).await;
    let mq = MessageQueue::new("T2", "broker-0", 0);
    let cb = Arc::new(Recorder::default());

    producer
        .send_async(body_of(8), cb.clone(), Some(3_000), Some(mq))
        .expect("dispatchable within the runtime");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(cb.ok.load(Ordering::SeqCst), 0);
    assert_eq!(cb.done.load(Ordering::SeqCst), 1, "the failure may be delivered only once");
    let errors = cb.errors();
    assert!(
        errors.first().is_some_and(|e| e.contains(PINNED_TOPIC_MISMATCH_ASYNC)),
        "async wording checked verbatim: {errors:?}"
    );
    assert_eq!(cluster.requests(0), 0, "the refusal must happen before any request");
    producer.shutdown();
}

/// First do one ordinary send to feed the broker address table: oneway pinned sends do no route lookup themselves
/// (`need_addr` only reads the cache), so this step is unrelated to the guard.
#[tokio::test]
async fn oneway_pinned_send_has_no_topic_guard() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = started("pinned_guard_oneway", &cluster).await;
    let mut warmup = body_of(8);
    producer
        .send(&mut warmup, Some(3_000), None)
        .await
        .expect("let broker-0 appear in the address table first");

    let mq = MessageQueue::new("T2", "broker-0", 0);
    let mut msg = body_of(8);
    producer
        .send_oneway(&mut msg, Some(&mq))
        .await
        .expect("this oneway path has no queue-ownership guard");
    // Oneway sends wait for no answer; when the client returns, the mock broker may not have read this frame yet -- poll until it lands.
    assert!(
        wait_until(|| cluster.requests(0) == 2).await,
        "the topic-mismatched oneway message is sent anyway (the mock broker actually received {} requests)",
        cluster.requests(0)
    );
    assert_eq!(
        ext_value(&cluster.send_ext(0, 1), "b"),
        Some("T1"),
        "the topic in the header comes from the message's own topic; only queueId comes from the named queue -- without the guard that is exactly how the mismatch goes on the wire"
    );
    producer.shutdown();
}

#[tokio::test]
async fn state_check_precedes_the_pinned_guard() {
    let producer = DefaultMQProducer::new("GID_pinned_unstarted").expect("group name is valid");
    let mq = MessageQueue::new("T2", "broker-0", 0);
    let mut msg = body_of(8);
    let err = producer
        .send(&mut msg, Some(1_000), Some(&mq))
        .await
        .expect_err("before start it must error");
    assert!(
        err.to_string().contains("producer not started"),
        "the state check must precede the guard: {err}"
    );
}

