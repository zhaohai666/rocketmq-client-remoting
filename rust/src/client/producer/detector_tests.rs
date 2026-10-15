//! A real cluster cannot produce the "probe times out" timing nor expose the "probe recovery
//! releases the broker" timing, so this module stands up an in-process fake cluster (1 namesrv +
//! 1 broker, speaking only the remoting protocol) whose `GET_MAX_OFFSET(30)` response is
//! scriptable (immediate SUCCESS / delay beyond detectTimeout / business error code); whether probing actually sent requests is asserted via the request counter.

#![cfg(test)]

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

use super::*;
use crate::remoting::protocol::route::{BrokerData, QueueData, TopicRouteData};

const MAX_FRAME: i32 = 20 * 1024 * 1024;

// ---------------------------------------------------------------- fake cluster

/// Behavior switches for the broker's `GET_MAX_OFFSET` (requests other than probes always get immediate SUCCESS).
struct DetectState {
    /// Milliseconds to sleep before answering GET_MAX_OFFSET (> detectTimeout ⇒ probe timeout).
    max_offset_delay_ms: AtomicU64,
    /// Whether GET_MAX_OFFSET answers SUCCESS; false ⇒ SYSTEM_ERROR.
    max_offset_ok: AtomicBool,
    /// Number of GET_MAX_OFFSET requests received (the only observation point for whether probes were sent and whether they are periodic).
    max_offset_requests: AtomicUsize,
}

/// In-process fake cluster: 1 namesrv (answers routes only) + 1 broker (broker-0).
/// Spawned task handles are dropped on the spot (detach): the tasks follow the test runtime and are reaped when it ends.
struct DetectCluster {
    namesrv_addr: String,
    state: Arc<DetectState>,
}

impl DetectCluster {
    async fn start() -> DetectCluster {
        let broker = TcpListener::bind("127.0.0.1:0").await.expect("bind mock broker");
        let broker_addr = broker.local_addr().expect("mock broker addr").to_string();
        let namesrv = TcpListener::bind("127.0.0.1:0").await.expect("bind mock namesrv");
        let namesrv_addr = namesrv.local_addr().expect("mock namesrv addr").to_string();
        let state = Arc::new(DetectState {
            // default: immediate SUCCESS, the shape of a healthy broker
            max_offset_delay_ms: AtomicU64::new(0),
            max_offset_ok: AtomicBool::new(true),
            max_offset_requests: AtomicUsize::new(0),
        });
        // Handles are detached directly: same effect as send_retry_tests' "hold but do not await";
        // the tasks are aborted together when the test runtime drops.
        let _namesrv_task = spawn_namesrv(namesrv, broker_addr.clone());
        let _broker_task = spawn_broker(broker, state.clone());
        DetectCluster { namesrv_addr, state }
    }

    /// Script the probe response: whether SUCCESS and how many milliseconds to delay before answering.
    fn set_probe(&self, ok: bool, delay_ms: u64) {
        self.state.max_offset_ok.store(ok, Ordering::SeqCst);
        self.state.max_offset_delay_ms.store(delay_ms, Ordering::SeqCst);
    }

    /// Number of GET_MAX_OFFSET requests the broker has received.
    fn requests(&self) -> usize {
        self.state.max_offset_requests.load(Ordering::SeqCst)
    }
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

/// Response carrying the request's `opaque` (the client pairs by opaque; a mismatched one is dropped as noise).
fn response_for(request: &RemotingCommand, code: i32) -> RemotingCommand {
    let mut response = RemotingCommand::create_response(code, Some("mock failure".to_string()));
    response.opaque = request.opaque;
    response.serialize_type_current_rpc = request.serialize_type_current_rpc;
    response
}

fn route_json(broker_addr: &str) -> Vec<u8> {
    let route = TopicRouteData {
        queue_datas: vec![QueueData::new(
            "broker-0".to_string(),
            1,
            1,
            PermName::PERM_READ | PermName::PERM_WRITE,
            0,
        )],
        broker_datas: vec![BrokerData::new(
            "MockCluster",
            "broker-0".to_string(),
            vec![(i64::from(MixAll::MASTER_ID), broker_addr.to_string())],
            "",
        )],
        ..Default::default()
    };
    serde_json::to_vec(&route.to_json_value()).expect("route is serializable")
}

/// namesrv: answers every topic with the same route (broker-0 → fixed address), everything else SUCCESS.
fn spawn_namesrv(listener: TcpListener, broker_addr: String) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else { return };
            let addr = broker_addr.clone();
            tokio::spawn(async move {
                while let Some(frame) = read_frame(&mut stream).await {
                    let Ok(request) = RemotingCommand::decode(&frame) else { return };
                    let mut response = if request.code == request_code::GET_ROUTEINFO_BY_TOPIC {
                        let mut r = response_for(&request, response_code::SUCCESS);
                        r.set_body(Some(route_json(&addr)));
                        r
                    } else {
                        response_for(&request, response_code::SUCCESS)
                    };
                    write_frame(&mut stream, &mut response).await;
                }
            });
        }
    })
}

/// broker: answers GET_MAX_OFFSET per the `DetectState` script, SEND with SUCCESS, everything else immediate SUCCESS.
fn spawn_broker(listener: TcpListener, state: Arc<DetectState>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else { return };
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                while let Some(frame) = read_frame(&mut stream).await {
                    let Ok(request) = RemotingCommand::decode(&frame) else { return };
                    if request.code == request_code::GET_MAX_OFFSET {
                        state.max_offset_requests.fetch_add(1, Ordering::SeqCst);
                        let delay = state.max_offset_delay_ms.load(Ordering::SeqCst);
                        if delay > 0 {
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                        }
                        if request.is_oneway_rpc() {
                            continue;
                        }
                        let code = if state.max_offset_ok.load(Ordering::SeqCst) {
                            response_code::SUCCESS
                        } else {
                            response_code::SYSTEM_ERROR
                        };
                        let mut response = response_for(&request, code);
                        if code == response_code::SUCCESS {
                            response.add_ext_field("offset", "7");
                        }
                        write_frame(&mut stream, &mut response).await;
                        continue;
                    }
                    if is_send_code(request.code) {
                        if request.is_oneway_rpc() {
                            continue;
                        }
                        let mut response = response_for(&request, response_code::SUCCESS);
                        response.add_ext_field("msgId", "MOCK-0-0");
                        response.add_ext_field("queueId", "0");
                        response.add_ext_field("queueOffset", "7");
                        write_frame(&mut stream, &mut response).await;
                    } else if !request.is_oneway_rpc() {
                        let mut response = response_for(&request, response_code::SUCCESS);
                        write_frame(&mut stream, &mut response).await;
                    }
                }
            });
        }
    })
}

fn is_send_code(code: i32) -> bool {
    matches!(
        code,
        request_code::SEND_MESSAGE
            | request_code::SEND_MESSAGE_V2
            | request_code::SEND_BATCH_MESSAGE
    )
}

// ---------------------------------------------------------------- fixtures

/// Starts a producer pointed at the fake cluster with probing enabled per the given parameters. `instance` must be unique:
/// the same clientId shares one [`MQClientInstance`] (route table included).
async fn started(
    instance: &str,
    cluster: &DetectCluster,
    detector_enable: bool,
    detect_timeout: i64,
    detect_interval: i64,
) -> DefaultMQProducer {
    let producer = DefaultMQProducer::new("GID_fault_detector").expect("group name is valid");
    producer.set_instance_name(instance);
    producer.set_namesrv_addr(&cluster.namesrv_addr);
    producer.set_send_latency_fault_enable(true);
    producer.set_start_detector_enable(detector_enable);
    producer.set_detect_timeout(detect_timeout);
    producer.set_detect_interval(detect_interval);
    assert_eq!(producer.is_start_detector_enable(), detector_enable);
    assert_eq!(producer.get_detect_timeout(), detect_timeout);
    assert_eq!(producer.get_detect_interval(), detect_interval);
    producer.start().await.expect("start should succeed against the fake cluster");
    producer
}

/// Sends one message to seed the route into the client instance: the detector's `Resolver`
/// (broker address table) and `pickTopic` (route table) both depend on it; without this step probing has nothing to work with.
async fn seed_route(producer: &DefaultMQProducer) {
    let mut msg = Message::new("T1", Some(b"body"));
    producer
        .send(&mut msg, Some(30_000), None)
        .await
        .expect("send should succeed against the fake cluster");
}

/// Polls until `cond` holds (probes run every 3s; the budget is a generous 20s to avoid flakiness on slow environments).
async fn wait_for(cond: impl Fn() -> bool, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !cond() {
        assert!(Instant::now() < deadline, "not reached within 20s: {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ---------------------------------------------------------------- cases

#[test]
fn detector_config_defaults_match_golden_config() {
    let cfg = ProducerConfig::default();
    assert!(!cfg.start_detector_enable, "startDetectorEnable defaults to false");
    assert_eq!(cfg.detect_timeout, 200);
    assert_eq!(cfg.detect_interval, 2 * 1000);

    // setter/getter round trip
    let producer = DefaultMQProducer::new("GID_fault_detector").expect("group name is valid");
    assert!(!producer.is_start_detector_enable());
    producer.set_start_detector_enable(true);
    assert!(producer.is_start_detector_enable());
    producer.set_detect_timeout(500);
    assert_eq!(producer.get_detect_timeout(), 500);
    producer.set_detect_interval(4_000);
    assert_eq!(producer.get_detect_interval(), 4_000);
}

/// The switch defaults to off: even with an unreachable broker in the fault table, there must be no probe traffic at all.
#[tokio::test]
async fn detector_disabled_by_default_sends_no_probes() {
    let cluster = DetectCluster::start().await;
    let producer = started("detector_off", &cluster, false, 200, 300).await;
    seed_route(&producer).await;

    producer.fault_strategy().update_fault_item("broker-0", 100, true, false);
    assert!(!producer
        .fault_strategy()
        .latency_fault_tolerance()
        .is_reachable("broker-0"));

    // the probe task is indeed running (first round after 3s), but with the switch off no GET_MAX_OFFSET may be sent
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert_eq!(cluster.requests(), 0, "no probe traffic while the detector is off");
    assert!(
        !producer
            .fault_strategy()
            .latency_fault_tolerance()
            .is_reachable("broker-0"),
        "nobody probes, the broker stays unreachable"
    );
    producer.shutdown();
}

/// Core scenario: after the send path marks the reachability bad, the detector periodically
/// sends `GET_MAX_OFFSET`; a successful probe puts the broker back (recovery release), and probing keeps running on schedule afterwards.
#[tokio::test]
async fn detector_restores_unreachable_broker_and_keeps_probing() {
    let cluster = DetectCluster::start().await;
    let producer = started("detector_recover", &cluster, true, 200, 300).await;
    seed_route(&producer).await;

    let tolerance = producer.inner.fault_strategy.latency_fault_tolerance();
    producer.fault_strategy().update_fault_item("broker-0", 100, true, false);
    assert!(!tolerance.is_reachable("broker-0"));

    wait_for(
        || tolerance.is_reachable("broker-0"),
        "broker-0 should be reachable again after a successful probe",
    )
    .await;
    assert!(cluster.requests() >= 1, "the detector must actually have sent GET_MAX_OFFSET(30)");

    // periodic triggering: after recovery the fault item stays in the table and later rounds keep probing (the request counter keeps rising)
    let seen = cluster.requests();
    wait_for(
        || cluster.requests() > seen,
        "the detector should keep sending GET_MAX_OFFSET on the 3s period",
    )
    .await;
    assert!(tolerance.is_reachable("broker-0"));

    producer.shutdown();
}

/// Timeout path: a response delay beyond `detectTimeout` ⇒ every probe round fails ⇒ the broker stays unreachable;
/// once the broker answers immediately again, the next probe round releases it.
#[tokio::test]
async fn detect_timeout_keeps_broker_unreachable_until_it_recovers() {
    let cluster = DetectCluster::start().await;
    // response 800ms > detectTimeout=200ms ⇒ the probe must time out (via the protocol-layer timeout, not a dead port)
    cluster.set_probe(true, 800);
    let producer = started("detector_timeout", &cluster, true, 200, 300).await;
    seed_route(&producer).await;

    let tolerance = producer.inner.fault_strategy.latency_fault_tolerance();
    producer.fault_strategy().update_fault_item("broker-0", 100, true, false);
    assert!(!tolerance.is_reachable("broker-0"));

    wait_for(
        || cluster.requests() >= 2,
        "at least two probe rounds should happen within 12s (3s first round + 3s period)",
    )
    .await;
    assert!(
        !tolerance.is_reachable("broker-0"),
        "a probe timeout must not mark the broker reachable"
    );

    // the broker answers immediately again ⇒ the next probe round releases it
    cluster.set_probe(true, 0);
    wait_for(
        || tolerance.is_reachable("broker-0"),
        "after the broker recovers, the next probe round should put broker-0 back",
    )
    .await;

    producer.shutdown();
}
