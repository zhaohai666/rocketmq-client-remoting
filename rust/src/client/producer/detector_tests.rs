//! 真集群造不出「探测超时」也观察不到「探测恢复放行」的时序，所以这里在进程内
//! 起一个假集群（1 个 namesrv + 1 个 broker，只说 remoting 协议），broker 的
//! `GET_MAX_OFFSET(30)` 应答可脚本化（即时 SUCCESS / 延迟超过 detectTimeout /
//! 回业务错误码），探测是否真的发过请求靠请求计数断言。

#![cfg(test)]

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

use super::*;
use crate::remoting::protocol::route::{BrokerData, QueueData, TopicRouteData};

const MAX_FRAME: i32 = 20 * 1024 * 1024;

// ---------------------------------------------------------------- 假集群

/// broker 的 `GET_MAX_OFFSET` 行为开关（探测之外的请求一律即时 SUCCESS）。
struct DetectState {
    /// GET_MAX_OFFSET 应答前 sleep 的毫秒数（> detectTimeout ⇒ 探测超时）。
    max_offset_delay_ms: AtomicU64,
    /// GET_MAX_OFFSET 是否回 SUCCESS；false ⇒ SYSTEM_ERROR。
    max_offset_ok: AtomicBool,
    /// 收到的 GET_MAX_OFFSET 请求数（探测是否真发过、是否周期发的唯一观察点）。
    max_offset_requests: AtomicUsize,
}

/// 进程内假集群：1 个 namesrv（只答路由）+ 1 个 broker（broker-0）。
/// spawn 出的任务句柄就地丢弃（detach）：任务跟着测试运行时走，运行时结束即被回收。
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
            // 默认即时 SUCCESS：healthy broker 的形状
            max_offset_delay_ms: AtomicU64::new(0),
            max_offset_ok: AtomicBool::new(true),
            max_offset_requests: AtomicUsize::new(0),
        });
        // 句柄直接 detach：与 send_retry_tests 的「只持有、不等待」同效，
        // 测试运行时 drop 时这些任务一并被 abort。
        let _namesrv_task = spawn_namesrv(namesrv, broker_addr.clone());
        let _broker_task = spawn_broker(broker, state.clone());
        DetectCluster { namesrv_addr, state }
    }

    /// 脚本化探测应答：是否 SUCCESS、应答前延迟多少毫秒。
    fn set_probe(&self, ok: bool, delay_ms: u64) {
        self.state.max_offset_ok.store(ok, Ordering::SeqCst);
        self.state.max_offset_delay_ms.store(delay_ms, Ordering::SeqCst);
    }

    /// broker 已收到的 GET_MAX_OFFSET 请求数。
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

/// 带请求 `opaque` 的响应（客户端按 opaque 配对，串了就当噪声丢掉）。
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
    serde_json::to_vec(&route.to_json_value()).expect("路由可序列化")
}

/// namesrv：对任何 topic 都回同一份路由（broker-0 → 固定地址），其余 SUCCESS。
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

/// broker：GET_MAX_OFFSET 按 `DetectState` 脚本应答，SEND 回 SUCCESS，其余即时 SUCCESS。
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

// ---------------------------------------------------------------- 夹具

/// 起一个指向假集群、按给定参数开启探测的生产者。`instance` 必须唯一：
/// 同 clientId 会共享 [`MQClientInstance`]（连带路由表）。
async fn started(
    instance: &str,
    cluster: &DetectCluster,
    detector_enable: bool,
    detect_timeout: i64,
    detect_interval: i64,
) -> DefaultMQProducer {
    let producer = DefaultMQProducer::new("GID_fault_detector").expect("组名合法");
    producer.set_instance_name(instance);
    producer.set_namesrv_addr(&cluster.namesrv_addr);
    producer.set_send_latency_fault_enable(true);
    producer.set_start_detector_enable(detector_enable);
    producer.set_detect_timeout(detect_timeout);
    producer.set_detect_interval(detect_interval);
    assert_eq!(producer.is_start_detector_enable(), detector_enable);
    assert_eq!(producer.get_detect_timeout(), detect_timeout);
    assert_eq!(producer.get_detect_interval(), detect_interval);
    producer.start().await.expect("假集群里 start 应当成功");
    producer
}

/// 发一笔消息把路由种进客户端实例：探测的 `Resolver`（broker 地址表）与
/// `pickTopic`（路由表）都靠它，没有这一步探测无从下手。
async fn seed_route(producer: &DefaultMQProducer) {
    let mut msg = Message::new("T1", Some(b"body"));
    producer
        .send(&mut msg, Some(30_000), None)
        .await
        .expect("假集群里发送应当成功");
}

/// 轮询等待 `cond` 成立（探测 3s 一轮，预算按 20s 给足，避免环境慢导致的抖动）。
async fn wait_for(cond: impl Fn() -> bool, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !cond() {
        assert!(Instant::now() < deadline, "20s 内没有等到：{what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ---------------------------------------------------------------- 用例

#[test]
fn detector_config_defaults_match_java_client_config() {
    let cfg = ProducerConfig::default();
    assert!(!cfg.start_detector_enable, "startDetectorEnable 默认 false");
    assert_eq!(cfg.detect_timeout, 200);
    assert_eq!(cfg.detect_interval, 2 * 1000);

    // setter/getter 往返
    let producer = DefaultMQProducer::new("GID_fault_detector").expect("组名合法");
    assert!(!producer.is_start_detector_enable());
    producer.set_start_detector_enable(true);
    assert!(producer.is_start_detector_enable());
    producer.set_detect_timeout(500);
    assert_eq!(producer.get_detect_timeout(), 500);
    producer.set_detect_interval(4_000);
    assert_eq!(producer.get_detect_interval(), 4_000);
}

/// 开关默认关：故障表里哪怕有不可达的 broker，也不该有任何探测流量。
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

    // 探测任务确实在跑（3s 首轮），但开关关着不该发任何 GET_MAX_OFFSET
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert_eq!(cluster.requests(), 0, "探测关着不该有探测流量");
    assert!(
        !producer
            .fault_strategy()
            .latency_fault_tolerance()
            .is_reachable("broker-0"),
        "没人探测，broker 维持不可达"
    );
    producer.shutdown();
}

/// 核心场景：发送链路记坏可达性之后，探测周期发 `GET_MAX_OFFSET`，
/// 探活成功把 broker 放回（恢复放行），且之后仍按周期继续探。
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
        "探测成功后 broker-0 应回到可达",
    )
    .await;
    assert!(cluster.requests() >= 1, "探测必须真的发过 GET_MAX_OFFSET(30)");

    // 周期触发：恢复后故障项仍在表里，后续轮次继续探（请求计数继续涨）
    let seen = cluster.requests();
    wait_for(
        || cluster.requests() > seen,
        "探测应按 3s 周期继续发起 GET_MAX_OFFSET",
    )
    .await;
    assert!(tolerance.is_reachable("broker-0"));

    producer.shutdown();
}

/// 超时路径：应答延迟超过 `detectTimeout` ⇒ 每轮探测都失败 ⇒ broker 维持不可达；
/// broker 恢复即时应答后，下一轮探测把它放回来。
#[tokio::test]
async fn detect_timeout_keeps_broker_unreachable_until_it_recovers() {
    let cluster = DetectCluster::start().await;
    // 应答 800ms > detectTimeout=200ms ⇒ 探测必超时（走协议层超时，不碰 dead port）
    cluster.set_probe(true, 800);
    let producer = started("detector_timeout", &cluster, true, 200, 300).await;
    seed_route(&producer).await;

    let tolerance = producer.inner.fault_strategy.latency_fault_tolerance();
    producer.fault_strategy().update_fault_item("broker-0", 100, true, false);
    assert!(!tolerance.is_reachable("broker-0"));

    wait_for(
        || cluster.requests() >= 2,
        "12s 内应发生至少两轮探测（3s 首轮 + 3s 周期）",
    )
    .await;
    assert!(
        !tolerance.is_reachable("broker-0"),
        "探测超时不能把 broker 探回来"
    );

    // broker 恢复即时应答 ⇒ 下一轮探测放行
    cluster.set_probe(true, 0);
    wait_for(
        || tolerance.is_reachable("broker-0"),
        "broker 恢复后下一轮探测应把 broker-0 放回",
    )
    .await;

    producer.shutdown();
}
