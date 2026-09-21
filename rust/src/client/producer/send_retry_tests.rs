//! 发送重试内核（Java `sendDefaultImpl`）的离线对拍。
//!
//! 真集群造不出 `SYSTEM_BUSY`，也造不出「慢 broker 把总预算吃光」，而这两条
//! 恰好是这段内核的全部难点，所以这里在进程内起一个**假集群**（1 个 namesrv +
//! N 个 broker，只说 remoting 协议），把每个 broker 的应答码和应答延迟脚本化。
//!
//! 末尾另有一节「unitMode / enableStreamRequestType」用同一套假集群做**线上报文**断言
//! （`unitMode` 落在 V2 头的单字母键 `k`、`ReqT` 由 stream 钩子在 encode 之前写入），
//! 与 `python/tests/test_send_retry.py`、`cpp/tests/test_send_retry.cpp` 同题。

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

// ---------------------------------------------------------------- 假集群

/// 单个 broker 的脚本：按顺序弹出 `(应答码, 应答前 sleep 毫秒)`，耗尽后一直用 `tail`。
#[derive(Debug)]
struct BrokerScript {
    steps: VecDeque<(i32, u64)>,
    tail: (i32, u64),
    requests: usize,
    /// 每笔 SEND 请求**上线时**的 extFields 快照（钩子已经跑完，等价于真报文）。
    sends: Vec<Vec<(String, String)>>,
    /// 与 `sends` 一一对应的请求码（310 单条 / 320 批量 / 325 应答）。
    send_codes: Vec<i32>,
}

impl BrokerScript {
    fn new() -> BrokerScript {
        BrokerScript {
            steps: VecDeque::new(),
            tail: (response_code::SUCCESS, 0),
            requests: 0,
            sends: Vec::new(),
            send_codes: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
struct ClusterState {
    /// `false` ⇒ namesrv 对任何 topic 都回 `TOPIC_NOT_EXIST`（拿不到路由）
    route_ok: bool,
    brokers: Vec<BrokerScript>,
}

/// 进程内假集群。broker 名固定为 `broker-0..N`，路由里的队列顺序与之一致，
/// 因此第一次发送必然落在 `broker-0`（选队是轮询，游标从 0 起）。
struct MockCluster {
    namesrv_addr: String,
    state: Arc<Mutex<ClusterState>>,
    /// 只持有、不等待：测试运行时结束时会被 abort。
    tasks: Vec<JoinHandle<()>>,
}

impl MockCluster {
    /// 起 `broker_count` 个真监听的 broker + 一个 namesrv。
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

    /// 只起 namesrv，路由指向给定地址（可以用一个已经关掉的端口造连接失败）。
    async fn with_addrs(broker_addrs: Vec<String>, route_ok: bool) -> MockCluster {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock namesrv");
        let namesrv_addr = listener.local_addr().expect("mock namesrv addr").to_string();
        let state = Arc::new(Mutex::new(ClusterState {
            route_ok,
            brokers: (0..broker_addrs.len()).map(|_| BrokerScript::new()).collect(),
        }));
        let tasks = vec![spawn_namesrv(listener, Arc::clone(&state), broker_addrs)];
        MockCluster { namesrv_addr, state, tasks }
    }

    /// 脚本化第 `index` 个 broker：先按 `steps` 依次应答，之后一直用 `tail`。
    fn script(&self, index: usize, steps: Vec<(i32, u64)>, tail: (i32, u64)) {
        let mut state = lock(&self.state);
        let broker = &mut state.brokers[index];
        broker.steps = steps.into();
        broker.tail = tail;
        broker.requests = 0;
        broker.sends.clear();
        broker.send_codes.clear();
    }

    /// 第 `index` 个 broker 收到的 SEND 请求数。
    fn requests(&self, index: usize) -> usize {
        lock(&self.state).brokers[index].requests
    }

    /// 第 `index` 个 broker 收到的第 `n` 笔 SEND 的 extFields 快照。
    fn send_ext(&self, index: usize, n: usize) -> Vec<(String, String)> {
        lock(&self.state).brokers[index].sends[n].clone()
    }

    /// 第 `index` 个 broker 收到的第 `n` 笔 SEND 的请求码。
    fn send_code(&self, index: usize, n: usize) -> i32 {
        lock(&self.state).brokers[index].send_codes[n]
    }
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

/// 带请求 `opaque` 的响应（客户端按 opaque 配对，串了就当噪声丢掉）。
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
    serde_json::to_vec(&route.to_json_value()).expect("路由可序列化")
}

/// namesrv：只答 `GET_ROUTEINFO_BY_TOPIC`，其余一律 SUCCESS。
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

/// broker：SEND 按脚本应答，其它请求（心跳等）回 SUCCESS。
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
                        // 快照必须在应答之前：这时拿到的是钩子处理完、真正上线的那份头
                        broker.sends.push(
                            request
                                .ext_fields()
                                .iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect(),
                        );
                        broker.send_codes.push(request.code);
                        (step.0, step.1, broker.requests)
                    };
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

// ---------------------------------------------------------------- 生产者夹具

/// 起一个指向假集群的生产者。`instance` 必须每个用例唯一：同 clientId 会共享
/// [`MQClientInstance`]（连带路由表和 namesrv 地址）。
async fn started(instance: &str, cluster: &MockCluster) -> DefaultMQProducer {
    let producer = DefaultMQProducer::new("GID_send_retry").expect("组名合法");
    producer.set_instance_name(instance);
    producer.set_namesrv_addr(&cluster.namesrv_addr);
    producer.start().await.expect("假集群里 start 应当成功");
    producer
}

/// 失败时把错误打出来，断言才好读。
fn expect_client_code(err: Error) -> (Option<i32>, String) {
    match err {
        Error::Client { response_code, message } => (response_code, message),
        other => panic!("期望 MQClientException，实际 {other}"),
    }
}

// ---------------------------------------------------------------- 用例

/// 默认集合与 Java `DefaultMQProducer#retryResponseCodes` 完全一致。
#[test]
fn default_retry_response_codes_match_java() {
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

/// 三档异常在故障表和重试决策上各不相同，不能合成一个 `retryable`。
#[test]
fn send_errors_classify_like_python_except_arms() {
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

/// 挂钟会回退，算重试预算只能用单调时钟（负延迟会把慢 broker 记成快 broker）。
#[test]
fn latency_since_never_goes_backwards() {
    let began = monotonic_millis();
    std::thread::sleep(Duration::from_millis(15));
    let first = latency_since(began);
    let second = latency_since(began);
    assert!(first >= 10, "睡了 15ms，实测 {first}ms");
    assert!(second >= first, "单调时钟不能倒退: {first} -> {second}");
}

#[tokio::test]
async fn retryable_broker_code_switches_to_another_broker() {
    let cluster = MockCluster::start(2, true).await;
    cluster.script(0, vec![(response_code::SYSTEM_BUSY, 0)], (response_code::SUCCESS, 0));
    cluster.script(1, vec![], (response_code::SUCCESS, 0));
    let producer = started("retry_switch", &cluster).await;

    let mut msg = Message::new("T1", Some(b"body"));
    let result = producer.send(&mut msg, None, None).await.expect("可重试码应换 broker 成功");
    assert_eq!(result.status, SendStatus::SendOk);
    assert_eq!(cluster.requests(0), 1, "第一台回了 SYSTEM_BUSY");
    assert_eq!(cluster.requests(1), 1, "第二台被轮到一次");
    producer.shutdown();
}

#[tokio::test]
async fn non_retryable_broker_code_throws_at_once() {
    let cluster = MockCluster::start(2, true).await;
    cluster.script(0, vec![], (response_code::MESSAGE_ILLEGAL, 0));
    cluster.script(1, vec![], (response_code::SUCCESS, 0));
    let producer = started("retry_illegal", &cluster).await;

    let mut msg = Message::new("T1", Some(b"body"));
    let err = producer.send(&mut msg, None, None).await.expect_err("MESSAGE_ILLEGAL 必须原样抛");
    assert_eq!(err.response_code(), Some(response_code::MESSAGE_ILLEGAL));
    assert!(matches!(err, Error::Broker { .. }), "抛的应当是 MQBrokerException: {err}");
    assert_eq!(cluster.requests(0), 1);
    assert_eq!(cluster.requests(1), 0, "确定性错误不该再试第二台");
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
    // 压根没等到响应码（连接都没通）等于不可重试，与 Python `None in set` 一致
    assert!(!producer.is_retry_response_code(None));

    let mut msg = Message::new("T1", Some(b"body"));
    let result = producer.send(&mut msg, None, None).await.expect("加了码就该换 broker");
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
    let err = producer.send(&mut msg, None, None).await.expect_err("全失败必须抛错");
    let (code, message) = expect_client_code(err);
    // 最后一次失败是 broker 码 ⇒ 沿用该码（Python `code = last_exc.response_code`）
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
    let err = producer.send(&mut msg, None, None).await.expect_err("没有路由必须失败");
    let (code, message) = expect_client_code(err);
    assert_eq!(code, Some(client_error_code::NOT_FOUND_TOPIC_EXCEPTION));
    assert!(message.contains("Message Queue"), "{message}");
    assert_eq!(cluster.requests(0), 0, "路由都拿不到，不该发一条");
    producer.shutdown();
}

#[tokio::test]
async fn connect_failure_is_qualified_with_10001() {
    // 先占一个端口再立刻释放，路由指向它 ⇒ 连接必然被拒
    let dead_addr = {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind dead port");
        listener.local_addr().expect("dead addr").to_string()
    };
    let cluster = MockCluster::with_addrs(vec![dead_addr], true).await;
    let producer = started("retry_connect", &cluster).await;

    let mut msg = Message::new("T1", Some(b"body"));
    let err = producer.send(&mut msg, None, None).await.expect_err("连不上必须失败");
    let (code, message) = expect_client_code(err);
    assert_eq!(code, Some(client_error_code::CONNECT_BROKER_EXCEPTION), "{message}");
    producer.shutdown();
}

/// `sendMsgMaxTimeoutPerRequest` 的全部意义：慢 broker 只能吃掉被压过的那一小段，
/// 剩下的预算留给下一台。不设上限时第一台会独占 700ms，整次发送必然更慢。
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
        .expect("被压过的单次超时之后应当换到健康的 broker");
    let elapsed = began.elapsed();
    assert_eq!(result.status, SendStatus::SendOk);
    assert!(
        elapsed < Duration::from_millis(500),
        "单次超时没被压到 150ms：整次发送用了 {}ms",
        elapsed.as_millis()
    );
    assert_eq!(cluster.requests(0), 1);
    assert_eq!(cluster.requests(1), 1);
    producer.shutdown();
}

/// 总预算被第一台吃光后，剩下的 broker 一次都试不到 —— 这时抛的是
/// `RemotingTooMuchRequestException("sendDefaultImpl call timeout")`。
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
        .expect_err("500ms 的 broker 遇上的 50ms 预算必然超时");
    assert!(
        matches!(err, Error::TooMuchRequest(_)),
        "期望 sendDefaultImpl call timeout，实际 {err}"
    );
    assert_eq!(err.to_string(), "too much request: sendDefaultImpl call timeout");
    producer.shutdown();
}

#[tokio::test]
async fn not_store_ok_only_switches_broker_when_configured() {
    // FLUSH_DISK_TIMEOUT 不是异常，是「存了但没存好」的正常应答
    let cluster = MockCluster::start(2, true).await;
    cluster.script(0, vec![], (response_code::FLUSH_DISK_TIMEOUT, 0));
    cluster.script(1, vec![], (response_code::SUCCESS, 0));
    let producer = started("retry_not_store", &cluster).await;

    let mut msg = Message::new("T1", Some(b"body"));
    let result = producer.send(&mut msg, None, None).await.expect("默认原样返回该结果");
    assert_eq!(result.status, SendStatus::FlushDiskTimeout);
    assert_eq!(cluster.requests(1), 0, "没开开关就不该换 broker");

    producer.set_retry_another_broker_when_not_store_ok(true);
    assert!(producer.is_retry_another_broker_when_not_store_ok());
    let mut msg2 = Message::new("T1", Some(b"body"));
    let result = producer.send(&mut msg2, None, None).await.expect("开了开关应当换 broker");
    assert_eq!(result.status, SendStatus::SendOk);
    assert_eq!(cluster.requests(1), 1);
    producer.shutdown();
}

/// 故障规避打开时：失败的 broker 要被隔离（不可选），故障表里记的仍是**真实延迟**，
/// 只有算隔离窗口时才换成 10000ms 的哨兵值。
#[tokio::test]
async fn failed_broker_is_isolated_and_latency_is_recorded() {
    let cluster = MockCluster::start(2, true).await;
    cluster.script(0, vec![], (response_code::SERVICE_NOT_AVAILABLE, 30));
    cluster.script(1, vec![], (response_code::SUCCESS, 0));
    let producer = started("retry_fault", &cluster).await;
    producer.set_send_latency_fault_enable(true);
    assert!(producer.is_retry_response_code(Some(response_code::SERVICE_NOT_AVAILABLE)));

    let mut msg = Message::new("T1", Some(b"body"));
    let result = producer.send(&mut msg, None, None).await.expect("隔离后应换到健康 broker");
    assert_eq!(result.status, SendStatus::SendOk);

    let tolerance = producer.inner.fault_strategy.latency_fault_tolerance();
    assert!(!tolerance.is_available("broker-0"), "回了错误码的 broker 必须被隔离");
    let failed = tolerance.get_fault_item("broker-0").expect("失败的 broker 要进表");
    assert!(
        failed.get_current_latency() >= 10 && failed.get_current_latency() < ISOLATION_LATENCY,
        "故障表里该记真实延迟（30ms 左右），实际 {}",
        failed.get_current_latency()
    );
    assert!(!failed.is_reachable(), "MQBrokerException 一档传 reachable=False");
    let ok = tolerance.get_fault_item("broker-1").expect("成功的 broker 要进表");
    assert!(ok.is_available(tolerance.now_millis()), "健康的 broker 不该被隔离");
    assert!(ok.is_reachable(), "成功一档传 reachable=True");
    producer.shutdown();
}

// ---------------------------------------------------------------- unitMode / stream

/// 从 extFields 快照里取值（顺序无关，只按 key 找）。
fn ext_value<'a>(ext: &'a [(String, String)], key: &str) -> Option<&'a str> {
    ext.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

/// Java `sendKernelImpl:1004` 把 `tc.isUnitMode()` 写进发送头，
/// V2 头再映射成单字母键 `k`（`SendMessageRequestHeaderV2`）。
#[tokio::test]
async fn unit_mode_is_carried_in_the_send_header() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));

    let on = DefaultMQProducer::new("GID_send_retry").expect("组名合法");
    on.set_instance_name("unit_mode_on");
    on.set_namesrv_addr(&cluster.namesrv_addr);
    on.set_unit_mode(true);
    on.start().await.expect("假集群里 start 应当成功");
    let mut msg = Message::new("T1", Some(b"unit-on"));
    on.send(&mut msg, None, None).await.expect("发送应当成功");

    let off = DefaultMQProducer::new("GID_send_retry").expect("组名合法");
    off.set_instance_name("unit_mode_off");
    off.set_namesrv_addr(&cluster.namesrv_addr);
    off.start().await.expect("默认 unitMode=false");
    let mut msg = Message::new("T1", Some(b"unit-off"));
    off.send(&mut msg, None, None).await.expect("发送应当成功");

    assert_eq!(ext_value(&cluster.send_ext(0, 0), "k"), Some("true"));
    assert_eq!(ext_value(&cluster.send_ext(0, 1), "k"), Some("false"));
    assert_eq!(ext_value(&cluster.send_ext(0, 1), "a"), Some("GID_send_retry"));
    on.shutdown();
    off.shutdown();
}

/// 单位名要同时出现在 clientId 与线上报文里，且不影响发送。
#[tokio::test]
async fn unit_name_only_changes_the_client_id() {
    let cluster = MockCluster::start(1, true).await;
    let producer = DefaultMQProducer::new("GID_send_retry").expect("组名合法");
    producer.set_instance_name("unit_name_case");
    producer.set_namesrv_addr(&cluster.namesrv_addr);
    producer.set_unit_name(Some("unitA"));
    producer.start().await.expect("start");
    assert_eq!(
        producer.client_id(),
        Some(format!("{}@unit_name_case@unitA", MixAll::cached_ip_str()))
    );
    let mut msg = Message::new("T1", Some(b"body"));
    producer.send(&mut msg, None, None).await.expect("发送应当成功");
    // unitName 不进发送头：Java 只有 unitMode 上线，unitName 只影响 clientId/地址服务器
    assert!(cluster.send_ext(0, 0).iter().all(|(k, _)| k != "unitName"));
    producer.shutdown();
}

/// Java `MQClientAPIImpl#sendMessage:550-563` 的三级判据，逐条抓线取证：
/// 先 `isReply` → 325，再 `msg instanceof MessageBatch` → 320，否则 310。
///
/// ⚠ 请求码与 V2 头的 `m`（batch）是**两件不同的事**：broker 真正按 `m` 选
/// `sendBatchMessage` 还是单条写入（`SendMessageProcessor:117` 读
/// `requestHeader.isBatch()`），码只影响服务端按码归类（proxy/auth 把 310/320 列在
/// 同一个 case，见 `AbstractRemotingActivity:69`、
/// `DefaultAuthorizationContextBuilder:230-240`）。所以两个都得断言 ——
/// 只对齐码不对齐 `m`，批量 body 会被按单条解析。
#[tokio::test]
async fn send_request_code_follows_java_three_way_branch() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let producer = started("send_code_branch", &cluster).await;

    let mut single = Message::new("T1", Some(b"single"));
    producer.send(&mut single, None, None).await.expect("发送应当成功");
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
    batch.expect("批量发送应当成功");
    assert_eq!(cluster.send_code(0, 1), request_code::SEND_BATCH_MESSAGE);
    assert_eq!(ext_value(&cluster.send_ext(0, 1), "m"), Some("true"));

    // reply 优先于 batch：Java 先判 isReply，所以「带 reply 属性的批量」仍走 325。
    let mut reply_batch = MessageBatch::generate_from_list(vec![
        Message::new("T1", Some(b"r-0")),
        Message::new("T1", Some(b"r-1")),
    ])
    .expect("同 topic、非延迟、非重试，应当合法");
    reply_batch
        .message
        .put_property(
            crate::common::message_const::PROPERTY_MESSAGE_TYPE,
            MixAll::REPLY_MESSAGE_FLAG,
        );
    let mut publish = PublishMessage::Batch(&mut reply_batch);
    let mq = MessageQueue::new("T1", "broker-0", 0);
    let client = producer.require_client().expect("client 已启动");
    client
        .send_message("GID_send_retry", &mut publish, &mq, 3_000, 0, false)
        .await
        .expect("应答批量发送应当成功");
    assert_eq!(cluster.send_code(0, 2), request_code::SEND_REPLY_MESSAGE_V2);
    assert_eq!(ext_value(&cluster.send_ext(0, 2), "m"), Some("true"));

    producer.shutdown();
}

/// `ReqT` 必须由 stream 钩子**在用户钩子之前**写入：Java 的注释
/// "Inject stream rpc hook first to make reserve field signature" 说明它得进 ACL 签名，
/// 而签名由用户钩子（AclClientRPCHook）算 —— 顺序反了签名内容就不含 ReqT。
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
    .expect("组名合法");
    producer.set_instance_name("stream_on");
    producer.set_namesrv_addr(&cluster.namesrv_addr);
    producer.set_enable_stream_request_type(true);
    producer.start().await.expect("start");
    assert!(
        producer.client_id().unwrap_or_default().ends_with("@STREAM"),
        "开启 stream 后 clientId 要带 @STREAM 后缀"
    );

    let mut msg = Message::new("T1", Some(b"body"));
    producer.send(&mut msg, None, None).await.expect("发送应当成功");
    let ext = cluster.send_ext(0, 0);
    assert_eq!(
        ext_value(&ext, MixAll::REQ_T),
        Some("0"),
        "ReqT 要出现在上线报文里（值是 RequestType.STREAM 的 code）"
    );
    assert!(
        probe.req_t_seen_by_user_hook.load(Ordering::SeqCst),
        "用户钩子跑的时候 ReqT 必须已经写入"
    );
    producer.shutdown();
}

/// 默认不开：普通生产者的请求里不该出现 ReqT。
#[tokio::test]
async fn stream_request_type_is_off_by_default_for_producers() {
    let cluster = MockCluster::start(1, true).await;
    let producer = started("stream_off", &cluster).await;
    assert!(!producer.config().enable_stream_request_type);
    let mut msg = Message::new("T1", Some(b"body"));
    producer.send(&mut msg, None, None).await.expect("发送应当成功");
    let ext = cluster.send_ext(0, 0);
    assert!(ext_value(&ext, MixAll::REQ_T).is_none(), "默认不该带 ReqT: {ext:?}");
    assert!(
        !producer.client_id().unwrap_or_default().ends_with("@STREAM"),
        "默认 clientId 不该带 @STREAM"
    );
    producer.shutdown();
}
