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

/// 脚本里表示「读到请求、快照下来，然后**关掉连接不回答**」的应答码。
///
/// 对端关闭会让客户端拿到 [`Error::SendRequest`]（`connection closed`），也就是 Java
/// 的 `RemotingSendRequestException` —— 异步链要按「换一台 broker 再试」处理它。
/// 真集群造不出这种失败，脚本化的应答码又都代表「收到了响应」，所以单独开一个哨兵值。
const CLOSE_WITHOUT_ANSWER: i32 = -1;

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
    /// 与 `sends` 一一对应的 `opaque`：异步重试必须复用同一个请求、**换新的 opaque**，
    /// 这是唯一能看出「换 opaque 了」的地方。
    send_opaques: Vec<i32>,
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
    /// 路由里 `broker-0..N` 的监听地址，顺序与路由一致。`with_addrs` 下就是传进来的那份。
    broker_addrs: Vec<String>,
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
        let count = broker_addrs.len();
        let state = Arc::new(Mutex::new(ClusterState {
            route_ok,
            brokers: (0..count).map(|_| BrokerScript::new()).collect(),
        }));
        let tasks = vec![spawn_namesrv(listener, Arc::clone(&state), broker_addrs.clone())];
        MockCluster { namesrv_addr, broker_addrs, state, tasks }
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
        broker.send_opaques.clear();
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

    /// 第 `index` 个 broker 收到的第 `n` 笔 SEND 的 `opaque`。
    fn send_opaque(&self, index: usize, n: usize) -> i32 {
        lock(&self.state).brokers[index].send_opaques[n]
    }
}

/// 一个**没人监听**的地址：连上去立刻被拒（`Error::Connect`）。
///
/// 绑定后再立刻关掉，比凭空编一个端口更可靠（那个端口随时可能被人占上）。
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
                        broker.send_opaques.push(request.opaque);
                        (step.0, step.1, broker.requests)
                    };
                    if code == CLOSE_WITHOUT_ANSWER {
                        // 丢掉连接：客户端看到的是 `Error::SendRequest`（connection closed）
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

// ---------------------------------------------------------------- 异步发送背压
//
// 对端是 Java `DefaultMQProducerImpl:635-682`（两道闸、共享一份预算）与 `:577-633`
// （`BackpressureSendCallBack` 的归还），与 `python/tests/test_producer_async.py`、
// `cpp/tests/test_producer_async.cpp` 及 .NET 的同题用例一一对应。
//
// 字节闸的地板值是 1M（Java `:148-153`），所以只能拿「1M 少掉多少」来断言在途字节，
// 不能把上限配成几百字节 —— 那会被夹回 1M。
//
// 「发送队列满了怎么办」（Java `:675-681`）在下一节「异步发送内核」里：那道分支要有界
// 队列才谈得上，本端口 #50 之后才有。

use std::sync::atomic::AtomicUsize;

/// 记录回调的发送回调（Python 测试里的 `_Callback`）。
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

/// 数 before/after 钩子各跑了几次，并记下 after 有没有看到结果/异常。
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

/// 一个**耗时的**拦截钩子：用来把异步内核的准备段预算花光（真集群造不出这段时长）。
struct SleepingForbiddenHook {
    millis: u64,
}

impl CheckForbiddenHook for SleepingForbiddenHook {
    fn hook_name(&self) -> &str {
        "sleeping"
    }

    fn check_forbidden(&self, _context: &mut CheckForbiddenContext) -> Result<()> {
        // 钩子接口是同步的（Java 也是），所以只能睡挂钟；挂钟正是预算用的时钟。
        std::thread::sleep(Duration::from_millis(self.millis));
        Ok(())
    }
}

/// 一个**阻塞式**的 before 钩子：既把「在途」占住（许可要到链终点才归还），又按标签记下
/// 每笔进钩子的相对时刻 —— 真机验证脚本里的 `SlowHook` 同构。
///
/// 占住的是**池消费者所在的线程**，所以这个钩子同时测到「池子被在途占满」这一维。
struct ParkingSendHook {
    millis: std::sync::atomic::AtomicI64,
    clock: Mutex<Option<Instant>>,
    entries: Mutex<Vec<(String, u128)>>,
}

impl ParkingSendHook {
    fn new(millis: i64) -> ParkingSendHook {
        ParkingSendHook {
            millis: std::sync::atomic::AtomicI64::new(millis),
            clock: Mutex::new(None),
            entries: Mutex::new(Vec::new()),
        }
    }

    /// 重新计时并清空记录，让相邻用例互不干扰。
    fn begin(&self) {
        *lock(&self.clock) = Some(Instant::now());
        lock(&self.entries).clear();
    }

    fn count_of(&self, label: &str) -> usize {
        lock(&self.entries).iter().filter(|(l, _)| l == label).count()
    }

    fn first_entry_of(&self, label: &str) -> Option<u128> {
        lock(&self.entries)
            .iter()
            .filter(|(l, _)| l == label)
            .map(|(_, at)| *at)
            .min()
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
        let millis = self.millis.load(Ordering::SeqCst);
        if millis > 0 {
            std::thread::sleep(Duration::from_millis(millis as u64));
        }
        Ok(())
    }
}

/// 带标签的消息（钩子按 keys 认领是哪一笔）。
fn labelled(topic_len: usize, label: &str) -> Message {
    let mut msg = body_of(topic_len);
    msg.set_keys(label);
    msg
}

/// 开了背压的生产者：条数闸设成 `num`、字节闸夹到地板值 1M（同 Python 的夹具）。
///
/// 字节闸只能配到 1M —— 再小会被夹回来，而 1M 已经够把「一笔扣了多少字节」算清楚
/// （body 只有几百字节）。
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

/// 同上，再挂一个阻塞式 before 钩子（占住在途用）。
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

/// 真机验证 B3 的离线对拍（`examples/live_backpressure.rs`）：**池子消费者全被在途占住
/// 时，运行时扩容仍然要把卡在闸上的那一笔叫醒并真的发出去**。
///
/// 单线程运行时测不出这件事——那里池任务只在调用方 await 时才跑，闸和池纠缠不到一起，
/// 所以这一条要显式多样本运行时（与真机脚本同样的形状）。丢唤醒如果回归，这里会先红。
#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
async fn resize_wakes_the_parked_sender_while_the_pool_is_saturated() {
    const HOLD_MS: i64 = 600;
    const RESIZE_AT: u64 = 100;

    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 0));
    let hook = Arc::new(ParkingSendHook::new(HOLD_MS));
    let producer =
        backpressure_producer_with_hook("resize_in_pool", &cluster, MIN_ASYNC_SEND_NUM, hook.clone())
            .await;
    hook.begin();

    let held = Arc::new(Recorder::default());
    for _ in 0..MIN_ASYNC_SEND_NUM {
        producer
            .send_async(labelled(8, "held"), held.clone(), Some(15_000), None)
            .expect("10 笔都该排进池队列");
    }
    // 必须等闸门真的被占满再补第 11 笔：池子里的任务和主任务并发，谁先到闸口不一定。
    // 先过闸的那笔会拿到许可，于是「卡住的第 11 笔」会变成「卡住的第 11 笔 held」，
    // 下面的判定就只是运气了（真机 B3 用看门线程盯同一件事）。
    assert!(
        wait_until(|| producer.semaphore_async_send_num_available_permits() == 0).await,
        "10 笔没能占满条数闸（空闲 {}）",
        producer.semaphore_async_send_num_available_permits()
    );

    let woken = Arc::new(Recorder::default());
    producer
        .send_async(labelled(8, "woken"), woken.clone(), Some(15_000), None)
        .expect("第 11 笔也该排进池队列");

    tokio::time::sleep(Duration::from_millis(RESIZE_AT)).await;
    assert_eq!(
        hook.count_of("woken"),
        0,
        "扩容之前第 11 笔不该过闸（钩子记录: {}）",
        hook.dump()
    );
    producer.set_back_pressure_for_async_send_num(MIN_ASYNC_SEND_NUM + 2);

    assert!(
        wait_until(|| woken.done.load(Ordering::SeqCst) == 1).await,
        "扩容没把卡在闸上的那笔叫醒: {:?}",
        woken.errors()
    );
    assert_eq!(woken.ok.load(Ordering::SeqCst), 1, "{:?}", woken.errors());
    // 放行它的只可能是扩容：那一刻既晚于扩容、又早于 10 笔在途归还。
    let entered = hook.first_entry_of("woken");
    assert!(
        entered.is_some_and(|at| at as u64 >= RESIZE_AT && (at as i64) < HOLD_MS),
        "第 11 笔进内核于 {entered:?}ms（扩容在 {RESIZE_AT}ms、在途到 ≈{HOLD_MS}ms 才归还）",
    );

    assert!(wait_until(|| held.done.load(Ordering::SeqCst)
        == MIN_ASYNC_SEND_NUM as usize)
        .await);
    assert_eq!(cluster.requests(0), MIN_ASYNC_SEND_NUM as usize + 1);
    assert_eq!(
        producer.semaphore_async_send_num_available_permits(),
        MIN_ASYNC_SEND_NUM + 2,
        "全部落地后空闲许可 = 新容量"
    );
    producer.shutdown();
}

fn body_of(len: usize) -> Message {
    let body = vec![b'x'; len];
    Message::new("T1", Some(&body))
}

/// 轮询等待条件成立（最多约 3s），避免用固定 sleep 猜时长。
async fn wait_until<F: Fn() -> bool>(cond: F) -> bool {
    for _ in 0..300 {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cond()
}

/// Java `DefaultMQProducer:169/175/181` —— 默认关，1024 条 / 100M 字节。
#[test]
fn backpressure_defaults_match_java() {
    let producer = DefaultMQProducer::new("GID_bp_default").expect("组名合法");
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

/// 配置越界时**构造**就夹到地板值（Java 建 impl 时的 `:141-153` 分支）。
#[test]
fn constructor_floors_undersized_capacities() {
    let cfg = ProducerConfig {
        producer_group: "GID_bp_floor".to_string(),
        back_pressure_for_async_send_num: 1,
        back_pressure_for_async_send_size: 1024,
        ..Default::default()
    };
    let producer = DefaultMQProducer::with_config(cfg).expect("配置合法");
    assert_eq!(
        producer.get_back_pressure_for_async_send_num(),
        1,
        "配置值本身不改（Java 的字段同样原样留着）"
    );
    assert_eq!(
        producer.semaphore_async_send_num_available_permits(),
        MIN_ASYNC_SEND_NUM,
        "但信号量按地板值建"
    );
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        MIN_ASYNC_SEND_SIZE
    );
}

/// Java `DefaultMQProducer:1385/1402` —— setter 也夹地板值，且配置跟着走。
#[test]
fn setter_floors_both_gates() {
    let producer = DefaultMQProducer::new("GID_bp_setter").expect("组名合法");
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

/// 关着的时候一笔发送既不扣也不还许可。
#[tokio::test]
async fn disabled_gate_stays_out_of_the_way() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 300));
    let producer = started("bp_off", &cluster).await;
    assert!(!producer.is_enable_backpressure_for_async_mode());

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(400), cb.clone(), Some(3_000), None)
        .expect("运行时内可派发");
    // 在途时容量一分未动
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

/// Java `:654-658` —— 条数拿不到就回调，一次请求都不发（预算被闸自己花光）。
#[tokio::test]
async fn num_gate_rejects_with_java_message_and_sends_nothing() {
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
        .expect("运行时内可派发");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    let errors = cb.errors();
    assert_eq!(errors.len(), 1);
    assert!(
        errors[0].contains("send message tryAcquire semaphoreAsyncNum timeout"),
        "文案要与 Java 逐字一致: {}",
        errors[0]
    );
    assert!(
        began.elapsed() >= Duration::from_millis(250),
        "没等到超时就把失败交出去了：{:?}",
        began.elapsed()
    );
    assert_eq!(cluster.requests(0), 0, "被闸拒绝时一次请求都不该发出去");
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        MIN_ASYNC_SEND_SIZE,
        "条数闸没过时不该去扣字节"
    );
    producer
        .inner
        .semaphore_async_send_num
        .release(MIN_ASYNC_SEND_NUM);
    producer.shutdown();
}

/// Java `:667-671` —— 字节闸没过时，**已经拿到**的条数许可必须归还。
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
        .expect("运行时内可派发");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    let errors = cb.errors();
    assert!(
        errors[0].contains("send message tryAcquire semaphoreAsyncSize timeout"),
        "文案要与 Java 逐字一致: {}",
        errors[0]
    );
    assert_eq!(
        producer.semaphore_async_send_num_available_permits(),
        MIN_ASYNC_SEND_NUM,
        "条数许可漏还了"
    );
    assert_eq!(cluster.requests(0), 0);
    producer
        .inner
        .semaphore_async_send_size
        .release(MIN_ASYNC_SEND_SIZE);
    producer.shutdown();
}

/// 一条在途发送 = 1 个条数许可 + body.length 个字节许可；回调之后两者都回来。
#[tokio::test]
async fn permits_are_borrowed_per_in_flight_send_and_given_back() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SUCCESS, 500));
    let producer = backpressure_producer("bp_borrow", &cluster, MIN_ASYNC_SEND_NUM).await;

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(400), cb.clone(), Some(3_000), None)
        .expect("运行时内可派发");
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

/// 归还挂在失败回调上（Java `semaphoreProcessor` 在两个回调里都跑），失败不能漏。
#[tokio::test]
async fn failure_also_gives_the_permits_back() {
    let cluster = MockCluster::start(1, true).await;
    cluster.script(0, vec![], (response_code::SYSTEM_ERROR, 0));
    let producer = backpressure_producer("bp_failure", &cluster, MIN_ASYNC_SEND_NUM).await;

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(300), cb.clone(), Some(3_000), None)
        .expect("运行时内可派发");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert!(!cb.errors().is_empty());
    assert_eq!(producer.semaphore_async_send_num_available_permits(), 10);
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        MIN_ASYNC_SEND_SIZE
    );
    producer.shutdown();
}

/// 重试链只占**一份**许可（一次发送一笔），不是一笔尝试一份；还多次会把容量虚增。
///
/// 重试由「broker 关掉连接不回答」触发（异步链只对没收到响应的失败换 broker），
/// 第二轮落在慢应答的 `broker-1` 上，好留出观察窗口。
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
        .expect("运行时内可派发");
    // 第二轮尝试已经在路上，扣掉的仍然只是一笔的量
    assert!(wait_until(|| cluster.requests(1) >= 1).await);
    assert_eq!(producer.semaphore_async_send_num_available_permits(), 9);
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        MIN_ASYNC_SEND_SIZE - 300
    );
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(cluster.requests(0), 1, "第一笔落在换掉的那台");
    assert_eq!(cluster.requests(1), 1);
    assert!(cb.errors().is_empty(), "换 broker 之后应当发成功: {:?}", cb.errors());
    assert_eq!(producer.semaphore_async_send_num_available_permits(), 10);
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        MIN_ASYNC_SEND_SIZE
    );
    producer.shutdown();
}

/// Java `:636-664` —— 闸是**等**到超时为止，不是看一眼不够就报错，这才是限流。
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
        .expect("运行时内可派发");
    // 让它在闸上排上队，然后确认它**还活着**（没提前失败、也没发出去）
    assert!(wait_until(|| producer
        .inner
        .semaphore_async_send_num
        .waiting_count()
        >= 1)
    .await);
    assert_eq!(cb.done.load(Ordering::SeqCst), 0);
    assert_eq!(cluster.requests(0), 0);

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(cb.done.load(Ordering::SeqCst), 0, "还没到超时就不该提前失败");
    producer
        .inner
        .semaphore_async_send_num
        .release(MIN_ASYNC_SEND_NUM);
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert!(
        began.elapsed() >= Duration::from_millis(100),
        "没有等许可，看一眼不够就失败了：{:?}",
        began.elapsed()
    );
    assert!(cb.errors().is_empty(), "等到许可之后这一笔应当发出去");
    assert_eq!(cb.ok.load(Ordering::SeqCst), 1);
    assert_eq!(cluster.requests(0), 1);
    producer.shutdown();
}

/// Java `DefaultMQProducerTest:593-595` 的那条断言：空闲许可 + 在途份数 == 新配置。
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

/// 本端口改容量不丢等待者（Java 换对象会把等待者留在旧信号量上等自己的超时）。
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
        .expect("运行时内可派发");
    assert!(wait_until(|| producer
        .inner
        .semaphore_async_send_num
        .waiting_count()
        >= 1)
    .await);
    producer.set_back_pressure_for_async_send_num(MIN_ASYNC_SEND_NUM + 1); // 凭空多出 1 条容量
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert!(cb.errors().is_empty(), "扩容后这一笔应当发出去: {:?}", cb.errors());
    assert_eq!(cluster.requests(0), 1);
    producer.shutdown();
}

/// Java `:642` —— `getBody() == null ? 1 : getBody().length`，空 body 也要扣 1。
#[test]
fn empty_body_still_costs_one_size_permit() {
    assert_eq!(back_pressure_msg_len(&body_of(5)), 5);
    assert_eq!(back_pressure_msg_len(&body_of(0)), 1);
    let no_body = Message::new("T1", None);
    assert_eq!(back_pressure_msg_len(&no_body), 1);
}

/// Python `_run` / Java `:555` —— 排队吃掉整个预算就直接回调，不发请求。
///
/// 「排队」在这里是任务从入队到被池内消费者跑起来之间的间隔，真集群造不出稳定时长，
/// 所以直接把 `began` 摆在过去，等价于那段等待已经花光了预算。
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
        "文案要与 Java 逐字一致: {}",
        errors[0]
    );
    assert_eq!(cluster.requests(0), 0, "预算没了就不该再发请求");
    producer.shutdown();
}

// ================================================================ 异步发送内核
//
// 队列有界、地址解析、复用请求换 opaque、`retryTimesWhenSendAsyncFailed`、异步不看
// `retryResponseCodes` —— 这些只有真异步内核才谈得上，全部在假集群上对拍。

/// Java `:674-681`（Python `submit` 抛 `RejectedExecutionException`）—— 队列满了
/// 就地报错给**调用方**，不排队、不走回调。
///
/// 这里能确定性地造出队满：`#[tokio::test]` 用的是**单线程**运行时，两次 `send_async`
/// 之间没有 `await`，池内消费者没机会被调度，所以容量 1 的队列在第二笔时必然是满的。
/// 容量是**建池时**（`start()`）读的，所以要配在 `ProducerConfig` 里。
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
    .expect("配置合法");
    producer.start().await.expect("假集群里 start 应当成功");
    assert_eq!(producer.get_async_sender_queue_capacity(), 1);

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(10), cb.clone(), Some(3_000), None)
        .expect("第一笔总能进队列");
    let err = producer
        .send_async(body_of(10), cb.clone(), Some(3_000), None)
        .expect_err("队列满了要把这一笔退回调用方");
    assert!(
        err.to_string().contains("executor rejected"),
        "文案要对齐 Python（Java 的字面量末尾多一个空格）: {err}"
    );
    assert_eq!(cb.done.load(Ordering::SeqCst), 0, "被拒的一笔不该有回调");
    assert_eq!(cluster.requests(0), 0, "被拒之前一次请求都不该发出去");

    // 放行之后第一笔照常发出去、回调照样跑
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(cb.ok.load(Ordering::SeqCst), 1);
    producer.shutdown();
}

/// 队列满 + **开了背压**：Java `:675-681` 就地跑完（许可已扣），本端口扣许可发生在出队
/// 之后，所以改成派发到队列之外 —— 两笔都要发出去，且各只扣一份许可。
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
    .expect("配置合法");
    producer.start().await.expect("假集群里 start 应当成功");

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(10), cb.clone(), Some(3_000), None)
        .expect("第一笔进队列");
    producer
        .send_async(body_of(10), cb.clone(), Some(3_000), None)
        .expect("开了背压时队满也不该把这一笔退回调用方");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 2).await);
    assert_eq!(cb.ok.load(Ordering::SeqCst), 2, "两笔都要发出去");
    assert_eq!(cluster.requests(0), 2);
    assert_eq!(
        producer.semaphore_async_send_num_available_permits(),
        MIN_ASYNC_SEND_NUM,
        "归还必须恰好等于扣掉的"
    );
    producer.shutdown();
}

/// 池已随 `shutdown()` 关掉 ⇒ 同步报错给调用方（Python 同），而不是把任务投进一个
/// 不会再有人消费的队列。回调一次都不跑。
#[tokio::test]
async fn send_async_after_shutdown_is_rejected() {
    let cluster = MockCluster::start(1, true).await;
    let producer = started("async_after_shutdown", &cluster).await;
    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(10), cb.clone(), Some(3_000), None)
        .expect("启动后接受异步发送");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    producer.shutdown();
    let err = producer
        .send_async(body_of(10), cb.clone(), Some(1_000), None)
        .expect_err("关闭后不该接受异步发送");
    assert!(err.to_string().contains("producer not started"), "{err}");
    assert_eq!(cb.done.load(Ordering::SeqCst), 1, "被拒的一笔不该有回调");
}

/// Java `:1043-1046`（Python `_send_kernel_async`）—— 准备工作（这里用拦截钩子的耗时
/// 代表）把预算花光时，**不建请求**，但仍然跑一次 after 钩子并归还许可。
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
        .expect("运行时内可派发");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    let errors = cb.errors();
    assert!(
        errors[0].contains("sendKernelImpl call timeout"),
        "文案要与 Java 逐字一致: {}",
        errors[0]
    );
    assert_eq!(cluster.requests(0), 0, "预算没了就不该把请求交出去");
    assert_eq!(hooks.before(), 1, "before 钩子在预算检查之前");
    assert_eq!(hooks.after(), 1, "after 钩子要在终止前跑一次");
    assert_eq!(producer.semaphore_async_send_num_available_permits(), MIN_ASYNC_SEND_NUM);
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        MIN_ASYNC_SEND_SIZE
    );
    producer.shutdown();
}

/// Java `onExceptionImpl` —— 连不上（`RemotingConnectException`）算「没收到响应」，
/// 要换一台 broker 重试；重试复用**同一个请求**但换新 `opaque`。
#[tokio::test]
async fn connect_failure_retries_on_another_broker_with_a_fresh_opaque() {
    let live = MockCluster::start(1, true).await;
    live.script(0, vec![], (response_code::SUCCESS, 0));
    let dead = dead_addr().await;
    // 路由里 broker-0 是死地址、broker-1 是真应答的 broker（选队从 0 起，重试避开 0）
    let cluster = MockCluster::with_addrs(vec![dead, live.broker_addrs[0].clone()], true).await;
    let producer = started("async_retry_broker", &cluster).await;

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(10), cb.clone(), Some(5_000), None)
        .expect("运行时内可派发");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert!(cb.errors().is_empty(), "换 broker 之后应当发成功: {:?}", cb.errors());
    assert_eq!(cb.ok.load(Ordering::SeqCst), 1);
    assert_eq!(live.requests(0), 1, "重试落在另一台 broker 上");
    producer.shutdown();
}

/// 同上，但第二台 broker 也是坏的：重试上限是 `retryTimesWhenSendAsyncFailed`，
/// 一共 3 笔尝试，最后把**包装过**的失败交给回调（Java `"unknown reason"` 分支）。
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
        .expect("运行时内可派发");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(
        cluster.requests(0) + cluster.requests(1),
        3,
        "retry_times_when_send_async_failed=2 ⇒ 一共 3 笔尝试"
    );
    let errors = cb.errors();
    assert_eq!(errors.len(), 1);
    assert!(
        errors[0].contains("send request failed"),
        "Java 的 RemotingSendRequestException 分支: {}",
        errors[0]
    );
    producer.shutdown();
}

/// 配成 0 就一笔都不重试（Java 的 `times <= timesTotal` 判据）。
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
        .expect("运行时内可派发");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(cluster.requests(0), 1);
    assert_eq!(cluster.requests(1), 0, "配 0 就不该换 broker");
    producer.shutdown();
}

/// **异步发送不看 `retryResponseCodes`**：broker 明确回了错（这里回 `SYSTEM_ERROR`）
/// 就原样交给回调，一次尝试就结束。这与同步发送的语义**不同**，别照搬。
#[tokio::test]
async fn broker_rejected_response_ends_the_chain_after_one_attempt() {
    let cluster = MockCluster::start(2, true).await;
    cluster.script(0, vec![], (response_code::SYSTEM_ERROR, 0));
    cluster.script(1, vec![], (response_code::SUCCESS, 0));
    let producer = started("async_broker_code", &cluster).await;
    assert!(
        producer.is_retry_response_code(Some(response_code::SYSTEM_ERROR)),
        "这个码在同步路径里是可重试的"
    );

    let cb = Arc::new(Recorder::default());
    producer
        .send_async(body_of(10), cb.clone(), Some(5_000), None)
        .expect("运行时内可派发");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(cluster.requests(0), 1);
    assert_eq!(cluster.requests(1), 0, "异步链不换 broker（Java needRetry=false）");
    let errors = cb.errors();
    assert!(
        errors[0].contains("MQBrokerException"),
        "broker 的错误码要**原样**交付，不包装成 unknown reason: {}",
        errors[0]
    );
    producer.shutdown();
}

/// 一笔异步发送只跑**一次** before/after 钩子（Java 的 `sendKernelImpl` 末尾对 ASYNC
/// 还会再跑一次 after，本端口跟 Python：只在链的终点跑一次）。
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
        .expect("运行时内可派发");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert_eq!(cb.ok.load(Ordering::SeqCst), 1);
    assert_eq!(hooks.before(), 1);
    assert_eq!(hooks.after(), 1);
    producer.shutdown();
}

/// after 钩子要看到 `sendResult`（成功）或 `exception`（失败），与同步内核一致。
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
    // 第一笔失败（broker 明确回了错）、第二笔成功：两条终点都要带上各自的结果
    for i in 1..=2 {
        producer
            .send_async(body_of(10), cb.clone(), Some(3_000), None)
            .expect("运行时内可派发");
        assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == i).await);
    }
    assert_eq!(cb.ok.load(Ordering::SeqCst), 1);
    assert_eq!(cb.errors().len(), 1);
    assert!(hooks.saw_result(), "成功分支要带 sendResult");
    assert!(hooks.saw_exception(), "失败分支要带 exception");
    assert_eq!(hooks.before(), 2);
    assert_eq!(hooks.after(), 2, "每笔发送在终点各跑一次 after");
    producer.shutdown();
}

/// 定点发送（调用方给了 `mq`）不换 broker：Java 传下去的 topicPublishInfo 是 null，
/// 重试只能在**同一台**上换 opaque —— 那里连接是好的，所以三次尝试都打到同一台 broker。
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
        .expect("运行时内可派发");
    assert!(wait_until(|| cb.done.load(Ordering::SeqCst) == 1).await);
    assert!(cb.errors().is_empty(), "第三次尝试应当成功: {:?}", cb.errors());
    assert_eq!(cluster.requests(0), 3, "三次尝试都在定点的那台");
    assert_ne!(
        cluster.send_opaque(0, 0),
        cluster.send_opaque(0, 1),
        "重试必须换新 opaque，否则两次尝试的应答会串台"
    );
    assert_ne!(
        cluster.send_opaque(0, 1),
        cluster.send_opaque(0, 2),
        "重试必须换新 opaque，否则两次尝试的应答会串台"
    );
    producer.shutdown();
}

/// 未 start 的异步发送**同步抛**（Java 走回调，Python 与这里一致），回调一次都不跑。
#[test]
fn send_async_before_start_raises_without_calling_back() {
    let producer = DefaultMQProducer::new("GID_async_unstarted").expect("组名合法");
    let cb = Arc::new(Recorder::default());
    let err = producer
        .send_async(body_of(10), cb.clone(), Some(1_000), None)
        .expect_err("未启动就该同步报错");
    assert!(
        err.to_string().contains("producer not started"),
        "文案要对齐 Python `_require_client`: {err}"
    );
    assert_eq!(cb.done.load(Ordering::SeqCst), 0);
}

