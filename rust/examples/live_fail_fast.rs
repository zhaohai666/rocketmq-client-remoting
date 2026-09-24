//! broker 真的死了：在途请求必须**立刻**有终态（Java `failFast` → `requestFail`）真机验证。
//!
//! 与 `python/verify_fail_fast_live.py`、`cpp/examples/live_fail_fast.cpp`、
//! `dotnet/examples/RocketMQ.Examples/LiveFailFast.cs` 同场景、同断言。
//!
//! 前置：NameServer + Broker 已起（脚本会**停一次 broker 再拉起**，不删 store）。
//!
//! 离线用例（`src/remoting/client.rs` 的 failFast 组）锁的是传输层契约：本机假对端读完就关，
//! 断言"毫秒级判死 + 报 Error::SendRequest 而不是 Timeout + 回调只投一次"。但这条路径存在的
//! 意义正是**真机上的 broker 重启 / 主备切换 / 网络抖动**，只有真集群能回答：
//! - L1 基线：真 broker 上发送与长轮询都正常（先确认后面的失败不是环境造成的）。
//! - L2 挂起：把三条**真的挂在 broker 上**的长轮询（suspend 20s、客户端超时 30s）钉在在途表里。
//! - L3 收口：杀掉 broker（读任务见到 EOF）→ 长轮询必须立刻拿到 `Error::SendRequest`，
//!   而不是等满 30s 报一个 `Error::Timeout`。类型不能错：异步发送的重试分类按异常**种类**
//!   分流（`client/producer.rs`），报成超时等于换了一整套重试决策。
//! - L4 范围：判死只牵连死掉那条连接；同一个传输实例上的 namesrv 连接照常服务。
//! - L5 恢复：broker 拉起后同一个 producer 实例重新建连照常发送；已拿到 SEND_OK 的消息
//!   一条都不能少。
//!
//! L3 的阈值（8s）远小于客户端超时（30s），也小于 broker 的 suspend 上限（20s）：缺了
//! failFast 这条断言必然失败，不是碰运气。
//!
//! 用法：
//! ```text
//! cargo run --example live_fail_fast -- 127.0.0.1:9876
//! ```

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::{DefaultMQProducer, ProducerConfig};
use rocketmq_client_remoting::client::pull_consumer::{DefaultMQPullConsumer, PullConsumerConfig};
use rocketmq_client_remoting::client::result::SendStatus;
use rocketmq_client_remoting::common::message::{Message, MessageQueue};
use rocketmq_client_remoting::common::sysflag::PullSysFlag;
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::client::RemotingClient;
use rocketmq_client_remoting::remoting::protocol::codes::{request_code, response_code};
use rocketmq_client_remoting::remoting::protocol::headers::PullMessageRequestHeader;
use rocketmq_client_remoting::remoting::protocol::remoting_command::RemotingCommand;

/// 客户端侧超时故意放到 30s：判死若走的是超时路径，至少要等这么久。
const CLIENT_TIMEOUT_MILLIS: i64 = 30_000;
/// broker 侧挂起上限，故意小于客户端超时。
const BROKER_SUSPEND_MILLIS: i64 = 20_000;
/// failFast 应当是毫秒级；留 8s 给真机调度（EOF 到达 + 回调投递）。
const FAIL_FAST_LIMIT: Duration = Duration::from_secs(8);
const BASELINE_MSGS: usize = 5;
const PARKED: usize = 3;

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
            println!("  [PASS] {name}  {detail}");
        } else {
            println!("  [FAIL] {name}: {detail}");
            self.failed.push(format!("{name}: {detail}"));
        }
    }
}

/// broker 开关：`scripts/rmq_test_broker.sh`，四个语言的 live 用例共用同一份口径。
fn broker_ctl(script: &PathBuf, action: &str) -> (bool, String) {
    let out_path = format!("/tmp/rmq_fail_fast_rs_broker_ctl.{action}.log");
    let file = match fs::File::create(&out_path) {
        Ok(f) => f,
        Err(e) => return (false, format!("open {out_path} failed: {e}")),
    };
    let stdout = match file.try_clone() {
        Ok(f) => f,
        Err(e) => return (false, format!("dup {out_path} failed: {e}")),
    };
    // 输出走**文件**而不是管道：start 会把 broker 拉成常驻进程，谁继承它的写端
    // 谁就等不到 EOF。
    let status = std::process::Command::new("sh")
        .arg(script)
        .arg(action)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(file))
        .status();
    match status {
        Ok(s) => {
            let text = fs::read_to_string(&out_path).unwrap_or_default();
            (s.success(), text.trim().to_string())
        }
        Err(e) => (false, format!("run {} {action} failed: {e}", script.display())),
    }
}

fn parked_pull_request(group: &str, mq: &MessageQueue, queue_offset: i64) -> RemotingCommand {
    let header = PullMessageRequestHeader {
        consumer_group: Some(group.to_string()),
        topic: Some(mq.topic.clone()),
        lite_topic: None,
        queue_id: Some(mq.queue_id),
        queue_offset: Some(queue_offset),
        max_msg_nums: Some(32),
        sys_flag: Some(PullSysFlag::build_sys_flag_basic(
            false, true, true, false,
        )),
        commit_offset: Some(0),
        suspend_timeout_millis: Some(BROKER_SUSPEND_MILLIS),
        subscription: Some("*".to_string()),
        sub_version: Some(0),
        expression_type: Some("TAG".to_string()),
        max_msg_bytes: Some(-1),
        request_source: Some(0),
        proxy_froward_client_id: None,
    };
    // 手工构造：不经 pull consumer 的钳制，30s 客户端超时与 20s broker suspend
    // 都由本用例说了算。
    RemotingCommand::create_request_command(request_code::PULL_MESSAGE, Some(Box::new(header)))
}

/// 收尾保险：无论用例走到哪一步（含提前 return），都不能把测试集群留在停机状态
/// 交给下一个用例——脚本的 start 本来就是幂等的（已在跑就直接返回）。
struct BrokerGuard {
    script: PathBuf,
}

impl Drop for BrokerGuard {
    fn drop(&mut self) {
        let (ok, out) = broker_ctl(&self.script, "status");
        if ok {
            return;
        }
        println!("  [cleanup] 用例结束时 broker 是 DOWN（{out}），补一次 start");
        let (ok, out) = broker_ctl(&self.script, "start");
        if !ok {
            println!("  [cleanup] broker 仍没起来：{out}");
        }
    }
}

/// 一条挂起的长轮询的结局：`(异常种类, 文案, 耗时)`；成功回响应则 kind 是 `response:<code>`。
async fn park_one(
    remoting: RemotingClient,
    addr: String,
    group: String,
    mq: MessageQueue,
    offset: i64,
    returned: std::sync::Arc<AtomicUsize>,
) -> (String, String, Duration) {
    let mut cmd = parked_pull_request(&group, &mq, offset);
    let started = Instant::now();
    let (kind, message) =
        match remoting.invoke_sync(&addr, &mut cmd, Some(CLIENT_TIMEOUT_MILLIS)).await {
        Ok(resp) => (format!("response:{}", resp.code), String::new()),
        Err(e) => {
            let k = match &e {
                Error::SendRequest { .. } => "send_request".to_string(),
                Error::Timeout { .. } => "timeout".to_string(),
                Error::Connect { .. } => "connect".to_string(),
                other => format!("other:{other}"),
            };
            (k, e.to_string())
        }
    };
    returned.fetch_add(1, Ordering::SeqCst);
    (kind, message, started.elapsed())
}

async fn run(namesrv: &str, broker_script: PathBuf) -> Checker {
    let mut ck = Checker::new();
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default();
    let topic = format!("FailFastRs_{stamp}");
    let group = format!("fail_fast_rs_{stamp}");

    let (ok, out) = broker_ctl(&broker_script, "status");
    if !ok {
        println!("broker 没在跑：先按本地集群 runbook 起 namesrv + broker（{out}）");
        ck.check("前置 broker UP", false, &out);
        return ck;
    }

    let producer = match DefaultMQProducer::with_config(ProducerConfig {
        producer_group: format!("{group}_p"),
        instance_name: format!("ff_rs_{stamp}"),
        name_server_addrs: vec![namesrv.to_string()],
        ..Default::default()
    }) {
        Ok(p) => p,
        Err(e) => {
            ck.check("producer 构造", false, &e.to_string());
            return ck;
        }
    };
    let consumer = match DefaultMQPullConsumer::with_config(PullConsumerConfig {
        consumer_group: group.clone(),
        name_server_addrs: vec![namesrv.to_string()],
        instance_name: format!("ff_rs_{stamp}"),
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            ck.check("pull consumer 构造", false, &e.to_string());
            return ck;
        }
    };

    let _guard = BrokerGuard {
        script: broker_script.clone(),
    };

    // -------------------------------------------------------------- L1
    if let Err(e) = producer.start().await {
        ck.check("L1 producer start", false, &e.to_string());
        return ck;
    }
    let mut landed = 0usize;
    for i in 0..BASELINE_MSGS {
        let body = format!("fail-fast-rs-base-{i}");
        let mut msg = Message::new(&topic, Some(body.as_bytes()));
        msg.set_keys(&format!("ff-base-{i}"));
        match producer.send(&mut msg, Some(5000), None).await {
            Ok(r) if r.status == SendStatus::SendOk => landed += 1,
            _ => {}
        }
    }
    ck.check(
        "L1 基线：5 条同步发送 SEND_OK",
        landed == BASELINE_MSGS,
        &format!("landed={landed}"),
    );

    if let Err(e) = consumer.start().await {
        ck.check("L1 consumer start", false, &e.to_string());
        producer.shutdown();
        return ck;
    }
    let queues = consumer
        .fetch_subscribe_message_queues(&topic)
        .await
        .unwrap_or_default();
    ck.check(
        "L1 取到队列",
        !queues.is_empty(),
        &format!("queues={}", queues.len()),
    );
    let Some(mq) = queues.first().cloned() else {
        ck.check("L1 取到队列", false, "没有队列，后续无法进行");
        consumer.shutdown();
        producer.shutdown();
        return ck;
    };

    let client = match producer.client() {
        Some(c) => c,
        None => {
            ck.check("L1 拿到 MQClientInstance", false, "producer 未启动");
            consumer.shutdown();
            producer.shutdown();
            return ck;
        }
    };
    let route = client.get_topic_route_data(&topic).await;
    let addr = route
        .as_ref()
        .and_then(|r| MQClientInstance::find_broker_addr_in_route(r, &mq.broker_name));
    let Some(addr) = addr else {
        ck.check("L1 拿到 broker 地址", false, "路由里没有这台 broker");
        consumer.shutdown();
        producer.shutdown();
        return ck;
    };
    ck.check("L1 拿到 broker 地址", true, &format!("addr={addr}"));

    let max_offset = match consumer.max_offset(&mq).await {
        Ok(o) => o,
        Err(e) => {
            ck.check("L1 待挂长轮询的队列有位点可用", false, &e.to_string());
            consumer.shutdown();
            producer.shutdown();
            return ck;
        }
    };
    // 发送是跨队列轮转的，单条队列的队尾只覆盖落在它上面的那部分，
    // 所以"真的落盘"要看全部队列的合计。
    let mut total_max = max_offset;
    for q in queues.iter().skip(1) {
        if let Ok(o) = consumer.max_offset(q).await {
            total_max += o;
        }
    }
    ck.check(
        "L1 各队列队尾位点合计覆盖刚发的 5 条（真的落盘）",
        total_max >= BASELINE_MSGS as i64,
        &format!("max_offset 合计={total_max}"),
    );

    // -------------------------------------------------------------- L2
    let remoting = client.remoting_client().clone();
    let baseline_in_flight = remoting.in_flight_count();
    let returned = std::sync::Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for i in 0..PARKED {
        let remoting = remoting.clone();
        let addr = addr.clone();
        let group = group.clone();
        let mq = mq.clone();
        let returned = std::sync::Arc::clone(&returned);
        handles.push(tokio::spawn(async move {
            let tag = format!("park-{i}");
            let (kind, message, cost) =
                park_one(remoting, addr, group, mq, max_offset, returned).await;
            (tag, kind, message, cost)
        }));
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    ck.check(
        "L2 三条长轮询真的挂在 broker 上（2s 后仍未返回）",
        returned.load(Ordering::SeqCst) == 0,
        &format!("returned={}", returned.load(Ordering::SeqCst)),
    );
    ck.check(
        "L2 在途表里有它们",
        remoting.in_flight_count() >= baseline_in_flight + PARKED,
        &format!(
            "in_flight={} baseline={baseline_in_flight}",
            remoting.in_flight_count()
        ),
    );

    // -------------------------------------------------------------- L3
    let script = broker_script.clone();
    let stopped = tokio::task::spawn_blocking(move || broker_ctl(&script, "stop")).await;
    let (ok, out) = stopped.unwrap_or_else(|e| (false, format!("stop 任务 panic: {e}")));
    ck.check("L3 停掉 broker", ok, &out);

    let mut parked = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(r) => parked.push(r),
            Err(e) => ck.check("L3 挂起的长轮询全部返回（没有卡死）", false, &e.to_string()),
        }
    }
    ck.check(
        "L3 挂起的长轮询全部返回（没有卡死）",
        parked.len() == PARKED,
        &format!("got={}", parked.len()),
    );

    let send_request = parked.iter().filter(|p| p.1 == "send_request").count();
    let timeouts = parked.iter().filter(|p| p.1 == "timeout").count();
    let worst = parked
        .iter()
        .map(|p| p.3)
        .max()
        .unwrap_or(Duration::ZERO);
    let kinds = parked
        .iter()
        .map(|p| p.1.clone())
        .collect::<Vec<_>>()
        .join(",");
    let first_msg = parked
        .iter()
        .find(|p| p.1 == "send_request")
        .map(|p| p.2.clone())
        .unwrap_or_default();
    // 报的是 Error::SendRequest（Java failFast 的口径）
    ck.check(
        "L3 报的是 RemotingSendRequestException（Java failFast 的口径）",
        send_request == PARKED,
        &format!("kinds={kinds}"),
    );
    ck.check(
        "L3 一条都没被报成超时（类型错 = 重试决策错）",
        timeouts == 0,
        &format!("timeout={timeouts}"),
    );
    ck.check(
        "L3 判死耗时远小于 30s 客户端超时",
        worst < FAIL_FAST_LIMIT,
        &format!("worst={:.2}s limit={}s", worst.as_secs_f64(), FAIL_FAST_LIMIT.as_secs()),
    );
    ck.check(
        "L3 异常文案带着断连原因",
        first_msg.contains("connection closed"),
        &first_msg,
    );
    // 在途表被 failFast 排空（不是等超时自己扫）
    let mut drained = false;
    for _ in 0..40 {
        if remoting.in_flight_count() == 0 {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    ck.check(
        "L3 判死之后在途表排空",
        drained,
        &format!("in_flight={}", remoting.in_flight_count()),
    );

    // -------------------------------------------------------------- L4
    // 判死必须只牵连死掉那条连接：namesrv 走的是同一个传输实例的另一条连接。
    let mut probe = RemotingCommand::create_request_command(
        request_code::GET_ALL_TOPIC_LIST_FROM_NAMESERVER,
        None,
    );
    let (ns_kind, ns_code) = match remoting.invoke_sync(namesrv, &mut probe, Some(5000)).await {
        Ok(resp) => ("response".to_string(), resp.code),
        Err(e) => (e.to_string(), -1),
    };
    ck.check(
        "L4 namesrv 连接没被牵连（broker 死了它还在服务）",
        ns_kind == "response" && ns_code == response_code::SUCCESS,
        &format!("{ns_kind} code={ns_code}"),
    );

    // -------------------------------------------------------------- L5
    let script = broker_script.clone();
    let started = tokio::task::spawn_blocking(move || broker_ctl(&script, "start")).await;
    let (ok, out) = started.unwrap_or_else(|e| (false, format!("start 任务 panic: {e}")));
    ck.check("L5 broker 重新拉起", ok, &out);

    let mut recovered = false;
    let mut attempts = 0usize;
    let mut rec_err = String::from("never attempted");
    for i in 0..20 {
        attempts = i + 1;
        let body = format!("fail-fast-rs-recover-{i}");
        let mut msg = Message::new(&topic, Some(body.as_bytes()));
        msg.set_keys("ff-recover");
        match producer.send(&mut msg, Some(5000), None).await {
            Ok(r) if r.status == SendStatus::SendOk => {
                recovered = true;
                break;
            }
            Ok(r) => rec_err = format!("status={:?}", r.status),
            Err(e) => rec_err = e.to_string(),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    ck.check(
        "L5 同一个 producer 实例重新建连后照常发送",
        recovered,
        &format!("attempts={attempts} {rec_err}"),
    );

    let mut after = 0i64;
    for _ in 0..20 {
        after = 0;
        let mut ok_all = true;
        for q in &queues {
            match consumer.max_offset(q).await {
                Ok(o) => after += o,
                Err(_) => ok_all = false,
            }
        }
        if ok_all && after >= BASELINE_MSGS as i64 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        "L5 重启后 broker 上仍有那 5 条 SEND_OK 的消息",
        after >= BASELINE_MSGS as i64,
        &format!("max_offset 合计={after}"),
    );

    consumer.shutdown();
    producer.shutdown();
    ck
}

fn report(ck: &Checker) {
    println!(
        "== 结果: {}/{} 通过 ==",
        ck.passed,
        ck.passed as usize + ck.failed.len()
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
    let root = env::var("RMQ_REPO_ROOT").unwrap_or_else(|_| "..".to_string());
    let broker_script = PathBuf::from(root).join("scripts/rmq_test_broker.sh");
    let ck = run(&namesrv, broker_script).await;
    report(&ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
