//! Live verification of the failFast transport layer on a real cluster.
//!
//! Prerequisite: NameServer + Broker are running (the script **stops the broker once and starts it again**, without deleting the store).
//!
//! The offline cases (the failFast group in `src/remoting/client.rs`) lock the transport-layer contract: a local fake peer closes right after
//! reading, asserting "millisecond-level kill + an Error::SendRequest instead of Timeout + the callback delivered exactly once". But the reason this
//! path exists is precisely **broker restarts / primary-secondary failover / network jitter on a real machine**; only a real cluster can answer:
//! - L1 baseline: sending and long polling both work on a real broker (confirms the failures below are not environmental).
//! - L2 parking: three long polls **really parked on the broker** (suspend 20s, client timeout 30s) pinned in the in-flight table.
//! - L3 kill: kill the broker (the read task sees EOF) => the long polls must immediately get `Error::SendRequest`,
//!   instead of waiting the full 30s and reporting `Error::Timeout`. The type cannot be wrong: the retry classification of async sends branches on the exception
//!   **kind** (`client/producer.rs`); reporting a timeout means swapping in a whole different retry policy.
//! - L4 scope: the kill must only implicate the dead connection; the namesrv connection on the same transport instance keeps serving.
//! - L5 recovery: after the broker is restarted the same producer instance reconnects and sends as usual; every message that already got SEND_OK
//!   must still be there, not one missing.
//!
//! L3's threshold (8s) is far below the client timeout (30s) and also below the broker's suspend cap (20s): without
//! failFast this assertion is guaranteed to fail, it is not luck.
//!
//! Usage:
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

/// The client-side timeout is deliberately set to 30s: if the kill took the timeout path, it would have to wait at least this long.
const CLIENT_TIMEOUT_MILLIS: i64 = 30_000;
/// The broker-side suspend cap, deliberately smaller than the client timeout.
const BROKER_SUSPEND_MILLIS: i64 = 20_000;
/// failFast should be millisecond-level; 8s is left for real-machine scheduling (EOF arrival + callback delivery).
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

/// Broker switch: `scripts/rmq_test_broker.sh`; this crate's live cases all share this one contract.
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
    // Output goes to **files** rather than pipes: start turns the broker into a resident process, and whoever inherits its
    // write end never sees EOF.
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
    // Built by hand: without the pull consumer's clamping, so the 30s client timeout and the 20s broker suspend
    // are both decided by this case.
    RemotingCommand::create_request_command(request_code::PULL_MESSAGE, Some(Box::new(header)))
}

/// Cleanup safety net: no matter where the case stops (including an early return), the test cluster must never be left
/// powered off for the next case -- the script's start is idempotent by design (it returns directly if already running).
struct BrokerGuard {
    script: PathBuf,
}

impl Drop for BrokerGuard {
    fn drop(&mut self) {
        let (ok, out) = broker_ctl(&self.script, "status");
        if ok {
            return;
        }
        println!("  [cleanup] the broker is DOWN when the case ends ({out}); running one extra start");
        let (ok, out) = broker_ctl(&self.script, "start");
        if !ok {
            println!("  [cleanup] the broker still did not come up: {out}");
        }
    }
}

/// The outcome of one parked long poll: `(exception kind, message text, elapsed)`; on a successful response the kind is `response:<code>`.
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
        println!("broker is not running: start namesrv + broker per the local cluster runbook first ({out})");
        ck.check("prerequisite broker UP", false, &out);
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
            ck.check("producer construction", false, &e.to_string());
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
            ck.check("pull consumer construction", false, &e.to_string());
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
        "L1 baseline: 5 sync sends all SEND_OK",
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
        "L1 queues fetched",
        !queues.is_empty(),
        &format!("queues={}", queues.len()),
    );
    let Some(mq) = queues.first().cloned() else {
        ck.check("L1 queues fetched", false, "no queue, the rest cannot proceed");
        consumer.shutdown();
        producer.shutdown();
        return ck;
    };

    let client = match producer.client() {
        Some(c) => c,
        None => {
            ck.check("L1 MQClientInstance obtained", false, "producer not started");
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
        ck.check("L1 broker address obtained", false, "the route does not contain this broker");
        consumer.shutdown();
        producer.shutdown();
        return ck;
    };
    ck.check("L1 broker address obtained", true, &format!("addr={addr}"));

    let max_offset = match consumer.max_offset(&mq).await {
        Ok(o) => o,
        Err(e) => {
            ck.check("L1 the queue to park long polls on has an offset available", false, &e.to_string());
            consumer.shutdown();
            producer.shutdown();
            return ck;
        }
    };
    // Sends rotate across queues, so a single queue's tail offset only covers the messages that landed on it;
    // "really persisted" therefore means the sum over all queues.
    let mut total_max = max_offset;
    for q in queues.iter().skip(1) {
        if let Ok(o) = consumer.max_offset(q).await {
            total_max += o;
        }
    }
    ck.check(
        "L1 the sum of all queues' tail offsets covers the 5 just-sent messages (really persisted)",
        total_max >= BASELINE_MSGS as i64,
        &format!("max_offset sum={total_max}"),
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
        "L2 the three long polls are really parked on the broker (still unreturned after 2s)",
        returned.load(Ordering::SeqCst) == 0,
        &format!("returned={}", returned.load(Ordering::SeqCst)),
    );
    ck.check(
        "L2 they sit in the in-flight table",
        remoting.in_flight_count() >= baseline_in_flight + PARKED,
        &format!(
            "in_flight={} baseline={baseline_in_flight}",
            remoting.in_flight_count()
        ),
    );

    // -------------------------------------------------------------- L3
    let script = broker_script.clone();
    let stopped = tokio::task::spawn_blocking(move || broker_ctl(&script, "stop")).await;
    let (ok, out) = stopped.unwrap_or_else(|e| (false, format!("stop task panicked: {e}")));
    ck.check("L3 broker stopped", ok, &out);

    let mut parked = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(r) => parked.push(r),
            Err(e) => ck.check("L3 all parked long polls returned (none stuck)", false, &e.to_string()),
        }
    }
    ck.check(
        "L3 all parked long polls returned (none stuck)",
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
    ck.check(
        "L3 the error is RemotingSendRequestException (the failFast contract)",
        send_request == PARKED,
        &format!("kinds={kinds}"),
    );
    ck.check(
        "L3 not a single one reported as a timeout (wrong type = wrong retry policy)",
        timeouts == 0,
        &format!("timeout={timeouts}"),
    );
    ck.check(
        "L3 the kill takes far less than the 30s client timeout",
        worst < FAIL_FAST_LIMIT,
        &format!("worst={:.2}s limit={}s", worst.as_secs_f64(), FAIL_FAST_LIMIT.as_secs()),
    );
    ck.check(
        "L3 the error text carries the disconnect reason",
        first_msg.contains("connection closed"),
        &first_msg,
    );
    // The in-flight table is drained by failFast (not left to the timeout sweeper)
    let mut drained = false;
    for _ in 0..40 {
        if remoting.in_flight_count() == 0 {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    ck.check(
        "L3 the in-flight table is drained after the kill",
        drained,
        &format!("in_flight={}", remoting.in_flight_count()),
    );

    // -------------------------------------------------------------- L4
    // The kill must implicate only the dead connection: namesrv runs on another connection of the same transport instance.
    let mut probe = RemotingCommand::create_request_command(
        request_code::GET_ALL_TOPIC_LIST_FROM_NAMESERVER,
        None,
    );
    let (ns_kind, ns_code) = match remoting.invoke_sync(namesrv, &mut probe, Some(5000)).await {
        Ok(resp) => ("response".to_string(), resp.code),
        Err(e) => (e.to_string(), -1),
    };
    ck.check(
        "L4 the namesrv connection is untouched (it keeps serving while the broker is dead)",
        ns_kind == "response" && ns_code == response_code::SUCCESS,
        &format!("{ns_kind} code={ns_code}"),
    );

    // -------------------------------------------------------------- L5
    let script = broker_script.clone();
    let started = tokio::task::spawn_blocking(move || broker_ctl(&script, "start")).await;
    let (ok, out) = started.unwrap_or_else(|e| (false, format!("start task panicked: {e}")));
    ck.check("L5 broker restarted", ok, &out);

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
        "L5 the same producer instance reconnects and sends as usual",
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
        "L5 the broker still holds those 5 SEND_OK messages after the restart",
        after >= BASELINE_MSGS as i64,
        &format!("max_offset sum={after}"),
    );

    consumer.shutdown();
    producer.shutdown();
    ck
}

fn report(ck: &Checker) {
    println!(
        "== result: {}/{} passed ==",
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
