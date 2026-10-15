//! Live verification of send-latency fault tolerance (sendLatencyFaultEnable) (covering the five scenarios S1-S5).
//!
//! Usage (start the local single-broker cluster per the runbook first):
//! ```text
//! cargo run --example live_latency -- 127.0.0.1:9876
//! ```
//!
//! Scenarios (single broker broker-a / DefaultCluster):
//!   S1 off by default: send 10 messages, all OK, the fault table records nothing
//!   S2 fault avoidance enabled: send 20 messages, all OK (broker healthy, no isolation triggered)
//!   S3 after successful sends the fault table has an entry: current_latency > 0 and available/reachable
//!   S4 manually injected isolation → single broker walks the available→reachable→plain round-robin degradation chain and still sends all
//!   S5 isolation expires and recovers (inject 2000ms, available again and sending normally after ≈2s)

use std::env;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::common::message::Message;
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};

const QUEUE_NUMS: i32 = 4;

// ------------------------------------------------------------------ Skeleton

fn stamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

struct Checker {
    passed: usize,
    failed: Vec<String>,
}

impl Checker {
    fn new() -> Checker {
        Checker { passed: 0, failed: Vec::new() }
    }

    fn check(&mut self, name: &str, cond: bool, detail: &str) {
        if cond {
            self.passed += 1;
            println!("  [PASS] {name}{}", if detail.is_empty() { String::new() } else { format!("  {detail}") });
        } else {
            let label = format!("{name}{}", if detail.is_empty() { String::new() } else { format!("  {detail}") });
            self.failed.push(label.clone());
            println!("  [FAIL] {label}");
        }
    }
}

/// Sends synchronously one by one, returns the count of successes.
async fn send_ok(p: &DefaultMQProducer, topic: &str, count: usize, key: &str) -> usize {
    let mut ok = 0;
    for i in 0..count {
        let body = format!("{key}-{i}");
        let mut msg = Message::new(topic, Some(body.as_bytes()));
        match p.send(&mut msg, Some(5000), None).await {
            Ok(_) => ok += 1,
            Err(e) => println!("    send {body} failed: {e}"),
        }
    }
    ok
}

/// Takes (brokerName, brokerAddr) from the default topic route -- a single-broker cluster has exactly one entry.
fn broker_of(
    route: &rocketmq_client_remoting::remoting::protocol::route::TopicRouteData,
) -> Option<(String, String)> {
    let bd = route.broker_datas.first()?;
    let addr = bd.select_broker_addr()?;
    Some((bd.broker_name.clone(), addr))
}

// ------------------------------------------------------------------ Driver

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let topic = format!("LatencyRust{stamp}");

    println!("== LatencyFaultTolerance live verification  namesrv={namesrv}  topic={topic} ==");

    // Pre-create the topic (neither consumer nor producer falls back to the default topic; create first, then send).
    let admin = MQClientInstance::new(&format!("rust-live-lat-admin-{stamp}"), vec![namesrv.to_string()]);
    if let Err(e) = admin.start().await {
        ck.check("A0 admin start", false, &e.to_string());
        return ck;
    }
    let route = admin.get_topic_route_data(MixAll::DEFAULT_TOPIC).await;
    let (broker_name, broker_addr) = match route.as_ref().and_then(broker_of) {
        Some(v) => v,
        None => {
            ck.check("A0 default topic route fetchable", false, "no route of TBW102");
            return ck;
        }
    };
    if let Err(e) = admin
        .create_topic_in_broker(
            &broker_addr,
            MixAll::DEFAULT_TOPIC,
            &topic,
            QUEUE_NUMS,
            QUEUE_NUMS,
            DEFAULT_PERM,
            0,
            TopicFilterType::SINGLE_TAG,
            false,
            None,
            5000,
            2,
        )
        .await
    {
        ck.check("A0 pre-create topic", false, &e.to_string());
        return ck;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    // ---------------- S1 off by default ----------------
    println!("\nS1 off by default: behavior unchanged, fault table not recorded");
    let p1 = match DefaultMQProducer::new(&format!("rust-live-lat-off-{stamp}")) {
        Ok(p) => p,
        Err(e) => {
            ck.check("S1 build", false, &e.to_string());
            return ck;
        }
    };
    p1.set_namesrv_addr(namesrv);
    ck.check(
        "S1 default send_latency_fault_enable=false",
        !p1.fault_strategy().is_send_latency_fault_enable(),
        "",
    );
    if let Err(e) = p1.start().await {
        ck.check("S1 start", false, &e.to_string());
        return ck;
    }
    let n = send_ok(&p1, &topic, 10, "off").await;
    ck.check("S1 with the flag off all 10 sends succeed", n == 10, &format!("ok={n}"));
    ck.check(
        "S1 with the flag off the fault table records nothing",
        p1.fault_strategy()
            .latency_fault_tolerance()
            .get_fault_item(&broker_name)
            .is_none(),
        "",
    );
    p1.shutdown();

    // ---------------- S2/S3 healthy path with the flag on ----------------
    println!("\nS2/S3 fault avoidance on: healthy broker sends normally and records latency");
    let p2 = match DefaultMQProducer::new(&format!("rust-live-lat-on-{stamp}")) {
        Ok(p) => p,
        Err(e) => {
            ck.check("S2 build", false, &e.to_string());
            return ck;
        }
    };
    p2.set_namesrv_addr(namesrv);
    p2.set_send_latency_fault_enable(true);
    ck.check(
        "S2 the switch takes effect",
        p2.fault_strategy().is_send_latency_fault_enable(),
        "",
    );
    if let Err(e) = p2.start().await {
        ck.check("S2 start", false, &e.to_string());
        return ck;
    }
    let n = send_ok(&p2, &topic, 20, "on").await;
    ck.check("S2 with the flag on all 20 sends succeed", n == 20, &format!("ok={n}"));
    let item = p2
        .fault_strategy()
        .latency_fault_tolerance()
        .get_fault_item(&broker_name);
    ck.check("S3 fault table recorded broker-a", item.is_some(), "");
    if let Some(item) = item {
        ck.check(
            "S3 the recorded measured latency is > 0",
            item.current_latency > 0,
            &format!("latency={}ms", item.current_latency),
        );
        ck.check(
            "S3 the healthy broker is still available",
            p2.fault_strategy().latency_fault_tolerance().is_available(&broker_name)
                && item.is_reachable(),
            "",
        );
        ck.check(
            "S3 latency below the first-tier threshold (no isolation triggered)",
            item.start_timestamp == 0,
            &format!("startTs={}", item.start_timestamp),
        );
    }

    // ---------------- S4 injected isolation → degradation chain ----------------
    println!("\nS4 injected isolation: the available→reachable→plain round-robin degradation chain still sends");
    p2.fault_strategy().update_fault_item(&broker_name, 99999, true, false);
    ck.check(
        "S4 after injection broker-a is unavailable",
        !p2.fault_strategy().latency_fault_tolerance().is_available(&broker_name),
        "",
    );
    let n = send_ok(&p2, &topic, 5, "iso").await;
    // Single broker: none of the three filters can pick one → fall back to plain round-robin and messages still go out (isolation does not lose the send capability).
    ck.check("S4 under isolation the single broker's fallback round-robin still sends 5", n == 5, &format!("ok={n}"));

    // ---------------- S5 recovery when isolation expires ----------------
    println!("\nS5 recovery when isolation expires (after remove, inject 2000ms)");
    let tolerance = p2.fault_strategy().latency_fault_tolerance();
    tolerance.remove(&broker_name);
    tolerance.update_fault_item(&broker_name, 1, 2000, true);
    ck.check(
        "S5 unavailable while isolated",
        !tolerance.is_available(&broker_name),
        "",
    );
    tokio::time::sleep(Duration::from_millis(2300)).await;
    ck.check("S5 available again after expiry", tolerance.is_available(&broker_name), "");
    let n = send_ok(&p2, &topic, 3, "after").await;
    ck.check("S5 sending normal after recovery", n == 3, &format!("ok={n}"));
    p2.shutdown();

    // Clean up this topic (a failure does not change the verdict, notice only).
    if let Err(e) = admin.delete_topic_in_broker(&broker_addr, &topic, 5000).await {
        println!("  [WARN] deleting topic {topic} failed: {e}");
    }

    ck
}

fn report(ck: &Checker) {
    println!(
        "== summary: {} passed, {} failed ==",
        ck.passed,
        ck.failed.len()
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
    let ck = run(&namesrv).await;
    report(&ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
