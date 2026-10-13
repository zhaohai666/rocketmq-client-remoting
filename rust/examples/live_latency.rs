//! 发送延迟故障容错（sendLatencyFaultEnable）真机验证（覆盖 S1–S5 五场景）。
//!
//! 用法（先按 runbook 起本地单 broker 集群）：
//! ```text
//! cargo run --example live_latency -- 127.0.0.1:9876
//! ```
//!
//! 场景（单 broker broker-a / DefaultCluster）：
//!   S1 默认关闭：发 10 条全 OK，容错表不记录
//!   S2 开启故障规避：发 20 条全 OK（broker 健康，不触发隔离）
//!   S3 成功发送后容错表有记录：current_latency > 0 且可用/可达
//!   S4 手工注入隔离 → 单 broker 走 available→reachable→普通轮询 退化链仍全部发出
//!   S5 隔离到期恢复（注入 2000ms，≈2s 后恢复可用、发送正常）

use std::env;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::common::message::Message;
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};

const QUEUE_NUMS: i32 = 4;

// ------------------------------------------------------------------ 骨架

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

/// 逐条同步发送，返回成功条数。
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

/// 从默认 topic 路由里取（brokerName, brokerAddr）——单 broker 集群只有一项。
fn broker_of(
    route: &rocketmq_client_remoting::remoting::protocol::route::TopicRouteData,
) -> Option<(String, String)> {
    let bd = route.broker_datas.first()?;
    let addr = bd.select_broker_addr()?;
    Some((bd.broker_name.clone(), addr))
}

// ------------------------------------------------------------------ 驱动

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let topic = format!("LatencyRust{stamp}");

    println!("== LatencyFaultTolerance 真机验证  namesrv={namesrv}  topic={topic} ==");

    // 预建 topic（消费者/生产者都不做默认 topic 兜底，先建再发）。
    let admin = MQClientInstance::new(&format!("rust-live-lat-admin-{stamp}"), vec![namesrv.to_string()]);
    if let Err(e) = admin.start().await {
        ck.check("A0 admin 启动", false, &e.to_string());
        return ck;
    }
    let route = admin.get_topic_route_data(MixAll::DEFAULT_TOPIC).await;
    let (broker_name, broker_addr) = match route.as_ref().and_then(broker_of) {
        Some(v) => v,
        None => {
            ck.check("A0 默认 topic 路由可取", false, "no route of TBW102");
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
        ck.check("A0 预建 topic", false, &e.to_string());
        return ck;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    // ---------------- S1 默认关闭 ----------------
    println!("\nS1 默认关闭：行为不变，不记录容错表");
    let p1 = match DefaultMQProducer::new(&format!("rust-live-lat-off-{stamp}")) {
        Ok(p) => p,
        Err(e) => {
            ck.check("S1 构造", false, &e.to_string());
            return ck;
        }
    };
    p1.set_namesrv_addr(namesrv);
    ck.check(
        "S1 默认 send_latency_fault_enable=false",
        !p1.fault_strategy().is_send_latency_fault_enable(),
        "",
    );
    if let Err(e) = p1.start().await {
        ck.check("S1 start", false, &e.to_string());
        return ck;
    }
    let n = send_ok(&p1, &topic, 10, "off").await;
    ck.check("S1 关闭时发 10 条全部成功", n == 10, &format!("ok={n}"));
    ck.check(
        "S1 关闭时不记录容错表",
        p1.fault_strategy()
            .latency_fault_tolerance()
            .get_fault_item(&broker_name)
            .is_none(),
        "",
    );
    p1.shutdown();

    // ---------------- S2/S3 开启后健康路径 ----------------
    println!("\nS2/S3 开启故障规避：健康 broker 正常发送并记录延迟");
    let p2 = match DefaultMQProducer::new(&format!("rust-live-lat-on-{stamp}")) {
        Ok(p) => p,
        Err(e) => {
            ck.check("S2 构造", false, &e.to_string());
            return ck;
        }
    };
    p2.set_namesrv_addr(namesrv);
    p2.set_send_latency_fault_enable(true);
    ck.check(
        "S2 开关生效",
        p2.fault_strategy().is_send_latency_fault_enable(),
        "",
    );
    if let Err(e) = p2.start().await {
        ck.check("S2 start", false, &e.to_string());
        return ck;
    }
    let n = send_ok(&p2, &topic, 20, "on").await;
    ck.check("S2 开启后发 20 条全部成功", n == 20, &format!("ok={n}"));
    let item = p2
        .fault_strategy()
        .latency_fault_tolerance()
        .get_fault_item(&broker_name);
    ck.check("S3 容错表已记录 broker-a", item.is_some(), "");
    if let Some(item) = item {
        ck.check(
            "S3 记录的实测延迟 > 0",
            item.current_latency > 0,
            &format!("latency={}ms", item.current_latency),
        );
        ck.check(
            "S3 健康 broker 仍可用",
            p2.fault_strategy().latency_fault_tolerance().is_available(&broker_name)
                && item.is_reachable(),
            "",
        );
        ck.check(
            "S3 延迟低于第一档阈值（未触发隔离）",
            item.start_timestamp == 0,
            &format!("startTs={}", item.start_timestamp),
        );
    }

    // ---------------- S4 注入隔离 → 退化链 ----------------
    println!("\nS4 注入隔离：available→reachable→普通轮询 退化链仍能发出");
    p2.fault_strategy().update_fault_item(&broker_name, 99999, true, false);
    ck.check(
        "S4 注入后 broker-a 不可用",
        !p2.fault_strategy().latency_fault_tolerance().is_available(&broker_name),
        "",
    );
    let n = send_ok(&p2, &topic, 5, "iso").await;
    // 单 broker：三个过滤器都选不出 → 退化普通轮询，消息照发（不因隔离而丢发送能力）。
    ck.check("S4 隔离中单 broker 退化轮询仍发出 5 条", n == 5, &format!("ok={n}"));

    // ---------------- S5 隔离到期恢复 ----------------
    println!("\nS5 隔离到期恢复（remove 后注入 2000ms）");
    let tolerance = p2.fault_strategy().latency_fault_tolerance();
    tolerance.remove(&broker_name);
    tolerance.update_fault_item(&broker_name, 1, 2000, true);
    ck.check(
        "S5 隔离期内不可用",
        !tolerance.is_available(&broker_name),
        "",
    );
    tokio::time::sleep(Duration::from_millis(2300)).await;
    ck.check("S5 到期后恢复可用", tolerance.is_available(&broker_name), "");
    let n = send_ok(&p2, &topic, 3, "after").await;
    ck.check("S5 恢复后发送正常", n == 3, &format!("ok={n}"));
    p2.shutdown();

    // 清理本次 topic（失败不改变结论，仅提示）。
    if let Err(e) = admin.delete_topic_in_broker(&broker_addr, &topic, 5000).await {
        println!("  [WARN] 删除 topic {topic} 失败: {e}");
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
