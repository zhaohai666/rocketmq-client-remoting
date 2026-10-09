//! 验证 Rust 轨迹默认自建 producer 真的把轨迹发到了 RMQ_SYS_TRACE_TOPIC。
//!
//! 修复前：未注入 `TraceDispatcherConfig::producer` 时兜底
//! `DisabledTraceProducer`，轨迹链路默认**静默断开**（其他端都没这个问题）。
//! 修复后：与 Python `_get_and_create_trace_producer` 对齐，自建真实
//! DefaultMQProducer。本工具发 3 条业务消息后用 admin 查 RMQ_SYS_TRACE_TOPIC
//! 的 maxOffset，> 0 即轨迹链路真的通了。
//!
//! 用法：cargo run --example live_trace_default -- 127.0.0.1:9876
use std::process::ExitCode;
use std::time::Duration;

use rocketmq_client_remoting::client::admin::DefaultMQAdminExt;
use rocketmq_client_remoting::client::producer::{DefaultMQProducer, ProducerConfig};
use rocketmq_client_remoting::common::message::Message;

#[tokio::main]
async fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().collect();
    let namesrv = argv
        .get(1)
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "127.0.0.1:9876".to_string());

    // 业务生产者：打开轨迹（默认自建内部轨迹 producer，无需注入桥接）
    let cfg = ProducerConfig {
        producer_group: "GID_RsTraceDefault".to_string(),
        enable_trace: true,
        ..Default::default()
    };
    let producer = match DefaultMQProducer::with_config(cfg) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("producer build: {e}");
            return ExitCode::FAILURE;
        }
    };
    producer.set_namesrv_addr(&namesrv);
    if let Err(e) = producer.start().await {
        eprintln!("producer start: {e}");
        return ExitCode::FAILURE;
    }

    const N: usize = 3;
    let mut sent = 0usize;
    for i in 0..N {
        let mut msg = Message::new(
            "RsTraceDefaultTopic",
            Some(format!("trace-default-{i}").as_bytes()),
        );
        match producer.send(&mut msg, Some(5000), None).await {
            Ok(_) => sent += 1,
            Err(e) => eprintln!("send {i}: {e}"),
        }
    }
    println!("sent={sent}/{N}");

    // 轨迹批量上报有窗口期，给一点时间
    tokio::time::sleep(Duration::from_secs(6)).await;

    let admin = DefaultMQAdminExt::new();
    admin.set_namesrv_addr(&namesrv);
    admin.start().await.expect("admin start");
    let stats = admin
        .examine_topic_stats("RMQ_SYS_TRACE_TOPIC")
        .await
        .expect("examine trace topic stats");
    let mut max_total: i64 = 0;
    for (_, off) in stats.offset_table.iter() {
        max_total += off.max_offset;
    }
    println!("RMQ_SYS_TRACE_TOPIC maxOffset total = {max_total}");
    admin.shutdown();
    producer.shutdown();

    if sent == N && max_total > 0 {
        println!("PASS  trace dispatcher built its own real producer and reported");
        ExitCode::SUCCESS
    } else {
        println!("FAIL  sent={sent}/{N} traceMaxOffset={max_total}");
        ExitCode::FAILURE
    }
}
