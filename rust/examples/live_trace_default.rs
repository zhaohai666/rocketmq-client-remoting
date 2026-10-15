//! Verifies that the Rust trace default built-in producer really sends trace data to RMQ_SYS_TRACE_TOPIC.
//!
//! Before the fix: when `TraceDispatcherConfig::producer` was not injected, the code fell back to
//! `DisabledTraceProducer`, and the trace pipeline was **silently disconnected** by default.
//! After the fix: when not injected, a real
//! DefaultMQProducer is built. This tool sends 3 business messages, then uses admin to check RMQ_SYS_TRACE_TOPIC
//! maxOffset; > 0 means the trace pipeline is really connected.
//!
//! Usage: cargo run --example live_trace_default -- 127.0.0.1:9876
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

    // Business producer: trace enabled (builds its own internal trace producer by default, no bridge injection needed)
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

    // Trace batch reporting has a window period; give it a little time
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
