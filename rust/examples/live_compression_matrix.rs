//! 跨语言压缩互通的一端：`send` 发一条会自动压缩的消息，`recv` 用
//! [`DefaultLitePullConsumer`] 收回来并本地重建同一份载荷比 CRC32。
//!
//! 载荷配方与 `python/verify_compression_live.py`、`cpp/examples/compression_live.cpp`、
//! `.NET CompressionLive` **逐字节相同**（同一行文本重复后截断），所以四端不需要交换
//! 文件就能互相判定：只看接收端打印的 `match=`，**不要**比两边打印的 CRC 数字
//! （Java 口径的 `UtilAll.crc32` 会 `& 0x7FFFFFFF`，本仓库四端都用标准 CRC-32）。
//!
//! 用法：
//! ```text
//! cargo run --example live_compression_matrix -- send <topic> <group> <size>
//! cargo run --example live_compression_matrix -- recv <topic> <group> <size> [namesrv]
//! ```
//!
//! `recv` 走 lite 拉取消费者（订阅 + 后台灌缓冲 + poll），顺带证明解压发生在
//! 解码路径里（`MessageDecoder` 会清掉 `COMPRESSED_FLAG`）。

use std::env;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultLitePullConsumer, LitePullConsumerConfig,
};
use rocketmq_client_remoting::common::message::Message;
use rocketmq_client_remoting::common::util_all::crc32;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

/// 与 Java `CompressProbe.buildPayload` / Python `build_payload` 完全一致。
const LINE: &[u8] = b"rocketmq-compress-interop-payload-line-0123456789\n";

fn build_payload(size: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(size);
    while out.len() < size {
        out.extend_from_slice(LINE);
    }
    out.truncate(size);
    out
}

fn usage() -> ExitCode {
    eprintln!(
        "usage: live_compression_matrix send <topic> <group> <size> [namesrv]\n\
        \x20      live_compression_matrix recv <topic> <group> <size> [namesrv]"
    );
    ExitCode::FAILURE
}

#[tokio::main]
async fn main() -> ExitCode {
    let argv: Vec<String> = env::args().skip(1).collect();
    let mode = match argv.first() {
        Some(m) => m.as_str(),
        None => return usage(),
    };
    if argv.len() < 4 {
        return usage();
    }
    let topic = &argv[1];
    let group = &argv[2];
    let size: usize = match argv[3].parse() {
        Ok(n) => n,
        Err(_) => {
            eprintln!("size must be a number: {}", argv[3]);
            return ExitCode::FAILURE;
        }
    };
    let namesrv = argv
        .get(4)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:9876".to_string());
    let payload = build_payload(size);
    let crc = crc32(&payload);
    match mode {
        "send" => send(&namesrv, topic, group, &payload, crc).await,
        "recv" => recv(&namesrv, topic, group, &payload, crc).await,
        other => {
            eprintln!("unknown mode: {other}");
            usage()
        }
    }
}

/// `group` 只在 recv 侧有用；send 侧用它派生一个稳定的 producer group。
async fn send(namesrv: &str, topic: &str, group: &str, payload: &[u8], crc: u32) -> ExitCode {
    let mut msg = Message::new(topic, Some(payload));
    msg.set_keys(&format!("{topic}-probe"));
    let producer = match DefaultMQProducer::new(&format!("{group}_prod")) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("SEND_FAIL build failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    producer.set_namesrv_addr(namesrv);
    if let Err(e) = producer.start().await {
        eprintln!("SEND_FAIL start failed: {e}");
        return ExitCode::FAILURE;
    }
    let result = producer.send(&mut msg, Some(10_000), None).await;
    producer.shutdown();
    match result {
        Ok(r) => {
            println!(
                "SEND_OK len={} crc32={} msgId={}",
                payload.len(),
                crc,
                r.msg_id.clone().unwrap_or_default()
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("SEND_FAIL {e}");
            ExitCode::FAILURE
        }
    }
}

async fn recv(namesrv: &str, topic: &str, group: &str, payload: &[u8], crc: u32) -> ExitCode {
    let cfg = LitePullConsumerConfig {
        consumer_group: group.to_string(),
        name_server_addrs: vec![namesrv.to_string()],
        instance_name: format!("cm-{group}"),
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        poll_timeout_millis: 1000,
        ..Default::default()
    };
    let consumer = match DefaultLitePullConsumer::with_config(cfg) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("RECV_FAIL build failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    consumer.subscribe(topic, "*");
    if let Err(e) = consumer.start().await {
        eprintln!("RECV_FAIL start failed: {e}");
        return ExitCode::FAILURE;
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut got = None;
    while Instant::now() < deadline {
        let batch = consumer.poll(Some(1000)).await;
        if let Some(m) = batch.into_iter().find(|m| m.topic == *topic) {
            got = Some(m);
            break;
        }
    }
    consumer.shutdown();
    match got {
        Some(m) => {
            let body = m.get_body();
            let matched = body == payload && crc32(body) == crc;
            println!(
                "RECV_{} len={} crc32={} storeSize={} match={}",
                if matched { "OK" } else { "BAD" },
                body.len(),
                crc32(body),
                m.store_size,
                if matched { 1 } else { 0 },
            );
            if matched {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        None => {
            println!("RECV_NONE len=0 crc32=0 storeSize=0 match=0");
            ExitCode::FAILURE
        }
    }
}
