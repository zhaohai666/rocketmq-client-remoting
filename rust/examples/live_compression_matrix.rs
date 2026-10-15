//! One end of the cross-language compression interop matrix: `send` sends a message that gets compressed automatically, `recv`
//! uses [`DefaultLitePullConsumer`] to receive it back, rebuilds the same payload locally and compares CRC32.
//!
//!
//! Usage:
//! ```text
//! cargo run --example live_compression_matrix -- send <topic> <group> <size> [namesrv] [codec]
//! cargo run --example live_compression_matrix -- recv <topic> <group> <size> [namesrv] [codec]
//! cargo run --example live_compression_matrix -- reuse <topic> <group> <size> [namesrv] [codec]
//! ```
//!
//! `codec` (`zlib` / `lz4` / `zstd`, default `zlib`) only decides which compression algorithm the sender uses;
//! the receiver decompresses automatically according to the message sysFlag type bits, with or without it.
//!
//! `recv` goes through the lite pull consumer (subscribe + background buffer fill + poll), and incidentally proves that decompression happens
//! in the decode path (`MessageDecoder` clears the `COMPRESSED_FLAG`).

use std::env;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultLitePullConsumer, LitePullConsumerConfig,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::sysflag::MessageSysFlag;
use rocketmq_client_remoting::common::util_all::crc32;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

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
        "usage: live_compression_matrix send <topic> <group> <size> [namesrv] [codec]\n\
        \x20      live_compression_matrix recv <topic> <group> <size> [namesrv] [codec]\n\
        \x20      live_compression_matrix reuse <topic> <group> <size> [namesrv] [codec]"
    );
    ExitCode::FAILURE
}

/// `zlib` / `lz4` / `zstd` → the [`MessageSysFlag`] algorithm numbers; an unknown name fails outright,
/// so the matrix does not mistake "a misspelled codec" for "an interop failure".
fn parse_codec(raw: &str) -> Result<i32, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "zlib" => Ok(MessageSysFlag::ZLIB_TYPE),
        "lz4" => Ok(MessageSysFlag::LZ4_TYPE),
        "zstd" => Ok(MessageSysFlag::ZSTD_TYPE),
        other => Err(format!("unknown codec: {other}")),
    }
}

fn codec_name(codec: i32) -> &'static str {
    match codec {
        MessageSysFlag::LZ4_TYPE => "lz4",
        MessageSysFlag::ZSTD_TYPE => "zstd",
        _ => "zlib",
    }
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
    let codec = match argv.get(5).map(|s| parse_codec(s)) {
        None => MessageSysFlag::ZLIB_TYPE,
        Some(Ok(c)) => c,
        Some(Err(e)) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let payload = build_payload(size);
    let crc = crc32(&payload);
    match mode {
        "send" => send(&namesrv, topic, group, &payload, crc, codec).await,
        "recv" => recv(&namesrv, topic, group, &payload, crc).await,
        "reuse" => reuse(&namesrv, topic, group, &payload, crc, codec).await,
        other => {
            eprintln!("unknown mode: {other}");
            usage()
        }
    }
}

/// `group` is only meaningful on the recv side; the send side derives a stable producer group from it.
async fn send(
    namesrv: &str,
    topic: &str,
    group: &str,
    payload: &[u8],
    crc: u32,
    codec: i32,
) -> ExitCode {
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
    if let Err(e) = producer.set_compress_type(codec) {
        eprintln!("SEND_FAIL bad codec {codec}: {e}");
        return ExitCode::FAILURE;
    }
    // Below the threshold nothing gets compressed, and the matrix needs "really compressed", so the caller supplies a large enough payload (default threshold 4 KiB).
    if let Err(e) = producer.start().await {
        eprintln!("SEND_FAIL start failed: {e}");
        return ExitCode::FAILURE;
    }
    let result = producer.send(&mut msg, Some(10_000), None).await;
    producer.shutdown();
    match result {
        Ok(r) => {
            println!(
                "SEND_OK codec={} len={} crc32={} msgId={}",
                codec_name(codec),
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

/// Starts a lite pull consumer and collects up to `want` messages (60s timeout); returns `None` if setup fails.
async fn collect(namesrv: &str, topic: &str, group: &str, want: usize) -> Option<Vec<MessageExt>> {
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
            return None;
        }
    };
    consumer.subscribe(topic, "*");
    if let Err(e) = consumer.start().await {
        eprintln!("RECV_FAIL start failed: {e}");
        return None;
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut got = Vec::new();
    while Instant::now() < deadline && got.len() < want {
        for m in consumer.poll(Some(1000)).await {
            if m.topic == *topic {
                got.push(m);
            }
        }
    }
    consumer.shutdown();
    Some(got)
}

async fn recv(namesrv: &str, topic: &str, group: &str, payload: &[u8], crc: u32) -> ExitCode {
    let got = match collect(namesrv, topic, group, 1).await {
        Some(got) => got,
        None => return ExitCode::FAILURE,
    };
    match got.into_iter().next() {
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

/// Sends the same `Message` twice in a row, then receives one back for each and compares.
async fn reuse(
    namesrv: &str,
    topic: &str,
    group: &str,
    payload: &[u8],
    crc: u32,
    codec: i32,
) -> ExitCode {
    let producer = match DefaultMQProducer::new(&format!("{group}_prod")) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("REUSE_FAIL build failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    producer.set_namesrv_addr(namesrv);
    if let Err(e) = producer.set_compress_type(codec) {
        eprintln!("REUSE_FAIL bad codec {codec}: {e}");
        return ExitCode::FAILURE;
    }
    if let Err(e) = producer.start().await {
        eprintln!("REUSE_FAIL start failed: {e}");
        return ExitCode::FAILURE;
    }
    let mut msg = Message::new(topic, Some(payload));
    let first = producer.send(&mut msg, Some(10_000), None).await;
    let after_first = msg.get_body().to_vec();
    let second = producer.send(&mut msg, Some(10_000), None).await;
    producer.shutdown();

    let restored = after_first == payload && crc32(&after_first) == crc;
    println!(
        "REUSE_AFTER_FIRST_{} len={} crc32={}",
        if restored { "OK" } else { "BAD" },
        after_first.len(),
        crc32(&after_first),
    );
    match (first, second) {
        (Ok(a), Ok(b)) => println!(
            "REUSE_SEND_OK codec={} msgId1={} msgId2={}",
            codec_name(codec),
            a.msg_id.clone().unwrap_or_default(),
            b.msg_id.clone().unwrap_or_default(),
        ),
        (Err(e), _) | (_, Err(e)) => {
            eprintln!("REUSE_FAIL send failed: {e}");
            return ExitCode::FAILURE;
        }
    }

    let got = match collect(namesrv, topic, group, 2).await {
        Some(got) => got,
        None => return ExitCode::FAILURE,
    };
    let matched = got.len() == 2
        && got
            .iter()
            .all(|m| m.get_body() == payload && crc32(m.get_body()) == crc);
    println!(
        "REUSE_RECV_{} count={} storeSize={:?} len={:?}",
        if matched { "OK" } else { "BAD" },
        got.len(),
        got.iter().map(|m| m.store_size).collect::<Vec<_>>(),
        got.iter().map(|m| m.get_body().len()).collect::<Vec<_>>(),
    );
    if restored && matched {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
