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
//! cargo run --example live_compression_matrix -- send <topic> <group> <size> [namesrv] [codec]
//! cargo run --example live_compression_matrix -- recv <topic> <group> <size> [namesrv] [codec]
//! cargo run --example live_compression_matrix -- reuse <topic> <group> <size> [namesrv] [codec]
//! ```
//!
//! `codec`（`zlib` / `lz4` / `zstd`，默认 `zlib`）只决定发送端用哪个压缩算法；
//! 接收端按消息 sysFlag 的类型位自动解压，传不传都一样。
//!
//! `recv` 走 lite 拉取消费者（订阅 + 后台灌缓冲 + poll），顺带证明解压发生在
//! 解码路径里（`MessageDecoder` 会清掉 `COMPRESSED_FLAG`）。
//!
//! `reuse` 用**同一条 `Message`** 连发两次再各收一条：证明发送后调用方那条消息被
//! 还原（Java `sendKernelImpl:1095-1096` 的 finally）。不还原时第二次发出去的是
//! 压缩流、长度已低于阈值不再压、`COMPRESSED_FLAG` 也不置位 —— broker 照存、
//! 消费端不解压，业务拿到 zlib 字节，全程零报错。

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
        "usage: live_compression_matrix send <topic> <group> <size> [namesrv] [codec]\n\
        \x20      live_compression_matrix recv <topic> <group> <size> [namesrv] [codec]\n\
        \x20      live_compression_matrix reuse <topic> <group> <size> [namesrv] [codec]"
    );
    ExitCode::FAILURE
}

/// `zlib` / `lz4` / `zstd` → [`MessageSysFlag`] 的算法号；未知名字直接失败，
/// 免得矩阵里把「拼错 codec」当成「互通失败」。
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

/// `group` 只在 recv 侧有用；send 侧用它派生一个稳定的 producer group。
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
    producer.set_compress_type(codec);
    // 阈值以下不会压缩，矩阵要的是「真的压过」，所以调用方给够载荷尺寸（默认阈值 4 KiB）。
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

/// 起一个 lite 拉取消费者，尽量收满 `want` 条（60s 超时）；搭建失败返回 `None`。
async fn collect(
    namesrv: &str,
    topic: &str,
    group: &str,
    want: usize,
) -> Option<Vec<MessageExt>> {
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

/// 同一条 `Message` 连发两次，再各收一条回来比对。
///
/// 第一项检查不依赖 broker：第一次 `send` 之后调用方手里的 body 必须还是原文
/// （压缩是就地的，Java 靠 `finally` 里的 `prevBody` 换回来）。
/// 第二项是真机端到端：第二条消息在 broker 里必须是**压缩体**、收回来必须是原文。
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
    producer.set_compress_type(codec);
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
