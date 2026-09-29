//! 轨迹 dispatcher **自动桥接**对真实 5.5.1 broker 的联调验证（任务 #24）。
//!
//! 与 `live_rebalance_and_trace.rs`（R3：手工构造 TraceContext + 手工挂钩子）和
//! `live_producer.rs`（P4：调用方注入 dispatcher）互补：这里验证的是
//! **`enable_trace=true` 且什么都不注入**时 —— 生产者 / 消费者的 `start()` 自动建
//! `AsyncTraceDispatcher`（含真实内部生产者，组名 `_INNER_TRACE_PRODUCER-...`）、
//! 自动挂轨迹钩子、自动 start —— 轨迹记录真的落到 `RMQ_SYS_TRACE_TOPIC`
//! 并能被读回来解码。此前这条路径默认 `DisabledTraceProducer`，轨迹完全发不出去。
//!
//! 场景：
//! - B1 自动桥接的形状：`start()` 后 `trace_dispatcher()` 是 Some，且指向系统轨迹
//!   topic；关闭 trace 的对照生产者 `trace_dispatcher()` 仍是 None。
//! - B2 生产侧端到端：trace 生产者发 3 条，trace reader（订阅 RMQ_SYS_TRACE_TOPIC
//!   的独立 push consumer）读到含这 3 个 msgId 的轨迹文本，解码出 3 条 Pub
//!   （success、msgId 命中）。
//! - B3 消费侧端到端：trace 消费者把这 3 条消费掉，轨迹里出现 ≥3 条 SubBefore +
//!   ≥3 条 SubAfter，组名都是业务消费组，SubBefore↔SubAfter 按 requestId 配对。
//! - B4 对照：`enable_trace=false` 的生产者发的 2 条**没有** Pub 记录（它们的
//!   msgId 不出现在轨迹文本里）—— 证明轨迹确实来自自动桥接，不是别的通道。
//! - 清理：删业务 topic（RMQ_SYS_TRACE_TOPIC 是系统 topic，保留）。
//!
//! 前置：broker 配置 `traceTopicEnable=true`（否则 RMQ_SYS_TRACE_TOPIC 不预建，
//! 系统 topic 也无法经 admin 创建）。
//!
//! 用法：
//! ```text
//! cargo run --example live_trace_bridge -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::producer::{DefaultMQProducer, ProducerConfig};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently, SendStatus,
};
use rocketmq_client_remoting::client::trace::{TraceDataEncoder, TraceType};
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

struct Inbox {
    items: Mutex<Vec<String>>,
    signal: Condvar,
}

impl Inbox {
    fn new() -> Arc<Self> {
        Arc::new(Inbox {
            items: Mutex::new(Vec::new()),
            signal: Condvar::new(),
        })
    }
    fn push(&self, text: String) {
        self.items.lock().unwrap().push(text);
        self.signal.notify_all();
    }
    fn wait_for(&self, deadline: Instant, mut predicate: impl FnMut(&[String]) -> bool) -> bool {
        let mut guard = self.items.lock().unwrap();
        while !predicate(&guard) {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (g, _timeout) = self
                .signal
                .wait_timeout(guard, deadline.saturating_duration_since(now))
                .unwrap();
            guard = g;
        }
        true
    }
}

/// 业务消息 listener：全部认成功。
struct BizListener;

impl MessageListenerConcurrently for BizListener {
    fn consume_message(
        &self,
        _msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

/// trace topic reader：把每条轨迹消息的 body 文本原样收进 Inbox。
struct TraceReaderListener {
    inbox: Arc<Inbox>,
}

impl MessageListenerConcurrently for TraceReaderListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        for m in msgs {
            self.inbox
                .push(String::from_utf8_lossy(m.get_body()).into_owned());
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let namesrv = match env::args().nth(1) {
        Some(a) => a,
        None => {
            eprintln!("usage: live_trace_bridge <namesrv>");
            return ExitCode::FAILURE;
        }
    };
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let topic = format!("RustTraceBridge{stamp}");
    let biz_group = format!("GID_trace_bridge_{stamp}");
    let reader_group = format!("GID_trace_bridge_reader_{stamp}");
    let notrace_group = format!("GID_trace_bridge_notrace_{stamp}");

    let mut pass = 0usize;
    let mut fail = 0usize;
    macro_rules! check {
        ($name:expr, $ok:expr $(,)?) => {
            check!($name, $ok, "")
        };
        ($name:expr, $ok:expr, $($detail:expr),+ $(,)?) => {
            if $ok {
                pass += 1;
                println!("  [PASS] {} {}", $name, format!($($detail),+));
            } else {
                fail += 1;
                println!("  [FAIL] {} {}", $name, format!($($detail),+));
            }
        };
    }

    // ---------- B1 自动桥接的形状 ----------
    let producer = DefaultMQProducer::with_config(ProducerConfig {
        producer_group: format!("GID_trace_bridge_producer_{stamp}"),
        name_server_addrs: vec![namesrv.clone()],
        instance_name: format!("live-trace-bridge-producer-{stamp}"),
        enable_trace: true,
        ..Default::default()
    })
    .unwrap();
    producer.start().await.unwrap();
    producer.create_topic(&topic, 1, 0).await.unwrap();

    let bridged = producer.trace_dispatcher();
    check!("B1 enable_trace 生产者 start 后自动带 dispatcher", bridged.is_some());
    let inner_topic = bridged
        .as_ref()
        .map(|d| d.trace_topic_name())
        .unwrap_or_default();
    check!(
        "B1 内部 dispatcher 指向系统轨迹 topic",
        inner_topic == MixAll::TRACE_TOPIC,
        "trace_topic={inner_topic}"
    );

    let notrace = DefaultMQProducer::with_config(ProducerConfig {
        producer_group: notrace_group.clone(),
        name_server_addrs: vec![namesrv.clone()],
        instance_name: format!("live-trace-bridge-notrace-{stamp}"),
        enable_trace: false,
        ..Default::default()
    })
    .unwrap();
    notrace.start().await.unwrap();
    check!(
        "B1 enable_trace=false 的生产者仍无 dispatcher",
        notrace.trace_dispatcher().is_none()
    );

    // ---------- B2/B4 生产 ----------
    // create_topic 写 broker 成功后，broker 还要把路由注册到 namesrv（周期可达 30s），
    // 期间发送按 10005 客户端错误重试等路由。
    let mut traced_ids = Vec::new();
    for i in 0..3 {
        let body = format!("trace-bridge-{stamp}-{i}");
        let mut msg = Message::new(&topic, Some(body.as_bytes()));
        msg.set_tags("TagA");
        let mut sr = None;
        let mut last_err = String::new();
        for attempt in 0..30 {
            match producer.send(&mut msg, None, None).await {
                Ok(r) => {
                    sr = Some(r);
                    break;
                }
                Err(e) => {
                    if attempt < 3 {
                        eprintln!("  send retry {attempt}: {e}");
                    }
                    last_err = format!("{e}");
                    tokio::time::sleep(Duration::from_secs(2)).await
                }
            }
        }
        let sr = sr.unwrap_or_else(|| panic!("等 60s 仍发不出去, last_err={last_err}"));
        check!("B2 消息 SEND_OK", sr.status == SendStatus::SendOk, "i={i}");
        traced_ids.push(sr.msg_id.unwrap_or_default());
    }

    let mut untraced_ids = Vec::new();
    for i in 0..2 {
        let body = format!("trace-bridge-nofx-{stamp}-{i}");
        let mut msg = Message::new(&topic, Some(body.as_bytes()));
        msg.set_tags("TagB");
        let sr = notrace.send(&mut msg, None, None).await.unwrap();
        untraced_ids.push(sr.msg_id.unwrap_or_default());
    }

    // ---------- 消费者（业务 + trace reader）----------
    let biz = DefaultMQPushConsumer::with_config(ConsumerConfig {
        consumer_group: biz_group.clone(),
        name_server_addrs: vec![namesrv.clone()],
        instance_name: format!("live-trace-bridge-consumer-{stamp}"),
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        enable_trace: true,
        ..Default::default()
    })
    .unwrap();
    biz.subscribe(&topic, "*").unwrap();
    biz.set_message_listener_concurrently(Arc::new(BizListener));
    biz.start().await.unwrap();
    check!(
        "B3 enable_trace 消费者 start 后自动带 dispatcher",
        biz.trace_dispatcher().is_some()
    );

    let reader_inbox = Inbox::new();
    let reader = DefaultMQPushConsumer::with_config(ConsumerConfig {
        consumer_group: reader_group.clone(),
        name_server_addrs: vec![namesrv.clone()],
        instance_name: format!("live-trace-bridge-reader-{stamp}"),
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..Default::default()
    })
    .unwrap();
    reader
        .subscribe(MixAll::TRACE_TOPIC, "*")
        .map_err(|e| eprintln!("subscribe trace topic failed: {e}"))
        .unwrap();
    reader.set_message_listener_concurrently(Arc::new(TraceReaderListener {
        inbox: reader_inbox.clone(),
    }));
    reader.start().await.unwrap();

    // 等 reader 拉到含全部 3 个 msgId 的轨迹（flush 周期 5s + reader 首轮 rebalance）
    let deadline = Instant::now() + Duration::from_secs(120);
    let got_all = reader_inbox.wait_for(deadline, |texts| {
        let corpus = texts.join("");
        traced_ids.iter().all(|id| !id.is_empty() && corpus.contains(id))
    });
    check!(
        "B2 轨迹文本含全部 3 个业务 msgId（生产侧轨迹真的离开进程落到 broker）",
        got_all
    );

    // 解码核对 Pub / SubBefore / SubAfter
    let texts = reader_inbox.items.lock().unwrap().clone();
    let corpus = texts.join("");
    let mut pub_hits = 0usize;
    let mut pub_ok = 0usize;
    let mut sub_before = 0usize;
    let mut sub_after = 0usize;
    let mut before_ids: Vec<String> = Vec::new();
    let mut after_ids: Vec<String> = Vec::new();
    for text in &texts {
        for ctx in TraceDataEncoder::decoder_from_trace_data_string(Some(text)) {
            match ctx.trace_type {
                Some(TraceType::Pub) => {
                    if let Some(bean) = ctx.trace_beans.first() {
                        if traced_ids.contains(&bean.msg_id) {
                            pub_hits += 1;
                            if ctx.is_success {
                                pub_ok += 1;
                            }
                        }
                    }
                }
                Some(TraceType::SubBefore) if ctx.group_name == biz_group => {
                    sub_before += 1;
                    before_ids.push(ctx.request_id.clone());
                }
                Some(TraceType::SubAfter) if ctx.group_name == biz_group => {
                    sub_after += 1;
                    after_ids.push(ctx.request_id.clone());
                }
                _ => {}
            }
        }
    }
    let paired = before_ids
        .iter()
        .filter(|id| after_ids.contains(id))
        .count();
    check!(
        "B2 解码出 3 条 Pub 且 success",
        pub_hits == 3 && pub_ok == 3,
        "pub_hits={pub_hits} pub_ok={pub_ok}"
    );
    check!(
        "B3 SubBefore/SubAfter 各 ≥3 条且 requestId 配对",
        sub_before >= 3 && sub_after >= 3 && paired >= 3,
        "before={sub_before} after={sub_after} paired={paired}"
    );

    check!(
        "B4 对照：enable_trace=false 的消息没有 Pub 记录",
        untraced_ids
            .iter()
            .all(|id| id.is_empty() || !corpus.contains(id))
    );

    // ---------- 清理 ----------
    let admin = DefaultMQAdminExt::with_config(AdminConfig {
        instance_name: format!("ADMIN-trace-bridge-{stamp}"),
        name_server_addrs: vec![namesrv.clone()],
        timeout_millis: 10_000,
        ..Default::default()
    });
    admin.start().await.unwrap();
    let _ = admin.delete_topic(&topic, None).await;
    let _ = admin.delete_topic(&format!("%RETRY%{biz_group}"), None).await;
    admin.shutdown();
    producer.shutdown();
    notrace.shutdown();
    biz.shutdown();
    reader.shutdown();
    println!("== summary: {pass} passed, {fail} failed ==");
    if fail == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
