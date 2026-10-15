//! Integration verification of the trace dispatcher **auto-bridging** against a real 5.5.1 broker.
//!
//! Complementary to `live_rebalance_and_trace.rs` (R3: hand-built TraceContext + manually attached hooks) and
//! `live_producer.rs` (P4: caller-injected dispatcher): what is verified here is that when
//! **`enable_trace=true` and nothing is injected**, the producer's / consumer's `start()` automatically builds
//! an `AsyncTraceDispatcher` (with a real inner producer, group name `_INNER_TRACE_PRODUCER-...`),
//! attaches the trace hooks and starts it automatically, so trace records really land on `RMQ_SYS_TRACE_TOPIC`
//! and can be read back and decoded. Previously this path defaulted to `DisabledTraceProducer` and traces could not be sent at all.
//!
//! Scenarios:
//! - B1 the shape of the auto-bridging: after `start()`, `trace_dispatcher()` is Some and points at the system
//!   trace topic; the trace-off control producer's `trace_dispatcher()` stays None.
//! - B2 producer-side end to end: the trace producer sends 3 messages; the trace reader (an independent push consumer
//!   subscribed to RMQ_SYS_TRACE_TOPIC) reads trace text containing these 3 msgIds and decodes 3 Pub entries
//!   (success, msgId hit).
//! - B3 consumer-side end to end: the trace consumer consumes these 3 messages; the traces show ≥3 SubBefore +
//!   ≥3 SubAfter entries, all carrying the business consumer group, with SubBefore↔SubAfter paired by requestId.
//! - B4 control: the 2 messages sent by the `enable_trace=false` producer have **no** Pub records (their
//!   msgIds do not appear in the trace text) -- proving the traces really come from the auto-bridging, not another channel.
//! - Cleanup: delete the business topic (RMQ_SYS_TRACE_TOPIC is a system topic, kept).
//!
//! Precondition: broker configured with `traceTopicEnable=true` (otherwise RMQ_SYS_TRACE_TOPIC is not pre-created,
//! and system topics cannot be created via admin either).
//!
//! Usage:
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

/// Business message listener: acknowledges everything as success.
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

/// Trace topic reader: collects each trace message's body text verbatim into the Inbox.
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

    // ---------- B1 the shape of the auto-bridging ----------
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
    check!("B1 enable_trace producer automatically carries a dispatcher after start", bridged.is_some());
    let inner_topic = bridged
        .as_ref()
        .map(|d| d.trace_topic_name())
        .unwrap_or_default();
    check!(
        "B1 the inner dispatcher points at the system trace topic",
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
        "B1 the enable_trace=false producer still has no dispatcher",
        notrace.trace_dispatcher().is_none()
    );

    // ---------- B2/B4 produce ----------
    // After create_topic succeeds on the broker, the broker still has to register the route with the namesrv (period up to 30s);
    // meanwhile sends retry on the 10005 client error while waiting for the route.
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
        let sr = sr.unwrap_or_else(|| panic!("still unsent after waiting 60s, last_err={last_err}"));
        check!("B2 message SEND_OK", sr.status == SendStatus::SendOk, "i={i}");
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

    // ---------- Consumers (business + trace reader) ----------
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
        "B3 enable_trace consumer automatically carries a dispatcher after start",
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

    // Wait for the reader to pull traces containing all 3 msgIds (flush period 5s + the reader's first rebalance round)
    let deadline = Instant::now() + Duration::from_secs(120);
    let got_all = reader_inbox.wait_for(deadline, |texts| {
        let corpus = texts.join("");
        traced_ids.iter().all(|id| !id.is_empty() && corpus.contains(id))
    });
    check!(
        "B2 the trace text contains all 3 business msgIds (producer-side traces really leave the process and land on the broker)",
        got_all
    );

    // Decode and verify Pub / SubBefore / SubAfter
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
        "B2 decodes 3 Pub entries, all success",
        pub_hits == 3 && pub_ok == 3,
        "pub_hits={pub_hits} pub_ok={pub_ok}"
    );
    check!(
        "B3 SubBefore/SubAfter ≥3 each and requestId-paired",
        sub_before >= 3 && sub_after >= 3 && paired >= 3,
        "before={sub_before} after={sub_after} paired={paired}"
    );

    check!(
        "B4 control: the enable_trace=false messages have no Pub records",
        untraced_ids
            .iter()
            .all(|id| id.is_empty() || !corpus.contains(id))
    );

    // ---------- Cleanup ----------
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
