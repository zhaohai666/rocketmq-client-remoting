//! Live-broker verification of the data-loss contract for immediate shutdown / immediate process exit,
//! against a real 5.5.1 broker.
//!
//! The shutdown path has two data-loss windows, **both closed by the single act of "process exit"**:
//!
//! 1. **Send-back window**: when the listener returns RECONSUME_LATER / Suspend for everything, the dispatch
//!    loop still carries an in-flight send-back RPC; if shutdown merely spawns the finalizer and returns, the
//!    imminent process exit tears down the runtime together with the finalizer tasks riding on it -- the
//!    send-back never gets sent, and for messages whose offset was already advanced past "unsettled", the
//!    broker will never redeliver them and they never show up in `%DLQ%` (the user-reported scenario).
//!    Fixed contract: **`shutdown()` returning means finalization has landed** (in-flight batches drained,
//!    send-back RPCs answered, offsets clamped to the minimum offset of the still-unsettled messages).
//! 2. **Trace window**: when a short-lived trace producer shuts down, the last trace batch has just been
//!    flushed into in-flight send tasks; likewise, "spawn and return" cuts the whole batch off at process
//!    exit -- pulling `RMQ_SYS_TRACE_TOPIC` shows none of these `Pub` entries. Fixed contract:
//!    by the time `shutdown()` returns, the last trace batch has landed on the broker.
//!
//! **This example was rewritten around two points** (the old version had the "spawn and return" shape, and
//! padded after `shutdown()` with `sleep(2s)` -- the runtime was still alive, so the detached finalizer tasks
//! ran to completion and the test kept "passing", masking the bug):
//!
//! * Each phase runs in an **isolated runtime**; when the phase function returns, `drop(rt)` fires immediately,
//!   equivalent to process exit: any finalizer task still attached to the runtime is cancelled at that moment
//!   (the same semantics as the offline guard in `client::shutdown`);
//! * After `shutdown()` there is **no padding time at all**; every discriminator is pinned to the moment shutdown returns.
//!
//! Scenarios and discriminators:
//! - S1 concurrent consumption all failing + immediate process exit (two rounds):
//!   Round A runs at rt=0, and `consumeMessageBatchMaxSize = 20` hands the whole batch to the listener in one shot;
//!   the moment all 20 messages have reached the listener, `shutdown()` + process exit -- the whole batch of
//!   send-back RPCs is in flight. Round B reuses the group but **subscribes only to `%RETRY%`**: the mere presence
//!   of those 20 original bodies is the discriminator for "Round A's in-flight send-back landed before shutdown
//!   returned" (in the old implementation the send-back had not even been sent when shutdown returned, so
//!   %RETRY% would hold nothing); failing everything advances the round, and rt=1 >= the cap of 1 ⇒ the broker
//!   sends straight to `%DLQ%` (`AbstractSendMessageProcessor.consumerSendMsgBack`:
//!   `msgExt.getReconsumeTimes() >= maxReconsumeTimes` ⇒ DLQ); Round B likewise exits immediately once the listener
//!   has collected everything; Phase C verifies `%DLQ%` holds all 20 original bodies -- exactly the user-reported "messages never reached %DLQ%".
//! - S2 orderly consumption with SuspendCurrentQueueAMoment, suspended one message per batch: each message takes 3
//!   rounds (two local suspends, the third crossing the retry cap into orderly send-back); exit immediately when delivery
//!   rounds >= 45 (the first 15 messages have crossed the cap, the send-back chain is running) -- two legs verified
//!   together: the orderly send-back path (`orderly_send_message_back`, equally gated by `started` inside the shutdown-drain
//!   window) and the "minimum offset of the suspended batch": `%DLQ%` already holds some entries, and topic ∪ `%RETRY%` ∪ `%DLQ%` must collect all 20 original bodies.
//! - T short-lived trace producer: send 6 messages then exit immediately (the 5s flush period never fires,
//!   the whole batch is still queued/in flight); the trace reader must read back the `Pub` entries of all
//!   6 msgIds on RMQ_SYS_TRACE_TOPIC.
//!
//! Usage:
//! ```text
//! cargo run --example live_shutdown_race -- 127.0.0.1:9876 [legs]
//! ```
//! `legs` is comma-separated `s1,s2,t` (default or `all` = run all three legs).

use std::collections::HashSet;
use std::env;
use std::future::Future;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::producer::{DefaultMQProducer, ProducerConfig};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, ConsumeOrderlyContext,
    ConsumeOrderlyStatus, MessageListenerConcurrently, MessageListenerOrderly, SendStatus,
};
use rocketmq_client_remoting::client::trace::{TraceDataEncoder, TraceType};
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

const MSG_COUNT: usize = 20;
const TRACE_COUNT: usize = 6;
/// Window for "one round trip through the broker": %RETRY% first-redelivery delay level 3 = 10s, plus
/// route visibility of the redelivery/DLQ topics in the name server (broker heartbeat period 30s) and rebalance.
const ROUND_TRIP_WINDOW: Duration = Duration::from_secs(90);
/// Window for "messages already inside the subscribed topic": only rebalance + fetching remain.
const COLLECT_WINDOW: Duration = Duration::from_secs(60);
/// Window for the first message to reach the listener (triggers the immediate shutdown).
const FIRST_DELIVERY_WINDOW: Duration = Duration::from_secs(60);

/// Runs a "process" segment in an isolated runtime: when the phase function returns, **the runtime is torn down
/// immediately** -- equivalent to process exit (`Runtime`'s destructor cancels every unfinished task outright).
/// Under the fixed contract `shutdown()` returning means finalization has landed, so dropping the runtime loses
/// nothing; the old implementation (spawning the finalizer and returning) would cut off in-flight send-back / trace finalizers right here.
fn run_isolated<T>(what: &str, phase: impl Future<Output = T>) -> T {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build phase runtime");
    let out = rt.block_on(phase);
    drop(rt);
    println!("  [exit] {what}: runtime torn down (≈ process exit; detached finalizer tasks cancelled at this moment)");
    out
}

/// One collected message: source topic + original body.
#[derive(Clone)]
struct Hit {
    topic: String,
    body: String,
}

struct Inbox {
    items: Mutex<Vec<Hit>>,
    signal: Condvar,
}

impl Inbox {
    fn new() -> Arc<Self> {
        Arc::new(Inbox {
            items: Mutex::new(Vec::new()),
            signal: Condvar::new(),
        })
    }

    fn push(&self, topic: &str, body: String) {
        self.items.lock().unwrap().push(Hit {
            topic: topic.to_string(),
            body,
        });
        self.signal.notify_all();
    }

    fn wait_until(&self, deadline: Instant, mut predicate: impl FnMut(&[Hit]) -> bool) -> bool {
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

    /// Waits until every body in `expect` has appeared (optionally filtered by source topic).
    fn wait_for_bodies(&self, deadline: Instant, expect: &[String], topic: Option<&str>) -> bool {
        self.wait_until(deadline, |hits| {
            expect.iter().all(|e| {
                hits.iter()
                    .any(|h| (topic.is_none() || topic == Some(h.topic.as_str())) && &h.body == e)
            })
        })
    }

    /// Number of distinct bodies (optionally filtered by source topic).
    fn count_bodies(&self, topic: Option<&str>) -> usize {
        let hits = self.items.lock().unwrap();
        let mut set = HashSet::new();
        for h in hits.iter() {
            if topic.is_none() || topic == Some(h.topic.as_str()) {
                set.insert(&h.body);
            }
        }
        set.len()
    }

    fn hits_of(&self, topic: &str) -> Vec<Hit> {
        self.items
            .lock()
            .unwrap()
            .iter()
            .filter(|h| h.topic == topic)
            .cloned()
            .collect()
    }
}

/// S1 rounds A/B: fail everything and record. Round A waits for "all 20 delivered to the listener" (the whole
/// batch of send-backs in flight at that instant) to trigger the immediate shutdown; Round B collecting all 20
/// from `%RETRY%` is the discriminator for "Round A's send-back has landed".
struct FailAllConcurrent {
    inbox: Arc<Inbox>,
}

impl MessageListenerConcurrently for FailAllConcurrent {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        for m in msgs {
            self.inbox
                .push(&m.topic, String::from_utf8_lossy(m.get_body()).into_owned());
        }
        ConsumeConcurrentlyStatus::ReconsumeLater
    }
}

/// S2 first round: suspend everything; also counts delivery rounds (the main thread waits for the round count to
/// build up before shutting down, so some messages cross the orderly send-back cap and the send-back RPC is truly in flight).
struct SuspendAllOrderlyShared(Arc<AtomicUsize>);

impl MessageListenerOrderly for SuspendAllOrderlyShared {
    fn consume_message(
        &self,
        _msgs: &[MessageExt],
        _context: &mut ConsumeOrderlyContext,
    ) -> ConsumeOrderlyStatus {
        self.0.fetch_add(1, Ordering::SeqCst);
        ConsumeOrderlyStatus::SuspendCurrentQueueAMoment
    }
}

/// Collector (concurrent): pushes original bodies into the Inbox, acks everything as success.
struct Collector {
    inbox: Arc<Inbox>,
}

impl MessageListenerConcurrently for Collector {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        for m in msgs {
            self.inbox
                .push(&m.topic, String::from_utf8_lossy(m.get_body()).into_owned());
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

/// S2's v2 also consumes orderly (same-group semantics), likewise acking everything as success.
struct OrderlyCollector {
    inbox: Arc<Inbox>,
}

impl MessageListenerOrderly for OrderlyCollector {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeOrderlyContext,
    ) -> ConsumeOrderlyStatus {
        for m in msgs {
            self.inbox
                .push(&m.topic, String::from_utf8_lossy(m.get_body()).into_owned());
        }
        ConsumeOrderlyStatus::Success
    }
}

async fn send_with_route_retry(
    producer: &DefaultMQProducer,
    topic: &str,
    body: &str,
) -> Option<String> {
    let mut msg = Message::new(topic, Some(body.as_bytes()));
    msg.set_tags("TagA");
    let mut msg_id = None;
    for attempt in 0..30 {
        match producer.send(&mut msg, None, None).await {
            Ok(r) if r.status == SendStatus::SendOk => {
                msg_id = r.msg_id;
                break;
            }
            Ok(r) => eprintln!("  send not OK ({r:?}), retry {attempt}"),
            Err(e) => eprintln!("  send retry {attempt}: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    msg_id
}

async fn produce_n(producer: &DefaultMQProducer, topic: &str, prefix: &str, n: usize) {
    for i in 0..n {
        let body = format!("{prefix}-{i}");
        let id = send_with_route_retry(producer, topic, &body)
            .await
            .unwrap_or_else(|| panic!("still unsent after 60s: {body}"));
        assert!(!id.is_empty(), "empty msg id for {body}");
    }
}

fn consumer_config(
    group: &str,
    instance: &str,
    namesrv: &str,
    orderly: bool,
    max_reconsume_times: i32,
    batch_max: i32,
) -> ConsumerConfig {
    ConsumerConfig {
        consumer_group: group.to_string(),
        name_server_addrs: vec![namesrv.to_string()],
        instance_name: instance.to_string(),
        // FIRST_OFFSET: a fresh group must read the pre-sent messages back from the head of the queue on first
        // start; same when this group has no offset on the redelivery topic (from_where only applies without an offset).
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        max_reconsume_times,
        // S1 needs the whole batch in flight together: with the default batch of 1, only one send-back would be
        // in flight at the moment of shutdown, diluting the discriminator into an atomic timing coincidence.
        consume_message_batch_max_size: batch_max,
        suspend_current_queue_time_millis: if orderly { 50 } else { 1000 },
        ..Default::default()
    }
}

fn main() -> ExitCode {
    let namesrv = match env::args().nth(1) {
        Some(a) => a,
        None => {
            eprintln!("usage: live_shutdown_race <namesrv> [legs: s1,s2,t]");
            return ExitCode::FAILURE;
        }
    };
    let legs: Vec<String> = env::args()
        .nth(2)
        .map(|s| s.split(',').map(|x| x.trim().to_string()).collect())
        .unwrap_or_default();
    let want = |leg: &str| legs.is_empty() || legs.iter().any(|l| l == "all" || l == leg);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let s1_topic = format!("RustShdnRaceS1{stamp}");
    let s1_group = format!("GID_shdn_race_s1_{stamp}");
    let s1_prod_group = format!("GID_shdn_race_s1_prod_{stamp}");
    let s2_topic = format!("RustShdnRaceS2{stamp}");
    let s2_group = format!("GID_shdn_race_s2_{stamp}");
    let s2_prod_group = format!("GID_shdn_race_s2_prod_{stamp}");
    let t_topic = format!("RustShdnRaceT{stamp}");
    let t_group = format!("GID_shdn_race_t_{stamp}");

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

    // ============ S1: concurrent RECONSUME_LATER, two rounds + immediate process exit each round ============
    if want("s1") {
        let s1_prefix = format!("s1-{stamp}");
        let s1_expect: Vec<String> = (0..MSG_COUNT).map(|i| format!("{s1_prefix}-{i}")).collect();

        // Round A: send 20 messages, then shut down immediately after v1 delivers the whole batch (all 20 at once)
        // to the listener (phase end after return = process exit; the whole batch of send-back RPCs is in flight)
        let a_delivered = run_isolated("S1-A v1 all-fail + immediate process exit", async {
            let producer = DefaultMQProducer::with_config(ProducerConfig {
                producer_group: s1_prod_group.clone(),
                name_server_addrs: vec![namesrv.clone()],
                instance_name: format!("live-shdn-race-s1-prod-{stamp}"),
                ..Default::default()
            })
            .unwrap();
            producer.start().await.unwrap();
            producer.create_topic(&s1_topic, 1, 0).await.unwrap();
            produce_n(&producer, &s1_topic, &s1_prefix, MSG_COUNT).await;

            let v1 = DefaultMQPushConsumer::with_config(consumer_config(
                &s1_group,
                &format!("live-shdn-race-s1-v1-{stamp}"),
                &namesrv,
                false,
                1,
                MSG_COUNT as i32,
            ))
            .unwrap();
            v1.subscribe(&s1_topic, "*").unwrap();
            let inbox = Inbox::new();
            v1.set_message_listener_concurrently(Arc::new(FailAllConcurrent {
                inbox: inbox.clone(),
            }));
            v1.start().await.unwrap();
            let delivered =
                inbox.wait_for_bodies(Instant::now() + FIRST_DELIVERY_WINDOW, &s1_expect, None);
            // Immediate shutdown: once it returns this is "process exit", no padding time at all
            v1.shutdown();
            delivered
        });
        check!(
            "S1-A all 20 delivered to the listener (whole batch of send-backs in flight, immediate process exit)",
            a_delivered
        );

        // Round B: a second instance of the same group **subscribing only to %RETRY%** -- the mere presence of
        // these 20 is the discriminator for "Round A's in-flight send-back landed before shutdown returned";
        // failing everything advances the round, rt=1 >= cap 1 ⇒ the broker reroutes to %DLQ%; exit immediately
        // after collecting everything again, and Round B's own in-flight send-backs must land too, or Phase C can never collect all 20.
        let (b_got_all, b_seen) = run_isolated("S1-B v2 second round consuming %RETRY% + immediate process exit", async {
            let inbox = Inbox::new();
            let retry_topic = MixAll::get_retry_topic(&s1_group);
            let v2 = DefaultMQPushConsumer::with_config(consumer_config(
                &s1_group,
                &format!("live-shdn-race-s1-v2-{stamp}"),
                &namesrv,
                false,
                1,
                MSG_COUNT as i32,
            ))
            .unwrap();
            v2.subscribe(&retry_topic, "*").unwrap();
            v2.set_message_listener_concurrently(Arc::new(FailAllConcurrent {
                inbox: inbox.clone(),
            }));
            v2.start().await.unwrap();
            let got = inbox.wait_for_bodies(Instant::now() + ROUND_TRIP_WINDOW, &s1_expect, None);
            let seen = inbox.count_bodies(None);
            v2.shutdown();
            (got, seen)
        });
        check!(
            "S1-A send-back landed before shutdown returned: v2 collected all 20 original bodies from %RETRY%",
            b_got_all,
            "seen={b_seen}"
        );

        // Phase C: subscribe only to %DLQ% (the user-reported scenario) -- if even one of Round B's 20 rt=1
        // send-backs failed to land "before shutdown returned", the 20 original bodies cannot be collected here.
        let (c_dlq, c_detail) = run_isolated("S1-C verify %DLQ%", async {
            let inbox = Inbox::new();
            let dlq_topic = MixAll::get_dlq_topic(&s1_group);
            let v3 = DefaultMQPushConsumer::with_config(consumer_config(
                &format!("GID_shdn_race_s1_probe_{stamp}"),
                &format!("live-shdn-race-s1-probe-{stamp}"),
                &namesrv,
                false,
                1,
                1,
            ))
            .unwrap();
            v3.subscribe(&dlq_topic, "*").unwrap();
            v3.set_message_listener_concurrently(Arc::new(Collector {
                inbox: inbox.clone(),
            }));
            v3.start().await.unwrap();
            // First wait for %DLQ% to fill up; if it cannot, count what is there (discriminator fails)
            let _ = inbox.wait_for_bodies(
                Instant::now() + COLLECT_WINDOW,
                &s1_expect,
                Some(&dlq_topic),
            );
            let dlq_n = inbox.count_bodies(Some(&dlq_topic));
            let detail = format!("dlq={dlq_n}");
            v3.shutdown();
            (dlq_n, detail)
        });
        check!(
            "S1-B DLQ send-back landed before shutdown returned: %DLQ% holds all 20 original bodies",
            c_dlq == MSG_COUNT,
            "{c_detail}"
        );
    }

    // ====== S2: orderly Suspend + immediate process exit while the send-back chain is in flight ======
    if want("s2") {
        let s2_prefix = format!("s2-{stamp}");
        let s2_expect: Vec<String> = (0..MSG_COUNT).map(|i| format!("{s2_prefix}-{i}")).collect();

        let a_deliveries = run_isolated("S2-A orderly all-suspend + immediate process exit", async {
            let producer = DefaultMQProducer::with_config(ProducerConfig {
                producer_group: s2_prod_group.clone(),
                name_server_addrs: vec![namesrv.clone()],
                instance_name: format!("live-shdn-race-s2-prod-{stamp}"),
                ..Default::default()
            })
            .unwrap();
            producer.start().await.unwrap();
            producer.create_topic(&s2_topic, 1, 0).await.unwrap();
            produce_n(&producer, &s2_topic, &s2_prefix, MSG_COUNT).await;

            let v1 = DefaultMQPushConsumer::with_config(consumer_config(
                &s2_group,
                &format!("live-shdn-race-s2-v1-{stamp}"),
                &namesrv,
                true,
                2,
                1,
            ))
            .unwrap();
            v1.subscribe(&s2_topic, "*").unwrap();
            let deliveries = Arc::new(AtomicUsize::new(0));
            v1.set_message_listener_orderly(Arc::new(SuspendAllOrderlyShared(Arc::clone(
                &deliveries,
            ))));
            v1.start().await.unwrap();
            // Wait for delivery rounds to build up: one queue cycling suspend per message, so 45 rounds means the
            // first few messages have crossed the retry cap and orderly send-back is running -- shut down now.
            let deadline = Instant::now() + Duration::from_secs(60);
            while deliveries.load(Ordering::SeqCst) < 45 && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let seen = deliveries.load(Ordering::SeqCst);
            v1.shutdown();
            seen
        });
        check!(
            "S2-A orderly suspend rounds are underway (triggering the immediate shutdown)",
            a_deliveries >= 45,
            "deliveries={a_deliveries}"
        );

        let (b_got_all, b_seen, b_dlq) =
            run_isolated("S2-B joint reproduction check + immediate process exit", async {
                let inbox = Inbox::new();
                let dlq_topic = MixAll::get_dlq_topic(&s2_group);
                let v2 = DefaultMQPushConsumer::with_config(consumer_config(
                    &s2_group,
                    &format!("live-shdn-race-s2-v2-{stamp}"),
                    &namesrv,
                    true,
                    2,
                    1,
                ))
                .unwrap();
                for t in [
                    s2_topic.as_str(),
                    MixAll::get_retry_topic(&s2_group).as_str(),
                    dlq_topic.as_str(),
                ] {
                    v2.subscribe(t, "*").unwrap();
                }
                v2.set_message_listener_orderly(Arc::new(OrderlyCollector {
                    inbox: inbox.clone(),
                }));
                v2.start().await.unwrap();
                let got =
                    inbox.wait_for_bodies(Instant::now() + ROUND_TRIP_WINDOW, &s2_expect, None);
                let seen = inbox.count_bodies(None);
                let dlq_n = inbox.count_bodies(Some(&dlq_topic));
                v2.shutdown();
                (got, seen, dlq_n)
            });
        check!(
            "S2 all 20 original bodies reproduced after immediate process exit (topic∪RETRY∪DLQ)",
            b_got_all,
            "seen={b_seen}"
        );
        check!(
            "S2 orderly send-back chain landed in %DLQ% (entries past the local retry cap)",
            b_dlq >= 1,
            "dlq={b_dlq}"
        );
    }

    // ============ T: short-lived trace producer + immediate process exit ============
    if want("t") {
        let t_msg_ids = run_isolated("T-A short-lived trace producer + immediate process exit", async {
            let producer = DefaultMQProducer::with_config(ProducerConfig {
                producer_group: t_group.clone(),
                name_server_addrs: vec![namesrv.clone()],
                instance_name: format!("live-shdn-race-t-prod-{stamp}"),
                enable_trace: true,
                ..Default::default()
            })
            .unwrap();
            producer.start().await.unwrap();
            producer.create_topic(&t_topic, 1, 0).await.unwrap();
            let mut msg_ids = Vec::new();
            for i in 0..TRACE_COUNT {
                let body = format!("t-{stamp}-{i}");
                let id = send_with_route_retry(&producer, &t_topic, &body)
                    .await
                    .unwrap_or_else(|| panic!("still unsent after 60s: {body}"));
                msg_ids.push(id);
            }
            // Immediate shutdown: the flush period (5s) has not fired, the whole trace batch is still queued/in flight
            producer.shutdown();
            msg_ids
        });

        let (t_got_all, t_pubs) = run_isolated("T-B read back traces", async {
            let inbox = Inbox::new();
            let reader = DefaultMQPushConsumer::with_config(ConsumerConfig {
                consumer_group: format!("GID_shdn_race_t_reader_{stamp}"),
                name_server_addrs: vec![namesrv.clone()],
                instance_name: format!("live-shdn-race-t-reader-{stamp}"),
                consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
                ..Default::default()
            })
            .unwrap();
            reader.subscribe(MixAll::TRACE_TOPIC, "*").unwrap();
            reader.set_message_listener_concurrently(Arc::new(Collector {
                inbox: inbox.clone(),
            }));
            reader.start().await.unwrap();
            let got = inbox.wait_until(Instant::now() + COLLECT_WINDOW, |hits| {
                let corpus: String = hits
                    .iter()
                    .filter(|h| h.topic == MixAll::TRACE_TOPIC)
                    .map(|h| h.body.as_str())
                    .collect();
                t_msg_ids
                    .iter()
                    .all(|id| !id.is_empty() && corpus.contains(id))
            });
            let mut pubs = 0usize;
            for hit in inbox.hits_of(MixAll::TRACE_TOPIC) {
                for ctx in TraceDataEncoder::decoder_from_trace_data_string(Some(&hit.body)) {
                    if matches!(ctx.trace_type, Some(TraceType::Pub))
                        && ctx
                            .trace_beans
                            .first()
                            .is_some_and(|b| t_msg_ids.contains(&b.msg_id))
                    {
                        pubs += 1;
                    }
                }
            }
            reader.shutdown();
            (got, pubs)
        });
        check!(
            "T after immediate process exit the last batch of 6 Pub trace entries is fully readable (shutdown return means already on the broker)",
            t_got_all && t_pubs >= TRACE_COUNT,
            "pubs={t_pubs}"
        );
    }

    // ================= cleanup =================
    run_isolated("cleanup", async {
        let admin = DefaultMQAdminExt::with_config(AdminConfig {
            instance_name: format!("ADMIN-shdn-race-{stamp}"),
            name_server_addrs: vec![namesrv.clone()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        admin.start().await.unwrap();
        for t in [
            s1_topic.as_str(),
            s2_topic.as_str(),
            t_topic.as_str(),
            MixAll::get_retry_topic(&s1_group).as_str(),
            MixAll::get_dlq_topic(&s1_group).as_str(),
            MixAll::get_retry_topic(&s2_group).as_str(),
            MixAll::get_dlq_topic(&s2_group).as_str(),
        ] {
            let _ = admin.delete_topic(t, None).await;
        }
        admin.shutdown();
    });

    println!("== summary: {pass} passed, {fail} failed ==");
    if fail == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
