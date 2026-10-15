//! Live verification of the flow-control gates (covers the six scenarios S0..S5).
//!
//! The offline unit test (`flow_control_hits_each_threshold` in `src/client/consumer.rs`) locks the
//! **criteria themselves**; this file locks two things on a live machine that offline tests never can:
//! - **A** the gates **do fire on a real broker**. One wrong unit or one misread threshold field and an
//!   offline mock buffer still "fires", while on a real machine it never fires (or always fires). This crate
//!   once misused a queue-level switch on the `pull_threshold_size_for_topic` gate, with offline all green.
//! - **B** after a hit **not one message may be lost**. Flow control only "pauses pulling", it is not "discard/skip": while
//!   paused the offset must not move past unconsumed messages, and after recovery the same queue must keep consuming to
//!   the end. An implementation written as "drop the batch / exit the loop" is invisible in a ten-second window; only counting every message exposes it.
//!
//!
//! ⚠ Big messages must be **incompressible** pseudo-random bytes: the producer compresses bodies above the
//!   compression threshold, so an all-same-byte payload compresses to a few hundred bytes and the broker's
//!   on-disk `storeSize` follows at a few hundred bytes -- "the size gate never fires" then becomes a fixture problem.
//! ⚠ The S1/S4 topics must have exactly **1 queue**: 8 x 400KB spread over 4 queues is only 800KB each,
//!   forever short of the 1MiB **queue-level** gate.
//! ⚠ Big messages must be **delivered concurrently** (see `Fixture::produce`): the gate watches the buffer built up
//!   by "in faster than out"; serial sends at ~190ms per message are nearly in sync with the 300ms/batch slow
//!   consumer, the buffer only reaches 1~2 messages, and the byte gate "never fires" on a real machine -- that is fixture pacing, not the criteria.
//!
//! Usage:
//! ```text
//! cargo run --example live_flow_control -- 127.0.0.1:9876
//! ```

use std::collections::BTreeSet;
use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::consumer::{
    ConsumerConfig, DefaultMQPushConsumer, MAX_POP_INVISIBLE_TIME, MIN_POP_INVISIBLE_TIME,
};
use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;
use tokio::task::JoinSet;

/// A threshold "far larger than any live-machine buffer", equivalent to turning that gate off.
const GATE_OFF: i32 = 65535;
/// The byte-gate "off" spelling is the same idea: the legal domain of `pullThresholdSizeForQueue` is [1, 1024] (in MiB),
/// 0 is already illegal, so 1024 MiB means "do not throttle".
const GATE_OFF_SIZE_MIB: i32 = 1024;
const GATE_ONE_MIB: i32 = 1;
const BIG: usize = 400 * 1024;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// Assertion accumulator: every scenario runs to completion before the summary; the first failure does not exit early.
struct Checker {
    passed: u32,
    failed: Vec<String>,
}

impl Checker {
    fn new() -> Checker {
        Checker {
            passed: 0,
            failed: Vec::new(),
        }
    }

    fn check(&mut self, name: &str, cond: bool, detail: &str) {
        if cond {
            self.passed += 1;
            println!("  [PASS] {name}  {detail}");
        } else {
            println!("  [FAIL] {name}: {detail}");
            self.failed.push(format!("{name}: {detail}"));
        }
    }

    fn abort(&mut self, name: &str, err: &str) {
        println!("  [FAIL] {name}: {err}");
        self.failed.push(format!("{name}: {err}"));
    }
}

async fn poll_until(mut pred: impl FnMut() -> bool, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if pred() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// For consuming: records bodies and optionally sleeps `batch_cost` per batch (creates a "pulled but unconsumed" backlog).
struct Sink {
    bodies: Mutex<BTreeSet<String>>,
    count: Mutex<usize>,
    /// The largest `MessageExt.store_size` seen: the byte gate's input is exactly this; 0 means the decode never carried
    /// the broker's TOTALSIZE (the size gate then can never fire, unrelated to the criteria implementation).
    max_store_size: Mutex<i32>,
    batch_cost: Duration,
}

impl Sink {
    fn new(batch_cost: Duration) -> Arc<Sink> {
        Arc::new(Sink {
            bodies: Mutex::new(BTreeSet::new()),
            count: Mutex::new(0),
            max_store_size: Mutex::new(0),
            batch_cost,
        })
    }

    fn count(&self) -> usize {
        *lock(&self.count)
    }

    fn distinct(&self) -> usize {
        lock(&self.bodies).len()
    }

    fn max_store_size(&self) -> i32 {
        *lock(&self.max_store_size)
    }
}

impl MessageListenerConcurrently for Sink {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        if !self.batch_cost.is_zero() {
            std::thread::sleep(self.batch_cost);
        }
        let mut bodies = lock(&self.bodies);
        let mut count = lock(&self.count);
        {
            let mut mx = lock(&self.max_store_size);
            for m in msgs {
                *mx = (*mx).max(m.store_size);
            }
        }
        for m in msgs {
            bodies.insert(String::from_utf8_lossy(m.get_body()).into_owned());
            *count += 1;
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

/// A 400KB **incompressible** body: a prefix + an xorshift64* pseudo-random tail.
/// Not `rand` (no new crate dependency), and it need not be reproducible -- it only has to resist compression.
fn big_body(tag: usize) -> Vec<u8> {
    let mut out = format!("FCBIG-{tag:03}").into_bytes();
    let mut state: u64 = 0x9E3779B97F4A7C15 ^ (tag as u64).wrapping_mul(0xD1B54A32D192ED03);
    if state == 0 {
        state = 0x2545F4914F6CDD1D;
    }
    while out.len() < BIG {
        // xorshift64*: emits one 8-byte block per step
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        out.extend_from_slice(&(state.wrapping_mul(0x2545F4914F6CDD1D)).to_le_bytes());
    }
    out.truncate(BIG);
    out
}

struct Fixture {
    namesrv: String,
    stamp: u64,
    producer: DefaultMQProducer,
    admin: MQClientInstance,
    broker_addr: String,
}

impl Fixture {
    fn new(namesrv: &str) -> Result<Fixture, String> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let producer = DefaultMQProducer::new(&format!("rust-live-fc-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            stamp,
            producer,
            admin: MQClientInstance::new(&format!("rust-live-fc-admin-{stamp}"), vec![namesrv.to_string()]),
            broker_addr: String::new(),
        })
    }

    async fn start(&mut self) -> Result<(), String> {
        self.admin
            .start()
            .await
            .map_err(|e| format!("admin instance start failed: {e}"))?;
        self.producer
            .start()
            .await
            .map_err(|e| format!("producer start failed: {e}"))?;
        let route = self
            .admin
            .get_topic_route_data(MixAll::DEFAULT_TOPIC)
            .await
            .ok_or_else(|| format!("no route of {} from namesrv", MixAll::DEFAULT_TOPIC))?;
        self.broker_addr = broker_of(&route)?;
        Ok(())
    }

    fn topic_name(&self, kind: &str) -> String {
        format!("RustLiveFlow{kind}{}", self.stamp)
    }

    fn group_name(&self, kind: &str) -> String {
        format!("rust-live-flow-{kind}-{}", self.stamp)
    }

    /// Creates the topic explicitly (not relying on autoCreateTopicEnable, so the queue count is definite -- S1 depends on "exactly 1 queue").
    async fn create_topic(&self, topic: &str, queues: i32) -> Result<(), String> {
        self.admin
            .create_topic_in_broker(
                &self.broker_addr,
                MixAll::DEFAULT_TOPIC,
                topic,
                queues,
                queues,
                DEFAULT_PERM,
                0,
                TopicFilterType::SINGLE_TAG,
                false,
                None,
                5000,
                2,
            )
            .await
            .map_err(|e| format!("create topic {topic} failed: {e}"))?;
        // Wait for NameServer route propagation, otherwise the consumer's first rebalance still finds nothing
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    /// Builds a subscribed, listener-equipped **not-yet-started** consumer (gates are set by the caller via update_config).
    fn consumer(
        &self,
        group: &str,
        topic: &str,
        sink: Arc<Sink>,
    ) -> Result<DefaultMQPushConsumer, String> {
        let cfg = ConsumerConfig {
            consumer_group: group.to_string(),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("live-{group}-{}", self.stamp),
            ..Default::default()
        };
        let consumer = DefaultMQPushConsumer::with_config(cfg)
            .map_err(|e| format!("build consumer failed: {e}"))?;
        consumer
            .subscribe(topic, "*")
            .map_err(|e| format!("subscribe {topic} failed: {e}"))?;
        consumer.set_message_listener_concurrently(sink);
        Ok(consumer)
    }

    /// Fires a batch of messages into the broker concurrently -- whether the flow-control fixture works at all hinges on the pacing of this step.
    ///
    /// The byte gate's input is the "pulled but unconsumed" buffer, and whether that backlog builds depends on **in faster than out**.
    /// Serial sends measured ~190ms per 400KB message, roughly in sync with the 300ms/batch slow listener;
    /// the backlog stalls at 1~2 messages (≈0.8MiB) waiting for the next one: a gate that "never fires on a real
    /// machine" is then a delivery-pacing problem, not wrong criteria. With concurrent delivery all 8 messages land
    /// within ~100ms, the backlog necessarily crosses 1MiB, and the criteria decouple from thread/network pacing.
    async fn produce(&self, topic: &str, bodies: Vec<Vec<u8>>) -> Result<(), String> {
        let mut set: JoinSet<Result<(), String>> = JoinSet::new();
        for body in bodies {
            let producer = self.producer.clone();
            let topic = topic.to_string();
            set.spawn(async move {
                let mut msg = Message::new(&topic, Some(body.as_slice()));
                producer
                    .send(&mut msg, Some(20000), None)
                    .await
                    .map_err(|e| format!("send failed: {e}"))?;
                Ok(())
            });
        }
        while let Some(joined) = set.join_next().await {
            joined.map_err(|e| format!("send task failed: {e}"))??;
        }
        Ok(())
    }

    fn shutdown(&self) {
        self.producer.shutdown();
        self.admin.shutdown();
    }
}

fn broker_of(route: &TopicRouteData) -> Result<String, String> {
    let bd = route
        .broker_datas
        .first()
        .ok_or_else(|| "route has no brokerData".to_string())?;
    bd.select_broker_addr()
        .ok_or_else(|| format!("broker {} has no address", bd.broker_name))
}

/// Gate parameters: `count`/`size`/`span`/`topic_count`; None means use the default value.
#[derive(Default, Clone, Copy)]
struct Gates {
    count: Option<i32>,
    size: Option<i32>,
    span: Option<i64>,
    topic_count: Option<i32>,
    /// When true, pins the consume thread pool to 1 thread.
    single_consumer: bool,
}

fn apply_gates(c: &DefaultMQPushConsumer, g: Gates) {
    c.update_config(|x| {
        if g.single_consumer {
            x.consume_thread_min = 1;
            x.consume_thread_max = 1;
        }
        if let Some(v) = g.count {
            x.pull_threshold_for_queue = v;
        }
        if let Some(v) = g.size {
            x.pull_threshold_size_for_queue = v;
        }
        if let Some(v) = g.span {
            x.consume_concurrently_max_span = v;
        }
        if let Some(v) = g.topic_count {
            x.pull_threshold_for_topic = v;
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn run_gate_case(
    ck: &mut Checker,
    fx: &Fixture,
    label: &str,
    kind: &str,
    queues: i32,
    bodies: Vec<Vec<u8>>,
    slow_ms: u64,
    gates: Gates,
    group: Option<String>,
) -> Option<(Arc<Sink>, DefaultMQPushConsumer)> {
    let topic = fx.topic_name(kind);
    if group.is_none() {
        if let Err(e) = fx.create_topic(&topic, queues).await {
            ck.abort(&format!("{label} create topic"), &e);
            return None;
        }
    }
    let group = group.unwrap_or_else(|| fx.group_name(&format!("{}-lower", label.to_lowercase())));
    let sink = Sink::new(Duration::from_millis(slow_ms));
    let consumer = match fx.consumer(&group, &topic, sink.clone()) {
        Ok(c) => c,
        Err(e) => {
            ck.abort(&format!("{label} build consumer"), &e);
            return None;
        }
    };
    apply_gates(&consumer, gates);
    if let Err(e) = consumer.start().await {
        ck.abort(&format!("{label} start"), &format!("{e}"));
        return None;
    }
    // Wait for the assignment to settle: the consumer must first be registered with the broker (30s heartbeat, shortened during start-up) before rebalance can assign anything
    tokio::time::sleep(Duration::from_secs(3)).await;
    if let Err(e) = fx.produce(&topic, bodies).await {
        ck.abort(&format!("{label} produce"), &e);
        return None;
    }
    Some((sink, consumer))
}

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    println!("== flow-control live verification: {namesrv} ==");
    let mut fx = match Fixture::new(namesrv) {
        Ok(v) => v,
        Err(e) => {
            ck.abort("fixture build", &e);
            return ck;
        }
    };
    if let Err(e) = fx.start().await {
        ck.abort("fixture start", &e);
        return ck;
    }

    let small: Vec<Vec<u8>> = (0..12).map(|i| format!("ok-{i:03}").into_bytes()).collect();

    // ---------------- S0 default gates + fast consumption: must not fire ----------------
    if let Some((sink, c)) = run_gate_case(
        &mut ck,
        &fx,
        "S0",
        "Defaults",
        4,
        small.clone(),
        0,
        Gates::default(),
        None,
    )
    .await
    {
        let all = poll_until(|| sink.count() >= 12, 40).await;
        let fc = c.flow_control_triggered();
        c.shutdown();
        // A gate falsely throttling normal traffic is the hardest incident to debug (in production it shows as throughput mysteriously halved), so lock triggered==0
        ck.check(
            "S0 default gates do not fire on ordinary traffic",
            fc == 0,
            &format!("triggered={fc}"),
        );
        ck.check(
            "S0 everything still arrives under the default gates",
            all && sink.count() == 12 && sink.distinct() == 12,
            &format!("count={} distinct={}", sink.count(), sink.distinct()),
        );
    }

    // ---------------- S1 queue-level byte gate (must be a single queue) ----------------
    let big8: Vec<Vec<u8>> = (0..8).map(big_body).collect();
    let size_gates = Gates {
        count: Some(GATE_OFF),
        size: Some(GATE_ONE_MIB),
        span: Some(GATE_OFF as i64),
        topic_count: None,
        single_consumer: true,
    };
    if let Some((sink, c)) = run_gate_case(
        &mut ck,
        &fx,
        "S1",
        "Size",
        1,
        big8,
        300,
        size_gates,
        None,
    )
    .await
    {
        // The buffered-count peak is printed alongside: the byte gate's input is exactly this buffer, and a peak <3 messages
        // (3×400KB>1MiB) means no backlog formed, so firing or not is moot -- when the fire assertion fails, look at this
        // number first to tell "gate not wired up" from "fixture never built a backlog" (the latter once came from serial delivery).
        let mut peak = 0usize;
        let hit = poll_until(
            || {
                peak = peak.max(c.buffered_message_count());
                c.flow_control_triggered() > 0
            },
            30,
        )
        .await;
        let all = poll_until(|| sink.count() >= 8, 40).await;
        let fc = c.flow_control_triggered();
        ck.check(
            "S1 a 400KB message reaches the listener with broker storeSize >= 400KB",
            sink.max_store_size() >= 400 * 1024,
            &format!("max_store_size={}", sink.max_store_size()),
        );
        ck.check(
            "S1 the per-queue byte gate fires on a real broker",
            hit,
            &format!("triggered={fc} buffered_peak_count={peak}"),
        );
        ck.check(
            "S1 no 400KB message is lost while the gate is engaged",
            all && sink.count() == 8 && sink.distinct() == 8,
            &format!("count={} distinct={}", sink.count(), sink.distinct()),
        );
        c.shutdown();
    }

    // ---------------- S2 offset-span gate ----------------
    let span_gates = Gates {
        count: Some(GATE_OFF),
        size: Some(GATE_OFF_SIZE_MIB),
        span: Some(2),
        topic_count: None,
        single_consumer: true,
    };
    let small14: Vec<Vec<u8>> = (0..14).map(|i| format!("s-{i:03}").into_bytes()).collect();
    if let Some((sink, c)) = run_gate_case(
        &mut ck,
        &fx,
        "S2",
        "Span",
        4,
        small14,
        300,
        span_gates,
        None,
    )
    .await
    {
        let hit = poll_until(|| c.flow_control_triggered() > 0, 30).await;
        let all = poll_until(|| sink.count() >= 14, 40).await;
        let fc = c.flow_control_triggered();
        c.shutdown();
        ck.check(
            "S2 the offset-span gate fires on a real broker",
            hit,
            &format!("triggered={fc}"),
        );
        ck.check(
            "S2 an over-span buffer is still drained completely",
            all && sink.count() == 14,
            &format!("count={}", sink.count()),
        );
    }

    // ---------------- S3 topic-level count gate (accumulated across queues) ----------------
    let topic_gates = Gates {
        count: Some(GATE_OFF),
        size: Some(GATE_OFF_SIZE_MIB),
        span: Some(GATE_OFF as i64),
        topic_count: Some(4),
        single_consumer: true,
    };
    let small16: Vec<Vec<u8>> = (0..16).map(|i| format!("t-{i:03}").into_bytes()).collect();
    if let Some((sink, c)) = run_gate_case(
        &mut ck,
        &fx,
        "S3",
        "Topic",
        4,
        small16,
        300,
        topic_gates,
        None,
    )
    .await
    {
        let hit = poll_until(|| c.flow_control_triggered() > 0, 30).await;
        let all = poll_until(|| sink.count() >= 16, 40).await;
        let fc = c.flow_control_triggered();
        c.shutdown();
        ck.check(
            "S3 the topic-level gate needs cross-queue accumulation and does fire",
            hit,
            &format!("triggered={fc}"),
        );
        ck.check(
            "S3 every queue of the throttled topic is still consumed to the end",
            all && sink.count() == 16,
            &format!("count={}", sink.count()),
        );
    }

    // ---------------- S4 a queue that hit flow control keeps consuming after recovery ----------------
    // Reuses S1's group and topic (the offset was already committed by S1 to the broker's end). This locks the
    // "pause 100ms" written as "exit the pull loop" bug -- that queue would halt forever, and S1's already-consumed messages cannot tell the difference.
    let big6: Vec<Vec<u8>> = (100..106).map(big_body).collect();
    if let Some((sink, c)) = run_gate_case(
        &mut ck,
        &fx,
        "S4",
        "Size",
        1,
        big6,
        300,
        size_gates,
        Some(fx.group_name("s1-lower")),
    )
    .await
    {
        let all = poll_until(|| sink.count() >= 6, 40).await;
        let hit = poll_until(|| c.flow_control_triggered() > 0, 10).await;
        let fc = c.flow_control_triggered();
        c.shutdown();
        ck.check(
            "S4 the queue that hit the gate keeps consuming after it restarts",
            all && sink.count() == 6 && sink.distinct() == 6,
            &format!("count={} distinct={}", sink.count(), sink.distinct()),
        );
        ck.check(
            "S4 the gate is still armed for a second round (not one-shot)",
            hit,
            &format!("triggered={fc}"),
        );
    }

    let s5_topic = fx.topic_name("Boundary");
    let s5_group = fx.group_name("s5");
    let bad_group = fx.group_name("s5-rejected");
    if let Err(e) = fx.create_topic(&s5_topic, 4).await {
        ck.abort("S5 create topic", &e);
    } else {
        let sink5 = Sink::new(Duration::ZERO);
        match fx.consumer(&s5_group, &s5_topic, sink5.clone()) {
            Err(e) => ck.abort("S5 build consumer", &e),
            Ok(c5) => {
                // Each gate takes an endpoint: spellings like `pullBatchSize=1024` -- "right at the upper bound = effectively
                // no throttle" -- are common in production; a false rejection locks the user out (the consequence of writing the interval as open).
                c5.update_config(|x| {
                    x.consume_thread_min = 1;
                    x.consume_thread_max = 2;
                    x.consume_concurrently_max_span = i64::from(GATE_OFF);
                    x.pull_threshold_for_queue = GATE_OFF;
                    x.pull_threshold_for_topic = -1;
                    x.pull_threshold_size_for_queue = GATE_OFF_SIZE_MIB;
                    x.pull_threshold_size_for_topic = -1;
                    x.pull_interval = 0;
                    x.consume_message_batch_max_size = 1;
                    x.pull_batch_size = 1024;
                    x.pop_invisible_time = MAX_POP_INVISIBLE_TIME;
                    x.pop_batch_nums = 32;
                });
                if let Err(e) = c5.start().await {
                    ck.abort("S5 boundary config must start", &format!("{e}"));
                } else {
                    ck.check("S5 boundary config starts", true, "is_started=true");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    let small10: Vec<Vec<u8>> =
                        (0..10).map(|i| format!("b-{i:03}").into_bytes()).collect();
                    if let Err(e) = fx.produce(&s5_topic, small10).await {
                        ck.abort("S5 produce", &e);
                    }
                    let all = poll_until(|| sink5.count() >= 10, 40).await;
                    ck.check(
                        "S5 every message arrives under the boundary config",
                        all && sink5.count() == 10 && sink5.distinct() == 10,
                        &format!("count={} distinct={}", sink5.count(), sink5.distinct()),
                    );

                    // Out-of-range configs: rejected locally + the started flag is not rolled back (a failed start must leave a clean object)
                    type GateBreaker = fn(&mut ConsumerConfig);
                    let illegal: Vec<(GateBreaker, &str)> = vec![
                        (
                            |c| c.pull_threshold_size_for_queue = 0,
                            "MQClientException: pullThresholdSizeForQueue Out of range [1, 1024]",
                        ),
                        (
                            |c| c.pull_batch_size = 1025,
                            "MQClientException: pullBatchSize Out of range [1, 1024]",
                        ),
                        (
                            |c| c.pop_invisible_time = MIN_POP_INVISIBLE_TIME - 1,
                            "MQClientException: popInvisibleTime Out of range [5000, 300000]",
                        ),
                        (
                            |c| c.pop_batch_nums = 33,
                            "MQClientException: popBatchNums Out of range [1, 32]",
                        ),
                        (
                            |c| {
                                c.consume_thread_min = 8;
                                c.consume_thread_max = 4;
                            },
                            "MQClientException: consumeThreadMin (8) is larger than consumeThreadMax (4)",
                        ),
                    ];
                    for (break_it, want) in illegal {
                        let c6 = match fx.consumer(&bad_group, &s5_topic, Sink::new(Duration::ZERO))
                        {
                            Ok(v) => v,
                            Err(e) => {
                                ck.abort("S5 build rejected consumer", &e);
                                continue;
                            }
                        };
                        c6.update_config(break_it);
                        match c6.start().await {
                            Err(e) => ck.check(
                                "S5 out-of-range config is rejected",
                                e.to_string() == want,
                                &format!("want={want} actual={e}"),
                            ),
                            Ok(()) => {
                                c6.shutdown();
                                ck.abort("S5 out-of-range config is rejected", "start() succeeded");
                            }
                        }
                        ck.check(
                            "S5 a rejected start leaves nothing half-started",
                            !c6.is_started(),
                            "is_started() must be false after a rejected start",
                        );
                    }

                    // Counter-evidence from the broker side
                    match fx.admin.get_topic_route_data(&s5_topic).await {
                        None => ck.abort("S5 route of the boundary topic", "no route from namesrv"),
                        Some(route) => match broker_of(&route) {
                            Err(e) => ck.abort("S5 broker address", &e),
                            Ok(addr) => {
                                // A group that never registered successfully: the broker's GET_CONSUMER_LIST_BY_GROUP answers
                                // not an empty list but "no consumer for this group" (code=1). Both shapes count as
                                // "group not found", but **no** clientId may ever be included.
                                let bad = fx
                                    .admin
                                    .get_consumer_list_by_group(&bad_group, 5000, Some(&addr))
                                    .await;
                                let (absent, detail) = match bad {
                                    Ok(body) => (
                                        body.consumer_id_list.is_empty(),
                                        format!("ids={:?}", body.consumer_id_list),
                                    ),
                                    // Only an error the broker itself returns counts as "group not found". Connection failures/timeouts must
                                    // count as a failure, otherwise the counter-evidence degenerates into the empty claim "nothing was found".
                                    Err(e @ Error::Broker { .. }) => {
                                        (true, format!("broker refused: {e}"))
                                    }
                                    Err(e) => (false, format!("probe request failed: {e}")),
                                };
                                ck.check("S5 broker never heard of the rejected group", absent, &detail);
                                let ok_ids = fx
                                    .admin
                                    .get_consumer_list_by_group(&s5_group, 5000, Some(&addr))
                                    .await
                                    .map(|b| b.consumer_id_list)
                                    .unwrap_or_default();
                                ck.check(
                                    "S5 broker knows the boundary-value consumer",
                                    ok_ids.len() == 1,
                                    &format!("ids={ok_ids:?}"),
                                );
                            }
                        },
                    }
                }
                // The counter-evidence must happen before shutdown: an early return would tear down the broker-side registration
                c5.shutdown();
            }
        }
    }

    fx.shutdown();
    ck
}

fn report(ck: &mut Checker) {
    println!(
        "\n== result: {} PASS / {} FAIL ==",
        ck.passed,
        ck.failed.len()
    );
    for f in &ck.failed {
        println!("  FAILED: {f}");
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
    let mut ck = run(&namesrv).await;
    report(&mut ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
