//! Real-cluster verification of publish routing picking the master.
//!
//! Precondition: namesrv + master + slave are all running (per the local cluster runbook); the script itself **stops the master exactly once**
//! (`scripts/rmq_test_broker.sh` only recognises the master's java process), and the `Drop` guard guarantees the master always comes back.
//!
//!
//! The offline unit tests (the `publish_route_*` group in `src/client/mq_client.rs`) pin the **criteria**; the real cluster pins
//! what those criteria yield on a real route shape -- in the name server broker-a really ends up with only `{1: slave}`.
//!
//!
//! Usage:
//! ```text
//! cargo run --example live_publish_route_master -- 127.0.0.1:9876 127.0.0.1:10911 [127.0.0.1:10931]
//! ```

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::{ExitCode, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::{DefaultMQProducer, ProducerConfig};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently, SendStatus,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::error::{client_error_code, Error};
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

const QUEUES: i32 = 4;
const BROKER_NAME: &str = "broker-a";
/// Upper bound for the local failure: an empty publish info makes no wire call at all, and 500ms leaves an order-of-magnitude margin on a real cluster.
const LOCAL_BUDGET_MS: f64 = 500.0;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

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
}

/// Broker switch: `scripts/rmq_test_broker.sh` (**touches the master only**); shared infrastructure for the live cases.
fn broker_ctl(script: &PathBuf, action: &str) -> (bool, String) {
    let out_path = format!("/tmp/rmq_pr_master_rs_broker_ctl.{action}.log");
    let file = match fs::File::create(&out_path) {
        Ok(f) => f,
        Err(e) => return (false, format!("open {out_path} failed: {e}")),
    };
    let stdout = match file.try_clone() {
        Ok(f) => f,
        Err(e) => return (false, format!("dup {out_path} failed: {e}")),
    };
    // Output goes to a **file**, not a pipe: start turns the broker into a resident process that inherits the pipe's
    // write end, and whoever inherits it never sees EOF.
    let status = std::process::Command::new("sh")
        .arg(script)
        .arg(action)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(file))
        .status();
    match status {
        Ok(s) => {
            let text = fs::read_to_string(&out_path).unwrap_or_default();
            (s.success(), text.trim().to_string())
        }
        Err(e) => (false, format!("run {} {action} failed: {e}", script.display())),
    }
}

/// Finaliser guard: no matter where the case stops (including an early return / panic), the test cluster must never be
/// left in the "master down" state for the next case -- start is idempotent and returns directly if already running.
struct BrokerGuard {
    script: PathBuf,
}

impl Drop for BrokerGuard {
    fn drop(&mut self) {
        let (up, _) = broker_ctl(&self.script, "status");
        if up {
            return;
        }
        println!("  [cleanup] master was DOWN when the case ended, running a compensating start");
        let (ok, out) = broker_ctl(&self.script, "start");
        if !ok {
            println!("  [cleanup] master still not up: {out}");
        }
    }
}

/// Async condition wait: route/offset convergence inside the real-cluster window is asynchronous; a timeout is not an
/// assertion failure, the caller asserts on the return value (None means it never converged).
async fn wait_until<T, F, Fut>(mut f: F, secs: u64, what: &str) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(v) = f().await {
            return Some(v);
        }
        if Instant::now() >= deadline {
            println!("    (timed out waiting for {what})");
            return None;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn has_id(addrs: &[(i64, String)], id: i64) -> bool {
    addrs.iter().any(|(k, _)| *k == id)
}

/// Collects bodies only: this pass only cares whether "the 4 seeded messages can all be received off the slave", not offsets or order.
struct Sink {
    bodies: Mutex<Vec<Vec<u8>>>,
}

impl Sink {
    fn new() -> Arc<Sink> {
        Arc::new(Sink {
            bodies: Mutex::new(Vec::new()),
        })
    }

    fn bodies(&self) -> Vec<Vec<u8>> {
        lock(&self.bodies).clone()
    }
}

impl MessageListenerConcurrently for Sink {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let mut bodies = lock(&self.bodies);
        for m in msgs {
            bodies.push(m.get_body().to_vec());
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

struct Fixture {
    namesrv: String,
    master: String,
    slave: String,
    stamp: u64,
    topic: String,
    producer: DefaultMQProducer,
    admin: MQClientInstance,
    consumer: Mutex<Option<DefaultMQPushConsumer>>,
}

impl Fixture {
    async fn new(namesrv: &str, master: &str, slave: &str) -> Result<Fixture, String> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        // The admin instance and producer instance have different clientIds (different instance_name) ⇒ two separate
        // MQClientInstances in-process: S5's "stale cache" looks at the send instance's own table, so both must be drivable independently.
        let admin = MQClientInstance::new(
            &format!("pr_master_rs_admin_{stamp}"),
            vec![namesrv.to_string()],
        );
        admin
            .start()
            .await
            .map_err(|e| format!("admin start failed: {e}"))?;
        let producer = DefaultMQProducer::with_config(ProducerConfig {
            producer_group: format!("PID_PrMasterRs_{stamp}"),
            instance_name: format!("pr_master_rs_{stamp}"),
            name_server_addrs: vec![namesrv.to_string()],
            ..Default::default()
        })
        .map_err(|e| format!("producer build failed: {e}"))?;
        producer
            .start()
            .await
            .map_err(|e| format!("producer start failed: {e}"))?;
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            master: master.to_string(),
            slave: slave.to_string(),
            stamp,
            topic: format!("PrMasterRs{stamp}"),
            producer,
            admin,
            consumer: Mutex::new(None),
        })
    }

    /// Fetches broker-a's brokerAddrs after a forced refresh (None if it cannot be refreshed).
    async fn route_addrs(&self, topic: &str) -> Option<Vec<(i64, String)>> {
        if let Err(e) = self
            .admin
            .update_topic_route_info_from_name_server(topic, 5000, false)
            .await
        {
            println!("    (route refresh failed: {e})");
        }
        let route = self.admin.get_topic_route_data(topic).await?;
        route
            .get_broker_datas()
            .iter()
            .find(|bd| bd.broker_name == BROKER_NAME)
            .map(|bd| bd.broker_addrs.clone())
    }

    fn build_consumer(&self, group: &str, sink: Arc<Sink>) -> Result<DefaultMQPushConsumer, String> {
        let cfg = ConsumerConfig {
            consumer_group: group.to_string(),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("pr_master_rs_c_{}", self.stamp),
            consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
            ..Default::default()
        };
        let consumer =
            DefaultMQPushConsumer::with_config(cfg).map_err(|e| format!("build consumer: {e}"))?;
        consumer
            .subscribe(&self.topic, "*")
            .map_err(|e| format!("subscribe: {e}"))?;
        consumer.set_message_listener_concurrently(sink);
        Ok(consumer)
    }

    async fn seed_queues(&self) -> Result<Vec<MessageQueue>, Error> {
        let info = self.admin.get_topic_publish_info(&self.topic, false).await?;
        Ok(info.msg_queue_list())
    }

    /// Shuts the clients down + deletes the topic. Before deleting, make sure the master is up (`start` is idempotent) --
    /// on an early exit inside the stop window the topic must still be deletable; no orphan config may be left behind.
    async fn cleanup(&self, script: &PathBuf) {
        let _ = broker_ctl(script, "start");
        for addr in [&self.master, &self.slave] {
            if let Err(e) = self
                .admin
                .delete_topic_in_broker(addr, &self.topic, 5000)
                .await
            {
                println!("    (delete topic on {addr} failed: {e})");
            }
        }
        if let Err(e) = self.admin.delete_topic_in_namesrv(&self.topic, 5000).await {
            println!("    (delete topic in namesrv failed: {e})");
        }
        if let Some(c) = lock(&self.consumer).take() {
            c.shutdown();
        }
        self.producer.shutdown();
        self.admin.shutdown();
    }
}

fn fmt_bodies(bodies: &[Vec<u8>]) -> String {
    bodies
        .iter()
        .map(|b| String::from_utf8_lossy(b).to_string())
        .collect::<Vec<String>>()
        .join(",")
}

async fn scenario(ck: &mut Checker, fx: &Fixture, script: &std::path::Path) {
    let topic = fx.topic.clone();

    // ---------- S0 control leg ----------
    println!("S0 control leg (master up): route {{0: master, 1: slave}}, {QUEUES} publish/subscribe queues each");
    if let Err(e) = fx
        .admin
        .create_topic_in_route(&topic, QUEUES, QUEUES, DEFAULT_PERM, 0, None, 5000)
        .await
    {
        ck.check("S0 create topic (master)", false, &e.to_string());
        return;
    }
    // Also create it directly on the slave instead of betting on SlaveSynchronize's 5s cycle
    if let Err(e) = fx
        .admin
        .create_topic_in_broker(
            &fx.slave,
            MixAll::DEFAULT_TOPIC,
            &topic,
            QUEUES,
            QUEUES,
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
        println!("    (creating the topic on the slave failed: {e})");
    }

    let addrs = wait_until(
        || async {
            let addrs = fx.route_addrs(&topic).await?;
            if has_id(&addrs, 0) && has_id(&addrs, 1) {
                Some(addrs)
            } else {
                None
            }
        },
        30,
        "route {0, 1}",
    )
    .await;
    ck.check(
        "S0 the route contains {0: master, 1: slave}",
        addrs.is_some(),
        &format!("broker_addrs={addrs:?}"),
    );
    let Some(addrs) = addrs else {
        return;
    };
    let route_slave = addrs.iter().find(|(id, _)| *id == 1).map(|(_, a)| a.clone());
    ck.check(
        "S0 the slave address matches the argument",
        route_slave.as_deref() == Some(fx.slave.as_str()),
        &format!("route={route_slave:?} argv={}", fx.slave),
    );

    let queues = match fx.seed_queues().await {
        Ok(q) => q,
        Err(e) => {
            ck.check("S0 publish queues 4 (control)", false, &e.to_string());
            return;
        }
    };
    let mut queues = queues;
    queues.sort_by_key(|q| q.queue_id);
    ck.check(
        &format!("S0 publish queues {QUEUES} (control)"),
        queues.len() == QUEUES as usize,
        &format!(
            "queues={:?}",
            queues.iter().map(|q| (&q.broker_name, q.queue_id)).collect::<Vec<_>>()
        ),
    );
    let subs = fx.admin.get_topic_subscribe_info(&topic).await;
    ck.check(
        &format!("S0 subscribe queues {QUEUES} (control)"),
        subs.len() == QUEUES as usize,
        &format!(
            "queues={:?}",
            subs.iter().map(|q| (&q.broker_name, q.queue_id)).collect::<Vec<_>>()
        ),
    );
    if queues.len() != QUEUES as usize {
        return;
    }

    // ---------- S1 seed ----------
    println!("\nS1 seed {QUEUES} messages (one pinned per queue) and wait for the slave's store to catch up");
    let mut seeded: Vec<Vec<u8>> = Vec::new();
    for (i, mq) in queues.iter().enumerate() {
        let body = format!("pr-master-{i}").into_bytes();
        let mut msg = Message::new(&topic, Some(&body));
        match fx.producer.send(&mut msg, Some(20000), Some(mq)).await {
            Ok(r) if r.status == SendStatus::SendOk => seeded.push(body),
            Ok(r) => ck.check(
                &format!("S1 seed {i} SEND_OK"),
                false,
                &format!("status={:?}", r.status),
            ),
            Err(e) => ck.check(&format!("S1 seed {i} SEND_OK"), false, &e.to_string()),
        }
    }
    ck.check(
        &format!("S1 all {QUEUES} seeded sends are SEND_OK"),
        seeded.len() == QUEUES as usize,
        &format!("seeded={}", seeded.len()),
    );
    if seeded.len() != QUEUES as usize {
        return;
    }

    let mut master_max: Vec<(i32, i64)> = Vec::new();
    for mq in &queues {
        match fx.admin.get_max_offset(mq, 5000, None).await {
            Ok(o) => master_max.push((mq.queue_id, o)),
            Err(e) => println!("    (master maxOffset query failed: {e})"),
        }
    }
    let caught_up = wait_until(
        || async {
            if master_max.is_empty() {
                return None;
            }
            let mut slave_max: Vec<(i32, i64)> = Vec::new();
            for mq in &queues {
                match fx.admin.get_max_offset(mq, 5000, Some(&fx.slave)).await {
                    Ok(o) => slave_max.push((mq.queue_id, o)),
                    Err(e) => {
                        println!("    (fetching the slave's maxOffset failed: {e})");
                        return None;
                    }
                }
            }
            let ok = master_max
                .iter()
                .all(|(qid, m)| slave_max.iter().any(|(sq, s)| sq == qid && s >= m));
            if ok {
                Some(slave_max)
            } else {
                None
            }
        },
        30,
        "slave replication caught up",
    )
    .await;
    ck.check(
        &format!("S1 all {QUEUES} messages are replicated to the slave's store"),
        caught_up.is_some(),
        &format!("master={master_max:?} slave={:?}", caught_up.unwrap_or_default()),
    );

    // ---------- S2 stop master ----------
    println!("\nS2 stop the master (scripts/rmq_test_broker.sh stop), wait until the route has only the slave left");
    let script_for_stop = script.to_path_buf();
    let stopped = tokio::task::spawn_blocking(move || broker_ctl(&script_for_stop, "stop")).await;
    let (ok, out) = stopped.unwrap_or_else(|e| (false, format!("stop task panicked: {e}")));
    ck.check("S2 the master shut down gracefully", ok, &out);

    let down_addrs = wait_until(
        || async {
            let addrs = fx.route_addrs(&topic).await?;
            if !has_id(&addrs, 0) && has_id(&addrs, 1) {
                Some(addrs)
            } else {
                None
            }
        },
        60,
        "masterless route",
    )
    .await;
    ck.check(
        "S2 in the route broker-a has only {1: slave} left",
        down_addrs.is_some(),
        &format!("broker_addrs={down_addrs:?}"),
    );
    if down_addrs.is_none() {
        return;
    }

    // ---------- S3 (A) publish info yields no queues ----------
    println!("\nS3 (A) publish info skips brokers without a master");
    // Accessor contract: Ok only when the publish info **has queues**, otherwise this side raises 10005. It throws here,
    // equivalent to the `msg_queue_list == 0` case (and this also proves once more that "a freshly re-fetched route still
    // cannot form queues" -- the accessor refreshes the route first).
    let publish_down = fx.admin.get_topic_publish_info(&topic, false).await;
    let publish_err = match &publish_down {
        Ok(info) => format!("unexpectedly got {} queues", info.msg_queue_list().len()),
        Err(e) => e.to_string(),
    };
    ck.check(
        "S3 after stopping the master, publish queues == 0 (the accessor raises the local \"no queue selectable\" error)",
        publish_down
            .as_ref()
            .err()
            .map(|e| e.to_string().contains("Can not find Message Queue for topic"))
            .unwrap_or(false),
        &publish_err,
    );

    // ---------- S4 (C) subscribe queues ----------
    let subs_down = fx.admin.get_topic_subscribe_info(&topic).await;
    ck.check(
        &format!("S4 (C) subscribe queues still {QUEUES}, all on {BROKER_NAME} (the consume side does not look at the master)"),
        subs_down.len() == QUEUES as usize && subs_down.iter().all(|q| q.broker_name == BROKER_NAME),
        &format!(
            "queues={:?}",
            subs_down.iter().map(|q| (&q.broker_name, q.queue_id)).collect::<Vec<_>>()
        ),
    );

    // ---------- S5 (A fixed) send fails fast ----------
    // Two phases: while **the cache is not yet refreshed** (in reality the 30s periodic task has not fired) the send instance still holds
    // the pre-stop route, address resolution lands on the dead master, fails fast and never silently re-targets the slave; once **the route is
    // refreshed into the post-stop shape** (periodic task / explicit refresh), (A) kicks in: publish queues are empty and the send emits no wire call at all.
    println!("\nS5 sync send without a queue: stale cache fails fast → after refresh this side fails fast");
    let began = Instant::now();
    let mut msg = Message::new(&topic, Some(b"must-not-send"));
    let stale = fx.producer.send(&mut msg, Some(20000), None).await;
    let stale_ms = began.elapsed().as_secs_f64() * 1000.0;
    let stale_text = match &stale {
        Ok(r) => format!("Ok(status={:?})", r.status),
        Err(e) => e.to_string(),
    };
    // This leg is opportunistic: whether the periodic refresh already ran is not up to this script. Both legs share the
    // criterion "fails fast + never lands on the slave address" (the stale-cache leg aims at the dead master).
    ck.check(
        "S5 the send fails fast and the error never mentions the slave address (never re-targets the slave)",
        stale.is_err() && stale_ms < LOCAL_BUDGET_MS && !stale_text.contains(&fx.slave),
        &format!("{stale_ms:.0}ms {stale_text}"),
    );

    // Refresh the send instance's own route cache into the post-stop shape (the same code path as the 30s periodic task)
    let Some(pcli) = fx.producer.client() else {
        ck.check("S5b got the send instance", false, "the producer is not started");
        return;
    };
    if let Err(e) = pcli
        .update_topic_route_info_from_name_server(&topic, 5000, false)
        .await
    {
        println!("    (send instance route refresh failed: {e})");
    }
    let send_publish = pcli.get_topic_publish_info(&topic, false).await;
    ck.check(
        "S5b the send instance's publish queues are also == 0 ((A) acts exactly here)",
        send_publish
            .as_ref()
            .err()
            .map(|e| e.to_string().contains("Can not find Message Queue for topic"))
            .unwrap_or(false),
        &match &send_publish {
            Ok(info) => format!("unexpectedly got {} queues", info.msg_queue_list().len()),
            Err(e) => e.to_string(),
        },
    );

    let began = Instant::now();
    let mut msg2 = Message::new(&topic, Some(b"must-not-send-2"));
    let fresh = fx.producer.send(&mut msg2, Some(20000), None).await;
    let fresh_ms = began.elapsed().as_secs_f64() * 1000.0;
    let fresh_text = match &fresh {
        Ok(r) => format!("Ok(status={:?})", r.status),
        Err(e) => e.to_string(),
    };
    ck.check(
        "S5c after refresh: local side raises 10005 \"no queue selectable\", no wire call (no BrokersSent)",
        fresh
            .as_ref()
            .err()
            .map(|e| {
                e.response_code() == Some(client_error_code::NOT_FOUND_TOPIC_EXCEPTION)
                    && e.to_string().contains("Can not find Message Queue for topic")
                    && !e.to_string().contains("BrokersSent")
            })
            .unwrap_or(false)
            && fresh_ms < LOCAL_BUDGET_MS,
        &format!(
            "{fresh_ms:.0}ms code={:?}: {fresh_text}",
            fresh.as_ref().err().and_then(|e| e.response_code())
        ),
    );

    // ---------- S5d control: what happens to a pinned send ----------
    println!("\nS5d control: pinned send to that queue -> local side reports broker-not-exist, not a single wire call");
    let mq0 = MessageQueue::new(&topic, BROKER_NAME, 0);
    let mut msg3 = Message::new(&topic, Some(b"pinned-to-slave"));
    let began = Instant::now();
    let pinned = fx.producer.send(&mut msg3, Some(20000), Some(&mq0)).await;
    let pinned_ms = began.elapsed().as_secs_f64() * 1000.0;
    let pinned_text = match &pinned {
        Ok(r) => format!("Ok(status={:?})", r.status),
        Err(e) => e.to_string(),
    };
    ck.check(
        "S5d pinned send: local side reports \"The broker[broker-a] not exist\", no wire call (no BrokersSent)",
        pinned
            .as_ref()
            .err()
            .map(|e| {
                e.to_string().contains(&format!("The broker[{BROKER_NAME}] not exist"))
                    && e.response_code().is_none()
                    && !e.to_string().contains("BrokersSent")
                    && !e.to_string().contains(&fx.slave)
            })
            .unwrap_or(false)
            && pinned_ms < LOCAL_BUDGET_MS,
        &format!("{pinned_ms:.0}ms {pinned_text}"),
    );

    println!("\nS5e (D) orderly lock: whole broker skipped inside the stop window (no route refresh, no wire)");
    let group = format!("GID_PrMasterRs_{}", fx.stamp);
    let client_id = format!("pr_master_rs_lock_{}", fx.stamp);
    let lock_mqs = vec![MessageQueue::new(&topic, BROKER_NAME, 0)];
    let began = Instant::now();
    let locks = match fx.admin.lock_batch_mq(&group, &client_id, &lock_mqs, 3000).await {
        Ok(l) => l,
        Err(e) => {
            println!("    (lock_batch_mq failed: {e})");
            Vec::new()
        }
    };
    let lock_ms = began.elapsed().as_secs_f64() * 1000.0;
    ck.check(
        "S5e with only the slave left nothing can be locked (old behavior would fall back to locking on the slave)",
        locks.is_empty() && lock_ms < LOCAL_BUDGET_MS,
        &format!(
            "{lock_ms:.0}ms locked={:?}",
            locks.iter().map(|q| (&q.broker_name, q.queue_id)).collect::<Vec<_>>()
        ),
    );
    // Control leg: inside the same window the slave still serves this queue normally (the local
    // invoke_sync port is not exposed to examples, so the raw "lock on the named slave" request
    // cannot be produced; use the slave still serving the same queue to rule out the explanation
    // that the empty lock set is because the slave is unreachable).
    let slave_serving = fx
        .admin
        .get_max_offset(&lock_mqs[0], 5000, Some(&fx.slave))
        .await;
    ck.check(
        "S5e2 control: inside the window the slave still serves this queue (the empty set is not the slave being unreachable)",
        matches!(slave_serving, Ok(v) if v >= 1),
        &format!("slave maxOffset={slave_serving:?}"),
    );
    let began = Instant::now();
    let _ = fx.admin.unlock_batch_mq(&group, &client_id, &lock_mqs, 3000).await;
    let unlock_ms = began.elapsed().as_secs_f64() * 1000.0;
    ck.check(
        "S5e3 unlock is likewise quietly skipped (no throw, no send)",
        unlock_ms < LOCAL_BUDGET_MS,
        &format!("{unlock_ms:.0}ms"),
    );

    // ---------- S5f (E) subscribe semantics: POP only accepts the master ----------
    println!("\nS5f (E) POP pull: local side reports \"The broker[broker-a] not exist\" (no wire sent)");
    let began = Instant::now();
    let pop = fx
        .admin
        .pop_message(
            &group, &topic, 0, 1, 30000, 100, 0, None, None, false, None, 5000, None,
        )
        .await;
    let pop_ms = began.elapsed().as_secs_f64() * 1000.0;
    let pop_text = match &pop {
        Ok(r) => format!("Ok(status={:?})", r.status),
        Err(e) => e.to_string(),
    };
    ck.check(
        "S5f inside the stop window POP reports \"The broker[broker-a] not exist\" locally (not an error returned by the slave)",
        pop.as_ref()
            .err()
            .map(|e| {
                e.to_string().contains(&format!("The broker[{BROKER_NAME}] not exist"))
                    && e.response_code().is_none()
            })
            .unwrap_or(false)
            && pop_ms < LOCAL_BUDGET_MS,
        &format!("{pop_ms:.0}ms {pop_text}"),
    );

    println!("\nS5g (F) offset read: a cold instance relaxes to the slave after one route refresh");
    let cold = MQClientInstance::new(
        &format!("pr_master_rs_cold_{}", fx.stamp),
        vec![fx.namesrv.clone()],
    );
    let began = Instant::now();
    let off = cold
        .query_consumer_offset(&group, &lock_mqs[0], 5000, None, false)
        .await;
    let cold_ms = began.elapsed().as_secs_f64() * 1000.0;
    let cold_text = match &off {
        Ok(v) => format!("offset={v:?}"),
        Err(e) => e.to_string(),
    };
    ck.check(
        "S5g cold-instance offset read does not error: refresh route -> fall back to the slave, answered by the broker",
        off.is_ok(),
        &format!("{cold_ms:.0}ms {cold_text}"),
    );
    cold.shutdown();

    // ---------- S6 (C end-to-end) consume inside the stop window ----------
    println!("\nS6 push consumer started inside the stop window: {QUEUES} queues + collect the {QUEUES} seeded messages from the slave");
    let sink = Sink::new();
    let consumer = match fx.build_consumer(&group, sink.clone()) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S6 consumer", false, &e);
            return;
        }
    };
    let consumer_client = consumer.client();
    if let Err(e) = consumer.start().await {
        ck.check("S6 consumer start", false, &e.to_string());
        return;
    }
    // The client instance is only built at start: fetch it again **after** start.
    let consumer_client = consumer.client().or(consumer_client);
    *lock(&fx.consumer) = Some(consumer);
    match consumer_client {
        Some(ccli) => {
            let subs_in_window = ccli.get_topic_subscribe_info(&topic).await;
            ck.check(
                &format!("S6a the consumer's own subscribe info inside the window is also {QUEUES} queues"),
                subs_in_window.len() == QUEUES as usize,
                &format!(
                    "queues={:?}",
                    subs_in_window.iter().map(|q| (&q.broker_name, q.queue_id)).collect::<Vec<_>>()
                ),
            );
        }
        None => ck.check("S6a got the consumer's own instance", false, "consumer.client() is None"),
    }
    let mut expected = seeded.clone();
    expected.sort();
    let got_ok = wait_until(
        || async {
            let mut got = sink.bodies();
            got.sort();
            if got == expected {
                Some(got)
            } else {
                None
            }
        },
        60,
        "4 seeded messages",
    )
    .await;
    ck.check(
        &format!("S6 collected all {QUEUES} from the slave while the master is down"),
        got_ok.is_some(),
        &format!("got=[{}]", fmt_bodies(&sink.bodies())),
    );

    // ---------- S7 negative control (bring the master back first) ----------
    println!("\nS7 negative control: after the master is back, publish queues and sends recover");
    let script_for_start = script.to_path_buf();
    let started = tokio::task::spawn_blocking(move || broker_ctl(&script_for_start, "start")).await;
    let (ok, out) = started.unwrap_or_else(|e| (false, format!("start task panicked: {e}")));
    if !ok {
        ck.check("S7 master reset", false, &out);
    }

    let queues_back = wait_until(
        || async {
            match fx.admin.get_topic_publish_info(&topic, false).await {
                Ok(info) => {
                    let q = info.msg_queue_list();
                    if q.len() == QUEUES as usize {
                        Some(q)
                    } else {
                        None
                    }
                }
                Err(_) => None,
            }
        },
        60,
        "publish queues recovered",
    )
    .await;
    ck.check(
        &format!("S7 publish queues recovered {QUEUES}"),
        queues_back.is_some(),
        &format!(
            "queues={:?}",
            queues_back
                .as_ref()
                .map(|q| q.iter().map(|x| (&x.broker_name, x.queue_id)).collect::<Vec<_>>())
        ),
    );
    // Neither failed send left a message on the broker: every seeded queue's maxOffset is still 1
    let mut after: Vec<(i32, i64)> = Vec::new();
    for mq in queues_back.as_ref().unwrap_or(&queues).iter() {
        match fx.admin.get_max_offset(mq, 5000, None).await {
            Ok(o) => after.push((mq.queue_id, o)),
            Err(e) => println!("    (maxOffset query failed: {e})"),
        }
    }
    ck.check(
        "S7b the two failed sends left no message (maxOffset still 1)",
        !after.is_empty() && after.iter().all(|(_, v)| *v == 1),
        &format!("maxOffset={after:?}"),
    );
    let mut back = Message::new(&topic, Some(b"pr-master-back"));
    let back_res = fx.producer.send(&mut back, Some(20000), None).await;
    ck.check(
        "S7c send recovered SEND_OK",
        matches!(&back_res, Ok(r) if r.status == SendStatus::SendOk),
        &match &back_res {
            Ok(r) => format!("status={:?} msgId={:?}", r.status, r.msg_id),
            Err(e) => e.to_string(),
        },
    );
}

async fn run(namesrv: &str, master: &str, slave: &str) -> Checker {
    let mut ck = Checker::new();
    let root = env::var("RMQ_REPO_ROOT").unwrap_or_else(|_| "..".to_string());
    let broker_script = PathBuf::from(root).join("scripts/rmq_test_broker.sh");

    let (ok, out) = broker_ctl(&broker_script, "status");
    if !ok {
        println!("master is not running: start namesrv + master + slave first per the local cluster runbook ({out})");
        ck.check("precondition master UP", false, &out);
        return ck;
    }

    let fx = match Fixture::new(namesrv, master, slave).await {
        Ok(fx) => fx,
        Err(e) => {
            ck.check("fixture", false, &e);
            return ck;
        }
    };
    // Order: declare the guard first (dropped later), then run the scenario; any early return inside the scenario comes back here.
    let _guard = BrokerGuard {
        script: broker_script.clone(),
    };
    println!("topic={}", fx.topic);

    scenario(&mut ck, &fx, &broker_script).await;
    fx.cleanup(&broker_script).await;
    ck
}

fn report(ck: &Checker) {
    println!(
        "\n== result: {}/{} passed ==",
        ck.passed,
        ck.passed as usize + ck.failed.len()
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
    let master = argv
        .get(2)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:10911".to_string());
    let slave = argv
        .get(3)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:10931".to_string());
    let ck = run(&namesrv, &master, &slave).await;
    report(&ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
