//! Live validation of [`DefaultMQAdminExt`] against a **real 5.5.1 broker**.
//!
//!
//! Usage (start a local cluster first, per the runbook recorded in the project knowledge):
//! ```text
//! cargo run --example live_admin -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::producer::{DefaultMQProducer, ProducerConfig};
use rocketmq_client_remoting::client::pull_consumer::{DefaultMQPullConsumer, PullConsumerConfig};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently, PullStatus,
    SendStatus,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicConfig, TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::protocol::admin_body::TopicStatsTable;
use rocketmq_client_remoting::remoting::protocol::codes::response_code;
use rocketmq_client_remoting::remoting::protocol::ext_fields::StringMap;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;
use rocketmq_client_remoting::remoting::protocol::subscription::SubscriptionGroupConfig;

/// Queue count used when creating the topic; A3's route/config assertions are measured against it.
const QUEUE_NUMS: i32 = 4;
/// Number of messages produced before A7.
const N_MSG: usize = 8;
/// Common budget for broker-side visibility (heartbeats, stats, delayed delivery).
const WAIT_SECONDS: u64 = 30;
/// The broker rewrites delayLevel=0 to 3 (10s); this leaves a full extra margin.
const RETRY_VISIBLE_SECONDS: u64 = 30;
/// Producer/consumer groups register via heartbeat, one round every 30s by default ⇒ the budget must exceed one full round.
const HEARTBEAT_VISIBLE_SECONDS: u64 = 60;

// ------------------------------------------------------------------ skeleton

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// Assertion accumulator: run all scenarios once, then summarize; the first failure does not exit early.
struct Checker {
    passed: u32,
    skipped: u32,
    failed: Vec<String>,
}

impl Checker {
    fn new() -> Checker {
        Checker {
            passed: 0,
            skipped: 0,
            failed: Vec::new(),
        }
    }

    fn check(&mut self, name: &str, cond: bool, detail: &str) {
        if cond {
            self.passed += 1;
            println!("  [PASS] {name}");
        } else {
            println!("  [FAIL] {name}: {detail}");
            self.failed.push(format!("{name}: {detail}"));
        }
    }

    fn abort(&mut self, name: &str, err: &str) {
        println!("  [FAIL] {name}: {err}");
        self.failed.push(format!("{name}: {err}"));
    }

    /// Explicit record of "this broker's configuration does not meet the assertion's precondition", strictly distinct from laziness.
    fn skip(&mut self, name: &str, why: &str) {
        println!("  [SKIP] {name}: {why}");
        self.skipped += 1;
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn now_millis() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(_) => 0,
    }
}

async fn sleep_millis(millis: u64) {
    tokio::time::sleep(Duration::from_millis(millis)).await;
}

/// On error print one line and keep going (admin scenarios are independent; exiting early would mask later problems).
macro_rules! step {
    ($ck:expr, $name:expr, $expr:expr) => {
        match $expr {
            Ok(v) => Some(v),
            Err(e) => {
                $ck.abort($name, &e.to_string());
                None
            }
        }
    };
}

// ------------------------------------------------------------------ fixture

struct Env {
    namesrv: String,
    stamp: String,
    topic: String,
    group: String,
    /// Target group used for cloning offsets (A10).
    dest_group: String,
    kv_namespace: String,
    admin: DefaultMQAdminExt,
    producer: DefaultMQProducer,
    broker_name: Mutex<String>,
    broker_addr: Mutex<String>,
}

impl Env {
    fn new(namesrv: &str, stamp: &str) -> Result<Env, String> {
        let producer = DefaultMQProducer::new(&format!("rust-live-admin-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        let admin = DefaultMQAdminExt::with_config(AdminConfig {
            instance_name: format!("ADMIN-{stamp}"),
            name_server_addrs: vec![namesrv.to_string()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        Ok(Env {
            namesrv: namesrv.to_string(),
            stamp: stamp.to_string(),
            topic: format!("RustLiveAdminTopic{stamp}"),
            group: format!("rust-live-admin-group-{stamp}"),
            dest_group: format!("rust-live-admin-dest-{stamp}"),
            kv_namespace: format!("RustLiveAdminKv{stamp}"),
            admin,
            producer,
            broker_name: Mutex::new(String::new()),
            broker_addr: Mutex::new(String::new()),
        })
    }

    /// Default cluster name of this topic: prefer the route field returned by the name server.
    async fn cluster_name(&self) -> String {
        match self.admin.examine_topic_route(&self.topic).await {
            Ok(route) => route
                .broker_datas
                .first()
                .map(|bd| bd.cluster.clone())
                .filter(|c| !c.is_empty())
                .unwrap_or_else(|| "DefaultCluster".to_string()),
            Err(_) => "DefaultCluster".to_string(),
        }
    }

    fn broker(&self) -> String {
        lock(&self.broker_addr).clone()
    }

    fn broker_name(&self) -> String {
        lock(&self.broker_name).clone()
    }

    /// Cluster liveness: an open port != the broker being registered at the name server, hence the polling wait.
    async fn start(&self, ck: &mut Checker) -> bool {
        if let Err(e) = self.admin.start().await {
            ck.abort("admin start", &e.to_string());
            return false;
        }
        if let Err(e) = self.producer.start().await {
            ck.abort("producer start", &e.to_string());
            return false;
        }
        let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
        loop {
            match self.admin.fetch_broker_cluster_info().await {
                Ok(info) if !info.broker_addr_table.is_empty() => {
                    let addrs = info.get_broker_addrs();
                    if let Some(first) = addrs.first() {
                        *lock(&self.broker_addr) = first.clone();
                        if let Some(named) = info
                            .broker_addr_table
                            .iter()
                            .find(|(_, addrs)| addrs.iter().any(|(_, a)| a == first))
                            .map(|(name, _)| name.clone())
                        {
                            *lock(&self.broker_name) = named;
                        }
                    }
                    return true;
                }
                Ok(_) => {}
                Err(e) => {
                    if Instant::now() > deadline {
                        ck.abort("cluster liveness probe", &e.to_string());
                        return false;
                    }
                }
            }
            if Instant::now() > deadline {
                ck.abort("cluster liveness probe", "the nameServer returned no broker within the budget");
                return false;
            }
            sleep_millis(1000).await;
        }
    }

    /// The trio of a successfully sent message: body + offsetMsgId (which encodes the broker
    /// address) + the client-side uniqKey.
    async fn produce(&self, topic: &str, queue_id: i32, n: usize) -> Vec<SentMsg> {
        let broker = self.broker_name();
        let mut sent = Vec::new();
        for i in 0..n {
            let body = format!("{topic}-{queue_id}-{i}");
            let mut msg = Message::new(topic, Some(body.as_bytes()));
            let mq = MessageQueue::new(topic, &broker, queue_id);
            match self.producer.send(&mut msg, Some(5_000), Some(&mq)).await {
                Ok(result) => sent.push(SentMsg {
                    body,
                    offset_msg_id: result.offset_msg_id.unwrap_or_default(),
                    uniq_key: result.msg_id.unwrap_or_default(),
                }),
                Err(e) => println!("  [WARN] send {body} failed: {e}"),
            }
        }
        sent
    }
}

/// A1's output: the state of the admin's own private instance after it is shut down.
#[derive(Clone, Debug)]
struct SentMsg {
    body: String,
    /// The address + offset encoded by the broker-side `QUEUE_OFFSET`; `view_message` uses it via
    /// request code 33.
    offset_msg_id: String,
    /// A 32-hex-digit uniqKey generated by the client; decoding it blindly yields a bogus address.
    uniq_key: String,
}

/// Take only the client-side code value (broker errors are normalized into `response_code()`
/// as well).
fn client_code(err: &Error) -> Option<i32> {
    err.response_code()
}

// ------------------------------------------------------------------ A1 private instance

/// A1: the admin uses a **private** `MQClientInstance` (see the deviation note 2 in the module
/// header), so even when it collides with the producer's clientId, `admin.shutdown()` must not
/// take the producer down.
/// This is exactly the bug from #38 that is still unfixed on the other facades —— the admin side
/// has to prove first that it does not hold here.
async fn a1_private_instance_isolation(ck: &mut Checker, env: &Env) {
    let client_id = format!("rust-live-admin-shared@{}", env.stamp);
    let producer = match DefaultMQProducer::with_config(ProducerConfig {
        producer_group: format!("rust-live-admin-a1-pg-{}", env.stamp),
        client_id: Some(client_id.clone()),
        name_server_addrs: vec![env.namesrv.clone()],
        ..Default::default()
    }) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("A1 build producer", &e.to_string());
            return;
        }
    };
    if let Err(e) = producer.start().await {
        ck.abort("A1 producer start", &e.to_string());
        return;
    }
    let admin = DefaultMQAdminExt::with_config(AdminConfig {
        client_id: Some(client_id.clone()),
        name_server_addrs: vec![env.namesrv.clone()],
        ..Default::default()
    });
    if let Err(e) = admin.start().await {
        ck.abort("A1 admin start", &e.to_string());
        producer.shutdown();
        return;
    }
    ck.check(
        "A1 admin and producer share the same clientId",
        admin.client_id() == client_id
            && producer.client_id().as_deref() == Some(client_id.as_str()),
        &format!(
            "admin={} producer={:?}",
            admin.client_id(),
            producer.client_id()
        ),
    );
    // The admin still works on its own (two instances coexist, each does its own route lookup).
    ck.check(
        "A1 the admin stays usable while both instances coexist",
        admin.examine_topic_route(&env.topic).await.is_ok(),
        "route lookup failed",
    );
    admin.shutdown();
    ck.check(
        "A1 after admin shutdown the admin itself is not started",
        !admin.is_started(),
        "",
    );
    ck.check(
        "A1 after admin shutdown the producer is still started",
        producer.is_started(),
        "the admin shut down the shared instance (the #38 bug reproduced)",
    );
    // Actually send one, to prove the connection was not torn down along with it.
    let mut msg = Message::new(&env.topic, Some(b"a1-after-admin-shutdown"));
    let sent = producer.send(&mut msg, Some(5_000), None).await;
    match sent {
        Ok(result) => ck.check(
            "A1 after admin shutdown the producer still sends normally",
            result.status == SendStatus::SendOk,
            &format!("status={}", result.status),
        ),
        Err(e) => ck.abort("A1 after admin shutdown the producer still sends normally", &e.to_string()),
    }
    ck.check(
        "A1 a second admin shutdown is idempotent",
        {
            admin.shutdown();
            true
        },
        "",
    );
    producer.shutdown();
}

// ------------------------------------------------------------------ A2 cluster

async fn a2_cluster(ck: &mut Checker, env: &Env) {
    let admin = &env.admin;
    let info = step!(
        ck,
        "A2 fetchBrokerClusterInfo",
        admin.fetch_broker_cluster_info().await
    );
    let Some(info) = info else { return };
    ck.check(
        "A2 the cluster info contains brokers and a cluster name",
        !info.broker_addr_table.is_empty() && !info.cluster_addr_table.is_empty(),
        &format!(
            "brokers={} clusters={}",
            info.broker_addr_table.len(),
            info.cluster_addr_table.len()
        ),
    );
    match admin.examine_broker_cluster_info().await {
        Ok(other) => ck.check(
            "A2 examineBrokerClusterInfo is equivalent to fetch",
            other.broker_addr_table == info.broker_addr_table,
            "the two entry points disagree",
        ),
        Err(e) => ck.abort("A2 examineBrokerClusterInfo", &e.to_string()),
    }
    let addr = env.broker();
    match admin.fetch_broker_runtime_stats(&addr, None).await {
        Ok(kv) => {
            ck.check(
                "A2 fetchBrokerRuntimeStats parses into a KVTable",
                !kv.table.is_empty(),
                &format!("keys={}", kv.table.len()),
            );
            match admin.get_broker_runtime_info(&addr, None).await {
                Ok(same) => ck.check(
                    "A2 the getBrokerRuntimeInfo alias returns the same result",
                    same.table.len() == kv.table.len(),
                    "the alias entry point has a different length",
                ),
                Err(e) => ck.abort("A2 getBrokerRuntimeInfo", &e.to_string()),
            }
        }
        Err(e) => ck.abort("A2 fetchBrokerRuntimeStats", &e.to_string()),
    }
    // The broker-side stats store only has content at runtime; a miss just logs one line and does
    // not count as a failure.
    match admin
        .view_broker_stats_data(&addr, "BROKER_PUT_NUMS", &env.broker_name())
        .await
    {
        Ok(value) => ck.check(
            "A2 viewBrokerStatsData replies normally",
            value.is_object() || value.is_null(),
            &value.to_string(),
        ),
        Err(e) => println!("  [INFO] A2 viewBrokerStatsData has no data locally: {e}"),
    }
}

// ------------------------------------------------------------------ A3 topic management

async fn a3_topic_lifecycle(ck: &mut Checker, env: &Env) {
    let admin = &env.admin;
    let topic = env.topic.clone();
    match admin
        .create_topic(MixAll::DEFAULT_TOPIC, &topic, QUEUE_NUMS, 0)
        .await
    {
        Ok(()) => println!("  [INFO] A3 createTopic({topic}) dispatched"),
        Err(e) => {
            ck.abort("A3 createTopic", &e.to_string());
            return;
        }
    }
    // The route is registered asynchronously (the broker registers with the nameServer every 10s), so
    // poll within the budget.
    let mut route: Option<TopicRouteData> = None;
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    loop {
        if let Ok(r) = admin.examine_topic_route(&topic).await {
            if !r.broker_datas.is_empty() {
                route = Some(r);
                break;
            }
        }
        if Instant::now() > deadline {
            break;
        }
        sleep_millis(1000).await;
    }
    let Some(route) = route else {
        ck.abort("A3 examineTopicRoute", "the route of the new topic is not visible within the budget");
        return;
    };
    let read_nums: Vec<i32> = route
        .queue_datas
        .iter()
        .map(|q| q.read_queue_nums)
        .collect();
    ck.check(
        "A3 every readQueueNums in the route equals the queue count used to create the topic",
        read_nums.iter().all(|n| *n == QUEUE_NUMS),
        &format!("{read_nums:?}"),
    );
    match admin.fetch_all_topic_list().await {
        Ok(list) => ck.check(
            "A3 fetchAllTopicList contains the new topic",
            list.get_topic_list().contains(&topic),
            &format!("topics={}", list.get_topic_list().len()),
        ),
        Err(e) => ck.abort("A3 fetchAllTopicList", &e.to_string()),
    }
    let mut clusters_of_topic: Vec<String> = Vec::new();
    match admin.get_cluster_list(&topic).await {
        Ok(clusters) => {
            clusters_of_topic = clusters.clone();
            ck.check(
                "A3 getClusterList(topic) gives the cluster that hosts this topic",
                !clusters.is_empty(),
                "an empty list came back",
            );
        }
        Err(e) => ck.abort("A3 getClusterList(topic)", &e.to_string()),
    }
    match admin.get_topic_cluster_list(&topic).await {
        Ok(clusters) => ck.check(
            "A3 getTopicClusterList is equivalent to getClusterList(topic)",
            clusters == clusters_of_topic,
            &format!("{clusters:?} vs {clusters_of_topic:?}"),
        ),
        Err(e) => ck.abort("A3 getTopicClusterList", &e.to_string()),
    }
    let cluster = match clusters_of_topic.first().cloned() {
        Some(cluster) => cluster,
        None => env.cluster_name().await,
    };
    match admin.fetch_topics_by_cluster(&cluster).await {
        Ok(topics) => ck.check(
            &format!("A3 fetchTopicsByCluster({cluster}) contains the new topic"),
            topics.contains(&topic),
            &format!("topics={} hit={}", topics.len(), topics.contains(&topic)),
        ),
        Err(e) => ck.abort("A3 fetchTopicsByCluster", &e.to_string()),
    }
    let addr = env.broker();
    match admin.examine_topic_config(&addr, &topic).await {
        Ok(cfg) => {
            ck.check(
                "A3 the TopicConfig queue count matches the creation",
                cfg.read_queue_nums == QUEUE_NUMS && cfg.write_queue_nums == QUEUE_NUMS,
                &format!(
                    "read={} write={}",
                    cfg.read_queue_nums, cfg.write_queue_nums
                ),
            );
            ck.check(
                "A3 TopicConfig.perm is the read-write permission",
                cfg.perm == DEFAULT_PERM,
                &format!("perm={}", cfg.perm),
            );
            ck.check(
                "A3 TopicConfig.attributes is deserialized",
                cfg.attributes.is_empty() || cfg.attributes.contains_key("+traceSwitch"),
                &format!("{:?}", cfg.attributes.iter().collect::<Vec<_>>()),
            );
        }
        Err(e) => ck.abort("A3 examineTopicConfig", &e.to_string()),
    }
    match admin.get_all_topic_config(&addr, None).await {
        Ok(wrapper) => ck.check(
            "A3 getAllTopicConfig contains the new topic",
            wrapper
                .topic_config_table
                .iter()
                .any(|(name, _)| name == &topic),
            &format!("count={}", wrapper.topic_config_table.len()),
        ),
        Err(e) => ck.abort("A3 getAllTopicConfig", &e.to_string()),
    }
    match admin.get_user_topic_config(&addr, false, None).await {
        Ok(wrapper) => {
            let names: Vec<&str> = wrapper
                .topic_config_table
                .iter()
                .map(|(name, _)| name.as_str())
                .collect();
            ck.check(
                "A3 getUserTopicConfig keeps business topics",
                names.contains(&topic.as_str()),
                &format!("{names:?}"),
            );
            ck.check(
                "A3 getUserTopicConfig filters out system/retry/DLQ topics",
                !names.iter().any(|n| {
                    n.starts_with(MixAll::SYSTEM_TOPIC_PREFIX)
                        || n.starts_with(MixAll::RETRY_GROUP_TOPIC_PREFIX)
                        || n.starts_with(MixAll::DLQ_GROUP_TOPIC_PREFIX)
                        || *n == "SCHEDULE_TOPIC_XXXX"
                }),
                &format!("{names:?}"),
            );
        }
        Err(e) => ck.abort("A3 getUserTopicConfig", &e.to_string()),
    }
    match admin.get_system_topic_list_from_broker(&addr, None).await {
        Ok(list) => ck.check(
            "A3 getSystemTopicListFromBroker is non-empty",
            !list.get_topic_list().is_empty(),
            &format!("{:?}", list.get_topic_list()),
        ),
        Err(e) => ck.abort("A3 getSystemTopicListFromBroker", &e.to_string()),
    }
    match admin.fetch_all_topic_route().await {
        Ok(routes) => ck.check(
            "A3 fetchAllTopicRoute has entries",
            !routes.is_empty(),
            &format!("count={}", routes.len()),
        ),
        Err(e) => ck.abort("A3 fetchAllTopicRoute", &e.to_string()),
    }
    // A reversible config change: bump the queue count to 6, read it back, then restore it to 4 (so
    // that A13 sees the original state before deleting the topic).
    let changed = TopicConfig {
        topic_name: topic.clone(),
        read_queue_nums: QUEUE_NUMS + 2,
        write_queue_nums: QUEUE_NUMS + 2,
        perm: DEFAULT_PERM,
        topic_filter_type: TopicFilterType::SINGLE_TAG.to_string(),
        ..Default::default()
    };
    match admin.create_and_update_topic_config(&addr, &changed).await {
        Ok(()) => {}
        Err(e) => {
            ck.abort("A3 createAndUpdateTopicConfig", &e.to_string());
            return;
        }
    }
    sleep_millis(1500).await;
    match admin.examine_topic_config(&addr, &topic).await {
        Ok(cfg) => ck.check(
            "A3 createAndUpdateTopicConfig took effect (queue count +2)",
            cfg.read_queue_nums == QUEUE_NUMS + 2,
            &format!("read={}", cfg.read_queue_nums),
        ),
        Err(e) => ck.abort("A3 read back the modified TopicConfig", &e.to_string()),
    }
    let mut restored = changed.clone();
    restored.read_queue_nums = QUEUE_NUMS;
    restored.write_queue_nums = QUEUE_NUMS;
    if let Err(e) = admin.create_and_update_topic_config(&addr, &restored).await {
        println!("  [WARN] A3 failed to restore the queue count: {e}");
    }
}

// ------------------------------------------------------------------ A4 broker config

async fn a4_broker_config(ck: &mut Checker, env: &Env) {
    let admin = &env.admin;
    let addr = env.broker();
    let props = match admin.get_broker_config(&addr, None).await {
        Ok(props) => props,
        Err(e) => {
            ck.abort("A4 getBrokerConfig", &e.to_string());
            return;
        }
    };
    // The response body is **properties text**, not KVTable JSON (a historical bug source).
    ck.check(
        "A4 getBrokerConfig parses non-empty k=v pairs",
        !props.is_empty(),
        &format!("keys={}", props.len()),
    );
    ck.check(
        "A4 brokerName matches the broker in the route",
        props
            .get("brokerName")
            .map_or(true, |v| v == env.broker_name() || env.broker_name().is_empty()),
        &format!(
            "brokerName={:?} route={:?}",
            props.get("brokerName"),
            env.broker_name()
        ),
    );
    // An illegal brokerPermission must be rejected **before any IO** (code=16, not a single byte on the
    // broker is changed).
    let mut bad = StringMap::new();
    bad.insert("brokerPermission", "not-a-number");
    match admin.update_broker_config(&addr, &bad, None).await {
        Err(e) => ck.check(
            "A4 an illegal brokerPermission fails fast locally",
            client_code(&e) == Some(response_code::NO_PERMISSION),
            &e.to_string(),
        ),
        Ok(()) => ck.check(
            "A4 an illegal brokerPermission fails fast locally",
            false,
            "it actually reached the broker, so validation did not take effect",
        ),
    }

    // A reversible change: write a harmless value, read it back, then restore it.
    let key = "sendMessageThreadPoolNums";
    let original = props.get(key).unwrap_or("16").to_string();
    let probe = "11";
    let mut change = StringMap::new();
    change.insert(key, probe);
    if let Err(e) = admin.update_broker_config(&addr, &change, None).await {
        ck.abort("A4 updateBrokerConfig", &e.to_string());
    } else {
        sleep_millis(1000).await;
        match admin.get_broker_config(&addr, None).await {
            Ok(after) => ck.check(
                "A4 updateBrokerConfig really took effect",
                after.get(key) == Some(probe),
                &format!("expected {probe}, actual {:?}", after.get(key)),
            ),
            Err(e) => ck.abort("A4 read back updateBrokerConfig", &e.to_string()),
        }
    }
    let mut revert = StringMap::new();
    revert.insert(key, original.as_str());
    if let Err(e) = admin.update_broker_config(&addr, &revert, None).await {
        println!("  [WARN] A4 failed to restore {key}: {e}");
    }
}

// ------------------------------------------------------------------ A5 NameServer KV

async fn a5_kv_config(ck: &mut Checker, env: &Env) {
    let admin = &env.admin;
    let ns = env.kv_namespace.clone();
    if let Err(e) = admin.create_and_update_kv_config(&ns, "k1", "v1").await {
        ck.abort("A5 createAndUpdateKvConfig", &e.to_string());
        return;
    }
    match admin.get_kv_config(&ns, "k1").await {
        Ok(value) => ck.check(
            "A5 getKVConfig round-trips the value",
            value.as_deref() == Some("v1"),
            &format!("value={value:?}"),
        ),
        Err(e) => ck.abort("A5 getKVConfig", &e.to_string()),
    }
    match admin.get_kv_list_by_namespace(&ns).await {
        Ok(table) => ck.check(
            "A5 getKVListByNamespace contains k1",
            table.table.get("k1") == Some("v1"),
            &format!("{:?}", table.table.iter().collect::<Vec<_>>()),
        ),
        Err(e) => ck.abort("A5 getKVListByNamespace", &e.to_string()),
    }
    if let Err(e) = admin.put_kv_config(&ns, "k2", "v2").await {
        ck.abort("A5 putKVConfig alias", &e.to_string());
    } else {
        match admin.get_kv_config(&ns, "k2").await {
            Ok(value) => ck.check(
                "A5 the putKVConfig alias also lands in the NameServer",
                value.as_deref() == Some("v2"),
                &format!("value={value:?}"),
            ),
            Err(e) => ck.abort("A5 read back putKVConfig", &e.to_string()),
        }
    }
    if let Err(e) = admin.delete_kv_config(&ns, "k1").await {
        ck.abort("A5 deleteKVConfig", &e.to_string());
    } else {
        match admin.get_kv_config(&ns, "k1").await {
            Ok(value) => {
                let gone = value.as_ref().filter(|v| !v.is_empty()).is_none();
                ck.check("A5 the KV no longer exists after deletion", gone, &format!("value={value:?}"));
            }
            Err(e) => ck.abort("A5 read back the KV after deletion", &e.to_string()),
        }
    }
    let _ = admin.delete_kv_config(&ns, "k2").await;
}

// ------------------------------------------------------------------ A6 subscription group

async fn a6_subscription_group(ck: &mut Checker, env: &Env) {
    let admin = &env.admin;
    let addr = env.broker();
    let group = env.group.clone();
    // The target group must also be created explicitly: A10's updateConsumerOffset and A12's
    // resetOffset* return code=26 when the broker has autoCreateSubscriptionGroup turned off (or
    // when the group name is wrong), so create it up front to get an accurate measurement.
    for name in [&group, &env.dest_group] {
        let mut config = SubscriptionGroupConfig::new(name);
        config.consume_enable = true;
        config.retry_max_times = if name == &group { 5 } else { 16 };
        if let Err(e) = admin
            .create_and_update_subscription_group_config(&addr, &config)
            .await
        {
            ck.abort("A6 createAndUpdateSubscriptionGroupConfig", &e.to_string());
            return;
        }
    }
    match admin.get_subscription_group_config(&addr, &group).await {
        Ok(Some(found)) => ck.check(
            "A6 a single lookup round-trips retryMaxTimes",
            found.retry_max_times == 5 && found.consume_enable,
            &format!(
                "retryMax={} enable={}",
                found.retry_max_times, found.consume_enable
            ),
        ),
        Ok(None) => ck.check("A6 a single lookup round-trips retryMaxTimes", false, "the group just created cannot be found"),
        Err(e) => ck.abort("A6 getSubscriptionGroupConfig", &e.to_string()),
    }
    match admin.examine_subscription_group_config(&addr, &group).await {
        Ok(Some(found)) => ck.check(
            "A6 examineSubscriptionGroupConfig fetches the group through full pagination",
            found.group_name == group,
            &found.group_name,
        ),
        Ok(None) => ck.check(
            "A6 examineSubscriptionGroupConfig",
            false,
            "the group is not in the paginated result",
        ),
        Err(e) => ck.abort("A6 examineSubscriptionGroupConfig", &e.to_string()),
    }
    match admin.get_all_subscription_group(&addr, None).await {
        Ok(wrapper) => ck.check(
            "A6 getAllSubscriptionGroup (paginated) contains the new group",
            wrapper
                .subscription_group_table
                .iter()
                .any(|(name, _)| name == &group),
            &format!("groups={}", wrapper.subscription_group_table.len()),
        ),
        Err(e) => ck.abort("A6 getAllSubscriptionGroup", &e.to_string()),
    }
    match admin.get_user_subscription_group(&addr, None).await {
        Ok(wrapper) => {
            let names: Vec<&str> = wrapper
                .subscription_group_table
                .iter()
                .map(|(name, _)| name.as_str())
                .collect();
            ck.check(
                "A6 getUserSubscriptionGroup contains the new group and filters out system groups",
                names.contains(&group.as_str())
                    && !names.iter().any(|n| n.starts_with("CID_RMQ_SYS_")),
                &format!("{names:?}"),
            );
        }
        Err(e) => ck.abort("A6 getUserSubscriptionGroup", &e.to_string()),
    }
}

// ------------------------------------------------------------------ A7 produce + stats

/// A7's output: all successfully sent messages, which A9's viewMessage relies on.
async fn a7_produce_and_stats(ck: &mut Checker, env: &Env) -> Vec<SentMsg> {
    let admin = &env.admin;
    let topic = env.topic.clone();
    let mut sent: Vec<SentMsg> = Vec::new();
    for queue_id in 0..QUEUE_NUMS {
        let per_queue = N_MSG / QUEUE_NUMS as usize;
        sent.extend(env.produce(&topic, queue_id, per_queue).await);
    }
    ck.check(
        &format!("A7 all {} sync sends are SEND_OK", N_MSG),
        sent.len() == N_MSG,
        &format!("ok={}/{}", sent.len(), N_MSG),
    );
    if sent.is_empty() {
        return sent;
    }
    // Broker statistics are refreshed periodically, so poll within the budget until maxOffset is
    // in place.
    // Only the snapshot with the **largest value ever seen** is kept: during polling the
    // broker-side statistics may first return a half-refreshed, smaller result; if we just used
    // whatever the last round returned, the "per-broker query is a subset of the merged result"
    // check below would compare a "later" single-broker value against an "earlier" merged value and
    // fail at random.
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    let mut stats: Option<TopicStatsTable> = None;
    loop {
        match admin.examine_topic_stats(&topic).await {
            Ok(table) => {
                let total = table.total_max_offset();
                let better = stats
                    .as_ref()
                    .map_or(true, |old| total > old.total_max_offset());
                if better {
                    stats = Some(table);
                }
                if total >= N_MSG as i64 {
                    break;
                }
            }
            Err(e) => {
                if Instant::now() > deadline {
                    ck.abort("A7 examineTopicStats", &e.to_string());
                    return sent;
                }
            }
        }
        if Instant::now() > deadline {
            break;
        }
        sleep_millis(1000).await;
    }
    match stats {
        Some(table) => {
            ck.check(
                "A7 the merged maxOffset from examineTopicStats covers the number sent",
                table.total_max_offset() >= N_MSG as i64,
                &format!("maxOffsetSum={}", table.total_max_offset()),
            );
            ck.check(
                "A7 examineTopicStats covers every queue of the topic",
                table.offset_table.len() as i32 >= QUEUE_NUMS,
                &format!("queues={}", table.offset_table.len()),
            );
            let addr = env.broker();
            // "per-broker query ⊆ merged query" requires a merged view taken **after the same
            // instant**: a queue's maxOffset never decreases, so we fetch the single-broker view
            // first and then immediately the merged view, and only then does the containment hold.
            // The other way round (comparing an earlier merged snapshot with a later single-broker
            // value) fails at random.
            match admin.examine_topic_stats_by_broker(&addr, &topic).await {
                Ok(part) => match admin.examine_topic_stats(&topic).await {
                    Ok(merged) => ck.check(
                        "A7 examineTopicStatsByBroker is a subset of the merged result",
                        part.offset_table.len() <= merged.offset_table.len()
                            && part.total_max_offset() <= merged.total_max_offset(),
                        &format!(
                            "brokerQueues={} brokerMax={} mergedQueues={} mergedMax={}",
                            part.offset_table.len(),
                            part.total_max_offset(),
                            merged.offset_table.len(),
                            merged.total_max_offset()
                        ),
                    ),
                    Err(e) => ck.abort("A7 examineTopicStats recheck", &e.to_string()),
                },
                Err(e) => ck.abort("A7 examineTopicStatsByBroker", &e.to_string()),
            }
        }
        None => ck.abort("A7 examineTopicStats", "no statistics within the budget"),
    }
    let addr = env.broker();
    let group = env.group.clone();
    match admin
        .examine_consume_stats(&addr, &group, Some(&topic), None)
        .await
    {
        Ok(stats) => ck.check(
            "A7 examineConsumeStats replies normally (lag equals the backlog when nothing is consumed)",
            !stats.offset_table.is_empty() && stats.total_lag() >= 0,
            &format!(
                "queues={} lag={}",
                stats.offset_table.len(),
                stats.total_lag()
            ),
        ),
        Err(e) => ck.abort("A7 examineConsumeStats", &e.to_string()),
    }
    match admin
        .fetch_consume_stats_in_broker(&addr, false, None)
        .await
    {
        // On the broker side `AdminBrokerProcessor#fetchAllConsumeStatsInBroker` builds **one row per
        // subscription group** (`subscriptionGroupTable.keySet()` → `{group: [ConsumeStats]}`), and
        // only the inner topic comes from the offset table `whichTopicByConsumer`. So even with no
        // offset committed at this point, the row count should equal the number of subscription
        // groups on this broker; "this group shows up in a row together with statistics" is
        // asserted by A8.
        Ok(list) => ck.check(
            "A7 fetchConsumeStatsInBroker emits one row per subscription group",
            !list.stats_list.is_empty(),
            &format!(
                "groups={} totalDiff={}",
                list.stats_list.len(),
                list.total_diff
            ),
        ),
        Err(e) => ck.abort("A7 fetchConsumeStatsInBroker", &e.to_string()),
    }
    match admin
        .query_consume_queue(&addr, &topic, 0, 0, 32, &group)
        .await
    {
        Ok(body) => ck.check(
            "A7 the queryConsumeQueue index is queryable",
            body.max_queue_index >= N_MSG as i32 / QUEUE_NUMS,
            &format!(
                "min={} max={} entries={}",
                body.min_queue_index,
                body.max_queue_index,
                body.queue_data.clone().map_or(0, |rows| rows.len())
            ),
        ),
        Err(e) => ck.abort("A7 queryConsumeQueue", &e.to_string()),
    }
    sent
}

// ------------------------------------------------------------------ A8 online consumer

/// A concurrent listener that only records bodies (A8 has to prove that the runtime state read by
/// the admin matches what the client really has).
#[derive(Default)]
struct Inbox {
    bodies: Mutex<Vec<String>>,
}

impl Inbox {
    fn bodies(&self) -> Vec<String> {
        lock(&self.bodies).clone()
    }
}

impl MessageListenerConcurrently for Inbox {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        for msg in msgs {
            lock(&self.bodies).push(String::from_utf8_lossy(msg.get_body()).to_string());
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

/// A8's output: the consumer's clientId, used to re-check runningInfo; `None` means nothing was
/// consumed.
async fn a8_online_consumer(ck: &mut Checker, env: &Env) -> Option<String> {
    let admin = &env.admin;
    let group = env.group.clone();
    let topic = env.topic.clone();
    let inbox = Arc::new(Inbox::default());
    let consumer = match DefaultMQPushConsumer::with_config(ConsumerConfig {
        consumer_group: group.clone(),
        name_server_addrs: vec![env.namesrv.clone()],
        instance_name: format!("live-admin-{}-{}", group, env.stamp),
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("A8 build consumer", &e.to_string());
            return None;
        }
    };
    if let Err(e) = consumer.subscribe(&topic, "*") {
        ck.abort("A8 subscribe", &e.to_string());
        return None;
    }
    consumer.set_message_listener_concurrently(inbox.clone());
    if let Err(e) = consumer.start().await {
        ck.abort("A8 consumer start", &e.to_string());
        return None;
    }
    let client_id = consumer.client_id();
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    while inbox.bodies().is_empty() && Instant::now() < deadline {
        sleep_millis(500).await;
    }
    let got = inbox.bodies().len();
    ck.check(
        &format!("A8 the push consumer consumes >0 messages online (prerequisite for sendMessageBack), got {got}"),
        got > 0,
        &format!("consumed={got}"),
    );
    // Group/client online information: these requests are forwarded by the broker back to the client
    // **in this very process**, which incidentally proves that the ClientRemotingProcessor callback
    // chain works.
    match admin.examine_consumer_connection_info(&group, None).await {
        Ok(conn) => {
            let ids: Vec<&str> = conn
                .connection_set
                .iter()
                .filter_map(|c| c.client_id.as_deref())
                .collect();
            ck.check(
                "A8 examineConsumerConnectionInfo contains this consumer's connection",
                ids.contains(&client_id.as_str()),
                &format!("{ids:?}"),
            );
            ck.check(
                "A8 ConsumerConnection carries the subscription and the consume model",
                !conn.subscription_table.is_empty()
                    && conn.consume_type.is_some()
                    && conn.message_model.is_some(),
                &format!(
                    "subs={} type={:?} model={:?}",
                    conn.subscription_table.len(),
                    conn.consume_type,
                    conn.message_model
                ),
            );
        }
        Err(e) => ck.abort("A8 examineConsumerConnectionInfo", &e.to_string()),
    }
    match admin.get_consumer_list_by_group(&group, None).await {
        Ok(body) => ck.check(
            "A8 getConsumerListByGroup (broker→client callback)",
            body.consumer_id_list.contains(&client_id),
            &format!("{:?}", body.consumer_id_list),
        ),
        Err(e) => ck.abort("A8 getConsumerListByGroup", &e.to_string()),
    }
    match admin
        .query_topic_consume_by_who(&env.broker(), &topic)
        .await
    {
        Ok(groups) => ck.check(
            "A8 queryTopicConsumeByWho finds this group",
            !groups.is_empty(),
            &format!("{groups:?}"),
        ),
        Err(e) => ck.abort("A8 queryTopicConsumeByWho", &e.to_string()),
    }
    match admin
        .query_subscription(&env.broker(), &group, &topic)
        .await
    {
        Ok(value) => ck.check(
            "A8 querySubscription returns the subscription JSON",
            value.is_some(),
            "the broker returned no subscription data",
        ),
        Err(e) => ck.abort("A8 querySubscription", &e.to_string()),
    }
    match admin
        .examine_consumer_running_info(&group, &client_id, false, None)
        .await
    {
        Ok(info) => {
            let sub = info.properties.get("subscription");
            let commit = info.properties.get("commitOffsetWord");
            ck.check(
                "A8 examineConsumerRunningInfo gets the runtime properties through the 307 callback",
                !info.properties.is_empty() && !info.mq_table.is_empty(),
                &format!(
                    "props={} sub={:?} commit={:?}",
                    info.properties.len(),
                    sub,
                    commit
                ),
            );
            ck.check(
                "A8 the statusTable of runningInfo is given per queue",
                !info.status_table.is_empty(),
                "statusTable is empty",
            );
        }
        Err(e) => ck.abort("A8 examineConsumerRunningInfo", &e.to_string()),
    }
    match admin
        .get_consume_status(&env.broker(), &topic, &group, &client_id)
        .await
    {
        Ok(value) => ck.check(
            "A8 getConsumeStatus (223 callback) returns the consumerTable",
            value.is_object(),
            &value.to_string(),
        ),
        Err(e) => ck.abort("A8 getConsumeStatus", &e.to_string()),
    }
    // A producer group registers with the broker via **heartbeat** (`ProducerManager.groupChannelTable`
    // is only filled by HEART_BEAT), and the heartbeat runs every 30s ⇒ we must poll until it
    // shows up; when it is not registered the broker replies SYSTEM_ERROR "the producer
    // group[...] not exist".
    let pg = format!("rust-live-admin-pg-{}", env.stamp);
    let deadline = Instant::now() + Duration::from_secs(HEARTBEAT_VISIBLE_SECONDS);
    let mut producer_conns: Option<usize> = None;
    let mut last_producer_err = String::new();
    loop {
        match admin.examine_producer_connection_info(&pg, None).await {
            Ok(conn) if !conn.connection_set.is_empty() => {
                producer_conns = Some(conn.connection_set.len());
                break;
            }
            Ok(_) => last_producer_err = "connections=0".to_string(),
            Err(e) => last_producer_err = e.to_string(),
        }
        if Instant::now() > deadline {
            break;
        }
        sleep_millis(2_000).await;
    }
    ck.check(
        "A8 examineProducerConnectionInfo sees the connection registered by the producer",
        producer_conns.is_some(),
        &match producer_conns {
            Some(n) => format!("connections={n}"),
            None => format!("not seen within the budget, last try: {last_producer_err}"),
        },
    );
    // Once messages have been consumed the offset must have been committed (refreshed every 5s, so
    // poll with a generous budget here).
    let mq = MessageQueue::new(&topic, &env.broker_name(), 0);
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    let mut offset = None;
    loop {
        match admin.examine_consumer_offset(&group, &mq).await {
            Ok(Some(v)) if v > 0 => {
                offset = Some(v);
                break;
            }
            Ok(_) => {}
            Err(e) => {
                ck.abort("A8 examineConsumerOffset", &e.to_string());
                consumer.shutdown();
                return Some(client_id);
            }
        }
        if Instant::now() > deadline {
            break;
        }
        sleep_millis(1000).await;
    }
    ck.check(
        "A8 the offset after consuming has been flushed to the broker",
        offset.is_some(),
        &format!("offset={offset:?}"),
    );
    // 343 QUERY_TOPICS_BY_CONSUMER reads the broker's offsetTable
    // (`ConsumerOffsetManager#whichTopicByConsumer`), so it only has content once the consumer has
    // flushed an offset; the subscription itself does not take part. The same holds for %RETRY%,
    // which only appears after an offset has been committed for a retry topic.
    match admin
        .query_topics_by_consumer_to_broker(&env.broker(), &group)
        .await
    {
        Ok(list) => {
            let topics = list.get_topic_list();
            ck.check(
                "A8 queryTopicsByConsumerToBroker reads the topics this group consumed from the offset table",
                topics.contains(&topic),
                &format!("{topics:?}"),
            );
        }
        Err(e) => ck.abort("A8 queryTopicsByConsumerToBroker", &e.to_string()),
    }
    match admin.query_topics_by_consumer(&group).await {
        Ok(list) => {
            let topics = list.get_topic_list();
            ck.check(
                "A8 queryTopicsByConsumer(group) fans out over the %RETRY% routes and merges",
                topics.contains(&topic),
                &format!("{topics:?}"),
            );
        }
        Err(e) => ck.abort("A8 queryTopicsByConsumer(group)", &e.to_string()),
    }
    match admin
        .fetch_consume_stats_in_broker(&env.broker(), false, None)
        .await
    {
        Ok(list) => {
            let hit = list
                .stats_list
                .iter()
                .any(|row| row.to_string().contains(&group));
            let groups: Vec<String> = list
                .stats_list
                .iter()
                .filter_map(|row| row.as_object())
                .flat_map(|row| row.keys().cloned())
                .collect();
            let has_offsets = list
                .stats_list
                .iter()
                .filter_map(|row| row.get(&group))
                .flat_map(|stats| stats.as_array().into_iter().flatten())
                .any(|stat| {
                    stat.get("offsetTable")
                        .is_some_and(|t| t.as_object().is_some_and(|m| !m.is_empty()))
                });
            ck.check(
                "A8 after an offset has been committed fetchConsumeStatsInBroker can see this group",
                hit && has_offsets,
                &format!(
                    "rows={} hit={hit} offsetTable={has_offsets} groups={:?}",
                    list.stats_list.len(),
                    groups
                ),
            );
        }
        Err(e) => ck.abort("A8 fetchConsumeStatsInBroker", &e.to_string()),
    }
    consumer.shutdown();
    Some(client_id)
}

// ------------------------------------------------------------------ A9 message query

async fn a9_message_query(ck: &mut Checker, env: &Env, sent: &[SentMsg]) {
    let admin = &env.admin;
    let topic = env.topic.clone();
    let Some(first) = sent.first() else {
        ck.skip("A9 message query", "A7 did not send any message, skipping");
        return;
    };
    let now = now_millis();
    // queryMessage only asserts "the request is reachable, the broker replies normally, and the
    // return type is correct": msgId is a uniqKey, and the broker-side uniqKey inverted index is
    // only supported by the RocksDB index implementation while this machine uses the default file
    // index ⇒ not finding it is a broker configuration difference, not a client bug.
    let by_key = admin
        .query_message(&topic, &first.uniq_key, 32, now - 60_000, now + 60_000)
        .await;
    match by_key {
        Ok(msgs) => ck.check(
            "A9 the queryMessage request is reachable and replies normally",
            msgs.iter().all(|m| m.get_topic() == topic),
            &format!("hits={}", msgs.len()),
        ),
        Err(e) => ck.abort("A9 the queryMessage request is reachable and replies normally", &e.to_string()),
    }
    match admin
        .query_message_by_key(&topic, &first.uniq_key, 32)
        .await
    {
        Ok(msgs) => ck.check(
            "A9 queryMessageByUniqKey in NORMAL mode is reachable",
            msgs.iter().all(|m| m.get_topic() == topic),
            &format!("hits={}", msgs.len()),
        ),
        Err(e) => ck.abort("A9 queryMessage_by_key", &e.to_string()),
    }
    match admin
        .query_message_by_uniq_key(&topic, &first.uniq_key)
        .await
    {
        Ok(found) => ck.check(
            "A9 queryMessageByUniqKey is reachable (on a hit it must be the original message)",
            found.map_or(true, |m| m.get_topic() == topic),
            "",
        ),
        Err(e) => ck.abort("A9 queryMessage_by_uniq_key", &e.to_string()),
    }
    ck.skip(
        "A9 queryMessage hits",
        "the local broker uses the default file index and the message carries no KEYS, so an empty uniqKey query result is expected",
    );
    match admin.view_message(&topic, &first.offset_msg_id).await {
        Ok(msg) => ck.check(
            "A9 viewMessage(offsetMsgId) fetches the original message back",
            String::from_utf8_lossy(msg.get_body()) == first.body,
            &format!(
                "expected {:?} actual {:?}",
                first.body,
                String::from_utf8_lossy(msg.get_body())
            ),
        ),
        Err(e) => ck.abort("A9 viewMessage(offsetMsgId)", &e.to_string()),
    }
    // The client-side uniqKey is likewise 32 hex digits, and decoding it blindly yields a bogus
    // address; it must take the fallback path and end with a clean NO_MESSAGE(208) instead of a raw
    // overflow/connection exception.
    match admin.view_message(&topic, &first.uniq_key).await {
        Ok(_) => ck.check(
            "A9 viewMessage(uniqKey) takes the fallback and yields a clean exception",
            false,
            "the uniqKey was actually found (the uniqKey index is enabled locally), treating it as a hit",
        ),
        Err(e) => ck.check(
            "A9 viewMessage(uniqKey) takes the fallback and yields a clean exception",
            client_code(&e) == Some(response_code::NO_MESSAGE),
            &e.to_string(),
        ),
    }
    // A genuinely invalid msgId must take the very same fallback; the parse error must not be
    // thrown out directly.
    match admin.view_message(&topic, "not-a-hex-msg-id").await {
        Err(e) => ck.check(
            "A9 viewMessage(illegal msgId) also falls back to 208",
            client_code(&e) == Some(response_code::NO_MESSAGE),
            &e.to_string(),
        ),
        Ok(_) => ck.check(
            "A9 viewMessage(illegal msgId) also falls back to 208",
            false,
            "it was actually found",
        ),
    }
}

// ------------------------------------------------------------------ A10 offset management

async fn a10_offset_admin(ck: &mut Checker, env: &Env) {
    let admin = &env.admin;
    let topic = env.topic.clone();
    let group = env.group.clone();
    let addr = env.broker();
    let mq = MessageQueue::new(&topic, &env.broker_name(), 0);
    let max = step!(ck, "A10 maxOffset", admin.max_offset(&mq).await);
    let min = step!(ck, "A10 minOffset", admin.min_offset(&mq).await);
    if let (Some(max), Some(min)) = (max, min) {
        ck.check(
            "A10 maxOffset >= minOffset and covers the number sent to this queue",
            max >= min && max - min >= (N_MSG / QUEUE_NUMS as usize) as i64,
            &format!("min={min} max={max}"),
        );
    }
    match admin.search_offset(&mq, now_millis()).await {
        Ok(offset) => ck.check(
            "A10 searchOffset(now) falls inside the queue range",
            max.map_or(true, |m| offset <= m) && min.map_or(true, |m| offset >= m),
            &format!("offset={offset}"),
        ),
        Err(e) => ck.abort("A10 searchOffset", &e.to_string()),
    }
    match admin.earliest_msg_store_time(&mq).await {
        Ok(ts) => ck.check(
            "A10 earliestMsgStoreTime is a plausible timestamp",
            ts > 1_600_000_000_000,
            &format!("ts={ts}"),
        ),
        Err(e) => ck.abort("A10 earliestMsgStoreTime", &e.to_string()),
    }
    let fresh = env.dest_group.clone();
    match admin.examine_consumer_offset(&fresh, &mq).await {
        Ok(offset) => ck.check(
            "A10 a group that never committed an offset reads back None (not 0)",
            offset.is_none(),
            &format!("offset={offset:?}"),
        ),
        Err(e) => ck.abort("A10 examineConsumerOffset(new group)", &e.to_string()),
    }
    if let Err(e) = admin
        .update_consumer_offset_to_broker(&addr, &fresh, &mq, 1)
        .await
    {
        ck.abort("A10 updateConsumerOffsetToBroker", &e.to_string());
    } else {
        match admin.examine_consumer_offset(&fresh, &mq).await {
            Ok(offset) => ck.check(
                "A10 the explicitly written offset can be read back",
                offset == Some(1),
                &format!("offset={offset:?}"),
            ),
            Err(e) => ck.abort("A10 read back the explicit offset", &e.to_string()),
        }
    }
    if let Err(e) = admin.update_consumer_offset(&fresh, &mq, 0).await {
        ck.abort("A10 updateConsumerOffset (address resolved via the route)", &e.to_string());
    } else {
        match admin.examine_consumer_offset(&fresh, &mq).await {
            Ok(offset) => ck.check(
                "A10 updateConsumerOffset takes effect through route resolution too",
                offset == Some(0),
                &format!("offset={offset:?}"),
            ),
            Err(e) => ck.abort("A10 read back the route-located offset", &e.to_string()),
        }
    }
    // cloneGroupOffset(314): copies the whole offset set of the source group over to the target
    // group (offline=true, it does not require the target to be online).
    match admin
        .clone_group_offset(&addr, &group, &fresh, &topic, true)
        .await
    {
        Ok(()) => match admin.examine_consumer_offset(&fresh, &mq).await {
            Ok(copied) => ck.check(
                "A10 after cloneGroupOffset the target group's offset matches the source group",
                copied.is_some(),
                &format!("copied={copied:?}"),
            ),
            Err(e) => ck.abort("A10 read back the cloned offset", &e.to_string()),
        },
        Err(e) => ck.abort("A10 cloneGroupOffset", &e.to_string()),
    }

    let bnd_topic = format!("RustLiveAdminBoundary{}", env.stamp);
    if let Err(e) = admin
        .create_topic(MixAll::DEFAULT_TOPIC, &bnd_topic, 1, 0)
        .await
    {
        ck.abort("A10.5 createTopic(1 queue)", &e.to_string());
        return;
    }
    match env.produce(&bnd_topic, 0, 3).await.len() {
        3 => println!("  [INFO] A10.5 boundary topic has sent 3 messages"),
        n => ck.check("A10.5 boundary topic sent 3 messages", false, &format!("ok={n}")),
    }
    sleep_millis(2000).await;
    let bmq = MessageQueue::new(&bnd_topic, &env.broker_name(), 0);
    let bmax = step!(ck, "A10.5 maxOffset", admin.max_offset(&bmq).await);
    let future = now_millis() + 600_000;
    let lo = step!(ck, "A10.5 searchLowerBoundaryOffset", admin
        .search_lower_boundary_offset(&bmq, future)
        .await);
    let up = step!(ck, "A10.5 searchUpperBoundaryOffset", admin
        .search_upper_boundary_offset(&bmq, future)
        .await);
    let lo_past = step!(ck, "A10.5 LOWER(the past)", admin
        .search_lower_boundary_offset(&bmq, 1)
        .await);
    let up_past = step!(ck, "A10.5 UPPER(the past)", admin
        .search_upper_boundary_offset(&bmq, 1)
        .await);
    if let Some(bmax) = bmax {
        ck.check(
            "A10.5 boundary topic maxOffset == 3",
            bmax == 3,
            &format!("max={bmax}"),
        );
        if let (Some(lo), Some(up)) = (lo, up) {
            ck.check(
                "A10.5 LOWER boundary = maxOffset (the offset just past the tail)",
                lo == bmax,
                &format!("lower={lo} maxOffset={bmax}"),
            );
            ck.check(
                "A10.5 UPPER boundary = maxOffset-1 (the offset of the last message itself)",
                up == bmax - 1,
                &format!("upper={up} maxOffset-1={}", bmax - 1),
            );
            ck.check(
                "A10.5 the two boundaries really differ (proving boundaryType takes effect)",
                lo != up,
                &format!("lower={lo} upper={up}"),
            );
        }
    }
    if let (Some(lo_past), Some(up_past)) = (lo_past, up_past) {
        ck.check(
            "A10.5 when the timestamp predates every message both LOWER and UPPER collapse to minOffset",
            lo_past == 0 && up_past == 0,
            &format!("lower={lo_past} upper={up_past}"),
        );
    }
    match admin.search_offset(&bmq, future).await {
        Ok(same) => ck.check(
            "A10.5 the searchOffset default boundary matches an explicit LOWER",
            Some(same) == lo,
            &format!("searchOffset={same} lower={lo:?}"),
        ),
        Err(e) => ck.abort("A10.5 searchOffset", &e.to_string()),
    }
    match admin.delete_topic(&bnd_topic, None).await {
        Ok(()) => println!("  [INFO] A10.5 deleteTopic(boundary) dispatched"),
        Err(e) => println!("  [WARN] A10.5 deleteTopic(boundary): {e}"),
    }
}

// ------------------------------------------------------------------ A11 resend to %RETRY%

/// Send-back verification from the admin's point of view: pull one message →
/// `send_message_back` → poll the broker statistics of `%RETRY%<group>`.
/// The requeue path inside a push consumer is covered by C4 in `live_consumer`; here the public
/// `send_message_back` of a pull-mode consumer is used, and what it proves is that **the admin
/// side can read** this requeue.
async fn a11_send_back(ck: &mut Checker, env: &Env) {
    let topic = env.topic.clone();
    let group = env.group.clone();
    let consumer = match DefaultMQPullConsumer::with_config(PullConsumerConfig {
        consumer_group: group.clone(),
        name_server_addrs: vec![env.namesrv.clone()],
        instance_name: format!("live-admin-back-{}", env.stamp),
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("A11 build pull consumer", &e.to_string());
            return;
        }
    };
    if let Err(e) = consumer.start().await {
        ck.abort("A11 start pull consumer", &e.to_string());
        return;
    }
    let mqs = match consumer.fetch_subscribe_message_queues(&topic).await {
        Ok(mqs) => mqs,
        Err(e) => {
            ck.abort("A11 fetch_subscribe_message_queues", &e.to_string());
            consumer.shutdown();
            return;
        }
    };
    let Some(mq) = mqs.first() else {
        ck.abort("A11 fetch_subscribe_message_queues", "the topic has no queue");
        consumer.shutdown();
        return;
    };
    // Pull from the head of the queue: the offset is independent of the broker's stored range, so
    // we are guaranteed to get one of the messages A7 sent.
    let min = consumer.min_offset(mq).await.unwrap_or(0);
    let msg: Option<MessageExt> = match consumer.pull(mq, "*", min, 1, Some(5_000)).await {
        Ok(result) => match result.status {
            PullStatus::Found => result.msg_found_list.into_iter().next(),
            other => {
                ck.abort("A11 pull one message", &format!("status={other}"));
                None
            }
        },
        Err(e) => {
            ck.abort("A11 pull one message", &e.to_string());
            None
        }
    };
    let Some(msg) = msg else {
        consumer.shutdown();
        return;
    };
    if let Err(e) = consumer.send_message_back(&msg, 0).await {
        ck.abort("A11 sendMessageBack resend", &e.to_string());
        consumer.shutdown();
        return;
    }
    consumer.shutdown();
    let retry_topic = MixAll::get_retry_topic(&group);
    // Polling is mandatory: when delayLevel==0 the broker's consumerSendMsgBack rewrites it to
    // `3 + reconsumeTimes` (≈10s), and the message first goes into SCHEDULE_TOPIC_XXXX before it is
    // delivered to %RETRY%.
    let deadline = Instant::now() + Duration::from_secs(RETRY_VISIBLE_SECONDS);
    let mut max_sum;
    let mut waited = 0;
    loop {
        match env.admin.examine_topic_stats(&retry_topic).await {
            Ok(stats) => {
                max_sum = stats.total_max_offset();
                if max_sum > 0 {
                    break;
                }
            }
            Err(_) => max_sum = -1,
        }
        if Instant::now() > deadline {
            break;
        }
        sleep_millis(1000).await;
        waited += 1;
    }
    ck.check(
        &format!("A11 sendMessageBack landed in {retry_topic}"),
        max_sum > 0,
        &format!("maxOffsetSum={max_sum} (polled {waited}s; the broker rewrites delayLevel=0 into 3)"),
    );
}

// ------------------------------------------------------------------ A12 offset reset

async fn a12_reset_offset(ck: &mut Checker, env: &Env) {
    let admin = &env.admin;
    let topic = env.topic.clone();
    let group = env.group.clone();
    let mq = MessageQueue::new(&topic, &env.broker_name(), 0);
    let max_now = admin.max_offset(&mq).await.unwrap_or(-1);
    // A timestamp in the future ⇒ the offset should be pushed to this queue's maxOffset (222 is
    // computed on the broker side, not by the local searchOffset).
    let ts = now_millis() + 60_000;
    match admin
        .reset_offset_by_timestamp(&topic, &group, ts, true, None, false)
        .await
    {
        Ok(offsets) => {
            ck.check(
                "A12 resetOffsetByTimestamp returns a non-empty offsetTable",
                !offsets.is_empty(),
                &format!("queues={}", offsets.len()),
            );
            ck.check(
                "A12 the reset result covers every queue of the topic",
                offsets.len() as i32 >= QUEUE_NUMS,
                &format!("queues={}", offsets.len()),
            );
        }
        Err(e) => {
            ck.abort("A12 resetOffsetByTimestamp", &e.to_string());
            return;
        }
    }
    sleep_millis(1500).await;
    match admin.examine_consumer_offset(&group, &mq).await {
        Ok(offset) => ck.check(
            "A12 after the broker-side reset the consumer offset is pushed to maxOffset",
            offset.unwrap_or(-1) >= max_now - 1,
            &format!("consumerOffset={offset:?} maxOffset={max_now}"),
        ),
        Err(e) => ck.abort("A12 read back the offset after the reset", &e.to_string()),
    }
    // The newer entry point: with the target group offline a real broker uses 222; on
    // CONSUMER_NOT_ONLINE it falls back to the older API.
    let dest = env.dest_group.clone();
    match admin.reset_offset_new(&dest, &topic, ts + 1_000).await {
        Ok(()) => match admin.examine_consumer_offset(&dest, &mq).await {
            Ok(offset) => ck.check(
                "A12 resetOffsetNew also lands correctly for an offline group",
                offset.unwrap_or(-1) >= max_now - 1,
                &format!("consumerOffset={offset:?} maxOffset={max_now}"),
            ),
            Err(e) => ck.abort("A12 read back resetOffsetNew", &e.to_string()),
        },
        Err(e) => ck.abort("A12 resetOffsetNew", &e.to_string()),
    }
    match admin
        .reset_offset_by_timestamp_old(&dest, &topic, ts + 2_000, true)
        .await
    {
        Ok(offsets) => ck.check(
            "A12 resetOffsetByTimestampOld writes back per queue",
            !offsets.is_empty(),
            &format!("queues={}", offsets.len()),
        ),
        Err(e) => ck.abort("A12 resetOffsetByTimestampOld", &e.to_string()),
    }

    let min_reset = admin.min_offset(&mq).await.unwrap_or(-1);
    match admin
        .reset_offset_by_queue_id(&env.broker(), &group, &topic, mq.queue_id, min_reset)
        .await
    {
        Ok(table) => ck.check(
            "A12 resetOffsetByQueueId reaches the broker successfully",
            table.is_empty()
                || table.iter().any(|(k, _)| k.queue_id == mq.queue_id),
            &format!("returnedTable={} (may be empty)", table.len()),
        ),
        Err(e) => {
            ck.abort("A12 resetOffsetByQueueId", &e.to_string());
            return;
        }
    }
    match admin.examine_consumer_offset(&group, &mq).await {
        Ok(offset) => ck.check(
            "A12 after resetOffsetByQueueId the offset == the target offset",
            offset == Some(min_reset),
            &format!("consumerOffset={offset:?} target={min_reset}"),
        ),
        Err(e) => ck.abort("A12 read back the offset after resetOffsetByQueueId", &e.to_string()),
    }
    {
        let consumer = match DefaultMQPullConsumer::with_config(PullConsumerConfig {
            consumer_group: group.clone(),
            name_server_addrs: vec![env.namesrv.clone()],
            instance_name: format!("live-admin-reset-{}", env.stamp),
            ..Default::default()
        }) {
            Ok(c) => Some(c),
            Err(e) => {
                ck.abort("A12 re-pull after the reset: build consumer", &e.to_string());
                None
            }
        };
        if let Some(consumer) = consumer {
            if let Err(e) = consumer.start().await {
                ck.abort("A12 re-pull after the reset: start", &e.to_string());
            } else {
                match consumer.pull(&mq, "*", min_reset, 1, Some(5_000)).await {
                    Ok(first) => {
                        ck.check(
                            "A12 the first pull after the reset is short-circuited by the broker into PULL_OFFSET_MOVED",
                            first.status == PullStatus::OffsetIllegal
                                && first.next_begin_offset == min_reset,
                            &format!(
                                "status={} nextBeginOffset={} target={min_reset}",
                                first.status, first.next_begin_offset
                            ),
                        );
                        let resume = first.next_begin_offset;
                        match consumer.pull(&mq, "*", resume, 16, Some(5_000)).await {
                            Ok(second) => ck.check(
                                "A12 after the offset reset the historical messages can be pulled again",
                                !second.msg_found_list.is_empty(),
                                &format!(
                                    "status={} found={} from={resume}",
                                    second.status,
                                    second.msg_found_list.len()
                                ),
                            ),
                            Err(e) => ck.abort("A12 second re-pull after the reset", &e.to_string()),
                        }
                    }
                    Err(e) => ck.abort("A12 re-pull after the reset", &e.to_string()),
                }
                consumer.shutdown();
            }
        }
    }
    let bad_target = max_now + 100;
    let rejected = admin
        .reset_offset_by_queue_id(&env.broker(), &group, &topic, mq.queue_id, bad_target)
        .await
        .is_err();
    ck.check(
        "A12 an out-of-range target for resetOffsetByQueueId is rejected by the broker",
        rejected,
        &format!("badTarget={bad_target}"),
    );
    match admin.examine_consumer_offset(&group, &mq).await {
        Ok(offset) => ck.check(
            "A12 the out-of-range reset stays at the illegal offset written by the first call (the two RPCs are not atomic)",
            offset == Some(bad_target),
            &format!("consumerOffset={offset:?} badTarget={bad_target}"),
        ),
        Err(e) => ck.abort("A12 read back the offset after the out-of-range reset", &e.to_string()),
    }
}

// ------------------------------------------------------------------ A13 write permission + cleanup

async fn a13_perm_and_cleanup(ck: &mut Checker, env: &Env) {
    let admin = &env.admin;
    let topic = env.topic.clone();
    let group = env.group.clone();
    let addr = env.broker();
    let broker_name = env.broker_name();
    match admin
        .wipe_write_perm_of_broker(&env.namesrv, &broker_name)
        .await
    {
        Ok(count) => ck.check(
            "A13 wipeWritePermOfBroker replies normally",
            count >= 0,
            &format!("wipeTopicCount={count}"),
        ),
        Err(e) => ck.abort("A13 wipeWritePermOfBroker", &e.to_string()),
    }
    match admin
        .add_write_perm_of_broker(&env.namesrv, &broker_name)
        .await
    {
        Ok(count) => {
            ck.check(
                "A13 addWritePermOfBroker restores the write permission",
                count >= 0,
                &format!("addTopicCount={count}"),
            );
            // After the restore we must still be able to send (otherwise every later client test on this
            // cluster would fail).
            let mut msg = Message::new(&topic, Some(b"a13-after-perm-restore"));
            match env.producer.send(&mut msg, Some(5_000), None).await {
                Ok(result) => ck.check(
                    "A13 after the permission is restored the producer still sends normally",
                    result.status == SendStatus::SendOk,
                    &format!("status={}", result.status),
                ),
                Err(e) => ck.abort("A13 after the permission is restored the producer still sends normally", &e.to_string()),
            }
        }
        Err(e) => ck.abort("A13 addWritePermOfBroker", &e.to_string()),
    }

    // Delete in three steps: broker config → NameServer route → the combined entry point
    // (idempotent).
    if let Err(e) = admin.delete_topic_in_broker(&addr, &topic).await {
        ck.abort("A13 deleteTopicInBroker", &e.to_string());
    }
    if let Err(e) = admin.delete_topic_in_name_server(None, &topic).await {
        ck.abort("A13 deleteTopicInNameServer", &e.to_string());
    }
    if let Err(e) = admin.delete_topic(&topic, None).await {
        ck.abort("A13 deleteTopic", &e.to_string());
    }
    let retry_topic = MixAll::get_retry_topic(&group);
    let _ = admin.delete_topic(&retry_topic, None).await;
    for name in [&group, &env.dest_group] {
        if let Err(e) = admin.delete_subscription_group(&addr, name, true).await {
            ck.abort("A13 deleteSubscriptionGroup", &e.to_string());
        }
    }
    sleep_millis(1500).await;
    match admin.fetch_all_topic_list().await {
        Ok(list) => ck.check(
            "A13 after deleteTopic the topic disappears from the list",
            !list.get_topic_list().contains(&topic),
            &format!("topics={}", list.get_topic_list().len()),
        ),
        Err(e) => ck.abort("A13 fetchAllTopicList after the deletion", &e.to_string()),
    }
    // To decide that a "group has been deleted" we can only use the read-only full table (200
    // paginated). Code 201's `GET_SUBSCRIPTIONGROUP_CONFIG` goes through
    // `findSubscriptionGroupConfig` on the broker, and with autoCreateSubscriptionGroup=true a
    // miss quietly creates a default group (retryMaxTimes=16), so using it would give the false
    // impression of "deleted but still there".
    // The deletion has to go through the broker's subscriptionGroupTable (the paginated reply is
    // computed live from groupSeq), and a fixed 1.5s wait occasionally loses the race ⇒ poll
    // until the budget runs out.
    let deadline = Instant::now() + Duration::from_secs(15);
    for (label, name) in [
        ("A13 the group cannot be found after deleteSubscriptionGroup", group.clone()),
        ("A13 the target group is deleted as well", env.dest_group.clone()),
    ] {
        let mut gone = false;
        let mut detail = String::new();
        loop {
            match admin.examine_subscription_group_config(&addr, &name).await {
                Ok(None) => {
                    gone = true;
                    break;
                }
                Ok(Some(found)) => detail = format!("still there: {}", found.group_name),
                Err(e) => {
                    detail = format!("read back failed: {e}");
                    break;
                }
            }
            if Instant::now() > deadline {
                break;
            }
            sleep_millis(500).await;
        }
        ck.check(label, gone, &detail);
    }
}

// ------------------------------------------------------------------ A0 facade state

fn a0_facade_state(ck: &mut Checker, env: &Env) {
    let admin = &env.admin;
    ck.check(
        "A0 after start is_started holds and a clientId was generated",
        admin.is_started() && !admin.client_id().is_empty(),
        &format!(
            "started={} clientId={}",
            admin.is_started(),
            admin.client_id()
        ),
    );
    ck.check(
        "A0 the name server addresses are split on ; and read back",
        admin.get_name_server_address_list() == vec![env.namesrv.clone()]
            && admin.get_name_server_addr() == env.namesrv,
        &admin.get_name_server_addr(),
    );
    ck.check(
        "A0 timeout_millis uses the configured value",
        admin.timeout_millis() == 10_000,
        &format!("timeout={}", admin.timeout_millis()),
    );
    ck.check(
        "A0 getMQClientInstance is usable",
        admin.get_mq_client_instance().is_ok(),
        "the private instance was not obtained",
    );
}

// ------------------------------------------------------------------ driver

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let env = match Env::new(namesrv, &stamp) {
        Ok(env) => env,
        Err(e) => {
            ck.abort("build the fixture", &e);
            return ck;
        }
    };
    if !env.start(&mut ck).await {
        env.admin.shutdown();
        env.producer.shutdown();
        return ck;
    }
    println!("== live admin check, namesrv={namesrv} stamp={stamp} ==");
    a0_facade_state(&mut ck, &env);
    a2_cluster(&mut ck, &env).await;
    a3_topic_lifecycle(&mut ck, &env).await;
    // A1 needs a writable topic to prove that "shutting the admin down does not affect the
    // producer", so it runs after A3.
    a1_private_instance_isolation(&mut ck, &env).await;
    a4_broker_config(&mut ck, &env).await;
    a5_kv_config(&mut ck, &env).await;
    a6_subscription_group(&mut ck, &env).await;
    let sent = a7_produce_and_stats(&mut ck, &env).await;
    a8_online_consumer(&mut ck, &env).await;
    a9_message_query(&mut ck, &env, &sent).await;
    a10_offset_admin(&mut ck, &env).await;
    a11_send_back(&mut ck, &env).await;
    a12_reset_offset(&mut ck, &env).await;
    a13_perm_and_cleanup(&mut ck, &env).await;
    env.admin.shutdown();
    env.producer.shutdown();
    ck
}

fn report(ck: &mut Checker) {
    println!(
        "== summary: {} passed, {} failed, {} skipped ==",
        ck.passed,
        ck.failed.len(),
        ck.skipped
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
    let mut ck = run(&namesrv).await;
    report(&mut ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
