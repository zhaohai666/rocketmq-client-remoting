//! The pull consumer (`DefaultMQPullConsumer`) heartbeat must register the consumer group with the broker (#98) --
//! verified against a real 5.5.1 cluster.
//!
//! Why a real cluster is required: offline unit tests can only prove the wire shape (`pull_heartbeat_matches_the_wire_shape` etc.),
//! they cannot prove the broker's `ConsumerManager.consumerTable` really registered this group, nor that after request 35
//! the group is removed right away -- only a real broker has these two.
//!
//! Usage (start the local cluster per the runbook first):
//! ```text
//! cargo run --example live_pull_heartbeat -- 127.0.0.1:9876 127.0.0.1:10911 [127.0.0.1:10931]
//! ```

use std::env;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::DefaultMQAdminExt;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultMQPullConsumer, PullConsumerConfig,
};
use rocketmq_client_remoting::client::result::PullStatus;
use rocketmq_client_remoting::common::topic_config::DEFAULT_PERM;
use rocketmq_client_remoting::remoting::protocol::body::ConsumerConnection;

// ------------------------------------------------------------------ Skeleton

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// Assertion accumulator: runs all scenarios in one pass before summarizing; the first failure does not exit early.
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
}

fn report(ck: &Checker) {
    println!();
    println!("PullHeartbeat: PASS={} FAIL={}", ck.passed, ck.failed.len());
    for f in &ck.failed {
        println!("  FAILED: {f}");
    }
}

// ------------------------------------------------------------------ Criteria

/// The raw answer of 203: when online returns `Some(connection)`; when the group is absent the broker fails
/// the request (206 CONSUMER_NOT_ONLINE) → always treated as offline, with the error text carried back.
async fn group_is_online(
    admin: &DefaultMQAdminExt,
    group: &str,
    addr: &str,
) -> Result<ConsumerConnection, String> {
    admin
        .examine_consumer_connection_info(group, Some(addr))
        .await
        .map_err(|e| e.to_string())
}

/// The raw answer of 38: when the group is absent the broker directly replies `no consumer for this group`, treated as an empty list.
async fn consumer_ids(
    admin: &DefaultMQAdminExt,
    group: &str,
    addr: &str,
) -> Result<Vec<String>, String> {
    admin
        .get_consumer_list_by_group(group, Some(addr))
        .await
        .map(|body| body.consumer_id_list)
        .map_err(|e| e.to_string())
}

/// The `subString` of a topic in the 203 subscription table (the table is a topic → SubscriptionData JSON key-value map).
fn sub_string_of(conn: &ConsumerConnection, topic: &str) -> Option<String> {
    conn.subscription_table
        .iter()
        .find(|(t, _)| t == topic)
        .and_then(|(_, v)| v.get("subString").or_else(|| v.get("sub_string")))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

// ------------------------------------------------------------------ Main flow

async fn run(namesrv: &str, master: &str, slave: Option<&str>) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let topic = format!("PullHbRust{stamp}");
    let group = format!("rust-live-pullhb-{stamp}");
    let ghost = format!("rust-live-pullhb-ghost-{stamp}");
    println!("namesrv={namesrv} master={master} topic={topic} group={group}");

    let admin = DefaultMQAdminExt::new();
    admin.set_namesrv_addr(namesrv);
    admin.set_instance_name(&format!("rust-live-pullhb-admin-{stamp}"));
    admin.set_timeout_millis(10_000);
    if let Err(e) = admin.start().await {
        ck.abort("admin start", &e.to_string());
        return ck;
    }

    // ---- A0 create topic ----
    if let Err(e) = admin
        .create_topic_in_broker(master, &topic, 4, 4, DEFAULT_PERM)
        .await
    {
        ck.abort("A0 create topic", &e.to_string());
        admin.shutdown();
        return ck;
    }

    // ---- A1 start the pull consumer and pull one round ----
    let mut consumer: Option<DefaultMQPullConsumer> = None;
    match DefaultMQPullConsumer::with_config(PullConsumerConfig {
        consumer_group: group.clone(),
        name_server_addrs: vec![namesrv.to_string()],
        instance_name: format!("rust-live-pullhb-{stamp}"),
        ..Default::default()
    }) {
        Ok(c) => {
            c.register_topic(&topic);
            match c.start().await {
                Ok(()) => {
                    let queues = c.fetch_subscribe_message_queues(&topic).await.unwrap_or_default();
                    let pull = match queues.first() {
                        Some(mq) => Some(c.pull(mq, "*", 0, 32, None).await),
                        None => None,
                    };
                    let pulled = pull
                        .as_ref()
                        .and_then(|r| r.as_ref().ok())
                        .map(|r| r.status);
                    ck.check(
                        "A1 pull consumer starts and pulls one round successfully",
                        c.heartbeat_count() >= 1
                            && matches!(pulled, Some(PullStatus::Found) | Some(PullStatus::NoNewMsg)),
                        &format!(
                            "queues={} status={pulled:?} heartbeats={}",
                            queues.len(),
                            c.heartbeat_count()
                        ),
                    );
                    consumer = Some(c);
                }
                Err(e) => ck.abort("A1 consumer start", &e.to_string()),
            }
        }
        Err(e) => ck.abort("A1 consumer build", &e.to_string()),
    }

    let Some(c) = consumer else {
        cleanup(&admin, master, &topic).await;
        admin.shutdown();
        return ck;
    };

    // ---- A2 master, request 203 ----
    let master_conn = match group_is_online(&admin, &group, master).await {
        Ok(conn) => {
            ck.check(
                "A2 master 203 finds this group (heartbeat registered)",
                true,
                &format!("connections={}", conn.connection_set.len()),
            );
            ck.check(
                "A2 consume type is CONSUME_ACTIVELY",
                conn.consume_type.as_deref() == Some("CONSUME_ACTIVELY"),
                &format!("consumeType={:?}", conn.consume_type),
            );
            ck.check(
                "A2 consume-from-where is CONSUME_FROM_LAST_OFFSET",
                conn.consume_from_where.as_deref() == Some("CONSUME_FROM_LAST_OFFSET"),
                &format!("consumeFromWhere={:?}", conn.consume_from_where),
            );
            ck.check(
                "A2 broadcast/clustering mode is CLUSTERING",
                conn.message_model.as_deref() == Some("CLUSTERING"),
                &format!("messageModel={:?}", conn.message_model),
            );
            Some(conn)
        }
        Err(e) => {
            ck.abort("A2 master 203 finds this group (heartbeat registered)", &e);
            None
        }
    };

    // ---- A2b the subscription set comes from registerTopics ----
    if let Some(conn) = &master_conn {
        let sub = sub_string_of(conn, &topic);
        ck.check(
            "A2b the 203 subscription table carries the registerTopics topic with subString=*",
            sub.as_deref() == Some("*"),
            &format!("subscriptionTable={:?}", conn.subscription_table),
        );
    } else {
        ck.abort("A2b 203 subscription table", "203 unavailable, skipped");
    }

    // ---- A3 master, request 38 ----
    match consumer_ids(&admin, &group, master).await {
        Ok(ids) => ck.check(
            "A3 master 38 finds this clientId",
            ids.iter().any(|id| id == &c.client_id()),
            &format!("ids={ids:?} clientId={}", c.client_id()),
        ),
        Err(e) => ck.abort("A3 master 38 finds this clientId", &e),
    }

    // ---- A4 slave ----
    if let Some(slave) = slave {
        match group_is_online(&admin, &group, slave).await {
            Ok(conn) => ck.check(
                "A4 slave 203 also finds this group (heartbeat fans out to the slave)",
                true,
                &format!("slave={slave} connections={}", conn.connection_set.len()),
            ),
            Err(e) => ck.abort("A4 slave 203 also finds this group (heartbeat fans out to the slave)", &e),
        }
        match consumer_ids(&admin, &group, slave).await {
            Ok(ids) => ck.check(
                "A4 slave 38 also finds this clientId",
                ids.iter().any(|id| id == &c.client_id()),
                &format!("ids={ids:?}"),
            ),
            Err(e) => ck.abort("A4 slave 38 also finds this clientId", &e),
        }
    }

    // ---- A5 control: ghost group (never heartbeated) must not be found ----
    match group_is_online(&admin, &ghost, master).await {
        Ok(conn) => ck.check(
            "A5 control: the never-heartbeated ghost group is not found by 203",
            false,
            &format!("ghost connections={}", conn.connection_set.len()),
        ),
        Err(e) => ck.check(
            "A5 control: the never-heartbeated ghost group is not found by 203",
            true,
            &format!("err={e}"),
        ),
    }
    match consumer_ids(&admin, &ghost, master).await {
        Ok(ids) => ck.check(
            "A5 control: the never-heartbeated ghost group gets an empty list from 38",
            ids.is_empty(),
            &format!("ids={ids:?}"),
        ),
        Err(_) => ck.check("A5 control: the never-heartbeated ghost group gets an empty list from 38", true, "broker reports no consumer"),
    }

    // ---- A6 shutdown unregisters immediately (35) ----
    let client_id = c.client_id();
    c.shutdown();
    let mut gone = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if group_is_online(&admin, &group, master).await.is_err() {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        "A6 after shutdown 203 immediately cannot find this group (unregister 35 was sent)",
        gone,
        &format!("clientId={client_id}"),
    );

    cleanup(&admin, master, &topic).await;
    admin.shutdown();
    ck
}

async fn cleanup(admin: &DefaultMQAdminExt, master: &str, topic: &str) {
    if let Err(e) = admin.delete_topic_in_broker(master, topic).await {
        println!("  (cleanup: delete topic {topic} failed: {e})");
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let argv: Vec<String> = env::args().collect();
    let arg = |n: usize, default: &str| -> String {
        argv.get(n)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| default.to_string())
    };
    let namesrv = arg(1, "127.0.0.1:9876");
    let master = arg(2, "127.0.0.1:10911");
    let slave = argv
        .get(3)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let ck = run(&namesrv, &master, slave.as_deref()).await;
    report(&ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
