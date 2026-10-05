//! 拉模式消费者（`DefaultMQPullConsumer`）的心跳必须把消费组注册进 broker（#98）——
//! 对真实 5.5.1 集群的验证。
//!
//! 场景与 `python/verify_pull_consumer_heartbeat_live.py`、`cpp/examples/live_pull_heartbeat.cpp`
//! 和 `csharp/examples/RocketMQ.Examples/LivePullHeartbeat.cs` 一致（四语言同一套断言）：
//!
//! - A0 建 topic（主节点一份配置）。
//! - A1 起拉模式消费者 + 拉一轮：拉取本身正常。
//! - A2 【核心】主节点 203 查到本组，`consumeType=CONSUME_ACTIVELY`
//!   （Java `DefaultMQPullConsumerImpl:348`）、`messageModel=CLUSTERING`、
//!   `consumeFromWhere=CONSUME_FROM_LAST_OFFSET`（:353）。
//! - A2b 203 的订阅表带 registerTopics 的 topic 且 `subString="*"`（`subscriptions():357-385`）。
//! - A3 主节点 38（GET_CONSUMER_LIST_BY_GROUP）查到本 clientId。
//! - A4 从节点 203/38 同样看得到：心跳扇出到每一台（ConsumerManager 每台一份状态）。
//! - A5 对照：从未心跳过的幽灵组 → 203 报错、38 空列表（判据本身有效）。
//! - A6 shutdown → 35 注销 → 203 随即查不到（不必等 ~120s 通道扫描）。
//!
//! 为什么必须真机：离线单测只能证明报文形状（`pull_heartbeat_matches_the_java_shape` 等），
//! 证明不了 broker 的 `ConsumerManager.consumerTable` 真的登记了本组，也证明不了 35 之后
//! 立刻摘除 —— 这两件事只有真 broker 有。
//!
//! 用法（先按 runbook 起本地集群）：
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

// ------------------------------------------------------------------ 骨架

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// 断言累积器：一次跑完所有场景再汇总，首个失败不提前退出。
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

// ------------------------------------------------------------------ 判据

/// 203 的原始答案：在线返回 `Some(connection)`；组不在时 broker 抛
/// `MQBrokerException`（206 CONSUMER_NOT_ONLINE）→ 一律当不在线，报错文本带回。
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

/// 38 的原始答案：组不在时 broker 直接回 `no consumer for this group`，当空列表。
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

/// 203 订阅表里某 topic 的 `subString`（表是 topic → SubscriptionData JSON 的键值对）。
fn sub_string_of(conn: &ConsumerConnection, topic: &str) -> Option<String> {
    conn.subscription_table
        .iter()
        .find(|(t, _)| t == topic)
        .and_then(|(_, v)| v.get("subString").or_else(|| v.get("sub_string")))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

// ------------------------------------------------------------------ 主流程

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

    // ---- A0 建 topic ----
    if let Err(e) = admin
        .create_topic_in_broker(master, &topic, 4, 4, DEFAULT_PERM)
        .await
    {
        ck.abort("A0 建 topic", &e.to_string());
        admin.shutdown();
        return ck;
    }

    // ---- A1 起拉模式消费者并拉一轮 ----
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
                        "A1 拉模式消费者启动并成功拉取一轮",
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
                Err(e) => ck.abort("A1 消费者 start", &e.to_string()),
            }
        }
        Err(e) => ck.abort("A1 消费者构造", &e.to_string()),
    }

    let Some(c) = consumer else {
        cleanup(&admin, master, &topic).await;
        admin.shutdown();
        return ck;
    };

    // ---- A2 主节点 203 ----
    let master_conn = match group_is_online(&admin, &group, master).await {
        Ok(conn) => {
            ck.check(
                "A2 主节点 203 查到本组（心跳已注册）",
                true,
                &format!("connections={}", conn.connection_set.len()),
            );
            ck.check(
                "A2 消费类型是 CONSUME_ACTIVELY（Java DefaultMQPullConsumerImpl:348）",
                conn.consume_type.as_deref() == Some("CONSUME_ACTIVELY"),
                &format!("consumeType={:?}", conn.consume_type),
            );
            ck.check(
                "A2 消费位点是 CONSUME_FROM_LAST_OFFSET（:353）",
                conn.consume_from_where.as_deref() == Some("CONSUME_FROM_LAST_OFFSET"),
                &format!("consumeFromWhere={:?}", conn.consume_from_where),
            );
            ck.check(
                "A2 广播/集群口径是 CLUSTERING",
                conn.message_model.as_deref() == Some("CLUSTERING"),
                &format!("messageModel={:?}", conn.message_model),
            );
            Some(conn)
        }
        Err(e) => {
            ck.abort("A2 主节点 203 查到本组（心跳已注册）", &e);
            None
        }
    };

    // ---- A2b 订阅集来自 registerTopics（subscriptions():357-385）----
    if let Some(conn) = &master_conn {
        let sub = sub_string_of(conn, &topic);
        ck.check(
            "A2b 203 的订阅表带 registerTopics 的 topic 且 subString=*",
            sub.as_deref() == Some("*"),
            &format!("subscriptionTable={:?}", conn.subscription_table),
        );
    } else {
        ck.abort("A2b 203 的订阅表", "203 不可用，跳过");
    }

    // ---- A3 主节点 38 ----
    match consumer_ids(&admin, &group, master).await {
        Ok(ids) => ck.check(
            "A3 主节点 38 查到本 clientId",
            ids.iter().any(|id| id == &c.client_id()),
            &format!("ids={ids:?} clientId={}", c.client_id()),
        ),
        Err(e) => ck.abort("A3 主节点 38 查到本 clientId", &e),
    }

    // ---- A4 从节点 ----
    if let Some(slave) = slave {
        match group_is_online(&admin, &group, slave).await {
            Ok(conn) => ck.check(
                "A4 从节点 203 也查到本组（心跳扇出到从节点）",
                true,
                &format!("slave={slave} connections={}", conn.connection_set.len()),
            ),
            Err(e) => ck.abort("A4 从节点 203 也查到本组（心跳扇出到从节点）", &e),
        }
        match consumer_ids(&admin, &group, slave).await {
            Ok(ids) => ck.check(
                "A4 从节点 38 也查到本 clientId",
                ids.iter().any(|id| id == &c.client_id()),
                &format!("ids={ids:?}"),
            ),
            Err(e) => ck.abort("A4 从节点 38 也查到本 clientId", &e),
        }
    }

    // ---- A5 对照：幽灵组（从未心跳）必须查不到 ----
    match group_is_online(&admin, &ghost, master).await {
        Ok(conn) => ck.check(
            "A5 对照：未心跳的幽灵组 203 查不到",
            false,
            &format!("ghost connections={}", conn.connection_set.len()),
        ),
        Err(e) => ck.check(
            "A5 对照：未心跳的幽灵组 203 查不到",
            true,
            &format!("err={e}"),
        ),
    }
    match consumer_ids(&admin, &ghost, master).await {
        Ok(ids) => ck.check(
            "A5 对照：未心跳的幽灵组 38 空列表",
            ids.is_empty(),
            &format!("ids={ids:?}"),
        ),
        Err(_) => ck.check("A5 对照：未心跳的幽灵组 38 空列表", true, "broker 报 no consumer"),
    }

    // ---- A6 shutdown 立刻注销（35）----
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
        "A6 shutdown 后 203 立刻查不到本组（发过 35 注销）",
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
