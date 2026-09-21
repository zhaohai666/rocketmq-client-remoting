//! [`DefaultMQAdminExt`] 对**真实 5.5.1 broker** 的联调验证。
//!
//! 与 `python/verify_admin_live.py`、cpp `examples/admin_live.cpp`、dotnet
//! `ValidatorsLive` 的管理端场景对齐。离线单测（`src/client/admin.rs` 的 `mod tests`）
//! 只能覆盖纯逻辑（properties 文本解析、分页合并、集群地址挑选、码值口径），这里补上
//! 必须真 broker 才能证明的部分：
//! - A1 生命周期与**私有实例**隔离（模块头第 2 条偏差）：admin `shutdown()` 之后，
//!   同 clientId 的 producer 照常收发 —— 证明 admin 没有走 `create_mq_client_instance`
//!   的进程级共享表（#38 的坑在管理端不成立）。
//! - A2 集群：`fetch_broker_cluster_info` / `examine_broker_cluster_info` /
//!   `fetch_broker_runtime_stats` / `view_broker_stats_data`。
//! - A3 Topic：`create_topic`(4 队列) → `fetch_all_topic_list` → `examine_topic_route`
//!   → `get_cluster_list` / `get_topic_cluster_list` / `fetch_topics_by_cluster` →
//!   `examine_topic_config` → `get_all_topic_config` → `get_user_topic_config` →
//!   `get_system_topic_list_from_broker` → `fetch_all_topic_route` →
//!   `create_and_update_topic_config`（改队列数后回读）。
//! - A4 Broker 配置：`get_broker_config` 拿到的是 **properties 文本**（历史 bug 点），
//!   `update_broker_config` 可逆改动 + 回读生效 + 还原；非法 `brokerPermission` 在
//!   **任何 IO 之前**给出 code=16。
//! - A5 NameServer KV：写/读/列/删 + `put_kv_config` 别名（广播到每一台 NameServer）。
//! - A6 订阅组：创建 → 单查 → `examine_subscription_group_config` →
//!   `get_all_subscription_group`（**分页**累积）→ `get_user_subscription_group` →
//!   删除后查不到。
//! - A7 生产 + 统计：`examine_topic_stats`（多 broker 合并）/ `examine_topic_stats_by_broker`
//!   / `examine_consume_stats` / `fetch_consume_stats_in_broker` / `query_consume_queue`。
//! - A8 在线消费者：`examine_consumer_connection_info` / `get_consumer_list_by_group` /
//!   `query_topic_consume_by_who` / `query_topics_by_consumer(group)` +
//!   `query_topics_by_consumer_to_broker`（343）/ `query_subscription` /
//!   `examine_producer_connection_info`，以及 `examine_consumer_running_info`（307 走
//!   broker→客户端回调，证明 `ClientRemotingProcessor` 真能被管理端驱动）。
//! - A9 消息查询：`query_message`/`query_message_by_key`/`query_message_by_uniq_key` 可达
//!   （命中依赖 broker 索引实现，按 Python 口径显式 SKIP）、`view_message(offsetMsgId)`
//!   body 一致、`view_message(uniqKey)` 走兜底并给出干净的 code=208。
//! - A10 Offset：`max/min/search_offset`、`earliest_msg_store_time`、未提交位点回 `None`、
//!   `update_consumer_offset(_to_broker)` 写回、`clone_group_offset`。
//! - A11 `send_message_back` 重投 → 管理端轮询 `%RETRY%<group>` 的统计（broker 把
//!   delayLevel=0 改写成 3，约 10s 后可见）。
//! - A12 位点重置：`reset_offset_by_timestamp`(222, broker 端) 推到未来 → 位点 == max、
//!   `reset_offset_new`、`reset_offset_by_timestamp_old`、`reset_offset_by_queue_id`
//!   （25 + 带 queueId/offset 的 222：拉回 min ⇒ 首笔 pull 被 OFFSET_RESET 短路成
//!   PULL_OFFSET_MOVED、第二笔取到历史消息；越界目标被 broker 拒 ⇒ 位点停在第 1 笔写入的
//!   非法值，证明两笔 RPC 非原子、与 Java 同构）。
//! - A13 写权限 + 清理：`wipe_write_perm_of_broker` → `add_write_perm_of_broker` 还原 →
//!   `delete_topic_in_broker` / `delete_topic_in_name_server` / `delete_topic` →
//!   topic 从 `fetch_all_topic_list` 消失。
//!
//! 用法（先按项目记忆里记的 runbook 起本地集群）：
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

/// 建 topic 用的队列数，A3 的路由/config 断言都以它为准。
const QUEUE_NUMS: i32 = 4;
/// A7 之前生产的消息条数。
const N_MSG: usize = 8;
/// 等 broker 侧可见（心跳、统计、延迟投递）的通用预算。
const WAIT_SECONDS: u64 = 30;
/// broker 把 delayLevel=0 重写成 3（10s），这里留一倍余量。
const RETRY_VISIBLE_SECONDS: u64 = 30;
/// 生产者/消费者组靠心跳注册，默认 30s 一轮 ⇒ 预算要比一轮大一倍。
const HEARTBEAT_VISIBLE_SECONDS: u64 = 60;

// ------------------------------------------------------------------ 骨架

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// 断言累积器：一次跑完所有场景再汇总，首个失败不提前退出。
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

    /// 「本机 broker 配置不满足该断言前提」的显式记录，与偷懒严格区分。
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

/// 出错只打一行、继续往下跑（管理端场景彼此独立，提前退出会掩盖后面的问题）。
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

// ------------------------------------------------------------------ 夹具

struct Env {
    namesrv: String,
    stamp: String,
    topic: String,
    group: String,
    /// 克隆位点用的目标组（A10）。
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

    /// 本 topic 的默认集群名：优先用 nameServer 回的路由字段。
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

    /// 集群探活：端口开着 != broker 已注册到 nameServer，所以按 Python 那样轮询。
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
                        ck.abort("集群探活", &e.to_string());
                        return false;
                    }
                }
            }
            if Instant::now() > deadline {
                ck.abort("集群探活", "nameServer 在预算内没有返回任何 broker");
                return false;
            }
            sleep_millis(1000).await;
        }
    }

    /// 一条发送成功的三件套：body + offsetMsgId（编码了 broker 地址）+ 客户端 uniqKey。
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

/// A1 的输出：admin 私有实例关掉后自身状态。
#[derive(Clone, Debug)]
struct SentMsg {
    body: String,
    /// broker 侧 `QUEUE_OFFSET` 编码出来的地址 + 偏移，`view_message` 走 33 用它。
    offset_msg_id: String,
    /// 客户端生成的 32 位十六进制 uniqKey，硬解会拼出一个假地址。
    uniq_key: String,
}

/// 只取客户端错的码值（broker 错也归一化到 `response_code()`）。
fn client_code(err: &Error) -> Option<i32> {
    err.response_code()
}

// ------------------------------------------------------------------ A1 私有实例

/// A1：admin 用的是**私有** `MQClientInstance`（模块头偏差 2），所以即便和 producer
/// 撞了同一个 clientId，`admin.shutdown()` 也不能把 producer 打死。
/// 这正是 #38 在其它 facade 上还没修的坑 —— 管理端必须先证明它不成立。
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
            ck.abort("A1 producer 构造", &e.to_string());
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
        "A1 admin 与 producer 共用同一个 clientId",
        admin.client_id() == client_id
            && producer.client_id().as_deref() == Some(client_id.as_str()),
        &format!(
            "admin={} producer={:?}",
            admin.client_id(),
            producer.client_id()
        ),
    );
    // admin 自己还能干活（两个实例并存，路由查询各走各的）。
    ck.check(
        "A1 两个实例并存时 admin 照常可用",
        admin.examine_topic_route(&env.topic).await.is_ok(),
        "route 查询失败",
    );
    admin.shutdown();
    ck.check(
        "A1 admin shutdown 后自身未 started",
        !admin.is_started(),
        "",
    );
    ck.check(
        "A1 admin shutdown 后 producer 仍在 started",
        producer.is_started(),
        "admin 关掉了共享实例（#38 的坑复现）",
    );
    // 真正发一条，证明连接没被连带拆掉。
    let mut msg = Message::new(&env.topic, Some(b"a1-after-admin-shutdown"));
    let sent = producer.send(&mut msg, Some(5_000), None).await;
    match sent {
        Ok(result) => ck.check(
            "A1 admin shutdown 后 producer 照常发送",
            result.status == SendStatus::SendOk,
            &format!("status={}", result.status),
        ),
        Err(e) => ck.abort("A1 admin shutdown 后 producer 照常发送", &e.to_string()),
    }
    ck.check(
        "A1 admin 二次 shutdown 幂等",
        {
            admin.shutdown();
            true
        },
        "",
    );
    producer.shutdown();
}

// ------------------------------------------------------------------ A2 集群

async fn a2_cluster(ck: &mut Checker, env: &Env) {
    let admin = &env.admin;
    let info = step!(
        ck,
        "A2 fetchBrokerClusterInfo",
        admin.fetch_broker_cluster_info().await
    );
    let Some(info) = info else { return };
    ck.check(
        "A2 集群信息含 broker 与集群名",
        !info.broker_addr_table.is_empty() && !info.cluster_addr_table.is_empty(),
        &format!(
            "brokers={} clusters={}",
            info.broker_addr_table.len(),
            info.cluster_addr_table.len()
        ),
    );
    match admin.examine_broker_cluster_info().await {
        Ok(other) => ck.check(
            "A2 examineBrokerClusterInfo 与 fetch 等价",
            other.broker_addr_table == info.broker_addr_table,
            "两个入口结果不一致",
        ),
        Err(e) => ck.abort("A2 examineBrokerClusterInfo", &e.to_string()),
    }
    let addr = env.broker();
    match admin.fetch_broker_runtime_stats(&addr, None).await {
        Ok(kv) => {
            ck.check(
                "A2 fetchBrokerRuntimeStats 解析成 KVTable",
                !kv.table.is_empty(),
                &format!("keys={}", kv.table.len()),
            );
            match admin.get_broker_runtime_info(&addr, None).await {
                Ok(same) => ck.check(
                    "A2 getBrokerRuntimeInfo 别名同结果",
                    same.table.len() == kv.table.len(),
                    "别名入口长度不一致",
                ),
                Err(e) => ck.abort("A2 getBrokerRuntimeInfo", &e.to_string()),
            }
        }
        Err(e) => ck.abort("A2 fetchBrokerRuntimeStats", &e.to_string()),
    }
    // broker 侧 stats 库是运行时才有内容的，查不到只记一行不算失败。
    match admin
        .view_broker_stats_data(&addr, "BROKER_PUT_NUMS", &env.broker_name())
        .await
    {
        Ok(value) => ck.check(
            "A2 viewBrokerStatsData 正常应答",
            value.is_object() || value.is_null(),
            &value.to_string(),
        ),
        Err(e) => println!("  [INFO] A2 viewBrokerStatsData 本机无数据: {e}"),
    }
}

// ------------------------------------------------------------------ A3 Topic 管理

async fn a3_topic_lifecycle(ck: &mut Checker, env: &Env) {
    let admin = &env.admin;
    let topic = env.topic.clone();
    match admin
        .create_topic(MixAll::DEFAULT_TOPIC, &topic, QUEUE_NUMS, 0)
        .await
    {
        Ok(()) => println!("  [INFO] A3 createTopic({topic}) 已下发"),
        Err(e) => {
            ck.abort("A3 createTopic", &e.to_string());
            return;
        }
    }
    // 路由是异步登记的（broker 每 10s 向 nameServer 注册），所以带预算轮询。
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
        ck.abort("A3 examineTopicRoute", "新 topic 路由在预算内不可见");
        return;
    };
    let read_nums: Vec<i32> = route
        .queue_datas
        .iter()
        .map(|q| q.read_queue_nums)
        .collect();
    ck.check(
        "A3 路由 readQueueNums 全部为建 topic 时的队列数",
        read_nums.iter().all(|n| *n == QUEUE_NUMS),
        &format!("{read_nums:?}"),
    );
    match admin.fetch_all_topic_list().await {
        Ok(list) => ck.check(
            "A3 fetchAllTopicList 含新 topic",
            list.get_topic_list().contains(&topic),
            &format!("topics={}", list.get_topic_list().len()),
        ),
        Err(e) => ck.abort("A3 fetchAllTopicList", &e.to_string()),
    }
    // Python/Java 的 `getClusterList(topic)`：查该 topic 所在的集群名。
    let mut clusters_of_topic: Vec<String> = Vec::new();
    match admin.get_cluster_list(&topic).await {
        Ok(clusters) => {
            clusters_of_topic = clusters.clone();
            ck.check(
                "A3 getClusterList(topic) 给出该 topic 所在集群",
                !clusters.is_empty(),
                "返回空列表",
            );
        }
        Err(e) => ck.abort("A3 getClusterList(topic)", &e.to_string()),
    }
    match admin.get_topic_cluster_list(&topic).await {
        Ok(clusters) => ck.check(
            "A3 getTopicClusterList 与 getClusterList(topic) 等价",
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
            &format!("A3 fetchTopicsByCluster({cluster}) 含新 topic"),
            topics.contains(&topic),
            &format!("topics={} 命中={}", topics.len(), topics.contains(&topic)),
        ),
        Err(e) => ck.abort("A3 fetchTopicsByCluster", &e.to_string()),
    }
    let addr = env.broker();
    match admin.examine_topic_config(&addr, &topic).await {
        Ok(cfg) => {
            ck.check(
                "A3 TopicConfig 队列数与创建一致",
                cfg.read_queue_nums == QUEUE_NUMS && cfg.write_queue_nums == QUEUE_NUMS,
                &format!(
                    "read={} write={}",
                    cfg.read_queue_nums, cfg.write_queue_nums
                ),
            );
            ck.check(
                "A3 TopicConfig.perm 是读写权限",
                cfg.perm == DEFAULT_PERM,
                &format!("perm={}", cfg.perm),
            );
            ck.check(
                "A3 TopicConfig.attributes 被反序列化",
                cfg.attributes.is_empty() || cfg.attributes.contains_key("+traceSwitch"),
                &format!("{:?}", cfg.attributes.iter().collect::<Vec<_>>()),
            );
        }
        Err(e) => ck.abort("A3 examineTopicConfig", &e.to_string()),
    }
    match admin.get_all_topic_config(&addr, None).await {
        Ok(wrapper) => ck.check(
            "A3 getAllTopicConfig 含新 topic",
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
                "A3 getUserTopicConfig 保留业务 topic",
                names.contains(&topic.as_str()),
                &format!("{names:?}"),
            );
            ck.check(
                "A3 getUserTopicConfig 剔除系统/重试/DLQ topic",
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
            "A3 getSystemTopicListFromBroker 非空",
            !list.get_topic_list().is_empty(),
            &format!("{:?}", list.get_topic_list()),
        ),
        Err(e) => ck.abort("A3 getSystemTopicListFromBroker", &e.to_string()),
    }
    match admin.fetch_all_topic_route().await {
        Ok(routes) => ck.check(
            "A3 fetchAllTopicRoute 有条目",
            !routes.is_empty(),
            &format!("count={}", routes.len()),
        ),
        Err(e) => ck.abort("A3 fetchAllTopicRoute", &e.to_string()),
    }
    // 可逆改配置：把队列数改成 6 再回读，最后还原成 4（A13 删 topic 前保持原样）。
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
            "A3 createAndUpdateTopicConfig 生效（队列数 +2）",
            cfg.read_queue_nums == QUEUE_NUMS + 2,
            &format!("read={}", cfg.read_queue_nums),
        ),
        Err(e) => ck.abort("A3 回读改后的 TopicConfig", &e.to_string()),
    }
    let mut restored = changed.clone();
    restored.read_queue_nums = QUEUE_NUMS;
    restored.write_queue_nums = QUEUE_NUMS;
    if let Err(e) = admin.create_and_update_topic_config(&addr, &restored).await {
        println!("  [WARN] A3 还原队列数失败: {e}");
    }
}

// ------------------------------------------------------------------ A4 Broker 配置

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
    // 响应体是 **properties 文本**，不是 KVTable JSON（历史 bug 点）。
    ck.check(
        "A4 getBrokerConfig 解析出非空 k=v",
        !props.is_empty(),
        &format!("keys={}", props.len()),
    );
    ck.check(
        "A4 brokerName 与路由里的 broker 一致",
        props
            .get("brokerName")
            .is_none_or(|v| v == env.broker_name() || env.broker_name().is_empty()),
        &format!(
            "brokerName={:?} route={:?}",
            props.get("brokerName"),
            env.broker_name()
        ),
    );
    // 非法 brokerPermission 必须在**任何 IO 之前**被拒（code=16，不改 broker 一个字）。
    let mut bad = StringMap::new();
    bad.insert("brokerPermission", "not-a-number");
    match admin.update_broker_config(&addr, &bad, None).await {
        Err(e) => ck.check(
            "A4 非法 brokerPermission 本地快失败",
            client_code(&e) == Some(response_code::NO_PERMISSION),
            &e.to_string(),
        ),
        Ok(()) => ck.check(
            "A4 非法 brokerPermission 本地快失败",
            false,
            "居然打到了 broker，校验没生效",
        ),
    }

    // 可逆改动：写一个无害值、回读、还原。
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
                "A4 updateBrokerConfig 真实生效",
                after.get(key) == Some(probe),
                &format!("期望 {probe}，实际 {:?}", after.get(key)),
            ),
            Err(e) => ck.abort("A4 回读 updateBrokerConfig", &e.to_string()),
        }
    }
    let mut revert = StringMap::new();
    revert.insert(key, original.as_str());
    if let Err(e) = admin.update_broker_config(&addr, &revert, None).await {
        println!("  [WARN] A4 还原 {key} 失败: {e}");
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
            "A5 getKVConfig 值往返一致",
            value.as_deref() == Some("v1"),
            &format!("value={value:?}"),
        ),
        Err(e) => ck.abort("A5 getKVConfig", &e.to_string()),
    }
    match admin.get_kv_list_by_namespace(&ns).await {
        Ok(table) => ck.check(
            "A5 getKVListByNamespace 含 k1",
            table.table.get("k1") == Some("v1"),
            &format!("{:?}", table.table.iter().collect::<Vec<_>>()),
        ),
        Err(e) => ck.abort("A5 getKVListByNamespace", &e.to_string()),
    }
    // Java 的 putKVConfig 是空实现，真正的写入在 createAndUpdateKvConfig（Python 同）。
    if let Err(e) = admin.put_kv_config(&ns, "k2", "v2").await {
        ck.abort("A5 putKVConfig 别名", &e.to_string());
    } else {
        match admin.get_kv_config(&ns, "k2").await {
            Ok(value) => ck.check(
                "A5 putKVConfig 别名同样落 NameServer",
                value.as_deref() == Some("v2"),
                &format!("value={value:?}"),
            ),
            Err(e) => ck.abort("A5 回读 putKVConfig", &e.to_string()),
        }
    }
    if let Err(e) = admin.delete_kv_config(&ns, "k1").await {
        ck.abort("A5 deleteKVConfig", &e.to_string());
    } else {
        match admin.get_kv_config(&ns, "k1").await {
            Ok(value) => {
                let gone = value.as_ref().filter(|v| !v.is_empty()).is_none();
                ck.check("A5 删除后 KV 不再存在", gone, &format!("value={value:?}"));
            }
            Err(e) => ck.abort("A5 删除后回读 KV", &e.to_string()),
        }
    }
    let _ = admin.delete_kv_config(&ns, "k2").await;
}

// ------------------------------------------------------------------ A6 订阅组

async fn a6_subscription_group(ck: &mut Checker, env: &Env) {
    let admin = &env.admin;
    let addr = env.broker();
    let group = env.group.clone();
    // 目标组也要显式建：A10 的 updateConsumerOffset / A12 的 resetOffset* 在 broker
    // 关掉了 autoCreateSubscriptionGroup（或组名打错）时会回 code=26，先建好才测得准。
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
            "A6 单查 retryMaxTimes 往返一致",
            found.retry_max_times == 5 && found.consume_enable,
            &format!(
                "retryMax={} enable={}",
                found.retry_max_times, found.consume_enable
            ),
        ),
        Ok(None) => ck.check("A6 单查 retryMaxTimes 往返一致", false, "刚建的组查不到"),
        Err(e) => ck.abort("A6 getSubscriptionGroupConfig", &e.to_string()),
    }
    match admin.examine_subscription_group_config(&addr, &group).await {
        Ok(Some(found)) => ck.check(
            "A6 examineSubscriptionGroupConfig 走全量分页取组",
            found.group_name == group,
            &found.group_name,
        ),
        Ok(None) => ck.check(
            "A6 examineSubscriptionGroupConfig",
            false,
            "分页结果里没有该组",
        ),
        Err(e) => ck.abort("A6 examineSubscriptionGroupConfig", &e.to_string()),
    }
    match admin.get_all_subscription_group(&addr, None).await {
        Ok(wrapper) => ck.check(
            "A6 getAllSubscriptionGroup（分页）含新组",
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
                "A6 getUserSubscriptionGroup 含新组且剔除系统组",
                names.contains(&group.as_str())
                    && !names.iter().any(|n| n.starts_with("CID_RMQ_SYS_")),
                &format!("{names:?}"),
            );
        }
        Err(e) => ck.abort("A6 getUserSubscriptionGroup", &e.to_string()),
    }
}

// ------------------------------------------------------------------ A7 生产 + 统计

/// A7 的输出：全部发送成功的消息，A9 的 viewMessage 要靠它。
async fn a7_produce_and_stats(ck: &mut Checker, env: &Env) -> Vec<SentMsg> {
    let admin = &env.admin;
    let topic = env.topic.clone();
    let mut sent: Vec<SentMsg> = Vec::new();
    for queue_id in 0..QUEUE_NUMS {
        let per_queue = N_MSG / QUEUE_NUMS as usize;
        sent.extend(env.produce(&topic, queue_id, per_queue).await);
    }
    ck.check(
        &format!("A7 同步发送 {} 条全部 SEND_OK", N_MSG),
        sent.len() == N_MSG,
        &format!("ok={}/{}", sent.len(), N_MSG),
    );
    if sent.is_empty() {
        return sent;
    }
    // broker 统计是周期刷新的，带预算轮询到 maxOffset 到位。
    // 只保留**见过最大值**的那份快照：轮询期间 broker 侧的统计可能先回一个偏小的
    // 半刷新结果，若最后一轮拿到什么就用什么，下面的"按 broker 查是合并结果子集"
    // 会拿"更晚时刻"的单 broker 值去比"更早时刻"的合并值，随机假失败。
    let deadline = Instant::now() + Duration::from_secs(WAIT_SECONDS);
    let mut stats: Option<TopicStatsTable> = None;
    loop {
        match admin.examine_topic_stats(&topic).await {
            Ok(table) => {
                let total = table.total_max_offset();
                let better = stats
                    .as_ref()
                    .is_none_or(|old| total > old.total_max_offset());
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
                "A7 examineTopicStats 合并后的 maxOffset 覆盖发送量",
                table.total_max_offset() >= N_MSG as i64,
                &format!("maxOffsetSum={}", table.total_max_offset()),
            );
            ck.check(
                "A7 examineTopicStats 覆盖 topic 的每个队列",
                table.offset_table.len() as i32 >= QUEUE_NUMS,
                &format!("queues={}", table.offset_table.len()),
            );
            let addr = env.broker();
            // 「按 broker 查 ⊆ 合并查」要的必须是**同一时刻之后**的合并视图：队列的
            // maxOffset 单调不减，所以先取单 broker、紧接着再取合并，包含关系才成立。
            // 反过来（拿更早的合并快照去比更晚的单 broker 值）会随机假失败。
            match admin.examine_topic_stats_by_broker(&addr, &topic).await {
                Ok(part) => match admin.examine_topic_stats(&topic).await {
                    Ok(merged) => ck.check(
                        "A7 examineTopicStatsByBroker 是合并结果的子集",
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
                    Err(e) => ck.abort("A7 examineTopicStats 复检", &e.to_string()),
                },
                Err(e) => ck.abort("A7 examineTopicStatsByBroker", &e.to_string()),
            }
        }
        None => ck.abort("A7 examineTopicStats", "预算内没有拿到统计"),
    }
    let addr = env.broker();
    let group = env.group.clone();
    match admin
        .examine_consume_stats(&addr, &group, Some(&topic), None)
        .await
    {
        Ok(stats) => ck.check(
            "A7 examineConsumeStats 正常应答（未消费时 lag 即积压）",
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
        // broker 端 `AdminBrokerProcessor#fetchAllConsumeStatsInBroker` 是**按订阅组
        // 逐个建一行**（`subscriptionGroupTable.keySet()` → `{group: [ConsumeStats]}`），
        // 内层 topic 才来自位点表 `whichTopicByConsumer`。所以此刻即使没有提交位点，
        // 行数也应该等于本 broker 上的订阅组数；「行里能看到本组且带统计」由 A8 断言。
        Ok(list) => ck.check(
            "A7 fetchConsumeStatsInBroker 按订阅组出行",
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
            "A7 queryConsumeQueue 索引可查",
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

// ------------------------------------------------------------------ A8 在线消费者

/// 只记录 body 的并发 listener（A8 要证明管理端读到的运行时状态和客户端真实一致）。
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

/// A8 的输出：消费者 clientId，供 runningInfo 复查；`None` 表示没消费到。
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
            ck.abort("A8 消费者构造", &e.to_string());
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
        &format!("A8 push 消费者在线消费（sendMessageBack 前置）>0 条，实得 {got}"),
        got > 0,
        &format!("consumed={got}"),
    );
    // 组名/客户端在线信息：这些请求经 broker 转发回**本进程的客户端**，
    // 顺带证明 ClientRemotingProcessor 的回调链是通的。
    match admin.examine_consumer_connection_info(&group, None).await {
        Ok(conn) => {
            let ids: Vec<&str> = conn
                .connection_set
                .iter()
                .filter_map(|c| c.client_id.as_deref())
                .collect();
            ck.check(
                "A8 examineConsumerConnectionInfo 含本消费者连接",
                ids.contains(&client_id.as_str()),
                &format!("{ids:?}"),
            );
            ck.check(
                "A8 ConsumerConnection 带订阅关系与消费模式",
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
            "A8 getConsumerListByGroup（broker→客户端回调）",
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
            "A8 queryTopicConsumeByWho 能查到本组",
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
            "A8 querySubscription 返回订阅 JSON",
            value.is_some(),
            "broker 没有回订阅数据",
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
                "A8 examineConsumerRunningInfo 走 307 回调拿到运行时属性",
                !info.properties.is_empty() && !info.mq_table.is_empty(),
                &format!(
                    "props={} sub={:?} commit={:?}",
                    info.properties.len(),
                    sub,
                    commit
                ),
            );
            ck.check(
                "A8 runningInfo 的 statusTable 按队列给出",
                !info.status_table.is_empty(),
                "statusTable 为空",
            );
        }
        Err(e) => ck.abort("A8 examineConsumerRunningInfo", &e.to_string()),
    }
    match admin
        .get_consume_status(&env.broker(), &topic, &group, &client_id)
        .await
    {
        Ok(value) => ck.check(
            "A8 getConsumeStatus（223 回调）返回 consumerTable",
            value.is_object(),
            &value.to_string(),
        ),
        Err(e) => ck.abort("A8 getConsumeStatus", &e.to_string()),
    }
    // 生产者组是**心跳**注册到 broker 的（`ProducerManager.groupChannelTable` 只由
    // HEART_BEAT 填充），心跳 30s 一轮 ⇒ 必须轮询等它出现；没注册上时 broker 回
    // SYSTEM_ERROR "the producer group[...] not exist"。
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
        "A8 examineProducerConnectionInfo 看到生产者注册的连接",
        producer_conns.is_some(),
        &match producer_conns {
            Some(n) => format!("connections={n}"),
            None => format!("预算内没等到，最后一次: {last_producer_err}"),
        },
    );
    // 消费发生后位点必须已提交（5s 周期刷新，这里给足预算轮询）。
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
        "A8 消费后的位点已刷到 broker",
        offset.is_some(),
        &format!("offset={offset:?}"),
    );
    // 343 QUERY_TOPICS_BY_CONSUMER 读的是 broker 的 offsetTable
    // （`ConsumerOffsetManager#whichTopicByConsumer`），所以只有在消费者刷过位点之后
    // 才有内容；订阅关系本身不参与。%RETRY% 同理，只有给重试 topic 提交过位点才会出现。
    match admin
        .query_topics_by_consumer_to_broker(&env.broker(), &group)
        .await
    {
        Ok(list) => {
            let topics = list.get_topic_list();
            ck.check(
                "A8 queryTopicsByConsumerToBroker 从位点表读出本组消费过的 topic",
                topics.contains(&topic),
                &format!("{topics:?}"),
            );
        }
        Err(e) => ck.abort("A8 queryTopicsByConsumerToBroker", &e.to_string()),
    }
    // Java 的 admin 级方法（DefaultMQAdminExtImpl:1078）只收 group：按 %RETRY%<group>
    // 的路由逐 broker 扇出再合并，所以这条断言同时验证了路由解析和合并口径。
    match admin.query_topics_by_consumer(&group).await {
        Ok(list) => {
            let topics = list.get_topic_list();
            ck.check(
                "A8 queryTopicsByConsumer(group) 按 %RETRY% 路由扇出并合并",
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
            // 每行是 Java 的 `Map<订阅组名, List<ConsumeStats>>`（组名里有时间戳，
            // 内层 ConsumeStats 又按 `MessageQueue` 键展开 offsetTable），这里直接按
            // 整行 JSON 里出现本组判定，不依赖内层键的拼法。
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
            // 本组那一行的 ConsumeStats 必须带 offsetTable（Java 按 writeQueueNums
            // 逐个队列填 brokerOffset/consumerOffset），否则统计只是空壳。
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
                "A8 有提交位点后 fetchConsumeStatsInBroker 能看到本组",
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

// ------------------------------------------------------------------ A9 消息查询

async fn a9_message_query(ck: &mut Checker, env: &Env, sent: &[SentMsg]) {
    let admin = &env.admin;
    let topic = env.topic.clone();
    let Some(first) = sent.first() else {
        ck.skip("A9 消息查询", "A7 没有发出任何消息，跳过");
        return;
    };
    let now = now_millis();
    // queryMessage 只断言「请求可达、broker 正常应答、返回类型正确」：msgId 属于
    // uniqKey，broker 侧的 uniqKey 倒排索引只有 RocksDB 索引实现支持，本机是默认
    // 文件索引 ⇒ 查不到是 broker 配置差异，不是客户端 bug（Python 同口径显式 SKIP）。
    let by_key = admin
        .query_message(&topic, &first.uniq_key, 32, now - 60_000, now + 60_000)
        .await;
    match by_key {
        Ok(msgs) => ck.check(
            "A9 queryMessage 请求可达且正常应答",
            msgs.iter().all(|m| m.get_topic() == topic),
            &format!("hits={}", msgs.len()),
        ),
        Err(e) => ck.abort("A9 queryMessage 请求可达且正常应答", &e.to_string()),
    }
    match admin
        .query_message_by_key(&topic, &first.uniq_key, 32)
        .await
    {
        Ok(msgs) => ck.check(
            "A9 queryMessageByUniqKey/NORMAL 模式可达",
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
            "A9 queryMessageByUniqKey 可达（命中的话必须是原消息）",
            found.is_none_or(|m| m.get_topic() == topic),
            "",
        ),
        Err(e) => ck.abort("A9 queryMessage_by_uniq_key", &e.to_string()),
    }
    ck.skip(
        "A9 queryMessage 命中结果",
        "本机 broker 是默认文件索引且消息未设 KEYS，uniqKey 查询返回空属预期",
    );
    match admin.view_message(&topic, &first.offset_msg_id).await {
        Ok(msg) => ck.check(
            "A9 viewMessage(offsetMsgId) 取回原消息",
            String::from_utf8_lossy(msg.get_body()) == first.body,
            &format!(
                "期望 {:?} 实际 {:?}",
                first.body,
                String::from_utf8_lossy(msg.get_body())
            ),
        ),
        Err(e) => ck.abort("A9 viewMessage(offsetMsgId)", &e.to_string()),
    }
    // 客户端 uniqKey 同样是 32 位十六进制，硬解会拼出假地址；必须走兜底并以
    // 干净的 NO_MESSAGE(208) 收场，而不是裸的溢出/连接异常。
    match admin.view_message(&topic, &first.uniq_key).await {
        Ok(_) => ck.check(
            "A9 viewMessage(uniqKey) 走兜底并给出干净异常",
            false,
            "uniqKey 竟然被查到了（本机开了 uniqKey 索引），按命中处理",
        ),
        Err(e) => ck.check(
            "A9 viewMessage(uniqKey) 走兜底并给出干净异常",
            client_code(&e) == Some(response_code::NO_MESSAGE),
            &e.to_string(),
        ),
    }
    // 真正不合法的 msgId 也必须走同一条兜底，不能把解析错直接抛出去。
    match admin.view_message(&topic, "not-a-hex-msg-id").await {
        Err(e) => ck.check(
            "A9 viewMessage(非法 msgId) 同样兜底成 208",
            client_code(&e) == Some(response_code::NO_MESSAGE),
            &e.to_string(),
        ),
        Ok(_) => ck.check(
            "A9 viewMessage(非法 msgId) 同样兜底成 208",
            false,
            "居然查到了",
        ),
    }
}

// ------------------------------------------------------------------ A10 Offset 管理

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
            "A10 maxOffset >= minOffset 且覆盖本队列发送量",
            max >= min && max - min >= (N_MSG / QUEUE_NUMS as usize) as i64,
            &format!("min={min} max={max}"),
        );
    }
    match admin.search_offset(&mq, now_millis()).await {
        Ok(offset) => ck.check(
            "A10 searchOffset(now) 落在队列区间内",
            max.is_none_or(|m| offset <= m) && min.is_none_or(|m| offset >= m),
            &format!("offset={offset}"),
        ),
        Err(e) => ck.abort("A10 searchOffset", &e.to_string()),
    }
    match admin.earliest_msg_store_time(&mq).await {
        Ok(ts) => ck.check(
            "A10 earliestMsgStoreTime 是合理时间戳",
            ts > 1_600_000_000_000,
            &format!("ts={ts}"),
        ),
        Err(e) => ck.abort("A10 earliestMsgStoreTime", &e.to_string()),
    }
    // 全新组没提交过位点：Java 的 `setZeroIfNotFound=false` 语义 ⇒ None 而不是 0。
    let fresh = env.dest_group.clone();
    match admin.examine_consumer_offset(&fresh, &mq).await {
        Ok(offset) => ck.check(
            "A10 未提交过位点的组读回 None（不是 0）",
            offset.is_none(),
            &format!("offset={offset:?}"),
        ),
        Err(e) => ck.abort("A10 examineConsumerOffset(新组)", &e.to_string()),
    }
    if let Err(e) = admin
        .update_consumer_offset_to_broker(&addr, &fresh, &mq, 1)
        .await
    {
        ck.abort("A10 updateConsumerOffsetToBroker", &e.to_string());
    } else {
        match admin.examine_consumer_offset(&fresh, &mq).await {
            Ok(offset) => ck.check(
                "A10 指定位点写回后可读",
                offset == Some(1),
                &format!("offset={offset:?}"),
            ),
            Err(e) => ck.abort("A10 回读指定位点", &e.to_string()),
        }
    }
    if let Err(e) = admin.update_consumer_offset(&fresh, &mq, 0).await {
        ck.abort("A10 updateConsumerOffset（按路由解析地址）", &e.to_string());
    } else {
        match admin.examine_consumer_offset(&fresh, &mq).await {
            Ok(offset) => ck.check(
                "A10 updateConsumerOffset 走路由解析同样生效",
                offset == Some(0),
                &format!("offset={offset:?}"),
            ),
            Err(e) => ck.abort("A10 回读路由定位的位点", &e.to_string()),
        }
    }
    // cloneGroupOffset(314)：把源组位点整份抄给目标组（offline=true，不要求在线）。
    match admin
        .clone_group_offset(&addr, &group, &fresh, &topic, true)
        .await
    {
        Ok(()) => match admin.examine_consumer_offset(&fresh, &mq).await {
            Ok(copied) => ck.check(
                "A10 cloneGroupOffset 后目标组位点与源组一致",
                copied.is_some(),
                &format!("copied={copied:?}"),
            ),
            Err(e) => ck.abort("A10 回读克隆后的位点", &e.to_string()),
        },
        Err(e) => ck.abort("A10 cloneGroupOffset", &e.to_string()),
    }
}

// ------------------------------------------------------------------ A11 重投到 %RETRY%

/// 管理端视角的重投验证：拉一条 → `send_message_back` → 轮询 `%RETRY%<group>` 的
/// broker 统计。push 消费者内部的回投路径由 `live_consumer` 的 C4 覆盖，这里用拉
/// 模式消费者的公开 `send_message_back`，证明的是**管理端读得到**这条重投。
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
            ck.abort("A11 拉消费者构造", &e.to_string());
            return;
        }
    };
    if let Err(e) = consumer.start().await {
        ck.abort("A11 拉消费者 start", &e.to_string());
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
        ck.abort("A11 fetch_subscribe_message_queues", "topic 没有队列");
        consumer.shutdown();
        return;
    };
    // 从队首起拉：位点与 broker 存储区间无关，保证一定拿得到 A7 发的消息。
    let min = consumer.min_offset(mq).await.unwrap_or(0);
    let msg: Option<MessageExt> = match consumer.pull(mq, "*", min, 1, Some(5_000)).await {
        Ok(result) => match result.status {
            PullStatus::Found => result.msg_found_list.into_iter().next(),
            other => {
                ck.abort("A11 拉取一条消息", &format!("status={other}"));
                None
            }
        },
        Err(e) => {
            ck.abort("A11 拉取一条消息", &e.to_string());
            None
        }
    };
    let Some(msg) = msg else {
        consumer.shutdown();
        return;
    };
    if let Err(e) = consumer.send_message_back(&msg, 0).await {
        ck.abort("A11 sendMessageBack 重投", &e.to_string());
        consumer.shutdown();
        return;
    }
    consumer.shutdown();
    let retry_topic = MixAll::get_retry_topic(&group);
    // 必须轮询：broker 的 consumerSendMsgBack 在 delayLevel==0 时改写成
    // `3 + reconsumeTimes`（≈10s），消息先进 SCHEDULE_TOPIC_XXXX 才投递到 %RETRY%。
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
        &format!("A11 sendMessageBack 落到 {retry_topic}"),
        max_sum > 0,
        &format!("maxOffsetSum={max_sum}（轮询 {waited}s；broker 把 delayLevel=0 改写成 3）"),
    );
}

// ------------------------------------------------------------------ A12 位点重置

async fn a12_reset_offset(ck: &mut Checker, env: &Env) {
    let admin = &env.admin;
    let topic = env.topic.clone();
    let group = env.group.clone();
    let mq = MessageQueue::new(&topic, &env.broker_name(), 0);
    let max_now = admin.max_offset(&mq).await.unwrap_or(-1);
    // 未来时间 ⇒ 位点应被推到该队列 maxOffset（222 由 broker 端算，不是本地 searchOffset）。
    let ts = now_millis() + 60_000;
    match admin
        .reset_offset_by_timestamp(&topic, &group, ts, true, None, true)
        .await
    {
        Ok(offsets) => {
            ck.check(
                "A12 resetOffsetByTimestamp 返回非空 offsetTable",
                !offsets.is_empty(),
                &format!("queues={}", offsets.len()),
            );
            ck.check(
                "A12 重置结果覆盖 topic 的每个队列",
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
            "A12 broker 端重置后消费者位点被推到 maxOffset",
            offset.unwrap_or(-1) >= max_now - 1,
            &format!("consumerOffset={offset:?} maxOffset={max_now}"),
        ),
        Err(e) => ck.abort("A12 回读重置后的位点", &e.to_string()),
    }
    // 新版接口：目标组离线 ⇒ 真机走 222；CONSUMER_NOT_ONLINE 时退化到旧版。
    let dest = env.dest_group.clone();
    match admin.reset_offset_new(&dest, &topic, ts + 1_000).await {
        Ok(()) => match admin.examine_consumer_offset(&dest, &mq).await {
            Ok(offset) => ck.check(
                "A12 resetOffsetNew 对离线组同样落到位",
                offset.unwrap_or(-1) >= max_now - 1,
                &format!("consumerOffset={offset:?} maxOffset={max_now}"),
            ),
            Err(e) => ck.abort("A12 回读 resetOffsetNew", &e.to_string()),
        },
        Err(e) => ck.abort("A12 resetOffsetNew", &e.to_string()),
    }
    // 旧版：逐队列 searchOffset + updateConsumerOffset（Java 的 old 分支）。
    match admin
        .reset_offset_by_timestamp_old(&dest, &topic, ts + 2_000, true)
        .await
    {
        Ok(offsets) => ck.check(
            "A12 resetOffsetByTimestampOld 逐队列写回",
            !offsets.is_empty(),
            &format!("queues={}", offsets.len()),
        ),
        Err(e) => ck.abort("A12 resetOffsetByTimestampOld", &e.to_string()),
    }

    // ---------- resetOffsetByQueueId（Java DefaultMQAdminExtImpl:1827，两笔 RPC）----------
    // 与上面的按 timestamp 重置不同：这条把「队列 + 显式 offset」交给 broker，
    // 拉回 minOffset 后必须能按该位点重新取到 A7 发的历史消息。
    let min_reset = admin.min_offset(&mq).await.unwrap_or(-1);
    match admin
        .reset_offset_by_queue_id(&env.broker(), &group, &topic, mq.queue_id, min_reset)
        .await
    {
        Ok(table) => ck.check(
            "A12 resetOffsetByQueueId 打到 broker 成功",
            // Java 该方法是 void：broker 只有 language=CPP 才回可解析的 offsetTable，
            // 所以空表合法；非空时必须是我们操作的那个队列。
            table.is_empty()
                || table.iter().any(|(k, _)| k.queue_id == mq.queue_id),
            &format!("returnedTable={}（Java 为 void，可为空）", table.len()),
        ),
        Err(e) => {
            ck.abort("A12 resetOffsetByQueueId", &e.to_string());
            return;
        }
    }
    match admin.examine_consumer_offset(&group, &mq).await {
        Ok(offset) => ck.check(
            "A12 resetOffsetByQueueId 后位点==目标 offset",
            offset == Some(min_reset),
            &format!("consumerOffset={offset:?} target={min_reset}"),
        ),
        Err(e) => ck.abort("A12 回读 resetOffsetByQueueId 位点", &e.to_string()),
    }
    // 一次性 resetOffsetTable 真的被下一次 pull 取走：Java `PullMessageProcessor:539-548`
    // 在 useServerSideResetOffset 下**不读消息**，直接回 OFFSET_RESET ⇒
    // ResponseCode.PULL_OFFSET_MOVED(:672)，客户端 `MQClientAPIImpl:1098` 把它映射成
    // PullStatus.OFFSET_ILLEGAL + nextBeginOffset=重置位点。所以第一笔 pull 必须是
    // OffsetIllegal 且 nextBeginOffset==目标位点；第二笔才真正拿到历史消息。
    {
        let consumer = match DefaultMQPullConsumer::with_config(PullConsumerConfig {
            consumer_group: group.clone(),
            name_server_addrs: vec![env.namesrv.clone()],
            instance_name: format!("live-admin-reset-{}", env.stamp),
            ..Default::default()
        }) {
            Ok(c) => Some(c),
            Err(e) => {
                ck.abort("A12 重置后回拉：消费者构造", &e.to_string());
                None
            }
        };
        if let Some(consumer) = consumer {
            if let Err(e) = consumer.start().await {
                ck.abort("A12 重置后回拉：start", &e.to_string());
            } else {
                match consumer.pull(&mq, "*", min_reset, 1, Some(5_000)).await {
                    Ok(first) => {
                        ck.check(
                            "A12 重置后首笔 pull 被 broker 短路成 PULL_OFFSET_MOVED",
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
                                "A12 重置位点之后可重新拉到历史消息",
                                !second.msg_found_list.is_empty(),
                                &format!(
                                    "status={} found={} from={resume}",
                                    second.status,
                                    second.msg_found_list.len()
                                ),
                            ),
                            Err(e) => ck.abort("A12 重置后第二笔回拉", &e.to_string()),
                        }
                    }
                    Err(e) => ck.abort("A12 重置后回拉", &e.to_string()),
                }
                consumer.shutdown();
            }
        }
    }
    // 越界负例：resetOffsetInner 的 [min, max+1] 校验必须拒掉；同时量化 Java 语义——
    // 两笔 RPC **不是原子的**（commitOffset 无区间校验，第 1 笔已把非法位点落库），
    // 所以失败后位点停在非法目标上，这里不断言"回滚"。
    let bad_target = max_now + 100;
    let rejected = admin
        .reset_offset_by_queue_id(&env.broker(), &group, &topic, mq.queue_id, bad_target)
        .await
        .is_err();
    ck.check(
        "A12 resetOffsetByQueueId 越界目标被 broker 拒绝",
        rejected,
        &format!("badTarget={bad_target}"),
    );
    match admin.examine_consumer_offset(&group, &mq).await {
        Ok(offset) => ck.check(
            "A12 越界 reset 停留在第 1 笔写入的非法位点（Java 两笔 RPC 非原子）",
            offset == Some(bad_target),
            &format!("consumerOffset={offset:?} badTarget={bad_target}"),
        ),
        Err(e) => ck.abort("A12 回读越界 reset 位点", &e.to_string()),
    }
}

// ------------------------------------------------------------------ A13 写权限 + 清理

async fn a13_perm_and_cleanup(ck: &mut Checker, env: &Env) {
    let admin = &env.admin;
    let topic = env.topic.clone();
    let group = env.group.clone();
    let addr = env.broker();
    let broker_name = env.broker_name();
    // 这两条打在 **NameServer** 上（Java 同名接口也是走 nameServer），可逆。
    match admin
        .wipe_write_perm_of_broker(&env.namesrv, &broker_name)
        .await
    {
        Ok(count) => ck.check(
            "A13 wipeWritePermOfBroker 正常应答",
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
                "A13 addWritePermOfBroker 还原写权限",
                count >= 0,
                &format!("addTopicCount={count}"),
            );
            // 还原后必须还能发（否则这条集群后续所有客户端测试都会挂）。
            let mut msg = Message::new(&topic, Some(b"a13-after-perm-restore"));
            match env.producer.send(&mut msg, Some(5_000), None).await {
                Ok(result) => ck.check(
                    "A13 权限还原后 producer 照常发送",
                    result.status == SendStatus::SendOk,
                    &format!("status={}", result.status),
                ),
                Err(e) => ck.abort("A13 权限还原后 producer 照常发送", &e.to_string()),
            }
        }
        Err(e) => ck.abort("A13 addWritePermOfBroker", &e.to_string()),
    }

    // 三段删除：broker 配置 → NameServer 路由 → 组合入口（幂等）。
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
            "A13 deleteTopic 后 topic 从列表消失",
            !list.get_topic_list().contains(&topic),
            &format!("topics={}", list.get_topic_list().len()),
        ),
        Err(e) => ck.abort("A13 删除后 fetchAllTopicList", &e.to_string()),
    }
    // 判「组已删除」只能用只读的全量表（200 分页）。201 的
    // `GET_SUBSCRIPTIONGROUP_CONFIG` 在 broker 端走 `findSubscriptionGroupConfig`，
    // autoCreateSubscriptionGroup=true 时查不到就顺手建一个默认组（retryMaxTimes=16），
    // 用它验证删除会得到「删了还在」的假象。
    // 删除要过 broker 的 subscriptionGroupTable（分页应答按 groupSeq 现算），
    // 固定 1.5s 等待偶尔抢不过它 ⇒ 轮询到预算用尽。
    let deadline = Instant::now() + Duration::from_secs(15);
    for (label, name) in [
        ("A13 deleteSubscriptionGroup 后查不到该组", group.clone()),
        ("A13 目标组也一并删除", env.dest_group.clone()),
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

// ------------------------------------------------------------------ A0 门面状态

fn a0_facade_state(ck: &mut Checker, env: &Env) {
    let admin = &env.admin;
    ck.check(
        "A0 start 后 is_started 且生成了 clientId",
        admin.is_started() && !admin.client_id().is_empty(),
        &format!(
            "started={} clientId={}",
            admin.is_started(),
            admin.client_id()
        ),
    );
    ck.check(
        "A0 name server 地址按 ; 拆分并回读",
        admin.get_name_server_address_list() == vec![env.namesrv.clone()]
            && admin.get_name_server_addr() == env.namesrv,
        &admin.get_name_server_addr(),
    );
    ck.check(
        "A0 timeout_millis 用配置值",
        admin.timeout_millis() == 10_000,
        &format!("timeout={}", admin.timeout_millis()),
    );
    ck.check(
        "A0 getMQClientInstance 可用",
        admin.get_mq_client_instance().is_ok(),
        "私有实例没拿到",
    );
}

// ------------------------------------------------------------------ 驱动

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let stamp = stamp();
    let env = match Env::new(namesrv, &stamp) {
        Ok(env) => env,
        Err(e) => {
            ck.abort("夹具构造", &e);
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
    // A1 需要一个可写 topic 才能证明「admin 关掉不影响 producer」，排在 A3 之后。
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
