//! `MQClientInstance` 对**真实 5.5.1 broker** 的联调验证。
//!
//! 与另两个 live 示例的分工：`live_protocol.rs` 打协议编解码，
//! `live_client_modules.rs` 打取址/容错/统计/钩子骨架，
//! `live_rebalance_and_trace.rs` 打重平衡/线程池/轨迹链路。这里打**实例本身** ——
//! 它是 producer / consumer / admin 三层唯一的出口，所以「请求发出去了、响应回来了」
//! 这两件事不足以判合格。本示例锁死的是只有真集群能证明的性质：
//!
//! - M1 **实例身份与生命周期**：clientId 登记进进程级实例表、同 id 复用同一实例、
//!   `start` / `shutdown` 翻转 `started` 且对所有 handle 可见、`shutdown` 之后登记表
//!   里不再留死的实例、空 namesrv 列表不改写
//!   地址、没配地址服务器时 `fetch_name_server_addr` 回 `None`（Python 同）。
//! - M2 **路由与发布信息缓存**：真实路由落库后 `TopicPublishInfo` 的队列视图、
//!   轮询游标（含 `reset_index` 与过滤器语义：过滤全拒回 `None` 而不是报错）、
//!   brokerName→addr 解析、未知 topic 在 `is_default=true` 时按 Java **生产者**语义
//!   回退到 `TBW102` 并真能把消息发进去；消费者路径（`is_default=false`）禁止回退。
//! - M3 **收发**：`send_message` 自己解析路由，`SendResult` 逐字段对齐 Python
//!   `_parse_send_response`；UNIQ_KEY 由实例补写且调用方看得见；批量走 `batch` 标志、
//!   在 broker 上落成一条物理消息；oneway 之后必须能被拉回。
//! - M4 **位点**：`query/update/max/min/searchOffset` 五个 RPC 对真实队列成立，
//!   `setZeroIfNotFound` 与 broker 行为一致。
//! - M5 **注册表与心跳**：`RegisteredConsumer` 注入后 `prepare_heartbeat_data` 的
//!   JSON 字段名、心跳真被 broker 采纳（用 GET_CONSUMER_LIST_BY_GROUP 反查客户端 ID
//!   证明）、`persist_consumer_offsets` 与 `adjust_thread_pool` 真的逐个调到消费者。
//! - M6 **POP 链路**：弹回来的消息必须带上客户端反构的 8 段 `POP_CK` 与
//!   `1ST_POP_TIME`；用它 ACK 成功；`change_invisible_time` 返回的**新** extraInfo
//!   能直接用于后续 ACK；全部确认后同组再弹拿不到这些消息。
//! - M7 **管理与清理**：集群信息 / namesrv topic 列表 / 队列批量锁（含「锁真互斥」
//!   的反证）/ 注销，最后删掉本次建的 topic。
//! - M8 **共用实例的关闭守卫**（Java `MQClientInstance#shutdown` 读的那三张表 +
//!   `removeClientFactory`）：同 clientId 的两个生产者与一个 lite 消费者共用一份实例，
//!   先退的门面不能把还在用的心跳、路由刷新和连接拆掉（用「兄弟退出后另一个仍能真发消息」
//!   证明），最后一个退掉时实例要真拆并**从进程级登记表摘掉**，同 clientId 才能拿到新实例。
//!   排在 M7 之前跑，因为要用 M7 删掉的 topic 发消息。
//!
//! ⚠ 未覆盖：broker 主动请求（220/221/307/309/326）无法从外部注入 —— 它们走 broker
//! 已建立的那条连接。协议与分派由 `mq_client.rs` 的离线单测覆盖（对齐
//! `python/tests/test_broker_requests.py`），这里只验 `RegisteredConsumer` seam
//! 在实例侧的调用面。
//!
//! 用法（先按项目 README 起本地集群）：
//! ```text
//! cargo run --example live_mq_client -- 127.0.0.1:9876
//! ```

use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::consumer_stats::ConsumerStatsManager;
use rocketmq_client_remoting::client::latency::QueueFilter;
use rocketmq_client_remoting::client::mq_client::{
    ConsumerFuture, MQClientInstance, MQClientInstanceConfig, PublishMessage, RegisteredConsumer,
};
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultLitePullConsumer, LitePullConsumerConfig,
};
use rocketmq_client_remoting::client::result::{PopStatus, PullStatus, SendStatus};
use rocketmq_client_remoting::common::message::{Message, MessageBatch, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::message_client_id_setter::get_uniq_id;
use rocketmq_client_remoting::common::message_const::{
    PROPERTY_FIRST_POP_TIME, PROPERTY_KEYS, PROPERTY_POP_CK, PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::sysflag::{ConsumeInitMode, PullSysFlag};
use rocketmq_client_remoting::common::topic_config::{self, TopicFilterType};
use rocketmq_client_remoting::common::util_all::current_time_millis;
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::client::RemotingClient;
use rocketmq_client_remoting::remoting::protocol::body::{
    ConsumeMessageDirectlyResult, ConsumerRunningInfo,
};
use rocketmq_client_remoting::remoting::protocol::codes::{request_code, response_code};
use rocketmq_client_remoting::remoting::protocol::extra_info;
use rocketmq_client_remoting::remoting::protocol::headers::{
    CreateTopicRequestHeader, GetConsumerListByGroupRequestHeader, GetRouteInfoRequestHeader,
};
use rocketmq_client_remoting::remoting::protocol::heartbeat::{
    ConsumeFromWhere, ConsumeType, ExpressionType, MessageModel, SubscriptionData,
};
use rocketmq_client_remoting::remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;
use rocketmq_client_remoting::remoting::protocol::serialize::RemotingSerializable;

/// 建出来的 topic 队列数（对齐 Python `create_topic_in_broker` 默认 4）。
const QUEUE_NUMS: i32 = 4;
/// M3 走轮询发送的消息条数（正好覆盖 4 个队列两轮）。
const MSG_TOTAL: i32 = 8;
/// M6 POP 一次弹多少条 / broker 挂起等待时间 / 不可见时间。
const POP_MAX_MSGS: i32 = 32;
const POP_POLL_TIME: i64 = 1_000;
/// 不可见时间给足，保证「不 ack 就重投」不会在测试中途发生。
const POP_INVISIBLE_TIME: i64 = 60_000;
/// `change_invisible_time` 的续期时长。
const RENEW_INVISIBLE_TIME: i64 = 30_000;

// ------------------------------------------------------------------ 骨架

fn stamp() -> String {
    let secs = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => 0,
    };
    format!("{secs}")
}

/// 断言累积器：一次跑完所有场景再汇总，首个失败不提前退出。
struct Checker {
    passed: u32,
    failed: Vec<String>,
}

impl Checker {
    fn new() -> Checker {
        Checker { passed: 0, failed: Vec::new() }
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

type Live = Result<(), String>;

/// 锁中毒时照常取内值：别处的 panic 不该让联调现场一起丢。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// 轮询异步条件直到成立，超时返回 `false` 让调用方打出现场。
async fn wait_async<F, Fut>(mut cond: F, budget: Duration) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + budget;
    loop {
        if cond().await {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn rpc(
    client: &RemotingClient,
    addr: &str,
    mut cmd: RemotingCommand,
    timeout_millis: i64,
) -> Result<RemotingCommand, String> {
    client
        .invoke_sync(addr, &mut cmd, Some(timeout_millis))
        .await
        .map_err(|e| format!("invoke {addr} failed: {e}"))
}

async fn get_route(
    client: &RemotingClient,
    namesrv: &str,
    topic: &str,
) -> Result<RemotingCommand, String> {
    let req = RemotingCommand::create_request_command(
        request_code::GET_ROUTEINFO_BY_TOPIC,
        Some(Box::new(GetRouteInfoRequestHeader {
            topic: Some(topic.to_string()),
            accept_standard_json_only: None,
        })),
    );
    rpc(client, namesrv, req, 5000).await
}

fn decode_route(resp: &RemotingCommand, what: &str) -> Result<TopicRouteData, String> {
    let raw = match resp.body() {
        Some(b) => b.to_vec(),
        None => return Err(format!("{what} response has empty body")),
    };
    // namesrv 的 route 是 fastjson 风格（数字 map 键不带引号），必须走兼容解码器。
    let value = RemotingSerializable::decode(&raw)
        .map_err(|e| format!("{what} body is not parsable: {e}"))?;
    TopicRouteData::from_json_value(&value).map_err(|e| format!("{what} route invalid: {e}"))
}

/// 建 topic（走 TBW102 路由找 broker），与另两个 live 示例同一套做法。
async fn bootstrap_topic(client: &RemotingClient, namesrv: &str, topic: &str) -> Live {
    let resp = get_route(client, namesrv, MixAll::DEFAULT_TOPIC).await?;
    if resp.code != response_code::SUCCESS {
        return Err(format!(
            "no route of default topic {}: code={} remark={:?}",
            MixAll::DEFAULT_TOPIC,
            resp.code,
            resp.remark
        ));
    }
    let route = decode_route(&resp, "TBW102")?;
    let broker_addr = match route.broker_datas.first().and_then(|b| b.select_broker_addr()) {
        Some(a) => a,
        None => return Err("default route has no usable broker address".to_string()),
    };
    let resp = rpc(
        client,
        &broker_addr,
        RemotingCommand::create_request_command(
            request_code::UPDATE_AND_CREATE_TOPIC,
            Some(Box::new(CreateTopicRequestHeader {
                topic: Some(topic.to_string()),
                default_topic: Some(MixAll::DEFAULT_TOPIC.to_string()),
                read_queue_nums: Some(QUEUE_NUMS),
                write_queue_nums: Some(QUEUE_NUMS),
                perm: Some(topic_config::DEFAULT_PERM),
                topic_filter_type: Some(TopicFilterType::SINGLE_TAG.to_string()),
                topic_sys_flag: Some(0),
                order: Some(false),
                attributes: Some(String::new()),
                force: Some(false),
            })),
        ),
        5000,
    )
    .await?;
    if resp.code != response_code::SUCCESS {
        return Err(format!(
            "create topic {topic} on {broker_addr} failed: code={} remark={:?}",
            resp.code, resp.remark
        ));
    }
    Ok(())
}

/// 一条待发消息。UNIQ_KEY 故意**不**预先设置：M3 要验实例会补。
fn build_message(topic: &str, body: &[u8], tags: &str, keys: &str) -> Message {
    let mut msg = Message::new(topic, Some(body));
    if !tags.is_empty() {
        msg.set_tags(tags);
    }
    if !keys.is_empty() {
        msg.set_keys(keys);
    }
    msg
}

/// 一次普通发送（实例侧），返回 `SendResult`。
async fn send_one(
    instance: &MQClientInstance,
    producer_group: &str,
    msg: &mut Message,
    mq: &MessageQueue,
) -> Result<rocketmq_client_remoting::client::result::SendResult, String> {
    let mut pm = PublishMessage::Single(msg);
    instance
        // unitMode=false：本例是实例级裸发送，不模拟 unit mode
        .send_message(
            producer_group,
            &mut pm,
            mq,
            10_000,
            0,
            false,
            MixAll::DEFAULT_TOPIC,
            MixAll::DEFAULT_TOPIC_QUEUE_NUMS,
        )
        .await
        .map_err(|e| format!("send to {} queue {} failed: {e}", mq.topic, mq.queue_id))
}

/// 实例侧 `pull_message` 的常用形状：订阅表达式 `*`、不带悬挂等待。
async fn pull_from(
    instance: &MQClientInstance,
    consumer_group: &str,
    mq: &MessageQueue,
    from_offset: i64,
    broker_addr: &str,
) -> Result<rocketmq_client_remoting::client::result::PullResult, String> {
    instance
        .pull_message(
            consumer_group,
            mq,
            from_offset,
            POP_MAX_MSGS,
            PullSysFlag::build_sys_flag_basic(false, false, true, false),
            0,
            "*",
            0,
            ExpressionType::TAG,
            30_000,
            -1,
            0,
            Some(broker_addr),
            0,
        )
        .await
        .map_err(|e| format!("pull {} queue {} failed: {e}", mq.topic, mq.queue_id))
}

fn bodies(msgs: &[MessageExt]) -> Vec<String> {
    msgs.iter()
        .map(|m| String::from_utf8_lossy(&m.body.clone().unwrap_or_default()).into_owned())
        .collect()
}

fn queue_ids(mqs: &[MessageQueue]) -> Vec<i32> {
    mqs.iter().map(|q| q.queue_id).collect()
}

/// 直接问 broker 某个 group 的成员（不经实例：证明「注册成功」是 broker 侧的事实）。
async fn listed_consumer_ids(
    client: &RemotingClient,
    broker_addr: &str,
    consumer_group: &str,
) -> Vec<String> {
    let request = RemotingCommand::create_request_command(
        request_code::GET_CONSUMER_LIST_BY_GROUP,
        Some(Box::new(GetConsumerListByGroupRequestHeader {
            consumer_group: Some(consumer_group.to_string()),
        })),
    );
    let Ok(response) = rpc(client, broker_addr, request, 5000).await else {
        return Vec::new();
    };
    let Some(body) = response.body() else {
        return Vec::new();
    };
    let Ok(value) = RemotingSerializable::decode(body) else {
        return Vec::new();
    };
    value
        .get("consumerIdList")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

// ------------------------------------------------- 注入给实例的消费者替身

/// 只实现 [`RegisteredConsumer`] 契约的最小消费者（真实消费者还没移植）。
/// 作用是让实例侧的调用面（心跳数据、位点持久化、线程巡检、按 group 分派）
/// 在真集群上走通，并让 broker 能反查到这个客户端 ID。
struct StubConsumer {
    client_id: String,
    group: String,
    topic: String,
    broker_name: String,
    /// `persist_consumer_offset` 被调了几次（M5 断言）。
    persisted: AtomicUsize,
    /// `adjust_thread_pool` 被调了几次。
    adjusted: AtomicUsize,
    /// 220 落下来的 (topic, 队列数)；离线单测用得上，这里只保证可实现。
    resets: Mutex<Vec<(String, usize)>>,
}

impl StubConsumer {
    fn new(client_id: &str, group: &str, topic: &str, broker_name: &str) -> Arc<StubConsumer> {
        Arc::new(StubConsumer {
            client_id: client_id.to_string(),
            group: group.to_string(),
            topic: topic.to_string(),
            broker_name: broker_name.to_string(),
            persisted: AtomicUsize::new(0),
            adjusted: AtomicUsize::new(0),
            resets: Mutex::new(Vec::new()),
        })
    }
}

impl RegisteredConsumer for StubConsumer {
    fn client_id(&self) -> String {
        self.client_id.clone()
    }

    fn consumer_group(&self) -> String {
        self.group.clone()
    }

    fn consume_type(&self) -> String {
        ConsumeType::CONSUME_PASSIVELY.to_string()
    }

    fn message_model(&self) -> String {
        MessageModel::CLUSTERING.to_string()
    }

    fn consume_from_where(&self) -> String {
        ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET.to_string()
    }

    fn is_unit_mode(&self) -> bool {
        false
    }

    fn subscription(&self) -> Vec<String> {
        vec![self.topic.clone()]
    }

    fn subscriptions(&self) -> Vec<SubscriptionData> {
        let mut sub = SubscriptionData::new(self.topic.clone(), "*");
        sub.tags_set = vec!["*".to_string()];
        vec![sub]
    }

    fn adjust_thread_pool(&self) {
        self.adjusted.fetch_add(1, Ordering::SeqCst);
    }

    fn reset_offset(
        self: Arc<Self>,
        topic: String,
        offset_table: Vec<(MessageQueue, i64)>,
    ) -> ConsumerFuture<()> {
        Box::pin(async move {
            lock(&self.resets).push((topic, offset_table.len()));
            Ok(())
        })
    }

    fn get_consumer_status(&self, topic: Option<&str>) -> Vec<(MessageQueue, i64)> {
        vec![(
            MessageQueue::new(topic.unwrap_or("<absent>"), &self.broker_name, 0),
            7,
        )]
    }

    fn consumer_running_info(&self) -> ConsumerRunningInfo {
        let mut info = ConsumerRunningInfo::default();
        info.properties.insert(
            ConsumerRunningInfo::PROP_NAMESERVER_ADDR.to_string(),
            "stub-namesrv".to_string(),
        );
        info
    }

    fn consume_message_directly(
        &self,
        msg: MessageExt,
        _broker_name: Option<String>,
    ) -> std::result::Result<ConsumeMessageDirectlyResult, Error> {
        let result = ConsumeMessageDirectlyResult {
            consume_result: Some(
                if msg.body.is_some() { "SUCCESS" } else { "FAILED" }.to_string(),
            ),
            ..Default::default()
        };
        Ok(result)
    }

    fn persist_consumer_offset(self: Arc<Self>) -> ConsumerFuture<()> {
        Box::pin(async move {
            self.persisted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------- M1

/// 实例身份与生命周期。
async fn m1_identity(namesrv: &str, ck: &mut Checker) -> Live {
    let run = stamp();
    let client_id = format!("rust-live-mqclient-{run}@m1");
    let other_id = format!("rust-live-mqclient-{run}@other");
    let instance = MQClientInstance::new(&client_id, vec![namesrv.to_string()]);

    ck.check(
        "M1 the instance registers itself under its clientId, like Python INSTANCE_MAP",
        MQClientInstance::find_instance(&client_id).is_some(),
        "find_instance returned None",
    );
    ck.check(
        "M1 an unknown clientId is not in the instance table",
        MQClientInstance::find_instance(&other_id).is_none(),
        "find_instance returned Some",
    );
    ck.check(
        "M1 the handle carries its clientId and the namesrv list it was built with",
        instance.client_id() == client_id && instance.name_server_addrs() == vec![namesrv.to_string()],
        &format!("{} / {:?}", instance.client_id(), instance.name_server_addrs()),
    );

    // Java `createMQClientInstance`：同 id 复用存活实例（Python 只写不读）。
    let reused = MQClientInstance::create_mq_client_instance(
        &client_id,
        vec!["10.0.0.1:9876".to_string()],
        MQClientInstanceConfig::default(),
    );
    ck.check(
        "M1 create_mq_client_instance reuses the live instance instead of building a second one",
        reused.name_server_addrs() == vec![namesrv.to_string()],
        &format!("addrs {:?}", reused.name_server_addrs()),
    );

    ck.check(
        "M1 start() flips the flag and every handle sees it",
        !instance.is_started() && reused.start().await.is_ok() && instance.is_started(),
        "start failed or flag not visible",
    );
    reused.shutdown();
    instance.shutdown();
    instance.shutdown();
    ck.check(
        "M1 shutdown() is shared through every handle and unregisters the factory",
        !instance.is_started() && MQClientInstance::find_instance(&client_id).is_none(),
        "flag stuck or the factory stayed in INSTANCE_MAP",
    );
    // Java `removeClientFactory`：摘掉登记后同 clientId 拿到的是干净的新实例，
    // 复用的工厂不会把一个已关掉的死实例发给后来者。
    let rebuilt = MQClientInstance::create_mq_client_instance(
        &client_id,
        vec![namesrv.to_string()],
        MQClientInstanceConfig::default(),
    );
    ck.check(
        "M1 a fresh factory takes over the clientId after shutdown",
        rebuilt.start().await.is_ok() && rebuilt.is_started(),
        "the replacement factory could not start",
    );
    rebuilt.shutdown();

    // 动态取址：没配地址服务器时 Python `fetch_name_server_addr` 直接返回 None。
    let fresh = MQClientInstance::new(&format!("{client_id}-fresh"), vec![namesrv.to_string()]);
    let fetched = fresh
        .fetch_name_server_addr()
        .await
        .map_err(|e| format!("fetch_name_server_addr failed: {e}"))?;
    ck.check(
        "M1 without an address server fetch_name_server_addr yields None and keeps the addrs",
        fetched.is_none() && fresh.name_server_addrs() == vec![namesrv.to_string()],
        &format!("{fetched:?} {:?}", fresh.name_server_addrs()),
    );
    // Python `update_name_server_address_list`：空列表一律不改。
    fresh.update_name_server_address_list(&[]);
    ck.check(
        "M1 an empty namesrv update is a no-op, like Python",
        fresh.name_server_addrs() == vec![namesrv.to_string()],
        &format!("{:?}", fresh.name_server_addrs()),
    );
    fresh.update_name_server_address_list(&[namesrv.to_string(), "127.0.0.1:9877".to_string()]);
    ck.check(
        "M1 a non-empty namesrv update replaces the whole list",
        fresh.name_server_addrs().len() == 2,
        &format!("{:?}", fresh.name_server_addrs()),
    );
    // 注入与默认构造要在 Debug 上分得开（配置对象会被上层复用）。
    let cfg = MQClientInstanceConfig {
        consumer_stats_manager: Some(Arc::new(ConsumerStatsManager::new())),
        ..MQClientInstanceConfig::default()
    };
    let debug = format!("{cfg:?}");
    ck.check(
        "M1 MQClientInstanceConfig::Debug distinguishes injected seams from defaults",
        debug.contains("consumer_stats_manager: \"injected\"")
            && debug.contains("trace_dispatcher: \"default\""),
        &debug,
    );
    // 关掉后台任务不该影响别的实例（各实例持有独立的 RemotingClient）。
    let second = MQClientInstance::new(&other_id, vec![namesrv.to_string()]);
    let fresh_started = fresh.start().await.is_ok();
    ck.check(
        "M1 a second clientId really is a second instance",
        fresh_started && second.client_id() == other_id && fresh.is_started() && !second.is_started(),
        &format!(
            "id={} expected={other_id} freshStarted={} secondStarted={}",
            second.client_id(),
            fresh.is_started(),
            second.is_started()
        ),
    );
    fresh.shutdown();
    second.shutdown();
    Ok(())
}

// ---------------------------------------------------------------------- M2

/// 路由缓存 + 发布信息的队列视图与轮询游标。
#[allow(clippy::too_many_arguments)]
async fn m2_route_and_publish(
    instance: &MQClientInstance,
    topic: &str,
    fallback_topic: &str,
    broker_name: &str,
    broker_addr: &str,
    producer_group: &str,
    ck: &mut Checker,
) -> Live {
    let refreshed = instance
        .update_topic_route_info_from_name_server(topic, 5000, false)
        .await
        .map_err(|e| format!("route refresh failed: {e}"))?;
    ck.check(
        "M2 update_topic_route_info_from_name_server reports the route landed",
        refreshed,
        "returned false",
    );

    let route = match instance.get_topic_route_data(topic).await {
        Some(r) => r,
        None => return Err("route cache miss after a successful refresh".to_string()),
    };
    // 真实路由里 QueueData 是**每台 broker 一条**（带 read/write 队列数），
    // 由客户端展开成 queueId 0..n-1；不是一队列一条。
    ck.check(
        "M2 the cached route is the broker's own answer, not a synthesized one",
        route.queue_datas.len() == 1
            && route.broker_datas.len() == 1
            && route.broker_datas[0].broker_name == broker_name
            && route.queue_datas[0].broker_name == broker_name
            && route.queue_datas[0].read_queue_nums == QUEUE_NUMS
            && route.queue_datas[0].write_queue_nums == QUEUE_NUMS
            && route.queue_datas[0].perm == 6,
        &format!(
            "queueDatas={:?} broker={}",
            route.queue_datas, route.broker_datas[0].broker_name
        ),
    );
    ck.check(
        "M2 brokerName -> addr resolves through the cache exactly like Python's helper",
        MQClientInstance::find_broker_addr_in_route(&route, broker_name).as_deref()
            == Some(broker_addr)
            && instance.broker_addr_of(broker_name).as_deref() == Some(broker_addr)
            && instance.broker_addr_of("no-such-broker").is_none(),
        &format!(
            "cache={:?} all={:?}",
            instance.broker_addr_of(broker_name),
            instance.get_route_of_all_brokers()
        ),
    );
    ck.check(
        "M2 get_route_of_all_brokers lists every broker of every cached route",
        instance.get_route_of_all_brokers().contains(&broker_addr.to_string()),
        &format!("{:?}", instance.get_route_of_all_brokers()),
    );

    let publish = instance
        .get_topic_publish_info(topic, false)
        .await
        .map_err(|e| format!("publish info unavailable: {e}"))?;
    ck.check(
        "M2 publish info exposes exactly the writable queues of the route",
        publish.ok()
            && queue_ids(&publish.msg_queue_list()) == vec![0, 1, 2, 3]
            && !publish.order_topic()
            && publish.topic_route_data().is_some(),
        &format!("{:?}", publish.to_dict()),
    );
    ck.check(
        "M2 to_dict keeps Python's publish-info shape (orderTopic + messageQueueList only)",
        serde_json::to_string(&publish.to_dict())
            .unwrap_or_default()
            .starts_with(r#"{"orderTopic":false,"messageQueueList":[{"topic":"#,),
        &format!("{:?}", publish.to_dict()),
    );

    // 轮询游标：两轮 4 个队列 ⇒ 0,1,2,3,0,1,2,3；reset 后从头开始。
    let no_filters: &[&QueueFilter<'_>] = &[];
    let mut cursor: Vec<i32> = Vec::new();
    for _ in 0..MSG_TOTAL {
        let mq = publish
            .select_one_message_queue(no_filters)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "unfiltered selection returned None".to_string())?;
        cursor.push(mq.queue_id);
    }
    ck.check(
        "M2 select_one_message_queue round-robins the whole ring, twice",
        cursor == vec![0, 1, 2, 3, 0, 1, 2, 3],
        &format!("{cursor:?}"),
    );
    publish.reset_index();
    let after_reset = publish
        .select_one_message_queue(no_filters)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "selection returned None".to_string())?;
    ck.check(
        "M2 reset_index rewinds the cursor to the head of the ring",
        after_reset.queue_id == 0,
        &format!("queueId={}", after_reset.queue_id),
    );

    // 过滤器：只放行 queueId=2，且必须每次都落在 2 上。
    let only_two = |mq: &MessageQueue| mq.queue_id == 2;
    let filters: &[&QueueFilter<'_>] = &[&only_two];
    let mut picked = Vec::new();
    for _ in 0..4 {
        picked.push(
            publish
                .select_one_message_queue(filters)
                .map_err(|e| e.to_string())?
                .map(|mq| mq.queue_id)
                .unwrap_or(-1),
        );
    }
    ck.check(
        "M2 a filter that admits one queue pins selection to it",
        picked == vec![2, 2, 2, 2],
        &format!("{picked:?}"),
    );
    // 全部被拒 ⇒ Python 也是「试完一圈返回 None」，不是抛错。
    let reject_all = |_mq: &MessageQueue| false;
    let all_rejected: &[&QueueFilter<'_>] = &[&reject_all];
    let rejected = publish.select_one_message_queue(all_rejected);
    ck.check(
        "M2 when every queue is rejected selection returns None, not an error",
        matches!(rejected, Ok(None)),
        &format!("{:?}", rejected.as_ref().err().map(|e| e.to_string())),
    );

    // 未知 topic + is_default=false ⇒ 拉不到路由（消费者路径禁止兜底）。
    let missing = fallback_topic.replace("Fallback", "Missing");
    let none = instance
        .update_topic_route_info_from_name_server(&missing, 5000, false)
        .await
        .map_err(|e| format!("refresh of an unknown topic should not error: {e}"))?;
    let err = instance
        .get_topic_publish_info(&missing, false)
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    ck.check(
        "M2 an unknown topic refreshes to false and carries Python's queue error",
        !none && err.contains("Can not find Message Queue for topic:"),
        &format!("refreshed={none} err={err}"),
    );

    // 未知 topic + is_default=true ⇒ 按 Java 生产者语义回退 TBW102，并裁剪到
    // broker 实际创建用的队列数，之后真能把消息发进去（broker autoCreateTopicEnable）。
    let fallback = instance
        .update_topic_route_info_from_name_server(fallback_topic, 5000, true)
        .await
        .map_err(|e| format!("fallback route refresh failed: {e}"))?;
    let publish_default = instance
        .get_topic_publish_info(fallback_topic, true)
        .await
        .map_err(|e| format!("fallback publish info unavailable: {e}"))?;
    let queues = publish_default.msg_queue_list();
    ck.check(
        "M2 is_default=true falls back to the TBW102 route and caps queues at defaultTopicQueueNums",
        fallback
            && queues.len() as i32 == MixAll::DEFAULT_TOPIC_QUEUE_NUMS
            && queues.iter().all(|q| q.topic == fallback_topic),
        &format!("{:?}", queue_ids(&queues)),
    );
    let mut fallback_msg = build_message(fallback_topic, b"via-default-route", "", "");
    let target = MessageQueue::new(fallback_topic, broker_name, queues[0].queue_id);
    let sent = send_one(instance, producer_group, &mut fallback_msg, &target).await?;
    ck.check(
        "M2 the synthesized publish info is usable: the broker auto-creates the topic on send",
        sent.status == SendStatus::SendOk && sent.queue_offset >= 0,
        &format!("{sent:?}"),
    );
    let intact = instance
        .get_topic_route_data(topic)
        .await
        .map(|r| (r.queue_datas.len(), r.queue_datas[0].read_queue_nums))
        .unwrap_or((0, 0));
    ck.check(
        "M2 falling back for one topic leaves the other topic's cache intact",
        intact == (1, QUEUE_NUMS),
        &format!("queueDatas/readQueueNums={intact:?}"),
    );
    Ok(())
}

// ---------------------------------------------------------------------- M3

/// 发送与拉取：`SendResult` 逐字段、UNIQ_KEY 补齐、批量、oneway、按队列读回。
async fn m3_send_and_pull(
    instance: &MQClientInstance,
    broker_addr: &str,
    topic: &str,
    broker_name: &str,
    producer_group: &str,
    consumer_group: &str,
    ck: &mut Checker,
) -> Live {
    let publish = instance
        .get_topic_publish_info(topic, false)
        .await
        .map_err(|e| format!("publish info unavailable: {e}"))?;
    let no_filters: &[&QueueFilter<'_>] = &[];
    let mut per_queue: HashMap<i32, Vec<String>> = HashMap::new();

    // ---- 8 条走实例的 send_message（路由解析也在实例里）
    for i in 0..MSG_TOTAL {
        let mq = publish
            .select_one_message_queue(no_filters)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "no queue to send to".to_string())?;
        let body = format!("mqclient-{topic}-{i}");
        let mut msg = build_message(topic, body.as_bytes(), "TagMqClient", &format!("mqKey{i}"));
        let result = send_one(instance, producer_group, &mut msg, &mq).await?;
        per_queue.entry(mq.queue_id).or_default().push(body);
        let uniq = get_uniq_id(&msg).unwrap_or_default();
        // Java broker `SendMessageProcessor:504` 会把客户端 UNIQ_KEY 原样回填成
        // transactionId，所以普通消息也该看得到它。
        let ids_ok = result.msg_id.as_deref() == Some(uniq.as_str())
            && result.offset_msg_id.as_deref().unwrap_or_default().len() == 32
            && result.transaction_id.as_deref() == Some(uniq.as_str());
        let queue_ok = result
            .message_queue
            .as_ref()
            .map(|q| q.queue_id == mq.queue_id && q.broker_name == broker_name && q.topic == topic)
            .unwrap_or(false);
        let region_ok = result.region_id.as_deref() == Some(MixAll::DEFAULT_TRACE_REGION_ID)
            && result.trace_on;
        ck.check(
            &format!("M3 send #{i} returns Python-shaped SendResult fields"),
            result.status == SendStatus::SendOk && ids_ok && queue_ok && region_ok,
            &format!("uniq={uniq} result={result:?}"),
        );
    }

    // ---- UNIQ_KEY 由实例补上（Java 在 sendKernelImpl 里、发请求之前）
    let mut bare = build_message(topic, b"needs-uniq-key", "", "mqKeyBare");
    ck.check(
        "M3 the caller only sets KEYS; UNIQ_KEY is still absent before send",
        !bare.properties.contains_key(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            && bare.properties.get(PROPERTY_KEYS) == Some("mqKeyBare"),
        &format!("{:?}", bare.properties.get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)),
    );
    let bare_mq = MessageQueue::new(topic, broker_name, 0);
    let bare_result = send_one(instance, producer_group, &mut bare, &bare_mq).await?;
    let stamped = bare
        .properties
        .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        .unwrap_or_default()
        .to_string();
    ck.check(
        "M3 the instance stamps UNIQ_KEY into the caller's message and reports it as msgId",
        stamped.len() > 30 && bare_result.msg_id.as_deref() == Some(stamped.as_str()),
        &format!("stamped={stamped} msgId={:?}", bare_result.msg_id),
    );
    per_queue.entry(0).or_default().push("needs-uniq-key".to_string());

    // ---- 批量消息：一次写入，broker 的 batchCQ 给每条子消息连续的逻辑位点
    let batch_bodies: Vec<String> = (0..3).map(|i| format!("batch-{i}")).collect();
    let mut batch = MessageBatch::generate_from_list(
        batch_bodies
            .iter()
            .map(|b| build_message(topic, b.as_bytes(), "TagBatch", ""))
            .collect(),
    )
    .map_err(|e| format!("batch build failed: {e}"))?;
    let raw_len = batch.message.body.clone().unwrap_or_default().len();
    let batch_mq = MessageQueue::new(topic, broker_name, 1);
    let mut pm_batch = PublishMessage::Batch(&mut batch);
    let batch_result = instance
        .send_message(
            producer_group,
            &mut pm_batch,
            &batch_mq,
            10_000,
            0,
            false,
            MixAll::DEFAULT_TOPIC,
            MixAll::DEFAULT_TOPIC_QUEUE_NUMS,
        )
        .await;
    match batch_result {
        Ok(result) => {
            let mut pulled =
                pull_from(instance, consumer_group, &batch_mq, result.queue_offset, broker_addr)
                    .await?;
            // batchCQ 的 N 条子消息是 broker 分发线程异步写进 consumeQueue 的，
            // 刚发完立刻拉可能为空 —— 拉到为止（最多 ~4s）。
            for _ in 0..20 {
                if !pulled.msg_found_list.is_empty() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                pulled = pull_from(
                    instance,
                    consumer_group,
                    &batch_mq,
                    result.queue_offset,
                    broker_addr,
                )
                .await?;
            }
            let offsets: Vec<i64> = pulled.msg_found_list.iter().map(|m| m.queue_offset).collect();
            let expected_offsets: Vec<i64> = (0..batch_bodies.len() as i64)
                .map(|k| result.queue_offset + k)
                .collect();
            ck.check(
                "M3 a batch decodes back into its sub-messages with contiguous queue offsets",
                result.status == SendStatus::SendOk
                    && bodies(&pulled.msg_found_list) == batch_bodies
                    && offsets == expected_offsets,
                &format!(
                    "rawLen={raw_len} status={:?} parentOffset={} offsets={offsets:?} bodies={:?}",
                    result.status,
                    result.queue_offset,
                    bodies(&pulled.msg_found_list)
                ),
            );
        }
        Err(e) => ck.check(
            "M3 a batch decodes back into its sub-messages with contiguous queue offsets",
            false,
            &format!("batch send rejected: {e}"),
        ),
    }

    // ---- oneway：不等响应，但消息必须在 broker 上看得见
    let oneway_body = "sent-oneway".to_string();
    let mut oneway_msg = build_message(topic, oneway_body.as_bytes(), "", "mqKeyOneway");
    let oneway_mq = MessageQueue::new(topic, broker_name, 2);
    {
        let mut pm = PublishMessage::Single(&mut oneway_msg);
        instance
            .send_message_oneway(
                producer_group,
                &mut pm,
                &oneway_mq,
                broker_addr,
                0,
                false,
                MixAll::DEFAULT_TOPIC,
                MixAll::DEFAULT_TOPIC_QUEUE_NUMS,
            )
            .await
            .map_err(|e| format!("oneway send failed: {e}"))?;
    }
    let landed = wait_async(
        || async {
            let pulled = pull_from(instance, consumer_group, &oneway_mq, 0, broker_addr)
                .await
                .unwrap_or_default();
            bodies(&pulled.msg_found_list).iter().any(|b| b == &oneway_body)
        },
        Duration::from_secs(15),
    )
    .await;
    ck.check(
        "M3 a oneway send has no result but really reaches the commitlog",
        landed,
        "the oneway body never showed up on queue 2",
    );

    // ---- 按队列拉回：每个队列只看见自己那批
    let mut all_ok = true;
    let mut detail = String::new();
    for (queue_id, expected) in &per_queue {
        let mq = MessageQueue::new(topic, broker_name, *queue_id);
        let pulled = pull_from(instance, consumer_group, &mq, 0, broker_addr).await?;
        let got = bodies(&pulled.msg_found_list);
        let missing: Vec<&String> = expected.iter().filter(|b| !got.contains(b)).collect();
        if pulled.status != PullStatus::Found || !missing.is_empty() {
            all_ok = false;
            detail = format!("queue {queue_id}: status={:?} missing={missing:?}", pulled.status);
            break;
        }
        // broker 侧的 min/max 与消息条数必须自洽（M4 也依赖它）。
        if pulled.min_offset != 0
            || pulled.max_offset - pulled.min_offset < got.len() as i64
            || pulled.next_begin_offset != pulled.max_offset
        {
            all_ok = false;
            detail = format!(
                "queue {queue_id}: min={} max={} next={} msgs={}",
                pulled.min_offset,
                pulled.max_offset,
                pulled.next_begin_offset,
                got.len()
            );
            break;
        }
    }
    ck.check(
        "M3 pull_message on the instance returns everything that queue received, with consistent offsets",
        all_ok,
        &detail,
    );
    ck.check(
        "M3 pull stamps brokerName/queueId onto every decoded message",
        pull_from(instance, consumer_group, &MessageQueue::new(topic, broker_name, 0), 0, broker_addr)
            .await
            .map(|r| {
                r.msg_found_list
                    .iter()
                    .all(|m| m.broker_name.as_deref() == Some(broker_name) && m.queue_id == 0)
            })
            .unwrap_or(false),
        "stamping missing",
    );

    // ---- 追上末尾再拉：broker 直接回 PULL_NOT_FOUND ⇒ NO_NEW_MSG
    let tail = MessageQueue::new(topic, broker_name, 0);
    let max_offset = instance
        .get_max_offset(&tail, 5000, Some(broker_addr))
        .await
        .map_err(|e| format!("get_max_offset failed: {e}"))?;
    let empty = pull_from(instance, consumer_group, &tail, max_offset, broker_addr).await?;
    ck.check(
        "M3 pulling past the tail maps PULL_NOT_FOUND to NO_NEW_MSG at the same offset",
        empty.status == PullStatus::NoNewMsg
            && empty.msg_found_list.is_empty()
            && empty.next_begin_offset == max_offset,
        &format!("status={:?} next={} max={}", empty.status, empty.next_begin_offset, max_offset),
    );

    // ---- 越界位点：broker 回 PULL_OFFSET_MOVED，Java/Python 映射成 OFFSET_ILLEGAL
    let moved = instance
        .pull_message(
            consumer_group,
            &tail,
            max_offset + 1000,
            POP_MAX_MSGS,
            PullSysFlag::build_sys_flag_basic(false, false, true, false),
            0,
            "*",
            0,
            ExpressionType::TAG,
            30_000,
            -1,
            0,
            Some(broker_addr),
            0,
        )
        .await;
    let moved = match moved {
        Ok(r) => r,
        Err(e) => {
            ck.check(
                "M3 an out-of-range offset maps PULL_OFFSET_MOVED to OFFSET_ILLEGAL",
                false,
                &format!("pull errored: {e}"),
            );
            return Ok(());
        }
    };
    ck.check(
        "M3 an out-of-range offset maps PULL_OFFSET_MOVED to OFFSET_ILLEGAL with a fix-up offset",
        moved.status == PullStatus::OffsetIllegal && moved.next_begin_offset >= max_offset,
        &format!("status={:?} next={} max={}", moved.status, moved.next_begin_offset, max_offset),
    );
    Ok(())
}

// ---------------------------------------------------------------------- M4

/// 五个 offset RPC 对真实队列成立。
async fn m4_offsets(
    instance: &MQClientInstance,
    broker_addr: &str,
    topic: &str,
    broker_name: &str,
    consumer_group: &str,
    ck: &mut Checker,
) -> Live {
    let mq = MessageQueue::new(topic, broker_name, 0);
    let max_offset = instance
        .get_max_offset(&mq, 5000, Some(broker_addr))
        .await
        .map_err(|e| format!("get_max_offset failed: {e}"))?;
    let min_offset = instance
        .get_min_offset(&mq, 5000, Some(broker_addr))
        .await
        .map_err(|e| format!("get_min_offset failed: {e}"))?;
    ck.check(
        "M4 get_max_offset / get_min_offset agree with what the queue holds",
        max_offset > min_offset && min_offset == 0,
        &format!("min={min_offset} max={max_offset}"),
    );
    let past = instance
        .search_offset_by_timestamp(&mq, current_time_millis() - 60_000, 5000, Some(broker_addr))
        .await
        .map_err(|e| format!("search_offset_by_timestamp(past) failed: {e}"))?;
    let future = instance
        .search_offset_by_timestamp(&mq, current_time_millis() + 600_000, 5000, Some(broker_addr))
        .await
        .map_err(|e| format!("search_offset_by_timestamp(future) failed: {e}"))?;
    ck.check(
        "M4 search_offset_by_timestamp brackets the queue: past -> min, future -> max",
        past == min_offset && future == max_offset,
        &format!("past={past} future={future} min={min_offset} max={max_offset}"),
    );

    // 未知 group：setZeroIfNotFound=true ⇒ broker 回 0；false ⇒ QUERY_NOT_FOUND 透传成 None。
    let stray = "CID_rust_live_missing";
    let zero = instance
        .query_consumer_offset(stray, &mq, 5000, Some(broker_addr), true)
        .await
        .map_err(|e| format!("query offset (setZero) failed: {e}"))?;
    let absent = instance
        .query_consumer_offset(stray, &mq, 5000, Some(broker_addr), false)
        .await
        .map_err(|e| format!("query offset failed: {e}"))?;
    ck.check(
        "M4 query_consumer_offset honours setZeroIfNotFound exactly like the broker",
        zero == Some(0) && absent.is_none(),
        &format!("zero={zero:?} absent={absent:?}"),
    );
    // 提交位点必须能被读回来（rebalance 之后第一个被读到的就是它）。
    let committed = (max_offset - 1).max(0);
    instance
        .update_consumer_offset(consumer_group, &mq, committed, 5000, Some(broker_addr))
        .await
        .map_err(|e| format!("update offset failed: {e}"))?;
    let read_back = instance
        .query_consumer_offset(consumer_group, &mq, 5000, Some(broker_addr), false)
        .await
        .map_err(|e| format!("query offset failed: {e}"))?;
    ck.check(
        "M4 update_consumer_offset then query_consumer_offset round-trips",
        read_back == Some(committed),
        &format!("committed={committed} read_back={read_back:?}"),
    );
    Ok(())
}

// ---------------------------------------------------------------------- M5

/// 消费者注册表 → 心跳 → broker 侧反查，闭环证明心跳真的被 broker 采纳。
async fn m5_heartbeat(
    instance: &MQClientInstance,
    client: &RemotingClient,
    broker_addr: &str,
    broker_name: &str,
    topic: &str,
    consumer_group: &str,
    ck: &mut Checker,
) -> Live {
    let stub = StubConsumer::new(instance.client_id(), consumer_group, topic, broker_name);
    instance.register_consumer(consumer_group, Arc::clone(&stub) as Arc<dyn RegisteredConsumer>);
    ck.check(
        "M5 find_consumer returns the same registration Python's dict would",
        instance
            .find_consumer(consumer_group)
            .map(|c| c.consumer_group())
            .as_deref()
            == Some(consumer_group),
        &format!("{:?}", instance.find_consumer(consumer_group).map(|c| c.client_id())),
    );

    let heartbeat = instance.prepare_heartbeat_data();
    let encoded = String::from_utf8_lossy(&heartbeat.encode()).into_owned();
    let cd = &heartbeat.consumer_data_set[0];
    ck.check(
        "M5 prepare_heartbeat_data builds the Java-shaped heartbeat body from the registry",
        heartbeat.client_id == instance.client_id()
            && heartbeat.consumer_data_set.len() == 1
            && cd.group_name == consumer_group
            && cd.consume_type == ConsumeType::CONSUME_PASSIVELY
            && cd.message_model == MessageModel::CLUSTERING
            && cd.consume_from_where == ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET
            && !cd.unit_mode
            && cd.subscription_data_set.len() == 1
            && heartbeat.heartbeat_fingerprint == 0
            && !heartbeat.without_sub,
        &encoded,
    );
    ck.check(
        "M5 the heartbeat JSON carries the wire field names the broker parses",
        encoded.contains("\"clientID\"")
            && encoded.contains("\"groupName\"")
            && encoded.contains("\"consumeType\"")
            && encoded.contains("\"messageModel\"")
            && encoded.contains("\"subscriptionDataSet\"")
            && encoded.contains("\"subString\":\"*\""),
        &encoded,
    );

    instance
        .send_heartbeat(broker_addr, &heartbeat, 5000)
        .await
        .map_err(|e| format!("send_heartbeat failed: {e}"))?;
    let ok_count = instance.send_heartbeat_to_all_broker(5000).await;
    ck.check(
        "M5 send_heartbeat_to_all_broker counts the brokers it reached",
        ok_count == 1,
        &format!("ok={ok_count}"),
    );

    // 反查：broker 只有真收了心跳才会把这个 client_id 挂到 group 上。
    let expected = instance.client_id().to_string();
    let registered = wait_async(
        move || {
            let expected = expected.clone();
            async move {
                listed_consumer_ids(client, broker_addr, consumer_group)
                    .await
                    .iter()
                    .any(|id| id == &expected)
            }
        },
        Duration::from_secs(15),
    )
    .await;
    let listed = listed_consumer_ids(client, broker_addr, consumer_group).await;
    ck.check(
        "M5 the broker reports our clientId as a group member, so the heartbeat was accepted",
        registered,
        &format!("consumerIdList={listed:?}"),
    );

    // 实例管理的两条同类 RPC 走的是同一份 broker 状态。
    let via_instance = instance
        .get_consumer_list_by_group(consumer_group, 5000, Some(broker_addr))
        .await
        .map_err(|e| format!("get_consumer_list_by_group failed: {e}"))?;
    ck.check(
        "M5 get_consumer_list_by_group reads the same registry back",
        via_instance.consumer_id_list.contains(&instance.client_id().to_string()),
        &format!("{:?}", via_instance.consumer_id_list),
    );
    let ids = instance.get_consumer_id_list_by_group(topic, consumer_group, 5000).await;
    ck.check(
        "M5 get_consumer_id_list_by_group resolves the broker through the topic route",
        ids.as_ref()
            .map(|list| list.iter().any(|id| id == instance.client_id()))
            .unwrap_or(false),
        &format!("{ids:?}"),
    );

    instance.adjust_thread_pool();
    ck.check(
        "M5 adjust_thread_pool visits every registered consumer",
        stub.adjusted.load(Ordering::SeqCst) == 1,
        &format!("adjusted={}", stub.adjusted.load(Ordering::SeqCst)),
    );
    instance.persist_consumer_offsets().await;
    ck.check(
        "M5 persist_consumer_offsets drives each consumer's own persist call",
        stub.persisted.load(Ordering::SeqCst) == 1,
        &format!("persisted={}", stub.persisted.load(Ordering::SeqCst)),
    );

    instance.unregister_consumer(consumer_group);
    let dropped = instance.find_consumer(consumer_group).is_none();
    instance.register_consumer(consumer_group, Arc::clone(&stub) as Arc<dyn RegisteredConsumer>);
    let back = instance.find_consumer(consumer_group).is_some();
    ck.check(
        "M5 unregister_consumer drops it and register_consumer puts it back",
        dropped && back,
        &format!("dropped={dropped} reregistered={back}"),
    );
    Ok(())
}

// ---------------------------------------------------------------------- M6

/// POP / ACK / changeInvisibleTime 全链路：客户端反构的 POP_CK 必须真的能用。
async fn m6_pop(
    instance: &MQClientInstance,
    broker_addr: &str,
    topic: &str,
    broker_name: &str,
    producer_group: &str,
    consumer_group: &str,
    ck: &mut Checker,
) -> Live {
    let pop_group = format!("{consumer_group}-pop");
    let queue = MessageQueue::new(topic, broker_name, 3);
    for i in 0..3 {
        let body = format!("popped-{i}");
        let mut msg = build_message(topic, body.as_bytes(), "TagPop", "");
        send_one(instance, producer_group, &mut msg, &queue).await?;
    }

    let pop = instance
        .pop_message(
            &pop_group,
            topic,
            -1,
            POP_MAX_MSGS,
            POP_INVISIBLE_TIME,
            POP_POLL_TIME,
            ConsumeInitMode::MIN,
            None,
            None,
            false,
            Some(broker_name),
            30_000,
            Some(broker_addr),
        )
        .await
        .map_err(|e| format!("pop_message failed: {e}"))?;
    ck.check(
        "M6 pop_message on a fresh group finds what is already on the topic",
        pop.status == PopStatus::Found && !pop.msg_found_list.is_empty(),
        &format!("{pop}"),
    );
    let ck_strings: Vec<String> = pop
        .msg_found_list
        .iter()
        .map(|m| {
            m.properties
                .get(PROPERTY_POP_CK)
                .unwrap_or_default()
                .to_string()
        })
        .collect();
    let all_eight = ck_strings.iter().all(|value| {
        extra_info::split(value)
            .map(|segments| segments.len() == 8)
            .unwrap_or(false)
    });
    let pop_time = pop.pop_time.to_string();
    let first_pop = pop
        .msg_found_list
        .iter()
        .all(|m| m.properties.get(PROPERTY_FIRST_POP_TIME) == Some(pop_time.as_str()));
    let stamped = pop.msg_found_list.iter().all(|m| {
        m.broker_name.as_deref() == Some(broker_name)
            && m.topic == topic
            && m.queue_id >= 0
            && m.queue_id < QUEUE_NUMS
    });
    ck.check(
        "M6 every popped message carries a client-reconstructed 8-segment POP_CK and the pop time",
        all_eight && first_pop && stamped,
        &format!("{ck_strings:?} popTime={} stamped={stamped}", pop.pop_time),
    );
    ck.check(
        "M6 the POP response header fields travel into PopResult",
        pop.pop_time > 0 && pop.invisible_time == POP_INVISIBLE_TIME && pop.revive_qid >= 0,
        &format!("{pop}"),
    );
    let popped_bodies = bodies(&pop.msg_found_list);
    ck.check(
        "M6 the three messages just sent are all in the popped batch",
        ["popped-0", "popped-1", "popped-2"]
            .iter()
            .all(|b| popped_bodies.iter().any(|m| m == b)),
        &format!("popped={popped_bodies:?}"),
    );

    // 映射键口径：带 POP_CK 的用 retry 段，不带的用 topic+queueId。
    let key_ok = pop.msg_found_list.iter().all(|m| {
        let got = MQClientInstance::pop_queue_map_key(m).unwrap_or_default();
        match m.properties.get(PROPERTY_POP_CK) {
            Some(value) => extra_info::split(value)
                .and_then(|segments| extra_info::get_retry(&segments))
                .map(|retry| got == format!("{retry}@{}", m.queue_id))
                .unwrap_or(false),
            None => {
                got == extra_info::get_start_offset_info_map_key(&m.topic, i64::from(m.queue_id))
            }
        }
    });
    ck.check(
        "M6 pop_queue_map_key agrees with extra_info's own key builders",
        key_ok,
        "map key mismatch",
    );

    // 先给第一条续期，再用**新的** extraInfo 去 ACK。
    let first = &pop.msg_found_list[0];
    let first_ck = ck_strings[0].clone();
    let first_offset = extra_info::split(&first_ck)
        .and_then(|segments| extra_info::get_ck_queue_offset(&segments))
        .map_err(|e| format!("first POP_CK is unusable: {e}"))?;
    let renewed = instance
        .change_invisible_time(
            &pop_group,
            topic,
            first.queue_id,
            &first_ck,
            first_offset,
            RENEW_INVISIBLE_TIME,
            Some(broker_name),
            5000,
            Some(broker_addr),
        )
        .await
        .map_err(|e| format!("change_invisible_time failed: {e}"))?;
    let new_ck = renewed.extra_info.clone().unwrap_or_default();
    let new_segments = extra_info::split(&new_ck).map(|s| s.len()).unwrap_or_default();
    ck.check(
        "M6 change_invisible_time returns fresh popTime/invisibleTime and a usable new extraInfo",
        renewed.success
            && new_segments == 8
            && renewed.invisible_time == RENEW_INVISIBLE_TIME
            && renewed.pop_time >= pop.pop_time,
        &format!("{renewed:?}"),
    );
    let ack_renewed = instance
        .ack_message(
            &pop_group,
            topic,
            first.queue_id,
            &new_ck,
            first_offset,
            Some(broker_name),
            5000,
            Some(broker_addr),
        )
        .await
        .map_err(|e| format!("ack with renewed extraInfo failed: {e}"))?;
    ck.check(
        "M6 an ACK with the changed-invisible-time extraInfo succeeds",
        ack_renewed == response_code::SUCCESS,
        &format!("code={ack_renewed}"),
    );

    // 其余消息按原 POP_CK 确认；brokerName / addr 省略 ⇒ 实例自己从 extraInfo + 路由解。
    let mut all_acked = true;
    let mut ack_detail = String::new();
    for (index, msg) in pop.msg_found_list.iter().enumerate().skip(1) {
        let value = ck_strings[index].clone();
        let offset = match extra_info::split(&value).and_then(|s| extra_info::get_ck_queue_offset(&s)) {
            Ok(offset) => offset,
            Err(e) => {
                all_acked = false;
                ack_detail = format!("bad POP_CK at {index}: {e}");
                break;
            }
        };
        match instance
            .ack_message(&pop_group, topic, msg.queue_id, &value, offset, None, 5000, None)
            .await
        {
            Ok(code) if code == response_code::SUCCESS => {}
            Ok(code) => {
                all_acked = false;
                ack_detail = format!("ack {index} returned code={code}");
                break;
            }
            Err(e) => {
                all_acked = false;
                ack_detail = format!("ack {index} failed: {e}");
                break;
            }
        }
    }
    ck.check(
        "M6 every popped message is ACKed with its own POP_CK, addr resolved by the instance",
        all_acked,
        &ack_detail,
    );

    // 全部确认后再弹一次：同一组已弹过的位点不会重来，已 ack 的更不会再出现。
    let again = instance
        .pop_message(
            &pop_group,
            topic,
            -1,
            POP_MAX_MSGS,
            POP_INVISIBLE_TIME,
            POP_POLL_TIME,
            ConsumeInitMode::MIN,
            None,
            None,
            false,
            Some(broker_name),
            30_000,
            Some(broker_addr),
        )
        .await
        .map_err(|e| format!("second pop_message failed: {e}"))?;
    let again_bodies = bodies(&again.msg_found_list);
    let repeats: Vec<&String> = again_bodies
        .iter()
        .filter(|b| popped_bodies.contains(b))
        .collect();
    ck.check(
        "M6 after ACK the same POP group does not get those messages back",
        repeats.is_empty(),
        &format!("repeated={repeats:?} again={again_bodies:?}"),
    );
    Ok(())
}

// ---------------------------------------------------------------------- M8

/// 共用实例的关闭守卫（Java `MQClientInstance#shutdown`:1101-1137 读的三张表 +
/// `MQClientManager#removeClientFactory`）在**真门面**上的效果：同 clientId 的
/// producer / lite 消费者共用一份实例时，先退的那个不能把还在用的心跳、路由刷新
/// 和连接一起拆掉；最后一个退掉之后，实例要从进程级登记表里消失。
///
/// 排在 M7 之前跑：M7 会删掉本示例的 topic，这里要用它真发消息。
async fn m8_shared_factory(
    namesrv: &str,
    topic: &str,
    group_a: &str,
    group_b: &str,
    lite_group: &str,
    ck: &mut Checker,
) -> Live {
    let client_id = format!("rust-live-mqclient-{}@m8", stamp());

    let a = DefaultMQProducer::new(group_a).map_err(|e| format!("A build failed: {e}"))?;
    a.set_namesrv_addr(namesrv);
    a.set_client_id(Some(&client_id));
    a.start().await.map_err(|e| format!("A start failed: {e}"))?;

    let shared = match MQClientInstance::find_instance(&client_id) {
        Some(instance) => instance,
        None => return Err("A's factory is missing from INSTANCE_MAP".to_string()),
    };
    ck.check(
        "M8 producer start registers its group on the factory (Java registerProducer)",
        shared.has_producer(group_a) && !shared.has_producer(group_b),
        &format!("clientId={}", shared.client_id()),
    );

    // 同 clientId 的第二个生产者：复用实例，不能再起第二份心跳/路由循环。
    let b = DefaultMQProducer::new(group_b).map_err(|e| format!("B build failed: {e}"))?;
    b.set_namesrv_addr(namesrv);
    b.set_client_id(Some(&client_id));
    b.start().await.map_err(|e| format!("B start failed: {e}"))?;
    ck.check(
        "M8 the second producer with the same clientId shares that one factory",
        shared.has_producer(group_b) && b.client().is_some(),
        &format!("producerTable missing {group_b}"),
    );
    let mut warm = Message::new(topic, Some(b"m8-warm"));
    let warm_ok = b.send(&mut warm, Some(10_000), None).await.is_ok();
    ck.check(
        "M8 both producers send on the shared factory",
        warm_ok,
        "B's send failed",
    );

    // A 先退：B 还在用 ⇒ 守卫让 `shutdown()` 变成 no-op，B 的发送照常走同一条连接。
    a.shutdown();
    ck.check(
        "M8 the first producer leaving keeps the shared factory running",
        shared.is_started() && !shared.has_producer(group_a) && shared.has_producer(group_b),
        &format!(
            "started={} a={} b={}",
            shared.is_started(),
            shared.has_producer(group_a),
            shared.has_producer(group_b)
        ),
    );
    let mut after_a = Message::new(topic, Some(b"m8-after-a-shutdown"));
    let sent = match b.send(&mut after_a, Some(10_000), None).await {
        Ok(r) => Ok(r),
        Err(e) => Err(format!("{e}")),
    };
    ck.check(
        "M8 the remaining producer really sends after its sibling shut down",
        sent.is_ok(),
        sent.err().as_deref().unwrap_or(""),
    );

    // 拉模式消费者同样进守卫（Java 把它和推送消费者一起放 consumerTable；这里按
    // 组名登记）。它复用同一份实例，不是新建。
    let lite = DefaultLitePullConsumer::with_config(LitePullConsumerConfig {
        consumer_group: lite_group.to_string(),
        name_server_addrs: vec![namesrv.to_string()],
        client_id: Some(client_id.clone()),
        ..Default::default()
    })
    .map_err(|e| format!("lite build failed: {e}"))?;
    lite.subscribe(topic, "*");
    lite.start().await.map_err(|e| format!("lite start failed: {e}"))?;
    ck.check(
        "M8 a pull-mode consumer is a tenant of the factory too",
        shared.is_started() && shared.has_consumer_group(lite_group),
        &format!("started={} groupRegistered={}", shared.is_started(), shared.has_consumer_group(lite_group)),
    );

    // B 再退：lite 还在用，实例仍不许拆。
    b.shutdown();
    ck.check(
        "M8 the factory survives while the lite consumer still uses it",
        shared.is_started() && shared.has_consumer_group(lite_group),
        "shutdown tore down an instance that still had a consumer",
    );

    // 最后一个租户退出（lite 的末次提交与关实例是串在同一个后台任务里的）：
    // 这次要真拆，并且把 INSTANCE_MAP 的登记摘掉。
    lite.shutdown();
    let gone = wait_async(
        || async {
            !shared.is_started() && MQClientInstance::find_instance(&client_id).is_none()
        },
        Duration::from_secs(10),
    )
    .await;
    ck.check(
        "M8 the last tenant shuts the factory down and unregisters it",
        gone,
        &format!(
            "started={} stillRegistered={}",
            shared.is_started(),
            MQClientInstance::find_instance(&client_id).is_some()
        ),
    );
    // 摘掉登记之后同 clientId 必须拿到可用的新实例，而不是一个已关掉的死实例。
    let rebuilt = MQClientInstance::create_mq_client_instance(
        &client_id,
        vec![namesrv.to_string()],
        MQClientInstanceConfig::default(),
    );
    let restart_ok = rebuilt.start().await.is_ok() && rebuilt.is_started();
    ck.check(
        "M8 a shutdown factory is replaced by a fresh one for the same clientId",
        restart_ok,
        "the rebuilt factory could not start",
    );
    rebuilt.shutdown();
    Ok(())
}

// ---------------------------------------------------------------------- M7

/// 集群信息 / topic 列表 / 队列锁（含互斥反证）/ 注销，然后删掉本次建的 topic。
#[allow(clippy::too_many_arguments)]
async fn m7_admin_and_cleanup(
    instance: &MQClientInstance,
    client: &RemotingClient,
    namesrv: &str,
    broker_addr: &str,
    broker_name: &str,
    topic: &str,
    fallback_topic: &str,
    producer_group: &str,
    consumer_group: &str,
    ck: &mut Checker,
) -> Live {
    let cluster = instance
        .get_broker_cluster_info(10_000)
        .await
        .map_err(|e| format!("get_broker_cluster_info failed: {e}"))?;
    let addrs = cluster.get_broker_addrs();
    ck.check(
        "M7 get_broker_cluster_info decodes the broker's ClusterInfo",
        cluster.cluster_addr_table.iter().any(|(name, _)| name == "DefaultCluster")
            && cluster
                .broker_addr_table
                .iter()
                .any(|(name, _)| name == broker_name)
            && addrs.iter().any(|a| a == broker_addr),
        &format!("clusters={:?} addrs={addrs:?}", cluster.cluster_addr_table),
    );

    let topics = instance
        .get_all_topic_list_from_name_server(10_000)
        .await
        .map_err(|e| format!("get_all_topic_list_from_name_server failed: {e}"))?;
    ck.check(
        "M7 the namesrv topic list contains the topic this run created",
        topics.topic_list.iter().any(|t| t == topic),
        &format!("{} topics, looking for {topic}", topics.topic_list.len()),
    );

    // 批量锁：A 锁住之后 B 锁不到，A 解锁之后 B 才锁得到 —— 这才叫锁。
    let mqs: Vec<MessageQueue> = (0..QUEUE_NUMS)
        .map(|q| MessageQueue::new(topic, broker_name, q))
        .collect();
    let locked_a = instance
        .lock_batch_mq(consumer_group, "rust-client-A", &mqs, 5000)
        .await
        .map_err(|e| format!("lock_batch_mq(A) failed: {e}"))?;
    let locked_b_held = instance
        .lock_batch_mq(consumer_group, "rust-client-B", &mqs, 5000)
        .await
        .map_err(|e| format!("lock_batch_mq(B while held) failed: {e}"))?;
    instance
        .unlock_batch_mq(consumer_group, "rust-client-A", &mqs, 5000)
        .await
        .map_err(|e| format!("unlock_batch_mq failed: {e}"))?;
    let locked_b_free = instance
        .lock_batch_mq(consumer_group, "rust-client-B", &mqs, 5000)
        .await
        .map_err(|e| format!("lock_batch_mq(B after unlock) failed: {e}"))?;
    instance
        .unlock_batch_mq(consumer_group, "rust-client-B", &mqs, 5000)
        .await
        .map_err(|e| format!("second unlock_batch_mq failed: {e}"))?;
    let mut sorted_a = queue_ids(&locked_a);
    let mut sorted_b_free = queue_ids(&locked_b_free);
    sorted_a.sort_unstable();
    sorted_b_free.sort_unstable();
    ck.check(
        "M7 lock_batch_mq / unlock_batch_mq really serialize two clients on the same queues",
        sorted_a == vec![0, 1, 2, 3]
            && locked_b_held.is_empty()
            && sorted_b_free == vec![0, 1, 2, 3],
        &format!(
            "A={:?} B(while held)={:?} B(after unlock)={:?}",
            queue_ids(&locked_a),
            queue_ids(&locked_b_held),
            queue_ids(&locked_b_free)
        ),
    );

    // 注销前先撤掉注册表里的消费者：否则 30s 心跳循环会把成员关系重新登记回来。
    instance.unregister_consumer(consumer_group);
    instance
        .unregister_client(broker_addr, instance.client_id(), producer_group, consumer_group, 5000)
        .await
        .map_err(|e| format!("unregister_client failed: {e}"))?;
    instance
        .unregister_client_all_brokers(instance.client_id(), producer_group, consumer_group, 5000)
        .await;
    let mine = instance.client_id().to_string();
    let gone = wait_async(
        || async {
            let ids = listed_consumer_ids(client, broker_addr, consumer_group).await;
            !ids.iter().any(|id| id == &mine)
        },
        Duration::from_secs(15),
    )
    .await;
    let still = listed_consumer_ids(client, broker_addr, consumer_group).await;
    ck.check(
        "M7 after UNREGISTER_CLIENT the broker no longer lists us under the group",
        gone,
        &format!("consumerIdList={still:?}"),
    );

    // 管理接口收尾：删 broker 配置 + namesrv 路由（5.x 默认不随注册删路由）。
    for t in [topic, fallback_topic] {
        match instance.delete_topic_in_broker(broker_addr, t, 5000).await {
            Ok(()) => ck.check(&format!("M7 delete_topic_in_broker {t}"), true, ""),
            Err(e) => ck.check(&format!("M7 delete_topic_in_broker {t}"), false, &e.to_string()),
        }
        match instance.delete_topic_in_namesrv(t, 5000).await {
            Ok(()) => ck.check(&format!("M7 delete_topic_in_namesrv {t}"), true, ""),
            Err(e) => ck.check(&format!("M7 delete_topic_in_namesrv {t}"), false, &e.to_string()),
        }
        let after = get_route(client, namesrv, t).await?;
        ck.check(
            &format!("M7 the route of {t} is gone from the namesrv"),
            after.code == response_code::TOPIC_NOT_EXIST,
            &format!("code={} remark={:?}", after.code, after.remark),
        );
    }
    Ok(())
}

// ------------------------------------------------------------------- main

async fn run(namesrv: &str) -> Checker {
    let run = stamp();
    let topic = format!("RustLiveMqClient_{run}");
    let fallback_topic = format!("RustLiveFallback_{run}");
    let producer_group = format!("PID_rust_mqclient_{run}");
    let consumer_group = format!("CID_rust_mqclient_{run}");

    println!("== rocketmq rust live MQClientInstance test ==");
    println!("   namesrv        = {namesrv}");
    println!("   topic          = {topic}");
    println!("   fallback topic = {fallback_topic} (deliberately not pre-created)");
    println!("   groups         = {producer_group} / {consumer_group}");

    let client = RemotingClient::new();
    let mut ck = Checker::new();

    let mut broker_name = String::new();
    let mut broker_addr = String::new();
    let mut prepared = bootstrap_topic(&client, namesrv, &topic).await;
    if prepared.is_ok() {
        prepared = match get_route(&client, namesrv, &topic)
            .await
            .and_then(|r| decode_route(&r, "route"))
        {
            Ok(route) => match route.broker_datas.first() {
                Some(bd) => match bd.select_broker_addr() {
                    Some(addr) => {
                        broker_name = bd.broker_name.clone();
                        broker_addr = addr;
                        Ok(())
                    }
                    None => Err(format!("broker {} has no address", bd.broker_name)),
                },
                None => Err("route has no brokerData".to_string()),
            },
            Err(e) => Err(e),
        };
    }
    if let Err(e) = prepared {
        ck.abort("M0 bootstrap", &e);
        client.shutdown();
        return ck;
    }
    println!("   broker         = {broker_name} @ {broker_addr}");

    let instance = MQClientInstance::new(
        &format!("rust-live-mqclient-{run}@main"),
        vec![namesrv.to_string()],
    );
    if let Err(e) = instance.start().await {
        ck.abort("M0 start", &e.to_string());
        client.shutdown();
        return ck;
    }
    instance.register_topic_in_use(&topic);

    let scenarios: Vec<(&str, Live)> = vec![
        ("M1 identity", m1_identity(namesrv, &mut ck).await),
        (
            "M2 route + publish info",
            m2_route_and_publish(
                &instance,
                &topic,
                &fallback_topic,
                &broker_name,
                &broker_addr,
                &producer_group,
                &mut ck,
            )
            .await,
        ),
        (
            "M3 send + pull",
            m3_send_and_pull(
                &instance,
                &broker_addr,
                &topic,
                &broker_name,
                &producer_group,
                &consumer_group,
                &mut ck,
            )
            .await,
        ),
        (
            "M4 offsets",
            m4_offsets(
                &instance,
                &broker_addr,
                &topic,
                &broker_name,
                &consumer_group,
                &mut ck,
            )
            .await,
        ),
        (
            "M5 heartbeat + registry",
            m5_heartbeat(
                &instance,
                &client,
                &broker_addr,
                &broker_name,
                &topic,
                &consumer_group,
                &mut ck,
            )
            .await,
        ),
        (
            "M6 pop + ack",
            m6_pop(
                &instance,
                &broker_addr,
                &topic,
                &broker_name,
                &producer_group,
                &consumer_group,
                &mut ck,
            )
            .await,
        ),
        (
            "M8 shared client factory lifecycle",
            m8_shared_factory(
                namesrv,
                &topic,
                &format!("{producer_group}_m8a"),
                &format!("{producer_group}_m8b"),
                &format!("{consumer_group}_m8"),
                &mut ck,
            )
            .await,
        ),
        (
            "M7 admin + cleanup",
            m7_admin_and_cleanup(
                &instance,
                &client,
                namesrv,
                &broker_addr,
                &broker_name,
                &topic,
                &fallback_topic,
                &producer_group,
                &consumer_group,
                &mut ck,
            )
            .await,
        ),
    ];
    for (name, outcome) in scenarios {
        if let Err(e) = outcome {
            ck.abort(name, &e);
        }
    }

    instance.shutdown();
    client.shutdown();
    ck
}

fn report(ck: &mut Checker) {
    println!("== summary: {} passed, {} failed ==", ck.passed, ck.failed.len());
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
