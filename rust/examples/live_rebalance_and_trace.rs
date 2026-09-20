//! 重平衡 / 消费线程池 / 消息轨迹三个模块对**真实 5.5.1 broker** 的联调验证。
//!
//! 与 `live_client_modules.rs`（T1~T8 打的是取址、容错、统计、钩子骨架、编码格式等
//! 「单点」能力）互补：这里验证的是**只有配上真实收发才成立**的三件事 ——
//! 队列分配结果能不能真的把消息切开、消费线程池能不能把真实消息跑完、
//! 轨迹钩子产出的记录能不能真的落到 broker 并被控制台（这里用解码器）读回来。
//!
//! 场景：
//! - R1 `allocate_strategy`：用**真实路由**的可写队列跑 AVG / AVG_BY_CIRCLE / CONFIG，
//!   核对分区算术、确定性、守卫，最后做端到端证明：按两个客户端的分配结果各发各的、
//!   各拉各的，A 只能看到自己队列里的消息。
//! - R2 `consume_executor`：真实拉回的消息交给 core/max 两档线程池，用「卡住的
//!   任务」确定性地观察 `worker_count` / `queued_count`，验证每条消息恰好被
//!   listener 消费一次、listener 看到的是真字段（storeHost / storeTimestamp /
//!   queueOffset / reconsumeTimes），以及 `Clone` 与原池共享关停状态。
//! - R3 `trace_hook`：把 `SendMessageTraceHook` / `ConsumeMessageTraceHook` 挂上
//!   真实发送链路（`SendResult` 完全按 Python `_parse_send_response` 的口径构造），
//!   轨迹通道**真的把编码结果发到独立轨迹 topic**，再从 broker 拉回来解码，
//!   核对 Pub / SubBefore / SubAfter 三类记录与 SubBefore↔SubAfter 共享的 requestId。
//! - R4 清理：删掉业务 topic 与轨迹 topic 的 broker 配置和 namesrv 路由。
//!
//! ⚠ 唯一的非真实数据：R2 里「任务卡住」用的门闩（`AtomicBool` + 轮询）与
//! `RECONSUME_LATER` 的哨兵 body 前缀是测试脚手架；消息本身、队列、offset、耗时、
//! `MSG_REGION` / `TRACE_ON` 属性全部来自 broker 的真实返回。
//!
//! 未覆盖：`EndTransactionTraceHook` 需要事务半消息（`SEND_MESSAGE_V2` +
//! `END_TRANSACTION(37)`），等 `producer.rs` 落地事务链路后一并验证；该钩子自身的
//! 字段口径已由 `trace_hook.rs` 的 Python 对拍单测覆盖。
//!
//! 用法（先按项目 README 起本地集群）：
//! ```text
//! cargo run --example live_rebalance_and_trace -- 127.0.0.1:9876
//! ```

use std::collections::HashMap;
use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::runtime::Handle;

use rocketmq_client_remoting::client::allocate_strategy::{
    AllocateMessageQueueAveragely, AllocateMessageQueueAveragelyByCircle,
    AllocateMessageQueueByConfig, AllocateMessageQueueStrategy,
};
use rocketmq_client_remoting::client::consume_executor::{ConsumeExecutor, ConsumeTask};
use rocketmq_client_remoting::client::hook::{
    execute_consume_hook_after, execute_consume_hook_before, execute_send_message_hook_after,
    execute_send_message_hook_before, CommunicationMode, ConsumeMessageContext,
    ConsumeMessageHook, ConsumeMessageHookList, HookList, SendMessageContext, SendMessageHook,
    SendMessageHookList,
};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently, SendResult,
    SendStatus,
};
use rocketmq_client_remoting::client::trace::{
    local_address, TraceConstants, TraceContext, TraceDataEncoder, TraceType,
};
use rocketmq_client_remoting::client::trace_hook::{
    ConsumeMessageTraceHook, SendMessageTraceHook, TraceReportSink, CONSUME_CONTEXT_TYPE,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::message_client_id_setter;
use rocketmq_client_remoting::common::message_const;
use rocketmq_client_remoting::common::message_decoder;
use rocketmq_client_remoting::common::message_type::MessageType;
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::sysflag::PullSysFlag;
use rocketmq_client_remoting::common::topic_config::{self, TopicFilterType};
use rocketmq_client_remoting::common::util_all;
use rocketmq_client_remoting::remoting::client::RemotingClient;
use rocketmq_client_remoting::remoting::protocol::codes::{request_code, response_code};
use rocketmq_client_remoting::remoting::protocol::heartbeat::ExpressionType;
use rocketmq_client_remoting::remoting::protocol::headers::{
    CreateTopicRequestHeader, DeleteTopicRequestHeader, GetRouteInfoRequestHeader,
    PullMessageRequestHeader, SendMessageRequestHeaderV2, SendMessageResponseHeader,
};
use rocketmq_client_remoting::remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;
use rocketmq_client_remoting::remoting::protocol::serialize::RemotingSerializable;

/// 建出来的 topic 队列数（对齐 Python `create_topic_in_broker` 默认 4）。
const QUEUE_NUMS: i32 = 4;
/// body 以 `later-` 开头的那一批返回 RECONSUME_LATER，用来证明状态真的回传给调用方。
const LATER_PREFIX: &str = "later-";
/// R2 的消息条数（4 批 × 2 条）。
const MSG_TOTAL: i32 = 8;
/// R3 的三条被追踪消息都发到这个队列，便于按 offset 精确拉回。
const TRACE_QUEUE: i32 = 3;

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

/// 锁中毒时照常取内值（轨迹/消费路径不许因为别处的 panic 连锁崩）。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// 轮询等到条件成立（后台任务落库用），超时返回 `false` 让调用方打出现场。
async fn wait_until(cond: impl FnMut() -> bool) -> bool {
    wait_within(cond, Duration::from_secs(20)).await
}

async fn wait_within(mut cond: impl FnMut() -> bool, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    loop {
        if cond() {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
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

/// 带实测耗时的 RPC。
async fn rpc_timed(
    client: &RemotingClient,
    addr: &str,
    cmd: RemotingCommand,
    timeout_millis: i64,
) -> (Result<RemotingCommand, String>, i64) {
    let started = Instant::now();
    let resp = rpc(client, addr, cmd, timeout_millis).await;
    (resp, started.elapsed().as_millis() as i64)
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

// ------------------------------------------------------ 真实收发（共用）

/// 一条待发消息：tags/keys 可空，UNIQ_KEY 与 WAIT 标记按 Java 发送路径补齐。
fn build_message(topic: &str, body: &[u8], tags: &str, keys: &str) -> Message {
    let mut msg = Message::new(topic, Some(body));
    if !tags.is_empty() {
        msg.set_tags(tags);
    }
    if !keys.is_empty() {
        msg.set_keys(keys);
    }
    // Java：非批量消息在发请求前补 UNIQ_KEY，它决定 SendResult.msgId。
    message_client_id_setter::set_uniq_id(&mut msg);
    msg.put_property(message_const::PROPERTY_WAIT_STORE_MSG_OK, "true");
    msg
}

/// Python `_parse_send_response` 的 status_map（响应码 → SendStatus）。
fn send_status_from_code(code: i32) -> Option<SendStatus> {
    match code {
        response_code::SUCCESS => Some(SendStatus::SendOk),
        response_code::FLUSH_DISK_TIMEOUT => Some(SendStatus::FlushDiskTimeout),
        response_code::FLUSH_SLAVE_TIMEOUT => Some(SendStatus::FlushSlaveTimeout),
        response_code::SLAVE_NOT_AVAILABLE => Some(SendStatus::SlaveNotAvailable),
        _ => None,
    }
}

/// 一次真实发送的结果。`result` 逐字段照抄 Python `mq_client._parse_send_response`
/// —— R3 的轨迹钩子只认这个口径（`regionId` 或 `traceOn` 缺一个就不落库）。
struct Sent {
    /// 客户端 UNIQ_KEY（= `result.msg_id`）。
    msg_id: String,
    /// broker 侧的 offsetMsgId（= `result.offset_msg_id` = 响应头 msgId）。
    offset_msg_id: String,
    queue_id: i32,
    queue_offset: i64,
    result: SendResult,
    #[allow(dead_code)]
    rt_millis: i64,
}

async fn send_full(
    client: &RemotingClient,
    broker_addr: &str,
    broker_name: &str,
    queue_id: i32,
    producer_group: &str,
    msg: &Message,
) -> Result<Sent, String> {
    let topic = msg.get_topic().to_string();
    let header = SendMessageRequestHeaderV2 {
        producer_group: Some(producer_group.to_string()),
        topic: Some(topic.clone()),
        default_topic: Some(MixAll::DEFAULT_TOPIC.to_string()),
        default_topic_queue_nums: Some(MixAll::DEFAULT_TOPIC_QUEUE_NUMS),
        queue_id: Some(queue_id),
        sys_flag: Some(0),
        born_timestamp: Some(util_all::current_time_millis()),
        flag: Some(msg.get_flag()),
        properties: Some(message_decoder::message_properties_2_string(msg.get_properties())),
        reconsume_times: Some(0),
        unit_mode: Some(false),
        max_reconsume_times: Some(0),
        batch: Some(false),
        broker_name: Some(broker_name.to_string()),
    };
    let mut req = RemotingCommand::create_request_command(
        request_code::SEND_MESSAGE_V2,
        Some(Box::new(header)),
    );
    req.set_body(Some(msg.get_body().to_vec()));
    let (resp, rt) = rpc_timed(client, broker_addr, req, 10_000).await;
    let resp = resp?;
    let Some(status) = send_status_from_code(resp.code) else {
        return Err(format!(
            "send to {topic} queue {queue_id} failed: code={} remark={:?}",
            resp.code, resp.remark
        ));
    };
    let resp_header: SendMessageResponseHeader = resp
        .decode_command_custom_header()
        .map_err(|e| format!("decode SendMessageResponseHeader failed: {e}"))?;
    let broker_msg_id = resp_header.msg_id.clone().unwrap_or_default();
    let uniq = message_client_id_setter::get_uniq_id(msg).unwrap_or_default();
    let region = resp.get_ext_field(message_const::PROPERTY_MSG_REGION).unwrap_or_default();
    let trace_switch = resp.get_ext_field(message_const::PROPERTY_TRACE_SWITCH);
    let result = SendResult {
        status,
        // Python `get_uniq_id(msg) or header.msg_id`
        msg_id: Some(if uniq.is_empty() { broker_msg_id.clone() } else { uniq.clone() }),
        message_queue: Some(MessageQueue::new(
            &topic,
            broker_name,
            resp_header.queue_id.unwrap_or(queue_id),
        )),
        queue_offset: resp_header.queue_offset.unwrap_or(0),
        transaction_id: resp_header.transaction_id.clone(),
        // Python `offset_msg_id = header.msg_id`：响应头的 msgId 是 broker 侧的 offsetMsgId
        offset_msg_id: Some(broker_msg_id.clone()),
        // 定时消息才有；普通消息恒为 None
        recall_handle: resp_header.recall_handle.clone(),
        region_id: Some(if region.is_empty() {
            MixAll::DEFAULT_TRACE_REGION_ID.to_string()
        } else {
            region.to_string()
        }),
        // Python `str(ext.get(TRACE_ON)) != "false"`：字段缺失时是 "None" ⇒ true
        trace_on: trace_switch.map(|v| v != "false").unwrap_or(true),
    };
    Ok(Sent {
        msg_id: result.msg_id.clone().unwrap_or_default(),
        offset_msg_id: broker_msg_id,
        queue_id: resp_header.queue_id.unwrap_or(queue_id),
        queue_offset: result.queue_offset,
        rt_millis: rt,
        result,
    })
}

/// `PULL_MESSAGE(11)`：返回解出的消息 + 实测耗时；`PULL_NOT_FOUND` 当空批。
async fn pull_messages(
    client: &RemotingClient,
    broker_addr: &str,
    topic: &str,
    consumer_group: &str,
    queue_id: i32,
    from_offset: i64,
    max_msgs: i32,
) -> Result<(Vec<MessageExt>, i64), String> {
    let sys_flag = PullSysFlag::build_sys_flag_basic(false, false, true, false);
    let req = RemotingCommand::create_request_command(
        request_code::PULL_MESSAGE,
        Some(Box::new(PullMessageRequestHeader {
            consumer_group: Some(consumer_group.to_string()),
            topic: Some(topic.to_string()),
            lite_topic: None,
            queue_id: Some(queue_id),
            queue_offset: Some(from_offset),
            max_msg_nums: Some(max_msgs),
            sys_flag: Some(sys_flag),
            commit_offset: Some(0),
            suspend_timeout_millis: Some(15_000),
            subscription: Some("*".to_string()),
            sub_version: Some(0),
            expression_type: Some(ExpressionType::TAG.to_string()),
            max_msg_bytes: Some(-1),
            request_source: Some(0),
            proxy_froward_client_id: None,
        })),
    );
    let (resp, rt) = rpc_timed(client, broker_addr, req, 30_000).await;
    let resp = resp?;
    if resp.code == response_code::PULL_NOT_FOUND {
        return Ok((Vec::new(), rt));
    }
    if resp.code != response_code::SUCCESS {
        return Err(format!(
            "pull {topic} queue {queue_id} failed: code={} remark={:?}",
            resp.code, resp.remark
        ));
    }
    let raw = match resp.body() {
        Some(b) => b.to_vec(),
        None => return Ok((Vec::new(), rt)),
    };
    Ok((message_decoder::decode_messages(&raw), rt))
}

/// 轮询直到拉够 `want` 条。
async fn pull_until(
    client: &RemotingClient,
    broker_addr: &str,
    topic: &str,
    consumer_group: &str,
    queue_id: i32,
    from_offset: i64,
    want: usize,
) -> Result<Vec<MessageExt>, String> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (msgs, _) =
            pull_messages(client, broker_addr, topic, consumer_group, queue_id, from_offset, 32)
                .await?;
        if msgs.len() >= want || Instant::now() > deadline {
            return Ok(msgs);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn bodies(msgs: &[MessageExt]) -> Vec<String> {
    msgs.iter()
        .map(|m| String::from_utf8_lossy(m.get_body()).into_owned())
        .collect()
}

fn queue_ids(mqs: &[MessageQueue]) -> Vec<i32> {
    mqs.iter().map(|q| q.queue_id).collect()
}

/// 分区性：`parts` 拼起来正好是 `0..n` 且不重不漏。
fn is_partition(parts: &[Vec<i32>], n: i32) -> bool {
    let mut flat: Vec<i32> = parts.iter().flatten().copied().collect();
    flat.sort_unstable();
    flat == (0..n).collect::<Vec<i32>>()
}

// ------------------------------------------------------------------ R1

#[allow(clippy::too_many_arguments)]
async fn r1_allocate_strategy(
    client: &RemotingClient,
    namesrv: &str,
    broker_addr: &str,
    broker_name: &str,
    topic: &str,
    producer_group: &str,
    consumer_group: &str,
    ck: &mut Checker,
) -> Live {
    let resp = get_route(client, namesrv, topic).await?;
    let route = decode_route(&resp, "route")?;
    // 真实路由的可写队列（`get_all_message_queue` 已按 PERM_WRITE 过滤）。
    let mut mq_all: Vec<MessageQueue> = route
        .get_all_message_queue(topic)
        .into_iter()
        .map(|k| MessageQueue::new(&k.topic, &k.broker_name, k.queue_id))
        .collect();
    // Java `RebalanceImpl#rebalanceByTopic` 与 Python 都在分配前排序；策略只保证
    // 「输出是输入的有序子序列」，所以排序是调用方的责任。
    mq_all.sort();
    ck.check(
        "R1 the real route exposes exactly the writable queues we created",
        queue_ids(&mq_all) == (0..QUEUE_NUMS).collect::<Vec<i32>>()
            && mq_all.iter().all(|q| q.topic == topic && q.broker_name == broker_name),
        &format!("mqs={mq_all:?} want queues 0..{QUEUE_NUMS} on {broker_name}"),
    );

    let avg: Arc<dyn AllocateMessageQueueStrategy> = Arc::new(AllocateMessageQueueAveragely);
    let circle: Arc<dyn AllocateMessageQueueStrategy> =
        Arc::new(AllocateMessageQueueAveragelyByCircle);
    let config: Arc<dyn AllocateMessageQueueStrategy> = Arc::new(AllocateMessageQueueByConfig::default());
    ck.check(
        "R1 strategy names are Java's AVG / AVG_BY_CIRCLE / CONFIG",
        avg.get_name() == "AVG" && circle.get_name() == "AVG_BY_CIRCLE" && config.get_name() == "CONFIG",
        &format!(
            "names=[{}, {}, {}]",
            avg.get_name(),
            circle.get_name(),
            config.get_name()
        ),
    );

    let alloc = |s: &Arc<dyn AllocateMessageQueueStrategy>, cid: &str, cids: &[String]| -> Result<Vec<i32>, String> {
        s.allocate(consumer_group, cid, &mq_all, cids)
            .map(|v| queue_ids(&v))
            .map_err(|e| format!("allocate failed: {e}"))
    };
    let cids = |n: usize| -> Vec<String> {
        (0..n).map(|i| format!("{consumer_group}@127.0.0.1#{i}")).collect()
    };

    // 2 个消费者：4 个队列正好切两刀，前一半归 A、后一半归 B（连续区间）。
    let two = cids(2);
    let a_two = alloc(&avg, &two[0], &two)?;
    let b_two = alloc(&avg, &two[1], &two)?;
    ck.check(
        "R1 AVG splits 4 real queues into two contiguous halves",
        a_two == vec![0, 1] && b_two == vec![2, 3],
        &format!("A={a_two:?} B={b_two:?}"),
    );

    // 3 个消费者：average=1、余数 1 ⇒ 第一个消费者多拿一条（Java/Python 同一算式）。
    let three = cids(3);
    let mut avg3: Vec<Vec<i32>> = Vec::new();
    let mut circle3: Vec<Vec<i32>> = Vec::new();
    for cid in &three {
        avg3.push(alloc(&avg, cid, &three)?);
        circle3.push(alloc(&circle, cid, &three)?);
    }
    ck.check(
        "R1 AVG hands the remainder out to the first consumers (4 = 1*3 + 1)",
        avg3 == vec![vec![0, 1], vec![2], vec![3]],
        &format!("avg3={avg3:?}"),
    );
    ck.check(
        "R1 AVG_BY_CIRCLE walks the ring instead (queue 3 wraps back to the first consumer)",
        circle3 == vec![vec![0, 3], vec![1], vec![2]],
        &format!("circle3={circle3:?}"),
    );
    let four = cids(4);
    let mut avg4: Vec<Vec<i32>> = Vec::new();
    for cid in &four {
        avg4.push(alloc(&avg, cid, &four)?);
    }
    ck.check(
        "R1 consumers == queues means one queue each, in route order",
        avg4 == vec![vec![0], vec![1], vec![2], vec![3]],
        &format!("avg4={avg4:?}"),
    );
    ck.check(
        "R1 every strategy partitions the real queues exactly once",
        is_partition(&avg3, QUEUE_NUMS) && is_partition(&circle3, QUEUE_NUMS)
            && is_partition(&avg4, QUEUE_NUMS)
            && is_partition(&[a_two.clone(), b_two.clone()], QUEUE_NUMS),
        &format!("avg3={avg3:?} circle3={circle3:?} avg4={avg4:?}"),
    );
    let again = alloc(&avg, &three[0], &three)?;
    let again_circle = alloc(&circle, &three[0], &three)?;
    ck.check(
        "R1 allocation is deterministic across repeated calls",
        again == avg3[0] && again_circle == circle3[0],
        &format!("avg {:?} -> {again:?}, circle {:?} -> {again_circle:?}", avg3[0], circle3[0]),
    );

    // 守卫：客户端 id 不在组内 / 没有队列 / 没有同伴 ⇒ 空结果（Python 一致，不报错）。
    let outsider = alloc(&avg, &format!("{consumer_group}@10.0.0.9#9"), &three)?;
    let no_queues = avg
        .allocate(consumer_group, &three[0], &[], &three)
        .map_err(|e| format!("guard failed: {e}"))?;
    let no_peers = circle
        .allocate(consumer_group, &three[0], &mq_all, &[])
        .map_err(|e| format!("guard failed: {e}"))?;
    ck.check(
        "R1 the three guards return an empty allocation instead of an error",
        outsider.is_empty() && no_queues.is_empty() && no_peers.is_empty(),
        &format!("outsider={outsider:?} noQueues={no_queues:?} noPeers={no_peers:?}"),
    );

    // CONFIG：完全无视 cidAll，只返回运维配好的那张表（连顺序都不改）。
    let by_config = AllocateMessageQueueByConfig::new(vec![
        MessageQueue::new(topic, broker_name, 3),
        MessageQueue::new(topic, broker_name, 1),
    ]);
    let got = by_config
        .allocate(consumer_group, "not-in-the-group", &mq_all, &[])
        .map_err(|e| format!("CONFIG failed: {e}"))?;
    ck.check(
        "R1 CONFIG returns the configured queues verbatim, guards included",
        queue_ids(&got) == vec![3, 1] && by_config.get_name() == "CONFIG",
        &format!("got={:?}", queue_ids(&got)),
    );

    // ---- 端到端证明：按分配结果各发各的、各拉各的，两个消费者互不打扰 ----
    let mut per_queue: HashMap<i32, Vec<String>> = HashMap::new();
    for i in 0..MSG_TOTAL {
        let q = i % QUEUE_NUMS;
        let body = format!("reb-{topic}-q{q}-{i}").into_bytes();
        let msg = build_message(topic, &body, "TagRebalance", "rebKey");
        let sent = send_full(client, broker_addr, broker_name, q, producer_group, &msg).await?;
        ck.check(
            "R1 the broker stores the message on the queueId we asked for",
            sent.queue_id == q,
            &format!("asked {q}, got {}", sent.queue_id),
        );
        per_queue.entry(q).or_default().push(String::from_utf8_lossy(&body).into_owned());
    }
    for (cid, mine) in two.iter().zip([a_two.clone(), b_two.clone()]) {
        let group = format!("{consumer_group}-part{}", cid.rsplit('#').next().unwrap_or_default());
        let mut seen: Vec<String> = Vec::new();
        let mut foreign = 0;
        for q in &mine {
            let msgs = pull_until(client, broker_addr, topic, &group, *q, 0, 2).await?;
            foreign += msgs.iter().filter(|m| m.queue_id != *q).count();
            seen.extend(bodies(&msgs));
        }
        let mut want: Vec<String> =
            mine.iter().flat_map(|q| per_queue.get(q).cloned().unwrap_or_default()).collect();
        seen.sort();
        want.sort();
        ck.check(
            "R1 reading only the allocated queues sees exactly those messages",
            foreign == 0 && !seen.is_empty() && seen == want,
            &format!("cid={cid} mine={mine:?} seen={seen:?} want={want:?} foreign={foreign}"),
        );
    }
    Ok(())
}

// ------------------------------------------------------------------ R2

/// 真实 listener：记录每批的状态 + body，并核对消息带回来的真字段。
struct LiveListener {
    outcomes: Mutex<Vec<(String, Vec<String>)>>,
    ctx_queue_ids: Mutex<Vec<i32>>,
    /// 看到「broker 填过的字段」（storeHost / storeTimestamp / 真实 offset）的条数。
    real_field_msgs: AtomicUsize,
}

impl LiveListener {
    fn new() -> LiveListener {
        LiveListener {
            outcomes: Mutex::new(Vec::new()),
            ctx_queue_ids: Mutex::new(Vec::new()),
            real_field_msgs: AtomicUsize::new(0),
        }
    }
}

impl MessageListenerConcurrently for LiveListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let list = bodies(msgs);
        let later = list.iter().any(|b| b.starts_with(LATER_PREFIX));
        let status = if later {
            ConsumeConcurrentlyStatus::ReconsumeLater
        } else {
            ConsumeConcurrentlyStatus::ConsumeSuccess
        };
        let real = msgs
            .iter()
            .filter(|m| {
                !m.store_host.as_deref().unwrap_or_default().is_empty()
                    && m.store_host_port > 0
                    && m.store_timestamp > 0
                    && m.born_timestamp > 0
                    && m.queue_offset >= 0
                    && m.reconsume_times == 0
                    && !m.msg_id.as_deref().unwrap_or_default().is_empty()
            })
            .count();
        self.real_field_msgs.fetch_add(real, Ordering::SeqCst);
        lock(&self.ctx_queue_ids)
            .push(context.message_queue.as_ref().map(|q| q.queue_id).unwrap_or(-1));
        lock(&self.outcomes).push((status.name().to_string(), list));
        status
    }
}

/// 一个「消费批次」= 一个任务：先过一道门闩，再同步调用 listener
/// （对齐 Java：listener 是阻塞接口，线程池只负责并发度）。
fn make_consume_task(
    idx: usize,
    batch: Arc<Vec<MessageExt>>,
    listener: Arc<dyn MessageListenerConcurrently>,
    started: Arc<Mutex<Vec<usize>>>,
    gate: Arc<AtomicBool>,
    broker_name: &str,
    topic: &str,
) -> ConsumeTask {
    let broker_name = broker_name.to_string();
    let topic = topic.to_string();
    Box::pin(async move {
        lock(&started).push(idx);
        while !gate.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let mq = MessageQueue::new(&topic, &broker_name, 0);
        let mut context = ConsumeConcurrentlyContext::new(Some(mq));
        listener.consume_message(&batch, &mut context);
    })
}

async fn r2_consume_executor(
    client: &RemotingClient,
    broker_addr: &str,
    broker_name: &str,
    topic: &str,
    producer_group: &str,
    consumer_group: &str,
    ck: &mut Checker,
) -> Live {
    // 8 条真实消息 → 4 批，每批 2 条；最后一批带 later- 哨兵。全部发到队列 0。
    let mut sent_bodies: Vec<String> = Vec::new();
    let mut prev_offset: Option<i64> = None;
    for i in 0..MSG_TOTAL {
        let prefix = if i >= MSG_TOTAL - 2 { LATER_PREFIX } else { "ok-" };
        let body = format!("{prefix}consume-{topic}-{i}").into_bytes();
        let msg = build_message(topic, &body, "TagConsume", "consumeKey");
        let sent = send_full(client, broker_addr, broker_name, 0, producer_group, &msg).await?;
        // R1 已经往队列 0 写过消息，所以这里只能断言「严格递增 1」。
        ck.check(
            "R2 queue offsets advance by exactly one per send",
            prev_offset.map(|p| sent.queue_offset == p + 1).unwrap_or(true),
            &format!("prev={prev_offset:?} now={}", sent.queue_offset),
        );
        prev_offset = Some(sent.queue_offset);
        ck.check(
            "R2 the real SendResult carries Python's msgId / offsetMsgId pair",
            sent.msg_id != sent.offset_msg_id
                && sent.result.region_id.as_deref() == Some(MixAll::DEFAULT_TRACE_REGION_ID)
                && sent.result.trace_on,
            &format!("uniq={} broker={}", sent.msg_id, sent.offset_msg_id),
        );
        sent_bodies.push(String::from_utf8_lossy(&body).into_owned());
    }
    let from = prev_offset.unwrap_or(0) - i64::from(MSG_TOTAL) + 1;
    let msgs = pull_until(client, broker_addr, topic, consumer_group, 0, from, MSG_TOTAL as usize)
        .await?;
    if msgs.len() != MSG_TOTAL as usize {
        return Err(format!(
            "pulled {} of {MSG_TOTAL} messages from offset {from}, cannot run R2",
            msgs.len()
        ));
    }
    let batches: Vec<Arc<Vec<MessageExt>>> =
        msgs.chunks(2).map(|c| Arc::new(c.to_vec())).collect();
    ck.check(
        "R2 the real batch splits into 4 pairs",
        batches.len() == 4 && batches.iter().all(|b| b.len() == 2),
        &format!("batches={}", batches.len()),
    );

    let listener = Arc::new(LiveListener::new());
    let dyn_listener = Arc::clone(&listener) as Arc<dyn MessageListenerConcurrently>;
    let executor = ConsumeExecutor::new(2, 4);
    ck.check(
        "R2 a fresh executor reports an idle pool",
        executor.get_core_pool_size() == 2
            && executor.get_max_pool_size() == 4
            && executor.worker_count() == 0
            && executor.queued_count() == 0
            && executor.handler_exception_count() == 0,
        &format!("{executor:?}"),
    );

    // 入参照抄 Python 的夹取：core 不为负、max 不小于 core。
    let clamped = ConsumeExecutor::with_params(-1, 3, Duration::ZERO, "rmq-clamp");
    let raised = ConsumeExecutor::with_params(5, 2, Duration::from_secs(1), "rmq-raise");
    ck.check(
        "R2 a negative core degrades to 0 and max never drops below core",
        clamped.get_core_pool_size() == 0
            && clamped.get_max_pool_size() == 3
            && raised.get_core_pool_size() == 5
            && raised.get_max_pool_size() == 5,
        &format!("clamp={clamped:?} raise={raised:?}"),
    );
    let rejected = executor.set_core_pool_size(-1).err().map(|e| e.to_string());
    ck.check(
        "R2 set_core_pool_size(-1) is rejected with Python's ValueError text",
        rejected.as_deref().map(|m| m.ends_with("core pool size must be >= 0")) == Some(true),
        &format!("err={rejected:?}"),
    );

    // 3 个「卡住」的任务：core=2 ⇒ 只有 2 个能跑，第 3 个必须排队（确定性观测点）。
    let gate = Arc::new(AtomicBool::new(false));
    let started: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    for (idx, batch) in batches.iter().take(3).enumerate() {
        let task = make_consume_task(
            idx,
            Arc::clone(batch),
            Arc::clone(&dyn_listener),
            Arc::clone(&started),
            Arc::clone(&gate),
            broker_name,
            topic,
        );
        executor.submit(task).map_err(|e| format!("submit failed: {e}"))?;
    }
    let two_running = wait_within(|| lock(&started).len() == 2, Duration::from_secs(5)).await;
    ck.check(
        "R2 with core=2 exactly two tasks run concurrently",
        two_running && executor.worker_count() == 2,
        &format!("started={:?} workers={}", lock(&started), executor.worker_count()),
    );
    ck.check(
        "R2 the third task waits in the unbounded queue and spawns no extra worker",
        executor.queued_count() == 1 && lock(&started).len() == 2,
        &format!("queued={} started={}", executor.queued_count(), lock(&started).len()),
    );

    gate.store(true, Ordering::SeqCst);
    ck.check(
        "R2 all three gated batches were consumed",
        wait_until(|| lock(&listener.outcomes).len() == 3).await,
        &format!("outcomes={}", lock(&listener.outcomes).len()),
    );
    ck.check(
        "R2 the pool drains back to an empty queue but keeps its core workers",
        executor.queued_count() == 0 && executor.worker_count() == 2,
        &format!("queued={} workers={}", executor.queued_count(), executor.worker_count()),
    );

    // 第 4 批走 clone：`Clone` 共享同一个池（Python 里 consumer 持有的就是同一个引用）。
    let shared = executor.clone();
    let task = make_consume_task(
        3,
        Arc::clone(&batches[3]),
        Arc::clone(&dyn_listener),
        Arc::clone(&started),
        Arc::clone(&gate),
        broker_name,
        topic,
    );
    shared.submit(task).map_err(|e| format!("submit via clone failed: {e}"))?;
    let four_done = wait_until(|| lock(&listener.outcomes).len() == 4).await;
    let mut batch_ids = lock(&started).clone();
    batch_ids.sort_unstable();
    ck.check(
        "R2 a cloned executor feeds the same pool and spawns no extra workers",
        four_done && batch_ids == vec![0, 1, 2, 3] && shared.worker_count() == 2,
        &format!("started={batch_ids:?} cloneWorkers={}", shared.worker_count()),
    );

    // 每条真实消息恰好被消费一次，且 listener 看到的是真字段。
    let outcomes = lock(&listener.outcomes).clone();
    let mut consumed: Vec<String> = outcomes.iter().flat_map(|(_, b)| b.clone()).collect();
    consumed.sort();
    let mut want = sent_bodies.clone();
    want.sort();
    ck.check(
        "R2 every real message reached the listener exactly once",
        consumed == want,
        &format!("consumed={consumed:?}\n           want={want:?}"),
    );
    let success = outcomes.iter().filter(|(s, _)| s == "CONSUME_SUCCESS").count();
    ck.check(
        "R2 the listener's status travels back per batch (one RECONSUME_LATER, on the sentinel batch)",
        success == 3
            && outcomes.iter().filter(|(s, _)| s == "RECONSUME_LATER").count() == 1
            && outcomes.iter().any(|(s, b)| {
                s == "RECONSUME_LATER" && b.iter().all(|x| x.starts_with(LATER_PREFIX))
            }),
        &format!("outcomes={outcomes:?}"),
    );
    let real_seen = listener.real_field_msgs.load(Ordering::SeqCst);
    let ctx_queues = lock(&listener.ctx_queue_ids).clone();
    ck.check(
        "R2 the listener saw broker-filled fields on all 8 real messages",
        real_seen == MSG_TOTAL as usize && ctx_queues.iter().all(|q| *q == 0),
        &format!("real={real_seen} ctxQueues={ctx_queues:?}"),
    );
    ck.check(
        "R2 nothing on the consume path was swallowed by the exception handler",
        executor.handler_exception_count() == 0,
        &format!("exceptions={}", executor.handler_exception_count()),
    );
    let named = ConsumeExecutor::with_params(3, 7, Duration::from_secs(9), "rmq-live-consume");
    ck.check(
        "R2 Debug prints the pool sizing and the thread-name prefix",
        format!("{named:?}").contains("rmq-live-consume"),
        &format!("{named:?}"),
    );

    // core 抬到 3：只补「队列里真等着的那点」，不是补到跟队列一样长。
    executor.set_core_pool_size(3).map_err(|e| format!("resize failed: {e}"))?;
    ck.check(
        "R2 raising core is recorded immediately and clamps max upward",
        executor.get_core_pool_size() == 3
            && executor.get_max_pool_size() >= 3
            && executor.worker_count() == 2,
        &format!(
            "core={} max={} workers={}",
            executor.get_core_pool_size(),
            executor.get_max_pool_size(),
            executor.worker_count()
        ),
    );

    // 关停：先 shutdown（不再接单、跑完手上的活），再等 worker 全部退出。
    executor.shutdown();
    let refused = shared.submit(Box::pin(async {})).err().map(|e| e.to_string());
    ck.check(
        "R2 submit after shutdown is refused with Python's RuntimeError text, and the clone sees the same flag",
        refused.as_deref().map(|m| m.ends_with("ConsumeExecutor has been shut down")) == Some(true),
        &format!("err={refused:?} cloneWorkers={}", shared.worker_count()),
    );
    let terminated =
        tokio::time::timeout(Duration::from_secs(10), executor.await_termination()).await;
    ck.check(
        "R2 await_termination returns once the core workers retire",
        terminated.is_ok() && executor.worker_count() == 0,
        &format!("timedOut={} workers={}", terminated.is_err(), executor.worker_count()),
    );
    let graceful = tokio::time::timeout(Duration::from_secs(5), shared.shutdown_gracefully()).await;
    ck.check(
        "R2 shutdown_gracefully on an already stopped pool is a no-op",
        graceful.is_ok() && shared.worker_count() == 0,
        &format!("done={} workers={}", graceful.is_ok(), shared.worker_count()),
    );
    Ok(())
}

// ------------------------------------------------------------------ R3

/// 真实轨迹通道：钩子交出的每条 `TraceContext` 编码后**真的发到轨迹 topic**，
/// 对应 Python `AsyncTraceDispatcher._send_trace_data_by_mq`
/// （body = trans_data，KEYS = 排序后的 trans_key，不带 tags）。
struct LiveTraceSink {
    handle: Handle,
    client: RemotingClient,
    broker_addr: String,
    broker_name: String,
    trace_topic: String,
    producer_group: String,
    client_id: String,
    reported: Mutex<Vec<TraceContext>>,
    /// 后台发送的完成数与失败原因（失败只记账，绝不能影响业务链路）。
    accepted: AtomicUsize,
    sent: Arc<AtomicUsize>,
    failures: Arc<Mutex<Vec<String>>>,
    /// `false` 模拟分发器已停止：`report` 返回 false，钩子按 Java/Python 口径忽略它。
    accepting: AtomicBool,
}

impl LiveTraceSink {
    fn new(
        client: &RemotingClient,
        broker_addr: &str,
        broker_name: &str,
        trace_topic: &str,
        producer_group: &str,
        client_id: &str,
    ) -> LiveTraceSink {
        LiveTraceSink {
            handle: Handle::current(),
            client: client.clone(),
            broker_addr: broker_addr.to_string(),
            broker_name: broker_name.to_string(),
            trace_topic: trace_topic.to_string(),
            producer_group: producer_group.to_string(),
            client_id: client_id.to_string(),
            reported: Mutex::new(Vec::new()),
            accepted: AtomicUsize::new(0),
            sent: Arc::new(AtomicUsize::new(0)),
            failures: Arc::new(Mutex::new(Vec::new())),
            accepting: AtomicBool::new(true),
        }
    }

    fn reports(&self) -> Vec<TraceContext> {
        lock(&self.reported).clone()
    }

    fn take_reports(&self) -> Vec<TraceContext> {
        let mut v = lock(&self.reported);
        std::mem::take(&mut *v)
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    fn sent(&self) -> usize {
        self.sent.load(Ordering::SeqCst)
    }
}

impl TraceReportSink for LiveTraceSink {
    fn trace_topic_name(&self) -> String {
        self.trace_topic.clone()
    }

    fn report(&self, context: TraceContext) -> bool {
        if !self.accepting.load(Ordering::SeqCst) {
            // 对应 Java/Python `append()` 在分发器停止后返回 false
            return false;
        }
        let Some(bean) = TraceDataEncoder::encoder_from_context_bean(Some(&context)) else {
            lock(&self.failures).push("encoder returned nothing".to_string());
            return false;
        };
        self.accepted.fetch_add(1, Ordering::SeqCst);
        lock(&self.reported).push(context);
        // Python `KEY_SEPARATOR.join(sorted(key_set))`：trans_key 是 BTreeSet，
        // 迭代序即排序序，与 Python 的 sorted() 同。
        let keys = bean
            .trans_key
            .iter()
            .filter(|k| !k.is_empty())
            .cloned()
            .collect::<Vec<String>>()
            .join(message_const::KEY_SEPARATOR);
        let client = self.client.clone();
        let (addr, name, trace_topic, group) = (
            self.broker_addr.clone(),
            self.broker_name.clone(),
            self.trace_topic.clone(),
            self.producer_group.clone(),
        );
        let done = Arc::clone(&self.sent);
        let failures = Arc::clone(&self.failures);
        let body = bean.trans_data.into_bytes();
        self.handle.spawn(async move {
            let msg = build_message(&trace_topic, &body, "", &keys);
            match send_full(&client, &addr, &name, 0, &group, &msg).await {
                Ok(_) => {
                    done.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => lock(&failures).push(format!("send trace record failed: {e}")),
            }
        });
        true
    }

    fn client_id(&self) -> String {
        self.client_id.clone()
    }
}

/// 组一个发送上下文（R3 的两个负例与正例共用）。
fn send_context(
    msg: &Message,
    producer_group: &str,
    broker_addr: &str,
    msg_type: MessageType,
) -> SendMessageContext {
    SendMessageContext {
        producer_group: producer_group.to_string(),
        message: Some(msg.clone()),
        broker_addr: broker_addr.to_string(),
        born_host: local_address().to_string(),
        communication_mode: Some(CommunicationMode::Sync),
        msg_type,
        ..Default::default()
    }
}

#[allow(clippy::too_many_arguments)]
async fn r3_trace_hook(
    client: &RemotingClient,
    broker_addr: &str,
    broker_name: &str,
    topic: &str,
    trace_topic: &str,
    producer_group: &str,
    consumer_group: &str,
    ck: &mut Checker,
) -> Live {
    let sink = Arc::new(LiveTraceSink::new(
        client,
        broker_addr,
        broker_name,
        trace_topic,
        producer_group,
        &format!("{}@rust-live-reb", local_address()),
    ));
    let dyn_sink = Arc::clone(&sink) as Arc<dyn TraceReportSink>;
    let send_hooks: SendMessageHookList = HookList::new();
    send_hooks.register(Arc::new(SendMessageTraceHook::new(Arc::clone(&dyn_sink)))
        as Arc<dyn SendMessageHook>);
    let consume_hooks: ConsumeMessageHookList = HookList::new();
    consume_hooks.register(Arc::new(ConsumeMessageTraceHook::new(dyn_sink))
        as Arc<dyn ConsumeMessageHook>);

    // ---- 发送侧：3 条真实消息，每条跑一次 before/after ----
    let mut uniq_keys: Vec<String> = Vec::new();
    let mut broker_ids: Vec<String> = Vec::new();
    let mut results: Vec<SendResult> = Vec::new();
    for i in 0..3 {
        let body = format!("trace-{topic}-payload-{i}").into_bytes();
        let msg = build_message(topic, &body, "TagTraceHook", &format!("traceKey{i}"));
        let mut ctx = send_context(&msg, producer_group, broker_addr, MessageType::NormalMsg);
        ctx.mq = Some(MessageQueue::new(topic, broker_name, TRACE_QUEUE));
        let reports_before = sink.reports().len();
        execute_send_message_hook_before(&send_hooks, &mut ctx);
        ck.check(
            "R3 sendMessageBefore hangs a Pub context on mqTraceContext and reports nothing yet",
            ctx.mq_trace_context.is_some() && sink.reports().len() == reports_before,
            &format!(
                "holder={} reports {} -> {}",
                ctx.mq_trace_context.is_some(),
                reports_before,
                sink.reports().len()
            ),
        );
        let sent =
            send_full(client, broker_addr, broker_name, TRACE_QUEUE, producer_group, &msg).await?;
        ctx.send_result = Some(sent.result.clone());
        execute_send_message_hook_after(&send_hooks, &mut ctx);
        uniq_keys.push(sent.msg_id.clone());
        broker_ids.push(sent.offset_msg_id.clone());
        results.push(sent.result.clone());
    }
    ck.check(
        "R3 the broker really turns TRACE_ON on and stamps MSG_REGION, so records can land",
        results.iter().all(|r| {
            r.trace_on && r.region_id.as_deref() == Some(MixAll::DEFAULT_TRACE_REGION_ID)
        }),
        &format!(
            "regions={:?}",
            results.iter().map(|r| r.region_id.clone()).collect::<Vec<_>>()
        ),
    );
    let pub_reports = sink.take_reports();
    let pubs: Vec<&TraceContext> = pub_reports
        .iter()
        .filter(|c| c.trace_type == Some(TraceType::Pub))
        .collect();
    let first_pub = match pubs.first() {
        Some(p) => *p,
        None => return Err("the send hook reported no Pub record".to_string()),
    };
    let first_bean = match first_pub.trace_beans.first() {
        Some(b) => b,
        None => return Err("the Pub record has no bean".to_string()),
    };
    ck.check(
        "R3 three Pub records were reported, each naming the real client msgId",
        pubs.len() == 3
            && pubs.iter().map(|c| c.trace_beans[0].msg_id.clone()).collect::<Vec<_>>() == uniq_keys
            && pubs.iter().all(|c| c.cost_time >= 0 && c.is_success),
        &format!("pubs={} ids={uniq_keys:?}", pubs.len()),
    );
    ck.check(
        "R3 the Pub bean holds topic/tags/keys/bodyLength/storeHost/offsetMsgId from the real send",
        first_bean.topic == topic
            && first_bean.tags == "TagTraceHook"
            && first_bean.keys == "traceKey0"
            && first_bean.body_length == format!("trace-{topic}-payload-0").len() as i32
            && first_bean.store_host == broker_addr
            && first_bean.offset_msg_id == broker_ids[0]
            && first_bean.msg_type == MessageType::NormalMsg
            && first_bean.retry_times == 0,
        &format!("bean={first_bean:?} wantBodyLen={}", format!("trace-{topic}-payload-0").len()),
    );
    ck.check(
        "R3 storeTime is time_stamp + costTime/2 (Python's floor division)",
        first_bean.store_time == first_pub.time_stamp + i64::from(first_pub.cost_time) / 2,
        &format!(
            "ts={} cost={} storeTime={}",
            first_pub.time_stamp, first_pub.cost_time, first_bean.store_time
        ),
    );
    ck.check(
        "R3 the Pub record carries the producer group and the broker's region",
        first_pub.group_name == producer_group
            && first_pub.region_id == MixAll::DEFAULT_TRACE_REGION_ID,
        &format!("group={} region={}", first_pub.group_name, first_pub.region_id),
    );

    // 轨迹 topic 自身不再被追踪（Java/Python 都在 before/after 开头判前缀）。
    let trace_msg = build_message(trace_topic, format!("trace-in-trace-{topic}").as_bytes(), "", "");
    let mut skip_ctx = send_context(&trace_msg, producer_group, broker_addr, MessageType::NormalMsg);
    execute_send_message_hook_before(&send_hooks, &mut skip_ctx);
    let held = skip_ctx.mq_trace_context.is_some();
    let before = sink.accepted();
    execute_send_message_hook_after(&send_hooks, &mut skip_ctx);
    ck.check(
        "R3 a message on the trace topic is never traced again",
        !held && sink.accepted() == before,
        &format!("holder={held} accepted={before}->{}", sink.accepted()),
    );

    // 分发器拒收时业务链路照常（Java/Python 都忽略 append() 的返回值）。
    sink.accepting.store(false, Ordering::SeqCst);
    let mut off_ctx =
        send_context(&build_message(topic, b"trace-off", "", ""), producer_group, broker_addr, MessageType::NormalMsg);
    execute_send_message_hook_before(&send_hooks, &mut off_ctx);
    off_ctx.send_result = Some(results[0].clone());
    let before_off = sink.accepted();
    execute_send_message_hook_after(&send_hooks, &mut off_ctx);
    sink.accepting.store(true, Ordering::SeqCst);
    ck.check(
        "R3 a stopped dispatcher drops the record without touching the send path",
        sink.accepted() == before_off && off_ctx.send_result.is_some(),
        &format!("accepted={before_off}->{}", sink.accepted()),
    );

    // ---- 消费侧：把这 3 条真实消息作为一批投出去 ----
    // R1 的轮询发送也落在 queue 3，所以必须从第一条 R3 消息自己的 offset 开始拉。
    let consumed = pull_until(
        client,
        broker_addr,
        topic,
        &format!("{consumer_group}-trace"),
        TRACE_QUEUE,
        results[0].queue_offset,
        3,
    )
    .await?;
    let consumed_bodies = bodies(&consumed);
    if consumed.len() != 3 || !consumed_bodies.iter().enumerate().all(|(i, b)| {
        *b == format!("trace-{topic}-payload-{i}")
    }) {
        return Err(format!(
            "pulled {} trace-hook messages from queue {TRACE_QUEUE} at offset {}: {consumed_bodies:?}",
            consumed.len(),
            results[0].queue_offset
        ));
    }
    let mut cctx = ConsumeMessageContext::new(
        consumer_group,
        Some(consumed.clone()),
        Some(MessageQueue::new(topic, broker_name, TRACE_QUEUE)),
    );
    cctx.access_channel = Some("LOCAL".to_string());
    let mut props = HashMap::new();
    props.insert(CONSUME_CONTEXT_TYPE.to_string(), "EXCEPTION".to_string());
    cctx.props = Some(props);
    let before = sink.accepted();
    execute_consume_hook_before(&consume_hooks, &mut cctx);
    let after_before = sink.accepted();
    execute_consume_hook_after(&consume_hooks, &mut cctx);
    let consume_reports = sink.take_reports();
    ck.check(
        "R3 consumeMessageBefore and After report exactly one context each",
        after_before == before + 1 && consume_reports.len() == 2,
        &format!("before={before} mid={after_before} drained={}", consume_reports.len()),
    );
    let (sub_before, sub_after) = (&consume_reports[0], &consume_reports[1]);
    ck.check(
        "R3 SubBefore/SubAfter chain through the same requestId (the console's only join key)",
        sub_before.trace_type == Some(TraceType::SubBefore)
            && sub_after.trace_type == Some(TraceType::SubAfter)
            && !sub_before.request_id.is_empty()
            && sub_before.request_id == sub_after.request_id,
        &format!("before={:?} after={:?}", sub_before.request_id, sub_after.request_id),
    );
    let pulled_ids: Vec<String> =
        consumed.iter().map(|m| m.msg_id.clone().unwrap_or_default()).collect();
    let pulled_keys: Vec<String> =
        consumed.iter().map(|m| m.get_keys().unwrap_or_default().to_string()).collect();
    ck.check(
        "R3 SubBefore beans come from the real pulled messages (broker msgId, storeSize, retryTimes)",
        sub_before.trace_beans.len() == 3
            && sub_before.group_name == consumer_group
            && sub_before.trace_beans.iter().map(|b| b.msg_id.clone()).collect::<Vec<_>>() == pulled_ids
            && sub_before.trace_beans.iter().all(|b| b.retry_times == 0 && b.body_length > 0)
            && sub_before.trace_beans.iter().map(|b| b.keys.clone()).collect::<Vec<_>>() == pulled_keys,
        &format!("beans={:?} ids={pulled_ids:?}", sub_before.trace_beans),
    );
    ck.check(
        "R3 SubAfter copies success/costTime from the delivery and maps the enum-name contextCode",
        sub_after.is_success
            && sub_after.cost_time >= 0
            && sub_after.context_code == 2
            && sub_after.group_name == consumer_group
            && sub_after.request_id == sub_before.request_id
            && sub_after.trace_beans.len() == 3,
        &format!("after={sub_after:?}"),
    );

    // 空批次：before/after 都不该产出任何记录（Python 的第一道 `if not msg_list`）。
    let before_empty = sink.accepted();
    let mut empty_ctx = ConsumeMessageContext::new(consumer_group, None, None);
    execute_consume_hook_before(&consume_hooks, &mut empty_ctx);
    execute_consume_hook_after(&consume_hooks, &mut empty_ctx);
    ck.check(
        "R3 an empty batch reports nothing and leaves the private slot untouched",
        sink.accepted() == before_empty && empty_ctx.mq_trace_context.is_none(),
        &format!("accepted={before_empty}->{} holder={}", sink.accepted(), empty_ctx.mq_trace_context.is_some()),
    );

    // ---- 关键一跳：轨迹记录真的落到 broker，再拉回来解码 ----
    let expected = sink.accepted();
    let want_msgs = 3 + 1 + 1; // 3 条 Pub + 1 条 SubBefore + 1 条 SubAfter
    ck.check(
        "R3 the hook chain reported exactly the records we expect",
        expected == want_msgs,
        &format!("accepted={expected} want={want_msgs}"),
    );
    let failures = lock(&sink.failures).clone();
    if !failures.is_empty() {
        return Err(format!("trace record sends failed: {failures:?}"));
    }
    let flushed = wait_until(|| sink.sent() == expected).await;
    ck.check(
        "R3 every reported record was really sent to the trace topic",
        flushed,
        &format!("sent={}/{}", sink.sent(), expected),
    );
    let (trace_msgs, _) = pull_messages(
        client,
        broker_addr,
        trace_topic,
        &format!("CID_{trace_topic}"),
        0,
        0,
        32,
    )
    .await?;
    if trace_msgs.len() != want_msgs {
        return Err(format!(
            "the trace topic holds {} messages, want {want_msgs}; bodies={:?}",
            trace_msgs.len(),
            bodies(&trace_msgs)
        ));
    }
    let texts: Vec<String> =
        trace_msgs.iter().map(|m| String::from_utf8_lossy(m.get_body()).into_owned()).collect();
    ck.check(
        "R3 each trace message ends with STX, so records are self-delimiting on disk",
        texts.iter().all(|t| t.ends_with(TraceConstants::FIELD_SPLITOR)
            && t.contains(TraceConstants::CONTENT_SPLITOR)),
        &format!("first={:?}", texts[0]),
    );
    ck.check(
        "R3 the trace records carry the traced messages' msgIds in KEYS",
        trace_msgs.iter().any(|m| {
            m.get_keys()
                .unwrap_or_default()
                .split(message_const::KEY_SEPARATOR)
                .any(|k| uniq_keys.contains(&k.to_string()))
        }),
        &format!("keys={:?}", trace_msgs.iter().map(|m| m.get_keys().unwrap_or_default()).collect::<Vec<_>>()),
    );
    let mut decoded: Vec<TraceContext> = Vec::new();
    for text in &texts {
        decoded.extend(TraceDataEncoder::decoder_from_trace_data_string(Some(text)));
    }
    let count = |t: TraceType| decoded.iter().filter(|c| c.trace_type == Some(t)).count();
    ck.check(
        "R3 one transport message carries several records (SubBefore/SubAfter write one line per bean)",
        count(TraceType::Pub) == 3
            && count(TraceType::SubBefore) == 3
            && count(TraceType::SubAfter) == 3,
        &format!(
            "pub={} before={} after={} total={} ({} messages)",
            count(TraceType::Pub),
            count(TraceType::SubBefore),
            count(TraceType::SubAfter),
            decoded.len(),
            trace_msgs.len()
        ),
    );
    let decoded_pubs: Vec<&TraceContext> =
        decoded.iter().filter(|c| c.trace_type == Some(TraceType::Pub)).collect();
    ck.check(
        "R3 the Pub records read back from the broker still name the real msgIds",
        decoded_pubs
            .iter()
            .map(|c| c.trace_beans[0].msg_id.clone())
            .collect::<Vec<_>>()
            .iter()
            .all(|id| uniq_keys.contains(id))
            && decoded_pubs.iter().all(|c| {
                c.group_name == producer_group
                    && c.region_id == MixAll::DEFAULT_TRACE_REGION_ID
                    && c.is_success
            }),
        &format!(
            "ids={:?}",
            decoded_pubs.iter().map(|c| c.trace_beans[0].msg_id.clone()).collect::<Vec<_>>()
        ),
    );
    // ⚠ 实测 Java Pub 布局：14 段里没有 storeTime，clientHost 只在第 15 段之后才有，
    // 所以从 broker 读回来的 Pub bean storeTime=0、clientHost 回落成本机地址。
    let store_time_ok = decoded_pubs
        .iter()
        .zip(pubs.iter())
        .all(|(d, o)| d.trace_beans[0].store_time == 0 && d.trace_beans[0].client_host == local_address()
            && d.trace_beans[0].body_length == o.trace_beans[0].body_length
            && d.cost_time == o.cost_time
            && d.time_stamp == o.time_stamp);
    ck.check(
        "R3 a decoded Pub record keeps costTime/bodyLength and has no storeTime",
        store_time_ok,
        &format!("decoded={:?}", decoded_pubs[0].trace_beans[0]),
    );
    let before_ids: Vec<String> = decoded
        .iter()
        .filter(|c| c.trace_type == Some(TraceType::SubBefore))
        .map(|c| c.request_id.clone())
        .collect();
    let after_ids: Vec<String> = decoded
        .iter()
        .filter(|c| c.trace_type == Some(TraceType::SubAfter))
        .map(|c| c.request_id.clone())
        .collect();
    ck.check(
        "R3 the three SubBefore lines and the three SubAfter lines share one requestId",
        before_ids.iter().all(|id| *id == sub_before.request_id)
            && after_ids.iter().all(|id| *id == sub_before.request_id),
        &format!("before={before_ids:?} after={after_ids:?} want={:?}", sub_before.request_id),
    );
    let bean_ids: Vec<String> = decoded
        .iter()
        .filter(|c| matches!(c.trace_type, Some(TraceType::SubBefore) | Some(TraceType::SubAfter)))
        .map(|c| c.trace_beans[0].msg_id.clone())
        .collect();
    ck.check(
        "R3 SubBefore and SubAfter records name the real consumed msgIds on the wire",
        pulled_ids.iter().all(|id| bean_ids.contains(id)) && bean_ids.len() == 6,
        &format!("wire={bean_ids:?} pulled={pulled_ids:?}"),
    );
    match (
        decoded.iter().find(|c| c.trace_type == Some(TraceType::SubBefore)),
        decoded.iter().find(|c| c.trace_type == Some(TraceType::SubAfter)),
    ) {
        (Some(b), Some(a)) => ck.check(
            "R3 SubBefore/SubAfter round-trip keeps group, region, keys and contextCode",
            b.group_name == consumer_group
                && a.group_name == consumer_group
                && b.time_stamp == sub_before.time_stamp
                && b.region_id == sub_before.region_id
                && !b.trace_beans[0].keys.is_empty()
                && a.context_code == 2
                && a.is_success
                && a.cost_time >= 0,
            &format!("before={b:?} after={a:?}"),
        ),
        (b, a) => ck.abort(
            "R3 the sub records decoded back",
            &format!("before={b:?} after={a:?}"),
        ),
    }
    println!(
        "        trace records: {} reported, {} messages on {trace_topic}, {} decoded contexts",
        expected,
        trace_msgs.len(),
        decoded.len()
    );
    Ok(())
}

// ------------------------------------------------------------------ R4

async fn r4_cleanup(
    client: &RemotingClient,
    namesrv: &str,
    broker_addr: &str,
    topics: &[&str],
    ck: &mut Checker,
) -> Live {
    for topic in topics {
        let resp = rpc(
            client,
            broker_addr,
            RemotingCommand::create_request_command(
                request_code::DELETE_TOPIC_IN_BROKER,
                Some(Box::new(DeleteTopicRequestHeader { topic: Some(topic.to_string()) })),
            ),
            5000,
        )
        .await?;
        ck.check(
            &format!("R4 DELETE_TOPIC_IN_BROKER {topic}"),
            resp.code == response_code::SUCCESS,
            &format!("code={} remark={:?}", resp.code, resp.remark),
        );
        // 5.x 默认 deleteTopicWithBrokerRegistration=false ⇒ 还得显式删 namesrv 路由。
        let mut del =
            RemotingCommand::create_request_command(request_code::DELETE_TOPIC_IN_NAMESRV, None);
        del.add_ext_field("topic", topic);
        let resp = rpc(client, namesrv, del, 5000).await?;
        ck.check(
            &format!("R4 DELETE_TOPIC_IN_NAMESRV {topic}"),
            resp.code == response_code::SUCCESS,
            &format!("code={} remark={:?}", resp.code, resp.remark),
        );
        let after = get_route(client, namesrv, topic).await?;
        ck.check(
            &format!("R4 the route of {topic} is gone"),
            after.code == response_code::TOPIC_NOT_EXIST,
            &format!("code={} remark={:?}", after.code, after.remark),
        );
    }
    Ok(())
}

// ----------------------------------------------------------------- main

#[tokio::main]
async fn main() -> ExitCode {
    let argv: Vec<String> = env::args().collect();
    let namesrv = argv
        .get(1)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:9876".to_string());

    let stamp = stamp();
    let topic = format!("RustLiveReb_{stamp}");
    let trace_topic = format!("RustLiveTrace_{stamp}");
    let producer_group = format!("PID_rust_reb_{stamp}");
    let consumer_group = format!("CID_rust_reb_{stamp}");

    println!("== rocketmq rust live rebalance / consume executor / trace hook test ==");
    println!("   namesrv     = {namesrv}");
    println!("   topic       = {topic}");
    println!("   trace topic = {trace_topic}");
    println!("   groups      = {producer_group} / {consumer_group}");

    let client = RemotingClient::new();
    let mut ck = Checker::new();

    let mut broker_name = String::new();
    let mut broker_addr = String::new();
    let mut prepared: Live = Ok(());
    for t in [&topic, &trace_topic] {
        if let Err(e) = bootstrap_topic(&client, &namesrv, t).await {
            prepared = Err(format!("bootstrap topic {t}: {e}"));
            break;
        }
    }
    if prepared.is_ok() {
        prepared = match get_route(&client, &namesrv, &topic)
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
        ck.abort("R0 bootstrap", &e);
        client.shutdown();
        report(&mut ck);
        return ExitCode::FAILURE;
    }
    println!("   broker      = {broker_name} @ {broker_addr}");

    let scenarios: Vec<(&str, Live)> = vec![
        (
            "R1 allocate_strategy",
            r1_allocate_strategy(
                &client,
                &namesrv,
                &broker_addr,
                &broker_name,
                &topic,
                &producer_group,
                &consumer_group,
                &mut ck,
            )
            .await,
        ),
        (
            "R2 consume_executor",
            r2_consume_executor(
                &client,
                &broker_addr,
                &broker_name,
                &topic,
                &producer_group,
                &consumer_group,
                &mut ck,
            )
            .await,
        ),
        (
            "R3 trace_hook",
            r3_trace_hook(
                &client,
                &broker_addr,
                &broker_name,
                &topic,
                &trace_topic,
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

    if let Err(e) =
        r4_cleanup(&client, &namesrv, &broker_addr, &[topic.as_str(), trace_topic.as_str()], &mut ck)
            .await
    {
        ck.abort("R4 cleanup", &e);
    }

    client.shutdown();
    report(&mut ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn report(ck: &mut Checker) {
    println!("== summary: {} passed, {} failed ==", ck.passed, ck.failed.len());
    for f in &ck.failed {
        println!("   FAILED {f}");
    }
}
