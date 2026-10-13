//! 真机静态主题腿（逐条真机断言）：
//!
//! S1 createStaticTopic(513)：单 broker，totalQueues=8，物理 4 队列；
//! S2 路由回读 TopicQueueMappingByBroker；
//! S3 topicRouteData2EndpointsForStaticTopic：8 条端点（0-3 → broker，
//!    4-7 → `__syslo__none__`）；
//! S4 消费者实例把 mock 队列名重写到真实 broker（表外 id 透传）；
//! S5 端到端：生产者发 4 条、PUSH 消费者收齐 4 条。
//!
//! ```text
//! cargo run --example live_static_topic -- 127.0.0.1:9876
//! ```
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{DefaultMQPushConsumer, ConsumerConfig};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently, SendStatus,
};
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::topic_config::TopicConfig;
use rocketmq_client_remoting::client::static_topic::{
    get_mock_broker_name, topic_route_data_2_endpoints_for_static_topic,
    LOGICAL_QUEUE_MOCK_NOT_EXIST,
};

const PHYSICAL_QUEUES: i32 = 4;
const TOTAL_QUEUES: i32 = 8;

struct Tally {
    pass: AtomicUsize,
    fail: AtomicUsize,
}

impl Tally {
    fn new() -> Arc<Tally> {
        Arc::new(Tally {
            pass: AtomicUsize::new(0),
            fail: AtomicUsize::new(0),
        })
    }
    fn check(&self, name: &str, ok: bool, detail: &str) {
        if ok {
            self.pass.fetch_add(1, Ordering::Relaxed);
            println!("  [PASS] {name}  {detail}");
        } else {
            self.fail.fetch_add(1, Ordering::Relaxed);
            println!("  [FAIL] {name}  {detail}");
        }
    }
    fn result(&self) -> (usize, usize) {
        (
            self.pass.load(Ordering::Relaxed),
            self.fail.load(Ordering::Relaxed),
        )
    }
}

#[derive(Default)]
struct Inbox(Mutex<Vec<Vec<u8>>>);

impl Inbox {
    fn push(&self, body: Vec<u8>) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).push(body);
    }
    fn count(&self) -> usize {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

impl MessageListenerConcurrently for Inbox {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        for m in msgs {
            self.push(m.get_body().to_vec());
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

fn mapping_detail(topic: &str, broker_name: &str) -> serde_json::Value {
    let mut hosted = serde_json::Map::new();
    for gid in 0..PHYSICAL_QUEUES {
        hosted.insert(
            gid.to_string(),
            serde_json::json!([{
                "gen": 0,
                "queueId": gid,
                "bname": broker_name,
                "logicOffset": 0,
                "startOffset": 0,
                "endOffset": -1,
                "timeOfStart": -1,
                "timeOfEnd": -1,
            }]),
        );
    }
    let curr: serde_json::Map<String, serde_json::Value> = (0..PHYSICAL_QUEUES)
        .map(|g| (g.to_string(), serde_json::json!(g)))
        .collect();
    serde_json::json!({
        "topic": topic,
        "scope": "__global__",
        "totalQueues": TOTAL_QUEUES,
        "bname": broker_name,
        "epoch": 0,
        "dirty": false,
        "currIdMap": curr,
        "hostedQueues": hosted,
    })
}

#[tokio::main]
async fn main() -> ExitCode {
    let ns = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9876".into());
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let topic = format!("RustStaticLive{stamp}");
    let group = format!("rust-static-pg-{stamp}");
    let tally = Tally::new();

    println!("\nS1 起管理端，createStaticTopic(513)");
    let admin = DefaultMQAdminExt::with_config(AdminConfig {
        instance_name: format!("STADMIN-{stamp}"),
        name_server_addrs: vec![ns.clone()],
        timeout_millis: 10_000,
        ..Default::default()
    });
    if let Err(e) = admin.start().await {
        println!("  [FATAL] admin start: {e}");
        return ExitCode::FAILURE;
    }
    let mut broker_name = String::new();
    let mut broker_addr = String::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        match admin.fetch_broker_cluster_info().await {
            Ok(info) if !info.broker_addr_table.is_empty() => {
                let (name, addrs) = &info.broker_addr_table[0];
                broker_name = name.clone();
                broker_addr = addrs
                    .iter()
                    .min_by_key(|(id, _)| *id)
                    .map(|(_, a)| a.clone())
                    .unwrap_or_default();
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    if broker_addr.is_empty() {
        println!("  [FATAL] broker 未注册");
        return ExitCode::FAILURE;
    }
    tally.check("S1 broker 地址解析", true, &format!("{broker_name}({broker_addr})"));

    let detail = mapping_detail(&topic, &broker_name);
    let config = TopicConfig {
        topic_name: topic.clone(),
        read_queue_nums: PHYSICAL_QUEUES,
        write_queue_nums: PHYSICAL_QUEUES,
        ..Default::default()
    };
    match admin
        .create_static_topic(&broker_addr, "TBW102", &config, &detail, true)
        .await
    {
        Ok(()) => tally.check("S1 createStaticTopic(513) 被接受", true,
            &format!("totalQueues={TOTAL_QUEUES}")),
        Err(e) => {
            tally.check("S1 createStaticTopic(513) 被接受", false, &e.to_string());
            return ExitCode::FAILURE;
        }
    }

    println!("\nS2/S3 路由回读 + 端点集推导");
    let mut route = None;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        match admin.examine_topic_route(&topic).await {
            Ok(r) if r
                .topic_queue_mapping_by_broker
                .as_ref()
                .map(|m| !m.is_empty())
                .unwrap_or(false) =>
            {
                route = Some(r);
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(1000)).await,
        }
    }
    match route {
        Some(_) => tally.check("S2 路由携带 TopicQueueMappingByBroker", true, ""),
        None => {
            tally.check("S2 路由携带 TopicQueueMappingByBroker", false, "30s 超时");
            return ExitCode::FAILURE;
        }
    }
    let route = route.unwrap();
    let endpoints = topic_route_data_2_endpoints_for_static_topic(&topic, &route);
    match &endpoints {
        Some(_) => tally.check("S3 端点集非空", true, ""),
        None => {
            tally.check("S3 端点集非空", false, "端点集为 None");
            return ExitCode::FAILURE;
        }
    }
    let endpoints = endpoints.unwrap();
    let mock_name = get_mock_broker_name("__global__");
    tally.check(
        "S3 端点总数 = totalQueues",
        endpoints.len() == TOTAL_QUEUES as usize,
        &format!("got {} want {TOTAL_QUEUES}", endpoints.len()),
    );
    let hosted_ok = (0..PHYSICAL_QUEUES).all(|gid| {
        endpoints
            .get(&MessageQueue::new(&topic, &mock_name, gid))
            .map(|v| v == &broker_name)
            .unwrap_or(false)
    });
    tally.check(
        &format!("S3 承载 id 0-{} → broker", PHYSICAL_QUEUES - 1),
        hosted_ok,
        "",
    );
    let unhosted_ok = (PHYSICAL_QUEUES..TOTAL_QUEUES).all(|gid| {
        endpoints
            .get(&MessageQueue::new(&topic, &mock_name, gid))
            .map(|v| v == LOGICAL_QUEUE_MOCK_NOT_EXIST)
            .unwrap_or(false)
    });
    tally.check(
        &format!("S3 无承载 id {PHYSICAL_QUEUES}-{} → __syslo__none__", TOTAL_QUEUES - 1),
        unhosted_ok,
        "",
    );

    println!("\nS4/S5 实例重写 + 端到端收发");
    let inbox = Arc::new(Inbox::default());
    let consumer = match DefaultMQPushConsumer::with_config(ConsumerConfig {
        consumer_group: group.clone(),
        name_server_addrs: vec![ns.clone()],
        instance_name: format!("live-static-{}-{stamp}", group),
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            println!("  [FATAL] 消费者构造: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(e) = consumer.subscribe(&topic, "*") {
        println!("  [FATAL] subscribe: {e}");
        return ExitCode::FAILURE;
    }
    consumer.set_message_listener_concurrently(inbox.clone());
    if let Err(e) = consumer.start().await {
        println!("  [FATAL] consumer start: {e}");
        return ExitCode::FAILURE;
    }
    tokio::time::sleep(Duration::from_secs(4)).await;

    match consumer.mq_client() {
        Ok(inst) => {
            let rewrite_ok = (0..PHYSICAL_QUEUES).all(|gid| {
                inst.get_broker_name_from_message_queue(&MessageQueue::new(
                    &topic, &mock_name, gid,
                )) == broker_name
            });
            tally.check("S4 实例把 mock 队列重写到真实 broker", rewrite_ok, "");
            let sentinel = inst.get_broker_name_from_message_queue(&MessageQueue::new(
                &topic,
                &mock_name,
                TOTAL_QUEUES - 1,
            ));
            tally.check(
                "S4 无承载 id 返回哨兵值",
                sentinel == LOGICAL_QUEUE_MOCK_NOT_EXIST,
                &sentinel,
            );
            let outside = inst.get_broker_name_from_message_queue(&MessageQueue::new(
                &topic, &mock_name, 99,
            ));
            tally.check("S4 表外 id 透传 mock 名", outside == mock_name, &outside);
            let plain = inst.get_broker_name_from_message_queue(&MessageQueue::new(
                &format!("Plain{stamp}"),
                &broker_name,
                0,
            ));
            tally.check("S4 非静态主题原样透传", plain == broker_name, &plain);
        }
        Err(e) => tally.check("S4 实例访问", false, &e.to_string()),
    }

    let producer = match DefaultMQProducer::new(&format!("rust-static-prod-{stamp}")) {
        Ok(p) => p,
        Err(e) => {
            println!("  [FATAL] producer 构造: {e}");
            return ExitCode::FAILURE;
        }
    };
    producer.set_namesrv_addr(&ns);
    if let Err(e) = producer.start().await {
        println!("  [FATAL] producer start: {e}");
        return ExitCode::FAILURE;
    }
    let mut sent = 0;
    for i in 0..PHYSICAL_QUEUES {
        let mut msg = Message::new(&topic, Some(format!("static-{i}").as_bytes()));
        match producer.send(&mut msg, Some(5_000), None).await {
            Ok(sr) if sr.status == SendStatus::SendOk => {
                sent += 1;
            }
            other => println!("  [WARN] send #{i}: {other:?}"),
        }
    }
    producer.shutdown();
    tally.check(&format!("S5 发送 {PHYSICAL_QUEUES} 条"), sent == PHYSICAL_QUEUES as usize,
        &format!("sent={sent}"));

    let deadline = Instant::now() + Duration::from_secs(40);
    while inbox.count() < PHYSICAL_QUEUES as usize && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let got = inbox.count();
    tally.check(
        &format!("S5 消费收齐 {PHYSICAL_QUEUES} 条"),
        got >= PHYSICAL_QUEUES as usize,
        &format!("got={got}"),
    );
    consumer.shutdown();
    admin.shutdown();

    let (pass, fail) = tally.result();
    println!("\n=== RESULT pass={pass} fail={fail} ===");
    if fail == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
