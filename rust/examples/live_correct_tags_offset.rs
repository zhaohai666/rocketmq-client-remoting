//! correctTagsOffset（Java `DefaultMQPushConsumerImpl:713-717`，调用点 `:394-401`）真机验证。
//!
//! 与 `python/verify_correct_tags_offset_live.py`、`cpp/examples/live_correct_tags_offset.cpp`、
//! csharp 的对应场景同题、逐条对应。
//!
//! 离线单测（`src/client/consumer.rs` 的 `correct_tags_offset_*` 与
//! `dispatch_loop_keeps_the_correction_out_until_the_listener_returns`）锁的是**判据**；
//! 这里锁真机上两件离线锁不住的事：
//! - **A 修正确实走到了 broker**：位点最终由 `UPDATE_CONSUMER_OFFSET` 落盘，只有真集群能
//!   证明 broker 上的已提交位点前移了。
//! - **B 是在零投递的前提下前移的**：订阅表达式永不匹配时，broker 侧按组订阅过滤
//!   （`PullMessageProcessor` 拿 heartbeat 注册的 SubscriptionData）→ `PULL_RETRY_IMMEDIATELY`
//!   → 客户端 `NO_MATCHED_MSG`。没有修正时这条队列的位点永远停在"未提交"。
//!
//! 场景：
//! - S1 对照组：`TagA` 订阅正常消费 5 条 —— 证明消息确实在队列里，且"已提交位点 == 各队列
//!   maxOffset"这个数值口径就是常规消费的落点（排除 S2 的假绿）。
//! - S2 NO_MATCHED_MSG：`TagB` 订阅（永不匹配）。断言 listener 0 条 + 每条队列的已提交位点
//!   == 该队列 maxOffset。
//! - S3 NO_NEW_MSG：同一个消费者启动时自动补上 `%RETRY%<group>`（该队列空）→
//!   `PULL_NOT_FOUND` → `NO_NEW_MSG`。断言 broker 上出现值 == maxOffset(0) 的记录。
//! - S4 收尾：再等一个静默窗口，listener 依旧是 0 条（修正不会凭空投递）。
//!
//! 用法：
//! ```text
//! cargo run --example live_correct_tags_offset -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt, MessageQueue};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;

const QUEUES: i32 = 4;
const MSGS: usize = 5;
/// 持久化首跳 10s + 周期 5s（默认值），再留一个周期的余量。
const PERSIST_WINDOW_SECS: u64 = 45;

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

async fn poll_until(mut pred: impl FnMut() -> bool, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if pred() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// 只数到达条数与 tag：这一趟关心的是"一条都不该来"，不是消息内容。
struct Sink {
    arrivals: Mutex<Vec<String>>,
}

impl Sink {
    fn new() -> Arc<Sink> {
        Arc::new(Sink {
            arrivals: Mutex::new(Vec::new()),
        })
    }

    fn arrivals(&self) -> usize {
        lock(&self.arrivals).len()
    }
}

impl MessageListenerConcurrently for Sink {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        let mut arrivals = lock(&self.arrivals);
        for m in msgs {
            arrivals.push(m.get_tags().unwrap_or("").to_string());
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

fn broker_route(route: &TopicRouteData) -> Result<(String, String), String> {
    let bd = route
        .broker_datas
        .first()
        .ok_or_else(|| "route has no brokerData".to_string())?;
    let addr = bd
        .select_broker_addr()
        .ok_or_else(|| format!("broker {} has no address", bd.broker_name))?;
    Ok((bd.broker_name.clone(), addr))
}

struct Fixture {
    namesrv: String,
    stamp: u64,
    broker_addr: String,
    producer: DefaultMQProducer,
    admin: MQClientInstance,
}

impl Fixture {
    async fn new(namesrv: &str) -> Result<Fixture, String> {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let producer = DefaultMQProducer::new(&format!("rust-live-cto-pg-{stamp}"))
            .map_err(|e| format!("producer build failed: {e}"))?;
        producer.set_namesrv_addr(namesrv);
        let admin = MQClientInstance::new(
            &format!("rust-live-cto-admin-{stamp}"),
            vec![namesrv.to_string()],
        );
        admin
            .start()
            .await
            .map_err(|e| format!("admin start failed: {e}"))?;
        producer
            .start()
            .await
            .map_err(|e| format!("producer start failed: {e}"))?;
        let route = admin
            .get_topic_route_data(MixAll::DEFAULT_TOPIC)
            .await
            .ok_or_else(|| format!("no route of {} from namesrv", MixAll::DEFAULT_TOPIC))?;
        let (_, broker_addr) = broker_route(&route)?;
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            stamp,
            broker_addr,
            producer,
            admin,
        })
    }

    fn topic(&self) -> String {
        format!("RustLiveCto{}", self.stamp)
    }

    /// 该 topic 在路由上的真实队列列表。
    ///
    /// 不能用「主 topic 有几个队列」当通用假设：`%RETRY%<group>` 是 broker 建的，
    /// 只有 1 个队列（Python 参考实现同样按路由查）。照 4 个去查会把"只有 q0 有记录"
    /// 误报成失败。
    async fn queues(&self, topic: &str) -> Vec<MessageQueue> {
        match self.admin.get_topic_publish_info(topic, false).await {
            Ok(publish) => publish
                .msg_queue_list()
                .into_iter()
                .map(|q| MessageQueue::new(&q.topic, &q.broker_name, q.queue_id))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    async fn create_topic(&self, topic: &str) -> Result<(), String> {
        self.admin
            .create_topic_in_broker(
                &self.broker_addr,
                MixAll::DEFAULT_TOPIC,
                topic,
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
            .map_err(|e| format!("create topic {topic} failed: {e}"))?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    fn consumer(
        &self,
        group: &str,
        topic: &str,
        expression: &str,
        sink: Arc<Sink>,
        from_first: bool,
    ) -> Result<DefaultMQPushConsumer, String> {
        let cfg = ConsumerConfig {
            consumer_group: group.to_string(),
            name_server_addrs: vec![self.namesrv.clone()],
            instance_name: format!("live-{group}-{}", self.stamp),
            consume_from_where: if from_first {
                ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string()
            } else {
                ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET.to_string()
            },
            consume_message_batch_max_size: 3,
            ..Default::default()
        };
        let consumer = DefaultMQPushConsumer::with_config(cfg)
            .map_err(|e| format!("build consumer failed: {e}"))?;
        consumer
            .subscribe(topic, expression)
            .map_err(|e| format!("subscribe {topic}/{expression} failed: {e}"))?;
        consumer.set_message_listener_concurrently(sink);
        Ok(consumer)
    }

    /// [(mq, maxOffset, committed)]；committed=None 表示 broker 上查无记录。
    async fn offsets(
        &self,
        group: &str,
        topic: &str,
    ) -> Result<Vec<(MessageQueue, i64, Option<i64>)>, String> {
        let queues = self.queues(topic).await;
        if queues.is_empty() {
            return Err(format!("route of {topic} has no queue"));
        }
        let mut out = Vec::new();
        for mq in queues {
            let max_off = self
                .admin
                .get_max_offset(&mq, 5000, Some(&self.broker_addr))
                .await
                .unwrap_or(-1);
            let committed = self
                .admin
                .query_consumer_offset(group, &mq, 5000, Some(&self.broker_addr), false)
                .await
                .unwrap_or(None);
            out.push((mq, max_off, committed));
        }
        Ok(out)
    }

    fn shutdown(&self) {
        self.producer.shutdown();
        self.admin.shutdown();
    }
}

fn fmt(rows: &[(MessageQueue, i64, Option<i64>)]) -> String {
    rows.iter()
        .map(|(mq, max_off, off)| {
            format!(
                "q{}:{}/{}",
                mq.queue_id,
                off.map(|o| o.to_string()).unwrap_or_else(|| "None".to_string()),
                max_off
            )
        })
        .collect::<Vec<String>>()
        .join(" ")
}

async fn run(namesrv: &str) -> Checker {
    let mut ck = Checker::new();
    let fx = match Fixture::new(namesrv).await {
        Ok(fx) => fx,
        Err(e) => {
            ck.check("fixture", false, &e);
            return ck;
        }
    };
    let topic = fx.topic();
    let group_ctrl = format!("GID_rust_live_cto_ctrl_{}", fx.stamp);
    let group_test = format!("GID_rust_live_cto_test_{}", fx.stamp);
    if let Err(e) = fx.create_topic(&topic).await {
        ck.check("create topic", false, &e);
        fx.shutdown();
        return ck;
    }
    println!("topic={topic} queues={QUEUES}");

    // ---------- S1 对照组 ----------
    let ctrl_sink = Sink::new();
    let ctrl = match fx.consumer(&group_ctrl, &topic, "TagA", ctrl_sink.clone(), true) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S1 consumer", false, &e);
            fx.shutdown();
            return ck;
        }
    };
    if let Err(e) = ctrl.start().await {
        ck.check("S1 start", false, &format!("{e}"));
        ctrl.shutdown();
        fx.shutdown();
        return ck;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    for i in 0..MSGS {
        let mut msg = Message::new(&topic, Some(format!("cto-{i}").as_bytes()));
        msg.set_tags("TagA");
        if let Err(e) = fx.producer.send(&mut msg, Some(20000), None).await {
            ck.check("S1 send", false, &format!("{e}"));
            ctrl.shutdown();
            fx.shutdown();
            return ck;
        }
    }
    println!("S1: 已发送 {MSGS} 条 TagA，等对照组消费...");
    let got = poll_until(|| ctrl_sink.arrivals() >= MSGS, 30).await;
    ck.check(
        &format!("S1-对照组（TagA）收齐 {MSGS} 条 —— 消息确实在队列里"),
        got && ctrl_sink.arrivals() == MSGS,
        &format!("arrivals={}", ctrl_sink.arrivals()),
    );
    // 轮询 broker 侧口径（此处不能用闭包跨 await 持锁，改为显式循环）
    let mut ctrl_rows = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        match fx.offsets(&group_ctrl, &topic).await {
            Ok(rows) => {
                ctrl_rows = rows;
                if !ctrl_rows.is_empty()
                    && ctrl_rows.iter().all(|(_, m, o)| o.is_some() && o == &Some(*m))
                {
                    break;
                }
            }
            Err(e) => println!("  [WARN] read offsets: {e}"),
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        "S1-对照组的已提交位点 == 各队列 maxOffset（数值口径）",
        !ctrl_rows.is_empty() && ctrl_rows.iter().all(|(_, m, o)| o.is_some() && o == &Some(*m)),
        &fmt(&ctrl_rows),
    );
    ck.check(
        "S1-对照组确实把消息推进了队列（maxOffset 总和 > 0）",
        ctrl_rows.iter().map(|(_, m, _)| *m).sum::<i64>() > 0,
        &format!(
            "maxSum={}",
            ctrl_rows.iter().map(|(_, m, _)| *m).sum::<i64>()
        ),
    );
    ctrl.shutdown();

    // ---------- S2 NO_MATCHED_MSG：永不匹配的订阅，零投递但位点要走 ----------
    let test_sink = Sink::new();
    let test = match fx.consumer(&group_test, &topic, "TagB", test_sink.clone(), false) {
        Ok(c) => c,
        Err(e) => {
            ck.check("S2 consumer", false, &e);
            fx.shutdown();
            return ck;
        }
    };
    if let Err(e) = test.start().await {
        ck.check("S2 start", false, &format!("{e}"));
        fx.shutdown();
        return ck;
    }
    println!("S2: 消费者（TagB，永不匹配）已启动，等空应答修正落盘（首跳 10s + 周期 5s）...");
    let mut test_rows = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(PERSIST_WINDOW_SECS);
    loop {
        match fx.offsets(&group_test, &topic).await {
            Ok(rows) => {
                test_rows = rows;
                if !test_rows.is_empty()
                    && test_rows.iter().all(|(_, m, o)| o.is_some() && o == &Some(*m))
                {
                    break;
                }
            }
            Err(e) => println!("  [WARN] read offsets: {e}"),
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        "S2-零投递（listener 一条都没收到）",
        test_sink.arrivals() == 0,
        &format!("arrivals={}", test_sink.arrivals()),
    );
    ck.check(
        "S2-每条队列的已提交位点都 == 该队列 maxOffset（空应答修正生效）",
        !test_rows.is_empty() && test_rows.iter().all(|(_, m, o)| o.is_some() && o == &Some(*m)),
        &fmt(&test_rows),
    );
    ck.check(
        "S2-修正后的位点总和 == 对照组（同一条队列的最大位点）",
        test_rows.iter().map(|(_, _, o)| o.unwrap_or(-1)).sum::<i64>()
            == ctrl_rows.iter().map(|(_, m, _)| *m).sum::<i64>(),
        &format!(
            "test={} ctrl={}",
            test_rows.iter().map(|(_, _, o)| o.unwrap_or(-1)).sum::<i64>(),
            ctrl_rows.iter().map(|(_, m, _)| *m).sum::<i64>()
        ),
    );

    // ---------- S3 NO_NEW_MSG：%RETRY%<group> 空队列也要留下位点记录 ----------
    let retry_topic = format!("%RETRY%{group_test}");
    println!("S3: 等 {retry_topic} 的位点记录（空队列 NO_NEW_MSG）...");
    let mut retry_rows = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(PERSIST_WINDOW_SECS);
    loop {
        match fx.offsets(&group_test, &retry_topic).await {
            Ok(rows) => {
                retry_rows = rows;
                if !retry_rows.is_empty()
                    && retry_rows.iter().all(|(_, m, o)| o.is_some() && o == &Some(*m))
                {
                    break;
                }
            }
            Err(e) => println!("  [WARN] read retry offsets: {e}"),
        }
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        &format!("S3-{retry_topic} 上出现位点记录且等于 maxOffset"),
        !retry_rows.is_empty() && retry_rows.iter().all(|(_, m, o)| o.is_some() && o == &Some(*m)),
        &fmt(&retry_rows),
    );
    ck.check(
        "S3-该位点确实是 0（空队列的 nextBeginOffset）",
        !retry_rows.is_empty() && retry_rows.iter().all(|(_, _, o)| o == &Some(0)),
        &format!(
            "offsets={:?}",
            retry_rows.iter().map(|(_, _, o)| *o).collect::<Vec<_>>()
        ),
    );

    // ---------- S4 收尾：静默窗口内仍然零投递 ----------
    tokio::time::sleep(Duration::from_secs(6)).await;
    let after = test_sink.arrivals();
    ck.check(
        "S4-整轮下来 listener 依旧是 0 条（修正不会凭空投递）",
        after == 0,
        &format!("arrivals={after}"),
    );

    test.shutdown();
    fx.shutdown();
    ck
}

fn report(ck: &mut Checker) {
    println!(
        "\n== 结果：{} PASS / {} FAIL ==",
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
