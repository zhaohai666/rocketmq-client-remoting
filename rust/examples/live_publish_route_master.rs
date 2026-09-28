//! 发布路由必须跳过「没有 master 的 broker」真机验证（Java `MQClientInstance:294-303`）。
//!
//! 与 `python/verify_publish_route_master_live.py`、`cpp/examples/live_publish_route_master.cpp`、
//! `dotnet/examples/RocketMQ.Examples/LivePublishRouteMaster.cs` 同场景、逐条同断言。
//!
//! 前置：namesrv + master + slave 都在跑（按本地集群 runbook）；脚本自己**只停一次 master**
//! （`scripts/rmq_test_broker.sh` 只认 master 的 java 进程），`Drop` 保险保证 master 一定回来。
//!
//! Java 依据：`MQClientInstance.topicRouteData2TopicPublishInfo:294-303` 组装发布信息时，
//! brokerDatas 里没有同名 broker、或它的 brokerAddrs 没有 MASTER_ID，整条 QueueData 跳过。
//! 从节点自己也注册进 namesrv，且默认配置下照样带写位（`RouteInfoManager` 只在「prime slave
//! 且 enableActingMaster」时才抹掉 WRITE，本机 broker.conf 是 false），所以 master 一掉线，
//! 路由里同一个 brokerName 只剩 brokerId=1 —— 漏判这条，生产者就会把消息发到从节点上，
//! 而从节点对发送请求一律 reject（`SendMessageProcessor` ⇒ `SYSTEM_BUSY(2)`，**还是可重试
//! 码**），白烧重试。消费侧是另一份口径（`topicRouteData2TopicSubscribeInfo:318-332`：
//! 读位 + readQueueNums、**不要求有 master**），停窗口内消费者仍要看得见队列、还得能从从节点拉。
//!
//! 离线单测（`src/client/mq_client.rs` 的 `publish_route_*` 组）锁的是**判据**；真机锁的是
//! 判据作用在真实路由形状上的结果 —— 名字服务里 broker-a 真的只剩 `{1: slave}`。
//!
//! 场景（同一停窗口里做完）：
//! - S0 控制腿（master 在）：路由 {0: master, 1: slave}；发布队列 4、订阅队列 4。
//! - S1 预埋：每队列定点一条共 4 条，等从节点 store 追上（不然 S6 无从消费）。
//! - S2 停 master → 刷新路由直到 broker-a 只剩 {1: slave}。
//! - S3 (A) 发布信息组不出队列：访问器本端抛 `Can not find Message Queue for topic`
//!   —— 本端口没有「读原始表」的公开访问器，改用访问器语义断言：它只在发布信息**有队列**
//!   时才 Ok，此刻连重新拉回来的路由都组不出队列（比读表多证一次刷新腿）。
//! - S4 (C) 订阅队列仍是 4（消费侧不看 master）。
//! - S5 发送快速失败、报错里没有从节点地址（旧缓存腿打的是死掉的 master；周期刷新恰好
//!   已跑过则是本端 10005）；再显式把发送实例刷成停后形状：(A) 生效 —— 发送本端 10005
//!   且**一条 wire 都不发**。
//! - S5d 对照：定点发到该队列 → 地址解析两步都只认 master，主没了 ⇒ 本端
//!   `MQClientException("The broker[broker-a] not exist")`、**一条 wire 都不发**
//!   （Java `sendKernelImpl:919-924` + `findBrokerAddressInPublish:1295-1305`）——
//!   从节点因此根本收不到写请求（旧版本会打到从节点换一个可重试的 SYSTEM_BUSY(2)，
//!   白烧一轮重试）；S5c 若漏做，不指定队列的发送就是 3 次 wire 全被拒的下场。
//! - S5e (D) 订阅口径：顺序锁整台跳过（`RebalanceImpl#lock:153/lockAll:195` 只认主、
//!   不刷路由；对照腿证明同一窗口内从节点对该队列照常服务 —— 空锁集不是"从节点不可达"）。
//! - S5f (E) 订阅口径：POP 本端报「broker 不存在」（`PullAPIWrapper#popAsync:369-373`）。
//! - S5g (F) 位点读取：冷实例（缓存里没这个 topic）刷一次路由后**放宽**到从节点
//!   （`RemoteBrokerOffsetStore#fetchConsumeOffsetFromBroker:237-241`）。
//! - S6 (C 端到端) 停窗口内新起的 push 消费者仍看到 4 条队列，并从**从节点**把 S1 的 4 条收齐。
//! - S7 负控：master 拉回 → 发布队列恢复 4、两条失败发送都没在 broker 上留下消息、发送 SEND_OK。
//!
//! 用法：
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
/// 本端失败上界：发布信息为空时压根没有 wire 调用，真机给 500ms 已留量级余量。
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

/// broker 开关：`scripts/rmq_test_broker.sh`（**只碰 master**），四个语言的 live 用例共用。
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
    // 输出走**文件**而不是管道：start 会把 broker 拉成常驻进程，管道的写端被它继承，
    // 谁继承谁就等不到 EOF。
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

/// 收尾保险：无论用例走到哪一步（含提前 return / panic），都不能把测试集群留在
/// 「master 停着」的状态交给下一个用例 —— start 幂等，已在跑就直接返回。
struct BrokerGuard {
    script: PathBuf,
}

impl Drop for BrokerGuard {
    fn drop(&mut self) {
        let (up, _) = broker_ctl(&self.script, "status");
        if up {
            return;
        }
        println!("  [cleanup] 用例结束时 master 是 DOWN，补一次 start");
        let (ok, out) = broker_ctl(&self.script, "start");
        if !ok {
            println!("  [cleanup] master 仍没起来：{out}");
        }
    }
}

/// 异步条件等待：真机窗口里路由/位点的收敛是异步的，超时不算断言失败，
/// 由调用方对返回值做断言（拿不到就是 None）。
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
            println!("    (等不到 {what})");
            return None;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn has_id(addrs: &[(i64, String)], id: i64) -> bool {
    addrs.iter().any(|(k, _)| *k == id)
}

/// 只收 body：这一趟关心的是"从节点上的 4 条能不能收齐"，不关心位点与顺序。
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
        // 管理实例与生产者实例的 clientId 不同（instance_name 不同）⇒ 进程内是两个
        // MQClientInstance：S5 的「旧缓存」看的是发送实例自己那张表，这里要能分别驱动。
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

    /// 强制刷新后取 broker-a 的 brokerAddrs（刷不到返回 None）。
    async fn route_addrs(&self, topic: &str) -> Option<Vec<(i64, String)>> {
        if let Err(e) = self
            .admin
            .update_topic_route_info_from_name_server(topic, 5000, false)
            .await
        {
            println!("    (路由刷新失败: {e})");
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

    /// 关闭客户端 + 删 topic。删之前先确保 master 在（`start` 幂等）——
    /// 停窗口里提前退出时 topic 也要删得掉，不能留一份孤儿配置。
    async fn cleanup(&self, script: &PathBuf) {
        let _ = broker_ctl(script, "start");
        for addr in [&self.master, &self.slave] {
            if let Err(e) = self
                .admin
                .delete_topic_in_broker(addr, &self.topic, 5000)
                .await
            {
                println!("    (delete topic on {addr} 失败: {e})");
            }
        }
        if let Err(e) = self.admin.delete_topic_in_namesrv(&self.topic, 5000).await {
            println!("    (delete topic in namesrv 失败: {e})");
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

    // ---------- S0 控制腿 ----------
    println!("S0 控制腿（master 在）：路由 {{0: master, 1: slave}}、发布/订阅各 {QUEUES} 条");
    if let Err(e) = fx
        .admin
        .create_topic_in_route(&topic, QUEUES, QUEUES, DEFAULT_PERM, 0, None, 5000)
        .await
    {
        ck.check("S0 建 topic（master）", false, &e.to_string());
        return;
    }
    // 从节点也直建一份，不赌 SlaveSynchronize 的 5s 周期
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
        println!("    (从节点建 topic 失败: {e})");
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
        "路由 {0, 1}",
    )
    .await;
    ck.check(
        "S0 路由含 {0: master, 1: slave}",
        addrs.is_some(),
        &format!("broker_addrs={addrs:?}"),
    );
    let Some(addrs) = addrs else {
        return;
    };
    let route_slave = addrs.iter().find(|(id, _)| *id == 1).map(|(_, a)| a.clone());
    ck.check(
        "S0 从节点地址与参数一致",
        route_slave.as_deref() == Some(fx.slave.as_str()),
        &format!("route={route_slave:?} argv={}", fx.slave),
    );

    let queues = match fx.seed_queues().await {
        Ok(q) => q,
        Err(e) => {
            ck.check("S0 发布队列 4（控制）", false, &e.to_string());
            return;
        }
    };
    let mut queues = queues;
    queues.sort_by_key(|q| q.queue_id);
    ck.check(
        &format!("S0 发布队列 {QUEUES}（控制）"),
        queues.len() == QUEUES as usize,
        &format!(
            "queues={:?}",
            queues.iter().map(|q| (&q.broker_name, q.queue_id)).collect::<Vec<_>>()
        ),
    );
    let subs = fx.admin.get_topic_subscribe_info(&topic).await;
    ck.check(
        &format!("S0 订阅队列 {QUEUES}（控制）"),
        subs.len() == QUEUES as usize,
        &format!(
            "queues={:?}",
            subs.iter().map(|q| (&q.broker_name, q.queue_id)).collect::<Vec<_>>()
        ),
    );
    if queues.len() != QUEUES as usize {
        return;
    }

    // ---------- S1 预埋 ----------
    println!("\nS1 预埋 {QUEUES} 条（每队列定点一条）并等从节点 store 追上");
    let mut seeded: Vec<Vec<u8>> = Vec::new();
    for (i, mq) in queues.iter().enumerate() {
        let body = format!("pr-master-{i}").into_bytes();
        let mut msg = Message::new(&topic, Some(&body));
        match fx.producer.send(&mut msg, Some(20000), Some(mq)).await {
            Ok(r) if r.status == SendStatus::SendOk => seeded.push(body),
            Ok(r) => ck.check(
                &format!("S1 第 {i} 条预埋 SEND_OK"),
                false,
                &format!("status={:?}", r.status),
            ),
            Err(e) => ck.check(&format!("S1 第 {i} 条预埋 SEND_OK"), false, &e.to_string()),
        }
    }
    ck.check(
        &format!("S1 {QUEUES} 条预埋全部 SEND_OK"),
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
            Err(e) => println!("    (master maxOffset 查询失败: {e})"),
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
                        println!("    (从节点取 maxOffset 失败: {e})");
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
        "从节点复制追上",
    )
    .await;
    ck.check(
        &format!("S1 {QUEUES} 条已复制到从节点 store"),
        caught_up.is_some(),
        &format!("master={master_max:?} slave={:?}", caught_up.unwrap_or_default()),
    );

    // ---------- S2 停 master ----------
    println!("\nS2 停 master（scripts/rmq_test_broker.sh stop），等路由只剩从节点");
    let script_for_stop = script.to_path_buf();
    let stopped = tokio::task::spawn_blocking(move || broker_ctl(&script_for_stop, "stop")).await;
    let (ok, out) = stopped.unwrap_or_else(|e| (false, format!("stop 任务 panic: {e}")));
    ck.check("S2 master 已优雅停机", ok, &out);

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
        "masterless 路由",
    )
    .await;
    ck.check(
        "S2 路由里 broker-a 只剩 {1: slave}",
        down_addrs.is_some(),
        &format!("broker_addrs={down_addrs:?}"),
    );
    if down_addrs.is_none() {
        return;
    }

    // ---------- S3 (A) 发布信息组不出队列 ----------
    println!("\nS3 (A) 发布信息跳过没有 master 的 broker");
    // 访问器契约：只在发布信息**有队列**时 Ok，否则本端 10005。此刻它抛，
    // 等价于 Python 读表得到的 `msg_queue_list == 0`（且这里还多证了一次
    // 「重新拉回来的路由照样组不出队列」——访问器会先刷一次路由）。
    let publish_down = fx.admin.get_topic_publish_info(&topic, false).await;
    let publish_err = match &publish_down {
        Ok(info) => format!("意外拿到 {} 条队列", info.msg_queue_list().len()),
        Err(e) => e.to_string(),
    };
    ck.check(
        "S3 停 master 后发布队列 == 0（访问器本端抛「选不到队列」）",
        publish_down
            .as_ref()
            .err()
            .map(|e| e.to_string().contains("Can not find Message Queue for topic"))
            .unwrap_or(false),
        &publish_err,
    );

    // ---------- S4 (C) 订阅队列 ----------
    let subs_down = fx.admin.get_topic_subscribe_info(&topic).await;
    ck.check(
        &format!("S4 (C) 订阅队列仍是 {QUEUES}、且都在 {BROKER_NAME}（消费侧不看 master）"),
        subs_down.len() == QUEUES as usize && subs_down.iter().all(|q| q.broker_name == BROKER_NAME),
        &format!(
            "queues={:?}",
            subs_down.iter().map(|q| (&q.broker_name, q.queue_id)).collect::<Vec<_>>()
        ),
    );

    // ---------- S5 (A 定型) 发送快速失败 ----------
    // 分两段看：**缓存还没刷**时（现实中 30s 周期任务未到）发送实例手里还是停前的旧路由，
    // 地址解析落在死掉的 master 上，快速失败、绝不静默改发从节点；**路由刷成停后形状**后
    // （周期任务 / 显式刷新），(A) 生效：发布队列为空，发送连一条 wire 都不发。
    println!("\nS5 不指定队列的同步发送：旧缓存快速失败 → 刷新后本端快速失败");
    let began = Instant::now();
    let mut msg = Message::new(&topic, Some(b"must-not-send"));
    let stale = fx.producer.send(&mut msg, Some(20000), None).await;
    let stale_ms = began.elapsed().as_secs_f64() * 1000.0;
    let stale_text = match &stale {
        Ok(r) => format!("Ok(status={:?})", r.status),
        Err(e) => e.to_string(),
    };
    // 这一腿是机会腿：周期刷新是否已经跑过不由本脚本定。两条腿的共同判据是
    // "快速失败 + 绝不落到从节点地址上"（旧缓存腿打的是死掉的 master）。
    ck.check(
        "S5 发送快速失败，且报错里没有从节点地址（绝不改发从节点）",
        stale.is_err() && stale_ms < LOCAL_BUDGET_MS && !stale_text.contains(&fx.slave),
        &format!("{stale_ms:.0}ms {stale_text}"),
    );

    // 让发送实例自己的路由缓存刷成停后形状（与 30s 周期任务同一条代码路径）
    let Some(pcli) = fx.producer.client() else {
        ck.check("S5b 拿到发送实例", false, "producer 未启动");
        return;
    };
    if let Err(e) = pcli
        .update_topic_route_info_from_name_server(&topic, 5000, false)
        .await
    {
        println!("    (发送实例路由刷新失败: {e})");
    }
    let send_publish = pcli.get_topic_publish_info(&topic, false).await;
    ck.check(
        "S5b 发送实例的发布队列也 == 0（(A) 就作用在这里）",
        send_publish
            .as_ref()
            .err()
            .map(|e| e.to_string().contains("Can not find Message Queue for topic"))
            .unwrap_or(false),
        &match &send_publish {
            Ok(info) => format!("意外拿到 {} 条队列", info.msg_queue_list().len()),
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
        "S5c 刷新后：本端 10005 抛「选不到队列」，无 wire 调用（无 BrokersSent）",
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

    // ---------- S5d 对照：定点发送的下场 ----------
    println!("\nS5d 对照：定点发到该队列 → 本端报「broker 不存在」，一条 wire 都不发");
    let mq0 = MessageQueue::new(&topic, BROKER_NAME, 0);
    let mut msg3 = Message::new(&topic, Some(b"pinned-to-slave"));
    let began = Instant::now();
    let pinned = fx.producer.send(&mut msg3, Some(20000), Some(&mq0)).await;
    let pinned_ms = began.elapsed().as_secs_f64() * 1000.0;
    let pinned_text = match &pinned {
        Ok(r) => format!("Ok(status={:?})", r.status),
        Err(e) => e.to_string(),
    };
    // 定点发送不走 `sendDefaultImpl` 的队列选择，地址解析是它唯一的路由来源：
    // 发布地址平表里只有 brokerId=1 ⇒ 刷一次路由仍拿不到 ⇒ 本端立刻报错
    // （Java `sendKernelImpl:919-924/:1100`），**不像旧版本那样打到从节点**换一个
    // 可重试的 SYSTEM_BUSY(2)。没有 BrokersSent 后缀 = 一次 send 都没发出去。
    ck.check(
        "S5d 定点发送本端报「The broker[broker-a] not exist」，无 wire 调用（无 BrokersSent）",
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

    // ---------- S5e (D) 订阅口径：顺序锁只认主 ----------
    // Java `RebalanceImpl#lock:153 / lockAll:195` 走 findBrokerAddressInSubscribe(brokerName,
    // MASTER_ID, true)：只认主、**不刷路由**，拿不到就整台跳过。退到从节点上锁等于锁在从
    // 节点的锁管理器里，master 不知情，顺序消费的互斥静默失效。此刻 admin 实例的路由缓存
    // 已被 S2 刷成 masterless 形状，任何"退让"口径都会落到从节点上并拿回非空锁集。
    println!("\nS5e (D) 顺序锁：停窗口内整台跳过（不刷路由、不发 wire）");
    let group = format!("GID_PrMasterRs_{}", fx.stamp);
    let client_id = format!("pr_master_rs_lock_{}", fx.stamp);
    let lock_mqs = vec![MessageQueue::new(&topic, BROKER_NAME, 0)];
    let began = Instant::now();
    let locks = match fx.admin.lock_batch_mq(&group, &client_id, &lock_mqs, 3000).await {
        Ok(l) => l,
        Err(e) => {
            println!("    (lock_batch_mq 失败: {e})");
            Vec::new()
        }
    };
    let lock_ms = began.elapsed().as_secs_f64() * 1000.0;
    ck.check(
        "S5e 只剩从节点时一台都锁不上（旧口径会退到从节点上锁）",
        locks.is_empty() && lock_ms < LOCAL_BUDGET_MS,
        &format!(
            "{lock_ms:.0}ms locked={:?}",
            locks.iter().map(|q| (&q.broker_name, q.queue_id)).collect::<Vec<_>>()
        ),
    );
    // 对照腿：同一窗口内从节点对该队列照常服务（本端口的 invoke_sync 不开放给 example，
    // 打不出"点名从节点锁一把"的原始报文；用从节点仍在服务同一条队列来排除
    // "空锁集是因为从节点不可达"这一解释）。
    let slave_serving = fx
        .admin
        .get_max_offset(&lock_mqs[0], 5000, Some(&fx.slave))
        .await;
    ck.check(
        "S5e2 对照：窗口内从节点对该队列照常服务（空集不是从节点不可达）",
        matches!(slave_serving, Ok(v) if v >= 1),
        &format!("slave maxOffset={slave_serving:?}"),
    );
    let began = Instant::now();
    let _ = fx.admin.unlock_batch_mq(&group, &client_id, &lock_mqs, 3000).await;
    let unlock_ms = began.elapsed().as_secs_f64() * 1000.0;
    ck.check(
        "S5e3 解锁同样安静跳过（不抛、不发）",
        unlock_ms < LOCAL_BUDGET_MS,
        &format!("{unlock_ms:.0}ms"),
    );

    // ---------- S5f (E) 订阅口径：POP 只认主 ----------
    println!("\nS5f (E) POP 拉取：本端报「The broker[broker-a] not exist」（不发 wire）");
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
        "S5f 停窗口内 POP 本端报「The broker[broker-a] not exist」（不是从节点回的错）",
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

    // ---------- S5g (F) 位点读取：刷一次路由后放宽到从节点 ----------
    // Java `RemoteBrokerOffsetStore#fetchConsumeOffsetFromBroker:237-241`：只认主 → 刷一次
    // 路由 → 重查**放宽**（onlyThisBroker=false，位点是 HA 复制来的同一份数据，可以从从节点
    // 读）。冷实例（路由缓存里没有这个 topic）是这条路径最纯的形状：旧的 `_broker_addr`
    // 口径在此直接报「No route info of this topic」，连刷新都没有。
    println!("\nS5g (F) 位点读取：冷实例刷一次路由后放宽到从节点");
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
        "S5g 冷实例位点读取不报错：刷路由 → 退到从节点由 broker 答复",
        off.is_ok(),
        &format!("{cold_ms:.0}ms {cold_text}"),
    );
    cold.shutdown();

    // ---------- S6 (C 端到端) 停窗口内消费 ----------
    println!("\nS6 停窗口内新起的 push 消费者：{QUEUES} 条队列 + 从从节点收齐预埋的 {QUEUES} 条");
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
    // client 实例是 start 时才建的：要在 start **之后**再取一次。
    let consumer_client = consumer.client().or(consumer_client);
    *lock(&fx.consumer) = Some(consumer);
    match consumer_client {
        Some(ccli) => {
            let subs_in_window = ccli.get_topic_subscribe_info(&topic).await;
            ck.check(
                &format!("S6a 窗口内消费者自己的订阅信息也是 {QUEUES} 条"),
                subs_in_window.len() == QUEUES as usize,
                &format!(
                    "queues={:?}",
                    subs_in_window.iter().map(|q| (&q.broker_name, q.queue_id)).collect::<Vec<_>>()
                ),
            );
        }
        None => ck.check("S6a 拿到消费者自己的实例", false, "consumer.client() 为 None"),
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
        "4 条预埋消息",
    )
    .await;
    ck.check(
        &format!("S6 停 master 期间从从节点收齐 {QUEUES} 条"),
        got_ok.is_some(),
        &format!("got=[{}]", fmt_bodies(&sink.bodies())),
    );

    // ---------- S7 负控（先把 master 拉回来） ----------
    println!("\nS7 负控：master 拉回后发布队列恢复、发送恢复");
    let script_for_start = script.to_path_buf();
    let started = tokio::task::spawn_blocking(move || broker_ctl(&script_for_start, "start")).await;
    let (ok, out) = started.unwrap_or_else(|e| (false, format!("start 任务 panic: {e}")));
    if !ok {
        ck.check("S7 master 复位", false, &out);
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
        "发布队列恢复",
    )
    .await;
    ck.check(
        &format!("S7 发布队列恢复 {QUEUES}"),
        queues_back.is_some(),
        &format!(
            "queues={:?}",
            queues_back
                .as_ref()
                .map(|q| q.iter().map(|x| (&x.broker_name, x.queue_id)).collect::<Vec<_>>())
        ),
    );
    // 两条失败发送都没在 broker 上留下消息：每条预埋队列的 maxOffset 仍是 1
    let mut after: Vec<(i32, i64)> = Vec::new();
    for mq in queues_back.as_ref().unwrap_or(&queues).iter() {
        match fx.admin.get_max_offset(mq, 5000, None).await {
            Ok(o) => after.push((mq.queue_id, o)),
            Err(e) => println!("    (maxOffset 查询失败: {e})"),
        }
    }
    ck.check(
        "S7b 两条失败发送没留下消息（maxOffset 仍是 1）",
        !after.is_empty() && after.iter().all(|(_, v)| *v == 1),
        &format!("maxOffset={after:?}"),
    );
    let mut back = Message::new(&topic, Some(b"pr-master-back"));
    let back_res = fx.producer.send(&mut back, Some(20000), None).await;
    ck.check(
        "S7c 发送恢复 SEND_OK",
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
        println!("master 没在跑：先按本地集群 runbook 起 namesrv + master + slave（{out}）");
        ck.check("前置 master UP", false, &out);
        return ck;
    }

    let fx = match Fixture::new(namesrv, master, slave).await {
        Ok(fx) => fx,
        Err(e) => {
            ck.check("fixture", false, &e);
            return ck;
        }
    };
    // 顺序：先声明 guard（后 drop），再跑场景；场景里任何提前 return 都会回到这里。
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
        "\n== 结果: {}/{} 通过 ==",
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
