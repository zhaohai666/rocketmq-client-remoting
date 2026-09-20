//! ACL 鉴权对**真实 5.5.1 broker** 的联调验证（对齐 `python/verify_acl_live.py` S1..S7，
//! 另补 Python 没测的拉模式签名链路 S8）。
//!
//! 前置：broker 开了新鉴权插件（broker.conf）：
//! ```text
//! authenticationEnabled=true
//! authenticationMetadataProvider=org.apache.rocketmq.auth.authentication.provider.LocalAuthenticationMetadataProvider
//! initAuthenticationUser={"username":"AK_TEST","password":"SK_TEST_SECRET_12345678"}
//! ```
//! `LocalAuthenticationMetadataProvider` 初始化时把 `initAuthenticationUser` 建成 SUPER
//! 用户，username 即 accessKey、password 即 secretKey。
//!
//! 断言的都是「签名真的被 broker 认/不认」，而不是本地编解码：
//! - S1 带凭据的实例建 topic 成功（管理路径签名被接受）。
//! - S2 **不带凭据**建 topic → broker 回 NO_PERMISSION(16)。
//! - S3 **secretKey 写错**的生产者发送 → 16。签名算错和没签名在 broker 侧同一种拒绝，
//!   所以这条同时反证「签名内容参与校验」而不是「只要带了 AccessKey 就放行」。
//! - S4 带凭据的生产者发 3 条全 SEND_OK，且 msgId 由 broker 赋值（非空即证签名请求被采纳）。
//! - S5 带凭据的 push 消费者收满 3 条 —— 心跳 / 长轮询拉取 / 位点提交三条 RPC 全程带签名，
//!   任一漏签都收不到消息。
//! - S6 **不带凭据**的原始 broker RPC（GET_CONSUMER_LIST_BY_GROUP）→ 16。
//!   注意必须走显式给 addr 的 [`get_consumer_list_by_group`]，不能用
//!   [`get_consumer_id_list_by_group`]：后者照 Python 把异常吞掉回 `None`，
//!   「被拒」和「成功」在调用方看来一模一样。
//! - S7 不带凭据查 **NameServer** 路由仍成功 —— 鉴权只在 broker 侧，
//!   反证钩子没把 namesrv 路径打坏。
//! - S8 带凭据的 pull 消费者：`fetch_subscribe_message_queues` / `pull` /
//!   `update_consume_offset` / `fetch_consume_offset` 四条签名 RPC 闭环。
//!   Python 的 ACL 验证只测了 push，拉模式的位点 RPC 是另一套请求码，这里补上。
//! - S9 清理：带凭据删掉本次建的 topic。
//!
//! ⚠ S4/S5 必须**先起消费者再发送**（Python 同一时序约束）：push 消费者默认
//! `CONSUME_FROM_LAST_OFFSET` 会把新消费组的初始位点解析成该队列**当时的** maxOffset，
//! 而 broker 的 consumequeue 是异步分发的，「先发再收」在三种语言之间结果不确定。
//! 这里既先起消费者、又显式用 `CONSUME_FROM_FIRST_OFFSET`，两道保险都留着。
//!
//! ⚠ 每个角色都用自己的 `instance_name`（→ 独立 clientId → 独立 [`MQClientInstance`]
//! → 独立 remoting 通道）。凭据是**注册到通道上**的，两个角色若复用同一实例，
//! 「无凭据被拒」会被另一个角色的签名带过去而变成假 PASS。
//!
//! 用法：
//! ```text
//! cargo run --example live_acl -- 127.0.0.1:9876 [accessKey] [secretKey]
//! ```
//!
//! [`get_consumer_list_by_group`]: rocketmq_client_remoting::client::mq_client::MQClientInstance::get_consumer_list_by_group
//! [`get_consumer_id_list_by_group`]: rocketmq_client_remoting::client::mq_client::MQClientInstance::get_consumer_id_list_by_group

use std::collections::BTreeSet;
use std::env;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::mq_client::MQClientInstance;
use rocketmq_client_remoting::client::producer::DefaultMQProducer;
use rocketmq_client_remoting::client::pull_consumer::{
    DefaultMQPullConsumer, PullConsumerConfig,
};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently, PullStatus,
};
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::common::topic_config::{TopicFilterType, DEFAULT_PERM};
use rocketmq_client_remoting::error::Error;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;
use rocketmq_client_remoting::remoting::protocol::route::TopicRouteData;
use rocketmq_client_remoting::remoting::rpchook::{AclClientRPCHook, RPCHook, SessionCredentials};

/// 建出来的 topic 队列数（与 Python 的 `create_topic(.., 4)` 一致）。
const QUEUE_NUMS: i32 = 4;
/// S4 发送条数。
const N_MSG: usize = 3;
/// broker 鉴权失败的响应码：所有拒绝都走 `AuthenticationPipeline` 抛
/// `AbortProcessException(NO_PERMISSION)`（Java broker/auth/pipeline/AuthenticationPipeline.java:53）。
const NO_PERMISSION: i32 = 16;
/// 等 push 消费者收满的窗口。
const RECV_WAIT: Duration = Duration::from_secs(30);

fn stamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// 断言累积器：跑完所有场景再汇总，首个失败不提前退出。
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

/// 锁中毒时照常取内值（一次 panic 不该让整轮验证连锁崩）。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// 拒绝类断言：必须是 broker 回的 NO_PERMISSION，不能是连不上/超时。
///
/// 「拿不到 16」和「拿到 16 但理由是别的」都得算失败 —— 没签名时如果 broker 被配成
/// 不鉴权，正向场景照样全绿，只有反向场景能戳穿这种假通过。
fn denied(err: &Error) -> bool {
    err.response_code() == Some(NO_PERMISSION)
}

fn code_of(err: &Error) -> String {
    match err.response_code() {
        Some(code) => format!("code={code}"),
        None => "code=?".to_string(),
    }
}

fn acl_hook(access_key: &str, secret_key: &str) -> Arc<dyn RPCHook> {
    Arc::new(AclClientRPCHook::new(SessionCredentials::new(
        access_key,
        secret_key,
    )))
}

// ------------------------------------------------------------------ listener

#[derive(Default)]
struct Inbox {
    bodies: Mutex<BTreeSet<String>>,
}

impl Inbox {
    fn len(&self) -> usize {
        lock(&self.bodies).len()
    }

    fn snapshot(&self) -> Vec<String> {
        lock(&self.bodies).iter().cloned().collect()
    }
}

struct AclListener(Arc<Inbox>);

impl MessageListenerConcurrently for AclListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        for m in msgs {
            lock(&self.0.bodies)
                .insert(String::from_utf8_lossy(m.get_body()).into_owned());
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

// ------------------------------------------------------------------ 夹具

struct Fixture {
    namesrv: String,
    stamp: String,
    access_key: String,
    secret_key: String,
    topic: String,
    group: String,
    broker_name: String,
    broker_addr: String,
    /// 只用来定位 broker 地址的无凭据实例（S6/S7 的反向与兼容场景都复用它）。
    probe: MQClientInstance,
}

impl Fixture {
    fn new(namesrv: &str, access_key: &str, secret_key: &str) -> Result<Fixture, String> {
        let stamp = stamp();
        let probe = MQClientInstance::new(
            &format!("rust-live-acl-probe-{stamp}"),
            vec![namesrv.to_string()],
        );
        Ok(Fixture {
            namesrv: namesrv.to_string(),
            topic: format!("RustLiveAcl{stamp}"),
            group: format!("rust-live-acl-{stamp}"),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            stamp,
            broker_name: String::new(),
            broker_addr: String::new(),
            probe,
        })
    }

    async fn start(&mut self) -> Result<(), String> {
        self.probe
            .start()
            .await
            .map_err(|e| format!("probe start failed: {e}"))?;
        // 路由查询不经鉴权（S7 就是这么证的），所以拿 TBW102 的路由就能定位一台 broker。
        let route = self
            .probe
            .get_topic_route_data(MixAll::DEFAULT_TOPIC)
            .await
            .ok_or_else(|| format!("no route of {} from namesrv", MixAll::DEFAULT_TOPIC))?;
        let (broker_name, broker_addr) = broker_of(&route)?;
        self.broker_name = broker_name;
        self.broker_addr = broker_addr;
        Ok(())
    }

    fn instance_name(&self, kind: &str) -> String {
        format!("live-acl-{kind}-{}", self.stamp)
    }

    /// 起一个**带凭据**的裸实例（当作 admin 用：建/删 topic）。
    ///
    /// Python 用 `DefaultMQAdminExt(rpc_hook=..)`；Rust 的 admin 层还没移植，
    /// 而 producer 内部做的正是「建实例 → 把钩子注册到 remoting 通道」，
    /// 这里直接照那两步做，语义等价。
    async fn signed_admin(&self, kind: &str) -> Result<MQClientInstance, String> {
        let instance = MQClientInstance::new(
            &format!("rust-live-acl-{kind}-{}", self.stamp),
            vec![self.namesrv.clone()],
        );
        instance
            .remoting_client()
            .register_rpc_hook(acl_hook(&self.access_key, &self.secret_key));
        instance
            .start()
            .await
            .map_err(|e| format!("{kind} instance start failed: {e}"))?;
        Ok(instance)
    }

    /// 带凭据建 topic（不靠 `autoCreateTopicEnable`：开了鉴权之后匿名客户端连不上 broker，
    /// 靠默认 topic 兜底也拿不到确定队列数）。
    async fn create_topic(&self, admin: &MQClientInstance, topic: &str) -> Result<(), String> {
        admin
            .create_topic_in_broker(
                &self.broker_addr,
                MixAll::DEFAULT_TOPIC,
                topic,
                QUEUE_NUMS,
                QUEUE_NUMS,
                DEFAULT_PERM,
                0,
                TopicFilterType::SINGLE_TAG,
                false,
                None,
                5000,
                2,
            )
            .await
            .map_err(|e| format!("create topic {topic} failed ({e}): {}", code_of(&e)))
    }

    /// 带凭据的生产者（`sk` 传空串表示用夹具给的 secretKey）。
    fn producer(&self, kind: &str, secret_key: Option<&str>) -> Result<DefaultMQProducer, String> {
        let sk = secret_key.unwrap_or(&self.secret_key);
        let producer = DefaultMQProducer::with_rpc_hook(
            &format!("rust-live-acl-{kind}-{}", self.stamp),
            Some(acl_hook(&self.access_key, sk)),
        )
        .map_err(|e| format!("{kind} producer build failed: {e}"))?;
        producer.set_namesrv_addr(&self.namesrv);
        producer.set_instance_name(&self.instance_name(kind));
        Ok(producer)
    }

    async fn send_bodies(&self, producer: &DefaultMQProducer, bodies: &[String]) -> Vec<String> {
        let mut evidence = Vec::new();
        for body in bodies {
            let mut msg = Message::new(&self.topic, Some(body.as_bytes()));
            msg.set_keys("acl");
            evidence.push(match producer.send(&mut msg, Some(5000), None).await {
                Ok(r) => format!("{}:{}", r.status, r.msg_id.clone().unwrap_or_default()),
                Err(e) => format!("EXC({}):{}", e, code_of(&e)),
            });
        }
        evidence
    }
}

fn broker_of(route: &TopicRouteData) -> Result<(String, String), String> {
    let bd = route
        .broker_datas
        .first()
        .ok_or_else(|| "route has no brokerData".to_string())?;
    let addr = bd
        .select_broker_addr()
        .ok_or_else(|| format!("broker {} has no address", bd.broker_name))?;
    Ok((bd.broker_name.clone(), addr))
}

// ------------------------------------------------------------------ S1/S2 admin

/// S1 带凭据建 topic 成功；S2 不带凭据建 topic 被拒。
async fn s1_s2_admin(ck: &mut Checker, fx: &Fixture, denied_topic: &str) -> bool {
    println!("\nS1/S2 admin 建 topic：带凭据 vs 不带凭据");
    let admin = match fx.signed_admin("admin").await {
        Ok(admin) => admin,
        Err(e) => {
            ck.abort("S1 带凭据实例启动", &e);
            return false;
        }
    };
    let created = match fx.create_topic(&admin, &fx.topic).await {
        Ok(()) => {
            ck.check("S1 带凭据建 topic 成功", true, "");
            true
        }
        Err(e) => {
            ck.abort("S1 带凭据建 topic", &e);
            false
        }
    };
    admin.shutdown();

    // S2：同一个 RPC、同一台 broker，只是没注册钩子。
    match fx
        .probe
        .create_topic_in_broker(
            &fx.broker_addr,
            MixAll::DEFAULT_TOPIC,
            denied_topic,
            QUEUE_NUMS,
            QUEUE_NUMS,
            DEFAULT_PERM,
            0,
            TopicFilterType::SINGLE_TAG,
            false,
            None,
            5000,
            0,
        )
        .await
    {
        Ok(()) => ck.check(
            "S2 无凭据建 topic 被 broker 拒绝",
            false,
            "broker 竟然接受了匿名请求（多半是 authenticationEnabled 没开）",
        ),
        Err(e) => ck.check(
            "S2 无凭据建 topic 被 broker 拒绝",
            denied(&e),
            &format!("{} {e}", code_of(&e)),
        ),
    }
    created
}

// ------------------------------------------------------------------ S3 错误 secretKey

async fn s3_bad_secret(ck: &mut Checker, fx: &Fixture) {
    println!("\nS3 错误 secretKey 的生产者必须被拒");
    let producer = match fx.producer("badsk", Some("WRONG_SECRET_KEY")) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("S3 构造", &e);
            return;
        }
    };
    if let Err(e) = producer.start().await {
        ck.abort("S3 start", &e.to_string());
        return;
    }
    let evidence = fx.send_bodies(&producer, &["should-not-send".to_string()]).await;
    // send_bodies 把错误压成证据串：这里靠「有没有 SEND_OK」判成败。
    let accepted = evidence.iter().any(|s| s.contains("SendOk") || s.starts_with("SEND_OK"));
    ck.check(
        "S3 错误 secretKey 被 broker 拒绝",
        !accepted,
        &format!("evidence={evidence:?}"),
    );
    producer.shutdown();
}

// ------------------------------------------------------------------ S4/S5 正向链路

/// 先起带凭据的 push 消费者，再发送，最后确认收满。
async fn s4_s5_signed_roundtrip(ck: &mut Checker, fx: &Fixture) -> Vec<String> {
    println!("\nS4/S5 带正确凭据的生产者/消费者（先起消费者再发送）");
    let bodies: Vec<String> = (0..N_MSG).map(|i| format!("acl-ok-{i}")).collect();

    let inbox = Arc::new(Inbox::default());
    let consumer = match DefaultMQPushConsumer::with_config(ConsumerConfig {
        consumer_group: fx.group.clone(),
        name_server_addrs: vec![fx.namesrv.clone()],
        instance_name: fx.instance_name("push"),
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("S5 消费者构造", &e.to_string());
            return Vec::new();
        }
    };
    consumer.set_rpc_hook(Some(acl_hook(&fx.access_key, &fx.secret_key)));
    if let Err(e) = consumer.subscribe(&fx.topic, "*") {
        ck.abort("S5 subscribe", &e.to_string());
        return Vec::new();
    }
    consumer.set_message_listener_concurrently(Arc::new(AclListener(inbox.clone())));
    if let Err(e) = consumer.start().await {
        ck.abort("S5 消费者 start（心跳/注册需签名）", &e.to_string());
        return Vec::new();
    }

    // 等首轮重平衡把队列分下来：初始位点必须在 topic 还空着的时候解析。
    tokio::time::sleep(Duration::from_secs(5)).await;

    let producer = match fx.producer("ok", None) {
        Ok(p) => p,
        Err(e) => {
            ck.abort("S4 构造", &e);
            consumer.shutdown();
            return Vec::new();
        }
    };
    if let Err(e) = producer.start().await {
        ck.abort("S4 生产者 start（注册需签名）", &e.to_string());
        consumer.shutdown();
        return Vec::new();
    }
    let evidence = fx.send_bodies(&producer, &bodies).await;
    producer.shutdown();
    let sent = evidence
        .iter()
        .filter(|s| s.starts_with("SEND_OK") || s.contains("SendOk"))
        .count();
    ck.check(
        &format!("S4 带凭据生产者发送 {N_MSG} 条"),
        sent == N_MSG,
        &format!("sent={sent} evidence={evidence:?}"),
    );
    // msgId 由 broker 赋值：非空即证 broker 真的采纳了这条签名请求。
    ck.check(
        "S4 broker 给消息赋了 msgId",
        evidence.iter().all(|s| s.split(':').nth(1).is_some_and(|id| !id.is_empty())),
        &format!("evidence={evidence:?}"),
    );

    let deadline = Instant::now() + RECV_WAIT;
    while Instant::now() < deadline && inbox.len() < sent {
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    ck.check(
        &format!("S5 带凭据 push 消费者收满 {sent} 条（心跳/拉取/提交位点全签名）"),
        sent == N_MSG && inbox.len() == sent,
        &format!("got={} bodies={:?}", inbox.len(), inbox.snapshot()),
    );
    consumer.shutdown();
    if sent == N_MSG { bodies } else { Vec::new() }
}

// ------------------------------------------------------------------ S6/S7 边界

async fn s6_unsigned_broker_rpc(ck: &mut Checker, fx: &Fixture) {
    println!("\nS6 不带凭据的直接 broker RPC 必须被拒");
    match fx
        .probe
        .get_consumer_list_by_group(&fx.group, 5000, Some(&fx.broker_addr))
        .await
    {
        Ok(list) => ck.check(
            "S6 无凭据 broker RPC 被拒绝",
            false,
            &format!("broker 竟然回了 {:?}（多半是没开鉴权）", list),
        ),
        Err(e) => ck.check(
            "S6 无凭据 broker RPC 被拒绝",
            denied(&e),
            &format!("{} {e}", code_of(&e)),
        ),
    }
}

async fn s7_namesrv_without_creds(ck: &mut Checker, fx: &Fixture) {
    println!("\nS7 不带凭据走 NameServer 路由查询仍应成功");
    match fx
        .probe
        .update_topic_route_info_from_name_server(&fx.topic, 5000, false)
        .await
    {
        Ok(ok) => ck.check(
            "S7 无凭据 namesrv 路由查询成功",
            ok,
            &format!("update returned {ok}"),
        ),
        Err(e) => ck.check(
            "S7 无凭据 namesrv 路由查询成功",
            false,
            &format!("{} {e}", code_of(&e)),
        ),
    }
}

// ------------------------------------------------------------------ S8 拉模式签名链路

async fn s8_signed_pull(ck: &mut Checker, fx: &Fixture, bodies: &[String]) {
    println!("\nS8 带凭据的 pull 消费者：取队列 / 拉取 / 提交位点 全签名");
    if bodies.is_empty() {
        ck.abort("S8 拉取", "S4/S5 没发成消息，跳过");
        return;
    }
    let consumer = match DefaultMQPullConsumer::with_config(PullConsumerConfig {
        consumer_group: format!("{}-pull", fx.group),
        name_server_addrs: vec![fx.namesrv.clone()],
        instance_name: fx.instance_name("pull"),
        ..Default::default()
    }) {
        Ok(c) => c,
        Err(e) => {
            ck.abort("S8 构造", &e.to_string());
            return;
        }
    };
    consumer.set_rpc_hook(Some(acl_hook(&fx.access_key, &fx.secret_key)));
    if let Err(e) = consumer.start().await {
        ck.abort("S8 start", &e.to_string());
        return;
    }
    let mqs = match consumer.fetch_subscribe_message_queues(&fx.topic).await {
        Ok(mqs) => mqs,
        Err(e) => {
            ck.abort(
                "S8 fetch_subscribe_message_queues（签名取路由）",
                &format!("{} {e}", code_of(&e)),
            );
            consumer.shutdown();
            return;
        }
    };
    let mut got: BTreeSet<String> = BTreeSet::new();
    let mut commit_failed: Vec<String> = Vec::new();
    let mut readback_failed: Vec<String> = Vec::new();
    for mq in &mqs {
        // 未提交过的组位点为 None（Python 同：`setZeroIfNotFound=false`），从 0 起拉。
        let offset = consumer
            .fetch_consume_offset(mq)
            .await
            .unwrap_or_default()
            .unwrap_or(0);
        match consumer.pull(mq, "*", offset, 32, Some(5000)).await {
            Ok(r) if r.status == PullStatus::Found => {
                for m in &r.msg_found_list {
                    got.insert(String::from_utf8_lossy(m.get_body()).into_owned());
                }
                // 位点提交是另一条 RPC（UPDATE_CONSUMER_OFFSET），漏签会静默失败成异常。
                if let Err(e) =
                    consumer.update_consume_offset(mq, r.next_begin_offset).await
                {
                    commit_failed.push(format!("{}:{e}", code_of(&e)));
                }
                match consumer.fetch_consume_offset(mq).await {
                    Ok(Some(v)) if v == r.next_begin_offset => {}
                    other => readback_failed.push(format!("{mq:?}=>{other:?}")),
                }
            }
            Ok(r) => {
                // 空队列 / 无匹配都算正常，只要签名没被拒。
                println!("  [INFO] {} 拉到 {:?}", mq.queue_id, r.status);
            }
            Err(e) => commit_failed.push(format!("pull:{} {e}", code_of(&e))),
        }
    }
    let want: BTreeSet<String> = bodies.iter().cloned().collect();
    ck.check(
        &format!("S8 带凭据 pull 拉到全部 {} 条", bodies.len()),
        got == want,
        &format!("got={got:?}"),
    );
    ck.check(
        "S8 带凭据提交位点并回读一致",
        commit_failed.is_empty() && readback_failed.is_empty(),
        &format!("commit={commit_failed:?} readback={readback_failed:?}"),
    );
    consumer.shutdown();
}

// ------------------------------------------------------------------ S9 清理

async fn s9_cleanup(ck: &mut Checker, fx: &Fixture) {
    println!("\nS9 清理：带凭据删掉本次建的 topic");
    match fx.signed_admin("cleaner").await {
        Ok(admin) => {
            let r = admin.delete_topic_in_broker(&fx.broker_addr, &fx.topic, 5000).await;
            ck.check(
                "S9 删除本次建的 topic",
                r.is_ok(),
                &match r {
                    Err(e) => format!("{} {e}", code_of(&e)),
                    Ok(()) => String::new(),
                },
            );
            admin.shutdown();
        }
        Err(e) => ck.abort("S9 实例启动", &e),
    }
}

// ------------------------------------------------------------------ 驱动

async fn run(namesrv: &str, access_key: &str, secret_key: &str) -> Checker {
    let mut ck = Checker::new();
    let mut fx = match Fixture::new(namesrv, access_key, secret_key) {
        Ok(fx) => fx,
        Err(e) => {
            ck.abort("夹具构造", &e);
            return ck;
        }
    };
    println!("== live ACL check, namesrv={namesrv} ak={access_key} ==");
    if let Err(e) = fx.start().await {
        ck.abort("夹具启动（查 namesrv 路由）", &e);
        return ck;
    }
    println!(
        "   broker={}@{} topic={} group={}",
        fx.broker_name, fx.broker_addr, fx.topic, fx.group
    );

    let denied_topic = format!("{}__DENIED", fx.topic);
    if s1_s2_admin(&mut ck, &fx, &denied_topic).await {
        s3_bad_secret(&mut ck, &fx).await;
        let bodies = s4_s5_signed_roundtrip(&mut ck, &fx).await;
        s6_unsigned_broker_rpc(&mut ck, &fx).await;
        s7_namesrv_without_creds(&mut ck, &fx).await;
        s8_signed_pull(&mut ck, &fx, &bodies).await;
        s9_cleanup(&mut ck, &fx).await;
    } else {
        ck.abort("后续场景", "topic 没建起来，跳过");
    }
    fx.probe.shutdown();
    ck
}

fn report(ck: &mut Checker) {
    println!(
        "\n== summary: {} passed, {} failed ==",
        ck.passed,
        ck.failed.len()
    );
    for f in &ck.failed {
        println!("   FAILED {f}");
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let argv: Vec<String> = env::args().skip(1).collect();
    let namesrv = argv
        .first()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:9876".to_string());
    let access_key = argv
        .get(1)
        .cloned()
        .unwrap_or_else(|| "AK_TEST".to_string());
    let secret_key = argv
        .get(2)
        .cloned()
        .unwrap_or_else(|| "SK_TEST_SECRET_12345678".to_string());
    let mut ck = run(&namesrv, &access_key, &secret_key).await;
    report(&mut ck);
    if ck.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
