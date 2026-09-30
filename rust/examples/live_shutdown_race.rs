//! 立即关停 / 立即退进程的丢数据契约，对真实 5.5.1 broker 的联调验证
//!（任务 #125 / #126 / #129 / #131）。
//!
//! 停机路径上有两扇丢数据的窗，**关闭方都是「进程退出」这一下**：
//!
//! 1. **回投窗**：listener 全返回 RECONSUME_LATER / Suspend 时，分发循环正带着
//!    在途回投 RPC；停机若只把收尾 `spawn` 出去就返回，进程随即退出会把运行时
//!    连同它上面的收尾任务一起拆掉 —— 回投没发完、位点又推进过了没落定的消息，
//!    broker 就永远不会再投递它们，`%DLQ%` 里也见不到（用户报告的场景）。
//!    修复后的契约：`shutdown()` **返回即表示收尾已落地**（在途批次收完、回投
//!    RPC 拿到应答、位点钳到仍未落定消息的最小 offset）。
//! 2. **轨迹窗**：短生命周期 trace 生产者 shutdown 时，最后一批轨迹刚被 flush
//!    成在途发送任务；同样地，「spawn 后即返回」会在进程退出时把整批掐掉 ——
//!    拉 `RMQ_SYS_TRACE_TOPIC` 根本看不到这批 `Pub` 轨迹。修复后的契约：
//!    `shutdown()` 返回时最后一批轨迹已经落到 broker 上。
//!
//! **本例子据此重写了两点**（旧版是「spawn 后即返回」的形态，而且 `shutdown()`
//! 之后垫了 `sleep(2s)` —— 运行时还活着，游离的收尾任务自然跑完了，测试因此
//! 一直「通过」而掩盖了 bug）：
//!
//! * 每个阶段都在**独立运行时**里跑，阶段函数返回时立刻 `drop(rt)`，
//!   等价于进程退出：任何还挂在运行时上的收尾任务在这一刻被取消
//!   （与 `client::shutdown` 的离线守卫同一套语义）；
//! * `shutdown()` 之后**不再有任何垫时间**，判别式全部落在「关停返回时点」。
//!
//! 场景与判别式：
//! - S1 并发消费全失败 + 立即退进程（两轮）：
//!   A 轮 rt=0，`consumeMessageBatchMaxSize = 20` 让整批一次性投给 listener，
//!   等 20 条全部投到 listener 的那一刻立即 `shutdown()` + 退进程 —— 此刻整批
//!   send-back RPC 正在飞。B 轮同组**只订 `%RETRY%`**：这 20 条原文的存在本身就是
//!   「A 轮在途 send-back 在关停返回前已落地」的判别式（旧实现 shutdown 返回时
//!   回投还没发出去，%RETRY% 一条都不会有）；全失败推进第二轮，rt=1 >= 上限 1 ⇒
//!   broker 直接改投 `%DLQ%`（`AbstractSendMessageProcessor.consumerSendMsgBack`：
//!   `msgExt.getReconsumeTimes() >= maxReconsumeTimes` ⇒ DLQ），B 轮同样等 listener
//!   收齐后立即退进程；C 阶段校验 `%DLQ%` 收齐 20 条原文 —— 正是用户报告的
//!   「消息未进入 %DLQ%」。
//! - S2 顺序消费 SuspendCurrentQueueAMoment，按 1 条一批挂起：每条 3 轮（两次本地
//!   挂起、第三次越过重试上限走顺序回投），投递轮次 >= 45（前 15 条已越过上限、
//!   回投链路正在跑）时立即退进程 —— 顺序回投路径（`orderly_send_message_back`，
//!   停机排空窗口内同样受 `started` 闸门影响）与「挂起批次的位点下限」两条腿一起
//!   验证：`%DLQ%` 至少已有若干条、topic ∪ `%RETRY%` ∪ `%DLQ%` 必须收齐 20 条原文。
//! - T 短生命周期 trace 生产者：发 6 条后立即退进程（flush 周期 5s 根本没到，
//!   整批都还在队列/在途上），trace reader 必须能在 RMQ_SYS_TRACE_TOPIC 上读回
//!   全部 6 个 msgId 的 `Pub` 轨迹。
//!
//! 用法：
//! ```text
//! cargo run --example live_shutdown_race -- 127.0.0.1:9876 [legs]
//! ```
//! `legs` 逗号分隔 `s1,s2,t`（缺省或 `all` = 三段全跑）。

use std::collections::HashSet;
use std::env;
use std::future::Future;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::producer::{DefaultMQProducer, ProducerConfig};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, ConsumeOrderlyContext,
    ConsumeOrderlyStatus, MessageListenerConcurrently, MessageListenerOrderly, SendStatus,
};
use rocketmq_client_remoting::client::trace::{TraceDataEncoder, TraceType};
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

const MSG_COUNT: usize = 20;
const TRACE_COUNT: usize = 6;
/// 等「经 broker 一个来回」的窗口：%RETRY% 首轮延迟档 3 = 10s，再叠加
/// 重投/DLQ topic 在 namesrv 的路由可见性（broker 心跳周期 30s）与 rebalance。
const ROUND_TRIP_WINDOW: Duration = Duration::from_secs(90);
/// 等「消息已经在订阅的 topic 里」的窗口：只差 rebalance + 拉取。
const COLLECT_WINDOW: Duration = Duration::from_secs(60);
/// 等首条消息投到 listener 的窗口（触发立即关停）。
const FIRST_DELIVERY_WINDOW: Duration = Duration::from_secs(60);

/// 把一段「进程」跑在独立运行时里：阶段函数返回后**立刻拆掉运行时** —— 等价于
/// 进程退出（`Runtime` 析构会把所有未完成的任务直接取消）。修复后的契约是
/// `shutdown()` 返回即收尾落地，所以拆运行时不会丢任何东西；旧实现（只 spawn
/// 收尾就返回）会在这里把在途 send-back / trace finalizer 掐断。
fn run_isolated<T>(what: &str, phase: impl Future<Output = T>) -> T {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build phase runtime");
    let out = rt.block_on(phase);
    drop(rt);
    println!("  [exit] {what}：运行时已拆除（≈ 进程退出，游离收尾任务在此刻被取消）");
    out
}

/// 一条收上来的消息：来源 topic + body 原文。
#[derive(Clone)]
struct Hit {
    topic: String,
    body: String,
}

struct Inbox {
    items: Mutex<Vec<Hit>>,
    signal: Condvar,
}

impl Inbox {
    fn new() -> Arc<Self> {
        Arc::new(Inbox {
            items: Mutex::new(Vec::new()),
            signal: Condvar::new(),
        })
    }

    fn push(&self, topic: &str, body: String) {
        self.items.lock().unwrap().push(Hit {
            topic: topic.to_string(),
            body,
        });
        self.signal.notify_all();
    }

    fn wait_until(&self, deadline: Instant, mut predicate: impl FnMut(&[Hit]) -> bool) -> bool {
        let mut guard = self.items.lock().unwrap();
        while !predicate(&guard) {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (g, _timeout) = self
                .signal
                .wait_timeout(guard, deadline.saturating_duration_since(now))
                .unwrap();
            guard = g;
        }
        true
    }

    /// 等 `expect` 里的 body 全部出现（可选按来源 topic 过滤）。
    fn wait_for_bodies(&self, deadline: Instant, expect: &[String], topic: Option<&str>) -> bool {
        self.wait_until(deadline, |hits| {
            expect.iter().all(|e| {
                hits.iter()
                    .any(|h| (topic.is_none() || topic == Some(h.topic.as_str())) && &h.body == e)
            })
        })
    }

    /// 去重后的 body 个数（可选按来源 topic 过滤）。
    fn count_bodies(&self, topic: Option<&str>) -> usize {
        let hits = self.items.lock().unwrap();
        let mut set = HashSet::new();
        for h in hits.iter() {
            if topic.is_none() || topic == Some(h.topic.as_str()) {
                set.insert(&h.body);
            }
        }
        set.len()
    }

    fn hits_of(&self, topic: &str) -> Vec<Hit> {
        self.items
            .lock()
            .unwrap()
            .iter()
            .filter(|h| h.topic == topic)
            .cloned()
            .collect()
    }
}

/// S1 A/B 轮：全失败并记录。A 轮等「20 条全部投到 listener」（整批 send-back 此刻
/// 在飞）触发立即关停；B 轮从 `%RETRY%` 收齐 20 条就是「A 轮 send-back 已落地」的
/// 判别式。
struct FailAllConcurrent {
    inbox: Arc<Inbox>,
}

impl MessageListenerConcurrently for FailAllConcurrent {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        for m in msgs {
            self.inbox
                .push(&m.topic, String::from_utf8_lossy(m.get_body()).into_owned());
        }
        ConsumeConcurrentlyStatus::ReconsumeLater
    }
}

/// S2 首轮：全返回挂起；顺带数投递轮次（主线程等投递轮次攒起来再关，
/// 让一部分消息越过顺序回投上限、回投 RPC 真正在飞）。
struct SuspendAllOrderlyShared(Arc<AtomicUsize>);

impl MessageListenerOrderly for SuspendAllOrderlyShared {
    fn consume_message(
        &self,
        _msgs: &[MessageExt],
        _context: &mut ConsumeOrderlyContext,
    ) -> ConsumeOrderlyStatus {
        self.0.fetch_add(1, Ordering::SeqCst);
        ConsumeOrderlyStatus::SuspendCurrentQueueAMoment
    }
}

/// 收集器（并发）：body 原文收进 Inbox，全部认成功。
struct Collector {
    inbox: Arc<Inbox>,
}

impl MessageListenerConcurrently for Collector {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        for m in msgs {
            self.inbox
                .push(&m.topic, String::from_utf8_lossy(m.get_body()).into_owned());
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

/// S2 的 v2 也要走顺序消费（同组语义），同样全部认成功。
struct OrderlyCollector {
    inbox: Arc<Inbox>,
}

impl MessageListenerOrderly for OrderlyCollector {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeOrderlyContext,
    ) -> ConsumeOrderlyStatus {
        for m in msgs {
            self.inbox
                .push(&m.topic, String::from_utf8_lossy(m.get_body()).into_owned());
        }
        ConsumeOrderlyStatus::Success
    }
}

async fn send_with_route_retry(
    producer: &DefaultMQProducer,
    topic: &str,
    body: &str,
) -> Option<String> {
    let mut msg = Message::new(topic, Some(body.as_bytes()));
    msg.set_tags("TagA");
    let mut msg_id = None;
    for attempt in 0..30 {
        match producer.send(&mut msg, None, None).await {
            Ok(r) if r.status == SendStatus::SendOk => {
                msg_id = r.msg_id;
                break;
            }
            Ok(r) => eprintln!("  send not OK ({r:?}), retry {attempt}"),
            Err(e) => eprintln!("  send retry {attempt}: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    msg_id
}

async fn produce_n(producer: &DefaultMQProducer, topic: &str, prefix: &str, n: usize) {
    for i in 0..n {
        let body = format!("{prefix}-{i}");
        let id = send_with_route_retry(producer, topic, &body)
            .await
            .unwrap_or_else(|| panic!("等 60s 仍发不出去: {body}"));
        assert!(!id.is_empty(), "empty msg id for {body}");
    }
}

fn consumer_config(
    group: &str,
    instance: &str,
    namesrv: &str,
    orderly: bool,
    max_reconsume_times: i32,
    batch_max: i32,
) -> ConsumerConfig {
    ConsumerConfig {
        consumer_group: group.to_string(),
        name_server_addrs: vec![namesrv.to_string()],
        instance_name: instance.to_string(),
        // FIRST_OFFSET：新组首启要从队列头把预先发出的消息读回来；重投 topic 上
        // 没有本组位点时同理（from_where 只在无位点时生效）。
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        // 并发侧：rt=0 先回 %RETRY%（broker 延迟 10s），rt 到上限后 broker 直接改投
        // %DLQ% —— 1 就是「一轮重投 + 一轮死信」，两轮都卡在「立即退进程」上。
        // 顺序侧：本地挂起计数到上限后走顺序回投（Java checkReconsumeTimes）。
        max_reconsume_times,
        // S1 要整批一起在途：默认 1 条一批的话，关停那一刻只有 1 条 send-back 在飞，
        // 判别式被稀释成一个原子的时序巧合。
        consume_message_batch_max_size: batch_max,
        suspend_current_queue_time_millis: if orderly { 50 } else { 1000 },
        ..Default::default()
    }
}

fn main() -> ExitCode {
    let namesrv = match env::args().nth(1) {
        Some(a) => a,
        None => {
            eprintln!("usage: live_shutdown_race <namesrv> [legs: s1,s2,t]");
            return ExitCode::FAILURE;
        }
    };
    let legs: Vec<String> = env::args()
        .nth(2)
        .map(|s| s.split(',').map(|x| x.trim().to_string()).collect())
        .unwrap_or_default();
    let want = |leg: &str| legs.is_empty() || legs.iter().any(|l| l == "all" || l == leg);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let s1_topic = format!("RustShdnRaceS1{stamp}");
    let s1_group = format!("GID_shdn_race_s1_{stamp}");
    let s1_prod_group = format!("GID_shdn_race_s1_prod_{stamp}");
    let s2_topic = format!("RustShdnRaceS2{stamp}");
    let s2_group = format!("GID_shdn_race_s2_{stamp}");
    let s2_prod_group = format!("GID_shdn_race_s2_prod_{stamp}");
    let t_topic = format!("RustShdnRaceT{stamp}");
    let t_group = format!("GID_shdn_race_t_{stamp}");

    let mut pass = 0usize;
    let mut fail = 0usize;
    macro_rules! check {
        ($name:expr, $ok:expr $(,)?) => {
            check!($name, $ok, "")
        };
        ($name:expr, $ok:expr, $($detail:expr),+ $(,)?) => {
            if $ok {
                pass += 1;
                println!("  [PASS] {} {}", $name, format!($($detail),+));
            } else {
                fail += 1;
                println!("  [FAIL] {} {}", $name, format!($($detail),+));
            }
        };
    }

    // ============ S1：并发 RECONSUME_LATER 两轮 + 每轮立即退进程 ============
    if want("s1") {
        let s1_prefix = format!("s1-{stamp}");
        let s1_expect: Vec<String> = (0..MSG_COUNT).map(|i| format!("{s1_prefix}-{i}")).collect();

        // A 轮：发 20 条，v1 整批（20 条一起）投给 listener 后立即关停
        //（返回后阶段结束 = 退进程，此刻整批 send-back RPC 正在飞）
        let a_delivered = run_isolated("S1-A v1 全失败 + 立即退进程", async {
            let producer = DefaultMQProducer::with_config(ProducerConfig {
                producer_group: s1_prod_group.clone(),
                name_server_addrs: vec![namesrv.clone()],
                instance_name: format!("live-shdn-race-s1-prod-{stamp}"),
                ..Default::default()
            })
            .unwrap();
            producer.start().await.unwrap();
            producer.create_topic(&s1_topic, 1, 0).await.unwrap();
            produce_n(&producer, &s1_topic, &s1_prefix, MSG_COUNT).await;

            let v1 = DefaultMQPushConsumer::with_config(consumer_config(
                &s1_group,
                &format!("live-shdn-race-s1-v1-{stamp}"),
                &namesrv,
                false,
                1,
                MSG_COUNT as i32,
            ))
            .unwrap();
            v1.subscribe(&s1_topic, "*").unwrap();
            let inbox = Inbox::new();
            v1.set_message_listener_concurrently(Arc::new(FailAllConcurrent {
                inbox: inbox.clone(),
            }));
            v1.start().await.unwrap();
            let delivered =
                inbox.wait_for_bodies(Instant::now() + FIRST_DELIVERY_WINDOW, &s1_expect, None);
            // 立即关停：返回后就是「进程退出」，没有任何垫时间
            v1.shutdown();
            delivered
        });
        check!(
            "S1-A 20 条全部投到 listener（整批回投在飞，立即退进程）",
            a_delivered
        );

        // B 轮：同组第二实例**只订 %RETRY%** —— 这 20 条的存在本身就是
        // 「A 轮在途 send-back 在关停返回前已落地」的判别式；全失败推进第二轮，
        // rt=1 >= 上限 1 ⇒ broker 改投 %DLQ%，同样收齐后立即退进程，B 轮在途的
        // send-back 也必须落地，C 阶段才可能收齐 20 条。
        let (b_got_all, b_seen) = run_isolated("S1-B v2 收 %RETRY% 第二轮 + 立即退进程", async {
            let inbox = Inbox::new();
            let retry_topic = MixAll::get_retry_topic(&s1_group);
            let v2 = DefaultMQPushConsumer::with_config(consumer_config(
                &s1_group,
                &format!("live-shdn-race-s1-v2-{stamp}"),
                &namesrv,
                false,
                1,
                MSG_COUNT as i32,
            ))
            .unwrap();
            v2.subscribe(&retry_topic, "*").unwrap();
            v2.set_message_listener_concurrently(Arc::new(FailAllConcurrent {
                inbox: inbox.clone(),
            }));
            v2.start().await.unwrap();
            let got = inbox.wait_for_bodies(Instant::now() + ROUND_TRIP_WINDOW, &s1_expect, None);
            let seen = inbox.count_bodies(None);
            v2.shutdown();
            (got, seen)
        });
        check!(
            "S1-A 关停返回前 send-back 已落地：v2 从 %RETRY% 收齐 20 条原文",
            b_got_all,
            "seen={b_seen}"
        );

        // C 阶段：只订 %DLQ%（用户报告的场景）——B 轮 20 条 rt=1 的 send-back
        // 只要有一条没在「关停返回前」落地，这里就凑不齐 20 条原文。
        let (c_dlq, c_detail) = run_isolated("S1-C 校验 %DLQ%", async {
            let inbox = Inbox::new();
            let dlq_topic = MixAll::get_dlq_topic(&s1_group);
            let v3 = DefaultMQPushConsumer::with_config(consumer_config(
                &format!("GID_shdn_race_s1_probe_{stamp}"),
                &format!("live-shdn-race-s1-probe-{stamp}"),
                &namesrv,
                false,
                1,
                1,
            ))
            .unwrap();
            v3.subscribe(&dlq_topic, "*").unwrap();
            v3.set_message_listener_concurrently(Arc::new(Collector {
                inbox: inbox.clone(),
            }));
            v3.start().await.unwrap();
            // 先等 %DLQ% 收齐；收不齐就按现状统计（判别式不通过）
            let _ = inbox.wait_for_bodies(
                Instant::now() + COLLECT_WINDOW,
                &s1_expect,
                Some(&dlq_topic),
            );
            let dlq_n = inbox.count_bodies(Some(&dlq_topic));
            let detail = format!("dlq={dlq_n}");
            v3.shutdown();
            (dlq_n, detail)
        });
        check!(
            "S1-B 关停返回前 DLQ 回投已落地：%DLQ% 收齐 20 条原文",
            c_dlq == MSG_COUNT,
            "{c_detail}"
        );
    }

    // ====== S2：顺序 Suspend + 回投链路在飞时立即退进程 ======
    if want("s2") {
        let s2_prefix = format!("s2-{stamp}");
        let s2_expect: Vec<String> = (0..MSG_COUNT).map(|i| format!("{s2_prefix}-{i}")).collect();

        let a_deliveries = run_isolated("S2-A 顺序全挂起 + 立即退进程", async {
            let producer = DefaultMQProducer::with_config(ProducerConfig {
                producer_group: s2_prod_group.clone(),
                name_server_addrs: vec![namesrv.clone()],
                instance_name: format!("live-shdn-race-s2-prod-{stamp}"),
                ..Default::default()
            })
            .unwrap();
            producer.start().await.unwrap();
            producer.create_topic(&s2_topic, 1, 0).await.unwrap();
            produce_n(&producer, &s2_topic, &s2_prefix, MSG_COUNT).await;

            let v1 = DefaultMQPushConsumer::with_config(consumer_config(
                &s2_group,
                &format!("live-shdn-race-s2-v1-{stamp}"),
                &namesrv,
                true,
                2,
                1,
            ))
            .unwrap();
            v1.subscribe(&s2_topic, "*").unwrap();
            let deliveries = Arc::new(AtomicUsize::new(0));
            v1.set_message_listener_orderly(Arc::new(SuspendAllOrderlyShared(Arc::clone(
                &deliveries,
            ))));
            v1.start().await.unwrap();
            // 等投递轮次攒起来：1 个队列逐条循环挂起，45 轮意味着前几条已经
            // 越过重试上限、顺序回投正在跑 —— 此时立即关停。
            let deadline = Instant::now() + Duration::from_secs(60);
            while deliveries.load(Ordering::SeqCst) < 45 && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let seen = deliveries.load(Ordering::SeqCst);
            v1.shutdown();
            seen
        });
        check!(
            "S2-A 顺序消费挂起轮次已起（触发立即关停）",
            a_deliveries >= 45,
            "deliveries={a_deliveries}"
        );

        let (b_got_all, b_seen, b_dlq) =
            run_isolated("S2-B 联合重现校验 + 立即退进程", async {
                let inbox = Inbox::new();
                let dlq_topic = MixAll::get_dlq_topic(&s2_group);
                let v2 = DefaultMQPushConsumer::with_config(consumer_config(
                    &s2_group,
                    &format!("live-shdn-race-s2-v2-{stamp}"),
                    &namesrv,
                    true,
                    2,
                    1,
                ))
                .unwrap();
                for t in [
                    s2_topic.as_str(),
                    MixAll::get_retry_topic(&s2_group).as_str(),
                    dlq_topic.as_str(),
                ] {
                    v2.subscribe(t, "*").unwrap();
                }
                v2.set_message_listener_orderly(Arc::new(OrderlyCollector {
                    inbox: inbox.clone(),
                }));
                v2.start().await.unwrap();
                let got =
                    inbox.wait_for_bodies(Instant::now() + ROUND_TRIP_WINDOW, &s2_expect, None);
                let seen = inbox.count_bodies(None);
                let dlq_n = inbox.count_bodies(Some(&dlq_topic));
                v2.shutdown();
                (got, seen, dlq_n)
            });
        check!(
            "S2 立即退进程后 20 条原文全部重现（topic∪RETRY∪DLQ）",
            b_got_all,
            "seen={b_seen}"
        );
        check!(
            "S2 顺序回投链路已落地 %DLQ%（越过本地重试上限的条目）",
            b_dlq >= 1,
            "dlq={b_dlq}"
        );
    }

    // ============ T：短生命周期 trace 生产者 + 立即退进程 ============
    if want("t") {
        let t_msg_ids = run_isolated("T-A 短生命周期 trace 生产者 + 立即退进程", async {
            let producer = DefaultMQProducer::with_config(ProducerConfig {
                producer_group: t_group.clone(),
                name_server_addrs: vec![namesrv.clone()],
                instance_name: format!("live-shdn-race-t-prod-{stamp}"),
                enable_trace: true,
                ..Default::default()
            })
            .unwrap();
            producer.start().await.unwrap();
            producer.create_topic(&t_topic, 1, 0).await.unwrap();
            let mut msg_ids = Vec::new();
            for i in 0..TRACE_COUNT {
                let body = format!("t-{stamp}-{i}");
                let id = send_with_route_retry(&producer, &t_topic, &body)
                    .await
                    .unwrap_or_else(|| panic!("等 60s 仍发不出去: {body}"));
                msg_ids.push(id);
            }
            // 立即关停：flush 周期（5s）没到，整批轨迹还压在队列/在途上
            producer.shutdown();
            msg_ids
        });

        let (t_got_all, t_pubs) = run_isolated("T-B 读回轨迹", async {
            let inbox = Inbox::new();
            let reader = DefaultMQPushConsumer::with_config(ConsumerConfig {
                consumer_group: format!("GID_shdn_race_t_reader_{stamp}"),
                name_server_addrs: vec![namesrv.clone()],
                instance_name: format!("live-shdn-race-t-reader-{stamp}"),
                consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
                ..Default::default()
            })
            .unwrap();
            reader.subscribe(MixAll::TRACE_TOPIC, "*").unwrap();
            reader.set_message_listener_concurrently(Arc::new(Collector {
                inbox: inbox.clone(),
            }));
            reader.start().await.unwrap();
            let got = inbox.wait_until(Instant::now() + COLLECT_WINDOW, |hits| {
                let corpus: String = hits
                    .iter()
                    .filter(|h| h.topic == MixAll::TRACE_TOPIC)
                    .map(|h| h.body.as_str())
                    .collect();
                t_msg_ids
                    .iter()
                    .all(|id| !id.is_empty() && corpus.contains(id))
            });
            let mut pubs = 0usize;
            for hit in inbox.hits_of(MixAll::TRACE_TOPIC) {
                for ctx in TraceDataEncoder::decoder_from_trace_data_string(Some(&hit.body)) {
                    if matches!(ctx.trace_type, Some(TraceType::Pub))
                        && ctx
                            .trace_beans
                            .first()
                            .is_some_and(|b| t_msg_ids.contains(&b.msg_id))
                    {
                        pubs += 1;
                    }
                }
            }
            reader.shutdown();
            (got, pubs)
        });
        check!(
            "T 立即退进程后最后一批 6 条 Pub 轨迹全部可读（shutdown 返回即已落 broker）",
            t_got_all && t_pubs >= TRACE_COUNT,
            "pubs={t_pubs}"
        );
    }

    // ================= 清理 =================
    run_isolated("清理", async {
        let admin = DefaultMQAdminExt::with_config(AdminConfig {
            instance_name: format!("ADMIN-shdn-race-{stamp}"),
            name_server_addrs: vec![namesrv.clone()],
            timeout_millis: 10_000,
            ..Default::default()
        });
        admin.start().await.unwrap();
        for t in [
            s1_topic.as_str(),
            s2_topic.as_str(),
            t_topic.as_str(),
            MixAll::get_retry_topic(&s1_group).as_str(),
            MixAll::get_dlq_topic(&s1_group).as_str(),
            MixAll::get_retry_topic(&s2_group).as_str(),
            MixAll::get_dlq_topic(&s2_group).as_str(),
        ] {
            let _ = admin.delete_topic(t, None).await;
        }
        admin.shutdown();
    });

    println!("== summary: {pass} passed, {fail} failed ==");
    if fail == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
