//! 立即关闭（immediate close）的两个丢失竞态，对真实 5.5.1 broker 的联调验证
//!（任务 #125 / #126）。
//!
//! 停机路径上有两扇丢数据的窗：
//!
//! 1. **回投窗**：listener 全返回 RECONSUME_LATER / Suspend 时，分发循环正带着
//!    在途回投 RPC；停机若把在途批次掐断、位点又推进过了没落定的消息，broker
//!    就永远不会再投递它们。修复后的契约：停机时「有界等在途批次收完」+
//!    「位点钳到仍未落定消息的最小 offset」—— 每条消息要么经 %RETRY%/%DLQ%
//!    重投（回投已跑完），要么从原 topic 重投（位点没越过它）。
//! 2. **轨迹窗**：短生命周期 trace 生产者 shutdown 时，最后一批轨迹刚被 flush
//!    成在途发送任务，内部生产者就被关掉 —— 批次撞上 "producer not started"
//!    静默丢失。修复后的契约：收尾挂游离任务，等在途发送收敛后再关内部生产者。
//!
//! 场景：
//! - S1 并发消费 RECONSUME_LATER + 首条消息一落地就立即 shutdown：同一消费组
//!   的 v2 消费 `topic ∪ %RETRY% ∪ %DLQ%`，20 条原文必须**全部**在窗口内重现
//!   （哪些消息走 %RETRY%、哪些走原 topic 重投不关心 —— 契约是「一条都不丢」）。
//! - S2 顺序消费 SuspendCurrentQueueAMoment + 消费若干轮（有条数越过重试上限、
//!   顺序回投已在进行）后立即 shutdown：同一契约。
//! - T 短生命周期 trace 生产者：发 6 条后**立即** shutdown（flush 周期 5s 根本
//!   没到，整批都还在队列/在途上），trace reader 必须能在 RMQ_SYS_TRACE_TOPIC
//!   上读回全部 6 个 msgId。
//!
//! 用法：
//! ```text
//! cargo run --example live_shutdown_race -- 127.0.0.1:9876
//! ```

use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rocketmq_client_remoting::client::admin::{AdminConfig, DefaultMQAdminExt};
use rocketmq_client_remoting::client::consumer::{ConsumerConfig, DefaultMQPushConsumer};
use rocketmq_client_remoting::client::producer::{DefaultMQProducer, ProducerConfig};
use rocketmq_client_remoting::client::result::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, ConsumeOrderlyContext,
    ConsumeOrderlyStatus, MessageListenerConcurrently, MessageListenerOrderly, SendStatus,
};
use rocketmq_client_remoting::client::trace::TraceDataEncoder;
use rocketmq_client_remoting::common::message::{Message, MessageExt};
use rocketmq_client_remoting::common::mix_all::MixAll;
use rocketmq_client_remoting::remoting::protocol::heartbeat::ConsumeFromWhere;

const MSG_COUNT: usize = 20;
/// 联合重现窗口：%RETRY% 首轮延迟档 3 = 10s、二轮 30s、三轮 1m、四轮 2m，
/// 再加上 v2 首轮 rebalance / 拉取，240s 足够五轮内的任何一条露面。
const REDELIVERY_WINDOW: Duration = Duration::from_secs(240);

struct Inbox {
    items: Mutex<Vec<String>>,
    signal: Condvar,
}

impl Inbox {
    fn new() -> Arc<Self> {
        Arc::new(Inbox {
            items: Mutex::new(Vec::new()),
            signal: Condvar::new(),
        })
    }
    fn push(&self, text: String) {
        self.items.lock().unwrap().push(text);
        self.signal.notify_all();
    }
    fn wait_for(&self, deadline: Instant, mut predicate: impl FnMut(&[String]) -> bool) -> bool {
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
}

/// S1：全返回 RECONSUME_LATER，同时把「首条消息已投到 listener」竖成旗子，
/// 主线程看到旗子立刻 shutdown —— 让停机砸进回投链路中间。
struct FailAllConcurrentShared(Arc<AtomicBool>);

impl MessageListenerConcurrently for FailAllConcurrentShared {
    fn consume_message(
        &self,
        _msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        self.0.store(true, Ordering::SeqCst);
        ConsumeConcurrentlyStatus::ReconsumeLater
    }
}

/// S2：全返回挂起；顺带数投递轮次（主线程等投递轮次攒起来再关，
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

/// v2 侧收集器：把 body 原样收进 Inbox，全部认成功。
struct BodyCollector {
    inbox: Arc<Inbox>,
}

impl MessageListenerConcurrently for BodyCollector {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        for m in msgs {
            self.inbox
                .push(String::from_utf8_lossy(m.get_body()).into_owned());
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
    }
}

/// S2 的 v2 也要走顺序消费（同组语义），同样全部认成功。
struct BodyCollectorOrderly {
    inbox: Arc<Inbox>,
}

impl MessageListenerOrderly for BodyCollectorOrderly {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeOrderlyContext,
    ) -> ConsumeOrderlyStatus {
        for m in msgs {
            self.inbox
                .push(String::from_utf8_lossy(m.get_body()).into_owned());
        }
        ConsumeOrderlyStatus::Success
    }
}

/// trace reader：把每条轨迹消息的 body 文本原样收进 Inbox。
struct TraceReaderListener {
    inbox: Arc<Inbox>,
}

impl MessageListenerConcurrently for TraceReaderListener {
    fn consume_message(
        &self,
        msgs: &[MessageExt],
        _context: &mut ConsumeConcurrentlyContext,
    ) -> ConsumeConcurrentlyStatus {
        for m in msgs {
            self.inbox
                .push(String::from_utf8_lossy(m.get_body()).into_owned());
        }
        ConsumeConcurrentlyStatus::ConsumeSuccess
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

fn consumer_config(group: &str, instance: &str, namesrv: &str, orderly: bool) -> ConsumerConfig {
    ConsumerConfig {
        consumer_group: group.to_string(),
        name_server_addrs: vec![namesrv.to_string()],
        instance_name: instance.to_string(),
        // FIRST_OFFSET：v1（新组首启）要从队列头把预先发出的消息读回来；
        // v2 同组虽然有 v1 落下的位点（from_where 只在无位点时生效），但就算
        // 位点缺失从 0 重放也只是超集 —— 联合重现的断言容忍重复。
        consume_from_where: ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET.to_string(),
        // 重试上限压到 2：几轮失败后就触发回投（%RETRY%→%DLQ%），立即关闭时
        // 回投链路是真的在飞（顺序侧几轮本地挂起后走顺序回投，同理）。
        max_reconsume_times: 2,
        suspend_current_queue_time_millis: if orderly { 50 } else { 1000 },
        ..Default::default()
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let namesrv = match env::args().nth(1) {
        Some(a) => a,
        None => {
            eprintln!("usage: live_shutdown_race <namesrv>");
            return ExitCode::FAILURE;
        }
    };
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

    // ================= S1：并发 RECONSUME_LATER + 立即关闭 =================
    {
        let producer = DefaultMQProducer::with_config(ProducerConfig {
            producer_group: s1_prod_group.clone(),
            name_server_addrs: vec![namesrv.clone()],
            instance_name: format!("live-shdn-race-s1-prod-{stamp}"),
            ..Default::default()
        })
        .unwrap();
        producer.start().await.unwrap();
        producer.create_topic(&s1_topic, 1, 0).await.unwrap();
        let prefix = format!("s1-{stamp}");
        produce_n(&producer, &s1_topic, &prefix, MSG_COUNT).await;
        let expect: Vec<String> = (0..MSG_COUNT).map(|i| format!("{prefix}-{i}")).collect();

        // v1：首条消息一落地就立即 shutdown
        let v1 = DefaultMQPushConsumer::with_config(consumer_config(
            &s1_group,
            &format!("live-shdn-race-s1-v1-{stamp}"),
            &namesrv,
            false,
        ))
        .unwrap();
        v1.subscribe(&s1_topic, "*").unwrap();
        let flag = Arc::new(AtomicBool::new(false));
        v1.set_message_listener_concurrently(Arc::new(FailAllConcurrentShared(flag.clone())));
        v1.start().await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        while !flag.load(Ordering::SeqCst) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        check!("S1 v1 首条消息已投递（触发立即关闭）", flag.load(Ordering::SeqCst));
        v1.shutdown();

        // v2：同一消费组，收 topic ∪ %RETRY% ∪ %DLQ%，全部认成功
        tokio::time::sleep(Duration::from_secs(2)).await;
        let inbox = Inbox::new();
        let v2 = DefaultMQPushConsumer::with_config(consumer_config(
            &s1_group,
            &format!("live-shdn-race-s1-v2-{stamp}"),
            &namesrv,
            false,
        ))
        .unwrap();
        for t in [
            s1_topic.as_str(),
            MixAll::get_retry_topic(&s1_group).as_str(),
            MixAll::get_dlq_topic(&s1_group).as_str(),
        ] {
            v2.subscribe(t, "*").unwrap();
        }
        v2.set_message_listener_concurrently(Arc::new(BodyCollector {
            inbox: inbox.clone(),
        }));
        v2.start().await.unwrap();

        let got_all = inbox.wait_for(Instant::now() + REDELIVERY_WINDOW, |bodies| {
            expect.iter().all(|e| bodies.iter().any(|b| b == e))
        });
        let seen = inbox.items.lock().unwrap().len();
        check!(
            "S1 立即关闭后 20 条原文全部重现（topic∪RETRY∪DLQ）",
            got_all,
            "seen={seen}"
        );
        v2.shutdown();
        producer.shutdown();
    }

    // ================= S2：顺序 Suspend + 回投链路在飞时立即关闭 =================
    {
        let producer = DefaultMQProducer::with_config(ProducerConfig {
            producer_group: s2_prod_group.clone(),
            name_server_addrs: vec![namesrv.clone()],
            instance_name: format!("live-shdn-race-s2-prod-{stamp}"),
            ..Default::default()
        })
        .unwrap();
        producer.start().await.unwrap();
        producer.create_topic(&s2_topic, 1, 0).await.unwrap();
        let prefix = format!("s2-{stamp}");
        produce_n(&producer, &s2_topic, &prefix, MSG_COUNT).await;
        let expect: Vec<String> = (0..MSG_COUNT).map(|i| format!("{prefix}-{i}")).collect();

        let v1 = DefaultMQPushConsumer::with_config(consumer_config(
            &s2_group,
            &format!("live-shdn-race-s2-v1-{stamp}"),
            &namesrv,
            true,
        ))
        .unwrap();
        v1.subscribe(&s2_topic, "*").unwrap();
        let deliveries = Arc::new(AtomicUsize::new(0));
        v1.set_message_listener_orderly(Arc::new(SuspendAllOrderlyShared(Arc::clone(
            &deliveries,
        ))));
        v1.start().await.unwrap();
        // 等投递轮次攒起来：1 个队列逐条循环挂起，~40 轮意味着前十几条已经
        // 越过重试上限、顺序回投正在跑 —— 此时立即关闭。
        let deadline = Instant::now() + Duration::from_secs(60);
        while deliveries.load(Ordering::SeqCst) < 40 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        check!(
            "S2 顺序消费挂起轮次已起（触发立即关闭）",
            deliveries.load(Ordering::SeqCst) >= 40,
            "deliveries={}",
            deliveries.load(Ordering::SeqCst)
        );
        v1.shutdown();

        tokio::time::sleep(Duration::from_secs(2)).await;
        let inbox = Inbox::new();
        let v2 = DefaultMQPushConsumer::with_config(consumer_config(
            &s2_group,
            &format!("live-shdn-race-s2-v2-{stamp}"),
            &namesrv,
            true,
        ))
        .unwrap();
        for t in [
            s2_topic.as_str(),
            MixAll::get_retry_topic(&s2_group).as_str(),
            MixAll::get_dlq_topic(&s2_group).as_str(),
        ] {
            v2.subscribe(t, "*").unwrap();
        }
        v2.set_message_listener_orderly(Arc::new(BodyCollectorOrderly {
            inbox: inbox.clone(),
        }));
        v2.start().await.unwrap();

        let got_all = inbox.wait_for(Instant::now() + REDELIVERY_WINDOW, |bodies| {
            expect.iter().all(|e| bodies.iter().any(|b| b == e))
        });
        let seen = inbox.items.lock().unwrap().len();
        check!(
            "S2 立即关闭后 20 条原文全部重现（topic∪RETRY∪DLQ）",
            got_all,
            "seen={seen}"
        );
        v2.shutdown();
        producer.shutdown();
    }

    // ================= T：短生命周期 trace 生产者 =================
    {
        let producer = DefaultMQProducer::with_config(ProducerConfig {
            producer_group: t_group.clone(),
            name_server_addrs: vec![namesrv.clone()],
            instance_name: format!("live-shdn-race-t-prod-{stamp}"),
            enable_trace: true,
            ..Default::default()
        })
        .unwrap();
        producer.start().await.unwrap();
        let mut msg_ids = Vec::new();
        for i in 0..6 {
            let body = format!("t-{stamp}-{i}");
            let id = send_with_route_retry(&producer, &s1_topic, &body)
                .await
                .unwrap_or_else(|| panic!("等 60s 仍发不出去: {body}"));
            msg_ids.push(id);
        }
        // 立即关闭：5s 的 flush 周期没到，整批轨迹还在队列/在途上
        producer.shutdown();

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
        reader.set_message_listener_concurrently(Arc::new(TraceReaderListener {
            inbox: inbox.clone(),
        }));
        reader.start().await.unwrap();

        let got_all = inbox.wait_for(Instant::now() + REDELIVERY_WINDOW, |texts| {
            let corpus = texts.join("");
            msg_ids.iter().all(|id| !id.is_empty() && corpus.contains(id))
        });
        let texts = inbox.items.lock().unwrap().clone();
        let mut pubs = 0usize;
        for text in &texts {
            for ctx in TraceDataEncoder::decoder_from_trace_data_string(Some(text)) {
                if matches!(ctx.trace_type, Some(rocketmq_client_remoting::client::trace::TraceType::Pub))
                    && ctx
                        .trace_beans
                        .first()
                        .is_some_and(|b| msg_ids.contains(&b.msg_id))
                {
                    pubs += 1;
                }
            }
        }
        check!(
            "T 短生命周期 trace 生产者最后一批轨迹不丢",
            got_all && pubs >= 6,
            "pubs={pubs}"
        );
        reader.shutdown();
    }

    // ================= 清理 =================
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
        MixAll::get_retry_topic(&s1_group).as_str(),
        MixAll::get_dlq_topic(&s1_group).as_str(),
        MixAll::get_retry_topic(&s2_group).as_str(),
        MixAll::get_dlq_topic(&s2_group).as_str(),
    ] {
        let _ = admin.delete_topic(t, None).await;
    }
    admin.shutdown();

    println!("== summary: {pass} passed, {fail} failed ==");
    if fail == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
