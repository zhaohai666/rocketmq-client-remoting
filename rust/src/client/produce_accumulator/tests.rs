//! `ProduceAccumulator`（自动攒批）单测 —— 与
//! `python/tests/test_produce_accumulator.py`（18 例）、
//! `csharp/tests/RocketMQ.Client.Tests/ProduceAccumulatorTests.cs`（17 例）同题。
//!
//! 覆盖 Java `ProduceAccumulatorTest` 的三个场景（sync / async / 指定 MessageQueue），另补：
//!
//! * 参数三档校验，与「累加器未建时 producer 的 getter 返回 0」的语义；
//! * `try_add_message` 全局字节闸门（放行即记账、归还、拒绝后调用方直发）；
//! * 批量应答**拆条**（逗号分隔 msgId/offsetMsgId → 每条各自的结果、queueOffset 递增）；
//! * `AggregateKey` 的四维分区（topic / mq / waitStoreMsgOK / tag）；
//! * 守卫线程把「已发完的空批次」置 closed 并摘表；
//! * 同步 / 异步失败路径（异常上抛 + 额度归还；回调各拿到一份）；
//! * `start → shutdown → start`（累加器按 clientId 复用，生产者重启会再 start 一次）。
//!
//! ⚠ 与 Java 单测一样，这里是**直接调累加器**的 `send` / `send_async`，绕过了
//! `DefaultMQProducer.send_by_accumulator` 里的 `set_uniq_id` —— 所以子消息都没有
//! UNIQ_KEY，`MessageBatch::encode()` 出来的 body 才与 `reference_batch_body` 可比
//! （`generate_from_list` 本身**不**打 ID）。批级 `KEYS` / `TAGS` 不进 body，所以
//! 累加器的 `build_batch` 多写这两个属性不影响 body 相等。
//!
//! ⚠ 和 Java 一样，`send_sync` 会把 `currently_hold_size` 扣掉，而真实调用链里是
//! `can_batch` → `try_add_message` 先记的账。这里直接调累加器，所以每条消息都得先
//! 自己补一次 `try_add_message`，否则归还后额度会变成负数。

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::{
    get_or_create_produce_accumulator, AggregateKey, AccumulatorSender, MessageAccumulation,
    ProduceAccumulator, DEFAULT_HOLD_MS, DEFAULT_HOLD_SIZE, DEFAULT_TOTAL_HOLD_SIZE,
};
use crate::client::producer::SendCallback;
use crate::client::result::{SendResult, SendStatus};
use crate::common::message::{Message, MessageBatch, MessageQueue};
use crate::common::message_const::{PROPERTY_KEYS, PROPERTY_TAGS};
use crate::error::{Error, Result};

const TOPIC: &str = "AccumTestTopic";

// ================================================================ 夹具

/// 累加器发出去的一批：批量对象 + 目标队列 + 有没有回调。
struct SentBatch {
    batch: MessageBatch,
    mq: Option<MessageQueue>,
    has_callback: bool,
}

/// 假的生产者（Java 单测里的 `MockMQProducer` 同款）：只记账 + 按剧本回结果。
///
/// `auto_complete = true`（默认）时同步交付回调，对应 Java mock 里 `callback.onSuccess`
/// 就地调用；置 `false` 可以让用例自己挑时机交付，用来验证「回调恰好一次」。
struct FakeSender {
    sent: Mutex<Vec<SentBatch>>,
    result: Mutex<Option<SendResult>>,
    error: Mutex<Option<String>>,
    auto_complete: AtomicBool,
    /// 两个 send 入口一共被调了几次。
    calls: Mutex<usize>,
}

impl Default for FakeSender {
    fn default() -> Self {
        FakeSender {
            sent: Mutex::new(Vec::new()),
            result: Mutex::new(None),
            error: Mutex::new(None),
            auto_complete: AtomicBool::new(true),
            calls: Mutex::new(0),
        }
    }
}

impl FakeSender {
    fn new() -> Arc<FakeSender> {
        Arc::new(FakeSender::default())
    }

    fn with_result(result: SendResult) -> Arc<FakeSender> {
        let sender = FakeSender::default();
        *sender.result.lock().unwrap() = Some(result);
        sender.into()
    }

    fn fail_with(message: &str) -> Arc<FakeSender> {
        let sender = FakeSender::default();
        *sender.error.lock().unwrap() = Some(message.to_string());
        sender.into()
    }

    /// 未指定结果时的兜底：`SEND_OK` + `123`，与 Python 的 `FakeProducer` 一致。
    fn resolve(&self, mq: Option<&MessageQueue>) -> SendResult {
        if let Some(result) = self.result.lock().unwrap().clone() {
            return result;
        }
        SendResult {
            status: SendStatus::SendOk,
            msg_id: Some("123".to_string()),
            message_queue: mq.cloned(),
            ..SendResult::default()
        }
    }

    fn take_error(&self) -> Option<Error> {
        self.error.lock().unwrap().take().map(Error::client)
    }

    fn record(&self, batch: MessageBatch, mq: Option<&MessageQueue>, has_callback: bool) {
        *self.calls.lock().unwrap() += 1;
        self.sent.lock().unwrap().push(SentBatch {
            batch,
            mq: mq.cloned(),
            has_callback,
        });
    }

    fn sent_count(&self) -> usize {
        self.sent.lock().unwrap().len()
    }

    fn calls(&self) -> usize {
        *self.calls.lock().unwrap()
    }

    /// 第 `i` 批发出去时的目标队列。
    fn mq_at(&self, i: usize) -> Option<MessageQueue> {
        self.sent.lock().unwrap()[i].mq.clone()
    }

    fn batch_at(&self, i: usize) -> MessageBatch {
        self.sent.lock().unwrap()[i].batch.clone()
    }

    fn callback_at(&self, i: usize) -> bool {
        self.sent.lock().unwrap()[i].has_callback
    }
}

impl AccumulatorSender for FakeSender {
    fn send_direct_blocking(
        &self,
        batch: MessageBatch,
        mq: Option<&MessageQueue>,
    ) -> Result<SendResult> {
        let result = self.resolve(mq);
        let error = self.take_error();
        self.record(batch, mq, false);
        match error {
            Some(e) => Err(e),
            None => Ok(result),
        }
    }

    fn send_direct_async(
        &self,
        batch: MessageBatch,
        mq: Option<&MessageQueue>,
        callback: Arc<dyn SendCallback>,
    ) -> Result<()> {
        let result = self.resolve(mq);
        let error = self.take_error();
        self.record(batch, mq, true);
        if !self.auto_complete.load(Ordering::Acquire) {
            return Ok(());
        }
        match error {
            Some(e) => callback.on_exception(e),
            None => callback.on_success(result),
        }
        Ok(())
    }
}

/// 收集回调（Python `CollectingCallback` 同款）。
#[derive(Default)]
struct CollectingCallback {
    results: Mutex<Vec<SendResult>>,
    errors: Mutex<Vec<String>>,
}

impl CollectingCallback {
    fn new() -> Arc<CollectingCallback> {
        Arc::new(CollectingCallback::default())
    }

    fn total(&self) -> usize {
        self.results.lock().unwrap().len() + self.errors.lock().unwrap().len()
    }

    fn msg_ids(&self) -> Vec<String> {
        self.results
            .lock()
            .unwrap()
            .iter()
            .filter_map(|r| r.msg_id.clone())
            .collect()
    }

    fn queue_offsets(&self) -> Vec<i64> {
        self.results
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.queue_offset)
            .collect()
    }

    fn error_count(&self) -> usize {
        self.errors.lock().unwrap().len()
    }

    fn errors(&self) -> Vec<String> {
        self.errors.lock().unwrap().clone()
    }
}

impl SendCallback for CollectingCallback {
    fn on_success(&self, result: SendResult) {
        self.results.lock().unwrap().push(result);
    }

    fn on_exception(&self, err: Error) {
        self.errors.lock().unwrap().push(err.to_string());
    }
}

fn as_callback(cb: &Arc<CollectingCallback>) -> Arc<dyn SendCallback> {
    Arc::clone(cb) as Arc<dyn SendCallback>
}

/// 与 Java / Python 单测同款 body（1 / 22 / 333 / 4444 / 55555 字节）。
fn make_messages(n: usize) -> Vec<Message> {
    (0..n)
        .map(|i| Message::new(TOPIC, Some(&vec![b'1'; i + 1])))
        .collect()
}

fn reference_batch_body(messages: &[Message]) -> Vec<u8> {
    MessageBatch::generate_from_list(messages.to_vec())
        .expect("same-topic messages batch fine")
        .encode()
}

fn wait_until(predicate: impl Fn() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    predicate()
}

fn deadline() -> Duration {
    Duration::from_secs(5)
}

fn accumulator(name: &str, sender: &Arc<FakeSender>) -> ProduceAccumulator {
    let sender: Arc<dyn AccumulatorSender> = sender.clone();
    ProduceAccumulator::new(name, sender)
}

/// 起一个线程走一次同步 `send`（真实调用链里调用方线程就是这样阻塞在 `add` 里的）。
fn spawn_send(
    acc: &ProduceAccumulator,
    msg: Message,
    mq: Option<MessageQueue>,
) -> JoinHandle<Result<SendResult>> {
    let acc = acc.clone();
    std::thread::spawn(move || acc.send(msg, mq.as_ref()))
}

/// 等某个同步批次攒够 `count` 条（`filter` 用来在表里挑出指定的那一批）。
fn wait_for_batch(
    acc: &ProduceAccumulator,
    count: i64,
    filter: impl Fn(&Arc<MessageAccumulation>) -> bool,
) -> Option<Arc<MessageAccumulation>> {
    let found = wait_until(
        || {
            acc.sync_batches_snapshot()
                .iter()
                .any(|b| b.count() == count && filter(b))
        },
        Duration::from_secs(3),
    );
    if !found {
        return None;
    }
    acc.sync_batches_snapshot()
        .into_iter()
        .find(|b| b.count() == count && filter(b))
}

// ================================================================ 参数

#[test]
fn default_params_match_java() {
    let acc = accumulator("params", &FakeSender::new());
    assert_eq!(acc.get_batch_max_delay_ms(), DEFAULT_HOLD_MS);
    assert_eq!(acc.get_batch_max_bytes(), DEFAULT_HOLD_SIZE);
    assert_eq!(acc.total_hold_size(), DEFAULT_TOTAL_HOLD_SIZE);
    // Java 的 getTotalBatchMaxBytes 实际返回 holdSize（上游笔误，照抄）
    assert_eq!(acc.get_total_batch_max_bytes(), DEFAULT_HOLD_SIZE);
    assert_eq!(acc.currently_hold_size(), 0);
    assert_eq!(acc.get_batch_max_delay_ms(), 10);
    assert_eq!(acc.get_batch_max_bytes(), 32 * 1024);
    assert_eq!(acc.total_hold_size(), 32 * 1024 * 1024);
}

#[test]
fn param_guards_copy_java_ranges() {
    let acc = accumulator("guards", &FakeSender::new());
    assert!(acc.batch_max_delay_ms(1).is_ok());
    assert!(acc.batch_max_delay_ms(30 * 1000).is_ok());
    assert!(acc.batch_max_delay_ms(0).is_err());
    assert!(acc.batch_max_delay_ms(30 * 1000 + 1).is_err());

    assert!(acc.batch_max_bytes(1).is_ok());
    assert!(acc.batch_max_bytes(2 * 1024 * 1024).is_ok());
    assert!(acc.batch_max_bytes(0).is_err());
    assert!(acc.batch_max_bytes(2 * 1024 * 1024 + 1).is_err());

    assert!(acc.total_batch_max_bytes(1).is_ok());
    assert!(acc.total_batch_max_bytes(0).is_err());

    // 文案逐字对齐 Java（Python / C# 两侧的断言同样比文案）
    let err = acc.batch_max_delay_ms(0).unwrap_err().to_string();
    assert!(err.contains("batchMaxDelayMs expect between 1ms and 30s, but get 0!"), "{err}");
    let err = acc.batch_max_bytes(0).unwrap_err().to_string();
    assert!(err.contains("batchMaxBytes expect between 1B and 2MB, but get 0!"), "{err}");
    let err = acc.total_batch_max_bytes(0).unwrap_err().to_string();
    assert!(err.contains("totalBatchMaxBytes must bigger then 0, but get 0!"), "{err}");
}

#[test]
fn registry_reuses_accumulator_by_client_id() {
    // Java `MQClientManager.getOrCreateProduceAccumulator`：按 clientId 复用 ——
    // 第二个 sender 被忽略，两边共享同一份阈值 / 守卫线程。
    let a = get_or_create_produce_accumulator(
        "rust-accum-test-shared",
        FakeSender::new() as Arc<dyn AccumulatorSender>,
    );
    let b = get_or_create_produce_accumulator(
        "rust-accum-test-shared",
        FakeSender::new() as Arc<dyn AccumulatorSender>,
    );
    a.batch_max_delay_ms(1234).unwrap();
    assert_eq!(b.get_batch_max_delay_ms(), 1234);
    a.batch_max_delay_ms(DEFAULT_HOLD_MS).unwrap();
    assert_eq!(b.get_batch_max_delay_ms(), DEFAULT_HOLD_MS);
}

// ================================================================ 全局字节闸门

#[test]
fn try_add_message_gate_and_release() {
    let acc = accumulator("gate", &FakeSender::new());
    acc.total_batch_max_bytes(10).unwrap();
    let msg = Message::new(TOPIC, Some(b"1234567890")); // 10 字节
    assert!(acc.try_add_message(&msg));
    assert_eq!(acc.currently_hold_size(), 10);
    // 额度已满（Java：`currentlyHoldSize < totalHoldSize` 才放行）
    assert!(!acc.try_add_message(&Message::new(TOPIC, Some(b"x"))));
    assert_eq!(acc.currently_hold_size(), 10);
    acc.release_hold(10);
    assert_eq!(acc.currently_hold_size(), 0);
    assert!(acc.try_add_message(&Message::new(TOPIC, Some(b"x"))));
    // 空 body 不记账但仍然放行
    acc.release_hold(1);
    acc.total_batch_max_bytes(5).unwrap();
    assert!(acc.try_add_message(&Message::new(TOPIC, Some(b""))));
    assert_eq!(acc.currently_hold_size(), 0);
}

// ================================================================ 归并

#[test]
fn sync_batch_merges_messages_and_returns_per_message_results() {
    // 5 条同键消息攒成 1 个 MessageBatch，各自拿到自己的拆条 SendResult
    let sender = FakeSender::with_result(SendResult {
        status: SendStatus::SendOk,
        msg_id: Some("id-0,id-1,id-2,id-3,id-4".to_string()),
        message_queue: Some(MessageQueue::new(TOPIC, "broker-a", 0)),
        queue_offset: 100,
        offset_msg_id: Some("off-0,off-1,off-2,off-3,off-4".to_string()),
        ..SendResult::default()
    });
    let acc = accumulator("sync-batch", &sender);
    // holdMs 拉长到 3s：保证 5 条都进同一批（用例手动触发发送，不等这 3s）
    acc.batch_max_delay_ms(3000).unwrap();
    let messages = make_messages(5);
    // 真实调用链里是 `can_batch` 先 `try_add_message` 记账，累加器发完再归还
    for m in &messages {
        assert!(acc.try_add_message(m));
    }

    let collected: Arc<Mutex<Vec<SendResult>>> = Arc::new(Mutex::new(Vec::new()));
    let threads: Vec<_> = messages
        .into_iter()
        .map(|msg| {
            let acc = acc.clone();
            let sink = Arc::clone(&collected);
            std::thread::spawn(move || {
                let result = acc.send(msg, None).expect("sync send ok");
                sink.lock().unwrap().push(result);
            })
        })
        .collect();

    let the_batch = wait_for_batch(&acc, 5, |_| true).expect("5 messages land in one batch");
    assert_eq!(the_batch.send_callbacks().len(), 0);
    assert!(the_batch.keys().is_empty());

    // 手动触发（等价于守卫线程在 holdMs 到点后叫醒某一个等待者去发）
    the_batch.force_sync_send().expect("forced sync send ok");
    for t in threads {
        t.join().expect("sender thread joined");
    }

    assert_eq!(sender.sent_count(), 1);
    assert!(sender.mq_at(0).is_none()); // 没指定 mq → 由生产者轮询选
    assert!(!sender.callback_at(0)); // 同步路径没有回调
    // 子消息集合与 reference 一致。⚠ 只比长度不比字节：5 条是**不同线程**并发 add 的，
    // 批内顺序不确定（Java 的同步用例也因此只断言长度，只有单线程的异步用例才全等比较）。
    assert_eq!(
        sender.batch_at(0).body().len(),
        reference_batch_body(&make_messages(5)).len()
    );

    let collected = collected.lock().unwrap();
    assert_eq!(collected.len(), 5);
    // 拆条结果按**批内位置**下发，而位置由 add 的先后决定 —— 并发 add 下「哪条消息拿哪个
    // 下标」不确定（Java 单测同样只能断言"成套"）。所以验两件事：① 五个 MsgId 一个不少；
    // ② 同一条结果里 id-N ↔ off-N ↔ 100+N 必须成套（串了就说明拆条下标算错了）。
    let mut ids: Vec<String> = collected.iter().filter_map(|r| r.msg_id.clone()).collect();
    ids.sort();
    assert_eq!(ids, vec!["id-0", "id-1", "id-2", "id-3", "id-4"]);
    for result in collected.iter() {
        let position: usize = result
            .msg_id
            .as_deref()
            .expect("msg id")
            .split('-')
            .nth(1)
            .expect("id-N")
            .parse()
            .expect("numeric position");
        assert_eq!(
            result.offset_msg_id.as_deref(),
            Some(format!("off-{position}").as_str())
        );
        assert_eq!(result.queue_offset, 100 + position as i64);
    }
    // 发完归还全局额度
    assert_eq!(acc.currently_hold_size(), 0);
}

#[test]
fn async_batch_merges_and_fires_every_callback() {
    // 异步：5 条同键消息由一个守卫线程攒成 1 批，回调各自拿到拆条结果
    let sender = FakeSender::with_result(SendResult {
        status: SendStatus::SendOk,
        msg_id: Some("a,b,c,d,e".to_string()),
        message_queue: Some(MessageQueue::new(TOPIC, "broker-a", 0)),
        queue_offset: 7,
        offset_msg_id: Some("p,q,r,s,t".to_string()),
        ..SendResult::default()
    });
    let acc = accumulator("async-batch", &sender);
    acc.start();
    let callbacks: Vec<Arc<CollectingCallback>> =
        (0..5).map(|_| CollectingCallback::new()).collect();
    let messages = make_messages(5);
    for m in &messages {
        assert!(acc.try_add_message(m));
    }
    for (msg, cb) in messages.into_iter().zip(callbacks.iter()) {
        acc.send_async(msg, None, as_callback(cb));
    }

    // 异步批次靠**守卫线程**唤醒：每 max(1, holdMs/2) ms 扫一遍，readyToSend 就发
    let done = wait_until(|| callbacks.iter().all(|cb| cb.total() == 1), deadline());
    acc.shutdown();
    assert!(done, "guard thread must flush the async batch");

    assert!(callbacks.iter().all(|cb| cb.error_count() == 0));
    let ids: Vec<String> = callbacks.iter().flat_map(|cb| cb.msg_ids()).collect();
    assert_eq!(ids, vec!["a", "b", "c", "d", "e"]);
    let offsets: Vec<i64> = callbacks.iter().flat_map(|cb| cb.queue_offsets()).collect();
    assert_eq!(offsets, vec![7, 8, 9, 10, 11]);

    assert_eq!(sender.sent_count(), 1);
    assert!(sender.callback_at(0)); // 异步路径回调不为 None
    // 单线程依次 add → 批内顺序确定，可以逐字节比
    assert_eq!(
        sender.batch_at(0).encode(),
        reference_batch_body(&make_messages(5))
    );
    // 异步批次**不**收集 keys（Java 的不对称行为），所以 batch 级 KEYS 为空串
    assert_eq!(sender.batch_at(0).get_property(PROPERTY_KEYS), Some(""));
    assert_eq!(acc.currently_hold_size(), 0);
}

#[test]
fn split_results_are_shared_when_msg_id_has_no_comma() {
    // 老 broker / 单条应答：msgId 不含逗号时所有下标指向同一份内容
    let sender = FakeSender::with_result(SendResult {
        status: SendStatus::SendOk,
        msg_id: Some("single-id".to_string()),
        message_queue: Some(MessageQueue::new(TOPIC, "b", 0)),
        queue_offset: 3,
        ..SendResult::default()
    });
    let acc = accumulator("shared-result", &sender);
    acc.batch_max_delay_ms(3000).unwrap();
    let collected: Arc<Mutex<Vec<SendResult>>> = Arc::new(Mutex::new(Vec::new()));
    let threads: Vec<_> = make_messages(3)
        .into_iter()
        .map(|msg| {
            let acc = acc.clone();
            let sink = Arc::clone(&collected);
            std::thread::spawn(move || {
                let result = acc.send(msg, None).expect("sync send ok");
                sink.lock().unwrap().push(result);
            })
        })
        .collect();

    let the_batch = wait_for_batch(&acc, 3, |_| true).expect("3 messages land in one batch");
    the_batch.force_sync_send().expect("forced sync send ok");
    for t in threads {
        t.join().expect("sender thread joined");
    }

    let collected = collected.lock().unwrap();
    assert_eq!(collected.len(), 3);
    // Java 是同一个 `SendResult` 实例；本端口按 `Clone` 复制（见模块头「语言级差异」），
    // 所以断言"每一份内容都等于应答本身"而不是指针相等。
    for result in collected.iter() {
        assert_eq!(result.msg_id.as_deref(), Some("single-id"));
        assert_eq!(result.offset_msg_id, None);
        assert_eq!(result.queue_offset, 3);
        assert_eq!(result.status, SendStatus::SendOk);
    }
    assert_eq!(
        sender.batch_at(0).body().len(),
        reference_batch_body(&make_messages(3)).len()
    );
}

#[test]
fn send_with_message_queue_pins_the_batch() {
    // 指定 mq（Java 的 send(msg, mq, producer)）：mq 原样透传给 sendDirect
    let mq = MessageQueue::new(TOPIC, "broker-pinned", 2);
    let sender = FakeSender::new();
    let acc = accumulator("pinned", &sender);
    acc.batch_max_delay_ms(3000).unwrap();

    let threads: Vec<_> = make_messages(2)
        .into_iter()
        .map(|msg| spawn_send(&acc, msg, Some(mq.clone())))
        .collect();
    let the_batch = wait_for_batch(&acc, 2, |_| true).expect("2 messages land in one batch");
    the_batch.force_sync_send().expect("forced sync send ok");
    for t in threads {
        t.join().expect("sender thread joined").expect("sync send ok");
    }
    assert_eq!(sender.sent_count(), 1);
    assert_eq!(sender.mq_at(0), Some(mq.clone()));

    // 异步 + 指定 mq
    let sender2 = FakeSender::new();
    let acc2 = accumulator("pinned-async", &sender2);
    acc2.start();
    let cb = CollectingCallback::new();
    let msg = make_messages(1).into_iter().next().unwrap();
    assert!(acc2.try_add_message(&msg));
    acc2.send_async(msg, Some(&mq), as_callback(&cb));
    let done = wait_until(|| cb.total() == 1, deadline());
    acc2.shutdown();
    assert!(done, "guard thread must flush the pinned async batch");
    assert_eq!(cb.error_count(), 0);
    assert_eq!(sender2.sent_count(), 1);
    assert_eq!(sender2.mq_at(0), Some(mq));
}

#[test]
fn batch_merges_keys_with_space_separator() {
    // 同步批次的 KEYS = 全体子消息 keys 的并集，空格 join（MessageConst.KEY_SEPARATOR）
    let sender = FakeSender::new();
    let acc = accumulator("keys", &sender);
    acc.batch_max_delay_ms(3000).unwrap();
    let m1 = Message::with_tags_and_keys(TOPIC, Some(b"aa"), None, Some("k1 k2"), 0);
    let m2 = Message::with_tags_and_keys(TOPIC, Some(b"bbb"), None, Some("k2 k3"), 0);
    let threads: Vec<_> = [m1, m2].into_iter().map(|msg| spawn_send(&acc, msg, None)).collect();
    let the_batch = wait_for_batch(&acc, 2, |_| true).expect("2 messages land in one batch");
    // 批里的 keys 是**并集**（去重）
    let mut keys: Vec<String> = the_batch.keys().into_iter().collect();
    keys.sort();
    assert_eq!(keys, vec!["k1", "k2", "k3"]);
    the_batch.force_sync_send().expect("forced sync send ok");
    for t in threads {
        t.join().expect("sender thread joined").expect("sync send ok");
    }

    let raw = sender
        .batch_at(0)
        .get_property(PROPERTY_KEYS)
        .expect("KEYS written unconditionally")
        .to_string();
    let set: HashSet<&str> = raw.split(' ').collect();
    assert_eq!(set, HashSet::from(["k1", "k2", "k3"]));
    // ⚠ 与 Java 有一处**可见**差异：Java 的 keys 是 `HashSet`，`String.join` 的顺序由哈希
    // 决定（未定义）；Rust 侧在 `build_batch` 里排了序，所以顺序确定。属性语义是"空格分隔的
    // 集合"，顺序无关（broker 只拿它做索引），这里断言排序后的结果。
    assert_eq!(raw, "k1 k2 k3");
}

#[test]
fn aggregate_key_partitions_by_topic_mq_wait_and_tag() {
    let msg = Message::new(TOPIC, Some(b"x"));
    let base = AggregateKey::of_message(&msg);
    // tag 缺省 → None（不是空串）
    assert_eq!(base.tag, None);
    assert!(base.wait_store_msg_ok);
    assert!(base.mq.is_none());
    assert_eq!(base.topic, TOPIC);

    let mut tagged = Message::new(TOPIC, Some(b"x"));
    tagged.set_tags("TagA");
    let tagged_key = AggregateKey::of_message(&tagged);
    assert_eq!(tagged_key.tag.as_deref(), Some("TagA"));
    assert_ne!(base, tagged_key);

    let mut no_wait = Message::new(TOPIC, Some(b"x"));
    no_wait.set_wait_store_msg_ok(false);
    let no_wait_key = AggregateKey::of_message(&no_wait);
    assert!(!no_wait_key.wait_store_msg_ok);
    assert_ne!(base, no_wait_key);

    let other_topic = AggregateKey::of_message(&Message::new("Other", Some(b"x")));
    assert_ne!(base, other_topic);

    let mq = MessageQueue::new(TOPIC, "broker-a", 0);
    let pinned = AggregateKey::of_message_with_mq(&msg, &mq);
    assert_eq!(pinned.mq, Some(mq));
    assert_ne!(base, pinned);

    // HashSet 语义：四维任一不同就是不同的键
    let keys: HashSet<AggregateKey> =
        [base.clone(), tagged_key, no_wait_key, other_topic, pinned].into_iter().collect();
    assert_eq!(keys.len(), 5);
}

#[test]
fn different_tags_do_not_merge() {
    // tag 不同的两条消息必须落进**两个**批次（一个 MessageBatch 只有一个 TAGS 属性），
    // 而且各自独立发出去
    let sender = FakeSender::new();
    let acc = accumulator("tags", &sender);
    acc.start(); // 用默认 holdMs(10ms)，让守卫线程把两个批次分别推出去
    let threads: Vec<_> = ["TagA", "TagB"]
        .into_iter()
        .map(|tag| {
            let acc = acc.clone();
            std::thread::spawn(move || {
                let mut msg = Message::new(TOPIC, Some(tag.as_bytes()));
                msg.set_tags(tag);
                acc.send(msg, None).expect("sync send ok");
            })
        })
        .collect();
    let both = wait_until(|| sender.sent_count() == 2, deadline());
    acc.shutdown();
    for t in threads {
        t.join().expect("sender thread joined");
    }
    assert!(both, "two distinct tags must produce two distinct batches");

    let mut tags: Vec<Option<String>> = (0..2)
        .map(|i| sender.batch_at(i).get_property(PROPERTY_TAGS).map(str::to_string))
        .collect();
    tags.sort();
    assert_eq!(tags, vec![Some("TagA".to_string()), Some("TagB".to_string())]);
    assert_eq!(sender.sent_count(), 2);
}

// ================================================================ 守卫线程

#[test]
fn guard_keeps_closed_batch_until_next_send() {
    // 发完的批次 messages_size 仍 > 0，所以会**留在表里**；
    // 下一次同键 send 拿到它、`add` 返回 -1 才被摘掉重取（Java 的真实行为）
    let sender = FakeSender::new();
    let acc = accumulator("closed-batch", &sender);
    acc.batch_max_delay_ms(3000).unwrap();
    let msg = make_messages(1).into_iter().next().unwrap();

    assert!(acc.try_add_message(&msg));
    let first_thread = spawn_send(&acc, msg.clone(), None);
    let first = wait_for_batch(&acc, 1, |_| true).expect("first batch");
    first.force_sync_send().expect("forced sync send ok");
    first_thread.join().expect("sender thread joined").expect("sync send ok");

    assert!(first.is_closed());
    assert_eq!(first.messages_size(), 1); // 只置 closed，不重置 size
    // 守卫线程看到 size > 0 → 不摘表
    acc.run_guard_once(true);
    assert_eq!(acc.sync_batch_count(), 1);

    // 同键再来一条 → 拿到的是那个已关闭的批次 → add 返回 -1 → 摘表重取（新批次）
    assert!(acc.try_add_message(&msg));
    let second_thread = spawn_send(&acc, msg.clone(), None);
    let second = wait_for_batch(&acc, 1, |b| !Arc::ptr_eq(b, &first))
        .expect("a fresh batch replaces the closed one");
    assert!(!second.is_closed());
    second.force_sync_send().expect("forced sync send ok");
    second_thread.join().expect("sender thread joined").expect("sync send ok");

    assert_eq!(sender.sent_count(), 2);
    assert_eq!(acc.currently_hold_size(), 0);
}

#[test]
fn guard_removes_empty_batch_without_sending() {
    // 空批次（还没人 add）由守卫线程置 closed 并摘表，且**不发**任何请求
    let sender = FakeSender::new();
    let acc = accumulator("empty-batch", &sender);
    acc.batch_max_delay_ms(3000).unwrap();
    let empty = acc.put_empty_sync_batch(AggregateKey::new(TOPIC, None, true, None));
    assert_eq!(acc.sync_batch_count(), 1);
    assert_eq!(empty.messages_size(), 0);

    acc.run_guard_once(true);
    assert_eq!(acc.sync_batch_count(), 0);
    assert!(empty.is_closed());
    assert_eq!(sender.sent_count(), 0);
    assert_eq!(sender.calls(), 0);
}

#[test]
fn accumulator_can_restart_after_shutdown() {
    // 累加器是**按 clientId 复用**的，生产者 stop → start 会再调一次 start()：
    // 守卫线程必须能重建（Java ServiceThread 同样可重复 start）
    let sender = FakeSender::new();
    let acc = accumulator("restart", &sender);
    acc.start();
    acc.shutdown();
    acc.start();

    let cb = CollectingCallback::new();
    let msg = make_messages(1).into_iter().next().unwrap();
    assert!(acc.try_add_message(&msg));
    acc.send_async(msg, None, as_callback(&cb));
    let done = wait_until(|| cb.total() == 1, deadline());
    acc.shutdown();
    assert!(done, "restarted guard thread must still flush");
    assert_eq!(cb.error_count(), 0);
    assert_eq!(sender.sent_count(), 1);
}

// ================================================================ 失败路径

#[test]
fn sync_send_failure_surfaces_and_returns_hold_size() {
    // 发送失败：异常抛给**触发发送的那个调用方**，全局额度照还（Java 的 finally）。
    // ⚠ 同一批里还在等的其他调用方拿不到 `sendResult`（本批没有结果可拆）——
    // Java 那边是在 `batch.getSendResults()[index]` 上抛 NPE，这里收敛成
    // `sendResult is illegal`。
    let sender = FakeSender::fail_with("boom");
    let acc = accumulator("sync-fail", &sender);
    acc.batch_max_delay_ms(3000).unwrap();
    let msg = make_messages(1).into_iter().next().unwrap();
    assert!(acc.try_add_message(&msg));
    let thread = spawn_send(&acc, msg, None);
    let batch = wait_for_batch(&acc, 1, |_| true).expect("batch");
    let err = batch.force_sync_send().expect_err("sender must report the failure");
    assert!(err.to_string().contains("boom"), "{err}");
    let waiter = thread.join().expect("sender thread joined").unwrap_err();
    assert!(waiter.to_string().contains("sendResult is illegal"), "{waiter}");
    assert_eq!(acc.currently_hold_size(), 0);
}

#[test]
fn async_send_failure_reaches_every_callback() {
    // 一批 3 条 → 每个回调恰好收到**一次**异常（Java 的 `onException` 逐个回调一次）。
    let sender = FakeSender::fail_with("async boom");
    let acc = accumulator("async-fail", &sender);
    acc.start();
    let callbacks: Vec<Arc<CollectingCallback>> =
        (0..3).map(|_| CollectingCallback::new()).collect();
    for cb in &callbacks {
        let msg = make_messages(1).into_iter().next().unwrap();
        assert!(acc.try_add_message(&msg));
        acc.send_async(msg, None, as_callback(cb));
    }
    let done = wait_until(|| callbacks.iter().all(|cb| cb.total() == 1), deadline());
    acc.shutdown();
    assert!(done, "guard thread must deliver the failure to every callback");
    for cb in &callbacks {
        assert_eq!(cb.msg_ids().len(), 0);
        assert_eq!(cb.error_count(), 1);
        let errors = cb.errors();
        assert!(errors.iter().all(|e| e.contains("async boom")), "{errors:?}");
    }
    // 异步路径：回调交付时才归还额度
    assert_eq!(acc.currently_hold_size(), 0);
    assert_eq!(sender.sent_count(), 1);
}
