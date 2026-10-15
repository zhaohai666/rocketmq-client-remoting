//! `ProduceAccumulator` (auto batching) unit tests -- protocol-alignment cases.
//!
//!
//! * Three-tier parameter validation, plus the semantics of "producer getters return 0 while no accumulator exists";
//! * `try_add_message` global byte gate (admission books the bytes, sending returns them, rejection means the caller sends directly);
//! * Batch response **splitting** (comma-separated msgId/offsetMsgId → each message gets its own result, queueOffset incrementing);
//! * The four dimensions of `AggregateKey` (topic / mq / waitStoreMsgOK / tag);
//! * The guard thread marks an "empty batch that was already sent" closed and removes it from the table;
//! * Sync / async failure paths (error propagated + quota returned; each callback gets its own copy);
//! * `start → shutdown → start` (the accumulator is reused by clientId; a producer restart starts it once more).

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

// ================================================================ fixtures

/// One batch sent by the accumulator: the batch object + target queue + whether there was a callback.
struct SentBatch {
    batch: MessageBatch,
    mq: Option<MessageQueue>,
    has_callback: bool,
}

struct FakeSender {
    sent: Mutex<Vec<SentBatch>>,
    result: Mutex<Option<SendResult>>,
    error: Mutex<Option<String>>,
    auto_complete: AtomicBool,
    /// How many times the two send entry points were called in total.
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

    /// Fallback when no result was set: `SEND_OK` + `123`.
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

    /// The target queue the `i`-th batch was sent to.
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

/// Collecting callback.
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

/// Spawns a thread that performs one synchronous `send` (in the real call chain the caller thread blocks inside `add` just like this).
fn spawn_send(
    acc: &ProduceAccumulator,
    msg: Message,
    mq: Option<MessageQueue>,
) -> JoinHandle<Result<SendResult>> {
    let acc = acc.clone();
    std::thread::spawn(move || acc.send(msg, mq.as_ref()))
}

/// Waits until some synchronous batch accumulates `count` messages (`filter` picks out the specific batch in the table).
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

// ================================================================ parameters

#[test]
fn default_params_match_contract() {
    let acc = accumulator("params", &FakeSender::new());
    assert_eq!(acc.get_batch_max_delay_ms(), DEFAULT_HOLD_MS);
    assert_eq!(acc.get_batch_max_bytes(), DEFAULT_HOLD_SIZE);
    assert_eq!(acc.total_hold_size(), DEFAULT_TOTAL_HOLD_SIZE);
    assert_eq!(acc.get_total_batch_max_bytes(), DEFAULT_HOLD_SIZE);
    assert_eq!(acc.currently_hold_size(), 0);
    assert_eq!(acc.get_batch_max_delay_ms(), 10);
    assert_eq!(acc.get_batch_max_bytes(), 32 * 1024);
    assert_eq!(acc.total_hold_size(), 32 * 1024 * 1024);
}

#[test]
fn param_guards_enforce_contract_ranges() {
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

    let err = acc.batch_max_delay_ms(0).unwrap_err().to_string();
    assert!(err.contains("batchMaxDelayMs expect between 1ms and 30s, but get 0!"), "{err}");
    let err = acc.batch_max_bytes(0).unwrap_err().to_string();
    assert!(err.contains("batchMaxBytes expect between 1B and 2MB, but get 0!"), "{err}");
    let err = acc.total_batch_max_bytes(0).unwrap_err().to_string();
    assert!(err.contains("totalBatchMaxBytes must bigger then 0, but get 0!"), "{err}");
}

#[test]
fn registry_reuses_accumulator_by_client_id() {
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

// ================================================================ global byte gate

#[test]
fn try_add_message_gate_and_release() {
    let acc = accumulator("gate", &FakeSender::new());
    acc.total_batch_max_bytes(10).unwrap();
    let msg = Message::new(TOPIC, Some(b"1234567890")); // 10 bytes
    assert!(acc.try_add_message(&msg));
    assert_eq!(acc.currently_hold_size(), 10);
    assert!(!acc.try_add_message(&Message::new(TOPIC, Some(b"x"))));
    assert_eq!(acc.currently_hold_size(), 10);
    acc.release_hold(10);
    assert_eq!(acc.currently_hold_size(), 0);
    assert!(acc.try_add_message(&Message::new(TOPIC, Some(b"x"))));
    // an empty body books nothing but is still admitted
    acc.release_hold(1);
    acc.total_batch_max_bytes(5).unwrap();
    assert!(acc.try_add_message(&Message::new(TOPIC, Some(b""))));
    assert_eq!(acc.currently_hold_size(), 0);
}

// ================================================================ merging

#[test]
fn sync_batch_merges_messages_and_returns_per_message_results() {
    // 5 same-key messages accumulate into 1 MessageBatch, each getting its own split SendResult
    let sender = FakeSender::with_result(SendResult {
        status: SendStatus::SendOk,
        msg_id: Some("id-0,id-1,id-2,id-3,id-4".to_string()),
        message_queue: Some(MessageQueue::new(TOPIC, "broker-a", 0)),
        queue_offset: 100,
        offset_msg_id: Some("off-0,off-1,off-2,off-3,off-4".to_string()),
        ..SendResult::default()
    });
    let acc = accumulator("sync-batch", &sender);
    // holdMs stretched to 3s: guarantees all 5 land in the same batch (the test triggers the send manually, never waiting out the 3s)
    acc.batch_max_delay_ms(3000).unwrap();
    let messages = make_messages(5);
    // in the real call chain `can_batch` books via `try_add_message` first, and the accumulator returns the quota after sending
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

    // manual trigger (equivalent to the guard thread waking one waiter to send once holdMs elapses)
    the_batch.force_sync_send().expect("forced sync send ok");
    for t in threads {
        t.join().expect("sender thread joined");
    }

    assert_eq!(sender.sent_count(), 1);
    assert!(sender.mq_at(0).is_none()); // no mq given → chosen by producer round-robin
    assert!(!sender.callback_at(0)); // the sync path has no callback
    assert_eq!(
        sender.batch_at(0).body().len(),
        reference_batch_body(&make_messages(5)).len()
    );

    let collected = collected.lock().unwrap();
    assert_eq!(collected.len(), 5);
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
    // the global quota is returned after sending
    assert_eq!(acc.currently_hold_size(), 0);
}

#[test]
fn async_batch_merges_and_fires_every_callback() {
    // async: 5 same-key messages accumulate into 1 batch by one guard thread, and the callbacks each get their split results
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

    // async batches are woken by the **guard thread**: a sweep every max(1, holdMs/2) ms, sending once readyToSend
    let done = wait_until(|| callbacks.iter().all(|cb| cb.total() == 1), deadline());
    acc.shutdown();
    assert!(done, "guard thread must flush the async batch");

    assert!(callbacks.iter().all(|cb| cb.error_count() == 0));
    let ids: Vec<String> = callbacks.iter().flat_map(|cb| cb.msg_ids()).collect();
    assert_eq!(ids, vec!["a", "b", "c", "d", "e"]);
    let offsets: Vec<i64> = callbacks.iter().flat_map(|cb| cb.queue_offsets()).collect();
    assert_eq!(offsets, vec![7, 8, 9, 10, 11]);

    assert_eq!(sender.sent_count(), 1);
    assert!(sender.callback_at(0)); // the async path callback is not None
    // single thread adds in turn → in-batch order is deterministic, so a byte-for-byte comparison is possible
    assert_eq!(
        sender.batch_at(0).encode(),
        reference_batch_body(&make_messages(5))
    );
    assert_eq!(sender.batch_at(0).get_property(PROPERTY_KEYS), Some(""));
    assert_eq!(acc.currently_hold_size(), 0);
}

#[test]
fn split_results_are_shared_when_msg_id_has_no_comma() {
    // old broker / single response: when msgId has no comma every index points at the same content
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

    // async + explicit mq
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
    // the sync batch's KEYS = the union of all sub-message keys, joined by spaces (MessageConst.KEY_SEPARATOR)
    let sender = FakeSender::new();
    let acc = accumulator("keys", &sender);
    acc.batch_max_delay_ms(3000).unwrap();
    let m1 = Message::with_tags_and_keys(TOPIC, Some(b"aa"), None, Some("k1 k2"), 0);
    let m2 = Message::with_tags_and_keys(TOPIC, Some(b"bbb"), None, Some("k2 k3"), 0);
    let threads: Vec<_> = [m1, m2].into_iter().map(|msg| spawn_send(&acc, msg, None)).collect();
    let the_batch = wait_for_batch(&acc, 2, |_| true).expect("2 messages land in one batch");
    // the batch's keys are the **union** (deduplicated)
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
    assert_eq!(raw, "k1 k2 k3");
}

#[test]
fn aggregate_key_partitions_by_topic_mq_wait_and_tag() {
    let msg = Message::new(TOPIC, Some(b"x"));
    let base = AggregateKey::of_message(&msg);
    // missing tag → None (not an empty string)
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

    // HashSet semantics: differing in any of the four dimensions makes a different key
    let keys: HashSet<AggregateKey> =
        [base.clone(), tagged_key, no_wait_key, other_topic, pinned].into_iter().collect();
    assert_eq!(keys.len(), 5);
}

#[test]
fn different_tags_do_not_merge() {
    // two messages with different tags must land in **two** batches (one MessageBatch has a single TAGS property),
    // and each must be sent independently
    let sender = FakeSender::new();
    let acc = accumulator("tags", &sender);
    acc.start(); // with the default holdMs(10ms), the guard thread pushes the two batches out separately
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

// ================================================================ guard thread

#[test]
fn guard_keeps_closed_batch_until_next_send() {
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
    assert_eq!(first.messages_size(), 1); // only sets closed, does not reset size
    // the guard thread sees size > 0 → does not remove it from the table
    acc.run_guard_once(true);
    assert_eq!(acc.sync_batch_count(), 1);

    // another same-key message → gets the closed batch → add returns -1 → the table entry is replaced with a fresh batch
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
    // an empty batch (nobody has added) is marked closed and removed by the guard thread, and **no** request is sent
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

// ================================================================ failure paths

#[test]
fn sync_send_failure_surfaces_and_returns_hold_size() {
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
    // async path: the quota is returned only when the callbacks are delivered
    assert_eq!(acc.currently_hold_size(), 0);
    assert_eq!(sender.sent_count(), 1);
}
