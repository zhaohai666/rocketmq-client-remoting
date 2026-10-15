//! With `autoBatch` on, `send(Message)` no longer sends one message at a time; messages are first
//! merged by `AggregateKey(topic, mq, waitStoreMsgOK, tag)` into `MessageAccumulation`, accumulated
//! until `holdMs` / `holdSize` is reached (or the guard thread wakes it), then composed into **one** `MessageBatch` and sent;
//! finally the broker's **batch** `SendResult` is split back into each message's own `SendResult` -- what the
//! caller sees is identical to direct sending.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::client::producer::SendCallback;
use crate::client::result::SendResult;
use crate::common::message::{Message, MessageBatch, MessageQueue};
use crate::common::message_client_id_setter::set_uniq_id;
use crate::common::message_const::KEY_SEPARATOR;
use crate::common::util_all;
use crate::error::{Error, Result};

pub const DEFAULT_TOTAL_HOLD_SIZE: i64 = 32 * 1024 * 1024;
pub const DEFAULT_HOLD_SIZE: i64 = 32 * 1024;
pub const DEFAULT_HOLD_MS: i64 = 10;

pub const TIMER_DELAY_KEYS: [&str; 3] = ["TIMER_DELAY_MS", "TIMER_DELAY_SEC", "TIMER_DELIVER_MS"];

pub trait AccumulatorSender: Send + Sync + 'static {
    fn send_direct_blocking(
        &self,
        batch: MessageBatch,
        mq: Option<&MessageQueue>,
    ) -> Result<SendResult>;

    fn send_direct_async(
        &self,
        batch: MessageBatch,
        mq: Option<&MessageQueue>,
        callback: Arc<dyn SendCallback>,
    ) -> Result<()>;
}

pub fn clone_error(err: &Error) -> Error {
    match err {
        Error::RemotingCommand(m) => Error::RemotingCommand(m.clone()),
        Error::Connect { addr } => Error::Connect { addr: addr.clone() },
        Error::SendRequest { addr, message } => {
            Error::SendRequest { addr: addr.clone(), message: message.clone() }
        }
        Error::Timeout { addr, timeout_millis } => {
            Error::Timeout { addr: addr.clone(), timeout_millis: *timeout_millis }
        }
        Error::TooMuchRequest(m) => Error::TooMuchRequest(m.clone()),
        Error::Server { response_code, remark } => {
            Error::Server { response_code: *response_code, remark: remark.clone() }
        }
        Error::Client { response_code, message } => {
            Error::Client { response_code: *response_code, message: message.clone() }
        }
        Error::Broker { response_code, message } => {
            Error::Broker { response_code: *response_code, message: message.clone() }
        }
        Error::RequestTimeout { topic, timeout_millis } => {
            Error::RequestTimeout { topic: topic.clone(), timeout_millis: *timeout_millis }
        }
        Error::Decode(m) => Error::Decode(m.clone()),
        Error::Encode(m) => Error::Encode(m.clone()),
        Error::Io(e) => Error::client(e.to_string()),
        Error::Json(e) => Error::client(e.to_string()),
    }
}

// ================================================================ aggregation key

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AggregateKey {
    pub topic: String,
    pub mq: Option<MessageQueue>,
    pub wait_store_msg_ok: bool,
    pub tag: Option<String>,
}

impl AggregateKey {
    pub fn new(topic: &str, mq: Option<MessageQueue>, wait_store_msg_ok: bool, tag: Option<String>) -> AggregateKey {
        AggregateKey { topic: topic.to_string(), mq, wait_store_msg_ok, tag }
    }

    pub fn of_message(msg: &Message) -> AggregateKey {
        AggregateKey::new(
            msg.get_topic(),
            None,
            msg.is_wait_store_msg_ok(),
            msg.get_tags().map(str::to_string),
        )
    }

    pub fn of_message_with_mq(msg: &Message, mq: &MessageQueue) -> AggregateKey {
        AggregateKey::new(
            msg.get_topic(),
            Some(mq.clone()),
            msg.is_wait_store_msg_ok(),
            msg.get_tags().map(str::to_string),
        )
    }
}

// ================================================================ batch

struct AccumState {
    closed: bool,
    messages: Vec<Message>,
    send_callbacks: Vec<Arc<dyn SendCallback>>,
    keys: HashSet<String>,
    messages_size: i64,
    count: i64,
}

pub struct MessageAccumulation {
    key: AggregateKey,
    owner: Weak<AccumulatorInner>,
    sender: Arc<dyn AccumulatorSender>,
    state: Mutex<AccumState>,
    wait_lock: Mutex<()>,
    wait_cond: Condvar,
    create_time: i64,
    send_results: Mutex<Vec<SendResult>>,
}

impl MessageAccumulation {
    fn new(key: AggregateKey, sender: Arc<dyn AccumulatorSender>, owner: Weak<AccumulatorInner>) -> MessageAccumulation {
        MessageAccumulation {
            key,
            owner,
            sender,
            state: Mutex::new(AccumState {
                closed: false,
                messages: Vec::new(),
                send_callbacks: Vec::new(),
                keys: HashSet::new(),
                messages_size: 0,
                count: 0,
            }),
            wait_lock: Mutex::new(()),
            wait_cond: Condvar::new(),
            create_time: util_all::current_time_millis(),
            send_results: Mutex::new(Vec::new()),
        }
    }

    pub fn aggregate_key(&self) -> &AggregateKey {
        &self.key
    }

    pub fn is_closed(&self) -> bool {
        lock(&self.state).closed
    }

    pub fn count(&self) -> i64 {
        lock(&self.state).count
    }

    pub fn messages_size(&self) -> i64 {
        lock(&self.state).messages_size
    }

    /// Snapshot of this batch's `send_callbacks` (written only by async `add`; forensic/testing use).
    pub fn send_callbacks(&self) -> Vec<Arc<dyn SendCallback>> {
        lock(&self.state).send_callbacks.clone()
    }

    /// Snapshot of the keys collected in this batch (written only by sync `add`; forensic/testing use).
    pub fn keys(&self) -> HashSet<String> {
        lock(&self.state).keys.clone()
    }

    /// Snapshot of the messages currently in this batch (forensic/testing use).
    pub fn messages_snapshot(&self) -> Vec<Message> {
        lock(&self.state).messages.clone()
    }

    /// Per-message split results (testing/forensic use).
    pub fn send_results_snapshot(&self) -> Vec<SendResult> {
        lock(&self.send_results).clone()
    }

    fn mark_closed(&self) {
        lock(&self.state).closed = true;
    }

    fn hold_ms(&self) -> i64 {
        match self.owner.upgrade() {
            Some(inner) => inner.hold_ms.load(Ordering::Acquire),
            None => DEFAULT_HOLD_MS,
        }
    }

    fn hold_size(&self) -> i64 {
        match self.owner.upgrade() {
            Some(inner) => inner.hold_size.load(Ordering::Acquire),
            None => DEFAULT_HOLD_SIZE,
        }
    }

    pub fn ready_to_send(&self) -> bool {
        self.messages_size() > self.hold_size()
            || util_all::current_time_millis() >= self.create_time + self.hold_ms()
    }

    pub fn wakeup(&self) {
        let _guard = lock(&self.wait_lock);
        if self.is_closed() {
            return;
        }
        self.wait_cond.notify_all();
    }

    pub fn add(&self, msg: Message) -> Result<i64> {
        let index = {
            let mut st = lock(&self.state);
            if st.closed {
                return Ok(-1);
            }
            let ret = st.count;
            st.count += 1;
            let body_len = msg.get_body().len() as i64;
            if body_len > 0 {
                st.messages_size += body_len;
            }
            if let Some(keys) = msg.get_keys() {
                split_keys_into(&mut st.keys, keys);
            }
            st.messages.push(msg);
            ret
        };

        let mut guard = lock(&self.wait_lock);
        while !lock(&self.state).closed {
            if self.ready_to_send() {
                self.send_sync()?;
                break;
            }
            guard = self.wait_cond.wait(guard).unwrap_or_else(|e| e.into_inner());
        }
        drop(guard);
        Ok(index)
    }

    pub fn add_async(self: &Arc<Self>, msg: Message, callback: Arc<dyn SendCallback>) -> bool {
        {
            let mut st = lock(&self.state);
            if st.closed {
                return false;
            }
            st.count += 1;
            let body_len = msg.get_body().len() as i64;
            if body_len > 0 {
                st.messages_size += body_len;
            }
            st.messages.push(msg);
            st.send_callbacks.push(callback);
        }
        if self.ready_to_send() {
            self.send_async_now();
        }
        true
    }

    fn build_batch(&self) -> MessageBatch {
        let st = lock(&self.state);
        let mut batch = MessageBatch::new(st.messages.clone());
        batch.message.set_topic(&self.key.topic);
        batch.message.set_wait_store_msg_ok(self.key.wait_store_msg_ok);
        // Written unconditionally (an empty set means KEYS="", see item 5 of the file header)
        let mut keys: Vec<&str> = st.keys.iter().map(String::as_str).collect();
        keys.sort_unstable();
        batch.message.set_keys(&keys.join(KEY_SEPARATOR));
        if let Some(tag) = &self.key.tag {
            batch.message.set_tags(tag);
        }
        set_uniq_id(&mut batch.message);
        let body = batch.encode();
        batch.message.set_body(Some(&body));
        batch
    }

    fn split_send_results(&self, send_result: &SendResult) -> Result<()> {
        let count = lock(&self.state).count as usize;
        let msg_id = send_result.get_msg_id().unwrap_or("").to_string();
        let mut results = Vec::with_capacity(count);
        if msg_id.contains(',') {
            let ids: Vec<&str> = msg_id.split(',').collect();
            let offsets: Vec<&str> = send_result.offset_msg_id.as_deref().unwrap_or("").split(',').collect();
            if ids.len() != count || offsets.len() != count {
                return Err(Error::client("sendResult is illegal"));
            }
            for i in 0..count {
                let mut item = send_result.clone();
                item.msg_id = Some(ids[i].to_string());
                item.offset_msg_id = Some(offsets[i].to_string());
                item.queue_offset = send_result.queue_offset + i as i64;
                results.push(item);
            }
        } else {
            for _ in 0..count {
                results.push(send_result.clone());
            }
        }
        *lock(&self.send_results) = results;
        Ok(())
    }

    pub fn send_result(&self, index: i64) -> Result<SendResult> {
        let results = lock(&self.send_results);
        results
            .get(index as usize)
            .cloned()
            .ok_or_else(|| Error::client("sendResult is illegal"))
    }

    fn send_sync(&self) -> Result<()> {
        {
            let mut st = lock(&self.state);
            if st.closed {
                return Ok(());
            }
            st.closed = true;
        }

        let batch = self.build_batch();
        let size = self.messages_size();
        let outcome = match self.sender.send_direct_blocking(batch, self.key.mq.as_ref()) {
            Ok(result) => self.split_send_results(&result),
            Err(e) => Err(e),
        };
        self.release_hold(size);
        self.wait_cond.notify_all();
        outcome
    }

    fn send_async_now(self: &Arc<Self>) {
        {
            let mut st = lock(&self.state);
            if st.closed {
                return;
            }
            st.closed = true;
        }

        let batch = self.build_batch();
        let callbacks = self.send_callbacks();
        let size = self.messages_size();
        let resolver: Arc<dyn SendCallback> = Arc::new(AccumulationCallback {
            batch: Arc::clone(self),
            callbacks: callbacks.clone(),
            size,
        });
        if let Err(e) = self.sender.send_direct_async(batch, self.key.mq.as_ref(), resolver) {
            for cb in &callbacks {
                cb.on_exception(clone_error(&e));
            }
        }
    }

    fn release_hold(&self, size: i64) {
        if let Some(inner) = self.owner.upgrade() {
            *lock(&inner.currently_hold_size) -= size;
        }
    }

    pub fn force_sync_send(&self) -> Result<()> {
        self.send_sync()
    }

    /// Test hook: manually triggers one async send (what the guard thread's `readyToSend` branch does).
    pub fn force_async_send(self: &Arc<Self>) {
        self.send_async_now();
    }
}

struct AccumulationCallback {
    batch: Arc<MessageAccumulation>,
    callbacks: Vec<Arc<dyn SendCallback>>,
    size: i64,
}

impl AccumulationCallback {
    fn finish(&self, outcome: Result<()>) {
        match outcome {
            Ok(()) => {
                let results = self.batch.send_results_snapshot();
                for (i, cb) in self.callbacks.iter().enumerate() {
                    match results.get(i) {
                        Some(r) => cb.on_success(r.clone()),
                        None => cb.on_exception(Error::client("sendResult is illegal")),
                    }
                }
            }
            Err(e) => {
                for cb in &self.callbacks {
                    cb.on_exception(clone_error(&e));
                }
            }
        }
        self.batch.release_hold(self.size);
    }
}

impl SendCallback for AccumulationCallback {
    fn on_success(&self, result: SendResult) {
        let outcome = self.batch.split_send_results(&result);
        self.finish(outcome);
    }

    fn on_exception(&self, err: Error) {
        self.finish(Err(err));
    }
}

fn split_keys_into(target: &mut HashSet<String>, keys: &str) {
    let mut parts: Vec<&str> = keys.split(KEY_SEPARATOR).collect();
    while matches!(parts.last(), Some(s) if s.is_empty()) {
        parts.pop();
    }
    for p in parts {
        target.insert(p.to_string());
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

// ================================================================ accumulator

/// Shared state of the accumulator (`ProduceAccumulator` is just an `Arc` handle to it,
/// matching the shape of `DefaultMQProducer`).
struct AccumulatorInner {
    instance_name: String,
    hold_ms: AtomicI64,
    hold_size: AtomicI64,
    total_hold_size: AtomicI64,
    currently_hold_size: Mutex<i64>,
    sync_batches: Mutex<HashMap<AggregateKey, Arc<MessageAccumulation>>>,
    async_batches: Mutex<HashMap<AggregateKey, Arc<MessageAccumulation>>>,
    sender: Arc<dyn AccumulatorSender>,
    guards: Mutex<Guards>,
    stopped: AtomicBool,
}

#[derive(Default)]
struct Guards {
    sync: Option<JoinHandle<()>>,
    asynchronous: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct ProduceAccumulator {
    inner: Arc<AccumulatorInner>,
}

impl ProduceAccumulator {
    pub fn new(instance_name: &str, sender: Arc<dyn AccumulatorSender>) -> ProduceAccumulator {
        ProduceAccumulator {
            inner: Arc::new(AccumulatorInner {
                instance_name: instance_name.to_string(),
                hold_ms: AtomicI64::new(DEFAULT_HOLD_MS),
                hold_size: AtomicI64::new(DEFAULT_HOLD_SIZE),
                total_hold_size: AtomicI64::new(DEFAULT_TOTAL_HOLD_SIZE),
                currently_hold_size: Mutex::new(0),
                sync_batches: Mutex::new(HashMap::new()),
                async_batches: Mutex::new(HashMap::new()),
                sender,
                guards: Mutex::new(Guards::default()),
                stopped: AtomicBool::new(false),
            }),
        }
    }

    pub fn instance_name(&self) -> &str {
        &self.inner.instance_name
    }

    pub fn start(&self) {
        let mut guards = lock(&self.inner.guards);
        if guards.sync.is_some() {
            return;
        }
        self.inner.stopped.store(false, Ordering::Release);
        let sync_this = self.clone();
        guards.sync = Some(
            std::thread::Builder::new()
                .name(format!("Client_{}_GuardForSyncSend", self.inner.instance_name))
                .spawn(move || sync_this.guard_loop(true))
                .expect("spawn guard for sync send"),
        );
        let async_this = self.clone();
        guards.asynchronous = Some(
            std::thread::Builder::new()
                .name(format!("Client_{}_GuardForAsyncSend", self.inner.instance_name))
                .spawn(move || async_this.guard_loop(false))
                .expect("spawn guard for async send"),
        );
    }

    pub fn shutdown(&self) {
        self.inner.stopped.store(true, Ordering::Release);
        let mut guards = lock(&self.inner.guards);
        if let Some(h) = guards.sync.take() {
            let _ = h.join();
        }
        if let Some(h) = guards.asynchronous.take() {
            let _ = h.join();
        }
    }

    pub fn get_batch_max_delay_ms(&self) -> i64 {
        self.inner.hold_ms.load(Ordering::Acquire)
    }

    pub fn batch_max_delay_ms(&self, hold_ms: i64) -> Result<()> {
        if hold_ms <= 0 || hold_ms > 30 * 1000 {
            return Err(Error::client(format!(
                "batchMaxDelayMs expect between 1ms and 30s, but get {hold_ms}!"
            )));
        }
        self.inner.hold_ms.store(hold_ms, Ordering::Release);
        Ok(())
    }

    pub fn get_batch_max_bytes(&self) -> i64 {
        self.inner.hold_size.load(Ordering::Acquire)
    }

    pub fn batch_max_bytes(&self, hold_size: i64) -> Result<()> {
        if hold_size <= 0 || hold_size > 2 * 1024 * 1024 {
            return Err(Error::client(format!(
                "batchMaxBytes expect between 1B and 2MB, but get {hold_size}!"
            )));
        }
        self.inner.hold_size.store(hold_size, Ordering::Release);
        Ok(())
    }

    pub fn get_total_batch_max_bytes(&self) -> i64 {
        self.inner.hold_size.load(Ordering::Acquire)
    }

    pub fn total_batch_max_bytes(&self, total_hold_size: i64) -> Result<()> {
        if total_hold_size <= 0 {
            return Err(Error::client(format!(
                "totalBatchMaxBytes must bigger then 0, but get {total_hold_size}!"
            )));
        }
        self.inner.total_hold_size.store(total_hold_size, Ordering::Release);
        Ok(())
    }

    pub fn total_hold_size(&self) -> i64 {
        self.inner.total_hold_size.load(Ordering::Acquire)
    }

    pub fn currently_hold_size(&self) -> i64 {
        *lock(&self.inner.currently_hold_size)
    }

    pub fn try_add_message(&self, message: &Message) -> bool {
        let mut held = lock(&self.inner.currently_hold_size);
        if *held < self.inner.total_hold_size.load(Ordering::Acquire) {
            let body_len = message.get_body().len() as i64;
            if body_len > 0 {
                *held += body_len;
            }
            return true;
        }
        false
    }

    /// Release after a batch send completes.
    pub fn release_hold(&self, size: i64) {
        *lock(&self.inner.currently_hold_size) -= size;
    }

    // ---------------- table operations ----------------
    fn get_or_create_sync_batch(&self, key: &AggregateKey) -> Arc<MessageAccumulation> {
        let mut table = lock(&self.inner.sync_batches);
        if let Some(existing) = table.get(key) {
            return Arc::clone(existing);
        }
        let batch = Arc::new(MessageAccumulation::new(
            key.clone(),
            Arc::clone(&self.inner.sender),
            Arc::downgrade(&self.inner),
        ));
        table.insert(key.clone(), Arc::clone(&batch));
        batch
    }

    fn get_or_create_async_batch(&self, key: &AggregateKey) -> Arc<MessageAccumulation> {
        let mut table = lock(&self.inner.async_batches);
        if let Some(existing) = table.get(key) {
            return Arc::clone(existing);
        }
        let batch = Arc::new(MessageAccumulation::new(
            key.clone(),
            Arc::clone(&self.inner.sender),
            Arc::downgrade(&self.inner),
        ));
        table.insert(key.clone(), Arc::clone(&batch));
        batch
    }

    fn remove_sync_batch(&self, key: &AggregateKey, batch: &Arc<MessageAccumulation>) {
        let mut table = lock(&self.inner.sync_batches);
        if matches!(table.get(key), Some(cur) if Arc::ptr_eq(cur, batch)) {
            table.remove(key);
        }
    }

    fn remove_async_batch(&self, key: &AggregateKey, batch: &Arc<MessageAccumulation>) {
        let mut table = lock(&self.inner.async_batches);
        if matches!(table.get(key), Some(cur) if Arc::ptr_eq(cur, batch)) {
            table.remove(key);
        }
    }

    pub fn send(&self, msg: Message, mq: Option<&MessageQueue>) -> Result<SendResult> {
        let key = match mq {
            Some(mq) => AggregateKey::of_message_with_mq(&msg, mq),
            None => AggregateKey::of_message(&msg),
        };
        loop {
            let batch = self.get_or_create_sync_batch(&key);
            let index = batch.add(msg.clone())?;
            if index < 0 {
                self.remove_sync_batch(&key, &batch);
                continue;
            }
            return batch.send_result(index);
        }
    }

    pub fn send_async(
        &self,
        msg: Message,
        mq: Option<&MessageQueue>,
        callback: Arc<dyn SendCallback>,
    ) {
        let key = match mq {
            Some(mq) => AggregateKey::of_message_with_mq(&msg, mq),
            None => AggregateKey::of_message(&msg),
        };
        loop {
            let batch = self.get_or_create_async_batch(&key);
            if !batch.add_async(msg.clone(), Arc::clone(&callback)) {
                self.remove_async_batch(&key, &batch);
                continue;
            }
            return;
        }
    }

    // ---------------- guard thread ----------------
    fn guard_loop(&self, sync: bool) {
        while !self.inner.stopped.load(Ordering::Acquire) {
            self.guard_once(sync);
        }
    }

    fn guard_once(&self, sync: bool) {
        let sleep_ms = std::cmp::max(1, self.get_batch_max_delay_ms() / 2);
        let batches = if sync { self.sync_batches_snapshot() } else { self.async_batches_snapshot() };
        for v in batches {
            if sync {
                v.wakeup();
                if v.messages_size() == 0 {
                    v.mark_closed();
                    self.remove_sync_batch(v.aggregate_key(), &v);
                } else {
                    v.wait_cond.notify_all();
                }
            } else {
                if v.ready_to_send() {
                    v.send_async_now();
                }
                if v.messages_size() == 0 {
                    v.mark_closed();
                    self.remove_async_batch(v.aggregate_key(), &v);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(sleep_ms as u64));
    }

    // ---------------- forensic / testing seams ----------------
    /// Current batch count in the sync table.
    pub fn sync_batch_count(&self) -> usize {
        lock(&self.inner.sync_batches).len()
    }

    /// Current batch count in the async table.
    pub fn async_batch_count(&self) -> usize {
        lock(&self.inner.async_batches).len()
    }

    /// Snapshot of the sync table (order unspecified).
    pub fn sync_batches_snapshot(&self) -> Vec<Arc<MessageAccumulation>> {
        lock(&self.inner.sync_batches).values().cloned().collect()
    }

    /// Snapshot of the async table (order unspecified).
    pub fn async_batches_snapshot(&self) -> Vec<Arc<MessageAccumulation>> {
        lock(&self.inner.async_batches).values().cloned().collect()
    }

    /// Test hook: puts a batch into the sync table that nobody has `add`ed to yet -- used to verify
    /// the guard thread's cleanup convention for `messages_size == 0`; in the normal call chain this window is only a line or two of code wide.
    pub fn put_empty_sync_batch(&self, key: AggregateKey) -> Arc<MessageAccumulation> {
        self.get_or_create_sync_batch(&key)
    }

    /// Test hook: manually runs one guard round (equivalent to one `doWork` cycle of the guard thread).
    pub fn run_guard_once(&self, sync: bool) {
        self.guard_once(sync);
    }
}

// ================================================================ process-level reuse table

fn registry() -> &'static Mutex<HashMap<String, ProduceAccumulator>> {
    static TABLE: OnceLock<Mutex<HashMap<String, ProduceAccumulator>>> = OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn get_or_create_produce_accumulator(
    client_id: &str,
    sender: Arc<dyn AccumulatorSender>,
) -> ProduceAccumulator {
    let mut table = lock(registry());
    if let Some(existing) = table.get(client_id) {
        return existing.clone();
    }
    let acc = ProduceAccumulator::new(client_id, sender);
    table.insert(client_id.to_string(), acc.clone());
    acc
}

#[cfg(test)]
mod tests;
