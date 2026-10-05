//! 生产者自动攒批（对应 `org.apache.rocketmq.client.producer.ProduceAccumulator`，
//! Java 5.5.0）。与 `python/client/produce_accumulator.py`、
//! `csharp/src/RocketMQ.Client/Client/ProduceAccumulator.cs` 同题。
//!
//! 打开 `autoBatch` 之后，`send(Message)` 不再一条一条直发，而是先按
//! `AggregateKey(topic, mq, waitStoreMsgOK, tag)` 归并进 `MessageAccumulation`，攒够
//! `holdMs` / `holdSize`（或被守卫线程唤醒）再合成**一个** `MessageBatch` 发出去，
//! 最后把 broker 回的**批量** `SendResult` 拆回每条消息各自的 `SendResult` —— 调用方
//! 看到的东西与直发一致。
//!
//! 契约（逐条对齐 Java，别"顺手优化"）：
//!   1. `AggregateKey` 是 topic + mq + waitStoreMsgOK + tag 四元组：tag 不同不合并
//!      （一个 `MessageBatch` 只有一个 TAGS 属性）；指定 mq 与不指定 mq 也不合并。
//!   2. `try_add_message` 是全局字节闸门：`currentlyHoldSize < totalHoldSize` 才放行，
//!      放行时把本条 body 长度记进去；**批次真的发完**才扣回。⚠ 上游真实口径是
//!      「先记账，再判延时/重试」—— `can_batch` 里因延时消息退回直发的那条，其字节数已被
//!      记进 `currentlyHoldSize` 且**永不归还**（Java 遗漏，照抄）。
//!   3. 批量应答拆条：broker 对批量消息回的 MsgId/OffsetMsgId 是**逗号分隔**的逐条 ID；
//!      含逗号才拆，条数对不上报错；不含逗号（老 broker / 单条）时**所有**下标指向
//!      同一份 `SendResult`（这里按 `Clone` 复制，语义等价）。
//!   4. **同步 `add` 收集 keys，异步 `add` 不收集**（Java 的不对称行为，照抄）。
//!   5. 批级 `KEYS` 是 `string.Join(" ", keys)`：分隔符是**空格**
//!      （[`crate::common::message_const::KEY_SEPARATOR`]），且**无条件**写属性 ——
//!      空集合写出 `KEYS=""`。
//!   6. 守卫线程每轮 `max(1, holdMs/2)` ms：sync 版对每个批次 `wakeup()`（叫醒正在
//!      `add` 里等阈值的调用方去自查 `ready_to_send`），再把 `messages_size == 0` 的空批次
//!      置 closed 并摘表；async 版先 `ready_to_send` 就发，再做同样的摘表。
//!      ⚠ 发完的批次 `messages_size` 仍 > 0（`send` 只置 closed，不重置 size），所以它
//!      会**留在表里**，直到下一次同键 `send` 拿到它、`add` 返回 -1 才被摘掉重取。
//!
//! ## 与 Java 的两处「语言级」差异（语义不变）
//!
//! * **同步等待的载体**：Java 是 `synchronized (this) { wait(); }`，这里是
//!   `Mutex<()> + Condvar`。守卫线程的 `wakeup()` 对应 `notify_all()`（Rust 的
//!   `Condvar` 不需要持锁即可唤醒）。
//! * **异常没有共享对象**：[`crate::error::Error`] 不是 `Clone`，而一批 5 条消息的 5 个
//!   回调在 Java 里拿到的是同一个 `Throwable` 实例。这里用 [`clone_error`] 复制：可结构化
//!   复制的变体逐字重建，`Io` / `Json` 退化成同文案的
//!   [`crate::error::Error::Client`]（那两类本就不会出现在批量发送的失败路径上）。

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

/// Java `ProduceAccumulator.DEFAULT_TOTAL_HOLD_SIZE`（32M）。
pub const DEFAULT_TOTAL_HOLD_SIZE: i64 = 32 * 1024 * 1024;
/// Java `ProduceAccumulator.DEFAULT_HOLD_SIZE`（32K）。
pub const DEFAULT_HOLD_SIZE: i64 = 32 * 1024;
/// Java `ProduceAccumulator.DEFAULT_HOLD_MS`（10ms）。
pub const DEFAULT_HOLD_MS: i64 = 10;

/// Java `canBatch` 里的三个定时/延时属性名（`message_const` 里没有对应常量，
/// 与 `producer.rs` 的 `DELAY_PROPERTY_KEYS` 同一口径，写死字面量）。
pub const TIMER_DELAY_KEYS: [&str; 3] = ["TIMER_DELAY_MS", "TIMER_DELAY_SEC", "TIMER_DELIVER_MS"];

/// 累加器唯一的对外依赖：一次「绕过累加器」的批量发送
/// （Java `DefaultMQProducer.sendDirect(MessageBatch, mq, callback)`）。
///
/// 抽成 trait 有两个原因：① Rust 的生产者 `send_batch*` 是 `async`，而累加器的守卫线程
/// 是普通 OS 线程，需要一个明确的同步入口；② 单测要能塞一个「只记账不联网」的假生产者
/// （Java 单测的 `MockMQProducer` 同款）。真实实现见
/// `impl AccumulatorSender for DefaultMQProducer`。
pub trait AccumulatorSender: Send + Sync + 'static {
    /// 同步语义（Java `sendDirect(batch, mq, null)`）：返回该批量的 `SendResult`。
    fn send_direct_blocking(
        &self,
        batch: MessageBatch,
        mq: Option<&MessageQueue>,
    ) -> Result<SendResult>;

    /// 异步语义（Java `sendDirect(batch, mq, callback)`）：**立刻返回**，结果走 callback。
    fn send_direct_async(
        &self,
        batch: MessageBatch,
        mq: Option<&MessageQueue>,
        callback: Arc<dyn SendCallback>,
    ) -> Result<()>;
}

/// 复制一个 [`Error`]（见文件头「与 Java 的两处语言级差异」）。
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

// ================================================================ 归并键

/// 归并键：topic + mq + waitStoreMsgOK + tag（Java `ProduceAccumulator.AggregateKey`）。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AggregateKey {
    pub topic: String,
    pub mq: Option<MessageQueue>,
    pub wait_store_msg_ok: bool,
    /// Java `Message.getTags()`：属性缺失返回 `null` —— 这里就是 `None`（**不是**空串）。
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
            // Java `AggregateKey(message)` 用 `message.isWaitStoreMsgOK()`：**缺省即 true**。
            // 直接比 `== Some("true")` 会把普通消息（没设过 WAIT）判成 false，
            // 攒出来的批量就会以 `WAIT=false` 下发。见 `message::wait_store_msg_ok_of`。
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

// ================================================================ 批次

/// 一个批次的状态（Java 用 `synchronized (this.closed)` 与 `synchronized (this)` 两把
/// 监视器保护；这里前者对应 `state`、后者对应 `wait_lock` + `wait_cond`）。
struct AccumState {
    closed: bool,
    messages: Vec<Message>,
    /// Java `sendCallbacks`（只有异步 `add` 会写）。
    send_callbacks: Vec<Arc<dyn SendCallback>>,
    /// Java `keys`（只有同步 `add` 会写）。
    keys: HashSet<String>,
    messages_size: i64,
    count: i64,
}

/// 一批待归并的消息（Java `ProduceAccumulator.MessageAccumulation`）。
pub struct MessageAccumulation {
    key: AggregateKey,
    /// 用 `Weak` 断开「累加器 → 表 → 批次 → 累加器」的引用环（Java 那个环是真泄漏，
    /// 这里不必照抄）。
    owner: Weak<AccumulatorInner>,
    sender: Arc<dyn AccumulatorSender>,
    state: Mutex<AccumState>,
    /// Java `synchronized (this)`：等阈值用的监视器（锁序恒为 `wait_lock` → `state`）。
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

    /// 本批的 `send_callbacks` 快照（只有异步 `add` 会写；取证/测试用）。
    pub fn send_callbacks(&self) -> Vec<Arc<dyn SendCallback>> {
        lock(&self.state).send_callbacks.clone()
    }

    /// 本批已收集到的 keys 快照（只有同步 `add` 会写；取证/测试用）。
    pub fn keys(&self) -> HashSet<String> {
        lock(&self.state).keys.clone()
    }

    /// 本批当前的消息快照（取证/测试用）。
    pub fn messages_snapshot(&self) -> Vec<Message> {
        lock(&self.state).messages.clone()
    }

    /// 拆条结果（测试/取证用）。
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

    /// Java `readyToSend()`：按**本批**字节数或本批存活时间（不是全局限额）。
    pub fn ready_to_send(&self) -> bool {
        self.messages_size() > self.hold_size()
            || util_all::current_time_millis() >= self.create_time + self.hold_ms()
    }

    /// Java `wakeup()`：叫醒一个正在 `add` 里等阈值的调用方。
    pub fn wakeup(&self) {
        let _guard = lock(&self.wait_lock);
        if self.is_closed() {
            return;
        }
        self.wait_cond.notify_all();
    }

    /// Java 同步 `add(Message)`：入队；返回本条消息在本批里的下标，`-1` 表示本批已关闭
    /// （调用方需重取）。返回前保证本批**已经发出去**（或已被别人发出去），因此
    /// `send_results[index]` 可用。
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

    /// Java 异步 `add(Message, SendCallback)`：`false` 表示本批已关闭（调用方需重取）。
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

    /// Java `batch()`：把本批组装成一个 `MessageBatch`。
    fn build_batch(&self) -> MessageBatch {
        let st = lock(&self.state);
        let mut batch = MessageBatch::new(st.messages.clone());
        batch.message.set_topic(&self.key.topic);
        batch.message.set_wait_store_msg_ok(self.key.wait_store_msg_ok);
        // 无条件写（空集合即 KEYS=""，见文件头第 5 条）
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

    /// Java `splitSendResults`：批量应答拆成逐条 `SendResult`。
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
            // 不含逗号：老 broker / 单条应答，所有下标都是同一份内容（Java 是同一个实例）
            for _ in 0..count {
                results.push(send_result.clone());
            }
        }
        *lock(&self.send_results) = results;
        Ok(())
    }

    /// 取下标对应的拆条结果（Java `batch.getSendResults()[index]`）。
    pub fn send_result(&self, index: i64) -> Result<SendResult> {
        let results = lock(&self.send_results);
        results
            .get(index as usize)
            .cloned()
            .ok_or_else(|| Error::client("sendResult is illegal"))
    }

    /// Java 同步 `send()`。⚠ 只在**持有 `wait_lock`** 时从 [`Self::add`] 里调用，
    /// 保证「发完 → 唤醒其他等待者」这条顺序（Java 的 `notifyAll()` 同样要求持有监视器）。
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
        // Java：finally 里无条件归还全局字节额度
        self.release_hold(size);
        self.wait_cond.notify_all();
        outcome
    }

    /// Java 异步 `send(SendCallback)`：参数与 Java 一样**不参与**逻辑（回调来自本批自己
    /// 收集的那一串，守卫线程传的就是 `None`）。
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
            // ⚠ Java 在这里**没有**归还 currentlyHoldSize（只有回调路径会还）—— 即"异步发送
            // 在发起阶段就抛异常"会漏掉一份字节额度。照抄，别修。
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

    // ---------------- 测试接缝 ----------------
    /// 测试用：手工触发一次同步发送 —— 等价于「守卫线程在 `holdMs` 到点后叫醒某个还堵在
    /// [`Self::add`] 里的等待者，让它自查 `ready_to_send` 并去发」。Java 单测直接
    /// `batch.send()`，两边都绕开守卫线程，好让断言只盯归并/拆条语义。
    pub fn force_sync_send(&self) -> Result<()> {
        self.send_sync()
    }

    /// 测试用：手工触发一次异步发送（守卫线程 `readyToSend` 分支干的那件事）。
    pub fn force_async_send(self: &Arc<Self>) {
        self.send_async_now();
    }
}

/// 「批量发送完成」的回调：拆条后按序交付给本批收集到的所有用户回调，并归还字节额度。
/// 对应 Java 匿名内部类里的 `onSuccess` / `onException`。
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

/// `msg.getKeys().split(MessageConst.KEY_SEPARATOR)` 的 Java 等价物：Java 的
/// `String.split`（limit=0）**丢弃所有尾部空串**，Rust 的 `split` 会保留。
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

// ================================================================ 累加器

/// 累加器的共享状态（`ProduceAccumulator` 只是它的一个 `Arc` 句柄，
/// 与 `DefaultMQProducer` 的形状一致）。
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

/// 对应 Java `ProduceAccumulator`：按 clientId 复用的自动攒批器。
#[derive(Clone)]
pub struct ProduceAccumulator {
    inner: Arc<AccumulatorInner>,
}

impl ProduceAccumulator {
    /// `sender` 对应 Java `MQClientManager.getOrCreateProduceAccumulator(producer)` 里被
    /// 记下来的那个生产者（批次创建时就捕获，之后不再换）。
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

    // ---------------- 生命周期 ----------------
    // 幂等且可重复：`start → shutdown → start`（生产者重启）会重建两根守卫线程
    // （Java 的 `ServiceThread` 同样可重复 start）。
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

    // ---------------- 参数（Java 的校验口径与文案逐字照抄）----------------
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

    /// Java 这里也返回 `holdSize`（不是 `totalHoldSize`）—— 上游笔误，照抄。
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

    /// 全局字节额度（Java 的 `totalHoldSize`）。
    pub fn total_hold_size(&self) -> i64 {
        self.inner.total_hold_size.load(Ordering::Acquire)
    }

    pub fn currently_hold_size(&self) -> i64 {
        *lock(&self.inner.currently_hold_size)
    }

    // ---------------- 全局字节闸门 ----------------
    /// Java `tryAddMessage`：还有额度就记账放行，否则拒绝（调用方退回直发）。
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

    /// 批次发送完成后的归还。
    pub fn release_hold(&self, size: i64) {
        *lock(&self.inner.currently_hold_size) -= size;
    }

    // ---------------- 表操作 ----------------
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

    /// Java `syncSendBatchs.remove(key, batch)`：只在值仍是它时才摘。
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

    // ---------------- 对外发送入口 ----------------
    /// Java `send(Message, DefaultMQProducer)`（无 mq）。
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

    /// Java `send(Message, MessageQueue, SendCallback, DefaultMQProducer)` 的异步版。
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

    // ---------------- 守卫线程 ----------------
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
                // Java GuardForSyncSendService.doWork：先 wakeup（叫醒等阈值的调用方去
                // 自查 readyToSend），再只摘 messages_size == 0 的空批次。
                v.wakeup();
                if v.messages_size() == 0 {
                    v.mark_closed();
                    self.remove_sync_batch(v.aggregate_key(), &v);
                } else {
                    // Java 这里写的是 `v.notify()`（在 `closed` 的监视器上）—— 那是个**空操作**；
                    // 本端口与 Python/C# 一致，落成一次真唤醒，效果等价且更贴近意图。
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

    // ---------------- 取证 / 测试接缝 ----------------
    /// 同步表里当前的批次数。
    pub fn sync_batch_count(&self) -> usize {
        lock(&self.inner.sync_batches).len()
    }

    /// 异步表里当前的批次数。
    pub fn async_batch_count(&self) -> usize {
        lock(&self.inner.async_batches).len()
    }

    /// 同步表快照（顺序不定）。
    pub fn sync_batches_snapshot(&self) -> Vec<Arc<MessageAccumulation>> {
        lock(&self.inner.sync_batches).values().cloned().collect()
    }

    /// 异步表快照（顺序不定）。
    pub fn async_batches_snapshot(&self) -> Vec<Arc<MessageAccumulation>> {
        lock(&self.inner.async_batches).values().cloned().collect()
    }

    /// 测试用：往同步表里放一个（尚无人 add 的）批次 —— 用来验证守卫线程对
    /// `messages_size == 0` 的清理口径，正常调用链里这个窗口只有一两行代码那么宽。
    pub fn put_empty_sync_batch(&self, key: AggregateKey) -> Arc<MessageAccumulation> {
        self.get_or_create_sync_batch(&key)
    }

    /// 测试用：手工跑一轮守卫（等价于守卫线程某个 `doWork` 周期）。
    pub fn run_guard_once(&self, sync: bool) {
        self.guard_once(sync);
    }
}

// ================================================================ 进程级复用表

/// 进程级复用表：对应 Java `MQClientManager.getOrCreateProduceAccumulator` —— 按 clientId
/// 缓存，所以同进程里两个 clientId 相同的 producer 共享同一个累加器与同一对守卫线程
/// （这也是「阈值先记在 producer 上、`start()` 时再同步下去」的原因）。
fn registry() -> &'static Mutex<HashMap<String, ProduceAccumulator>> {
    static TABLE: OnceLock<Mutex<HashMap<String, ProduceAccumulator>>> = OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 按 clientId 取（或建）累加器。第一次创建时记下的 `sender` 会一直用下去（Java 同）。
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
