//! Request-Reply（5.x）客户端侧支撑，逐条对齐 `python/rocketmq/client/request_reply.py`。
//!
//! 对应 Java 的这几个类：
//! * `org.apache.rocketmq.client.producer.RequestResponseFuture`
//! * `org.apache.rocketmq.client.producer.RequestFutureHolder`
//! * `org.apache.rocketmq.client.utils.MessageUtil#createReplyMessage`
//! * `org.apache.rocketmq.client.impl.ClientRemotingProcessor#receiveReplyMessage`
//!
//! 协议回顾（照 Java 逐字段复刻）：
//!
//! ```text
//! 请求方 (producer.request)                        应答方 (push consumer)
//! ─────────────────────────────                    ──────────────────────
//! msg.properties[CORRELATION_ID] = uuid
//! msg.properties[REPLY_TO_CLIENT] = clientId  ──►  收到请求消息（broker 已写入 CLUSTER）
//! msg.properties[TTL] = timeoutMillis               create_reply_message(request_msg, body):
//!                                                     topic       = <CLUSTER>_REPLY_TOPIC
//!                                                     CORRELATION_ID / REPLY_TO_CLIENT / TTL 原样带回
//!                                                     MSG_TYPE    = "reply"
//! ◄── PUSH_REPLY_MESSAGE_TO_CLIENT(326) ────────    producer.send(reply) →
//!     （broker 按 REPLY_TO_CLIENT 找到请求方连接）      broker 走 SEND_REPLY_MESSAGE_V2(325)
//! ```
//!
//! 两个关键点（错了真机就不通）：
//! 1. 应答消息必须带 `MSG_TYPE == "reply"`，客户端发送时据此把请求码从
//!    `SEND_MESSAGE_V2(310)` 换成 `SEND_REPLY_MESSAGE_V2(325)`；
//!    broker 的 `ReplyMessageProcessor` 只在 324/325 上注册。
//! 2. `REPLY_TO_CLIENT` 是**请求方的 clientId**，broker 用它在 producerManager 里
//!    反查 channel 才能把应答推回来 —— 所以请求方必须发过心跳（已注册为 producer）。
//!
//! 与 Python 的有意差异：Python 用 `threading.Event` 唤醒同步等待方；本实现整体是
//! tokio 异步的，等待方换成 [`tokio::sync::Notify`]（sticky 标志 + `notify_waiters`），
//! 唤醒语义与 Event 一致。Python 的 `react_callback`（把回调适配成 `(response, cause)`
//! 的脚本便捷函数）在本仓库无任何调用点，未移植。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, RwLock};
use std::time::Duration;

use tokio::sync::Notify;

use crate::common::message::{Message, MessageExt};
use crate::common::message_const::{
    PROPERTY_CLUSTER, PROPERTY_CORRELATION_ID, PROPERTY_MESSAGE_REPLY_TO_CLIENT,
    PROPERTY_MESSAGE_TYPE, PROPERTY_MESSAGE_TTL,
};
use crate::common::mix_all::MixAll;
use crate::common::util_all;
use crate::error::{Error, Result};

/// Java `RequestCallback` 的默认超时（`DefaultMQProducer.request` 未显式给超时时用）。
pub const DEFAULT_REQUEST_TIMEOUT_MILLIS: i64 = 3000;

// ---------------------------------------------------------------- 回调

/// 对应 Java `RequestCallback`：异步 `request` 的结果回调。
pub trait RequestCallback: Send + Sync {
    /// 拿到应答（`None` 表示 broker 回了空消息，Java 侧同样可能传 null）。
    fn on_success(&self, response_message: Option<MessageExt>);
    /// 请求或等待失败。
    fn on_exception(&self, cause: &Error);
}

// ---------------------------------------------------------------- 等待槽

/// 一次 `request` 的等待槽（对应 Java `RequestResponseFuture`）。
///
/// 与 Java 的差异（有意，与 Python 一致）：Java 额外起了一个 `scanExpiredRequest`
/// 定时线程清理超时项；本实现由 `request()` 的收尾路径保证移除，故不需要后台扫描线程。
pub struct RequestResponseFuture {
    pub correlation_id: String,
    pub timeout_millis: i64,
    /// Java `beginTimestamp`（毫秒）。测试可直接改写它来构造超时场景。
    pub begin_timestamp: i64,
    request_callback: Option<std::sync::Arc<dyn RequestCallback>>,
    /// 对应 Java `sendRequestOK`：发送阶段失败时置 false。
    send_request_ok: AtomicBool,
    /// 已投递过应答（对应 Python `_event` 的 sticky 语义）。
    responded: AtomicBool,
    /// 回调只允许触发一次的守卫（对应 Java `executeRequestCallback` 的 compareAndSet）。
    callback_fired: AtomicBool,
    state: Mutex<FutureState>,
    notify: Notify,
}

#[derive(Default)]
struct FutureState {
    response_msg: Option<MessageExt>,
    cause: Option<Error>,
}

impl RequestResponseFuture {
    /// Python `RequestResponseFuture(correlation_id, timeout_millis, request_callback=None)`。
    pub fn new(correlation_id: &str, timeout_millis: i64) -> RequestResponseFuture {
        RequestResponseFuture::with_callback(
            correlation_id,
            timeout_millis,
            None,
        )
    }

    pub fn with_callback(
        correlation_id: &str,
        timeout_millis: i64,
        request_callback: Option<std::sync::Arc<dyn RequestCallback>>,
    ) -> RequestResponseFuture {
        RequestResponseFuture {
            correlation_id: correlation_id.to_string(),
            timeout_millis,
            begin_timestamp: util_all::current_time_millis(),
            request_callback,
            send_request_ok: AtomicBool::new(true),
            responded: AtomicBool::new(false),
            callback_fired: AtomicBool::new(false),
            state: Mutex::new(FutureState::default()),
            notify: Notify::new(),
        }
    }

    // ---------------- 等待 / 投递 ----------------

    /// 对应 Java `waitResponseMessage`：等应答，超时返回 `None`。
    ///
    /// 先 `enable()` 出等待凭据再看当前值，保证「应答先于等待到达」不会丢唤醒
    /// （`Notify::notify_waiters` 只叫醒当时已在等的人）。
    pub async fn wait_response_message(&self, timeout_millis: i64) -> Option<MessageExt> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_millis.max(0) as u64);
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(msg) = self.response_message() {
                return Some(msg);
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                // 超时：与 Python 一样返回当时的值（没来过应答就是 None）
                return self.response_message();
            }
        }
    }

    /// 对应 Java `putResponseMessage`：写入应答并唤醒等待方（允许多次调用）。
    pub fn put_response_message(&self, response_msg: Option<MessageExt>) {
        if let Ok(mut state) = self.state.lock() {
            state.response_msg = response_msg;
        }
        self.responded.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn response_message(&self) -> Option<MessageExt> {
        self.state.lock().ok().and_then(|s| s.response_msg.clone())
    }

    /// 对应 Java `isTimeout`。
    pub fn is_timeout(&self) -> bool {
        util_all::current_time_millis() - self.begin_timestamp > self.timeout_millis
    }

    /// 对应 Java `executeRequestCallback`：回调只允许触发一次。
    ///
    /// 没有回调时是 no-op（同步调用方靠 [`Self::wait_response_message`] 唤醒）。
    pub fn execute_request_callback(&self) {
        let Some(callback) = self.request_callback.as_ref() else {
            return;
        };
        if self.callback_fired.swap(true, Ordering::SeqCst) {
            return;
        }
        let cause = self
            .state
            .lock()
            .ok()
            .and_then(|mut s| s.cause.take())
            .filter(|_| !self.send_request_ok.load(Ordering::Acquire));
        match cause {
            Some(e) => callback.on_exception(&e),
            None if self.send_request_ok.load(Ordering::Acquire) => {
                callback.on_success(self.response_message())
            }
            // sendRequestOK=false 但没有 cause：Java 会传 null，这里同样给 on_exception
            None => callback.on_exception(&Error::client(format!(
                "request failed, correlationId={}",
                self.correlation_id
            ))),
        }
    }

    /// 标记发送阶段失败（对应 Java `setSendRequestOK(false)` + `setCause`）。
    pub fn set_failed(&self, cause: Error) {
        self.send_request_ok.store(false, Ordering::Release);
        if let Ok(mut state) = self.state.lock() {
            state.cause = Some(cause);
        }
        self.responded.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub fn is_send_request_ok(&self) -> bool {
        self.send_request_ok.load(Ordering::Acquire)
    }
}

impl std::fmt::Debug for RequestResponseFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RequestResponseFuture [correlationId={}, timeoutMillis={}, beginTimestamp={}]",
            self.correlation_id, self.timeout_millis, self.begin_timestamp
        )
    }
}

// ---------------------------------------------------------------- 等待槽表

/// 对应 Java `RequestFutureHolder`：`correlationId -> 等待槽` 的表。
///
/// Java 里是**跨 producer 共享的单例**（`RequestFutureHolder.getInstance()`），
/// 因为应答由 clientId 级别的 remoting 通道推回，与具体 producer 实例无关；
/// 本仓库用进程内单例 [`request_future_holder()`] 对齐。
#[derive(Default)]
pub struct RequestFutureHolder {
    request_future_table: RwLock<HashMap<String, std::sync::Arc<RequestResponseFuture>>>,
}

impl RequestFutureHolder {
    pub fn new() -> RequestFutureHolder {
        RequestFutureHolder::default()
    }

    /// 对应 Java `putRequest`（同名 key 直接覆盖，与 Python 一致）。
    pub fn put_request(&self, correlation_id: &str, future: std::sync::Arc<RequestResponseFuture>) {
        if let Ok(mut table) = self.request_future_table.write() {
            table.insert(correlation_id.to_string(), future);
        }
    }

    pub fn get_request(&self, correlation_id: &str) -> Option<std::sync::Arc<RequestResponseFuture>> {
        self.request_future_table
            .read()
            .ok()
            .and_then(|t| t.get(correlation_id).cloned())
    }

    /// 对应 Java `removeRequest`：不存在返回 `None`，重复调用幂等。
    pub fn remove_request(
        &self,
        correlation_id: &str,
    ) -> Option<std::sync::Arc<RequestResponseFuture>> {
        self.request_future_table
            .write()
            .ok()
            .and_then(|mut t| t.remove(correlation_id))
    }

    /// 接收侧入口（对齐 Java `processReplyMessage`）：投递应答。
    ///
    /// Java 在这里做的是 **`getRequestFutureTable().remove(correlationId)`** ——
    /// 用「谁摘到谁负责」保证「应答到达」与「超时清理」两条路径只会有一个生效。
    /// 返回被填充的 future；查不到（已超时 / 已移除 / 应答重复）时返回 `None`。
    pub fn put_response(
        &self,
        correlation_id: Option<&str>,
        response_msg: MessageExt,
    ) -> Option<std::sync::Arc<RequestResponseFuture>> {
        let future = self.remove_request(correlation_id?)?;
        future.put_response_message(Some(response_msg));
        // 对齐 Java：成功路径也走 executeRequestCallback，让「只回调一次」的守卫生效；
        // 同步调用方（callback 为空）靠 put_response_message 唤醒。
        future.execute_request_callback();
        Some(future)
    }

    pub fn len(&self) -> usize {
        self.request_future_table.read().ok().map(|t| t.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// 进程内单例（对齐 Java 的 `INSTANCE` / Python 的 `REQUEST_FUTURE_HOLDER`）。
/// 应答方与请求方在同一进程时也共用它。
pub fn request_future_holder() -> &'static RequestFutureHolder {
    static HOLDER: OnceLock<RequestFutureHolder> = OnceLock::new();
    HOLDER.get_or_init(RequestFutureHolder::new)
}

// ---------------------------------------------------------------- 报文工具

/// 上一个 correlationId 的随机源状态（无 `rand` 依赖，与 `trace_context` 同一套做法）。
static ID_SEED: AtomicU64 = AtomicU64::new(0);

/// splitmix64 的黄金比例增量（`0x9E3779B97F4A7C15`，只对 u64 有意义）。
const SPLITMIX_DELTA: u64 = 0x9E37_79B9_7F4A_7C15;

/// 对应 Java `CorrelationIdUtil.createCorrelationId`（随机 UUID 字符串）。
///
/// 本 crate 不引 `uuid`/`rand`，用 splitmix64(nanos ^ pid ^ 自增) 造 128 bit，
/// 再按 v4 的 version/variant 位排成标准 8-4-4-4-12 形态，长度与字符集和 UUID 一致。
pub fn create_correlation_id() -> String {
    let base = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        ^ (util_all::get_pid() as u64) << 16
        ^ ID_SEED.fetch_add(1, Ordering::Relaxed);
    // 两个不同种子的 splitmix64 输出拼成 128 bit（hi 在前，即 hex 的第 0..16 位）。
    let bits = ((splitmix64(base) as u128) << 64) | splitmix64(base.wrapping_add(SPLITMIX_DELTA)) as u128;

    // 32 个 hex 字符里第 12 个（= bit 79..76）是 version，第 16 个的高 2 bit（= bit 63..62）
    // 是 variant；写成 v4 形态后长度与字符集跟 Java 的 UUID 完全一致。
    let bits = (bits & !(0xFu128 << 76)) | (4u128 << 76);
    let bits = (bits & !(0b11u128 << 62)) | (0b10u128 << 62);

    let hex = format!("{bits:032x}");
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(SPLITMIX_DELTA);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// 对应 Java `MessageUtil.createReplyMessage`：由请求消息派生出应答消息。
///
/// `CLUSTER` 属性由 **broker** 在投递时写入（`SendMessageProcessor`），拿不到就说明
/// 这条消息不是经 broker 转发过来的（或 topic 配得不对），与 Java 一样直接报错，
/// 而不是造一条投不出去的应答。
pub fn create_reply_message(request_message: &Message, body: &[u8]) -> Result<Message> {
    let cluster = request_message.get_property(PROPERTY_CLUSTER).unwrap_or_default();
    if cluster.is_empty() {
        crate::bail!(
            "create reply message fail, requestMessage error, property[{}] is null.",
            PROPERTY_CLUSTER
        );
    }
    let reply_topic = MixAll::get_reply_topic(cluster);
    let mut reply = Message::new(&reply_topic, Some(body));
    for name in [
        PROPERTY_MESSAGE_TYPE,
        PROPERTY_CORRELATION_ID,
        PROPERTY_MESSAGE_REPLY_TO_CLIENT,
        PROPERTY_MESSAGE_TTL,
    ] {
        // Python 用 get_property 原样带回；请求消息缺某项时不写该属性（不给 "None"）。
        if let Some(value) = request_message.get_property(name) {
            reply.put_property(name, value);
        }
    }
    reply.put_property(PROPERTY_MESSAGE_TYPE, MixAll::REPLY_MESSAGE_FLAG);
    Ok(reply)
}

/// `MSG_TYPE == "reply"` 的发送要走 `SEND_REPLY_MESSAGE_V2(325)`。
///
/// 大小写敏感：Java 是 `equals("reply")`。
pub fn is_reply_message(msg: &Message) -> bool {
    msg.get_property(PROPERTY_MESSAGE_TYPE) == Some(MixAll::REPLY_MESSAGE_FLAG)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_msg() -> Message {
        let mut m = Message::new("RRUnitTopic", Some(b"ping"));
        m.put_property(PROPERTY_CLUSTER, "DefaultCluster");
        m.put_property(PROPERTY_CORRELATION_ID, "corr-1");
        m.put_property(PROPERTY_MESSAGE_REPLY_TO_CLIENT, "10.0.0.1@pg#123");
        m.put_property(PROPERTY_MESSAGE_TTL, "3000");
        m
    }

    // ---------------- 应答消息构造 ----------------

    #[test]
    fn create_reply_message_matches_java_shape() {
        let reply = create_reply_message(&request_msg(), b"pong").unwrap();
        // topic 必须是 <cluster>_REPLY_TOPIC（Java MixAll.getReplyTopic）
        assert_eq!(reply.get_topic(), "DefaultCluster_REPLY_TOPIC");
        assert_eq!(reply.get_body(), b"pong");
        // 四个属性一个都不能少，且 CORRELATION_ID/REPLY_TO_CLIENT/TTL 是原样带回
        assert_eq!(reply.get_property(PROPERTY_MESSAGE_TYPE), Some("reply"));
        assert_eq!(reply.get_property(PROPERTY_CORRELATION_ID), Some("corr-1"));
        assert_eq!(
            reply.get_property(PROPERTY_MESSAGE_REPLY_TO_CLIENT),
            Some("10.0.0.1@pg#123")
        );
        assert_eq!(reply.get_property(PROPERTY_MESSAGE_TTL), Some("3000"));
    }

    #[test]
    fn create_reply_message_requires_cluster() {
        let m = Message::new("RRUnitTopic", Some(b"ping"));
        let err = create_reply_message(&m, b"pong").unwrap_err();
        assert!(err.to_string().contains("CLUSTER"), "got {err}");
    }

    #[test]
    fn is_reply_message_flag_is_case_sensitive() {
        let reply = create_reply_message(&request_msg(), b"pong").unwrap();
        assert!(is_reply_message(&reply));
        assert!(!is_reply_message(&Message::new("T", Some(b"x"))));
        let mut other = Message::new("T", Some(b"x"));
        other.put_property(PROPERTY_MESSAGE_TYPE, "Reply");
        assert!(!is_reply_message(&other));
    }

    #[test]
    fn reply_topic_is_a_postfix_not_a_prefix() {
        // 别把 Request-Reply 的 <cluster>_REPLY_TOPIC 与老的控制台前缀 %REPLY% 搞混
        assert_eq!(MixAll::REPLY_TOPIC_POSTFIX, "REPLY_TOPIC");
        assert_eq!(MixAll::REPLY_MESSAGE_FLAG, "reply");
        assert_eq!(MixAll::get_reply_topic("DefaultCluster"), "DefaultCluster_REPLY_TOPIC");
        assert_ne!(MixAll::get_reply_topic("DefaultCluster"), "%REPLY%DefaultCluster");
    }

    // ---------------- 等待槽 ----------------

    #[test]
    fn correlation_id_looks_like_a_uuid_and_is_unique() {
        let ids: std::collections::HashSet<String> =
            (0..50).map(|_| create_correlation_id()).collect();
        assert_eq!(ids.len(), 50);
        for id in ids {
            assert_eq!(id.len(), 36, "got {id}");
            assert_eq!(id.matches('-').count(), 4);
            assert!(
                id.chars()
                    .all(|c| c == '-' || c.is_ascii_digit() || ('a'..='f').contains(&c)),
                "got {id}"
            );
            assert_eq!(&id[14..15], "4", "version 位");
            assert!(
                matches!(&id[19..20], "8" | "9" | "a" | "b"),
                "variant 位: got {id}"
            );
        }
    }

    #[tokio::test]
    async fn wait_times_out_and_is_timeout() {
        // 等待预算必须明显大于 future 的超时预算：is_timeout 用的是严格大于（同 Java），
        // 而定时器可能比截止时刻早一丁点触发，两者相等时忙机上这条断言会抖。
        let f = RequestResponseFuture::new("c1", 20);
        assert!(f.wait_response_message(200).await.is_none());
        assert!(f.is_timeout());
    }

    #[tokio::test]
    async fn put_response_wakes_the_waiter() {
        let f = std::sync::Arc::new(RequestResponseFuture::new("c1", 5000));
        let filler = f.clone();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let mut msg = MessageExt::new();
            msg.set_topic("RRUnitTopic");
            msg.set_body(Some(b"pong"));
            filler.put_response_message(Some(msg));
        });
        let got = f.wait_response_message(2000).await.expect("必须被唤醒");
        assert_eq!(got.get_body(), b"pong");
        assert!(!f.is_timeout());
        handle.await.unwrap();
    }

    /// 应答先到、等待后起：sticky 语义（Python 的 `Event`）不允许丢唤醒。
    #[tokio::test]
    async fn response_before_wait_is_not_lost() {
        let f = RequestResponseFuture::new("c1", 1000);
        let mut msg = MessageExt::new();
        msg.set_body(Some(b"early"));
        f.put_response_message(Some(msg));
        let got = f.wait_response_message(10).await.expect("不能超时");
        assert_eq!(got.get_body(), b"early");
    }

    /// 记录回调触发次数（`Arc` 共享，回调后仍可断言）。
    struct Recorder {
        successes: std::sync::atomic::AtomicU32,
        failures: std::sync::atomic::AtomicU32,
        last_error: Mutex<Option<String>>,
    }

    impl Recorder {
        fn new() -> Recorder {
            Recorder {
                successes: std::sync::atomic::AtomicU32::new(0),
                failures: std::sync::atomic::AtomicU32::new(0),
                last_error: Mutex::new(None),
            }
        }

        fn successes(&self) -> u32 {
            self.successes.load(Ordering::SeqCst)
        }

        fn failures(&self) -> u32 {
            self.failures.load(Ordering::SeqCst)
        }
    }

    impl RequestCallback for Recorder {
        fn on_success(&self, _response: Option<MessageExt>) {
            self.successes.fetch_add(1, Ordering::SeqCst);
        }
        fn on_exception(&self, cause: &Error) {
            self.failures.fetch_add(1, Ordering::SeqCst);
            *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(cause.to_string());
        }
    }

    #[test]
    fn callback_fires_at_most_once() {
        let recorder = std::sync::Arc::new(Recorder::new());
        let f = RequestResponseFuture::with_callback("c1", 1000, Some(recorder.clone()));
        assert_eq!(recorder.successes(), 0, "没投递前不能回调");
        f.execute_request_callback();
        // 与 Java 一致：sendRequestOK 仍为 true、无 cause，但无应答也回调一次（success + 空消息）
        assert_eq!(recorder.successes(), 1);
        let mut msg = MessageExt::new();
        msg.set_body(Some(b"pong"));
        f.put_response_message(Some(msg));
        f.execute_request_callback();
        f.execute_request_callback();
        assert_eq!(recorder.successes(), 1, "回调只允许一次");
        assert_eq!(recorder.failures(), 0);
    }

    #[test]
    fn set_failed_reports_exception_once() {
        let recorder = std::sync::Arc::new(Recorder::new());
        let f = RequestResponseFuture::with_callback("c1", 1000, Some(recorder.clone()));
        f.set_failed(Error::client("send failed"));
        assert!(!f.is_send_request_ok());
        f.execute_request_callback();
        // 第二次不得再回调
        f.execute_request_callback();
        assert_eq!(recorder.failures(), 1);
        assert_eq!(recorder.successes(), 0);
        let logged = recorder.last_error.lock().unwrap_or_else(|e| e.into_inner()).clone();
        assert!(logged.unwrap_or_default().contains("send failed"));
    }

    // ---------------- 等待槽表 ----------------

    #[test]
    fn holder_put_response_removes_the_entry() {
        // Java 用 remove 抢所有权：应答到达与超时清理只能有一个生效。
        let holder = RequestFutureHolder::new();
        let f = std::sync::Arc::new(RequestResponseFuture::new("c1", 1000));
        holder.put_request("c1", f.clone());
        assert!(std::sync::Arc::ptr_eq(&holder.get_request("c1").unwrap(), &f));

        let mut msg = MessageExt::new();
        msg.set_body(Some(b"pong"));
        assert!(std::sync::Arc::ptr_eq(&holder.put_response(Some("c1"), msg.clone()).unwrap(), &f));
        assert!(holder.get_request("c1").is_none());
        // 重复应答：摘不到了，只记日志（Python 同语义）
        assert!(holder.put_response(Some("c1"), msg).is_none());
    }

    #[test]
    fn holder_remove_is_idempotent_and_missing_corr_is_none() {
        let holder = RequestFutureHolder::new();
        assert!(holder.is_empty());
        holder.put_request("c1", std::sync::Arc::new(RequestResponseFuture::new("c1", 1000)));
        assert_eq!(holder.len(), 1);
        assert!(holder.remove_request("c1").is_some());
        assert!(holder.remove_request("c1").is_none());
        let mut msg = MessageExt::new();
        msg.set_body(Some(b"x"));
        assert!(holder.put_response(None, msg.clone()).is_none());
        assert!(holder.put_response(Some("nope"), msg).is_none());
    }

    #[test]
    fn global_holder_is_a_process_singleton() {
        assert!(std::ptr::eq(request_future_holder(), request_future_holder()));
    }
}
