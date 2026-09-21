//! 客户端钩子（对应 Java `org.apache.rocketmq.client.hook` 包，
//! 逐条对齐 `python/rocketmq/client/hook.py`）。
//!
//! Java 侧钩子是「业务无关的切面」：生产者在 `sendKernelImpl` 前后各调一次
//! [`SendMessageHook`]，消费者在投递 listener 前后各调一次 [`ConsumeMessageHook`]。
//! 消息轨迹（`client.trace.hook.*`）正是建在这两个接口上的。
//!
//! 设计约定（与 Java / Python 一致）：
//! - 钩子「抛出的异常」**必须被吞掉并记 warn**（Java `DefaultMQProducerImpl:1159/1172`），
//!   绝不能因为轨迹出错影响正常收发。Rust 没有异常，所以钩子方法返回 [`Result`]，
//!   吞异常的职责落在本模块的 `execute_*` 系列函数上（对应 Python producer/consumer 里同名方法），
//!   调用方**不要**自己用 `?` 传播，否则破坏语义；
//! - **唯二的例外**：[`CheckForbiddenHook::check_forbidden`]（发送前拦截，Java 签名就是
//!   `throws MQClientException`）——它的异常要向上传播，见 [`execute_check_forbidden_hook`]；
//! - [`SendMessageContext::mq_trace_context`] 是钩子自己的私有状态：before 写入、after 取出，
//!   中间不允许别的钩子依赖它的具体类型（Java 是 `Object`，这里用 `Arc<dyn Any>`，
//!   取值用 [`any_downcast`]）。
//!
//! 与 Python 的**有意差异**（都不影响可观测行为）：
//! 1. `SendMessageContext.producer`（Java 回指 `DefaultMQProducerImpl`）未移植：
//!    Python 侧该字段**只写不读**（`producer.py:365`），而 Rust 里回指会形成 `Arc` 循环，
//!    钩子真正用到的是 `producer_group`；
//! 2. Python 抽象方法的 `raise NotImplementedError` 在 Rust 里没有等价物，于是
//!    「不关心的一侧」给默认空实现（同 [`crate::remoting::rpchook::RPCHook::do_after_response`]
//!    的口径），`hook_name` 保持必填；
//! 3. `access_channel` 存名字串（`"LOCAL"` / `"CLOUD"`）而不是枚举：Python 的
//!    `AccessChannel` 定义在 `client/trace.py`，不属于本文件，避免两处重复定义；
//! 4. Java `ConsumeMessageContext.namespace` 未移植（Python 的 `__init__` 里没有该字段）。

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::common::message::{Message, MessageExt, MessageQueue};
use crate::common::message_type::MessageType;
use crate::error::{Error, Result};
use crate::{rmq_error, rmq_warn};

use super::result::{LocalTransactionState, SendResult};

/// Java 的 `Object` 字段（`mqTraceContext` / `arg`）在 Rust 里的载体。
pub type AnyHolder = Arc<dyn Any + Send + Sync>;

/// 把 [`AnyHolder`] 取回具体类型（等价于 Java 的强制转型）。
///
/// 类型不匹配时返回 `None` 而不是 panic —— 钩子之间互不相干，
/// 一个钩子塞错类型不该让整个发送链路挂掉。
pub fn any_downcast<T: Any + Send + Sync>(holder: &Option<AnyHolder>) -> Option<&T> {
    holder.as_ref().and_then(|value| value.downcast_ref::<T>())
}

/// 对应 Java `org.apache.rocketmq.client.impl.CommunicationMode`。
///
/// Python 侧是三个字符串常量（`CommunicationMode.SYNC == "SYNC"`），Java 是枚举；
/// 这里用枚举 + [`Display`](fmt::Display) 输出同名串，两边都对得上。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CommunicationMode {
    /// `SYNC`
    #[default]
    Sync,
    /// `ASYNC`
    Async,
    /// `ONEWAY`
    Oneway,
}

impl CommunicationMode {
    /// Python 常量的值（也是 Java `Enum#name()`）。
    pub fn name(self) -> &'static str {
        match self {
            CommunicationMode::Sync => "SYNC",
            CommunicationMode::Async => "ASYNC",
            CommunicationMode::Oneway => "ONEWAY",
        }
    }

    /// 按名字解析（对应 Java `CommunicationMode.valueOf`）。
    /// 未知值回落 `SYNC`：与 `result.rs` 里 `from_code` 的口径一致，
    /// 认不出来的新枚举值不该让老客户端报错。
    pub fn from_name(name: &str) -> CommunicationMode {
        match name {
            "ASYNC" => CommunicationMode::Async,
            "ONEWAY" => CommunicationMode::Oneway,
            _ => CommunicationMode::Sync,
        }
    }
}

impl fmt::Display for CommunicationMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

// ------------------------------------------------------------ SendMessageHook

/// 对应 Java `org.apache.rocketmq.client.hook.SendMessageContext`。
#[derive(Default)]
pub struct SendMessageContext {
    /// 对应 Python `producer_group`（Java 的 `DefaultMQProducerImpl` 回指见模块头说明 1）。
    pub producer_group: String,
    pub message: Option<Message>,
    pub mq: Option<MessageQueue>,
    pub broker_addr: String,
    pub born_host: String,
    pub communication_mode: Option<CommunicationMode>,
    pub send_result: Option<SendResult>,
    /// 对应 Python `exception`：发送失败时由调用方写入，`after` 钩子里读它判成败。
    pub exception: Option<Error>,
    /// 钩子私有状态（Java `Object mqTraceContext`），见模块头约定。
    pub mq_trace_context: Option<AnyHolder>,
    /// Java `Map<String, String> props`；Python 默认 `None`（钩子自己决定要不要建）。
    pub props: Option<HashMap<String, String>>,
    pub msg_type: MessageType,
    pub namespace: String,
}

impl fmt::Debug for SendMessageContext {
    /// `Arc<dyn Any>` 无法 derive(Debug)，这里只打印「有没有值」，不暴露钩子私有状态。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendMessageContext")
            .field("producer_group", &self.producer_group)
            .field("message", &self.message)
            .field("mq", &self.mq)
            .field("broker_addr", &self.broker_addr)
            .field("born_host", &self.born_host)
            .field("communication_mode", &self.communication_mode)
            .field("send_result", &self.send_result)
            .field("exception", &self.exception)
            .field("mq_trace_context", &holder_state(&self.mq_trace_context))
            .field("props", &self.props)
            .field("msg_type", &self.msg_type)
            .field("namespace", &self.namespace)
            .finish()
    }
}

/// 对应 Java `org.apache.rocketmq.client.hook.SendMessageHook`。
pub trait SendMessageHook: Send + Sync {
    /// Java `hookName()`：日志与排错用，必填（Python 里是抽象方法）。
    fn hook_name(&self) -> &str;

    /// Java `sendMessageBefore`。`Err` 由 [`execute_send_message_hook_before`] 吞掉。
    fn send_message_before(&self, _context: &mut SendMessageContext) -> Result<()> {
        Ok(())
    }

    /// Java `sendMessageAfter`：成功时 `context.send_result` 有值，失败时 `context.exception` 有值。
    fn send_message_after(&self, _context: &mut SendMessageContext) -> Result<()> {
        Ok(())
    }
}

// ------------------------------------------------------ ConsumeMessageHook

/// 对应 Java `org.apache.rocketmq.client.hook.ConsumeMessageContext`。
#[derive(Clone, Default)]
pub struct ConsumeMessageContext {
    pub consumer_group: String,
    pub msg_list: Vec<MessageExt>,
    pub mq: Option<MessageQueue>,
    /// Python `__init__` 的缺省是 `True`；Java 与投递路径（`consumer.py:645`
    /// `_build_consume_hook_context`）会显式改成 `False`，两者别搞混。
    pub success: bool,
    /// 消费状态的名字串（Python `str(status)`，如 `"CONSUME_SUCCESS"`）。
    pub status: Option<String>,
    pub mq_trace_context: Option<AnyHolder>,
    pub props: Option<HashMap<String, String>>,
    /// `"LOCAL"` / `"CLOUD"`，见模块头差异说明 3。
    pub access_channel: Option<String>,
}

impl ConsumeMessageContext {
    /// 对应 Python `ConsumeMessageContext(consumer_group, msg_list, mq)`：
    /// `success` 缺省 `True`；`msg_list` 为 `None` 或空时得到空列表
    /// （Python 的 `list(msg_list) if msg_list else []` 两种入参结果相同）。
    pub fn new(
        consumer_group: &str,
        msg_list: Option<Vec<MessageExt>>,
        mq: Option<MessageQueue>,
    ) -> ConsumeMessageContext {
        ConsumeMessageContext {
            consumer_group: consumer_group.to_string(),
            msg_list: msg_list.filter(|list| !list.is_empty()).unwrap_or_default(),
            mq,
            success: true,
            ..Default::default()
        }
    }
}

impl fmt::Debug for ConsumeMessageContext {
    /// `msg_list` 只打长度（一批消息的完整 repr 太吵），私有轨迹状态只打「有没有」。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumeMessageContext")
            .field("consumer_group", &self.consumer_group)
            .field("msg_list", &self.msg_list.len())
            .field("mq", &self.mq)
            .field("success", &self.success)
            .field("status", &self.status)
            .field("mq_trace_context", &holder_state(&self.mq_trace_context))
            .field("props", &self.props)
            .field("access_channel", &self.access_channel)
            .finish()
    }
}

/// 对应 Java `org.apache.rocketmq.client.hook.ConsumeMessageHook`。
pub trait ConsumeMessageHook: Send + Sync {
    fn hook_name(&self) -> &str;

    /// Java `consumeMessageBefore`。
    fn consume_message_before(&self, _context: &mut ConsumeMessageContext) -> Result<()> {
        Ok(())
    }

    /// Java `consumeMessageAfter`。
    fn consume_message_after(&self, _context: &mut ConsumeMessageContext) -> Result<()> {
        Ok(())
    }
}

// ------------------------------------------------------ EndTransactionHook

/// 对应 Java `org.apache.rocketmq.client.hook.EndTransactionContext`。
#[derive(Debug, Clone, Default)]
pub struct EndTransactionContext {
    pub producer_group: String,
    pub message: Option<Message>,
    pub broker_addr: String,
    pub msg_id: Option<String>,
    pub transaction_id: Option<String>,
    pub transaction_state: Option<LocalTransactionState>,
    /// `true` 表示这次收尾来自 broker 的事务回查，而不是客户端主动提交（Java 同名标志）。
    pub from_transaction_check: bool,
    pub namespace: String,
}

/// 对应 Java `org.apache.rocketmq.client.hook.EndTransactionHook`。
pub trait EndTransactionHook: Send + Sync {
    fn hook_name(&self) -> &str;

    /// Java `endTransaction`。`Err` 由 [`execute_end_transaction_hook`] 吞掉。
    fn end_transaction(&self, _context: &mut EndTransactionContext) -> Result<()> {
        Ok(())
    }
}

// ------------------------------------------------------- CheckForbiddenHook

/// 对应 Java `org.apache.rocketmq.client.hook.CheckForbiddenContext`。
///
/// 发送前拦截钩子的上下文。与 [`SendMessageContext`] 的关键差别：**没有 sendResult**
/// （此刻还没发），带上 `arg`（`send(msg, selector, arg)` 里的业务参数）。
#[derive(Default)]
pub struct CheckForbiddenContext {
    pub name_srv_addr: String,
    pub group: String,
    pub message: Option<Message>,
    pub mq: Option<MessageQueue>,
    pub broker_addr: String,
    pub communication_mode: Option<CommunicationMode>,
    pub send_result: Option<SendResult>,
    pub exception: Option<Error>,
    /// Java `Object arg`。
    pub arg: Option<AnyHolder>,
    /// Java `CheckForbiddenContext#setUnitMode(tc.isUnitMode())`
    /// （`DefaultMQProducerImpl:964`），来自门面的 `ClientConfig#unitMode`。
    pub unit_mode: bool,
}

impl fmt::Debug for CheckForbiddenContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CheckForbiddenContext")
            .field("name_srv_addr", &self.name_srv_addr)
            .field("group", &self.group)
            .field("message", &self.message)
            .field("mq", &self.mq)
            .field("broker_addr", &self.broker_addr)
            .field("communication_mode", &self.communication_mode)
            .field("send_result", &self.send_result)
            .field("exception", &self.exception)
            .field("arg", &holder_state(&self.arg))
            .field("unit_mode", &self.unit_mode)
            .finish()
    }
}

/// 对应 Java `org.apache.rocketmq.client.hook.CheckForbiddenHook`。
///
/// ⚠ 与 Send/Consume 钩子**相反**：`check_forbidden` 返回的 `Err` **不会被吞掉**
/// （Java 签名就是 `throws MQClientException`），而是沿发送重试链向上传播 ——
/// 这正是「拦截」能力的实现方式。
pub trait CheckForbiddenHook: Send + Sync {
    fn hook_name(&self) -> &str;

    /// Java `checkForbidden`，`Err` 会传播给发送方。
    fn check_forbidden(&self, _context: &mut CheckForbiddenContext) -> Result<()> {
        Ok(())
    }
}

// --------------------------------------------------------- FilterMessageHook

/// 对应 Java `org.apache.rocketmq.client.hook.FilterMessageContext`。
///
/// `msg_list` 是**可变的**：钩子把它替换/裁剪掉的消息会被客户端直接丢弃
/// （拉取路径 = 静默跳过；POP 路径 = 立刻 ack）。
#[derive(Clone, Default)]
pub struct FilterMessageContext {
    pub consumer_group: String,
    pub msg_list: Vec<MessageExt>,
    pub mq: Option<MessageQueue>,
    /// Java `Object arg`（`pull(mq, sub, ..., arg)` 的业务参数）。
    pub arg: Option<AnyHolder>,
    pub unit_mode: bool,
}

impl FilterMessageContext {
    /// 对应 Python `FilterMessageContext(consumer_group, msg_list, mq)`：
    /// `arg` 为 None；`unit_mode` 取消费者的配置（Java 是
    /// `DefaultMQPushConsumerImpl:640` 的 `context.setUnitMode(...)`，Python 早期
    /// 固定 False，这里跟 Java 走）。
    pub fn new(
        consumer_group: &str,
        msg_list: Option<Vec<MessageExt>>,
        mq: Option<MessageQueue>,
    ) -> FilterMessageContext {
        FilterMessageContext {
            consumer_group: consumer_group.to_string(),
            msg_list: msg_list.filter(|list| !list.is_empty()).unwrap_or_default(),
            mq,
            arg: None,
            unit_mode: false,
        }
    }
}

impl fmt::Debug for FilterMessageContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilterMessageContext")
            .field("consumer_group", &self.consumer_group)
            .field("msg_list", &self.msg_list.len())
            .field("mq", &self.mq)
            .field("arg", &holder_state(&self.arg))
            .field("unit_mode", &self.unit_mode)
            .finish()
    }
}

/// 对应 Java `org.apache.rocketmq.client.hook.FilterMessageHook`。
pub trait FilterMessageHook: Send + Sync {
    fn hook_name(&self) -> &str;

    /// Java `filterMessage`：`Err` 由 [`execute_filter_hooks`] 吞掉并记 **error**
    /// （Java `PullAPIWrapper.executeHook:171-178`；级别与 send/consume 的 warn 不同）。
    fn filter_message(&self, _context: &mut FilterMessageContext) -> Result<()> {
        Ok(())
    }
}

// -------------------------------------------------------------- 钩子注册表

/// 对应 Python producer / consumer 上的 `send_message_hook_list` 等注册表。
///
/// Python 用 `list.append` + 顺序遍历，注册与触发可能跨线程，所以这里是
/// `RwLock<Vec<Arc<H>>>`；[`HookList::hooks`] 返回**快照**，
/// 语义等于 Python 遍历当时那张列表。
pub struct HookList<H: ?Sized> {
    inner: RwLock<Vec<Arc<H>>>,
}

impl<H: ?Sized> HookList<H> {
    /// 空表（Python `self.xxx_hook_list = []`）。
    pub fn new() -> Self {
        HookList { inner: RwLock::new(Vec::new()) }
    }

    /// 对应 Python `register_*_hook`：追加到末尾，触发顺序 = 注册顺序。
    /// Python 的 `if hook is not None` 判空在 Rust 由 `Arc` 保证非空，无需重复。
    pub fn register(&self, hook: Arc<H>) {
        write_guard(&self.inner).push(hook);
    }

    /// 对应 Python `has_*_hook`。
    pub fn has_hooks(&self) -> bool {
        !read_guard(&self.inner).is_empty()
    }

    pub fn len(&self) -> usize {
        read_guard(&self.inner).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 按注册顺序的快照。
    pub fn hooks(&self) -> Vec<Arc<H>> {
        read_guard(&self.inner).clone()
    }
}

impl<H: ?Sized> Default for HookList<H> {
    fn default() -> Self {
        HookList::new()
    }
}

impl<H: ?Sized> fmt::Debug for HookList<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HookList").field("len", &self.len()).finish()
    }
}

/// `send_message_hook_list`（Python `DefaultMQProducer`）。
pub type SendMessageHookList = HookList<dyn SendMessageHook>;
/// `consume_message_hook_list`（Python `DefaultMQPushConsumer`）。
pub type ConsumeMessageHookList = HookList<dyn ConsumeMessageHook>;
/// `end_transaction_hook_list`。
pub type EndTransactionHookList = HookList<dyn EndTransactionHook>;
/// `check_forbidden_hook_list`。
pub type CheckForbiddenHookList = HookList<dyn CheckForbiddenHook>;
/// `filter_message_hook_list`。
pub type FilterMessageHookList = HookList<dyn FilterMessageHook>;

/// `DefaultMQProducerImpl.executeSendMessageHookBefore`：异常吞掉并记 warn。
pub fn execute_send_message_hook_before(
    hooks: &SendMessageHookList,
    context: &mut SendMessageContext,
) {
    for hook in hooks.hooks() {
        if let Err(e) = hook.send_message_before(context) {
            rmq_warn!("failed to executeSendMessageHookBefore: {}", e);
        }
    }
}

/// `DefaultMQProducerImpl.executeSendMessageHookAfter`：异常吞掉并记 warn。
pub fn execute_send_message_hook_after(
    hooks: &SendMessageHookList,
    context: &mut SendMessageContext,
) {
    for hook in hooks.hooks() {
        if let Err(e) = hook.send_message_after(context) {
            rmq_warn!("failed to executeSendMessageHookAfter: {}", e);
        }
    }
}

/// `DefaultMQProducerImpl.executeCheckForbiddenHook`。
///
/// ⚠ 与上面两个相反：**不吞异常**，第一个拒绝就向上抛
/// （Python 同名方法就是裸 `for hook: hook.check_forbidden(ctx)`）。
pub fn execute_check_forbidden_hook(
    hooks: &CheckForbiddenHookList,
    context: &mut CheckForbiddenContext,
) -> Result<()> {
    if !hooks.has_hooks() {
        return Ok(());
    }
    for hook in hooks.hooks() {
        hook.check_forbidden(context)?;
    }
    Ok(())
}

/// `DefaultMQProducerImpl.executeEndTransactionHook`：异常吞掉并记 warn。
pub fn execute_end_transaction_hook(
    hooks: &EndTransactionHookList,
    context: &mut EndTransactionContext,
) {
    for hook in hooks.hooks() {
        if let Err(e) = hook.end_transaction(context) {
            rmq_warn!("failed to executeEndTransactionHook: {}", e);
        }
    }
}

/// `DefaultMQPushConsumerImpl.executeConsumeHookBefore`：异常吞掉并记 warn。
pub fn execute_consume_hook_before(hooks: &ConsumeMessageHookList, context: &mut ConsumeMessageContext) {
    for hook in hooks.hooks() {
        if let Err(e) = hook.consume_message_before(context) {
            rmq_warn!("consumeMessageHook executeHookBefore exception: {}", e);
        }
    }
}

/// `DefaultMQPushConsumerImpl.executeConsumeHookAfter`：异常吞掉并记 warn。
pub fn execute_consume_hook_after(hooks: &ConsumeMessageHookList, context: &mut ConsumeMessageContext) {
    for hook in hooks.hooks() {
        if let Err(e) = hook.consume_message_after(context) {
            rmq_warn!("consumeMessageHook executeHookAfter exception: {}", e);
        }
    }
}

/// `consumer.execute_filter_hooks`：异常吞掉并记 **error**（Java `PullAPIWrapper.executeHook`）。
pub fn execute_filter_hooks(hooks: &FilterMessageHookList, context: &mut FilterMessageContext) {
    for hook in hooks.hooks() {
        if let Err(e) = hook.filter_message(context) {
            // Python 还要用 `_safe_hook_name` 兜住 hook_name() 自己抛错；
            // Rust 的 hook_name 不可失败，直接带上。
            rmq_error!("execute hook error. hookName={}: {}", hook.hook_name(), e);
        }
    }
}

/// `Option<AnyHolder>` 的 Debug 口径：只暴露「有没有值」，不依赖具体类型。
fn holder_state(holder: &Option<AnyHolder>) -> &'static str {
    if holder.is_some() {
        "set"
    } else {
        "none"
    }
}

/// 锁毒化时照样读写（钩子表不该因为别的线程 panic 而整体不可用）。
fn read_guard<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_guard<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// 记录调用顺序的假钩子，等价于 Python 测试里的 `_Recording*` / `_Boom*` 类。
    #[derive(Default)]
    struct Recorder {
        calls: Mutex<Vec<String>>,
    }

    impl Recorder {
        fn record(&self, line: &str) {
            match self.calls.lock() {
                Ok(mut guard) => guard.push(line.to_string()),
                Err(poisoned) => poisoned.into_inner().push(line.to_string()),
            }
        }

        fn snapshot(&self) -> Vec<String> {
            match self.calls.lock() {
                Ok(guard) => guard.clone(),
                Err(poisoned) => poisoned.into_inner().clone(),
            }
        }
    }

    struct SendHook {
        recorder: Arc<Recorder>,
        boom: bool,
    }

    impl SendMessageHook for SendHook {
        fn hook_name(&self) -> &str {
            "send-hook"
        }

        fn send_message_before(&self, context: &mut SendMessageContext) -> Result<()> {
            self.recorder.record("before");
            context.mq_trace_context = Some(Arc::new("ctx-from-before"));
            if self.boom {
                crate::bail!("hook exploded");
            }
            Ok(())
        }

        fn send_message_after(&self, context: &mut SendMessageContext) -> Result<()> {
            let seen = any_downcast::<&str>(&context.mq_trace_context).copied().unwrap_or("");
            self.recorder.record(&format!("after:{}", seen));
            Ok(())
        }
    }

    struct ForbiddenHook {
        recorder: Arc<Recorder>,
        forbid: bool,
    }

    impl CheckForbiddenHook for ForbiddenHook {
        fn hook_name(&self) -> &str {
            "forbid"
        }

        fn check_forbidden(&self, context: &mut CheckForbiddenContext) -> Result<()> {
            self.recorder.record("check");
            if self.forbid {
                crate::bail!("forbidden by test hook");
            }
            let _ = context;
            Ok(())
        }
    }

    struct ConsumeHook {
        recorder: Arc<Recorder>,
        boom: bool,
    }

    impl ConsumeMessageHook for ConsumeHook {
        fn hook_name(&self) -> &str {
            "consume-hook"
        }

        fn consume_message_before(&self, _context: &mut ConsumeMessageContext) -> Result<()> {
            self.recorder.record("consume-before");
            if self.boom {
                crate::bail!("hook exploded");
            }
            Ok(())
        }

        fn consume_message_after(&self, _context: &mut ConsumeMessageContext) -> Result<()> {
            self.recorder.record("consume-after");
            Ok(())
        }
    }

    struct EndHook {
        recorder: Arc<Recorder>,
    }

    impl EndTransactionHook for EndHook {
        fn hook_name(&self) -> &str {
            "end-tx"
        }

        fn end_transaction(&self, _context: &mut EndTransactionContext) -> Result<()> {
            self.recorder.record("end");
            crate::bail!("hook exploded")
        }
    }

    struct DropFilter {
        recorder: Arc<Recorder>,
        drop_body: Vec<u8>,
    }

    impl FilterMessageHook for DropFilter {
        fn hook_name(&self) -> &str {
            "drop"
        }

        fn filter_message(&self, context: &mut FilterMessageContext) -> Result<()> {
            self.recorder.record("filter");
            context
                .msg_list
                .retain(|msg| msg.get_body() != self.drop_body.as_slice());
            Ok(())
        }
    }

    struct BoomFilter;

    impl FilterMessageHook for BoomFilter {
        fn hook_name(&self) -> &str {
            "boom"
        }

        fn filter_message(&self, _context: &mut FilterMessageContext) -> Result<()> {
            crate::bail!("hook exploded")
        }
    }

    fn msg_ext(body: &[u8]) -> MessageExt {
        let mut msg = MessageExt::new();
        msg.set_topic("TopicTest");
        msg.set_body(Some(body));
        msg
    }

    #[test]
    fn communication_mode_names_match_python_constants() {
        assert_eq!(CommunicationMode::Sync.name(), "SYNC");
        assert_eq!(CommunicationMode::Async.to_string(), "ASYNC");
        assert_eq!(CommunicationMode::Oneway.to_string(), "ONEWAY");
        // 未知名字回落 SYNC（Python 没有 from_name，与 result.rs 的 from_code 口径一致）
        assert_eq!(CommunicationMode::from_name("NOPE"), CommunicationMode::Sync);
        assert_eq!(CommunicationMode::from_name(""), CommunicationMode::Sync);
        assert_eq!(CommunicationMode::from_name("ONEWAY"), CommunicationMode::Oneway);
        assert_eq!(CommunicationMode::default(), CommunicationMode::Sync);
    }

    #[test]
    fn send_message_context_defaults_match_python() {
        let context = SendMessageContext::default();
        assert_eq!(context.producer_group, "");
        assert!(context.message.is_none());
        assert!(context.mq.is_none());
        assert_eq!(context.broker_addr, "");
        assert_eq!(context.born_host, "");
        assert!(context.communication_mode.is_none());
        assert!(context.send_result.is_none());
        assert!(context.exception.is_none());
        assert!(context.mq_trace_context.is_none());
        assert!(context.props.is_none());
        assert_eq!(context.msg_type, MessageType::NormalMsg);
        assert_eq!(context.namespace, "");
    }

    #[test]
    fn check_forbidden_context_defaults_and_has_no_send_result() {
        let context = CheckForbiddenContext::default();
        assert_eq!(context.name_srv_addr, "");
        assert_eq!(context.group, "");
        assert_eq!(context.broker_addr, "");
        assert!(context.communication_mode.is_none());
        // 此刻还没发：与 SendMessageContext 的关键差别
        assert!(context.send_result.is_none());
        assert!(context.arg.is_none());
        assert!(!context.unit_mode);
    }

    #[test]
    fn end_transaction_context_defaults_match_python() {
        let context = EndTransactionContext::default();
        assert_eq!(context.producer_group, "");
        assert!(context.message.is_none());
        assert_eq!(context.broker_addr, "");
        assert!(context.msg_id.is_none());
        assert!(context.transaction_id.is_none());
        assert!(context.transaction_state.is_none());
        assert!(!context.from_transaction_check);
        assert_eq!(context.namespace, "");
    }

    #[test]
    fn consume_context_success_defaults_true_like_python_init() {
        // Python `ConsumeMessageContext()` 的 success 是 True；
        // 投递路径上 consumer.py 会显式改成 False（_build_consume_hook_context）。
        let context = ConsumeMessageContext::new("GID_test", None, None);
        assert!(context.success);
        assert_eq!(context.consumer_group, "GID_test");
        assert!(context.msg_list.is_empty());
        assert!(context.status.is_none());
        assert!(context.mq_trace_context.is_none());
        assert!(context.props.is_none());
        assert!(context.access_channel.is_none());

        let with_msgs = ConsumeMessageContext::new(
            "G",
            Some(vec![msg_ext(b"a")]),
            Some(MessageQueue::new("TopicTest", "broker-a", 0)),
        );
        assert_eq!(with_msgs.msg_list.len(), 1);
        // 空列表与 None 一样落到空表（Python `list(x) if x else []`）
        assert!(ConsumeMessageContext::new("G", Some(Vec::new()), None).msg_list.is_empty());
    }

    #[test]
    fn filter_context_copies_list_and_defaults_unit_mode_false() {
        let context = FilterMessageContext::new(
            "G",
            Some(vec![msg_ext(b"drop"), msg_ext(b"keep")]),
            Some(MessageQueue::new("TopicTest", "broker-a", 1)),
        );
        assert_eq!(context.msg_list.len(), 2);
        assert_eq!(context.consumer_group, "G");
        assert!(!context.unit_mode);
        assert!(context.arg.is_none());
        assert!(context.mq.is_some());
    }

    #[test]
    fn hook_list_registers_in_order_and_reports_emptiness() {
        let hooks = SendMessageHookList::new();
        assert!(!hooks.has_hooks());
        assert!(hooks.is_empty());
        let recorder = Arc::new(Recorder::default());
        hooks.register(Arc::new(SendHook { recorder: recorder.clone(), boom: false }));
        assert!(hooks.has_hooks());
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks.hooks()[0].hook_name(), "send-hook");
    }

    #[test]
    fn send_hook_error_is_swallowed_and_next_hook_still_runs() {
        let hooks = SendMessageHookList::new();
        let boom = Arc::new(Recorder::default());
        let fine = Arc::new(Recorder::default());
        hooks.register(Arc::new(SendHook { recorder: boom.clone(), boom: true }));
        hooks.register(Arc::new(SendHook { recorder: fine.clone(), boom: false }));

        let mut context = SendMessageContext::default();
        execute_send_message_hook_before(&hooks, &mut context);
        // 第一个钩子的错误被吞（warn），第二个照常执行
        assert_eq!(boom.snapshot(), vec!["before".to_string()]);
        assert_eq!(fine.snapshot(), vec!["before".to_string()]);
        // before 写下的私有状态 after 能取回（Python 的 mq_trace_context 约定）
        execute_send_message_hook_after(&hooks, &mut context);
        assert_eq!(
            fine.snapshot(),
            vec!["before".to_string(), "after:ctx-from-before".to_string()]
        );
    }

    #[test]
    fn check_forbidden_error_propagates_and_blocks_later_hooks() {
        let hooks = CheckForbiddenHookList::new();
        let first = Arc::new(Recorder::default());
        let second = Arc::new(Recorder::default());
        hooks.register(Arc::new(ForbiddenHook { recorder: first.clone(), forbid: true }));
        hooks.register(Arc::new(ForbiddenHook { recorder: second.clone(), forbid: false }));

        let mut context = CheckForbiddenContext::default();
        let err = execute_check_forbidden_hook(&hooks, &mut context)
            .expect_err("test-only: 拦截异常必须向上抛");
        assert!(err.to_string().contains("forbidden by test hook"));
        assert_eq!(first.snapshot(), vec!["check".to_string()]);
        assert_eq!(second.snapshot(), Vec::<String>::new());
    }

    #[test]
    fn check_forbidden_passes_when_hook_allows() {
        let hooks = CheckForbiddenHookList::new();
        let recorder = Arc::new(Recorder::default());
        hooks.register(Arc::new(ForbiddenHook { recorder: recorder.clone(), forbid: false }));
        let mut context = CheckForbiddenContext {
            group: "GID_test".into(),
            name_srv_addr: "127.0.0.1:9876".into(),
            broker_addr: "127.0.0.1:10911".into(),
            communication_mode: Some(CommunicationMode::Oneway),
            arg: Some(Arc::new(7u32)),
            ..Default::default()
        };
        assert!(execute_check_forbidden_hook(&hooks, &mut context).is_ok());
        assert_eq!(recorder.snapshot(), vec!["check".to_string()]);
        assert_eq!(any_downcast::<u32>(&context.arg), Some(&7u32));
        assert_eq!(context.communication_mode, Some(CommunicationMode::Oneway));
        assert!(!context.unit_mode);
    }

    #[test]
    fn consume_and_end_transaction_hooks_swallow_errors() {
        let hooks = ConsumeMessageHookList::new();
        let boom = Arc::new(Recorder::default());
        let fine = Arc::new(Recorder::default());
        hooks.register(Arc::new(ConsumeHook { recorder: boom.clone(), boom: true }));
        hooks.register(Arc::new(ConsumeHook { recorder: fine.clone(), boom: false }));
        let mut context = ConsumeMessageContext::new("G", Some(vec![msg_ext(b"a")]), None);
        context.success = false;
        execute_consume_hook_before(&hooks, &mut context);
        execute_consume_hook_after(&hooks, &mut context);
        // 抛错只影响它自己那一侧：before 的错被吞，它的 after 照旧执行
        assert_eq!(
            boom.snapshot(),
            vec!["consume-before".to_string(), "consume-after".to_string()]
        );
        assert_eq!(
            fine.snapshot(),
            vec!["consume-before".to_string(), "consume-after".to_string()]
        );

        // 事务收尾钩子抛错同样只记 warn，且不影响后续钩子
        let end_hooks = EndTransactionHookList::new();
        let end_recorder = Arc::new(Recorder::default());
        end_hooks.register(Arc::new(EndHook { recorder: end_recorder.clone() }));
        execute_end_transaction_hook(&end_hooks, &mut EndTransactionContext::default());
        assert_eq!(end_recorder.snapshot(), vec!["end".to_string()]);
    }

    #[test]
    fn filter_hook_drops_messages_and_swallows_errors() {
        let hooks = FilterMessageHookList::new();
        let recorder = Arc::new(Recorder::default());
        hooks.register(Arc::new(DropFilter {
            recorder: recorder.clone(),
            drop_body: b"drop".to_vec(),
        }));
        let mut context = FilterMessageContext::new(
            "GID_test",
            Some(vec![msg_ext(b"drop"), msg_ext(b"keep")]),
            None,
        );
        execute_filter_hooks(&hooks, &mut context);
        assert_eq!(context.msg_list.len(), 1);
        assert_eq!(context.msg_list[0].get_body(), b"keep");
        assert_eq!(recorder.snapshot(), vec!["filter".to_string()]);

        // 抛错的过滤钩子不影响投递：列表原样（Python test_filter_hook_exception_is_swallowed）
        let boom_hooks = FilterMessageHookList::new();
        boom_hooks.register(Arc::new(BoomFilter));
        let mut context = FilterMessageContext::new("G", Some(vec![msg_ext(b"a")]), None);
        execute_filter_hooks(&boom_hooks, &mut context);
        assert_eq!(context.msg_list.len(), 1);
        assert_eq!(context.msg_list[0].get_body(), b"a");

        // 钩子也可以把列表整体清空（Python `context.msg_list = []`）
        let mut context = FilterMessageContext::new("G", Some(vec![msg_ext(b"a")]), None);
        context.msg_list.clear();
        assert!(context.msg_list.is_empty());
    }

    #[test]
    fn empty_hook_lists_are_no_ops() {
        let send_hooks = SendMessageHookList::default();
        let mut send_context = SendMessageContext::default();
        execute_send_message_hook_before(&send_hooks, &mut send_context);
        execute_send_message_hook_after(&send_hooks, &mut send_context);
        assert!(send_context.mq_trace_context.is_none());

        let forbidden = CheckForbiddenHookList::default();
        let mut forbid_context = CheckForbiddenContext::default();
        assert!(execute_check_forbidden_hook(&forbidden, &mut forbid_context).is_ok());

        let end_hooks = EndTransactionHookList::default();
        execute_end_transaction_hook(&end_hooks, &mut EndTransactionContext::default());

        let consume_hooks = ConsumeMessageHookList::default();
        execute_consume_hook_before(&consume_hooks, &mut ConsumeMessageContext::default());
        execute_consume_hook_after(&consume_hooks, &mut ConsumeMessageContext::default());

        let filter_hooks = FilterMessageHookList::default();
        execute_filter_hooks(&filter_hooks, &mut FilterMessageContext::default());
    }

    #[test]
    fn trait_defaults_make_single_sided_hooks_enough() {
        struct NameOnly;
        impl SendMessageHook for NameOnly {
            fn hook_name(&self) -> &str {
                "name-only"
            }
        }
        let hooks = SendMessageHookList::new();
        hooks.register(Arc::new(NameOnly));
        let mut context = SendMessageContext::default();
        execute_send_message_hook_before(&hooks, &mut context);
        execute_send_message_hook_after(&hooks, &mut context);
        assert!(context.mq_trace_context.is_none());
    }

    #[test]
    fn contexts_debug_without_leaking_holder_types() {
        let context = SendMessageContext {
            producer_group: "G".into(),
            mq: Some(MessageQueue::new("TopicTest", "broker-a", 0)),
            mq_trace_context: Some(Arc::new(123i64)),
            ..Default::default()
        };
        let text = format!("{:?}", context);
        assert!(text.contains("producer_group: \"G\""), "{text}");
        assert!(text.contains("mq_trace_context: \"set\""), "{text}");
        assert!(!text.contains("123"), "{text}");

        let filter = FilterMessageContext {
            consumer_group: "G".into(),
            msg_list: vec![msg_ext(b"a")],
            arg: Some(Arc::new("x")),
            unit_mode: true,
            ..Default::default()
        };
        let text = format!("{:?}", filter);
        assert!(text.contains("msg_list: 1"), "{text}");
        assert!(text.contains("arg: \"set\""), "{text}");
    }

    #[test]
    fn any_downcast_type_mismatch_returns_none() {
        let holder: Option<AnyHolder> = Some(Arc::new(42i32));
        assert!(any_downcast::<String>(&holder).is_none());
        assert_eq!(any_downcast::<i32>(&holder), Some(&42));
        assert!(any_downcast::<i32>(&None).is_none());
    }
}
