
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::common::message::{Message, MessageExt, MessageQueue};
use crate::common::message_type::MessageType;
use crate::error::{Error, Result};
use crate::{rmq_error, rmq_warn};

use super::result::{LocalTransactionState, SendResult};

pub type AnyHolder = Arc<dyn Any + Send + Sync>;

/// 类型不匹配时返回 `None` 而不是 panic —— 钩子之间互不相干，
/// 一个钩子塞错类型不该让整个发送链路挂掉。
pub fn any_downcast<T: Any + Send + Sync>(holder: &Option<AnyHolder>) -> Option<&T> {
    holder.as_ref().and_then(|value| value.downcast_ref::<T>())
}

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
    pub fn name(self) -> &'static str {
        match self {
            CommunicationMode::Sync => "SYNC",
            CommunicationMode::Async => "ASYNC",
            CommunicationMode::Oneway => "ONEWAY",
        }
    }

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

#[derive(Default)]
pub struct SendMessageContext {
    pub producer_group: String,
    pub message: Option<Message>,
    pub mq: Option<MessageQueue>,
    pub broker_addr: String,
    pub born_host: String,
    pub communication_mode: Option<CommunicationMode>,
    pub send_result: Option<SendResult>,
    /// 异常槽位：发送失败时由调用方写入，`after` 钩子里读它判成败。
    pub exception: Option<Error>,
    pub mq_trace_context: Option<AnyHolder>,
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

pub trait SendMessageHook: Send + Sync {
    fn hook_name(&self) -> &str;

    fn send_message_before(&self, _context: &mut SendMessageContext) -> Result<()> {
        Ok(())
    }

    fn send_message_after(&self, _context: &mut SendMessageContext) -> Result<()> {
        Ok(())
    }
}

// ------------------------------------------------------ ConsumeMessageHook

#[derive(Clone, Default)]
pub struct ConsumeMessageContext {
    pub consumer_group: String,
    pub msg_list: Vec<MessageExt>,
    pub mq: Option<MessageQueue>,
    pub success: bool,
    /// 消费状态的名字串（如 `"CONSUME_SUCCESS"`）。
    pub status: Option<String>,
    pub mq_trace_context: Option<AnyHolder>,
    pub props: Option<HashMap<String, String>>,
    /// `"LOCAL"` / `"CLOUD"`，见模块头差异说明 3。
    pub access_channel: Option<String>,
}

impl ConsumeMessageContext {
    /// 构造消费钩子上下文：`success` 缺省 `true`；
    /// `msg_list` 为 `None` 或空时都得到空列表。
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

pub trait ConsumeMessageHook: Send + Sync {
    fn hook_name(&self) -> &str;

    fn consume_message_before(&self, _context: &mut ConsumeMessageContext) -> Result<()> {
        Ok(())
    }

    fn consume_message_after(&self, _context: &mut ConsumeMessageContext) -> Result<()> {
        Ok(())
    }
}

// ------------------------------------------------------ EndTransactionHook

#[derive(Debug, Clone, Default)]
pub struct EndTransactionContext {
    pub producer_group: String,
    pub message: Option<Message>,
    pub broker_addr: String,
    pub msg_id: Option<String>,
    pub transaction_id: Option<String>,
    pub transaction_state: Option<LocalTransactionState>,
    pub from_transaction_check: bool,
    pub namespace: String,
}

pub trait EndTransactionHook: Send + Sync {
    fn hook_name(&self) -> &str;

    fn end_transaction(&self, _context: &mut EndTransactionContext) -> Result<()> {
        Ok(())
    }
}

// ------------------------------------------------------- CheckForbiddenHook

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
    pub arg: Option<AnyHolder>,
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

pub trait CheckForbiddenHook: Send + Sync {
    fn hook_name(&self) -> &str;

    fn check_forbidden(&self, _context: &mut CheckForbiddenContext) -> Result<()> {
        Ok(())
    }
}

// --------------------------------------------------------- FilterMessageHook

/// `msg_list` 是**可变的**：钩子把它替换/裁剪掉的消息会被客户端直接丢弃
/// （拉取路径 = 静默跳过；POP 路径 = 立刻 ack）。
#[derive(Clone, Default)]
pub struct FilterMessageContext {
    pub consumer_group: String,
    pub msg_list: Vec<MessageExt>,
    pub mq: Option<MessageQueue>,
    pub arg: Option<AnyHolder>,
    pub unit_mode: bool,
}

impl FilterMessageContext {
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

pub trait FilterMessageHook: Send + Sync {
    fn hook_name(&self) -> &str;

    fn filter_message(&self, _context: &mut FilterMessageContext) -> Result<()> {
        Ok(())
    }
}

// -------------------------------------------------------------- 钩子注册表

/// `send_message_hook_list` 等钩子注册表。
///
/// 注册与触发可能跨线程，所以这里是
/// `RwLock<Vec<Arc<H>>>`；[`HookList::hooks`] 返回**快照**，
/// 遍历语义等于「当时那张列表」。
pub struct HookList<H: ?Sized> {
    inner: RwLock<Vec<Arc<H>>>,
}

impl<H: ?Sized> HookList<H> {
    /// 空表。
    pub fn new() -> Self {
        HookList { inner: RwLock::new(Vec::new()) }
    }

    /// 追加到末尾，触发顺序 = 注册顺序；非空由 `Arc` 保证，无需判空。
    pub fn register(&self, hook: Arc<H>) {
        write_guard(&self.inner).push(hook);
    }

    /// 是否已注册钩子。
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

/// `send_message_hook_list`：producer 侧发送钩子注册表。
pub type SendMessageHookList = HookList<dyn SendMessageHook>;
/// `consume_message_hook_list`：consumer 侧消费钩子注册表。
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
/// ⚠ 与上面两个相反：**不吞异常**，第一个拒绝就向上抛。
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

pub fn execute_filter_hooks(hooks: &FilterMessageHookList, context: &mut FilterMessageContext) {
    for hook in hooks.hooks() {
        if let Err(e) = hook.filter_message(context) {
            // hook_name 不可失败，日志直接带上。
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

    /// 记录调用顺序的假钩子。
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
    fn communication_mode_names_match_golden_constants() {
        assert_eq!(CommunicationMode::Sync.name(), "SYNC");
        assert_eq!(CommunicationMode::Async.to_string(), "ASYNC");
        assert_eq!(CommunicationMode::Oneway.to_string(), "ONEWAY");
        // 未知名字回落 SYNC（与 result.rs 的 from_code 口径一致）
        assert_eq!(CommunicationMode::from_name("NOPE"), CommunicationMode::Sync);
        assert_eq!(CommunicationMode::from_name(""), CommunicationMode::Sync);
        assert_eq!(CommunicationMode::from_name("ONEWAY"), CommunicationMode::Oneway);
        assert_eq!(CommunicationMode::default(), CommunicationMode::Sync);
    }

    #[test]
    fn send_message_context_defaults_match_golden() {
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
    fn end_transaction_context_defaults_match_golden() {
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
    fn consume_context_success_defaults_true_like_golden_init() {
        // 构造时 success 是 true；投递路径上 consumer 侧会显式改成 false。
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
        // 空列表与 None 一样落到空表
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
        // before 写下的私有状态 after 能取回（mq_trace_context 约定）
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

        // 抛错的过滤钩子不影响投递：列表原样
        let boom_hooks = FilterMessageHookList::new();
        boom_hooks.register(Arc::new(BoomFilter));
        let mut context = FilterMessageContext::new("G", Some(vec![msg_ext(b"a")]), None);
        execute_filter_hooks(&boom_hooks, &mut context);
        assert_eq!(context.msg_list.len(), 1);
        assert_eq!(context.msg_list[0].get_body(), b"a");

        // 钩子也可以把列表整体清空
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
