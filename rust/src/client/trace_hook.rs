//! * [`SendMessageTraceHook`]    ← `trace.hook.SendMessageTraceHookImpl`
//! * [`ConsumeMessageTraceHook`] ← `trace.hook.ConsumeMessageTraceHookImpl`
//! * [`EndTransactionTraceHook`] ← `trace.hook.EndTransactionTraceHookImpl`
//!
//!
//! # 分发器依赖：[`TraceReportSink`]
//!
//! 三个钩子对分发器的依赖**只有三个方法**
//! （`get_trace_topic_name()` / `append(ctx)` / `client_id()`）。攒批、发送、
//! 生命周期都在 `client::trace_dispatcher`（另有人移植），所以这里只打一个依赖倒置的缝：
//! 本模块声明 [`TraceReportSink`]，**不** stub 分发器、也不碰 topic 选择与攒批。

use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;

use crate::client::result::SendStatus;
use crate::common::message_const::{PROPERTY_MSG_REGION, PROPERTY_TRACE_SWITCH};
use crate::common::message_type::MessageType;
use crate::common::mix_all::MixAll;
use crate::common::util_all;
use crate::error::Result;
use crate::remoting::protocol::namespace_util::NamespaceUtil;
use crate::{rmq_debug, rmq_warn};

use super::hook::{
    any_downcast, AnyHolder, ConsumeMessageContext, ConsumeMessageHook, EndTransactionContext,
    EndTransactionHook, SendMessageContext, SendMessageHook,
};
use super::trace::{AccessChannel, TraceBean, TraceContext, TraceType};

/// 消费者投递完成后把它写进 [`ConsumeMessageContext::props`]，值取
/// [`ConsumeReturnType`](crate::client::result::ConsumeReturnType) 的**枚举名**。
pub const CONSUME_CONTEXT_TYPE: &str = "ConsumeContextType";

/// 轨迹落地通道 —— 本模块唯一的对外依赖缝。
///
/// **由 `mq_client.rs` / `trace_dispatcher.rs` 实现**（那两个文件里才真正握有轨迹
/// topic、内部 trace producer 与 clientId）；本模块只声明、不实现，也不对攒批/发送
/// 做任何假设 —— 钩子只管把一条 [`TraceContext`] 交出去。
pub trait TraceReportSink: Send + Sync {
    /// 当前生效的轨迹 topic：[`MixAll::TRACE_TOPIC`](MixAll::TRACE_TOPIC)，或
    /// `AccessChannel::Cloud` 下的
    /// [`TraceConstants::TRACE_TOPIC_PREFIX`](super::trace::TraceConstants::TRACE_TOPIC_PREFIX)
    /// `+ regionId`。
    fn trace_topic_name(&self) -> String;

    /// 交出一条追踪记录。返回 `false` 表示分发器没收（停止中 / 队列满）。
    fn report(&self, context: TraceContext) -> bool;

    /// 进程内 clientId（`<ip>@<instanceName>`），写进 EndTransaction 轨迹的
    /// `clientHost` 字段。
    fn client_id(&self) -> String;
}

#[derive(Clone)]
pub struct SendMessageTraceHook {
    sink: Arc<dyn TraceReportSink>,
}

impl SendMessageTraceHook {
    pub fn new(sink: Arc<dyn TraceReportSink>) -> Self {
        SendMessageTraceHook { sink }
    }

    /// 交出钩子持有的通道。
    pub fn sink(&self) -> &Arc<dyn TraceReportSink> {
        &self.sink
    }
}

impl SendMessageHook for SendMessageTraceHook {
    fn hook_name(&self) -> &str {
        "SendMessageTraceHook"
    }

    /// 建一个 [`TraceType::Pub`] 的 [`TraceContext`] 挂到
    /// [`SendMessageContext::mq_trace_context`]，`after` 再取出来补 msgId 等字段。
    /// 此处只填「发送前就知道的」字段（topic / tags / keys / storeHost / bodyLength /
    /// msgType），`msgId` 留空等 `after` 用 SendResult 覆盖。
    fn send_message_before(&self, context: &mut SendMessageContext) -> Result<()> {
        // context 或 message 缺失时直接返回
        let Some(message) = context.message.as_ref() else {
            return Ok(());
        };
        let topic = message.topic.clone();
        if topic.starts_with(&self.sink.trace_topic_name()) {
            return Ok(());
        }
        let mut trace_context = TraceContext::new();
        trace_context.trace_type = Some(TraceType::Pub);
        trace_context.group_name = strip_namespace(&context.producer_group);
        let mut bean = TraceBean::new();
        bean.topic = strip_namespace(&topic);
        bean.tags = message.get_tags().unwrap_or("").to_string();
        bean.keys = message.get_keys().unwrap_or("").to_string();
        bean.store_host = context.broker_addr.clone();
        bean.body_length = message.get_body().len() as i32;
        bean.msg_type = context.msg_type;
        trace_context.trace_beans = vec![bean];
        context.mq_trace_context = Some(Arc::new(trace_context));
        Ok(())
    }

    /// 跳过条件逐条锁定（连顺序一致）：无 message / 是轨迹 topic /
    /// before 没建过 context / 没有 sendResult / broker 侧 `TRACE_ON=false` 或没回
    /// `MSG_REGION` / beans 为空。
    fn send_message_after(&self, context: &mut SendMessageContext) -> Result<()> {
        let Some(message) = context.message.as_ref() else {
            return Ok(());
        };
        let topic = message.topic.clone();
        if topic.starts_with(&self.sink.trace_topic_name()) {
            return Ok(());
        }
        let Some(mut trace_context) = held_trace_context(&context.mq_trace_context) else {
            return Ok(());
        };
        let Some(result) = context.send_result.as_ref() else {
            return Ok(());
        };
        if result.region_id.is_none() || !result.is_trace_on() {
            return Ok(());
        }
        if trace_context.trace_beans.is_empty() {
            return Ok(());
        }
        let cost_time = elapsed_per(
            util_all::current_time_millis(),
            trace_context.time_stamp,
            trace_context.trace_beans.len(),
        );
        trace_context.cost_time = cost_time;
        trace_context.is_success = result.get_send_status() == SendStatus::SendOk;
        trace_context.region_id = result.get_region_id().unwrap_or("").to_string();
        let store_time = trace_context.time_stamp + half_floor(cost_time);
        if let Some(bean) = trace_context.trace_beans.first_mut() {
            bean.msg_id = result.get_msg_id().unwrap_or("").to_string();
            bean.offset_msg_id = result.get_offset_msg_id().unwrap_or("").to_string();
            bean.store_time = store_time;
        }
        publish(&self.sink, &mut context.mq_trace_context, trace_context);
        Ok(())
    }
}

/// SubBefore / SubAfter 两条记录**共用同一个 `request_id`**（`after` 从 `before` 挂的
/// context 里抄），这是控制台把一次消费的前后串起来的唯一线索。
#[derive(Clone)]
pub struct ConsumeMessageTraceHook {
    sink: Arc<dyn TraceReportSink>,
}

impl ConsumeMessageTraceHook {
    pub fn new(sink: Arc<dyn TraceReportSink>) -> Self {
        ConsumeMessageTraceHook { sink }
    }

    /// 交出钩子持有的通道。
    pub fn sink(&self) -> &Arc<dyn TraceReportSink> {
        &self.sink
    }
}

impl ConsumeMessageHook for ConsumeMessageTraceHook {
    fn hook_name(&self) -> &str {
        "ConsumeMessageTraceHook"
    }

    fn consume_message_before(&self, context: &mut ConsumeMessageContext) -> Result<()> {
        if context.msg_list.is_empty() {
            return Ok(());
        }
        let mut trace_context = TraceContext::new();
        trace_context.trace_type = Some(TraceType::SubBefore);
        trace_context.group_name = strip_namespace(&context.consumer_group);
        // 在建好 beans 之前就把（此时还是空壳的）context 挂上去：
        // 一条都没记上时 `consume_message_after` 仍能拿到它并因 beans 为空而跳过。
        context.mq_trace_context = Some(Arc::new(trace_context.clone()));
        let mut beans: Vec<TraceBean> = Vec::with_capacity(context.msg_list.len());
        for msg in &context.msg_list {
            let region_id = msg.get_property(PROPERTY_MSG_REGION);
            let trace_on = msg.get_property(PROPERTY_TRACE_SWITCH);
            if trace_on == Some("false") {
                continue;
            }
            let mut bean = TraceBean::new();
            bean.topic = strip_namespace(&msg.topic);
            bean.msg_id = msg.msg_id.clone().unwrap_or_default();
            bean.tags = msg.get_tags().unwrap_or("").to_string();
            bean.keys = msg.get_keys().unwrap_or("").to_string();
            bean.store_time = msg.store_timestamp;
            bean.body_length = msg.store_size;
            bean.retry_times = msg.reconsume_times;
            // region 属性缺失/空串都写空串，且会覆盖前一条的值
            trace_context.region_id = region_id.unwrap_or("").to_string();
            beans.push(bean);
        }
        if !beans.is_empty() {
            trace_context.trace_beans = beans;
            trace_context.time_stamp = util_all::current_time_millis();
            publish(&self.sink, &mut context.mq_trace_context, trace_context);
        }
        Ok(())
    }

    fn consume_message_after(&self, context: &mut ConsumeMessageContext) -> Result<()> {
        if context.msg_list.is_empty() {
            return Ok(());
        }
        let Some(sub_before) = held_trace_context(&context.mq_trace_context) else {
            return Ok(());
        };
        if sub_before.trace_beans.is_empty() {
            return Ok(());
        }
        let mut sub_after = TraceContext::new();
        sub_after.trace_type = Some(TraceType::SubAfter);
        sub_after.region_id = sub_before.region_id.clone();
        sub_after.group_name = strip_namespace(&sub_before.group_name);
        sub_after.request_id = sub_before.request_id.clone();
        sub_after.access_channel = access_channel_of(context.access_channel.as_deref());
        sub_after.is_success = context.success;
        sub_after.cost_time = elapsed_per(
            util_all::current_time_millis(),
            sub_before.time_stamp,
            context.msg_list.len(),
        );
        // sub_after 与 sub_before 共享同一批 beans（原本就是同一个对象）；
        // append 之后再没人改它，克隆等价。
        sub_after.trace_beans = sub_before.trace_beans.clone();
        if let Some(props) = context.props.as_ref() {
            if let Some(context_type) = props.get(CONSUME_CONTEXT_TYPE) {
                match consume_return_code(context_type) {
                    Some(code) => sub_after.context_code = code,
                    None => {
                        rmq_debug!("unknown ConsumeContextType {context_type:?}, keep contextCode=0")
                    }
                }
            }
        }
        report(&self.sink, sub_after);
        Ok(())
    }
}

/// broker 的事务回查（`fromTransactionCheck=true`）也会走这里，所以「客户端主动提交」
/// 和「回查后提交」两种情况都能在轨迹里看到。
#[derive(Clone)]
pub struct EndTransactionTraceHook {
    sink: Arc<dyn TraceReportSink>,
}

impl EndTransactionTraceHook {
    pub fn new(sink: Arc<dyn TraceReportSink>) -> Self {
        EndTransactionTraceHook { sink }
    }

    /// 交出钩子持有的通道。
    pub fn sink(&self) -> &Arc<dyn TraceReportSink> {
        &self.sink
    }
}

impl EndTransactionHook for EndTransactionTraceHook {
    fn hook_name(&self) -> &str {
        "EndTransactionTraceHook"
    }

    fn end_transaction(&self, context: &mut EndTransactionContext) -> Result<()> {
        let Some(message) = context.message.as_ref() else {
            return Ok(());
        };
        let topic = message.topic.clone();
        if topic.starts_with(&self.sink.trace_topic_name()) {
            return Ok(());
        }
        let mut trace_context = TraceContext::new();
        trace_context.trace_type = Some(TraceType::EndTransaction);
        trace_context.group_name = strip_namespace(&context.producer_group);
        let mut bean = TraceBean::new();
        bean.topic = strip_namespace(&topic);
        bean.tags = message.get_tags().unwrap_or("").to_string();
        bean.keys = message.get_keys().unwrap_or("").to_string();
        bean.store_host = context.broker_addr.clone();
        bean.msg_type = MessageType::TransMsgCommit;
        bean.client_host = self.sink.client_id();
        bean.msg_id = context.msg_id.clone().unwrap_or_default();
        if let Some(state) = context.transaction_state {
            bean.set_transaction_state(state);
        }
        bean.transaction_id = context.transaction_id.clone();
        bean.from_transaction_check = context.from_transaction_check;
        let region_id = message.get_property(PROPERTY_MSG_REGION);
        trace_context.region_id = match region_id {
            Some(region) if !region.is_empty() => region.to_string(),
            // region 为空时回落默认 region
            _ => MixAll::DEFAULT_TRACE_REGION_ID.to_string(),
        };
        trace_context.trace_beans = vec![bean];
        trace_context.time_stamp = util_all::current_time_millis();
        report(&self.sink, trace_context);
        Ok(())
    }
}

// ------------------------------------------------------------------ 内部工具

/// 剥掉命名空间前缀（命名空间按空串处理）。
///
/// `NS1%TopicTest` → `TopicTest`；`%RETRY%NS1%GID_test` → `%RETRY%GID_test`；
/// 系统资源（`rmq_sys_` / `CID_RMQ_SYS_` 前缀）原样返回。
fn strip_namespace(resource: &str) -> String {
    NamespaceUtil::without_namespace(resource, "")
}

/// 从 [`crate::client::hook`] 的 `Object` 槽里取回本模块写下的 [`TraceContext`]。
fn held_trace_context(holder: &Option<AnyHolder>) -> Option<TraceContext> {
    match any_downcast::<TraceContext>(holder) {
        Some(ctx) => Some(ctx.clone()),
        None => {
            if holder.is_some() {
                rmq_warn!("mqTraceContext is not a TraceContext, skip message trace record");
            }
            None
        }
    }
}

/// 交出一条记录，并把它同步回 `mq_trace_context`（调用方读的就是挂在上面的那个对象，
/// 所以钩子跑完后调用方看到的状态必须是「已填好」的那一份）。
fn publish(
    sink: &Arc<dyn TraceReportSink>,
    holder: &mut Option<AnyHolder>,
    context: TraceContext,
) {
    *holder = Some(Arc::new(context.clone()));
    report(sink, context);
}

/// 交出一条记录（不需要回挂到上下文上时用）。
fn report(sink: &Arc<dyn TraceReportSink>, context: TraceContext) {
    let type_name = context.trace_type.map(TraceType::name).unwrap_or_default();
    if !sink.report(context) {
        rmq_debug!("trace sink refused a {type_name} record");
    }
}

/// `count` 为 0 在调用方都先判过空，这里再兜一次底，避免除零 panic。
fn elapsed_per(now: i64, start: i64, count: usize) -> i32 {
    let divisor = i64::try_from(count).unwrap_or(1).max(1);
    ((now - start) / divisor) as i32
}

fn half_floor(cost_time: i32) -> i64 {
    i64::from(cost_time.div_euclid(2))
}

/// [`ConsumeMessageContext::access_channel`] 存的是名字串（见该模块头差异说明 3），
/// 这里换回 [`AccessChannel`] 交给编码器判断。
///
/// 该字段本身就是可选的：`None` → 编码器按 LOCAL 处理；
/// `"CLOUD"` → CLOUD；其它值都不等于 CLOUD，编码效果与 LOCAL 相同 ——
/// 所以无法识别的名字映射成 `None`，字节输出与线上基准一致。
fn access_channel_of(name: Option<&str>) -> Option<AccessChannel> {
    match name? {
        "CLOUD" => Some(AccessChannel::Cloud),
        "LOCAL" => Some(AccessChannel::Local),
        _ => None,
    }
}

/// [`crate::client::result::ConsumeReturnType`] 没有 `from_name`（调用方都持强类型枚举），
/// 故在本模块内做名字 → 既有枚举的映射，不重复定义枚举；
/// 未知名返回 `None`，调用方据此保持 `contextCode = 0`。
fn consume_return_code(name: &str) -> Option<i32> {
    use crate::client::result::ConsumeReturnType as T;
    let code = match name {
        "SUCCESS" => T::Success,
        "TIME_OUT" => T::TimeOut,
        "EXCEPTION" => T::Exception,
        "RETURNNULL" => T::ReturnNull,
        "FAILED" => T::Failed,
        _ => return None,
    };
    Some(code.code())
}

/// 测试用的记录通道：原样存下钩子交出的 [`TraceContext`]，topic / clientId 可配。
#[cfg(test)]
struct RecordingSink {
    trace_topic: String,
    client_id: String,
    reported: Mutex<Vec<TraceContext>>,
    accept: bool,
}

#[cfg(test)]
impl RecordingSink {
    fn new(trace_topic: &str, client_id: &str) -> Self {
        RecordingSink {
            trace_topic: trace_topic.to_string(),
            client_id: client_id.to_string(),
            reported: Mutex::new(Vec::new()),
            accept: true,
        }
    }

    /// 总是「拒收」（分发器已停止 / 队列满）的记录通道。
    fn refusing(trace_topic: &str) -> Self {
        RecordingSink { accept: false, ..Self::new(trace_topic, "10.0.0.9@inst-1#0") }
    }

    fn reported(&self) -> Vec<TraceContext> {
        self.lock().clone()
    }

    fn last(&self) -> Option<TraceContext> {
        self.lock().last().cloned()
    }

    fn count(&self) -> usize {
        self.lock().len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<TraceContext>> {
        self.reported.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
impl TraceReportSink for RecordingSink {
    fn trace_topic_name(&self) -> String {
        self.trace_topic.clone()
    }

    fn report(&self, context: TraceContext) -> bool {
        self.lock().push(context);
        self.accept
    }

    fn client_id(&self) -> String {
        self.client_id.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::client::result::{ConsumeReturnType, LocalTransactionState, SendResult};
    use crate::client::trace::{java_split, TraceConstants, TraceDataEncoder};
    use crate::common::message::{Message, MessageExt, MessageQueue};

    const SOH: char = TraceConstants::CONTENT_SPLITOR;
    const STX: char = TraceConstants::FIELD_SPLITOR;

    const TRACE_TOPIC: &str = "RMQ_SYS_TRACE_TOPIC";
    const CLOUD_TRACE_TOPIC: &str = "rmq_sys_TRACE_DATA_cn-hangzhou";
    const CLIENT_ID: &str = "10.0.0.9@inst-1#0";
    const MSG_ID: &str = "AC1400A1F0A018B4AAC2A1B2C3D4E5F6";
    const OFFSET_MSG_ID: &str = "AC1400A1000027100000000000000001";
    const BROKER: &str = "127.0.0.1:10911";

    /// 把 golden 模板里的占位符换成实际值。
    ///
    /// 时钟相关的三个数（time_stamp / cost_time / store_time）与 `request_id` 在两侧
    /// 不可能相同（Rust 用真实时钟 + 随机 uniqId），所以模板里留占位；它们的**取值规则**
    /// 由 `pub_cost_and_store_time_follow_python_rules` 等用例单独钉住。
    fn render(template: &str, ctx: &TraceContext) -> String {
        let store_time = ctx
            .trace_beans
            .first()
            .map(|bean| bean.store_time.to_string())
            .unwrap_or_default();
        template
            .replace("{TS}", &ctx.time_stamp.to_string())
            .replace("{COST}", &ctx.cost_time.to_string())
            .replace("{STORE}", &store_time)
            .replace("{REQ}", &ctx.request_id)
    }

    fn encode(ctx: &TraceContext) -> String {
        TraceDataEncoder::encoder_from_context_bean(Some(ctx))
            .unwrap_or_default()
            .trans_data
    }

    fn transfer(ctx: &TraceContext) -> (String, Vec<String>) {
        let tb = TraceDataEncoder::encoder_from_context_bean(Some(ctx)).unwrap_or_default();
        (tb.trans_data.clone(), tb.trans_key.iter().cloned().collect())
    }

    /// 一条 `trans_data` 拆成「记录 -> 字段」。
    fn records(encoded: &str) -> Vec<Vec<String>> {
        java_split(encoded, STX)
            .into_iter()
            .map(|line| {
                java_split(line, SOH)
                    .into_iter()
                    .map(|field| field.to_string())
                    .collect()
            })
            .collect()
    }

    fn pub_message(topic: &str) -> Message {
        Message::with_tags_and_keys(
            topic,
            Some(&b"x".repeat(42)),
            Some("TagA"),
            Some("KeyA KeyB"),
            0,
        )
    }

    fn send_context(topic: &str) -> SendMessageContext {
        SendMessageContext {
            producer_group: "NS1%GID_test".to_string(),
            message: Some(pub_message(topic)),
            broker_addr: BROKER.to_string(),
            msg_type: MessageType::NormalMsg,
            ..Default::default()
        }
    }

    fn ok_result() -> SendResult {
        SendResult {
            status: SendStatus::SendOk,
            msg_id: Some(MSG_ID.to_string()),
            offset_msg_id: Some(OFFSET_MSG_ID.to_string()),
            region_id: Some("DefaultRegion".to_string()),
            queue_offset: 7,
            ..Default::default()
        }
    }

    /// 一条拉取到的消息。
    ///
    /// 参数表与 golden 用例逐字段一一对应，合并参数会破坏可读性。
    #[allow(clippy::too_many_arguments)]
    fn ext(
        topic: &str,
        msg_id: &str,
        tags: Option<&str>,
        keys: Option<&str>,
        region: Option<&str>,
        trace_on: Option<&str>,
        store_time: i64,
        store_size: i32,
        retry: i32,
    ) -> MessageExt {
        let mut msg = MessageExt::new();
        msg.set_topic(topic);
        msg.msg_id = Some(msg_id.to_string());
        if let Some(tags) = tags {
            msg.set_tags(tags);
        }
        if let Some(keys) = keys {
            msg.set_keys(keys);
        }
        if let Some(region) = region {
            msg.put_property(PROPERTY_MSG_REGION, region);
        }
        if let Some(trace_on) = trace_on {
            msg.put_property(PROPERTY_TRACE_SWITCH, trace_on);
        }
        msg.store_timestamp = store_time;
        msg.store_size = store_size;
        msg.reconsume_times = retry;
        msg
    }

    /// 固定的一批消费消息（第三条被 `TRACE_ON=false` 挡掉）。
    fn python_scenario_msgs() -> Vec<MessageExt> {
        vec![
            ext(
                "NS1%TopicTest",
                "MSGID-1",
                Some("TagA"),
                Some("KeyA KeyB"),
                Some("NS1%cn-hangzhou"),
                None,
                1_700_000_000_111,
                100,
                0,
            ),
            ext(
                "%RETRY%NS1%GID_test",
                "MSGID-2",
                None,
                Some("KeyC"),
                Some("cn-shanghai"),
                Some("true"),
                1_700_000_000_222,
                200,
                3,
            ),
            ext(
                "TopicTest",
                "MSGID-SKIPPED",
                Some("TagZ"),
                Some("KeyZ"),
                Some("cn-beijing"),
                Some("false"),
                1_700_000_000_333,
                300,
                1,
            ),
        ]
    }

    fn consume_context(msgs: Vec<MessageExt>) -> ConsumeMessageContext {
        ConsumeMessageContext::new(
            "NS1%GID_test",
            Some(msgs),
            Some(MessageQueue::new("NS1%TopicTest", "broker-a", 0)),
        )
    }

    // ------------------------------------------------ 发送钩子

    /// Pub 记录金标准（时钟两段留占位）。
    const GOLDEN_PUB: &str = "Pub\u{1}{TS}\u{1}DefaultRegion\u{1}GID_test\u{1}TopicTest\
         \u{1}AC1400A1F0A018B4AAC2A1B2C3D4E5F6\u{1}TagA\u{1}KeyA KeyB\u{1}127.0.0.1:10911\
         \u{1}42\u{1}{COST}\u{1}0\u{1}AC1400A1000027100000000000000001\u{1}true\u{2}";

    #[test]
    fn hook_names_are_canonical_trace_hook_names() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let send = SendMessageTraceHook::new(sink.clone());
        let consume = ConsumeMessageTraceHook::new(sink.clone());
        let end = EndTransactionTraceHook::new(sink.clone());
        assert_eq!(send.hook_name(), "SendMessageTraceHook");
        assert_eq!(consume.hook_name(), "ConsumeMessageTraceHook");
        assert_eq!(end.hook_name(), "EndTransactionTraceHook");
        // 钩子持有的通道可取回
        assert_eq!(send.sink().trace_topic_name(), TRACE_TOPIC.to_string());
        assert_eq!(consume.sink().client_id(), CLIENT_ID.to_string());
        assert_eq!(end.sink().trace_topic_name(), TRACE_TOPIC.to_string());
    }

    #[test]
    fn send_pub_record_matches_python_golden() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = SendMessageTraceHook::new(sink.clone());

        let mut context = send_context("NS1%TopicTest");
        hook.send_message_before(&mut context).unwrap();
        context.send_result = Some(ok_result());
        hook.send_message_after(&mut context).unwrap();

        let emitted = sink.last().expect("test-only");
        assert_eq!(sink.count(), 1);
        assert_eq!(emitted.trace_type, Some(TraceType::Pub));
        assert_eq!(emitted.region_id, "DefaultRegion");
        assert_eq!(emitted.group_name, "GID_test");
        assert!(emitted.is_success);
        let bean = &emitted.trace_beans[0];
        assert_eq!(bean.msg_id, MSG_ID);
        assert_eq!(bean.offset_msg_id, OFFSET_MSG_ID);
        assert_eq!(
            encode(&emitted),
            render(GOLDEN_PUB, &emitted),
            "actual = {:?}",
            encode(&emitted)
        );
        let (data, keys) = transfer(&emitted);
        assert_eq!(data, render(GOLDEN_PUB, &emitted));
        assert_eq!(keys, vec![MSG_ID.to_string(), "KeyA".to_string(), "KeyB".to_string()]);
        // 挂在上下文里的那份必须与交出去的那份一致（同一个对象）
        assert_eq!(held_trace_context(&context.mq_trace_context), Some(emitted));
    }

    #[test]
    fn send_before_fills_only_pre_send_fields() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = SendMessageTraceHook::new(sink.clone());
        let mut context = send_context("NS1%TopicTest");
        hook.send_message_before(&mut context).unwrap();

        let held = held_trace_context(&context.mq_trace_context).expect("test-only");
        assert_eq!(sink.count(), 0, "before 不落库");
        assert_eq!(held.trace_type, Some(TraceType::Pub));
        assert_eq!(held.group_name, "GID_test", "producerGroup 剥掉 NS1%");
        assert_eq!(held.region_id, "", "region 要等 SendResult 回填");
        assert!(held.is_success, "isSuccess 默认 true");
        assert_eq!(held.cost_time, 0);
        let bean = &held.trace_beans[0];
        assert_eq!(bean.topic, "TopicTest");
        assert_eq!(bean.tags, "TagA");
        assert_eq!(bean.keys, "KeyA KeyB");
        assert_eq!(bean.store_host, BROKER);
        assert_eq!(bean.body_length, 42);
        assert_eq!(bean.msg_type, MessageType::NormalMsg);
        assert_eq!(bean.msg_id, "", "msgId 要等 after");
        assert_eq!(bean.offset_msg_id, "");

        // 无 tags / keys / body 的裸消息：属性缺失 => 空串
        let mut bare = SendMessageContext {
            message: Some(Message::new("TopicTest", None)),
            ..Default::default()
        };
        hook.send_message_before(&mut bare).unwrap();
        let held = held_trace_context(&bare.mq_trace_context).expect("test-only");
        assert_eq!(held.group_name, "");
        assert_eq!(held.trace_beans[0].tags, "");
        assert_eq!(held.trace_beans[0].keys, "");
        assert_eq!(held.trace_beans[0].body_length, 0);
        assert_eq!(sink.count(), 0);
    }

    #[test]
    fn send_hooks_never_trace_the_trace_topic() {
        // 普通集群：RMQ_SYS_TRACE_TOPIC
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = SendMessageTraceHook::new(sink.clone());
        let mut context = send_context(TRACE_TOPIC);
        hook.send_message_before(&mut context).unwrap();
        context.send_result = Some(ok_result());
        hook.send_message_after(&mut context).unwrap();
        assert!(context.mq_trace_context.is_none());
        assert_eq!(sink.count(), 0);

        // 云集群：rmq_sys_TRACE_DATA_<region>
        let cloud = Arc::new(RecordingSink::new(CLOUD_TRACE_TOPIC, CLIENT_ID));
        let hook = SendMessageTraceHook::new(cloud.clone());
        let mut context = send_context(CLOUD_TRACE_TOPIC);
        hook.send_message_before(&mut context).unwrap();
        hook.send_message_after(&mut context).unwrap();
        assert!(context.mq_trace_context.is_none());
        assert_eq!(cloud.count(), 0);

        // 分发器没配 topic（空前缀）时轨迹 topic 前缀判断恒真 =>
        // 整条轨迹链路关掉
        let empty = Arc::new(RecordingSink::new("", CLIENT_ID));
        let hook = SendMessageTraceHook::new(empty.clone());
        let mut context = send_context("TopicTest");
        hook.send_message_before(&mut context).unwrap();
        assert!(context.mq_trace_context.is_none());
        assert_eq!(empty.count(), 0);
    }

    #[test]
    fn send_before_without_message_is_noop() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = SendMessageTraceHook::new(sink.clone());
        let mut context = SendMessageContext::default();
        hook.send_message_before(&mut context).unwrap();
        hook.send_message_after(&mut context).unwrap();
        assert!(context.mq_trace_context.is_none());
        assert_eq!(sink.count(), 0);
    }

    #[test]
    fn send_after_skips_when_broker_switches_trace_off() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = SendMessageTraceHook::new(sink.clone());

        // TRACE_ON=false
        let mut context = send_context("TopicTest");
        hook.send_message_before(&mut context).unwrap();
        let mut result = ok_result();
        result.set_trace_on(false);
        context.send_result = Some(result);
        hook.send_message_after(&mut context).unwrap();
        assert_eq!(sink.count(), 0, "TRACE_ON=false 不落库");
        assert!(context.mq_trace_context.is_some(), "上下文里的壳还在");

        // MSG_REGION 缺失
        let mut context = send_context("TopicTest");
        hook.send_message_before(&mut context).unwrap();
        let mut result = ok_result();
        result.set_region_id(None);
        context.send_result = Some(result);
        hook.send_message_after(&mut context).unwrap();
        assert_eq!(sink.count(), 0, "MSG_REGION 缺失不落库");

        // 完全没有 sendResult（ONEWAY / 异常路径）
        let mut context = send_context("TopicTest");
        hook.send_message_before(&mut context).unwrap();
        hook.send_message_after(&mut context).unwrap();
        assert_eq!(sink.count(), 0);

        // after 之前没跑 before
        let mut context = send_context("TopicTest");
        context.send_result = Some(ok_result());
        hook.send_message_after(&mut context).unwrap();
        assert_eq!(sink.count(), 0);
    }

    #[test]
    fn send_after_type_mismatch_is_skipped_not_panicked() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = SendMessageTraceHook::new(sink.clone());
        let mut context = send_context("TopicTest");
        context.mq_trace_context = Some(Arc::new("someone-else's-state"));
        context.send_result = Some(ok_result());
        hook.send_message_after(&mut context).unwrap();
        assert_eq!(sink.count(), 0);
        assert_eq!(held_trace_context(&context.mq_trace_context), None);
    }

    #[test]
    fn send_after_success_flag_follows_send_status() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = SendMessageTraceHook::new(sink.clone());
        let mut context = send_context("TopicTest");
        hook.send_message_before(&mut context).unwrap();
        let mut result = ok_result();
        result.status = SendStatus::FlushSlaveTimeout;
        result.msg_id = Some("M2".to_string());
        result.offset_msg_id = Some("O2".to_string());
        context.send_result = Some(result);
        hook.send_message_after(&mut context).unwrap();

        let emitted = sink.last().expect("test-only");
        assert!(!emitted.is_success, "非 SEND_OK => success=false");
        let fields = records(&encode(&emitted)).remove(0);
        assert_eq!(fields.len(), 14);
        assert_eq!(fields[0], "Pub");
        assert_eq!(fields[4], "TopicTest");
        assert_eq!(fields[5], "M2");
        assert_eq!(fields[12], "O2");
        assert_eq!(fields[13], "false");
    }

    #[test]
    fn pub_cost_and_store_time_follow_python_rules() {
        // 均摊耗时 (now*1000 - ts) / len(beans) 向零取整；storeTime = ts + cost/2 向下取整
        assert_eq!(elapsed_per(1_700_000_000_042, 1_700_000_000_000, 1), 42);
        assert_eq!(elapsed_per(1_700_000_000_042, 1_700_000_000_000, 2), 21);
        assert_eq!(elapsed_per(1_700_000_000_000, 1_700_000_000_000, 3), 0);
        // 时钟回拨：向零取整是 -3，向下取整是 -2
        assert_eq!(elapsed_per(1_699_999_999_997, 1_700_000_000_000, 1), -3);
        assert_eq!(half_floor(-3), -2, "half_floor 向下取整：-3/2 == -2");
        assert_eq!(half_floor(-4), -2);
        assert_eq!(half_floor(3), 1);
        assert_eq!(half_floor(42000), 21000);
        // 时钟回拨场景下预期的 storeTime
        assert_eq!(1_700_000_000_000 + half_floor(-3), 1_699_999_999_998);

        // 真实钩子跑完也必须满足同一关系
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = SendMessageTraceHook::new(sink.clone());
        let mut context = send_context("TopicTest");
        hook.send_message_before(&mut context).unwrap();
        context.send_result = Some(ok_result());
        hook.send_message_after(&mut context).unwrap();
        let emitted = sink.last().expect("test-only");
        assert!(emitted.cost_time >= 0, "cost = {}", emitted.cost_time);
        assert!(emitted.cost_time < 60_000, "cost = {}", emitted.cost_time);
        assert_eq!(
            emitted.trace_beans[0].store_time,
            emitted.time_stamp + half_floor(emitted.cost_time)
        );
        assert_eq!(emitted.trace_beans[0].store_time, render_store(&emitted));

        let mut skewed = emitted.clone();
        skewed.time_stamp = util_all::current_time_millis() + 50;
        context.mq_trace_context = Some(Arc::new(skewed));
        hook.send_message_after(&mut context).unwrap();
        let back = sink.reported().remove(1);
        assert!(back.cost_time <= -45, "cost = {}", back.cost_time);
        assert_eq!(
            back.trace_beans[0].store_time,
            back.time_stamp + half_floor(back.cost_time)
        );
        assert_eq!(back.trace_beans[0].store_time, render_store(&back));
        assert!(back.trace_beans[0].store_time <= back.time_stamp - 2);
    }

    /// 用同样的取整规则独立重算一遍 storeTime（不依赖钩子内部变量）。
    fn render_store(ctx: &TraceContext) -> i64 {
        let cost = ctx.cost_time;
        ctx.time_stamp + if cost >= 0 { (cost / 2) as i64 } else { -(((-cost + 1) / 2) as i64) }
    }

    #[test]
    fn send_after_reports_even_when_sink_refuses() {
        let sink = Arc::new(RecordingSink::refusing(TRACE_TOPIC));
        let hook = SendMessageTraceHook::new(sink.clone());
        let mut context = send_context("TopicTest");
        hook.send_message_before(&mut context).unwrap();
        context.send_result = Some(ok_result());
        assert!(hook.send_message_after(&mut context).is_ok());
        assert_eq!(sink.count(), 1);
        assert!(context.mq_trace_context.is_some());
    }

    // ------------------------------------------------ 消费钩子

    /// SubBefore 两段金标准（时钟 / uniqId 段留占位）。
    const GOLDEN_SUB_BEFORE: &str = "SubBefore\u{1}{TS}\u{1}cn-shanghai\u{1}GID_test\
         \u{1}{REQ}\u{1}MSGID-1\u{1}0\u{1}KeyA KeyB\
         \u{2}SubBefore\u{1}{TS}\u{1}cn-shanghai\u{1}GID_test\
         \u{1}{REQ}\u{1}MSGID-2\u{1}3\u{1}KeyC\u{2}";

    /// SubAfter 两段金标准（LOCAL 通道，9 字段）。
    const GOLDEN_SUB_AFTER: &str = "SubAfter\u{1}{REQ}\u{1}MSGID-1\u{1}{COST}\u{1}false\
         \u{1}KeyA KeyB\u{1}3\u{1}{TS}\u{1}GID_test\
         \u{2}SubAfter\u{1}{REQ}\u{1}MSGID-2\u{1}{COST}\u{1}false\
         \u{1}KeyC\u{1}3\u{1}{TS}\u{1}GID_test\u{2}";

    #[test]
    fn consume_before_and_after_records_match_python_golden() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = ConsumeMessageTraceHook::new(sink.clone());
        let mut context = consume_context(python_scenario_msgs());
        context.success = false;
        context.access_channel = Some("LOCAL".to_string());
        let mut props = HashMap::new();
        props.insert(CONSUME_CONTEXT_TYPE.to_string(), "RETURNNULL".to_string());
        context.props = Some(props);

        hook.consume_message_before(&mut context).unwrap();
        let before = sink.last().expect("test-only");
        assert_eq!(sink.count(), 1);
        assert_eq!(
            encode(&before),
            render(GOLDEN_SUB_BEFORE, &before),
            "actual = {:?}",
            encode(&before)
        );
        assert_eq!(
            transfer(&before).1,
            vec![
                "KeyA".to_string(),
                "KeyB".to_string(),
                "KeyC".to_string(),
                "MSGID-1".to_string(),
                "MSGID-2".to_string()
            ]
        );
        assert_eq!(before.trace_type, Some(TraceType::SubBefore));
        assert_eq!(before.group_name, "GID_test");
        assert_eq!(before.region_id, "cn-shanghai", "最后一条未跳过的消息说了算");
        assert!(before.time_stamp > 0);
        assert_eq!(before.trace_beans.len(), 2, "TRACE_ON=false 的那条不记");
        assert_eq!(before.trace_beans[0].topic, "TopicTest");
        assert_eq!(before.trace_beans[0].store_time, 1_700_000_000_111);
        assert_eq!(before.trace_beans[0].body_length, 100);
        assert_eq!(before.trace_beans[1].topic, "%RETRY%GID_test", "%RETRY% 前缀保留");
        assert_eq!(before.trace_beans[1].retry_times, 3);
        assert_eq!(before.trace_beans[1].body_length, 200);

        hook.consume_message_after(&mut context).unwrap();
        let after = sink.last().expect("test-only");
        assert_eq!(sink.count(), 2, "before / after 各一条");
        assert_eq!(
            encode(&after),
            render(GOLDEN_SUB_AFTER, &after),
            "actual = {:?}",
            encode(&after)
        );
        assert_eq!(after.trace_type, Some(TraceType::SubAfter));
        assert_eq!(after.request_id, before.request_id, "两条记录共用 request_id");
        assert_eq!(after.region_id, before.region_id);
        assert_eq!(after.context_code, ConsumeReturnType::ReturnNull.code());
        assert!(!after.is_success);
        assert_eq!(after.access_channel, Some(AccessChannel::Local));
        assert_eq!(after.trace_beans, before.trace_beans, "beans 与 SubBefore 同源");
        assert_eq!(records(&encode(&after)).len(), 2);
        // 上下文里挂的还是 SubBefore 那一份（after 只新建了 SubAfter）
        assert_eq!(
            held_trace_context(&context.mq_trace_context).map(|ctx| ctx.trace_type),
            Some(Some(TraceType::SubBefore))
        );
    }

    #[test]
    fn consume_after_cost_time_divides_whole_batch() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = ConsumeMessageTraceHook::new(sink.clone());
        let mut context = consume_context(python_scenario_msgs());
        hook.consume_message_before(&mut context).unwrap();
        let before = sink.last().expect("test-only");
        assert_eq!(before.trace_beans.len(), 2, "第三条被 TRACE_ON 挡掉");
        // 把 SubBefore 的时间戳往前拨 3 秒（钩子自己的私有槽，等价于「这批消息
        // 3 秒前开始消费」），再看 costTime 除的是 3 还是 2
        let mut stale = before.clone();
        stale.time_stamp -= 3000;
        context.mq_trace_context = Some(Arc::new(stale));
        hook.consume_message_after(&mut context).unwrap();
        let after = sink.last().expect("test-only");
        assert!(
            (1000..1200).contains(&after.cost_time),
            "cost = {} 应是 3000/3，不是 3000/2=1500",
            after.cost_time
        );
    }

    #[test]
    fn consume_after_cloud_channel_and_unknown_return_type() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = ConsumeMessageTraceHook::new(sink.clone());
        let mut context = ConsumeMessageContext::new(
            "GID_test",
            Some(vec![ext(
                "TopicTest",
                "MSGID-C",
                Some("TagA"),
                Some("KeyA"),
                Some("cn-hangzhou"),
                None,
                1_700_000_000_555,
                50,
                2,
            )]),
            None,
        );
        hook.consume_message_before(&mut context).unwrap();
        // 云端通道 + 无法识别返回类型的场景，SubBefore 单段
        assert_eq!(
            encode(&sink.last().expect("test-only")),
            render(
                "SubBefore\u{1}{TS}\u{1}cn-hangzhou\u{1}GID_test\u{1}{REQ}\u{1}MSGID-C\
                 \u{1}2\u{1}KeyA\u{2}",
                &sink.last().expect("test-only")
            )
        );
        context.access_channel = Some("CLOUD".to_string());
        let mut props = HashMap::new();
        props.insert(CONSUME_CONTEXT_TYPE.to_string(), "NOT_A_TYPE".to_string());
        context.props = Some(props);
        hook.consume_message_after(&mut context).unwrap();

        let after = sink.last().expect("test-only");
        assert_eq!(after.access_channel, Some(AccessChannel::Cloud));
        assert_eq!(after.context_code, 0, "认不出的 ConsumeContextType 保持 0");
        assert_eq!(
            encode(&after),
            render(
                "SubAfter\u{1}{REQ}\u{1}MSGID-C\u{1}{COST}\u{1}true\u{1}KeyA\u{1}0\u{2}",
                &after
            ),
            "CLOUD 通道不追加 timestamp / groupName 两段"
        );
        let fields = records(&encode(&after)).remove(0);
        assert_eq!(fields.len(), 7, "{fields:?}");

        // 名字串无法识别 => 编码效果与 LOCAL 相同（不等于 CLOUD 即是）
        let mut context = ConsumeMessageContext::new(
            "G",
            Some(vec![ext("T", "M", None, None, Some("r"), None, 1, 1, 0)]),
            None,
        );
        hook.consume_message_before(&mut context).unwrap();
        context.access_channel = Some("whatever".to_string());
        hook.consume_message_after(&mut context).unwrap();
        let after = sink.last().expect("test-only");
        assert_eq!(after.access_channel, None);
        assert_eq!(records(&encode(&after))[0].len(), 9);

        // props 为 None / 空 map / 没带这个键，contextCode 都是 0
        for props in [None, Some(HashMap::new())] {
            let mut context = ConsumeMessageContext::new(
                "G",
                Some(vec![ext("T", "M", None, None, Some("r"), None, 1, 1, 0)]),
                None,
            );
            hook.consume_message_before(&mut context).unwrap();
            context.props = props;
            hook.consume_message_after(&mut context).unwrap();
            assert_eq!(sink.last().expect("test-only").context_code, 0);
        }
    }

    #[test]
    fn consume_hooks_skip_degenerate_contexts() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = ConsumeMessageTraceHook::new(sink.clone());

        // 空 msg_list：before 连壳都不建
        let mut context = ConsumeMessageContext::new("GID_test", None, None);
        hook.consume_message_before(&mut context).unwrap();
        hook.consume_message_after(&mut context).unwrap();
        assert!(context.mq_trace_context.is_none());
        assert_eq!(sink.count(), 0);

        // 整批都被 TRACE_ON=false 挡掉：壳建了但 beans 为空，两条记录都不落
        let mut context = ConsumeMessageContext::new(
            "GID_test",
            Some(vec![ext("T", "M", None, None, Some("r"), Some("false"), 1, 1, 0)]),
            None,
        );
        hook.consume_message_before(&mut context).unwrap();
        let held = held_trace_context(&context.mq_trace_context).expect("test-only");
        assert_eq!(held.trace_type, Some(TraceType::SubBefore));
        assert!(held.trace_beans.is_empty());
        assert_eq!(sink.count(), 0);
        hook.consume_message_after(&mut context).unwrap();
        assert_eq!(sink.count(), 0, "没有 SubBefore bean 就不该有 SubAfter");

        // after 之前没跑 before
        let mut context = ConsumeMessageContext::new(
            "G",
            Some(vec![ext("T", "M", None, None, Some("r"), None, 1, 1, 0)]),
            None,
        );
        hook.consume_message_after(&mut context).unwrap();
        assert_eq!(sink.count(), 0);

        // 消息没有 MSG_REGION 属性 => region 为空串；
        // 没有 tags / keys => 空串，SubBefore 只剩 7 段（java_split 丢掉末尾空段）
        let mut context = ConsumeMessageContext::new(
            "G",
            Some(vec![ext("T", "M", None, None, None, None, 1, 1, 0)]),
            None,
        );
        hook.consume_message_before(&mut context).unwrap();
        let emitted = sink.last().expect("test-only");
        assert_eq!(emitted.region_id, "");
        assert_eq!(emitted.trace_beans[0].msg_id, "M");
        assert_eq!(records(&encode(&emitted))[0].len(), 7);
    }

    #[test]
    fn consume_before_keeps_retry_and_dlq_namespace_stripping() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = ConsumeMessageTraceHook::new(sink.clone());
        for (topic, want) in [
            ("%RETRY%NS1%GID_test", "%RETRY%GID_test"),
            ("%DLQ%NS1%GID_test", "%DLQ%GID_test"),
            ("NS1%TopicTest", "TopicTest"),
            ("TopicTest", "TopicTest"),
            // 系统资源不剥（rmq_sys_ 前缀），轨迹 topic 也一样被记录
            (CLOUD_TRACE_TOPIC, CLOUD_TRACE_TOPIC),
        ] {
            let mut context = ConsumeMessageContext::new(
                "NS1%GID_test",
                Some(vec![ext(topic, "M", None, None, Some("r"), None, 1, 1, 0)]),
                None,
            );
            hook.consume_message_before(&mut context).unwrap();
            let emitted = sink.last().expect("test-only");
            assert_eq!(emitted.trace_beans[0].topic, want, "topic = {topic}");
            assert_eq!(emitted.group_name, "GID_test");
        }
        assert_eq!(sink.count(), 5);
    }

    // ------------------------------------------------ 事务收尾钩子

    /// EndTransaction 记录金标准（时钟段留占位）。
    const GOLDEN_END_TX: &str = "EndTransaction\u{1}{TS}\u{1}NS1%cn-hangzhou\u{1}GID_test\
         \u{1}TopicTest\u{1}AC1400A1F0A018B4AAC2A1B2C3D4E5F6\u{1}TagA\u{1}KeyA KeyB\
         \u{1}127.0.0.1:10911\u{1}2\u{1}TRAN-001\u{1}COMMIT_MESSAGE\u{1}true\u{2}";

    /// 第二组 EndTransaction 金标准（region 空 => DefaultRegion、状态 UNKNOW）。
    const GOLDEN_END_TX_DEFAULT_REGION: &str =
        "EndTransaction\u{1}{TS}\u{1}DefaultRegion\u{1}GID_test\u{1}TopicTest\
         \u{1}AC1400A1F0A018B4AAC2A1B2C3D4E5F6\u{1}TagA\u{1}KeyA KeyB\u{1}127.0.0.1:10911\
         \u{1}2\u{1}TRAN-001\u{1}UNKNOW\u{1}true\u{2}";

    fn end_tx_context(message: Message, state: LocalTransactionState) -> EndTransactionContext {
        EndTransactionContext {
            producer_group: "NS1%GID_test".to_string(),
            message: Some(message),
            broker_addr: BROKER.to_string(),
            msg_id: Some(MSG_ID.to_string()),
            transaction_id: Some("TRAN-001".to_string()),
            transaction_state: Some(state),
            from_transaction_check: true,
            namespace: String::new(),
        }
    }

    #[test]
    fn end_transaction_record_matches_python_golden() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = EndTransactionTraceHook::new(sink.clone());
        let mut message = pub_message("NS1%TopicTest");
        message.put_property(PROPERTY_MSG_REGION, "NS1%cn-hangzhou");
        let mut context = end_tx_context(message, LocalTransactionState::CommitMessage);
        hook.end_transaction(&mut context).unwrap();

        let emitted = sink.last().expect("test-only");
        assert_eq!(sink.count(), 1);
        assert_eq!(
            encode(&emitted),
            render(GOLDEN_END_TX, &emitted),
            "actual = {:?}",
            encode(&emitted)
        );
        assert_eq!(
            transfer(&emitted).1,
            vec![MSG_ID.to_string(), "KeyA".to_string(), "KeyB".to_string()]
        );
        assert_eq!(emitted.trace_type, Some(TraceType::EndTransaction));
        let bean = &emitted.trace_beans[0];
        assert_eq!(bean.client_host, CLIENT_ID, "clientHost 取分发器的 clientId");
        assert_eq!(bean.msg_type, MessageType::TransMsgCommit, "msgType 硬编码为 2");
        assert_eq!(bean.transaction_state.as_deref(), Some("COMMIT_MESSAGE"));
        assert_eq!(bean.transaction_id.as_deref(), Some("TRAN-001"));
        assert!(bean.from_transaction_check, "broker 回查也要记");
        assert_eq!(bean.store_host, BROKER);
        assert_eq!(bean.body_length, 0, "EndTransaction 不填 bodyLength");
        assert_eq!(bean.store_time, 0);
        assert_eq!(emitted.region_id, "NS1%cn-hangzhou", "regionId 不剥命名空间");
        assert_eq!(emitted.group_name, "GID_test");
    }

    #[test]
    fn end_transaction_falls_back_to_default_region() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = EndTransactionTraceHook::new(sink.clone());
        let mut message = pub_message("NS1%TopicTest");
        message.put_property(PROPERTY_MSG_REGION, "");
        let mut context = end_tx_context(message, LocalTransactionState::Unknow);
        context.producer_group = "GID_test".to_string();
        hook.end_transaction(&mut context).unwrap();

        let emitted = sink.last().expect("test-only");
        assert_eq!(emitted.region_id, MixAll::DEFAULT_TRACE_REGION_ID);
        assert_eq!(
            encode(&emitted),
            render(GOLDEN_END_TX_DEFAULT_REGION, &emitted),
            "actual = {:?}",
            encode(&emitted)
        );
    }

    #[test]
    fn end_transaction_on_a_bare_message_writes_empty_state() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = EndTransactionTraceHook::new(sink.clone());
        let mut context = EndTransactionContext {
            producer_group: "GID_test".to_string(),
            message: Some(Message::new("TopicTest", None)),
            ..Default::default()
        };
        hook.end_transaction(&mut context).unwrap();
        let emitted = sink.last().expect("test-only");
        let fields = records(&encode(&emitted)).remove(0);
        assert_eq!(
            fields,
            vec![
                "EndTransaction".to_string(),
                emitted.time_stamp.to_string(),
                "DefaultRegion".to_string(),
                "GID_test".to_string(),
                "TopicTest".to_string(),
                "".to_string(),
                "".to_string(),
                "".to_string(),
                "".to_string(),
                "2".to_string(),
                "".to_string(),
                "".to_string(),
                "false".to_string()
            ]
        );
    }

    #[test]
    fn end_transaction_skips_trace_topic_and_bare_context() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = EndTransactionTraceHook::new(sink.clone());
        let mut context = EndTransactionContext {
            message: Some(Message::new(TRACE_TOPIC, Some(b"x"))),
            ..Default::default()
        };
        hook.end_transaction(&mut context).unwrap();
        // 云集群下换掉的 topic 同理
        let cloud = Arc::new(RecordingSink::new(CLOUD_TRACE_TOPIC, CLIENT_ID));
        let cloud_hook = EndTransactionTraceHook::new(cloud.clone());
        let mut context = EndTransactionContext {
            message: Some(Message::new(CLOUD_TRACE_TOPIC, Some(b"x"))),
            ..Default::default()
        };
        cloud_hook.end_transaction(&mut context).unwrap();
        // 没有 message
        let mut context = EndTransactionContext::default();
        hook.end_transaction(&mut context).unwrap();
        assert_eq!(sink.count(), 0, "轨迹 topic 与无 message 都不记");
        assert_eq!(cloud.count(), 0);
    }

    // ------------------------------------------------ 通道 / 工具

    #[test]
    fn hooks_are_shareable_and_records_keep_order() {
        // 三个钩子都要能塞进 HookList（`Arc<dyn ...Hook>` 要求 Send + Sync）
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let send: Arc<dyn SendMessageHook> = Arc::new(SendMessageTraceHook::new(sink.clone()));
        let consume: Arc<dyn ConsumeMessageHook> =
            Arc::new(ConsumeMessageTraceHook::new(sink.clone()));
        let end: Arc<dyn EndTransactionHook> =
            Arc::new(EndTransactionTraceHook::new(sink.clone()));

        let mut send_context = send_context("TopicTest");
        send.send_message_before(&mut send_context).unwrap();
        send_context.send_result = Some(ok_result());
        send.send_message_after(&mut send_context).unwrap();

        let mut consume_context = ConsumeMessageContext::new(
            "G",
            Some(vec![ext("T", "M", None, None, Some("r"), None, 1, 1, 0)]),
            None,
        );
        consume.consume_message_before(&mut consume_context).unwrap();
        consume.consume_message_after(&mut consume_context).unwrap();

        end.end_transaction(&mut end_tx_context(
            Message::new("T", Some(b"b")),
            LocalTransactionState::RollbackMessage,
        ))
        .unwrap();

        let types: Vec<Option<TraceType>> = sink
            .reported()
            .iter()
            .map(|ctx| ctx.trace_type)
            .collect();
        assert_eq!(
            types,
            vec![
                Some(TraceType::Pub),
                Some(TraceType::SubBefore),
                Some(TraceType::SubAfter),
                Some(TraceType::EndTransaction)
            ]
        );
        assert_eq!(sink.client_id(), CLIENT_ID.to_string());
        assert_eq!(sink.trace_topic_name(), TRACE_TOPIC.to_string());
        assert_eq!(
            records(&encode(&sink.reported()[3]))[0][11],
            "ROLLBACK_MESSAGE"
        );
    }

    #[test]
    fn helpers_and_constants() {
        assert_eq!(CONSUME_CONTEXT_TYPE, "ConsumeContextType");
        assert_eq!(strip_namespace("NS1%TopicTest"), "TopicTest");
        assert_eq!(strip_namespace(""), "");
        for (name, code) in [
            ("SUCCESS", 0),
            ("TIME_OUT", 1),
            ("EXCEPTION", 2),
            ("RETURNNULL", 3),
            ("FAILED", 4),
        ] {
            assert_eq!(consume_return_code(name), Some(code), "{name}");
        }
        assert_eq!(consume_return_code("0"), None);
        assert_eq!(consume_return_code("success"), None);
        assert_eq!(access_channel_of(None), None);
        assert_eq!(access_channel_of(Some("LOCAL")), Some(AccessChannel::Local));
        assert_eq!(access_channel_of(Some("CLOUD")), Some(AccessChannel::Cloud));
        assert_eq!(access_channel_of(Some("cloud")), None);
        assert_eq!(elapsed_per(1_000, 1_000, 0), 0, "count=0 兜底不 panic");
        assert_eq!(elapsed_per(1_100, 1_000, 0), 100);
    }
}
