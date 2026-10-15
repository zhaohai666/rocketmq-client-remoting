//! * [`SendMessageTraceHook`]    ← `trace.hook.SendMessageTraceHookImpl`
//! * [`ConsumeMessageTraceHook`] ← `trace.hook.ConsumeMessageTraceHookImpl`
//! * [`EndTransactionTraceHook`] ← `trace.hook.EndTransactionTraceHookImpl`
//!
//!
//! # Dispatcher dependency: [`TraceReportSink`]
//!
//! The three hooks depend on the dispatcher through **only three methods**
//! (`get_trace_topic_name()` / `append(ctx)` / `client_id()`). Batching, sending, and the
//! lifecycle all live in `client::trace_dispatcher` (ported as its own module), so this module only cuts a dependency-inversion seam:
//! this module declares [`TraceReportSink`] and does **not** stub the dispatcher, nor touch topic selection or batching.

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

/// Written into [`ConsumeMessageContext::props`] after the consumer delivers, with the value taken from
/// the **enum name** of [`ConsumeReturnType`](crate::client::result::ConsumeReturnType).
pub const CONSUME_CONTEXT_TYPE: &str = "ConsumeContextType";

/// Trace reporting channel -- the only outward dependency seam of this module.
///
/// **Implemented by `mq_client.rs` / `trace_dispatcher.rs`** (those two files actually hold the trace
/// topic, the internal trace producer, and the clientId); this module only declares it without implementing, and makes no assumptions about batching/sending
/// either -- the hooks just hand over one [`TraceContext`].
pub trait TraceReportSink: Send + Sync {
    /// The currently effective trace topic: [`MixAll::TRACE_TOPIC`](MixAll::TRACE_TOPIC), or,
    /// under `AccessChannel::Cloud`,
    /// [`TraceConstants::TRACE_TOPIC_PREFIX`](super::trace::TraceConstants::TRACE_TOPIC_PREFIX)
    /// `+ regionId`.
    fn trace_topic_name(&self) -> String;

    /// Hands over one trace record. Returning `false` means the dispatcher refused it (shutting down / queue full).
    fn report(&self, context: TraceContext) -> bool;

    /// The in-process clientId (`<ip>@<instanceName>`), written into the `clientHost` field of
    /// the EndTransaction trace record.
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

    /// Hands over the channel the hook holds.
    pub fn sink(&self) -> &Arc<dyn TraceReportSink> {
        &self.sink
    }
}

impl SendMessageHook for SendMessageTraceHook {
    fn hook_name(&self) -> &str {
        "SendMessageTraceHook"
    }

    /// Builds a [`TraceType::Pub`] [`TraceContext`] and hangs it on
    /// [`SendMessageContext::mq_trace_context`]; `after` picks it back up to fill in msgId and the rest.
    /// Only the fields "known before sending" are filled here (topic / tags / keys / storeHost / bodyLength /
    /// msgType); `msgId` is left empty for `after` to overwrite from the SendResult.
    fn send_message_before(&self, context: &mut SendMessageContext) -> Result<()> {
        // Return directly when context or message is missing
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

    /// Skip conditions are pinned one by one (order included): no message / is the trace topic /
    /// before never built a context / no sendResult / broker side `TRACE_ON=false` or no `MSG_REGION` returned /
    /// empty beans.
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

/// The SubBefore / SubAfter pair **shares one `request_id`** (`after` copies it from the context
/// hung by `before`); it is the only clue the console has for linking the two halves of one consumption.
#[derive(Clone)]
pub struct ConsumeMessageTraceHook {
    sink: Arc<dyn TraceReportSink>,
}

impl ConsumeMessageTraceHook {
    pub fn new(sink: Arc<dyn TraceReportSink>) -> Self {
        ConsumeMessageTraceHook { sink }
    }

    /// Hands over the channel the hook holds.
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
        // Hang the (still empty-shell) context before the beans are built:
        // when not a single message was recorded, `consume_message_after` still finds it and skips because beans is empty.
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
            // A missing or empty region property is written as an empty string, and overwrites the previous message's value
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
        // sub_after shares the same beans with sub_before (they were the same object to begin with);
        // nobody mutates them after append, so cloning is equivalent.
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

/// The broker's transaction check-back (`fromTransactionCheck=true`) also goes through here, so both "client-initiated commit"
/// and "commit after check-back" show up in the trace.
#[derive(Clone)]
pub struct EndTransactionTraceHook {
    sink: Arc<dyn TraceReportSink>,
}

impl EndTransactionTraceHook {
    pub fn new(sink: Arc<dyn TraceReportSink>) -> Self {
        EndTransactionTraceHook { sink }
    }

    /// Hands over the channel the hook holds.
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
            // Fall back to the default region when region is empty
            _ => MixAll::DEFAULT_TRACE_REGION_ID.to_string(),
        };
        trace_context.trace_beans = vec![bean];
        trace_context.time_stamp = util_all::current_time_millis();
        report(&self.sink, trace_context);
        Ok(())
    }
}

// ------------------------------------------------------------------ internal utilities

/// Strips the namespace prefix (namespace treated as an empty string).
///
/// `NS1%TopicTest` → `TopicTest`; `%RETRY%NS1%GID_test` → `%RETRY%GID_test`;
/// system resources (`rmq_sys_` / `CID_RMQ_SYS_` prefixes) are returned as-is.
fn strip_namespace(resource: &str) -> String {
    NamespaceUtil::without_namespace(resource, "")
}

/// Retrieves the [`TraceContext`] this module stored from the `Object` slot of [`crate::client::hook`].
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

/// Hands over one record and syncs it back into `mq_trace_context` (the caller reads the object hung there,
/// so after the hooks run the caller must see the "filled-in" copy).
fn publish(
    sink: &Arc<dyn TraceReportSink>,
    holder: &mut Option<AnyHolder>,
    context: TraceContext,
) {
    *holder = Some(Arc::new(context.clone()));
    report(sink, context);
}

/// Hands over one record (for when it does not need to be hung back onto the context).
fn report(sink: &Arc<dyn TraceReportSink>, context: TraceContext) {
    let type_name = context.trace_type.map(TraceType::name).unwrap_or_default();
    if !sink.report(context) {
        rmq_debug!("trace sink refused a {type_name} record");
    }
}

/// Callers already check for `count` == 0; this is one more fallback so a division by zero cannot panic.
fn elapsed_per(now: i64, start: i64, count: usize) -> i32 {
    let divisor = i64::try_from(count).unwrap_or(1).max(1);
    ((now - start) / divisor) as i32
}

fn half_floor(cost_time: i32) -> i64 {
    i64::from(cost_time.div_euclid(2))
}

/// [`ConsumeMessageContext::access_channel`] stores a name string (see difference note 3 in that module's header),
/// converted back to [`AccessChannel`] here for the encoder to judge.
///
/// The field itself is optional: `None` → the encoder treats it as LOCAL;
/// `"CLOUD"` → CLOUD; every other value differs from CLOUD and encodes the same as LOCAL --
/// so unrecognized names map to `None`, keeping the byte output identical to the wire baseline.
fn access_channel_of(name: Option<&str>) -> Option<AccessChannel> {
    match name? {
        "CLOUD" => Some(AccessChannel::Cloud),
        "LOCAL" => Some(AccessChannel::Local),
        _ => None,
    }
}

/// [`crate::client::result::ConsumeReturnType`] has no `from_name` (callers all hold the strongly typed enum),
/// so the name → existing-enum mapping is done inside this module instead of redefining the enum;
/// unknown names return `None`, and the caller keeps `contextCode = 0` accordingly.
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

/// Recording sink for tests: stores the [`TraceContext`] handed over by the hooks as-is; topic / clientId are configurable.
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

    /// A recording sink that always "refuses" (dispatcher stopped / queue full).
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

    /// Substitutes the placeholders in the golden template with actual values.
    ///
    /// The three clock-derived numbers (time_stamp / cost_time / store_time) and `request_id` cannot
    /// possibly match across the two sides (Rust uses the real clock + a random uniqId), so the template keeps placeholders; their **value rules**
    /// are pinned separately by tests such as `pub_cost_and_store_time_follows_golden_rules`.
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

    /// Splits one `trans_data` into "records -> fields".
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

    /// One pulled message.
    ///
    /// The parameter list mirrors the golden test case field by field; collapsing parameters would hurt readability.
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

    /// A fixed batch of consumed messages (the third one is blocked by `TRACE_ON=false`).
    fn scenario_msgs_for_reference_vector() -> Vec<MessageExt> {
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

    // ------------------------------------------------ send hooks

    /// Golden Pub record (the two clock segments left as placeholders).
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
        // the channel held by the hook can be retrieved
        assert_eq!(send.sink().trace_topic_name(), TRACE_TOPIC.to_string());
        assert_eq!(consume.sink().client_id(), CLIENT_ID.to_string());
        assert_eq!(end.sink().trace_topic_name(), TRACE_TOPIC.to_string());
    }

    #[test]
    fn send_pub_record_matches_golden() {
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
        // the copy hung on the context must equal the one handed over (the same object)
        assert_eq!(held_trace_context(&context.mq_trace_context), Some(emitted));
    }

    #[test]
    fn send_before_fills_only_pre_send_fields() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = SendMessageTraceHook::new(sink.clone());
        let mut context = send_context("NS1%TopicTest");
        hook.send_message_before(&mut context).unwrap();

        let held = held_trace_context(&context.mq_trace_context).expect("test-only");
        assert_eq!(sink.count(), 0, "before does not report");
        assert_eq!(held.trace_type, Some(TraceType::Pub));
        assert_eq!(held.group_name, "GID_test", "producerGroup has NS1% stripped");
        assert_eq!(held.region_id, "", "region waits for the SendResult to fill it in");
        assert!(held.is_success, "isSuccess defaults to true");
        assert_eq!(held.cost_time, 0);
        let bean = &held.trace_beans[0];
        assert_eq!(bean.topic, "TopicTest");
        assert_eq!(bean.tags, "TagA");
        assert_eq!(bean.keys, "KeyA KeyB");
        assert_eq!(bean.store_host, BROKER);
        assert_eq!(bean.body_length, 42);
        assert_eq!(bean.msg_type, MessageType::NormalMsg);
        assert_eq!(bean.msg_id, "", "msgId waits for after");
        assert_eq!(bean.offset_msg_id, "");

        // A bare message without tags / keys / body: missing property => empty string
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
        // Regular cluster: RMQ_SYS_TRACE_TOPIC
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = SendMessageTraceHook::new(sink.clone());
        let mut context = send_context(TRACE_TOPIC);
        hook.send_message_before(&mut context).unwrap();
        context.send_result = Some(ok_result());
        hook.send_message_after(&mut context).unwrap();
        assert!(context.mq_trace_context.is_none());
        assert_eq!(sink.count(), 0);

        // Cloud cluster: rmq_sys_TRACE_DATA_<region>
        let cloud = Arc::new(RecordingSink::new(CLOUD_TRACE_TOPIC, CLIENT_ID));
        let hook = SendMessageTraceHook::new(cloud.clone());
        let mut context = send_context(CLOUD_TRACE_TOPIC);
        hook.send_message_before(&mut context).unwrap();
        hook.send_message_after(&mut context).unwrap();
        assert!(context.mq_trace_context.is_none());
        assert_eq!(cloud.count(), 0);

        // When the dispatcher has no topic configured (empty prefix) the trace-topic prefix check is always true =>
        // the whole trace path is switched off
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
        assert_eq!(sink.count(), 0, "TRACE_ON=false does not report");
        assert!(context.mq_trace_context.is_some(), "the shell on the context is still there");

        // MSG_REGION missing
        let mut context = send_context("TopicTest");
        hook.send_message_before(&mut context).unwrap();
        let mut result = ok_result();
        result.set_region_id(None);
        context.send_result = Some(result);
        hook.send_message_after(&mut context).unwrap();
        assert_eq!(sink.count(), 0, "missing MSG_REGION does not report");

        // No sendResult at all (ONEWAY / exception path)
        let mut context = send_context("TopicTest");
        hook.send_message_before(&mut context).unwrap();
        hook.send_message_after(&mut context).unwrap();
        assert_eq!(sink.count(), 0);

        // after without before having run
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
        assert!(!emitted.is_success, "not SEND_OK => success=false");
        let fields = records(&encode(&emitted)).remove(0);
        assert_eq!(fields.len(), 14);
        assert_eq!(fields[0], "Pub");
        assert_eq!(fields[4], "TopicTest");
        assert_eq!(fields[5], "M2");
        assert_eq!(fields[12], "O2");
        assert_eq!(fields[13], "false");
    }

    #[test]
    fn pub_cost_and_store_time_follows_golden_rules() {
        // Amortized cost (now*1000 - ts) / len(beans) rounds toward zero; storeTime = ts + cost/2 rounds down
        assert_eq!(elapsed_per(1_700_000_000_042, 1_700_000_000_000, 1), 42);
        assert_eq!(elapsed_per(1_700_000_000_042, 1_700_000_000_000, 2), 21);
        assert_eq!(elapsed_per(1_700_000_000_000, 1_700_000_000_000, 3), 0);
        // Clock skew backwards: rounding toward zero gives -3, rounding down gives -2
        assert_eq!(elapsed_per(1_699_999_999_997, 1_700_000_000_000, 1), -3);
        assert_eq!(half_floor(-3), -2, "half_floor rounds down: -3/2 == -2");
        assert_eq!(half_floor(-4), -2);
        assert_eq!(half_floor(3), 1);
        assert_eq!(half_floor(42000), 21000);
        // the expected storeTime under the clock-skew scenario
        assert_eq!(1_700_000_000_000 + half_floor(-3), 1_699_999_999_998);

        // running the real hook must satisfy the same relation
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

    /// Recomputes storeTime independently with the same rounding rules (no dependence on hook internals).
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

    // ------------------------------------------------ consume hooks

    /// Golden SubBefore, two segments (clock / uniqId segments left as placeholders).
    const GOLDEN_SUB_BEFORE: &str = "SubBefore\u{1}{TS}\u{1}cn-shanghai\u{1}GID_test\
         \u{1}{REQ}\u{1}MSGID-1\u{1}0\u{1}KeyA KeyB\
         \u{2}SubBefore\u{1}{TS}\u{1}cn-shanghai\u{1}GID_test\
         \u{1}{REQ}\u{1}MSGID-2\u{1}3\u{1}KeyC\u{2}";

    /// Golden SubAfter, two segments (LOCAL channel, 9 fields).
    const GOLDEN_SUB_AFTER: &str = "SubAfter\u{1}{REQ}\u{1}MSGID-1\u{1}{COST}\u{1}false\
         \u{1}KeyA KeyB\u{1}3\u{1}{TS}\u{1}GID_test\
         \u{2}SubAfter\u{1}{REQ}\u{1}MSGID-2\u{1}{COST}\u{1}false\
         \u{1}KeyC\u{1}3\u{1}{TS}\u{1}GID_test\u{2}";

    #[test]
    fn consume_before_and_after_records_match_golden() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = ConsumeMessageTraceHook::new(sink.clone());
        let mut context = consume_context(scenario_msgs_for_reference_vector());
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
        assert_eq!(before.region_id, "cn-shanghai", "the last non-skipped message wins");
        assert!(before.time_stamp > 0);
        assert_eq!(before.trace_beans.len(), 2, "the TRACE_ON=false message is not recorded");
        assert_eq!(before.trace_beans[0].topic, "TopicTest");
        assert_eq!(before.trace_beans[0].store_time, 1_700_000_000_111);
        assert_eq!(before.trace_beans[0].body_length, 100);
        assert_eq!(before.trace_beans[1].topic, "%RETRY%GID_test", "the %RETRY% prefix is kept");
        assert_eq!(before.trace_beans[1].retry_times, 3);
        assert_eq!(before.trace_beans[1].body_length, 200);

        hook.consume_message_after(&mut context).unwrap();
        let after = sink.last().expect("test-only");
        assert_eq!(sink.count(), 2, "one record each for before / after");
        assert_eq!(
            encode(&after),
            render(GOLDEN_SUB_AFTER, &after),
            "actual = {:?}",
            encode(&after)
        );
        assert_eq!(after.trace_type, Some(TraceType::SubAfter));
        assert_eq!(after.request_id, before.request_id, "the two records share request_id");
        assert_eq!(after.region_id, before.region_id);
        assert_eq!(after.context_code, ConsumeReturnType::ReturnNull.code());
        assert!(!after.is_success);
        assert_eq!(after.access_channel, Some(AccessChannel::Local));
        assert_eq!(after.trace_beans, before.trace_beans, "beans come from the same source as SubBefore");
        assert_eq!(records(&encode(&after)).len(), 2);
        // the context still holds the SubBefore copy (after only created a new SubAfter)
        assert_eq!(
            held_trace_context(&context.mq_trace_context).map(|ctx| ctx.trace_type),
            Some(Some(TraceType::SubBefore))
        );
    }

    #[test]
    fn consume_after_cost_time_divides_whole_batch() {
        let sink = Arc::new(RecordingSink::new(TRACE_TOPIC, CLIENT_ID));
        let hook = ConsumeMessageTraceHook::new(sink.clone());
        let mut context = consume_context(scenario_msgs_for_reference_vector());
        hook.consume_message_before(&mut context).unwrap();
        let before = sink.last().expect("test-only");
        assert_eq!(before.trace_beans.len(), 2, "the third message is blocked by TRACE_ON");
        // Wind the SubBefore timestamp back by 3 seconds (the hook's own private slot; equivalent to "this batch started
        // consuming 3 seconds ago"), then check whether costTime divides by 3 or by 2
        let mut stale = before.clone();
        stale.time_stamp -= 3000;
        context.mq_trace_context = Some(Arc::new(stale));
        hook.consume_message_after(&mut context).unwrap();
        let after = sink.last().expect("test-only");
        assert!(
            (1000..1200).contains(&after.cost_time),
            "cost = {} should be 3000/3, not 3000/2=1500",
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
        // Cloud channel + unrecognized return type; SubBefore is one segment
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
        assert_eq!(after.context_code, 0, "an unrecognized ConsumeContextType keeps 0");
        assert_eq!(
            encode(&after),
            render(
                "SubAfter\u{1}{REQ}\u{1}MSGID-C\u{1}{COST}\u{1}true\u{1}KeyA\u{1}0\u{2}",
                &after
            ),
            "the CLOUD channel appends no timestamp / groupName segments"
        );
        let fields = records(&encode(&after)).remove(0);
        assert_eq!(fields.len(), 7, "{fields:?}");

        // unrecognized name string => encodes the same as LOCAL (anything not equal to CLOUD)
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

        // props None / empty map / key absent: contextCode is 0 in all cases
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

        // empty msg_list: before does not even build the shell
        let mut context = ConsumeMessageContext::new("GID_test", None, None);
        hook.consume_message_before(&mut context).unwrap();
        hook.consume_message_after(&mut context).unwrap();
        assert!(context.mq_trace_context.is_none());
        assert_eq!(sink.count(), 0);

        // the whole batch blocked by TRACE_ON=false: the shell is built but beans is empty, neither record reports
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
        assert_eq!(sink.count(), 0, "no SubAfter without a SubBefore bean");

        // after without before having run
        let mut context = ConsumeMessageContext::new(
            "G",
            Some(vec![ext("T", "M", None, None, Some("r"), None, 1, 1, 0)]),
            None,
        );
        hook.consume_message_after(&mut context).unwrap();
        assert_eq!(sink.count(), 0);

        // message without the MSG_REGION property => region is an empty string;
        // no tags / keys => empty strings, SubBefore is left with 7 fields (java_split drops the trailing empty one)
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
            // system resources are not stripped (rmq_sys_ prefix), and the trace topic is recorded like any other
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

    // ------------------------------------------------ end-transaction hooks

    /// Golden EndTransaction record (the clock segment left as a placeholder).
    const GOLDEN_END_TX: &str = "EndTransaction\u{1}{TS}\u{1}NS1%cn-hangzhou\u{1}GID_test\
         \u{1}TopicTest\u{1}AC1400A1F0A018B4AAC2A1B2C3D4E5F6\u{1}TagA\u{1}KeyA KeyB\
         \u{1}127.0.0.1:10911\u{1}2\u{1}TRAN-001\u{1}COMMIT_MESSAGE\u{1}true\u{2}";

    /// Second EndTransaction golden record (empty region => DefaultRegion, state UNKNOW).
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
    fn end_transaction_record_matches_golden() {
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
        assert_eq!(bean.client_host, CLIENT_ID, "clientHost takes the dispatcher's clientId");
        assert_eq!(bean.msg_type, MessageType::TransMsgCommit, "msgType is hardcoded to 2");
        assert_eq!(bean.transaction_state.as_deref(), Some("COMMIT_MESSAGE"));
        assert_eq!(bean.transaction_id.as_deref(), Some("TRAN-001"));
        assert!(bean.from_transaction_check, "broker check-backs are recorded too");
        assert_eq!(bean.store_host, BROKER);
        assert_eq!(bean.body_length, 0, "EndTransaction leaves bodyLength empty");
        assert_eq!(bean.store_time, 0);
        assert_eq!(emitted.region_id, "NS1%cn-hangzhou", "regionId keeps its namespace");
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
        // same for the swapped topic on a cloud cluster
        let cloud = Arc::new(RecordingSink::new(CLOUD_TRACE_TOPIC, CLIENT_ID));
        let cloud_hook = EndTransactionTraceHook::new(cloud.clone());
        let mut context = EndTransactionContext {
            message: Some(Message::new(CLOUD_TRACE_TOPIC, Some(b"x"))),
            ..Default::default()
        };
        cloud_hook.end_transaction(&mut context).unwrap();
        // no message
        let mut context = EndTransactionContext::default();
        hook.end_transaction(&mut context).unwrap();
        assert_eq!(sink.count(), 0, "neither the trace topic nor a missing message records anything");
        assert_eq!(cloud.count(), 0);
    }

    // ------------------------------------------------ channel / utilities

    #[test]
    fn hooks_are_shareable_and_records_keep_order() {
        // all three hooks must fit into the HookList (`Arc<dyn ...Hook>` requires Send + Sync)
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
        assert_eq!(elapsed_per(1_000, 1_000, 0), 0, "count=0 fallback must not panic");
        assert_eq!(elapsed_per(1_100, 1_000, 0), 100);
    }
}
