// OpenTracing-style hook trio for the message lifecycle.
//
//! Port of the Java client's `SendMessageOpenTracingHookImpl` /
//! `ConsumeMessageOpenTracingHookImpl` / `EndTransactionOpenTracingHookImpl`
//! (the same trio the PHP port carries as `OpenTracingHook.php`), on top of a
//! duck-typed Tracer SPI with zero third-party deps.
//!
//! The in-flight span rides in the context's `mq_trace_context` slot, exactly
//! like the Java original (`context.setMqTraceContext(span)`): before opens
//! the span and stashes it, after reads it back and closes it. The stash is
//! an [`OTSpanHolder`] / [`OTConsumeSpanHolder`] wrapped in the context's
//! `AnyHolder`, so a context carrying some other hook's state is skipped
//! rather than misinterpreted -- registering both the trace dispatcher hooks
//! and these hooks on the same producer is not supported (same restriction as
//! Java).
//!
//! Cross-process propagation rides on the message properties themselves: the
//! upstream context is extracted from the property map and the just-started
//! span's context is injected back into it (the context owns the Message, so
//! the injector's writes travel in-band with the message).

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::client::hook::{
    AnyHolder, ConsumeMessageContext, ConsumeMessageHook, EndTransactionContext,
    EndTransactionHook, SendMessageContext, SendMessageHook,
};
#[cfg(test)]
use crate::client::result::LocalTransactionState;
use crate::client::result::SendStatus;
use crate::client::trace::TraceConstants;
#[cfg(test)]
use crate::common::message::MessageExt;
use crate::common::message_const::PROPERTY_MSG_REGION;
use crate::common::message_type::MessageType;
use crate::remoting::protocol::ext_fields::StringMap;
use crate::remoting::protocol::namespace_util::NamespaceUtil;

// OpenTracing standard tags (io.opentracing.tag.Tags).
pub const OT_SPAN_KIND: &str = "span.kind";
pub const OT_SPAN_KIND_PRODUCER: &str = "producer";
pub const OT_SPAN_KIND_CONSUMER: &str = "consumer";
pub const OT_PEER_SERVICE: &str = "peer.service";
pub const OT_MESSAGE_BUS_DESTINATION: &str = "message_bus.destination";

// The rocketmq.* tag keys: the Java TraceConstants literals (the mgs_id /
// mgs_type spellings are the Java protocol literals -- the typo is
// load-bearing, do not "fix" it).
pub const OT_ROCKETMQ_SERVICE: &str = "rocketmq";
pub const OT_ROCKETMQ_SUCCESS: &str = "rocketmq.success";
pub const OT_ROCKETMQ_TAGS: &str = "rocketmq.tags";
pub const OT_ROCKETMQ_KEYS: &str = "rocketmq.keys";
pub const OT_ROCKETMQ_STORE_HOST: &str = "rocketmq.store_host";
pub const OT_ROCKETMQ_BODY_LENGTH: &str = "rocketmq.body_length";
pub const OT_ROCKETMQ_MSG_ID: &str = "rocketmq.mgs_id";
pub const OT_ROCKETMQ_MSG_TYPE: &str = "rocketmq.mgs_type";
pub const OT_ROCKETMQ_REGION_ID: &str = "rocketmq.region_id";
pub const OT_ROCKETMQ_TRANSACTION_ID: &str = "rocketmq.transaction_id";
pub const OT_ROCKETMQ_TRANSACTION_STATE: &str = "rocketmq.transaction_state";
pub const OT_ROCKETMQ_IS_FROM_TRANSACTION_CHECK: &str = "rocketmq.is_from_transaction_check";
pub const OT_ROCKETMQ_RETRY_TIMERS: &str = "rocketmq.retry_times";

// The transaction leg reports the commit state only.
pub const OT_TRANS_MSG_COMMIT: &str = "Trans_msg_Commit";

/// Duck-typed span context: an opaque object the tracer's inject understands.
pub trait OpenTracingSpanContext: Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Duck-typed span. Tag values arrive as strings (the hook formats ints and
/// bools: "true"/"false", lengths as decimal), keeping the SPI trivially
/// implementable without a boxed-value surface. Interior mutability is up to
/// the implementation -- the hook calls these through `&self`.
pub trait OpenTracingSpan: Send + Sync {
    fn set_tag(&self, key: &str, value: &str);
    fn context(&self) -> Option<Arc<dyn OpenTracingSpanContext>>;
    fn finish(&self);
}

pub type OTTagMap = BTreeMap<String, String>;

/// Duck-typed tracer:
///   - `start_span` opens a span; `refs` carries the upstream context (from
///     `extract`) when there is one;
///   - `extract` reads the upstream context from the message property map
///     (`None` means "no upstream");
///   - `inject` writes the propagation keys of the just-started span into the
///     carrier, which the hook writes back onto the message.
pub trait OpenTracingTracer: Send + Sync {
    fn start_span(
        &self,
        operation: &str,
        tags: &OTTagMap,
        refs: Vec<Arc<dyn OpenTracingSpanContext>>,
    ) -> Arc<dyn OpenTracingSpan>;
    fn extract(&self, carrier: &StringMap) -> Option<Arc<dyn OpenTracingSpanContext>>;
    fn inject(&self, span_context: &Arc<dyn OpenTracingSpanContext>, carrier: &mut StringMap);
}

// TraceContext-substitute holders carried through the context's mq_trace_context
// slot (the rust slot is an AnyHolder, so the type marker is the holder struct).
pub struct OTSpanHolder {
    pub span: Arc<dyn OpenTracingSpan>,
}

pub struct OTConsumeSpanHolder {
    pub spans: Vec<Arc<dyn OpenTracingSpan>>,
}

/// Java parity: the tag value is the MessageType mapped to the Java enum
/// *name* (NORMAL_MSG...).
pub fn ot_msg_type_name(msg_type: MessageType) -> &'static str {
    match msg_type {
        MessageType::TransMsgHalf => "TRANS_MSG_HALF",
        MessageType::TransMsgCommit => "TRANS_MSG_COMMIT",
        MessageType::DelayMsg => "DELAY_MSG",
        MessageType::OrderMsg => "ORDER_MSG",
        MessageType::NormalMsg => "NORMAL_MSG",
    }
}

fn ot_bool_str(b: bool) -> &'static str {
    if b {
        "true"
    } else {
        "false"
    }
}

fn ot_extract_upstream(
    tracer: &dyn OpenTracingTracer,
    props: &StringMap,
) -> Option<Arc<dyn OpenTracingSpanContext>> {
    tracer.extract(props)
}

/// Writes the just-started span's context back onto the message properties:
/// the downstream leg must parent onto THIS send, not onto whatever upstream
/// produced the incoming context. The context owns the Message, so the
/// injector's writes land on it directly; a failing inject never breaks the
/// send.
fn ot_inject_into_message(
    tracer: &dyn OpenTracingTracer,
    span: &Arc<dyn OpenTracingSpan>,
    msg: &mut crate::common::message::Message,
) {
    if let Some(sc) = span.context() {
        try_inject(tracer, &sc, &mut msg.properties);
    }
}

fn try_inject(
    tracer: &dyn OpenTracingTracer,
    sc: &Arc<dyn OpenTracingSpanContext>,
    carrier: &mut StringMap,
) {
    tracer.inject(sc, carrier);
}

// ---------------------------------------------------------------- send leg

/// Send leg: span "To_<topic>", kind producer, with the kind + service +
/// destination + tags + keys + store_host + msg_type + body_length tags at
/// open; success / msg_id / region_id at close. No result, or no region id on
/// it: the span is left open, exactly like the Java close-out flow.
pub struct SendMessageOpenTracingHook {
    pub tracer: Arc<dyn OpenTracingTracer>,
}

impl SendMessageHook for SendMessageOpenTracingHook {
    fn hook_name(&self) -> &str {
        "SendMessageOpenTracingHook"
    }

    fn send_message_before(&self, context: &mut SendMessageContext) -> crate::error::Result<()> {
        let Some(msg) = context.message.as_mut() else {
            return Ok(());
        };
        let upstream = ot_extract_upstream(self.tracer.as_ref(), &msg.properties);
        let mut tags: OTTagMap = BTreeMap::new();
        tags.insert(OT_SPAN_KIND.to_string(), OT_SPAN_KIND_PRODUCER.to_string());
        let refs: Vec<Arc<dyn OpenTracingSpanContext>> = upstream.clone().into_iter().collect();
        let span = self.tracer.start_span(
            &format!("{}{}", TraceConstants::TO_PREFIX, msg.get_topic()),
            &tags,
            refs,
        );
        span.set_tag(OT_PEER_SERVICE, OT_ROCKETMQ_SERVICE);
        span.set_tag(OT_MESSAGE_BUS_DESTINATION, msg.get_topic());
        span.set_tag(OT_ROCKETMQ_TAGS, msg.get_tags().unwrap_or(""));
        span.set_tag(OT_ROCKETMQ_KEYS, msg.get_keys().unwrap_or(""));
        span.set_tag(OT_ROCKETMQ_STORE_HOST, &context.broker_addr);
        span.set_tag(OT_ROCKETMQ_MSG_TYPE, ot_msg_type_name(context.msg_type));
        span.set_tag(OT_ROCKETMQ_BODY_LENGTH, &msg.get_body().len().to_string());
        ot_inject_into_message(self.tracer.as_ref(), &span, msg);
        context.mq_trace_context = Some(Arc::new(OTSpanHolder { span }) as AnyHolder);
        Ok(())
    }

    fn send_message_after(&self, context: &mut SendMessageContext) -> crate::error::Result<()> {
        let Some(holder) =
            crate::client::hook::any_downcast::<OTSpanHolder>(&context.mq_trace_context)
        else {
            return Ok(());
        };
        // No result, or no region id on it: nothing to report, span stays open.
        let Some(sr) = context.send_result.as_ref() else {
            return Ok(());
        };
        if sr.region_id.is_none() {
            return Ok(());
        }
        let span = holder.span.clone();
        span.set_tag(
            OT_ROCKETMQ_SUCCESS,
            ot_bool_str(sr.status == SendStatus::SendOk),
        );
        span.set_tag(OT_ROCKETMQ_MSG_ID, sr.msg_id.as_deref().unwrap_or(""));
        span.set_tag(OT_ROCKETMQ_REGION_ID, sr.region_id.as_deref().unwrap_or(""));
        span.finish();
        Ok(())
    }
}

// ---------------------------------------------------------------- consume leg

/// Consume leg: one span per message, "From_<topic>", kind consumer; msg_id /
/// tags / keys / body_length (store size) / retry_times / region_id at open;
/// success at close.
pub struct ConsumeMessageOpenTracingHook {
    pub tracer: Arc<dyn OpenTracingTracer>,
}

impl ConsumeMessageHook for ConsumeMessageOpenTracingHook {
    fn hook_name(&self) -> &str {
        "ConsumeMessageOpenTracingHook"
    }

    fn consume_message_before(
        &self,
        context: &mut ConsumeMessageContext,
    ) -> crate::error::Result<()> {
        if context.msg_list.is_empty() {
            return Ok(());
        }
        let mut spans: Vec<Arc<dyn OpenTracingSpan>> = Vec::with_capacity(context.msg_list.len());
        for msg in context.msg_list.iter() {
            let upstream = ot_extract_upstream(self.tracer.as_ref(), &msg.properties);
            let mut tags: OTTagMap = BTreeMap::new();
            tags.insert(OT_SPAN_KIND.to_string(), OT_SPAN_KIND_CONSUMER.to_string());
            let refs: Vec<Arc<dyn OpenTracingSpanContext>> = upstream.clone().into_iter().collect();
            let span = self.tracer.start_span(
                &format!("{}{}", TraceConstants::FROM_PREFIX, msg.get_topic()),
                &tags,
                refs,
            );
            span.set_tag(OT_PEER_SERVICE, OT_ROCKETMQ_SERVICE);
            span.set_tag(
                OT_MESSAGE_BUS_DESTINATION,
                &NamespaceUtil::without_namespace(msg.get_topic(), ""),
            );
            span.set_tag(OT_ROCKETMQ_MSG_ID, msg.get_msg_id().unwrap_or(""));
            span.set_tag(OT_ROCKETMQ_TAGS, msg.get_tags().unwrap_or(""));
            span.set_tag(OT_ROCKETMQ_KEYS, msg.get_keys().unwrap_or(""));
            span.set_tag(OT_ROCKETMQ_BODY_LENGTH, &msg.store_size.to_string());
            span.set_tag(OT_ROCKETMQ_RETRY_TIMERS, &msg.reconsume_times.to_string());
            span.set_tag(
                OT_ROCKETMQ_REGION_ID,
                msg.get_property(PROPERTY_MSG_REGION).unwrap_or(""),
            );
            spans.push(span);
        }
        context.mq_trace_context = Some(Arc::new(OTConsumeSpanHolder { spans }) as AnyHolder);
        Ok(())
    }

    fn consume_message_after(
        &self,
        context: &mut ConsumeMessageContext,
    ) -> crate::error::Result<()> {
        let Some(holder) =
            crate::client::hook::any_downcast::<OTConsumeSpanHolder>(&context.mq_trace_context)
        else {
            return Ok(());
        };
        for span in holder.spans.clone() {
            span.set_tag(OT_ROCKETMQ_SUCCESS, ot_bool_str(context.success));
            span.finish();
        }
        Ok(())
    }
}

// ------------------------------------------------------------ transaction leg

/// Transaction leg: span "EndTransaction" with the transaction identity keys,
/// closed immediately. The message is left untouched on this leg (the send
/// side already put the context on the properties).
pub struct EndTransactionOpenTracingHook {
    pub tracer: Arc<dyn OpenTracingTracer>,
}

impl EndTransactionHook for EndTransactionOpenTracingHook {
    fn hook_name(&self) -> &str {
        "EndTransactionOpenTracingHook"
    }

    fn end_transaction(&self, context: &mut EndTransactionContext) -> crate::error::Result<()> {
        let upstream = context
            .message
            .as_ref()
            .and_then(|msg| ot_extract_upstream(self.tracer.as_ref(), &msg.properties));
        let mut tags: OTTagMap = BTreeMap::new();
        tags.insert(OT_SPAN_KIND.to_string(), OT_SPAN_KIND_PRODUCER.to_string());
        let refs: Vec<Arc<dyn OpenTracingSpanContext>> = upstream.into_iter().collect();
        let span = self
            .tracer
            .start_span(TraceConstants::END_TRANSACTION, &tags, refs);
        span.set_tag(OT_PEER_SERVICE, OT_ROCKETMQ_SERVICE);
        span.set_tag(
            OT_MESSAGE_BUS_DESTINATION,
            context
                .message
                .as_ref()
                .map(|m| m.get_topic())
                .unwrap_or(""),
        );
        span.set_tag(
            OT_ROCKETMQ_TAGS,
            context
                .message
                .as_ref()
                .and_then(|m| m.get_tags())
                .unwrap_or(""),
        );
        span.set_tag(
            OT_ROCKETMQ_KEYS,
            context
                .message
                .as_ref()
                .and_then(|m| m.get_keys())
                .unwrap_or(""),
        );
        span.set_tag(OT_ROCKETMQ_STORE_HOST, &context.broker_addr);
        span.set_tag(OT_ROCKETMQ_MSG_ID, context.msg_id.as_deref().unwrap_or(""));
        span.set_tag(OT_ROCKETMQ_MSG_TYPE, OT_TRANS_MSG_COMMIT);
        span.set_tag(
            OT_ROCKETMQ_TRANSACTION_ID,
            context.transaction_id.as_deref().unwrap_or(""),
        );
        span.set_tag(
            OT_ROCKETMQ_TRANSACTION_STATE,
            context
                .transaction_state
                .map(|s| s.to_string())
                .unwrap_or_default()
                .as_str(),
        );
        span.set_tag(
            OT_ROCKETMQ_IS_FROM_TRANSACTION_CHECK,
            ot_bool_str(context.from_transaction_check),
        );
        span.finish();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use crate::common::message::Message;

    struct FakeSpanContext {
        id: u64,
    }

    impl OpenTracingSpanContext for FakeSpanContext {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    struct FakeSpanInner {
        operation: String,
        tags: BTreeMap<String, String>,
        refs: Vec<Arc<dyn OpenTracingSpanContext>>,
        finished: bool,
    }

    struct FakeSpan {
        inner: Mutex<FakeSpanInner>,
        ctx_id: u64,
    }

    impl OpenTracingSpan for FakeSpan {
        fn set_tag(&self, key: &str, value: &str) {
            self.inner
                .lock()
                .unwrap()
                .tags
                .insert(key.to_string(), value.to_string());
        }

        fn context(&self) -> Option<Arc<dyn OpenTracingSpanContext>> {
            Some(Arc::new(FakeSpanContext { id: self.ctx_id }))
        }

        fn finish(&self) {
            self.inner.lock().unwrap().finished = true;
        }
    }

    impl FakeSpan {
        fn snapshot(&self) -> (String, BTreeMap<String, String>, bool) {
            let g = self.inner.lock().unwrap();
            (g.operation.clone(), g.tags.clone(), g.finished)
        }
    }

    struct FakeTracer {
        spans: Mutex<Vec<Arc<FakeSpan>>>,
        injected: Mutex<Vec<u64>>,
        upstream: Option<u64>,
    }

    impl FakeTracer {
        fn new(upstream: Option<u64>) -> Self {
            FakeTracer {
                spans: Mutex::new(Vec::new()),
                injected: Mutex::new(Vec::new()),
                upstream,
            }
        }
    }

    impl OpenTracingTracer for FakeTracer {
        fn start_span(
            &self,
            operation: &str,
            tags: &OTTagMap,
            refs: Vec<Arc<dyn OpenTracingSpanContext>>,
        ) -> Arc<dyn OpenTracingSpan> {
            let mut ids = Vec::new();
            for r in &refs {
                if let Some(c) = r.as_any().downcast_ref::<FakeSpanContext>() {
                    ids.push(c.id);
                }
            }
            let ctx_id = ids.first().copied().unwrap_or(self.upstream.unwrap_or(0));
            let span = Arc::new(FakeSpan {
                inner: Mutex::new(FakeSpanInner {
                    operation: operation.to_string(),
                    tags: tags.clone(),
                    refs,
                    finished: false,
                }),
                ctx_id,
            });
            self.spans.lock().unwrap().push(span.clone());
            span
        }

        fn extract(&self, _carrier: &StringMap) -> Option<Arc<dyn OpenTracingSpanContext>> {
            self.upstream
                .map(|id| Arc::new(FakeSpanContext { id }) as Arc<dyn OpenTracingSpanContext>)
        }

        fn inject(&self, span_context: &Arc<dyn OpenTracingSpanContext>, carrier: &mut StringMap) {
            let id = span_context
                .as_any()
                .downcast_ref::<FakeSpanContext>()
                .map(|c| c.id)
                .unwrap_or(0);
            carrier.insert("traceparent", "fake-trace-context");
            self.injected.lock().unwrap().push(id);
        }
    }

    fn tag(span: &Arc<FakeSpan>, key: &str) -> String {
        span.inner
            .lock()
            .unwrap()
            .tags
            .get(key)
            .cloned()
            .unwrap_or_default()
    }

    fn make_send_msg() -> Message {
        let mut msg = Message::new("TopicTest", Some(b"hello-world"));
        msg.set_user_property("TAGS", "TagA");
        msg.set_user_property("KEYS", "KeyA");
        msg
    }

    fn make_ext() -> MessageExt {
        let mut m = MessageExt::new();
        m.set_topic("TopicTest");
        m.set_body(Some(b"xxxxxxxxxx"));
        m.set_msg_id(Some("EXT-MSG-ID"));
        m.store_size = 42;
        m.reconsume_times = 2;
        m.properties.insert(PROPERTY_MSG_REGION, "DefaultRegion");
        m.properties.insert("TAGS", "TagC");
        m.properties.insert("KEYS", "KeyC");
        m
    }

    fn hook_of(tracer: &Arc<FakeTracer>) -> Arc<FakeSpan> {
        tracer.spans.lock().unwrap()[0].clone()
    }

    // ---------------------------------------------------------- send leg

    #[test]
    fn send_before_opens_producer_span_with_documented_tags() {
        let tracer = Arc::new(FakeTracer::new(None));
        let hook = SendMessageOpenTracingHook {
            tracer: tracer.clone(),
        };
        let mut ctx = SendMessageContext::default();
        ctx.message = Some(make_send_msg());
        ctx.broker_addr = "127.0.0.1:10911".into();
        ctx.msg_type = MessageType::NormalMsg;
        hook.send_message_before(&mut ctx).unwrap();

        assert_eq!(tracer.spans.lock().unwrap().len(), 1);
        let span = hook_of(&tracer);
        let (operation, _, finished) = span.snapshot();
        assert_eq!(operation, "To_TopicTest");
        assert!(!finished);
        assert_eq!(tag(&span, OT_SPAN_KIND), OT_SPAN_KIND_PRODUCER);
        assert_eq!(tag(&span, OT_PEER_SERVICE), "rocketmq");
        assert_eq!(tag(&span, OT_MESSAGE_BUS_DESTINATION), "TopicTest");
        assert_eq!(tag(&span, OT_ROCKETMQ_TAGS), "TagA");
        assert_eq!(tag(&span, OT_ROCKETMQ_KEYS), "KeyA");
        assert_eq!(tag(&span, OT_ROCKETMQ_STORE_HOST), "127.0.0.1:10911");
        assert_eq!(tag(&span, OT_ROCKETMQ_MSG_TYPE), "NORMAL_MSG");
        assert_eq!(tag(&span, OT_ROCKETMQ_BODY_LENGTH), "11");
        // the span rides in the mq_trace_context slot (Java setMqTraceContext)
        assert!(crate::client::hook::any_downcast::<OTSpanHolder>(&ctx.mq_trace_context).is_some());
    }

    #[test]
    fn send_after_closes_with_success_msg_id_region_id() {
        let tracer = Arc::new(FakeTracer::new(None));
        let hook = SendMessageOpenTracingHook {
            tracer: tracer.clone(),
        };
        let mut ctx = SendMessageContext::default();
        ctx.message = Some(make_send_msg());
        hook.send_message_before(&mut ctx).unwrap();
        ctx.send_result = Some(crate::client::result::SendResult {
            status: SendStatus::SendOk,
            msg_id: Some("MSG-ID-1".into()),
            region_id: Some("DefaultRegion".into()),
            ..Default::default()
        });
        hook.send_message_after(&mut ctx).unwrap();

        let span = hook_of(&tracer);
        assert_eq!(tag(&span, OT_ROCKETMQ_SUCCESS), "true");
        assert_eq!(tag(&span, OT_ROCKETMQ_MSG_ID), "MSG-ID-1");
        assert_eq!(tag(&span, OT_ROCKETMQ_REGION_ID), "DefaultRegion");
        assert!(span.snapshot().2);
    }

    #[test]
    fn send_after_no_result_or_region_id_leaves_span_open() {
        let tracer = Arc::new(FakeTracer::new(None));
        let hook = SendMessageOpenTracingHook {
            tracer: tracer.clone(),
        };
        let mut ctx = SendMessageContext::default();
        ctx.message = Some(make_send_msg());
        hook.send_message_before(&mut ctx).unwrap();
        hook.send_message_after(&mut ctx).unwrap();
        assert!(!hook_of(&tracer).snapshot().2);
        ctx.send_result = Some(crate::client::result::SendResult {
            status: SendStatus::SendOk,
            msg_id: Some("M".into()),
            region_id: None,
            ..Default::default()
        });
        hook.send_message_after(&mut ctx).unwrap();
        assert!(!hook_of(&tracer).snapshot().2);
    }

    #[test]
    fn send_after_failure_reports_success_false() {
        let tracer = Arc::new(FakeTracer::new(None));
        let hook = SendMessageOpenTracingHook {
            tracer: tracer.clone(),
        };
        let mut ctx = SendMessageContext::default();
        ctx.message = Some(make_send_msg());
        hook.send_message_before(&mut ctx).unwrap();
        ctx.send_result = Some(crate::client::result::SendResult {
            status: SendStatus::FlushDiskTimeout,
            msg_id: Some("M".into()),
            region_id: Some("R".into()),
            ..Default::default()
        });
        hook.send_message_after(&mut ctx).unwrap();
        assert_eq!(tag(&hook_of(&tracer), OT_ROCKETMQ_SUCCESS), "false");
        assert!(hook_of(&tracer).snapshot().2);
    }

    #[test]
    fn send_injects_span_context_into_message_properties() {
        let tracer = Arc::new(FakeTracer::new(None));
        let hook = SendMessageOpenTracingHook {
            tracer: tracer.clone(),
        };
        let mut ctx = SendMessageContext::default();
        ctx.message = Some(make_send_msg());
        hook.send_message_before(&mut ctx).unwrap();

        let msg = ctx.message.as_ref().unwrap();
        assert_eq!(
            msg.properties.get("traceparent"),
            Some("fake-trace-context")
        );
        assert_eq!(tracer.injected.lock().unwrap().len(), 1);
    }

    #[test]
    fn send_upstream_context_becomes_ref() {
        let tracer = Arc::new(FakeTracer::new(Some(99)));
        let hook = SendMessageOpenTracingHook {
            tracer: tracer.clone(),
        };
        let mut ctx = SendMessageContext::default();
        ctx.message = Some(make_send_msg());
        hook.send_message_before(&mut ctx).unwrap();
        let span = hook_of(&tracer);
        assert_eq!(span.inner.lock().unwrap().refs.len(), 1);
        assert_eq!(span.ctx_id, 99);
    }

    #[test]
    fn send_msg_type_tag_uses_java_enum_name() {
        assert_eq!(ot_msg_type_name(MessageType::NormalMsg), "NORMAL_MSG");
        assert_eq!(
            ot_msg_type_name(MessageType::TransMsgHalf),
            "TRANS_MSG_HALF"
        );
        assert_eq!(
            ot_msg_type_name(MessageType::TransMsgCommit),
            "TRANS_MSG_COMMIT"
        );
        assert_eq!(ot_msg_type_name(MessageType::DelayMsg), "DELAY_MSG");
        assert_eq!(ot_msg_type_name(MessageType::OrderMsg), "ORDER_MSG");
    }

    // ---------------------------------------------------------- consume leg

    #[test]
    fn consume_before_opens_one_span_per_message() {
        let tracer = Arc::new(FakeTracer::new(None));
        let hook = ConsumeMessageOpenTracingHook {
            tracer: tracer.clone(),
        };
        let mut ctx =
            ConsumeMessageContext::new("CID_test", Some(vec![make_ext(), make_ext()]), None);
        hook.consume_message_before(&mut ctx).unwrap();

        let spans = tracer.spans.lock().unwrap().clone();
        assert_eq!(spans.len(), 2);
        for span in &spans {
            let (operation, _, finished) = span.snapshot();
            assert_eq!(operation, "From_TopicTest");
            assert!(!finished);
            assert_eq!(tag(span, OT_SPAN_KIND), OT_SPAN_KIND_CONSUMER);
            assert_eq!(tag(span, OT_PEER_SERVICE), "rocketmq");
            assert_eq!(tag(span, OT_MESSAGE_BUS_DESTINATION), "TopicTest");
            assert_eq!(tag(span, OT_ROCKETMQ_MSG_ID), "EXT-MSG-ID");
            assert_eq!(tag(span, OT_ROCKETMQ_TAGS), "TagC");
            assert_eq!(tag(span, OT_ROCKETMQ_KEYS), "KeyC");
            assert_eq!(tag(span, OT_ROCKETMQ_BODY_LENGTH), "42");
            assert_eq!(tag(span, OT_ROCKETMQ_RETRY_TIMERS), "2");
            assert_eq!(tag(span, OT_ROCKETMQ_REGION_ID), "DefaultRegion");
        }
        let holder =
            crate::client::hook::any_downcast::<OTConsumeSpanHolder>(&ctx.mq_trace_context)
                .expect("consume spans stashed");
        assert_eq!(holder.spans.len(), 2);
    }

    #[test]
    fn consume_after_tags_success_and_finishes_all() {
        let tracer = Arc::new(FakeTracer::new(None));
        let hook = ConsumeMessageOpenTracingHook {
            tracer: tracer.clone(),
        };
        let mut ctx = ConsumeMessageContext::new("CID_test", Some(vec![make_ext()]), None);
        hook.consume_message_before(&mut ctx).unwrap();
        ctx.success = false;
        hook.consume_message_after(&mut ctx).unwrap();
        let span = hook_of(&tracer);
        assert_eq!(tag(&span, OT_ROCKETMQ_SUCCESS), "false");
        assert!(span.snapshot().2);
    }

    #[test]
    fn consume_empty_msg_list_is_noop() {
        let tracer = Arc::new(FakeTracer::new(None));
        let hook = ConsumeMessageOpenTracingHook {
            tracer: tracer.clone(),
        };
        let mut ctx = ConsumeMessageContext::new("CID_test", None, None);
        hook.consume_message_before(&mut ctx).unwrap();
        hook.consume_message_after(&mut ctx).unwrap();
        assert!(tracer.spans.lock().unwrap().is_empty());
        assert!(ctx.mq_trace_context.is_none());
    }

    // ------------------------------------------------------ transaction leg

    #[test]
    fn end_transaction_opens_and_closes_immediately() {
        let tracer = Arc::new(FakeTracer::new(None));
        let hook = EndTransactionOpenTracingHook {
            tracer: tracer.clone(),
        };
        let mut ctx = EndTransactionContext::default();
        ctx.message = Some(make_send_msg());
        ctx.broker_addr = "127.0.0.1:10911".into();
        ctx.msg_id = Some("TX-MSG-ID".into());
        ctx.transaction_id = Some("TX-ID-9".into());
        ctx.transaction_state = Some(LocalTransactionState::CommitMessage);
        ctx.from_transaction_check = true;
        hook.end_transaction(&mut ctx).unwrap();

        assert_eq!(tracer.spans.lock().unwrap().len(), 1);
        let span = hook_of(&tracer);
        let (operation, _, finished) = span.snapshot();
        assert_eq!(operation, "EndTransaction");
        assert!(finished);
        assert_eq!(tag(&span, OT_SPAN_KIND), OT_SPAN_KIND_PRODUCER);
        assert_eq!(tag(&span, OT_MESSAGE_BUS_DESTINATION), "TopicTest");
        assert_eq!(tag(&span, OT_ROCKETMQ_TAGS), "TagA");
        assert_eq!(tag(&span, OT_ROCKETMQ_KEYS), "KeyA");
        assert_eq!(tag(&span, OT_ROCKETMQ_STORE_HOST), "127.0.0.1:10911");
        assert_eq!(tag(&span, OT_ROCKETMQ_MSG_ID), "TX-MSG-ID");
        assert_eq!(tag(&span, OT_ROCKETMQ_MSG_TYPE), OT_TRANS_MSG_COMMIT);
        assert_eq!(tag(&span, OT_ROCKETMQ_TRANSACTION_ID), "TX-ID-9");
        assert_eq!(tag(&span, OT_ROCKETMQ_TRANSACTION_STATE), "COMMIT_MESSAGE");
        assert_eq!(tag(&span, OT_ROCKETMQ_IS_FROM_TRANSACTION_CHECK), "true");
    }

    #[test]
    fn end_transaction_without_message_still_opens_span() {
        let tracer = Arc::new(FakeTracer::new(None));
        let hook = EndTransactionOpenTracingHook {
            tracer: tracer.clone(),
        };
        let mut ctx = EndTransactionContext::default();
        ctx.msg_id = Some("TX".into());
        ctx.transaction_state = Some(LocalTransactionState::RollbackMessage);
        hook.end_transaction(&mut ctx).unwrap();
        let span = hook_of(&tracer);
        assert_eq!(tag(&span, OT_MESSAGE_BUS_DESTINATION), "");
        assert_eq!(
            tag(&span, OT_ROCKETMQ_TRANSACTION_STATE),
            "ROLLBACK_MESSAGE"
        );
        assert!(span.snapshot().2);
    }

    #[test]
    fn hook_names_match_java_impls() {
        assert_eq!(
            SendMessageOpenTracingHook {
                tracer: Arc::new(FakeTracer::new(None))
            }
            .hook_name(),
            "SendMessageOpenTracingHook"
        );
        assert_eq!(
            ConsumeMessageOpenTracingHook {
                tracer: Arc::new(FakeTracer::new(None))
            }
            .hook_name(),
            "ConsumeMessageOpenTracingHook"
        );
        assert_eq!(
            EndTransactionOpenTracingHook {
                tracer: Arc::new(FakeTracer::new(None))
            }
            .hook_name(),
            "EndTransactionOpenTracingHook"
        );
    }
}
