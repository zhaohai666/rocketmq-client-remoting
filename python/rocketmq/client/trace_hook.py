# -*- coding: utf-8 -*-
"""消息轨迹钩子（对应 org.apache.rocketmq.client.trace.hook 包）。

  * ``SendMessageTraceHook``    ← SendMessageTraceHookImpl
  * ``ConsumeMessageTraceHook`` ← ConsumeMessageTraceHookImpl

两条硬性约定：
  1. **轨迹消息本身不再被追踪**：before/after 都先看 topic 是否以轨迹 topic 开头，
     是则直接 return（否则轨迹会自我复制）。
  2. **是否落轨迹由 broker 说了算**：发送侧看 SendResult 的 ``region_id`` /
     ``trace_on``（由 SEND 响应头的 MSG_REGION / TRACE_ON 解析而来，broker 默认
     traceOn=true）；消费侧看消息属性 TRACE_ON 是否为 "false"。
"""
from __future__ import annotations

import time

from ..common.message_const import MessageConst
from ..common.message_type import MessageType
from ..common.mix_all import MixAll
from ..logging import get_logger
from ..remoting.protocol.namespace_util import NamespaceUtil
from .hook import ConsumeMessageContext, ConsumeMessageHook, SendMessageContext, SendMessageHook
from .send_result import SendStatus
from .trace import TraceBean, TraceConstants, TraceContext, TraceType

logger = get_logger()


class SendMessageTraceHook(SendMessageHook):
    """发送侧轨迹钩子。"""

    def __init__(self, local_dispatcher) -> None:
        self.local_dispatcher = local_dispatcher

    def hook_name(self) -> str:
        return "SendMessageTraceHook"

    def send_message_before(self, context: SendMessageContext) -> None:
        if context is None or context.message is None:
            return
        topic = context.message.topic or ""
        if topic.startswith(self.local_dispatcher.get_trace_topic_name()):
            return
        trace_context = TraceContext()
        context.mq_trace_context = trace_context
        trace_context.trace_type = TraceType.PUB
        trace_context.group_name = NamespaceUtil.without_namespace(context.producer_group)
        bean = TraceBean()
        bean.topic = NamespaceUtil.without_namespace(topic)
        bean.tags = context.message.get_tags() or ""
        bean.keys = context.message.get_keys() or ""
        bean.store_host = context.broker_addr or ""
        body = context.message.get_body()
        bean.body_length = len(body) if body else 0
        bean.msg_type = context.msg_type
        trace_context.trace_beans = [bean]

    def send_message_after(self, context: SendMessageContext) -> None:
        if context is None or context.message is None:
            return
        topic = context.message.topic or ""
        if topic.startswith(self.local_dispatcher.get_trace_topic_name()):
            return
        if context.mq_trace_context is None:
            return
        if context.send_result is None:
            return
        result = context.send_result
        # broker 侧 traceOn=false 或没带回 region 时不落库（对齐 Java）
        if result.region_id is None or not result.trace_on:
            return
        trace_context: TraceContext = context.mq_trace_context
        if not trace_context.trace_beans:
            return
        bean = trace_context.trace_beans[0]
        cost_time = int((time.time() * 1000 - trace_context.time_stamp)
                        / len(trace_context.trace_beans))
        trace_context.cost_time = cost_time
        trace_context.is_success = result.send_status == SendStatus.SEND_OK
        trace_context.region_id = result.region_id
        bean.msg_id = result.msg_id or ""
        bean.offset_msg_id = result.offset_msg_id or ""
        bean.store_time = trace_context.time_stamp + cost_time // 2
        self.local_dispatcher.append(trace_context)


class ConsumeMessageTraceHook(ConsumeMessageHook):
    """消费侧轨迹钩子（SubBefore / SubAfter 两条记录共用同一个 request_id）。"""

    def __init__(self, local_dispatcher) -> None:
        self.local_dispatcher = local_dispatcher

    def hook_name(self) -> str:
        return "ConsumeMessageTraceHook"

    def consume_message_before(self, context: ConsumeMessageContext) -> None:
        if context is None or not context.msg_list:
            return
        trace_context = TraceContext()
        context.mq_trace_context = trace_context
        trace_context.trace_type = TraceType.SUB_BEFORE
        trace_context.group_name = NamespaceUtil.without_namespace(context.consumer_group)
        beans = []
        for msg in context.msg_list:
            if msg is None:
                continue
            region_id = msg.get_property(MessageConst.PROPERTY_MSG_REGION)
            trace_on = msg.get_property(MessageConst.PROPERTY_TRACE_SWITCH)
            if trace_on is not None and trace_on == "false":
                continue
            bean = TraceBean()
            bean.topic = NamespaceUtil.without_namespace(msg.topic)
            bean.msg_id = msg.msg_id or ""
            bean.tags = msg.get_tags() or ""
            bean.keys = msg.get_keys() or ""
            bean.store_time = msg.store_timestamp
            bean.body_length = msg.store_size
            bean.retry_times = msg.reconsume_times
            trace_context.region_id = region_id or ""
            beans.append(bean)
        if beans:
            trace_context.trace_beans = beans
            trace_context.time_stamp = int(time.time() * 1000)
            self.local_dispatcher.append(trace_context)

    def consume_message_after(self, context: ConsumeMessageContext) -> None:
        if context is None or not context.msg_list:
            return
        sub_before: TraceContext = context.mq_trace_context
        if sub_before is None or not sub_before.trace_beans:
            return
        sub_after = TraceContext()
        sub_after.trace_type = TraceType.SUB_AFTER
        sub_after.region_id = sub_before.region_id
        sub_after.group_name = NamespaceUtil.without_namespace(sub_before.group_name)
        sub_after.request_id = sub_before.request_id
        sub_after.access_channel = context.access_channel
        sub_after.is_success = context.success
        sub_after.cost_time = int((time.time() * 1000 - sub_before.time_stamp)
                                  / len(context.msg_list))
        sub_after.trace_beans = sub_before.trace_beans
        props = context.props
        if props:
            # props["ConsumeContextType"] 是 ConsumeReturnType 的**名字**（Java 用
            # ConsumeReturnType.valueOf(contextType) 按名查），其 ordinal 就是轨迹里的
            # contextCode（Java ConsumeMessageTraceHookImpl:113）。
            # ⚠ 必须用 [name] 查名，不能写 ConsumeReturnType(context_type)（那按值查，
            # 值是 0..4 的整数，传名字必然 ValueError → 静默退化成 SUCCESS）。
            from .consumer_result import ConsumeReturnType
            context_type = props.get("ConsumeContextType")
            if context_type is not None:
                try:
                    sub_after.context_code = ConsumeReturnType[context_type].value
                except KeyError:
                    pass
        self.local_dispatcher.append(sub_after)


class EndTransactionTraceHook:
    """事务收尾轨迹钩子（对应 EndTransactionTraceHookImpl）。

    broker 的事务回查（fromTransactionCheck=true）也会走这里，
    所以「客户端主动提交」和「回查后提交」两种情况都能在轨迹里看到。
    """

    def __init__(self, local_dispatcher) -> None:
        self.local_dispatcher = local_dispatcher

    def hook_name(self) -> str:
        return "EndTransactionTraceHook"

    def end_transaction(self, context) -> None:
        if context is None or context.message is None:
            return
        topic = context.message.topic or ""
        if topic.startswith(self.local_dispatcher.get_trace_topic_name()):
            return
        msg = context.message
        tuxe_context = TraceContext()
        tuxe_context.trace_type = TraceType.END_TRANSACTION
        tuxe_context.group_name = NamespaceUtil.without_namespace(context.producer_group)
        bean = TraceBean()
        bean.topic = NamespaceUtil.without_namespace(topic)
        bean.tags = msg.get_tags() or ""
        bean.keys = msg.get_keys() or ""
        bean.store_host = context.broker_addr or ""
        bean.msg_type = MessageType.TRANS_MSG_COMMIT
        bean.client_host = self.local_dispatcher._client_id()
        bean.msg_id = context.msg_id or ""
        bean.transaction_state = context.transaction_state
        bean.transaction_id = context.transaction_id
        bean.from_transaction_check = context.from_transaction_check
        region_id = msg.get_property(MessageConst.PROPERTY_MSG_REGION)
        tuxe_context.region_id = region_id if region_id else MixAll.DEFAULT_TRACE_REGION_ID
        tuxe_context.trace_beans = [bean]
        tuxe_context.time_stamp = int(time.time() * 1000)
        self.local_dispatcher.append(tuxe_context)


__all__ = ["SendMessageTraceHook", "ConsumeMessageTraceHook", "EndTransactionTraceHook",
           "TraceConstants"]
