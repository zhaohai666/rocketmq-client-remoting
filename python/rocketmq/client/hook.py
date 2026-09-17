# -*- coding: utf-8 -*-
"""客户端钩子（对应 org.apache.rocketmq.client.hook 包）。

Java 侧钩子是「业务无关的切面」：生产者在 sendKernelImpl 前后各调一次
SendMessageHook，消费者在投递 listener 前后各调一次 ConsumeMessageHook。
消息轨迹（client.trace.hook.*）正是建在这两个接口上的。

设计约定（与 Java 一致）：
  * 钩子抛出的异常**必须被吞掉并记 warn**（DefaultMQProducerImpl:1159/1172），
    绝不能因为轨迹出错影响正常收发；
  * **唯二的例外**：``CheckForbiddenHook``（发送前拦截，Java 签名就是
    ``throws MQClientException``，用于"禁止发送"）——它的异常要向上传播；
  * ``mq_trace_context`` 是钩子自己的私有状态：before 写入、after 取出，
    中间不允许别的钩子依赖它的具体类型。
"""
from __future__ import annotations

from typing import List, Optional

from ..common.message import Message, MessageExt, MessageQueue
from ..common.message_type import MessageType


class SendMessageContext:
    """对应 org.apache.rocketmq.client.hook.SendMessageContext。"""

    def __init__(self) -> None:
        self.producer = None
        self.producer_group: str = ""
        self.message: Optional[Message] = None
        self.mq: Optional[MessageQueue] = None
        self.broker_addr: str = ""
        self.born_host: str = ""
        self.communication_mode = None
        self.send_result = None
        self.exception: Optional[BaseException] = None
        self.mq_trace_context = None
        self.props = None
        self.msg_type: MessageType = MessageType.NORMAL_MSG
        self.namespace: str = ""


class SendMessageHook:
    """对应 org.apache.rocketmq.client.hook.SendMessageHook。"""

    def hook_name(self) -> str:
        raise NotImplementedError

    def send_message_before(self, context: SendMessageContext) -> None:
        raise NotImplementedError

    def send_message_after(self, context: SendMessageContext) -> None:
        raise NotImplementedError


class ConsumeMessageContext:
    """对应 org.apache.rocketmq.client.hook.ConsumeMessageContext。"""

    def __init__(self, consumer_group: str = "", msg_list: Optional[List[MessageExt]] = None,
                 mq: Optional[MessageQueue] = None) -> None:
        self.consumer_group = consumer_group
        self.msg_list: List[MessageExt] = list(msg_list) if msg_list else []
        self.mq = mq
        self.success: bool = True
        self.status: Optional[str] = None
        self.mq_trace_context = None
        self.props = None
        self.access_channel = None


class ConsumeMessageHook:
    """对应 org.apache.rocketmq.client.hook.ConsumeMessageHook。"""

    def hook_name(self) -> str:
        raise NotImplementedError

    def consume_message_before(self, context: ConsumeMessageContext) -> None:
        raise NotImplementedError

    def consume_message_after(self, context: ConsumeMessageContext) -> None:
        raise NotImplementedError


class EndTransactionContext:
    """对应 org.apache.rocketmq.client.hook.EndTransactionContext。"""

    def __init__(self) -> None:
        self.producer_group: str = ""
        self.message: Optional[Message] = None
        self.broker_addr: str = ""
        self.msg_id: Optional[str] = None
        self.transaction_id: Optional[str] = None
        self.transaction_state = None
        self.from_transaction_check: bool = False
        self.namespace: str = ""


class EndTransactionHook:
    """对应 org.apache.rocketmq.client.hook.EndTransactionHook。"""

    def hook_name(self) -> str:
        raise NotImplementedError

    def end_transaction(self, context: EndTransactionContext) -> None:
        raise NotImplementedError


class CommunicationMode:
    """对应 org.apache.rocketmq.client.impl.CommunicationMode（Java 是枚举，三个常量）。"""

    SYNC = "SYNC"
    ASYNC = "ASYNC"
    ONEWAY = "ONEWAY"


class CheckForbiddenContext:
    """对应 org.apache.rocketmq.client.hook.CheckForbiddenContext。

    发送前拦截钩子的上下文。与 SendMessageContext 的关键差别：**没有 sendResult**
    （此刻还没发），带上 ``arg``（send(msg, selector, arg) 里的业务参数）。
    """

    def __init__(self) -> None:
        self.name_srv_addr: str = ""
        self.group: str = ""
        self.message: Optional[Message] = None
        self.mq: Optional[MessageQueue] = None
        self.broker_addr: str = ""
        self.communication_mode = None
        self.send_result = None
        self.exception: Optional[BaseException] = None
        self.arg = None
        self.unit_mode: bool = False


class CheckForbiddenHook:
    """对应 org.apache.rocketmq.client.hook.CheckForbiddenHook。

    ⚠ 与 Send/Consume 钩子**相反**：``check_forbidden`` 抛出的异常**不会被吞掉**
    （Java 签名就是 ``throws MQClientException``），而是沿发送重试链向上传播 ——
    这正是"拦截"能力的实现方式。
    """

    def hook_name(self) -> str:
        raise NotImplementedError

    def check_forbidden(self, context: CheckForbiddenContext) -> None:
        raise NotImplementedError


class FilterMessageContext:
    """对应 org.apache.rocketmq.client.hook.FilterMessageContext。

    ``msg_list`` 是**可变的**：钩子把它替换/裁剪掉的消息会被客户端直接丢弃
    （拉取路径 = 静默跳过；POP 路径 = 立刻 ack）。
    """

    def __init__(self, consumer_group: str = "", msg_list: Optional[List[MessageExt]] = None,
                 mq: Optional[MessageQueue] = None) -> None:
        self.consumer_group = consumer_group
        self.msg_list: List[MessageExt] = list(msg_list) if msg_list else []
        self.mq = mq
        self.arg = None
        self.unit_mode: bool = False


class FilterMessageHook:
    """对应 org.apache.rocketmq.client.hook.FilterMessageHook。"""

    def hook_name(self) -> str:
        raise NotImplementedError

    def filter_message(self, context: FilterMessageContext) -> None:
        raise NotImplementedError


__all__ = ["CommunicationMode",
           "SendMessageContext", "SendMessageHook", "ConsumeMessageContext",
           "ConsumeMessageHook", "EndTransactionContext", "EndTransactionHook",
           "CheckForbiddenContext", "CheckForbiddenHook",
           "FilterMessageContext", "FilterMessageHook"]
