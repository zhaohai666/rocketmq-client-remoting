# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import List, Optional

from common.message import Message, MessageExt, MessageQueue
from common.message_type import MessageType


class SendMessageContext:
    pass

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
    pass

    def hook_name(self) -> str:
        raise NotImplementedError

    def send_message_before(self, context: SendMessageContext) -> None:
        raise NotImplementedError

    def send_message_after(self, context: SendMessageContext) -> None:
        raise NotImplementedError


class ConsumeMessageContext:
    pass

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
    pass

    def hook_name(self) -> str:
        raise NotImplementedError

    def consume_message_before(self, context: ConsumeMessageContext) -> None:
        raise NotImplementedError

    def consume_message_after(self, context: ConsumeMessageContext) -> None:
        raise NotImplementedError


class EndTransactionContext:
    pass

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
    pass

    def hook_name(self) -> str:
        raise NotImplementedError

    def end_transaction(self, context: EndTransactionContext) -> None:
        raise NotImplementedError


class CommunicationMode:
    pass

    SYNC = "SYNC"
    ASYNC = "ASYNC"
    ONEWAY = "ONEWAY"


class CheckForbiddenContext:
    pass

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
    pass

    def hook_name(self) -> str:
        raise NotImplementedError

    def check_forbidden(self, context: CheckForbiddenContext) -> None:
        raise NotImplementedError


class FilterMessageContext:
    pass

    def __init__(self, consumer_group: str = "", msg_list: Optional[List[MessageExt]] = None,
                 mq: Optional[MessageQueue] = None) -> None:
        self.consumer_group = consumer_group
        self.msg_list: List[MessageExt] = list(msg_list) if msg_list else []
        self.mq = mq
        self.arg = None
        self.unit_mode: bool = False


class FilterMessageHook:
    pass

    def hook_name(self) -> str:
        raise NotImplementedError

    def filter_message(self, context: FilterMessageContext) -> None:
        raise NotImplementedError


__all__ = ["CommunicationMode",
           "SendMessageContext", "SendMessageHook", "ConsumeMessageContext",
           "ConsumeMessageHook", "EndTransactionContext", "EndTransactionHook",
           "CheckForbiddenContext", "CheckForbiddenHook",
           "FilterMessageContext", "FilterMessageHook"]
