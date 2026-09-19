# -*- coding: utf-8 -*-
"""客户端异常（对应 org.apache.rocketmq.client.exception.*）。"""
from __future__ import annotations

from typing import Optional


class ClientErrorCode:
    """对应 org.apache.rocketmq.client.exception.ClientErrorCode。

    sendDefaultImpl 重试耗尽后用它给最终的 MQClientException 定性：
    连不上 broker→10001，等响应超时→10002，客户端自身问题→10003，
    地址服务器没给地址→10004，路由查不到→10005。
    """

    CONNECT_BROKER_EXCEPTION = 10001
    ACCESS_BROKER_TIMEOUT = 10002
    BROKER_NOT_EXIST_EXCEPTION = 10003
    NO_NAME_SERVER_EXCEPTION = 10004
    NOT_FOUND_TOPIC_EXCEPTION = 10005


class MQClientException(Exception):
    """对应 MQClientException。response_code 非 0 时为 broker 侧错误码。"""

    def __init__(self, message: str = "", response_code: Optional[int] = None, cause: Optional[Exception] = None):
        super().__init__(message)
        self.response_code = response_code
        self.cause = cause

    def __repr__(self):
        return "MQClientException(code=%s, msg=%s)" % (self.response_code, str(self))


class MQBrokerException(Exception):
    """对应 MQBrokerException：broker 返回非 SUCCESS 码。"""

    def __init__(self, response_code: int, error_message: str = ""):
        super().__init__(error_message)
        self.response_code = response_code
        self.error_message = error_message

    def __repr__(self):
        return "MQBrokerException(code=%s, msg=%s)" % (self.response_code, self.error_message)


class MQTimeOutException(MQClientException):
    pass


class MQQueueException(MQClientException):
    pass


class RequestTimeoutException(MQClientException):
    """对应 Java ``RequestTimeoutException extends MQClientException``。

    Request-Reply 专用语义：请求消息**已经发成功**，但在超时窗口内没等到应答。
    与「发送本身失败」（会抛 MQClientException / RemotingException）区分开 ——
    前者可能只是应答方没回，消息其实已经投递；后者连 broker 都没收到。
    """

    def __init__(self, message: str = "", response_code: Optional[int] = None,
                 cause: Optional[Exception] = None):
        super().__init__(message, response_code, cause)


__all__ = ["ClientErrorCode", "MQClientException", "MQBrokerException", "MQTimeOutException",
           "MQQueueException", "RequestTimeoutException"]