# -*- coding: utf-8 -*-
"""客户端异常（对应 org.apache.rocketmq.client.exception.*）。"""
from __future__ import annotations

from typing import Optional


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


__all__ = ["MQClientException", "MQBrokerException", "MQTimeOutException", "MQQueueException"]