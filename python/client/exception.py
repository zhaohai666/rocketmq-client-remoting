# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Optional


class ClientErrorCode:
    pass

    CONNECT_BROKER_EXCEPTION = 10001
    ACCESS_BROKER_TIMEOUT = 10002
    BROKER_NOT_EXIST_EXCEPTION = 10003
    NO_NAME_SERVER_EXCEPTION = 10004
    NOT_FOUND_TOPIC_EXCEPTION = 10005
    REQUEST_TIMEOUT_EXCEPTION = 10006
    CREATE_REPLY_MESSAGE_EXCEPTION = 10007


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
    pass

    def __init__(self, message: str = "", response_code: Optional[int] = None,
                 cause: Optional[Exception] = None):
        super().__init__(message, response_code, cause)


__all__ = ["ClientErrorCode", "MQClientException", "MQBrokerException", "MQTimeOutException",
           "MQQueueException", "RequestTimeoutException"]