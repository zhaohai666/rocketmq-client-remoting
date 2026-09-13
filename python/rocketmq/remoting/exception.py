# -*- coding: utf-8 -*-
"""Remoting 异常（对应 org.apache.rocketmq.remoting.exception）。"""
from __future__ import annotations


class RemotingException(Exception):
    pass


class RemotingCommandException(RemotingException):
    pass


class RemotingConnectException(RemotingException):
    def __init__(self, addr: str = ""):
        super().__init__("connect to %s failed" % addr)
        self.addr = addr


class RemotingSendRequestException(RemotingException):
    def __init__(self, addr: str = "", msg: str = ""):
        super().__init__("send request to %s failed: %s" % (addr, msg))
        self.addr = addr


class RemotingTimeoutException(RemotingException):
    def __init__(self, addr: str = "", timeout_millis: int = 0, msg: str = ""):
        super().__init__("wait response on the channel %s timeout, %dms: %s" % (addr, timeout_millis, msg))
        self.addr = addr
        self.timeout_millis = timeout_millis


class RemotingTooMuchRequestException(RemotingException):
    pass


class RemotingNoCodecException(RemotingException):
    pass


class RemotingServerException(RemotingException):
    pass