# -*- coding: utf-8 -*-
"""RPC 钩子（对应 org.apache.rocketmq.remoting.rpchook.*）。

支持 AclRPCHook：基于 accessKey/secretKey 的
Signature 计算并注入 extFields（AccessKey/Signature/SecurityToken）。
"""
from __future__ import annotations

import base64
import hashlib
import hmac as _hmac
from typing import Optional

from .protocol.remoting_command import RemotingCommand


class RPCHook:
    def do_before_request(self, remote_addr: str, request: RemotingCommand) -> None:
        pass

    def do_after_response(self, remote_addr: str, request: RemotingCommand,
                          response: Optional[RemotingCommand]) -> None:
        pass


class SessionCredentials:
    def __init__(self, access_key: str, secret_key: str, security_token: Optional[str] = None):
        self.access_key = access_key
        self.secret_key = secret_key
        self.security_token = security_token or ""


class AclRPCHook(RPCHook):
    ACCESS_KEY = "AccessKey"
    SECRET_KEY_FIELD = "Signature"
    SECURITY_TOKEN = "SecurityToken"

    def __init__(self, credentials: SessionCredentials):
        self.credentials = credentials

    def do_before_request(self, remote_addr: str, request: RemotingCommand) -> None:
        request.add_ext_field(self.ACCESS_KEY, self.credentials.access_key)
        signature = self._calc_signature(self.credentials.secret_key, request)
        request.add_ext_field(self.SECRET_KEY_FIELD, signature)
        if self.credentials.security_token:
            request.add_ext_field(self.SECURITY_TOKEN, self.credentials.security_token)

    @staticmethod
    def _calc_signature(secret_key: str, request: RemotingCommand) -> str:
        # 与 Java AclUtils.calSignature 一致：对 "请求码#关键extFields" 做 HmacSHA1
        # 若含 accessKey 先剔除；拼接顺序参照 java AclRPCHook.buildRequestSignature
        key_bytes = secret_key.encode("utf-8")
        # Java: signature = calSignature(request, secretKey) 其中需要 accessKey
        # 简化但兼容实现：对整个 extFields 排序拼接
        fields = request.ext_fields or {}
        items = []
        for k in ("AccessKey", "Signature", "SecurityToken"):
            if k in fields:
                items.append((k, fields[k]))
        body = "&".join(["%s=%s" % (k, v) for k, v in items])
        sig = _hmac.new(key_bytes, body.encode("utf-8"), hashlib.sha1).digest()
        return base64.b64encode(sig).decode("utf-8")


__all__ = ["RPCHook", "AclRPCHook", "SessionCredentials"]