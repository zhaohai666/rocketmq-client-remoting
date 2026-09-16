# -*- coding: utf-8 -*-
"""RPC 钩子（对应 org.apache.rocketmq.remoting.RPCHook）。

AclClientRPCHook：基于 accessKey/secretKey 计算签名并注入 extFields
（AccessKey / Signature / SecurityToken）。签名算法**逐字节对齐** Java：

  - org.apache.rocketmq.acl.common.AclClientRPCHook#doBeforeRequest
  - org.apache.rocketmq.acl.common.AclUtils#combineRequestContent
  - org.apache.rocketmq.acl.common.AclSigner#calSignature

签名内容：按 key 字典序取 extFields 的**全部** value（排除 Signature 键自身；
只有 value，不带 key、不带 `=`/`&` 之类分隔符）按 UTF-8 拼接，再拼上 body 原始字节。
签名值：标准 Base64( HMAC-SHA1(key = secretKey, data = 上述内容) )。

注意两处顺序：AccessKey / SecurityToken 必须在算 content **之前**写入 extFields
（它们参与签名）；Signature 最后写入（它自己不参与签名）。
"""
from __future__ import annotations

import base64
import hashlib
import hmac as _hmac
from typing import Optional

from .protocol.remoting_command import RemotingCommand


class RPCHook:
    """对应 org.apache.rocketmq.remoting.RPCHook。"""

    def do_before_request(self, remote_addr: str, request: RemotingCommand) -> None:
        pass

    def do_after_response(self, remote_addr: str, request: RemotingCommand,
                          response: Optional[RemotingCommand]) -> None:
        pass


class SessionCredentials:
    """对应 org.apache.rocketmq.acl.common.SessionCredentials。"""

    CHARSET = "utf-8"
    ACCESS_KEY = "AccessKey"
    SECRET_KEY = "SecretKey"
    SIGNATURE = "Signature"
    SECURITY_TOKEN = "SecurityToken"

    def __init__(self, access_key: str, secret_key: str, security_token: Optional[str] = None):
        self.access_key = access_key
        self.secret_key = secret_key
        self.security_token = security_token or ""


class AclClientRPCHook(RPCHook):
    """对应 org.apache.rocketmq.acl.common.AclClientRPCHook。"""

    ACCESS_KEY = SessionCredentials.ACCESS_KEY
    SIGNATURE = SessionCredentials.SIGNATURE
    SECURITY_TOKEN = SessionCredentials.SECURITY_TOKEN

    def __init__(self, credentials: SessionCredentials):
        self.credentials = credentials

    def do_before_request(self, remote_addr: str, request: RemotingCommand) -> None:
        # 顺序必须与 Java 一致：先写 AccessKey/SecurityToken（参与签名），
        # 再算签名，最后写 Signature（自身不参与签名）。
        request.add_ext_field(self.ACCESS_KEY, self.credentials.access_key)
        if self.credentials.security_token:
            request.add_ext_field(self.SECURITY_TOKEN, self.credentials.security_token)
        signature = self.calc_signature(self.credentials.secret_key, request)
        request.add_ext_field(self.SIGNATURE, signature)

    @staticmethod
    def build_request_content(request: RemotingCommand) -> bytes:
        """对应 Java AclUtils.combineRequestContent。

        Java 的 parseRequestContent 会先 request.makeCustomHeaderToNet() 把
        customHeader 的字段落进 extFields，再按 TreeMap（key 字典序）取全部 value。
        这里等价：先 make_custom_header_to_net()，再对 ext_fields 排序。
        """
        request.make_custom_header_to_net()
        fields = request.ext_fields or {}
        buf = bytearray()
        for key in sorted(fields.keys()):
            if key == SessionCredentials.SIGNATURE:
                continue
            value = fields[key]
            if value is None:
                continue
            buf += str(value).encode(SessionCredentials.CHARSET)
        body = request.body
        if body:
            buf += body
        return bytes(buf)

    @staticmethod
    def calc_signature(secret_key: str, request: RemotingCommand) -> str:
        """对应 Java AclSigner.calSignature：HmacSHA1 + 标准 Base64（带 `=` 填充）。"""
        data = AclClientRPCHook.build_request_content(request)
        digest = _hmac.new(secret_key.encode(SessionCredentials.CHARSET), data,
                           hashlib.sha1).digest()
        return base64.b64encode(digest).decode(SessionCredentials.CHARSET)


# 兼容旧名（最初写作 AclRPCHook）
AclRPCHook = AclClientRPCHook

__all__ = ["RPCHook", "SessionCredentials", "AclClientRPCHook", "AclRPCHook"]
