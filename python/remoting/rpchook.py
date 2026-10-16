# -*- coding: utf-8 -*-
"""The RPC hook base class and its implementations.

AclClientRPCHook: computes a signature from accessKey/secretKey and injects it into
extFields (AccessKey / Signature / SecurityToken). The signature algorithm is
**byte-for-byte identical** to the verification on the broker side's acl module
(combineRequestContent + calSignature):

Signed content: take the **all** values of extFields in key lexicographic order (excluding
the Signature key itself; only the values, without the keys and without separators such
as `=`/`&`), concatenate them as UTF-8, then append the raw body bytes.
Signature value: standard Base64( HMAC-SHA1(key = secretKey, data = the content above) ).

Note the two ordering rules: AccessKey / SecurityToken must be written into extFields
**before** the content is computed (they take part in the signature); Signature is written
last (it does not take part in the signature itself).
"""
from __future__ import annotations

import base64
import hashlib
import hmac as _hmac
from typing import Optional

from common.mix_all import MixAll
from .protocol.codes import RequestType
from .protocol.remoting_command import RemotingCommand


class RPCHook:
    """The RPC hook base class: the interception points before sending / after receiving a response."""

    def do_before_request(self, remote_addr: str, request: RemotingCommand) -> None:
        pass

    def do_after_response(self, remote_addr: str, request: RemotingCommand,
                          response: Optional[RemotingCommand]) -> None:
        pass


class SessionCredentials:
    """ACL session credentials: AccessKey / SecretKey / SecurityToken."""

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
    """The ACL signature hook: signs requests with SessionCredentials."""

    ACCESS_KEY = SessionCredentials.ACCESS_KEY
    SIGNATURE = SessionCredentials.SIGNATURE
    SECURITY_TOKEN = SessionCredentials.SECURITY_TOKEN

    def __init__(self, credentials: SessionCredentials):
        self.credentials = credentials

    def do_before_request(self, remote_addr: str, request: RemotingCommand) -> None:
        # The order is fixed: write AccessKey/SecurityToken first (they take part in
        # the signature), then compute the signature, then write Signature last (it
        # does not take part in the signature itself).
        request.add_ext_field(self.ACCESS_KEY, self.credentials.access_key)
        if self.credentials.security_token:
            request.add_ext_field(self.SECURITY_TOKEN, self.credentials.security_token)
        signature = self.calc_signature(self.credentials.secret_key, request)
        request.add_ext_field(self.SIGNATURE, signature)

    @staticmethod
    def build_request_content(request: RemotingCommand) -> bytes:
        """Build the ACL content to be signed (matching the broker side's combineRequestContent).

        The verifying side first calls request.makeCustomHeaderToNet() to put the
        customHeader fields into extFields, then takes all values in a TreeMap
        (key lexicographic order).
        The equivalent here: call make_custom_header_to_net() first, then sort ext_fields.
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
        """Compute the signature: HmacSHA1 + standard Base64 (with `=` padding), identical to the broker-side verification."""
        data = AclClientRPCHook.build_request_content(request)
        digest = _hmac.new(secret_key.encode(SessionCredentials.CHARSET), data,
                           hashlib.sha1).digest()
        return base64.b64encode(digest).decode(SessionCredentials.CHARSET)


# Kept for the old name (originally spelled AclRPCHook)
AclRPCHook = AclClientRPCHook


class NamespaceRpcHook(RPCHook):
    """The namespaceV2 request header hook.

    `do_before_request` has a single non-empty guard: when namespaceV2 is not empty it
    adds two extra fields to every request::

        nsd = "true"            (MixAll.RPC_REQUEST_HEADER_NAMESPACED_FIELD)
        ns  = <namespaceV2>     (MixAll.RPC_REQUEST_HEADER_NAMESPACE_FIELD)

    This is the **server-side** namespace mechanism of 5.x (an Alibaba Cloud serverless
    instance id and the like, with the broker resolving the real topic from the request
    header); it is unrelated to the old `namespace` field that this client uses to build
    topic names.

    When it is not configured it **does nothing at all**: it must not create an empty
    extFields (extFields has to stay null -- that is the contract pinned by
    `tests/test_namespace_rpc_hook.py`). `do_after_response` is an empty implementation.

    CAUTION about the registration order (see `MQClientInstance.__init__` in
    `client/mq_client.py`): this hook must be registered **before** the Stream hook and
    the user hook (the ACL signature), so that `nsd`/`ns` end up in the ACL signed content.
    """

    def __init__(self, namespace_v2: Optional[str] = None):
        self.namespace_v2 = namespace_v2 or ""

    def do_before_request(self, remote_addr: str, request: RemotingCommand) -> None:
        if not self.namespace_v2:
            return
        request.add_ext_field(MixAll.RPC_REQUEST_HEADER_NAMESPACED_FIELD, "true")
        request.add_ext_field(MixAll.RPC_REQUEST_HEADER_NAMESPACE_FIELD,
                              self.namespace_v2)


class StreamTypeRPCHook(RPCHook):
    """Tag every request with its request type marker (the Stream type).

    `do_before_request` is a single line:
    `request.addExtField(MixAll.REQ_T, String.valueOf(RequestType.STREAM.getCode()))`,
    that is, `ReqT = "0"` (`RequestType` currently has only the single enum entry STREAM,
    whose code is `(byte) 0`).

    CAUTION: the registration order matters -- the Stream hook must be registered
    **before** the user's rpcHook (usually the ACL signature hook), so that ReqT ends up
    in the signed content; otherwise the broker side rejects the signature because of an
    extra unsigned field.
    """

    def do_before_request(self, remote_addr: str, request: RemotingCommand) -> None:
        from common.mix_all import MixAll
        from .protocol.codes import RequestType
        request.add_ext_field(MixAll.REQ_T, str(RequestType.STREAM))


__all__ = ["RPCHook", "SessionCredentials", "AclClientRPCHook", "AclRPCHook",
           "NamespaceRpcHook", "StreamTypeRPCHook"]
