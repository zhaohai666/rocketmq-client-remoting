# -*- coding: utf-8 -*-
"""TLS 传输层 + W3C traceparent 透传离线单测。

TLS：进程内起一个 ssl 包裹的 TCP 服务端（自签证书），验证
RemotingClient(tls_enable=True) 能完成握手并按 RocketMQ 帧收发；
以及 TLS 客户端打到明文端口必须报 RemotingConnectException。
"""
import os
import shutil
import socket
import ssl
import struct
import subprocess
import tempfile
import threading

import pytest

from rocketmq.remoting.client import RemotingClient
from rocketmq.remoting.protocol import remoting_command as rc_mod
from rocketmq.remoting.protocol import headers as headers_mod
from rocketmq.remoting.protocol.codes import RequestCode, ResponseCode

# ---------------------------------------------------------------- traceparent

from rocketmq.client.trace_context import (
    TRACE_CONTEXT_PROPERTY, child_traceparent, extract_traceparent,
    generate_traceparent, inject_trace_context, is_valid_traceparent)
from rocketmq.common.message import Message, MessageExt


def test_generate_traceparent_shape():
    tp = generate_traceparent()
    version, trace_id, parent_id, flags = tp.split("-")
    assert version == "00"
    assert len(trace_id) == 32 and all(c in "0123456789abcdef" for c in trace_id)
    assert len(parent_id) == 16
    assert flags == "01"
    assert trace_id != "0" * 32 and parent_id != "0" * 16


def test_validate_traceparent():
    good = generate_traceparent()
    assert is_valid_traceparent(good)
    assert is_valid_traceparent(good.upper())          # 宽松：大写也认
    assert not is_valid_traceparent("")                # 空
    assert not is_valid_traceparent("00-abc-def-01")   # 段长不对
    assert not is_valid_traceparent("00-%s-%s-01" % ("0" * 32, "1" * 16))   # trace-id 全 0
    assert not is_valid_traceparent("00-%s-%s-01" % ("1" * 32, "0" * 16))   # parent-id 全 0
    assert not is_valid_traceparent("ff-%s-%s-01" % ("1" * 32, "1" * 16))   # version ff
    assert not is_valid_traceparent("zz-%s-%s-01" % ("1" * 32, "1" * 16))   # version 非 hex


def test_child_traceparent_keeps_trace_id():
    parent = generate_traceparent()
    child = child_traceparent(parent)
    assert child is not None and is_valid_traceparent(child)
    assert child.split("-")[1] == parent.split("-")[1]   # trace-id 不变
    assert child.split("-")[2] != parent.split("-")[2]   # parent-id 换新
    assert child_traceparent("garbage") is None


def test_inject_does_not_overwrite_and_extract_roundtrip():
    msg = Message(topic="t", body=b"x")
    tp1 = inject_trace_context(msg)
    assert msg.get_property(TRACE_CONTEXT_PROPERTY) == tp1
    tp2 = inject_trace_context(msg)                      # 已有值不覆盖
    assert tp2 == tp1

    ext = MessageExt()
    ext.properties[TRACE_CONTEXT_PROPERTY] = tp1
    assert extract_traceparent(ext) == tp1
    assert extract_traceparent(MessageExt()) is None


# ---------------------------------------------------------------- TLS

@pytest.fixture(scope="module")
def tls_cert():
    """用 openssl CLI 生成一张自签证书（server 侧用）。"""
    exe = shutil.which("openssl")
    if exe is None:
        exe = next((p for p in ("/usr/local/bin/openssl", "/usr/bin/openssl")
                    if os.path.exists(p)), None)
    if exe is None:
        pytest.skip("openssl CLI not found; TLS transport test needs it to mint a cert")
    d = tempfile.mkdtemp(prefix="rmq_tls_")
    cert = os.path.join(d, "cert.pem")
    key = os.path.join(d, "key.pem")
    subprocess.run(
        [exe, "req", "-x509", "-newkey", "rsa:2048", "-keyout", key, "-out", cert,
         "-days", "1", "-nodes", "-subj", "/CN=127.0.0.1"],
        check=True, capture_output=True)
    return cert, key


class TlsFrameServer:
    """ssl 包裹的帧回显服务端：收请求帧 → 回 SUCCESS 响应（同 opaque）。"""

    def __init__(self, cert, key):
        self._ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self._ctx.load_cert_chain(cert, key)
        self._listen = socket.socket()
        self._listen.bind(("127.0.0.1", 0))
        self._listen.listen(4)
        self.port = self._listen.getsockname()[1]
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self):
        """accept 循环。

        每条连接单独起线程服务：若在 accept 线程里内联执行，服务端一次只服务一条连接，
        全套件跑时别的用例残留的连接（端口复用）会把 accept 线程卡死，而 Windows 下
        connect() 打进 backlog 是成功的——于是真正的客户端只会等满 15s 超时。
        """
        try:
            while True:
                conn, _ = self._listen.accept()
                try:
                    tls = self._ctx.wrap_socket(conn, server_side=True)
                except (OSError, ssl.SSLError):
                    conn.close()
                    continue
                threading.Thread(target=self._serve, args=(tls,), daemon=True).start()
        except OSError:
            pass  # stop() 关掉监听套接字

    def _serve(self, tls):
        try:
            while True:
                head = self._recv_exact(tls, 4)
                if head is None:
                    return
                (total,) = struct.unpack(">i", head)
                frame = self._recv_exact(tls, total)
                if frame is None:
                    return
                cmd = rc_mod.RemotingCommand.decode(head + frame)
                resp = rc_mod.RemotingCommand.create_response_command(
                    ResponseCode.SUCCESS, None, None)
                resp.opaque = cmd.opaque
                data = resp.encode()
                tls.sendall(data)
        except (OSError, ssl.SSLError):
            pass
        finally:
            try:
                tls.close()
            except OSError:
                pass

    @staticmethod
    def _recv_exact(tls, n):
        buf = b""
        while len(buf) < n:
            try:
                chunk = tls.recv(n - len(buf))
            except (OSError, ssl.SSLError):
                return None
            if not chunk:
                return None
            buf += chunk
        return buf

    def stop(self):
        try:
            self._listen.close()
        except OSError:
            pass


def test_invoke_sync_over_tls(tls_cert):
    server = TlsFrameServer(*tls_cert)
    try:
        client = RemotingClient(tls_enable=True)
        req = rc_mod.RemotingCommand.create_request_command(
            RequestCode.GET_ROUTEINFO_BY_TOPIC, (lambda h: (setattr(h, "topic", "t"), h)[1])(headers_mod.GetRouteInfoRequestHeader()))
        resp = client.invoke_sync("127.0.0.1:%d" % server.port, req, 15000)
        assert resp.code == ResponseCode.SUCCESS
        assert resp.opaque == req.opaque
        client.shutdown()
    finally:
        server.stop()


def test_tls_client_to_plaintext_server_fails():
    """TLS 客户端打到明文端口：握手必失败（收到的响应不是 TLS 握手）。"""
    listen = socket.socket()
    listen.bind(("127.0.0.1", 0))
    listen.listen(4)

    def loop():
        try:
            while True:
                conn, _ = listen.accept()
                conn.close()   # 直接关：TLS 握手必然失败
        except OSError:
            pass

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    try:
        client = RemotingClient(tls_enable=True)
        req = rc_mod.RemotingCommand.create_request_command(
            RequestCode.GET_ROUTEINFO_BY_TOPIC, (lambda h: (setattr(h, "topic", "t"), h)[1])(headers_mod.GetRouteInfoRequestHeader()))
        with pytest.raises(Exception):
            client.invoke_sync("127.0.0.1:%d" % listen.getsockname()[1], req, 15000)
        client.shutdown()
    finally:
        listen.close()
