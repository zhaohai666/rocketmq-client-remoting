# -*- coding: utf-8 -*-
"""GO_AWAY(1500) 的传输层处理（镜像 Rust `remoting::client` 的三个 go_away 用例）。

对应 Java `NettyRemotingClient#invokeImpl:828-873`：broker / proxy 优雅下线时会给
在途请求回 GO_AWAY，语义是"这条连接别再用了"。客户端必须换连接重发一次，且**只**
重发一次；第二次还是 GO_AWAY 就抛 `RemotingSendRequestException`，不在下线中的集群
上无限打转。开关 `enableReconnectForGoAway` 关掉时直接报错、不重连。

修复前这条码在四个端口里都被当成普通业务响应交给上层：同步调用拿到 GO_AWAY 就当作
"发送成功"继续往下走，消息实际并没有落到 broker。
"""
from __future__ import annotations

import socket
import struct
import threading
import time

import pytest

from rocketmq.remoting.client import RemotingClient
from rocketmq.remoting.exception import RemotingSendRequestException
from rocketmq.remoting.protocol import remoting_command as rc_mod
from rocketmq.remoting.protocol.codes import RequestCode, ResponseCode


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except OSError:
            return None
        if not chunk:
            return None
        buf += chunk
    return buf


def _read_frame(sock):
    head = _recv_exact(sock, 4)
    if head is None:
        return None
    (total,) = struct.unpack(">i", head)
    body = _recv_exact(sock, total)
    if body is None:
        return None
    return head + body


class GoAwayServer:
    """前 ``go_away_conns`` 条连接一律回 GO_AWAY，之后的连接回 SUCCESS。"""

    def __init__(self, go_away_conns):
        self.go_away_conns = go_away_conns
        self.conns = 0
        self.requests = 0
        self.opaques = []
        self._listen = socket.socket()
        self._listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listen.bind(("127.0.0.1", 0))
        self._listen.listen(4)
        self.port = self._listen.getsockname()[1]
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    @property
    def addr(self):
        return "127.0.0.1:%d" % self.port

    def stop(self):
        self._stop.set()
        try:
            self._listen.close()
        except OSError:
            pass
        self._thread.join(timeout=3.0)

    def _loop(self):
        self._listen.settimeout(0.2)
        while not self._stop.is_set():
            try:
                conn, _ = self._listen.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            index = self.conns
            self.conns += 1
            threading.Thread(target=self._serve, args=(conn, index), daemon=True).start()

    def _serve(self, conn, index):
        go_away = index < self.go_away_conns
        try:
            while not self._stop.is_set():
                frame = _read_frame(conn)
                if frame is None:
                    return
                self.requests += 1
                cmd = rc_mod.RemotingCommand.decode(frame)
                self.opaques.append(cmd.opaque)
                resp = rc_mod.RemotingCommand.create_response_command(
                    ResponseCode.GO_AWAY if go_away else ResponseCode.SUCCESS, None, None)
                resp.opaque = cmd.opaque
                conn.sendall(resp.encode())
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass


def _wait_until(predicate, timeout=8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def test_go_away_reconnects_and_retries_once():
    server = GoAwayServer(1)
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.SEND_MESSAGE_V2)
        resp = client.invoke_sync(server.addr, req, 5000)
        assert resp.code == ResponseCode.SUCCESS, "重发必须拿到真应答"
        assert server.conns == 2, "GO_AWAY 之后必须换新连接"
        assert server.requests == 2
        assert len(set(server.opaques)) == 2, "重发必须用新 opaque，否则响应会错配"
        assert not client._response_table
    finally:
        client.shutdown()
        server.stop()


def test_second_go_away_fails_instead_of_looping():
    server = GoAwayServer(10 ** 6)
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.SEND_MESSAGE_V2)
        with pytest.raises(RemotingSendRequestException) as caught:
            client.invoke_sync(server.addr, req, 5000)
        # 文案对齐 Java：RemotingSendRequestException("Receive GO_AWAY twice ...")
        assert "GO_AWAY twice" in str(caught.value)
        assert server.conns == 2, "只重发一次，不能无限重连"
        assert not client._response_table
    finally:
        client.shutdown()
        server.stop()


def test_go_away_without_reconnect_flag_surfaces_error():
    server = GoAwayServer(10 ** 6)
    client = RemotingClient(enable_reconnect_for_go_away=False)
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.SEND_MESSAGE_V2)
        with pytest.raises(RemotingSendRequestException) as caught:
            client.invoke_sync(server.addr, req, 5000)
        assert "Receive GO_AWAY from channel" in str(caught.value)
        assert server.conns == 1, "关掉开关就不该重连"
        assert not client._response_table
    finally:
        client.shutdown()
        server.stop()


def test_async_go_away_retries_via_callback():
    """异步路径与同步共用同一套 GO_AWAY 语义（Java 里同一段逻辑在 invokeImpl）。"""
    server = GoAwayServer(1)
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.SEND_MESSAGE_V2)
        calls = []
        client.invoke_async(server.addr, req, lambda resp, err: calls.append((resp, err)), 5000)
        assert _wait_until(lambda: len(calls) == 1), "回调未触发: %r" % (calls,)
        resp, err = calls[0]
        assert err is None, "重发成功就不该把错误交给回调: %r" % err
        assert resp.code == ResponseCode.SUCCESS
        assert server.conns == 2
        assert _wait_until(lambda: server.requests == 2)
    finally:
        client.shutdown()
        server.stop()


def test_async_second_go_away_reports_error():
    server = GoAwayServer(10 ** 6)
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.SEND_MESSAGE_V2)
        calls = []
        client.invoke_async(server.addr, req, lambda resp, err: calls.append((resp, err)), 5000)
        assert _wait_until(lambda: len(calls) == 1), "回调未触发: %r" % (calls,)
        resp, err = calls[0]
        assert resp is None
        assert isinstance(err, RemotingSendRequestException)
        assert "GO_AWAY twice" in str(err)
        assert server.conns == 2
    finally:
        client.shutdown()
        server.stop()
