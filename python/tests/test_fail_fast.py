# -*- coding: utf-8 -*-
"""连接断开时立刻失败在途请求（对应 Java ``NettyRemotingAbstract#failFast`` / ``#requestFail``）。

Java 的 ``NettyRemotingHandler#close`` 在 ``closeChannel`` 之后紧接着调
``failFast(ctx.channel())``（``NettyRemotingClient.java:1191``）：把 ``responseTable`` 里
属于这条连接的请求逐条 ``requestFail`` —— ``setSendRequestOK(false)`` +
``putResponse(null)`` + 投递一次回调。缺了这一步会错两件事：

1. **时机**：同步调用方要等满 invoke 超时、异步回调要等 ``timeout + 1s``（清理线程的宽限
   口径）才发现"连接早就没了"。对端重启 / broker 主备切换 / 网络抖动时，这段无意义等待
   会直接压在发送链路上。
2. **语义**：报出来的是 ``RemotingTimeoutException``，而 Java 报
   ``RemotingSendRequestException``。异步发送的重试分类按这两个类型分流
   （``client/producer.py`` 的 ``_classify_async_failure``），错类型等于错重试决策。

这里用真 socket 对端（收到请求就关掉连接）来证明，而不是 mock：只有真 EOF 才会走到
读线程退出这条路径。修复前三项全部不成立（等待时间等于 timeout、异常类型是超时）。
"""
from __future__ import annotations

import socket
import struct
import threading
import time

import pytest

from rocketmq.remoting.client import RemotingClient, _ResponseFuture
from rocketmq.remoting.exception import (RemotingSendRequestException,
                                         RemotingTimeoutException)
from rocketmq.remoting.protocol import remoting_command as rc_mod
from rocketmq.remoting.protocol.codes import RequestCode


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


class DropOnReadServer:
    """读掉每个请求然后**关掉连接**，一个字节响应都不回（模拟对端掉线/重启）。

    ``hold`` 为真时读完第一帧后挂住不关，用来单独测"另一条连接不该被牵连"。
    """

    def __init__(self, hold=False):
        self.hold = hold
        self.requests = 0
        self.closed = threading.Event()
        self.first_frame_read = threading.Event()
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
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn):
        try:
            while not self._stop.is_set():
                frame = _read_frame(conn)
                if frame is None:
                    return
                self.requests += 1
                if self.hold:
                    self.first_frame_read.set()
                    continue
                self.first_frame_read.set()
                self.closed.set()
                return  # 关连接：客户端读线程下一次 recv 见到 EOF
        except OSError:
            self.closed.set()
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


# 远超断连传播所需的时间；真机 loopback 上 EOF 在毫秒级就到，用它当"没修好"的判据
LONG_TIMEOUT_MILLIS = 30000


def test_sync_call_fails_fast_instead_of_waiting_for_timeout():
    server = DropOnReadServer()
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.HEART_BEAT)
        started = time.time()
        with pytest.raises(RemotingSendRequestException) as caught:
            client.invoke_sync(server.addr, req, LONG_TIMEOUT_MILLIS)
        cost = time.time() - started
        assert cost < LONG_TIMEOUT_MILLIS / 1000.0 / 3, (
            "同步调用等满/接近超时才返回：说明连接断开没有立刻失败在途请求 (%.2fs)" % cost)
        # 语义必须是"发送失败"，不是"超时"——重试分类按类型分流
        assert not isinstance(caught.value, RemotingTimeoutException)
        assert "connection closed" in str(caught.value)
        assert req.opaque not in client._response_table, "在途条目未摘除 -> 泄漏"
    finally:
        client.shutdown()
        server.stop()


def test_async_callback_reports_send_failure_immediately():
    server = DropOnReadServer()
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.HEART_BEAT)
        calls = []
        started = time.time()
        client.invoke_async(server.addr, req,
                            lambda resp, err: calls.append((resp, err)),
                            LONG_TIMEOUT_MILLIS)
        assert _wait_until(lambda: len(calls) == 1), "回调未触发: %r" % (calls,)
        cost = time.time() - started
        resp, err = calls[0]
        assert resp is None
        assert isinstance(err, RemotingSendRequestException), (
            "回调拿到的是 %r：连接断开应报发送失败，不是超时" % (err,))
        assert cost < LONG_TIMEOUT_MILLIS / 1000.0 / 3, (
            "回调等了 %.2fs 才触发，接近 timeout+宽限" % cost)
        assert req.opaque not in client._response_table
    finally:
        client.shutdown()
        server.stop()


def test_fail_fast_callback_fires_exactly_once():
    """断连与清理线程抢同一条请求：回调也只能有一次（Java 的 executeCallbackOnlyOnce）。"""
    server = DropOnReadServer()
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.HEART_BEAT)
        calls = []
        # 短超时：清理线程大概率比 EOF 更晚，但两者都会抢同一条条目
        client.invoke_async(server.addr, req, lambda resp, err: calls.append((resp, err)), 200)
        assert _wait_until(lambda: len(calls) >= 1)
        time.sleep(1.8)  # 跨过 timeout + 1s 宽限，给清理线程充分机会重复投递
        assert len(calls) == 1, "回调被投递了 %d 次" % len(calls)
        assert not client._response_table
    finally:
        client.shutdown()
        server.stop()


def test_other_connections_are_not_collateral_damage():
    """只失败断开那条连接自己的请求：别的地址、以及同地址的新连接都不能被牵连。"""
    dying = DropOnReadServer()
    alive = DropOnReadServer(hold=True)  # 只收请求、既不回也不关
    client = RemotingClient()
    try:
        quiet = []
        req_alive = rc_mod.RemotingCommand.create_request_command(RequestCode.HEART_BEAT)
        client.invoke_async(alive.addr, req_alive,
                            lambda resp, err: quiet.append((resp, err)), LONG_TIMEOUT_MILLIS)
        assert _wait_until(lambda: alive.requests == 1)

        calls = []
        req_dying = rc_mod.RemotingCommand.create_request_command(RequestCode.HEART_BEAT)
        client.invoke_async(dying.addr, req_dying,
                            lambda resp, err: calls.append((resp, err)), LONG_TIMEOUT_MILLIS)
        assert _wait_until(lambda: len(calls) == 1), "断开的连接没有立刻失败: %r" % (calls,)
        assert isinstance(calls[0][1], RemotingSendRequestException)

        assert req_alive.opaque in client._response_table, "另一条连接的在途请求被误杀"
        assert not quiet, "仍在途的请求收到了回调: %r" % (quiet,)
    finally:
        client.shutdown()
        alive.stop()
        dying.stop()


def test_same_address_new_connection_survives_old_reader():
    """按套接字身份认领：同地址换新连接时，旧读线程收尾不能判死新连接上的请求。

    这是 GO_AWAY 换连接重发、以及写失败后 close_channel 的实际形状——地址串没变，
    变的是连接。只比地址串就会把新连接刚登记的请求一起误杀。
    """
    client = RemotingClient()
    old_sock = socket.socket()
    new_sock = socket.socket()
    try:
        keep_alive = _ResponseFuture(1, LONG_TIMEOUT_MILLIS, addr="127.0.0.1:9999")
        keep_alive.conn = new_sock
        victim = _ResponseFuture(2, LONG_TIMEOUT_MILLIS, addr="127.0.0.1:9999")
        victim.conn = old_sock
        never_written = _ResponseFuture(3, LONG_TIMEOUT_MILLIS, addr="127.0.0.1:9999")
        for f in (keep_alive, victim, never_written):
            client._response_table[f.opaque] = f

        assert client._fail_fast("127.0.0.1:9999", old_sock) == 1
        assert victim.opaque not in client._response_table
        assert keep_alive.opaque in client._response_table
        assert never_written.opaque in client._response_table, "还没写出去的请求不该被牵连"
        assert keep_alive.cause is None and never_written.cause is None
        assert victim.cause is not None and not victim.send_request_ok
    finally:
        client.shutdown()
        old_sock.close()
        new_sock.close()


def test_shutdown_drains_and_reports_in_flight_requests():
    """shutdown 时仍在途的请求不能悄悄消失：回调必须带着失败原因落地。"""
    server = DropOnReadServer(hold=True)
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.HEART_BEAT)
        calls = []
        client.invoke_async(server.addr, req, lambda resp, err: calls.append((resp, err)),
                            LONG_TIMEOUT_MILLIS)
        assert _wait_until(lambda: server.requests == 1)
        assert req.opaque in client._response_table
        client.shutdown()
        assert _wait_until(lambda: len(calls) == 1), "shutdown 之后回调永不落地"
        assert isinstance(calls[0][1], RemotingSendRequestException)
        assert not client._response_table, "shutdown 泄漏了在途条目"
    finally:
        server.stop()
