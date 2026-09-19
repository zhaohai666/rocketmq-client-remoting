# -*- coding: utf-8 -*-
"""invoke_async 的真实 socket 用例（镜像 C++ test_transport.cpp 的第 3 / 3b 段）。

覆盖的是同一条链路：异步请求登记进在途表之后，必须**恰好一次**地拿到结果——
正常响应带 response，收不到响应带 error，且条目要从表里摘掉。
修复前 timeout_millis 参数被直接丢弃：对端不回包时回调永不触发、条目永久泄漏。
"""
from __future__ import annotations

import socket
import struct
import threading
import time

import pytest

from rocketmq.remoting.client import RemotingClient
from rocketmq.remoting.exception import RemotingTimeoutException
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


class FrameServer:
    """本机回环帧服务端。reply=False 时收到请求但永不回复（用来测超时）。"""

    def __init__(self, reply=True):
        self.reply = reply
        self.served = 0
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
                self.served += 1
                if not self.reply:
                    # 拖过用例的超时窗口，确保客户端只能靠自己的清理线程判失败
                    self._stop.wait(3.0)
                    return
                cmd = rc_mod.RemotingCommand.decode(frame)
                resp = rc_mod.RemotingCommand.create_response_command(
                    ResponseCode.SUCCESS, None, None)
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


def test_invoke_async_success_fires_once_with_response():
    server = FrameServer(reply=True)
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.HEART_BEAT)
        calls = []
        client.invoke_async(server.addr, req, lambda resp, err: calls.append((resp, err)))
        assert _wait_until(lambda: len(calls) == 1), "回调未触发: %r" % (calls,)
        resp, err = calls[0]
        assert err is None
        assert resp is not None
        assert resp.code == ResponseCode.SUCCESS
        # 响应必须回填同一个 opaque，否则说明在途表关联错了
        assert resp.opaque == req.opaque
        # 投递完成后条目必须已不在表里（pop 由读线程完成）
        assert req.opaque not in client._response_table
        client.shutdown()
        # 迟到的重复投递不允许发生
        assert len(calls) == 1
    finally:
        server.stop()


def test_invoke_async_timeout_fires_once_with_error():
    """对端收下请求但永不回复：timeout_millis 必须生效且条目不得泄漏。"""
    server = FrameServer(reply=False)
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.HEART_BEAT)
        calls = []
        client.invoke_async(server.addr, req,
                            lambda resp, err: calls.append((resp, err)), 300)
        # 清理线程与 Java scanResponseTable 同式：deadline + 1s 宽限才判超时
        assert _wait_until(lambda: len(calls) == 1), "超时后回调没有触发"
        resp, err = calls[0]
        assert resp is None
        assert isinstance(err, RemotingTimeoutException)
        assert req.opaque not in client._response_table, "超时条目未摘除 -> 在途表泄漏"
        time.sleep(0.5)
        assert len(calls) == 1, "回调被重复投递"
        client.shutdown()
    finally:
        server.stop()


def test_sweeper_thread_does_not_outlive_shutdown():
    """shutdown 必须收掉清理线程：否则进程里会留一个常驻线程。"""
    server = FrameServer(reply=True)
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.HEART_BEAT)
        client.invoke_async(server.addr, req, lambda resp, err: None)
        thread = client._sweeper_thread
        assert thread is not None and thread.is_alive()
        client.shutdown()
        assert _wait_until(lambda: not thread.is_alive(), timeout=3.0), "清理线程未随 shutdown 退出"
    finally:
        server.stop()
