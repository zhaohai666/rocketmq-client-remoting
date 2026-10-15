# -*- coding: utf-8 -*-
"""Real-socket cases for invoke_async (a real connection on loopback, not a mock).

What is covered is one chain: after an async request is registered in the in-flight table, it must get the result **exactly once** --
a normal response carries response, no response carries error, and the entry must be removed from the table.
Before the fix the timeout_millis parameter was discarded outright: when the peer did not reply the callback never fired and the entry leaked permanently.
"""
from __future__ import annotations

import socket
import struct
import threading
import time

import pytest

from remoting.client import RemotingClient
from remoting.exception import RemotingTimeoutException
from remoting.protocol import remoting_command as rc_mod
from remoting.protocol.codes import RequestCode, ResponseCode


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
    """A local loopback frame server. When reply=False it receives requests but never replies (used to test timeouts)."""

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
                    # drag past the test's timeout window, ensuring the client can only fail via its own sweeper thread
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
        assert _wait_until(lambda: len(calls) == 1), "callback not fired: %r" % (calls,)
        resp, err = calls[0]
        assert err is None
        assert resp is not None
        assert resp.code == ResponseCode.SUCCESS
        # the response must fill back the same opaque, otherwise the in-flight table association is wrong
        assert resp.opaque == req.opaque
        # after delivery the entry must already be out of the table (the pop is done by the read thread)
        assert req.opaque not in client._response_table
        client.shutdown()
        # late duplicate delivery must not happen
        assert len(calls) == 1
    finally:
        server.stop()


def test_invoke_async_timeout_fires_once_with_error():
    """The peer accepts the request but never replies: timeout_millis must take effect and the entry must not leak."""
    server = FrameServer(reply=False)
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.HEART_BEAT)
        calls = []
        client.invoke_async(server.addr, req,
                            lambda resp, err: calls.append((resp, err)), 300)
        assert _wait_until(lambda: len(calls) == 1), "callback did not fire after the timeout"
        resp, err = calls[0]
        assert resp is None
        assert isinstance(err, RemotingTimeoutException)
        assert req.opaque not in client._response_table, "timeout entry not removed -> in-flight table leak"
        time.sleep(0.5)
        assert len(calls) == 1, "callback was delivered more than once"
        client.shutdown()
    finally:
        server.stop()


def test_sweeper_thread_does_not_outlive_shutdown():
    """shutdown must reap the sweeper thread: otherwise a resident thread remains in the process."""
    server = FrameServer(reply=True)
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.HEART_BEAT)
        client.invoke_async(server.addr, req, lambda resp, err: None)
        thread = client._sweeper_thread
        assert thread is not None and thread.is_alive()
        client.shutdown()
        assert _wait_until(lambda: not thread.is_alive(), timeout=3.0), "the sweeper thread did not exit with shutdown"
    finally:
        server.stop()
