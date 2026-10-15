# -*- coding: utf-8 -*-
from __future__ import annotations

import socket
import struct
import threading
import time

import pytest

from remoting.client import RemotingClient
from remoting.exception import RemotingSendRequestException
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


class GoAwayServer:
    """The first ``go_away_conns`` connections all reply GO_AWAY; subsequent connections reply SUCCESS."""

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
        assert resp.code == ResponseCode.SUCCESS, "the resend must get a real response"
        assert server.conns == 2, "a new connection must be made after GO_AWAY"
        assert server.requests == 2
        assert len(set(server.opaques)) == 2, "the resend must use a new opaque, otherwise responses would be mismatched"
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
        assert "GO_AWAY twice" in str(caught.value)
        assert server.conns == 2, "resend only once, no infinite reconnecting"
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
        assert server.conns == 1, "with the switch off, no reconnect should happen"
        assert not client._response_table
    finally:
        client.shutdown()
        server.stop()


def test_async_go_away_retries_via_callback():
    pass
    server = GoAwayServer(1)
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.SEND_MESSAGE_V2)
        calls = []
        client.invoke_async(server.addr, req, lambda resp, err: calls.append((resp, err)), 5000)
        assert _wait_until(lambda: len(calls) == 1), "callback not fired: %r" % (calls,)
        resp, err = calls[0]
        assert err is None, "a successful resend must not hand an error to the callback: %r" % err
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
        assert _wait_until(lambda: len(calls) == 1), "callback not fired: %r" % (calls,)
        resp, err = calls[0]
        assert resp is None
        assert isinstance(err, RemotingSendRequestException)
        assert "GO_AWAY twice" in str(err)
        assert server.conns == 2
    finally:
        client.shutdown()
        server.stop()
