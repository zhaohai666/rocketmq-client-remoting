# -*- coding: utf-8 -*-
from __future__ import annotations

import socket
import struct
import threading
import time

import pytest

from remoting.client import RemotingClient, _ResponseFuture
from remoting.exception import (RemotingSendRequestException,
                                         RemotingTimeoutException)
from remoting.protocol import remoting_command as rc_mod
from remoting.protocol.codes import RequestCode


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
    """Reads every request then **closes the connection**, without responding a single byte (simulating a peer going offline / restarting).

    When ``hold`` is true, it hangs after reading the first frame instead of closing, to test "another connection must not be affected" in isolation.
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
                return  # close the connection: the client's read thread sees EOF on its next recv
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


# Far longer than disconnection propagation needs; on a real loopback machine EOF arrives in milliseconds, so this serves as the criterion for "not fixed"
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
            "the sync call returned only at/near the timeout: the disconnect did not fail in-flight requests immediately (%.2fs)" % cost)
        # The semantics must be "send failure", not "timeout" — retry classification branches by type
        assert not isinstance(caught.value, RemotingTimeoutException)
        assert "connection closed" in str(caught.value)
        assert req.opaque not in client._response_table, "in-flight entry not removed -> leak"
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
        assert _wait_until(lambda: len(calls) == 1), "callback not fired: %r" % (calls,)
        cost = time.time() - started
        resp, err = calls[0]
        assert resp is None
        assert isinstance(err, RemotingSendRequestException), (
            "the callback got %r: a disconnect should report a send failure, not a timeout" % (err,))
        assert cost < LONG_TIMEOUT_MILLIS / 1000.0 / 3, (
            "the callback took %.2fs to fire, close to timeout + grace" % cost)
        assert req.opaque not in client._response_table
    finally:
        client.shutdown()
        server.stop()


def test_mid_send_wind_down_scan_still_claims_the_async_future():
    """The wind-down scan of a dying connection can land while the send is still
    completing its bookkeeping: the future must already be claimable by that socket,
    otherwise the callback silently hangs until the timeout sweeper (timeout + grace)."""
    server = DropOnReadServer(hold=True)  # stays alive: no real EOF scan may rescue the timing
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.HEART_BEAT)
        calls = []
        real_write_on = client._write_on

        def write_and_scan(addr, sock, cmd):
            real_write_on(addr, sock, cmd)
            client._fail_fast(addr, sock)  # the wind-down scan, landing mid-send

        client._write_on = write_and_scan
        client.invoke_async(server.addr, req, lambda resp, err: calls.append((resp, err)),
                            LONG_TIMEOUT_MILLIS)
        assert _wait_until(lambda: len(calls) == 1), "callback not fired: %r" % (calls,)
        resp, err = calls[0]
        assert resp is None
        assert isinstance(err, RemotingSendRequestException), (
            "the callback got %r: a mid-send wind-down scan is a send failure" % (err,))
        assert req.opaque not in client._response_table
    finally:
        client.shutdown()
        server.stop()


def test_sync_mid_send_wind_down_scan_reports_send_failure():
    server = DropOnReadServer(hold=True)
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.HEART_BEAT)
        real_write_on = client._write_on

        def write_and_scan(addr, sock, cmd):
            real_write_on(addr, sock, cmd)
            client._fail_fast(addr, sock)

        client._write_on = write_and_scan
        started = time.time()
        with pytest.raises(RemotingSendRequestException):
            client.invoke_sync(server.addr, req, 1500)
        assert time.time() - started < 1.5, (
            "the sync call waited out its timeout: the mid-send scan did not wake it")
    finally:
        client.shutdown()
        server.stop()


def test_fail_fast_callback_fires_exactly_once():
    pass
    server = DropOnReadServer()
    client = RemotingClient()
    try:
        req = rc_mod.RemotingCommand.create_request_command(RequestCode.HEART_BEAT)
        calls = []
        # Short timeout: the cleanup thread most likely runs later than EOF, but both race for the same entry
        client.invoke_async(server.addr, req, lambda resp, err: calls.append((resp, err)), 200)
        assert _wait_until(lambda: len(calls) >= 1)
        time.sleep(1.8)  # cross timeout + 1s grace, giving the cleanup thread ample chance to redeliver
        assert len(calls) == 1, "the callback was delivered %d times" % len(calls)
        assert not client._response_table
    finally:
        client.shutdown()
        server.stop()


def test_other_connections_are_not_collateral_damage():
    """Only the requests of the disconnected connection itself fail: other addresses, and new connections to the same address, must not be affected."""
    dying = DropOnReadServer()
    alive = DropOnReadServer(hold=True)  # only receives requests, neither replies nor closes
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
        assert _wait_until(lambda: len(calls) == 1), "the disconnected connection did not fail immediately: %r" % (calls,)
        assert isinstance(calls[0][1], RemotingSendRequestException)

        assert req_alive.opaque in client._response_table, "the other connection's in-flight request was killed by mistake"
        assert not quiet, "an in-flight request received a callback: %r" % (quiet,)
    finally:
        client.shutdown()
        alive.stop()
        dying.stop()


def test_same_address_new_connection_survives_old_reader():
    """Claim by socket identity: when the same address switches to a new connection, the old reader's teardown must not kill requests on the new connection.

    This is the actual shape of GO_AWAY reconnect-and-resend, as well as close_channel after a write failure — the address string stays the same,
    but the connection changes. Comparing only address strings would kill the newly registered requests on the new connection too.
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
        assert never_written.opaque in client._response_table, "requests not yet written out must not be affected"
        assert keep_alive.cause is None and never_written.cause is None
        assert victim.cause is not None and not victim.send_request_ok
    finally:
        client.shutdown()
        old_sock.close()
        new_sock.close()


def test_shutdown_drains_and_reports_in_flight_requests():
    """Requests still in flight at shutdown must not silently disappear: the callbacks must land with a failure reason."""
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
        assert _wait_until(lambda: len(calls) == 1), "the callback never landed after shutdown"
        assert isinstance(calls[0][1], RemotingSendRequestException)
        assert not client._response_table, "shutdown leaked in-flight entries"
    finally:
        server.stop()
