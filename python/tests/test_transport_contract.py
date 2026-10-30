# -*- coding: utf-8 -*-
"""The connection-lifecycle contract, measured against a loopback peer.

Five behaviours of the transport, none of which a mocked client can show, because all five
are about what the connection table looks like *after* the peer misbehaved:

  - a frame that does not decode (valid length prefix, header that does not fit inside it)
    is logged at ERROR naming the peer, and the connection goes the way a peer-EOF
    connection goes -- the next valid frame from that peer is served on a fresh connection;
  - a length prefix outside the accepted range is the same event one level earlier, so it
    gets the same treatment;
  - a response whose opaque nobody is waiting for is dropped, but not silently: the line
    says which opaque came from which address, and the connection stays up;
  - a connection that carries no traffic in either direction for longer than the configured
    idle limit is closed by the sweep, while one that is still being written to survives --
    and a non-positive configured value restores the default rather than switching the
    sweep off, because the sweep is always installed;
  - a synchronous wait that runs out closes the connection it was sent on by default, and
    keeps it when that is turned off.

Every case asserts on the log line as well as the table, because "the frame disappeared
without a trace" is the defect being fixed here, not the exception path.
"""
from __future__ import annotations

import logging
import socket
import struct
import threading
import time

import pytest

from remoting.client import (DEFAULT_CHANNEL_NOT_ACTIVE_INTERVAL_MILLIS,
                             MAX_FRAME_LENGTH, RemotingClient, _ResponseFuture,
                             _interval_millis_or_default, _mono_millis)
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
    if total <= 0 or total > MAX_FRAME_LENGTH:
        return head
    body = _recv_exact(sock, total)
    if body is None:
        return None
    return head + body


def undecodable_frame() -> bytes:
    """A frame inside a legal length prefix whose own header length does not fit in it.

    decode() raises on this, which is the shape the contract cares about: the peer and this
    client no longer agree on where the next frame starts.
    """
    payload = struct.pack(">i", 0x0FFFFFFF) + b"junk"
    return struct.pack(">i", len(payload)) + payload


def bad_length_frame() -> bytes:
    """A length prefix of -1: the reader cannot even tell how many bytes belong to the frame."""
    return struct.pack(">i", -1)


class ScriptedPeer:
    """Loopback peer that answers every accepted connection according to ``mode``.

    ``mode`` is read per connection, so a test can flip the peer's behaviour between two
    invokes on the same client. Reply modes hold the connection open until ``stop`` so the
    test can read the connection table while the connection is still live.
    """

    def __init__(self, mode: str = "reply"):
        self.mode = mode
        self.connect_count = 0
        self.requests: list = []
        self._listen = socket.socket()
        self._listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listen.bind(("127.0.0.1", 0))
        self._listen.listen(4)
        self.port = self._listen.getsockname()[1]
        self._stop = threading.Event()
        self._threads: list = []
        self._live: list = []
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    @property
    def addr(self) -> str:
        return "127.0.0.1:%d" % self.port

    def stop(self):
        self._stop.set()
        for conn in list(self._live):
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                conn.close()
            except OSError:
                pass
        try:
            self._listen.close()
        except OSError:
            pass
        self._thread.join(timeout=3.0)
        for t in self._threads:
            t.join(timeout=2.0)

    def _loop(self):
        self._listen.settimeout(0.2)
        while not self._stop.is_set():
            try:
                conn, _ = self._listen.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            self.connect_count += 1
            self._live.append(conn)
            t = threading.Thread(target=self._serve, args=(conn,), daemon=True)
            self._threads.append(t)
            t.start()

    def _answer(self, conn, opaque, offset=0):
        resp = rc_mod.RemotingCommand.create_response_command(ResponseCode.SUCCESS, None, None)
        resp.opaque = opaque + offset
        conn.sendall(resp.encode())

    def _serve(self, conn):
        try:
            frame = _read_frame(conn)
            if frame is None:
                return
            mode = self.mode
            if mode == "silent":
                # Accepted, read, and then nothing: the only way a test can make the client
                # reach its own timeout instead of a connection break.
                self._stop.wait(5.0)
                return
            if mode in ("reply", "unmatched_then_reply"):
                cmd = rc_mod.RemotingCommand.decode(frame)
                self.requests.append(cmd)
                if mode == "unmatched_then_reply":
                    # An answer to a request this client never sent: the table has no such
                    # opaque, so the frame must be dropped -- with a line saying so.
                    self._answer(conn, cmd.opaque, offset=99999)
                self._answer(conn, cmd.opaque)
            else:
                self.requests.append(None)
                conn.sendall(undecodable_frame() if mode == "corrupt" else bad_length_frame())
            self._stop.wait(5.0)
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


def _records(caplog, level):
    return [r.getMessage() for r in caplog.records if r.levelno == level]


def _messages(caplog, level):
    return "\n".join(_records(caplog, level))


def _request():
    return rc_mod.RemotingCommand.create_request_command(RequestCode.HEART_BEAT)


# ---------- T1: an undecodable frame takes the connection down ----------
def test_undecodable_frame_logs_error_and_closes_the_connection(caplog):
    peer = ScriptedPeer(mode="corrupt")
    client = RemotingClient()
    try:
        with caplog.at_level(logging.DEBUG):
            with pytest.raises(Exception):
                client.invoke_sync(peer.addr, _request(), 2000)
        assert _wait_until(lambda: peer.addr not in client._conns), \
            "the read loop must stop serving a connection whose frame did not decode"
        assert "cannot decode frame" in _messages(caplog, logging.ERROR), \
            "a decode failure is an error, not a silent drop"
        assert peer.addr in _messages(caplog, logging.ERROR), \
            "the error line has to name the peer the frame came from"
    finally:
        client.shutdown()
        peer.stop()


def test_next_valid_frame_from_the_same_peer_is_served_on_a_fresh_connection(caplog):
    """The point of closing: the broken stream must not poison the connection it is reused on.

    One client, one address, two connects. The first peer answer is undecodable; the second
    replies normally, and that reply has to reach the caller.
    """
    peer = ScriptedPeer(mode="corrupt")
    client = RemotingClient()
    try:
        with caplog.at_level(logging.DEBUG):
            with pytest.raises(Exception):
                client.invoke_sync(peer.addr, _request(), 2000)
        assert _wait_until(lambda: peer.addr not in client._conns)
        peer.mode = "reply"
        response = client.invoke_sync(peer.addr, _request(), 2000)
        assert response.code == ResponseCode.SUCCESS
        assert peer.connect_count == 2, "the second invoke must reconnect, not reuse the dead stream"
    finally:
        client.shutdown()
        peer.stop()


def test_bad_frame_length_takes_the_connection_down_too(caplog):
    peer = ScriptedPeer(mode="bad_length")
    client = RemotingClient()
    try:
        with caplog.at_level(logging.DEBUG):
            with pytest.raises(Exception):
                client.invoke_sync(peer.addr, _request(), 2000)
        assert _wait_until(lambda: peer.addr not in client._conns)
        assert "frame length header" in _messages(caplog, logging.ERROR)
    finally:
        client.shutdown()
        peer.stop()


# ---------- T2: an unmatched response is dropped, with a line ----------
def test_response_with_no_in_flight_request_warns_and_keeps_the_connection(caplog):
    peer = ScriptedPeer(mode="unmatched_then_reply")
    client = RemotingClient()
    try:
        with caplog.at_level(logging.DEBUG):
            response = client.invoke_sync(peer.addr, _request(), 2000)
        assert response.code == ResponseCode.SUCCESS, \
            "the real answer still has to reach the caller after the stray one"
        assert _wait_until(lambda: peer.addr in client._conns)
        warns = _messages(caplog, logging.WARNING)
        assert "no in-flight request" in warns, \
            "a dropped response has to be findable in the log"
        stray = peer.requests[0].opaque + 99999
        assert "opaque=%s" % stray in warns, "the warn names the opaque nobody was waiting for"
        assert peer.addr in warns, "and the address it arrived from"
    finally:
        client.shutdown()
        peer.stop()


# ---------- T3: writable is about the outbound side, not about having connected ----------
def test_is_channel_writable_true_without_a_connection():
    client = RemotingClient()
    try:
        assert client.is_channel_writable("127.0.0.1:1") is True, \
            "an address this client has never connected to is writable: the next send connects"
    finally:
        client.shutdown()


def test_is_channel_writable_true_for_a_live_quiet_connection():
    peer = ScriptedPeer(mode="reply")
    client = RemotingClient()
    try:
        assert client.invoke_sync(peer.addr, _request(), 2000).code == ResponseCode.SUCCESS
        assert client.is_channel_writable(peer.addr) is True, \
            "a connection with room in its send buffer is writable"
    finally:
        client.shutdown()
        peer.stop()


def test_is_channel_writable_false_while_the_send_buffer_is_full():
    client = RemotingClient()
    ours, theirs = socket.socketpair()
    try:
        ours.setblocking(False)
        payload = b"x" * 65536
        while True:
            try:
                ours.send(payload)
            except BlockingIOError:
                break
            except OSError:
                break
        addr = "127.0.0.1:1"
        client._conns[addr] = ours
        assert client.is_channel_writable(addr) is False, \
            "a live connection whose outbound side has no room is the one unwritable case"
        theirs.close()
        while True:
            try:
                ours.send(payload)
            except BlockingIOError:
                continue
            except OSError:
                break
        assert client.is_channel_writable(addr) is True, \
            "once the peer drains the buffer the same connection is writable again"
    finally:
        client.shutdown()
        for s in (ours, theirs):
            try:
                s.close()
            except OSError:
                pass


# ---------- T4: the idle sweep ----------
def test_idle_sweep_closes_a_quiet_connection_and_keeps_a_busy_one(caplog):
    quiet = ScriptedPeer(mode="reply")
    busy = ScriptedPeer(mode="reply")
    client = RemotingClient(channel_not_active_interval_millis=250)
    try:
        assert client.invoke_sync(quiet.addr, _request(), 2000).code == ResponseCode.SUCCESS
        assert client.invoke_sync(busy.addr, _request(), 2000).code == ResponseCode.SUCCESS
        assert client.channel_not_active_interval_millis == 250
        # The thread that reclaims expired requests also does this sweep, so stop it before
        # staging the two stamps: this case is about the decision, and the timer leg is the
        # next one.
        client._stop_sweeper()
        # One connection keeps being written to; the other's stamp is aged past the limit.
        client._conn_last_active[busy.addr] = _mono_millis()
        client._conn_last_active[quiet.addr] = _mono_millis() - 1000
        with caplog.at_level(logging.DEBUG):
            client._sweep_idle_connections()
        assert quiet.addr not in client._conns, \
            "a connection idle past the limit must be gone from the table"
        assert busy.addr in client._conns, \
            "a connection that is still carrying writes is not an idle one"
        assert quiet.addr not in client._conn_last_active, \
            "the swept connection must not leave a stamp behind"
        warns = _messages(caplog, logging.WARNING)
        assert "has been idle" in warns and quiet.addr in warns, \
            "the close is logged against the address it closed"
        assert busy.addr not in warns
    finally:
        client.shutdown()
        quiet.stop()
        busy.stop()


def test_sweep_closes_a_real_idle_connection_after_the_configured_time(caplog):
    """End-to-end leg for the sweep: the thread itself, with no hand-written stamps.

    The peer keeps this connection open for longer than the whole test, so the only way the
    table can lose it is the sweep -- and the sweep, unlike a peer EOF, says so on the way.
    """
    peer = ScriptedPeer(mode="reply")
    client = RemotingClient(channel_not_active_interval_millis=200)
    try:
        assert client.invoke_sync(peer.addr, _request(), 2000).code == ResponseCode.SUCCESS
        with caplog.at_level(logging.DEBUG):
            assert _wait_until(lambda: peer.addr not in client._conns, timeout=4.0), \
                "the sweeper thread must take a connection that stays quiet past the limit"
        assert "has been idle" in _messages(caplog, logging.WARNING), \
            "the close came from the sweep, not from the peer hanging up"
    finally:
        client.shutdown()
        peer.stop()


def test_idle_interval_never_disables_the_sweep():
    """A non-positive configured value falls back to the default: the sweep is always installed."""
    assert _interval_millis_or_default(0, 120000) == 120000
    assert _interval_millis_or_default(-5, 120000) == 120000
    assert _interval_millis_or_default(None, 120000) == 120000
    assert _interval_millis_or_default("not a number", 120000) == 120000
    assert _interval_millis_or_default(500, 120000) == 500
    assert RemotingClient().channel_not_active_interval_millis == \
        DEFAULT_CHANNEL_NOT_ACTIVE_INTERVAL_MILLIS
    assert RemotingClient(channel_not_active_interval_millis=0). \
        channel_not_active_interval_millis == DEFAULT_CHANNEL_NOT_ACTIVE_INTERVAL_MILLIS


# ---------- T5: an expired in-flight entry logs its own removal ----------
def test_expired_in_flight_entries_warn_once_each(caplog):
    client = RemotingClient()
    fired: list = []
    try:
        for opaque in (11, 12):
            future = _ResponseFuture(opaque, 100,
                                     invoke_callback=lambda resp, err, o=opaque: fired.append(o),
                                     request=_request(), addr="127.0.0.1:9001")
            # Push the entry past the expiry rule (begin + timeout + the 1 s grace).
            future.begin_timestamp = _mono_millis() - 2000
            with client._response_lock:
                client._response_table[opaque] = future
        with caplog.at_level(logging.DEBUG):
            client._sweep_expired()
        warns = _records(caplog, logging.WARNING)
        assert len(warns) == 2, "one line per reclaimed entry, not one line per sweep"
        assert all("expired without a response" in line for line in warns)
        assert any("opaque=11" in line for line in warns)
        assert any("opaque=12" in line for line in warns)
        assert fired == [11, 12], "each expired caller fires its callback exactly once"
        assert client._response_table == {}
    finally:
        client.shutdown()


# ---------- T6: a sync wait that runs out ----------
def test_sync_timeout_closes_the_connection_by_default(caplog):
    peer = ScriptedPeer(mode="silent")
    client = RemotingClient()
    try:
        assert client.enable_close_channel_on_sync_timeout is True
        with caplog.at_level(logging.DEBUG):
            with pytest.raises(RemotingTimeoutException):
                client.invoke_sync(peer.addr, _request(), 300)
        assert _wait_until(lambda: peer.addr not in client._conns), \
            "the connection a sync wait timed out on is dropped by default"
        warns = _messages(caplog, logging.WARNING)
        assert "closing the connection" in warns, "the close is logged"
        assert "timed out" in warns, "and so is the timeout itself"
    finally:
        client.shutdown()
        peer.stop()


def test_sync_timeout_keeps_the_connection_when_the_knob_is_off(caplog):
    peer = ScriptedPeer(mode="silent")
    client = RemotingClient(enable_close_channel_on_sync_timeout=False)
    try:
        with caplog.at_level(logging.DEBUG):
            with pytest.raises(RemotingTimeoutException):
                client.invoke_sync(peer.addr, _request(), 300)
        assert peer.addr in client._conns, \
            "with the knob off the timeout is a caller-side event, not a connection event"
        assert "closing the connection" not in _messages(caplog, logging.WARNING)
    finally:
        client.shutdown()
        peer.stop()


# ---------- channel-active listeners ----------
class _ChannelRecorder:
    def __init__(self):
        self.addrs: list = []

    def on_channel_active(self, addr):
        self.addrs.append(addr)


def test_channel_active_fires_once_per_new_connection_and_again_after_a_reconnect():
    """The event marks "this connection just came up", so a listener can restore the
    per-connection broker state (a consumer group's heartbeat) without waiting for the next
    periodic round. Reusing a live connection is not a new one and must not fire."""
    peer = ScriptedPeer(mode="reply")
    client = RemotingClient()
    recorder = _ChannelRecorder()
    client.register_channel_listener(recorder)
    try:
        client.invoke_sync(peer.addr, _request(), 2000)
        assert recorder.addrs == [peer.addr]
        # Reusing the live connection is not a new connection (the scripted peer answers one
        # frame per connection, so ask the table directly instead of sending a second request).
        client._get_or_create_conn(peer.addr)
        assert recorder.addrs == [peer.addr], \
            "an address that already has a live connection has nothing to announce"
        assert peer.connect_count == 1

        client.close_channel(peer.addr)
        client.invoke_sync(peer.addr, _request(), 2000)
        assert recorder.addrs == [peer.addr, peer.addr], \
            "a reconnect is a new connection and has to be announced again"
        assert peer.connect_count == 2
    finally:
        client.shutdown()
        peer.stop()


def test_a_raising_channel_listener_does_not_break_the_invoke(caplog):
    peer = ScriptedPeer(mode="reply")
    client = RemotingClient()

    class _Broken:
        def on_channel_active(self, addr):
            raise RuntimeError("listener exploded")

    client.register_channel_listener(_Broken())
    try:
        with caplog.at_level(logging.WARNING):
            response = client.invoke_sync(peer.addr, _request(), 2000)
        assert response.code == ResponseCode.SUCCESS, "the request still completes"
        assert "channel listener" in _messages(caplog, logging.WARNING), \
            "the listener failure is reported instead of swallowed"
    finally:
        client.shutdown()
        peer.stop()
