# -*- coding: utf-8 -*-
"""Offline unit tests for the TLS transport layer + W3C traceparent propagation.

TLS: start an ssl-wrapped TCP server in-process (self-signed certificate), verifying
RemotingClient(tls_enable=True) can complete the handshake and send/receive RocketMQ frames;
and that a TLS client hitting a plaintext port must report RemotingConnectException.
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

from remoting.client import RemotingClient
from remoting.protocol import remoting_command as rc_mod
from remoting.protocol import headers as headers_mod
from remoting.protocol.codes import RequestCode, ResponseCode

# ---------------------------------------------------------------- traceparent

from client.trace_context import (
    TRACE_CONTEXT_PROPERTY, child_traceparent, extract_traceparent,
    generate_traceparent, inject_trace_context, is_valid_traceparent)
from common.message import Message, MessageExt


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
    assert is_valid_traceparent(good.upper())          # lenient: uppercase also accepted
    assert not is_valid_traceparent("")                # empty
    assert not is_valid_traceparent("00-abc-def-01")   # wrong segment length
    assert not is_valid_traceparent("00-%s-%s-01" % ("0" * 32, "1" * 16))   # trace-id all zeros
    assert not is_valid_traceparent("00-%s-%s-01" % ("1" * 32, "0" * 16))   # parent-id all zeros
    assert not is_valid_traceparent("ff-%s-%s-01" % ("1" * 32, "1" * 16))   # version ff
    assert not is_valid_traceparent("zz-%s-%s-01" % ("1" * 32, "1" * 16))   # version non-hex


def test_child_traceparent_keeps_trace_id():
    parent = generate_traceparent()
    child = child_traceparent(parent)
    assert child is not None and is_valid_traceparent(child)
    assert child.split("-")[1] == parent.split("-")[1]   # trace-id unchanged
    assert child.split("-")[2] != parent.split("-")[2]   # parent-id renewed
    assert child_traceparent("garbage") is None


def test_inject_does_not_overwrite_and_extract_roundtrip():
    msg = Message(topic="t", body=b"x")
    tp1 = inject_trace_context(msg)
    assert msg.get_property(TRACE_CONTEXT_PROPERTY) == tp1
    tp2 = inject_trace_context(msg)                      # an existing value is not overwritten
    assert tp2 == tp1

    ext = MessageExt()
    ext.properties[TRACE_CONTEXT_PROPERTY] = tp1
    assert extract_traceparent(ext) == tp1
    assert extract_traceparent(MessageExt()) is None


# ---------------------------------------------------------------- TLS

@pytest.fixture(scope="module")
def tls_cert():
    """Generate a self-signed certificate with the openssl CLI (used on the server side)."""
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
    """An ssl-wrapped frame echo server: receives request frames -> replies with a SUCCESS response (same opaque)."""

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
        """The accept loop: handshake and send/receive both happen in the connection thread.

        The handshake must not be executed inline in the accept thread: a half-open connection whose peer has already vanished would stick the accept
        thread on ``wrap_socket``, while on Windows connect() into the backlog succeeds --
        the real client would only wait out the full timeout (this test used to hang at 15s exactly this way).
        """
        try:
            while True:
                conn, _ = self._listen.accept()
                threading.Thread(target=self._handshake_and_serve, args=(conn,),
                                 daemon=True).start()
        except OSError:
            pass  # stop() closes the listening socket

    def _handshake_and_serve(self, conn):
        try:
            tls = self._ctx.wrap_socket(conn, server_side=True)
        except (OSError, ssl.SSLError):
            conn.close()
            return
        self._serve(tls)

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
            self._close_tls(tls)

    @staticmethod
    def _close_tls(tls):
        """close_notify first, then close: consistent with Netty's SslHandler shutdown behavior.

        When closing the socket directly, if the kernel receive buffer still holds an unread TLS 1.3 NewSessionTicket,
        Windows sends RST instead of FIN; this RST can hit the next new connection reusing the same 4-tuple,
        silently swallowing its first request.
        """
        try:
            tls.settimeout(2.0)
            tls.unwrap()
        except (OSError, ssl.SSLError, ValueError):
            pass
        try:
            tls.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
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


def _route_request():
    return rc_mod.RemotingCommand.create_request_command(
        RequestCode.GET_ROUTEINFO_BY_TOPIC,
        (lambda h: (setattr(h, "topic", "t"), h)[1])(headers_mod.GetRouteInfoRequestHeader()))


def test_invoke_sync_over_tls(tls_cert):
    server = TlsFrameServer(*tls_cert)
    try:
        client = RemotingClient(tls_enable=True)
        req = _route_request()
        resp = client.invoke_sync("127.0.0.1:%d" % server.port, req, 5000)
        assert resp.code == ResponseCode.SUCCESS
        assert resp.opaque == req.opaque
        client.shutdown()
    finally:
        server.stop()


def test_tls_reconnect_cycles_every_request_round_trips(tls_cert):
    """Repeatedly building/dropping TLS connections: the first request of every new connection must be delivered.

    Two pitfalls measured on a real machine are being watched:

    1. The shutdown path. If the previous round's connection was closed hard (no close_notify sent), the kernel receive buffer still holds the peer's
       unread TLS 1.3 NewSessionTicket, so RST is sent instead of FIN; this RST hits the next new connection reusing the same
       4-tuple, and the new connection's first request is silently swallowed.
    2. The read thread starts too early. Letting the read thread enter OpenSSL right after the handshake, the TLS 1.3 first
       record written immediately after has about a 3~5% chance of never reaching the peer (the peer's TLS read waits until timeout), and the caller can only wait out the invoke
       timeout; plaintext connections do not show this. So TLS connections defer the read thread until after the first record is written
       (see RemotingClient._write).

    Before the fix, each new loopback connection hung about 25~30% of the time; with both fixes it must be 0 times. 30 rounds and not just
    12: at a 5% per-round probability, 12 rounds only gives about a coin-flip chance of catching the regression.
    """
    server = TlsFrameServer(*tls_cert)
    addr = "127.0.0.1:%d" % server.port
    try:
        for _ in range(30):
            client = RemotingClient(tls_enable=True)
            try:
                # 10s instead of 3s: what is asserted here is "the request must be delivered", not latency. When the whole suite runs concurrently
                # the TLS handshake on Windows occasionally exceeds 3s, which would be misjudged as the first packet being swallowed.
                resp = client.invoke_sync(addr, _route_request(), 10000)
                assert resp.code == ResponseCode.SUCCESS
            finally:
                client.shutdown()
    finally:
        server.stop()


def test_tls_reader_starts_only_after_the_first_record(tls_cert):
    """A TLS connection's read thread must start only after the first record is written; plaintext connections still start immediately."""
    server = TlsFrameServer(*tls_cert)
    addr = "127.0.0.1:%d" % server.port
    try:
        client = RemotingClient(tls_enable=True)
        try:
            sock = client._get_or_create_conn(addr)
            assert isinstance(sock, ssl.SSLSocket)
            assert client._reader_threads.get(addr) is None, \
                "the TLS read thread started when the connection was built, so the first packet would race with the write path for OpenSSL"
            assert client.invoke_sync(addr, _route_request(), 5000).code == ResponseCode.SUCCESS
            reader = client._reader_threads.get(addr)
            assert reader is not None and reader.is_alive()
            # the second request reuses the same connection: the read thread is already running, unaffected
            assert client.invoke_sync(addr, _route_request(), 5000).code == ResponseCode.SUCCESS
        finally:
            client.shutdown()
        assert not [t for t in threading.enumerate() if t.name.startswith("rmq-read-")]
    finally:
        server.stop()


def test_tls_shutdown_leaves_no_reader_threads(tls_cert):
    """shutdown() must wait for the read threads to exit cleanly: the socket is closed by the read thread itself, and someone else grabbing the close is unsafe."""
    server = TlsFrameServer(*tls_cert)
    addr = "127.0.0.1:%d" % server.port
    before = {t.name for t in threading.enumerate()}
    try:
        for _ in range(3):
            client = RemotingClient(tls_enable=True)
            resp = client.invoke_sync(addr, _route_request(), 3000)
            assert resp.code == ResponseCode.SUCCESS
            client.shutdown()
    finally:
        server.stop()
    leaked = [t.name for t in threading.enumerate()
              if t.name not in before and t.name.startswith("rmq-read-")]
    assert not leaked, "reader threads survived shutdown: %s" % leaked


def test_tls_client_to_plaintext_server_fails():
    """A TLS client hitting a plaintext port: the handshake must fail (the response received is not a TLS handshake)."""
    listen = socket.socket()
    listen.bind(("127.0.0.1", 0))
    listen.listen(4)

    def loop():
        try:
            while True:
                conn, _ = listen.accept()
                conn.close()   # close directly: the TLS handshake must fail
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
