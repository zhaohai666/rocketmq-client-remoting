# -*- coding: utf-8 -*-
"""Long-lived socket client.

Provides: connection management (lazy connect + reuse), invokeSync/invokeAsync/invokeOneway,
opaque-keyed callback dispatch, timeout control, connection liveness probing, GO_AWAY
reconnect-and-resend, and immediate failure of in-flight requests on a connection when that
connection drops (fail-fast).

Each connection carries a last-activity stamp, refreshed on every successful socket read and on
every completed write. The thread that reclaims expired in-flight requests also watches those
stamps and closes any connection that has carried no traffic at all for strictly longer than
channel_not_active_interval_millis (default 120 s), logging one WARN per closed connection, so an
all-quiet connection cannot sit in the connection table indefinitely. A connection whose incoming
bytes no longer fit the framing rules -- a length header outside the allowed range, or a frame
whose header cannot be decoded -- is taken down the same way a peer EOF takes it down: the read
loop ends, the socket closes, the in-flight requests still on that connection fail fast, and the
other connections keep working. A response whose opaque is not in the in-flight table is dropped
with a WARN naming it, and a synchronous invoke whose wait runs out closes that connection before
the timeout is raised (unless enable_close_channel_on_sync_timeout is turned off).
"""
from __future__ import annotations

import os
import select
import socket
import ssl
import struct
import threading
import time
from typing import Callable, Dict, List, Optional

from .exception import (RemotingConnectException, RemotingSendRequestException,
                        RemotingTimeoutException)
from .protocol.codes import ResponseCode
from .protocol.remoting_command import RemotingCommand
from .socks_proxy import SocksProxyConfig, SocksProxyError, socks_handshake
from rocketmq_logging import get_logger

logger = get_logger()

MAX_FRAME_LENGTH = 16 * 1024 * 1024

# Default idle limit of one connection. A connection that has had neither a successful socket
# read nor a completed write for strictly longer than this is closed by the sweep thread.
DEFAULT_CHANNEL_NOT_ACTIVE_INTERVAL_MILLIS = 120 * 1000
# Tick of the sweep thread: it reclaims expired in-flight requests and idle connections.
_SWEEP_INTERVAL_SECONDS = 0.1
# A synchronous request that ran out of time closes its connection, unless the budget its wait
# started with was tiny: below both floors a reconnect would not have finished in the leftover
# time anyway, so the connection is kept.
MIN_LEFT_MILLIS_TO_CLOSE_ON_SYNC_TIMEOUT = 100.0

# Poll interval for the reader thread waiting on data. Even when a connection stays quiet, "I should close" must be noticed within this second.
_READ_POLL_SECONDS = 1.0
# Upper bound for waiting on the TLS close_notify: a peer that never sends close_notify must not hang the close path.
_TLS_SHUTDOWN_TIMEOUT_SECONDS = 2.0
# Total budget for shutdown() to wait for reader threads to exit (not this long per thread).
_SHUTDOWN_JOIN_SECONDS = 5.0


# Dedicated monotonic clock for timeout decisions: changing the system time (forward or back) must not
# make in-flight requests time out early or never, so timeout accounting always uses the monotonic clock
# rather than the wall clock.
def _mono_millis() -> float:
    return time.monotonic() * 1000.0


def _interval_millis_or_default(value, default_millis: int) -> int:
    """Normalize a configured idle interval: a finite positive number of milliseconds is kept,
    anything else (missing, non-numeric, zero, negative) falls back to the default.

    The sweep is always installed, so no configured value switches it off.
    """
    try:
        millis = int(float(value))
    except (TypeError, ValueError, OverflowError):
        return default_millis
    return millis if millis > 0 else default_millis


def _retry_request(request: RemotingCommand) -> RemotingCommand:
    """Request copy used for a GO_AWAY resend (copies code/custom header/body/ext_fields, only opaque changes).

    It must be a **new** opaque: the old one has already been removed from the in-flight table, and
    reusing it would mismatch the response to the wrong request.
    ext_fields is copied by value — the ACL signature (AccessKey/Signature) lives there and the
    resend must carry the original signature, so the copy must not be re-derived from custom_header
    (re-deriving would drop those two signature fields).
    """
    retry = RemotingCommand(code=request.code, custom_header=request.custom_header)
    retry.language = request.language
    retry.version = request.version
    retry.flag = request.flag
    retry.remark = request.remark
    retry.ext_fields = dict(request.ext_fields)
    retry.body = request.body
    retry.serialize_type_current_rpc = request.serialize_type_current_rpc
    retry.cached_header = request.cached_header
    return retry


def _close_socket(sock: socket.socket) -> None:
    """Close a connection cleanly: for TLS do the handshake-level close (close_notify) first, then close the underlying socket.

    Omitting close_notify has two consequences observed locally:

    1. The kernel receive buffer still holds the peer's TLS 1.3 NewSessionTicket that the SSL layer
       never read, so ``closesocket()`` sends RST instead of FIN. That RST hits the next new
       connection reusing the same 4-tuple — on loopback, "a new TLS connection per round" was
       measured to silently swallow the first request about 25~30% of the time, leaving callers to
       wait out the full invoke timeout (plaintext and non-TLS paths: 0%). With close_notify added
       this cause went to zero, though about 3~5% remained at the time, caused by the reader thread
       starting too early; see _write.
    2. On the broker side only an abnormal disconnect is visible; a normal shutdown is indistinguishable
       from a real drop.

    Only call it on a socket that is **no longer recv/send by another thread**: two threads entering
    OpenSSL simultaneously corrupt its internal state (measured: outright segfault), so closing is
    always done by that connection's reader thread.
    """
    if isinstance(sock, ssl.SSLSocket):
        try:
            sock.settimeout(_TLS_SHUTDOWN_TIMEOUT_SECONDS)
        except (OSError, ValueError):
            pass
        try:
            sock.unwrap()  # send close_notify and drain the peer's copy
        except (OSError, ssl.SSLError, ValueError):
            pass
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


class _ResponseFuture:
    """One in-flight request: records the callback, timeout accounting, and the socket actually written to.

    Callback contract (success/failure dichotomy):
    ``callback(response, error)`` — on success error is None; on timeout/failure response is None.
    The callback fires **exactly once**: the reader thread and the timeout sweeper race for the same
    future, decided by the once flag in ``execute_invoke_callback``.
    """

    def __init__(self, opaque: int, timeout_millis: int, invoke_callback=None,
                 request: Optional[RemotingCommand] = None, addr: str = ""):
        self.opaque = opaque
        self.timeout_millis = timeout_millis
        self.invoke_callback = invoke_callback
        # Keep the request command for the do_after_response hook
        self.request = request
        # Timeout accounting (start timestamp + timeout budget): monotonic clock, unaffected by system time changes
        self.addr = addr
        self.begin_timestamp = _mono_millis()
        self.response: Optional[RemotingCommand] = None
        self.send_request_ok = False
        # The socket the request was actually written on.
        # When a connection drops, in-flight requests are claimed by **object identity**: comparing only
        # the address string would cause collateral damage — the same address may already have a new
        # connection (exactly the shape of close_channel/GO_AWAY resend), and the old connection's reader
        # thread must not kill requests on the new one too. None means not written out yet, so this
        # connection's failFast must not implicate it.
        self.conn: Optional[socket.socket] = None
        # Failure cause. Synchronous waiters use it to tell "send failed" from "timeout":
        # a dropped connection (request definitely not sent or write failed) reports RemotingSendRequestException;
        # only waiting out the full budget with no response reports RemotingTimeoutException.
        self.cause: Optional[BaseException] = None
        self._done = threading.Event()
        self._lock = threading.Lock()
        self._callback_once = threading.Lock()
        self._callback_fired = False
        self._timer = None

    def put_response(self, cmd: RemotingCommand) -> None:
        with self._lock:
            self.response = cmd
        self._done.set()

    def wait_response(self) -> RemotingCommand:
        self._done.wait(timeout=self.timeout_millis / 1000.0)
        return self.response

    def is_timeout(self) -> bool:
        # Strictly greater counts as timeout: the exact deadline moment does not
        return _mono_millis() - self.begin_timestamp > self.timeout_millis

    def execute_invoke_callback(self, error: Optional[BaseException] = None) -> bool:
        """Deliver the async callback, at most once. Returns whether this call actually delivered it."""
        if self.invoke_callback is None:
            return False
        with self._callback_once:
            if self._callback_fired:
                return False
            self._callback_fired = True
        response = self.response if error is None else None
        try:
            self.invoke_callback(response, error)
        except Exception as e:
            # An exception raised inside the callback must not take down the reader/sweeper thread; just log it
            logger.warning("remoting: invoke callback raised: %s" % e)
        return True

    def fail_fast(self) -> bool:
        """Connection is dead: mark this in-flight request as a send failure.

        All four steps are required: set ``send_request_ok=false``, ``put_response(None)``,
        ``execute_invoke_callback``, and return resources. The first step is the **semantic** key —
        with ``send_request_ok=false`` it reports ``RemotingSendRequestException`` (not a timeout),
        and the async send's retry classification relies on that difference.
        ``put_response(None)`` is what wakes the synchronous waiter: without it, a caller whose send
        already failed would still wait out the full invoke timeout. Resource return does not exist at
        the transport layer (back-pressure permits are returned by the producer's callback chain,
        see ``_release`` in ``client/producer.py``); delivering the callback triggers the return.
        """
        self.send_request_ok = False
        self.cause = RemotingSendRequestException(self.addr, "connection closed")
        self.put_response(None)
        return self.execute_invoke_callback(error=self.cause)


class RemotingClient:
    def __init__(self, connect_timeout_millis: int = 3000, invoke_timeout_millis: int = 15000,
                 tls_enable: Optional[bool] = None,
                 tls_options: Optional[Dict[str, str]] = None,
                 enable_reconnect_for_go_away: bool = True,
                 socks_proxy_config: Optional[SocksProxyConfig] = None,
                 channel_not_active_interval_millis: int = DEFAULT_CHANNEL_NOT_ACTIVE_INTERVAL_MILLIS,
                 enable_close_channel_on_sync_timeout: bool = True):
        self.connect_timeout_millis = connect_timeout_millis
        self.invoke_timeout_millis = invoke_timeout_millis
        # Whether to switch to another connection and resend once when
        # ResponseCode.GO_AWAY(1500) arrives (default **true**).
        self.enable_reconnect_for_go_away = enable_reconnect_for_go_away
        # How long a connection may carry no traffic at all before the sweep thread closes it
        # (a non-positive configured value falls back to the 120 s default).
        self.channel_not_active_interval_millis = _interval_millis_or_default(
            channel_not_active_interval_millis, DEFAULT_CHANNEL_NOT_ACTIVE_INTERVAL_MILLIS)
        # Whether a synchronous invoke whose wait ran out also closes the connection it was
        # sent on (default **true**); see _close_channel_after_sync_timeout for the floors.
        self.enable_close_channel_on_sync_timeout = bool(enable_close_channel_on_sync_timeout)
        # TLS switch. An explicit argument wins, otherwise the ROCKETMQ_TLS_ENABLE
        # environment variable is read (equivalent to the broker-side
        # tls.enable system property).
        if tls_enable is None:
            tls_enable = os.environ.get("ROCKETMQ_TLS_ENABLE", "").strip().lower() in ("1", "true", "yes")
        self.tls_enable = bool(tls_enable)
        # TLS details (certificate path family: caCert / clientCert / clientKey / serverName).
        #   caCert: CA certificate path -- when given, **strict verification** is used
        #           (the tls.test.mode.enable=false flavour): the broker certificate must
        #           chain up to that CA and the hostname/SAN must match (the connect host
        #           is used by default); when absent = test-mode (trust self-signed,
        #           CERT_NONE, historical behaviour).
        #   clientCert/clientKey: mTLS client certificate and private key paths.
        #   serverName: hostname/SNI override (defaults to the connect host).
        self.tls_options: Dict[str, str] = dict(tls_options or {})
        self.socks_proxy_config = socks_proxy_config
        self._lock = threading.RLock()
        self._conns: Dict[str, socket.socket] = {}
        self._sock_locks: Dict[str, threading.Lock] = {}
        self._response_table: Dict[int, _ResponseFuture] = {}
        self._response_lock = threading.Lock()
        # In-flight request timeout sweeping and idle connection sweeping share one thread, which
        # periodically scans the in-flight table, reclaims expired requests and delivers their
        # failure callbacks, and closes connections that have carried no traffic for too long.
        # Without it an asynchronous request that never gets a response hangs forever:
        # the entry leaks in the table and the callback never fires, and a connection that goes
        # quiet stays in the connection table until the operating system reaps it.
        # The thread starts on the first connection or the first invoke_async, so a client that
        # never connected anywhere leaves no thread behind.
        self._sweeper_lock = threading.Lock()
        self._sweeper_stop = threading.Event()
        self._sweeper_thread: Optional[threading.Thread] = None
        self._running = True
        self._closed = False
        self.rpc_hooks = []
        # Channel listeners: each one is told when a connection has just been established, so a
        # caller can re-register per-connection broker state (a consumer group's heartbeat) without
        # waiting for the next periodic round. A broker drops a consumer group when the last channel
        # of that group closes, so after a reconnect the group is unknown to it again until the next
        # heartbeat arrives. Notified after the connection is in the table (never under ``_lock``:
        # a listener that sends a request would otherwise re-enter the table).
        self._channel_listeners: List = []
        self._channel_listeners_lock = threading.Lock()
        # Per-connection reader thread
        self._reader_threads: Dict[str, threading.Thread] = {}
        # Per-connection "should exit" flag: the reader thread uses it to wind down and
        # close the socket without blocking itself forever
        self._conn_stops: Dict[str, threading.Event] = {}
        # Per-connection last-activity stamp (monotonic milliseconds), refreshed by the reader
        # thread on every successful socket read and by the write path on every completed write;
        # the sweep thread compares it against channel_not_active_interval_millis. Single-key
        # assignment/read is atomic here, so this dict is used lock-free.
        self._conn_last_active: Dict[str, float] = {}
        # Broker-initiated request processors: request_code -> handler(cmd, addr) -> None
        self._processors: Dict[int, Callable] = {}

    # ---------- connection management ----------
    def register_channel_listener(self, listener) -> None:
        """Register an object whose ``on_channel_active(addr)`` runs after a new connection is up."""
        with self._channel_listeners_lock:
            self._channel_listeners.append(listener)

    def _fire_channel_active(self, addr: str) -> None:
        with self._channel_listeners_lock:
            listeners = list(self._channel_listeners)
        for listener in listeners:
            try:
                listener.on_channel_active(addr)
            except Exception as e:  # noqa: BLE001
                logger.warning("remoting: channel listener for %s raised: %s", addr, e)

    def _get_or_create_conn(self, addr: str) -> socket.socket:
        # Idle connections are reclaimed by the sweep thread, and a connection can also be
        # created by a purely synchronous call, so the thread is started here rather than only
        # on the first invoke_async.
        self._ensure_sweeper()
        with self._lock:
            conn = self._conns.get(addr)
            if conn is not None and self._is_alive(conn):
                return conn
            sock = self._create_conn(addr)
            self._conns[addr] = sock
            self._sock_locks[addr] = threading.Lock()
            self._conn_last_active[addr] = _mono_millis()
            stop = threading.Event()
            self._conn_stops[addr] = stop
            # Plaintext connections spawn the reader thread right away; TLS connections
            # wait until the first request has been written, see the notes in _write.
            if not isinstance(sock, ssl.SSLSocket):
                self._spawn_reader(addr, sock, stop)
        # Outside ``_lock``: a listener typically sends a request on the new connection, which
        # takes the same lock through this method again.
        self._fire_channel_active(addr)
        return sock

    def _spawn_reader(self, addr: str, sock: socket.socket,
                      stop: threading.Event) -> None:
        """Start the reader thread. The caller must hold ``self._lock``."""
        t = threading.Thread(target=self._read_loop, args=(addr, sock, stop), daemon=True,
                             name="rmq-read-%s" % addr)
        t.start()
        self._reader_threads[addr] = t

    def _start_reader(self, addr: str) -> None:
        """Start a deferred reader thread; a no-op if it is already running.

        Closing a connection and shutdown also go through here: the socket is always
        closed by that connection's own reader thread (see _close_socket), so a
        connection whose reader thread "has not started yet" must have it started
        first, otherwise nobody closes that file descriptor.
        """
        with self._lock:
            if addr in self._reader_threads:
                return
            sock = self._conns.get(addr)
            stop = self._conn_stops.get(addr)
            if sock is None or stop is None:
                return
            self._spawn_reader(addr, sock, stop)

    def _create_conn(self, addr: str) -> socket.socket:
        host, port = self._parse_addr(addr)
        proxy = self.socks_proxy_config
        use_proxy = proxy is not None and proxy.is_enabled()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.connect_timeout_millis / 1000.0)
        try:
            if use_proxy:
                proxy_host, proxy_port = proxy.endpoint()
                sock.connect((proxy_host, int(proxy_port)))
                socks_handshake(sock, host, int(port), proxy)
            else:
                sock.connect((host, int(port)))
        except OSError:
            sock.close()
            raise RemotingConnectException(addr)
        except SocksProxyError as e:
            sock.close()
            raise RemotingConnectException("%s (socks proxy: %s)" % (addr, e))
        if self.tls_enable:
            # TLS wraps the whole stream and completes the handshake before any RocketMQ
            # frame. test-mode (broker-side tls.test.mode.enable defaults to
            # true) = trust the broker's self-signed certificate, do not verify the
            # hostname and present no client certificate (a PERMISSIVE broker works
            # out of the box).
            # When tls_options['caCert'] is given, **strict verification** is used:
            # the certificate chain must reach that CA and the hostname/SAN must
            # match (PROTOCOL_TLS_CLIENT defaults to check_hostname=True).
            try:
                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ca_cert = self.tls_options.get("caCert")
                client_cert = self.tls_options.get("clientCert")
                if client_cert:
                    # mTLS: test-mode also allows a client certificate (it must be presentable
                    # whenever the server demands one)
                    ctx.load_cert_chain(certfile=client_cert,
                                        keyfile=self.tls_options.get("clientKey"))
                if ca_cert:
                    ctx.load_verify_locations(cafile=ca_cert)
                    server_name = self.tls_options.get("serverName") or host
                else:
                    ctx.check_hostname = False
                    ctx.verify_mode = ssl.CERT_NONE
                    server_name = host
                sock = ctx.wrap_socket(sock, server_hostname=server_name)
            except (OSError, ssl.SSLError) as e:
                try:
                    sock.close()
                except OSError:
                    pass
                raise RemotingConnectException("%s (tls handshake: %s)" % (addr, e))
        sock.settimeout(None)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return sock

    @staticmethod
    def _parse_addr(addr: str):
        if addr.startswith("["):  # IPv6
            host, _, rest = addr[1:].partition("]")
            port = rest.lstrip(":")
            return host, port
        host, _, port = addr.rpartition(":")
        return host, port

    @staticmethod
    def _is_alive(sock: socket.socket) -> bool:
        try:
            r, _, _ = select.select([sock], [], [], 0)
            if r:
                # Readable data may be a response or a peer close; try peername to tell
                try:
                    sock.getpeername()
                    return True
                except OSError:
                    return False
            return True
        except OSError:
            return False

    def close_channel(self, addr: str) -> None:
        """Drop the connection to addr: remove it from the connection table first, then
        close it.

        The socket itself is closed by that connection's reader thread: here we only
        set the stop flag and wait for the thread to exit. Closing an SSLSocket
        directly on the calling thread is unsafe -- the reader thread may be blocked
        in recv on the very same socket, and two threads entering OpenSSL at once
        corrupt its internal state (observed segfault). A TLS connection whose reader
        thread has not started yet (see _write) gets its reader started first,
        otherwise nobody closes that file descriptor.
        """
        self._start_reader(addr)
        with self._lock:
            self._conns.pop(addr, None)
            self._conn_last_active.pop(addr, None)
            stop = self._conn_stops.pop(addr, None)
            thread = self._reader_threads.pop(addr, None)
        if stop is not None:
            stop.set()
        # When writing the response fails, close_channel may run on the reader thread
        # itself (the reply path for broker-initiated requests); in that case we must
        # not join ourselves, so let its own loop wind down.
        if (thread is not None and thread.is_alive()
                and thread is not threading.current_thread()):
            thread.join(timeout=2 * _READ_POLL_SECONDS + _TLS_SHUTDOWN_TIMEOUT_SECONDS)

    def is_channel_writable(self, addr: str) -> bool:
        """Whether a request sent to ``addr`` right now can go out on the wire.

        An address with no connection reports True: this transport connects on demand, so "not
        connected yet" is not "cannot write" -- the next send simply creates the connection. The
        same holds for a connection that is no longer usable (closed, or its peer gone): the next
        send replaces it. Only an existing connection that is still live and whose outbound side
        currently has no room for more data reports False.
        """
        conn = self._conns.get(addr)
        if conn is None:
            return True
        try:
            conn.getpeername()
        except OSError:
            return True
        try:
            _, writable, _ = select.select([], [conn], [], 0)
        except (OSError, ValueError):
            return True
        return bool(writable)

    # ---------- read loop ----------
    @staticmethod
    def _wait_readable(sock: socket.socket, stop: threading.Event) -> bool:
        """Wait for readable data, at most _READ_POLL_SECONDS; returns False when stop is
        set or the wait times out.

        For TLS, ``pending()`` is checked first: records that OpenSSL has already
        decrypted but the application has not read yet do not make the file
        descriptor readable, so relying on select alone would leave those bytes
        sitting there -- and they are exactly what triggers an RST on closesocket.
        """
        pending = getattr(sock, "pending", None)
        if callable(pending):
            try:
                if pending() > 0:
                    return True
            except OSError:
                return True
        deadline = time.monotonic() + _READ_POLL_SECONDS
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            if stop.is_set():
                return False
            try:
                readable, _, _ = select.select([sock], [], [], remaining)
            except (OSError, ValueError):
                # If select fails (the fd was already closed, etc.) let recv report it, the same
                # behaviour as when no polling is used
                return True
            if readable:
                return True

    def _read_loop(self, addr: str, sock: socket.socket,
                   stop: Optional[threading.Event] = None) -> None:
        """The reader thread of one connection.

        On exit **this thread** closes the socket: closing an SSLSocket behind the
        reader thread's back means two threads entering OpenSSL at once, which
        corrupts its internal state (observed to segfault the whole process).
        """
        stop = stop if stop is not None else threading.Event()
        buf = bytearray()
        try:
            while self._running and not stop.is_set():
                if not self._wait_readable(sock, stop):
                    continue
                try:
                    chunk = sock.recv(65536)
                except socket.timeout:
                    continue
                except OSError:
                    break
                if not chunk:
                    break
                self._conn_last_active[addr] = _mono_millis()
                buf += chunk
                while True:
                    if len(buf) < 4:
                        break
                    (total_len,) = struct.unpack_from(">i", bytes(buf), 0)
                    if total_len <= 0 or total_len > MAX_FRAME_LENGTH:
                        # A length header outside the framing range does not desynchronise only
                        # this frame: every byte after it belongs to an unknown frame boundary,
                        # so the stream is no longer trustworthy and the connection goes.
                        logger.error(
                            "remoting: frame length header %d from %s is outside the accepted range "
                            "(1..%d), closing the connection", total_len, addr, MAX_FRAME_LENGTH)
                        return
                    if len(buf) < 4 + total_len:
                        break
                    frame = bytes(buf[:4 + total_len])
                    del buf[:4 + total_len]
                    if not self._dispatch(frame, addr):
                        # _dispatch already logged why: stop serving this connection. The finally
                        # block below runs the same teardown a peer EOF runs.
                        return
        finally:
            with self._lock:
                if self._conns.get(addr) is sock:
                    self._conns.pop(addr, None)
                    # Only this connection's stamp: an address that already has a replacement
                    # connection must keep the stamp the new one wrote.
                    self._conn_last_active.pop(addr, None)
                if self._reader_threads.get(addr) is threading.current_thread():
                    self._reader_threads.pop(addr, None)
                    self._conn_stops.pop(addr, None)
            _close_socket(sock)
            # The peer will never answer these requests again: declare them dead right away
            # instead of making the caller wait out the full timeout (fail-fast)
            self._fail_fast(addr, sock)

    def _dispatch(self, frame: bytes, addr: str) -> bool:
        """Handle one complete frame from ``addr``; False means this connection must be taken down.

        A header that does not decode is reported rather than skipped: the frame arrived inside a
        valid length prefix, so the peer and this client no longer agree on the protocol, and
        keeping the socket would leave every later frame of that connection misinterpreted.
        """
        try:
            cmd = RemotingCommand.decode(frame)
        except Exception as e:
            logger.error("remoting: cannot decode frame from %s (%d byte(s)): %s; "
                         "closing the connection", addr, len(frame), e, exc_info=True)
            return False
        if cmd.is_response_type():
            future = None
            with self._response_lock:
                future = self._response_table.pop(cmd.opaque, None)
            if future is not None:
                future.put_response(cmd)
                self._apply_after_response_hooks(addr, future.request, cmd)
                future.execute_invoke_callback()
            else:
                # Nobody is waiting for this opaque: it was already reclaimed (the caller timed
                # out, or the sweep thread removed the entry) or this client never sent such a
                # request. Nothing to deliver, so drop it -- but log it, otherwise the frame
                # disappears from this connection without a trace.
                logger.warning("remoting: response from %s has no in-flight request to answer "
                               "(opaque=%s, code=%s), dropping it", addr, cmd.opaque, cmd.code)
            return True
        # Non-response command: a request initiated by the broker (such as
        # CHECK_TRANSACTION_STATE=39). The opaque of such a request is generated by
        # the broker and never appears in the local in-flight table, so treat it as
        # a broker-initiated request.
        future = None
        with self._response_lock:
            future = self._response_table.pop(cmd.opaque, None)
        if future is not None:
            # Fallback for the odd case: it should have been a response from the peer
            # but carried no response flag
            future.put_response(cmd)
            self._apply_after_response_hooks(addr, future.request, cmd)
            future.execute_invoke_callback()
            return True
        handler = self._processors.get(cmd.code)
        if handler is not None:
            # The handler **may** return a response command. The typical case that needs one
            # is PUSH_REPLY_MESSAGE_TO_CLIENT(326): the broker pushes its reply with
            # invokeSync, and without a response the client side would wait until it
            # times out. Transaction check 39 is sent by the broker as oneway, so the
            # handler may just return None.
            response = None
            try:
                response = handler(cmd, addr)
            except Exception:
                logger.warning("processor for request code %s raised", cmd.code, exc_info=True)
                response = RemotingCommand.create_response_command(
                    ResponseCode.SYSTEM_ERROR, "process request fail", None)
            if response is not None and not cmd.is_oneway_rpc():
                response.opaque = cmd.opaque
                self._write_response(addr, response)
        else:
            logger.debug("no processor registered for request code %s (opaque=%s) from %s",
                         cmd.code, cmd.opaque, addr)
        return True

    def register_processor(self, request_code: int,
                           handler: Callable[["RemotingCommand", str], Optional[RemotingCommand]]) -> None:
        """Register a processor for broker-initiated requests (a dispatch table keyed by
        RequestCode).

        The handler signature is ``handler(cmd, addr) -> Optional[RemotingCommand]``:
        ``cmd`` is the decoded RemotingCommand (with ext_fields / body), ``addr`` is
        the peer (broker) address. **Returning a non-None value writes that command
        back as the response** (the framework fills in the request's opaque); returning
        None means no response is needed (a oneway request such as the transaction
        check CHECK_TRANSACTION_STATE(39)).

        Note: it is dispatched here only when the command is of "request type" and is
        not present in the local in-flight response table, so it does not interfere
        with the existing invokeSync/invokeAsync response dispatch.
        """
        self._processors[request_code] = handler

    def unregister_processor(self, request_code: int) -> None:
        self._processors.pop(request_code, None)

    # ---------- RPC hooks ----------
    def _apply_before_request_hooks(self, addr: str, cmd: RemotingCommand) -> None:
        """Run the RPC hooks in order before sending.

        **Must be called before cmd.encode()**: the ACL hook writes
        AccessKey/Signature into ext_fields, and the Signature covers the complete
        ext_fields + body content produced after makeCustomHeaderToNet() -- that is,
        exactly the bytes that will go on the wire. Injecting after encode() is too
        late.
        """
        for hook in tuple(self.rpc_hooks):
            hook.do_before_request(addr, cmd)

    def _apply_after_response_hooks(self, addr: str, request: Optional[RemotingCommand],
                                    response: Optional[RemotingCommand]) -> None:
        """Run the RPC hooks in order after a response arrives.

        Zero overhead when no hook is registered; if the request is missing (theoretically
        impossible) the hooks are skipped and response dispatch is unaffected.
        """
        if not self.rpc_hooks or request is None:
            return
        for hook in tuple(self.rpc_hooks):
            try:
                hook.do_after_response(addr, request, response)
            except Exception:
                pass

    # ---------- request sending ----------
    def _send(self, addr: str, cmd: RemotingCommand) -> socket.socket:
        self._apply_before_request_hooks(addr, cmd)
        return self._write(addr, cmd)
    def _write_response(self, addr: str, cmd: RemotingCommand) -> None:
        """Write a response back to the broker.

        It **must not** go through ``_send``: ``_send`` runs the RPC hooks (ACL signs
        requests), while a response message neither needs nor may carry a signature
        (the RPC hooks only run on the three caller-initiated paths
        invokeSync/invokeAsync/invokeOneway). A failed write is only logged: this
        response corresponds to a ``Broker2Client.callClient`` timeout on the broker
        side, and it must not kill the reader thread with that exception (one dead
        reader thread loses every in-flight request on the same connection).
        """
        try:
            self._write(addr, cmd)
        except Exception:
            logger.warning("remoting: failed to write response (code=%s) to %s",
                           cmd.code, addr, exc_info=True)

    def _write(self, addr: str, cmd: RemotingCommand) -> socket.socket:
        """Get-or-create the connection, write the command, return the socket used.

        The return value is for in-flight request bookkeeping
        (``_ResponseFuture.conn``): when a connection breaks, only the requests that
        were really written to that connection should be dragged down with it.
        The invoke paths must not go through here -- they claim the socket for the
        future **before** the write (see _invoke_once) -- so they call _write_on directly.
        """
        sock = self._get_or_create_conn(addr)
        self._write_on(addr, sock, cmd)
        return sock

    def _write_on(self, addr: str, sock: socket.socket, cmd: RemotingCommand) -> None:
        """Write the command on the given socket and make sure that connection's reader
        thread has been started.

        For TLS connections the reader thread is deferred to here and started only
        after the first record has been written: measurements (macOS loopback, peer is
        CPython ``ssl``, a fresh TLS connection per round) showed that when the reader
        thread enters OpenSSL right after the handshake completes, the immediately
        following first request record is lost about 3~5% of the time -- the peer's TLS
        read waits until it times out and the caller can only wait out the invoke
        timeout (0% for plaintext connections). After switching to "write the first
        record first, then start the reader thread", 270 freshly created TLS
        connections in the same scenario reproduced it 0 times. Against a real cluster
        (5.5.1 nameServer and the broker's TLS sniffing port) both orderings lost 0
        messages over 60 rounds each -- the broker's TLS stack does not expose this
        race, so it can only be guarded on a local TLS peer
        (``tests/test_tls_trace.py``); on real machines ``verify_tls_live.py`` guards
        the whole chain. Plaintext connections do not have this problem and still start
        their reader thread immediately.
        """
        data = cmd.encode()
        try:
            with self._sock_locks.get(addr, threading.Lock()):
                try:
                    sock.sendall(data)
                except OSError:
                    self.close_channel(addr)
                    raise RemotingSendRequestException(addr)
                except AttributeError:
                    raise RemotingSendRequestException(addr)
                # A completed write counts as activity on this connection: a connection that is
                # still being written to is not an idle one, however little comes back.
                self._conn_last_active[addr] = _mono_millis()
        finally:
            self._start_reader(addr)

    def invoke_sync(self, addr: str, request: RemotingCommand, timeout_millis: Optional[int] = None) -> RemotingCommand:
        """Synchronous RPC, including the GO_AWAY reconnect-and-resend path."""
        timeout = timeout_millis if timeout_millis is not None else self.invoke_timeout_millis
        started = _mono_millis()
        response = self._invoke_once(addr, request, timeout)
        if response.code != ResponseCode.GO_AWAY:
            return response
        response = self._handle_go_away(addr, request, timeout, started, response)
        return response

    def _handle_go_away(self, addr: str, request: RemotingCommand, timeout: int,
                        started: float, response: RemotingCommand) -> RemotingCommand:
        """The GO_AWAY decision point: when the switch is off it raises immediately;
        otherwise it reconnects and resends once, and a second GO_AWAY raises.

        Extracted on its own because both the synchronous and the asynchronous paths
        need exactly the same decision, and the resend count must be identical too
        (once).
        """
        if not self.enable_reconnect_for_go_away:
            raise RemotingSendRequestException(addr, "Receive GO_AWAY from channel %s" % addr)
        logger.info("remoting: receive GO_AWAY from %s, reconnect and retry once", addr)
        self.close_channel(addr)
        # The resend only spends the remaining budget: the elapsed time is subtracted from
        # the total timeoutMillis.
        spent = int(_mono_millis() - started)
        retry_timeout = max(1, timeout - spent)
        retry = _retry_request(request)
        response = self._invoke_once(addr, retry, retry_timeout)
        if response.code == ResponseCode.GO_AWAY:
            raise RemotingSendRequestException(
                addr, "Receive GO_AWAY twice in request from channel %s" % addr)
        return response

    def _invoke_once(self, addr: str, request: RemotingCommand, timeout: int) -> RemotingCommand:
        future = _ResponseFuture(request.opaque, timeout, request=request, addr=addr)
        with self._response_lock:
            self._response_table[request.opaque] = future
        try:
            # The socket is claimed BEFORE the bytes hit the wire: a plaintext
            # connection's reader thread is already running, and a peer that closes
            # right after reading the frame can land its fail-fast scan in the gap
            # between "sendall returned" and "the caller records the socket". Claiming
            # early makes that scan find (and fail) this request instead of leaving it
            # hanging until the timeout sweeper.
            self._apply_before_request_hooks(addr, request)
            future.conn = self._get_or_create_conn(addr)
            self._write_on(addr, future.conn, request)
            future.send_request_ok = True
        except Exception:
            with self._response_lock:
                self._response_table.pop(request.opaque, None)
            raise
        response = future.wait_response()
        if response is None:
            with self._response_lock:
                self._response_table.pop(request.opaque, None)
            # When the connection breaks, _fail_fast has already set the cause and woken us up:
            # report "send failure" instead of waiting out the timeout and then
            # reporting a timeout. The two paths differ in retriability (see the
            # producer's failure classification).
            if future.cause is not None:
                raise future.cause
            self._close_channel_after_sync_timeout(addr, request, timeout)
            logger.warning("remoting: synchronous invoke to %s timed out (opaque=%s, timeoutMillis=%s)",
                           addr, request.opaque, timeout)
            raise RemotingTimeoutException(addr, timeout)
        return response

    def _close_channel_after_sync_timeout(self, addr: str, request: RemotingCommand,
                                          timeout_millis: int) -> None:
        """A synchronous invoke that ran out of time also drops the connection it was sent on.

        Both floors below compare against the time the wait was handed when it began. This
        transport does not subtract the connect and write cost from the configured budget, so the
        wait always starts with the whole of it: the leftover then equals ``timeout_millis``, the
        second floor is out of reach for any positive budget, and a positive timeout therefore
        always closes. A budget of zero or less is the one shape that keeps the connection, and
        such a request was never going to be answered anyway.
        """
        if not self.enable_close_channel_on_sync_timeout:
            return
        left_millis = float(timeout_millis)
        if (left_millis <= MIN_LEFT_MILLIS_TO_CLOSE_ON_SYNC_TIMEOUT
                and left_millis <= timeout_millis / 4.0):
            return
        logger.warning("remoting: synchronous invoke to %s timed out (opaque=%s, timeoutMillis=%s), "
                       "closing the connection", addr, request.opaque, timeout_millis)
        self.close_channel(addr)

    def invoke_async(self, addr: str, request: RemotingCommand,
                     callback: Callable[[Optional[RemotingCommand], Optional[BaseException]], None],
                     timeout_millis: Optional[int] = None) -> None:
        """Asynchronous call: returns immediately. The callback fires **exactly once**:

        - a normal response arrives -> ``callback(response, None)``
        - no response after timeout_millis -> ``callback(None, RemotingTimeoutException)``
          (delivered by the timeout sweeping thread)
        - the connection breaks before the response -> ``callback(None, RemotingSendRequestException)``
          (delivered when the reader thread winds down; it does not wait for the
          timeout sweeper, because at that point no response can possibly arrive any
          more)

        invoke_timeout_millis is used when timeout_millis is None.

        GO_AWAY follows the same rule as the synchronous path: reconnect and resend
        once, and if the second attempt is GO_AWAY too the callback reports the error
        (the synchronous and asynchronous paths share this resend logic).
        """
        timeout = timeout_millis if timeout_millis is not None else self.invoke_timeout_millis
        self._invoke_async_once(addr, request, callback, timeout, _mono_millis())

    def _invoke_async_once(self, addr: str, request: RemotingCommand,
                           callback: Callable[[Optional[RemotingCommand], Optional[BaseException]], None],
                           timeout: int, started: float) -> None:
        def on_response(response, error):
            if response is None or error is not None or response.code != ResponseCode.GO_AWAY:
                callback(response, error)
                return
            # The resend needs a full round trip, and this code runs on that connection's
            # reader thread: blocking here would also hold up delivery of every other
            # in-flight request on the same connection. GO_AWAY is rare, so hand it to
            # a short-lived thread.
            threading.Thread(
                target=self._retry_after_go_away,
                args=(addr, request, callback, timeout, started, response),
                name="rmq-go-away-retry", daemon=True).start()

        future = _ResponseFuture(request.opaque, timeout, invoke_callback=on_response,
                                 request=request, addr=addr)
        self._ensure_sweeper()
        with self._response_lock:
            self._response_table[request.opaque] = future
        try:
            # Same early claim as the synchronous path: the fail-fast scan of a
            # connection that dies while the write is in flight must find this future.
            self._apply_before_request_hooks(addr, request)
            future.conn = self._get_or_create_conn(addr)
            self._write_on(addr, future.conn, request)
            future.send_request_ok = True
        except Exception:
            with self._response_lock:
                claimed = self._response_table.pop(request.opaque, None) is not None
            if claimed:
                raise
            # fail-fast or the sweeper already claimed this future and delivered the
            # outcome through the callback; raising here would report the send twice
            # (the producer's retry path would then run on top of the callback's)

    def _retry_after_go_away(self, addr: str, request: RemotingCommand,
                             callback: Callable[[Optional[RemotingCommand], Optional[BaseException]], None],
                             timeout: int, started: float,
                             response: RemotingCommand) -> None:
        try:
            callback(self._handle_go_away(addr, request, timeout, started, response), None)
        except BaseException as retry_error:  # noqa: BLE001 - callback contract: errors must also go through the error argument
            callback(None, retry_error)

    # ---------- sweep thread: expired in-flight requests and idle connections ----------
    def _ensure_sweeper(self) -> None:
        with self._sweeper_lock:
            if self._sweeper_thread is not None and self._sweeper_thread.is_alive():
                return
            self._sweeper_stop.clear()
            self._sweeper_thread = threading.Thread(
                target=self._sweep_loop, name="rmq-response-sweeper", daemon=True)
            self._sweeper_thread.start()

    def _stop_sweeper(self) -> None:
        with self._sweeper_lock:
            thread = self._sweeper_thread
            self._sweeper_thread = None
        self._sweeper_stop.set()
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)

    def _sweep_loop(self) -> None:
        while not self._sweeper_stop.wait(_SWEEP_INTERVAL_SECONDS):
            try:
                self._sweep_expired()
            except Exception as e:
                logger.warning("remoting: response sweep failed: %s" % e)
            try:
                self._sweep_idle_connections()
            except Exception as e:
                logger.warning("remoting: idle connection sweep failed: %s" % e)

    # ---------- in-flight request timeout sweeping ----------
    def _sweep_expired(self) -> None:
        # Decision rule: beginTimestamp + timeoutMillis + 1000 <= now. That 1s grace is the
        # margin left for "the response is already on its way" -- a response that
        # arrives within 1s after the timeout is still delivered as a success.
        grace_millis = 1000.0
        now = _mono_millis()
        expired = []
        with self._response_lock:
            for opaque in list(self._response_table.keys()):
                f = self._response_table.get(opaque)
                if f is None:
                    continue
                if now - f.begin_timestamp <= f.timeout_millis + grace_millis:
                    continue
                # pop decides ownership: when the reader thread pops the same opaque at the same
                # time only one side gets the non-None value
                removed = self._response_table.pop(opaque, None)
                if removed is not None:
                    expired.append((removed, opaque))
        for f, opaque in expired:
            # One line per reclaimed entry: the caller of a request that never got an answer has
            # to be able to find it in the log by address and opaque.
            logger.warning("remoting: in-flight request to %s expired without a response "
                           "(opaque=%s, timeoutMillis=%s)", f.addr, opaque, f.timeout_millis)
            error = RemotingTimeoutException(f.addr, f.timeout_millis)
            # The callback must run **outside** _response_lock: callbacks often reach
            # back into the transport or business layers, and calling them while
            # holding the lock races with shutdown for the same lock, or even
            # self-deadlocks.
            f.execute_invoke_callback(error=error)

    # ---------- idle connection sweeping ----------
    def _sweep_idle_connections(self) -> None:
        """Close every connection that has been idle strictly longer than the configured limit.

        The stamp is written by this connection's reader thread on each successful socket read and
        by the write path on each completed write, so an old stamp means the connection carried no
        traffic in either direction for that whole time. Closing runs through ``close_channel``,
        which is the same teardown a peer EOF takes: the reader thread ends, the socket is closed
        by that thread, and any request still in flight on it fails fast.
        """
        if self._closed:
            return
        limit_millis = self.channel_not_active_interval_millis
        now = _mono_millis()
        for addr, last_active in list(self._conn_last_active.items()):
            idle_millis = now - last_active
            if idle_millis <= limit_millis:
                continue
            if addr not in self._conns:
                # Already gone (EOF, a write failure, a concurrent close): nothing left to sweep.
                self._conn_last_active.pop(addr, None)
                continue
            logger.warning("remoting: connection to %s has been idle for %.1f s (limit %.1f s), "
                           "closing it", addr, idle_millis / 1000.0, limit_millis / 1000.0)
            self.close_channel(addr)

    # ---------- fail in-flight requests immediately when a connection breaks (fail-fast) ----------
    def _fail_fast(self, addr: str, sock: socket.socket) -> int:
        """The connection on ``sock`` is closed: declare every still-unanswered in-flight
        request it owns as a send failure.

        The teardown of a closed connection must be followed immediately by one
        fail-fast scan, because requests on that connection will never get a response
        any more. Without this step two things go wrong, and neither of them is merely
        "a bit slower":

        1. A synchronous caller only notices the long-dead connection after waiting
           out the full invoke timeout; an asynchronous callback is worse, it waits
           ``timeout + 1s`` (that is the sweeper thread's grace rule).
        2. What gets reported is ``RemotingTimeoutException``, whereas the correct one
           is ``RemotingSendRequestException``. The retry classification of an
           asynchronous send branches on these two types
           (``_classify_async_failure`` in ``client/producer.py``), so the wrong type
           means the wrong semantics.

        Requests are claimed by **object identity** of the socket, not by address: the
        same address with a freshly replaced connection is a common shape (resending
        after GO_AWAY, close_channel after a failed write), and the old reader thread
        must not damage the new connection while winding down. Returns the number of
        requests declared dead, which serves logging and test assertions only.
        """
        doomed = []
        with self._response_lock:
            for opaque in list(self._response_table.keys()):
                f = self._response_table.get(opaque)
                if f is None or f.conn is not sock:
                    continue
                # pop decides ownership: when the reader thread / sweeper thread races for the same
                # opaque only one side gets the non-None value
                removed = self._response_table.pop(opaque, None)
                if removed is not None:
                    doomed.append(removed)
        for f in doomed:
            # Also delivered outside _response_lock, for the same reason as in
            # _sweep_expired
            f.fail_fast()
        if doomed:
            logger.warning("remoting: connection to %s closed, %d in-flight request(s) failed fast",
                           addr, len(doomed))
        return len(doomed)

    def invoke_oneway(self, addr: str, request: RemotingCommand) -> None:
        request.mark_oneway_rpc()
        self._send(addr, request)

    def register_rpc_hook(self, hook) -> None:
        self.rpc_hooks.append(hook)

    def unregister_rpc_hook(self, hook) -> None:
        if hook in self.rpc_hooks:
            self.rpc_hooks.remove(hook)

    def update_name_server_address_list(self, addrs) -> None:
        pass

    def shutdown(self) -> None:
        self._running = False
        self._closed = True
        # Stop the sweeper thread first: it fires callbacks on other threads, so it must
        # exit before the connections are closed
        self._stop_sweeper()
        # Give TLS connections whose reader thread has not started yet (see _write)
        # a reader thread so that it can close their socket
        for addr in list(self._conn_stops):
            self._start_reader(addr)
        with self._lock:
            stops = list(self._conn_stops.values())
            threads = [t for t in self._reader_threads.values()
                       if t is not threading.current_thread()]
            self._conns.clear()
            self._conn_stops.clear()
            self._reader_threads.clear()
            self._conn_last_active.clear()
        # Each socket is closed by its own reader thread (TLS must send close_notify
        # first), so here we only wait for them to exit
        for stop in stops:
            stop.set()
        deadline = time.monotonic() + _SHUTDOWN_JOIN_SECONDS
        for t in threads:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            t.join(timeout=remaining)
