# -*- coding: utf-8 -*-
"""socket 长连接客户端（对应 org.apache.rocketmq.remoting.netty.NettyRemotingClient 的核心能力）。

提供：连接管理（惰性建连 + 复用）、invokeSync/invokeAsync/invokeOneway、
opaque 映射回调分发、超时控制、连接状态探活。
"""
from __future__ import annotations

import socket
import struct
import threading
import time
from typing import Callable, Dict, Optional

from .exception import (RemotingConnectException, RemotingSendRequestException,
                        RemotingTimeoutException)
from .protocol.remoting_command import RemotingCommand

MAX_FRAME_LENGTH = 16 * 1024 * 1024


class _ResponseFuture:
    def __init__(self, opaque: int, timeout_millis: int, invoke_callback=None):
        self.opaque = opaque
        self.timeout_millis = timeout_millis
        self.invoke_callback = invoke_callback
        self.response: Optional[RemotingCommand] = None
        self.send_request_ok = False
        self._done = threading.Event()
        self._lock = threading.Lock()
        self._timer = None

    def put_response(self, cmd: RemotingCommand) -> None:
        with self._lock:
            self.response = cmd
        self._done.set()

    def wait_response(self) -> RemotingCommand:
        self._done.wait(timeout=self.timeout_millis / 1000.0)
        return self.response

    def is_timeout(self) -> bool:
        return not self._done.is_set()


class RemotingClient:
    def __init__(self, connect_timeout_millis: int = 3000, invoke_timeout_millis: int = 15000):
        self.connect_timeout_millis = connect_timeout_millis
        self.invoke_timeout_millis = invoke_timeout_millis
        self._lock = threading.RLock()
        self._conns: Dict[str, socket.socket] = {}
        self._sock_locks: Dict[str, threading.Lock] = {}
        self._response_table: Dict[int, _ResponseFuture] = {}
        self._response_lock = threading.Lock()
        self._running = True
        self._closed = False
        self.rpc_hooks = []
        # 每连接读线程
        self._reader_threads: Dict[str, threading.Thread] = {}

    # ---------- 连接管理 ----------
    def _get_or_create_conn(self, addr: str) -> socket.socket:
        with self._lock:
            conn = self._conns.get(addr)
            if conn is not None and self._is_alive(conn):
                return conn
            sock = self._create_conn(addr)
            self._conns[addr] = sock
            self._sock_locks[addr] = threading.Lock()
            t = threading.Thread(target=self._read_loop, args=(addr, sock), daemon=True,
                                 name="rmq-read-%s" % addr)
            t.start()
            self._reader_threads[addr] = t
            return sock

    def _create_conn(self, addr: str) -> socket.socket:
        host, port = self._parse_addr(addr)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.connect_timeout_millis / 1000.0)
        try:
            sock.connect((host, int(port)))
        except OSError:
            sock.close()
            raise RemotingConnectException(addr)
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
            import select
            r, _, _ = select.select([sock], [], [], 0)
            if r:
                # 有可读数据可能是响应也可能是对端关闭；尝试 peername 判断
                try:
                    sock.getpeername()
                    return True
                except OSError:
                    return False
            return True
        except OSError:
            return False

    def close_channel(self, addr: str) -> None:
        with self._lock:
            sock = self._conns.pop(addr, None)
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass

    def is_channel_writable(self, addr: str) -> bool:
        conn = self._conns.get(addr)
        if conn is None:
            return False
        try:
            conn.getpeername()
            return True
        except OSError:
            return False

    # ---------- 读循环 ----------
    def _read_loop(self, addr: str, sock: socket.socket) -> None:
        buf = bytearray()
        try:
            while self._running:
                try:
                    chunk = sock.recv(65536)
                except socket.timeout:
                    continue
                except OSError:
                    break
                if not chunk:
                    break
                buf += chunk
                while True:
                    if len(buf) < 4:
                        break
                    (total_len,) = struct.unpack_from(">i", bytes(buf), 0)
                    if total_len <= 0 or total_len > MAX_FRAME_LENGTH:
                        buf.clear()
                        break
                    if len(buf) < 4 + total_len:
                        break
                    frame = bytes(buf[:4 + total_len])
                    del buf[:4 + total_len]
                    self._dispatch(frame)
        finally:
            with self._lock:
                if self._conns.get(addr) is sock:
                    self._conns.pop(addr, None)
            try:
                sock.close()
            except OSError:
                pass

    def _dispatch(self, frame: bytes) -> None:
        try:
            cmd = RemotingCommand.decode(frame)
        except Exception:
            return
        future = None
        with self._response_lock:
            future = self._response_table.pop(cmd.opaque, None)
        if future is not None:
            future.put_response(cmd)
            if future.invoke_callback is not None:
                try:
                    future.invoke_callback(cmd)
                except Exception:
                    pass

    # ---------- 请求发送 ----------
    def _send(self, addr: str, cmd: RemotingCommand) -> None:
        sock = self._get_or_create_conn(addr)
        data = cmd.encode()
        with self._sock_locks.get(addr, threading.Lock()):
            try:
                sock.sendall(data)
                return
            except OSError:
                self.close_channel(addr)
                raise RemotingSendRequestException(addr)
            except AttributeError:
                raise RemotingSendRequestException(addr)

    def invoke_sync(self, addr: str, request: RemotingCommand, timeout_millis: Optional[int] = None) -> RemotingCommand:
        timeout = timeout_millis if timeout_millis is not None else self.invoke_timeout_millis
        future = _ResponseFuture(request.opaque, timeout)
        with self._response_lock:
            self._response_table[request.opaque] = future
        try:
            self._send(addr, request)
            future.send_request_ok = True
        except Exception:
            with self._response_lock:
                self._response_table.pop(request.opaque, None)
            raise
        response = future.wait_response()
        if response is None:
            with self._response_lock:
                self._response_table.pop(request.opaque, None)
            raise RemotingTimeoutException(addr, timeout)
        return response

    def invoke_async(self, addr: str, request: RemotingCommand, callback: Callable[[RemotingCommand], None],
                     timeout_millis: Optional[int] = None) -> None:
        timeout = timeout_millis if timeout_millis is not None else self.invoke_timeout_millis
        future = _ResponseFuture(request.opaque, timeout, invoke_callback=callback)
        with self._response_lock:
            self._response_table[request.opaque] = future
        try:
            self._send(addr, request)
            future.send_request_ok = True
        except Exception:
            with self._response_lock:
                self._response_table.pop(request.opaque, None)
            raise

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
        with self._lock:
            conns = list(self._conns.values())
            self._conns.clear()
        for sock in conns:
            try:
                sock.close()
            except OSError:
                pass