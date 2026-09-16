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
from .protocol.codes import ResponseCode
from .protocol.remoting_command import RemotingCommand
from ..logging import get_logger

logger = get_logger()

MAX_FRAME_LENGTH = 16 * 1024 * 1024


class _ResponseFuture:
    def __init__(self, opaque: int, timeout_millis: int, invoke_callback=None,
                 request: Optional[RemotingCommand] = None):
        self.opaque = opaque
        self.timeout_millis = timeout_millis
        self.invoke_callback = invoke_callback
        # 保留请求命令，供 do_after_response 钩子使用（对应 Java 的 request 形参）
        self.request = request
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
        # broker 主动请求处理器：request_code -> handler(cmd, addr) -> None
        self._processors: Dict[int, Callable] = {}

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
                    self._dispatch(frame, addr)
        finally:
            with self._lock:
                if self._conns.get(addr) is sock:
                    self._conns.pop(addr, None)
            try:
                sock.close()
            except OSError:
                pass

    def _dispatch(self, frame: bytes, addr: str) -> None:
        try:
            cmd = RemotingCommand.decode(frame)
        except Exception:
            return
        if cmd.is_response_type():
            future = None
            with self._response_lock:
                future = self._response_table.pop(cmd.opaque, None)
            if future is not None:
                future.put_response(cmd)
                self._apply_after_response_hooks(addr, future.request, cmd)
                if future.invoke_callback is not None:
                    try:
                        future.invoke_callback(cmd)
                    except Exception:
                        pass
            return
        # 非响应命令：broker 主动发起的请求（如 CHECK_TRANSACTION_STATE=39）。
        # 这类请求的 opaque 由 broker 生成，不会出现在本地在途表里，按主动请求处理。
        future = None
        with self._response_lock:
            future = self._response_table.pop(cmd.opaque, None)
        if future is not None:
            # 异常兜底：本应是对端响应却没带响应标志
            future.put_response(cmd)
            self._apply_after_response_hooks(addr, future.request, cmd)
            if future.invoke_callback is not None:
                try:
                    future.invoke_callback(cmd)
                except Exception:
                    pass
            return
        handler = self._processors.get(cmd.code)
        if handler is not None:
            # 处理器**可以**返回一个响应命令（对应 Java NettyRequestProcessor#processRequest
            # 的返回值）。需要返回的典型是 PUSH_REPLY_MESSAGE_TO_CLIENT(326)：
            # broker 用 invokeSync 推应答，客户端不回响应它那边就会等到超时。
            # 事务回查 39 是 oneway（Java 里 broker 用 invokeOneway），处理器返回 None 即可。
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

    def register_processor(self, request_code: int,
                           handler: Callable[["RemotingCommand", str], Optional[RemotingCommand]]) -> None:
        """注册 broker 主动请求处理器（对应 Java NettyRemotingServer 的 processor 表）。

        handler 签名 ``handler(cmd, addr) -> Optional[RemotingCommand]``：``cmd`` 是解码后的
        RemotingCommand（含 ext_fields / body），``addr`` 是对端（broker）地址；
        **返回非 None 就会把该命令作为响应写回**（opaque 由框架填成请求的 opaque），
        返回 None 表示不需要响应（oneway 请求，如事务回查 CHECK_TRANSACTION_STATE(39)）。

        注意：仅当命令是「请求类型」且不在本地在途响应表里时才派发到这里，
        不会破坏现有 invokeSync/invokeAsync 的响应分发。
        """
        self._processors[request_code] = handler

    def unregister_processor(self, request_code: int) -> None:
        self._processors.pop(request_code, None)

    # ---------- RPC 钩子 ----------
    def _apply_before_request_hooks(self, addr: str, cmd: RemotingCommand) -> None:
        """发送前依次执行 RPC 钩子（对应 Java NettyRemotingAbstract#doBeforeRpcHooks）。

        **必须在 cmd.encode() 之前调用**：ACL 钩子要把 AccessKey/Signature 写进
        ext_fields，而 Signature 覆盖的是 makeCustomHeaderToNet() 之后的完整
        ext_fields + body —— 即真正会上线的那份内容。encode() 之后再注入就晚了。
        """
        for hook in tuple(self.rpc_hooks):
            hook.do_before_request(addr, cmd)

    def _apply_after_response_hooks(self, addr: str, request: Optional[RemotingCommand],
                                    response: Optional[RemotingCommand]) -> None:
        """收到响应后依次执行 RPC 钩子（对应 Java NettyRemotingAbstract#doAfterRpcHooks）。

        无钩子时零开销；request 缺失（理论上不会）则跳过，不影响响应分发。
        """
        if not self.rpc_hooks or request is None:
            return
        for hook in tuple(self.rpc_hooks):
            try:
                hook.do_after_response(addr, request, response)
            except Exception:
                pass

    # ---------- 请求发送 ----------
    def _send(self, addr: str, cmd: RemotingCommand) -> None:
        self._apply_before_request_hooks(addr, cmd)
        self._write(addr, cmd)

    def _write_response(self, addr: str, cmd: RemotingCommand) -> None:
        """把响应写回 broker。

        **不能**走 ``_send``：``_send`` 会执行 RPC 钩子（ACL 会给请求加签），
        而响应报文不需要也不能带签名（对齐 Java：``doBeforeRpcHooks`` 只在
        invokeSync/invokeAsync/invokeOneway 三条主动发起路径上调用）。
        写失败只记日志：这条响应对应 broker 侧的 ``Broker2Client.callClient`` 超时，
        不该让读线程因为这个异常退出（读线程一死，同连接上所有在途请求全丢）。
        """
        try:
            self._write(addr, cmd)
        except Exception:
            logger.warning("remoting: failed to write response (code=%s) to %s",
                           cmd.code, addr, exc_info=True)

    def _write(self, addr: str, cmd: RemotingCommand) -> None:
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
        future = _ResponseFuture(request.opaque, timeout, request=request)
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
        future = _ResponseFuture(request.opaque, timeout, invoke_callback=callback,
                                 request=request)
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