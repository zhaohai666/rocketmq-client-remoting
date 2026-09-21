# -*- coding: utf-8 -*-
"""socket 长连接客户端（对应 org.apache.rocketmq.remoting.netty.NettyRemotingClient 的核心能力）。

提供：连接管理（惰性建连 + 复用）、invokeSync/invokeAsync/invokeOneway、
opaque 映射回调分发、超时控制、连接状态探活、GO_AWAY 换连接重发。

与 Java 的一处口径差异（刻意如此）：NettyRemotingClient#scanChannelTablesOfNameServer
（channelNotActiveInterval=60s）在 Java 客户端里**从未被调度**——client + remoting 全树
grep 不到调用点，属于死代码，所以这里不做空闲连接回收；对端真断开时读线程立刻见到 EOF，
惰性清理已覆盖真实场景。
"""
from __future__ import annotations

import os
import select
import socket
import ssl
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

# 读线程等待数据的轮询间隔。连接一直安静时，也要在这一秒内察觉到"要关我了"。
_READ_POLL_SECONDS = 1.0
# TLS close_notify 的等待上限：对端不回 close_notify 也不能把关闭路径挂死。
_TLS_SHUTDOWN_TIMEOUT_SECONDS = 2.0
# shutdown() 等读线程退出的总预算（不是每个线程各等这么久）。
_SHUTDOWN_JOIN_SECONDS = 5.0


# 超时判定专用单调时钟（Java 用 System.currentTimeMillis，这里刻意取更稳的口径：
# 改系统时间不应让在途请求提前超时或永不超时）
def _mono_millis() -> float:
    return time.monotonic() * 1000.0


def _retry_request(request: RemotingCommand) -> RemotingCommand:
    """GO_AWAY 重发用的请求副本（对应 Java 的 createRequestCommand + setBody + setExtFields）。

    必须是**新** opaque：旧的那条已经在途表里摘掉了，复用会让响应错配到别的请求。
    ext_fields 按值复制——ACL 签名（AccessKey/Signature）就存在这里，重发要带原签名，
    所以副本不能从 custom_header 重新推导（推导会丢掉签名那两项）。
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
    """干净地关掉一条连接：TLS 先做握手级关闭（close_notify），再关底层套接字。

    少了 close_notify 会有两个在本机实测到的后果：

    1. 内核接收缓冲里还留着对端 TLS 1.3 的 NewSessionTicket 没被 SSL 层读走，
       ``closesocket()`` 于是发 RST 而不是 FIN。这条 RST 会打到下一条复用同一
       4 元组的新连接上——loopback 上"每轮新建 TLS 连接"实测约 25~30% 把第一个请求
       静默吞掉，调用方只能等满 invoke 超时（明文与非 TLS 路径 0%）。补上 close_notify
       后这个成因归零，但当时还残留约 3~5%，那一份是读线程起得太早，见 _write。
    2. broker 侧只看到异常断连，正常下线与真掉线分不出来。

    只对**已经不再被别的线程 recv/send** 的套接字调用：两个线程同时进 OpenSSL
    会踩坏它的内部状态（实测直接段错误），所以关闭一律由该连接的读线程来做。
    """
    if isinstance(sock, ssl.SSLSocket):
        try:
            sock.settimeout(_TLS_SHUTDOWN_TIMEOUT_SECONDS)
        except (OSError, ValueError):
            pass
        try:
            sock.unwrap()  # 发出 close_notify，并把对端那份读干净
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
    """一次在途请求（对应 Java ``ResponseFuture``）。

    回调契约（与 Java InvokeCallback 的 operationSucceed / operationFail 二分等价）：
    ``callback(response, error)`` —— 成功时 error 为 None，超时/失败时 response 为 None。
    回调**恰好触发一次**：读线程与超时清理线程抢同一个 future，由 ``execute_invoke_callback``
    里的 once 标志定胜负（Java 用 AtomicBoolean executeCallbackOnlyOnce 表达同一约束）。
    """

    def __init__(self, opaque: int, timeout_millis: int, invoke_callback=None,
                 request: Optional[RemotingCommand] = None, addr: str = ""):
        self.opaque = opaque
        self.timeout_millis = timeout_millis
        self.invoke_callback = invoke_callback
        # 保留请求命令，供 do_after_response 钩子使用（对应 Java 的 request 形参）
        self.request = request
        # 超时账目（对应 Java 的 beginTimestamp + timeoutMillis）：单调时钟，不受改系统时间影响
        self.addr = addr
        self.begin_timestamp = _mono_millis()
        self.response: Optional[RemotingCommand] = None
        self.send_request_ok = False
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
        # 与 Java 同式：严格大于，等于 deadline 那一刻还不算超时
        return _mono_millis() - self.begin_timestamp > self.timeout_millis

    def execute_invoke_callback(self, error: Optional[BaseException] = None) -> bool:
        """投递异步回调，最多一次。返回是否由本次调用真正投递。"""
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
            # 回调里抛出的异常不能带走读线程/清理线程（对应 Java 的 try-catch + warn）
            logger.warning("remoting: invoke callback raised: %s" % e)
        return True


class RemotingClient:
    def __init__(self, connect_timeout_millis: int = 3000, invoke_timeout_millis: int = 15000,
                 tls_enable: Optional[bool] = None,
                 enable_reconnect_for_go_away: bool = True):
        self.connect_timeout_millis = connect_timeout_millis
        self.invoke_timeout_millis = invoke_timeout_millis
        # 对应 Java NettyClientConfig.enableReconnectForGoAway（默认 **true**）：
        # 收到 ResponseCode.GO_AWAY(1500) 时换一条连接重发一次。
        self.enable_reconnect_for_go_away = enable_reconnect_for_go_away
        # TLS（对应 Java NettyRemotingClient 的 isUseTLS / tls.enable）。显式参数优先，
        # 否则读 ROCKETMQ_TLS_ENABLE（Java 是 JVM 系统属性 -Dtls.enable，这里等价为 env）。
        if tls_enable is None:
            tls_enable = os.environ.get("ROCKETMQ_TLS_ENABLE", "").strip().lower() in ("1", "true", "yes")
        self.tls_enable = bool(tls_enable)
        self._lock = threading.RLock()
        self._conns: Dict[str, socket.socket] = {}
        self._sock_locks: Dict[str, threading.Lock] = {}
        self._response_table: Dict[int, _ResponseFuture] = {}
        self._response_lock = threading.Lock()
        # 在途请求超时清理（对应 Java NettyRemotingAbstract.scanResponseTable 及其定时线程）。
        # 缺了它，异步请求一旦收不到响应就永久悬挂：条目泄漏在表里，回调永不触发。
        # 首次 invoke_async 才起线程，纯同步用法不留线程。
        self._sweeper_lock = threading.Lock()
        self._sweeper_stop = threading.Event()
        self._sweeper_thread: Optional[threading.Thread] = None
        self._running = True
        self._closed = False
        self.rpc_hooks = []
        # 每连接读线程
        self._reader_threads: Dict[str, threading.Thread] = {}
        # 每连接"该退了"标志：读线程靠它在不把自己阻塞死的前提下收尾并关掉套接字
        self._conn_stops: Dict[str, threading.Event] = {}
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
            stop = threading.Event()
            self._conn_stops[addr] = stop
            # 明文连接立刻起读线程；TLS 连接等第一个请求写出去再起，
            # 原因见 _write 里的说明。
            if not isinstance(sock, ssl.SSLSocket):
                self._spawn_reader(addr, sock, stop)
            return sock

    def _spawn_reader(self, addr: str, sock: socket.socket,
                      stop: threading.Event) -> None:
        """起读线程。调用方必须持有 ``self._lock``。"""
        t = threading.Thread(target=self._read_loop, args=(addr, sock, stop), daemon=True,
                             name="rmq-read-%s" % addr)
        t.start()
        self._reader_threads[addr] = t

    def _start_reader(self, addr: str) -> None:
        """补起被延后的读线程；已经起了就是空操作。

        关连接和 shutdown 也走这里：套接字一律由该连接的读线程关（见 _close_socket），
        所以「还没起读线程」的连接在被关闭前必须先把它起起来，否则没人关这条 fd。
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
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.connect_timeout_millis / 1000.0)
        try:
            sock.connect((host, int(port)))
        except OSError:
            sock.close()
            raise RemotingConnectException(addr)
        if self.tls_enable:
            # 对应 Java pipeline.addFirst(SslHandler)：TLS 包住整个流，在任何 RocketMQ
            # 帧之前完成握手。test mode（Java tls.test.mode.enable 默认 true）= 信任
            # broker 的自签证书、不校验主机名、不带客户端证书（PERMISSIVE broker 即配即通）。
            try:
                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                sock = ctx.wrap_socket(sock, server_hostname=host)
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
        """摘掉到 addr 的连接（对应 Java closeChannel 的 removeChannel + channel.close）。

        真正关套接字的是该连接的读线程：这里只置停止标志并等它退出。
        在调用方线程上直接关 SSLSocket 不安全——读线程可能正阻塞在同一条 socket 的
        recv 里，两个线程同时进 OpenSSL 会踩坏它的内部状态（实测段错误）。
        读线程还没起的 TLS 连接（见 _write）先把它补起来，否则这条 fd 没人关。
        """
        self._start_reader(addr)
        with self._lock:
            self._conns.pop(addr, None)
            stop = self._conn_stops.pop(addr, None)
            thread = self._reader_threads.pop(addr, None)
        if stop is not None:
            stop.set()
        # 写响应失败时 close_channel 可能跑在读线程自己身上（broker 主动请求的回应路径），
        # 这时不能 join 自己，交给它自己的循环收尾。
        if (thread is not None and thread.is_alive()
                and thread is not threading.current_thread()):
            thread.join(timeout=2 * _READ_POLL_SECONDS + _TLS_SHUTDOWN_TIMEOUT_SECONDS)

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
    @staticmethod
    def _wait_readable(sock: socket.socket, stop: threading.Event) -> bool:
        """等有可读数据，最多等 _READ_POLL_SECONDS；stop 置位或超时就返回 False。

        TLS 还要先看 ``pending()``：OpenSSL 已经解出来、应用还没读走的记录不会让 fd
        变可读，只看 select 就会把这些字节晾在那里——它们正是 closesocket 时发 RST
        的诱因。
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
                # select 出问题（fd 已被关掉等）交给 recv 去报，口径与不轮询时一致
                return True
            if readable:
                return True

    def _read_loop(self, addr: str, sock: socket.socket,
                   stop: Optional[threading.Event] = None) -> None:
        """一条连接的读线程。

        退出时由**本线程**关套接字：绕开读线程去关 SSLSocket，等于两个线程同时进
        OpenSSL，会踩坏它的内部状态（实测直接把整个进程搞段错误）。
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
                if self._reader_threads.get(addr) is threading.current_thread():
                    self._reader_threads.pop(addr, None)
                    self._conn_stops.pop(addr, None)
            _close_socket(sock)

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
                future.execute_invoke_callback()
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
            future.execute_invoke_callback()
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
        """把命令写到连接上，并保证这条连接的读线程已经起起来。

        TLS 连接的读线程推迟到这里、第一个记录写出去之后才起：实测（macOS loopback，
        对端是 CPython ``ssl``，每轮新建 TLS 连接）握手刚完成就读线程已经进过 OpenSSL 的
        情况下，紧跟着的第一个请求记录有约 3~5% 根本没到对端 —— 对端的 TLS 读一直等到
        超时，调用方只能等满 invoke 超时（明文连接 0%）。改成「先写第一个记录、再起读
        线程」后同一场景 270 条新建 TLS 连接 0 次复现。对真集群（5.5.1 nameServer 与
        broker 的 TLS 嗅探端口）各 60 轮两种时序都是 0 丢 —— Java 的 TLS 栈不把这个竞争
        暴露出来，所以这条只能在本地 TLS 对端上守（``tests/test_tls_trace.py``），
        真机侧由 ``verify_tls_live.py`` 守整链路。明文连接没有这个问题，仍然立刻起读线程。
        """
        sock = self._get_or_create_conn(addr)
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
        finally:
            self._start_reader(addr)

    def invoke_sync(self, addr: str, request: RemotingCommand, timeout_millis: Optional[int] = None) -> RemotingCommand:
        """同步 RPC，含 GO_AWAY 换连接重发（对应 Java NettyRemotingClient#invokeImpl:828-873）。"""
        timeout = timeout_millis if timeout_millis is not None else self.invoke_timeout_millis
        started = _mono_millis()
        response = self._invoke_once(addr, request, timeout)
        if response.code != ResponseCode.GO_AWAY:
            return response
        response = self._handle_go_away(addr, request, timeout, started, response)
        return response

    def _handle_go_away(self, addr: str, request: RemotingCommand, timeout: int,
                        started: float, response: RemotingCommand) -> RemotingCommand:
        """GO_AWAY 的收口：开关关掉直接报错；否则换连接重发一次，第二次还是 GO_AWAY 就抛。

        单独抽出来是因为同步与异步两条路都要同一套判定，重发次数也必须一样（一次）。
        """
        if not self.enable_reconnect_for_go_away:
            raise RemotingSendRequestException(addr, "Receive GO_AWAY from channel %s" % addr)
        logger.info("remoting: receive GO_AWAY from %s, reconnect and retry once", addr)
        self.close_channel(addr)
        # 重发只花剩余预算：Java 用同一个 Stopwatch 的 elapsed 扣减 timeoutMillis。
        spent = int(_mono_millis() - started)
        retry_timeout = max(1, timeout - spent)
        retry = _retry_request(request)
        response = self._invoke_once(addr, retry, retry_timeout)
        if response.code == ResponseCode.GO_AWAY:
            raise RemotingSendRequestException(
                addr, "Receive GO_AWAY twice in request from channel %s" % addr)
        return response

    def _invoke_once(self, addr: str, request: RemotingCommand, timeout: int) -> RemotingCommand:
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

    def invoke_async(self, addr: str, request: RemotingCommand,
                     callback: Callable[[Optional[RemotingCommand], Optional[BaseException]], None],
                     timeout_millis: Optional[int] = None) -> None:
        """异步调用：立即返回。回调**恰好触发一次**（对应 Java invokeAsyncImpl）：

        - 正常收到响应 -> ``callback(response, None)``
        - 超过 timeout_millis 仍无响应 -> ``callback(None, RemotingTimeoutException)``
          （由超时清理线程投递，等价于 Java scanResponseTable 里的 operationFail）

        timeout_millis 为 None 时使用 invoke_timeout_millis。

        GO_AWAY 与同步路径同口径：换连接重发一次，第二次仍是 GO_AWAY 则回调报错
        （Java 的这段逻辑在 ``invokeImpl`` 里，同步与异步共用）。
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
            # 重发要等一次完整的往返，而这里跑在该连接的读线程上：就地阻塞会把
            # 同连接其它在途请求的投递一起拖住。GO_AWAY 是稀有事件，交给短命线程做。
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
            self._send(addr, request)
            future.send_request_ok = True
        except Exception:
            with self._response_lock:
                self._response_table.pop(request.opaque, None)
            raise

    def _retry_after_go_away(self, addr: str, request: RemotingCommand,
                             callback: Callable[[Optional[RemotingCommand], Optional[BaseException]], None],
                             timeout: int, started: float,
                             response: RemotingCommand) -> None:
        try:
            callback(self._handle_go_away(addr, request, timeout, started, response), None)
        except BaseException as retry_error:  # noqa: BLE001 - 回调契约：错误也只能走 error 参数
            callback(None, retry_error)

    # ---------- 在途请求超时清理（对应 Java scanResponseTable） ----------
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
        while not self._sweeper_stop.wait(0.1):
            try:
                self._sweep_expired()
            except Exception as e:
                logger.warning("remoting: response sweep failed: %s" % e)

    def _sweep_expired(self) -> None:
        # 与 Java 同式：beginTimestamp + timeoutMillis + 1000 <= now。这 1s 宽限是给
        # "响应已经在路上"留的余量——超时后 1s 内到达的响应仍按成功投递。
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
                # pop 决定归属：与读线程同时摘同一个 opaque 时只有一方拿到非空值
                removed = self._response_table.pop(opaque, None)
                if removed is not None:
                    expired.append((removed, opaque))
        for f, opaque in expired:
            error = RemotingTimeoutException(f.addr, f.timeout_millis)
            # 回调必须在 _response_lock **之外**执行：回调里常常还要回到传输层或业务层，
            # 持锁回调会和 shutdown 抢同一把锁，甚至自锁。
            f.execute_invoke_callback(error=error)

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
        # 先停清理线程：它在别的线程上触发回调，必须早于关连接退出
        self._stop_sweeper()
        # 读线程还没起的 TLS 连接（见 _write）补一个读线程来关它的套接字
        for addr in list(self._conn_stops):
            self._start_reader(addr)
        with self._lock:
            stops = list(self._conn_stops.values())
            threads = [t for t in self._reader_threads.values()
                       if t is not threading.current_thread()]
            self._conns.clear()
            self._conn_stops.clear()
            self._reader_threads.clear()
        # 套接字由各连接自己的读线程关（TLS 要先送 close_notify），这里只等它们退完
        for stop in stops:
            stop.set()
        deadline = time.monotonic() + _SHUTDOWN_JOIN_SECONDS
        for t in threads:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            t.join(timeout=remaining)