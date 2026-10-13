# -*- coding: utf-8 -*-
"""SOCKS 代理传输层离线单测。

进程内起一个假 SOCKS 代理（socketserver 线程，最小握手 + CONNECT + 字节中继）
和一个明文 RocketMQ 帧回显服务端，验证：

- RemotingClient 配置 SocksProxyConfig 后经代理建连，请求正常往返（SOCKS5 无认证）；
- SOCKS5 用户名密码子协商：凭据正确通过、凭据错误被代理解拒、代理要认证但客户端
  没带凭据三种形状；
- 代理对 CONNECT 回失败应答（rep != 0）时客户端报 RemotingConnectException；
- SOCKS4（含 4a 域名目标）握手；
- enable 语义：enable=False 时留着配置但不走代理（直连可达）；
- SocksProxyConfig 自身的字段解析（host:port 合写 / enable 缺省 / 非法类型）。

关于本机 loopback 怪癖（Windows：刚关闭的端口约 2s 内拒绝连接）：涉及连接失败的
断言全部走**协议层拒绝**（代理先收下 TCP 再在 SOCKS 层拒绝），不依赖任何"连不上的
端口"；同时这类用例的建连超时给到 35s 预算，即使碰上瞬时怪癖也不会误判。
"""
import select
import socket
import struct
import threading
import socketserver

import pytest

from remoting.client import RemotingClient
from remoting.exception import RemotingConnectException
from remoting.protocol import remoting_command as rc_mod
from remoting.protocol import headers as headers_mod
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.socks_proxy import (SOCKS4, SOCKS5, SocksProxyConfig, SocksProxyError)

# 建连预算：失败类断言也不许因为本机 loopback 的瞬时怪癖而假失败（预算 ≥30s）
_CONNECT_TIMEOUT_MILLIS = 35000


def _route_request():
    return rc_mod.RemotingCommand.create_request_command(
        RequestCode.GET_ROUTEINFO_BY_TOPIC,
        (lambda h: (setattr(h, "topic", "t"), h)[1])(headers_mod.GetRouteInfoRequestHeader()))


class FrameServer:
    """明文 RocketMQ 帧回显服务端：收请求帧 → 回 SUCCESS 响应（同 opaque）。"""

    def __init__(self):
        self._listen = socket.socket()
        self._listen.bind(("127.0.0.1", 0))
        self._listen.listen(8)
        self.port = self._listen.getsockname()[1]
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self):
        try:
            while True:
                conn, _ = self._listen.accept()
                threading.Thread(target=self._serve, args=(conn,), daemon=True).start()
        except OSError:
            pass  # stop() 关掉监听套接字

    def _serve(self, conn):
        try:
            while True:
                head = self._recv_exact(conn, 4)
                if head is None:
                    return
                (total,) = struct.unpack(">i", head)
                frame = self._recv_exact(conn, total)
                if frame is None:
                    return
                cmd = rc_mod.RemotingCommand.decode(head + frame)
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

    @staticmethod
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

    def stop(self):
        try:
            self._listen.close()
        except OSError:
            pass


class _SocksHandler(socketserver.BaseRequestHandler):
    """单连接处理器：最小 SOCKS 握手 + CONNECT + 双向字节中继。"""

    def handle(self):
        proxy = self.server
        try:
            if proxy.proxy_type == SOCKS4:
                self._handle_socks4()
            else:
                self._handle_socks5()
        except OSError:
            pass
        finally:
            try:
                self.request.close()
            except OSError:
                pass

    # ---------------- SOCKS5 ----------------
    def _handle_socks5(self):
        proxy = self.server
        head = self._recv_exact(2)
        if head is None or head[0] != 0x05:
            return
        nmethods = head[1]
        methods = self._recv_exact(nmethods) if nmethods else b""
        proxy.greetings.append(list(methods))
        if proxy.username is not None:
            # 代理要求认证：客户端声明了 0x02 就走子协商，否则回 0xFF（不可接受）
            if 0x02 not in methods:
                self.request.sendall(b"\x05\xff")
                return
            self.request.sendall(b"\x05\x02")
            if not self._username_password_auth():
                return
        else:
            if 0x00 not in methods:
                self.request.sendall(b"\x05\xff")
                return
            self.request.sendall(b"\x05\x00")
        # CONNECT
        req = self._recv_exact(4)
        if req is None or req[0] != 0x05 or req[1] != 0x01:
            return
        atyp = req[3]
        if atyp == 0x01:
            raw = self._recv_exact(4)
            host = socket.inet_ntoa(raw)
        elif atyp == 0x03:
            n = self._recv_exact(1)[0]
            host = self._recv_exact(n).decode("utf-8")
        elif atyp == 0x04:
            raw = self._recv_exact(16)
            host = socket.inet_ntop(socket.AF_INET6, raw)
        else:
            return
        (port,) = struct.unpack(">H", self._recv_exact(2))
        proxy.connect_requests.append((host, port))
        if proxy.connect_reply != 0x00:
            self._reply5(proxy.connect_reply)
            return
        self._relay(host, port, lambda: self._reply5(0x00))

    def _username_password_auth(self):
        proxy = self.server
        ver = self._recv_exact(1)[0]
        ulen = self._recv_exact(1)[0]
        uname = self._recv_exact(ulen).decode("utf-8")
        plen = self._recv_exact(1)[0]
        passwd = self._recv_exact(plen).decode("utf-8")
        ok = (uname == proxy.username and passwd == proxy.password)
        proxy.auth_results.append(ok)
        self.request.sendall(b"\x01\x00" if ok else b"\x01\x01")
        return ok

    def _reply5(self, rep):
        self.request.sendall(b"\x05" + bytes((rep,)) + b"\x00\x01"
                             + socket.inet_aton("0.0.0.0") + struct.pack(">H", 0))

    # ---------------- SOCKS4 / 4a ----------------
    def _handle_socks4(self):
        proxy = self.server
        head = self._recv_exact(8)
        if head is None or head[0] != 0x04:
            return
        (port,) = struct.unpack(">H", head[2:4])
        ip = head[4:8]
        # userid 到 NUL 为止
        user = bytearray()
        while True:
            c = self._recv_exact(1)
            if c == b"\x00":
                break
            user += c
        host = socket.inet_ntoa(ip)
        if ip[:3] == b"\x00\x00\x00" and ip[3] != 0:
            # SOCKS4a：域名跟在 userid 的 NUL 之后
            domain = bytearray()
            while True:
                c = self._recv_exact(1)
                if c == b"\x00":
                    break
                domain += c
            host = domain.decode("utf-8")
        proxy.connect_requests.append((host, port, bytes(user)))
        if proxy.connect_reply != 0x00:
            self.request.sendall(b"\x00" + bytes((proxy.connect_reply,))
                                 + struct.pack(">H", 0) + b"\x00\x00\x00\x00")
            return

        def grant():
            self.request.sendall(b"\x00\x5a" + struct.pack(">H", 0) + b"\x00\x00\x00\x00")

        self._relay(host, port, grant)

    # ---------------- 公共 ----------------
    def _relay(self, host, port, send_grant):
        """CONNECT 成功后在客户端与真实目标之间双向搬字节。"""
        try:
            target = socket.create_connection((host, port), timeout=5)
        except OSError:
            # 连不上目标：SOCKS5 回 host unreachable，SOCKS4 回 0x5B
            rep = 0x04 if self.server.proxy_type != SOCKS4 else 0x5B
            try:
                if self.server.proxy_type == SOCKS4:
                    self.request.sendall(b"\x00" + bytes((rep,))
                                         + struct.pack(">H", 0) + b"\x00\x00\x00\x00")
                else:
                    self._reply5(rep)
            except OSError:
                pass
            return
        send_grant()
        self._pump(self.request, target)

    @staticmethod
    def _pump(a, b):
        try:
            while True:
                readable = select.select([a, b], [], [], 1.0)[0]
                if not readable:
                    continue
                for s in readable:
                    data = s.recv(65536)
                    if not data:
                        return
                    (b if s is a else a).sendall(data)
        except OSError:
            pass
        finally:
            try:
                b.close()
            except OSError:
                pass

    def _recv_exact(self, n):
        buf = b""
        while len(buf) < n:
            try:
                chunk = self.request.recv(n - len(buf))
            except OSError:
                return None
            if not chunk:
                return None
            buf += chunk
        return buf


class FakeSocksProxy(socketserver.ThreadingTCPServer):
    """假 SOCKS 代理：只实现握手与 CONNECT 转发，足够单测用。

    ``username``/``password`` 非 None 表示代理要求用户名密码认证（SOCKS5）；
    ``connect_reply`` 非 0x00 时对 CONNECT 回该失败码（SOCKS5 rep / SOCKS4 CD）。
    """

    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, proxy_type=SOCKS5, username=None, password=None,
                 connect_reply=0x00):
        self.proxy_type = proxy_type
        self.username = username
        self.password = password
        self.connect_reply = connect_reply
        # 观测点：客户端发了什么、代理怎么答的
        self.greetings = []
        self.auth_results = []       # True/False，走一次子协商记一条
        self.connect_requests = []   # (host, port[, userid])
        socketserver.ThreadingTCPServer.__init__(
            self, ("127.0.0.1", 0), _SocksHandler)
        self.port = self.server_address[1]
        self._thread = threading.Thread(target=self.serve_forever, daemon=True)
        self._thread.start()

    def stop(self):
        self.shutdown()
        self.server_close()
        self._thread.join(timeout=5)


# ---------------------------------------------------------------- SOCKS5 无认证

def test_invoke_sync_over_socks5_no_auth():
    target = FrameServer()
    proxy = FakeSocksProxy()
    try:
        client = RemotingClient(
            socks_proxy_config=SocksProxyConfig(addr="127.0.0.1", port=proxy.port))
        try:
            req = _route_request()
            resp = client.invoke_sync("127.0.0.1:%d" % target.port, req, 10000)
            assert resp.code == ResponseCode.SUCCESS
            assert resp.opaque == req.opaque
        finally:
            client.shutdown()
        # 代理确实被用上了：一次无认证握手，CONNECT 的目标就是帧服务端
        assert proxy.greetings == [[0x00]]
        assert proxy.auth_results == []
        assert proxy.connect_requests == [("127.0.0.1", target.port)]
    finally:
        proxy.stop()
        target.stop()


def test_socks5_domain_target_reaches_proxy_unresolved():
    pass
    proxy = FakeSocksProxy()
    try:
        client = RemotingClient(
            socks_proxy_config=SocksProxyConfig(addr="127.0.0.1", port=proxy.port))
        try:
            with pytest.raises(RemotingConnectException):
                client.invoke_sync("no-such-host.invalid:9876", _route_request(), 10000)
        finally:
            client.shutdown()
        assert len(proxy.connect_requests) == 1
        host, port = proxy.connect_requests[0]
        assert host == "no-such-host.invalid" and port == 9876
    finally:
        proxy.stop()


# ---------------------------------------------------------------- SOCKS5 认证

def test_socks5_username_password_auth_ok():
    target = FrameServer()
    proxy = FakeSocksProxy(username="rmq", password="secret")
    try:
        cfg = SocksProxyConfig(addr="127.0.0.1", port=proxy.port,
                               username="rmq", password="secret")
        client = RemotingClient(socks_proxy_config=cfg)
        try:
            resp = client.invoke_sync("127.0.0.1:%d" % target.port,
                                      _route_request(), 10000)
            assert resp.code == ResponseCode.SUCCESS
        finally:
            client.shutdown()
        assert proxy.auth_results == [True]
        assert proxy.connect_requests == [("127.0.0.1", target.port)]
    finally:
        proxy.stop()
        target.stop()


def test_socks5_username_password_auth_rejected():
    """凭据错误：代理在子协商里拒绝，客户端收口成 RemotingConnectException。"""
    proxy = FakeSocksProxy(username="rmq", password="secret")
    try:
        cfg = SocksProxyConfig(addr="127.0.0.1", port=proxy.port,
                               username="rmq", password="WRONG")
        client = RemotingClient(socks_proxy_config=cfg,
                                connect_timeout_millis=_CONNECT_TIMEOUT_MILLIS)
        try:
            with pytest.raises(RemotingConnectException) as ei:
                client.invoke_sync("127.0.0.1:9876", _route_request(), 10000)
            assert "auth rejected" in str(ei.value)
        finally:
            client.shutdown()
        assert proxy.auth_results == [False]
        assert proxy.connect_requests == []   # 认证没过，CONNECT 根本没发
    finally:
        proxy.stop()


def test_socks5_client_without_credentials_when_required():
    """代理要认证但客户端没配凭据：方法协商即被拒（0xFF），同样收口成连接异常。"""
    proxy = FakeSocksProxy(username="rmq", password="secret")
    try:
        client = RemotingClient(
            socks_proxy_config=SocksProxyConfig(addr="127.0.0.1", port=proxy.port),
            connect_timeout_millis=_CONNECT_TIMEOUT_MILLIS)
        try:
            with pytest.raises(RemotingConnectException):
                client.invoke_sync("127.0.0.1:9876", _route_request(), 10000)
        finally:
            client.shutdown()
        assert proxy.auth_results == []
    finally:
        proxy.stop()


# ---------------------------------------------------------------- CONNECT 拒绝

def test_socks5_connect_rejected_by_proxy():
    """代理对 CONNECT 回失败应答（rep=0x05 connection refused）：
    客户端必须立刻拿到 RemotingConnectException（协议层拒绝，不走本机 loopback
    的 connect 拒绝路径，但仍给 35s 建连预算防瞬时怪癖）。"""
    proxy = FakeSocksProxy(connect_reply=0x05)
    try:
        client = RemotingClient(
            socks_proxy_config=SocksProxyConfig(addr="127.0.0.1", port=proxy.port),
            connect_timeout_millis=_CONNECT_TIMEOUT_MILLIS)
        try:
            with pytest.raises(RemotingConnectException) as ei:
                client.invoke_sync("127.0.0.1:9876", _route_request(), 10000)
            assert "socks proxy" in str(ei.value)
        finally:
            client.shutdown()
        assert proxy.connect_requests == [("127.0.0.1", 9876)]
    finally:
        proxy.stop()


# ---------------------------------------------------------------- SOCKS4

def test_invoke_sync_over_socks4():
    target = FrameServer()
    proxy = FakeSocksProxy(proxy_type=SOCKS4)
    try:
        cfg = SocksProxyConfig(addr="127.0.0.1", port=proxy.port, proxy_type=SOCKS4,
                               username="rmq-user")
        client = RemotingClient(socks_proxy_config=cfg)
        try:
            resp = client.invoke_sync("127.0.0.1:%d" % target.port,
                                      _route_request(), 10000)
            assert resp.code == ResponseCode.SUCCESS
        finally:
            client.shutdown()
        assert len(proxy.connect_requests) == 1
        host, port, user = proxy.connect_requests[0]
        assert (host, port, user) == ("127.0.0.1", target.port, b"rmq-user")
    finally:
        proxy.stop()
        target.stop()


def test_socks4_rejected_by_proxy():
    proxy = FakeSocksProxy(proxy_type=SOCKS4, connect_reply=0x5D)
    try:
        cfg = SocksProxyConfig(addr="127.0.0.1", port=proxy.port, proxy_type=SOCKS4)
        client = RemotingClient(socks_proxy_config=cfg,
                                connect_timeout_millis=_CONNECT_TIMEOUT_MILLIS)
        try:
            with pytest.raises(RemotingConnectException):
                client.invoke_sync("127.0.0.1:9876", _route_request(), 10000)
        finally:
            client.shutdown()
        assert proxy.connect_requests == [("127.0.0.1", 9876, b"")]
    finally:
        proxy.stop()


# ---------------------------------------------------------------- enable 语义

def test_socks_proxy_disabled_falls_back_to_direct():
    """enable=False：配置留着但不走代理，直连照常工作，代理零接触。"""
    target = FrameServer()
    proxy = FakeSocksProxy()
    try:
        cfg = SocksProxyConfig(addr="127.0.0.1", port=proxy.port, enable=False)
        client = RemotingClient(socks_proxy_config=cfg)
        try:
            resp = client.invoke_sync("127.0.0.1:%d" % target.port,
                                      _route_request(), 10000)
            assert resp.code == ResponseCode.SUCCESS
        finally:
            client.shutdown()
        assert proxy.greetings == [] and proxy.connect_requests == []
    finally:
        proxy.stop()
        target.stop()


# ---------------------------------------------------------------- 配置载体

def test_socks_proxy_config_parsing_and_semantics():
    cfg = SocksProxyConfig("127.0.0.1:1080")
    assert cfg.addr == "127.0.0.1" and cfg.port == 1080
    assert cfg.proxy_type == SOCKS5
    assert cfg.is_enabled() is True            # enable 缺省 = addr 非空即开
    # 显式 enable 优先
    assert SocksProxyConfig("127.0.0.1:1080", enable=False).is_enabled() is False
    assert SocksProxyConfig("", enable=True).is_enabled() is True
    assert SocksProxyConfig("").is_enabled() is False
    # 显式 port 覆盖 addr 里带的端口
    assert SocksProxyConfig("127.0.0.1:1080", port=2099).port == 2099
    # IPv6 合写
    v6 = SocksProxyConfig("[::1]:1080")
    assert v6.addr == "::1" and v6.port == 1080
    # 非法代理类型当场抛，不等建连
    with pytest.raises(SocksProxyError):
        SocksProxyConfig("127.0.0.1", proxy_type="HTTP")
    # repr 不泄漏密码明文
    r = repr(SocksProxyConfig("127.0.0.1", 1080, username="u", password="p"))
    assert "password=****" in r and "password=p" not in r


def test_socks_proxy_config_flows_to_remoting_client():
    """门面 setter → MQClientInstance → RemotingClient 的传递链不丢字段。"""
    from client.mq_client import MQClientInstance
    cfg = SocksProxyConfig("127.0.0.1", 1080, SOCKS5, "u", "secret", enable=True)
    instance = MQClientInstance("socks-plumb-cid", ["127.0.0.1:9876"],
                                socks_proxy_config=cfg)
    try:
        assert instance.socks_proxy_config is cfg
        assert instance.remoting_client.socks_proxy_config is cfg
    finally:
        instance.remoting_client.shutdown()
        MQClientInstance.INSTANCE_MAP.pop("socks-plumb-cid", None)

    facades = DefaultMQProducerLike()
    facades.set_socks_proxy_config(cfg)
    for obj in facades.objs:
        assert obj.get_socks_proxy_config() is cfg


class DefaultMQProducerLike:
    """门面 setter 冒烟：五个门面同名字段的存取（不起后台线程）。"""

    def __init__(self):
        from client.producer import DefaultMQProducer
        from client.consumer import (DefaultMQPushConsumer, DefaultMQPullConsumer,
                                     DefaultLitePullConsumer)
        from client.admin import DefaultMQAdminExt
        self.objs = [DefaultMQProducer("PID_SOCKS_PLUMB"),
                     DefaultMQPushConsumer("GID_SOCKS_PLUMB"),
                     DefaultMQPullConsumer("GID_SOCKS_PLUMB"),
                     DefaultLitePullConsumer("GID_SOCKS_PLUMB"),
                     DefaultMQAdminExt()]

    def set_socks_proxy_config(self, cfg):
        for o in self.objs:
            o.set_socks_proxy_config(cfg)
