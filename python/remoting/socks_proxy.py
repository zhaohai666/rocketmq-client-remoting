# -*- coding: utf-8 -*-
from __future__ import annotations

import socket
import struct
from typing import Optional

SOCKS4 = "SOCKS4"
SOCKS5 = "SOCKS5"
SUPPORTED_PROXY_TYPES = (SOCKS4, SOCKS5)

# ---- SOCKS5 (RFC 1928) ----
_S5_VERSION = 0x05
_S5_CMD_CONNECT = 0x01
_S5_METHOD_NO_AUTH = 0x00
_S5_METHOD_USERNAME_PASSWORD = 0x02
_S5_METHOD_NO_ACCEPTABLE = 0xFF
_S5_REP_SUCCEEDED = 0x00

_S5_REP_TEXT = {
    0x01: "general SOCKS server failure",
    0x02: "connection not allowed by ruleset",
    0x03: "network unreachable",
    0x04: "host unreachable",
    0x05: "connection refused",
    0x06: "TTL expired",
    0x07: "command not supported",
    0x08: "address type not supported",
}

# ---- SOCKS5 username/password sub-negotiation (RFC 1929) ----
_UP_VERSION = 0x01

# ---- SOCKS4 (including the 4a domain extension) ----
_S4_VERSION = 0x04
_S4_CMD_CONNECT = 0x01
_S4_GRANTED = 0x5A
# SOCKS4a: the target address is filled with 0.0.0.x (x non-zero) and the domain follows the
# userid, telling the proxy to do the resolution
_S4A_IP_PLACEHOLDER = b"\x00\x00\x00\x01"


class SocksProxyError(Exception):
    """SOCKS handshake failure (rejected at the protocol level / version mismatch / connection closed by the proxy)."""


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Receive exactly n bytes; the peer closing the connection early counts as a handshake failure (the SOCKS reply length is fixed)."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise SocksProxyError("proxy closed connection during handshake")
        buf += chunk
    return bytes(buf)


class SocksProxyConfig:
    pass

    def __init__(self, addr: str = "", port: Optional[int] = None,
                 proxy_type: str = SOCKS5, username: str = "", password: str = "",
                 enable: Optional[bool] = None):
        addr = str(addr or "").strip()
        if addr.startswith("["):  # IPv6 literal [::1]:1080
            host, _, rest = addr[1:].partition("]")
            tail = rest.lstrip(":")
            if port is None and tail:
                try:
                    port = int(tail)
                except ValueError:
                    raise SocksProxyError("invalid port in proxy addr %r" % addr)
            addr = host
        elif ":" in addr:
            host, _, tail = addr.rpartition(":")
            if not tail.isdigit():
                raise SocksProxyError("invalid port in proxy addr %r" % addr)
            if port is None:
                port = int(tail)
            addr = host
        ptype = str(proxy_type or SOCKS5).strip().upper()
        if ptype not in SUPPORTED_PROXY_TYPES:
            raise SocksProxyError(
                "unsupported proxy type %r (expect one of %s)" % (proxy_type, SUPPORTED_PROXY_TYPES))
        self.addr = addr
        self.port = int(port) if port else 0
        self.proxy_type = ptype
        self.username = str(username or "")
        self.password = str(password or "")
        self.enable = enable

    def is_enabled(self) -> bool:
        """The enable semantics: an explicit switch wins; when it is not set explicitly (None), a non-empty addr means enabled."""
        if self.enable is None:
            return bool(self.addr)
        return bool(self.enable)

    def endpoint(self):
        """The proxy's own (host, port); an empty addr or a missing port means the configuration is incomplete, so it raises right away."""
        if not self.addr:
            raise SocksProxyError("socks proxy addr is empty")
        if not self.port:
            raise SocksProxyError("socks proxy port is not set (addr=%r)" % self.addr)
        return self.addr, self.port

    def __repr__(self) -> str:
        return ("SocksProxyConfig{type=%s, addr=%s, port=%d, username=%s, "
                "password=****, enable=%s}" % (self.proxy_type, self.addr, self.port,
                                               self.username or "-", self.enable))

    def __eq__(self, other) -> bool:
        if not isinstance(other, SocksProxyConfig):
            return NotImplemented
        return (self.addr, self.port, self.proxy_type, self.username,
                self.password, self.enable) == (
            other.addr, other.port, other.proxy_type, other.username,
            other.password, other.enable)


def _encode_socks5_target(host: str, port: int) -> bytes:
    """The target address segment of a CONNECT request (atyp + addr + port). A domain name is left for the proxy to resolve."""
    try:
        return b"\x01" + socket.inet_aton(host) + struct.pack(">H", port)
    except OSError:
        pass
    try:
        return b"\x04" + socket.inet_pton(socket.AF_INET6, host) + struct.pack(">H", port)
    except OSError:
        pass
    raw = host.encode("utf-8")
    if not raw or len(raw) > 255:
        raise SocksProxyError("invalid target host %r" % host)
    return b"\x03" + bytes([len(raw)]) + raw + struct.pack(">H", port)


def _socks5_handshake(sock: socket.socket, host: str, port: int,
                      config: SocksProxyConfig) -> None:
    has_auth = bool(config.username or config.password)
    methods = (_S5_METHOD_NO_AUTH, _S5_METHOD_USERNAME_PASSWORD) if has_auth \
        else (_S5_METHOD_NO_AUTH,)
    sock.sendall(bytes((_S5_VERSION, len(methods))) + bytes(methods))
    resp = _recv_exact(sock, 2)
    if resp[0] != _S5_VERSION:
        raise SocksProxyError("socks5 greeting: unexpected version 0x%02x" % resp[0])
    method = resp[1]
    if method == _S5_METHOD_USERNAME_PASSWORD:
        if not (config.username or config.password):
            raise SocksProxyError(
                "socks5 proxy requires username/password auth but config has none")
        u = config.username.encode("utf-8")
        p = config.password.encode("utf-8")
        if len(u) > 255 or len(p) > 255:
            raise SocksProxyError("socks5 username/password longer than 255 bytes")
        # RFC 1929: ver(1) ulen(1) uname ulen plen passwd
        sock.sendall(bytes((_UP_VERSION, len(u))) + u + bytes((len(p),)) + p)
        auth = _recv_exact(sock, 2)
        if auth[0] != _UP_VERSION:
            raise SocksProxyError("socks5 auth: unexpected version 0x%02x" % auth[0])
        if auth[1] != 0x00:
            raise SocksProxyError("socks5 username/password auth rejected by proxy")
    elif method != _S5_METHOD_NO_AUTH:
        raise SocksProxyError(
            "socks5: no acceptable auth method (proxy picked 0x%02x)" % method)
    # 2) CONNECT
    sock.sendall(bytes((_S5_VERSION, _S5_CMD_CONNECT, 0x00)) +
                 _encode_socks5_target(host, port))
    resp = _recv_exact(sock, 4)
    if resp[0] != _S5_VERSION:
        raise SocksProxyError("socks5 reply: unexpected version 0x%02x" % resp[0])
    rep = resp[1]
    if rep != _S5_REP_SUCCEEDED:
        raise SocksProxyError("socks5 CONNECT failed: 0x%02x (%s)"
                              % (rep, _S5_REP_TEXT.get(rep, "unknown")))
    # 3) Consume BND.ADDR/BND.PORT (the following bytes already belong to the RocketMQ frame and must not be left unread)
    atyp = resp[3]
    if atyp == 0x01:
        _recv_exact(sock, 4 + 2)
    elif atyp == 0x03:
        n = _recv_exact(sock, 1)[0]
        _recv_exact(sock, n + 2)
    elif atyp == 0x04:
        _recv_exact(sock, 16 + 2)
    else:
        raise SocksProxyError("socks5 reply: unexpected address type 0x%02x" % atyp)


def _socks4_handshake(sock: socket.socket, host: str, port: int,
                      config: SocksProxyConfig) -> None:
    # SOCKS4 has no method negotiation; when the target is a domain name it uses the 4a
    # extension (ver stays 4, the address is filled with the 0.0.0.x placeholder, and the
    # NUL-terminated domain follows the userid for the proxy to resolve).
    try:
        ip_bytes = socket.inet_aton(host)
        domain_tail = b""
    except OSError:
        ip_bytes = _S4A_IP_PLACEHOLDER
        domain_tail = host.encode("utf-8") + b"\x00"
    user = config.username.encode("utf-8")
    # ver(1) cmd(1) port(2) ip(4) userid NUL [domain NUL]
    sock.sendall(struct.pack(">BBH", _S4_VERSION, _S4_CMD_CONNECT, port)
                 + ip_bytes + user + b"\x00" + domain_tail)
    resp = _recv_exact(sock, 8)
    if resp[1] != _S4_GRANTED:
        raise SocksProxyError("socks4 CONNECT rejected: 0x%02x" % resp[1])


def socks_handshake(sock: socket.socket, host: str, port: int,
                    config: SocksProxyConfig) -> None:
    """Perform a SOCKS CONNECT handshake for ``host:port`` on a ``sock`` that is already connected to the proxy.

    Preconditions: ``sock`` is already TCP-connected to the proxy (``RemotingClient._create_conn``
    takes care of that) and is under timeout control (the connect timeout bounds the handshake
    duration as well). On success the socket is equivalent to a direct connection to the
    target; on failure ``SocksProxyError`` is raised and the caller turns it into a
    ``RemotingConnectException`` (the same exit as a rejected direct connection, so callers
    need no extra exception branch).
    """
    if config.proxy_type == SOCKS5:
        _socks5_handshake(sock, host, port, config)
    else:
        _socks4_handshake(sock, host, port, config)
