// SOCKS proxy support for the remoting transport (Java SocksProxyConfig +
// NettyRemotingClient's proxy bootstraps).
//
// Java keys proxy configs by destination CIDR (socksProxyConfig JSON) and lets
// Netty's Socks5ProxyHandler run the handshake inside the pipeline. This port
// keeps the SocksProxyConfig fields (addr / username / password) and applies
// ONE proxy to every remoting connection: the TCP connection is opened to the
// proxy, the SOCKS CONNECT handshake runs against it, and only then is TLS
// layered on top of the tunnel. The handshake is hand-written on the standard
// library — no third-party dependency — covering SOCKS5 (no-auth and
// username/password sub-negotiation) and SOCKS4/4a.
//
// The TopAddressing-style address-server HTTP fetch lives in the client layer
// and deliberately does NOT go through this path (same as Java, where the
// proxy only wraps the Netty channel bootstrap).
package remoting

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// Supported proxy types (SocksProxyConfig.Type). An empty type means SOCKS5,
// matching the Java default where only SOCKS5 is implemented.
const (
	SocksProxyTypeSocks5  = "SOCKS5"
	SocksProxyTypeSocks4  = "SOCKS4"
	SocksProxyTypeSocks4a = "SOCKS4A"
)

// SOCKS wire constants.
const (
	socksVersion5           = 0x05
	socksVersion4           = 0x04
	socksCmdConnect         = 0x01
	socksMethodNoAuth       = 0x00
	socksMethodUserPass     = 0x02
	socksMethodUnacceptable = 0xFF
	socks5AuthVersion       = 0x01
	socks5ReplySuccess      = 0x00
	socks4ReplyGranted      = 0x5A // 90
)

// SocksProxyConfig mirrors org.apache.rocketmq.remoting.proxy.SocksProxyConfig:
// the proxy address plus optional username/password. Type extends it with the
// protocol selector (SOCKS5 by default, SOCKS4/4A for legacy proxies).
type SocksProxyConfig struct {
	// Addr is the proxy endpoint, "host:port" (IPv6 as "[host]:port").
	Addr string
	// Username / Password enable the SOCKS5 username/password sub-negotiation
	// (RFC 1929) when both are non-empty. SOCKS4 uses Username as userid.
	Username string
	Password string
	// Type picks the protocol: "" / SOCKS5 (default), SOCKS4 or SOCKS4A.
	Type string
}

// NewSocksProxyConfig builds a no-auth proxy config (Java SocksProxyConfig(addr)).
func NewSocksProxyConfig(addr string) *SocksProxyConfig {
	return &SocksProxyConfig{Addr: addr}
}

// NewSocksProxyConfigWithAuth builds a proxy config with credentials
// (Java SocksProxyConfig(addr, username, password)).
func NewSocksProxyConfigWithAuth(addr, username, password string) *SocksProxyConfig {
	return &SocksProxyConfig{Addr: addr, Username: username, Password: password}
}

// String mirrors Java SocksProxyConfig#toString but masks the password.
func (c *SocksProxyConfig) String() string {
	password := ""
	if c.Password != "" {
		password = "<set>"
	}
	return fmt.Sprintf("SocksProxy address: %s, username: %s, password: %s", c.Addr, c.Username, password)
}

// normalized resolves the proxy address, validates the port and folds Type to
// one of the SocksProxyType constants.
func (c *SocksProxyConfig) normalized() (proxyAddr, typ string, err error) {
	proxyAddr = strings.TrimSpace(c.Addr)
	if proxyAddr == "" {
		return "", "", fmt.Errorf("socks proxy addr is empty")
	}
	host, portStr := common.ParseAddr(proxyAddr)
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return "", "", fmt.Errorf("socks proxy addr %q has no valid port", c.Addr)
	}
	if host == "" {
		return "", "", fmt.Errorf("socks proxy addr %q has an empty host", c.Addr)
	}
	switch strings.ToUpper(strings.TrimSpace(c.Type)) {
	case "", SocksProxyTypeSocks5:
		typ = SocksProxyTypeSocks5
	case SocksProxyTypeSocks4:
		typ = SocksProxyTypeSocks4
	case SocksProxyTypeSocks4a:
		typ = SocksProxyTypeSocks4a
	default:
		return "", "", fmt.Errorf("unsupported socks proxy type %q (want SOCKS4, SOCKS4A or SOCKS5)", c.Type)
	}
	return proxyAddr, typ, nil
}

// dialSocksProxy opens the tunnel: TCP connect to the proxy, SOCKS CONNECT to
// targetHost:targetPort. The returned net.Conn is the raw tunnel — TLS (if
// enabled) is layered on top by the caller. timeout budgets BOTH the TCP dial
// and the SOCKS handshake together.
func dialSocksProxy(proxy *SocksProxyConfig, targetHost string, targetPort int, timeout time.Duration) (net.Conn, error) {
	proxyAddr, typ, err := proxy.normalized()
	if err != nil {
		return nil, err
	}
	if targetHost == "" {
		return nil, fmt.Errorf("socks proxy target host is empty")
	}
	if targetPort <= 0 || targetPort > 65535 {
		return nil, fmt.Errorf("socks proxy target port %d is invalid", targetPort)
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial("tcp", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("dial socks proxy %s: %w", proxyAddr, err)
	}
	// The whole handshake (TCP dial already consumed part of the budget on the
	// deadline below) must finish before the transport layer sees a dead proxy
	// as a hung connect.
	deadline := time.Now().Add(timeout)
	if err := conn.SetDeadline(deadline); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("socks proxy %s: set deadline: %w", proxyAddr, err)
	}
	switch typ {
	case SocksProxyTypeSocks5:
		err = socks5Handshake(conn, proxy, targetHost, targetPort)
	case SocksProxyTypeSocks4, SocksProxyTypeSocks4a:
		err = socks4Handshake(conn, proxy, targetHost, targetPort, typ == SocksProxyTypeSocks4a)
	}
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	// Handshake done: the tunnel carries framed traffic with no per-call
	// deadline (the remoting layer owns read/write lifecycles).
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

// ---------------------------------------------------------------- SOCKS5

// socks5Handshake runs RFC 1928 method negotiation (no-auth and, when
// credentials are configured, the RFC 1929 username/password sub-negotiation)
// followed by CONNECT.
func socks5Handshake(conn net.Conn, proxy *SocksProxyConfig, host string, port int) error {
	hasCreds := proxy.Username != "" && proxy.Password != ""
	methods := []byte{socksMethodNoAuth}
	if hasCreds {
		// Offer both: a server that accepts no-auth spares one round trip;
		// one that demands auth picks the sub-negotiation.
		methods = append(methods, socksMethodUserPass)
	}
	greeting := make([]byte, 2+len(methods))
	greeting[0] = socksVersion5
	greeting[1] = byte(len(methods))
	copy(greeting[2:], methods)
	if _, err := conn.Write(greeting); err != nil {
		return fmt.Errorf("socks5 proxy %s: send method selection: %w", proxy.Addr, err)
	}
	var picked [2]byte
	if _, err := io.ReadFull(conn, picked[:]); err != nil {
		return fmt.Errorf("socks5 proxy %s: read method selection reply: %w", proxy.Addr, err)
	}
	if picked[0] != socksVersion5 {
		return fmt.Errorf("socks5 proxy %s: unexpected method reply version %#x", proxy.Addr, picked[0])
	}
	switch picked[1] {
	case socksMethodNoAuth:
		// nothing to negotiate
	case socksMethodUserPass:
		if !hasCreds {
			return fmt.Errorf("socks5 proxy %s: server demands username/password auth but no credentials configured", proxy.Addr)
		}
		if err := socks5UserPassAuth(conn, proxy); err != nil {
			return err
		}
	case socksMethodUnacceptable:
		return fmt.Errorf("socks5 proxy %s: no acceptable authentication method", proxy.Addr)
	default:
		return fmt.Errorf("socks5 proxy %s: server picked unsupported auth method %#x", proxy.Addr, picked[1])
	}
	return socks5Connect(conn, proxy, host, port)
}

// socks5UserPassAuth performs the RFC 1929 username/password sub-negotiation.
func socks5UserPassAuth(conn net.Conn, proxy *SocksProxyConfig) error {
	if len(proxy.Username) > 255 || len(proxy.Password) > 255 {
		return fmt.Errorf("socks5 proxy %s: username/password longer than 255 bytes", proxy.Addr)
	}
	req := make([]byte, 0, 3+len(proxy.Username)+len(proxy.Password))
	req = append(req, socks5AuthVersion, byte(len(proxy.Username)))
	req = append(req, proxy.Username...)
	req = append(req, byte(len(proxy.Password)))
	req = append(req, proxy.Password...)
	if _, err := conn.Write(req); err != nil {
		return fmt.Errorf("socks5 proxy %s: send username/password auth: %w", proxy.Addr, err)
	}
	var reply [2]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		return fmt.Errorf("socks5 proxy %s: read auth reply: %w", proxy.Addr, err)
	}
	if reply[0] != socks5AuthVersion {
		return fmt.Errorf("socks5 proxy %s: unexpected auth reply version %#x", proxy.Addr, reply[0])
	}
	if reply[1] != 0x00 {
		return fmt.Errorf("socks5 proxy %s: username/password authentication rejected (status %#x)", proxy.Addr, reply[1])
	}
	return nil
}

func socks5Connect(conn net.Conn, proxy *SocksProxyConfig, host string, port int) error {
	target, err := socks5TargetBytes(host, port)
	if err != nil {
		return fmt.Errorf("socks5 proxy %s: %w", proxy.Addr, err)
	}
	req := make([]byte, 0, 5+len(target))
	req = append(req, socksVersion5, socksCmdConnect, 0x00)
	req = append(req, target...)
	if _, err := conn.Write(req); err != nil {
		return fmt.Errorf("socks5 proxy %s: send CONNECT: %w", proxy.Addr, err)
	}
	var head [4]byte
	if _, err := io.ReadFull(conn, head[:]); err != nil {
		return fmt.Errorf("socks5 proxy %s: read CONNECT reply: %w", proxy.Addr, err)
	}
	if head[0] != socksVersion5 {
		return fmt.Errorf("socks5 proxy %s: unexpected CONNECT reply version %#x", proxy.Addr, head[0])
	}
	if head[1] != socks5ReplySuccess {
		return fmt.Errorf("socks5 proxy %s: CONNECT refused: %s (rep %#x)", proxy.Addr, socks5ReplyText(head[1]), head[1])
	}
	// Skip BND.ADDR/BND.PORT — usually zeroes, but the shape varies by ATYP.
	skip, err := socks5ReplyAddrLen(conn, head[3])
	if err != nil {
		return fmt.Errorf("socks5 proxy %s: read CONNECT reply address: %w", proxy.Addr, err)
	}
	if skip > 0 {
		buf := make([]byte, skip)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return fmt.Errorf("socks5 proxy %s: read CONNECT reply address: %w", proxy.Addr, err)
		}
	}
	return nil
}

// socks5TargetBytes encodes DST.ADDR/DST.PORT (ATYP-prefixed): IPv4, IPv6 or
// a length-prefixed domain name.
func socks5TargetBytes(host string, port int) ([]byte, error) {
	var buf []byte
	if ip := net.ParseIP(host); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			buf = append([]byte{0x01}, ip4...)
		} else {
			buf = append([]byte{0x04}, ip.To16()...)
		}
	} else {
		if len(host) > 255 {
			return nil, fmt.Errorf("target host longer than 255 bytes")
		}
		buf = append([]byte{0x03}, byte(len(host)))
		buf = append(buf, host...)
	}
	var portBytes [2]byte
	binary.BigEndian.PutUint16(portBytes[:], uint16(port))
	return append(buf, portBytes[:]...), nil
}

// socks5ReplyAddrLen returns how many bytes of BND.ADDR/BND.PORT remain after
// the 4-byte reply head.
func socks5ReplyAddrLen(conn net.Conn, atyp byte) (int, error) {
	switch atyp {
	case 0x01:
		return 4 + 2, nil
	case 0x04:
		return 16 + 2, nil
	case 0x03:
		var n [1]byte
		if _, err := io.ReadFull(conn, n[:]); err != nil {
			return 0, err
		}
		return int(n[0]) + 2, nil
	default:
		return 0, fmt.Errorf("unknown reply address type %#x", atyp)
	}
}

func socks5ReplyText(rep byte) string {
	switch rep {
	case 0x00:
		return "succeeded"
	case 0x01:
		return "general SOCKS server failure"
	case 0x02:
		return "connection not allowed by ruleset"
	case 0x03:
		return "network unreachable"
	case 0x04:
		return "host unreachable"
	case 0x05:
		return "connection refused"
	case 0x06:
		return "TTL expired"
	case 0x07:
		return "command not supported"
	case 0x08:
		return "address type not supported"
	}
	return "unknown reply code"
}

// ---------------------------------------------------------------- SOCKS4/4A

// socks4Handshake runs the SOCKS4 CONNECT (userid, no auth beyond that) or the
// SOCKS4A extension that lets the proxy resolve domain names.
func socks4Handshake(conn net.Conn, proxy *SocksProxyConfig, host string, port int, allowDomain bool) error {
	var dst [4]byte
	var domain string
	if ip := net.ParseIP(host); ip != nil {
		ip4 := ip.To4()
		if ip4 == nil {
			return fmt.Errorf("socks4 proxy %s: IPv6 target %q is not supported", proxy.Addr, host)
		}
		copy(dst[:], ip4)
	} else {
		if !allowDomain {
			return fmt.Errorf("socks4 proxy %s: cannot resolve hostnames; use type SOCKS4A for target %q", proxy.Addr, host)
		}
		if len(host) > 255 {
			return fmt.Errorf("socks4 proxy %s: target host longer than 255 bytes", proxy.Addr)
		}
		// 4a sentinel: DST.IP starts with 0.0.0 and the last byte non-zero.
		dst = [4]byte{0, 0, 0, 1}
		domain = host
	}
	if len(proxy.Username) > 255 {
		return fmt.Errorf("socks4 proxy %s: userid longer than 255 bytes", proxy.Addr)
	}
	req := make([]byte, 0, 8+len(proxy.Username)+len(domain)+2)
	req = append(req, socksVersion4, socksCmdConnect)
	var portBytes [2]byte
	binary.BigEndian.PutUint16(portBytes[:], uint16(port))
	req = append(req, portBytes[:]...)
	req = append(req, dst[:]...)
	req = append(req, proxy.Username...)
	req = append(req, 0x00)
	if domain != "" {
		req = append(req, domain...)
		req = append(req, 0x00)
	}
	if _, err := conn.Write(req); err != nil {
		return fmt.Errorf("socks4 proxy %s: send CONNECT: %w", proxy.Addr, err)
	}
	var reply [8]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		return fmt.Errorf("socks4 proxy %s: read CONNECT reply: %w", proxy.Addr, err)
	}
	if reply[1] != socks4ReplyGranted {
		return fmt.Errorf("socks4 proxy %s: CONNECT refused: %s (code %d)", proxy.Addr, socks4ReplyText(reply[1]), reply[1])
	}
	return nil
}

func socks4ReplyText(code byte) string {
	switch code {
	case 0x5A:
		return "request granted"
	case 0x5B:
		return "request rejected or failed"
	case 0x5C:
		return "request rejected: cannot reach identd"
	case 0x5D:
		return "request rejected: identd could not confirm the user id"
	}
	return "unknown reply code"
}
