// SOCKS proxy tests: a minimal fake SOCKS5 / SOCKS4(A) server is enough to
// exercise the full handshake path — method negotiation, the RFC 1929
// username/password sub-negotiation, CONNECT success and the rejection paths —
// because the tunnel payload after CONNECT is opaque bytes to the proxy.
package remoting

import (
	"bytes"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

// ---------------- fakes ----------------

// byteEchoServer answers every byte back — the "real service" behind the
// proxy for raw-tunnel tests.
func byteEchoServer(t *testing.T) string {
	t.Helper()
	listener := listenLocal(t)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}(conn)
		}
	}()
	return listener.Addr().String()
}

// fakeSocks5Server is a minimal RFC 1928 server: method negotiation,
// optional username/password sub-negotiation, CONNECT accept/reject, then a
// raw byte pipe to the dialled target (a real proxy's data phase).
type fakeSocks5Server struct {
	listener      net.Listener
	requireAuth   bool
	username      string
	password      string
	rejectConnect bool

	mu           sync.Mutex
	authAttempts []string
	targets      []string
	conns        int
}

func newFakeSocks5Server(t *testing.T, requireAuth bool, username, password string) *fakeSocks5Server {
	t.Helper()
	s := &fakeSocks5Server{
		listener:    listenLocal(t),
		requireAuth: requireAuth,
		username:    username,
		password:    password,
	}
	go s.serve()
	t.Cleanup(func() { _ = s.listener.Close() })
	return s
}

func (s *fakeSocks5Server) addr() string { return s.listener.Addr().String() }
func (s *fakeSocks5Server) connCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns
}
func (s *fakeSocks5Server) authUsernames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.authAttempts...)
}
func (s *fakeSocks5Server) connectTargets() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.targets...)
}

func (s *fakeSocks5Server) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns++
		s.mu.Unlock()
		go s.handle(conn)
	}
}

func (s *fakeSocks5Server) handle(conn net.Conn) {
	defer conn.Close()
	// --- method selection ---
	var head [2]byte
	if _, err := io.ReadFull(conn, head[:]); err != nil {
		return
	}
	if head[0] != 0x05 {
		return
	}
	methods := make([]byte, int(head[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return
	}
	var chosen byte = 0xFF
	if s.requireAuth {
		if bytes.IndexByte(methods, 0x02) >= 0 {
			chosen = 0x02
		}
	} else if bytes.IndexByte(methods, 0x00) >= 0 {
		chosen = 0x00
	}
	if _, err := conn.Write([]byte{0x05, chosen}); err != nil {
		return
	}
	if chosen == 0xFF {
		return
	}
	// --- username/password sub-negotiation (RFC 1929) ---
	if chosen == 0x02 {
		var verUl [2]byte
		if _, err := io.ReadFull(conn, verUl[:]); err != nil || verUl[0] != 0x01 {
			return
		}
		user := make([]byte, int(verUl[1]))
		if _, err := io.ReadFull(conn, user); err != nil {
			return
		}
		var plen [1]byte
		if _, err := io.ReadFull(conn, plen[:]); err != nil {
			return
		}
		pass := make([]byte, int(plen[0]))
		if _, err := io.ReadFull(conn, pass); err != nil {
			return
		}
		s.mu.Lock()
		s.authAttempts = append(s.authAttempts, string(user))
		s.mu.Unlock()
		ok := string(user) == s.username && string(pass) == s.password
		status := byte(0x00)
		if !ok {
			status = 0x01
		}
		if _, err := conn.Write([]byte{0x01, status}); err != nil {
			return
		}
		if !ok {
			return
		}
	}
	// --- CONNECT ---
	var reqHead [4]byte
	if _, err := io.ReadFull(conn, reqHead[:]); err != nil {
		return
	}
	if reqHead[0] != 0x05 || reqHead[1] != 0x01 {
		_, _ = conn.Write([]byte{0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	host, err := socks5ReadHost(conn, reqHead[3])
	if err != nil {
		return
	}
	var portBytes [2]byte
	if _, err := io.ReadFull(conn, portBytes[:]); err != nil {
		return
	}
	target := net.JoinHostPort(host, strconv.Itoa(int(portBytes[0])<<8|int(portBytes[1])))
	s.mu.Lock()
	s.targets = append(s.targets, target)
	s.mu.Unlock()
	if s.rejectConnect {
		_, _ = conn.Write([]byte{0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	_, _ = conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	// data phase: pipe both ways until either side closes.
	upstream, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		return
	}
	defer upstream.Close()
	go func() {
		_, _ = io.Copy(upstream, conn)
		_ = upstream.Close()
	}()
	_, _ = io.Copy(conn, upstream)
}

// socks5ReadHost decodes the ATYP-prefixed DST.ADDR.
func socks5ReadHost(conn net.Conn, atyp byte) (string, error) {
	switch atyp {
	case 0x01:
		var ip [4]byte
		if _, err := io.ReadFull(conn, ip[:]); err != nil {
			return "", err
		}
		return net.IP(ip[:]).String(), nil
	case 0x04:
		var ip [16]byte
		if _, err := io.ReadFull(conn, ip[:]); err != nil {
			return "", err
		}
		return net.IP(ip[:]).String(), nil
	case 0x03:
		var n [1]byte
		if _, err := io.ReadFull(conn, n[:]); err != nil {
			return "", err
		}
		host := make([]byte, int(n[0]))
		if _, err := io.ReadFull(conn, host); err != nil {
			return "", err
		}
		return string(host), nil
	}
	return "", io.ErrUnexpectedEOF
}

// fakeSocks4Server answers SOCKS4/4A CONNECTs and pipes to the target.
type fakeSocks4Server struct {
	listener net.Listener

	mu         sync.Mutex
	targets    []string
	userids    []string
	reject     bool
	replyBytes [8]byte
}

func newFakeSocks4Server(t *testing.T) *fakeSocks4Server {
	t.Helper()
	s := &fakeSocks4Server{listener: listenLocal(t)}
	go s.serve()
	t.Cleanup(func() { _ = s.listener.Close() })
	return s
}

func (s *fakeSocks4Server) addr() string { return s.listener.Addr().String() }

func (s *fakeSocks4Server) record() (targets, userids []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.targets...), append([]string(nil), s.userids...)
}

func (s *fakeSocks4Server) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *fakeSocks4Server) handle(conn net.Conn) {
	defer conn.Close()
	var head [8]byte
	if _, err := io.ReadFull(conn, head[:]); err != nil {
		return
	}
	if head[0] != 0x04 || head[1] != 0x01 {
		return
	}
	port := int(head[2])<<8 | int(head[3])
	// userid: NUL-terminated.
	userid, err := readCString(conn)
	if err != nil {
		return
	}
	host := ""
	if head[4] == 0 && head[5] == 0 && head[6] == 0 && head[7] != 0 {
		// SOCKS4A: domain follows the userid.
		domain, err := readCString(conn)
		if err != nil {
			return
		}
		host = domain
	} else {
		host = net.IPv4(head[4], head[5], head[6], head[7]).String()
	}
	s.mu.Lock()
	s.targets = append(s.targets, net.JoinHostPort(host, strconv.Itoa(port)))
	s.userids = append(s.userids, userid)
	s.mu.Unlock()
	reply := [8]byte{0x00, 0x5A}
	if s.reject {
		reply[1] = 0x5B
	}
	if _, err := conn.Write(reply[:]); err != nil {
		return
	}
	if s.reject {
		return
	}
	upstream, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), 5*time.Second)
	if err != nil {
		return
	}
	defer upstream.Close()
	go func() {
		_, _ = io.Copy(upstream, conn)
		_ = upstream.Close()
	}()
	_, _ = io.Copy(conn, upstream)
}

func readCString(conn net.Conn) (string, error) {
	var sb bytes.Buffer
	var b [1]byte
	for {
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			return "", err
		}
		if b[0] == 0x00 {
			return sb.String(), nil
		}
		sb.WriteByte(b[0])
	}
}

// ---------------- helpers ----------------

func dialThrough(t *testing.T, proxy *SocksProxyConfig, target string, timeout time.Duration) net.Conn {
	t.Helper()
	host, portStr := splitHostPortForTest(target)
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := dialSocksProxy(proxy, host, port, timeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func splitHostPortForTest(addr string) (string, string) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, ""
	}
	return host, port
}

func roundTripRaw(t *testing.T, conn net.Conn, payload string) string {
	t.Helper()
	if _, err := conn.Write([]byte(payload)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	return string(got)
}

// ---------------- SOCKS5 ----------------

func TestSocks5ProxyConnectRoundTrip(t *testing.T) {
	target := byteEchoServer(t)
	proxy := newFakeSocks5Server(t, false, "", "")
	cfg := NewSocksProxyConfig(proxy.addr())
	conn := dialThrough(t, cfg, target, 10*time.Second)
	if got := roundTripRaw(t, conn, "ping-through-socks5"); got != "ping-through-socks5" {
		t.Fatalf("echo mismatch: %q", got)
	}
	targets := proxy.connectTargets()
	if len(targets) != 1 || targets[0] != target {
		t.Fatalf("CONNECT target = %v, want [%s]", targets, target)
	}
}

func TestSocks5ProxyDomainTarget(t *testing.T) {
	// "localhost" forces the ATYP=0x03 (domain) encoding on the client side.
	target := byteEchoServer(t)
	_, port, _ := net.SplitHostPort(target)
	domainTarget := net.JoinHostPort("localhost", port)
	proxy := newFakeSocks5Server(t, false, "", "")
	cfg := NewSocksProxyConfig(proxy.addr())
	conn := dialThrough(t, cfg, domainTarget, 10*time.Second)
	if got := roundTripRaw(t, conn, "domain"); got != "domain" {
		t.Fatalf("echo mismatch: %q", got)
	}
	targets := proxy.connectTargets()
	if len(targets) != 1 || targets[0] != domainTarget {
		t.Fatalf("CONNECT target = %v, want [%s]", targets, domainTarget)
	}
}

func TestSocks5UsernamePasswordAuth(t *testing.T) {
	target := byteEchoServer(t)
	proxy := newFakeSocks5Server(t, true, "rmqUser", "rmqPass")
	cfg := NewSocksProxyConfigWithAuth(proxy.addr(), "rmqUser", "rmqPass")
	conn := dialThrough(t, cfg, target, 10*time.Second)
	if got := roundTripRaw(t, conn, "authed"); got != "authed" {
		t.Fatalf("echo mismatch: %q", got)
	}
	users := proxy.authUsernames()
	if len(users) != 1 || users[0] != "rmqUser" {
		t.Fatalf("auth usernames = %v, want [rmqUser]", users)
	}
	if targets := proxy.connectTargets(); len(targets) != 1 || targets[0] != target {
		t.Fatalf("CONNECT target = %v, want [%s]", targets, target)
	}
}

func TestSocks5WrongPasswordRejected(t *testing.T) {
	target := byteEchoServer(t)
	proxy := newFakeSocks5Server(t, true, "rmqUser", "rmqPass")
	cfg := NewSocksProxyConfigWithAuth(proxy.addr(), "rmqUser", "WRONG")
	// Failure-class assertions get a generous budget: the handshake itself is
	// bounded by this timeout, not by any retry loop (a port just closed on the
	// local loopback is refused within ~2s, so failure cases are given a full budget).
	_, err := dialSocksProxy(cfg, hostOf(target), portOf(target), 35*time.Second)
	if err == nil {
		t.Fatal("expected auth failure, got nil error")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("authentication rejected")) {
		t.Fatalf("unexpected error: %v", err)
	}
	conn, dialErr := dialSocksProxy(NewSocksProxyConfigWithAuth(proxy.addr(), "rmqUser", "rmqPass"), hostOf(target), portOf(target), 10*time.Second)
	if dialErr != nil {
		t.Fatalf("correct credentials must still succeed after a rejected attempt: %v", dialErr)
	}
	_ = conn.Close()
}

func TestSocks5NoCredsAgainstAuthServerRejected(t *testing.T) {
	target := byteEchoServer(t)
	proxy := newFakeSocks5Server(t, true, "u", "p")
	// Client offers only no-auth; the server must answer 0xFF and the client
	// must surface it instead of hanging.
	_, err := dialSocksProxy(NewSocksProxyConfig(proxy.addr()), hostOf(target), portOf(target), 35*time.Second)
	if err == nil {
		t.Fatal("expected no-acceptable-method failure, got nil error")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("no acceptable authentication method")) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSocks5ConnectRejected(t *testing.T) {
	target := byteEchoServer(t)
	proxy := newFakeSocks5Server(t, false, "", "")
	proxy.rejectConnect = true
	_, err := dialSocksProxy(NewSocksProxyConfig(proxy.addr()), hostOf(target), portOf(target), 35*time.Second)
	if err == nil {
		t.Fatal("expected CONNECT rejection, got nil error")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("general SOCKS server failure")) {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestRemotingInvokeSyncThroughSocks5Proxy pins the integration point: the
// remoting client's connect() dials the PROXY and the SOCKS CONNECT target is
// the remoting peer, so a full InvokeSync round trip works end to end.
func TestRemotingInvokeSyncThroughSocks5Proxy(t *testing.T) {
	addr := echoServer(t)
	proxy := newFakeSocks5Server(t, false, "", "")
	client := NewRemotingClientWithConfig(ClientConfig{
		SocksProxy:                NewSocksProxyConfig(proxy.addr()),
		ConnectTimeoutMillis:      3000,
		InvokeTimeoutMillis:       3000,
		EnableReconnectForGoAway:  true,
		CloseChannelOnSyncTimeout: true,
	})
	defer client.Shutdown()
	response, err := client.InvokeSync(addr, request(ReqSendMessageV2, "via-proxy"), 3000)
	if err != nil {
		t.Fatal(err)
	}
	if response.Code != RespSuccess {
		t.Fatalf("response code = %d, want %d", response.Code, RespSuccess)
	}
	if response.Remark != "pong:via-proxy" {
		t.Fatalf("remark = %q", response.Remark)
	}
	if targets := proxy.connectTargets(); len(targets) == 0 || targets[0] != addr {
		t.Fatalf("CONNECT targets = %v, want [%s]", targets, addr)
	}
}

// ---------------- SOCKS4/4A ----------------

func TestSocks4ConnectRoundTrip(t *testing.T) {
	target := byteEchoServer(t)
	proxy := newFakeSocks4Server(t)
	cfg := &SocksProxyConfig{Addr: proxy.addr(), Type: SocksProxyTypeSocks4, Username: "rmq"}
	conn := dialThrough(t, cfg, target, 10*time.Second)
	if got := roundTripRaw(t, conn, "socks4"); got != "socks4" {
		t.Fatalf("echo mismatch: %q", got)
	}
	targets, userids := proxy.record()
	if len(targets) != 1 || targets[0] != target {
		t.Fatalf("CONNECT targets = %v, want [%s]", targets, target)
	}
	if len(userids) != 1 || userids[0] != "rmq" {
		t.Fatalf("userids = %v, want [rmq]", userids)
	}
}

func TestSocks4aDomainConnectRoundTrip(t *testing.T) {
	target := byteEchoServer(t)
	_, port, _ := net.SplitHostPort(target)
	domainTarget := net.JoinHostPort("localhost", port)
	proxy := newFakeSocks4Server(t)
	cfg := &SocksProxyConfig{Addr: proxy.addr(), Type: SocksProxyTypeSocks4a}
	conn := dialThrough(t, cfg, domainTarget, 10*time.Second)
	if got := roundTripRaw(t, conn, "socks4a"); got != "socks4a" {
		t.Fatalf("echo mismatch: %q", got)
	}
	targets, _ := proxy.record()
	if len(targets) != 1 || targets[0] != domainTarget {
		t.Fatalf("CONNECT targets = %v, want [%s]", targets, domainTarget)
	}
}

func TestSocks4DomainTargetRequires4a(t *testing.T) {
	proxy := newFakeSocks4Server(t)
	// Plain SOCKS4 cannot carry a hostname: the client must refuse up front
	// instead of sending the proxy an unresolvable request.
	_, err := dialSocksProxy(&SocksProxyConfig{Addr: proxy.addr(), Type: SocksProxyTypeSocks4},
		"localhost", 9876, 10*time.Second)
	if err == nil {
		t.Fatal("expected SOCKS4-with-domain error, got nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("SOCKS4A")) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSocks4ConnectRejected(t *testing.T) {
	target := byteEchoServer(t)
	proxy := newFakeSocks4Server(t)
	proxy.reject = true
	_, err := dialSocksProxy(&SocksProxyConfig{Addr: proxy.addr(), Type: SocksProxyTypeSocks4},
		hostOf(target), portOf(target), 35*time.Second)
	if err == nil {
		t.Fatal("expected SOCKS4 rejection, got nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("request rejected or failed")) {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------- config validation ----------------

func TestSocksProxyConfigValidation(t *testing.T) {
	cases := []struct {
		name    string
		cfg     *SocksProxyConfig
		wantErr string
	}{
		{"empty addr", &SocksProxyConfig{}, "addr is empty"},
		{"no port", &SocksProxyConfig{Addr: "127.0.0.1"}, "no valid port"},
		{"bad port", &SocksProxyConfig{Addr: "127.0.0.1:99999"}, "no valid port"},
		{"bad type", &SocksProxyConfig{Addr: "127.0.0.1:1080", Type: "SOCKS7"}, "unsupported socks proxy type"},
	}
	for _, tc := range cases {
		_, _, err := tc.cfg.normalized()
		if err == nil {
			t.Fatalf("%s: expected error", tc.name)
		}
		if !bytes.Contains([]byte(err.Error()), []byte(tc.wantErr)) {
			t.Fatalf("%s: error %q does not contain %q", tc.name, err, tc.wantErr)
		}
	}
	// Default type is SOCKS5; case-insensitive types fold.
	if _, typ, err := (&SocksProxyConfig{Addr: "127.0.0.1:1080"}).normalized(); err != nil || typ != SocksProxyTypeSocks5 {
		t.Fatalf("default type = %q err=%v, want SOCKS5", typ, err)
	}
	if _, typ, err := (&SocksProxyConfig{Addr: "127.0.0.1:1080", Type: "socks4a"}).normalized(); err != nil || typ != SocksProxyTypeSocks4a {
		t.Fatalf("lowercase type = %q err=%v, want SOCKS4A", typ, err)
	}
}

func hostOf(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

func portOf(addr string) int {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(port)
	return n
}
