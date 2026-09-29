// Transport tests: real in-process TCP/TLS servers, mirroring the Rust
// client.rs regression net — round trips, half-packet reassembly, opaque
// matching, GO_AWAY retry, fail-fast on EOF, shutdown draining, processor
// dispatch and hooks.
package remoting

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"io"
	"math"
	"math/big"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

func readFrame(conn net.Conn) ([]byte, bool) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return nil, false
	}
	total := int32(binary.BigEndian.Uint32(lenBuf[:]))
	if total <= 0 || total > MaxFrameLength {
		return nil, false
	}
	frame := make([]byte, 4+int(total))
	copy(frame, lenBuf[:])
	if _, err := io.ReadFull(conn, frame[4:]); err != nil {
		return nil, false
	}
	return frame, true
}

func writeFrame(conn net.Conn, cmd *RemotingCommand, chunk int) {
	data := cmd.Encode()
	if chunk <= 0 || chunk >= len(data) {
		_, _ = conn.Write(data)
		return
	}
	for off := 0; off < len(data); off += chunk {
		end := off + chunk
		if end > len(data) {
			end = len(data)
		}
		_, _ = conn.Write(data[off:end])
		time.Sleep(time.Millisecond)
	}
}

func request(code int32, remark string) *RemotingCommand {
	cmd := CreateRequestCommand(code, nil)
	cmd.Remark = remark
	return cmd
}

func responseFor(req *RemotingCommand, code int32) *RemotingCommand {
	cmd := CreateResponseCommand(code, "pong")
	cmd.Opaque = req.Opaque
	cmd.SerializeTypeCurrentRPC = req.SerializeTypeCurrentRPC
	return cmd
}

// serveEcho answers every non-oneway request with the same opaque; chunk > 0
// splits each response into byte-sized pieces to force the reassembly path.
func serveEcho(conn net.Conn, chunk int) {
	defer conn.Close()
	for {
		frame, ok := readFrame(conn)
		if !ok {
			return
		}
		cmd, err := Decode(frame)
		if err != nil {
			return
		}
		if cmd.IsOnewayRPC() {
			continue
		}
		response := responseFor(cmd, RespSuccess)
		if cmd.Remark != "" {
			response.Remark = "pong:" + cmd.Remark
		}
		writeFrame(conn, response, chunk)
	}
}

func listenLocal(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	return listener
}

func echoServer(t *testing.T) string {
	t.Helper()
	listener := listenLocal(t)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveEcho(conn, 0)
		}
	}()
	return listener.Addr().String()
}

// GO_AWAY server: the first goAwayConns connections always answer GO_AWAY,
// later ones answer SUCCESS. The counter lets tests prove the retry dialed a
// new connection.
func goAwayServer(t *testing.T, goAwayConns int64) (string, *atomic.Int64) {
	t.Helper()
	listener := listenLocal(t)
	var conns atomic.Int64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			isGoAway := conns.Add(1)-1 < goAwayConns
			go func(conn net.Conn) {
				defer conn.Close()
				code := RespSuccess
				if isGoAway {
					code = RespGoAway
				}
				for {
					frame, ok := readFrame(conn)
					if !ok {
						return
					}
					cmd, err := Decode(frame)
					if err != nil {
						return
					}
					if cmd.IsOnewayRPC() {
						continue
					}
					writeFrame(conn, responseFor(cmd, code), 0)
				}
			}(conn)
		}
	}()
	return listener.Addr().String(), &conns
}

// The first dropConns connections read one request and hang up without
// replying — a real "request written, peer never answers" EOF; later
// connections echo normally.
func eofServer(t *testing.T, dropConns int64) (string, *atomic.Int64) {
	t.Helper()
	listener := listenLocal(t)
	var conns atomic.Int64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			idx := conns.Add(1) - 1
			if idx < dropConns {
				// 收到请求再关：制造"请求已写出、对端永不回复"的 EOF。
				_, _ = readFrame(conn)
				conn.Close()
				continue
			}
			go serveEcho(conn, 0)
		}
	}()
	return listener.Addr().String(), &conns
}

// Only accepts and counts requests: replies nothing, closes nothing —
// requests stay in flight until the client gives up itself.
func silentServer(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	listener := listenLocal(t)
	var frames atomic.Int64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				for {
					_, ok := readFrame(conn)
					if !ok {
						return
					}
					frames.Add(1)
				}
			}(conn)
		}
	}()
	return listener.Addr().String(), &frames
}

func waitUntil(t *testing.T, cond func() bool, what string) {
	t.Helper()
	for i := 0; i < 500; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等不到%s", what)
}

// ---------------- basic RPC ----------------

func TestInvokeSyncRoundTrip(t *testing.T) {
	addr := echoServer(t)
	client := NewRemotingClient()
	defer client.Shutdown()
	cmd := request(ReqSendMessageV2, "hi")
	response, err := client.InvokeSync(addr, cmd, 3000)
	if err != nil {
		t.Fatal(err)
	}
	if response.Code != RespSuccess {
		t.Fatalf("code = %d", response.Code)
	}
	if response.Opaque != cmd.Opaque {
		t.Fatalf("响应必须带回请求的 opaque: %d != %d", response.Opaque, cmd.Opaque)
	}
	if response.Remark != "pong:hi" {
		t.Fatalf("remark = %q", response.Remark)
	}
	if !response.IsResponseType() {
		t.Fatal("必须是响应帧")
	}
	if got := client.ConnectionAddrs(); len(got) != 1 || got[0] != addr {
		t.Fatalf("connection addrs = %v", got)
	}
}

func TestHalfPacketReassembly(t *testing.T) {
	addr := echoServer(t)
	client := NewRemotingClient()
	defer client.Shutdown()
	cmd := request(ReqPullMessage, "x")
	response, err := client.InvokeSync(addr, cmd, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if response.Code != RespSuccess {
		t.Fatalf("code = %d", response.Code)
	}
}

func TestConcurrentRequestsMatchTheirOwnOpaques(t *testing.T) {
	addr := echoServer(t)
	client := NewRemotingClient()
	defer client.Shutdown()
	const n = 8
	remarks := make(chan string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := request(ReqSendMessage, "n"+string(rune('0'+i)))
			response, err := client.InvokeSync(addr, cmd, 5000)
			if err != nil {
				t.Error(err)
				return
			}
			if response.Opaque != cmd.Opaque {
				t.Errorf("opaque mismatch: %d != %d", response.Opaque, cmd.Opaque)
				return
			}
			remarks <- response.Remark
		}(i)
	}
	wg.Wait()
	close(remarks)
	got := make([]string, 0, n)
	for r := range remarks {
		got = append(got, r)
	}
	sort.Strings(got)
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		want = append(want, "pong:n"+string(rune('0'+i)))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("remarks = %v, want %v", got, want)
		}
	}
}

func TestInvokeOnewayDoesNotWait(t *testing.T) {
	addr := echoServer(t)
	client := NewRemotingClient()
	defer client.Shutdown()
	cmd := request(ReqHeartBeat, "once")
	if err := client.InvokeOneway(addr, cmd); err != nil {
		t.Fatal(err)
	}
	if !cmd.IsOnewayRPC() {
		t.Fatal("oneway 标志必须打上")
	}
	if client.InFlightCount() != 0 {
		t.Fatalf("oneway 不登记在途表, got %d", client.InFlightCount())
	}
}

func TestTimeoutWhenBrokerStaysSilent(t *testing.T) {
	addr, _ := silentServer(t)
	client := NewRemotingClient()
	defer client.Shutdown()
	_, err := client.InvokeSync(addr, request(ReqPullMessage, "silent"), 300)
	if !common.IsKind(err, common.KindTimeout) {
		t.Fatalf("got %v", err)
	}
	if client.InFlightCount() != 0 {
		t.Fatalf("超时后在途表要清空, got %d", client.InFlightCount())
	}
}

// ---------------- GO_AWAY ----------------

func TestGoAwayReconnectsAndRetriesOnce(t *testing.T) {
	addr, conns := goAwayServer(t, 1)
	client := NewRemotingClient()
	defer client.Shutdown()
	response, err := client.InvokeSync(addr, request(ReqSendMessageV2, "goaway"), 3000)
	if err != nil {
		t.Fatal(err)
	}
	if response.Code != RespSuccess {
		t.Fatalf("重发必须拿到真应答, code = %d", response.Code)
	}
	if conns.Load() != 2 {
		t.Fatalf("GO_AWAY 后必须换新连接, conns = %d", conns.Load())
	}
	if client.InFlightCount() != 0 {
		t.Fatalf("in-flight = %d", client.InFlightCount())
	}
}

func TestSecondGoAwayFailsInsteadOfLooping(t *testing.T) {
	addr, conns := goAwayServer(t, math.MaxInt64)
	client := NewRemotingClient()
	defer client.Shutdown()
	_, err := client.InvokeSync(addr, request(ReqSendMessageV2, "goaway"), 3000)
	if !common.IsKind(err, common.KindSendRequest) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(err.Error(), "GO_AWAY twice") {
		t.Fatalf("文案要对齐 Java RemotingSendRequestException: %v", err)
	}
	if conns.Load() != 2 {
		t.Fatalf("只重发一次, 不能无限重连, conns = %d", conns.Load())
	}
	if client.InFlightCount() != 0 {
		t.Fatalf("in-flight = %d", client.InFlightCount())
	}
}

func TestGoAwayWithoutReconnectFlagSurfacesError(t *testing.T) {
	addr, conns := goAwayServer(t, math.MaxInt64)
	config := NewClientConfig()
	config.EnableReconnectForGoAway = false
	client := NewRemotingClientWithConfig(config)
	defer client.Shutdown()
	_, err := client.InvokeSync(addr, request(ReqSendMessageV2, "goaway"), 3000)
	if !common.IsKind(err, common.KindSendRequest) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(err.Error(), "Receive GO_AWAY from channel") {
		t.Fatalf("got %v", err)
	}
	if conns.Load() != 1 {
		t.Fatalf("关掉开关就不该重连, conns = %d", conns.Load())
	}
}

// ---------------- connect failures ----------------

func TestConnectFailureMapsToConnectError(t *testing.T) {
	listener := listenLocal(t)
	addr := listener.Addr().String()
	listener.Close()
	client := NewRemotingClient()
	defer client.Shutdown()
	_, err := client.InvokeSync(addr, request(ReqHeartBeat, "dead"), 1000)
	if !common.IsKind(err, common.KindConnect) {
		t.Fatalf("got %v", err)
	}
	if client.IsChannelWritable(addr) {
		t.Fatal("失败后不该有可写连接")
	}
}

func TestBadPortIsReported(t *testing.T) {
	client := NewRemotingClient()
	defer client.Shutdown()
	for _, addr := range []string{"127.0.0.1:notaport", "127.0.0.1:", ":9876"} {
		_, err := client.InvokeSync(addr, request(ReqHeartBeat, "x"), 500)
		if !common.IsKind(err, common.KindSendRequest) {
			t.Fatalf("addr %q: got %v", addr, err)
		}
	}
}

func TestCloseChannelForcesReconnect(t *testing.T) {
	addr := echoServer(t)
	client := NewRemotingClient()
	defer client.Shutdown()
	if _, err := client.InvokeSync(addr, request(ReqSendMessage, "first"), 3000); err != nil {
		t.Fatal(err)
	}
	client.CloseChannel(addr)
	if client.IsChannelWritable(addr) {
		t.Fatal("close 后连接应不可写")
	}
	response, err := client.InvokeSync(addr, request(ReqSendMessage, "second"), 3000)
	if err != nil {
		t.Fatal(err)
	}
	if response.Remark != "pong:second" {
		t.Fatalf("remark = %q", response.Remark)
	}
}

// ---------------- fail-fast on EOF (Java failFast) ----------------
//
// Connection death must fail its in-flight requests immediately as send
// failures (Java RemotingSendRequestException), not timeouts — the async send
// retry classifies by error kind, so the semantics matter as much as the
// timing.

func TestConnectionEofFailsInFlightRequestFast(t *testing.T) {
	addr, conns := eofServer(t, math.MaxInt64)
	client := NewRemotingClient()
	defer client.Shutdown()
	started := time.Now()
	_, err := client.InvokeSync(addr, request(ReqSendMessageV2, "eof"), 30000)
	cost := time.Since(started)
	if !common.IsKind(err, common.KindSendRequest) {
		t.Fatalf("必须是发送失败, got %v", err)
	}
	if !strings.Contains(err.Error(), "connection closed") {
		t.Fatalf("文案对齐 Java requestFail: %v", err)
	}
	if cost >= 5*time.Second {
		t.Fatalf("毫秒级判死, 不能等满 30s 超时: %v", cost)
	}
	if client.InFlightCount() != 0 {
		t.Fatalf("判死之后在途表要清空, got %d", client.InFlightCount())
	}
	if conns.Load() != 1 {
		t.Fatalf("conns = %d", conns.Load())
	}
}

func TestConnectionEofFailsAsyncCallbackExactlyOnce(t *testing.T) {
	addr, _ := eofServer(t, math.MaxInt64)
	client := NewRemotingClient()
	defer client.Shutdown()
	hits := make(chan struct{}, 4)
	results := make(chan error, 1)
	client.InvokeAsync(addr, request(ReqSendMessageV2, "eof"), func(_ *RemotingCommand, err error) {
		hits <- struct{}{}
		results <- err
	}, 30000)
	select {
	case err := <-results:
		if !common.IsKind(err, common.KindSendRequest) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("回调没触发")
	}
	if len(hits) != 1 {
		t.Fatalf("回调只能投递一次, got %d", len(hits))
	}
}

// A dead connection's teardown must not doom requests on other connections.
func TestFailFastLeavesOtherConnectionsAlone(t *testing.T) {
	aliveAddr, frames := silentServer(t)
	doomedAddr, _ := eofServer(t, math.MaxInt64)
	client := NewRemotingClient()
	defer client.Shutdown()
	slowResult := make(chan error, 1)
	go func() {
		_, err := client.InvokeSync(aliveAddr, request(ReqPullMessage, "slow"), 30000)
		slowResult <- err
	}()
	waitUntil(t, func() bool { return frames.Load() >= 1 }, "活连接收到请求")
	_, err := client.InvokeSync(doomedAddr, request(ReqSendMessageV2, "eof"), 30000)
	if !common.IsKind(err, common.KindSendRequest) {
		t.Fatalf("got %v", err)
	}
	if client.InFlightCount() != 1 {
		t.Fatalf("只该判死自己那条连接上的请求, in-flight = %d", client.InFlightCount())
	}
	select {
	case <-slowResult:
		t.Fatal("活连接上的请求不该被误伤")
	default:
	}
}

// After fail-fast the same address must dial a fresh connection and finish a
// new request: the dead entry must be gone from the table.
func TestSameAddressWorksAfterFailFast(t *testing.T) {
	addr, conns := eofServer(t, 1)
	client := NewRemotingClient()
	defer client.Shutdown()
	_, err := client.InvokeSync(addr, request(ReqSendMessageV2, "eof"), 30000)
	if !common.IsKind(err, common.KindSendRequest) {
		t.Fatalf("got %v", err)
	}
	if client.IsChannelWritable(addr) {
		t.Fatal("死连接要摘出连接表")
	}
	response, err := client.InvokeSync(addr, request(ReqSendMessageV2, "ok"), 5000)
	if err != nil {
		t.Fatal(err)
	}
	if response.Code != RespSuccess {
		t.Fatalf("code = %d", response.Code)
	}
	if conns.Load() != 2 {
		t.Fatalf("第二次请求走的是新建的连接, conns = %d", conns.Load())
	}
}

// Shutdown must settle in-flight requests too: waiters get a send failure,
// never an eternal hang.
func TestShutdownDrainsInFlightRequests(t *testing.T) {
	addr, frames := silentServer(t)
	client := NewRemotingClient()
	result := make(chan error, 1)
	go func() {
		_, err := client.InvokeSync(addr, request(ReqPullMessage, "pending"), 30000)
		result <- err
	}()
	waitUntil(t, func() bool { return frames.Load() >= 1 }, "请求写出")
	client.Shutdown()
	select {
	case err := <-result:
		if !common.IsKind(err, common.KindSendRequest) {
			t.Fatalf("shutdown 要立刻收口, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("调用方被挂住")
	}
	if client.InFlightCount() != 0 {
		t.Fatalf("in-flight = %d", client.InFlightCount())
	}
}

// ---------------- processor dispatch / hooks ----------------

func TestBrokerPushedRequestReachesProcessor(t *testing.T) {
	listener := listenLocal(t)
	addr := listener.Addr().String()
	responseCh := make(chan *RemotingCommand, 1)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		push := CreateRequestCommand(ReqPushReplyMessageToClient, nil)
		push.Opaque = 9876
		push.MarkOnewayRPC()
		writeFrame(conn, push, 0)
		// oneway 不应有响应；再发一条请求型的，要求回包
		needReply := CreateRequestCommand(ReqCheckTransactionState, nil)
		needReply.Opaque = 5555
		writeFrame(conn, needReply, 0)
		for {
			frame, ok := readFrame(conn)
			if !ok {
				return
			}
			cmd, err := Decode(frame)
			if err != nil {
				return
			}
			if cmd.IsResponseType() {
				responseCh <- cmd
				return
			}
		}
	}()
	echo := func(req *RemotingCommand, _ string, sink *ResponseSink) {
		sink.Respond(CreateResponseCommand(RespSuccess, ""))
	}
	client := NewRemotingClient()
	defer client.Shutdown()
	client.RegisterProcessor(ReqPushReplyMessageToClient, echo)
	client.RegisterProcessor(ReqCheckTransactionState, echo)
	// 先建连（服务端 accept 后才会推请求）
	warm := request(ReqHeartBeat, "warm")
	if err := client.InvokeOneway(addr, warm); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-responseCh:
		if response.Code != RespSuccess {
			t.Fatalf("code = %d", response.Code)
		}
		if response.Opaque != 5555 {
			t.Fatalf("响应必须回填 broker 请求的 opaque, got %d", response.Opaque)
		}
		if !response.IsResponseType() {
			t.Fatal("必须是响应帧")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("客户端应回包")
	}
	<-serverDone
}

type markingHook struct{}

func (h *markingHook) DoBeforeRequest(remoteAddr string, request *RemotingCommand) {
	request.AddExtField("Hooked", "yes")
}

func (h *markingHook) DoAfterResponse(remoteAddr string, request, response *RemotingCommand) {}

func TestRpcHooksRunBeforeEncode(t *testing.T) {
	listener := listenLocal(t)
	addr := listener.Addr().String()
	wireCh := make(chan *RemotingCommand, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		frame, ok := readFrame(conn)
		if !ok {
			return
		}
		cmd, err := Decode(frame)
		if err != nil {
			return
		}
		writeFrame(conn, responseFor(cmd, RespSuccess), 0)
		wireCh <- cmd
	}()
	client := NewRemotingClient()
	defer client.Shutdown()
	client.RegisterRPCHook(&markingHook{})
	if client.RpcHookCount() != 1 {
		t.Fatalf("hook count = %d", client.RpcHookCount())
	}
	if _, err := client.InvokeSync(addr, request(ReqSendMessage, "hook"), 3000); err != nil {
		t.Fatal(err)
	}
	onWire := <-wireCh
	if v, _ := onWire.GetExtField("Hooked"); v != "yes" {
		t.Fatalf("钩子必须作用于上线报文, Hooked = %q", v)
	}
}

func TestRocketMQSerializeTypeRoundTrips(t *testing.T) {
	addr := echoServer(t)
	client := NewRemotingClient()
	defer client.Shutdown()
	cmd := request(ReqSendMessage, "binary")
	cmd.SerializeTypeCurrentRPC = SerializeTypeRocketMQ
	response, err := client.InvokeSync(addr, cmd, 3000)
	if err != nil {
		t.Fatal(err)
	}
	if response.SerializeTypeCurrentRPC != SerializeTypeRocketMQ {
		t.Fatalf("私有二进制协议位必须在高 8 位往返, got %d", response.SerializeTypeCurrentRPC)
	}
}

func TestFrameLengthGuard(t *testing.T) {
	if MaxFrameLength != 16*1024*1024 {
		t.Fatalf("MaxFrameLength = %d", MaxFrameLength)
	}
}

// ---------------- TLS ----------------
//
// The Go port needs no openssl: a self-signed certificate is generated in
// process. The suite keeps the memory-note discipline — a permissive TLS
// round trip is never enough on its own, so the plaintext-rejection control
// is part of the net.

func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// TLS-only server: TLS handshake on every accept, then echo. chunk > 0
// splits responses into small pieces to exercise the reader's buffering.
func tlsEchoServer(t *testing.T, chunk int) string {
	t.Helper()
	cert := selfSignedCert(t)
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	listener := listenLocal(t)
	go func() {
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			go func(raw net.Conn) {
				defer raw.Close()
				conn := tls.Server(raw, tlsCfg)
				defer conn.Close()
				if err := conn.Handshake(); err != nil {
					return
				}
				serveEcho(conn, chunk)
			}(raw)
		}
	}()
	return listener.Addr().String()
}

func tlsTestClient(t *testing.T) *RemotingClient {
	t.Helper()
	config := NewClientConfig()
	config.TLSEnable = true
	config.TLSTestMode = true
	client := NewRemotingClientWithConfig(config)
	t.Cleanup(client.Shutdown)
	return client
}

func TestTlsEchoRoundTrip(t *testing.T) {
	addr := tlsEchoServer(t, 7)
	client := tlsTestClient(t)
	cmd := request(ReqSendMessageV2, "tls")
	response, err := client.InvokeSync(addr, cmd, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if response.Code != RespSuccess || response.Remark != "pong:tls" {
		t.Fatalf("code = %d, remark = %q", response.Code, response.Remark)
	}
}

func TestTlsConcurrentRequests(t *testing.T) {
	addr := tlsEchoServer(t, 0)
	client := tlsTestClient(t)
	const n = 4
	remarks := make(chan string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := request(ReqSendMessage, "tls"+string(rune('0'+i)))
			response, err := client.InvokeSync(addr, cmd, 5000)
			if err != nil {
				t.Error(err)
				return
			}
			remarks <- response.Remark
		}(i)
	}
	wg.Wait()
	close(remarks)
	for r := range remarks {
		if !strings.HasPrefix(r, "pong:tls") {
			t.Fatalf("remark = %q", r)
		}
	}
}

// Plaintext-rejection control: a non-TLS client against the TLS-only server
// must fail (the server drops the handshake), never look like a silent
// success.
func TestTlsPlaintextRejected(t *testing.T) {
	addr := tlsEchoServer(t, 0)
	client := NewRemotingClient()
	defer client.Shutdown()
	_, err := client.InvokeSync(addr, request(ReqHeartBeat, "plain"), 3000)
	if !common.IsKind(err, common.KindSendRequest) {
		t.Fatalf("明文打 TLS 服务端必须失败, got %v", err)
	}
}
