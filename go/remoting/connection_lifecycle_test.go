// Transport contract tests, all offline against in-process fake peers:
// an undecodable frame takes its connection down, an unmatched response only
// warns, the writability predicate, the idle-connection sweep, the per-entry
// expiry warning and the close-on-sync-timeout knob.
package remoting

import (
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// ---------------- log capture ----------------

// logCapture collects the lines the client logs while it is installed as the
// sink. Every assertion filters by the address its own fake peer owns, so lines
// from other connections cannot leak into a count.
type logCapture struct {
	mu    sync.Mutex
	lines []string
	tail  string
}

func (l *logCapture) Write(p []byte) (int, error) {
	text := l.tail + string(p)
	parts := strings.Split(text, "\n")
	l.tail = parts[len(parts)-1]
	complete := parts[:len(parts)-1]
	l.mu.Lock()
	l.lines = append(l.lines, complete...)
	l.mu.Unlock()
	return len(p), nil
}

// matching returns every captured line holding all the given fragments.
func (l *logCapture) matching(parts ...string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, line := range l.lines {
		all := true
		for _, part := range parts {
			if !strings.Contains(line, part) {
				all = false
				break
			}
		}
		if all {
			out = append(out, line)
		}
	}
	return out
}

func captureLogs(t *testing.T) *logCapture {
	t.Helper()
	capture := &logCapture{}
	t.Cleanup(common.SetLogSink(capture))
	return capture
}

func hasAddr(addrs []string, want string) bool {
	for _, addr := range addrs {
		if addr == want {
			return true
		}
	}
	return false
}

// ---------------- fake peers ----------------

// corruptHeader keeps a frame's own length words intact and replaces its header
// payload with bytes that are not valid utf8, so the frame reaches the decoder
// and the decoder rejects it.
func corruptHeader(frame []byte) []byte {
	headerLen := int(binary.BigEndian.Uint32(frame[4:8]) & 0x00FF_FFFF)
	if headerLen == 0 || 8+headerLen > len(frame) {
		panic("fixture frame has no header payload to corrupt")
	}
	for i := 8; i < 8+headerLen; i++ {
		frame[i] = 0xFF
	}
	return frame
}

// poisonPeer answers the first request of every connection with a frame whose
// header cannot be decoded, then parks until the client hangs up. It never
// closes the socket itself, so only the client's own decode failure can end it.
func poisonPeer(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	listener := listenLocal(t)
	var conns atomic.Int64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conns.Add(1)
			go func(conn net.Conn) {
				defer conn.Close()
				frame, ok := readFrame(conn)
				if !ok {
					return
				}
				cmd, err := Decode(frame)
				if err != nil {
					return
				}
				_, _ = conn.Write(corruptHeader(responseFor(cmd, RespSuccess).Encode()))
				// Park in read: returns as soon as the client closes.
				_, _ = conn.Read(make([]byte, 1))
			}(conn)
		}
	}()
	return listener.Addr().String(), &conns
}

// unmatchedPeer answers each request with a response whose opaque no pending
// call registered, and additionally pushes one request-type frame carrying that
// same unknown opaque. While echoMode is on it answers normally instead, so the
// test can prove the connection survived and is still usable.
func unmatchedPeer(t *testing.T, echoMode *atomic.Bool) (string, *atomic.Int64) {
	t.Helper()
	listener := listenLocal(t)
	var conns atomic.Int64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conns.Add(1)
			go func(conn net.Conn) {
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
					reply := responseFor(cmd, RespSuccess)
					if cmd.Remark != "" {
						reply.Remark = "pong:" + cmd.Remark
					}
					if echoMode.Load() {
						writeFrame(conn, reply, 0)
						continue
					}
					unknown := cmd.Opaque + 1_000_000
					reply.Opaque = unknown
					writeFrame(conn, reply, 0)
					push := CreateRequestCommand(ReqCheckTransactionState, nil)
					push.Opaque = unknown
					writeFrame(conn, push, 0)
				}
			}(conn)
		}
	}()
	return listener.Addr().String(), &conns
}

// stallingPeer accepts connections and reads nothing until the test closes the
// drain channel; the client's own socket buffers then fill and its write parks
// in the kernel.
func stallingPeer(t *testing.T, drain <-chan struct{}) string {
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
				<-drain
				_, _ = io.Copy(io.Discard, conn)
			}(conn)
		}
	}()
	return listener.Addr().String()
}

// ---------------- T1: undecodable frame ----------------

func TestUndecodableFrameLogsErrorAndClosesConnection(t *testing.T) {
	poisonAddr, poisonConns := poisonPeer(t)
	healthyAddr := echoServer(t)
	client := NewRemotingClient()
	defer client.Shutdown()
	logs := captureLogs(t)

	started := time.Now()
	_, err := client.InvokeSync(poisonAddr, request(ReqSendMessageV2, "poison"), 30000)
	cost := time.Since(started)
	if !common.IsKind(err, common.KindSendRequest) {
		t.Fatalf("the request riding the broken connection must fail as a send failure, got %v", err)
	}
	if cost > 5*time.Second {
		t.Fatalf("the request must fail fast, took %v", cost)
	}
	// ERROR level, naming the peer address, and the closure it caused.
	if got := logs.matching("ERROR", "undecodable frame", poisonAddr); len(got) != 1 {
		t.Fatalf("want one ERROR naming %s, got %q", poisonAddr, got)
	}
	if got := logs.matching("failed to decode frame"); len(got) != 0 {
		t.Fatalf("the warning-only line must be gone, got %q", got)
	}
	if hasAddr(client.ConnectionAddrs(), poisonAddr) {
		t.Fatalf("the connection must be dropped from the table, got %v", client.ConnectionAddrs())
	}
	if client.InFlightCount() != 0 {
		t.Fatalf("in-flight table must be cleared, got %d", client.InFlightCount())
	}
	// The other connection is untouched and still serves a round trip.
	response, err := client.InvokeSync(healthyAddr, request(ReqSendMessage, "healthy"), 5000)
	if err != nil {
		t.Fatalf("a second connection must keep serving: %v", err)
	}
	if response.Remark != "pong:healthy" {
		t.Fatalf("remark = %q", response.Remark)
	}
	if got := logs.matching("ERROR", healthyAddr); len(got) != 0 {
		t.Fatalf("the healthy address must not appear in any error line, got %q", got)
	}
	if poisonConns.Load() != 1 {
		t.Fatalf("the client must not quietly dial a replacement onto the same broken peer, conns = %d", poisonConns.Load())
	}
}

func TestBadFrameLengthTakesConnectionDownToo(t *testing.T) {
	// The length-guard branch and the decode branch reach the same teardown: the
	// read loop unwinds, the table entry goes and the request fails fast.
	listener := listenLocal(t)
	addr := listener.Addr().String()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, ok := readFrame(conn); !ok {
			return
		}
		// A length beyond the protocol cap.
		var bad [8]byte
		binary.BigEndian.PutUint32(bad[:4], uint32(MaxFrameLength+1))
		_, _ = conn.Write(bad[:])
		_, _ = conn.Read(make([]byte, 1))
	}()
	client := NewRemotingClient()
	defer client.Shutdown()
	_, err := client.InvokeSync(addr, request(ReqSendMessageV2, "bad-length"), 30000)
	if !common.IsKind(err, common.KindSendRequest) {
		t.Fatalf("got %v", err)
	}
	if len(client.ConnectionAddrs()) != 0 {
		t.Fatalf("bad-length connection must be gone, got %v", client.ConnectionAddrs())
	}
}

// timeoutPeer accepts, reads requests and never replies; it sends one token on
// the returned channel as soon as one of its connections sees the client hang up.
func timeoutPeer(t *testing.T) (string, *atomic.Int64, chan struct{}) {
	t.Helper()
	listener := listenLocal(t)
	var frames atomic.Int64
	closed := make(chan struct{}, 8)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				for {
					if _, ok := readFrame(conn); !ok {
						select {
						case closed <- struct{}{}:
						default:
						}
						return
					}
					frames.Add(1)
				}
			}(conn)
		}
	}()
	return listener.Addr().String(), &frames, closed
}

// ---------------- T2: unmatched response ----------------

func TestUnknownOpaqueResponseWarnsAndKeepsConnection(t *testing.T) {
	echoMode := &atomic.Bool{}
	addr, conns := unmatchedPeer(t, echoMode)
	client := NewRemotingClient()
	defer client.Shutdown()
	logs := captureLogs(t)

	// Oneway keeps this test out of the sync-timeout path entirely: the only
	// thing that can happen to the connection is what the unmatched response
	// does (or does not do) to it.
	if err := client.InvokeOneway(addr, request(ReqHeartBeat, "unmatched")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool {
		return len(logs.matching("response for unknown opaque", addr)) == 1 &&
			len(logs.matching("no processor for request code", addr)) == 1
	}, "the unmatched-response WARN and the request-code WARN")
	line := logs.matching("response for unknown opaque", addr)[0]
	if !strings.Contains(line, "WARN") {
		t.Fatalf("the unmatched response is a warning, not an error: %q", line)
	}
	// The request-type frame the same peer pushed carries the same unknown
	// opaque and must be routed as a request, not warned as a stray response.
	if got := logs.matching("no processor for request code", addr); len(got) != 1 {
		t.Fatalf("want the pushed request warned once as a request, got %q", got)
	}
	if got := logs.matching("response for unknown opaque", addr); len(got) != 1 {
		t.Fatalf("the request frame must not add a second unmatched-response line, got %q", got)
	}
	// Nothing closed: same connection, same single accept, and a normal round trip
	// still completes on it.
	if !hasAddr(client.ConnectionAddrs(), addr) {
		t.Fatalf("connection table = %v, want %s still open", client.ConnectionAddrs(), addr)
	}
	echoMode.Store(true)
	response, err := client.InvokeSync(addr, request(ReqSendMessage, "after-unknown"), 5000)
	if err != nil {
		t.Fatalf("the connection must still serve: %v", err)
	}
	if response.Remark != "pong:after-unknown" {
		t.Fatalf("remark = %q", response.Remark)
	}
	if conns.Load() != 1 {
		t.Fatalf("conns = %d, want the one connection kept", conns.Load())
	}
}

// ---------------- T3: writability ----------------

func TestIsChannelWritableTrueWithoutConnectionAndForDrainedOne(t *testing.T) {
	addr := echoServer(t)
	client := NewRemotingClient()
	defer client.Shutdown()
	// Nothing has been dialed yet: no connection is not a blocked connection.
	if !client.IsChannelWritable(addr) {
		t.Fatal("an address with no connection at all must report writable")
	}
	if _, err := client.InvokeSync(addr, request(ReqSendMessage, "warm"), 3000); err != nil {
		t.Fatal(err)
	}
	// Live and drained.
	if !client.IsChannelWritable(addr) {
		t.Fatal("a live connection with nothing parked must report writable")
	}
	client.CloseChannel(addr)
	if !client.IsChannelWritable(addr) {
		t.Fatal("a closed connection is gone from the table, so the address is writable again")
	}
}

func TestIsChannelWritableFalseWhileWriteIsParked(t *testing.T) {
	drain := make(chan struct{})
	var once sync.Once
	drop := func() { once.Do(func() { close(drain) }) }
	t.Cleanup(drop)
	addr := stallingPeer(t, drain)
	client := NewRemotingClient()
	defer client.Shutdown()

	// Establish the connection, then shrink its send buffer so the big frame
	// below has nowhere to go while the peer reads nothing.
	if err := client.InvokeOneway(addr, request(ReqHeartBeat, "small")); err != nil {
		t.Fatal(err)
	}
	conn := client.existingConn(addr)
	if conn == nil {
		t.Fatal("the oneway must have left a live connection")
	}
	if tcp, ok := conn.conn.(*net.TCPConn); ok {
		_ = tcp.SetWriteBuffer(4096)
	}
	if !client.IsChannelWritable(addr) {
		t.Fatal("before the parked write the connection must report writable")
	}

	big := request(ReqSendMessage, "parked")
	big.SetBody(make([]byte, 12*1024*1024))
	written := make(chan error, 1)
	go func() { written <- client.InvokeOneway(addr, big) }()

	// The write is stuck in the kernel: this is the one state this port can
	// honestly observe as "cannot take more data".
	waitUntil(t, func() bool { return !client.IsChannelWritable(addr) }, "the saturated connection to report unwritable")
	if conn.parkedWriteNano.Load() == 0 {
		t.Fatal("the predicate must go false because a write is parked, not for any other reason")
	}

	drop()
	select {
	case err := <-written:
		if err != nil {
			t.Fatalf("the parked write must complete once the peer drains: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the parked write never completed")
	}
	if !client.IsChannelWritable(addr) {
		t.Fatal("after the write drained the connection must be writable again")
	}
}

// ---------------- T4: idle connection sweep ----------------

func TestIdleSweepClosesIdleConnectionAndKeepsBusyOne(t *testing.T) {
	idleAddr := echoServer(t)
	busyAddr, _ := silentServer(t)
	config := NewClientConfig()
	config.ChannelIdleTimeoutSeconds = 1
	client := NewRemotingClientWithConfig(config)
	defer client.Shutdown()
	logs := captureLogs(t)

	if _, err := client.InvokeSync(idleAddr, request(ReqSendMessage, "prime"), 3000); err != nil {
		t.Fatal(err)
	}
	// The busy connection is poked with a small write well inside the idle limit.
	stopPokes := make(chan struct{})
	pokesDone := make(chan struct{})
	go func() {
		defer close(pokesDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopPokes:
				return
			case <-ticker.C:
				_ = client.InvokeOneway(busyAddr, request(ReqHeartBeat, "poke"))
			}
		}
	}()
	// The first poke must land before the sweep can judge the connection.
	waitUntil(t, func() bool { return hasAddr(client.ConnectionAddrs(), busyAddr) }, "the busy connection established")

	waitUntil(t, func() bool { return !hasAddr(client.ConnectionAddrs(), idleAddr) }, "the idle connection closed by the sweep")
	warned := logs.matching("idle for", idleAddr)
	if len(warned) != 1 {
		t.Fatalf("want exactly one idle WARN naming %s, got %q", idleAddr, warned)
	}
	if !strings.Contains(warned[0], "WARN") {
		t.Fatalf("idle close is a warning: %q", warned[0])
	}
	if client.InFlightCount() != 0 {
		t.Fatalf("in-flight = %d", client.InFlightCount())
	}
	// Keep watching past another idle window: the written-to connection is never
	// idle past the limit, so the sweep leaves it alone.
	time.Sleep(700 * time.Millisecond)
	if !hasAddr(client.ConnectionAddrs(), busyAddr) {
		t.Fatalf("the connection with writes at a shorter cadence must survive the sweep, table = %v", client.ConnectionAddrs())
	}
	if got := logs.matching("idle for", busyAddr); len(got) != 0 {
		t.Fatalf("the busy connection must never be warned about, got %q", got)
	}
	close(stopPokes)
	<-pokesDone
}

func TestIdleTimeoutConfigDefaultsAndNeverDisables(t *testing.T) {
	// The default is 120 s, and a non-positive value falls back to it: there is
	// no "0 disables the sweep" reading.
	if got := NewClientConfig().ChannelIdleTimeoutSeconds; got != 120 {
		t.Fatalf("default idle limit = %d, want 120", got)
	}
	for _, seconds := range []int64{120, 0, -1, 30} {
		config := NewClientConfig()
		config.ChannelIdleTimeoutSeconds = seconds
		client := NewRemotingClientWithConfig(config)
		want := time.Duration(seconds) * time.Second
		if seconds <= 0 {
			want = time.Duration(DefaultChannelIdleTimeoutSeconds) * time.Second
		}
		if got := client.channelIdleTimeout(); got != want {
			t.Errorf("ChannelIdleTimeoutSeconds = %d resolved to %s, want %s", seconds, got, want)
		}
		client.Shutdown()
	}
}

// ---------------- T5: expired pending entry ----------------

func TestExpiredPendingEntryWarnsExactlyOnceEach(t *testing.T) {
	addr, _ := silentServer(t)
	config := NewClientConfig()
	// The teardown is not what is under test here, so the timeout close is off and
	// both requests expire on their own.
	config.CloseChannelOnSyncTimeout = false
	client := NewRemotingClientWithConfig(config)
	defer client.Shutdown()
	logs := captureLogs(t)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := client.InvokeSync(addr, request(ReqPullMessage, "expire"+string(rune('0'+i))), 200)
			if !common.IsKind(err, common.KindTimeout) {
				t.Errorf("got %v, want the timeout error", err)
			}
		}(i)
	}
	wg.Wait()
	expired := logs.matching("expired after", addr)
	if len(expired) != 2 {
		t.Fatalf("want one WARN per expired entry, got %q", expired)
	}
	for _, line := range expired {
		if !strings.Contains(line, "WARN") {
			t.Errorf("expiry is a warning: %q", line)
		}
		if !strings.Contains(line, "opaque") || !strings.Contains(line, "200 ms") {
			t.Errorf("the line must name the opaque and the spent timeout: %q", line)
		}
	}
	if client.InFlightCount() != 0 {
		t.Fatalf("in-flight = %d", client.InFlightCount())
	}
	// A response that arrives first leaves nothing to expire, so no extra line.
	echoAddr := echoServer(t)
	if _, err := client.InvokeSync(echoAddr, request(ReqSendMessage, "answered"), 3000); err != nil {
		t.Fatal(err)
	}
	if got := logs.matching("expired after", echoAddr); len(got) != 0 {
		t.Fatalf("a completed request must not be warned about, got %q", got)
	}
}

// ---------------- T6: close on sync timeout ----------------

func TestShouldCloseOnSyncTimeoutRule(t *testing.T) {
	client := NewRemotingClient()
	defer client.Shutdown()
	cases := []struct {
		left    int64
		timeout int64
		closes  bool
		why     string
	}{
		{3000, 3000, true, "a full budget spent without an answer"},
		{120, 120, true, "above the 100 ms floor"},
		{100, 400, false, "at the floor and a quarter of the timeout"},
		{50, 300, false, "below the floor and below a quarter"},
		{101, 400, true, "one millisecond over the floor"},
		{100, 300, true, "over the quarter of the timeout"},
	}
	for _, c := range cases {
		if got := client.shouldCloseOnSyncTimeout(c.left, c.timeout); got != c.closes {
			t.Errorf("left=%d timeout=%d: closes = %v, want %v (%s)", c.left, c.timeout, got, c.closes, c.why)
		}
	}
	// With the knob off the rule never closes.
	off := NewClientConfig()
	off.CloseChannelOnSyncTimeout = false
	offClient := NewRemotingClientWithConfig(off)
	defer offClient.Shutdown()
	if offClient.shouldCloseOnSyncTimeout(3000, 3000) {
		t.Error("the knob must switch the close off")
	}
}

func TestSyncTimeoutClosesConnectionByDefault(t *testing.T) {
	addr, _, hungUp := timeoutPeer(t)
	client := NewRemotingClient()
	defer client.Shutdown()
	if !client.Config().CloseChannelOnSyncTimeout {
		t.Fatal("the knob must default to true")
	}
	logs := captureLogs(t)

	_, err := client.InvokeSync(addr, request(ReqPullMessage, "slow"), 300)
	if !common.IsKind(err, common.KindTimeout) {
		t.Fatalf("the caller must still get the timeout error, got %v", err)
	}
	if len(client.ConnectionAddrs()) != 0 {
		t.Fatalf("a timed-out sync invoke must drop the connection, table = %v", client.ConnectionAddrs())
	}
	if got := logs.matching("closing the connection", addr); len(got) != 1 {
		t.Fatalf("want one WARN about the close naming %s, got %q", addr, got)
	}
	if got := logs.matching("expired after", addr); len(got) != 1 {
		t.Fatalf("want the expired-entry WARN too, got %q", got)
	}
	// The close really reached the wire: the peer's read ended.
	select {
	case <-hungUp:
	case <-time.After(5 * time.Second):
		t.Fatal("the peer never saw the socket close")
	}
	// No usable connection is left for that address, so the next call to a
	// serving peer still works on a fresh socket.
	echoAddr := echoServer(t)
	if _, err := client.InvokeSync(echoAddr, request(ReqSendMessage, "fresh"), 3000); err != nil {
		t.Fatal(err)
	}
}

func TestSyncTimeoutKeepsConnectionWhenKnobOff(t *testing.T) {
	addr, _, hungUp := timeoutPeer(t)
	config := NewClientConfig()
	config.CloseChannelOnSyncTimeout = false
	client := NewRemotingClientWithConfig(config)
	defer client.Shutdown()
	logs := captureLogs(t)

	_, err := client.InvokeSync(addr, request(ReqPullMessage, "slow"), 300)
	if !common.IsKind(err, common.KindTimeout) {
		t.Fatalf("the caller must still get the timeout error, got %v", err)
	}
	if !hasAddr(client.ConnectionAddrs(), addr) {
		t.Fatalf("with the knob off the connection must be kept, table = %v", client.ConnectionAddrs())
	}
	if got := logs.matching("closing the connection"); len(got) != 0 {
		t.Fatalf("no close may be warned about, got %q", got)
	}
	if got := logs.matching("expired after", addr); len(got) != 1 {
		t.Fatalf("the expiry WARN is independent of the knob, got %q", got)
	}
	conn := client.existingConn(addr)
	if conn == nil {
		t.Fatal("the live connection must still be there")
	}
	if !conn.alive.Load() {
		t.Fatal("the kept connection must still be alive")
	}
	select {
	case <-hungUp:
		t.Fatal("the peer must not have been hung up on")
	case <-time.After(300 * time.Millisecond):
	}
}
