// Long-lived remoting client.
//
// One goroutine reads each connection; writes go straight through the socket
// under a per-connection mutex. In-flight requests are keyed by opaque; when a
// connection dies, every request written onto it fails fast ("connection
// closed") instead of waiting out its timeout. GO_AWAY(1500) responses trigger
// one reconnect-and-retry with a fresh opaque and the remaining time budget.
// A connection that served neither a read nor a write for the configured idle
// limit (120 s by default) is closed by the sweep goroutine, and a sync invoke
// whose own wait expired closes the connection it was carried on unless the
// caller opted out of that knob.
package remoting

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// MaxFrameLength caps one inbound frame (16 MiB, the protocol limit).
const MaxFrameLength = int32(16 * 1024 * 1024)

// DefaultChannelIdleTimeoutSeconds is the idle limit a connection is allowed to
// sit unused for before the sweep closes it (120 s). A non-positive configured
// value falls back to it — there is no "idle close disabled" setting.
const DefaultChannelIdleTimeoutSeconds int64 = 120

// writeParkedAfter is how long a write may stay inside the kernel before the
// connection counts as not writable. A healthy write returns immediately; one
// that has been parked this long means the socket send buffer is full and the
// peer is not draining, so the connection genuinely cannot take more data.
const writeParkedAfter = 100 * time.Millisecond

// closeOnSyncTimeoutFloorMillis is the budget boundary of the
// CloseChannelOnSyncTimeout rule: a wait that began with no more than this many
// milliseconds left (and no more than a quarter of its timeout) is not punished
// with a reconnect, because the caller never had a real chance to be answered.
const closeOnSyncTimeoutFloorMillis int64 = 100

// RPCHook is a request/response interceptor. DoBeforeRequest must
// run before encode so the signed bytes are the ones that hit the wire.
type RPCHook interface {
	DoBeforeRequest(remoteAddr string, request *RemotingCommand)
	DoAfterResponse(remoteAddr string, request *RemotingCommand, response *RemotingCommand)
}

// ProcessorFunc handles a request pushed by the peer. Call ResponseSink.Respond when a reply is expected;
// oneway requests get no reply. Do not block for long — the read loop of the
// connection runs processors inline.
type ProcessorFunc func(request *RemotingCommand, addr string, sink *ResponseSink)

// ResponseSink writes a reply back to the requester. The framework back-fills
// the opaque; this path runs no RPC hooks (responses never carry signatures).
type ResponseSink struct {
	client *RemotingClient
	addr   string
	opaque int32
	// Peer pushed the request with the oneway flag: no reply may go back
	// (the same rule the server applies to oneway pushes).
	wantsReply bool
}

func (s *ResponseSink) Addr() string { return s.addr }

func (s *ResponseSink) Respond(response *RemotingCommand) {
	if !s.wantsReply {
		common.LogDebugf("remoting: drop response (code=%d) for oneway request opaque=%d from %s",
			response.Code, s.opaque, s.addr)
		return
	}
	response.Opaque = s.opaque
	client, addr := s.client, s.addr
	go func() {
		if err := client.respondFrame(addr, response); err != nil {
			common.LogWarnf("remoting: failed to write response (code=%d) to %s: %v",
				response.Code, addr, err)
		}
	}()
}

type ClientConfig struct {
	ConnectTimeoutMillis int64
	InvokeTimeoutMillis  int64
	TLSEnable            bool
	// TLS test mode (tls.test.mode.enable, default true): trust
	// self-signed certs, skip hostname checks.
	TLSTestMode bool
	// On GO_AWAY(1500) retry once over a fresh connection; off -> the
	// SendRequest error surfaces directly (enableReconnectForGoAway = false).
	EnableReconnectForGoAway bool
	SocksProxy               *SocksProxyConfig
	// ChannelIdleTimeoutSeconds: the sweep closes any connection that served
	// neither a read nor a write for this long (default 120; a non-positive
	// value falls back to the default).
	ChannelIdleTimeoutSeconds int64
	// CloseChannelOnSyncTimeout (default true): a sync invoke whose own wait
	// expired drops the connection instead of reusing it, because the request
	// it carried may still be answered on that socket later.
	CloseChannelOnSyncTimeout bool
}

func NewClientConfig() ClientConfig {
	return ClientConfig{
		ConnectTimeoutMillis:      3000,
		InvokeTimeoutMillis:       3000,
		TLSEnable:                 envBool("ROCKETMQ_TLS_ENABLE", false),
		TLSTestMode:               envBool("ROCKETMQ_TLS_TEST_MODE", true),
		EnableReconnectForGoAway:  true,
		ChannelIdleTimeoutSeconds: DefaultChannelIdleTimeoutSeconds,
		CloseChannelOnSyncTimeout: true,
	}
}

// channelIdleTimeout resolves the idle limit, falling back to the default for a
// non-positive configured value.
func (c *RemotingClient) channelIdleTimeout() time.Duration {
	seconds := c.config.ChannelIdleTimeoutSeconds
	if seconds <= 0 {
		seconds = DefaultChannelIdleTimeoutSeconds
	}
	return time.Duration(seconds) * time.Second
}

func envBool(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// pendingCall is one in-flight request. connID is the connection the request
// is actually written onto; address cannot be
// the identity because same-address reconnects (GO_AWAY retry, EOF) are
// routine and would doom new-connection requests.
type pendingCall struct {
	ch chan *RemotingCommand
	// request is kept for DoAfterResponse hooks.
	request *RemotingCommand
	addr    string
	connID  uint64
}

type connection struct {
	id      uint64
	addr    string
	conn    net.Conn
	writeMu sync.Mutex
	alive   atomic.Bool
	// lastActivityNano (unix nanos) is refreshed on every successful read and
	// every completed write; the idle sweep compares it against the configured
	// idle limit.
	lastActivityNano atomic.Int64
	// parkedWriteNano is the unix-ns instant a write began and 0 once it
	// returned; a value older than writeParkedAfter means the socket buffer is
	// full and this connection cannot take more data.
	parkedWriteNano atomic.Int64
}

// touch stamps the connection with "activity just now"; reads and writes both
// call it, so a connection serving either direction is never seen as idle.
func (c *connection) touch() {
	c.lastActivityNano.Store(time.Now().UnixNano())
}

// idleFor reports how long the connection has served no read and no write.
func (c *connection) idleFor(now time.Time) time.Duration {
	last := c.lastActivityNano.Load()
	if last == 0 {
		// Never stamped: treat the connection as just created so it gets a full
		// idle window instead of being closed by the first sweep that sees it.
		c.touch()
		return 0
	}
	return now.Sub(time.Unix(0, last))
}

// writeParked reports whether a write is stuck in the kernel (peer not
// draining, send buffer full) — the only observable "cannot take more data"
// signal a Go socket offers.
func (c *connection) writeParked(now time.Time) bool {
	started := c.parkedWriteNano.Load()
	if started == 0 {
		return false
	}
	return now.Sub(time.Unix(0, started)) >= writeParkedAfter
}

func (c *connection) writeAll(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if !c.alive.Load() {
		return net.ErrClosed
	}
	c.parkedWriteNano.Store(time.Now().UnixNano())
	_, err := c.conn.Write(data)
	c.parkedWriteNano.Store(0)
	if err != nil {
		return err
	}
	c.touch()
	return nil
}

func (c *connection) close() {
	c.alive.Store(false)
	_ = c.conn.Close()
}

// RemotingClient is the long-lived remoting transport, shared by the whole
// client instance.
type RemotingClient struct {
	config  ClientConfig
	running atomic.Bool
	connSeq atomic.Uint64

	mu      sync.Mutex
	conns   map[string]*connection
	gates   map[string]*sync.Mutex
	pending map[int32]*pendingCall

	hooksMu    sync.RWMutex
	hooks      []RPCHook
	procMu     sync.RWMutex
	processors map[int32]ProcessorFunc

	// Idle sweep: one goroutine per running client, started with the client and
	// stopped by Shutdown, so no connection outlives the configured idle limit
	// and no ticker outlives the client.
	sweepMu   sync.Mutex
	sweeping  bool
	sweepStop chan struct{}
}

func NewRemotingClient() *RemotingClient {
	return NewRemotingClientWithConfig(NewClientConfig())
}

func NewRemotingClientWithConfig(config ClientConfig) *RemotingClient {
	c := &RemotingClient{
		config:     config,
		conns:      make(map[string]*connection),
		gates:      make(map[string]*sync.Mutex),
		pending:    make(map[int32]*pendingCall),
		processors: make(map[int32]ProcessorFunc),
	}
	c.running.Store(true)
	c.startIdleSweep()
	return c
}

func (c *RemotingClient) Config() ClientConfig { return c.config }

// ---------------- hooks / processors ----------------

func (c *RemotingClient) RegisterRPCHook(hook RPCHook) {
	c.hooksMu.Lock()
	c.hooks = append(c.hooks, hook)
	c.hooksMu.Unlock()
}

func (c *RemotingClient) UnregisterRPCHook(hook RPCHook) {
	c.hooksMu.Lock()
	for i, h := range c.hooks {
		if h == hook {
			c.hooks = append(c.hooks[:i], c.hooks[i+1:]...)
			break
		}
	}
	c.hooksMu.Unlock()
}

func (c *RemotingClient) RpcHookCount() int {
	c.hooksMu.RLock()
	defer c.hooksMu.RUnlock()
	return len(c.hooks)
}

func (c *RemotingClient) RegisterProcessor(requestCode int32, processor ProcessorFunc) {
	c.procMu.Lock()
	c.processors[requestCode] = processor
	c.procMu.Unlock()
}

func (c *RemotingClient) UnregisterProcessor(requestCode int32) ProcessorFunc {
	c.procMu.Lock()
	defer c.procMu.Unlock()
	proc := c.processors[requestCode]
	delete(c.processors, requestCode)
	return proc
}

// HasProcessor reports whether a broker->client request code has a handler.
//
// A code with NO handler is not an error here, but the read loop logs a WARN and
// sends no reply — so for a code the broker really pushes, silence is
// indistinguishable from a broken client. Tests use this to pin the set.
func (c *RemotingClient) HasProcessor(requestCode int32) bool {
	c.procMu.RLock()
	defer c.procMu.RUnlock()
	_, ok := c.processors[requestCode]
	return ok
}

// ---------------- RPC ----------------

// InvokeSync performs one RPC; timeoutMillis <= 0 falls back to the config
// default. GO_AWAY(1500) is broker/proxy graceful-shutdown signalling for
// in-flight requests: the semantics are "stop using this connection", so the
// retry must run on a NEW connection. Retry exactly once; a second GO_AWAY
// fails with SendRequest to avoid spinning on a dying cluster.
func (c *RemotingClient) InvokeSync(addr string, request *RemotingCommand, timeoutMillis int64) (*RemotingCommand, error) {
	if timeoutMillis <= 0 {
		timeoutMillis = c.config.InvokeTimeoutMillis
	}
	started := time.Now()
	response, err := c.invokeWithTimeout(addr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response.Code != RespGoAway {
		return response, nil
	}
	if !c.config.EnableReconnectForGoAway {
		return nil, common.SendRequestError(addr, fmt.Sprintf("Receive GO_AWAY from channel %s", addr))
	}
	common.LogWarnf("remoting: receive GO_AWAY from %s, reconnect and retry once", addr)
	c.CloseChannel(addr)
	// Retry burns only the remaining budget (one stopwatch across attempts).
	retryTimeout := timeoutMillis - time.Since(started).Milliseconds()
	if retryTimeout < 1 {
		retryTimeout = 1
	}
	retry := request.Clone()
	// Fresh opaque: the original request already got its answer, reusing it
	// would mis-route the next response.
	retry.Opaque = NextOpaque()
	response, err = c.invokeWithTimeout(addr, retry, retryTimeout)
	if err != nil {
		return nil, err
	}
	if response.Code == RespGoAway {
		return nil, common.SendRequestError(addr, fmt.Sprintf("Receive GO_AWAY twice in request from channel %s", addr))
	}
	return response, nil
}

// InvokeAsync runs the same invokeImpl as InvokeSync on a goroutine (so the
// async path gets GO_AWAY reconnect semantics too) and delivers exactly one
// result to the callback.
func (c *RemotingClient) InvokeAsync(addr string, request *RemotingCommand, callback func(*RemotingCommand, error), timeoutMillis int64) {
	go func() {
		response, err := c.InvokeSync(addr, request, timeoutMillis)
		callback(response, err)
	}()
}

// InvokeOneway sends without registering a pending call — nothing can come
// back, so no GO_AWAY retry applies.
func (c *RemotingClient) InvokeOneway(addr string, request *RemotingCommand) error {
	request.MarkOnewayRPC()
	c.runBeforeRequestHooks(addr, request)
	conn, err := c.getOrCreateConn(addr)
	if err != nil {
		return err
	}
	return c.writeConn(conn, request)
}

// ---------------- connection state ----------------

// IsChannelWritable reports whether a write to addr could proceed now.
//
// A missing connection is not a saturated one: nothing is queued against that
// address, and the next call simply dials, so the answer is true. Only a live
// connection whose write has been parked in the kernel because the peer stopped
// draining answers false. No production path in this client calls it — it is a
// diagnostic for the host application.
func (c *RemotingClient) IsChannelWritable(addr string) bool {
	conn := c.existingConn(addr)
	if conn == nil {
		return true
	}
	return !conn.writeParked(time.Now())
}

// CloseChannel drops the current connection to addr and fails fast only the
// requests written onto it.
func (c *RemotingClient) CloseChannel(addr string) {
	c.mu.Lock()
	conn := c.conns[addr]
	c.mu.Unlock()
	if conn == nil {
		return
	}
	c.closeConn(conn)
}

// closeConn tears down one specific connection: the socket is closed, the table
// entry is dropped only while it still points at this connection (a same-address
// reconnect that already happened must survive), and the requests written onto
// it fail fast.
func (c *RemotingClient) closeConn(conn *connection) {
	c.mu.Lock()
	if cur, ok := c.conns[conn.addr]; ok && cur.id == conn.id {
		delete(c.conns, conn.addr)
	}
	c.mu.Unlock()
	conn.close()
	c.failPendingFor(conn.id)
}

func (c *RemotingClient) ConnectionAddrs() []string {
	c.mu.Lock()
	addrs := make([]string, 0, len(c.conns))
	for addr := range c.conns {
		addrs = append(addrs, addr)
	}
	c.mu.Unlock()
	sort.Strings(addrs)
	return addrs
}

func (c *RemotingClient) InFlightCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// Start re-opens a transport closed by Shutdown; connections are built lazily
// anyway, and the idle sweep runs from here on.
func (c *RemotingClient) Start() {
	c.running.Store(true)
	c.startIdleSweep()
}

func (c *RemotingClient) Shutdown() {
	if !c.running.CompareAndSwap(true, false) {
		return
	}
	c.stopIdleSweep()
	for _, addr := range c.ConnectionAddrs() {
		c.CloseChannel(addr)
	}
	// Requests that never got written (connID unset, not yet attached) get
	// the same immediate "connection closed" fate — nobody may hang.
	c.mu.Lock()
	rest := make([]*pendingCall, 0, len(c.pending))
	for opaque, call := range c.pending {
		delete(c.pending, opaque)
		rest = append(rest, call)
	}
	c.mu.Unlock()
	for _, call := range rest {
		select {
		case call.ch <- nil:
		default:
		}
	}
}

// ---------------- idle connection sweep ----------------

// startIdleSweep launches the sweep goroutine unless one is already running. It
// is called with the client (and from Start), and stopIdleSweep shuts it down,
// so the ticker never outlives the transport.
func (c *RemotingClient) startIdleSweep() {
	c.sweepMu.Lock()
	if c.sweeping {
		c.sweepMu.Unlock()
		return
	}
	stop := make(chan struct{})
	c.sweepStop = stop
	c.sweeping = true
	c.sweepMu.Unlock()
	go c.idleSweep(stop)
}

func (c *RemotingClient) stopIdleSweep() {
	c.sweepMu.Lock()
	stop := c.sweepStop
	c.sweepStop = nil
	c.sweeping = false
	c.sweepMu.Unlock()
	if stop != nil {
		close(stop)
	}
}

// idleSweep ticks at a quarter of the idle limit (never faster than 250 ms) and
// closes everything idle past the limit. An idle limit of 120 s is therefore
// acted on within at most 150 s of the last activity.
func (c *RemotingClient) idleSweep(stop <-chan struct{}) {
	interval := c.channelIdleTimeout() / 4
	if interval < 250*time.Millisecond {
		interval = 250 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			c.closeIdleChannels()
		}
	}
}

// closeIdleChannels closes every connection whose last read/write stamp is
// strictly older than the configured idle limit, warning per connection with the
// address it timed out on. Other connections are untouched.
func (c *RemotingClient) closeIdleChannels() {
	if !c.running.Load() {
		return
	}
	limit := c.channelIdleTimeout()
	now := time.Now()
	c.mu.Lock()
	var idle []*connection
	for _, conn := range c.conns {
		if conn.idleFor(now) > limit {
			idle = append(idle, conn)
		}
	}
	c.mu.Unlock()
	for _, conn := range idle {
		common.LogWarnf("remoting: connection to %s (id=%d) idle for %s (limit %s), closing",
			conn.addr, conn.id, conn.idleFor(now), limit)
		c.closeConn(conn)
	}
}

func (c *RemotingClient) runBeforeRequestHooks(addr string, request *RemotingCommand) {
	c.hooksMu.RLock()
	defer c.hooksMu.RUnlock()
	for _, hook := range c.hooks {
		hook.DoBeforeRequest(addr, request)
	}
}

func (c *RemotingClient) applyAfterResponseHooks(addr string, request, response *RemotingCommand) {
	c.hooksMu.RLock()
	hooks := c.hooks
	c.hooksMu.RUnlock()
	for _, hook := range hooks {
		hook.DoAfterResponse(addr, request, response)
	}
}

func (c *RemotingClient) invokeWithTimeout(addr string, request *RemotingCommand, timeoutMillis int64) (*RemotingCommand, error) {
	// Hooks run before encode so the ACL signature covers the extFields that
	// actually go out.
	c.runBeforeRequestHooks(addr, request)
	conn, err := c.getOrCreateConn(addr)
	if err != nil {
		return nil, err
	}
	call := &pendingCall{ch: make(chan *RemotingCommand, 1), request: request, addr: addr, connID: conn.id}
	c.mu.Lock()
	c.pending[request.Opaque] = call
	c.mu.Unlock()
	// Timing starts here, so the write cost is counted and `left` below is the
	// budget still available when the wait itself begins.
	waitBegins := time.Now()
	if err := c.writeConn(conn, request); err != nil {
		return nil, err
	}
	d := time.Duration(timeoutMillis) * time.Millisecond
	if d <= 0 {
		d = time.Millisecond
	}
	// left is the budget still available when the wait begins (the write above
	// already spent part of it); the close-on-timeout rule keys off it.
	left := timeoutMillis - time.Since(waitBegins).Milliseconds()
	if left < 0 {
		left = 0
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case response := <-call.ch:
		if response == nil {
			return nil, common.SendRequestError(addr, "connection closed")
		}
		return response, nil
	case <-timer.C:
		closeChannel := c.shouldCloseOnSyncTimeout(left, timeoutMillis)
		if closeChannel {
			common.LogWarnf("remoting: sync invoke to %s spent %d ms without a response, closing the connection",
				addr, timeoutMillis)
		}
		// The expired entry is dropped before the connection teardown, which
		// would otherwise purge it without the per-request expiry warning.
		c.cancelPending(addr, request.Opaque, timeoutMillis)
		if closeChannel {
			c.closeConn(conn)
		}
		return nil, common.TimeoutError(addr, timeoutMillis)
	}
}

// shouldCloseOnSyncTimeout applies the close-on-timeout rule: the connection
// goes unless the wait began with almost nothing left — 100 ms or less AND a
// quarter of the timeout or less, i.e. a request that never had a real chance to
// be answered.
func (c *RemotingClient) shouldCloseOnSyncTimeout(leftMillis, timeoutMillis int64) bool {
	if !c.config.CloseChannelOnSyncTimeout {
		return false
	}
	if leftMillis <= closeOnSyncTimeoutFloorMillis && leftMillis <= timeoutMillis/4 {
		return false
	}
	return true
}

// writeConn encodes and writes one frame. Write failure closes the channel:
// the read loop would see EOF soon anyway, and failing now lets this caller
// surface "connection closed" as a SendRequest error.
func (c *RemotingClient) writeConn(conn *connection, request *RemotingCommand) error {
	data := request.Encode()
	if err := conn.writeAll(data); err != nil {
		conn.alive.Store(false)
		c.CloseChannel(conn.addr)
		return common.SendRequestError(conn.addr, "connection closed")
	}
	return nil
}

func (c *RemotingClient) respondFrame(addr string, response *RemotingCommand) error {
	conn, err := c.getOrCreateConn(addr)
	if err != nil {
		return err
	}
	return c.writeConn(conn, response)
}

// cancelPending drops one expired in-flight request at its removal site and
// warns there, so an expired entry always produces exactly one line naming the
// address, the opaque and the timeout that ran out. Removing an entry that is
// already gone (the answer arrived first) logs nothing.
func (c *RemotingClient) cancelPending(addr string, opaque int32, timeoutMillis int64) {
	c.mu.Lock()
	_, found := c.pending[opaque]
	delete(c.pending, opaque)
	c.mu.Unlock()
	if found {
		common.LogWarnf("remoting: pending request to %s (opaque %d) expired after %d ms",
			addr, opaque, timeoutMillis)
	}
}

// failPendingFor fails
// every in-flight request written onto the connection that just died, waking
// its waiter with nil -> "connection closed". Claims go by connID: when an
// old read loop unwinds after a same-address reconnect, the requests on the
// new connection must survive. A late response for a failed request then
// lands in dispatch as "response for unknown opaque" — harmless.
func (c *RemotingClient) failPendingFor(connID uint64) {
	var dropped []*pendingCall
	c.mu.Lock()
	for opaque, call := range c.pending {
		if call.connID == connID {
			delete(c.pending, opaque)
			dropped = append(dropped, call)
		}
	}
	c.mu.Unlock()
	if len(dropped) > 0 {
		common.LogWarnf("remoting: connection to %s (id=%d) closed, %d in-flight request(s) failed fast",
			dropped[0].addr, connID, len(dropped))
	}
	for _, call := range dropped {
		select {
		case call.ch <- nil:
		default:
		}
	}
}

// onConnectionLost runs when a read loop unwinds with its own connID. Remove
// the table entry only if it is still this connection — never a replacement —
// but fail this connection's requests regardless, so nobody waits out a
// timeout.
func (c *RemotingClient) onConnectionLost(addr string, connID uint64) {
	c.mu.Lock()
	if cur, ok := c.conns[addr]; ok && cur.id == connID {
		delete(c.conns, addr)
	}
	c.mu.Unlock()
	c.failPendingFor(connID)
}

// ---------------- connections ----------------

func (c *RemotingClient) existingConn(addr string) *connection {
	c.mu.Lock()
	conn := c.conns[addr]
	c.mu.Unlock()
	if conn != nil && conn.alive.Load() {
		return conn
	}
	return nil
}

// getOrCreateConn dials lazily, one connection per address. The per-address
// gate serializes dialers; whoever wins inserts, losers reuse.
func (c *RemotingClient) getOrCreateConn(addr string) (*connection, error) {
	if !c.running.Load() {
		return nil, common.SendRequestError(addr, "client already shutdown")
	}
	if conn := c.existingConn(addr); conn != nil {
		return conn, nil
	}
	gate := c.connectGate(addr)
	gate.Lock()
	defer gate.Unlock()
	if conn := c.existingConn(addr); conn != nil {
		return conn, nil
	}
	conn, err := c.connect(addr)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if old, ok := c.conns[addr]; ok && old.alive.Load() {
		c.mu.Unlock()
		conn.close()
		return old, nil
	}
	c.conns[addr] = conn
	c.mu.Unlock()
	go c.readLoop(conn)
	return conn, nil
}

func (c *RemotingClient) connectGate(addr string) *sync.Mutex {
	c.mu.Lock()
	gate := c.gates[addr]
	if gate == nil {
		gate = &sync.Mutex{}
		c.gates[addr] = gate
	}
	c.mu.Unlock()
	return gate
}

func (c *RemotingClient) connect(addr string) (*connection, error) {
	host, portStr := common.ParseAddr(addr)
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return nil, common.SendRequestError(addr, fmt.Sprintf("bad port in address %q", addr))
	}
	if host == "" {
		return nil, common.SendRequestError(addr, fmt.Sprintf("empty host in address %q", addr))
	}
	timeout := time.Duration(c.config.ConnectTimeoutMillis) * time.Millisecond
	if timeout <= 0 {
		timeout = time.Second
	}
	var raw net.Conn
	if c.config.SocksProxy != nil {
		raw, err = dialSocksProxy(c.config.SocksProxy, host, port, timeout)
		if err != nil {
			return nil, common.ConnectError(fmt.Sprintf("%s (via socks proxy %s: %v)", addr, c.config.SocksProxy.Addr, err))
		}
	} else {
		dialer := &net.Dialer{Timeout: timeout}
		raw, err = dialer.Dial("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
		if err != nil {
			return nil, common.ConnectError(addr)
		}
	}
	if c.config.TLSEnable {
		raw, err = c.wrapTLS(raw, host, addr)
		if err != nil {
			return nil, err
		}
	}
	conn := &connection{id: c.connSeq.Add(1), addr: addr, conn: raw}
	conn.alive.Store(true)
	conn.touch()
	return conn, nil
}

func (c *RemotingClient) wrapTLS(raw net.Conn, host, addr string) (net.Conn, error) {
	cfg := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
	if c.config.TLSTestMode {
		cfg.InsecureSkipVerify = true
	}
	timeout := time.Duration(c.config.ConnectTimeoutMillis) * time.Millisecond
	if timeout <= 0 {
		timeout = time.Second
	}
	_ = raw.SetDeadline(time.Now().Add(timeout))
	conn := tls.Client(raw, cfg)
	if err := conn.Handshake(); err != nil {
		_ = raw.Close()
		return nil, common.ConnectError(fmt.Sprintf("%s (tls handshake: %v)", addr, err))
	}
	_ = raw.SetDeadline(time.Time{})
	return conn, nil
}

// readLoop reassembles frames (totalLength prefix + headerLength-marked
// header + body) and hands each to dispatch. It unwinds on running=false,
// peer EOF, a bad frame length, a frame dispatch cannot decode, or any read
// error; the defer always runs the connection-lost bookkeeping with its own
// connID. Every successful read refreshes the connection's activity stamp.
func (c *RemotingClient) readLoop(conn *connection) {
	defer func() {
		conn.alive.Store(false)
		c.onConnectionLost(conn.addr, conn.id)
	}()
	reader := bufio.NewReaderSize(conn.conn, 16*1024)
	var lenBuf [4]byte
	for {
		if !c.running.Load() || !conn.alive.Load() {
			return
		}
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			return
		}
		conn.touch()
		total := int32(binary.BigEndian.Uint32(lenBuf[:]))
		if total <= 0 || total > MaxFrameLength {
			common.LogWarnf("remoting: bad frame length %d from %s", total, conn.addr)
			return
		}
		frame := make([]byte, 4+int(total))
		copy(frame, lenBuf[:])
		if _, err := io.ReadFull(reader, frame[4:]); err != nil {
			return
		}
		conn.touch()
		// A frame the client cannot decode means the length-prefixed stream is no
		// longer aligned, so every later frame would be read from the wrong offset:
		// the connection goes, the same way a peer EOF takes it (its in-flight
		// requests fail fast, other connections are untouched).
		if err := c.dispatch(conn.addr, frame); err != nil {
			common.LogErrorf("remoting: undecodable frame from %s, closing the connection: %v",
				conn.addr, err)
			return
		}
	}
}

// dispatch routes one decoded frame: responses pop the pending table by
// opaque, requests go to the processor table. A processor panic must not kill
// the read loop — answer SYSTEM_ERROR, the protocol's exception guard. A header
// that cannot be decoded is returned to the read loop, which closes the
// connection.
func (c *RemotingClient) dispatch(addr string, frame []byte) error {
	cmd, err := Decode(frame)
	if err != nil {
		return err
	}
	opaque := cmd.Opaque
	c.mu.Lock()
	call, ok := c.pending[opaque]
	if ok {
		delete(c.pending, opaque)
	}
	c.mu.Unlock()
	if ok {
		// Popped by opaque even without the response flag — the abnormal
		// fallback shape and the normal one take the same action.
		c.applyAfterResponseHooks(addr, call.request, cmd)
		select {
		case call.ch <- cmd:
		default:
		}
		return nil
	}
	if cmd.IsResponseType() {
		common.LogWarnf("remoting: response for unknown opaque %d from %s", opaque, addr)
		return nil
	}
	c.procMu.RLock()
	proc := c.processors[cmd.Code]
	c.procMu.RUnlock()
	if proc == nil {
		common.LogWarnf("remoting: no processor for request code %d (opaque %d) from %s", cmd.Code, opaque, addr)
		return nil
	}
	sink := &ResponseSink{client: c, addr: addr, opaque: opaque, wantsReply: !cmd.IsOnewayRPC()}
	c.safeProcess(proc, cmd, addr, sink)
	return nil
}

func (c *RemotingClient) safeProcess(proc ProcessorFunc, request *RemotingCommand, addr string, sink *ResponseSink) {
	defer func() {
		if r := recover(); r != nil {
			common.LogWarnf("remoting: processor panic for code=%d: %v", request.Code, r)
			sink.Respond(CreateResponseCommand(RespSystemError, "process request fail"))
		}
	}()
	proc(request, addr, sink)
}
