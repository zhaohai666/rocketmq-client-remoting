// Instance tests against an in-process mock cluster: a nameserver serving
// Java-format route bodies and per-broker TCP servers that record requests
// and can push broker->client requests (40/220/221) back over the warm
// connection.
package client

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- mock server

func readMockFrame(conn net.Conn) ([]byte, bool) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return nil, false
	}
	total := int32(binary.BigEndian.Uint32(lenBuf[:]))
	if total <= 0 {
		return nil, false
	}
	frame := make([]byte, 4+int(total))
	copy(frame, lenBuf[:])
	if _, err := io.ReadFull(conn, frame[4:]); err != nil {
		return nil, false
	}
	return frame, true
}

func writeMockFrame(conn net.Conn, cmd *remoting.RemotingCommand) {
	_, _ = conn.Write(cmd.Encode())
}

// mockServer is one in-process nameserver or broker. onReq answers sync
// requests; returning nil sends nothing. Requests the server itself pushes
// (40/220/221) go through pushSync/pushOneway on the warm connection.
type mockServer struct {
	t        *testing.T
	addr     string
	listener net.Listener

	mu      sync.Mutex
	conns   []net.Conn
	reqs    []*remoting.RemotingCommand
	errs    []string
	pend    map[int32]chan *remoting.RemotingCommand
	onReq   func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand
	dropReq bool // close the connection after each request (transport-error case)
	closed  bool

	handlerDone atomic.Bool
}

// fail records an in-handler assertion failure; t.Errorf must not be called
// from the server goroutine (it may outlive the test).
func (s *mockServer) fail(format string, args ...any) {
	s.mu.Lock()
	s.errs = append(s.errs, fmt.Sprintf(format, args...))
	s.mu.Unlock()
}

// waitHandler blocks until the handler signals completion, then surfaces any
// recorded assertion failures.
func (s *mockServer) waitHandler(t *testing.T, what string) {
	t.Helper()
	waitFor(t, what, func() bool { return s.handlerDone.Load() })
	for _, e := range s.errors() {
		t.Error(e)
	}
}

func (s *mockServer) errors() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.errs...)
}

func startMockServer(t *testing.T, onReq func(*mockServer, *remoting.RemotingCommand) *remoting.RemotingCommand) *mockServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &mockServer{
		t:        t,
		addr:     listener.Addr().String(),
		listener: listener,
		pend:     map[int32]chan *remoting.RemotingCommand{},
		onReq:    onReq,
	}
	t.Cleanup(func() { s.close() })
	go s.acceptLoop()
	return s
}

func (s *mockServer) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		go s.serve(conn)
	}
}

func (s *mockServer) serve(conn net.Conn) {
	defer func() {
		s.mu.Lock()
		for i, c := range s.conns {
			if c == conn {
				s.conns = append(s.conns[:i], s.conns[i+1:]...)
				break
			}
		}
		s.mu.Unlock()
		conn.Close()
	}()
	for {
		frame, ok := readMockFrame(conn)
		if !ok {
			return
		}
		cmd, err := remoting.Decode(frame)
		if err != nil {
			return
		}
		if cmd.IsResponseType() {
			s.mu.Lock()
			ch := s.pend[cmd.Opaque]
			delete(s.pend, cmd.Opaque)
			s.mu.Unlock()
			if ch != nil {
				ch <- cmd
			}
			continue
		}
		s.mu.Lock()
		s.reqs = append(s.reqs, cmd)
		drop := s.dropReq
		s.mu.Unlock()
		if drop {
			return
		}
		// Serve in a goroutine: a handler may push a request of its own and
		// wait for the client's ack, which only arrives if this read loop
		// keeps reading.
		go func() {
			var resp *remoting.RemotingCommand
			if s.onReq != nil {
				resp = s.onReq(s, cmd)
			}
			if resp != nil && !cmd.IsOnewayRPC() {
				resp.Opaque = cmd.Opaque
				resp.SerializeTypeCurrentRPC = cmd.SerializeTypeCurrentRPC
				writeMockFrame(conn, resp)
			}
		}()
	}
}

func (s *mockServer) requests(code int32) []*remoting.RemotingCommand {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*remoting.RemotingCommand
	for _, r := range s.reqs {
		if r.Code == code {
			out = append(out, r)
		}
	}
	return out
}

func (s *mockServer) setDrop(on bool) {
	s.mu.Lock()
	s.dropReq = on
	s.mu.Unlock()
}

// pushSync writes a server-initiated request over an established connection
// and waits for the client's response.
func (s *mockServer) pushSync(cmd *remoting.RemotingCommand, timeout time.Duration) (*remoting.RemotingCommand, error) {
	s.mu.Lock()
	if len(s.conns) == 0 {
		s.mu.Unlock()
		return nil, fmt.Errorf("no client connection to push on")
	}
	conn := s.conns[0]
	ch := make(chan *remoting.RemotingCommand, 1)
	s.pend[cmd.Opaque] = ch
	s.mu.Unlock()

	writeMockFrame(conn, cmd)
	select {
	case resp := <-ch:
		return resp, nil
	case <-time.After(timeout):
		s.mu.Lock()
		delete(s.pend, cmd.Opaque)
		s.mu.Unlock()
		return nil, fmt.Errorf("timed out waiting for the client's response to opaque %d", cmd.Opaque)
	}
}

// pushOneway writes a server-initiated oneway request (no reply expected).
func (s *mockServer) pushOneway(cmd *remoting.RemotingCommand) error {
	s.mu.Lock()
	if len(s.conns) == 0 {
		s.mu.Unlock()
		return fmt.Errorf("no client connection to push on")
	}
	conn := s.conns[0]
	s.mu.Unlock()
	writeMockFrame(conn, cmd)
	return nil
}

func (s *mockServer) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	conns := append([]net.Conn(nil), s.conns...)
	s.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
	s.listener.Close()
}

// ---------------------------------------------------------------- helpers

func testConfig() ClientInstanceConfig {
	cfg := NewClientInstanceConfig()
	// The scheduled tasks' real periods would only add noise here; the start
	// test overrides them to tiny values on purpose.
	return cfg
}

func routeBodyFor(master, slave string) string {
	addrs := `"0":"` + master + `"`
	if slave != "" {
		addrs += `,"1":"` + slave + `"`
	}
	return `{"orderTopicConf":null,` +
		`"queueDatas":[{"brokerName":"b1","readQueueNums":4,"writeQueueNums":4,"perm":6,"topicSysFlag":0}],` +
		`"brokerDatas":[{"cluster":"DefaultCluster","brokerName":"b1","brokerAddrs":{` + addrs + `},"zoneName":null,"enableActingMaster":false}],` +
		`"filterServerTable":{}}`
}

func emptyRouteBody() string {
	return `{"orderTopicConf":null,"queueDatas":[],"brokerDatas":[],"filterServerTable":{}}`
}

// nameserverOnReq answers GET_ROUTE_INFO_BY_TOPIC(105) with the per-topic body.
func nameserverOnReq(bodies map[string]string) func(*mockServer, *remoting.RemotingCommand) *remoting.RemotingCommand {
	return func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code != remoting.ReqGetRouteInfoByTopic {
			return nil
		}
		var header remoting.GetRouteInfoRequestHeader
		header.FromExtFields(req.ExtFields())
		topic := ""
		if header.Topic != nil {
			topic = *header.Topic
		}
		body, ok := bodies[topic]
		if !ok {
			resp := remoting.CreateResponseCommand(remoting.RespTopicNotExist, "topic["+topic+"] not exist")
			return resp
		}
		resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
		resp.SetBody([]byte(body))
		return resp
	}
}

// stubConsumer implements Consumer, recording what the instance drives.
type stubConsumer struct {
	group      string
	subs       []*remoting.SubscriptionData
	status     remoting.MQOffsetTable
	rebalances atomic.Int64
	adjusts    atomic.Int64
	persists   atomic.Int64

	mu     sync.Mutex
	resets []resetCall
}

type resetCall struct {
	topic string
	table remoting.MQOffsetTable
}

func (c *stubConsumer) ConsumerGroup() string    { return c.group }
func (c *stubConsumer) ConsumeType() string      { return remoting.ConsumeTypeConsumePassively }
func (c *stubConsumer) MessageModel() string     { return remoting.MessageModelClustering }
func (c *stubConsumer) ConsumeFromWhere() string { return remoting.ConsumeFromWhereLastOffset }
func (c *stubConsumer) IsUnitMode() bool         { return false }
func (c *stubConsumer) Subscriptions() []*remoting.SubscriptionData {
	return c.subs
}
func (c *stubConsumer) RebalanceImmediately() { c.rebalances.Add(1) }
func (c *stubConsumer) AdjustThreadPool()     { c.adjusts.Add(1) }
func (c *stubConsumer) ResetOffset(topic string, table remoting.MQOffsetTable) {
	c.mu.Lock()
	c.resets = append(c.resets, resetCall{topic: topic, table: append(remoting.MQOffsetTable(nil), table...)})
	c.mu.Unlock()
}
func (c *stubConsumer) GetConsumerStatus(topic *string) remoting.MQOffsetTable {
	return c.status
}
func (c *stubConsumer) PersistConsumerOffset() error { c.persists.Add(1); return nil }

// ConsumerRunningInfo returns nil on purpose: the stub stands in for a consumer
// that is not a push consumer, which is exactly the arm Java's
// MQClientInstance.consumerRunningInfo answers with null (and therefore the arm
// that makes both 307 and 309 reply "The Consumer Group <g> not exist in this
// consumer" instead of a body).
func (c *stubConsumer) ConsumerRunningInfo() *remoting.ConsumerRunningInfo { return nil }
func (c *stubConsumer) resetCalls() []resetCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]resetCall(nil), c.resets...)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

var clientIDSeries atomic.Int64

func uniqueClientID(t *testing.T) string {
	return fmt.Sprintf("%s-%d", strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()),
		clientIDSeries.Add(1))
}

// warmRoute fetches the topic route and fails the test when the nameserver
// mock was not reachable.
func warmRoute(t *testing.T, inst *Instance, topic string) {
	t.Helper()
	if ok, err := inst.UpdateTopicRouteInfoFromNameServer(topic, 2000, false); err != nil || !ok {
		t.Fatalf("route fetch failed: ok=%v err=%v", ok, err)
	}
}

// ---------------------------------------------------------------- tests

func TestInstanceRegistryReuseAndShutdownGuard(t *testing.T) {
	cfg := testConfig()
	cid := uniqueClientID(t)
	inst1 := CreateOrGetInstance(cid, nil, cfg)
	inst2 := CreateOrGetInstance(cid, nil, cfg)
	if inst1 != inst2 {
		t.Fatalf("same clientId must reuse the live instance")
	}

	// A still-registered producer must block Shutdown (Java's guard).
	inst1.RegisterProducer("PG")
	inst1.Start()
	inst1.Shutdown()
	if !inst1.IsStarted() {
		t.Errorf("Shutdown with tenants registered must be a no-op")
	}
	if FindInstance(cid) == nil {
		t.Errorf("guarded instance must stay in the registry")
	}

	inst1.UnregisterProducer("PG")
	inst1.Shutdown()
	if inst1.IsStarted() {
		t.Errorf("Shutdown without tenants must stop the factory")
	}
	if FindInstance(cid) != nil {
		t.Errorf("torn-down instance must leave the registry")
	}
	inst3 := CreateOrGetInstance(cid, nil, cfg)
	if inst3 == inst1 {
		t.Errorf("after teardown a fresh instance must be built")
	}
	inst3.Shutdown()
}

func TestInstanceStartFetchesNameServerFromAddressServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("127.0.0.1:1;127.0.0.1:2;\n"))
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.NamesrvWsAddr = srv.URL
	inst := CreateOrGetInstance(uniqueClientID(t), nil, cfg)
	defer inst.Shutdown()
	if err := inst.Start(); err != nil {
		t.Fatal(err)
	}
	addrs := inst.NameServerAddrs()
	if len(addrs) != 2 || addrs[0] != "127.0.0.1:1" || addrs[1] != "127.0.0.1:2" {
		t.Errorf("address-server fetch must fill the nameserver list, got %v", addrs)
	}
}

func TestInstanceStartFailsWhenAddressServerEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(" \n"))
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.NamesrvWsAddr = srv.URL
	inst := CreateOrGetInstance(uniqueClientID(t), nil, cfg)
	err := inst.Start()
	if err == nil {
		inst.Shutdown()
		t.Fatalf("empty address-server answer must fail Start")
	}
	if !common.IsKind(err, common.KindClient) {
		t.Fatalf("want client error, got %v", err)
	}
	if code, ok := err.(*common.Error).ResponseCode(); !ok || code != common.NoNameServerException {
		t.Errorf("want code 10004, got %v ok=%v", err, ok)
	}
	if inst.IsStarted() {
		t.Errorf("failed start must not leave the factory running")
	}
	if FindInstance(inst.ClientID()) != nil {
		t.Errorf("failed start must clean the registry entry")
	}
}

func TestRouteFetchAndPublishInfo(t *testing.T) {
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		"Tt": routeBodyFor("10.1.0.1:10911", "10.1.0.2:10911"),
	}))
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, testConfig())
	defer inst.Shutdown()

	ok, err := inst.UpdateTopicRouteInfoFromNameServer("Tt", 2000, false)
	if err != nil || !ok {
		t.Fatalf("route fetch failed: ok=%v err=%v", ok, err)
	}
	info, err := inst.GetTopicPublishInfo("Tt", false)
	if err != nil {
		t.Fatal(err)
	}
	if !info.OK() || len(info.MsgQueueList()) != 4 {
		t.Fatalf("publish info broken: ok=%v queues=%d", info.OK(), len(info.MsgQueueList()))
	}

	// Publish lookups are master-only; the fan-out set is every id.
	if addr, ok := inst.FindBrokerAddressInPublish("b1"); !ok || addr != "10.1.0.1:10911" {
		t.Errorf("FindBrokerAddressInPublish = %q ok=%v, want master", addr, ok)
	}
	if addr, err := inst.PublishAddrFor("b1", "Tt"); err != nil || addr != "10.1.0.1:10911" {
		t.Errorf("PublishAddrFor = %q err=%v, want master", addr, err)
	}
	if addr, ok := inst.BrokerAddrOf("b1"); !ok || addr != "10.1.0.1:10911" {
		t.Errorf("BrokerAddrOf (master preferred) = %q ok=%v", addr, ok)
	}
	got := inst.GetAllBrokerAddrs()
	if len(got) != 2 || got[0] != "10.1.0.1:10911" || got[1] != "10.1.0.2:10911" {
		t.Errorf("GetAllBrokerAddrs = %v, want master+slave sorted", got)
	}
	if got := inst.GetRouteOfAllBrokers(); len(got) != 1 || got[0] != "10.1.0.1:10911" {
		t.Errorf("GetRouteOfAllBrokers = %v, want [master]", got)
	}
	if mqs := inst.GetTopicSubscribeInfo("Tt"); len(mqs) != 4 {
		t.Errorf("subscribe info = %d queues, want 4", len(mqs))
	}
}

func TestRouteFetchNoNameServerIs10004(t *testing.T) {
	inst := CreateOrGetInstance(uniqueClientID(t), nil, testConfig())
	defer inst.Shutdown()
	_, err := inst.UpdateTopicRouteInfoFromNameServer("Tt", 2000, false)
	if !common.IsKind(err, common.KindClient) {
		t.Fatalf("want client error, got %v", err)
	}
	if code, _ := err.(*common.Error).ResponseCode(); code != common.NoNameServerException {
		t.Errorf("want 10004, got %v", err)
	}
	_, err = inst.GetTopicPublishInfo("Tt", false)
	if !common.IsKind(err, common.KindClient) {
		t.Errorf("publish info on empty namesrv list must surface 10004, got %v", err)
	}
}

func TestRouteFetchTopicNotExistIsQuietlyFalse(t *testing.T) {
	ns := startMockServer(t, nameserverOnReq(map[string]string{}))
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, testConfig())
	defer inst.Shutdown()

	ok, err := inst.UpdateTopicRouteInfoFromNameServer("Tt", 2000, false)
	if err != nil || ok {
		t.Fatalf("TOPIC_NOT_EXIST must be (false, nil), got ok=%v err=%v", ok, err)
	}
	_, err = inst.GetTopicPublishInfo("Tt", false)
	if err == nil || !strings.Contains(err.Error(), "Can not find Message Queue for topic: Tt") {
		t.Errorf("publish info must fail with the Java wording, got %v", err)
	}
}

func TestRouteFetchSuccessNoBodyStopsTheSearch(t *testing.T) {
	// A SUCCESS-without-body answer ends the nameserver walk: the remaining
	// nameservers would answer the same.
	ns1 := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		return remoting.CreateResponseCommand(remoting.RespSuccess, "")
	})
	ns2 := startMockServer(t, nameserverOnReq(map[string]string{"Tt": routeBodyFor("10.1.0.1:10911", "")}))
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns1.addr, ns2.addr}, testConfig())
	defer inst.Shutdown()

	ok, err := inst.UpdateTopicRouteInfoFromNameServer("Tt", 2000, false)
	if err != nil || ok {
		t.Fatalf("empty-body break must give (false, nil), got ok=%v err=%v", ok, err)
	}
	time.Sleep(100 * time.Millisecond)
	if len(ns2.requests(remoting.ReqGetRouteInfoByTopic)) != 0 {
		t.Errorf("second nameserver must not be contacted after a body-less success")
	}
}

func TestHeartbeatFanOutToMasterAndSlave(t *testing.T) {
	brokerM := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code == remoting.ReqHeartBeat {
			return remoting.CreateResponseCommand(remoting.RespSuccess, "")
		}
		return nil
	})
	brokerS := startMockServer(t, brokerM.onReq)
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		"Tt": routeBodyFor(brokerM.addr, brokerS.addr),
	}))
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, testConfig())
	defer inst.Shutdown()
	warmRoute(t, inst, "Tt")

	stub := &stubConsumer{group: "CG", subs: []*remoting.SubscriptionData{
		{Topic: "Tt", SubString: "*", ExpressionType: remoting.ExpressionTypeTag},
	}}
	inst.RegisterConsumer("CG", stub)

	// No consumers registered -> zero sends (Java's short-circuit).
	inst.UnregisterConsumer("CG")
	if n := inst.SendHeartbeatToAllBroker(2000); n != 0 {
		t.Fatalf("heartbeat without consumers must not send, got %d", n)
	}
	inst.RegisterConsumer("CG", stub)

	if n := inst.SendHeartbeatToAllBroker(2000); n != 2 {
		t.Fatalf("heartbeat success count = %d, want 2 (master+slave)", n)
	}
	if len(brokerM.requests(remoting.ReqHeartBeat)) != 1 || len(brokerS.requests(remoting.ReqHeartBeat)) != 1 {
		t.Fatalf("both broker ids must receive exactly one heartbeat")
	}
	hb, err := remoting.DecodeHeartbeatData(brokerM.requests(remoting.ReqHeartBeat)[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	if hb.ClientID != inst.ClientID() || len(hb.ConsumerDataSet) != 1 ||
		hb.ConsumerDataSet[0].GroupName != "CG" || hb.ConsumerDataSet[0].ConsumeType != remoting.ConsumeTypeConsumePassively {
		t.Errorf("heartbeat body mismatch: clientId=%q consumerSet=%+v", hb.ClientID, hb.ConsumerDataSet)
	}
}

func TestNotifyConsumerIDsChangedWakesRebalance(t *testing.T) {
	broker := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code == remoting.ReqHeartBeat {
			// First heartbeat: ride the warm connection to push 40 oneway.
			push := remoting.CreateRequestCommand(remoting.ReqNotifyConsumerIDsChanged,
				&remoting.NotifyConsumerIdsChangedRequestHeader{ConsumerGroup: remoting.StrPtr("CG")})
			push.MarkOnewayRPC()
			if err := s.pushOneway(push); err != nil {
				s.fail("push 40 failed: %v", err)
			}
			s.handlerDone.Store(true)
			return remoting.CreateResponseCommand(remoting.RespSuccess, "")
		}
		return nil
	})
	ns := startMockServer(t, nameserverOnReq(map[string]string{"Tt": routeBodyFor(broker.addr, "")}))
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, testConfig())
	defer inst.Shutdown()
	warmRoute(t, inst, "Tt")

	stub := &stubConsumer{group: "CG"}
	inst.RegisterConsumer("CG", stub)
	if n := inst.SendHeartbeatToAllBroker(2000); n != 1 {
		t.Fatalf("heartbeat count = %d", n)
	}
	waitFor(t, "NOTIFY_CONSUMER_IDS_CHANGED processing", func() bool {
		return inst.ConsumerIDsChangedCount() > 0 && stub.rebalances.Load() > 0
	})
}

func TestResetOffsetPushGoroutine(t *testing.T) {
	mq := common.NewMessageQueue("Tt", "b1", 1)
	broker := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code == remoting.ReqHeartBeat {
			body := (&remoting.ResetOffsetBody{OffsetTable: remoting.MQOffsetTable{
				{Queue: mq, Offset: 7},
			}}).Encode()
			push := remoting.CreateRequestCommand(remoting.ReqResetConsumerClientOffset,
				&remoting.ResetOffsetRequestHeader{Topic: remoting.StrPtr("Tt"), Group: remoting.StrPtr("CG")})
			push.SetBody(body)
			resp, err := s.pushSync(push, 3*time.Second)
			if err != nil {
				s.fail("220 push failed: %v", err)
				s.handlerDone.Store(true)
				return nil
			}
			if resp.Code != remoting.RespSuccess {
				s.fail("220 ack code = %d remark %q", resp.Code, resp.Remark)
			}
			s.handlerDone.Store(true)
			return remoting.CreateResponseCommand(remoting.RespSuccess, "")
		}
		return nil
	})
	ns := startMockServer(t, nameserverOnReq(map[string]string{"Tt": routeBodyFor(broker.addr, "")}))
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, testConfig())
	defer inst.Shutdown()
	warmRoute(t, inst, "Tt")

	stub := &stubConsumer{group: "CG"}
	inst.RegisterConsumer("CG", stub)
	if n := inst.SendHeartbeatToAllBroker(2000); n != 1 {
		t.Fatalf("heartbeat count = %d", n)
	}
	waitFor(t, "220 reset reaching the consumer", func() bool { return len(stub.resetCalls()) > 0 })
	broker.waitHandler(t, "220 ack assertions")
	call := stub.resetCalls()[0]
	if call.topic != "Tt" || len(call.table) != 1 ||
		call.table[0].Queue != mq || call.table[0].Offset != 7 {
		t.Errorf("reset payload mismatch: %+v", call)
	}
}

func TestGetConsumerStatusPush(t *testing.T) {
	mq := common.NewMessageQueue("Tt", "b1", 2)
	broker := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code == remoting.ReqHeartBeat {
			push := remoting.CreateRequestCommand(remoting.ReqGetConsumerStatusFromClient,
				&remoting.GetConsumerStatusRequestHeader{Topic: remoting.StrPtr("Tt"), Group: remoting.StrPtr("CG")})
			resp, err := s.pushSync(push, 3*time.Second)
			if err != nil {
				s.fail("221 push failed: %v", err)
				s.handlerDone.Store(true)
				return nil
			}
			if resp.Code != remoting.RespSuccess {
				s.fail("221 response code = %d", resp.Code)
				s.handlerDone.Store(true)
				return nil
			}
			body, err := remoting.DecodeGetConsumerStatusBody(resp.Body)
			if err != nil {
				s.fail("221 body decode: %v", err)
				s.handlerDone.Store(true)
				return nil
			}
			if len(body.MessageQueueTable) != 1 || body.MessageQueueTable[0].Queue != mq ||
				body.MessageQueueTable[0].Offset != 33 {
				s.fail("221 status table mismatch: %+v", body.MessageQueueTable)
			}
			s.handlerDone.Store(true)
			return remoting.CreateResponseCommand(remoting.RespSuccess, "")
		}
		return nil
	})
	ns := startMockServer(t, nameserverOnReq(map[string]string{"Tt": routeBodyFor(broker.addr, "")}))
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, testConfig())
	defer inst.Shutdown()
	warmRoute(t, inst, "Tt")

	stub := &stubConsumer{group: "CG", status: remoting.MQOffsetTable{
		{Queue: mq, Offset: 33},
	}}
	inst.RegisterConsumer("CG", stub)
	if n := inst.SendHeartbeatToAllBroker(2000); n != 1 {
		t.Fatalf("heartbeat count = %d", n)
	}
	waitFor(t, "221 round trip", func() bool { return len(broker.requests(remoting.ReqHeartBeat)) > 0 })
	broker.waitHandler(t, "221 assertions")
}

func TestGetConsumerStatusUnknownGroupIsSystemError(t *testing.T) {
	broker := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code == remoting.ReqHeartBeat {
			push := remoting.CreateRequestCommand(remoting.ReqGetConsumerStatusFromClient,
				&remoting.GetConsumerStatusRequestHeader{Group: remoting.StrPtr("NOPE")})
			resp, err := s.pushSync(push, 3*time.Second)
			if err != nil {
				s.fail("221 push failed: %v", err)
				s.handlerDone.Store(true)
				return nil
			}
			if resp.Code != remoting.RespSystemError {
				s.fail("unknown group must answer SYSTEM_ERROR, got code=%d", resp.Code)
			}
			s.handlerDone.Store(true)
			return remoting.CreateResponseCommand(remoting.RespSuccess, "")
		}
		return nil
	})
	ns := startMockServer(t, nameserverOnReq(map[string]string{"Tt": routeBodyFor(broker.addr, "")}))
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, testConfig())
	defer inst.Shutdown()
	warmRoute(t, inst, "Tt")
	stub := &stubConsumer{group: "CG"}
	inst.RegisterConsumer("CG", stub)
	if n := inst.SendHeartbeatToAllBroker(2000); n != 1 {
		t.Fatalf("heartbeat count = %d", n)
	}
	broker.waitHandler(t, "221 unknown-group assertions")
}

func TestUnregisterClientAllBrokers(t *testing.T) {
	brokerM := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		return remoting.CreateResponseCommand(remoting.RespSuccess, "")
	})
	brokerS := startMockServer(t, brokerM.onReq)
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		"Tt": routeBodyFor(brokerM.addr, brokerS.addr),
	}))
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, testConfig())
	defer inst.Shutdown()
	if _, err := inst.UpdateTopicRouteInfoFromNameServer("Tt", 2000, false); err != nil {
		t.Fatal(err)
	}

	inst.UnregisterClientAllBrokers("", "CG")
	if len(brokerM.requests(remoting.ReqUnregisterClient)) != 1 ||
		len(brokerS.requests(remoting.ReqUnregisterClient)) != 1 {
		t.Fatalf("unregister must reach every broker id")
	}
	var header remoting.UnregisterClientRequestHeader
	header.FromExtFields(brokerM.requests(remoting.ReqUnregisterClient)[0].ExtFields())
	if header.ClientID == nil || *header.ClientID != inst.ClientID() {
		t.Errorf("clientID missing: %+v", header)
	}
	if header.ConsumerGroup == nil || *header.ConsumerGroup != "CG" {
		t.Errorf("consumerGroup missing: %+v", header)
	}
	if header.ProducerGroup != nil {
		t.Errorf("blank producerGroup must stay off the wire, got %q", *header.ProducerGroup)
	}

	inst.UnregisterClientAllBrokers(" ", " ")
	for _, srv := range []*mockServer{brokerM, brokerS} {
		if got := len(srv.requests(remoting.ReqUnregisterClient)); got != 2 {
			t.Fatalf("second unregister missing on %s: %d", srv.addr, got)
		}
		var h remoting.UnregisterClientRequestHeader
		h.FromExtFields(srv.requests(remoting.ReqUnregisterClient)[1].ExtFields())
		if h.ConsumerGroup != nil || h.ProducerGroup != nil {
			t.Errorf("blank groups must be omitted, got %+v", h)
		}
	}
}

func TestCheckSubscriptionsInBroker(t *testing.T) {
	broker := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code == remoting.ReqCheckClientConfig {
			body, err := remoting.DecodeCheckClientRequestBody(req.Body)
			if err != nil {
				s.fail("46 body decode: %v", err)
				return remoting.CreateResponseCommand(remoting.RespSystemError, "bad body")
			}
			if body.Group == nil || *body.Group != "CG" || body.SubscriptionData == nil ||
				body.SubscriptionData.Topic != "Tt" || body.SubscriptionData.ExpressionType != remoting.ExpressionTypeSQL92 {
				s.fail("46 body mismatch: %+v", body)
			}
			return remoting.CreateResponseCommand(remoting.RespSuccess, "")
		}
		return nil
	})
	ns := startMockServer(t, nameserverOnReq(map[string]string{"Tt": routeBodyFor(broker.addr, "")}))
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, testConfig())
	defer inst.Shutdown()
	if _, err := inst.UpdateTopicRouteInfoFromNameServer("Tt", 2000, false); err != nil {
		t.Fatal(err)
	}

	// TAG subscriptions never go on the wire.
	tagSub, err := remoting.FilterAPI{}.BuildSubscriptionData("Tt", "TagA")
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.CheckSubscriptionsInBroker("CG", []*remoting.SubscriptionData{tagSub}); err != nil {
		t.Fatalf("TAG sub must be skipped: %v", err)
	}
	if len(broker.requests(remoting.ReqCheckClientConfig)) != 0 {
		t.Fatalf("TAG sub must not be sent")
	}

	sqlSub := &remoting.SubscriptionData{Topic: "Tt", SubString: "a > 1", ExpressionType: remoting.ExpressionTypeSQL92}
	if err := inst.CheckSubscriptionsInBroker("CG", []*remoting.SubscriptionData{sqlSub}); err != nil {
		t.Fatalf("SQL92 sub must pass: %v", err)
	}
	if len(broker.requests(remoting.ReqCheckClientConfig)) != 1 {
		t.Fatalf("SQL92 sub must be checked once")
	}

	// A broker rejection propagates the broker's code (client-kind error).
	code23 := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		return remoting.CreateResponseCommand(23, "browser not found")
	})
	ns3 := startMockServer(t, nameserverOnReq(map[string]string{"Tt": routeBodyFor(code23.addr, "")}))
	inst3 := CreateOrGetInstance(uniqueClientID(t), []string{ns3.addr}, testConfig())
	defer inst3.Shutdown()
	if _, err := inst3.UpdateTopicRouteInfoFromNameServer("Tt", 2000, false); err != nil {
		t.Fatal(err)
	}
	err = inst3.CheckSubscriptionsInBroker("CG", []*remoting.SubscriptionData{sqlSub})
	if err == nil || !common.IsKind(err, common.KindClient) {
		t.Fatalf("broker rejection must surface as client error, got %v", err)
	}
	if code, _ := err.(*common.Error).ResponseCode(); code != 23 {
		t.Errorf("want broker code 23, got %v", err)
	}

	// A transport-level failure gets Java's fixed wording.
	dropper := startMockServer(t, nil)
	dropper.setDrop(true)
	ns4 := startMockServer(t, nameserverOnReq(map[string]string{"Tt": routeBodyFor(dropper.addr, "")}))
	inst4 := CreateOrGetInstance(uniqueClientID(t), []string{ns4.addr}, testConfig())
	defer inst4.Shutdown()
	if _, err := inst4.UpdateTopicRouteInfoFromNameServer("Tt", 2000, false); err != nil {
		t.Fatal(err)
	}
	err = inst4.CheckSubscriptionsInBroker("CG", []*remoting.SubscriptionData{sqlSub})
	if err == nil || !strings.Contains(err.Error(), "Check client in broker error, maybe because you use SQL92") {
		t.Fatalf("transport failure must get the fixed Java wording, got %v", err)
	}
}

func TestCleanOfflineBroker(t *testing.T) {
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		"Tt": routeBodyFor("10.1.0.1:10911", "10.1.0.2:10911"),
	}))
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, testConfig())
	defer inst.Shutdown()
	if _, err := inst.UpdateTopicRouteInfoFromNameServer("Tt", 2000, false); err != nil {
		t.Fatal(err)
	}
	if _, ok := inst.FindBrokerAddressInPublish("b1"); !ok {
		t.Fatalf("precondition: b1 in broker table")
	}

	// The nameserver now reports an empty route: b1 has no live entry left.
	ns2 := startMockServer(t, nameserverOnReq(map[string]string{"Tt": emptyRouteBody()}))
	inst2 := CreateOrGetInstance(uniqueClientID(t), []string{ns2.addr}, testConfig())
	defer inst2.Shutdown()
	if _, err := inst2.UpdateTopicRouteInfoFromNameServer("Tt", 2000, false); err != nil {
		t.Fatal(err)
	}
	inst2.cleanOfflineBroker()
	if _, ok := inst2.FindBrokerAddressInPublish("b1"); ok {
		t.Errorf("broker absent from every cached route must be cleaned")
	}
}
