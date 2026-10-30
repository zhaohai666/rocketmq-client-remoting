package client

import (
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// TestChannelActiveReregistersKnownBroker covers the re-registration itself:
// a newly established connection to a known broker makes the instance beat
// that broker immediately, an address that is not a known broker is ignored,
// and the switch turns it off.
func TestChannelActiveReregistersKnownBroker(t *testing.T) {
	broker := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code == remoting.ReqHeartBeat {
			return remoting.CreateResponseCommand(remoting.RespSuccess, "")
		}
		return nil
	})
	ns := startMockServer(t, nameserverOnReq(routes(map[string]string{
		"Tt": routeBodyFor(broker.addr, ""),
	})))
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, testConfig())
	defer inst.Shutdown()
	warmRoute(t, inst, "Tt")

	if !inst.IsEnableHeartbeatChannelEventListener() {
		t.Fatalf("channel-active re-registration must default to on")
	}

	stub := &stubConsumer{group: "CG", subs: []*remoting.SubscriptionData{
		{Topic: "Tt", SubString: "*", ExpressionType: remoting.ExpressionTypeTag},
	}}
	inst.RegisterConsumer("CG", stub)

	// Warm the channel with the switch off: opening it would otherwise fire
	// the listener, and this test wants each driven event counted on its own.
	inst.SetEnableHeartbeatChannelEventListener(false)
	if err := inst.SendHeartbeat(broker.addr, inst.PrepareHeartbeatData(), 2000); err != nil {
		t.Fatalf("warming the broker channel failed: %v", err)
	}
	baseBeats := len(broker.requests(remoting.ReqHeartBeat))
	if baseBeats == 0 {
		t.Fatalf("the warm-up heartbeat must reach the broker")
	}
	if got := stub.rebalances.Load(); got != 0 {
		t.Fatalf("switch off must suppress the warm-up re-registration, wakeups = %d", got)
	}
	inst.SetEnableHeartbeatChannelEventListener(true)

	inst.OnChannelActive(broker.addr)
	beats := broker.requests(remoting.ReqHeartBeat)
	if len(beats) != baseBeats+1 {
		t.Fatalf("known broker must get exactly one re-registration beat, got %d (base %d)",
			len(beats), baseBeats)
	}
	hb, err := remoting.DecodeHeartbeatData(beats[len(beats)-1].Body)
	if err != nil {
		t.Fatal(err)
	}
	if hb.ClientID != inst.ClientID() || len(hb.ConsumerDataSet) != 1 ||
		hb.ConsumerDataSet[0].GroupName != "CG" {
		t.Errorf("beat body mismatch: clientId=%q consumerSet=%+v", hb.ClientID, hb.ConsumerDataSet)
	}
	if got := stub.rebalances.Load(); got != 1 {
		t.Errorf("rebalance wakeups = %d, want 1", got)
	}

	// An address that is not a known broker has no group state to restore.
	inst.OnChannelActive("127.0.0.1:1")
	if got := len(broker.requests(remoting.ReqHeartBeat)); got != baseBeats+1 {
		t.Errorf("unknown address must not beat the broker, beats = %d", got)
	}
	if got := stub.rebalances.Load(); got != 1 {
		t.Errorf("unknown address must not wake rebalance, wakeups = %d", got)
	}

	inst.SetEnableHeartbeatChannelEventListener(false)
	inst.OnChannelActive(broker.addr)
	if got := len(broker.requests(remoting.ReqHeartBeat)); got != baseBeats+1 {
		t.Errorf("switch off must suppress the beat, beats = %d", got)
	}
	if got := stub.rebalances.Load(); got != 1 {
		t.Errorf("switch off must not wake rebalance, wakeups = %d", got)
	}

	inst.SetEnableHeartbeatChannelEventListener(true)
	inst.OnChannelActive(broker.addr)
	if got := len(broker.requests(remoting.ReqHeartBeat)); got != baseBeats+2 {
		t.Errorf("switch back on must beat again, beats = %d", got)
	}
	if got := stub.rebalances.Load(); got != 2 {
		t.Errorf("switch back on must wake rebalance again, wakeups = %d", got)
	}
}

// TestChannelListenerWiredIntoTheInstance proves registerClientProcessors
// hooked the instance onto the transport: the first request that opens a
// connection to the broker reaches the broker twice -- the caller's
// heartbeat and the listener's re-registration beat -- and wakes rebalance.
func TestChannelListenerWiredIntoTheInstance(t *testing.T) {
	broker := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code == remoting.ReqHeartBeat {
			return remoting.CreateResponseCommand(remoting.RespSuccess, "")
		}
		return nil
	})
	ns := startMockServer(t, nameserverOnReq(routes(map[string]string{
		"Tt": routeBodyFor(broker.addr, ""),
	})))
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, testConfig())
	defer inst.Shutdown()
	warmRoute(t, inst, "Tt")

	stub := &stubConsumer{group: "CG", subs: []*remoting.SubscriptionData{
		{Topic: "Tt", SubString: "*", ExpressionType: remoting.ExpressionTypeTag},
	}}
	inst.RegisterConsumer("CG", stub)

	if n := inst.SendHeartbeatToAllBroker(2000); n != 1 {
		t.Fatalf("heartbeat success count = %d, want 1", n)
	}
	waitFor(t, "channel-active re-registration beat", func() bool {
		return len(broker.requests(remoting.ReqHeartBeat)) >= 2 && stub.rebalances.Load() >= 1
	})
}
