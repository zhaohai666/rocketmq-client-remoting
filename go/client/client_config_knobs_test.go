// Wire-visible effects of the client-config switches: the decode-read-body
// switch on the POP receive path, the internal-API timeout the heartbeat RPC
// runs under, and the concurrent-heartbeat dispatch decision.
//
// Every positive case sits next to a control that goes red when the switch is
// reverted, and every wait is bounded and recorded, never a fixed sleep.
package client

import (
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

func TestPopDecodeSwitchKeepsHeadersAndDropsBodies(t *testing.T) {
	popTime := common.CurrentTimeMillis()

	// Control: with the switch at its default the bodies travel up.
	inst := newTestInstanceForPop(t)
	broker := inst.broker
	broker.setPopFixture(popFixtureOf("TopicA", "b1", 0, 10, 3, popTime, 60000))
	broker.mu.Lock()
	broker.popFixture.msgs = []*common.MessageExt{
		popMessage("TopicA", 0, 10, "m10"),
		popMessage("TopicA", 0, 11, "m11"),
		popMessage("TopicA", 0, 12, "m12"),
	}
	broker.mu.Unlock()

	result, err := popOnceForTest(inst, "TopicA", 0)
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if len(result.MsgFoundList) != 3 {
		t.Fatalf("msgs decoded = %d, want 3", len(result.MsgFoundList))
	}
	for i, want := range []string{"m10", "m11", "m12"} {
		if got := string(result.MsgFoundList[i].Body); got != want {
			t.Fatalf("msg %d body = %q, want %q", i, got, want)
		}
	}
	inst.Shutdown()

	// Switch off: the same stream yields the same three messages, but each
	// arrives as a header-only shell. Queue offsets and properties survive;
	// bodies do not.
	instOff := newTestInstanceForPop(t)
	instOff.SetDecodeReadBody(false)
	broker = instOff.broker
	broker.setPopFixture(popFixtureOf("TopicA", "b1", 0, 10, 3, popTime, 60000))
	broker.mu.Lock()
	broker.popFixture.msgs = []*common.MessageExt{
		popMessage("TopicA", 0, 10, "m10"),
		popMessage("TopicA", 0, 11, "m11"),
		popMessage("TopicA", 0, 12, "m12"),
	}
	broker.mu.Unlock()

	off, err := popOnceForTest(instOff, "TopicA", 0)
	if err != nil {
		t.Fatalf("pop with the switch off: %v", err)
	}
	if len(off.MsgFoundList) != 3 {
		t.Fatalf("header-only msgs = %d, want 3 (the batch must not be dropped)", len(off.MsgFoundList))
	}
	for i, msg := range off.MsgFoundList {
		if len(msg.Body) != 0 {
			t.Fatalf("msg %d body = %q, want none with the switch off", i, msg.Body)
		}
		if msg.QueueOffset != 10+int64(i) {
			t.Fatalf("msg %d queue offset = %d, want %d", i, msg.QueueOffset, 10+i)
		}
		if got := propertyOf(msg, common.PropertyUniqKey); got == "" {
			t.Fatalf("msg %d lost its properties with the switch off", i)
		}
	}
	instOff.Shutdown()
}

func TestConcurrentHeartbeatDispatchUsesThePoolAndTheSerialControlDoesNot(t *testing.T) {
	newBeatTarget := func(t *testing.T) (*Instance, *mockServer) {
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
		warmRoute(t, inst, "Tt")
		// The channel-active listener would add one re-registration beat when
		// the first connection opens; this test counts dispatch, not that.
		inst.SetEnableHeartbeatChannelEventListener(false)
		inst.RegisterConsumer("CG", &stubConsumer{group: "CG", subs: []*remoting.SubscriptionData{
			{Topic: "Tt", SubString: "*", ExpressionType: remoting.ExpressionTypeTag},
		}})
		t.Cleanup(inst.Shutdown)
		return inst, broker
	}

	// Control: the serial round answers both requests without ever touching
	// the worker pool.
	serial, serialBroker := newBeatTarget(t)
	if n := serial.SendHeartbeatToAllBroker(2000); n != 1 {
		t.Fatalf("serial heartbeat success count = %d, want 1", n)
	}
	if len(serialBroker.requests(remoting.ReqHeartBeat)) != 1 {
		t.Fatalf("serial control beats = %d, want 1", len(serialBroker.requests(remoting.ReqHeartBeat)))
	}
	if serial.concurrentHeartbeatPool != nil {
		t.Fatal("the serial round must not create the heartbeat pool")
	}

	// The concurrent round goes through the pool; a revert of the dispatch
	// decision leaves the pool uncreated and this assertion red.
	concurrent, concurrentBroker := newBeatTarget(t)
	if n := concurrent.SendHeartbeatToAllBrokerConcurrently(2000); n != 1 {
		t.Fatalf("concurrent heartbeat success count = %d, want 1", n)
	}
	if len(concurrentBroker.requests(remoting.ReqHeartBeat)) != 1 {
		t.Fatalf("concurrent beats = %d, want 1", len(concurrentBroker.requests(remoting.ReqHeartBeat)))
	}
	if concurrent.concurrentHeartbeatPool == nil {
		t.Fatal("the concurrent round must dispatch through the heartbeat pool")
	}
}

func TestAPITimeoutDefaultsTo3000AndAPositiveValueReplacesIt(t *testing.T) {
	// A broker that answers only after the short configured budget has run
	// out. Both outcomes are recorded on the wire, not on the clock alone.
	broker := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code == remoting.ReqHeartBeat {
			time.Sleep(500 * time.Millisecond)
			return remoting.CreateResponseCommand(remoting.RespSuccess, "")
		}
		return nil
	})
	inst := CreateOrGetInstance(uniqueClientID(t), nil, testConfig())
	defer inst.Shutdown()
	setBrokerAddrForTest(inst, "b1", broker.addr)

	if got := inst.MQClientAPITimeoutMillis(); got != DefaultMQClientAPITimeoutMillis {
		t.Fatalf("default api timeout = %d, want %d", got, DefaultMQClientAPITimeoutMillis)
	}

	inst.SetMQClientAPITimeoutMillis(120)
	hb := inst.PrepareHeartbeatData()
	start := time.Now()
	err := inst.SendHeartbeat(broker.addr, hb, 0)
	if err == nil {
		t.Fatal("a 500ms broker must outlast a 120ms budget: the configured timeout never reached the RPC")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("timeout took %s, the configured budget clearly never reached the call", elapsed)
	}

	// A generous budget beats the same broker: the value really is the knob.
	inst.SetMQClientAPITimeoutMillis(2000)
	if err := inst.SendHeartbeat(broker.addr, hb, 0); err != nil {
		t.Fatalf("heartbeat under a 2000ms budget: %v", err)
	}

	// A non-positive value is not a budget: it restores the default.
	inst.SetMQClientAPITimeoutMillis(-1)
	if got := inst.MQClientAPITimeoutMillis(); got != DefaultMQClientAPITimeoutMillis {
		t.Fatalf("fallback api timeout = %d, want %d", got, DefaultMQClientAPITimeoutMillis)
	}
}
