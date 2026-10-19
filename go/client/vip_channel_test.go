// Where the fast-channel port shift happens.
//
// The switch is off by default and, when it is on, moves only the port of the
// broker RPCs that are served there -- and it moves it one request at a time, at
// the moment the request leaves, never inside the cached route. Two failure
// directions are pinned here:
//
//   - a call site that does not consult the switch loses the fast channel; the
//     "moved" cases below only reach the broker stub because the client shifted
//     the published port down by 2, and the same call with the switch off does
//     not arrive at all;
//   - a call site that consults it when it must not sends registration traffic
//     or a broker-config read to a port a plain broker never opens; the
//     "never moved" cases below arrive only while the listening port is kept.
package client

import (
	"net"
	"strconv"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// shiftPort returns addr with its port moved by delta: handing a client the
// address two ports above a listener means a request only lands when the client
// moves it down by 2.
func shiftPort(t *testing.T, addr string, delta int) string {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("bad stub address %q: %v", addr, err)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("bad stub port in %q: %v", addr, err)
	}
	return net.JoinHostPort(host, strconv.Itoa(n+delta))
}

// vipStub answers everything with SUCCESS: what matters is which port it is
// reached on, not what comes back.
func vipStub(t *testing.T) *mockServer {
	t.Helper()
	return startMockServer(t, func(_ *mockServer, _ *remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	})
}

func vipInstance(t *testing.T, on bool) *Instance {
	t.Helper()
	cfg := testConfig()
	cfg.VipChannelEnabled = on
	inst := CreateOrGetInstance(uniqueClientID(t), []string{"127.0.0.1:1"}, cfg)
	t.Cleanup(inst.Shutdown)
	return inst
}

func vipStr(s string) *string { return &s }
func vipI32(i int32) *int32   { return &i }
func vipI64(i int64) *int64   { return &i }

// ------------------------------------------------------------------- the moved family

// The RPCs served on the fast channel are sent there, one request at a time.
func TestVipChannelMovesTheBrokerRpcFamily(t *testing.T) {
	stub := vipStub(t)
	// Two ports above the listener: nothing answers there, so an arrival proves
	// the client shifted the address.
	published := shiftPort(t, stub.addr, 2)
	inst := vipInstance(t, true)
	mq := common.MessageQueue{Topic: "VipTopic", BrokerName: "b1", QueueID: 0}

	_, _, _ = inst.QueryConsumerOffset("GID_vip", mq, 2000, published, false)
	_ = inst.UpdateConsumerOffset("GID_vip", mq, 7, 2000, published)
	_, _ = inst.GetConsumerListByGroup(published, "GID_vip", 2000)

	for code, want := range map[int32]int{
		remoting.ReqQueryConsumerOffset:    1,
		remoting.ReqUpdateConsumerOffset:   1,
		remoting.ReqGetConsumerListByGroup: 1,
	} {
		if got := len(stub.requests(code)); got != want {
			t.Errorf("request %d arrived %d times, want %d: the fast channel is applied per request at the invoke", code, got, want)
		}
	}
	if got := len(stub.requests(remoting.ReqGetMaxOffset)); got != 0 {
		t.Errorf("unrelated request %d leaked into the stub: %d times", remoting.ReqGetMaxOffset, got)
	}
}

// Control: with the switch off the same address is used verbatim, so none of it
// reaches the broker the published address cannot answer at all.
func TestVipChannelOffLeavesEveryAddressAlone(t *testing.T) {
	stub := vipStub(t)
	published := shiftPort(t, stub.addr, 2)
	inst := vipInstance(t, false)
	mq := common.MessageQueue{Topic: "VipTopic", BrokerName: "b1", QueueID: 0}

	_, _, err := inst.QueryConsumerOffset("GID_vip", mq, 2000, published, false)
	if err == nil {
		t.Fatal("a request to a port nothing listens on must fail while the switch is off")
	}
	if got := len(stub.requests(remoting.ReqQueryConsumerOffset)); got != 0 {
		t.Errorf("the stub saw %d offset queries through an address no route shift explains", got)
	}

	// The same call on the listening port works -- the difference is the switch,
	// not the address.
	if _, _, err := inst.QueryConsumerOffset("GID_vip", mq, 2000, stub.addr, false); err != nil {
		t.Fatalf("query offset on the published address: %v", err)
	}
	if got := len(stub.requests(remoting.ReqQueryConsumerOffset)); got != 1 {
		t.Errorf("request arrived %d times with the switch off, want 1", got)
	}
}

// ---------------------------------------------------------------- the never-moved family

// Registration traffic, the pull/pop family and the broker's own configuration
// read stay on the listening port even with the switch on.
func TestVipChannelNeverMovesRegistrationOrPopTraffic(t *testing.T) {
	stub := vipStub(t)
	inst := vipInstance(t, true)

	if err := inst.SendHeartbeat(stub.addr, remoting.NewHeartbeatData("vip-client"), 2000); err != nil {
		t.Fatalf("heartbeat should reach the listening port: %v", err)
	}
	if err := inst.UnregisterClient(stub.addr, "GID_vip_p", "GID_vip", 2000); err != nil {
		t.Fatalf("unregister should reach the listening port: %v", err)
	}
	_ = inst.AckMessage(stub.addr, &remoting.AckMessageRequestHeader{
		Bname: vipStr("b1"), ConsumerGroup: vipStr("GID_vip"), Topic: vipStr("VipTopic"),
		QueueID: vipI32(0), ExtraInfo: vipStr("0 0 0 0 VipTopic b1 0 1"), Offset: vipI64(0),
	}, 2000)
	_, _ = inst.ChangeInvisibleTime(stub.addr, &remoting.ChangeInvisibleTimeRequestHeader{
		Bname: vipStr("b1"), ConsumerGroup: vipStr("GID_vip"), Topic: vipStr("VipTopic"),
		QueueID: vipI32(0), ExtraInfo: vipStr("0 0 0 0 VipTopic b1 0 1"), Offset: vipI64(0),
		InvisibleTime: vipI64(1000),
	}, 2000)

	for _, code := range []int32{
		remoting.ReqHeartBeat,
		remoting.ReqUnregisterClient,
		remoting.ReqAckMessage,
		remoting.ReqChangeMessageInvisibleTime,
	} {
		if got := len(stub.requests(code)); got != 1 {
			t.Errorf("request %d arrived %d times, want exactly 1: a port shift here would aim at a port the broker keeps closed", code, got)
		}
	}
}

// -------------------------------------------------------------------- the admin boundary

// The administration family rides the same per-request shift, while reading a
// broker's own configuration stays on the listening port.
func TestVipChannelMovesTheAdminFamilyButNotTheBrokerConfigRead(t *testing.T) {
	c := newAdminCluster(t)
	c.admin.SetVipChannelEnabled(true)

	// Two ports above the broker stub: an admin RPC only arrives if it was moved.
	unreachable := shiftPort(t, c.brokerSrv.addr, 2)
	_, _ = c.admin.FetchBrokerRuntimeStats(unreachable, 2000)
	if n := c.brokerLog.count(remoting.ReqGetBrokerRuntimeInfo); n != 1 {
		t.Errorf("GET_BROKER_RUNTIME_INFO(28) arrived %d times, want 1 on the shifted port", n)
	}

	// The one read the protocol keeps on the listening port.
	if _, err := c.admin.GetBrokerConfig(c.brokerSrv.addr, 2000); err != nil {
		t.Fatalf("the broker-config read should reach the listening port: %v", err)
	}
	if n := c.brokerLog.count(remoting.ReqGetBrokerConfig); n != 1 {
		t.Errorf("GET_BROKER_CONFIG(26) arrived %d times, want 1: this read never moves", n)
	}
}
