package client

import (
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

func TestAdminUpdateNameServerConfigSendsPropertiesBodyToEveryNameServer(t *testing.T) {
	c := newAdminCluster(t)
	c.nsAnswer[remoting.ReqUpdateNameSrvConfig] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	}
	requireNoError(t, "updateNameServerConfig",
		c.admin.UpdateNameServerConfig(map[string]string{"listenPort": "10915"}, 0))

	if got := c.nsLog.count(remoting.ReqUpdateNameSrvConfig); got != 1 {
		t.Fatalf("nameserver saw %d UPDATE_NAMESRV_CONFIG requests, want 1 (one ns in fixture)", got)
	}
	req := c.nsLog.first(remoting.ReqUpdateNameSrvConfig)
	if req == nil {
		t.Fatal("no UPDATE_NAMESRV_CONFIG request reached the nameserver")
	}
	body := string(req.Body)
	if !strings.Contains(body, "listenPort=10915") {
		t.Fatalf("body = %q, want Properties text with listenPort=10915", body)
	}

	before := c.nsLog.count(remoting.ReqUpdateNameSrvConfig)
	requireNoError(t, "empty update", c.admin.UpdateNameServerConfig(map[string]string{}, 0))
	if after := c.nsLog.count(remoting.ReqUpdateNameSrvConfig); after != before {
		t.Fatalf("empty properties must not touch the network: %d -> %d", before, after)
	}
}

// GET_NAMESRV_CONFIG(319): one request per nameserver, the body is Properties
// TEXT parsed back into a map keyed by server address.
func TestAdminGetNameServerConfigParsesPropertiesBody(t *testing.T) {
	c := newAdminCluster(t)
	c.nsAnswer[remoting.ReqGetNameSrvConfig] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK([]byte("listenPort=9876\nserverWorkerThreads=8\n"))
	}
	got, err := c.admin.GetNameServerConfig(nil, 0)
	requireNoError(t, "getNameServerConfig", err)
	if len(got) != 1 {
		t.Fatalf("got config for %d nameservers, want 1: %v", len(got), got)
	}
	for _, cfg := range got {
		if cfg["listenPort"] != "9876" || cfg["serverWorkerThreads"] != "8" {
			t.Fatalf("parsed config = %v", cfg)
		}
	}
}

// A nameserver rejection surfaces as an error carrying the response code.
func TestAdminGetNameServerConfigRejectsFailure(t *testing.T) {
	c := newAdminCluster(t)
	c.nsAnswer[remoting.ReqGetNameSrvConfig] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "boom")
	}
	if _, err := c.admin.GetNameServerConfig(nil, 0); err == nil {
		t.Fatal("a failing nameserver must fail the call")
	}
}

func TestAdminConsumeMessageDirectlySendsHeaderAndParsesResult(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqConsumeMessageDirectly] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		result := remoting.NewConsumeMessageDirectlyResult()
		cr := remoting.CMResultSuccess
		result.ConsumeResult = &cr
		result.SpentTimeMills = 3
		return respOK(result.Encode())
	}
	got, err := c.admin.consumeMessageDirectlyTo(c.brokerSrv.addr, "GID", "cid@instance", "T", "msg-id")
	requireNoError(t, "consumeMessageDirectly", err)
	if got == nil || got.ConsumeResult == nil || *got.ConsumeResult != remoting.CMResultSuccess {
		t.Fatalf("decoded result = %+v, want CR_SUCCESS", got)
	}

	req := c.brokerLog.first(remoting.ReqConsumeMessageDirectly)
	if req == nil {
		t.Fatal("no CONSUME_MESSAGE_DIRECTLY request reached the broker")
	}
	checks := map[string]string{
		"consumerGroup": "GID",
		"clientId":      "cid@instance",
		"msgId":         "msg-id",
		"topic":         "T",
	}
	for k, want := range checks {
		if v := ext(t, req, k); v != want {
			t.Errorf("extField %s = %q, want %q", k, v, want)
		}
	}
}

// A broker rejection must surface, not decode into a zero result.
func TestAdminConsumeMessageDirectlyRejectsFailure(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqConsumeMessageDirectly] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "client offline")
	}
	if _, err := c.admin.consumeMessageDirectlyTo(c.brokerSrv.addr, "GID", "cid", "T", "msg-id"); err == nil {
		t.Fatal("a failing 309 must return the broker error")
	}
}

func storedOnBroker(t *testing.T, c *adminCluster) (raw []byte, msgID string) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(c.brokerSrv.addr)
	if err != nil {
		t.Fatalf("split mock broker addr %q: %v", c.brokerSrv.addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse mock broker port %q: %v", portStr, err)
	}
	stored := newQueryableExt(c.t, "MyTopic", 0, 7, "hello", "Key1", "MSGID0000000001")
	stored.StoreHost = host
	stored.StoreHostPort = uint32(port)
	raw = encodeStored(t, stored)
	decoded := common.DecodeMessages(raw)
	if len(decoded) != 1 {
		t.Fatalf("round trip produced %d messages", len(decoded))
	}
	return raw, decoded[0].MsgID
}

func TestAdminConsumeMessageDirectlyResolvesMsgIDToStoreHost(t *testing.T) {
	c := newAdminCluster(t)
	raw, msgID := storedOnBroker(t, c)
	c.brokerAnswer[remoting.ReqViewMessageByID] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(raw)
	}
	c.brokerAnswer[remoting.ReqConsumeMessageDirectly] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		result := remoting.NewConsumeMessageDirectlyResult()
		cr := remoting.CMResultSuccess
		result.ConsumeResult = &cr
		return respOK(result.Encode())
	}

	got, err := c.admin.ConsumeMessageDirectly("GID", "cid@instance", "MyTopic", msgID)
	requireNoError(t, "consumeMessageDirectly", err)
	if got == nil || got.ConsumeResult == nil || *got.ConsumeResult != remoting.CMResultSuccess {
		t.Fatalf("decoded result = %+v, want CR_SUCCESS", got)
	}

	codes := c.brokerLog.codes()
	if len(codes) != 2 ||
		codes[0] != remoting.ReqViewMessageByID ||
		codes[1] != remoting.ReqConsumeMessageDirectly {
		t.Fatalf("request order = %v, want [VIEW_MESSAGE_BY_ID, CONSUME_MESSAGE_DIRECTLY]", codes)
	}
	req := c.brokerLog.first(remoting.ReqConsumeMessageDirectly)
	checks := map[string]string{
		"consumerGroup": "GID",
		"clientId":      "cid@instance",
		"msgId":         msgID,
		"topic":         "MyTopic",
	}
	for k, want := range checks {
		if v := ext(t, req, k); v != want {
			t.Errorf("extField %s = %q, want %q", k, v, want)
		}
	}
}

func TestAdminConsumeMessageDirectlySwapsUniqKeyForOffsetMsgID(t *testing.T) {
	c := newAdminCluster(t)
	host, portStr, err := net.SplitHostPort(c.brokerSrv.addr)
	if err != nil {
		t.Fatalf("split mock broker addr %q: %v", c.brokerSrv.addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse mock broker port %q: %v", portStr, err)
	}
	stored := newQueryableExt(c.t, "MyTopic", 0, 7, "hello", "Key1", "MSGID0000000001")
	stored.StoreHost = host
	stored.StoreHostPort = uint32(port)
	stored.PutProperty(common.PropertyUniqKey, "UNIQ-1")
	raw := encodeStored(t, stored)
	decoded := common.DecodeMessages(raw)
	if len(decoded) != 1 {
		t.Fatalf("round trip produced %d messages", len(decoded))
	}
	offsetMsgID := decoded[0].MsgID

	// A caller id that is a VALID offset msgId for the same broker but points
	// at a different commitLog position — the 309 must carry the stored
	// message's own offset id, not this one.
	addrBytes, err := common.IPAndPortToBytes(host, uint32(port), false)
	if err != nil {
		t.Fatalf("encode store host: %v", err)
	}
	decoy := common.CreateMessageID(addrBytes, 42)
	if decoy == offsetMsgID {
		t.Fatal("decoy msgId collides with the stored one; the case proves nothing")
	}

	c.brokerAnswer[remoting.ReqViewMessageByID] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(raw)
	}
	c.brokerAnswer[remoting.ReqConsumeMessageDirectly] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		result := remoting.NewConsumeMessageDirectlyResult()
		cr := remoting.CMResultSuccess
		result.ConsumeResult = &cr
		return respOK(result.Encode())
	}

	if _, err := c.admin.ConsumeMessageDirectly("GID", "cid@instance", "MyTopic", decoy); err != nil {
		t.Fatalf("consumeMessageDirectly: %v", err)
	}
	req := c.brokerLog.first(remoting.ReqConsumeMessageDirectly)
	if got := ext(t, req, "msgId"); got != offsetMsgID {
		t.Errorf("309 msgId = %q, want the resolved offset msgId %q", got, offsetMsgID)
	}
}

func TestAdminConsumeMessageDirectlyWithoutResolutionSendsNothing(t *testing.T) {
	c := newAdminCluster(t)
	// The uniq-key fallback (12) finds nothing for this key.
	c.brokerAnswer[remoting.ReqQueryMessage] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespQueryNotFound, "no message")
	}
	if _, err := c.admin.ConsumeMessageDirectly("GID", "cid", "MyTopic", "no-such-msg-id"); err == nil {
		t.Fatal("an unresolvable msgId must fail the call")
	}
	if c.brokerLog.count(remoting.ReqConsumeMessageDirectly) != 0 {
		t.Fatal("no 309 may be sent for an unresolvable msgId")
	}
}
