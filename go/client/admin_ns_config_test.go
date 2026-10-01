// Tests for the admin surface aligned with Java's
// updateNameServerConfig / getNameServerConfig / consumeMessageDirectly.
package client

import (
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// UPDATE_NAMESRV_CONFIG(318): the properties ride in the BODY as
// java.util.Properties TEXT, and the request reaches EVERY nameserver; the
// first failing nameserver decides the error (Java's errResponse handling).
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

	// An empty property set is a silent no-op, exactly as in Java.
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

// The admin-initiated CONSUME_MESSAGE_DIRECTLY(309) goes to the broker with
// consumerGroup/clientId/msgId/topic ext fields and decodes the verdict body.
func TestAdminConsumeMessageDirectlySendsHeaderAndParsesResult(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqConsumeMessageDirectly] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		result := remoting.NewConsumeMessageDirectlyResult()
		cr := remoting.CMResultSuccess
		result.ConsumeResult = &cr
		result.SpentTimeMills = 3
		return respOK(result.Encode())
	}
	got, err := c.admin.ConsumeMessageDirectly("GID", "cid@instance", "T", "msg-id", c.brokerSrv.addr)
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
	if _, err := c.admin.ConsumeMessageDirectly("GID", "cid", "T", "msg-id", c.brokerSrv.addr); err == nil {
		t.Fatal("a failing 309 must return the broker error")
	}
}
