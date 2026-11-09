package client

import (
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// The instance-level ViewMessage is Java MQAdminImpl#viewMessage
// (MQAdminImpl.java:267): exactly one VIEW_MESSAGE_BY_ID against the store
// host encoded in the offset msgId — no uniq-key fallback, no route traffic.
func TestInstanceViewMessageHitsOnlyTheStoreHostBroker(t *testing.T) {
	c := newAdminCluster(t)
	raw, msgID := storedOnBroker(t, c)
	c.brokerAnswer[remoting.ReqViewMessageByID] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(raw)
	}
	inst, err := c.admin.requireClient()
	if err != nil {
		t.Fatalf("resolve admin instance: %v", err)
	}

	got, err := inst.ViewMessage("MyTopic", msgID, 3000)
	requireNoError(t, "viewMessage", err)
	if got == nil || string(got.Body) != "hello" {
		t.Fatalf("decoded message = %+v, want body \"hello\"", got)
	}
	if got.MsgID != msgID {
		t.Errorf("round-trip MsgID = %q, want the offset id %q", got.MsgID, msgID)
	}
	req := c.brokerLog.first(remoting.ReqViewMessageByID)
	if req == nil {
		t.Fatal("no VIEW_MESSAGE_BY_ID reached the broker")
	}
	if got := ext(t, req, "topic"); got != "MyTopic" {
		t.Errorf("extField topic = %q, want MyTopic", got)
	}
	if codes := c.brokerLog.codes(); len(codes) != 1 {
		t.Fatalf("broker request log = %v, want exactly one VIEW_MESSAGE_BY_ID", codes)
	}
}

// A msgId that does not decode as an offset id maps to the NO_MESSAGE client
// error Java raises in MQAdminImpl, and must not touch the network — the
// uniq-key fallback belongs to the admin facade only.
func TestInstanceViewMessageDecodeFailureIsNoMessage(t *testing.T) {
	c := newAdminCluster(t)
	inst, err := c.admin.requireClient()
	if err != nil {
		t.Fatalf("resolve admin instance: %v", err)
	}

	got, err := inst.ViewMessage("MyTopic", "MSGID0000000001", 3000)
	if err == nil {
		t.Fatal("a uniq-key string must fail the single-leg viewMessage")
	}
	if !strings.Contains(err.Error(), "query message by id finished") {
		t.Errorf("error = %v, want the MQAdminImpl NO_MESSAGE wording", err)
	}
	if got != nil {
		t.Errorf("got message %+v on a decode failure", got)
	}
	if codes := c.brokerLog.codes(); len(codes) != 0 {
		t.Fatalf("broker request log = %v, want no traffic", codes)
	}
}

// The admin facade is Java DefaultMQAdminExtImpl#viewMessage: any leg-1
// failure ("the msgId maybe created by new client") falls back to the
// uniq-key query, and the fallback hit is returned.
func TestAdminViewMessageFallsBackToQueryWhenLeg1Fails(t *testing.T) {
	c := newAdminCluster(t)
	raw, msgID := storedOnBroker(t, c)
	c.brokerAnswer[remoting.ReqViewMessageByID] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespNoMessage, "message not found")
	}
	c.brokerAnswer[remoting.ReqQueryMessage] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(raw)
	}

	got, err := c.admin.ViewMessage("MyTopic", msgID)
	requireNoError(t, "viewMessage", err)
	if got == nil || string(got.Body) != "hello" {
		t.Fatalf("fallback message = %+v, want body \"hello\"", got)
	}
	codes := c.brokerLog.codes()
	if len(codes) != 2 ||
		codes[0] != remoting.ReqViewMessageByID ||
		codes[1] != remoting.ReqQueryMessage {
		t.Fatalf("request log = %v, want [VIEW_MESSAGE_BY_ID, QUERY_MESSAGE]", codes)
	}
}

// When neither leg matches, Java's admin viewMessage returns null instead of
// raising — the go facade mirrors that with (nil, nil).
func TestAdminViewMessageReturnsNilWhenNeitherLegMatches(t *testing.T) {
	c := newAdminCluster(t)
	_, msgID := storedOnBroker(t, c)
	c.brokerAnswer[remoting.ReqViewMessageByID] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespNoMessage, "message not found")
	}
	c.brokerAnswer[remoting.ReqQueryMessage] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespQueryNotFound, "")
	}

	got, err := c.admin.ViewMessage("MyTopic", msgID)
	if err != nil {
		t.Fatalf("a no-match viewMessage must not raise: %v", err)
	}
	if got != nil {
		t.Fatalf("got message %+v, want nil", got)
	}
}

// consumeMessageDirectly rides viewMessage; a no-match must surface as a
// clear client error, not a nil dereference (Java NPEs here unguarded).
func TestConsumeMessageDirectlyRejectsNoMatchMsgID(t *testing.T) {
	c := newAdminCluster(t)
	_, msgID := storedOnBroker(t, c)
	c.brokerAnswer[remoting.ReqViewMessageByID] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespNoMessage, "message not found")
	}
	c.brokerAnswer[remoting.ReqQueryMessage] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespQueryNotFound, "")
	}

	if _, err := c.admin.ConsumeMessageDirectly("GID", "cid@instance", "MyTopic", msgID); err == nil {
		t.Fatal("a no-match msgId must fail consumeMessageDirectly")
	}
}

// All three facades (producer / push / pull) ride the started client and
// reject a not-started consumer up front.
func TestViewMessageFacadesRequireStartedClient(t *testing.T) {
	p, err := NewDefaultMQProducer("G1")
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	if _, err := p.ViewMessage("T", "some-uniq-key"); err == nil {
		t.Error("producer: a not-started client must be rejected")
	} else if !strings.Contains(err.Error(), "not started") {
		t.Errorf("producer: error = %v", err)
	}

	push, err := NewDefaultMQPushConsumer("G1")
	if err != nil {
		t.Fatalf("new push consumer: %v", err)
	}
	if _, err := push.ViewMessage("T", "some-uniq-key"); err == nil {
		t.Error("push consumer: a not-started client must be rejected")
	} else if !strings.Contains(err.Error(), "not started") {
		t.Errorf("push consumer: error = %v", err)
	}

	pull, err := NewDefaultMQPullConsumer("G1")
	if err != nil {
		t.Fatalf("new pull consumer: %v", err)
	}
	if _, err := pull.ViewMessage("T", "some-uniq-key"); err == nil {
		t.Error("pull consumer: a not-started client must be rejected")
	} else if !strings.Contains(err.Error(), "not started") {
		t.Errorf("pull consumer: error = %v", err)
	}
}
