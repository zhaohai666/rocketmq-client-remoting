// Request-reply and recallMessage tests: unit tests for the future/holder
// machinery plus producer round trips against the in-process mock cluster
// (the broker pushes the 326 reply back through the same connection).
package client

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- fixtures

type requestCallbackFunc struct {
	successFn func(*common.MessageExt)
	exceptFn  func(error)
}

func (c requestCallbackFunc) OnSuccess(response *common.MessageExt) {
	if c.successFn != nil {
		c.successFn(response)
	}
}

func (c requestCallbackFunc) OnException(cause error) {
	if c.exceptFn != nil {
		c.exceptFn(cause)
	}
}

// replyOnReq answers the SEND family like sendBrokerOnReq and, unless
// configured otherwise, pushes the 326 reply SYNCHRONOUSLY inside the handler
// (pushSync blocks until the client acks it, so the SEND response is written
// afterwards — no concurrent frame writes on this connection).
type replyOnReq struct {
	sendCode int32
	noReply  bool
}

func (h *replyOnReq) handle(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
	switch req.Code {
	case remoting.ReqSendMessageV2, remoting.ReqSendReplyMessageV2:
		if h.sendCode != 0 {
			return remoting.CreateResponseCommand(h.sendCode, "injected failure")
		}
		if !h.noReply {
			var header remoting.SendMessageRequestHeaderV2
			header.FromExtFields(req.ExtFields())
			props := common.String2MessageProperties(strValue(header.Properties))
			corr, _ := props.Get(common.PropertyCorrelationID)
			if _, err := s.pushSync(buildReplyCommand(corr, []byte("pong")), 5*time.Second); err != nil {
				s.fail("push reply: %v", err)
			}
		}
		resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
		resp.SetCustomHeader(&remoting.SendMessageResponseHeader{
			MsgID:         strPtr("0A0B0C0D000000000000000000000001"),
			QueueID:       i32Ptr(0),
			QueueOffset:   i64Ptr(7),
			TransactionID: strPtr("TID-1"),
		})
		return resp
	default:
		return remoting.CreateResponseCommand(remoting.RespSuccess, "")
	}
}

func buildReplyCommand(correlationID string, body []byte) *remoting.RemotingCommand {
	props := common.NewStringMap()
	props.Put(common.PropertyMessageType, common.ReplyMessageFlag)
	props.Put(common.PropertyCorrelationID, correlationID)
	props.Put(common.PropertyMessageReplyToClient, "mock-responder")
	props.Put(common.PropertyCluster, "DefaultCluster")
	props.Put(common.PropertyReplyMessageArriveTime, "1700000000000")
	cmd := remoting.CreateRequestCommand(remoting.ReqPushReplyMessageToClient, &remoting.ReplyMessageRequestHeader{
		Topic:         strPtr(common.GetReplyTopic("DefaultCluster")),
		ProducerGroup: strPtr("PID_REPLY"),
		SysFlag:       i32Ptr(0),
		Flag:          i32Ptr(0),
		BornTimestamp: i64Ptr(1700000000000),
		BornHost:      strPtr("127.0.0.1:10911"),
		Properties:    strPtr(common.MessageProperties2String(props)),
	})
	cmd.Body = body
	return cmd
}

// startProducerWithHandler wires one broker + one nameserver around a custom
// broker handler and starts a producer pointed at them.
func startProducerWithHandler(t *testing.T, topic, group string,
	onReq func(*mockServer, *remoting.RemotingCommand) *remoting.RemotingCommand,
) (*DefaultMQProducer, *mockServer) {
	t.Helper()
	broker := startMockServer(t, onReq)
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		topic: routeBodyFor(broker.addr, ""),
	}))
	p := MustNewDefaultMQProducer(group)
	p.SetNameServerAddresses([]string{ns.addr})
	p.SetInstanceName(uniqueClientID(t))
	p.SetSendMsgTimeout(3000)
	if err := p.Start(); err != nil {
		t.Fatalf("producer start: %v", err)
	}
	t.Cleanup(p.Shutdown)
	return p, broker
}

// ---------------------------------------------------------------- unit tests

func TestCreateCorrelationIDShape(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		id := CreateCorrelationID()
		if len(id) != 36 {
			t.Fatalf("correlationId %q has length %d, want 36", id, len(id))
		}
		if strings.Count(id, "-") != 4 {
			t.Fatalf("correlationId %q must have 4 dashes", id)
		}
		if id[14] != '4' {
			t.Fatalf("correlationId %q version nibble at [14] = %q, want '4'", id, id[14])
		}
		if id[19] != '8' && id[19] != '9' && id[19] != 'a' && id[19] != 'b' {
			t.Fatalf("correlationId %q variant nibble at [19] = %q", id, id[19])
		}
		for _, c := range id {
			if c == '-' {
				continue
			}
			if !strings.ContainsRune("0123456789abcdef", c) {
				t.Fatalf("correlationId %q contains non-hex %q", id, c)
			}
		}
		if seen[id] {
			t.Fatalf("correlationId %q generated twice", id)
		}
		seen[id] = true
	}
}

func TestCreateReplyMessageShapeAndClusterGuard(t *testing.T) {
	request := common.NewMessage("someTopic", nil)
	request.PutProperty(common.PropertyCluster, "DefaultCluster")
	request.PutProperty(common.PropertyMessageType, "request")
	request.PutProperty(common.PropertyCorrelationID, "CORR-1")
	request.PutProperty(common.PropertyMessageReplyToClient, "client-a")
	request.PutProperty(common.PropertyMessageTTL, "3000")

	reply, err := CreateReplyMessage(request, []byte("answer"))
	if err != nil {
		t.Fatalf("createReplyMessage: %v", err)
	}
	if reply.Topic != common.GetReplyTopic("DefaultCluster") {
		t.Fatalf("reply topic = %q, want %q", reply.Topic, common.GetReplyTopic("DefaultCluster"))
	}
	if string(reply.Body) != "answer" {
		t.Fatalf("reply body = %q", reply.Body)
	}
	if got, _ := reply.GetProperty(common.PropertyCorrelationID); got != "CORR-1" {
		t.Fatalf("reply correlationId = %q", got)
	}
	if got, _ := reply.GetProperty(common.PropertyMessageReplyToClient); got != "client-a" {
		t.Fatalf("reply REPLY_TO_CLIENT = %q", got)
	}
	if got, _ := reply.GetProperty(common.PropertyMessageTTL); got != "3000" {
		t.Fatalf("reply TTL = %q", got)
	}
	if got, _ := reply.GetProperty(common.PropertyMessageType); got != common.ReplyMessageFlag {
		t.Fatalf("reply MSG_TYPE = %q, want forced %q", got, common.ReplyMessageFlag)
	}

	_, err = CreateReplyMessage(common.NewMessage("t", nil), []byte("x"))
	if err == nil {
		t.Fatal("missing CLUSTER must fail the reply build")
	}
	if ce, ok := err.(*common.Error); !ok || ce.Code != common.CreateReplyMessageException {
		t.Fatalf("missing CLUSTER error = %v, want code %d", err, common.CreateReplyMessageException)
	}
}

func TestRequestResponseFutureStickyWakeAndOnceCallback(t *testing.T) {
	var mu sync.Mutex
	successes := 0
	f := newRequestResponseFuture("corr", 1000, requestCallbackFunc{
		successFn: func(*common.MessageExt) { mu.Lock(); successes++; mu.Unlock() },
	})

	early := common.NewMessageExt()
	early.Body = []byte("early")
	// The reply lands BEFORE anyone waits: the sticky check must still see it.
	f.PutResponseMessage(early)
	if got := f.WaitResponseMessage(1000); got == nil || string(got.Body) != "early" {
		t.Fatalf("WaitResponseMessage after early reply = %v", got)
	}
	f.ExecuteRequestCallback()
	f.ExecuteRequestCallback()
	mu.Lock()
	defer mu.Unlock()
	if successes != 1 {
		t.Fatalf("callback fired %d times, want exactly 1", successes)
	}
}

func TestRequestResponseFutureSetFailed(t *testing.T) {
	errCh := make(chan error, 1)
	f := newRequestResponseFuture("corr", 1000, requestCallbackFunc{
		exceptFn: func(err error) { errCh <- err },
	})
	boom := common.ClientError("boom")
	f.SetFailed(boom)
	f.ExecuteRequestCallback()
	if f.IsSendRequestOK() {
		t.Fatal("SetFailed must flip sendRequestOK")
	}
	if f.Cause() != boom {
		t.Fatalf("Cause() = %v, want %v", f.Cause(), boom)
	}
	if resp := f.WaitResponseMessage(500); resp != nil {
		t.Fatalf("WaitResponseMessage after SetFailed = %v, want nil", resp)
	}
	select {
	case got := <-errCh:
		if got != boom {
			t.Fatalf("callback cause = %v, want %v", got, boom)
		}
	default:
		t.Fatal("SetFailed must wake and fire the callback")
	}

	// A nil cause still records something (Java throws NPE-free generic).
	f2 := newRequestResponseFuture("corr2", 1000, nil)
	f2.SetFailed(nil)
	if f2.Cause() == nil {
		t.Fatal("SetFailed(nil) must record a generic cause")
	}
}

// The async timeout watchdog uses failWith: the send succeeded, so
// sendRequestOK must stay true while the cause still reaches the callback.
func TestRequestResponseFutureTimeoutInjectionKeepsSendOK(t *testing.T) {
	errCh := make(chan error, 1)
	f := newRequestResponseFuture("corr", 50, requestCallbackFunc{
		exceptFn: func(err error) { errCh <- err },
	})
	if resp := f.WaitResponseMessage(20); resp != nil {
		t.Fatalf("no reply expected, got %v", resp)
	}
	timeout := common.RequestTimeoutError("T", 50)
	f.failWith(timeout)
	f.ExecuteRequestCallback()
	if !f.IsSendRequestOK() {
		t.Fatal("failWith (timeout injection) must keep sendRequestOK true")
	}
	select {
	case got := <-errCh:
		if got != timeout {
			t.Fatalf("callback cause = %v, want %v", got, timeout)
		}
	default:
		t.Fatal("failWith must wake and fire the callback")
	}
}

func TestRequestFutureHolderPutResponseRemovesFirst(t *testing.T) {
	h := &RequestFutureHolder{table: map[string]*RequestResponseFuture{}}
	if h.PutResponse("nope", common.NewMessageExt()) != nil {
		t.Fatal("unmatched push must return nil")
	}
	if h.RemoveRequest("missing") != nil {
		t.Fatal("removing an absent key must return nil")
	}

	f := newRequestResponseFuture("corr", 1000, nil)
	h.PutRequest("corr", f)
	if got := h.PutResponse("corr", common.NewMessageExt()); got != f {
		t.Fatalf("matched push returned %v, want the future", got)
	}
	if _, ok := h.GetRequest("corr"); ok {
		t.Fatal("PutResponse must remove the future")
	}
	if h.PutResponse("corr", common.NewMessageExt()) != nil {
		t.Fatal("a duplicate push must land in the unmatched branch")
	}
	if h.PutResponse("", common.NewMessageExt()) != nil {
		t.Fatal("an empty correlationId never matches")
	}
	if h.Size() != 0 {
		t.Fatalf("holder size = %d, want 0", h.Size())
	}
}

// ---------------------------------------------------------------- producer

func TestProducerRequestReplyRoundTrip(t *testing.T) {
	const topic = "ReqReplyTopic"
	handler := &replyOnReq{}
	p, broker := startProducerWithHandler(t, topic, "GID_req_reply", handler.handle)

	msg := common.NewMessage(topic, []byte("ping"))
	resp, err := p.Request(msg)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	if string(resp.Body) != "pong" {
		t.Fatalf("reply body = %q, want pong", resp.Body)
	}
	if resp.Topic != common.GetReplyTopic("DefaultCluster") {
		t.Fatalf("reply topic = %q, want %q", resp.Topic, common.GetReplyTopic("DefaultCluster"))
	}
	if got, _ := resp.GetProperty(common.PropertyMessageType); got != common.ReplyMessageFlag {
		t.Fatalf("reply MSG_TYPE = %q", got)
	}
	if got, _ := resp.GetProperty(common.PropertyReplyMessageArriveTime); got == "" {
		t.Fatal("reply must carry REPLY_MESSAGE_ARRIVE_TIME")
	}

	reqs := broker.requests(remoting.ReqSendMessageV2)
	if len(reqs) != 1 {
		t.Fatalf("expected one SEND_MESSAGE_V2, got %d", len(reqs))
	}
	var header remoting.SendMessageRequestHeaderV2
	header.FromExtFields(reqs[0].ExtFields())
	props := common.String2MessageProperties(strValue(header.Properties))
	corr, _ := props.Get(common.PropertyCorrelationID)
	if corr == "" {
		t.Fatal("the request must carry a correlationId on the wire")
	}
	if got, _ := resp.GetProperty(common.PropertyCorrelationID); got != corr {
		t.Fatalf("reply correlationId = %q, want the echoed %q", got, corr)
	}
	if got, _ := props.Get(common.PropertyMessageReplyToClient); got != p.ClientID() {
		t.Fatalf("REPLY_TO_CLIENT = %q, want the producer clientId %q", got, p.ClientID())
	}
	if got, _ := props.Get(common.PropertyMessageTTL); got != "3000" {
		t.Fatalf("TTL = %q, want the request budget 3000", got)
	}
}

func TestProducerAsyncRequestReplyRoundTrip(t *testing.T) {
	const topic = "AsyncReqReplyTopic"
	handler := &replyOnReq{}
	p, _ := startProducerWithHandler(t, topic, "GID_async_req_reply", handler.handle)

	done := make(chan *common.MessageExt, 1)
	errCh := make(chan error, 1)
	callback := requestCallbackFunc{
		successFn: func(m *common.MessageExt) { done <- m },
		exceptFn:  func(err error) { errCh <- err },
	}
	if err := p.AsyncRequest(common.NewMessage(topic, []byte("ping")), callback); err != nil {
		t.Fatalf("async request: %v", err)
	}
	select {
	case resp := <-done:
		if string(resp.Body) != "pong" {
			t.Fatalf("reply body = %q, want pong", resp.Body)
		}
	case err := <-errCh:
		t.Fatalf("async request failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the reply callback")
	}
}

func TestProducerRequestTimesOutWithoutReply(t *testing.T) {
	const topic = "ReqTimeoutTopic"
	handler := &replyOnReq{noReply: true}
	p, _ := startProducerWithHandler(t, topic, "GID_req_timeout", handler.handle)

	_, err := p.RequestWithTimeout(common.NewMessage(topic, []byte("ping")), 300)
	if err == nil {
		t.Fatal("expected a request timeout")
	}
	if !common.IsKind(err, common.KindRequestTimeout) {
		t.Fatalf("error = %v, want KindRequestTimeout", err)
	}
	if e, ok := err.(*common.Error); ok && e.Topic != topic {
		t.Fatalf("timeout error topic = %q, want %q", e.Topic, topic)
	}
}

func TestProducerRequestSendFailureSurfaces(t *testing.T) {
	const topic = "ReqSendFailTopic"
	handler := &replyOnReq{sendCode: remoting.RespMessageIllegal}
	p, _ := startProducerWithHandler(t, topic, "GID_req_send_fail", handler.handle)

	_, err := p.Request(common.NewMessage(topic, []byte("ping")))
	if err == nil {
		t.Fatal("expected the send failure to surface")
	}
	// A failed send is NOT a request timeout: Java wraps it as
	// "send request message to <topic> fail" with the cause attached.
	if !strings.Contains(err.Error(), "send request message to <"+topic+"> fail") {
		t.Fatalf("error = %v, want the send-request wrapper", err)
	}
	if e, ok := err.(*common.Error); ok && e.Cause == nil {
		t.Fatal("the send failure must be attached as the cause")
	}
}

// ---------------------------------------------------------------- recall

type recallOnReq struct {
	refuse bool
}

func (h *recallOnReq) handle(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
	switch req.Code {
	case remoting.ReqRecallMessage:
		if h.refuse {
			return remoting.CreateResponseCommand(remoting.RespNoPermission, "recallMessageEnable is false")
		}
		resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
		resp.SetCustomHeader(&remoting.RecallMessageResponseHeader{MsgID: strPtr("UNIQ-RECALLED-1")})
		return resp
	default:
		return remoting.CreateResponseCommand(remoting.RespSuccess, "")
	}
}

func TestProducerRecallMessageRoundTrip(t *testing.T) {
	const topic = "RecallTopic"
	handler := &recallOnReq{}
	p, broker := startProducerWithHandler(t, topic, "GID_recall", handler.handle)

	handle := common.BuildRecallHandle(topic, "b1", "1700000000000", "UNIQKEY123")
	got, err := p.RecallMessage(topic, handle)
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if got != "UNIQ-RECALLED-1" {
		t.Fatalf("recall msgId = %q, want UNIQ-RECALLED-1", got)
	}

	reqs := broker.requests(remoting.ReqRecallMessage)
	if len(reqs) != 1 {
		t.Fatalf("expected one RECALL_MESSAGE(370), got %d", len(reqs))
	}
	var header remoting.RecallMessageRequestHeader
	header.FromExtFields(reqs[0].ExtFields())
	if strValue(header.ProducerGroup) != "GID_recall" {
		t.Fatalf("producerGroup = %q", strValue(header.ProducerGroup))
	}
	if strValue(header.Topic) != topic {
		t.Fatalf("topic = %q", strValue(header.Topic))
	}
	if strValue(header.RecallHandle) != handle {
		t.Fatalf("recallHandle = %q, want the handle echoed", strValue(header.RecallHandle))
	}
	if strValue(header.Bname) != "b1" {
		t.Fatalf("bname = %q, want the handle's broker name", strValue(header.Bname))
	}
}

func TestProducerRecallRejectsBadHandleAndTopics(t *testing.T) {
	const topic = "RecallRejectTopic"
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		topic: routeBodyFor("127.0.0.1:1", ""),
	}))
	p := MustNewDefaultMQProducer("GID_recall_reject")
	p.SetNameServerAddresses([]string{ns.addr})
	p.SetInstanceName(uniqueClientID(t))
	if err := p.Start(); err != nil {
		t.Fatalf("producer start: %v", err)
	}
	t.Cleanup(p.Shutdown)

	if _, err := p.RecallMessage(topic, "garbage"); err == nil || !strings.Contains(err.Error(), common.RecallHandleInvalid) {
		t.Fatalf("bad handle error = %v, want %q", err, common.RecallHandleInvalid)
	}
	if _, err := p.RecallMessage(topic, ""); err == nil || !strings.Contains(err.Error(), common.RecallHandleInvalid) {
		t.Fatalf("empty handle error = %v, want %q", err, common.RecallHandleInvalid)
	}
	// %RETRY%/%DLQ% rejection runs BEFORE the handle decode: a bad handle on a
	// retry topic must report the topic, not the handle.
	if _, err := p.RecallMessage("%RETRY%GID_recall_reject", "garbage"); err == nil || !strings.Contains(err.Error(), "topic is not supported") {
		t.Fatalf("retry topic error = %v, want \"topic is not supported\"", err)
	}
	if _, err := p.RecallMessage("%DLQ%GID_recall_reject", ""); err == nil || !strings.Contains(err.Error(), "topic is not supported") {
		t.Fatalf("dlq topic error = %v, want \"topic is not supported\"", err)
	}
}

func TestProducerRecallBrokerRefusal(t *testing.T) {
	const topic = "RecallRefuseTopic"
	handler := &recallOnReq{refuse: true}
	p, _ := startProducerWithHandler(t, topic, "GID_recall_refuse", handler.handle)

	handle := common.BuildRecallHandle(topic, "b1", "1700000000000", "UNIQKEY123")
	_, err := p.RecallMessage(topic, handle)
	if err == nil {
		t.Fatal("expected the broker's NO_PERMISSION refusal")
	}
	if !common.IsKind(err, common.KindBroker) {
		t.Fatalf("error = %v, want KindBroker", err)
	}
}
