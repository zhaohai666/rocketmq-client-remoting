// Producer tests against the in-process mock cluster from instance_test.go:
// one mock nameserver serving route bodies plus per-broker mock servers that
// record every request (and can push broker->client requests back).
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

// twoBrokerRouteBody is routeBodyFor's two-broker sibling: the retry tests need
// a second broker to escape to. Queue order is deterministic —
// b1q0..b1q3, b2q0..b2q3 (TopicRouteData.GetAllMessageQueue iterates
// queueDatas in order).
func twoBrokerRouteBody(master1, master2 string) string {
	return `{"orderTopicConf":null,` +
		`"queueDatas":[` +
		`{"brokerName":"b1","readQueueNums":4,"writeQueueNums":4,"perm":6,"topicSysFlag":0},` +
		`{"brokerName":"b2","readQueueNums":4,"writeQueueNums":4,"perm":6,"topicSysFlag":0}],` +
		`"brokerDatas":[` +
		`{"cluster":"DefaultCluster","brokerName":"b1","brokerAddrs":{"0":"` + master1 + `"},"zoneName":null,"enableActingMaster":false},` +
		`{"cluster":"DefaultCluster","brokerName":"b2","brokerAddrs":{"0":"` + master2 + `"},"zoneName":null,"enableActingMaster":false}],` +
		`"filterServerTable":{}}`
}

// sendBrokerOnReq answers the SEND family with a Java-shaped success response
// and everything else with the given status. sendCode (non-zero) overrides the
// SEND answer so the retry/error paths can be exercised.
type sendBrokerOnReq struct {
	sendCode int32
	offset   int64
	queued   func(count int)
	count    int
}

func (h *sendBrokerOnReq) handle(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
	switch req.Code {
	case remoting.ReqSendMessageV2, remoting.ReqSendBatchMessage, remoting.ReqSendReplyMessageV2:
		h.count++
		if h.queued != nil {
			h.queued(h.count)
		}
		if h.sendCode != 0 {
			return remoting.CreateResponseCommand(h.sendCode, "injected failure")
		}
		var header remoting.SendMessageRequestHeaderV2
		header.FromExtFields(req.ExtFields())
		resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
		queueID := int32(0)
		if header.QueueID != nil {
			queueID = *header.QueueID
		}
		resp.SetCustomHeader(&remoting.SendMessageResponseHeader{
			MsgID:         strPtr("0A0B0C0D000000000000000000000001"),
			QueueID:       i32Ptr(queueID),
			QueueOffset:   i64Ptr(h.offset),
			TransactionID: strPtr("TID-1"),
		})
		return resp
	default:
		return remoting.CreateResponseCommand(remoting.RespSuccess, "")
	}
}

// startProducerFixture wires one broker + one nameserver and starts a producer
// pointed at them. The nameserver is created first so the broker address is
// known when its route body is built.
func startProducerFixture(t *testing.T, topic, group string) (*DefaultMQProducer, *mockServer, *sendBrokerOnReq) {
	t.Helper()
	handler := &sendBrokerOnReq{offset: 7}
	broker := startMockServer(t, handler.handle)
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
	return p, broker, handler
}

func headerOf(t *testing.T, req *remoting.RemotingCommand) *remoting.SendMessageRequestHeaderV2 {
	t.Helper()
	h := &remoting.SendMessageRequestHeaderV2{}
	h.FromExtFields(req.ExtFields())
	return h
}

// ---------------------------------------------------------------- tests

func TestProducerSendSyncHeaderShapeAndResult(t *testing.T) {
	const topic = "ProdShapeTopic"
	p, broker, _ := startProducerFixture(t, topic, "GID_prod_shape")

	msg := common.NewMessage(topic, []byte("hello"))
	msg.SetKeys("k1")
	msg.SetTags("t1")
	result, err := p.Send(msg)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	reqs := broker.requests(remoting.ReqSendMessageV2)
	if len(reqs) != 1 {
		t.Fatalf("expected exactly one SEND_MESSAGE_V2(310), got %d", len(reqs))
	}
	req := reqs[0]
	h := headerOf(t, req)

	// V2 single-letter keys: a=group b=topic c=defaultTopic d=defaultQueueNums
	// e=queueId f=sysFlag g=bornTimestamp h=flag i=properties j=reconsumeTimes
	// k=unitMode m=batch n=brokerName
	if got := *h.ProducerGroup; got != "GID_prod_shape" {
		t.Errorf("a(producerGroup) = %q", got)
	}
	if got := *h.Topic; got != topic {
		t.Errorf("b(topic) = %q", got)
	}
	if got := *h.DefaultTopic; got != common.DefaultTopic {
		t.Errorf("c(defaultTopic) = %q, want %q", got, common.DefaultTopic)
	}
	if got := *h.DefaultTopicQueueNums; got != common.DefaultTopicQueueNums {
		t.Errorf("d(defaultTopicQueueNums) = %d", got)
	}
	if h.SysFlag == nil || *h.SysFlag != 0 {
		t.Errorf("f(sysFlag) = %v, want 0 (small uncompressed body)", h.SysFlag)
	}
	if h.BornTimestamp == nil || *h.BornTimestamp <= 0 {
		t.Errorf("g(bornTimestamp) must be a real clock value, got %v", h.BornTimestamp)
	}
	if got := *h.ReconsumeTimes; got != 0 {
		t.Errorf("j(reconsumeTimes) = %d, want 0", got)
	}
	if got := *h.Batch; got {
		t.Errorf("m(batch) = true for a single message")
	}
	// BrokerName is the V2 key "n" and must name the SELECTED broker — the
	// broker uses it, and a wrong value silently targets another cluster's queue.
	if got := *h.BrokerName; got != "b1" {
		t.Errorf("n(brokerName) = %q, want b1", got)
	}
	// maxReconsumeTimes must stay absent for a non-retry topic (Java only
	// hoists it for %RETRY%), otherwise a >= 3.4.9 broker takes it literally.
	if _, ok := req.GetExtField("l"); ok {
		t.Errorf("l(maxReconsumeTimes) must not be sent for topic %q", topic)
	}
	if !strings.Contains(*h.Properties, common.PropertyUniqKey+"\x01") {
		t.Errorf("properties must carry UNIQ_KEY, got %q", *h.Properties)
	}
	if !strings.Contains(*h.Properties, common.PropertyTags+"\x01t1") {
		t.Errorf("properties must carry TAGS, got %q", *h.Properties)
	}
	if string(req.Body) != "hello" {
		t.Errorf("body = %q", req.Body)
	}

	// msgId is the client UNIQ_KEY, offsetMsgId is the broker's id: swapping
	// them breaks trace/console correlation without failing anything.
	uniqID, ok := common.GetUniqID(msg)
	if !ok {
		t.Fatal("the sent message must carry UNIQ_KEY")
	}
	if result.MsgID != uniqID {
		t.Errorf("result.MsgID = %q, want the UNIQ_KEY %q", result.MsgID, uniqID)
	}
	if result.OffsetMsgID != "0A0B0C0D000000000000000000000001" {
		t.Errorf("result.OffsetMsgID = %q, want the header msgId", result.OffsetMsgID)
	}
	if result.SendStatus != SendOK {
		t.Errorf("send status = %s", result.SendStatus)
	}
	if result.QueueOffset != 7 {
		t.Errorf("queueOffset = %d, want 7", result.QueueOffset)
	}
	if result.TransactionID != "TID-1" {
		t.Errorf("transactionID = %q", result.TransactionID)
	}
	// MSG_REGION absent -> DefaultRegion; TRACE_ON absent -> true.
	if result.RegionID != common.DefaultTraceRegionID {
		t.Errorf("regionID = %q, want %q", result.RegionID, common.DefaultTraceRegionID)
	}
	if !result.TraceOn {
		t.Error("traceOn must default to true when the header omits TRACE_ON")
	}
	if result.MessageQueue.BrokerName != "b1" || result.MessageQueue.Topic != topic {
		t.Errorf("result queue = %v", result.MessageQueue)
	}
}

func TestProducerCompressesOnceAndRestoresCallerMessage(t *testing.T) {
	const topic = "ProdCompressTopic"
	p, broker, _ := startProducerFixture(t, topic, "GID_prod_compress")

	// Java compresses when body.length >= compressMsgBodyOverHowmuch.
	original := []byte(strings.Repeat("a", DefaultCompressMsgBodyOverHowmuch+100))
	msg := common.NewMessage(topic, original)
	if _, err := p.Send(msg); err != nil {
		t.Fatalf("send: %v", err)
	}

	reqs := broker.requests(remoting.ReqSendMessageV2)
	if len(reqs) != 1 {
		t.Fatalf("expected 1 send, got %d", len(reqs))
	}
	h := headerOf(t, reqs[0])
	if h.SysFlag == nil {
		t.Fatal("sysFlag missing")
	}
	if !common.IsCompressed(*h.SysFlag) {
		t.Fatalf("sysFlag %#x lacks the COMPRESSED bit", *h.SysFlag)
	}
	if got := common.GetCompressionType(*h.SysFlag); got != common.ZlibType {
		t.Errorf("compression type = %d, want ZLIB(%d)", got, common.ZlibType)
	}
	// The wire body must be the single compressed layer, decompressing back to
	// the original. A double compression would still decompress one level and
	// hand the consumer a compressed stream.
	if len(reqs[0].Body) >= len(original) {
		t.Errorf("body was not compressed: %d -> %d bytes", len(original), len(reqs[0].Body))
	}
	back, err := common.Decompress(reqs[0].Body, common.ZlibType)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if string(back) != string(original) {
		t.Errorf("decompressed body mismatch: %d vs %d bytes", len(back), len(original))
	}
	// Java's sendKernelImpl finally clause restores the caller's message.
	if string(msg.Body) != string(original) {
		t.Errorf("caller body must be restored after send, got %d bytes", len(msg.Body))
	}
}

func TestProducerPinnedTopicMismatchIsRejectedLocally(t *testing.T) {
	const topic = "ProdPinTopic"
	p, broker, _ := startProducerFixture(t, topic, "GID_prod_pin")

	msg := common.NewMessage(topic, []byte("x"))
	_, err := p.SendToQueue(msg, common.NewMessageQueue("AnotherTopic", "b1", 0))
	if err == nil {
		t.Fatal("a mismatched pinned topic must be rejected")
	}
	if !strings.Contains(err.Error(), pinnedTopicMismatchSync) {
		t.Errorf("error = %v, want the sync guard text", err)
	}
	// The rejection happens before any network I/O.
	if got := len(broker.requests(remoting.ReqSendMessageV2)); got != 0 {
		t.Errorf("no request must go out on a pinned-topic mismatch, got %d", got)
	}
}

func TestProducerBatchEncodingAndPerMessageUniqKeys(t *testing.T) {
	const topic = "ProdBatchTopic"
	p, broker, _ := startProducerFixture(t, topic, "GID_prod_batch")

	first := common.NewMessage(topic, []byte("m1"))
	second := common.NewMessage(topic, []byte("m2"))
	result, err := p.SendBatch([]*common.Message{first, second})
	if err != nil {
		t.Fatalf("batch send: %v", err)
	}

	reqs := broker.requests(remoting.ReqSendBatchMessage)
	if len(reqs) != 1 {
		t.Fatalf("expected 1 SEND_BATCH_MESSAGE(320), got %d", len(reqs))
	}
	h := headerOf(t, reqs[0])
	if h.Batch == nil || !*h.Batch {
		t.Error("m(batch) must be true for a batch send")
	}
	// Batches are never compressed (tryToCompressMessage returns 0 for them).
	if h.SysFlag == nil || *h.SysFlag != 0 {
		t.Errorf("f(sysFlag) = %v, want 0 for a batch", h.SysFlag)
	}
	// Every sub-message must carry its own UNIQ_KEY, else the consumer sees
	// clients ids missing on all of them.
	for i, m := range []*common.Message{first, second} {
		if _, ok := common.GetUniqID(m); !ok {
			t.Errorf("sub-message %d has no UNIQ_KEY", i)
		}
	}
	// The batch body decodes into the two sub-messages, ids included.
	decoded := decodeBatchBody(t, reqs[0].Body)
	if len(decoded) != 2 {
		t.Fatalf("decoded %d sub-messages, want 2", len(decoded))
	}
	if string(decoded[0].Body) != "m1" || string(decoded[1].Body) != "m2" {
		t.Errorf("sub-message bodies = %q, %q", decoded[0].Body, decoded[1].Body)
	}
	for i, m := range decoded {
		if _, ok := common.GetUniqID(m); !ok {
			t.Errorf("decoded sub-message %d lost its UNIQ_KEY", i)
		}
	}
	if result.SendStatus != SendOK {
		t.Errorf("status = %s", result.SendStatus)
	}
}

// decodeBatchBody splits a batch body into its per-message light frames. Java's
// batch unit is the compact 6-segment form (MessageDecoder.decodeMessage, NOT
// the 17-segment store format), so DecodeBatchMessage is the matching decoder.
func decodeBatchBody(t *testing.T, body []byte) []*common.Message {
	t.Helper()
	var out []*common.Message
	rest := body
	for len(rest) > 0 {
		var size int32
		if err := readI32(rest, &size); err != nil {
			t.Fatalf("batch frame size: %v", err)
		}
		if size <= 0 || int(size) > len(rest) {
			t.Fatalf("bad batch frame size %d (remaining %d)", size, len(rest))
		}
		frame := rest[:size]
		msg, err := common.DecodeBatchMessage(frame)
		if err != nil {
			t.Fatalf("decode sub-message: %v", err)
		}
		out = append(out, msg)
		rest = rest[size:]
	}
	return out
}

func readI32(data []byte, out *int32) error {
	if len(data) < 4 {
		return common.DecodeError("short frame")
	}
	*out = int32(uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3]))
	return nil
}

func TestProducerRetrySkipsTheFailingBroker(t *testing.T) {
	const topic = "ProdRetryTopic"
	first := &sendBrokerOnReq{offset: 1, sendCode: remoting.RespSystemBusy}
	broker1 := startMockServer(t, first.handle)
	second := &sendBrokerOnReq{offset: 2}
	broker2 := startMockServer(t, second.handle)
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		topic: twoBrokerRouteBody(broker1.addr, broker2.addr),
	}))

	p := MustNewDefaultMQProducer("GID_prod_retry")
	p.SetNameServerAddresses([]string{ns.addr})
	p.SetInstanceName(uniqueClientID(t))
	if err := p.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(p.Shutdown)

	result, err := p.Send(common.NewMessage(topic, []byte("retry me")))
	if err != nil {
		t.Fatalf("send must succeed on the second broker: %v", err)
	}
	if result.SendStatus != SendOK {
		t.Errorf("status = %s", result.SendStatus)
	}
	// SYSTEM_BUSY(2) is in the retryable set, so attempt 1 goes to b1 and
	// attempt 2 must land on b2 (the round-robin filter skips lastBrokerName).
	if got := len(broker1.requests(remoting.ReqSendMessageV2)); got != 1 {
		t.Errorf("broker1 received %d sends, want 1", got)
	}
	if got := len(broker2.requests(remoting.ReqSendMessageV2)); got != 1 {
		t.Errorf("broker2 received %d sends, want 1 (retry must move brokers)", got)
	}
}

func TestProducerDoesNotRetryNonRetryableBrokerCode(t *testing.T) {
	const topic = "ProdNoRetryTopic"
	// MESSAGE_ILLEGAL(13) is NOT in Java's retryResponseCodes: retrying would
	// fail identically everywhere, so it must surface immediately.
	handler := &sendBrokerOnReq{offset: 1, sendCode: remoting.RespMessageIllegal}
	broker := startMockServer(t, handler.handle)
	ns := startMockServer(t, nameserverOnReq(map[string]string{topic: routeBodyFor(broker.addr, "")}))

	p := MustNewDefaultMQProducer("GID_prod_noretry")
	p.SetNameServerAddresses([]string{ns.addr})
	p.SetInstanceName(uniqueClientID(t))
	if err := p.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(p.Shutdown)

	_, err := p.Send(common.NewMessage(topic, []byte("x")))
	if err == nil {
		t.Fatal("expected the broker error to surface")
	}
	code, ok := responseCodeOf(err)
	if !ok || code != remoting.RespMessageIllegal {
		t.Errorf("error code = (%d,%v), want %d", code, ok, remoting.RespMessageIllegal)
	}
	if got := len(broker.requests(remoting.ReqSendMessageV2)); got != 1 {
		t.Errorf("a non-retryable code must not be retried, got %d sends", got)
	}
}

func TestProducerValidatorsRejectBeforeAnyRequest(t *testing.T) {
	const topic = "ProdValidatorTopic"
	p, broker, _ := startProducerFixture(t, topic, "GID_prod_validator")

	// Empty body -> MESSAGE_ILLEGAL(13), purely local.
	if _, err := p.Send(common.NewMessage(topic, []byte{})); err == nil {
		t.Fatal("an empty body must be rejected")
	} else if code, ok := responseCodeOf(err); !ok || code != common.ResponseCodeMessageIllegal {
		t.Errorf("empty body error = (%d,%v), want 13", code, ok)
	}
	if _, err := p.Send(common.NewMessage("bad.topic", []byte("x"))); err == nil {
		t.Fatal("an illegal topic must be rejected")
	} else if code, ok := responseCodeOf(err); ok {
		t.Errorf("illegal topic must be a codeless client error, got code %d", code)
	}
	if got := len(broker.requests(remoting.ReqSendMessageV2)); got != 0 {
		t.Errorf("local validation failures must not reach the broker, got %d sends", got)
	}
}

func TestProducerRequiresGroupAndNameServer(t *testing.T) {
	// A blank group fails in the constructor (Java: producerGroup is empty).
	if _, err := NewDefaultMQProducer("  "); err == nil {
		t.Error("a blank producer group must be rejected")
	}
	// The default group is a start-time rejection: several processes sharing it
	// would kick each other offline.
	def := MustNewDefaultMQProducer(common.DefaultProducerGroup)
	def.SetNameServerAddresses([]string{"127.0.0.1:9876"})
	if err := def.Start(); err == nil {
		t.Error("DEFAULT_PRODUCER must be refused by Start")
	}
	def.Shutdown()

	// No nameserver is a start-time rejection carrying Java's 10004.
	noNS := MustNewDefaultMQProducer("GID_prod_nons")
	if err := noNS.Start(); err == nil {
		t.Fatal("Start without a nameserver must fail")
	} else if code, ok := responseCodeOf(err); !ok || code != common.NoNameServerException {
		t.Errorf("error = (%d,%v), want %d", code, ok, common.NoNameServerException)
	}
}

func TestProducerOnewayAndReplyCodes(t *testing.T) {
	const topic = "ProdOnewayTopic"
	p, broker, _ := startProducerFixture(t, topic, "GID_prod_oneway")

	if err := p.SendOneway(common.NewMessage(topic, []byte("fire")), nil); err != nil {
		t.Fatalf("oneway: %v", err)
	}
	waitFor(t, "the oneway send to arrive", func() bool {
		return len(broker.requests(remoting.ReqSendMessageV2)) == 1
	})
	req := broker.requests(remoting.ReqSendMessageV2)[0]
	if !req.IsOnewayRPC() {
		t.Error("a oneway send must carry the ONEWAY flag")
	}

	// MSG_TYPE == "reply" must use SEND_REPLY_MESSAGE_V2(325): the broker only
	// registers ReplyMessageProcessor for 324/325.
	reply := common.NewMessage(topic, []byte("answer"))
	reply.PutProperty(common.PropertyMessageType, common.ReplyMessageFlag)
	if _, err := p.Send(reply); err != nil {
		t.Fatalf("reply send: %v", err)
	}
	if got := len(broker.requests(remoting.ReqSendReplyMessageV2)); got != 1 {
		t.Errorf("reply message used %d request(s) of code 325, want 1", got)
	}
}

func TestProducerRetryTopicHoistsReconsumeTimes(t *testing.T) {
	// %RETRY% routes are publishable (sendMessageBack writes into them) even
	// though the topic of this fixture registered no route for it — the retry
	// topic resolves through the same nameserver body.
	const baseTopic = "ProdRetryHoist"
	handler := &sendBrokerOnReq{offset: 1}
	broker := startMockServer(t, handler.handle)
	retryTopic := common.RetryGroupTopicPrefix + "GID_prod_hoist"
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		baseTopic:  routeBodyFor(broker.addr, ""),
		retryTopic: routeBodyFor(broker.addr, ""),
	}))
	p := MustNewDefaultMQProducer("GID_prod_hoist")
	p.SetNameServerAddresses([]string{ns.addr})
	p.SetInstanceName(uniqueClientID(t))
	if err := p.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(p.Shutdown)

	msg := common.NewMessage(retryTopic, []byte("retried"))
	msg.PutProperty(common.PropertyReconsumeTime, "3")
	msg.PutProperty(common.PropertyMaxReconsumeTimes, "2")
	if _, err := p.Send(msg); err != nil {
		t.Fatalf("send: %v", err)
	}
	reqs := broker.requests(remoting.ReqSendMessageV2)
	if len(reqs) != 1 {
		t.Fatalf("expected 1 send, got %d", len(reqs))
	}
	h := headerOf(t, reqs[0])
	// Hoisted into the header: the broker's handleRetryAndDLQ reads
	// requestHeader.reconsumeTimes / maxReconsumeTimes, not the properties.
	if h.ReconsumeTimes == nil || *h.ReconsumeTimes != 3 {
		t.Errorf("j(reconsumeTimes) = %v, want 3", h.ReconsumeTimes)
	}
	if h.MaxReconsumeTimes == nil || *h.MaxReconsumeTimes != 2 {
		t.Errorf("l(maxReconsumeTimes) = %v, want 2", h.MaxReconsumeTimes)
	}
	// The clear happens AFTER the properties were serialized (Java's order), so
	// the wire property is still present.
	if !strings.Contains(*h.Properties, common.PropertyReconsumeTime+"\x013") {
		t.Errorf("properties = %q, want RECONSUME_TIME=3 preserved", *h.Properties)
	}
}

// ---------------------------------------------------------------- transaction

// recordingTransactionListener counts calls from two goroutines: the test
// goroutine polls the counters while the producer's check-back goroutine writes
// them, so every field is mutex-guarded (the race detector flags it otherwise).
type recordingTransactionListener struct {
	mu           sync.Mutex
	executeCalls int
	checkCalls   int
	executeState LocalTransactionState
	checkState   LocalTransactionState
	lastCheckMsg *common.MessageExt
}

func (l *recordingTransactionListener) ExecuteLocalTransaction(*common.Message, any) LocalTransactionState {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.executeCalls++
	return l.executeState
}

func (l *recordingTransactionListener) CheckLocalTransaction(msg *common.MessageExt) LocalTransactionState {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.checkCalls++
	l.lastCheckMsg = msg
	return l.checkState
}

func (l *recordingTransactionListener) snapshot() (executeCalls, checkCalls int, last *common.MessageExt) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.executeCalls, l.checkCalls, l.lastCheckMsg
}

func TestProducerTransactionTwoPhaseAndCheckBack(t *testing.T) {
	const topic = "ProdTxTopic"
	const group = "GID_prod_tx"
	p, broker, _ := startProducerFixture(t, topic, group)

	listener := &recordingTransactionListener{
		executeState: CommitMessage,
		checkState:   RollbackMessage,
	}
	msg := common.NewMessage(topic, []byte("tx body"))
	result, err := p.SendMessageInTransaction(msg, listener, "arg")
	if err != nil {
		t.Fatalf("transaction send: %v", err)
	}
	if executeCalls, _, _ := listener.snapshot(); executeCalls != 1 {
		t.Fatalf("executeLocalTransaction called %d times, want 1", executeCalls)
	}
	if result.LocalTransactionState != CommitMessage {
		t.Errorf("local state = %s", result.LocalTransactionState)
	}

	// Phase 1: the half message. TRAN_MSG/PGROUP must be on the wire (the
	// broker routes on them) and the sysFlag must carry the transaction bits.
	reqs := broker.requests(remoting.ReqSendMessageV2)
	if len(reqs) != 1 {
		t.Fatalf("expected 1 half-message send, got %d", len(reqs))
	}
	h := headerOf(t, reqs[0])
	if !strings.Contains(*h.Properties, common.PropertyTransactionPrepared+"\x01true") {
		t.Errorf("half message must be marked TRAN_MSG=true, properties = %q", *h.Properties)
	}
	if !strings.Contains(*h.Properties, common.PropertyProducerGroup+"\x01"+group) {
		t.Errorf("half message must carry PGROUP=%s, properties = %q", group, *h.Properties)
	}
	if h.SysFlag == nil {
		t.Fatal("sysFlag missing on the half message")
	}
	if got := common.GetTransactionValue(*h.SysFlag); got != common.MessageSysFlagTransactionPrepared {
		t.Errorf("sysFlag transaction bits = %d, want PREPARED(%d)", got, common.MessageSysFlagTransactionPrepared)
	}

	// Phase 2: END_TRANSACTION(37) oneway with COMMIT.
	waitFor(t, "END_TRANSACTION after the local commit", func() bool {
		return len(broker.requests(remoting.ReqEndTransaction)) >= 1
	})
	endReqs := broker.requests(remoting.ReqEndTransaction)
	endHeader := &remoting.EndTransactionRequestHeader{}
	endHeader.FromExtFields(endReqs[0].ExtFields())
	if endHeader.CommitOrRollback == nil || *endHeader.CommitOrRollback != common.MessageSysFlagTransactionCommit {
		t.Errorf("commitOrRollback = %v, want %d", endHeader.CommitOrRollback, common.MessageSysFlagTransactionCommit)
	}
	if endHeader.FromTransactionCheck == nil || *endHeader.FromTransactionCheck {
		t.Errorf("fromTransactionCheck = %v, want false", endHeader.FromTransactionCheck)
	}
	if endHeader.ProducerGroup == nil || *endHeader.ProducerGroup != group {
		t.Errorf("producerGroup = %v", endHeader.ProducerGroup)
	}
	if !endReqs[0].IsOnewayRPC() {
		t.Error("END_TRANSACTION is a oneway request")
	}

	// Phase 3: the broker checks the half message back. It is a ONEWAY request
	// whose body is the whole encoded MessageExt — the client must not reply,
	// and must end the transaction with the CHECK header's offsets.
	ext := common.ExtFromMessage(common.NewMessage(topic, []byte("tx body")))
	ext.MsgID = "0A0B0C0D000000000000000000000009"
	ext.SysFlag = common.MessageSysFlagTransactionPrepared
	ext.PutProperty(common.PropertyProducerGroup, group)
	ext.PutProperty(common.PropertyUniqKey, "UNIQ-TX-1")
	body, err := common.EncodeMessageExt(ext, false)
	if err != nil {
		t.Fatalf("encode check-back body: %v", err)
	}
	checkHeader := &remoting.CheckTransactionStateRequestHeader{
		Topic:                strPtr(topic),
		CommitLogOffset:      i64Ptr(1234),
		TranStateTableOffset: i64Ptr(56),
		TransactionID:        strPtr("TID-1"),
		MsgID:                strPtr("0A0B0C0D000000000000000000000009"),
		Bname:                strPtr("b1"),
	}
	checkCmd := remoting.CreateRequestCommand(remoting.ReqCheckTransactionState, checkHeader)
	checkCmd.SetBody(body)
	checkCmd.MarkOnewayRPC()
	if err := broker.pushOneway(checkCmd); err != nil {
		t.Fatalf("push check-back: %v", err)
	}

	waitFor(t, "the check-back to be handled", func() bool {
		_, checkCalls, _ := listener.snapshot()
		return checkCalls == 1
	})
	waitFor(t, "END_TRANSACTION from the check-back path", func() bool {
		return len(broker.requests(remoting.ReqEndTransaction)) >= 2
	})
	endReqs = broker.requests(remoting.ReqEndTransaction)
	checkEnd := &remoting.EndTransactionRequestHeader{}
	checkEnd.FromExtFields(endReqs[1].ExtFields())
	if checkEnd.FromTransactionCheck == nil || !*checkEnd.FromTransactionCheck {
		t.Errorf("fromTransactionCheck = %v, want true", checkEnd.FromTransactionCheck)
	}
	// The check-back path takes offsets from the broker's header, and the msgId
	// from the message's UNIQ_KEY.
	if checkEnd.CommitLogOffset == nil || *checkEnd.CommitLogOffset != 1234 {
		t.Errorf("commitLogOffset = %v, want 1234 (from the check header)", checkEnd.CommitLogOffset)
	}
	if checkEnd.MsgID == nil || *checkEnd.MsgID != "UNIQ-TX-1" {
		t.Errorf("msgId = %v, want the checked message's UNIQ_KEY", checkEnd.MsgID)
	}
	if checkEnd.CommitOrRollback == nil || *checkEnd.CommitOrRollback != common.MessageSysFlagTransactionRollback {
		t.Errorf("commitOrRollback = %v, want ROLLBACK(%d)", checkEnd.CommitOrRollback, common.MessageSysFlagTransactionRollback)
	}
	// The listener must receive the decoded MessageExt, PGROUP and all — that
	// property is what the client matches before running user code.
	_, _, lastCheckMsg := listener.snapshot()
	if lastCheckMsg == nil {
		t.Fatal("the check-back listener was never handed a message")
	}
	if lastCheckMsg.Topic != topic {
		t.Errorf("listener topic = %q, want %q", lastCheckMsg.Topic, topic)
	}
	if got, ok := lastCheckMsg.GetProperty(common.PropertyProducerGroup); !ok || got != group {
		t.Errorf("listener PGROUP = (%q,%v), want %q", got, ok, group)
	}
	if got, ok := common.GetUniqID(&lastCheckMsg.Message); !ok || got != "UNIQ-TX-1" {
		t.Errorf("listener UNIQ_KEY = (%q,%v), want UNIQ-TX-1", got, ok)
	}
}

func TestProducerTransactionRejectsDelay(t *testing.T) {
	const topic = "ProdTxDelayTopic"
	p, broker, _ := startProducerFixture(t, topic, "GID_prod_txdelay")

	listener := &recordingTransactionListener{executeState: CommitMessage}
	msg := common.NewMessage(topic, []byte("x"))
	msg.SetDelayTimeLevel(3)
	if _, err := p.SendMessageInTransaction(msg, listener, nil); err == nil {
		t.Fatal("a delayed transactional message must be rejected")
	}
	if got := len(broker.requests(remoting.ReqSendMessageV2)); got != 0 {
		t.Errorf("no request must go out, got %d", got)
	}
}

func TestLocalTransactionStateMapsToSysFlag(t *testing.T) {
	cases := []struct {
		state LocalTransactionState
		want  int32
	}{
		{CommitMessage, common.MessageSysFlagTransactionCommit},
		{RollbackMessage, common.MessageSysFlagTransactionRollback},
		{Unknow, common.MessageSysFlagTransactionNotType},
	}
	for _, c := range cases {
		if got := transactionFlag(c.state); got != c.want {
			t.Errorf("transactionFlag(%s) = %d, want %d", c.state, got, c.want)
		}
	}
}

// ---------------------------------------------------------------- misc units

func TestValidatorCodeMatchesRemotingConstant(t *testing.T) {
	// common cannot import remoting (remoting imports common), so the
	// MESSAGE_ILLEGAL constant is declared locally in both packages. Pin them
	// together here, where both are in scope.
	if common.ResponseCodeMessageIllegal != remoting.RespMessageIllegal {
		t.Fatalf("common.ResponseCodeMessageIllegal=%d != remoting.RespMessageIllegal=%d",
			common.ResponseCodeMessageIllegal, remoting.RespMessageIllegal)
	}
}

func TestFaultStrategySkipsLastBrokerAndFallsBack(t *testing.T) {
	info := NewTopicPublishInfo()
	info.UpdateFromRoute(mustRoute(t, twoBrokerRouteBody("1.1.1.1:10911", "2.2.2.2:10911")), "T")
	if len(info.MsgQueueList()) != 8 {
		t.Fatalf("expected 8 queues, got %d", len(info.MsgQueueList()))
	}

	strategy := newMQFaultStrategy(false)
	first, ok, err := strategy.selectOneMessageQueue(info, "", false)
	if err != nil || !ok {
		t.Fatalf("select: ok=%v err=%v", ok, err)
	}
	// Round robin from the ring start: b1q0.
	if first.BrokerName != "b1" || first.QueueID != 0 {
		t.Fatalf("first pick = %v", first)
	}
	second, ok, err := strategy.selectOneMessageQueue(info, first.BrokerName, false)
	if err != nil || !ok {
		t.Fatalf("second select: ok=%v err=%v", ok, err)
	}
	if second.BrokerName == first.BrokerName {
		t.Errorf("the retry pick must avoid %s, got %v", first.BrokerName, second)
	}

	// With every queue on the excluded broker the filtered round yields
	// nothing, and the strategy falls back to unconditional round-robin rather
	// than reporting "no queue".
	single := NewTopicPublishInfo()
	single.UpdateFromRoute(mustRoute(t, routeBodyFor("1.1.1.1:10911", "")), "T")
	only, ok, err := strategy.selectOneMessageQueue(single, "b1", false)
	if err != nil || !ok {
		t.Fatalf("single-broker fallback must still return a queue: ok=%v err=%v", ok, err)
	}
	if only.BrokerName != "b1" {
		t.Errorf("fallback pick = %v", only)
	}
}

func TestFaultStrategyIsolatesSlowBrokerWhenEnabled(t *testing.T) {
	strategy := newMQFaultStrategy(true)
	info := NewTopicPublishInfo()
	info.UpdateFromRoute(mustRoute(t, twoBrokerRouteBody("1.1.1.1:10911", "2.2.2.2:10911")), "T")

	// An exception pins the latency to 10000ms, which lands in the 10s bucket.
	strategy.updateFaultItem("b1", 0, true, true)
	if strategy.latencyFaultTolerance.isAvailable("b1") {
		t.Error("b1 must be isolated right after an exception")
	}
	// An untouched broker stays available and reachable.
	if !strategy.latencyFaultTolerance.isAvailable("b2") || !strategy.latencyFaultTolerance.isReachable("b2") {
		t.Error("an unknown broker must default to available and reachable")
	}
	if got := strategy.computeNotAvailableDuration(10000); got != 10000 {
		t.Errorf("computeNotAvailableDuration(10000) = %v, want 10000", got)
	}
	if got := strategy.computeNotAvailableDuration(1); got != 0 {
		t.Errorf("a sub-50ms latency must map to a zero window, got %v", got)
	}
	// Disabled strategy records nothing.
	off := newMQFaultStrategy(false)
	off.updateFaultItem("b1", 0, true, true)
	if !off.latencyFaultTolerance.isAvailable("b1") {
		t.Error("a disabled strategy must not isolate anything")
	}
}

func TestMessageQueueSelectors(t *testing.T) {
	mqs := []common.MessageQueue{
		common.NewMessageQueue("T", "b1", 0),
		common.NewMessageQueue("T", "b2", 0),
		common.NewMessageQueue("T", "b3", 0),
	}
	msg := common.NewMessage("T", []byte("x"))

	// Hash: deterministic, and the same arg always lands on the same queue.
	hash := SelectMessageQueueByHash{}
	first, err := hash.Select(mqs, msg, "order-123")
	if err != nil {
		t.Fatalf("hash select: %v", err)
	}
	second, err := hash.Select(mqs, msg, "order-123")
	if err != nil {
		t.Fatalf("hash select: %v", err)
	}
	if first != second {
		t.Errorf("hash select must be deterministic: %v vs %v", first, second)
	}
	if !containsMQ(mqs, first) {
		t.Errorf("hash select returned %v, not from the list", first)
	}

	// Random stays inside the list.
	rnd := SelectMessageQueueByRandom{}
	for i := 0; i < 20; i++ {
		got, err := rnd.Select(mqs, msg, nil)
		if err != nil {
			t.Fatalf("random select: %v", err)
		}
		if !containsMQ(mqs, got) {
			t.Fatalf("random select returned %v", got)
		}
	}

	// Machine room: prefix match, else the first queue.
	room := SelectMessageQueueByMachineRoom{}
	got, err := room.Select(mqs, msg, "b2")
	if err != nil {
		t.Fatalf("machine room select: %v", err)
	}
	if got.BrokerName != "b2" {
		t.Errorf("machine room pick = %v", got)
	}
	got, err = room.Select(mqs, msg, "zz")
	if err != nil {
		t.Fatalf("machine room select: %v", err)
	}
	if got.BrokerName != "b1" {
		t.Errorf("no prefix match must fall back to the first queue, got %v", got)
	}

	// Every selector refuses an empty list.
	for name, sel := range map[string]MessageQueueSelector{
		"hash": hash, "random": rnd, "room": room,
	} {
		if _, err := sel.Select(nil, msg, nil); err == nil {
			t.Errorf("%s selector must reject an empty queue list", name)
		}
	}
}

func TestSendStatusStringAndMapping(t *testing.T) {
	cases := []struct {
		code int32
		want SendStatus
	}{
		{remoting.RespSuccess, SendOK},
		{remoting.RespFlushDiskTimeout, FlushDiskTimeout},
		{remoting.RespFlushSlaveTimeout, FlushSlaveTimeout},
		{remoting.RespSlaveNotAvailable, SlaveNotAvailable},
	}
	for _, c := range cases {
		got, ok := sendStatusOf(c.code)
		if !ok || got != c.want {
			t.Errorf("sendStatusOf(%d) = (%v,%v), want %v", c.code, got, ok, c.want)
		}
	}
	// Anything else is a genuine error, not an "OK with a caveat".
	if _, ok := sendStatusOf(remoting.RespSystemError); ok {
		t.Error("SYSTEM_ERROR must not map to a send status")
	}
	if SendOK.String() != "SEND_OK" || SlaveNotAvailable.String() != "SLAVE_NOT_AVAILABLE" {
		t.Error("SendStatus strings drifted from Java's enum names")
	}
}

func TestSendAsyncInvokesCallbackOnce(t *testing.T) {
	const topic = "ProdAsyncTopic"
	p, _, _ := startProducerFixture(t, topic, "GID_prod_async")

	done := make(chan *SendResult, 1)
	failed := make(chan error, 1)
	if err := p.SendAsync(common.NewMessage(topic, []byte("async")), SendCallbackFunc{
		SuccessFn: func(r *SendResult) { done <- r },
		ExceptFn:  func(e error) { failed <- e },
	}); err != nil {
		t.Fatalf("SendAsync submit: %v", err)
	}
	select {
	case r := <-done:
		if r.SendStatus != SendOK {
			t.Errorf("status = %s", r.SendStatus)
		}
	case e := <-failed:
		t.Fatalf("async send failed: %v", e)
	case <-time.After(3 * time.Second):
		t.Fatal("async callback never fired")
	}
}

// ---------------------------------------------------------------- helpers

func containsMQ(list []common.MessageQueue, mq common.MessageQueue) bool {
	for _, q := range list {
		if q == mq {
			return true
		}
	}
	return false
}
