package client

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

const DefaultRequestTimeoutMillis = int64(3000)

// ---------------------------------------------------------------- future

// RequestCallback receives the outcome of a request. OnSuccess may run with a
// nil response only if the responder pushed no correlationId we could match;
// OnException carries either the send failure or the request timeout.
type RequestCallback interface {
	OnSuccess(response *common.MessageExt)
	OnException(cause error)
}

type RequestResponseFuture struct {
	CorrelationID  string
	TimeoutMillis  int64
	BeginTimestamp time.Time

	callback RequestCallback

	sendRequestOK atomic.Bool
	callbackFired atomic.Bool
	arrivedOnce   sync.Once
	arrived       chan struct{}

	mu       sync.Mutex
	response *common.MessageExt
	cause    error
}

func newRequestResponseFuture(correlationID string, timeoutMillis int64, callback RequestCallback) *RequestResponseFuture {
	f := &RequestResponseFuture{
		CorrelationID:  correlationID,
		TimeoutMillis:  timeoutMillis,
		BeginTimestamp: time.Now(),
		callback:       callback,
		arrived:        make(chan struct{}),
	}
	f.sendRequestOK.Store(true)
	return f
}

// WaitResponseMessage blocks until a response arrives or the budget runs out.
// A response already stored before the call is returned immediately.
func (f *RequestResponseFuture) WaitResponseMessage(timeoutMillis int64) *common.MessageExt {
	if resp := f.currentResponse(); resp != nil {
		return resp
	}
	if timeoutMillis < 0 {
		timeoutMillis = 0
	}
	timer := time.NewTimer(time.Duration(timeoutMillis) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-f.arrived:
		return f.currentResponse()
	case <-timer.C:
		return nil
	}
}

func (f *RequestResponseFuture) currentResponse() *common.MessageExt {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.response
}

func (f *RequestResponseFuture) PutResponseMessage(response *common.MessageExt) {
	f.mu.Lock()
	f.response = response
	f.mu.Unlock()
	f.arrivedOnce.Do(func() { close(f.arrived) })
}

func (f *RequestResponseFuture) IsTimeout() bool {
	return time.Since(f.BeginTimestamp).Milliseconds() > f.TimeoutMillis
}

// Cause returns the recorded failure, if any.
func (f *RequestResponseFuture) Cause() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cause
}

// IsSendRequestOK reports whether the REQUEST message itself reached the
// broker. A timeout after a successful send must report a request timeout,
// not a send failure.
func (f *RequestResponseFuture) IsSendRequestOK() bool {
	return f.sendRequestOK.Load()
}

func (f *RequestResponseFuture) SetFailed(cause error) {
	f.sendRequestOK.Store(false)
	f.failWith(cause)
}

// failWith records a cause WITHOUT flipping sendRequestOK — used by the async
// timeout path where the send succeeded and only the reply never came.
func (f *RequestResponseFuture) failWith(cause error) {
	if cause == nil {
		cause = common.ClientError("request failed")
	}
	f.mu.Lock()
	if f.cause == nil {
		f.cause = cause
	}
	f.mu.Unlock()
	f.arrivedOnce.Do(func() { close(f.arrived) })
}

func (f *RequestResponseFuture) ExecuteRequestCallback() {
	if f.callback == nil {
		return
	}
	if !f.callbackFired.CompareAndSwap(false, true) {
		return
	}
	f.mu.Lock()
	cause, response := f.cause, f.response
	f.mu.Unlock()
	if cause != nil {
		f.callback.OnException(cause)
	} else {
		f.callback.OnSuccess(response)
	}
}

// ---------------------------------------------------------------- holder

type RequestFutureHolder struct {
	mu    sync.RWMutex
	table map[string]*RequestResponseFuture
}

var (
	requestFutureHolderOnce sync.Once
	requestFutureHolder     *RequestFutureHolder
)

// GetRequestFutureHolder returns the process singleton.
func GetRequestFutureHolder() *RequestFutureHolder {
	requestFutureHolderOnce.Do(func() {
		requestFutureHolder = &RequestFutureHolder{table: map[string]*RequestResponseFuture{}}
	})
	return requestFutureHolder
}

func (h *RequestFutureHolder) PutRequest(correlationID string, future *RequestResponseFuture) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.table[correlationID] = future
}

func (h *RequestFutureHolder) GetRequest(correlationID string) (*RequestResponseFuture, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	f, ok := h.table[correlationID]
	return f, ok
}

// RemoveRequest deletes and returns the future: nil means someone else already
// took it (or it never existed).
func (h *RequestFutureHolder) RemoveRequest(correlationID string) *RequestResponseFuture {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, ok := h.table[correlationID]
	if ok {
		delete(h.table, correlationID)
		return f
	}
	return nil
}

// PutResponse removes the future FIRST (whoever removes owns it), then wakes
// it and fires its callback. A nil return means no one is
// waiting — a normal race when a request timed out or a duplicate push
// arrived.
func (h *RequestFutureHolder) PutResponse(correlationID string, response *common.MessageExt) *RequestResponseFuture {
	if correlationID == "" {
		return nil
	}
	future := h.RemoveRequest(correlationID)
	if future == nil {
		return nil
	}
	future.PutResponseMessage(response)
	future.ExecuteRequestCallback()
	return future
}

// Size is the number of in-flight requests.
func (h *RequestFutureHolder) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.table)
}

// ---------------------------------------------------------------- reply side

func CreateCorrelationID() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16])
}

func CreateReplyMessage(request *common.Message, body []byte) (*common.Message, error) {
	cluster, ok := request.GetProperty(common.PropertyCluster)
	if !ok || cluster == "" {
		return nil, common.ClientErrorCode(common.CreateReplyMessageException,
			"create reply message fail, requestMessage error, property[CLUSTER] is null.")
	}
	reply := common.NewMessage(common.GetReplyTopic(cluster), body)
	for _, name := range []string{
		common.PropertyMessageType,
		common.PropertyCorrelationID,
		common.PropertyMessageReplyToClient,
		common.PropertyMessageTTL,
	} {
		if v, ok := request.GetProperty(name); ok {
			reply.PutProperty(name, v)
		}
	}
	reply.PutProperty(common.PropertyMessageType, common.ReplyMessageFlag)
	return reply, nil
}

// buildReplyMessageExt turns a 326 push into a MessageExt. The push carries
// the message STRUCT (header fields + a properties string), not the wire
// MessageDecoder format — so a compressed body is decompressed HERE, before
// anything else looks at it.
func buildReplyMessageExt(header *remoting.ReplyMessageRequestHeader, body []byte) (*common.MessageExt, error) {
	ext := common.NewMessageExt()
	if header == nil {
		return ext, nil
	}
	if body == nil {
		body = []byte{}
	}
	sysFlag := i32Value(header.SysFlag)
	if common.IsCompressed(sysFlag) {
		decompressed, err := common.DecompressBody(body, common.GetCompressionType(sysFlag))
		if err != nil {
			return nil, err
		}
		body = decompressed
	}
	ext.Body = body
	ext.Topic = strValue(header.Topic)
	ext.Flag = i32Value(header.Flag)
	ext.SysFlag = sysFlag
	ext.QueueID = i32Value(header.QueueID)
	ext.StoreTimestamp = i64Value(header.StoreTimestamp)
	ext.BornTimestamp = i64Value(header.BornTimestamp)
	ext.ReconsumeTimes = i32Value(header.ReconsumeTimes)
	ext.BornHost = strValue(header.BornHost)
	ext.StoreHost = strValue(header.StoreHost)
	ext.Properties = common.String2MessageProperties(strValue(header.Properties))
	ext.Properties.Put(common.PropertyReplyMessageArriveTime,
		strconv.FormatInt(time.Now().UnixMilli(), 10))
	return ext, nil
}

func i32Value(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}

func i64Value(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

// ---------------------------------------------------------------- requester

func (p *DefaultMQProducer) Request(msg *common.Message) (*common.MessageExt, error) {
	return p.RequestWithTimeout(msg, p.requestTimeout)
}

// RequestWithTimeout is Request with an explicit end-to-end budget covering
// route preparation, the send and the wait.
func (p *DefaultMQProducer) RequestWithTimeout(msg *common.Message, timeoutMillis int64) (*common.MessageExt, error) {
	return p.requestSync(msg, nil, nil, nil, timeoutMillis)
}

func (p *DefaultMQProducer) RequestWithMQ(msg *common.Message, mq common.MessageQueue) (*common.MessageExt, error) {
	return p.RequestWithMQAndTimeout(msg, mq, p.requestTimeout)
}

func (p *DefaultMQProducer) RequestWithMQAndTimeout(msg *common.Message, mq common.MessageQueue, timeoutMillis int64) (*common.MessageExt, error) {
	return p.requestSync(msg, &mq, nil, nil, timeoutMillis)
}

func (p *DefaultMQProducer) RequestBySelector(msg *common.Message, selector MessageQueueSelector, arg any) (*common.MessageExt, error) {
	return p.RequestBySelectorWithTimeout(msg, selector, arg, p.requestTimeout)
}

func (p *DefaultMQProducer) RequestBySelectorWithTimeout(msg *common.Message, selector MessageQueueSelector, arg any, timeoutMillis int64) (*common.MessageExt, error) {
	return p.requestSync(msg, nil, selector, arg, timeoutMillis)
}

func (p *DefaultMQProducer) AsyncRequest(msg *common.Message, callback RequestCallback) error {
	return p.AsyncRequestWithTimeout(msg, callback, p.requestTimeout)
}

func (p *DefaultMQProducer) AsyncRequestWithTimeout(msg *common.Message, callback RequestCallback, timeoutMillis int64) error {
	return p.requestAsync(msg, nil, nil, nil, callback, timeoutMillis)
}

func (p *DefaultMQProducer) AsyncRequestWithMQ(msg *common.Message, mq common.MessageQueue, callback RequestCallback) error {
	return p.AsyncRequestWithMQAndTimeout(msg, mq, callback, p.requestTimeout)
}

func (p *DefaultMQProducer) AsyncRequestWithMQAndTimeout(msg *common.Message, mq common.MessageQueue, callback RequestCallback, timeoutMillis int64) error {
	return p.requestAsync(msg, &mq, nil, nil, callback, timeoutMillis)
}

func (p *DefaultMQProducer) AsyncRequestBySelector(msg *common.Message, selector MessageQueueSelector, arg any, callback RequestCallback) error {
	return p.AsyncRequestBySelectorWithTimeout(msg, selector, arg, callback, p.requestTimeout)
}

func (p *DefaultMQProducer) AsyncRequestBySelectorWithTimeout(msg *common.Message, selector MessageQueueSelector, arg any, callback RequestCallback, timeoutMillis int64) error {
	return p.requestAsync(msg, nil, selector, arg, callback, timeoutMillis)
}

// prepareRequest stamps the request properties, warms the route and the
// heartbeat (the broker finds the requester's channel via REPLY_TO_CLIENT, so
// this producer must be registered on the broker), registers the waiting slot
// and reports how much of the budget all that consumed.
func (p *DefaultMQProducer) prepareRequest(msg *common.Message, timeoutMillis int64, callback RequestCallback) (*RequestResponseFuture, string, int64, error) {
	if msg == nil {
		return nil, "", 0, common.ClientError("the message is null")
	}
	topic := p.withNamespace(msg.Topic)
	if err := p.checkMessage(msg); err != nil {
		return nil, "", 0, err
	}
	if _, err := p.requireClient(); err != nil {
		return nil, "", 0, err
	}

	correlationID := CreateCorrelationID()
	msg.PutProperty(common.PropertyCorrelationID, correlationID)
	msg.PutProperty(common.PropertyMessageReplyToClient, p.ClientID())
	msg.PutProperty(common.PropertyMessageTTL, strconv.FormatInt(timeoutMillis, 10))

	begin := time.Now()
	if _, err := p.topicPublishInfo(topic); err != nil {
		common.LogDebugf("request: prepare route of topic %s failed: %v", topic, err)
	}
	p.sendHeartbeatToAllBroker()
	cost := time.Since(begin).Milliseconds()

	future := newRequestResponseFuture(correlationID, timeoutMillis, callback)
	GetRequestFutureHolder().PutRequest(correlationID, future)
	return future, topic, cost, nil
}

func (p *DefaultMQProducer) requestSync(msg *common.Message, mq *common.MessageQueue, selector MessageQueueSelector, arg any, timeoutMillis int64) (*common.MessageExt, error) {
	future, topic, cost, err := p.prepareRequest(msg, timeoutMillis, nil)
	if err != nil {
		return nil, err
	}
	defer GetRequestFutureHolder().RemoveRequest(future.CorrelationID)

	sendTimeout := timeoutMillis - cost
	if sendTimeout <= 0 {
		sendTimeout = timeoutMillis
	}
	var sendErr error
	switch {
	case mq != nil:
		_, sendErr = p.sendToQueueWithTimeout(msg, *mq, sendTimeout)
	case selector != nil:
		_, sendErr = p.SendBySelectorWithTimeout(msg, selector, arg, sendTimeout)
	default:
		_, sendErr = p.SendWithTimeout(msg, sendTimeout)
	}
	if sendErr != nil {
		future.SetFailed(sendErr)
	}
	if resp := future.WaitResponseMessage(timeoutMillis - cost); resp != nil {
		return resp, nil
	}
	if future.IsSendRequestOK() {
		return nil, common.RequestTimeoutError(topic, timeoutMillis)
	}
	out := common.ClientError(fmt.Sprintf("send request message to <%s> fail", topic))
	out.Cause = sendErr
	return nil, out
}

func (p *DefaultMQProducer) requestAsync(msg *common.Message, mq *common.MessageQueue, selector MessageQueueSelector, arg any, callback RequestCallback, timeoutMillis int64) error {
	if callback == nil {
		return common.ClientError("the request callback is null")
	}
	future, topic, cost, err := p.prepareRequest(msg, timeoutMillis, callback)
	if err != nil {
		return err
	}
	inner := SendCallbackFunc{ExceptFn: func(err error) { future.SetFailed(err) }}
	sendTimeout := timeoutMillis - cost
	if sendTimeout <= 0 {
		sendTimeout = timeoutMillis
	}
	var sendErr error
	switch {
	case mq != nil:
		sendErr = p.SendAsyncToQueueWithTimeout(msg, *mq, inner, sendTimeout)
	case selector != nil:
		sendErr = p.SendAsyncBySelectorWithTimeout(msg, selector, arg, inner, sendTimeout)
	default:
		sendErr = p.SendAsyncWithTimeout(msg, inner, sendTimeout)
	}
	if sendErr != nil {
		future.SetFailed(sendErr)
	}
	go func() {
		resp := future.WaitResponseMessage(timeoutMillis - cost)
		if GetRequestFutureHolder().RemoveRequest(future.CorrelationID) != nil &&
			resp == nil && future.Cause() == nil {
			future.failWith(common.RequestTimeoutError(topic, timeoutMillis))
		}
		future.ExecuteRequestCallback()
	}()
	return nil
}
