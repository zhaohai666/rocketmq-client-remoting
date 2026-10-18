// QUERY_CONSUME_TIME_SPAN(303) through DefaultMQAdminExt: the fan-out shape, the
// header set and the two answer shapes a broker can give.
//
// Wire facts pinned here (the reference is
// DefaultMQAdminExtImpl#queryConsumeTimeSpan -> MQClientAPIImpl#queryConsumeTimeSpan):
//   - one request per broker GROUP, to that group's MASTER address: a slave owns
//     no consume queue of its own to report;
//   - the header is exactly `topic` + `group` and there is no body;
//   - a SUCCESS with an EMPTY body is a legitimate answer (a broker that owns no
//     queue of the topic reports no window) and contributes nothing;
//   - the sequential path lets a broker error abort the walk rather than return a
//     silently partial set.
package client

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- mock broker

// spanBroker is a mock broker that answers 303 with a scriptable result and
// records every request it saw.
type spanBroker struct {
	srv *mockServer

	mu     sync.Mutex
	reqs   []*remoting.RemotingCommand
	code   int32
	remark string
	body   []byte
}

func newSpanBroker(t *testing.T, code int32, remark string, body []byte) *spanBroker {
	t.Helper()
	b := &spanBroker{code: code, remark: remark, body: body}
	b.srv = startMockServer(t, b.handle)
	return b
}

func (b *spanBroker) handle(_ *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
	b.mu.Lock()
	b.reqs = append(b.reqs, req)
	code, remark, body := b.code, b.remark, b.body
	b.mu.Unlock()

	if req.Code != remoting.ReqQueryConsumeTimeSpan {
		return remoting.CreateResponseCommand(remoting.RespSuccess, "")
	}
	resp := remoting.CreateResponseCommand(code, remark)
	if body != nil {
		resp.SetBody(body)
	}
	return resp
}

func (b *spanBroker) requests(code int32) []*remoting.RemotingCommand {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []*remoting.RemotingCommand
	for _, r := range b.reqs {
		if r.Code == code {
			out = append(out, r)
		}
	}
	return out
}

func (b *spanBroker) count(code int32) int { return len(b.requests(code)) }

// errCodeOf pulls the broker's business code back out of a *common.Error so a
// test can assert the code survives the wrapping, not just the text.
func errCodeOf(err error) (int32, bool) {
	var e *common.Error
	if errors.As(err, &e) {
		return e.ResponseCode()
	}
	return 0, false
}

// ------------------------------------------------------------ route fixture

// spanRouteBody is a two-broker-group route: b1 has a master AND a slave, b2 has
// only a master. The queue set is irrelevant to 303, but the route must carry one
// queue per group for the nameserver answer to look sane.
func spanRouteBody(master1, slave1, master2 string) string {
	return `{"orderTopicConf":null,` +
		`"queueDatas":[` +
		`{"brokerName":"b1","readQueueNums":2,"writeQueueNums":2,"perm":6,"topicSysFlag":0},` +
		`{"brokerName":"b2","readQueueNums":1,"writeQueueNums":1,"perm":6,"topicSysFlag":0}],` +
		`"brokerDatas":[` +
		`{"cluster":"DefaultCluster","brokerName":"b1","brokerAddrs":{"0":"` + master1 +
		`","1":"` + slave1 + `"},"zoneName":null,"enableActingMaster":false},` +
		`{"cluster":"DefaultCluster","brokerName":"b2","brokerAddrs":{"0":"` + master2 +
		`"},"zoneName":null,"enableActingMaster":false}],` +
		`"filterServerTable":{}}`
}

func timeSpanBodyOf(entries ...common.MessageQueue) []byte {
	body := remoting.NewQueryConsumeTimeSpanBody()
	for i, mq := range entries {
		body.ConsumeTimeSpanSet = append(body.ConsumeTimeSpanSet, &remoting.QueueTimeSpan{
			MessageQueue:     mq,
			MinTimeStamp:     1550825710000 + int64(i),
			MaxTimeStamp:     1550825790000 + int64(i),
			ConsumeTimeStamp: 1550825760000 + int64(i),
			DelayTime:        int64(1000 * (i + 1)),
		})
	}
	return body.Encode()
}

// spanCluster is the nameserver + the three broker addresses of one route, plus a
// started admin wired to them.
type spanCluster struct {
	admin   *DefaultMQAdminExt
	master1 *spanBroker
	slave1  *spanBroker
	master2 *spanBroker
	topic   string
}

func newSpanCluster(t *testing.T, topic string, code1, code2 int32, body1, body2 []byte) *spanCluster {
	t.Helper()
	c := &spanCluster{
		master1: newSpanBroker(t, code1, "", body1),
		slave1:  newSpanBroker(t, remoting.RespSuccess, "", nil),
		master2: newSpanBroker(t, code2, "", body2),
		topic:   topic,
	}
	route := spanRouteBody(c.master1.srv.addr, c.slave1.srv.addr, c.master2.srv.addr)
	ns := startMockServer(t, nameserverOnReq(map[string]string{topic: route}))

	admin := NewDefaultMQAdminExt(nil)
	admin.SetNameServerAddresses([]string{ns.addr})
	admin.SetInstanceName(uniqueSuffix(t))
	admin.SetTimeoutMillis(5000)
	requireNoError(t, "admin start", admin.Start())
	t.Cleanup(admin.Shutdown)
	c.admin = admin
	return c
}

// ------------------------------------------------------------------- tests

func TestAdminQueryConsumeTimeSpanFansOutToEveryMaster(t *testing.T) {
	const topic = "SpanTopic"
	const group = "GID_span"

	c := newSpanCluster(t, topic, remoting.RespSuccess, remoting.RespSuccess,
		timeSpanBodyOf(common.NewMessageQueue(topic, "b1", 0), common.NewMessageQueue(topic, "b1", 1)),
		timeSpanBodyOf(common.NewMessageQueue(topic, "b2", 0)))

	spans, err := c.admin.QueryConsumeTimeSpan(topic, group)
	requireNoError(t, "query consume time span", err)
	if len(spans) != 3 {
		t.Fatalf("got %d windows, want the two from b1 plus the one from b2: %+v", len(spans), spans)
	}
	// Route order: b1's queue set first, then b2's.
	for i, want := range []common.MessageQueue{
		{Topic: topic, BrokerName: "b1", QueueID: 0},
		{Topic: topic, BrokerName: "b1", QueueID: 1},
		{Topic: topic, BrokerName: "b2", QueueID: 0},
	} {
		if spans[i].MessageQueue != want {
			t.Errorf("window %d is for %+v, want %+v", i, spans[i].MessageQueue, want)
		}
	}
	if spans[0].MinTimeStamp != 1550825710000 || spans[0].MaxTimeStamp != 1550825790000 ||
		spans[0].ConsumeTimeStamp != 1550825760000 || spans[0].DelayTime != 1000 {
		t.Errorf("first window lost its timestamps: %+v", spans[0])
	}

	// Exactly one 303 per broker GROUP, and never to the slave: a slave owns no
	// consume queue to report.
	if got := c.master1.count(remoting.ReqQueryConsumeTimeSpan); got != 1 {
		t.Errorf("b1 master got %d requests, want exactly 1", got)
	}
	if got := c.master2.count(remoting.ReqQueryConsumeTimeSpan); got != 1 {
		t.Errorf("b2 master got %d requests, want exactly 1", got)
	}
	if got := c.slave1.count(remoting.ReqQueryConsumeTimeSpan); got != 0 {
		t.Errorf("the slave got %d requests, want none", got)
	}

	// The header is exactly topic + group; 303 has no body.
	for _, b := range []*spanBroker{c.master1, c.master2} {
		req := b.requests(remoting.ReqQueryConsumeTimeSpan)[0]
		if got := ext(t, req, "topic"); got != topic {
			t.Errorf("topic ext = %q, want %q", got, topic)
		}
		if got := ext(t, req, "group"); got != group {
			t.Errorf("group ext = %q, want %q", got, group)
		}
		if hasExt(req, "lo") || hasExt(req, "consumerGroup") || hasExt(req, "brokerName") {
			t.Errorf("303 header carries extra keys: %v", req.ExtFields().Keys())
		}
		if len(req.Body) != 0 {
			t.Errorf("303 must be sent without a body, got %q", req.Body)
		}
	}
}

func TestAdminQueryConsumeTimeSpanFromBrokerTargetsOneAddress(t *testing.T) {
	const topic = "SpanSingleTopic"
	c := newSpanCluster(t, topic, remoting.RespSuccess, remoting.RespSuccess,
		timeSpanBodyOf(common.NewMessageQueue(topic, "b1", 7)), nil)

	spans, err := c.admin.QueryConsumeTimeSpanFromBroker(c.master1.srv.addr, topic, "GID_span")
	requireNoError(t, "single-broker query", err)
	if len(spans) != 1 || spans[0].MessageQueue.QueueID != 7 {
		t.Fatalf("single-broker answer = %+v", spans)
	}
	// The by-address form does not consult the route at all.
	if got := c.master2.count(remoting.ReqQueryConsumeTimeSpan); got != 0 {
		t.Errorf("the other broker got %d requests, want none", got)
	}
	// VIP channel is off by default, so the address goes out unchanged.
	if got := c.master1.srv.requests(remoting.ReqQueryConsumeTimeSpan); len(got) != 1 {
		t.Errorf("recorded %d requests on the mock server, want 1", len(got))
	}
}

func TestAdminQueryConsumeTimeSpanEmptyBodyBrokerContributesNothing(t *testing.T) {
	const topic = "SpanEmptyTopic"
	// b1 answers SUCCESS with no body (it owns no queue of the topic); b2 has one
	// window.
	c := newSpanCluster(t, topic, remoting.RespSuccess, remoting.RespSuccess, nil,
		timeSpanBodyOf(common.NewMessageQueue(topic, "b2", 0)))

	spans, err := c.admin.QueryConsumeTimeSpan(topic, "GID_span")
	requireNoError(t, "empty-body broker must not be an error", err)
	if len(spans) != 1 {
		t.Fatalf("got %d windows, want only b2's: %+v", len(spans), spans)
	}
	if spans[0].MessageQueue.BrokerName != "b2" {
		t.Errorf("window came from %+v", spans[0].MessageQueue)
	}
}

func TestAdminQueryConsumeTimeSpanPropagatesBrokerError(t *testing.T) {
	const topic = "SpanErrorTopic"
	c := newSpanCluster(t, topic, remoting.RespSystemError, remoting.RespSuccess, nil,
		timeSpanBodyOf(common.NewMessageQueue(topic, "b2", 0)))
	c.master1.mu.Lock()
	c.master1.remark = "broker had a bad day"
	c.master1.mu.Unlock()

	_, err := c.admin.QueryConsumeTimeSpan(topic, "GID_span")
	if err == nil {
		t.Fatal("a rejected 303 must surface, not be skipped")
	}
	if !strings.Contains(err.Error(), "broker had a bad day") {
		t.Errorf("the broker's remark must reach the caller, got %v", err)
	}
	if code, ok := errCodeOf(err); !ok || code != remoting.RespSystemError {
		t.Errorf("the broker's code must survive, got %v (ok=%v)", err, ok)
	}
	// The walk aborts at the first failure rather than returning a partial set.
	if got := c.master2.count(remoting.ReqQueryConsumeTimeSpan); got != 0 {
		t.Errorf("the second master got %d requests after the failure, want none", got)
	}
}

func TestAdminQueryConsumeTimeSpanWithoutRoute(t *testing.T) {
	const topic = "SpanNoRouteTopic"
	ns := startMockServer(t, nameserverOnReq(map[string]string{}))
	admin := NewDefaultMQAdminExt(nil)
	admin.SetNameServerAddresses([]string{ns.addr})
	admin.SetInstanceName(uniqueSuffix(t))
	admin.SetTimeoutMillis(5000)
	requireNoError(t, "admin start", admin.Start())
	t.Cleanup(admin.Shutdown)

	_, err := admin.QueryConsumeTimeSpan(topic, "GID_span")
	if err == nil {
		t.Fatal("a topic with no route must fail the walk up front")
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("topic %s not exist", topic)) {
		t.Errorf("want the route-missing wording, got %v", err)
	}
}
