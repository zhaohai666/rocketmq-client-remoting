// Push-consumer tests against the in-process mock cluster from
// instance_test.go: a mock nameserver serving route bodies plus a mock broker
// that answers the consumer RPC family from an in-memory topic store and
// records every request it saw.
//
// The assertions are deliberately about the WIRE and about OFFSET bookkeeping —
// the two places a port silently diverges from Java:
//
//   - the pull header's sysFlag bits (suspend / subscription / commitOffset),
//     the presence-or-absence of the `subscription` extField, and the timeout
//     fields;
//   - which offset the consumer resolves before the first pull
//     (CONSUME_FROM_LAST_OFFSET vs FIRST_OFFSET vs TIMESTAMP vs %RETRY%);
//   - whether an ack advances the committed offset, and whether an
//     OFFSET_ILLEGAL answer rebuilds the queue instead of acking the skipped
//     range.
package client

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- mock broker

// mqKey identifies one queue in the mock broker's store.
type mqKey struct {
	topic   string
	queueID int32
}

// consumerCommit is one UPDATE_CONSUMER_OFFSET(15) the consumer sent.
type consumerCommit struct {
	group   string
	topic   string
	queueID int32
	offset  int64
}

// consumerBroker is the mock broker's state and request log.
type consumerBroker struct {
	mu sync.Mutex

	msgs      map[mqKey][]*common.MessageExt
	maxOffset map[mqKey]int64
	minOffset map[mqKey]int64
	committed map[string]int64
	clientIDs []string

	pulls        []*remoting.PullMessageRequestHeader
	pullExt      []*common.StringMap
	commits      []consumerCommit
	sendBacks    []*remoting.ConsumerSendMsgBackRequestHeader
	lockBodies   []*remoting.LockBatchRequestBody
	unlockBodies []*remoting.UnlockBatchRequestBody
	searchTS     []int64
	maxOffsetQ   []mqKey
	minOffsetQ   []mqKey
	reqOrder     []int32
	// sends records SEND_MESSAGE_V2(310) requests. In these fixtures the only
	// sender is a trace dispatcher's internal producer, so the topic of each
	// send tells the test whether tracing fired.
	sends []consumerSend
	// createTopicExt records the raw extFields of every UPDATE_AND_CREATE_TOPIC
	// (17). The REAL broker parses `attributes` as `k=v;k=v` and answers
	// "kv string format wrong" for anything else, so the field values — not just
	// the round trip — are what the create-topic test asserts.
	createTopicExt []*common.StringMap

	// pullCode forces an answer code for PULL_MESSAGE(11); nextBegin and
	// suggest override the response header.
	pullCode  int32
	nextBegin *int64
	suggest   *int32
	// lockOKMQSet is what LOCK_BATCH_MQ(41) grants. nil => grant everything the
	// client asked for.
	lockOKMQSet []common.MessageQueue
	// maxOffsetErr makes GET_MAX_OFFSET(30) answer QUERY_NOT_FOUND.
	maxOffsetErr bool
}

func newConsumerBroker() *consumerBroker {
	return &consumerBroker{
		msgs:      map[mqKey][]*common.MessageExt{},
		maxOffset: map[mqKey]int64{},
		minOffset: map[mqKey]int64{},
		committed: map[string]int64{},
	}
}

func commitKey(group, topic string, queueID int32) string {
	return fmt.Sprintf("%s|%s|%d", group, topic, queueID)
}

// consumerSend is one SEND_MESSAGE_V2(310) the mock broker answered.
type consumerSend struct {
	topic string
	body  string
	props string
}

// add appends messages to one queue. Each message is a fresh ext with a
// deterministic commitLogOffset and the MAX_OFFSET property the backlog
// arithmetic reads.
func (b *consumerBroker) add(topic string, queueID int32, bodies ...string) []*common.MessageExt {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := mqKey{topic, queueID}
	base := b.maxOffset[key]
	out := make([]*common.MessageExt, 0, len(bodies))
	for i, body := range bodies {
		out = append(out, newProducedExt(topic, queueID, base+int64(i), body))
	}
	b.msgs[key] = append(b.msgs[key], out...)
	b.maxOffset[key] = base + int64(len(bodies))
	out[0].PutProperty(common.PropertyMaxOffset, i64Text(base+int64(len(bodies))))
	return out
}

// newProducedExt builds a broker-side MessageExt carrying the same 17 fields a
// commitlog record would (what common.DecodeMessageExt reads back).
func newProducedExt(topic string, queueID int32, queueOffset int64, body string) *common.MessageExt {
	ext := common.NewMessageExt()
	ext.Topic = topic
	ext.Body = []byte(body)
	ext.QueueID = queueID
	ext.QueueOffset = queueOffset
	ext.CommitLogOffset = 1000 + queueOffset
	ext.SysFlag = 0
	ext.BornTimestamp = common.CurrentTimeMillis()
	ext.BornHost = "127.0.0.1"
	ext.BornHostPort = 10911
	ext.StoreTimestamp = ext.BornTimestamp
	ext.StoreHost = "127.0.0.1"
	ext.StoreHostPort = 10911
	ext.PutProperty(common.PropertyWaitStoreMsgOK, "true")
	ext.PutProperty(common.PropertyUniqKey, fmt.Sprintf("UNIQ-%s-%d", topic, queueOffset))
	return ext
}

func (b *consumerBroker) handler() func(*mockServer, *remoting.RemotingCommand) *remoting.RemotingCommand {
	return func(_ *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		b.mu.Lock()
		b.reqOrder = append(b.reqOrder, req.Code)
		b.mu.Unlock()
		return b.answer(req)
	}
}

func (b *consumerBroker) answer(req *remoting.RemotingCommand) *remoting.RemotingCommand {
	switch req.Code {
	case remoting.ReqPullMessage, remoting.ReqLitePullMessage:
		return b.answerPull(req)
	case remoting.ReqGetConsumerListByGroup:
		b.mu.Lock()
		ids := append([]string(nil), b.clientIDs...)
		b.mu.Unlock()
		resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
		resp.SetBody(remoting.EncodeJSON(
			(&remoting.GetConsumerListByGroupResponseBody{ConsumerIDList: ids}).ToJSONValue()))
		return resp
	case remoting.ReqQueryConsumerOffset:
		header := &remoting.QueryConsumerOffsetRequestHeader{}
		header.FromExtFields(req.ExtFields())
		key := commitKey(deref(header.ConsumerGroup), deref(header.Topic), derefI32(header.QueueID))
		b.mu.Lock()
		offset, ok := b.committed[key]
		b.mu.Unlock()
		if !ok {
			return remoting.CreateResponseCommand(remoting.RespQueryNotFound,
				"Not found, V3_0_6_SNAPSHOT maybe this group consumer boot first")
		}
		resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
		resp.SetCustomHeader(&remoting.QueryConsumerOffsetResponseHeader{Offset: remoting.I64Ptr(offset)})
		return resp
	case remoting.ReqUpdateConsumerOffset:
		header := &remoting.UpdateConsumerOffsetRequestHeader{}
		header.FromExtFields(req.ExtFields())
		b.mu.Lock()
		b.commits = append(b.commits, consumerCommit{
			group:   deref(header.ConsumerGroup),
			topic:   deref(header.Topic),
			queueID: derefI32(header.QueueID),
			offset:  derefI64(header.CommitOffset),
		})
		b.mu.Unlock()
		return remoting.CreateResponseCommand(remoting.RespSuccess, "")
	case remoting.ReqGetMaxOffset:
		header := &remoting.GetMaxOffsetRequestHeader{}
		header.FromExtFields(req.ExtFields())
		key := mqKey{deref(header.Topic), derefI32(header.QueueID)}
		b.mu.Lock()
		b.maxOffsetQ = append(b.maxOffsetQ, key)
		fail := b.maxOffsetErr
		offset := b.maxOffset[key]
		b.mu.Unlock()
		if fail {
			return remoting.CreateResponseCommand(remoting.RespQueryNotFound, "no offset")
		}
		resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
		resp.SetCustomHeader(&remoting.GetMaxOffsetResponseHeader{Offset: remoting.I64Ptr(offset)})
		return resp
	case remoting.ReqGetMinOffset:
		header := &remoting.GetMinOffsetRequestHeader{}
		header.FromExtFields(req.ExtFields())
		key := mqKey{deref(header.Topic), derefI32(header.QueueID)}
		b.mu.Lock()
		b.minOffsetQ = append(b.minOffsetQ, key)
		offset := b.minOffset[key]
		b.mu.Unlock()
		resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
		resp.SetCustomHeader(&remoting.GetMinOffsetResponseHeader{Offset: remoting.I64Ptr(offset)})
		return resp
	case remoting.ReqSearchOffsetByTimestamp:
		header := &remoting.SearchOffsetRequestHeader{}
		header.FromExtFields(req.ExtFields())
		b.mu.Lock()
		b.searchTS = append(b.searchTS, derefI64(header.Timestamp))
		b.mu.Unlock()
		resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
		resp.SetCustomHeader(&remoting.SearchOffsetResponseHeader{Offset: remoting.I64Ptr(0)})
		return resp
	case remoting.ReqConsumerSendMsgBack:
		header := &remoting.ConsumerSendMsgBackRequestHeader{}
		header.FromExtFields(req.ExtFields())
		b.mu.Lock()
		b.sendBacks = append(b.sendBacks, header)
		b.mu.Unlock()
		return remoting.CreateResponseCommand(remoting.RespSuccess, "")
	case remoting.ReqLockBatchMQ, remoting.ReqUnlockBatchMQ:
		return b.answerBatchLock(req)
	case remoting.ReqUpdateAndCreateTopic:
		b.mu.Lock()
		b.createTopicExt = append(b.createTopicExt, req.ExtFields().Clone())
		b.mu.Unlock()
		return remoting.CreateResponseCommand(remoting.RespSuccess, "")
	case remoting.ReqSendMessageV2:
		header := &remoting.SendMessageRequestHeaderV2{}
		header.FromExtFields(req.ExtFields())
		b.mu.Lock()
		b.sends = append(b.sends, consumerSend{
			topic: deref(header.Topic), body: string(req.Body), props: deref(header.Properties),
		})
		b.mu.Unlock()
		resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
		resp.SetCustomHeader(&remoting.SendMessageResponseHeader{
			MsgID:       remoting.StrPtr("0A0B0C0D000000000000000000000002"),
			QueueID:     header.QueueID,
			QueueOffset: remoting.I64Ptr(0),
		})
		return resp
	default:
		return remoting.CreateResponseCommand(remoting.RespSuccess, "")
	}
}

func (b *consumerBroker) answerBatchLock(req *remoting.RemotingCommand) *remoting.RemotingCommand {
	body, err := remoting.DecodeLockBatchRequestBody(req.Body)
	if err != nil {
		return remoting.CreateResponseCommand(remoting.RespSystemError, "bad lock body: "+err.Error())
	}
	granted := []common.MessageQueue{}
	if req.Code == remoting.ReqLockBatchMQ {
		b.mu.Lock()
		b.lockBodies = append(b.lockBodies, body)
		override := b.lockOKMQSet
		b.mu.Unlock()
		if override != nil {
			granted = override
		} else {
			granted = body.MQSet
		}
	} else {
		b.mu.Lock()
		b.unlockBodies = append(b.unlockBodies, &remoting.UnlockBatchRequestBody{
			ConsumerGroup: body.ConsumerGroup, ClientID: body.ClientID, MQSet: body.MQSet,
		})
		b.mu.Unlock()
	}
	resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
	resp.SetBody(remoting.EncodeJSON((&remoting.LockBatchResponseBody{LockOKMQSet: granted}).ToJSONValue()))
	return resp
}

func (b *consumerBroker) answerPull(req *remoting.RemotingCommand) *remoting.RemotingCommand {
	header := &remoting.PullMessageRequestHeader{}
	header.FromExtFields(req.ExtFields())

	b.mu.Lock()
	b.pulls = append(b.pulls, header)
	b.pullExt = append(b.pullExt, req.ExtFields().Clone())
	forced := b.pullCode
	next := b.nextBegin
	suggest := b.suggest
	b.mu.Unlock()

	if forced != 0 {
		resp := remoting.CreateResponseCommand(forced, "forced")
		resp.SetCustomHeader(&remoting.PullMessageResponseHeader{
			NextBeginOffset:      next,
			MinOffset:            remoting.I64Ptr(0),
			MaxOffset:            remoting.I64Ptr(0),
			SuggestWhichBrokerID: suggest,
		})
		return resp
	}

	key := mqKey{deref(header.Topic), derefI32(header.QueueID)}
	offset := derefI64(header.QueueOffset)
	maxNums := int(derefI32(header.MaxMsgNums))
	if maxNums <= 0 {
		maxNums = 32
	}
	b.mu.Lock()
	all := append([]*common.MessageExt(nil), b.msgs[key]...)
	minOff := b.minOffset[key]
	maxOff := b.maxOffset[key]
	b.mu.Unlock()

	var found []*common.MessageExt
	for _, msg := range all {
		if msg.QueueOffset < offset {
			continue
		}
		found = append(found, msg)
		if len(found) >= maxNums {
			break
		}
	}
	nextBegin := maxOff
	if len(found) > 0 {
		nextBegin = found[len(found)-1].QueueOffset + 1
	}
	code := remoting.RespSuccess
	if len(found) == 0 {
		code = remoting.RespPullNotFound
	}
	resp := remoting.CreateResponseCommand(code, "")
	resp.SetCustomHeader(&remoting.PullMessageResponseHeader{
		NextBeginOffset:      remoting.I64Ptr(nextBegin),
		MinOffset:            remoting.I64Ptr(minOff),
		MaxOffset:            remoting.I64Ptr(maxOff),
		SuggestWhichBrokerID: suggest,
	})
	if len(found) > 0 {
		var body []byte
		for _, msg := range found {
			encoded, err := common.EncodeMessageExt(msg, false)
			if err != nil {
				return remoting.CreateResponseCommand(remoting.RespSystemError, "encode: "+err.Error())
			}
			body = append(body, encoded...)
		}
		resp.SetBody(body)
	}
	return resp
}

// Snapshot helpers: the handler runs on a server goroutine, so every read takes
// the same lock.

func (b *consumerBroker) pullCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pulls)
}

func (b *consumerBroker) firstPull() *remoting.PullMessageRequestHeader {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.pulls) == 0 {
		return nil
	}
	return b.pulls[0]
}

// firstPullHas reports whether the FIRST pull carried the given extField key.
func (b *consumerBroker) firstPullHas(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.pullExt) == 0 {
		return false
	}
	return b.pullExt[0].ContainsKey(key)
}

func (b *consumerBroker) pullsFor(topic string, queueID int32) []*remoting.PullMessageRequestHeader {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []*remoting.PullMessageRequestHeader
	for _, p := range b.pulls {
		if deref(p.Topic) == topic && derefI32(p.QueueID) == queueID {
			out = append(out, p)
		}
	}
	return out
}

// pullRequests snapshots every recorded pull.
func (b *consumerBroker) pullRequests() []*remoting.PullMessageRequestHeader {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*remoting.PullMessageRequestHeader(nil), b.pulls...)
}

func (b *consumerBroker) lastCommit(topic string, queueID int32) (consumerCommit, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out consumerCommit
	found := false
	for _, c := range b.commits {
		if c.topic == topic && c.queueID == queueID {
			out, found = c, true
		}
	}
	return out, found
}

// commitCount snapshots the number of UPDATE_CONSUMER_OFFSET requests.
func (b *consumerBroker) commitCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.commits)
}

func (b *consumerBroker) sendBackSnapshot() []*remoting.ConsumerSendMsgBackRequestHeader {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*remoting.ConsumerSendMsgBackRequestHeader(nil), b.sendBacks...)
}

func (b *consumerBroker) lockCount() (int, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.lockBodies), len(b.unlockBodies)
}

func (b *consumerBroker) indexOfReq(code int32) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, c := range b.reqOrder {
		if c == code {
			return i
		}
	}
	return -1
}

func (b *consumerBroker) minOffsetQueries() []mqKey {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]mqKey(nil), b.minOffsetQ...)
}

func (b *consumerBroker) searchTimestamps() []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int64(nil), b.searchTS...)
}

// createTopicRequests snapshots the raw extFields of every UPDATE_AND_CREATE_TOPIC.
func (b *consumerBroker) createTopicRequests() []*common.StringMap {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*common.StringMap, 0, len(b.createTopicExt))
	for _, ext := range b.createTopicExt {
		out = append(out, ext.Clone())
	}
	return out
}

// sendSnapshot snapshots every SEND_MESSAGE_V2 the broker answered.
func (b *consumerBroker) sendSnapshot() []consumerSend {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]consumerSend(nil), b.sends...)
}

// traceSends keeps the sends whose topic is the given trace topic.
func (b *consumerBroker) traceSends(traceTopic string) []consumerSend {
	var out []consumerSend
	for _, s := range b.sendSnapshot() {
		if strings.HasPrefix(s.topic, traceTopic) {
			out = append(out, s)
		}
	}
	return out
}

// setClientIDs replaces the group membership the broker reports.
func (b *consumerBroker) setClientIDs(ids ...string) {
	b.mu.Lock()
	b.clientIDs = ids
	b.mu.Unlock()
}

// consumerRouteBody renders a one-broker route with a fixed queue count.
func consumerRouteBody(brokerName, master string, queueNums int) string {
	return `{"orderTopicConf":null,` +
		fmt.Sprintf(`"queueDatas":[{"brokerName":"%s","readQueueNums":%d,"writeQueueNums":%d,"perm":6,"topicSysFlag":0}],`,
			brokerName, queueNums, queueNums) +
		fmt.Sprintf(`"brokerDatas":[{"cluster":"DefaultCluster","brokerName":"%s","brokerAddrs":{"0":"%s"},"zoneName":null,"enableActingMaster":false}],`,
			brokerName, master) +
		`"filterServerTable":{}}`
}

// ---------------------------------------------------------------- listeners

// recordingListener collects every message handed to it and answers with a
// scripted status.
type recordingListener struct {
	mu      sync.Mutex
	got     []string
	offsets []int64
	topics  []string
	batches [][]string
	status  ConsumeConcurrentlyStatus
	onCall  func(batch []*common.MessageExt, ctx *ConsumeConcurrentlyContext)
}

func newRecordingListener() *recordingListener {
	return &recordingListener{status: ConsumeSuccess}
}

func (l *recordingListener) ConsumeMessage(msgs []*common.MessageExt, ctx *ConsumeConcurrentlyContext) ConsumeConcurrentlyStatus {
	l.mu.Lock()
	bodies := make([]string, 0, len(msgs))
	for _, m := range msgs {
		l.got = append(l.got, string(m.Body))
		l.offsets = append(l.offsets, m.QueueOffset)
		l.topics = append(l.topics, m.Topic)
		bodies = append(bodies, string(m.Body))
	}
	l.batches = append(l.batches, bodies)
	status := l.status
	hook := l.onCall
	l.mu.Unlock()
	if hook != nil {
		hook(msgs, ctx)
	}
	return status
}

func (l *recordingListener) bodies() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.got...)
}

func (l *recordingListener) seenTopics() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.topics...)
}

func (l *recordingListener) batchCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.batches)
}

func (l *recordingListener) setStatus(s ConsumeConcurrentlyStatus) {
	l.mu.Lock()
	l.status = s
	l.mu.Unlock()
}

func (l *recordingListener) receivedAll(want int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.got) >= want
}

// fixedOrderlyListener records what an orderly listener was given.
type fixedOrderlyListener struct {
	mu  sync.Mutex
	got []string
}

func (l *fixedOrderlyListener) ConsumeMessage(msgs []*common.MessageExt, ctx *ConsumeOrderlyContext) ConsumeOrderlyStatus {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range msgs {
		l.got = append(l.got, string(m.Body))
	}
	return OrderlySuccess
}

func (l *fixedOrderlyListener) bodies() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.got...)
}

func (l *fixedOrderlyListener) receivedAll(want int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.got) >= want
}

// ---------------------------------------------------------------- fixtures

type consumerOpt func(*DefaultMQPushConsumer)

func withConsumeFromWhere(where string) consumerOpt {
	return func(c *DefaultMQPushConsumer) { c.SetConsumeFromWhere(where) }
}

func requireNoError(t *testing.T, what string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

var (
	uniqueMu  sync.Mutex
	uniqueSeq int
)

func uniqueSuffix(t *testing.T) string {
	uniqueMu.Lock()
	defer uniqueMu.Unlock()
	uniqueSeq++
	return fmt.Sprintf("%s-%d", strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()), uniqueSeq)
}

func uniqueTopic(prefix string, t *testing.T) string {
	uniqueMu.Lock()
	defer uniqueMu.Unlock()
	uniqueSeq++
	return fmt.Sprintf("%s_%s_%d", prefix, strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()), uniqueSeq)
}

// clusterMember returns an instance name plus the clientId the consumer will
// compute from it (ChangeInstanceNameToPID leaves a non-DEFAULT name alone).
//
// The mock broker must answer GET_CONSUMER_LIST_BY_GROUP with a list that
// actually CONTAINS this consumer: AllocateMessageQueueAveragely looks the
// consumer's own clientId up in cidAll, and logs "[BUG] ... not in cidAll" and
// allocates nothing when it is missing.
func clusterMember(t *testing.T) (instanceName, clientID string) {
	t.Helper()
	instanceName = uniqueSuffix(t)
	return instanceName, common.ClientIDFor(instanceName, "", false)
}

// clusterFixture is one mock broker plus one mock nameserver, sized for the
// consumers a single test starts.
type clusterFixture struct {
	broker    *consumerBroker
	nserver   *mockServer
	brokerSrv *mockServer
	instName  string
	clientID  string
}

// newClusterFixture starts the mock cluster. queuesPerTopic maps each topic the
// nameserver knows to its queue count.
func newClusterFixture(t *testing.T, queuesPerTopic map[string]int) *clusterFixture {
	t.Helper()
	instName, clientID := clusterMember(t)
	broker := newConsumerBroker()
	broker.clientIDs = []string{clientID}
	brokerSrv := startMockServer(t, broker.handler())
	bodies := map[string]string{}
	for topic, queueNums := range queuesPerTopic {
		bodies[topic] = consumerRouteBody("b1", brokerSrv.addr, queueNums)
	}
	nserver := startMockServer(t, nameserverOnReq(bodies))
	return &clusterFixture{
		broker: broker, nserver: nserver, brokerSrv: brokerSrv,
		instName: instName, clientID: clientID,
	}
}

// newConsumer builds a consumer wired to this fixture's nameserver, using the
// instance name whose clientId the broker reports as the group membership.
func (f *clusterFixture) newConsumer(t *testing.T, group string, listener any, opts ...consumerOpt) *DefaultMQPushConsumer {
	return f.newConsumerWithInstance(t, group, f.instName, listener, opts...)
}

// newConsumerWithInstance is newConsumer with an explicit instance name — for
// tests that need two consumers inside one group.
func (f *clusterFixture) newConsumerWithInstance(t *testing.T, group, instanceName string,
	listener any, opts ...consumerOpt) *DefaultMQPushConsumer {
	t.Helper()
	c := MustNewDefaultMQPushConsumer(group)
	c.SetNameServerAddresses([]string{f.nserver.addr})
	c.SetInstanceName(instanceName)
	c.SetConsumeThreadMin(1)
	c.SetConsumeThreadMax(4)
	requireNoError(t, "listener", c.SetMessageListener(listener))
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// start subscribes, starts and registers the shutdown hook.
func startConsumer(t *testing.T, c *DefaultMQPushConsumer, topic, expression string) {
	t.Helper()
	requireNoError(t, "subscribe", c.Subscribe(topic, expression))
	requireNoError(t, "consumer start", c.Start())
	t.Cleanup(c.Shutdown)
}

// ---------------------------------------------------------------- helpers

func deref(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func derefI32(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}

func derefI64(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func sortedBodies(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// consumerForQueue reads one queue's ProcessQueue (nil when not assigned).
func consumerForQueue(c *DefaultMQPushConsumer, topic, brokerName string, queueID int32) *processQueue {
	return c.processQueueOf(common.NewMessageQueue(topic, brokerName, queueID))
}

// ---------------------------------------------------------------- tests

// The pull request must carry suspend=1, the tag expression type, the batch
// limits, and — with postSubscriptionWhenPull off (the Java default) — NO
// `subscription` extField at all. Getting the bit/field pairing wrong makes the
// broker either ignore the subscription (over-delivery) or filter on an
// expression the client never intended to send.
func TestConsumerPullRequestShape(t *testing.T) {
	topic := uniqueTopic("GoConsumerPullShape", t)
	const group = "GID_go_pull_shape"

	f := newClusterFixture(t, map[string]int{topic: 4})
	c := f.newConsumer(t, group, newRecordingListener(),
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	waitFor(t, "first pull", func() bool { return f.broker.pullCount() > 0 })

	pull := f.broker.firstPull()
	if got := deref(pull.ConsumerGroup); got != group {
		t.Errorf("consumerGroup = %q, want %q", got, group)
	}
	if got := deref(pull.Topic); got != topic {
		t.Errorf("topic = %q, want %q", got, topic)
	}
	if got := derefI64(pull.QueueOffset); got != 0 {
		t.Errorf("queueOffset = %d, want 0 (CONSUME_FROM_FIRST_OFFSET)", got)
	}
	if got := derefI32(pull.MaxMsgNums); got != defaultPullBatchSize {
		t.Errorf("maxMsgNums = %d, want %d", got, defaultPullBatchSize)
	}
	if got := derefI32(pull.MaxMsgBytes); got != defaultPullBatchSizeInBytes {
		t.Errorf("maxMsgBytes = %d, want %d", got, defaultPullBatchSizeInBytes)
	}
	if got := derefI64(pull.SuspendTimeoutMillis); got != defaultPullSuspendTimeoutMillis {
		t.Errorf("suspendTimeoutMillis = %d, want %d", got, defaultPullSuspendTimeoutMillis)
	}
	if got := derefI64(pull.CommitOffset); got != 0 {
		t.Errorf("commitOffset = %d, want 0", got)
	}
	if got := deref(pull.ExpressionType); got != remoting.ExpressionTypeTag {
		t.Errorf("expressionType = %q, want TAG", got)
	}
	if pull.SubVersion == nil || *pull.SubVersion <= 0 {
		t.Errorf("subVersion must be a positive timestamp, got %v", pull.SubVersion)
	}
	if got := derefI32(pull.RequestSource); got != 0 {
		t.Errorf("requestSource = %d, want 0 (SDK)", got)
	}

	sysFlag := derefI32(pull.SysFlag)
	if !common.HasSuspendFlag(sysFlag) {
		t.Errorf("sysFlag %d: SUSPEND bit must be set for a push consumer long poll", sysFlag)
	}
	if common.HasSubscriptionFlag(sysFlag) {
		t.Errorf("sysFlag %d: SUBSCRIPTION bit must be CLEAR by default", sysFlag)
	}
	if common.HasCommitOffsetFlag(sysFlag) {
		t.Errorf("sysFlag %d: COMMIT_OFFSET bit must be clear (offsets go via UPDATE_CONSUMER_OFFSET)", sysFlag)
	}
	if common.HasClassFilterFlag(sysFlag) {
		t.Errorf("sysFlag %d: CLASS_FILTER bit must be clear", sysFlag)
	}
	if common.HasLitePullFlag(sysFlag) {
		t.Errorf("sysFlag %d: LITE_PULL bit must be clear", sysFlag)
	}
	// Java's makeCustomHeaderToNet drops a null field, so the key must be
	// ABSENT rather than present-and-empty.
	if pull.Subscription != nil {
		t.Errorf("subscription = %q, want the key absent when the SUBSCRIPTION bit is off", *pull.Subscription)
	}
	if f.broker.firstPullHas("subscription") {
		t.Errorf("the `subscription` extField must not be on the wire when the SUBSCRIPTION bit is clear")
	}
}

// With postSubscriptionWhenPull ON, the expression must ride along together
// with the SUBSCRIPTION bit.
func TestConsumerPullCarriesSubscriptionWhenEnabled(t *testing.T) {
	topic := uniqueTopic("GoConsumerSubOnPull", t)
	const group = "GID_go_sub_on_pull"

	f := newClusterFixture(t, map[string]int{topic: 1})
	c := f.newConsumer(t, group, newRecordingListener(), func(c *DefaultMQPushConsumer) {
		c.SetPostSubscriptionWhenPull(true)
	})
	startConsumer(t, c, topic, "TagA || TagB")

	waitFor(t, "first pull", func() bool { return f.broker.pullCount() > 0 })
	pull := f.broker.firstPull()
	sysFlag := derefI32(pull.SysFlag)
	if !common.HasSubscriptionFlag(sysFlag) {
		t.Fatalf("sysFlag %d: SUBSCRIPTION bit must be set when postSubscriptionWhenPull is on", sysFlag)
	}
	if got := deref(pull.Subscription); got != "TagA || TagB" {
		t.Errorf("subscription = %q, want %q", got, "TagA || TagB")
	}
}

// Heartbeat must precede the group query: the broker's ConsumerManager learns
// the group from the heartbeat, and rebalance asks it for the member list. The
// other order yields an empty list and a stalled rebalance.
func TestConsumerHeartbeatPrecedesRebalance(t *testing.T) {
	topic := uniqueTopic("GoConsumerHeartbeatFirst", t)

	f := newClusterFixture(t, map[string]int{topic: 1})
	c := f.newConsumer(t, "GID_go_hb_first", newRecordingListener(),
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	waitFor(t, "first pull", func() bool { return f.broker.pullCount() > 0 })

	hb := f.broker.indexOfReq(remoting.ReqHeartBeat)
	list := f.broker.indexOfReq(remoting.ReqGetConsumerListByGroup)
	if hb < 0 {
		t.Fatalf("no HEARTBEAT(34) reached the broker")
	}
	if list < 0 {
		t.Fatalf("no GET_CONSUMER_LIST_BY_GROUP(38) reached the broker")
	}
	if hb > list {
		t.Errorf("HEARTBEAT (index %d) came after GET_CONSUMER_LIST_BY_GROUP (index %d); the broker cannot answer the group query before the heartbeat", hb, list)
	}
}

// CONSUME_FROM_LAST_OFFSET with no committed offset starts at the broker's
// maxOffset, resolved at ASSIGNMENT time — anything produced before the
// consumer starts is skipped.
func TestConsumerLastOffsetStartsAtMaxOffset(t *testing.T) {
	topic := uniqueTopic("GoConsumerLastOffset", t)

	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "m0", "m1", "m2")
	listener := newRecordingListener()
	c := f.newConsumer(t, "GID_go_last_offset", listener)
	startConsumer(t, c, topic, "*")

	waitFor(t, "first pull", func() bool { return f.broker.pullCount() > 0 })
	if got := derefI64(f.broker.firstPull().QueueOffset); got != 3 {
		t.Errorf("queueOffset = %d, want 3 (maxOffset at assignment)", got)
	}
	if got := listener.batchCount(); got != 0 {
		t.Errorf("listener ran %d times, want 0: nothing was produced after the start offset", got)
	}
}

// CONSUME_FROM_FIRST_OFFSET returns 0 WITHOUT asking the broker for minOffset.
// The extra round trip goes through MQAdminImpl, which only talks to the
// master — with the master down a fresh consumer would receive nothing, while
// starting from 0 works through a slave and the broker corrects the position
// via PULL_OFFSET_MOVED.
func TestConsumerFirstOffsetSkipsMinOffsetQuery(t *testing.T) {
	topic := uniqueTopic("GoConsumerFirstOffset", t)

	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "a", "b")
	listener := newRecordingListener()
	c := f.newConsumer(t, "GID_go_first_offset", listener,
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	waitFor(t, "both messages consumed", func() bool { return listener.receivedAll(2) })
	got := sortedBodies(listener.bodies())
	if !equalStrings(got, []string{"a", "b"}) {
		t.Fatalf("consumed %v, want [a b]", got)
	}
	if queries := f.broker.minOffsetQueries(); len(queries) != 0 {
		t.Errorf("GET_MIN_OFFSET(31) was called %d times; CONSUME_FROM_FIRST_OFFSET must answer 0 locally",
			len(queries))
	}
}

// A successful consume advances the committed offset to max(batch)+1 and emits
// UPDATE_CONSUMER_OFFSET(15) toward the broker.
func TestConsumerAckAdvancesCommittedOffset(t *testing.T) {
	topic := uniqueTopic("GoConsumerAck", t)

	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "one", "two", "three")
	listener := newRecordingListener()
	c := f.newConsumer(t, "GID_go_ack", listener,
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	waitFor(t, "all three consumed", func() bool { return listener.receivedAll(3) })
	// The consumed offset advances in MEMORY on the ack; the broker write is the
	// periodic persistAll, which the instance only ticks every few seconds. Drive
	// it explicitly so the assertion is about the RPC, not about a timer.
	mq := common.NewMessageQueue(topic, "b1", 0)
	waitFor(t, "in-memory consumed offset = 3", func() bool {
		offset, err := c.offsetStore.ReadOffset(mq, ReadFromMemory)
		return err == nil && offset == 3
	})
	requireNoError(t, "persist", c.PersistConsumerOffset())
	waitFor(t, "offset committed to the broker", func() bool {
		commit, ok := f.broker.lastCommit(topic, 0)
		return ok && commit.offset == 3
	})
	waitFor(t, "process queue drained", func() bool {
		pq := consumerForQueue(c, topic, "b1", 0)
		return pq != nil && pq.MsgCount() == 0
	})

	found := false
	for _, entry := range c.GetConsumerStatus(nil) {
		if entry.Queue.Topic == topic && entry.Queue.QueueID == 0 {
			found = true
			if entry.Offset != 3 {
				t.Errorf("in-memory consumed offset = %d, want 3", entry.Offset)
			}
		}
	}
	if !found {
		t.Errorf("GetConsumerStatus did not report queue %s:0", topic)
	}
}

// RECONSUME_LATER must (a) report the failure to the broker over
// CONSUMER_SEND_MSG_BACK(36) using the message's commitLogOffset — NOT its
// queueOffset — and (b) hold the committed offset so the message is redelivered
// rather than skipped.
func TestConsumerReconsumeLaterSendsBackAndHoldsOffset(t *testing.T) {
	topic := uniqueTopic("GoConsumerReconsume", t)
	const group = "GID_go_reconsume"

	f := newClusterFixture(t, map[string]int{topic: 1})
	msgs := f.broker.add(topic, 0, "poison")
	listener := newRecordingListener()
	listener.setStatus(ReconsumeLater)
	c := f.newConsumer(t, group, listener, withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	waitFor(t, "send-back", func() bool { return len(f.broker.sendBackSnapshot()) > 0 })
	back := f.broker.sendBackSnapshot()[0]
	if got := derefI64(back.Offset); got != msgs[0].CommitLogOffset {
		t.Errorf("send-back offset = %d, want the commitLogOffset %d (NOT the queueOffset %d)",
			got, msgs[0].CommitLogOffset, msgs[0].QueueOffset)
	}
	if got := deref(back.Group); got != group {
		t.Errorf("send-back group = %q, want %q", got, group)
	}
	if got := deref(back.OriginTopic); got != topic {
		t.Errorf("send-back originTopic = %q, want %q", got, topic)
	}
	// Java substitutes the broker's retryMaxTimes default (16) when the
	// consumer's maxReconsumeTimes is left at -1.
	if got := derefI32(back.MaxReconsumeTimes); got != 16 {
		t.Errorf("send-back maxReconsumeTimes = %d, want 16", got)
	}
	if back.DelayLevel == nil || *back.DelayLevel < 1 {
		t.Errorf("send-back delayLevel = %v, want >= 1", back.DelayLevel)
	}
	time.Sleep(200 * time.Millisecond)
	if commit, ok := f.broker.lastCommit(topic, 0); ok && commit.offset > 0 {
		t.Errorf("committed offset advanced to %d after RECONSUME_LATER; want it held at 0", commit.offset)
	}
}

// ackIndex truncates the ack to a PREFIX of the batch: everything after it is
// handed back to the broker, and the committed offset must still move past the
// whole batch. Java's processConsumeResult calls
// `removeMessage(consumeRequest.getMsgs())` — the ACKED list — so with all three
// messages in one batch the buffer empties and the commit is
// `queueOffsetMax + 1` = 3. Deriving the floor from the FAILED set instead
// inverts the guard: offset 0 is still buffered and is not in that set, so the
// floor clamps the commit to 0 and a restart replays messages that were already
// acknowledged.
//
// The batch is taken by hand rather than by the dispatcher because the pull loop
// would overwrite the answer: with the buffer empty the next pull is NO_NEW_MSG
// and correctTagsOffset moves the offset onto the pull cursor within the same
// millisecond, hiding the value the ack itself wrote. The consumer therefore
// subscribes to a DIFFERENT topic — that still installs the b1 route the
// send-back needs, while leaving this queue unpulled. Found live by
// examples/live_redelivery S4 (the broker held 0; the client-side correction
// does not run there because the group's retry traffic keeps the queue busy).
func TestConsumerPartialAckCommitsWholeBatch(t *testing.T) {
	topic := uniqueTopic("GoConsumerAckIndex", t)
	unpulled := uniqueTopic("GoConsumerAckIndexIdle", t)
	const group = "GID_go_ack_index"

	f := newClusterFixture(t, map[string]int{topic: 1, unpulled: 1})
	msgs := f.broker.add(topic, 0, "ack-0", "ack-1", "ack-2")
	mq := common.NewMessageQueue(topic, "b1", 0)

	listener := newRecordingListener()
	listener.onCall = func(_ []*common.MessageExt, ctx *ConsumeConcurrentlyContext) {
		ctx.AckIndex = 0
	}
	c := f.newConsumer(t, group, listener, withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, unpulled, "*")

	pq := newProcessQueue(false)
	pq.PutMessage(msgs)
	batch := pq.TakeBatch(3)
	if len(batch) != 3 {
		t.Fatalf("buffered %d messages, want 3", len(batch))
	}
	for _, msg := range batch {
		msg.BrokerName = "b1" // sendMessageBack resolves the broker by name
	}
	c.mu.Lock()
	c.processQueueTable[mq] = pq
	c.mu.Unlock()

	c.consumeConcurrentlyBatch(mq, batch, c.queueEpochOf(mq))
	if got := len(f.broker.sendBackSnapshot()); got != 2 {
		t.Fatalf("broker saw %d send-backs, want the 2 unacked messages", got)
	}
	offset, err := c.offsetStore.ReadOffset(mq, ReadFromMemory)
	if err != nil {
		t.Fatalf("read offset: %v", err)
	}
	if offset != 3 {
		t.Errorf("consumed offset = %d after ackIndex=0 on a 3-message batch, want 3 "+
			"(the whole batch left the buffer even though the tail was sent back)", offset)
	}
}

// The send-back itself failing means the entry stays buffered and is re-consumed
// locally later — so the FAILED entries are exactly what the commit must stop
// at. Excluding them from the floor (the inverted derivation) makes the floor
// the acked prefix and the commit steps over messages that were never handed
// over; a crash then loses them. No instance and no broker route here, so every
// send-back fails deterministically.
func TestConsumerAckFloorCountsFailedSendBack(t *testing.T) {
	topic := uniqueTopic("GoConsumerAckFailedBack", t)
	const group = "GID_go_ack_failed"

	f := newClusterFixture(t, map[string]int{topic: 1})
	msgs := f.broker.add(topic, 0, "f0", "f1", "f2")
	mq := common.NewMessageQueue(topic, "b1", 0)

	pq := newProcessQueue(false)
	pq.PutMessage(msgs)
	batch := pq.TakeBatch(3)
	if len(batch) != 3 {
		t.Fatalf("buffered %d messages, want 3", len(batch))
	}

	listener := newRecordingListener()
	listener.onCall = func(_ []*common.MessageExt, ctx *ConsumeConcurrentlyContext) {
		ctx.AckIndex = 0
	}
	c := MustNewDefaultMQPushConsumer(group)
	requireNoError(t, "listener", c.SetMessageListener(listener))
	// nil instance: QueryConsumerOffset / sendMessageBack both stay off the wire.
	c.offsetStore = NewRemoteBrokerOffsetStore(nil, group)
	c.mu.Lock()
	c.processQueueTable[mq] = pq
	c.mu.Unlock()

	if c.consumeConcurrentlyBatch(mq, batch, c.queueEpochOf(mq)) {
		t.Errorf("consumeConcurrentlyBatch reported a clean ack with every send-back failing")
	}
	offset, err := c.offsetStore.ReadOffset(mq, ReadFromMemory)
	if err != nil {
		t.Fatalf("read offset: %v", err)
	}
	// Java: removeMessage([f0]) leaves {f1,f2} buffered -> commit 1, NOT 3.
	if offset != 1 {
		t.Errorf("consumed offset = %d, want 1 (the smallest FAILED offset still buffered pins the commit)", offset)
	}
}

// Batches of one queue are consumed in parallel, so the batch that finishes
// second is not necessarily the one with the higher offsets. Java's
// removeMessage returns the smallest offset still buffered, which for the
// higher batch that finishes FIRST is the lower batch's head (0) — NOT its own
// end. Committing 6 there means a crash before the lower batch is done skips
// offsets 0..2 for good.
func TestConsumerAckFloorHoldsForOutOfOrderBatches(t *testing.T) {
	topic := uniqueTopic("GoConsumerAckOrder", t)
	const group = "GID_go_ack_order"

	f := newClusterFixture(t, map[string]int{topic: 1})
	msgs := f.broker.add(topic, 0, "o0", "o1", "o2", "o3", "o4", "o5")
	mq := common.NewMessageQueue(topic, "b1", 0)

	pq := newProcessQueue(false)
	pq.PutMessage(msgs)
	// TakeBatch marks them dispatched, so a dispatcher would never hand them out
	// again; this test drives the two batches itself to fix the completion order
	// (the real dispatcher would decide it with a race).
	all := pq.TakeBatch(6)
	if len(all) != 6 {
		t.Fatalf("buffered %d messages, want 6", len(all))
	}

	c := MustNewDefaultMQPushConsumer(group)
	requireNoError(t, "listener", c.SetMessageListener(newRecordingListener()))
	c.offsetStore = NewRemoteBrokerOffsetStore(nil, group)
	c.mu.Lock()
	c.processQueueTable[mq] = pq
	c.mu.Unlock()
	epoch := c.queueEpochOf(mq)

	// The HIGHER batch completes first.
	if !c.consumeConcurrentlyBatch(mq, all[3:], epoch) {
		t.Fatalf("the higher batch reported a failed send-back; nothing should be re-sent here")
	}
	offset, err := c.offsetStore.ReadOffset(mq, ReadFromMemory)
	if err != nil {
		t.Fatalf("read offset: %v", err)
	}
	if offset != 0 {
		t.Errorf("consumed offset = %d after the higher batch (3..5) finished first, want 0 "+
			"(offsets 0..2 are still buffered)", offset)
	}

	// The lower batch now finishes; the queue is drained, so the commit jumps to 6.
	if !c.consumeConcurrentlyBatch(mq, all[:3], epoch) {
		t.Fatalf("the lower batch reported a failed send-back; nothing should be re-sent here")
	}
	offset, err = c.offsetStore.ReadOffset(mq, ReadFromMemory)
	if err != nil {
		t.Fatalf("read offset: %v", err)
	}
	if offset != 6 {
		t.Errorf("consumed offset = %d after both batches, want 6", offset)
	}
}

// NO_NEW_MSG with an empty buffer must move the consumed offset onto the pull
// cursor (Java correctTagsOffset); otherwise a queue whose messages the broker
// filtered parks forever with nobody to ack it.
func TestConsumerCorrectTagsOffsetOnNoNewMsg(t *testing.T) {
	topic := uniqueTopic("GoConsumerTagsOffset", t)

	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.mu.Lock()
	f.broker.maxOffset[mqKey{topic, 0}] = 7
	f.broker.mu.Unlock()
	listener := newRecordingListener()
	c := f.newConsumer(t, "GID_go_tags_offset", listener,
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	// Nothing is stored, so every pull answers NO_NEW_MSG with
	// nextBeginOffset == maxOffset == 7.
	mq := common.NewMessageQueue(topic, "b1", 0)
	waitFor(t, "consumed offset follows the pull cursor", func() bool {
		if consumerForQueue(c, topic, "b1", 0) == nil {
			return false
		}
		offset, err := c.offsetStore.ReadOffset(mq, ReadFromMemory)
		return err == nil && offset == 7
	})
	if got := listener.batchCount(); got != 0 {
		t.Errorf("listener ran %d times on an empty queue", got)
	}
}

// An OFFSET_ILLEGAL answer (PULL_OFFSET_MOVED) must rebuild the queue from the
// corrected offset instead of consuming and acking the range the broker just
// skipped over.
func TestConsumerOffsetIllegalRebuildsQueue(t *testing.T) {
	topic := uniqueTopic("GoConsumerOffsetIllegal", t)

	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "stale")
	corrected := int64(5)
	f.broker.mu.Lock()
	f.broker.pullCode = remoting.RespPullOffsetMoved
	f.broker.nextBegin = &corrected
	f.broker.mu.Unlock()

	listener := newRecordingListener()
	c := f.newConsumer(t, "GID_go_offset_illegal", listener,
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	waitFor(t, "corrected offset persisted", func() bool {
		commit, ok := f.broker.lastCommit(topic, 0)
		return ok && commit.offset == corrected
	})
	if got := listener.batchCount(); got != 0 {
		t.Errorf("listener ran %d times on an illegal-offset pull; the fetched range is void", got)
	}
}

// CONSUME_FROM_TIMESTAMP resolves the start point with
// SEARCH_OFFSET_BY_TIMESTAMP(29), interpreting consumeTimestamp as a 14-digit
// LOCAL wall clock (Java UtilAll.parseDate).
func TestConsumerTimestampOffsetUsesSearchOffset(t *testing.T) {
	topic := uniqueTopic("GoConsumerTimestamp", t)

	f := newClusterFixture(t, map[string]int{topic: 1})
	c := f.newConsumer(t, "GID_go_timestamp", newRecordingListener(), func(c *DefaultMQPushConsumer) {
		c.SetConsumeFromWhere(ConsumeFromWhereTimestamp)
		c.SetConsumeTimestamp("20240102030405")
	})
	startConsumer(t, c, topic, "*")

	waitFor(t, "timestamp search", func() bool { return len(f.broker.searchTimestamps()) > 0 })
	want := time.Date(2024, 1, 2, 3, 4, 5, 0, time.Local).UnixMilli()
	if got := f.broker.searchTimestamps()[0]; got != want {
		t.Errorf("searchOffset timestamp = %d, want %d (20240102030405 read as local wall clock)", got, want)
	}
}

// The %RETRY% exception for CONSUME_FROM_TIMESTAMP: Java answers maxOffset (not
// a searched offset, and not 0) because a retry topic with no committed offset
// has nothing to retry yet.
func TestConsumerRetryTopicTimestampUsesMaxOffset(t *testing.T) {
	const group = "GID_go_retry_ts"
	retryTopic := common.GetRetryTopic(group)

	f := newClusterFixture(t, map[string]int{retryTopic: 1})
	f.broker.mu.Lock()
	f.broker.maxOffset[mqKey{retryTopic, 0}] = 9
	f.broker.mu.Unlock()

	c := f.newConsumer(t, group, newRecordingListener(), func(c *DefaultMQPushConsumer) {
		c.SetConsumeFromWhere(ConsumeFromWhereTimestamp)
		c.SetConsumeTimestamp("20240102030405")
	})
	// Subscribing only to the retry topic keeps the assertion unambiguous:
	// rebalance resolves exactly one queue.
	startConsumer(t, c, retryTopic, "*")

	waitFor(t, "first pull on the retry topic", func() bool {
		return len(f.broker.pullsFor(retryTopic, 0)) > 0
	})
	if got := derefI64(f.broker.pullsFor(retryTopic, 0)[0].QueueOffset); got != 9 {
		t.Errorf("retry topic queueOffset = %d, want maxOffset 9", got)
	}
	if got := f.broker.searchTimestamps(); len(got) != 0 {
		t.Errorf("SEARCH_OFFSET_BY_TIMESTAMP was called %d times for a retry topic; Java answers maxOffset", len(got))
	}
}

// The LAST_OFFSET sibling of the rule above: a retry topic with no committed
// offset starts at 0 so that every retried message is retried.
func TestConsumerRetryTopicLastOffsetStartsAtZero(t *testing.T) {
	const group = "GID_go_retry_last"
	retryTopic := common.GetRetryTopic(group)

	f := newClusterFixture(t, map[string]int{retryTopic: 1})
	f.broker.mu.Lock()
	f.broker.maxOffset[mqKey{retryTopic, 0}] = 9
	f.broker.mu.Unlock()

	c := f.newConsumer(t, group, newRecordingListener(), withConsumeFromWhere(ConsumeFromWhereLastOffset))
	startConsumer(t, c, retryTopic, "*")

	waitFor(t, "first pull on the retry topic", func() bool {
		return len(f.broker.pullsFor(retryTopic, 0)) > 0
	})
	if got := derefI64(f.broker.pullsFor(retryTopic, 0)[0].QueueOffset); got != 0 {
		t.Errorf("retry topic queueOffset = %d, want 0 (retried messages must all be retried)", got)
	}
}

// A retried message physically lives under %RETRY%<group>; the listener must
// see the ORIGINAL topic restored from the RETRY_TOPIC property.
func TestConsumerRetryTopicIsRestoredBeforeListener(t *testing.T) {
	topic := uniqueTopic("GoConsumerRetryRestore", t)
	const group = "GID_go_retry_restore"
	retryTopic := common.GetRetryTopic(group)

	f := newClusterFixture(t, map[string]int{retryTopic: 1})
	retried := newProducedExt(retryTopic, 0, 0, "retried-body")
	retried.PutProperty(common.PropertyRetryTopic, topic)
	f.broker.mu.Lock()
	f.broker.msgs[mqKey{retryTopic, 0}] = []*common.MessageExt{retried}
	f.broker.maxOffset[mqKey{retryTopic, 0}] = 1
	f.broker.mu.Unlock()

	listener := newRecordingListener()
	c := f.newConsumer(t, group, listener, withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, retryTopic, "*")

	waitFor(t, "retried message delivered", func() bool { return listener.receivedAll(1) })
	topics := listener.seenTopics()
	if len(topics) == 0 {
		t.Fatalf("listener saw nothing")
	}
	if topics[0] != topic {
		t.Errorf("listener saw topic %q, want the original %q restored from RETRY_TOPIC", topics[0], topic)
	}
}

// A message whose TAGS the broker let through but the tag expression rejects
// must be filtered on the client BEFORE the listener sees it.
func TestConsumerClientSideTagFilterBeforeListener(t *testing.T) {
	topic := uniqueTopic("GoConsumerTagFilter", t)

	f := newClusterFixture(t, map[string]int{topic: 1})
	keep := newProducedExt(topic, 0, 0, "keep")
	keep.PutProperty(common.PropertyTags, "TagA")
	drop := newProducedExt(topic, 0, 1, "drop")
	drop.PutProperty(common.PropertyTags, "TagB")
	untagged := newProducedExt(topic, 0, 2, "untagged")
	f.broker.mu.Lock()
	f.broker.msgs[mqKey{topic, 0}] = []*common.MessageExt{keep, drop, untagged}
	f.broker.maxOffset[mqKey{topic, 0}] = 3
	f.broker.mu.Unlock()

	listener := newRecordingListener()
	c := f.newConsumer(t, "GID_go_tag_filter", listener,
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "TagA")

	waitFor(t, "the TagA message", func() bool { return listener.receivedAll(1) })
	time.Sleep(200 * time.Millisecond)
	got := listener.bodies()
	if !equalStrings(got, []string{"keep"}) {
		t.Errorf("listener saw %v, want only [keep]", got)
	}
}

// A wildcard subscription carries an EMPTY tagsSet and must admit everything —
// an empty set is not "match nothing".
func TestConsumerClientSideTagFilterWildcard(t *testing.T) {
	sub, err := remoting.FilterAPI{}.BuildSubscriptionData("TagFilterTopic", "*")
	requireNoError(t, "build wildcard subscription", err)
	if len(sub.TagsSet) != 0 {
		t.Fatalf("wildcard subscription has %d tags, want 0", len(sub.TagsSet))
	}
	keep := newProducedExt("TagFilterTopic", 0, 0, "a")
	keep.PutProperty(common.PropertyTags, "TagA")
	drop := newProducedExt("TagFilterTopic", 0, 1, "b")

	if got := clientSideTagFilter(sub, []*common.MessageExt{keep, drop}); len(got) != 2 {
		t.Errorf("wildcard filter kept %d messages, want both", len(got))
	}
}

// CLUSTERING must take only this client's slice of the queues. With two
// consumers and 4 queues, AVG gives 2 each and neither pulls outside its slice.
func TestConsumerRebalanceTakesOnlyItsSlice(t *testing.T) {
	topic := uniqueTopic("GoConsumerRebalance", t)
	const group = "GID_go_rebalance"

	f := newClusterFixture(t, map[string]int{topic: 4})
	instA, cidA := clusterMember(t)
	instB, cidB := clusterMember(t)
	f.broker.setClientIDs(cidA, cidB)

	first := f.newConsumerWithInstance(t, group, instA, newRecordingListener(),
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, first, topic, "*")
	second := f.newConsumerWithInstance(t, group, instB, newRecordingListener(),
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, second, topic, "*")

	waitFor(t, "a 2/2 split", func() bool {
		return first.AssignedQueueCount() == 2 && second.AssignedQueueCount() == 2
	})

	owned := map[int32]int{}
	for _, mq := range first.AssignedQueues() {
		owned[mq.QueueID]++
	}
	for _, mq := range second.AssignedQueues() {
		owned[mq.QueueID]++
	}
	if len(owned) != 4 {
		t.Fatalf("the two consumers cover %v, want all 4 queues exactly once", owned)
	}
	for queueID, count := range owned {
		if count != 1 {
			t.Errorf("queue %d assigned to %d consumers, want exactly 1", queueID, count)
		}
	}

	// Every pull must target a queue inside one of the two slices — never a
	// queue outside the group's ownership, and never outside the owner's slice.
	allowed := map[int32]struct{}{}
	for _, mq := range first.AssignedQueues() {
		allowed[mq.QueueID] = struct{}{}
	}
	for _, mq := range second.AssignedQueues() {
		allowed[mq.QueueID] = struct{}{}
	}
	time.Sleep(200 * time.Millisecond)
	for _, p := range f.broker.pullRequests() {
		if deref(p.Topic) != topic {
			continue
		}
		if _, ok := allowed[derefI32(p.QueueID)]; !ok {
			t.Errorf("a pull was issued for unowned queue %d", derefI32(p.QueueID))
		}
	}
}

// A group the broker cannot answer for must KEEP the current assignment rather
// than degrade to "I own every queue" — the degrade makes co-instances duplicate
// every message.
func TestConsumerKeepsAssignmentWhenGroupListMissing(t *testing.T) {
	topic := uniqueTopic("GoConsumerKeepAssign", t)

	f := newClusterFixture(t, map[string]int{topic: 4})
	c := f.newConsumer(t, "GID_go_keep_assign", newRecordingListener(),
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	waitFor(t, "initial assignment", func() bool { return c.AssignedQueueCount() > 0 })
	before := c.AssignedQueueCount()

	// From now on the broker answers the group query with an empty list, which
	// is what a broker does when it has never seen the group.
	f.broker.setClientIDs()
	requireNoError(t, "rebalance", c.doRebalance())

	if got := c.AssignedQueueCount(); got != before {
		t.Errorf("assignment changed from %d to %d queues on an unanswered group query; want it kept",
			before, got)
	}
}

// BROADCASTING owns every queue without asking the broker for a member list.
func TestConsumerBroadcastingOwnsEveryQueue(t *testing.T) {
	topic := uniqueTopic("GoConsumerBroadcast", t)

	f := newClusterFixture(t, map[string]int{topic: 4})
	f.broker.setClientIDs()
	c := f.newConsumer(t, "GID_go_broadcast", newRecordingListener(),
		func(c *DefaultMQPushConsumer) { c.SetMessageModel(MessageModelBroadcasting) })
	startConsumer(t, c, topic, "*")

	waitFor(t, "broadcast assignment", func() bool { return c.AssignedQueueCount() == 4 })
	if got := f.broker.indexOfReq(remoting.ReqGetConsumerListByGroup); got >= 0 {
		t.Errorf("BROADCASTING asked the broker for the group member list (request index %d); it must not", got)
	}
}

// Orderly consumption must take LOCK_BATCH_MQ before pulling, consume in offset
// order, and release the lock on shutdown.
func TestConsumerOrderlyLocksThenUnlocks(t *testing.T) {
	topic := uniqueTopic("GoConsumerOrderly", t)
	const group = "GID_go_orderly"

	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "o0", "o1", "o2")
	listener := &fixedOrderlyListener{}
	c := f.newConsumer(t, group, listener, withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	waitFor(t, "orderly consumption", func() bool { return listener.receivedAll(3) })
	if got := listener.bodies(); !equalStrings(got, []string{"o0", "o1", "o2"}) {
		t.Errorf("orderly listener saw %v, want [o0 o1 o2] in offset order", got)
	}
	if locks, _ := f.broker.lockCount(); locks == 0 {
		t.Errorf("no LOCK_BATCH_MQ(41) was sent; an orderly cluster consumer must lock before pulling")
	}

	c.Shutdown()
	waitFor(t, "unlock on shutdown", func() bool {
		_, unlocks := f.broker.lockCount()
		return unlocks > 0
	})
}

// Shutdown must WAIT for an in-flight batch: the listener call and the
// send-back it produces have to land before the client instance goes away.
// Without the drain an immediate process exit cuts the send-back short and the
// message never reaches %RETRY%/%DLQ% (the same failure Python joins and Rust
// finalizes to prevent).
func TestShutdownWaitsForInFlightSendBack(t *testing.T) {
	topic := uniqueTopic("GoConsumerDrain", t)
	const group = "GID_go_drain"

	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "d0")

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	listener := newRecordingListener()
	listener.setStatus(ReconsumeLater)
	listener.onCall = func(batch []*common.MessageExt, _ *ConsumeConcurrentlyContext) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}
	c := f.newConsumer(t, group, listener, withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("listener never received the batch")
	}

	done := make(chan struct{})
	go func() {
		c.Shutdown()
		close(done)
	}()

	// While the listener is still running, Shutdown must block, not return.
	select {
	case <-done:
		t.Fatal("Shutdown returned while a batch was still in flight")
	case <-time.After(150 * time.Millisecond):
	}

	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not return after the batch completed")
	}

	backs := f.broker.sendBackSnapshot()
	if len(backs) != 1 {
		t.Fatalf("broker saw %d send-backs, want 1 — the drain must let the in-flight send-back land", len(backs))
	}
	if got := derefI64(backs[0].Offset); got == 0 {
		t.Errorf("send-back offset = %d, want the message's commitLogOffset", got)
	}
}

// A listener that never returns must not hang Shutdown: the drain window is
// bounded by shutdownDrainBudget.
func TestShutdownDrainIsBounded(t *testing.T) {
	prev := shutdownDrainBudget
	shutdownDrainBudget = 100 * time.Millisecond
	t.Cleanup(func() { shutdownDrainBudget = prev })

	topic := uniqueTopic("GoConsumerDrainBudget", t)
	f := newClusterFixture(t, map[string]int{topic: 1})
	f.broker.add(topic, 0, "b0")

	entered := make(chan struct{}, 1)
	blockForever := make(chan struct{})
	t.Cleanup(func() { close(blockForever) })
	listener := newRecordingListener()
	listener.onCall = func(batch []*common.MessageExt, _ *ConsumeConcurrentlyContext) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-blockForever
	}
	c := f.newConsumer(t, "GID_go_drain_budget", listener,
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("listener never received the batch")
	}

	begin := time.Now()
	c.Shutdown()
	if elapsed := time.Since(begin); elapsed > 2*time.Second {
		t.Fatalf("Shutdown took %s with a hung listener; want it bounded by the drain budget", elapsed)
	}
}

// After the freeze, a batch already taken out of the buffer must be handed back
// instead of consumed — its offset was never advanced, so it stays pending for
// the next start rather than being consumed during shutdown.
func TestShutdownRequeuesFrozenBatch(t *testing.T) {
	topic := uniqueTopic("GoConsumerDrainRequeue", t)
	f := newClusterFixture(t, map[string]int{topic: 1})

	listener := newRecordingListener()
	c := f.newConsumer(t, "GID_go_drain_requeue", listener,
		withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	startConsumer(t, c, topic, "*")

	// Freeze first, then let a message show up. The pull loop may still fill
	// the buffer, but no batch may START being consumed.
	c.freezeInFlight()
	f.broker.add(topic, 0, "r0")

	pq := c.processQueueOf(common.MessageQueue{Topic: topic, BrokerName: "b1", QueueID: 0})
	if pq == nil {
		t.Fatal("consumer never created a process queue for the topic")
	}
	waitFor(t, "message buffered", func() bool { return pq.MsgCount() > 0 })
	time.Sleep(200 * time.Millisecond)

	if got := pq.PendingCount(); got != 1 {
		t.Errorf("pending = %d after the freeze, want 1 — the taken batch must be requeued, not held dispatched", got)
	}
	if got := listener.bodies(); len(got) != 0 {
		t.Errorf("listener saw %v while frozen; nothing may be consumed during the drain", got)
	}
}

// The pool size gate mirrors Java's guard:
// `n <= 0 || n > Short.MAX_VALUE || n >= consumeThreadMax`.
func TestConsumerUpdateCorePoolSizeGuard(t *testing.T) {
	c := MustNewDefaultMQPushConsumer("GID_go_pool")
	c.SetConsumeThreadMin(2)
	c.SetConsumeThreadMax(4)

	if ok := c.UpdateCorePoolSize(3); !ok {
		t.Errorf("UpdateCorePoolSize(3) was rejected, want it accepted")
	}
	for _, bad := range []int{0, -1, 4, math.MaxInt16 + 1} {
		if ok := c.UpdateCorePoolSize(bad); ok {
			t.Errorf("UpdateCorePoolSize(%d) was accepted, want it rejected", bad)
		}
	}
	if got := c.corePoolSize; got != 3 {
		t.Errorf("corePoolSize = %d after the rejected calls, want the last accepted 3", got)
	}
}

// checkConfigRanges must reject the out-of-range values BEFORE any network or
// thread is created; otherwise a 0 turns every pull into "backlogged" and the
// consumer silently stops receiving.
func TestConsumerCheckConfigRejectsOutOfRange(t *testing.T) {
	cases := []struct {
		name  string
		apply func(*DefaultMQPushConsumer)
	}{
		{"consumeThreadMin", func(c *DefaultMQPushConsumer) { c.SetConsumeThreadMin(0) }},
		{"consumeThreadMax", func(c *DefaultMQPushConsumer) { c.SetConsumeThreadMax(0) }},
		{"minGreaterThanMax", func(c *DefaultMQPushConsumer) {
			c.SetConsumeThreadMin(8)
			c.SetConsumeThreadMax(4)
		}},
		{"pullThresholdForQueue", func(c *DefaultMQPushConsumer) { c.SetPullThresholdForQueue(0) }},
		{"pullThresholdSizeForQueue", func(c *DefaultMQPushConsumer) { c.SetPullThresholdSizeForQueue(0) }},
		{"consumeConcurrentlyMaxSpan", func(c *DefaultMQPushConsumer) { c.SetConsumeConcurrentlyMaxSpan(0) }},
		{"pullBatchSize", func(c *DefaultMQPushConsumer) { c.SetPullBatchSize(0) }},
		{"consumeMessageBatchMaxSize", func(c *DefaultMQPushConsumer) { c.SetConsumeMessageBatchMaxSize(0) }},
	}
	for _, tc := range cases {
		c := MustNewDefaultMQPushConsumer("GID_go_config_" + tc.name)
		tc.apply(c)
		if err := c.checkConfigRanges(); err == nil {
			t.Errorf("%s: checkConfigRanges accepted an out-of-range value", tc.name)
		}
	}
	// pullThresholdForTopic / SizeForTopic use -1 as the "unset" sentinel and
	// are NOT range-checked.
	c := MustNewDefaultMQPushConsumer("GID_go_config_sentinels")
	c.SetPullThresholdForTopic(-1)
	c.SetPullThresholdSizeForTopic(-1)
	if err := c.checkConfigRanges(); err != nil {
		t.Errorf("the -1 topic-threshold sentinels must be accepted, got %v", err)
	}
}

// A parse failure on consumeTimestamp must HARD FAIL: silently falling back to
// "now - 30min" would move the start point with nobody noticing.
func TestConsumerRejectsMalformedConsumeTimestamp(t *testing.T) {
	topic := uniqueTopic("GoConsumerBadTS", t)

	f := newClusterFixture(t, map[string]int{topic: 1})
	c := f.newConsumer(t, "GID_go_bad_ts", newRecordingListener(), func(c *DefaultMQPushConsumer) {
		c.SetConsumeFromWhere(ConsumeFromWhereTimestamp)
		c.SetConsumeTimestamp("2024-01-02 03:04:05")
	})
	requireNoError(t, "subscribe", c.Subscribe(topic, "*"))

	if err := c.Start(); err == nil {
		c.Shutdown()
		t.Fatalf("Start accepted a malformed consumeTimestamp")
	}
}

// DefaultMQPushConsumer must refuse to start without its prerequisites.
func TestConsumerStartRequiresPrerequisites(t *testing.T) {
	topic := uniqueTopic("GoConsumerPrereq", t)

	c := MustNewDefaultMQPushConsumer("GID_go_prereq")
	c.SetNameServerAddresses([]string{"127.0.0.1:1"})
	if err := c.Start(); err == nil {
		t.Errorf("Start without a subscription or listener must fail")
	}

	c2 := MustNewDefaultMQPushConsumer("GID_go_prereq2")
	if err := c2.Start(); err == nil {
		t.Errorf("Start without a nameserver must fail")
	}

	c3 := MustNewDefaultMQPushConsumer(common.DefaultConsumerGroup)
	c3.SetNameServerAddresses([]string{"127.0.0.1:1"})
	requireNoError(t, "listener", c3.SetMessageListener(newRecordingListener()))
	requireNoError(t, "subscribe", c3.Subscribe(topic, "*"))
	if err := c3.Start(); err == nil {
		t.Errorf("Start with the DEFAULT_CONSUMER group must fail")
	}
}
