// DefaultMQAdminExt tests against an in-process mock cluster (one nameserver
// plus one broker).
//
// The assertions deliberately focus on the WIRE SHAPE rather than on happy-path
// return values: every bug this surface has had was a wrong extField name or a
// wrong body encoding, and those are invisible to a test that only checks the
// decoded result.
package client

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- request log

type reqLog struct {
	mu   sync.Mutex
	reqs []*remoting.RemotingCommand
}

func (l *reqLog) add(c *remoting.RemotingCommand) {
	l.mu.Lock()
	l.reqs = append(l.reqs, c)
	l.mu.Unlock()
}

func (l *reqLog) snapshot() []*remoting.RemotingCommand {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]*remoting.RemotingCommand(nil), l.reqs...)
}

func (l *reqLog) all(code int32) []*remoting.RemotingCommand {
	var out []*remoting.RemotingCommand
	for _, r := range l.snapshot() {
		if r.Code == code {
			out = append(out, r)
		}
	}
	return out
}

func (l *reqLog) count(code int32) int { return len(l.all(code)) }

func (l *reqLog) first(code int32) *remoting.RemotingCommand {
	all := l.all(code)
	if len(all) == 0 {
		return nil
	}
	return all[0]
}

// codes records the request codes in arrival order.
func (l *reqLog) codes() []int32 {
	var out []int32
	for _, r := range l.snapshot() {
		out = append(out, r.Code)
	}
	return out
}

// ext reads one extField off a recorded request.
func ext(t *testing.T, req *remoting.RemotingCommand, key string) string {
	t.Helper()
	if req == nil {
		t.Fatalf("no request recorded for ext %q", key)
	}
	v, ok := req.ExtFields().Get(key)
	if !ok {
		t.Fatalf("request code %d has no extField %q (has %v)", req.Code, key, req.ExtFields().Keys())
	}
	return v
}

// hasExt reports whether an extField is present (for "must NOT be sent" checks).
func hasExt(req *remoting.RemotingCommand, key string) bool {
	if req == nil {
		return false
	}
	return req.ExtFields().ContainsKey(key)
}

// ---------------------------------------------------------------- fixture

type adminCluster struct {
	t         *testing.T
	brokerLog *reqLog
	nsLog     *reqLog
	brokerSrv *mockServer
	nsSrv     *mockServer
	admin     *DefaultMQAdminExt

	// brokerAnswer / nsAnswer override the default per-code answer.
	brokerAnswer map[int32]func(*remoting.RemotingCommand) *remoting.RemotingCommand
	nsAnswer     map[int32]func(*remoting.RemotingCommand) *remoting.RemotingCommand
	topics       map[string]int
	clusterName  string
	brokerName   string
}

func respOK(body []byte) *remoting.RemotingCommand {
	resp := remoting.CreateResponseCommand(remoting.RespSuccess, "")
	if body != nil {
		resp.SetBody(body)
	}
	return resp
}

func okCode(code int32, remark string) *remoting.RemotingCommand {
	return remoting.CreateResponseCommand(code, remark)
}

// clusterInfoJSON renders the GET_BROKER_CLUSTER_INFO body for one broker.
func clusterInfoJSON(clusterName, brokerName, addr string) []byte {
	return []byte(`{"brokerAddrTable":{"` + brokerName +
		`":{"cluster":"` + clusterName + `","brokerName":"` + brokerName +
		`","brokerAddrs":{"0":"` + addr + `"},"enableActingMaster":false}},` +
		`"clusterAddrTable":{"` + clusterName + `":["` + brokerName + `"]}}`)
}

const (
	testClusterName = "DefaultCluster"
	testBrokerName  = "b1"
)

// newAdminCluster starts the mock cluster and a started admin wired to it.
func newAdminCluster(t *testing.T) *adminCluster {
	t.Helper()
	c := &adminCluster{
		t:            t,
		brokerLog:    &reqLog{},
		nsLog:        &reqLog{},
		brokerAnswer: map[int32]func(*remoting.RemotingCommand) *remoting.RemotingCommand{},
		nsAnswer:     map[int32]func(*remoting.RemotingCommand) *remoting.RemotingCommand{},
		topics: map[string]int{
			common.DefaultTopic: 1,
			"MyTopic":           4,
			// examineConsumeStatsGroup routes through %RETRY%<group>.
			common.GetRetryTopic("G1"): 1,
		},
		clusterName: testClusterName,
		brokerName:  testBrokerName,
	}
	c.brokerSrv = startMockServer(t, c.onBroker)
	c.nsSrv = startMockServer(t, c.onNameServer)

	admin := NewDefaultMQAdminExt(nil)
	admin.SetNameServerAddresses([]string{c.nsSrv.addr})
	admin.SetInstanceName(uniqueSuffix(t))
	admin.SetTimeoutMillis(5000)
	requireNoError(t, "admin start", admin.Start())
	t.Cleanup(admin.Shutdown)
	c.admin = admin
	return c
}

func (c *adminCluster) onBroker(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
	c.brokerLog.add(req)
	if fn, ok := c.brokerAnswer[req.Code]; ok {
		return fn(req)
	}
	switch req.Code {
	case remoting.ReqUpdateAndCreateTopic,
		remoting.ReqDeleteTopicInBroker,
		remoting.ReqUpdateBrokerConfig,
		remoting.ReqDeleteSubscriptionGroup,
		remoting.ReqUpdateAndCreateSubscriptionGroup,
		remoting.ReqCleanUnusedTopic,
		remoting.ReqCloneGroupOffset,
		remoting.ReqUpdateConsumerOffset:
		return respOK(nil)
	case remoting.ReqGetAllTopicConfig:
		w := remoting.NewTopicConfigSerializeWrapper()
		w.TopicConfigTable["MyTopic"] = remoting.NewTopicConfig("MyTopic")
		return respOK(w.Encode())
	case remoting.ReqGetTopicConfig:
		return respOK(remoting.NewTopicConfig("MyTopic").Encode())
	case remoting.ReqGetSystemTopicListFromBroker:
		tl := remoting.NewTopicList()
		tl.TopicList = []string{"rmq_sys_watermark"}
		return respOK(tl.Encode())
	case remoting.ReqGetTopicStatsInfo:
		table := remoting.NewTopicStatsTable()
		table.OffsetTable[common.NewMessageQueue("MyTopic", testBrokerName, 0)] = &remoting.TopicOffset{
			MinOffset: 0, MaxOffset: 26, LastUpdateTimestamp: 1700000000000,
		}
		table.TopicPutTps = 1.5
		return respOK(table.Encode())
	case remoting.ReqGetConsumeStats:
		cs := remoting.NewConsumeStats()
		cs.OffsetTable[common.NewMessageQueue("MyTopic", testBrokerName, 0)] = &remoting.OffsetWrapper{
			BrokerOffset: 26, ConsumerOffset: 20, LastTimestamp: 1700000000000,
		}
		cs.ConsumeTps = 2.5
		return respOK(cs.Encode())
	case remoting.ReqGetBrokerConsumeStats:
		list := remoting.NewConsumeStatsList()
		list.BrokerAddr = c.brokerSrv.addr
		list.HasBroker = true
		list.TotalDiff = 6
		return respOK(list.Encode())
	case remoting.ReqGetConsumerConnectionList:
		cc := remoting.NewConsumerConnection()
		cc.ConsumeType = "CONSUME_PASSIVELY"
		cc.MessageModel = "CLUSTERING"
		cc.ConnectionSet = append(cc.ConnectionSet, &remoting.Connection{
			ClientID: "cid-1", ClientAddr: "127.0.0.1:1234", Language: "JAVA", Version: 395,
		})
		cc.SubscriptionTable["MyTopic"] = map[string]any{
			"classFilterMode": false, "topic": "MyTopic", "subString": "*",
			"tagsSet": []any{"TagA"}, "codeSet": []any{},
		}
		return respOK(cc.Encode())
	case remoting.ReqGetProducerConnectionList:
		pc := remoting.NewProducerConnection()
		pc.ConnectionSet = append(pc.ConnectionSet, &remoting.Connection{ClientID: "pid-1"})
		return respOK(pc.Encode())
	case remoting.ReqGetConsumerRunningInfo:
		ri := remoting.NewConsumerRunningInfo()
		ri.Properties[remoting.PropConsumeType] = "CONSUME_PASSIVELY"
		ri.MQTable[common.NewMessageQueue("MyTopic", testBrokerName, 0)] = map[string]any{"lock": true}
		return respOK(ri.Encode())
	case remoting.ReqGetSubscriptionGroupConfig:
		return respOK(remoting.NewSubscriptionGroupConfig("G1").Encode())
	case remoting.ReqGetAllSubscriptionGroupConfig:
		return c.subscriptionGroupPage(req)
	case remoting.ReqGetBrokerConfig:
		// java.util.Properties TEXT, not JSON.
		return respOK([]byte("brokerName=b1\nbrokerId=0\n# comment\nmaxMessageSize: 4194304\n"))
	case remoting.ReqGetBrokerRuntimeInfo:
		kv := remoting.NewKVTable()
		kv.Table["brokerVersionDesc"] = "V5_5_1"
		return respOK(kv.Encode())
	case remoting.ReqViewBrokerStatsData:
		return respOK([]byte(`{"statsName":"tps","statsKey":"put","data":{}}`))
	case remoting.ReqGetMaxOffset:
		resp := respOK(nil)
		resp.AddExtField("offset", "26")
		return resp
	case remoting.ReqGetMinOffset:
		resp := respOK(nil)
		resp.AddExtField("offset", "0")
		return resp
	case remoting.ReqSearchOffsetByTimestamp:
		resp := respOK(nil)
		resp.AddExtField("offset", "17")
		return resp
	case remoting.ReqGetEarliestMsgStoretime:
		resp := respOK(nil)
		resp.AddExtField("timestamp", "1699999999000")
		return resp
	case remoting.ReqQueryConsumerOffset:
		resp := respOK(nil)
		resp.AddExtField("offset", "20")
		return resp
	case remoting.ReqInvokeBrokerToResetOffset:
		table := remoting.MQOffsetTable{
			{Queue: common.NewMessageQueue("MyTopic", testBrokerName, 0), Offset: 10},
			{Queue: common.NewMessageQueue("MyTopic", testBrokerName, 1), Offset: 11},
		}
		return respOK(remoting.EncodeJSON(map[string]any{"offsetTable": remoting.EncodeMQOffsetTable(table)}))
	case remoting.ReqQueryMessage:
		msg := newQueryableExt(c.t, "MyTopic", 0, 7, "hello", "Key1", "MSGID0000000001")
		other := newQueryableExt(c.t, "MyTopic", 0, 9, "world", "Other", "MSGID0000000002")
		batch := append(encodeStored(c.t, msg), encodeStored(c.t, other)...)
		return respOK(batch)
	case remoting.ReqQueryTopicConsumeByWho:
		return respOK([]byte(`{"groupList":["G1","G2"]}`))
	case remoting.ReqQueryTopicsByConsumer:
		tl := remoting.NewTopicList()
		tl.TopicList = []string{"MyTopic"}
		return respOK(tl.Encode())
	case remoting.ReqQuerySubscriptionByConsumer:
		return respOK([]byte(`{"topic":"MyTopic","subString":"*"}`))
	case remoting.ReqInvokeBrokerToGetConsumerStatus:
		return respOK([]byte(`{"consumerTable":{"cid-1":{"MyTopic":{"0":20}}}}`))
	case remoting.ReqQueryConsumeQueue:
		b := remoting.NewQueryConsumeQueueResponseBody()
		b.FilterData = remoting.StrPtr("*")
		b.MaxQueueIndex = 26
		b.MinQueueIndex = 0
		b.QueueData = []*remoting.ConsumeQueueData{{PhysicOffset: 100, PhysicSize: 200, TagsCode: 1}}
		return respOK(b.Encode())
	}
	return okCode(remoting.RespRequestCodeNotSupported,
		fmt.Sprintf("request code %d not supported", req.Code))
}

func (c *adminCluster) onNameServer(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
	c.nsLog.add(req)
	if fn, ok := c.nsAnswer[req.Code]; ok {
		return fn(req)
	}
	switch req.Code {
	case remoting.ReqGetRouteInfoByTopic:
		header := &remoting.GetRouteInfoRequestHeader{}
		header.FromExtFields(req.ExtFields())
		topic := ""
		if header.Topic != nil {
			topic = *header.Topic
		}
		queueNums, okTopic := c.topics[topic]
		if !okTopic {
			return okCode(remoting.RespTopicNotExist, "topic["+topic+"] not exist")
		}
		return respOK([]byte(consumerRouteBody(c.brokerName, c.brokerSrv.addr, queueNums)))
	case remoting.ReqGetBrokerClusterInfo:
		return respOK(clusterInfoJSON(c.clusterName, c.brokerName, c.brokerSrv.addr))
	case remoting.ReqGetAllTopicListFromNameServer:
		tl := remoting.NewTopicList()
		for topic := range c.topics {
			tl.TopicList = append(tl.TopicList, topic)
		}
		return respOK(tl.Encode())
	case remoting.ReqGetTopicsByCluster:
		return respOK([]byte(`{"topicList":["MyTopic"]}`))
	case remoting.ReqDeleteTopicInNameSrv, remoting.ReqPutKVConfig, remoting.ReqDeleteKVConfig,
		remoting.ReqWipeWritePermOfBroker, remoting.ReqAddWritePermOfBroker:
		return respOK(nil)
	case remoting.ReqGetKVConfig:
		resp := respOK(nil)
		resp.AddExtField("value", "v1")
		return resp
	case remoting.ReqGetKVListByNamespace:
		kv := remoting.NewKVTable()
		kv.Table["k1"] = "v1"
		return respOK(kv.Encode())
	}
	return okCode(remoting.RespRequestCodeNotSupported,
		fmt.Sprintf("request code %d not supported", req.Code))
}

// subscriptionGroupPage answers 201 ONE group per page (real brokers return up
// to maxGroupNum at once, but paging only ever shows up with >10000 groups —
// the unit test has to shrink the page to exercise the loop at all).
func (c *adminCluster) subscriptionGroupPage(req *remoting.RemotingCommand) *remoting.RemotingCommand {
	seqText, _ := req.ExtFields().Get("groupSeq")
	seq, _ := adminParseInt64(seqText)
	page := []string{"G1", "G2", "G3"}
	w := remoting.NewSubscriptionGroupWrapper()
	if int(seq) < len(page) {
		name := page[seq]
		w.SubscriptionGroupTable[name] = remoting.NewSubscriptionGroupConfig(name)
	}
	resp := respOK(w.Encode())
	resp.AddExtField("totalGroupNum", fmt.Sprintf("%d", len(page)))
	return resp
}

// newQueryableExt builds a stored message with the given KEYS property.
func newQueryableExt(t *testing.T, topic string, queueID int32, queueOffset int64, body, keys, msgID string) *common.MessageExt {
	t.Helper()
	msg := common.NewMessageExt()
	msg.Topic = topic
	msg.QueueID = queueID
	msg.QueueOffset = queueOffset
	msg.Body = []byte(body)
	msg.MsgID = msgID
	msg.SetKeys(keys)
	return msg
}

// encodeStored renders a stored message in the broker's 17/6-segment format.
func encodeStored(t *testing.T, msg *common.MessageExt) []byte {
	t.Helper()
	raw, err := common.EncodeMessageExt(msg, false)
	if err != nil {
		t.Fatalf("encode stored message: %v", err)
	}
	return raw
}

// ---------------- pure function: java.util.Properties ----------------

func TestString2PropertiesMatchesJavaPropertiesLoad(t *testing.T) {
	text := strings.Join([]string{
		"# comment line",
		"! also a comment",
		"",
		"brokerName=b1",
		"brokerId:0",
		"maxMessageSize 4194304",          // whitespace is a legal separator
		"  spacedKey   =   spacedValue  ", // inner padding trimmed, value tail kept
		"trailingTail=v   ",
		"continued=abc\\",
		"def",
		"noSeparatorKey",
	}, "\n")

	got := String2Properties(text)
	want := map[string]string{
		"brokerName":     "b1",
		"brokerId":       "0",
		"maxMessageSize": "4194304",
		"spacedKey":      "spacedValue  ",
		"trailingTail":   "v   ",
		"continued":      "abcdef",
		"noSeparatorKey": "",
	}
	if len(got) != len(want) {
		t.Fatalf("parsed %d entries, want %d: %v", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("properties[%q] = %q, want %q", k, got[k], v)
		}
	}
}

func TestString2PropertiesEmpty(t *testing.T) {
	if got := String2Properties(""); len(got) != 0 {
		t.Fatalf("empty text must yield no entries, got %v", got)
	}
}

// ---------------- lifecycle ----------------

// The first constructor parameter is the RPC hook, NOT a clientId: an earlier
// port passed a string and every admin call blew up inside hook dispatch while
// getTopicRouteData swallowed the error, surfacing as "topic xxx not exist".
func TestAdminRpcHookIsInstalledAndInvoked(t *testing.T) {
	cluster := newAdminCluster(t)
	hook := &countingHook{}

	admin := NewDefaultMQAdminExt(hook)
	admin.SetNameServerAddresses([]string{cluster.nsSrv.addr})
	admin.SetInstanceName(uniqueSuffix(t))
	requireNoError(t, "admin start", admin.Start())
	t.Cleanup(admin.Shutdown)

	if _, err := admin.FetchAllTopicList(); err != nil {
		t.Fatalf("fetchAllTopicList: %v", err)
	}
	// NamespaceRpcHook + user hook + DynamicalExtFieldRPCHook all run.
	if hook.before.Load() == 0 {
		t.Fatal("user RPC hook never invoked — is it really installed?")
	}
}

func TestAdminStartRequiresNameserver(t *testing.T) {
	admin := NewDefaultMQAdminExt(nil)
	admin.SetInstanceName(uniqueSuffix(t))
	err := admin.Start()
	if err == nil || !strings.Contains(err.Error(), "name server address is not set") {
		t.Fatalf("Start without a nameserver must fail with the Java message, got %v", err)
	}
}

func TestAdminNotStartedIsRejected(t *testing.T) {
	admin := NewDefaultMQAdminExt(nil)
	if _, err := admin.FetchAllTopicList(); err == nil ||
		!strings.Contains(err.Error(), "not started") {
		t.Fatalf("admin calls before Start must be rejected, got %v", err)
	}
}

// ---------------- topic management ----------------

func TestAdminCreateTopicSendsTopicFilterTypeAndDefaults(t *testing.T) {
	c := newAdminCluster(t)
	requireNoError(t, "createTopic", c.admin.CreateTopic("key", "NewTopic", 8, 0))

	req := c.brokerLog.first(remoting.ReqUpdateAndCreateTopic)
	if req == nil {
		t.Fatal("no UPDATE_AND_CREATE_TOPIC request reached the broker")
	}
	// topicFilterType is mandatory: the broker's checkFields turns it into an
	// enum and rejects null with "topicFilterType = [null] value invalid".
	if got := ext(t, req, "topicFilterType"); got != remoting.TopicFilterTypeSingleTag {
		t.Errorf("topicFilterType = %q, want SINGLE_TAG", got)
	}
	checks := map[string]string{
		"topic":          "NewTopic",
		"defaultTopic":   common.DefaultTopic,
		"readQueueNums":  "8",
		"writeQueueNums": "8",
		"perm":           "6",
		"topicSysFlag":   "0",
		"order":          "false",
		// Java's AttributeParser.parseToString(emptyMap) == "" and the field is
		// still sent.
		"attributes": "",
		"force":      "false",
	}
	for k, want := range checks {
		if got := ext(t, req, k); got != want {
			t.Errorf("extField %s = %q, want %q", k, got, want)
		}
	}
}

// A broker REJECTION is final — Java rethrows MQBrokerException out of the
// retry loop instead of retrying five times.
func TestAdminCreateTopicBrokerRejectionIsNotRetried(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqUpdateAndCreateTopic] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespTopicExistAlready, "topic exists")
	}
	err := c.admin.CreateTopicInBroker(c.brokerSrv.addr, "NewTopic", 4, 4, 6)
	if err == nil {
		t.Fatal("a rejected createTopic must return the broker error")
	}
	if code, ok := responseCodeOf(err); !ok || code != remoting.RespTopicExistAlready {
		t.Fatalf("error must carry the broker code, got %v", err)
	}
	if got := c.brokerLog.count(remoting.ReqUpdateAndCreateTopic); got != 1 {
		t.Fatalf("broker rejection was retried: %d requests, want 1", got)
	}
}

func TestAdminDeleteTopicClearsBrokersThenNameServer(t *testing.T) {
	c := newAdminCluster(t)
	requireNoError(t, "deleteTopic", c.admin.DeleteTopic("MyTopic", ""))

	if c.brokerLog.count(remoting.ReqDeleteTopicInBroker) == 0 {
		t.Error("deleteTopic must clear the topic on the brokers")
	}
	nsReq := c.nsLog.first(remoting.ReqDeleteTopicInNameSrv)
	if nsReq == nil {
		t.Fatal("deleteTopic must clear the route on the nameserver")
	}
	if got := ext(t, nsReq, "topic"); got != "MyTopic" {
		t.Errorf("nameserver delete topic = %q", got)
	}
	// Step order matters: every broker is cleared first, then the route. (The
	// nameserver log starts with the cluster-info lookup deleteTopic needs to
	// enumerate the brokers, so only the DELETE's position is asserted.)
	brokerCodes := c.brokerLog.codes()
	for _, code := range brokerCodes {
		if code != remoting.ReqDeleteTopicInBroker {
			t.Errorf("unexpected broker request %d during deleteTopic", code)
		}
	}
	if len(brokerCodes) != 1 {
		t.Errorf("brokers cleared = %d, want 1", len(brokerCodes))
	}
	nsCodes := c.nsLog.codes()
	if nsCodes[len(nsCodes)-1] != remoting.ReqDeleteTopicInNameSrv {
		t.Errorf("nameserver request order = %v, want the DELETE last", nsCodes)
	}
}

func TestAdminExamineTopicRouteMissing(t *testing.T) {
	c := newAdminCluster(t)
	_, err := c.admin.ExamineTopicRoute("NoSuchTopic")
	if err == nil || !strings.Contains(err.Error(), "topic NoSuchTopic not exist") {
		t.Fatalf("want the Java wording, got %v", err)
	}
}

// fetchTopicsByCluster must send `cluster`; with `clusterName` the nameserver
// NPEs internally, swallows it, and answers SUCCESS with an empty list.
func TestAdminFetchTopicsByClusterSendsClusterKey(t *testing.T) {
	c := newAdminCluster(t)
	topics, err := c.admin.FetchTopicsByCluster("DefaultCluster")
	requireNoError(t, "fetchTopicsByCluster", err)
	if !topics["MyTopic"] {
		t.Fatalf("topics = %v, want MyTopic", topics)
	}
	req := c.nsLog.first(remoting.ReqGetTopicsByCluster)
	if req == nil {
		t.Fatal("GET_TOPICS_BY_CLUSTER never reached the nameserver")
	}
	if got := ext(t, req, "cluster"); got != "DefaultCluster" {
		t.Errorf("cluster = %q", got)
	}
	if hasExt(req, "clusterName") {
		t.Error("the request must NOT carry clusterName")
	}
}

func TestAdminExamineTopicConfigRoundTrip(t *testing.T) {
	c := newAdminCluster(t)
	cfg, err := c.admin.ExamineTopicConfig(c.brokerSrv.addr, "MyTopic")
	requireNoError(t, "examineTopicConfig", err)
	if cfg.TopicName != "MyTopic" || cfg.ReadQueueNums != 16 || cfg.Perm != 6 {
		t.Fatalf("config = %s", cfg)
	}
	req := c.brokerLog.first(remoting.ReqGetTopicConfig)
	if got := ext(t, req, "lo"); got != "true" {
		t.Errorf("GET_TOPIC_CONFIG must carry lo=true, got %q", got)
	}
}

func TestAdminExamineTopicStatsMergesAndRejectsEmpty(t *testing.T) {
	c := newAdminCluster(t)
	stats, err := c.admin.ExamineTopicStats("MyTopic")
	requireNoError(t, "examineTopicStats", err)
	q := common.NewMessageQueue("MyTopic", testBrokerName, 0)
	off, ok := stats.OffsetTable[q]
	if !ok {
		t.Fatalf("offset table = %v", stats.OffsetTable)
	}
	if off.MaxOffset != 26 || off.MinOffset != 0 {
		t.Errorf("TopicOffset = %s", off)
	}
	if stats.TopicPutTps != 1.5 {
		t.Errorf("topicPutTps = %v", stats.TopicPutTps)
	}

	empty := newAdminCluster(t)
	empty.brokerAnswer[remoting.ReqGetTopicStatsInfo] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(remoting.NewTopicStatsTable().Encode())
	}
	if _, err := empty.admin.ExamineTopicStats("MyTopic"); err == nil ||
		!strings.Contains(err.Error(), "Not found the topic stats info") {
		t.Fatalf("empty stats must raise the Java message, got %v", err)
	}
}

// ---------------- broker config ----------------

// GET_BROKER_CONFIG(26) returns java.util.Properties TEXT. Parsing it as a
// KVTable JSON — which an earlier port did — fails on every real broker.
func TestAdminGetBrokerConfigParsesPropertiesText(t *testing.T) {
	c := newAdminCluster(t)
	props, err := c.admin.GetBrokerConfig(c.brokerSrv.addr, 0)
	requireNoError(t, "getBrokerConfig", err)
	want := map[string]string{
		"brokerName":     "b1",
		"brokerId":       "0",
		"maxMessageSize": "4194304",
	}
	for k, v := range want {
		if props[k] != v {
			t.Errorf("properties[%q] = %q, want %q (all: %v)", k, props[k], v, props)
		}
	}
	if _, ok := props["# comment"]; ok {
		t.Error("comment lines must be skipped")
	}
}

func TestAdminUpdateBrokerConfigRejectsInvalidPermission(t *testing.T) {
	c := newAdminCluster(t)
	// 8 == PermName.PERM_PRIORITY, the first invalid value.
	err := c.admin.UpdateBrokerConfig(c.brokerSrv.addr, map[string]string{"brokerPermission": "8"}, 0)
	if err == nil {
		t.Fatal("an invalid brokerPermission must be rejected client-side")
	}
	if code, ok := responseCodeOf(err); !ok || code != remoting.RespNoPermission {
		t.Fatalf("want NO_PERMISSION, got %v", err)
	}
	if got := c.brokerLog.count(remoting.ReqUpdateBrokerConfig); got != 0 {
		t.Fatalf("client-side validation must short-circuit, %d requests sent", got)
	}
}

func TestAdminUpdateBrokerConfigSendsPropertiesText(t *testing.T) {
	c := newAdminCluster(t)
	requireNoError(t, "updateBrokerConfig",
		c.admin.UpdateBrokerConfig(c.brokerSrv.addr, map[string]string{"maxMessageSize": "8388608"}, 0))
	req := c.brokerLog.first(remoting.ReqUpdateBrokerConfig)
	if req == nil {
		t.Fatal("no UPDATE_BROKER_CONFIG request")
	}
	if got := string(req.Body); got != "maxMessageSize=8388608\n" {
		t.Errorf("body = %q, want properties text", got)
	}
}

// ---------------- nameserver KV ----------------

// KV puts/deletes are BROADCAST: one rejecting nameserver must fail the whole
// call (Java's putKVConfigValue/deleteKVConfigValue semantics).
//
// The nameserver list has to be set BEFORE Start — the instance copies it once,
// exactly like Java's MQClientInstance, so a later SetNameServerAddresses is
// deliberately inert.
func TestAdminKVConfigBroadcastsToEveryNameserver(t *testing.T) {
	c := newAdminCluster(t)
	failing := startMockServer(t, func(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
		if req.Code == remoting.ReqPutKVConfig || req.Code == remoting.ReqDeleteKVConfig {
			return okCode(remoting.RespSystemError, "kv rejected")
		}
		return nil
	})

	admin := NewDefaultMQAdminExt(nil)
	admin.SetNameServerAddresses([]string{c.nsSrv.addr, failing.addr})
	admin.SetInstanceName(uniqueSuffix(t))
	admin.SetTimeoutMillis(5000)
	requireNoError(t, "admin start", admin.Start())
	t.Cleanup(admin.Shutdown)

	err := admin.PutKVConfig("ns1", "k1", "v1")
	if err == nil {
		t.Fatal("one failing nameserver must fail the broadcast")
	}
	if !strings.Contains(err.Error(), "kv rejected") {
		t.Errorf("error should carry the nameserver remark, got %v", err)
	}
	if got := c.nsLog.count(remoting.ReqPutKVConfig); got != 1 {
		t.Errorf("PUT_KV_CONFIG hits on the healthy nameserver = %d, want 1", got)
	}

	req := c.nsLog.first(remoting.ReqPutKVConfig)
	for k, want := range map[string]string{"namespace": "ns1", "key": "k1", "value": "v1"} {
		if got := ext(t, req, k); got != want {
			t.Errorf("PUT_KV_CONFIG %s = %q, want %q", k, got, want)
		}
	}
}

func TestAdminGetKVConfigAndList(t *testing.T) {
	c := newAdminCluster(t)
	value, found, err := c.admin.GetKVConfig("ns1", "k1")
	requireNoError(t, "getKVConfig", err)
	if !found || value != "v1" {
		t.Fatalf("getKVConfig = (%q, %v)", value, found)
	}
	table, err := c.admin.GetKVListByNamespace("ns1")
	requireNoError(t, "getKVListByNamespace", err)
	if table.Table["k1"] != "v1" {
		t.Fatalf("kv table = %v", table.Table)
	}
}

// ---------------- subscription groups ----------------

// GET_ALL_SUBSCRIPTIONGROUP_CONFIG(201) is PAGED: accumulate while
// groupSeq < totalGroupNum-1.
func TestAdminGetAllSubscriptionGroupAccumulatesPages(t *testing.T) {
	c := newAdminCluster(t)
	wrapper, err := c.admin.GetAllSubscriptionGroup(c.brokerSrv.addr, 0)
	requireNoError(t, "getAllSubscriptionGroup", err)
	// Pages are handed out one group at a time, and Java (hence the Python and
	// Go ports) stops once groupSeq >= totalGroupNum-1 — so with three groups
	// it makes two requests. Annotated deliberately: the -1 looks like an
	// off-by-one but is Java's actual condition, and deviating from it would
	// make this port page differently from the reference implementation.
	for _, name := range []string{"G1", "G2"} {
		if wrapper.SubscriptionGroupTable[name] == nil {
			t.Errorf("group %s missing; got %v", name, keysOfGroupTable(wrapper))
		}
	}
	pages := c.brokerLog.all(remoting.ReqGetAllSubscriptionGroupConfig)
	if len(pages) != 2 {
		t.Fatalf("expected 2 pages, got %d", len(pages))
	}
	if got := ext(t, pages[1], "groupSeq"); got != "1" {
		t.Errorf("second page groupSeq = %q, want 1", got)
	}
	if got := ext(t, pages[0], "maxGroupNum"); got != "10000" {
		t.Errorf("maxGroupNum = %q", got)
	}
	if hasExt(pages[0], "dataVersion") {
		t.Error("the first page must not carry dataVersion")
	}
	if !hasExt(pages[1], "dataVersion") {
		t.Error("later pages must echo dataVersion")
	}
}

// An older broker omits totalGroupNum and returns everything in one round.
func TestAdminGetAllSubscriptionGroupSingleRoundWithoutTotalGroupNum(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqGetAllSubscriptionGroupConfig] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		w := remoting.NewSubscriptionGroupWrapper()
		w.SubscriptionGroupTable["G1"] = remoting.NewSubscriptionGroupConfig("G1")
		return respOK(w.Encode())
	}
	wrapper, err := c.admin.GetAllSubscriptionGroup(c.brokerSrv.addr, 0)
	requireNoError(t, "getAllSubscriptionGroup", err)
	if wrapper.SubscriptionGroupTable["G1"] == nil {
		t.Fatalf("groups = %v", keysOfGroupTable(wrapper))
	}
	if got := c.brokerLog.count(remoting.ReqGetAllSubscriptionGroupConfig); got != 1 {
		t.Fatalf("pages requested = %d, want 1", got)
	}
}

func TestAdminDeleteSubscriptionGroupSendsCleanOffset(t *testing.T) {
	c := newAdminCluster(t)
	requireNoError(t, "deleteSubscriptionGroup",
		c.admin.DeleteSubscriptionGroup(c.brokerSrv.addr, "G1", true))
	req := c.brokerLog.first(remoting.ReqDeleteSubscriptionGroup)
	if got := ext(t, req, "groupName"); got != "G1" {
		t.Errorf("groupName = %q", got)
	}
	if got := ext(t, req, "cleanOffset"); got != "true" {
		t.Errorf("cleanOffset = %q, want true", got)
	}
}

// ---------------- connections / running info ----------------

func TestAdminExamineConsumerConnectionInfo(t *testing.T) {
	c := newAdminCluster(t)
	cc, err := c.admin.ExamineConsumerConnectionInfo("G1", "")
	requireNoError(t, "examineConsumerConnectionInfo", err)
	if cc.ConsumeType != "CONSUME_PASSIVELY" || cc.MessageModel != "CLUSTERING" {
		t.Fatalf("connection = %+v", cc)
	}
	if len(cc.ConnectionSet) != 1 || cc.ConnectionSet[0].ClientID != "cid-1" {
		t.Fatalf("connectionSet = %+v", cc.ConnectionSet)
	}
	if _, ok := cc.SubscriptionTable["MyTopic"]; !ok {
		t.Fatalf("subscriptionTable = %v", cc.SubscriptionTable)
	}
	// No brokerAddr given -> the admin picks one out of the cluster info.
	req := c.brokerLog.first(remoting.ReqGetConsumerConnectionList)
	if got := ext(t, req, "consumerGroup"); got != "G1" {
		t.Errorf("consumerGroup = %q", got)
	}
	if c.nsLog.count(remoting.ReqGetBrokerClusterInfo) == 0 {
		t.Error("an empty brokerAddr must fall back to the cluster info")
	}
}

// ConsumerRunningInfo.mqTable is MessageQueue-keyed, i.e. fastjson2 inline
// object keys — a strict JSON reader cannot parse it.
func TestAdminExamineConsumerRunningInfoDecodesInlineKeys(t *testing.T) {
	c := newAdminCluster(t)
	ri, err := c.admin.ExamineConsumerRunningInfo("G1", "cid-1", false, c.brokerSrv.addr)
	requireNoError(t, "examineConsumerRunningInfo", err)
	if ri.Properties[remoting.PropConsumeType] != "CONSUME_PASSIVELY" {
		t.Fatalf("properties = %v", ri.Properties)
	}
	q := common.NewMessageQueue("MyTopic", testBrokerName, 0)
	info, ok := ri.MQTable[q]
	if !ok {
		t.Fatalf("mqTable = %v", ri.MQTable)
	}
	if info["lock"] != true {
		t.Errorf("mqTable[%s] = %v", q, info)
	}
	req := c.brokerLog.first(remoting.ReqGetConsumerRunningInfo)
	if got := ext(t, req, "jstackEnable"); got != "false" {
		t.Errorf("jstackEnable = %q", got)
	}
}

// ---------------- consume stats ----------------

func TestAdminExamineConsumeStatsAndGroupMerge(t *testing.T) {
	c := newAdminCluster(t)
	stats, err := c.admin.ExamineConsumeStats(c.brokerSrv.addr, "G1", "MyTopic", nil)
	requireNoError(t, "examineConsumeStats", err)
	q := common.NewMessageQueue("MyTopic", testBrokerName, 0)
	w := stats.OffsetTable[q]
	if w == nil {
		t.Fatalf("offsetTable = %v", stats.OffsetTable)
	}
	if w.Lag() != 6 {
		t.Errorf("lag = %d, want brokerOffset-consumerOffset = 6", w.Lag())
	}
	if stats.TotalLag() != 6 {
		t.Errorf("totalLag = %d", stats.TotalLag())
	}

	merged, err := c.admin.ExamineConsumeStatsGroup("G1", "")
	requireNoError(t, "examineConsumeStatsGroup", err)
	if merged.OffsetTable[q] == nil {
		t.Fatalf("merged offsetTable = %v", merged.OffsetTable)
	}
	// The group path must route through %RETRY%<group>.
	if got := c.nsLog.count(remoting.ReqGetRouteInfoByTopic); got == 0 {
		t.Error("examineConsumeStatsGroup must resolve the retry-topic route")
	}
}

func TestAdminFetchConsumeStatsInBrokerUsesConsumeStatsListKey(t *testing.T) {
	c := newAdminCluster(t)
	list, err := c.admin.FetchConsumeStatsInBroker(c.brokerSrv.addr, true, 0)
	requireNoError(t, "fetchConsumeStatsInBroker", err)
	if list.TotalDiff != 6 {
		t.Fatalf("totalDiff = %d", list.TotalDiff)
	}
	if !list.HasBroker || list.BrokerAddr != c.brokerSrv.addr {
		t.Fatalf("brokerAddr = %q (has=%v)", list.BrokerAddr, list.HasBroker)
	}
	req := c.brokerLog.first(remoting.ReqGetBrokerConsumeStats)
	if got := ext(t, req, "isOrder"); got != "true" {
		t.Errorf("isOrder = %q", got)
	}
}

// ---------------- offsets ----------------

func TestAdminOffsetQueries(t *testing.T) {
	c := newAdminCluster(t)
	mq := common.NewMessageQueue("MyTopic", testBrokerName, 0)

	if got, err := c.admin.MaxOffset(mq); err != nil || got != 26 {
		t.Errorf("MaxOffset = (%d, %v)", got, err)
	}
	if got, err := c.admin.MinOffset(mq); err != nil || got != 0 {
		t.Errorf("MinOffset = (%d, %v)", got, err)
	}
	if got, err := c.admin.SearchOffset(mq, 1700000000000); err != nil || got != 17 {
		t.Errorf("SearchOffset = (%d, %v)", got, err)
	}
	if got, err := c.admin.EarliestMsgStoreTime(mq); err != nil || got != 1699999999000 {
		t.Errorf("EarliestMsgStoreTime = (%d, %v)", got, err)
	}

	lower := c.brokerLog.first(remoting.ReqSearchOffsetByTimestamp)
	if got := ext(t, lower, "boundaryType"); got != "LOWER" {
		t.Errorf("boundaryType = %q, want LOWER", got)
	}
	// The three offset RPCs each carry topic + queueId.
	for _, code := range []int32{remoting.ReqGetMaxOffset, remoting.ReqGetMinOffset, remoting.ReqGetEarliestMsgStoretime} {
		req := c.brokerLog.first(code)
		if req == nil {
			t.Fatalf("request %d never sent", code)
		}
		if got := ext(t, req, "topic"); got != "MyTopic" {
			t.Errorf("code %d topic = %q", code, got)
		}
		if got := ext(t, req, "queueId"); got != "0" {
			t.Errorf("code %d queueId = %q", code, got)
		}
	}

	c.admin.SearchUpperBoundaryOffset(mq, 1)
	upper := c.brokerLog.all(remoting.ReqSearchOffsetByTimestamp)[1]
	if got := ext(t, upper, "boundaryType"); got != "UPPER" {
		t.Errorf("upper boundaryType = %q", got)
	}
}

func TestAdminExamineAndUpdateConsumerOffset(t *testing.T) {
	c := newAdminCluster(t)
	mq := common.NewMessageQueue("MyTopic", testBrokerName, 0)

	offset, found, err := c.admin.ExamineConsumerOffset("G1", mq)
	requireNoError(t, "examineConsumerOffset", err)
	if !found || offset != 20 {
		t.Fatalf("examineConsumerOffset = (%d, %v)", offset, found)
	}
	requireNoError(t, "updateConsumerOffset", c.admin.UpdateConsumerOffset("G1", mq, 21))
	req := c.brokerLog.first(remoting.ReqUpdateConsumerOffset)
	for k, want := range map[string]string{
		"consumerGroup": "G1", "topic": "MyTopic", "queueId": "0", "commitOffset": "21",
	} {
		if got := ext(t, req, k); got != want {
			t.Errorf("UPDATE_CONSUMER_OFFSET %s = %q, want %q", k, got, want)
		}
	}
}

// The force field is spelled `isForce`: with `force` the broker's isForce
// stays false and timestamp=-1 echoes consumerOffset instead of jumping to
// maxOffset.
func TestAdminResetOffsetByTimestampSendsIsForce(t *testing.T) {
	c := newAdminCluster(t)
	table, err := c.admin.ResetOffsetByTimestamp("MyTopic", "G1", -1, true, "", false)
	requireNoError(t, "resetOffsetByTimestamp", err)
	if len(table) != 2 {
		t.Fatalf("reset table = %v", table)
	}

	req := c.brokerLog.first(remoting.ReqInvokeBrokerToResetOffset)
	if req == nil {
		t.Fatal("no INVOKE_BROKER_TO_RESET_OFFSET request")
	}
	if got := ext(t, req, "isForce"); got != "true" {
		t.Errorf("isForce = %q, want true", got)
	}
	if hasExt(req, "force") {
		t.Error("must NOT send `force` — the broker reads `isForce`")
	}
	if got := ext(t, req, "offset"); got != "-1" {
		t.Errorf("offset = %q, want -1 (Java's null-offset sentinel)", got)
	}
	if got := ext(t, req, "timestamp"); got != "-1" {
		t.Errorf("timestamp = %q", got)
	}
	if hasExt(req, "queueId") {
		t.Error("the whole-topic overload must not send queueId")
	}
	if req.Language == remoting.LangCPP {
		t.Error("isCpp=false must not switch the request language")
	}
}

func TestAdminResetOffsetByTimestampCppLanguageAndQueueID(t *testing.T) {
	c := newAdminCluster(t)
	_, err := c.admin.ResetOffsetByTimestamp("MyTopic", "G1", -1, true, "", true)
	requireNoError(t, "resetOffsetByTimestamp(cpp)", err)
	req := c.brokerLog.first(remoting.ReqInvokeBrokerToResetOffset)
	if req.Language != remoting.LangCPP {
		t.Fatalf("language = %d, want CPP(%d)", req.Language, remoting.LangCPP)
	}

	// The single-queue overload sends BOTH RPCs: 15 first, then 222.
	before := len(c.brokerLog.codes())
	_, err = c.admin.ResetOffsetByQueueID(c.brokerSrv.addr, "G1", "MyTopic", 0, 5)
	requireNoError(t, "resetOffsetByQueueID", err)
	after := c.brokerLog.codes()[before:]
	if len(after) != 2 || after[0] != remoting.ReqUpdateConsumerOffset ||
		after[1] != remoting.ReqInvokeBrokerToResetOffset {
		t.Fatalf("request order = %v, want [UPDATE_CONSUMER_OFFSET, INVOKE_BROKER_TO_RESET_OFFSET]", after)
	}
	qReq := c.brokerLog.all(remoting.ReqInvokeBrokerToResetOffset)[1]
	if got := ext(t, qReq, "queueId"); got != "0" {
		t.Errorf("queueId = %q", got)
	}
	if got := ext(t, qReq, "offset"); got != "5" {
		t.Errorf("offset = %q, want 5", got)
	}
	if got := ext(t, qReq, "isForce"); got != "false" {
		t.Errorf("single-queue overload must not force, got isForce=%q", got)
	}
}

// ---------------- message query ----------------

func TestAdminQueryMessageFiltersClientSide(t *testing.T) {
	c := newAdminCluster(t)
	messages, err := c.admin.QueryMessage("MyTopic", "Key1", 32, 0, 1700000000000)
	requireNoError(t, "queryMessage", err)
	if len(messages) != 1 {
		t.Fatalf("got %d messages, want 1 (the other must be filtered out client-side)", len(messages))
	}
	if string(messages[0].Body) != "hello" {
		t.Errorf("body = %q", messages[0].Body)
	}
	if messages[0].BrokerName != testBrokerName {
		t.Errorf("brokerName = %q", messages[0].BrokerName)
	}
	req := c.brokerLog.first(remoting.ReqQueryMessage)
	if got := ext(t, req, "indexType"); got != common.IndexKeyType {
		t.Errorf("indexType = %q, want K", got)
	}
	if hasExt(req, common.UniqueMsgQueryFlag) {
		t.Error("a plain key query must not set _UNIQUE_KEY_QUERY")
	}
	for _, k := range []string{"topic", "key", "maxNum", "beginTimestamp", "endTimestamp"} {
		if !hasExt(req, k) {
			t.Errorf("QUERY_MESSAGE is missing %q", k)
		}
	}
}

func TestAdminQueryMessageByUniqKeySetsUniqueFlag(t *testing.T) {
	c := newAdminCluster(t)
	// The uniqKey check is `decoded.MsgID == key`. For a broker-stored record
	// the decoder derives MsgID from the store host + commitLog offset, so the
	// test encodes the record and decodes it back to learn the id the client
	// will actually see.
	stored := newQueryableExt(c.t, "MyTopic", 0, 7, "hello", "K", "MSGID0000000001")
	raw := encodeStored(c.t, stored)
	decoded := common.DecodeMessages(raw)
	if len(decoded) != 1 {
		t.Fatalf("round trip produced %d messages", len(decoded))
	}
	msgID := decoded[0].MsgID

	c.brokerAnswer[remoting.ReqQueryMessage] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(raw)
	}
	msg, err := c.admin.QueryMessageByUniqKey("MyTopic", msgID)
	requireNoError(t, "queryMessageByUniqKey", err)
	if msg == nil || string(msg.Body) != "hello" {
		t.Fatalf("message = %v", msg)
	}
	req := c.brokerLog.first(remoting.ReqQueryMessage)
	if got := ext(t, req, "indexType"); got != common.IndexUniqueType {
		t.Errorf("indexType = %q, want U", got)
	}
	if got := ext(t, req, common.UniqueMsgQueryFlag); got != "true" {
		t.Errorf("%s = %q, want true", common.UniqueMsgQueryFlag, got)
	}
}

func TestAdminQueryConsumeQueue(t *testing.T) {
	c := newAdminCluster(t)
	body, err := c.admin.QueryConsumeQueue(c.brokerSrv.addr, "MyTopic", 0, 1, 32, "G1")
	requireNoError(t, "queryConsumeQueue", err)
	if body.MaxQueueIndex != 26 || body.MinQueueIndex != 0 {
		t.Fatalf("indexes = [%d,%d]", body.MinQueueIndex, body.MaxQueueIndex)
	}
	if body.FilterData == nil || *body.FilterData != "*" {
		t.Errorf("filterData = %v", body.FilterData)
	}
	if len(body.QueueData) != 1 || body.QueueData[0].PhysicOffset != 100 {
		t.Errorf("queueData = %+v", body.QueueData)
	}
	req := c.brokerLog.first(remoting.ReqQueryConsumeQueue)
	for k, want := range map[string]string{
		"topic": "MyTopic", "queueId": "0", "index": "1", "count": "32", "consumerGroup": "G1",
	} {
		if got := ext(t, req, k); got != want {
			t.Errorf("QUERY_CONSUME_QUEUE %s = %q, want %q", k, got, want)
		}
	}
}

// ---------------- message track ----------------

func TestAdminMessageTrackDetailClassifiesGroups(t *testing.T) {
	c := newAdminCluster(t)
	// G2 is offline, G1 consumes passively and already passed the message.
	c.brokerAnswer[remoting.ReqGetConsumerConnectionList] = func(req *remoting.RemotingCommand) *remoting.RemotingCommand {
		group := ""
		if v, ok := req.ExtFields().Get("consumerGroup"); ok {
			group = v
		}
		if group == "G2" {
			// 5.x answers SYSTEM_ERROR + "the consumer group[x] not online".
			return okCode(remoting.RespSystemError, "the consumer group[G2] not online")
		}
		cc := remoting.NewConsumerConnection()
		cc.ConsumeType = "CONSUME_PASSIVELY"
		cc.SubscriptionTable["MyTopic"] = map[string]any{"tagsSet": []any{"Other"}, "codeSet": []any{}}
		return respOK(cc.Encode())
	}

	msg := newQueryableExt(t, "MyTopic", 0, 5, "hello", "Key1", "MSGID0000000001")
	msg.StoreHost = c.brokerSrv.addr
	msg.SetTags("TagA")

	tracks, err := c.admin.MessageTrackDetail(msg)
	requireNoError(t, "messageTrackDetail", err)
	if len(tracks) != 2 {
		t.Fatalf("tracks = %d, want 2", len(tracks))
	}
	byGroup := map[string]*MessageTrack{}
	for _, tr := range tracks {
		byGroup[tr.ConsumerGroup] = tr
	}
	if got := byGroup["G2"]; got == nil || got.TrackType != TrackUnknown {
		t.Errorf("G2 track = %v (a SYSTEM_ERROR is not CONSUMER_NOT_ONLINE)", got)
	}
	g1 := byGroup["G1"]
	if g1 == nil {
		t.Fatal("G1 track missing")
	}
	// consumerOffset 20 > queueOffset 5 -> consumed; the subscription only
	// carries TagA... no: only "Other", so the record was filtered out.
	if g1.TrackType != TrackConsumedButFiltered {
		t.Fatalf("G1 track = %v, want CONSUMED_BUT_FILTERED", g1.TrackType)
	}
}

func TestAdminMessageTrackDetailPullAndBroadcast(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqGetConsumerConnectionList] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		cc := remoting.NewConsumerConnection()
		cc.ConsumeType = "CONSUME_ACTIVELY"
		return respOK(cc.Encode())
	}
	msg := newQueryableExt(t, "MyTopic", 0, 5, "hello", "Key1", "MSGID0000000001")
	tracks, err := c.admin.MessageTrackDetail(msg)
	requireNoError(t, "messageTrackDetail", err)
	for _, tr := range tracks {
		if tr.TrackType != TrackPull {
			t.Errorf("group %s track = %s, want PULL", tr.ConsumerGroup, tr.TrackType)
		}
	}
}

// ---------------- cluster info ----------------

func TestAdminClusterInfoAndFirstBroker(t *testing.T) {
	c := newAdminCluster(t)
	info, err := c.admin.FetchBrokerClusterInfo()
	requireNoError(t, "fetchBrokerClusterInfo", err)
	entry := info.BrokerAddrTable[testBrokerName]
	if entry == nil {
		t.Fatalf("brokerAddrTable = %v", info.BrokerAddrTable)
	}
	if entry.BrokerAddrs[0] != c.brokerSrv.addr {
		t.Errorf("master addr = %q, want %q", entry.BrokerAddrs[0], c.brokerSrv.addr)
	}
	if got := info.ClusterAddrTable[testClusterName]; len(got) != 1 || got[0] != testBrokerName {
		t.Errorf("clusterAddrTable = %v", info.ClusterAddrTable)
	}
	addrs := info.BrokerAddrs()
	if len(addrs) != 1 || addrs[0] != c.brokerSrv.addr {
		t.Errorf("BrokerAddrs() = %v", addrs)
	}
	first, err := c.admin.firstBrokerAddr()
	requireNoError(t, "firstBrokerAddr", err)
	if first != c.brokerSrv.addr {
		t.Errorf("firstBrokerAddr = %q", first)
	}
}

func TestAdminGetUserTopicConfigFiltersSystemTopics(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqGetAllTopicConfig] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		w := remoting.NewTopicConfigSerializeWrapper()
		w.TopicConfigTable["MyTopic"] = remoting.NewTopicConfig("MyTopic")
		w.TopicConfigTable["rmq_sys_watermark"] = remoting.NewTopicConfig("rmq_sys_watermark")
		w.TopicConfigTable["%RETRY%G1"] = remoting.NewTopicConfig("%RETRY%G1")
		return respOK(w.Encode())
	}
	c.brokerAnswer[remoting.ReqGetSystemTopicListFromBroker] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		tl := remoting.NewTopicList()
		tl.TopicList = []string{"rmq_sys_watermark"}
		return respOK(tl.Encode())
	}
	wrapper, err := c.admin.GetUserTopicConfig(c.brokerSrv.addr, false, 0)
	requireNoError(t, "getUserTopicConfig", err)
	if len(wrapper.TopicConfigTable) != 1 || wrapper.TopicConfigTable["MyTopic"] == nil {
		t.Fatalf("user topics = %v", keysOfTopicTable(wrapper))
	}
	withSpecial, err := c.admin.GetUserTopicConfig(c.brokerSrv.addr, true, 0)
	requireNoError(t, "getUserTopicConfig(special)", err)
	if withSpecial.TopicConfigTable["%RETRY%G1"] == nil {
		t.Fatalf("specialTopic must keep %%RETRY%% topics, got %v", keysOfTopicTable(withSpecial))
	}
}

// ---------------- helpers ----------------

type countingHook struct {
	before atomic.Int64
	after  atomic.Int64
}

func (h *countingHook) DoBeforeRequest(string, *remoting.RemotingCommand) { h.before.Add(1) }
func (h *countingHook) DoAfterResponse(string, *remoting.RemotingCommand, *remoting.RemotingCommand) {
	h.after.Add(1)
}

func keysOfGroupTable(w *remoting.SubscriptionGroupWrapper) []string {
	var out []string
	for k := range w.SubscriptionGroupTable {
		out = append(out, k)
	}
	return out
}

func keysOfTopicTable(w *remoting.TopicConfigSerializeWrapper) []string {
	var out []string
	for k := range w.TopicConfigTable {
		out = append(out, k)
	}
	return out
}
