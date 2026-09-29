// Route bean tests: Java-format decode (fastjson2 numeric-string broker ids),
// master requirements of the two queue assemblies, route-change detection
// exclusions and the publish-info ring cursor.
package client

import (
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

const javaRouteBody = `{"orderTopicConf":null,` +
	`"queueDatas":[{"brokerName":"b1","readQueueNums":4,"writeQueueNums":4,"perm":6,"topicSysFlag":0},` +
	`{"brokerName":"b2","readQueueNums":2,"writeQueueNums":2,"perm":4,"topicSysFlag":0},` +
	`{"brokerName":"b3","readQueueNums":3,"writeQueueNums":3,"perm":6,"topicSysFlag":0}],` +
	`"brokerDatas":[{"cluster":"DefaultCluster","brokerName":"b1","brokerAddrs":{"0":"10.0.0.1:10911","1":"10.0.0.2:10911"},"zoneName":null,"enableActingMaster":false},` +
	`{"cluster":"DefaultCluster","brokerName":"b2","brokerAddrs":{"0":"10.0.0.3:10911"},"zoneName":null,"enableActingMaster":false},` +
	`{"cluster":"DefaultCluster","brokerName":"b3","brokerAddrs":{"1":"10.0.0.4:10911"},"zoneName":null,"enableActingMaster":false}],` +
	`"filterServerTable":{}}`

func mustRoute(t *testing.T, body string) *TopicRouteData {
	t.Helper()
	route, err := DecodeTopicRouteData([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return route
}

func TestDecodeTopicRouteDataJavaShape(t *testing.T) {
	route := mustRoute(t, javaRouteBody)
	if len(route.QueueDatas) != 3 || len(route.BrokerDatas) != 3 {
		t.Fatalf("route decode lost entries: %d queues %d brokers", len(route.QueueDatas), len(route.BrokerDatas))
	}
	b1 := route.BrokerDatas[0]
	if b1.BrokerName != "b1" || b1.Cluster != "DefaultCluster" {
		t.Errorf("broker b1 decode mismatch: %+v", b1)
	}
	if len(b1.BrokerAddrs) != 2 || b1.BrokerAddrs[0].ID != 0 || b1.BrokerAddrs[0].Addr != "10.0.0.1:10911" ||
		b1.BrokerAddrs[1].ID != 1 {
		t.Errorf("numeric-string broker ids must decode: %+v", b1.BrokerAddrs)
	}
	if route.OrderTopicConf != nil {
		t.Errorf("orderTopicConf null must stay nil, got %q", *route.OrderTopicConf)
	}
	// Re-encode: orderTopicConf is ALWAYS present (null included); the absent
	// mapping stays absent.
	out := string(route.Encode())
	if !strings.Contains(out, `"orderTopicConf"`) {
		t.Errorf("re-encode dropped orderTopicConf: %s", out)
	}
	if strings.Contains(out, `"topicQueueMappingByBroker"`) {
		t.Errorf("nil mapping must not be written, got %s", out)
	}
}

func TestDecodeRouteMappingEmptyVsAbsent(t *testing.T) {
	empty := mustRoute(t, strings.Replace(javaRouteBody, `"filterServerTable":{}`,
		`"filterServerTable":{},"topicQueueMappingByBroker":{}`, 1))
	if empty.TopicQueueMappingByBroker == nil || len(empty.TopicQueueMappingByBroker) != 0 {
		t.Errorf("explicit {} must decode to empty non-nil, got %+v", empty.TopicQueueMappingByBroker)
	}
}

func TestGetAllMessageQueueMasterRequirement(t *testing.T) {
	route := mustRoute(t, javaRouteBody)
	mqs := route.GetAllMessageQueue("Tt")
	// b1 perm 6 + master: 4 queues; b2 perm 4 (no write): skipped;
	// b3 brokerAddrs has only id 1 (masterless): skipped.
	if len(mqs) != 4 {
		t.Fatalf("publish queues = %d, want 4 (b2 read-only, b3 masterless)", len(mqs))
	}
	for i, mq := range mqs {
		if mq.BrokerName != "b1" || mq.QueueID != int32(i) || mq.Topic != "Tt" {
			t.Errorf("mq[%d] = %+v", i, mq)
		}
	}
}

func TestGetAllSubscribeMessageQueueReadPermOnly(t *testing.T) {
	route := mustRoute(t, javaRouteBody)
	mqs := route.GetAllSubscribeMessageQueue("Tt")
	// b1 read 4 + b2 read 2 (perm 4 IS readable) + b3 read 3, masterlessness
	// irrelevant on the subscribe side.
	if len(mqs) != 9 {
		t.Fatalf("subscribe queues = %d, want 9", len(mqs))
	}
	seenB3 := false
	for _, mq := range mqs {
		if mq.BrokerName == "b3" {
			seenB3 = true
		}
	}
	if !seenB3 {
		t.Errorf("masterless b3 must still appear in the subscribe set")
	}
}

func TestBrokerDataDirtyAddrKeysSkipped(t *testing.T) {
	body := `{"orderTopicConf":null,"queueDatas":[],"brokerDatas":[` +
		`{"cluster":"c","brokerName":"b1","brokerAddrs":{"0":"10.0.0.1:10911","x":"bad"},"zoneName":null,"enableActingMaster":false}],` +
		`"filterServerTable":{}}`
	route := mustRoute(t, body)
	if len(route.BrokerDatas[0].BrokerAddrs) != 1 {
		t.Errorf("dirty key must be skipped, got %+v", route.BrokerDatas[0].BrokerAddrs)
	}
}

func TestSelectBrokerAddr(t *testing.T) {
	route := mustRoute(t, javaRouteBody)
	b1 := route.BrokerDatas[0]
	for i := 0; i < 10; i++ {
		if addr, ok := b1.SelectBrokerAddr(); !ok || addr != "10.0.0.1:10911" {
			t.Fatalf("master must always win, got %q ok=%v", addr, ok)
		}
	}
	b3 := route.BrokerDatas[2]
	if addr, ok := b3.SelectBrokerAddr(); !ok || addr != "10.0.0.4:10911" {
		t.Errorf("slave fallback: got %q ok=%v", addr, ok)
	}
	empty := &BrokerData{BrokerName: "bx"}
	if _, ok := empty.SelectBrokerAddr(); ok {
		t.Errorf("empty brokerAddrs must fail, got ok")
	}
}

func TestTopicRouteDataChanged(t *testing.T) {
	old := mustRoute(t, javaRouteBody)
	if old.TopicRouteDataChanged(mustRoute(t, javaRouteBody)) {
		t.Errorf("identical routes must compare unchanged")
	}
	// topicSysFlag is excluded from the comparison (broker-only flag flip).
	sysFlip := mustRoute(t, javaRouteBody)
	sysFlip.QueueDatas[0].TopicSysFlag = 1
	if sysFlip.TopicRouteDataChanged(old) {
		t.Errorf("topicSysFlag flip must not look like a route change")
	}
	// zoneName / enableActingMaster are excluded (Java BrokerData.equals).
	zoneMove := mustRoute(t, javaRouteBody)
	zoneMove.BrokerDatas[0].ZoneName = "zone-2"
	if zoneMove.TopicRouteDataChanged(old) {
		t.Errorf("zone move must not look like a route change")
	}
	// A real address change does fire.
	addrMove := mustRoute(t, javaRouteBody)
	addrMove.BrokerDatas[0].BrokerAddrs[0].Addr = "10.0.9.9:10911"
	if !addrMove.TopicRouteDataChanged(old) {
		t.Errorf("address change must be detected")
	}
	// Queue count change fires even when list order differs.
	grow := mustRoute(t, javaRouteBody)
	grow.QueueDatas[0].WriteQueueNums = 8
	if !grow.TopicRouteDataChanged(old) {
		t.Errorf("queue growth must be detected")
	}
	var nilRoute *TopicRouteData
	if !old.TopicRouteDataChanged(nilRoute) {
		t.Errorf("no old route means changed")
	}
}

func TestTopicPublishInfoRing(t *testing.T) {
	info := NewTopicPublishInfo()
	if _, _, err := info.SelectOneMessageQueue(nil); err == nil {
		t.Errorf("empty publish info must error")
	}
	if info.OK() {
		t.Errorf("empty publish info must not be OK")
	}
	route := mustRoute(t, javaRouteBody)
	info.UpdateFromRoute(route, "Tt")
	if !info.OK() {
		t.Fatalf("publish info must be OK after update")
	}
	if info.OrderTopic() {
		t.Errorf("null orderTopicConf means orderTopic=false")
	}
	if len(info.MsgQueueList()) != 4 {
		t.Fatalf("queue list = %d, want 4", len(info.MsgQueueList()))
	}
	// Unconditional round robin: 0,1,2,3,0.
	for i := 0; i < 5; i++ {
		mq, ok, err := info.SelectOneMessageQueue(nil)
		if err != nil || !ok {
			t.Fatalf("select %d failed: ok=%v err=%v", i, ok, err)
		}
		if want := int32(i % 4); mq.QueueID != want {
			t.Errorf("select %d: queueId=%d want %d", i, mq.QueueID, want)
		}
	}
	// The negative ringIndex (Java nextAddrAndIncrement overflow path).
	info.ResetIndex()
	mq, ok, err := info.SelectOneMessageQueue([]QueueFilter{
		func(q *common.MessageQueue) bool { return q.BrokerName == "b1" },
	})
	if err != nil || !ok || mq.BrokerName != "b1" {
		t.Fatalf("filter select failed: %+v ok=%v err=%v", mq, ok, err)
	}
	_, ok, err = info.SelectOneMessageQueue([]QueueFilter{
		func(q *common.MessageQueue) bool { return false },
	})
	if err != nil || ok {
		t.Errorf("all-filtered-out must be ok=false, got ok=%v err=%v", ok, err)
	}
}

func TestSubscriptionRouteFilterViaFindBrokerAddrInRoute(t *testing.T) {
	route := mustRoute(t, javaRouteBody)
	if addr, ok := FindBrokerAddrInRoute(route, "b2"); !ok || addr != "10.0.0.3:10911" {
		t.Errorf("b2 = %q ok=%v", addr, ok)
	}
	if _, ok := FindBrokerAddrInRoute(route, "nope"); ok {
		t.Errorf("unknown broker must miss")
	}
}

func TestSubscriptionDataHeartbeatRoutingKeys(t *testing.T) {
	// The heartbeat carries subscription data; make sure FilterAPI output
	// survives the remoting round trip through a ConsumerData.
	sd, err := remoting.FilterAPI{}.BuildSubscriptionData("Tt", "TagA || TagB")
	if err != nil {
		t.Fatal(err)
	}
	cd := remoting.NewConsumerData("CG", remoting.ConsumeTypeConsumePassively,
		remoting.MessageModelClustering, remoting.ConsumeFromWhereLastOffset)
	cd.AddSubscriptionData(sd)
	cd.AddSubscriptionData(sd) // duplicate must fold
	if len(cd.SubscriptionDataSet) != 1 {
		t.Fatalf("duplicate subscription must fold, got %d", len(cd.SubscriptionDataSet))
	}
	hb := remoting.NewHeartbeatData("cid")
	hb.AddConsumerData(cd)
	back, err := remoting.DecodeHeartbeatData(hb.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if len(back.ConsumerDataSet) != 1 || len(back.ConsumerDataSet[0].SubscriptionDataSet) != 1 {
		t.Errorf("heartbeat round trip lost data: %+v", back.ConsumerDataSet)
	}
}
