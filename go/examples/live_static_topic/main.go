// live_static_topic verifies the static-topic (513 + endpoint rewrite) path
// end-to-end against a real RocketMQ 5.x cluster — the seven-end canonical
// scenario, Go version (python/verify_static_topic_live.py,
// rust/examples/live_static_topic.rs, cpp/examples/live_static_topic.cpp,
// csharp/examples/LiveStaticTopic.cs, php/examples/live_static_topic.php).
//
//	go run ./examples/live_static_topic -ns 127.0.0.1:9876
//
// S1 createStaticTopic(513): single broker, totalQueues=8, physical 4 queues.
// S2 route read-back: TopicQueueMappingByBroker non-empty.
// S3 endpoints table: 8 entries (0-3 → broker, 4-7 → __syslo__none__).
// S4 instance rewrite: GetBrokerNameFromMessageQueue resolves the mock queue
//
//	name to the real broker; out-of-table ids and non-static topics pass
//	through untouched.
//
// S5 end-to-end: plain producer sends 4, PUSH consumer consumes 4.
//
// Prints PASS/FAIL per item; the last line is `PASS=<n> FAIL=<n>`.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/client"
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

const (
	// MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX + "__global__"[2:] and
	// MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST (client/static_topic.go).
	mockName   = "__syslo__global__"
	mockAbsent = "__syslo__none__"

	physicalQueues = 4
	totalQueues    = 8
)

var (
	passCount int
	failCount int
)

func check(name string, ok bool, detail string) {
	if ok {
		passCount++
		fmt.Printf("PASS  %s\n", name)
		return
	}
	failCount++
	if detail == "" {
		fmt.Printf("FAIL  %s\n", name)
		return
	}
	fmt.Printf("FAIL  %s - %s\n", name, detail)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "static_topic live: "+format+"\n", args...)
	os.Exit(2)
}

// mappingDetail builds the single-broker TopicQueueMappingDetail (the 513
// body, Java TopicQueueMappingDetail fastjson2 field names — same field set
// as the python reference version). Scope must be "__global__" explicitly:
// the zero-value fallback in TopicQueueMappingDetail.toJSONValue writes the
// legacy "GLOBAL", which is NOT what the broker round-trips.
func mappingDetail(topic, bname string) *client.TopicQueueMappingDetail {
	d := &client.TopicQueueMappingDetail{
		Topic:        topic,
		Scope:        "__global__",
		TotalQueues:  totalQueues,
		Bname:        bname,
		Epoch:        0,
		Dirty:        false,
		CurrIDMap:    map[int]int{},
		HostedQueues: map[int][]client.LogicQueueMappingItem{},
	}
	for gid := 0; gid < physicalQueues; gid++ {
		d.CurrIDMap[gid] = gid
		d.HostedQueues[gid] = []client.LogicQueueMappingItem{{
			Gen:         0,
			QueueID:     int32(gid),
			Bname:       bname,
			LogicOffset: 0,
			StartOffset: 0,
			EndOffset:   -1,
			TimeOfStart: -1,
			TimeOfEnd:   -1,
		}}
	}
	return d
}

type collector struct {
	bodies []string
}

func (c *collector) ConsumeMessage(msgs []*common.MessageExt,
	_ *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	for _, m := range msgs {
		c.bodies = append(c.bodies, string(m.Body))
	}
	return client.ConsumeSuccess
}

func main() {
	ns := flag.String("ns", "127.0.0.1:9876", "nameserver address")
	stamp := time.Now().Unix()
	topic := flag.String("topic", fmt.Sprintf("StaticLive_%d", stamp), "static topic")
	flag.Parse()
	group := fmt.Sprintf("StaticPG_%d", stamp)

	fmt.Printf("=== Go static topic live: ns=%s topic=%s ===\n", *ns, *topic)

	// ---------------- S1 createStaticTopic(513) ----------------
	fmt.Println("\nS1 admin up, CreateStaticTopic(513)")
	admin := client.NewDefaultMQAdminExt(nil)
	admin.SetNamesrvAddr(*ns)
	admin.SetInstanceName("STATIC_TOPIC_LIVE_ADMIN")
	if err := admin.Start(); err != nil {
		fatal("admin start: %v", err)
	}
	defer admin.Shutdown()

	ci, err := admin.FetchBrokerClusterInfo()
	if err != nil {
		fatal("fetchBrokerClusterInfo: %v", err)
	}
	brokerName := ""
	brokerAddr := ""
	for name, bd := range ci.BrokerAddrTable {
		if a := bd.BrokerAddrs[0]; a != "" {
			brokerName, brokerAddr = name, a
			break
		}
	}
	check("S1 broker 地址解析", brokerAddr != "", brokerName+"("+brokerAddr+")")
	if brokerAddr == "" {
		fatal("no master broker registered")
	}

	err = admin.CreateStaticTopic(brokerAddr, "TBW102",
		&remoting.TopicConfig{
			TopicName:      *topic,
			ReadQueueNums:  physicalQueues,
			WriteQueueNums: physicalQueues,
			// Go's TopicConfig has no zero-value defaults (unlike the Java/
			// python/C#/php TopicConfig whose perm defaults to 6): perm=0
			// would create an unwritable+unreadable topic and the producer
			// would see zero publish queues.
			Perm: common.PermRead | common.PermWrite,
		}, mappingDetail(*topic, brokerName), true)
	check("S1 CreateStaticTopic(513) 被接受", err == nil,
		fmt.Sprintf("totalQueues=%d err=%v", totalQueues, err))

	// A pull consumer is just a route-carrier here: its Instance() exposes
	// the endpoints table for S3/S4 (push consumers in Go do not expose it).
	pull, err := client.NewDefaultMQPullConsumer("StaticRouteCarrier_" + fmt.Sprint(stamp))
	if err != nil {
		fatal("pull consumer: %v", err)
	}
	pull.SetNameServerAddresses([]string{*ns})
	pull.SetInstanceName("STATIC_TOPIC_LIVE_ROUTE")
	if err := pull.Subscribe(*topic, "*"); err != nil {
		fatal("pull subscribe: %v", err)
	}
	if err := pull.Start(); err != nil {
		fatal("pull start: %v", err)
	}
	defer pull.Shutdown()

	// ---------------- S2/S3 route read-back + endpoints ----------------
	fmt.Println("\nS2/S3 route read-back + endpoints table")
	var endpoints map[common.MessageQueue]string
	lastDetail := "no poll ran"
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		// Fresh admin RPC per poll (same shape as the other ends). Note
		// GetTopicRouteData on the instance cache short-circuits on a hit,
		// so a route fetched before the mapping propagated would never
		// refresh — do not poll it.
		route, err := admin.ExamineTopicRoute(*topic)
		if err != nil {
			lastDetail = "examine err: " + err.Error()
		} else if route == nil {
			lastDetail = "examine returned nil route"
		} else {
			lastDetail = fmt.Sprintf("mappingEntries=%d queueDatas=%d brokerDatas=%d",
				len(route.TopicQueueMappingByBroker), len(route.QueueDatas), len(route.BrokerDatas))
			if len(route.TopicQueueMappingByBroker) > 0 {
				if inst := pull.Instance(); inst != nil {
					// Force the carrier instance past its possibly-stale cache.
					_, _ = inst.UpdateTopicRouteInfoFromNameServer(*topic, 5000, false)
					endpoints = inst.EndPointsOf(*topic)
				}
				break
			}
		}
		time.Sleep(2 * time.Second)
	}
	check("S2 路由携带 TopicQueueMappingByBroker", len(endpoints) > 0, lastDetail)
	if len(endpoints) == 0 {
		fmt.Printf("PASS=%d FAIL=%d\n", passCount, failCount)
		os.Exit(1)
	}

	check("S3 端点总数 = totalQueues", len(endpoints) == totalQueues,
		fmt.Sprintf("got=%d want=%d", len(endpoints), totalQueues))

	hostedOk := true
	for gid := 0; gid < physicalQueues; gid++ {
		mq := common.NewMessageQueue(*topic, mockName, int32(gid))
		if endpoints[mq] != brokerName {
			hostedOk = false
		}
	}
	check("S3 承载 id 0-3 → broker", hostedOk, "")

	unhostedOk := true
	for gid := physicalQueues; gid < totalQueues; gid++ {
		mq := common.NewMessageQueue(*topic, mockName, int32(gid))
		if endpoints[mq] != mockAbsent {
			unhostedOk = false
		}
	}
	check("S3 无承载 id 4-7 → __syslo__none__", unhostedOk, "")

	// ---------------- S4/S5 instance rewrite + end-to-end ----------------
	fmt.Println("\nS4/S5 instance rewrite + end-to-end")
	inst := pull.Instance()
	rewriteOk := inst != nil
	if inst != nil {
		for gid := 0; gid < physicalQueues; gid++ {
			if inst.GetBrokerNameFromMessageQueue(
				common.NewMessageQueue(*topic, mockName, int32(gid))) != brokerName {
				rewriteOk = false
				break
			}
		}
	}
	check("S4 实例把 mock 队列重写到真实 broker", rewriteOk, "")

	// id totalQueues-1 IS in the table, and the Java semantics return the
	// endpoint value, i.e. the sentinel itself; the passthrough case must
	// use an out-of-table id (the Go unit test uses 99).
	unhostedRewrite := inst.GetBrokerNameFromMessageQueue(
		common.NewMessageQueue(*topic, mockName, totalQueues-1))
	check("S4 无承载 id 返回哨兵值", unhostedRewrite == mockAbsent, unhostedRewrite)

	outsideRewrite := inst.GetBrokerNameFromMessageQueue(
		common.NewMessageQueue(*topic, mockName, 99))
	check("S4 表外 id 透传 mock 名", outsideRewrite == mockName, outsideRewrite)

	plainRewrite := inst.GetBrokerNameFromMessageQueue(
		common.NewMessageQueue("Plain_"+fmt.Sprint(stamp), brokerName, 0))
	check("S4 非静态主题原样透传", plainRewrite == brokerName, plainRewrite)

	coll := &collector{}
	push, err := client.NewDefaultMQPushConsumer(group)
	if err != nil {
		fatal("push consumer: %v", err)
	}
	push.SetNameServerAddresses([]string{*ns})
	push.SetInstanceName("STATIC_TOPIC_LIVE_PUSH")
	if err := push.SetMessageListener(coll); err != nil {
		fatal("set listener: %v", err)
	}
	if err := push.Subscribe(*topic, "*"); err != nil {
		fatal("push subscribe: %v", err)
	}
	if err := push.Start(); err != nil {
		fatal("push start: %v", err)
	}
	defer push.Shutdown()

	// 必须先起消费者再发：新组 CONSUME_FROM_LAST_OFFSET 会跳过启动前的消息。
	time.Sleep(4 * time.Second)

	prod := client.MustNewDefaultMQProducer("StaticProd_" + fmt.Sprint(stamp))
	prod.SetNameServerAddresses([]string{*ns})
	prod.SetInstanceName("STATIC_TOPIC_LIVE_PROD")
	if err := prod.Start(); err != nil {
		fatal("producer start: %v", err)
	}
	sent := 0
	lastSendErr := ""
	for i := 0; i < physicalQueues; i++ {
		if _, err := prod.Send(common.NewMessage(*topic,
			[]byte(fmt.Sprintf("static-%d", i)))); err != nil {
			lastSendErr = err.Error()
		} else {
			sent++
		}
	}
	prod.Shutdown()
	check("S5 发送 4 条", sent == physicalQueues,
		fmt.Sprintf("sent=%d lastErr=%s", sent, lastSendErr))

	deadline = time.Now().Add(40 * time.Second)
	for len(coll.bodies) < physicalQueues && time.Now().Before(deadline) {
		time.Sleep(1 * time.Second)
	}
	check("S5 消费收齐 4 条", len(coll.bodies) >= physicalQueues,
		fmt.Sprintf("got=%d", len(coll.bodies)))

	fmt.Printf("PASS=%d FAIL=%d\n", passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
}
