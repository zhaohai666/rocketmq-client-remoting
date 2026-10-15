// live_admin is the Go DefaultMQAdminExt smoke test against a real RocketMQ 5.x
// cluster: cluster/topic-route discovery, topic CRUD, broker config, KV config,
// subscription-group CRUD, message send + topic stats, the four offset queries a
// consumer actually uses, the KEYS index query and viewMessage, and the broker's
// stored consumer offset.
//
//	go run ./examples/live_admin -ns 127.0.0.1:9876
//
// This program covers the Go-side admin library (client/admin*.go): previously
// only 35 unit tests, with no live tool — a mis-encoded request body would
// still pass against a mock broker, so this layer must be exercised against a
// real cluster.
//
// Every check prints PASS/FAIL; any failure makes the process exit non-zero,
// and the closing line is `PASS=<n> FAIL=<n> TOPIC=<topic>`.
//
// The tool creates its own topic + subscription group + KV namespace (with a
// timestamped name) and deletes them afterwards, so it can be re-run freely;
// if it panics halfway, the leftover topics are just garbage and do not affect
// the next run.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/client"
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
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
	fmt.Fprintf(os.Stderr, "admin live: "+format+"\n", args...)
	os.Exit(2)
}

func main() {
	ns := flag.String("ns", "127.0.0.1:9876", "nameserver address")
	stamp := time.Now().Unix()
	topic := flag.String("topic", fmt.Sprintf("GoAdmin_%d", stamp), "topic to create/delete")
	group := flag.String("group", fmt.Sprintf("GID_GoAdmin_%d", stamp), "subscription group to create/delete")
	kvns := flag.String("kv-ns", fmt.Sprintf("GoAdminKV_%d", stamp), "kv namespace to create/delete")
	flag.Parse()

	const seed = 8 // messages sent; also the floor for topicStats maxOffset

	admin := client.NewDefaultMQAdminExt(nil)
	admin.SetNamesrvAddr(*ns)
	admin.SetInstanceName("ADMIN_LIVE")
	if err := admin.Start(); err != nil {
		fatal("admin start: %v", err)
	}
	defer admin.Shutdown()

	// ---- 1. cluster liveness + a master address to talk to ----------------------
	ci, err := admin.FetchBrokerClusterInfo()
	if err != nil {
		check("cluster liveness fetchBrokerClusterInfo", false, err.Error())
		fatal("no cluster info: %v", err)
	}
	masterAddr, clusterName := pickMaster(ci)
	if masterAddr == "" {
		check("cluster liveness fetchBrokerClusterInfo", false, "no broker registered a master address")
		fatal("no master broker registered")
	}
	check("cluster liveness fetchBrokerClusterInfo", len(ci.ClusterAddrTable) > 0,
		fmt.Sprintf("clusters=%d brokers=%d cluster=%s master=%s",
			len(ci.ClusterAddrTable), len(ci.BrokerAddrTable), clusterName, masterAddr))

	// ---- 2. topic CRUD + route -------------------------------------------------
	if err := admin.CreateTopic("", *topic, 4, 0); err != nil {
		check("CreateTopic(queueNum=4)", false, err.Error())
	} else {
		check("CreateTopic(queueNum=4)", true, "")
	}

	route, err := admin.ExamineTopicRoute(*topic)
	if err != nil || route == nil {
		check("route readQueueNums==4", false, fmt.Sprintf("route: %v", err))
	} else {
		qnums := make([]int32, 0, len(route.QueueDatas))
		all4 := true
		for _, qd := range route.QueueDatas {
			qnums = append(qnums, qd.ReadQueueNums)
			if qd.ReadQueueNums != 4 {
				all4 = false
			}
		}
		check("route readQueueNums==4", len(qnums) > 0 && all4,
			fmt.Sprintf("readQueueNums=%v brokers=%d", qnums, len(route.BrokerDatas)))
	}

	if tl, err := admin.FetchAllTopicList(); err != nil {
		check("new topic appears in FetchAllTopicList", false, err.Error())
	} else {
		check("new topic appears in FetchAllTopicList", contains(tl.TopicList, *topic), *topic)
	}

	if tcfg, err := admin.ExamineTopicConfig(masterAddr, *topic); err != nil || tcfg == nil {
		check("TopicConfig queue count matches the created one", false, fmt.Sprintf("examine: %v", err))
	} else {
		check("TopicConfig queue count matches the created one",
			tcfg.ReadQueueNums == 4 && tcfg.WriteQueueNums == 4,
			fmt.Sprintf("read=%d write=%d perm=%d", tcfg.ReadQueueNums, tcfg.WriteQueueNums, tcfg.Perm))
	}

	if cl, err := admin.GetTopicClusterList(*topic); err != nil {
		check("GetTopicClusterList contains this cluster", false, err.Error())
	} else {
		check("GetTopicClusterList contains this cluster", len(cl) > 0 && cl[clusterName],
			fmt.Sprintf("clusters=%v want=%s", keysOf(cl), clusterName))
	}

	// ---- 3. broker config / runtime stats --------------------------------------
	if cfg, err := admin.GetBrokerConfig(masterAddr, 5000); err != nil {
		check("getBrokerConfig parses non-empty k=v pairs", false, err.Error())
	} else {
		check("getBrokerConfig parses non-empty k=v pairs", len(cfg) > 0 && cfg["brokerName"] != "",
			fmt.Sprintf("%d keys brokerName=%q", len(cfg), cfg["brokerName"]))
	}

	if rt, err := admin.FetchBrokerRuntimeStats(masterAddr, 5000); err != nil {
		check("FetchBrokerRuntimeStats non-empty", false, err.Error())
	} else {
		// The broker answers a Properties text body under key "kvTable"; the
		// parse is the thing under test, not any particular runtime figure.
		check("FetchBrokerRuntimeStats non-empty", rt != nil && len(rt.Table) > 0,
			fmt.Sprintf("%d entries", len(rt.Table)))
	}

	// ---- 4. KV config round-trip -----------------------------------------------
	if err := admin.CreateAndUpdateKVConfig(*kvns, "k1", "v1"); err != nil {
		check("KV value round-trip consistent", false, err.Error())
	} else if v, ok, err := admin.GetKVConfig(*kvns, "k1"); err != nil {
		check("KV value round-trip consistent", false, err.Error())
	} else {
		check("KV value round-trip consistent", ok && v == "v1", fmt.Sprintf("ok=%v v=%q", ok, v))
	}
	if kvt, err := admin.GetKVListByNamespace(*kvns); err != nil {
		check("GetKVListByNamespace contains k1", false, err.Error())
	} else {
		check("GetKVListByNamespace contains k1", kvt != nil && kvt.Table["k1"] == "v1", fmt.Sprintf("%v", kvt.Table))
	}
	if err := admin.DeleteKVConfig(*kvns, "k1"); err != nil {
		check("KV entry no longer exists after delete", false, err.Error())
	} else if _, ok, _ := admin.GetKVConfig(*kvns, "k1"); ok {
		check("KV entry no longer exists after delete", false, "still readable after deletion")
	} else {
		check("KV entry no longer exists after delete", true, "")
	}

	// ---- 5. subscription group CRUD --------------------------------------------
	sgc := &remoting.SubscriptionGroupConfig{
		GroupName:                      *group,
		ConsumeEnable:                  true,
		ConsumeBroadcastEnable:         false,
		ConsumeMessageOrderly:          false,
		RetryQueueNums:                 1,
		RetryMaxTimes:                  5,
		BrokerID:                       0,
		WhichBrokerWhenConsumeSlowly:   1,
		NotifyConsumerIdsChangedEnable: true,
		ConsumeTimeoutMinute:           15,
	}
	if err := admin.CreateAndUpdateSubscriptionGroupConfig(masterAddr, sgc); err != nil {
		check("subscription group retryMaxTimes round-trip consistent", false, err.Error())
	} else if got, err := admin.GetSubscriptionGroupConfig(masterAddr, *group); err != nil || got == nil {
		check("subscription group retryMaxTimes round-trip consistent", false, fmt.Sprintf("read back: %v", err))
	} else {
		check("subscription group retryMaxTimes round-trip consistent", got.RetryMaxTimes == 5,
			fmt.Sprintf("retryMaxTimes=%d consumeEnable=%v", got.RetryMaxTimes, got.ConsumeEnable))
	}
	if all, err := admin.GetAllSubscriptionGroup(masterAddr, 5000); err != nil {
		check("paged result contains the new subscription group", false, err.Error())
	} else {
		_, hit := all.SubscriptionGroupTable[*group]
		check("paged result contains the new subscription group", hit, fmt.Sprintf("%d groups", len(all.SubscriptionGroupTable)))
	}

	// ---- 6. produce, then read the topic stats back -----------------------------
	producer, err := client.NewDefaultMQProducer(fmt.Sprintf("PID_GoAdmin_%d", stamp))
	if err != nil {
		fatal("producer: %v", err)
	}
	producer.SetNameServerAddr(*ns)
	if err := producer.Start(); err != nil {
		fatal("producer start: %v", err)
	}
	defer producer.Shutdown()

	// One shared key for every message keeps the KEYS-index query deterministic
	// (the index is per-topic, so a unique key per message would still work, but
	// "found >= 1" would then be a much weaker statement).
	key := fmt.Sprintf("adminkey-%d", stamp)
	sent := 0
	for i := 0; i < seed; i++ {
		m := common.NewMessage(*topic, []byte(fmt.Sprintf("admin-msg-%d", i)))
		m.SetKeys(key)
		if _, err := producer.Send(m); err != nil {
			continue
		}
		sent++
	}
	check(fmt.Sprintf("sync send %d messages", seed), sent == seed, fmt.Sprintf("ok=%d/%d", sent, seed))

	// Publish queues come from the real route, so the offset queries below run
	// against a broker/queue pair that actually holds these messages.
	queues, err := producer.FetchPublishMessageQueues(*topic)
	if err != nil || len(queues) == 0 {
		check("FetchPublishMessageQueues non-empty", false, fmt.Sprintf("%v", err))
		fatal("no publish queues")
	}
	check("FetchPublishMessageQueues non-empty", len(queues) == 4, fmt.Sprintf("%d queues", len(queues)))

	// The broker's consume queue is reput ASYNCHRONOUSLY, so an offset read taken
	// immediately after a send is a snapshot that can be behind (a known broker
	// behaviour, not a client bug: "an offset read right after the send may
	// return 0"). Poll until the total settles instead of asserting on the first
	// snapshot.
	qMaxSum := waitForMaxOffsetSum(admin, queues, int64(sent), 10*time.Second)
	check(fmt.Sprintf("four queues' MaxOffset sum == messages sent (%d)", sent), qMaxSum == int64(sent),
		fmt.Sprintf("maxOffsetSum=%d sent=%d", qMaxSum, sent))

	statsSum, statsQueues := waitForStatsMaxOffset(*topic, admin, int64(sent), 5*time.Second)
	check(fmt.Sprintf("TopicStatsTable maxOffset sum == messages sent (%d)", sent), statsSum == int64(sent),
		fmt.Sprintf("maxOffsetSum=%d sent=%d queues=%d", statsSum, sent, statsQueues))

	// ---- 7. offset queries on a queue that actually has messages ---------------
	// Every queue holds messages by now (round-robin over 4 queues), so pick by
	// (brokerName, queueId) order for determinism rather than by "first non-zero".
	q := pickQueue(queues)
	check("picked a non-empty queue", q != nil, fmt.Sprintf("%v", q))
	if q != nil {
		maxOff, errMax := admin.MaxOffset(*q)
		minOff, errMin := admin.MinOffset(*q)
		check("MaxOffset >= MinOffset and the queue is non-empty",
			errMax == nil && errMin == nil && maxOff >= minOff && maxOff >= 1,
			fmt.Sprintf("min=%d max=%d errMax=%v errMin=%v", minOff, maxOff, errMax, errMin))

		// UnixMilli granularity is 1ms and the messages were stored on this very
		// machine, so a `now` taken immediately after the send can EQUAL a
		// storeTimestamp — and then LOWER legitimately returns that message's own
		// offset instead of maxOffset. Sleep past the store window so the boundary
		// assertions are deterministic.
		time.Sleep(250 * time.Millisecond)
		now := time.Now().UnixMilli()
		lo, errLo := admin.SearchLowerBoundaryOffset(*q, now)
		up, errUp := admin.SearchUpperBoundaryOffset(*q, now)
		check("LOWER boundary = maxOffset (next offset after the queue tail)",
			errLo == nil && lo == maxOff, fmt.Sprintf("lo=%d max=%d err=%v", lo, maxOff, errLo))
		check("UPPER boundary = maxOffset-1 (the last message's own offset)",
			errUp == nil && up == maxOff-1, fmt.Sprintf("up=%d max=%d err=%v", up, maxOff, errUp))
		check("the two boundaries really differ (proves boundaryType takes effect)", lo != up, fmt.Sprintf("lo=%d up=%d", lo, up))

		// A timestamp older than every message collapses both boundaries to the
		// queue head, which is the other half of the boundary contract.
		earlyLo, errELo := admin.SearchLowerBoundaryOffset(*q, 0)
		earlyUp, errEUp := admin.SearchUpperBoundaryOffset(*q, 0)
		check("LOWER/UPPER both collapse to minOffset when the timestamp predates every message",
			errELo == nil && errEUp == nil && earlyLo == minOff && earlyUp == minOff,
			fmt.Sprintf("lo=%d up=%d min=%d", earlyLo, earlyUp, minOff))

		if est, err := admin.EarliestMsgStoreTime(*q); err != nil {
			check("EarliestMsgStoreTime non-zero", false, err.Error())
		} else {
			check("EarliestMsgStoreTime non-zero", est > 0, fmt.Sprintf("%d", est))
		}

		// Broker-stored consumer offset: write then read back through a SEPARATE
		// RPC, so the assertion proves the broker kept it (right group/topic/queue)
		// rather than that a local table echoed our own value.
		if err := admin.UpdateConsumerOffsetToBroker(masterAddr, *group, *q, 3); err != nil {
			check("offset written to the broker reads back", false, fmt.Sprintf("write: %v", err))
		} else if off, found, err := admin.ExamineConsumerOffset(*group, *q); err != nil {
			check("offset written to the broker reads back", false, fmt.Sprintf("read: %v", err))
		} else {
			check("offset written to the broker reads back", found && off == 3,
				fmt.Sprintf("found=%v offset=%d", found, off))
		}
	}

	time.Sleep(1500 * time.Millisecond)
	msgs, err := admin.QueryMessageByKey(*topic, key, 32)
	if err != nil {
		check("QueryMessageByKey hit", false, err.Error())
	} else if len(msgs) == 0 {
		check("QueryMessageByKey hit", false, "index has no messages")
	} else {
		check("QueryMessageByKey hit", true, fmt.Sprintf("%d messages", len(msgs)))
		if vm, err := admin.ViewMessage(*topic, vmLookupID(msgs[0])); err != nil {
			check("viewMessage body matches the index", false, err.Error())
		} else {
			check("viewMessage body matches the index",
				vm != nil && bytes.Equal(vm.Body, msgs[0].Body),
				fmt.Sprintf("want=%q got=%q", msgs[0].Body, vm.Body))
		}
	}

	// ---- 9. teardown: delete topic + group, then prove the topic is gone -------
	if err := admin.DeleteTopic(*topic, clusterName); err != nil {
		check("topic is gone after deleteTopic", false, fmt.Sprintf("delete: %v", err))
	} else if tl, err := admin.FetchAllTopicList(); err != nil {
		check("topic is gone after deleteTopic", false, fmt.Sprintf("relist: %v", err))
	} else {
		check("topic is gone after deleteTopic", !contains(tl.TopicList, *topic), *topic)
	}
	if err := admin.DeleteSubscriptionGroup(masterAddr, *group, true); err != nil {
		check("DeleteSubscriptionGroup", false, err.Error())
	} else {
		check("DeleteSubscriptionGroup", true, "")
	}

	fmt.Printf("\nPASS=%d FAIL=%d TOPIC=%s\n", passCount, failCount, *topic)
	if failCount > 0 {
		os.Exit(1)
	}
}

// pickMaster returns a master broker address (brokerAddrs key 0) and its cluster.
func pickMaster(ci *remoting.ClusterInfo) (addr, cluster string) {
	for _, entry := range ci.BrokerAddrTable {
		if entry == nil {
			continue
		}
		if a, ok := entry.BrokerAddrs[0]; ok && a != "" {
			return a, entry.Cluster
		}
	}
	return "", ""
}

// pickQueue returns a deterministic queue to run the offset queries against.
// Every queue holds messages once the round-robin sends have settled, so the
// choice only has to be stable (not "the first non-empty one", which would make
// the printed detail depend on the broker's reput order).
func pickQueue(queues []common.MessageQueue) *common.MessageQueue {
	best := -1
	for i := range queues {
		if best < 0 ||
			queues[i].BrokerName < queues[best].BrokerName ||
			(queues[i].BrokerName == queues[best].BrokerName && queues[i].QueueID < queues[best].QueueID) {
			best = i
		}
	}
	if best < 0 {
		return nil
	}
	q := queues[best]
	return &q
}

// waitForMaxOffsetSum polls GET_MAX_OFFSET over every publish queue until the
// total reaches `want` (or the deadline passes) and returns the last total. The
// broker reputs the consume queue asynchronously, so a single read right after a
// send can legitimately be short.
func waitForMaxOffsetSum(admin *client.DefaultMQAdminExt, queues []common.MessageQueue, want int64, timeout time.Duration) int64 {
	deadline := time.Now().Add(timeout)
	var sum int64
	for {
		sum = 0
		for _, q := range queues {
			if off, err := admin.MaxOffset(q); err == nil {
				sum += off
			}
		}
		if sum >= want || time.Now().After(deadline) {
			return sum
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitForStatsMaxOffset is waitForMaxOffsetSum for the TOPIC_STATS(202) path,
// which aggregates the same figures on the broker side.
func waitForStatsMaxOffset(topic string, admin *client.DefaultMQAdminExt, want int64, timeout time.Duration) (sum int64, queues int) {
	deadline := time.Now().Add(timeout)
	for {
		if stats, err := admin.ExamineTopicStats(topic); err == nil {
			sum, queues = 0, len(stats.OffsetTable)
			for _, o := range stats.OffsetTable {
				sum += o.MaxOffset
			}
			if sum >= want || time.Now().After(deadline) {
				return sum, queues
			}
		}
		if time.Now().After(deadline) {
			return sum, queues
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// vmLookupID picks the id viewMessage needs: the store (offset) message id, not
// the UNIQ_KEY the send returned. The KEYS-index query normally carries both.
func vmLookupID(m *common.MessageExt) string {
	if m.OffsetMsgID != "" {
		return m.OffsetMsgID
	}
	return m.MsgID
}
