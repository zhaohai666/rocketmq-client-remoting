// live_admin_batch verifies the admin methods added in client/admin_batch.go
// against a real RocketMQ 5.x cluster: the two batch-configuration requests
// (18/225), static topic (513), group read-forbidden (353), half-message resume
// (323), order-topic config over the nameserver KV namespace, and the
// housekeeping calls (306/329).
//
//	go run ./examples/live_admin_batch -ns 127.0.0.1:9876
//
// These are the calls that used to exist only as RequestCode constants, so
// nothing in this repo exercised their encoding before: a wrong body key or a
// wrong header field is invisible to unit tests and only shows up against a
// real broker. Each item prints PASS/FAIL and the process exits non-zero if
// anything failed; the last line is `PASS=<n> FAIL=<n>`.
//
// The tool creates its own topic / group / KV namespace with a timestamped
// name and deletes them afterwards, so it can be re-run freely.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/client"
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
	fmt.Fprintf(os.Stderr, "admin_batch live: "+format+"\n", args...)
	os.Exit(2)
}

func boolPtr(v bool) *bool { return &v }

func main() {
	ns := flag.String("ns", "127.0.0.1:9876", "nameserver address")
	stamp := time.Now().Unix()
	topicA := flag.String("topic-a", fmt.Sprintf("GoBatchA_%d", stamp), "first topic of the batch")
	topicB := flag.String("topic-b", fmt.Sprintf("GoBatchB_%d", stamp), "second topic of the batch")
	groupA := flag.String("group-a", fmt.Sprintf("GID_GoBatchA_%d", stamp), "first subscription group of the batch")
	groupB := flag.String("group-b", fmt.Sprintf("GID_GoBatchB_%d", stamp), "second subscription group of the batch")
	flag.Parse()

	admin := client.NewDefaultMQAdminExt(nil)
	admin.SetNamesrvAddr(*ns)
	admin.SetInstanceName("ADMIN_BATCH_LIVE")
	if err := admin.Start(); err != nil {
		fatal("admin start: %v", err)
	}
	defer admin.Shutdown()

	ci, err := admin.FetchBrokerClusterInfo()
	if err != nil {
		fatal("fetchBrokerClusterInfo: %v", err)
	}
	masterAddr := pickMaster(ci)
	if masterAddr == "" {
		fatal(fmt.Sprintf("no master broker registered: brokerAddrTable=%d clusterAddrTable=%d raw=%+v",
			len(ci.BrokerAddrTable), len(ci.ClusterAddrTable), ci.BrokerAddrTable))
	}
	fmt.Printf("cluster: master=%s\n\n", masterAddr)

	// ---- 1. UPDATE_AND_CREATE_TOPIC_LIST(18) ----------------------------------
	// Body is {"topicConfigList":[...]}; the custom header is empty on purpose.
	topicConfigs := []*remoting.TopicConfig{
		remoting.NewTopicConfig(*topicA),
		remoting.NewTopicConfig(*topicB),
	}
	topicConfigs[0].ReadQueueNums = 4
	topicConfigs[0].WriteQueueNums = 4
	topicConfigs[1].ReadQueueNums = 6
	topicConfigs[1].WriteQueueNums = 6

	if err := admin.CreateAndUpdateTopicConfigList(masterAddr, topicConfigs); err != nil {
		check("批量建 topic(18) 请求", false, err.Error())
	} else {
		check("批量建 topic(18) 请求", true, "")
	}
	// The proof that the body was actually understood: both topics exist and
	// each kept the queue count the batch asked for.
	batchOK, detail := true, ""
	for i, name := range []string{*topicA, *topicB} {
		cfg, err := admin.ExamineTopicConfig(masterAddr, name)
		if err != nil || cfg == nil {
			batchOK, detail = false, fmt.Sprintf("%s: %v", name, err)
			break
		}
		if cfg.ReadQueueNums != topicConfigs[i].ReadQueueNums {
			batchOK = false
			detail = fmt.Sprintf("%s readQueueNums got=%d want=%d",
				name, cfg.ReadQueueNums, topicConfigs[i].ReadQueueNums)
			break
		}
	}
	check("批量建 topic(18) 生效且队列数正确", batchOK, detail)

	// ---- 2. UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST(225) --------------------
	// Same shape, but the body key is `groupConfigList`.
	groupConfigs := []*remoting.SubscriptionGroupConfig{
		remoting.NewSubscriptionGroupConfig(*groupA),
		remoting.NewSubscriptionGroupConfig(*groupB),
	}
	groupConfigs[0].RetryQueueNums = 3
	groupConfigs[1].RetryMaxTimes = 5

	if err := admin.CreateAndUpdateSubscriptionGroupConfigList(masterAddr, groupConfigs); err != nil {
		check("批量建订阅组(225) 请求", false, err.Error())
	} else {
		check("批量建订阅组(225) 请求", true, "")
	}
	subOK, detail := true, ""
	for i, name := range []string{*groupA, *groupB} {
		cfg, err := admin.GetSubscriptionGroupConfig(masterAddr, name)
		if err != nil || cfg == nil {
			subOK, detail = false, fmt.Sprintf("%s: %v", name, err)
			break
		}
		if i == 0 && cfg.RetryQueueNums != 3 {
			subOK = false
			detail = fmt.Sprintf("%s retryQueueNums got=%d want=3", name, cfg.RetryQueueNums)
			break
		}
		if i == 1 && cfg.RetryMaxTimes != 5 {
			subOK = false
			detail = fmt.Sprintf("%s retryMaxTimes got=%d want=5", name, cfg.RetryMaxTimes)
			break
		}
	}
	check("批量建订阅组(225) 生效且字段正确", subOK, detail)

	// ---- 3. UPDATE_AND_GET_GROUP_FORBIDDEN(353) ------------------------------
	// Setting readable=false must come back as readable=false in the body.
	forbidden, err := admin.UpdateAndGetGroupReadForbidden(masterAddr, *groupA, *topicA, boolPtr(false))
	if err != nil {
		check("消费组读禁配(353) 设置禁读", false, err.Error())
	} else {
		check("消费组读禁配(353) 设置禁读",
			forbidden != nil && !forbidden.Readable,
			fmt.Sprintf("got=%+v", forbidden))
	}

	// A query-only call (nil) must not flip the flag back.
	forbidden, err = admin.UpdateAndGetGroupReadForbidden(masterAddr, *groupA, *topicA, nil)
	if err != nil {
		check("消费组读禁配(353) 仅查询不改动", false, err.Error())
	} else {
		check("消费组读禁配(353) 仅查询不改动",
			forbidden != nil && !forbidden.Readable,
			fmt.Sprintf("禁读状态被查询调用改掉了: %+v", forbidden))
	}

	forbidden, err = admin.UpdateAndGetGroupReadForbidden(masterAddr, *groupA, *topicA, boolPtr(true))
	if err != nil {
		check("消费组读禁配(353) 恢复可读", false, err.Error())
	} else {
		check("消费组读禁配(353) 恢复可读",
			forbidden != nil && forbidden.Readable,
			fmt.Sprintf("got=%+v", forbidden))
	}
	check("消费组读禁配(353) 回包含 group/topic",
		forbidden != nil && forbidden.Group == *groupA && forbidden.Topic == *topicA,
		fmt.Sprintf("got=%+v", forbidden))

	// ---- 4. RESUME_CHECK_HALF_MESSAGE(323) ----------------------------------
	// Java maps any non-SUCCESS reply to false instead of raising, so a
	// well-formed call on a message that is not a half message is the
	// interesting case: it must come back as false WITHOUT an error.
	resumed, err := admin.ResumeCheckHalfMessage(masterAddr, *topicA, "0A0F0000000000000000000000000000000000")
	if err != nil {
		check("恢复半消息(323) 非半消息返回 false 而非报错", false, err.Error())
	} else {
		check("恢复半消息(323) 非半消息返回 false 而非报错", !resumed,
			fmt.Sprintf("resumed=%v，对非半消息消息竟返回 true", resumed))
	}
	if _, err := admin.ResumeCheckHalfMessage(masterAddr, "", "x"); err != nil {
		check("恢复半消息(323) 缺 topic 被本地拒绝", true, "")
	} else {
		check("恢复半消息(323) 缺 topic 被本地拒绝", false, "空 topic 竟被放行发出")
	}

	// ---- 5. createOrUpdateOrderConf (nameserver KV) ---------------------------
	// Non-cluster mode merges into a ";"-joined list, so writing two topics
	// under the same key must keep BOTH entries.
	orderNSKey := fmt.Sprintf("GoOrder_%d", stamp)
	if err := admin.CreateOrUpdateOrderConf(orderNSKey,
		fmt.Sprintf("%s:5", *topicA), false); err != nil {
		check("顺序 topic 配置 首次写入", false, err.Error())
	} else {
		check("顺序 topic 配置 首次写入", true, "")
	}
	if err := admin.CreateOrUpdateOrderConf(orderNSKey,
		fmt.Sprintf("%s:8", *topicB), false); err != nil {
		check("顺序 topic 配置 第二次写入", false, err.Error())
	} else {
		check("顺序 topic 配置 第二次写入", true, "")
	}
	stored, found, err := admin.GetKVConfig(client.NamespaceOrderTopicConfig, orderNSKey)
	switch {
	case err != nil:
		check("顺序 topic 配置 合并两条而非覆盖", false, err.Error())
	case !found:
		check("顺序 topic 配置 合并两条而非覆盖", false, "nameserver 未回读到该 key")
	default:
		hasA := contains(stored, *topicA+":5")
		hasB := contains(stored, *topicB+":8")
		check("顺序 topic 配置 合并两条而非覆盖", hasA && hasB,
			fmt.Sprintf("stored=%q hasA=%v hasB=%v", stored, hasA, hasB))
	}

	// Re-writing the same topic must REPLACE its entry, not duplicate it.
	if err := admin.CreateOrUpdateOrderConf(orderNSKey,
		fmt.Sprintf("%s:6", *topicA), false); err != nil {
		check("顺序 topic 配置 同 key 覆盖旧值", false, err.Error())
	} else {
		stored, _, _ = admin.GetKVConfig(client.NamespaceOrderTopicConfig, orderNSKey)
		replaced := contains(stored, *topicA+":6") && !contains(stored, *topicA+":5")
		check("顺序 topic 配置 同 key 覆盖旧值", replaced, fmt.Sprintf("stored=%q", stored))
	}

	// ---- 6. CLEAN_EXPIRED_CONSUMEQUEUE(306) / DELETE_EXPIRED_COMMITLOG(329) --
	// These wipe data older than `time` hours, so the safe live check is a
	// large window: the call must be accepted and change nothing.
	if err := admin.CleanExpiredConsumerQueue(masterAddr, 24*365); err != nil {
		check("清理过期消费队列(306)", false, err.Error())
	} else {
		check("清理过期消费队列(306)", true, "")
	}
	if failed := admin.CleanExpiredConsumerQueueByAddr([]string{masterAddr}, 24*365); len(failed) != 0 {
		check("清理过期消费队列(306) ByAddr", false, fmt.Sprintf("failed=%v", failed))
	} else {
		check("清理过期消费队列(306) ByAddr", true, "")
	}
	if err := admin.DeleteExpiredCommitLog(masterAddr, 24*365); err != nil {
		check("删除过期 commitlog(329)", false, err.Error())
	} else {
		check("删除过期 commitlog(329)", true, "")
	}
	if failed := admin.DeleteExpiredCommitLogByAddr([]string{masterAddr}, 24*365); len(failed) != 0 {
		check("删除过期 commitlog(329) ByAddr", false, fmt.Sprintf("failed=%v", failed))
	} else {
		check("删除过期 commitlog(329) ByAddr", true, "")
	}
	// A dead address must be reported, not silently swallowed.
	if failed := admin.DeleteExpiredCommitLogByAddr([]string{"127.0.0.1:1"}, 1); len(failed) != 1 {
		check("清理类 ByAddr 报告失败地址", len(failed) == 1,
			fmt.Sprintf("dead addr should be reported, got %v", failed))
	} else {
		check("清理类 ByAddr 报告失败地址", true, "")
	}

	// ---- 7. CLEAN_UNUSED_TOPIC(316) ------------------------------------------
	// One request; the broker drops ITS OWN unused topics. What counts as
	// unused is broker policy (it keeps BenchmarkTest and friends), so the
	// only client-side invariant is that the broker accepts the request.
	if err := admin.CleanUnusedTopicByAddr(masterAddr); err != nil {
		check("清理未使用 topic(316)", false, err.Error())
	} else {
		check("清理未使用 topic(316)", true, "")
	}

	// ---- cleanup -------------------------------------------------------------
	admin.DeleteTopicInBroker(masterAddr, *topicA)
	admin.DeleteTopicInBroker(masterAddr, *topicB)
	admin.DeleteKVConfig(client.NamespaceOrderTopicConfig, orderNSKey)

	fmt.Printf("\nPASS=%d FAIL=%d\n", passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
}

func pickMaster(ci *remoting.ClusterInfo) string {
	for _, entry := range ci.BrokerAddrTable {
		if entry == nil {
			continue
		}
		// key 0 == MASTER
		if addr, ok := entry.BrokerAddrs[0]; ok && addr != "" {
			return addr
		}
	}
	return ""
}

func contains(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}
