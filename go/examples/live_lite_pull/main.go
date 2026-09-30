// live_lite_pull is the Go lite-pull-consumer (DefaultLitePullConsumer) smoke
// test against a real RocketMQ 5.x cluster: assign mode end to end, the two
// cursors, the commit round trip, restart continuity through the broker-side
// offset, Seek replay, subscribe mode (rebalance + auto-commit) and shutdown
// persistence.
//
//	go run ./examples/live_lite_pull -ns 127.0.0.1:9876
//
// Every check prints PASS/FAIL and the process exits non-zero if any failed.
// A fresh topic and fresh groups per run (defaults embed a timestamp) keep
// leftover state from an earlier run from making a check pass.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/client"
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

var (
	passCount int
	failCount int
	total     int
	mu        sync.Mutex
)

func check(name string, ok bool, detail string) {
	mu.Lock()
	defer mu.Unlock()
	if ok {
		passCount++
		fmt.Printf("PASS  %s%s\n", name, detail)
		return
	}
	failCount++
	fmt.Printf("FAIL  %s%s\n", name, detail)
}

func bump(n int) {
	mu.Lock()
	total += n
	mu.Unlock()
}

func totalCount() int {
	mu.Lock()
	defer mu.Unlock()
	return total
}

// recordingQueues collects the rebalance notifications.
type recordingQueues struct {
	mu    sync.Mutex
	calls int
	mqAll int
	got   int
}

func (l *recordingQueues) MessageQueueChanged(topic string, mqAll, divided []common.MessageQueue) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	l.mqAll = len(mqAll)
	l.got = len(divided)
}

func (l *recordingQueues) snapshot() (int, int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls, l.mqAll, l.got
}

// pollBodies drains the consumer until want bodies arrived or the deadline
// passed; it returns what it got (the caller decides pass/fail).
func pollBodies(c *client.DefaultLitePullConsumer, want int, within time.Duration) []string {
	deadline := time.Now().Add(within)
	var bodies []string
	for len(bodies) < want && time.Now().Before(deadline) {
		for _, m := range c.PollWithTimeout(300) {
			bodies = append(bodies, string(m.Body))
		}
	}
	return bodies
}

func bodiesOf(msgs []*common.MessageExt) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, string(m.Body))
	}
	return out
}

func main() {
	nsAddr := flag.String("ns", "127.0.0.1:9876", "nameserver address")
	topic := flag.String("topic", "", "topic name (default: GoLiteLive_<unix>)")
	flag.Parse()

	stamp := time.Now().Unix()
	topicName := *topic
	if topicName == "" {
		topicName = fmt.Sprintf("GoLiteLive_%d", stamp)
	}
	assignGroup := fmt.Sprintf("GID_GoLiteAssign_%d", stamp)
	subGroup := fmt.Sprintf("GID_GoLiteSub_%d", stamp)
	sendGroup := fmt.Sprintf("GID_GoLiteSend_%d", stamp)
	if !strings.Contains(*nsAddr, ":") {
		fmt.Fprintln(os.Stderr, "nameserver address must be host:port")
		os.Exit(2)
	}
	fmt.Printf("nameserver=%s topic=%s assignGroup=%s subGroup=%s\n\n",
		*nsAddr, topicName, assignGroup, subGroup)

	// ---- 0. producer + a classic pull consumer used ONLY to create the topic ----
	p, err := client.NewDefaultMQProducer(sendGroup)
	if err != nil {
		fmt.Fprintf(os.Stderr, "producer: %v\n", err)
		os.Exit(1)
	}
	p.SetNameServerAddresses([]string{*nsAddr})
	p.SetSendMsgTimeout(5000)
	if err := p.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "producer start: %v\n", err)
		os.Exit(1)
	}
	defer p.Shutdown()

	admin := client.MustNewDefaultMQPullConsumer(assignGroup + "_admin")
	admin.SetNameServerAddresses([]string{*nsAddr})
	if err := admin.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "admin pull consumer start: %v\n", err)
		os.Exit(1)
	}
	if err := admin.CreateTopic(common.DefaultTopic, topicName, 1, 0); err != nil {
		check("建 Topic", false, err.Error())
	} else {
		check("建 Topic 成功（1 个读写队列）", true, "")
	}
	var q0 common.MessageQueue
	deadline := time.Now().Add(15 * time.Second)
	for {
		queues, ferr := admin.FetchSubscribeMessageQueues(topicName)
		if ferr == nil && len(queues) == 1 {
			q0 = queues[0]
			break
		}
		if time.Now().After(deadline) {
			check("路由可见（1 个队列）", false, fmt.Sprintf("err=%v", ferr))
			fmt.Printf("\nPASS=%d FAIL=%d TOTAL=%d\n", passCount, failCount, totalCount())
			os.Exit(1)
		}
		time.Sleep(300 * time.Millisecond)
	}
	admin.Shutdown()
	check("路由可见（1 个队列）", q0.QueueID == 0, fmt.Sprintf("queue=%v", q0))

	send := func(body string) {
		m := common.NewMessage(topicName, []byte(body))
		m.SetTags("TagA")
		if _, err := p.Send(m); err != nil {
			check("消息写入 "+body, false, err.Error())
			return
		}
		bump(1)
		check("消息写入 "+body, true, "")
	}

	// ---- 1. assign mode, fresh group, FIRST_OFFSET: 5 seeds, delivered in order ----
	for i := 0; i < 5; i++ {
		send(fmt.Sprintf("lite-live-%d", i))
	}
	l1 := client.MustNewDefaultLitePullConsumer(assignGroup)
	l1.SetNameServerAddresses([]string{*nsAddr})
	l1.SetInstanceName("lite-assign-1")
	l1.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	l1.SetAutoCommit(false)
	l1.Assign([]common.MessageQueue{q0})
	l1.SetSubExpressionForAssign(topicName, "*")
	if err := l1.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "lite consumer start: %v\n", err)
		os.Exit(1)
	}
	check("assign 模式启动且 clientId 带 @STREAM",
		l1.IsStarted() && strings.Contains(l1.ClientID(), "@STREAM"),
		"clientId="+l1.ClientID())
	check("Assign 之后是 assign 模式", l1.IsAssignMode(), "")

	first := pollBodies(l1, 5, 10*time.Second)
	want := []string{"lite-live-0", "lite-live-1", "lite-live-2", "lite-live-3", "lite-live-4"}
	check("assign 模式按队列顺序交付 5 条", len(first) == 5 && strings.Join(first, ",") == strings.Join(want, ","),
		fmt.Sprintf("got=%v", first))
	check("拉取游标推进到 5（nextBeginOffset）", l1.PullCursorOf(q0) == 5,
		fmt.Sprintf("pull=%d", l1.PullCursorOf(q0)))
	check("消费游标推进到 5（只有 Poll 交付会推进）", l1.ConsumeCursorOf(q0) == 5,
		fmt.Sprintf("consume=%d", l1.ConsumeCursorOf(q0)))

	// ---- 2. the commit round trip: the broker really stored the consume cursor ----
	if err := l1.Commit(); err != nil {
		check("Commit 提交消费游标", false, err.Error())
	} else {
		check("Commit 提交消费游标", true, "")
	}
	if got, err := l1.Committed(q0); err != nil || got != 5 {
		check("本地读回已提交位点", false, fmt.Sprintf("got=%d err=%v", got, err))
	} else {
		check("本地读回已提交位点", true, "offset=5")
	}
	l1.Shutdown()
	check("assign 消费者关闭", !l1.IsStarted(), "")

	// ---- 3. restart continuity: a NEW consumer in the SAME group resumes at 5 -----
	l2 := client.MustNewDefaultLitePullConsumer(assignGroup)
	l2.SetNameServerAddresses([]string{*nsAddr})
	l2.SetInstanceName("lite-assign-2")
	l2.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	l2.SetAutoCommit(false)
	l2.Assign([]common.MessageQueue{q0})
	if err := l2.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "lite consumer 2 start: %v\n", err)
		os.Exit(1)
	}
	defer l2.Shutdown()
	if got, err := l2.Committed(q0); err != nil || got != 5 {
		check("新实例从 broker 读回位点 5", false, fmt.Sprintf("got=%d err=%v", got, err))
	} else {
		check("新实例从 broker 读回位点 5", true, "offset=5")
	}
	if got := l2.PullCursorOf(q0); got != 5 {
		check("新实例的拉取游标从已提交位点继续", false, fmt.Sprintf("pull=%d want=5", got))
	} else {
		check("新实例的拉取游标从已提交位点继续", true, "pull=5")
	}

	// ---- 4. no replay, no loss: 2 new messages deliver exactly, then Seek replays --
	time.Sleep(200 * time.Millisecond) // let the first (empty) pull round pass
	send("lite-live-5")
	send("lite-live-6")
	fresh := pollBodies(l2, 2, 10*time.Second)
	check("重启后不重放旧消息，只交付新增 2 条",
		len(fresh) == 2 && fresh[0] == "lite-live-5" && fresh[1] == "lite-live-6",
		fmt.Sprintf("got=%v", fresh))

	l2.Seek(q0, 3)
	replayed := pollBodies(l2, 4, 10*time.Second)
	check("Seek(3) 之后从位点 3 重放 4 条",
		len(replayed) == 4 && replayed[0] == "lite-live-3" && replayed[3] == "lite-live-6",
		fmt.Sprintf("got=%v", replayed))
	if err := l2.Commit(); err != nil {
		check("Seek 重放后的最终 Commit", false, err.Error())
	} else {
		check("Seek 重放后的最终 Commit", true, "offset=7")
	}

	// ---- 5. subscribe mode: rebalance assigns, FIRST_OFFSET reads from 0 ----------
	l3 := client.MustNewDefaultLitePullConsumer(subGroup)
	l3.SetNameServerAddresses([]string{*nsAddr})
	l3.SetInstanceName("lite-sub-1")
	l3.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	l3.SetAutoCommit(true)
	l3.SetAutoCommitIntervalMillis(1000)
	listener := &recordingQueues{}
	l3.SetMessageQueueListener(listener)
	if err := l3.Subscribe(topicName, "*"); err != nil {
		fmt.Fprintf(os.Stderr, "subscribe: %v\n", err)
		os.Exit(1)
	}
	if err := l3.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "lite consumer 3 start: %v\n", err)
		os.Exit(1)
	}
	defer l3.Shutdown()

	rebalanced := false
	rebalanceDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(rebalanceDeadline) {
		if got := len(l3.Assignment()); got == 1 {
			rebalanced = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	check("subscribe 模式重平衡分到 1 个队列", rebalanced,
		fmt.Sprintf("assignment=%d", len(l3.Assignment())))

	all := pollBodies(l3, 7, 15*time.Second)
	check("subscribe 模式从头消费全部 7 条", len(all) == 7, fmt.Sprintf("got=%d", len(all)))
	if calls, mqAll, got := listener.snapshot(); calls == 0 || mqAll != 1 || got != 1 {
		check("MessageQueueListener 收到重平衡通知", false,
			fmt.Sprintf("calls=%d mqAll=%d divided=%d", calls, mqAll, got))
	} else {
		check("MessageQueueListener 收到重平衡通知", true, "")
	}

	// ---- 6. auto-commit: the interval commit reaches the broker without a manual
	// Commit. A fourth consumer of the SAME group reads the broker's offset table.
	time.Sleep(1500 * time.Millisecond)
	l3.PollWithTimeout(300) // triggers maybeAutoCommit (deadline < now) → commit
	time.Sleep(500 * time.Millisecond)

	l4 := client.MustNewDefaultLitePullConsumer(subGroup)
	l4.SetNameServerAddresses([]string{*nsAddr})
	l4.SetInstanceName("lite-sub-2")
	l4.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	// assign mode keeps l4 out of the rebalance: it exists only to read the
	// broker's offset table for the group.
	l4.Assign([]common.MessageQueue{q0})
	if err := l4.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "lite consumer 4 start: %v\n", err)
		os.Exit(1)
	}
	if got, err := l4.Committed(q0); err != nil || got != 7 {
		check("自动提交的位点被 broker 记录", false, fmt.Sprintf("got=%d err=%v", got, err))
	} else {
		check("自动提交的位点被 broker 记录", true, "offset=7")
	}
	l4.Shutdown()

	l3.Shutdown()
	check("subscribe 消费者关闭（关机位点是幂等的）", !l3.IsStarted(), "")

	fmt.Printf("\nPASS=%d FAIL=%d TOTAL=%d\n", passCount, failCount, totalCount())
	if failCount > 0 {
		os.Exit(1)
	}
}
