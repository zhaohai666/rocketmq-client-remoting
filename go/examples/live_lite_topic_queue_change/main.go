// live_lite_topic_queue_change is the Go lite-pull-consumer topic-queue-change
// listener smoke test against a real RocketMQ 5.x cluster.
//
//	go run ./examples/live_lite_topic_queue_change -ns 127.0.0.1:9876
//
// Why it needs a real cluster: the check round is supposed to re-ask the name
// server for the topic's queues on every pass, while the ordinary route cache
// is only refreshed every 30s. A fake name server can prove the comparison
// logic; only a live one proves the freshness. So the metadata check interval
// is pushed to its 1s floor while the route poll interval stays at its 30s
// default, and the topic is really scaled. Once the first scheduled pass is
// over (10s initial delay), the callback must land within a couple of check
// rounds of the moment the name server starts reporting the new queue count —
// reading the cache instead would push that gap past half a minute.
//
//	L1 quiet      queues unchanged ⇒ the listener is not disturbed
//	L1b first pass the scheduled round has really run and stayed quiet
//	L2  freshness scale 2 -> 4, callback within a few 1s rounds
//	L3  converged the snapshot advances, so one change fires one callback
//	L4  scale-in  4 -> 2 is reported the same way
//	L5  unknown   a topic with no queues is "not found", never "zero queues"
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/client"
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

var (
	passCount int
	failCount int
	mu        sync.Mutex
)

func check(name string, ok bool, detail string) {
	mu.Lock()
	defer mu.Unlock()
	if ok {
		passCount++
		fmt.Printf("PASS  %s%s\n", name, detailSuffix(detail))
		return
	}
	failCount++
	fmt.Printf("FAIL  %s%s\n", name, detailSuffix(detail))
}

func detailSuffix(detail string) string {
	if detail == "" {
		return ""
	}
	return "  " + detail
}

// recorder only appends callbacks; the check round calls it from a background
// goroutine, so it needs its own lock.
type recorder struct {
	mu     sync.Mutex
	events [][]int
	topics []string
}

func (r *recorder) OnChanged(topic string, messageQueues []common.MessageQueue) {
	ids := make([]int, 0, len(messageQueues))
	for _, mq := range messageQueues {
		ids = append(ids, int(mq.QueueID))
	}
	sort.Ints(ids)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ids)
	r.topics = append(r.topics, topic)
}

func (r *recorder) snapshot() [][]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]int, len(r.events))
	copy(out, r.events)
	return out
}

func idsOf(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// matches reports whether the recorded events equal the wanted sequence.
func matches(events [][]int, want ...[]int) bool {
	if len(events) != len(want) {
		return false
	}
	for i := range want {
		if len(events[i]) != len(want[i]) {
			return false
		}
		for j := range want[i] {
			if events[i][j] != want[i][j] {
				return false
			}
		}
	}
	return true
}

const (
	baseQueueNum   = 2
	scaledQueueNum = 4
	// checkIntervalMs is the floor of the metadata check period: it makes
	// "re-query every round" and "read the 30s cache" far apart in time.
	checkIntervalMs = int64(1000)
	// firstDelayS is the initial delay of the scheduled check round.
	firstDelayS        = 10.0
	freshnessWindowSec = 5.0
)

func main() {
	nsAddr := flag.String("ns", "127.0.0.1:9876", "name server address")
	flag.Parse()

	stamp := time.Now().UnixNano() / int64(time.Millisecond)
	topic := fmt.Sprintf("LiteQcLive_%d", stamp)
	group := fmt.Sprintf("LiteQcG_%d", stamp)
	ghost := fmt.Sprintf("LiteQcGhost_%d", stamp)
	fmt.Printf("nameserver = %s, topic = %s\n", *nsAddr, topic)

	// ---- create the topic with 2 read/write queues
	admin := client.NewDefaultMQAdminExt(nil)
	admin.SetNameServerAddresses([]string{*nsAddr})
	if err := admin.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "admin start: %v\n", err)
		os.Exit(1)
	}
	defer admin.Shutdown()
	if err := admin.CreateTopic(common.DefaultTopic, topic, baseQueueNum, 0); err != nil {
		check("建 Topic（2 队列）", false, err.Error())
		os.Exit(1)
	}
	check("建 Topic（2 队列）", true, "")

	c := client.MustNewDefaultLitePullConsumer(group)
	c.SetNameServerAddresses([]string{*nsAddr})
	c.SetInstanceName("lite-qc-live")
	c.SetTopicMetadataCheckIntervalMillis(checkIntervalMs)
	if c.TopicMetadataCheckIntervalMillis() != checkIntervalMs {
		check("检查周期压到 1s 下限", false,
			fmt.Sprintf("actual=%d", c.TopicMetadataCheckIntervalMillis()))
		os.Exit(1)
	}
	check("检查周期压到 1s 下限", true, "")

	if err := c.Subscribe(topic, "*"); err != nil {
		check("Subscribe", false, err.Error())
		os.Exit(1)
	}
	if err := c.Start(); err != nil {
		check("消费者启动", false, err.Error())
		os.Exit(1)
	}
	defer c.Shutdown()
	loopStart := time.Now()

	ok, waited := waitQueueNum(c, topic, baseQueueNum, 30*time.Second)
	check("L0 路由可见（2 个队列）", ok, fmt.Sprintf("after=%s", waited))
	if !ok {
		fmt.Printf("\nPASS=%d FAIL=%d\n", passCount, failCount)
		os.Exit(1)
	}

	rec := &recorder{}
	// Registering while RUNNING snapshots the current set, so the first round
	// must stay quiet.
	if err := c.RegisterTopicMessageQueueChangeListener(topic, rec); err != nil {
		check("注册队列变更监听", false, err.Error())
		os.Exit(1)
	}
	time.Sleep(3 * time.Second)
	check("L1 队列没动 ⇒ 静默", len(rec.snapshot()) == 0,
		fmt.Sprintf("events=%v", rec.snapshot()))

	// Wait past the 10s initial delay so the following measurements only see
	// the steady 1s cadence.
	if wait := time.Duration(firstDelayS*1.5)*time.Second - time.Since(loopStart); wait > 0 {
		time.Sleep(wait)
	}
	check("L1b 首查那趟真的跑过 ⇒ 依旧静默", len(rec.snapshot()) == 0,
		fmt.Sprintf("events=%v", rec.snapshot()))

	// ---- L2 scale out 2 -> 4
	if err := admin.CreateTopic(common.DefaultTopic, topic, scaledQueueNum, 0); err != nil {
		check("扩容到 4 队列", false, err.Error())
	}
	nsSeen, tNs := waitQueueNum(c, topic, scaledQueueNum, 45*time.Second)
	check("L2a nameserver 报出 4 个队列", nsSeen, fmt.Sprintf("after=%s", tNs))
	if nsSeen {
		gotCB, tCB := waitEvents(rec, func(e [][]int) bool {
			return matches(e, idsOf(scaledQueueNum))
		}, freshnessWindowSec*time.Second)
		check("L2b 比对趟次现查路由（回调紧跟 nameserver，不等 30s 缓存）",
			gotCB, fmt.Sprintf("callback waited=%s", tCB))
	}

	// ---- L3 the snapshot advanced: one change, one callback
	time.Sleep(3 * time.Second)
	check("L3 回调后快照推进 ⇒ 不重复回调", len(rec.snapshot()) == 1,
		fmt.Sprintf("events=%v", rec.snapshot()))

	// ---- L4 scale back in 4 -> 2
	if err := admin.CreateTopic(common.DefaultTopic, topic, baseQueueNum, 0); err != nil {
		check("缩容回 2 队列", false, err.Error())
	}
	nsSeen, tNs = waitQueueNum(c, topic, baseQueueNum, 45*time.Second)
	check("L4a nameserver 报回 2 个队列", nsSeen, fmt.Sprintf("after=%s", tNs))
	if nsSeen {
		gotCB, tCB := waitEvents(rec, func(e [][]int) bool {
			return matches(e, idsOf(scaledQueueNum), idsOf(baseQueueNum))
		}, freshnessWindowSec*time.Second)
		check("L4b 缩容同样靠现查路由看到", gotCB,
			fmt.Sprintf("callback waited=%s", tCB))
	}

	// ---- L5 an unroutable topic is "not found", not "zero queues"
	notFound := ""
	if queues, err := c.FetchMessageQueues(ghost); err != nil {
		notFound = err.Error()
	} else {
		notFound = fmt.Sprintf("no error, queues=%d", len(queues))
	}
	check("L5 未知 topic 取队列报「查不到」而不是返回空",
		strings.Contains(notFound, "Namesrv return empty"), notFound)

	fmt.Printf("\nPASS=%d FAIL=%d\n", passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
}

// waitQueueNum polls FetchMessageQueues (which re-queries the name server every
// call) until the topic reports want queues.
func waitQueueNum(c *client.DefaultLitePullConsumer, topic string, want int,
	timeout time.Duration) (bool, time.Duration) {
	start := time.Now()
	deadline := start.Add(timeout)
	for time.Now().Before(deadline) {
		queues, err := c.FetchMessageQueues(topic)
		if err == nil && len(queues) == want {
			return true, time.Since(start)
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false, time.Since(start)
}

// waitEvents polls the recorder until cond is satisfied and reports how long
// that took.
func waitEvents(rec *recorder, cond func([][]int) bool, timeout time.Duration) (bool, time.Duration) {
	start := time.Now()
	deadline := start.Add(timeout)
	for time.Now().Before(deadline) {
		if cond(rec.snapshot()) {
			return true, time.Since(start)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false, time.Since(start)
}
