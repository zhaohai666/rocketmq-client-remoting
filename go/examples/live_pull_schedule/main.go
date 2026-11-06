// live_pull_schedule is the Go PullConsumerScheduleService (scheduled PullTask
// loop) smoke test against a real RocketMQ 5.x cluster: the service expands one
// PullTask per assigned queue, the pull loop delivers every message body
// through the callback, and the broker-side committed offset for every queue
// advances past the seed batch.
//
//	go run ./examples/live_pull_schedule -ns 127.0.0.1:9876
//
// Every check prints PASS/FAIL and the process exits non-zero if any failed.
// Use a fresh topic per run (the default embeds a timestamp) so leftover state
// from an earlier run cannot make a check pass.
//
// The last line is `PASS=<n> FAIL=<n>`.
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
	mu        sync.Mutex
	passCount int
	failCount int
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

func main() {
	nsAddr := flag.String("ns", "127.0.0.1:9876", "nameserver address")
	topic := flag.String("topic", "", "topic name (default: GoPullScheduleLive_<unix>)")
	group := flag.String("group", "", "schedule service group (default: GID_GoPullScheduleLive_<unix>)")
	seedCount := flag.Int("seed", 6, "messages to seed the topic with")
	flag.Parse()

	stamp := time.Now().Unix()
	topicName := *topic
	if topicName == "" {
		topicName = fmt.Sprintf("GoPullScheduleLive_%d", stamp)
	}
	groupName := *group
	if groupName == "" {
		groupName = fmt.Sprintf("GID_GoPullScheduleLive_%d", stamp)
	}
	const queues = 2
	if !strings.Contains(*nsAddr, ":") {
		fmt.Fprintln(os.Stderr, "nameserver address must be host:port")
		os.Exit(2)
	}
	fmt.Printf("nameserver=%s topic=%s group=%s seed=%d\n\n", *nsAddr, topicName, groupName, *seedCount)

	// ---- 0. admin: cluster probe + topic lifecycle ----
	admin := client.NewDefaultMQAdminExt(nil)
	admin.SetNamesrvAddr(*nsAddr)
	if err := admin.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "admin start: %v\n", err)
		os.Exit(1)
	}
	clusterInfo, err := admin.FetchBrokerClusterInfo()
	if err != nil || len(clusterInfo.BrokerAddrs()) == 0 {
		check("S1 cluster probe", false, "no broker registered with the name server")
		admin.Shutdown()
		os.Exit(1)
	}
	brokerAddr := clusterInfo.BrokerAddrs()[0]
	check("S1 cluster probe", true, "broker="+brokerAddr)

	if err := admin.CreateTopic(common.DefaultTopic, topicName, queues, 0); err != nil {
		check("S1 create topic", false, err.Error())
		admin.Shutdown()
		os.Exit(1)
	}
	check("S1 create topic", true, fmt.Sprintf("%s %d queues", topicName, queues))

	// ---- 1. producer (seeding happens AFTER the service started: the default
	// start offset is committed-offset-then-queue-tail, so a pre-seeded topic
	// would resolve to the tail and pull nothing) ----
	producer, err := client.NewDefaultMQProducer("GID_GoPullScheduleSend_" + fmt.Sprint(stamp))
	if err != nil {
		fmt.Fprintf(os.Stderr, "producer: %v\n", err)
		os.Exit(1)
	}
	producer.SetNameServerAddr(*nsAddr)
	producer.SetSendMsgTimeout(5000)

	// ---- 2. schedule service pulls through the callback ----
	var bodyMu sync.Mutex
	received := make(map[string]int)
	seenQueues := make(map[common.MessageQueue]bool)

	svc, err := client.NewMQPullConsumerScheduleService(groupName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "schedule service: %v\n", err)
		os.Exit(1)
	}
	// The service's default inner pull consumer carries no nameserver address:
	// point it at the cluster before Start().
	svc.PullConsumer().SetNameServerAddresses([]string{*nsAddr})
	if err := svc.RegisterPullTaskCallback(topicName, func(mq common.MessageQueue,
		result *client.PullResult, context *client.PullTaskContext) int64 {
		if result != nil && result.PullStatus == client.PullFound {
			bodyMu.Lock()
			seenQueues[mq] = true
			for _, m := range result.MsgFoundList {
				received[string(m.GetBody())]++
			}
			bodyMu.Unlock()
		}
		return 0 // default resident interval
	}); err != nil {
		fmt.Fprintf(os.Stderr, "register callback: %v\n", err)
		os.Exit(1)
	}
	if err := svc.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "schedule start: %v\n", err)
		os.Exit(1)
	}
	check("S2 schedule service started", true, "group="+groupName)

	// one PullTask per assigned queue
	queuesOK := false
	for i := 0; i < 100; i++ {
		if len(svc.TaskQueues()) >= queues {
			queuesOK = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	check("S2 one PullTask per assigned queue expanded", queuesOK,
		fmt.Sprintf("task_queues=%d (want >=%d)", len(svc.TaskQueues()), queues))

	// ---- 2b. producer seeds the topic while the pull loop is already running ----
	if err := producer.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "producer start: %v\n", err)
		os.Exit(1)
	}
	seedOK := true
	for i := 0; i < *seedCount; i++ {
		msg := common.NewMessage(topicName, []byte(fmt.Sprintf("go-pull-schedule-%d", i)))
		if _, err := producer.Send(msg); err != nil {
			check("S2 seed send", false, err.Error())
			seedOK = false
			break
		}
	}
	check("S2 producer delivered the seed batch", seedOK,
		fmt.Sprintf("%d messages", *seedCount))

	// the pull loop delivers every body
	delivered := false
	for i := 0; i < 200; i++ {
		bodyMu.Lock()
		n := len(received)
		bodyMu.Unlock()
		if n >= *seedCount {
			delivered = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	check("S3 pull loop delivered every seed body", delivered,
		fmt.Sprintf("collected=%d unique_bodies=%d (want %d)", func() int {
			bodyMu.Lock()
			defer bodyMu.Unlock()
			n := 0
			for _, c := range received {
				n += c
			}
			return n
		}(), len(received), *seedCount))

	// ---- 3. broker-side offsets advanced on every queue ----
	offsetsOK := true
	var detail string
	for _, mq := range svc.TaskQueues() {
		off, found, err := admin.ExamineConsumerOffset(groupName, mq)
		if err != nil {
			offsetsOK = false
			detail += fmt.Sprintf(" q%d=err(%v)", mq.QueueID, err)
			continue
		}
		if !found {
			offsetsOK = false
		}
		detail += fmt.Sprintf(" q%d=%d", mq.QueueID, off)
	}
	check("S4 broker-side committed offset exists on every assigned queue", offsetsOK, detail)

	// ---- 4. clean shutdown ----
	svc.Shutdown()
	check("S5 shutdown clean", true, "")
	admin.DeleteTopic(topicName, "")
	admin.DeleteSubscriptionGroup(brokerAddr, groupName, true)
	admin.Shutdown()
	producer.Shutdown()

	fmt.Printf("\nPASS=%d FAIL=%d\n", passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
}
