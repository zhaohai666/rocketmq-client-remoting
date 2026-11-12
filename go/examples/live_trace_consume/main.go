// live_trace_consume is the Go consume-side message-trace probe against a real
// RocketMQ 5.x cluster (broker needs traceTopicEnable=true).
//
//	go run ./examples/live_trace_consume -ns 127.0.0.1:9876
//
// The Java client (ConsumeMessageTraceHookImpl + AsyncTraceDispatcher) emits
// SubBefore before every delivery and SubAfter after it. A RECONSUME_LATER
// delivery yields SubAfter(success=false, contextCode=4 == ConsumeReturnType.FAILED)
// and the %RETRY% redelivery yields SubBefore(retryTimes=1) +
// SubAfter(success=true, contextCode=0). This probe asserts the whole failure
// ladder off RMQ_SYS_TRACE_TOPIC, decoded with the client's own
// DecodeTraceDataString.
//
// Closing line `PASS=<n> FAIL=<n>`; the process exits non-zero on any FAIL.
package main

import (
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
	mu        sync.Mutex
)

func check(name string, ok bool, detail ...string) {
	d := ""
	if len(detail) > 0 {
		d = detail[0]
	}
	mu.Lock()
	defer mu.Unlock()
	if ok {
		passCount++
		fmt.Printf("PASS  %s  [%s]\n", name, d)
		return
	}
	failCount++
	fmt.Printf("FAIL  %s  [%s]\n", name, d)
}

// traceCollector consumes RMQ_SYS_TRACE_TOPIC and decodes every record.
type traceCollector struct {
	mu      sync.Mutex
	records []*client.TraceContext
}

func (t *traceCollector) ConsumeMessage(msgs []*common.MessageExt, _ *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range msgs {
		for _, ctx := range client.DecodeTraceDataString(string(m.Body)) {
			t.records = append(t.records, ctx)
		}
	}
	return client.ConsumeSuccess
}

func (t *traceCollector) snapshot() []*client.TraceContext {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*client.TraceContext(nil), t.records...)
}

// bizListener fails the first delivery of the target body, accepts everything else.
type bizListener struct {
	mu     sync.Mutex
	target string
	recs   []delivery
}

type delivery struct {
	body  string
	recon int32
}

func (b *bizListener) ConsumeMessage(msgs []*common.MessageExt, _ *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	b.mu.Lock()
	fail := false
	for _, m := range msgs {
		b.recs = append(b.recs, delivery{body: string(m.Body), recon: m.ReconsumeTimes})
		if string(m.Body) == b.target && m.ReconsumeTimes == 0 {
			fail = true
		}
	}
	b.mu.Unlock()
	if fail {
		return client.ReconsumeLater
	}
	return client.ConsumeSuccess
}

func (b *bizListener) snapshot() []delivery {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]delivery(nil), b.recs...)
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return cond()
}

func main() {
	ns := "127.0.0.1:9876"
	for i, a := range os.Args {
		if a == "-ns" && i+1 < len(os.Args) {
			ns = os.Args[i+1]
		}
	}
	stamp := time.Now().UnixNano()
	topic := fmt.Sprintf("GoTraceConsume%d", stamp)
	group := fmt.Sprintf("GID_go_trace_%d", stamp)
	readerGroup := group + "_reader"
	target := fmt.Sprintf("fail-me-%d", stamp)

	fmt.Printf("=== Go consume-trace live: ns=%s topic=%s ===\n", ns, topic)

	// Create the topic explicitly (B1: no default-topic fallback on the consumer side).
	admin := client.NewDefaultMQAdminExt(nil)
	admin.SetNameServerAddresses([]string{ns})
	if err := admin.Start(); err != nil {
		fmt.Println("FATAL admin start:", err)
		os.Exit(2)
	}
	ci, err := admin.FetchBrokerClusterInfo()
	if err != nil {
		fmt.Println("FATAL cluster info:", err)
		os.Exit(2)
	}
	var brokerName string
	for name := range ci.BrokerAddrTable {
		if brokerName == "" || name < brokerName {
			brokerName = name
		}
	}
	if err := admin.CreateTopic("TBW102", topic, 2, 0); err != nil {
		fmt.Println("FATAL create topic:", err)
		os.Exit(2)
	}
	admin.Shutdown()
	fmt.Println("topic created on", brokerName)

	// Trace reader first (FIRST_OFFSET over RMQ_SYS_TRACE_TOPIC).
	tc := &traceCollector{}
	reader, err := client.NewDefaultMQPushConsumer(readerGroup)
	if err != nil {
		fmt.Println("FATAL reader:", err)
		os.Exit(2)
	}
	reader.SetNameServerAddresses([]string{ns})
	reader.SetInstanceName(fmt.Sprintf("go-trace-reader-%d", stamp))
	reader.SetConsumeFromWhere("CONSUME_FROM_FIRST_OFFSET")
	_ = reader.Subscribe(common.TraceTopic, "*")
	if err := reader.SetMessageListener(tc); err != nil {
		fmt.Println("FATAL reader listener:", err)
		os.Exit(2)
	}
	if err := reader.Start(); err != nil {
		fmt.Println("FATAL reader start:", err)
		os.Exit(2)
	}
	time.Sleep(500 * time.Millisecond)

	// Business consumer with consume-side trace enabled.
	bl := &bizListener{target: target}
	c, err := client.NewDefaultMQPushConsumer(group)
	if err != nil {
		fmt.Println("FATAL consumer:", err)
		os.Exit(2)
	}
	c.SetNameServerAddresses([]string{ns})
	c.SetInstanceName(fmt.Sprintf("go-trace-consumer-%d", stamp))
	_ = c.Subscribe(topic, "*")
	if err := c.SetMessageListener(bl); err != nil {
		fmt.Println("FATAL consumer listener:", err)
		os.Exit(2)
	}
	c.SetEnableTrace(true)
	if err := c.Start(); err != nil {
		fmt.Println("FATAL consumer start:", err)
		os.Exit(2)
	}
	// B5: start the consumer before sending, wait for the assignment.
	waitFor(20*time.Second, func() bool { return len(c.SubscribeQueuesOf(topic)) > 0 })

	p, err := client.NewDefaultMQProducer(group + "_prod")
	if err != nil {
		fmt.Println("FATAL producer:", err)
		os.Exit(2)
	}
	p.SetNameServerAddresses([]string{ns})
	if err := p.Start(); err != nil {
		fmt.Println("FATAL producer start:", err)
		os.Exit(2)
	}
	msg := common.NewMessageWithTags(topic, []byte(target), "TagA", "KeyFail", 0)
	res, err := p.Send(msg)
	if err != nil {
		fmt.Println("FATAL send:", err)
		os.Exit(2)
	}
	uniq := res.MsgID
	fmt.Printf("sent uniq_key=%s offset=%s\n", uniq, res.OffsetMsgID)

	ofUniq := func(ctx *client.TraceContext) bool {
		return len(ctx.TraceBeans) > 0 && ctx.TraceBeans[0].MsgID == uniq
	}

	// F1 first delivery.
	first := waitFor(25*time.Second, func() bool {
		for _, d := range bl.snapshot() {
			if d.body == target {
				return true
			}
		}
		return false
	})
	check("F1 the first delivery reached the listener", first,
		fmt.Sprintf("deliveries=%d", len(bl.snapshot())))

	// F3 drives F2: the failed SubAfter surfaces once the dispatcher flushes.
	failedAfter := waitFor(25*time.Second, func() bool {
		for _, r := range tc.snapshot() {
			if r.TraceType == client.TraceSubAfter && ofUniq(r) && !r.IsSuccess && r.ContextCode == 4 {
				return true
			}
		}
		return false
	})

	counts := func() (before, after []*client.TraceContext) {
		for _, r := range tc.snapshot() {
			if !ofUniq(r) {
				continue
			}
			if r.TraceType == client.TraceSubBefore {
				before = append(before, r)
			} else if r.TraceType == client.TraceSubAfter {
				after = append(after, r)
			}
		}
		return
	}
	b1, a1 := counts()
	check("F2 SubBefore(retryTimes=0) for the failed message", containsRetry(b1, 0),
		fmt.Sprintf("before=%d", len(b1)))
	check("F3 SubAfter(success=false, contextCode=4 FAILED)", failedAfter,
		summarize(a1))

	// F4/F5/F6 the retry ladder (~10s broker delay at delayLevel 3).
	retried := waitFor(60*time.Second, func() bool {
		for _, d := range bl.snapshot() {
			if d.recon >= 1 {
				return true
			}
		}
		return false
	})
	finalAfter := waitFor(25*time.Second, func() bool {
		for _, r := range tc.snapshot() {
			if r.TraceType == client.TraceSubAfter && ofUniq(r) && r.IsSuccess && r.ContextCode == 0 {
				return true
			}
		}
		return false
	})
	// shutdown also flushes the dispatcher's tail
	c.Shutdown()
	p.Shutdown()
	time.Sleep(time.Second)

	b2, a2 := counts()
	check("F4 the %RETRY% redelivery was consumed", retried,
		fmt.Sprintf("deliveries=%s", deliverySummary(bl.snapshot())))
	check("F5 SubBefore(retryTimes=1) for the same message id", containsRetry(b2, 1),
		fmt.Sprintf("retryTimes=%s", retrySummary(b2)))
	check("F6 SubAfter(success=true, contextCode=0) for the retry", finalAfter,
		summarize(a2))

	reqIDs := map[string]bool{}
	for _, r := range b2 {
		reqIDs[r.RequestID] = true
	}
	paired := true
	for _, r := range a2 {
		if !reqIDs[r.RequestID] {
			paired = false
		}
	}
	check("F7 every SubAfter requestId was preceded by a SubBefore", paired,
		fmt.Sprintf("before=%d after=%d", len(b2), len(a2)))

	reader.Shutdown()

	fmt.Printf("\nPASS=%d FAIL=%d\n", passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
}

func containsRetry(ctxs []*client.TraceContext, retry int32) bool {
	for _, r := range ctxs {
		if r.TraceBeans[0].RetryTimes == retry {
			return true
		}
	}
	return false
}

func retrySummary(ctxs []*client.TraceContext) string {
	var parts []string
	for _, r := range ctxs {
		parts = append(parts, fmt.Sprintf("%d", r.TraceBeans[0].RetryTimes))
	}
	return strings.Join(parts, ",")
}

func summarize(ctxs []*client.TraceContext) string {
	var parts []string
	for _, r := range ctxs {
		parts = append(parts, fmt.Sprintf("(%v,%d)", r.IsSuccess, r.ContextCode))
	}
	return strings.Join(parts, " ")
}

func deliverySummary(ds []delivery) string {
	var parts []string
	for _, d := range ds {
		parts = append(parts, fmt.Sprintf("(recon=%d)", d.recon))
	}
	return strings.Join(parts, " ")
}
