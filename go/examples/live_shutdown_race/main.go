// Command live_shutdown_race verifies the "close a client and exit the process
// immediately" data contract against a real RocketMQ 5.x cluster (tasks
// #125/#126/#129/#131).
//
//	go run ./examples/live_shutdown_race -ns 127.0.0.1:9876 [-legs s1,s2,t]
//
// There are two windows on the shutdown path where data can be lost, and in
// both of them the thing that closes the window is "the process exits":
//
//  1. SEND-BACK WINDOW. While a concurrent listener is returning
//     RECONSUME_LATER for a whole batch, the dispatch goroutine is carrying
//     that batch's send-back RPCs. If Shutdown() returned without joining it,
//     an immediate exit would kill those RPCs — and since the consume offset
//     has already advanced past them, the broker would never redeliver and the
//     messages would never show up in %DLQ% either. That is the "message never
//     reached %DLQ%" report.
//  2. TRACE WINDOW. A short-lived trace producer's last batch of records is
//     just being flushed when Shutdown() is called; an exit that does not wait
//     for the flush leaves nothing on RMQ_SYS_TRACE_TOPIC.
//
// # WHY EVERY PHASE RUNS IN ITS OWN PROCESS
//
// Go has no runtime teardown to imitate: a leaked goroutine keeps running until
// main returns. So "the process exits right after Shutdown()" has to be a real
// process exit — every phase below re-executes this same binary with -phase,
// and the child calls os.Exit straight after Shutdown() with no sleep, no
// defer, nothing. Without that, a leaked send-back goroutine would finish on
// its own and the test would pass even against the broken build.
//
// THE TRAP THIS AVOIDS (same one the Rust live_shutdown_race documents): a
// single-process version that pads a sleep(2s) after shutdown() is testing the
// sleep, not the contract — the goroutines it forgot to join simply finish
// during the padding.
//
// A SECOND TRAP, and it cost a debugging round — this one is in the ASSERTIONS.
// A message pulled from %RETRY%<group> is delivered with msg.Topic RESTORED to
// the business topic (resetRetryTopicAndNamespace / Java resetRetryAndNamespace),
// so counting the deliveries as "topic == retryTopic" reads ZERO even though
// every single one arrived. %DLQ% messages are not restored for a probe group
// whose name differs from the original one — which is exactly why the retry
// probe below counts by BODY and the DLQ probes count by TOPIC. Each phase also
// prints the topic distribution the listener saw, so a mismatch is visible.
//
// Scenarios and the discriminator each one uses:
//
//   - S1 concurrent, all RECONSUME_LATER, two rounds, immediate exit both
//     times. Round A uses consumeMessageBatchMaxSize = 20 so the whole batch is
//     handed to the listener in ONE call; the child exits the instant that call
//     returns, i.e. while all 20 send-backs are still to be sent. Round B is a
//     second instance of the same group subscribed ONLY to %RETRY%: the mere
//     existence of those 20 bodies there proves round A's in-flight send-backs
//     landed before the exit (against the old build %RETRY% would be empty).
//     Round B fails everything again, so rt=1 >= maxReconsumeTimes=1 and the
//     broker moves the messages straight to %DLQ%; it exits immediately too.
//     Phase C reads %DLQ% back and must see all 20.
//   - S2 orderly SuspendCurrentQueueAMoment, one message per batch. Each
//     message is suspended twice locally and handed back on the third delivery
//     (orderly send-back is a PLAIN send to %RETRY%, and because the group still
//     holds the queue lock the broker routes it to %DLQ%). The child exits once
//     45 deliveries have happened — past the point where the first 15 messages
//     are on the send-back path. Phase B then needs all 20 original bodies
//     across topic ∪ %RETRY% ∪ %DLQ%, and at least one of them in %DLQ%.
//   - T a short-lived traced producer: send 6, exit immediately (the 5s flush
//     interval has not elapsed, so the records are still queued or in flight).
//     A reader must find the Pub record of all 6 msgIds on RMQ_SYS_TRACE_TOPIC.
//
// The last line is `PASS=<n> FAIL=<n>`; the process exits non-zero on any FAIL.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/client"
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

const (
	msgCount   = 20
	traceCount = 6
	// orderlySuspendTrigger is how many deliveries must have happened before the
	// shutdown is triggered. With maxReconsumeTimes = 2 every message takes 3
	// deliveries (two suspensions, then the send-back), so 45 means the first 15
	// messages have already been handed back and that path is genuinely busy.
	orderlySuspendTrigger = 45

	// firstDeliveryWindow waits for a whole batch to reach the listener — that
	// instant is what triggers the shutdown.
	firstDeliveryWindow = 60 * time.Second
	// roundTripWindow waits for a full broker round trip: the %RETRY% first-round
	// delay is level 3 (10s), plus retry/DLQ topic route visibility in the name
	// server (the broker registers routes every 30s) plus rebalance.
	roundTripWindow = 90 * time.Second
	// collectWindow waits for messages that are already in a topic this reader
	// subscribed to — only rebalance and pulling are missing.
	collectWindow = 90 * time.Second
	// traceReadWindow is generous because the shared RMQ_SYS_TRACE_TOPIC is read
	// from its first offset and may carry earlier runs' records.
	traceReadWindow = 150 * time.Second
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
	} else {
		failCount++
	}
	verdict := "FAIL"
	if ok {
		verdict = "PASS"
	}
	if detail != "" {
		fmt.Printf("%s  %s  [%s]\n", verdict, name, detail)
		return
	}
	fmt.Printf("%s  %s\n", verdict, name)
}

// atoi is lenient on purpose: a missing RESULT key reads as 0, which is the
// failure value for every counter this tool asserts on.
func atoi(s string) int {
	v, _ := strconv.Atoi(strings.TrimSpace(s))
	return v
}

// ------------------------------------------------------------------ naming

// names holds every topic/group of one run. Both the orchestrator and the
// per-phase child derive them from the same -tag, so a child needs no channel
// back to its parent other than stdout.
type names struct {
	tag                      string
	s1Topic, s1Group, s1Prod string
	s2Topic, s2Group, s2Prod string
	tTopic, tGroup           string
}

func namesFor(tag string) names {
	return names{
		tag:     tag,
		s1Topic: "GoShdnRaceS1" + tag,
		s1Group: "GID_shdn_race_s1_" + tag,
		s1Prod:  "GID_shdn_race_s1_prod_" + tag,
		s2Topic: "GoShdnRaceS2" + tag,
		s2Group: "GID_shdn_race_s2_" + tag,
		s2Prod:  "GID_shdn_race_s2_prod_" + tag,
		tTopic:  "GoShdnRaceT" + tag,
		tGroup:  "GID_shdn_race_t_" + tag,
	}
}

// ------------------------------------------------------------------ inbox

type hit struct{ topic, body string }

// inbox is the shared record of everything a listener saw. Polling is enough
// here except in phase S1-A, which waits on an explicit signal instead (see
// failAll.full).
type inbox struct {
	mu   sync.Mutex
	hits []hit
}

func (b *inbox) add(topic, body string) {
	b.mu.Lock()
	b.hits = append(b.hits, hit{topic: topic, body: body})
	b.mu.Unlock()
}

// distinct counts unique bodies, optionally restricted to one topic.
func (b *inbox) distinct(topic string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	set := map[string]struct{}{}
	for _, h := range b.hits {
		if topic == "" || h.topic == topic {
			set[h.body] = struct{}{}
		}
	}
	return len(set)
}

func (b *inbox) ofTopic(topic string) []hit {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]hit, 0, len(b.hits))
	for _, h := range b.hits {
		if h.topic == topic {
			out = append(out, h)
		}
	}
	return out
}

// topicSummary renders "topic=n" pairs sorted by topic, for diagnostics. It is
// what makes the %RETRY% topic-restore trap visible in a failure log.
func (b *inbox) topicSummary() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	counts := map[string]int{}
	for _, h := range b.hits {
		counts[h.topic]++
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, counts[k]))
	}
	return strings.Join(parts, " ")
}

func (b *inbox) waitDistinct(topic string, want int, window time.Duration) bool {
	deadline := time.Now().Add(window)
	for {
		if b.distinct(topic) >= want {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitAllBodies waits until every expected body showed up (optionally from one
// topic) — used for "all 20 originals came back" assertions.
func (b *inbox) waitAllBodies(expect []string, topic string, window time.Duration) bool {
	deadline := time.Now().Add(window)
	for {
		if b.hasAllBodies(expect, topic) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (b *inbox) hasAllBodies(expect []string, topic string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, e := range expect {
		found := false
		for _, h := range b.hits {
			if (topic == "" || h.topic == topic) && h.body == e {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// ------------------------------------------------------------------ listeners

// failAll records everything and fails the whole batch, so the consumer has to
// run the send-back path. `full` is closed the first time a batch of at least
// msgCount messages arrives — that is the exact instant phase S1-A wants to
// shut down in, with every send-back still pending.
type failAll struct {
	box  *inbox
	once sync.Once
	full chan struct{}
}

func newFailAll(box *inbox) *failAll {
	return &failAll{box: box, full: make(chan struct{})}
}

func (l *failAll) ConsumeMessage(msgs []*common.MessageExt, _ *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	for _, m := range msgs {
		l.box.add(m.Topic, string(m.Body))
	}
	if len(msgs) >= msgCount {
		l.once.Do(func() { close(l.full) })
	}
	return client.ReconsumeLater
}

// collect records and acknowledges.
type collect struct{ box *inbox }

func (l *collect) ConsumeMessage(msgs []*common.MessageExt, _ *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	for _, m := range msgs {
		l.box.add(m.Topic, string(m.Body))
	}
	return client.ConsumeSuccess
}

// suspendAllOrderly suspends in place and counts deliveries, so the child can
// wait for the send-back path to be busy before exiting.
type suspendAllOrderly struct{ deliveries *int64 }

func (l *suspendAllOrderly) ConsumeMessage(msgs []*common.MessageExt, _ *client.ConsumeOrderlyContext) client.ConsumeOrderlyStatus {
	atomic.AddInt64(l.deliveries, int64(len(msgs)))
	return client.OrderlySuspendCurrentQueueAMoment
}

// collectOrderly records and acknowledges on the orderly path.
type collectOrderly struct{ box *inbox }

func (l *collectOrderly) ConsumeMessage(msgs []*common.MessageExt, _ *client.ConsumeOrderlyContext) client.ConsumeOrderlyStatus {
	for _, m := range msgs {
		l.box.add(m.Topic, string(m.Body))
	}
	return client.OrderlySuccess
}

// ------------------------------------------------------------------ clients

func newAdmin(ns, instance string) *client.DefaultMQAdminExt {
	a := client.NewDefaultMQAdminExt(nil)
	a.SetNameServerAddresses([]string{ns})
	a.SetInstanceName(instance)
	a.SetTimeoutMillis(10_000)
	return a
}

// ensureTopic creates the topic explicitly. Deriving the queue count instead of
// relying on auto-creation matters: this broker auto-creates with 8 queues
// (defaultTopicQueueNums), and S1 wants all 20 messages in ONE batch.
func ensureTopic(ns, instance, topic string, queues int32) error {
	a := newAdmin(ns, instance)
	if err := a.Start(); err != nil {
		return err
	}
	defer a.Shutdown()
	return a.CreateTopic(common.DefaultTopic, topic, queues, 0)
}

func newProducer(ns, group, instance string, trace bool) (*client.DefaultMQProducer, error) {
	p, err := client.NewDefaultMQProducer(group)
	if err != nil {
		return nil, err
	}
	p.SetNameServerAddresses([]string{ns})
	p.SetInstanceName(instance)
	p.SetSendMsgTimeout(5000)
	if trace {
		p.SetEnableTrace(true)
	}
	if err := p.Start(); err != nil {
		return nil, err
	}
	return p, nil
}

// sendWithRouteRetry rides out the "topic just created, route not registered
// yet" window (the broker re-registers every 30s).
func sendWithRouteRetry(p *client.DefaultMQProducer, topic, body string) (string, error) {
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		res, err := p.Send(common.NewMessage(topic, []byte(body)))
		if err == nil && res.SendStatus == client.SendOK {
			return res.MsgID, nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("send status %s", res.SendStatus)
		}
		time.Sleep(2 * time.Second)
	}
	return "", fmt.Errorf("send %q never succeeded: %w", body, lastErr)
}

type consCfg struct {
	group     string
	instance  string
	listener  any
	batchMax  int
	maxRecon  int32 // 0 = leave the client default
	suspendMS int64
}

func newPush(ns string, cfg consCfg, topics ...string) (*client.DefaultMQPushConsumer, error) {
	c, err := client.NewDefaultMQPushConsumer(cfg.group)
	if err != nil {
		return nil, err
	}
	c.SetNameServerAddresses([]string{ns})
	c.SetInstanceName(cfg.instance)
	// Every leg of this tool starts a brand-new group, so there is no committed
	// offset: reading from the first offset is what makes the pre-produced
	// messages visible (from-where only applies when no offset exists).
	c.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	c.SetConsumeThreadMin(2)
	c.SetConsumeThreadMax(8)
	if cfg.batchMax > 0 {
		c.SetConsumeMessageBatchMaxSize(cfg.batchMax)
	}
	if cfg.maxRecon != 0 {
		c.SetMaxReconsumeTimes(cfg.maxRecon)
	}
	if cfg.suspendMS > 0 {
		c.SetSuspendCurrentQueueTimeMillis(cfg.suspendMS)
	}
	if err := c.SetMessageListener(cfg.listener); err != nil {
		return nil, err
	}
	for _, t := range topics {
		if err := c.Subscribe(t, "*"); err != nil {
			return nil, err
		}
	}
	if err := c.Start(); err != nil {
		return nil, err
	}
	return c, nil
}

// killNow is the Go analogue of Rust's drop(runtime): os.Exit tears the process
// down without waiting for a single goroutine. Whatever the client still had in
// flight at this instant is gone — that is the contract under test.
func killNow() { os.Exit(0) }

// ------------------------------------------------------------------ main

func main() {
	ns := flag.String("ns", "127.0.0.1:9876", "name server address")
	phase := flag.String("phase", "", "internal: run exactly one phase in this process, then exit")
	tagFlag := flag.String("tag", "", "run tag (default: unix seconds)")
	msgIDs := flag.String("msgids", "", "internal: comma separated msgIds for the trace read-back phase")
	legs := flag.String("legs", "", "which legs to run: s1,s2,t (default: all)")
	flag.Parse()

	tag := *tagFlag
	if tag == "" {
		tag = strconv.FormatInt(time.Now().Unix(), 10)
	}
	n := namesFor(tag)

	if *phase != "" {
		runPhase(*phase, *ns, n, *msgIDs)
		return
	}

	want := func(leg string) bool {
		if strings.TrimSpace(*legs) == "" {
			return true
		}
		for _, l := range strings.Split(*legs, ",") {
			l = strings.TrimSpace(l)
			if l == "all" || l == leg {
				return true
			}
		}
		return false
	}

	fmt.Printf("=== Go shutdown-race live: ns=%s tag=%s ===\n", *ns, tag)
	fmt.Println("每个阶段都跑在独立进程里，Shutdown() 之后立刻 os.Exit —— 进程退出会掐断所有还在飞的 goroutine，")
	fmt.Println("这正是被测的契约（单进程版会在退出前把没 join 的 goroutine 跑完，从而假绿）。")

	// ---------------------------------------------------------------- S1
	if want("s1") {
		res := runChild(*ns, tag, "s1a", "")
		check("S1-A 20 条整批投到 listener（整批回投在飞，立即退进程）",
			atoi(res["delivered"]) == msgCount, "delivered="+res["delivered"])

		res = runChild(*ns, tag, "s1b", "")
		check("S1-A 关停返回前 send-back 已落地：%RETRY% 收齐 20 条原文",
			atoi(res["retry"]) == msgCount, "retry="+res["retry"])

		res = runChild(*ns, tag, "s1c", "")
		check("S1-B 关停返回前 DLQ 回投已落地：%DLQ% 收齐 20 条原文",
			atoi(res["dlq"]) == msgCount, "dlq="+res["dlq"])
	}

	// ---------------------------------------------------------------- S2
	if want("s2") {
		res := runChild(*ns, tag, "s2a", "")
		check("S2-A 顺序消费挂起轮次已起（触发立即关停）",
			atoi(res["deliveries"]) >= orderlySuspendTrigger, "deliveries="+res["deliveries"])

		res = runChild(*ns, tag, "s2b", "")
		check("S2 立即退进程后 20 条原文全部重现（topic∪RETRY∪DLQ）",
			atoi(res["all"]) == msgCount, "all="+res["all"])
		check("S2 顺序回投链路已落地 %DLQ%（越过本地重试上限的条目）",
			atoi(res["dlq"]) >= 1, "dlq="+res["dlq"])
	}

	// ---------------------------------------------------------------- T
	if want("t") {
		res := runChild(*ns, tag, "ta", "")
		ids := strings.TrimSpace(res["msgids"])
		sent := 0
		if ids != "" {
			sent = len(strings.Split(ids, ","))
		}
		check("T-A 6 条发送成功（轨迹生产者，发完立即退进程）", sent == traceCount,
			fmt.Sprintf("sent=%d", sent))

		if sent == traceCount {
			res = runChild(*ns, tag, "tb", ids)
			check("T 立即退进程后最后一批 6 条 Pub 轨迹全部可读（shutdown 返回即已落 broker）",
				res["matched"] == "1" && atoi(res["pubs"]) >= traceCount,
				fmt.Sprintf("pubs=%s seen=%s", res["pubs"], res["seen"]))
		} else {
			check("T 立即退进程后最后一批 6 条 Pub 轨迹全部可读（shutdown 返回即已落 broker）",
				false, "上游阶段没发出 6 条，无法判定")
		}
	}

	// ---------------------------------------------------------------- cleanup
	fmt.Println("\n--- 清理 ---")
	admin := newAdmin(*ns, "ADMIN-shdn-race-"+tag)
	if err := admin.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "admin start: %v\n", err)
	} else {
		for _, t := range []string{
			n.s1Topic, n.s2Topic, n.tTopic,
			common.GetRetryTopic(n.s1Group), common.GetDLQTopic(n.s1Group),
			common.GetRetryTopic(n.s2Group), common.GetDLQTopic(n.s2Group),
		} {
			if err := admin.DeleteTopic(t, ""); err != nil {
				fmt.Printf("  delete %s: %v\n", t, err)
			}
		}
		admin.Shutdown()
	}

	fmt.Printf("\nPASS=%d FAIL=%d\n", passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
}

// runChild re-executes this binary for one phase and returns the RESULT lines
// the child printed. The child's output is streamed through so a failure can be
// read in place.
func runChild(ns, tag, phase, msgids string) map[string]string {
	exe, err := os.Executable()
	if err != nil {
		exe = os.Args[0]
	}
	args := []string{"-ns", ns, "-phase", phase, "-tag", tag}
	if msgids != "" {
		args = append(args, "-msgids", msgids)
	}
	cmd := exec.Command(exe, args...)
	cmd.Env = os.Environ()
	var buf bytes.Buffer
	cmd.Stdout = io.MultiWriter(os.Stdout, &buf)
	cmd.Stderr = os.Stderr
	fmt.Printf("\n--- phase %s：独立子进程（阶段结束 = 进程退出）---\n", phase)
	if err := cmd.Run(); err != nil {
		fmt.Printf("--- phase %s 子进程非 0 退出: %v\n", phase, err)
	}
	return parseResults(buf.String())
}

func parseResults(out string) map[string]string {
	m := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "RESULT ") {
			continue
		}
		kv := strings.TrimPrefix(line, "RESULT ")
		if i := strings.Index(kv, "="); i > 0 {
			m[kv[:i]] = kv[i+1:]
		}
	}
	return m
}

func runPhase(phase, ns string, n names, msgIDs string) {
	switch phase {
	case "s1a":
		phaseS1A(ns, n)
	case "s1b":
		phaseS1B(ns, n)
	case "s1c":
		phaseS1C(ns, n)
	case "s2a":
		phaseS2A(ns, n)
	case "s2b":
		phaseS2B(ns, n)
	case "ta":
		phaseTraceSend(ns, n)
	case "tb":
		phaseTraceRead(ns, n, msgIDs)
	default:
		fmt.Fprintf(os.Stderr, "unknown phase %q\n", phase)
	}
}

// ------------------------------------------------------------------ phases

// phaseS1A produces the batch, delivers it in ONE listener call and exits the
// instant that call returns — with all 20 send-backs still pending.
func phaseS1A(ns string, n names) {
	fail := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "s1a: "+format+"\n", args...)
		fmt.Println("RESULT delivered=0")
	}

	if err := ensureTopic(ns, "go-shdn-s1a-"+n.tag, n.s1Topic, 1); err != nil {
		fail("create topic: %v", err)
		return
	}
	p, err := newProducer(ns, n.s1Prod, "go-shdn-s1-prod-"+n.tag, false)
	if err != nil {
		fail("producer: %v", err)
		return
	}
	prefix := "s1-" + n.tag
	for i := 0; i < msgCount; i++ {
		if _, err := sendWithRouteRetry(p, n.s1Topic, fmt.Sprintf("%s-%d", prefix, i)); err != nil {
			fail("send: %v", err)
			return
		}
	}
	p.Shutdown()

	box := &inbox{}
	fa := newFailAll(box)
	c, err := newPush(ns, consCfg{
		group: n.s1Group, instance: "go-shdn-s1-v1-" + n.tag,
		listener: fa, batchMax: msgCount, maxRecon: 1,
	}, n.s1Topic)
	if err != nil {
		fail("consumer: %v", err)
		return
	}
	select {
	case <-fa.full:
	case <-time.After(firstDeliveryWindow):
		fmt.Fprintf(os.Stderr, "s1a: 等不到整批投递（%d 条）\n", msgCount)
	}
	fmt.Printf("RESULT delivered=%d\n", box.distinct(n.s1Topic))
	fmt.Printf("  20 条整批已投到 listener（%v），此刻整批 send-back RPC 正在飞 → shutdown + 立刻退进程\n", box.distinct(n.s1Topic))
	c.Shutdown()
	killNow()
}

// phaseS1B is a second instance of the SAME group subscribed only to %RETRY%.
// Finding the 20 originals here IS the proof that round A's in-flight
// send-backs landed before its process exited. Everything fails again, so
// rt=1 >= maxReconsumeTimes=1 pushes them to %DLQ% (verified in phase C).
func phaseS1B(ns string, n names) {
	box := &inbox{}
	retryTopic := common.GetRetryTopic(n.s1Group)
	c, err := newPush(ns, consCfg{
		group: n.s1Group, instance: "go-shdn-s1-v2-" + n.tag,
		listener: newFailAll(box), batchMax: msgCount, maxRecon: 1,
	}, retryTopic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "s1b: consumer: %v\n", err)
		fmt.Println("RESULT retry=0")
		return
	}
	// TRAP, and it cost a debugging round: a message pulled from %RETRY%<group>
	// is delivered with msg.Topic RESTORED to the business topic
	// (resetRetryTopicAndNamespace / Java resetRetryAndNamespace). Counting
	// deliveries as "topic == retryTopic" therefore sees ZERO even when every
	// single one arrived — count by body, and print the topics the listener
	// actually saw so the trap is visible if this ever breaks again.
	box.waitDistinct("", msgCount, roundTripWindow)
	got := box.distinct("")
	fmt.Printf("RESULT retry=%d\n", got)
	fmt.Printf("  %s 收到 %d/%d 条原文（判据：A 轮在途 send-back 已在关停返回前落地）\n",
		retryTopic, got, msgCount)
	fmt.Printf("  listener 看到的主题分布：%s\n", box.topicSummary())
	c.Shutdown()
	killNow()
}

// phaseS1C verifies the terminal state: %DLQ% must hold all 20 originals.
func phaseS1C(ns string, n names) {
	box := &inbox{}
	dlqTopic := common.GetDLQTopic(n.s1Group)
	c, err := newPush(ns, consCfg{
		group: "GID_shdn_race_s1_probe_" + n.tag, instance: "go-shdn-s1-probe-" + n.tag,
		listener: &collect{box: box}, batchMax: 1, maxRecon: 1,
	}, dlqTopic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "s1c: consumer: %v\n", err)
		fmt.Println("RESULT dlq=0")
		return
	}
	// No topic restore HERE, unlike phase S1-B: resetRetryTopicAndNamespace only
	// rewrites msg.Topic when the physical topic is %RETRY%<this group>, and a
	// DLQ message lives under %DLQ%<the ORIGINAL group> — a different string for
	// this probe group. Counting by topic is therefore right on this path.
	box.waitDistinct(dlqTopic, msgCount, collectWindow)
	got := box.distinct(dlqTopic)
	fmt.Printf("RESULT dlq=%d\n", got)
	fmt.Printf("  %s 收到 %d/%d 条原文；listener 主题分布：%s\n", dlqTopic, got, msgCount, box.topicSummary())
	c.Shutdown()
	killNow()
}

// phaseS2A suspends every orderly delivery and exits once the send-back path is
// demonstrably busy.
func phaseS2A(ns string, n names) {
	fail := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "s2a: "+format+"\n", args...)
		fmt.Println("RESULT deliveries=0")
	}

	if err := ensureTopic(ns, "go-shdn-s2a-"+n.tag, n.s2Topic, 1); err != nil {
		fail("create topic: %v", err)
		return
	}
	p, err := newProducer(ns, n.s2Prod, "go-shdn-s2-prod-"+n.tag, false)
	if err != nil {
		fail("producer: %v", err)
		return
	}
	prefix := "s2-" + n.tag
	for i := 0; i < msgCount; i++ {
		if _, err := sendWithRouteRetry(p, n.s2Topic, fmt.Sprintf("%s-%d", prefix, i)); err != nil {
			fail("send: %v", err)
			return
		}
	}
	p.Shutdown()

	var deliveries int64
	c, err := newPush(ns, consCfg{
		group: n.s2Group, instance: "go-shdn-s2-v1-" + n.tag,
		listener: &suspendAllOrderly{deliveries: &deliveries},
		batchMax: 1, maxRecon: 2, suspendMS: 50,
	}, n.s2Topic)
	if err != nil {
		fail("consumer: %v", err)
		return
	}
	deadline := time.Now().Add(firstDeliveryWindow)
	for atomic.LoadInt64(&deliveries) < orderlySuspendTrigger && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	seen := atomic.LoadInt64(&deliveries)
	fmt.Printf("RESULT deliveries=%d\n", seen)
	fmt.Printf("  顺序投递轮次 %d（阈值 %d）：前几条已越过重试上限、顺序回投正在跑 → shutdown + 立刻退进程\n",
		seen, orderlySuspendTrigger)
	c.Shutdown()
	killNow()
}

// phaseS2B re-reads the whole chain: topic ∪ %RETRY% ∪ %DLQ%.
func phaseS2B(ns string, n names) {
	box := &inbox{}
	dlqTopic := common.GetDLQTopic(n.s2Group)
	c, err := newPush(ns, consCfg{
		group: n.s2Group, instance: "go-shdn-s2-v2-" + n.tag,
		listener: &collectOrderly{box: box}, batchMax: 1, maxRecon: 2,
	}, n.s2Topic, common.GetRetryTopic(n.s2Group), dlqTopic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "s2b: consumer: %v\n", err)
		fmt.Println("RESULT all=0")
		fmt.Println("RESULT dlq=0")
		return
	}
	prefix := "s2-" + n.tag
	expect := make([]string, 0, msgCount)
	for i := 0; i < msgCount; i++ {
		expect = append(expect, fmt.Sprintf("%s-%d", prefix, i))
	}
	box.waitAllBodies(expect, "", roundTripWindow)
	fmt.Printf("RESULT all=%d\n", box.distinct(""))
	fmt.Printf("RESULT dlq=%d\n", box.distinct(dlqTopic))
	c.Shutdown()
	killNow()
}

// phaseTraceSend is the short-lived traced producer: it exits before the 5s
// flush interval, so the last batch is still queued or in flight.
func phaseTraceSend(ns string, n names) {
	if err := ensureTopic(ns, "go-shdn-ta-"+n.tag, n.tTopic, 1); err != nil {
		fmt.Fprintf(os.Stderr, "ta: create topic: %v\n", err)
		fmt.Println("RESULT msgids=")
		return
	}
	p, err := newProducer(ns, n.tGroup, "go-shdn-t-prod-"+n.tag, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ta: producer: %v\n", err)
		fmt.Println("RESULT msgids=")
		return
	}
	ids := make([]string, 0, traceCount)
	for i := 0; i < traceCount; i++ {
		id, err := sendWithRouteRetry(p, n.tTopic, fmt.Sprintf("t-%s-%d", n.tag, i))
		if err != nil {
			fmt.Fprintf(os.Stderr, "ta: send %d: %v\n", i, err)
			continue
		}
		ids = append(ids, id)
	}
	fmt.Printf("RESULT msgids=%s\n", strings.Join(ids, ","))
	fmt.Printf("  已发 %d 条，flush 周期（5s）没到 → shutdown + 立刻退进程\n", len(ids))
	p.Shutdown()
	killNow()
}

// phaseTraceRead pulls RMQ_SYS_TRACE_TOPIC from its first offset and looks for
// the Pub record of every msgId the sender reported.
func phaseTraceRead(ns string, n names, msgIDs string) {
	ids := []string{}
	for _, id := range strings.Split(msgIDs, ",") {
		if id = strings.TrimSpace(id); id != "" {
			ids = append(ids, id)
		}
	}
	want := map[string]struct{}{}
	for _, id := range ids {
		want[id] = struct{}{}
	}
	emit := func(pubs int, seen int) {
		fmt.Printf("RESULT pubs=%d\n", pubs)
		fmt.Printf("RESULT seen=%d\n", seen)
		matched := "0"
		if pubs >= traceCount {
			matched = "1"
		}
		fmt.Printf("RESULT matched=%s\n", matched)
	}

	if len(ids) == 0 {
		fmt.Fprintln(os.Stderr, "tb: no msgIds to look for")
		emit(0, 0)
		return
	}
	box := &inbox{}
	reader, err := newPush(ns, consCfg{
		group: "GID_shdn_race_t_reader_" + n.tag, instance: "go-shdn-t-reader-" + n.tag,
		listener: &collect{box: box}, batchMax: 1,
	}, common.TraceTopic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tb: consumer: %v\n", err)
		emit(0, 0)
		return
	}

	// Wait until every msgId shows up somewhere in the trace topic's bodies;
	// the records are plain transfer strings, so a plain substring probe is the
	// cheapest early exit, and the precise decode follows.
	deadline := time.Now().Add(traceReadWindow)
	for time.Now().Before(deadline) {
		box.mu.Lock()
		corpus := strings.Builder{}
		for _, h := range box.hits {
			if h.topic == common.TraceTopic {
				corpus.WriteString(h.body)
			}
		}
		box.mu.Unlock()
		text := corpus.String()
		all := true
		for _, id := range ids {
			if !strings.Contains(text, id) {
				all = false
				break
			}
		}
		if all {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	pubs := 0
	var seenBodies []hit
	for _, h := range box.ofTopic(common.TraceTopic) {
		seenBodies = append(seenBodies, h)
		for _, ctx := range client.DecodeTraceDataString(h.body) {
			if ctx.TraceType != client.TracePub {
				continue
			}
			for _, bean := range ctx.TraceBeans {
				if _, ok := want[bean.MsgID]; ok {
					pubs++
					break
				}
			}
		}
	}
	fmt.Printf("  RMQ_SYS_TRACE_TOPIC 读到 %d 条轨迹消息，其中 %d 条 Pub 命中本次 6 个 msgId\n",
		len(seenBodies), pubs)
	emit(pubs, len(seenBodies))
	reader.Shutdown()
	killNow()
}
