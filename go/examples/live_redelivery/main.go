// Command live_redelivery verifies broker-side redelivery and the dead-letter
// terminal state against a real RocketMQ 5.x cluster.
//
//	go run ./examples/live_redelivery -ns 127.0.0.1:9876 [-legs s1,s2,s3,s4]
//
// This is the consume-side area the Go port had NO live coverage for: the
// existing live_consumer's S1 asserts the retry topic stays EMPTY, so nothing
// exercised %RETRY% redelivery, the reconsumeTimes ladder or %DLQ%.
//
// Scenarios:
//
//	S1 REDELIVERY. A listener returns RECONSUME_LATER for one message, on its
//	   first delivery only. The broker puts it back through %RETRY%<group> with
//	   delay level 3 (10s) and RECONSUME_TIMES=1, and the client restores the
//	   topic to the business topic before the listener sees it. The other
//	   message must be delivered exactly once.
//	S2 DEAD-LETTER TERMINAL. maxReconsumeTimes=2 means the broker refuses the
//	   third send-back (`msgExt.getReconsumeTimes() >= maxReconsumeTimes`, note
//	   the `>=`) and moves the message to %DLQ%<group> with reconsumeTimes=3,
//	   while the client delivered it exactly 3 times (0/1/2). Asserting BOTH
//	   halves is the point: getting either side wrong yields either "stuck
//	   retrying forever" or "dead-lettered too early", and neither shows up in
//	   a unit test.
//	S3 ORDERLY POISON MESSAGE. An orderly listener that always suspends gets
//	   exactly 3 local deliveries (the client bumps reconsumeTimes itself), then
//	   hands the message to the broker as a PLAIN send to %RETRY%; because the
//	   group still holds the queue lock, the broker routes it straight to %DLQ%.
//	   That is a different code path from S2's CONSUMER_SEND_MSG_BACK(36), and it
//	   is NOT the same test: the plain-send branch dead-letters on
//	   `reconsumeTimes > maxReconsumeTimes` (SendMessageProcessor:210, strict)
//	   and consults RebalanceLockManager.isLockAllExpired, while
//	   consumerSendMsgBack uses `>=`. A port that copies one bound onto the other
//	   still looks right in either scenario alone; only having both pins it.
//	S4 PARTIAL ACK (ackIndex). A 3-message batch acknowledged up to index 0 only
//	   sends the remaining 2 back through %RETRY% — the acked one is never
//	   redelivered — and the business queue offset still commits as a whole
//	   batch (3). A control consumer that leaves ackIndex alone must not
//	   redeliver anything, which is what makes the redelivery attributable to
//	   the ack rather than to the harness.
//
// Every scenario creates its topic first: a consumer has no default-topic
// fallback (only producers get TBW102), so a missing topic means "no route, no
// assignment, no consumption" — a false failure that reads like a client bug.
//
// The last line is `PASS=<n> FAIL=<n>`; the process exits non-zero on any FAIL.
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

// ------------------------------------------------------------------ records

// rec is one delivery as the LISTENER saw it: body, the topic the client
// presented (after resetRetryTopicAndNamespace), the reconsumeTimes on the wire
// and when it happened.
type rec struct {
	body  string
	topic string
	recon int32
	at    time.Time
}

type box struct {
	mu   sync.Mutex
	recs []rec
}

func (b *box) add(m *common.MessageExt) {
	b.mu.Lock()
	b.recs = append(b.recs, rec{
		body:  string(m.Body),
		topic: m.Topic,
		recon: m.ReconsumeTimes,
		at:    time.Now(),
	})
	b.mu.Unlock()
}

func (b *box) all() []rec {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]rec(nil), b.recs...)
}

func (b *box) ofBody(body string) []rec {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]rec, 0, 4)
	for _, r := range b.recs {
		if r.body == body {
			out = append(out, r)
		}
	}
	return out
}

func (b *box) distinctBodies() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	set := map[string]struct{}{}
	for _, r := range b.recs {
		set[r.body] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (b *box) topicSummary() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	counts := map[string]int{}
	for _, r := range b.recs {
		counts[r.topic]++
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

// ------------------------------------------------------------------ listeners

// retryOnce fails exactly the first delivery of `target` and nothing else.
type retryOnce struct {
	box    *box
	target string
}

func (l *retryOnce) ConsumeMessage(msgs []*common.MessageExt, _ *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	fail := false
	for _, m := range msgs {
		if string(m.Body) == l.target && m.ReconsumeTimes == 0 {
			fail = true
		}
		l.box.add(m)
	}
	if fail {
		return client.ReconsumeLater
	}
	return client.ConsumeSuccess
}

// alwaysFail fails every delivery — the reconsumeTimes ladder then marches to
// the ceiling and the broker dead-letters the message.
type alwaysFail struct{ box *box }

func (l *alwaysFail) ConsumeMessage(msgs []*common.MessageExt, _ *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	for _, m := range msgs {
		l.box.add(m)
	}
	return client.ReconsumeLater
}

// suspendOrderly suspends in place; the client counts the attempts locally.
type suspendOrderly struct {
	box        *box
	deliveries *int64
}

func (l *suspendOrderly) ConsumeMessage(msgs []*common.MessageExt, _ *client.ConsumeOrderlyContext) client.ConsumeOrderlyStatus {
	for _, m := range msgs {
		l.box.add(m)
	}
	*l.deliveries += int64(len(msgs))
	return client.OrderlySuspendCurrentQueueAMoment
}

// partialAckTo0 acknowledges only the first message of the first delivery, i.e.
// the whole acked prefix. A retry delivery (reconsumeTimes >= 1) is taken in
// full so the ladder terminates.
type partialAckTo0 struct{ box *box }

func (l *partialAckTo0) ConsumeMessage(msgs []*common.MessageExt, ctx *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	retry := false
	for _, m := range msgs {
		if m.ReconsumeTimes > 0 {
			retry = true
		}
		l.box.add(m)
	}
	if !retry {
		ctx.AckIndex = 0
	}
	return client.ConsumeSuccess
}

// collect acknowledges everything (control group).
type collect struct{ box *box }

func (l *collect) ConsumeMessage(msgs []*common.MessageExt, _ *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	for _, m := range msgs {
		l.box.add(m)
	}
	return client.ConsumeSuccess
}

// ------------------------------------------------------------------ helpers

func newAdmin(ns, instance string) *client.DefaultMQAdminExt {
	a := client.NewDefaultMQAdminExt(nil)
	a.SetNameServerAddresses([]string{ns})
	a.SetInstanceName(instance)
	a.SetTimeoutMillis(10_000)
	return a
}

// ensureTopic creates the topic explicitly. Relying on auto-creation would give
// this broker's default 8 queues (defaultTopicQueueNums), and several checks
// below want a deterministic single-queue topic.
func ensureTopic(ns, instance, topic string, queues int32) error {
	a := newAdmin(ns, instance)
	if err := a.Start(); err != nil {
		return err
	}
	defer a.Shutdown()
	return a.CreateTopic(common.DefaultTopic, topic, queues, 0)
}

type producerHandle struct {
	p *client.DefaultMQProducer
}

func startProducer(ns, group, instance string) (*producerHandle, error) {
	p, err := client.NewDefaultMQProducer(group)
	if err != nil {
		return nil, err
	}
	p.SetNameServerAddresses([]string{ns})
	p.SetInstanceName(instance)
	p.SetSendMsgTimeout(5000)
	if err := p.Start(); err != nil {
		return nil, err
	}
	return &producerHandle{p: p}, nil
}

func (h *producerHandle) send(topic, body string) error {
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		res, err := h.p.Send(common.NewMessage(topic, []byte(body)))
		if err == nil && res.SendStatus == client.SendOK {
			return nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("send status %s", res.SendStatus)
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("send %q never succeeded: %w", body, lastErr)
}

func (h *producerHandle) shutdown() { h.p.Shutdown() }

type consCfg struct {
	group     string
	instance  string
	listener  any
	batchMax  int
	maxRecon  int32
	suspendMS int64
}

func startConsumer(ns string, cfg consCfg, topics ...string) (*client.DefaultMQPushConsumer, error) {
	c, err := client.NewDefaultMQPushConsumer(cfg.group)
	if err != nil {
		return nil, err
	}
	c.SetNameServerAddresses([]string{ns})
	c.SetInstanceName(cfg.instance)
	// FIRST_OFFSET everywhere: every group here is brand new, and reading from
	// the queue head is what makes pre-produced messages visible no matter how
	// the scenario orders "start" versus "send".
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

// waitUntil polls cond until it holds or the window expires.
func waitUntil(window time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(window)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return cond()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitTopicRoute waits for the broker to publish a topic's route; %DLQ% and
// %RETRY% topics do not exist until the first message lands in them, and the
// broker only re-registers routes every 30s.
func waitTopicRoute(ns, instance, topic string, window time.Duration) bool {
	a := newAdmin(ns, instance)
	if err := a.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "route probe admin: %v\n", err)
		return false
	}
	defer a.Shutdown()
	return waitUntil(window, func() bool {
		route, err := a.ExamineTopicRoute(topic)
		return err == nil && route != nil && len(route.BrokerDatas) > 0
	})
}

// readTopicFromHead reads everything currently on `topic` with a throwaway
// consumer group, from the first offset. Used to prove what actually landed in
// %DLQ% rather than trusting an offset pointer.
func readTopicFromHead(ns, topic, group, instance string, window time.Duration) ([]rec, *box, error) {
	b := &box{}
	c, err := startConsumer(ns, consCfg{
		group: group, instance: instance, listener: &collect{box: b}, batchMax: 1,
	}, topic)
	if err != nil {
		return nil, b, err
	}
	waitUntil(window, func() bool { return len(b.all()) > 0 })
	time.Sleep(2 * time.Second) // let any straggler in the same pull land
	out := b.all()
	c.Shutdown()
	return out, b, nil
}

// consumedOffset reads one queue's committed offset for a group.
func consumedOffset(ns, instance, group, topic string) (int64, bool) {
	a := newAdmin(ns, instance)
	if err := a.Start(); err != nil {
		return -1, false
	}
	defer a.Shutdown()
	route, err := a.ExamineTopicRoute(topic)
	if err != nil || route == nil || len(route.BrokerDatas) == 0 {
		return -1, false
	}
	mq := common.NewMessageQueue(topic, route.BrokerDatas[0].BrokerName, 0)
	offset, found, err := a.ExamineConsumerOffset(group, mq)
	if err != nil {
		return -1, false
	}
	return offset, found
}

// ------------------------------------------------------------------ main

func main() {
	ns := flag.String("ns", "127.0.0.1:9876", "name server address")
	legs := flag.String("legs", "", "which scenarios to run: s1,s2,s3,s4 (default: all)")
	flag.Parse()

	stamp := fmt.Sprintf("%d", time.Now().Unix())
	prefix := "GoRedelivery" + stamp
	fmt.Printf("=== Go redelivery live: ns=%s prefix=%s legs=%s ===\n", *ns, prefix, orAll(*legs))

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

	topics := map[string]string{}
	defer cleanup(*ns, prefix, topics)

	// S2 and S3 both wait for a dead-letter terminal state, so they are the slow
	// ones (~2min each); -legs exists so a single scenario can be re-run while
	// iterating instead of paying for all four.
	if want("s1") {
		runS1(*ns, prefix, topics)
	}
	if want("s2") {
		runS2(*ns, prefix, topics)
	}
	if want("s3") {
		runS3(*ns, prefix, topics)
	}
	if want("s4") {
		runS4(*ns, prefix, topics)
	}

	fmt.Printf("\nPASS=%d FAIL=%d\n", passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
}

func cleanup(ns, prefix string, topics map[string]string) {
	a := newAdmin(ns, "ADMIN-redelivery-"+prefix)
	if err := a.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "cleanup admin: %v\n", err)
		return
	}
	defer a.Shutdown()
	names := make([]string, 0, len(topics))
	for _, t := range topics {
		names = append(names, t)
	}
	sort.Strings(names)
	for _, t := range names {
		if t == "" {
			continue
		}
		if err := a.DeleteTopic(t, ""); err != nil {
			fmt.Printf("  delete %s: %v\n", t, err)
		}
	}
}

// ------------------------------------------------------------------ S1

// S1: one message comes back through %RETRY% after 10s, the other is delivered
// once, and the retry delivery shows the BUSINESS topic (the client restores it
// before the listener; a harness that books by the physical topic reads zero).
func runS1(ns, prefix string, topics map[string]string) {
	fmt.Println("\n--- S1 回投：%RETRY% 二次投递 + 延迟梯度 + topic 还原 ---")
	topic := prefix + "_Retry"
	topics["s1"] = topic
	group := "GID_" + prefix + "_s1"
	if err := ensureTopic(ns, "go-rd-s1-"+prefix, topic, 1); err != nil {
		check("S1 建 topic", false, err.Error())
		return
	}
	p, err := startProducer(ns, "GID_"+prefix+"_s1_prod", "go-rd-s1-prod-"+prefix)
	if err != nil {
		check("S1 构造生产者", false, err.Error())
		return
	}
	defer p.shutdown()

	b := &box{}
	c, err := startConsumer(ns, consCfg{
		group: group, instance: "go-rd-s1-" + prefix,
		listener: &retryOnce{box: b, target: "retry-me"},
	}, topic)
	if err != nil {
		check("S1 构造/订阅/启动消费者", false, err.Error())
		return
	}
	defer c.Shutdown()

	for _, body := range []string{"retry-me", "normal-1"} {
		if err := p.send(topic, body); err != nil {
			check("S1 发送 "+body, false, err.Error())
			return
		}
	}

	// The first redelivery carries delay level 3 = 10s, and the %RETRY% route
	// only becomes visible on the next broker registration (<=30s) plus a
	// rebalance round, so the window has to cover both.
	waitUntil(60*time.Second, func() bool { return len(b.ofBody("retry-me")) >= 2 })

	retry := b.ofBody("retry-me")
	normal := b.ofBody("normal-1")
	check("S1 retry-me 被投递多次（>=2）", len(retry) >= 2, fmt.Sprintf("arrivals=%d", len(retry)))
	if len(retry) >= 2 {
		gap := retry[len(retry)-1].at.Sub(retry[0].at)
		check("S1 回投有延迟梯度（level3≈10s，>=8s）", gap >= 8*time.Second,
			fmt.Sprintf("gap=%.1fs", gap.Seconds()))
	} else {
		check("S1 回投有延迟梯度（level3≈10s，>=8s）", false, "不足两次投递")
	}
	redelivered := make([]rec, 0, 2)
	for _, r := range retry {
		if r.recon >= 1 {
			redelivered = append(redelivered, r)
		}
	}
	check("S1 二次投递带 RECONSUME_TIMES>=1", len(redelivered) >= 1,
		fmt.Sprintf("times=%s", reconList(retry)))
	ok := len(redelivered) >= 1
	for _, r := range redelivered {
		if r.topic != topic {
			ok = false
		}
	}
	check("S1 重投消息 topic 还原为业务 topic", ok, "listener 主题分布="+b.topicSummary())
	check("S1 正常消息只投一次", len(normal) == 1, fmt.Sprintf("arrivals=%d", len(normal)))
}

// ------------------------------------------------------------------ S2

// S2: maxReconsumeTimes=2 -> exactly 3 client deliveries (0/1/2) and then the
// broker dead-letters it (reconsumeTimes=3, RETRY_TOPIC kept).
func runS2(ns, prefix string, topics map[string]string) {
	fmt.Println("\n--- S2 死信终态：maxReconsumeTimes=2 ⇒ 3 次投递后进 %DLQ% ---")
	topic := prefix + "_Dlq"
	topics["s2"] = topic
	group := "GID_" + prefix + "_s2"
	dlqTopic := common.GetDLQTopic(group)
	topics["s2_retry"] = common.GetRetryTopic(group)
	topics["s2_dlq"] = dlqTopic
	if err := ensureTopic(ns, "go-rd-s2-"+prefix, topic, 1); err != nil {
		check("S2 建 topic", false, err.Error())
		return
	}
	p, err := startProducer(ns, "GID_"+prefix+"_s2_prod", "go-rd-s2-prod-"+prefix)
	if err != nil {
		check("S2 构造生产者", false, err.Error())
		return
	}
	defer p.shutdown()

	b := &box{}
	c, err := startConsumer(ns, consCfg{
		group: group, instance: "go-rd-s2-" + prefix,
		listener: &alwaysFail{box: b}, maxRecon: 2,
	}, topic)
	if err != nil {
		check("S2 构造/订阅/启动消费者", false, err.Error())
		return
	}
	if err := p.send(topic, "dlq-me"); err != nil {
		c.Shutdown()
		check("S2 发送 dlq-me", false, err.Error())
		return
	}
	// Delay levels 3 (10s) and 4 (30s) mean the third delivery lands ~40s after
	// the first; contention on a shared machine can push that out a lot, so the
	// window is wide and the timing is printed either way.
	waitUntil(150*time.Second, func() bool { return len(b.ofBody("dlq-me")) >= 3 })
	time.Sleep(15 * time.Second) // counter-proof window: no 4th delivery
	all := b.ofBody("dlq-me")
	times := reconList(all)
	seqOK := len(all) >= 3 && all[0].recon == 0 && all[1].recon == 1 && all[2].recon == 2
	check("S2 恰好投递 3 次且 RECONSUME_TIMES 为 0/1/2", seqOK, "times="+times)
	check("S2 用尽后不再投递（观察窗口内只有 3 次）", len(all) == 3,
		fmt.Sprintf("arrivals=%d times=%s", len(all), times))
	topicOK := len(all) >= 3
	for _, r := range all[:min(3, len(all))] {
		if r.topic != topic {
			topicOK = false
		}
	}
	check("S2 重投期间 topic 还原为业务 topic", topicOK, "listener 主题分布="+b.topicSummary())
	c.Shutdown()

	// The broker creates and registers %DLQ%<group> only when the first message
	// lands in it.
	check("S2 broker 自动创建并注册 %DLQ% 路由",
		waitTopicRoute(ns, "go-rd-s2-route-"+prefix, dlqTopic, 30*time.Second), "dlq="+dlqTopic)

	got, _, err := readTopicFromHead(ns, dlqTopic, "GID_"+prefix+"_s2probe", "go-rd-s2-probe-"+prefix, 25*time.Second)
	if err != nil {
		check("S2 读 %DLQ%（队首）", false, err.Error())
		return
	}
	check("S2 消息落在 %DLQ%<group>",
		len(got) >= 1 && got[0].body == "dlq-me",
		fmt.Sprintf("got=%s", recSummary(got)))
	// The broker stores reconsumeTimes+1 when it dead-letters, so 2 -> 3.
	check("S2 DLQ 消息 RECONSUME_TIMES=3（第 3 次回投转死信）",
		len(got) == 1 && got[0].recon == 3, fmt.Sprintf("got=%s", recSummary(got)))
	check("S2 DLQ 消息 topic 就是 %DLQ%<group>",
		len(got) == 1 && got[0].topic == dlqTopic, "listener 主题分布="+topicSummaryOf(got))
}

// ------------------------------------------------------------------ S3

// S3: the orderly retry path (a PLAIN send to %RETRY%) has its own ceiling and
// its own dead-letter route via the queue lock.
func runS3(ns, prefix string, topics map[string]string) {
	fmt.Println("\n--- S3 顺序毒消息：Suspend 到本地上限 → 顺序回投 → %DLQ% ---")
	topic := prefix + "_OrderlyDlq"
	topics["s3"] = topic
	group := "GID_" + prefix + "_s3"
	dlqTopic := common.GetDLQTopic(group)
	topics["s3_dlq"] = dlqTopic
	if err := ensureTopic(ns, "go-rd-s3-"+prefix, topic, 1); err != nil {
		check("S3 建 topic", false, err.Error())
		return
	}
	p, err := startProducer(ns, "GID_"+prefix+"_s3_prod", "go-rd-s3-prod-"+prefix)
	if err != nil {
		check("S3 构造生产者", false, err.Error())
		return
	}
	defer p.shutdown()

	b := &box{}
	var deliveries int64
	c, err := startConsumer(ns, consCfg{
		group: group, instance: "go-rd-s3-" + prefix,
		listener: &suspendOrderly{box: b, deliveries: &deliveries},
		batchMax: 1, maxRecon: 2, suspendMS: 50,
	}, topic)
	if err != nil {
		check("S3 构造/订阅/启动消费者", false, err.Error())
		return
	}
	if err := p.send(topic, "orderly-poison"); err != nil {
		c.Shutdown()
		check("S3 发送 orderly-poison", false, err.Error())
		return
	}
	waitUntil(60*time.Second, func() bool { return len(b.ofBody("orderly-poison")) >= 3 })
	time.Sleep(15 * time.Second)
	all := b.ofBody("orderly-poison")
	times := reconList(all)
	seqOK := len(all) >= 3 && all[0].recon == 0 && all[1].recon == 1 && all[2].recon == 2
	check("S3 顺序侧本地恰好投递 3 次（0/1/2）", seqOK, "times="+times)
	// After the third attempt the message is handed to the broker and the queue
	// offset advances, so a 4th delivery would mean the ceiling was not applied.
	check("S3 交 broker 后不再投递（观察窗口内只有 3 次）", len(all) == 3,
		fmt.Sprintf("arrivals=%d times=%s", len(all), times))
	c.Shutdown()

	check("S3 broker 自动创建并注册 %DLQ% 路由",
		waitTopicRoute(ns, "go-rd-s3-route-"+prefix, dlqTopic, 30*time.Second), "dlq="+dlqTopic)
	got, _, err := readTopicFromHead(ns, dlqTopic, "GID_"+prefix+"_s3probe", "go-rd-s3-probe-"+prefix, 25*time.Second)
	if err != nil {
		check("S3 读 %DLQ%（队首）", false, err.Error())
		return
	}
	check("S3 顺序回投的消息落在 %DLQ%（组仍持队列锁）",
		len(got) >= 1 && got[0].body == "orderly-poison",
		fmt.Sprintf("got=%s", recSummary(got)))
	// The orderly send-back writes msg.getReconsumeTimes() + 1 into
	// RECONSUME_TIME (ConsumeMessageOrderlyService:350), so a message that hit
	// the ceiling at 2 reaches the DLQ as 3. The +1 is easy to drop — the
	// concurrent path gets its +1 from the BROKER instead (see S2) — and the
	// result would look plausible in every other observable.
	check("S3 DLQ 消息 RECONSUME_TIMES=3（客户端侧 +1）",
		len(got) == 1 && got[0].recon == 3, fmt.Sprintf("got=%s", recSummary(got)))
	check("S3 DLQ 消息 topic 就是 %DLQ%<group>",
		len(got) == 1 && got[0].topic == dlqTopic, "listener 主题分布="+topicSummaryOf(got))
}

// ------------------------------------------------------------------ S4

// S4: ackIndex truncates the ack to the first message of a 3-message batch; the
// tail comes back through %RETRY% while the acked one never does, and the
// business offset still commits as a whole batch.
func runS4(ns, prefix string, topics map[string]string) {
	fmt.Println("\n--- S4 部分 ack（ackIndex=0）：尾巴 2 条回投、已 ack 的不回投 ---")
	topic := prefix + "_AckIndex"
	ctrlTopic := prefix + "_AckControl"
	topics["s4"] = topic
	topics["s4_ctrl"] = ctrlTopic
	group := "GID_" + prefix + "_s4"
	ctrlGroup := "GID_" + prefix + "_s4ctrl"
	topics["s4_retry"] = common.GetRetryTopic(group)
	topics["s4_ctrl_retry"] = common.GetRetryTopic(ctrlGroup)

	for _, t := range []string{topic, ctrlTopic} {
		if err := ensureTopic(ns, "go-rd-s4-"+prefix, t, 1); err != nil {
			check("S4 建 topic", false, err.Error())
			return
		}
	}
	p, err := startProducer(ns, "GID_"+prefix+"_s4_prod", "go-rd-s4-prod-"+prefix)
	if err != nil {
		check("S4 构造生产者", false, err.Error())
		return
	}
	defer p.shutdown()

	bodies := []string{"ack-0", "ack-1", "ack-2"}
	// Send BEFORE starting the consumer: with a 1-queue topic the very first
	// pull then carries all three and they form ONE batch, which is the premise
	// of the whole scenario.
	for _, t := range []string{topic, ctrlTopic} {
		for _, body := range bodies {
			if err := p.send(t, body); err != nil {
				check("S4 发送 "+body, false, err.Error())
				return
			}
		}
	}

	b := &box{}
	c, err := startConsumer(ns, consCfg{
		group: group, instance: "go-rd-s4-" + prefix,
		listener: &partialAckTo0{box: b}, batchMax: len(bodies),
	}, topic)
	if err != nil {
		check("S4 构造/订阅/启动消费者", false, err.Error())
		return
	}
	// The unacked tail goes back with delay level 3 = 10s.
	waitUntil(60*time.Second, func() bool {
		for _, body := range bodies[1:] {
			for _, r := range b.ofBody(body) {
				if r.recon >= 1 {
					return true
				}
			}
		}
		return false
	})
	waitUntil(30*time.Second, func() bool { return len(b.distinctBodies()) == len(bodies) })
	time.Sleep(3 * time.Second)
	c.Shutdown()

	first := b.ofBody(bodies[0])
	check("S4 已 ack 的首条整个窗口只投一次", len(first) == 1,
		fmt.Sprintf("arrivals=%d", len(first)))
	redelivered := 0
	tailDetail := make([]string, 0, 2)
	for _, body := range bodies[1:] {
		rs := b.ofBody(body)
		n := 0
		for _, r := range rs {
			if r.recon >= 1 {
				n++
			}
		}
		if n > 0 {
			redelivered++
		}
		tailDetail = append(tailDetail, fmt.Sprintf("%s=%s", body, reconList(rs)))
	}
	check("S4 未 ack 的尾巴 2 条经 %RETRY% 回投", redelivered == 2, strings.Join(tailDetail, " "))
	check("S4 3 条最终全部消费完", len(b.distinctBodies()) == len(bodies),
		fmt.Sprintf("bodies=%v", b.distinctBodies()))

	// The whole batch is still committed, ackIndex notwithstanding: the tail was
	// handed to the broker via send-back, so the offset is free to advance.
	offset, found := consumedOffset(ns, "go-rd-s4-off-"+prefix, group, topic)
	check("S4 业务队列位点仍整批提交到 3", found && offset == int64(len(bodies)),
		fmt.Sprintf("offset=%d found=%v", offset, found))

	// Control: the same three messages, no ackIndex manipulation -> no
	// redelivery at all. Without it the redelivery above could be blamed on the
	// harness rather than on the ack.
	cb := &box{}
	cc, err := startConsumer(ns, consCfg{
		group: ctrlGroup, instance: "go-rd-s4ctrl-" + prefix,
		listener: &collect{box: cb}, batchMax: len(bodies),
	}, ctrlTopic)
	if err != nil {
		check("S4 对照组构造/启动", false, err.Error())
		return
	}
	waitUntil(30*time.Second, func() bool { return len(cb.distinctBodies()) == len(bodies) })
	time.Sleep(12 * time.Second) // a redelivery would need level 3 = 10s to show up
	cc.Shutdown()
	ctrlOK := len(cb.distinctBodies()) == len(bodies) && len(cb.all()) == len(bodies)
	check("S4 对照组（不碰 ackIndex）一条都不回投", ctrlOK,
		fmt.Sprintf("deliveries=%d bodies=%v", len(cb.all()), cb.distinctBodies()))
}

// ------------------------------------------------------------------ formatting

func reconList(rs []rec) string {
	parts := make([]string, 0, len(rs))
	for _, r := range rs {
		parts = append(parts, fmt.Sprintf("%d@%.0fs", r.recon, r.at.Sub(firstAt(rs)).Seconds()))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func firstAt(rs []rec) time.Time {
	if len(rs) == 0 {
		return time.Time{}
	}
	t := rs[0].at
	for _, r := range rs {
		if r.at.Before(t) {
			t = r.at
		}
	}
	return t
}

func recSummary(rs []rec) string {
	parts := make([]string, 0, len(rs))
	for _, r := range rs {
		parts = append(parts, fmt.Sprintf("(%s,recon=%d)", r.body, r.recon))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func topicSummaryOf(rs []rec) string {
	set := map[string]int{}
	for _, r := range rs {
		set[r.topic]++
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, set[k]))
	}
	return strings.Join(parts, " ")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func orAll(s string) string {
	if strings.TrimSpace(s) == "" {
		return "all"
	}
	return s
}
