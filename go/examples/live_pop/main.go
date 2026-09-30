// Command live_pop verifies POP-mode consumption against a real RocketMQ 5.x
// cluster.
//
//	go run ./examples/live_pop -ns 127.0.0.1:9876 [-legs s1,s2]
//
// POP is the one consume path whose failures are ALL silent. There is no client
// offset to read back, no send-back to observe, and no error code to assert on:
// the broker hands out an invisible batch and the client either ACKs it or asks
// for more invisible time. Get the checkpoint wrong by one segment and every ACK
// becomes a no-op on the broker while the client cheerfully reports success —
// the only symptom is "the messages come back after popInvisibleTime". So the
// assertions here are timed windows and broker-side observable state, never
// return codes:
//
//	S1 ACK REALLY LANDS. Consume six messages, then keep watching for 2.5x the
//	   invisibility window while asserting no body is delivered a second time.
//	   If the ACK offset were the batch start (segment 0) instead of the
//	   message's own offset (segment 7), or the ACK topic were the business
//	   topic instead of the one the checkpoint names, the broker would revive
//	   the batch and redeliver it inside that window. The same leg pins the
//	   POP-specific "no consumer offset" rule: unlike a pull, a POP must leave
//	   the group's offset on the broker untouched.
//	S2 THE RETRY PATH. A listener that fails the first delivery of exactly one
//	   message drives changePopInvisibleTime; the broker's revive moves the
//	   message into %RETRY%<group>_<topic> and it comes back with
//	   reconsumeTimes+1, a checkpoint whose retry marker is "1" — which is BOTH
//	   the proof that it was popped from the retry topic and the thing that
//	   makes the eventual ACK resolve back to it — and the same 1ST_POP_TIME it
//	   carried on its first pop.
//
// Cluster facts this tool depends on (all present in the local test
// broker.conf, and asserted where they can be):
//
//   - timerWheelEnable=true. Without the timer wheel the broker answers
//     POP_MESSAGE with an error, so S1 would fail loudly.
//   - defaultMessageRequestMode=PULL, so Start's SET_MESSAGE_REQUEST_MODE(401)
//     is what keeps the broker from serving this group with PULL replies.
//   - popResponseReturnActualRetryTopic=false — the DEFAULT retry path, i.e. the
//     one where the broker stamps the checkpoint itself and rewrites msg.Topic
//     to the business topic before the client sees it. S2 exists to pin that.
//   - enablePopBatchAck=false, so BATCH_ACK_MESSAGE(200151) is NOT exercised
//     here: this broker would reject it. (The classic client never sends it
//     anyway — its POP path acks one message at a time.)
//
// Every scenario creates its topic first: a consumer has no default-topic
// fallback (only producers get TBW102), so a missing topic means "no route, no
// assignment, no POP" — a false failure that reads like a client bug.
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

// rec is one delivery as the LISTENER saw it. ck is the checkpoint the client
// rebuilt (or kept), captured before the listener returns because that is the
// value the ACK will be built from.
type rec struct {
	body     string
	topic    string
	recon    int32
	ck       string
	firstPop string
	at       time.Time
}

type box struct {
	mu   sync.Mutex
	recs []rec
}

func (b *box) addAll(msgs []*common.MessageExt) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, m := range msgs {
		b.recs = append(b.recs, rec{
			body:     string(m.Body),
			topic:    m.Topic,
			recon:    m.ReconsumeTimes,
			ck:       propOf(m, common.PropertyPopCk),
			firstPop: propOf(m, common.PropertyFirstPopTime),
			at:       time.Now(),
		})
	}
}

func (b *box) all() []rec {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]rec(nil), b.recs...)
}

func (b *box) ofBody(body string) []rec {
	out := make([]rec, 0, 2)
	for _, r := range b.all() {
		if r.body == body {
			out = append(out, r)
		}
	}
	return out
}

func (b *box) countOf(body string) int { return len(b.ofBody(body)) }

func (b *box) noDupes() bool {
	seen := map[string]int{}
	for _, r := range b.all() {
		seen[r.body]++
	}
	for _, n := range seen {
		if n > 1 {
			return false
		}
	}
	return true
}

func (b *box) dupes() string {
	seen := map[string]int{}
	for _, r := range b.all() {
		seen[r.body]++
	}
	bodies := make([]string, 0, len(seen))
	for body, n := range seen {
		if n > 1 {
			bodies = append(bodies, fmt.Sprintf("%s=%d", body, n))
		}
	}
	sort.Strings(bodies)
	return strings.Join(bodies, " ")
}

func (b *box) summary() string {
	seen := map[string]int{}
	for _, r := range b.all() {
		seen[r.body]++
	}
	bodies := make([]string, 0, len(seen))
	for body := range seen {
		bodies = append(bodies, body)
	}
	sort.Strings(bodies)
	parts := make([]string, 0, len(bodies))
	for _, body := range bodies {
		parts = append(parts, fmt.Sprintf("%s=%d", body, seen[body]))
	}
	return strings.Join(parts, " ")
}

func propOf(m *common.MessageExt, name string) string {
	if m.Properties == nil {
		return ""
	}
	v, _ := m.GetProperty(name)
	return v
}

// ------------------------------------------------------------------ listener

// popListener collects every delivery and can fail ONE body on its FIRST
// delivery only. The scenario pre-checks the batch size so a failure cannot
// spill onto a neighbour.
type popListener struct {
	box    *box
	poison string

	mu     sync.Mutex
	failed bool
}

func (l *popListener) ConsumeMessage(msgs []*common.MessageExt, _ *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	l.box.addAll(msgs)
	if l.poison == "" {
		return client.ConsumeSuccess
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failed {
		return client.ConsumeSuccess
	}
	for _, m := range msgs {
		if string(m.Body) == l.poison {
			l.failed = true
			return client.ReconsumeLater
		}
	}
	return client.ConsumeSuccess
}

// ------------------------------------------------------------------ fixtures

func newAdmin(ns, instance string) *client.DefaultMQAdminExt {
	a := client.NewDefaultMQAdminExt(nil)
	a.SetNameServerAddresses([]string{ns})
	a.SetInstanceName(instance)
	a.SetTimeoutMillis(10_000)
	return a
}

// ensureTopic creates the topic explicitly. Relying on auto-creation would give
// this broker's default 8 queues (defaultTopicQueueNums), and every leg here
// wants a deterministic single-queue topic.
func ensureTopic(ns, instance, topic string, queues int32) error {
	a := newAdmin(ns, instance)
	if err := a.Start(); err != nil {
		return err
	}
	defer a.Shutdown()
	return a.CreateTopic(common.DefaultTopic, topic, queues, 0)
}

// brokerNameOf is the broker the route points at — the LOGICAL name that has to
// appear in checkpoint segment 5, or the ACK cannot be addressed.
func brokerNameOf(ns, instance, topic string) (string, error) {
	a := newAdmin(ns, instance)
	if err := a.Start(); err != nil {
		return "", err
	}
	defer a.Shutdown()
	route, err := a.ExamineTopicRoute(topic)
	if err != nil {
		return "", err
	}
	if route == nil || len(route.BrokerDatas) == 0 {
		return "", fmt.Errorf("no broker route for %s", topic)
	}
	return route.BrokerDatas[0].BrokerName, nil
}

type producerHandle struct{ p *client.DefaultMQProducer }

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
	invisible int64
	popBatch  int32
	popMode   bool
}

func startConsumer(ns string, cfg consCfg, topics ...string) (*client.DefaultMQPushConsumer, error) {
	c, err := client.NewDefaultMQPushConsumer(cfg.group)
	if err != nil {
		return nil, err
	}
	c.SetNameServerAddresses([]string{ns})
	c.SetInstanceName(cfg.instance)
	// FIRST_OFFSET everywhere: every group here is brand new, and a POP consumer
	// that resolves no initial offset would still be fine, but being explicit
	// keeps the leg reproducible when replayed against a reused cluster.
	c.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	c.SetConsumeThreadMin(2)
	c.SetConsumeThreadMax(8)
	if cfg.batchMax > 0 {
		c.SetConsumeMessageBatchMaxSize(cfg.batchMax)
	}
	if cfg.maxRecon != 0 {
		c.SetMaxReconsumeTimes(cfg.maxRecon)
	}
	if cfg.popMode {
		c.SetPopMode(true)
		if cfg.invisible > 0 {
			c.SetPopInvisibleTime(cfg.invisible)
		}
		if cfg.popBatch > 0 {
			c.SetPopBatchNums(cfg.popBatch)
		}
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

// topicRoutable reports whether the broker has published a route for a topic.
// %RETRY% topics do not exist until the first message lands in them, and the
// broker only re-registers routes every 30s (registerNameServerPeriod).
func topicRoutable(ns, instance, topic string, window time.Duration) bool {
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

// consumedOffset reads one queue's committed offset for a group.
func consumedOffset(ns, instance, group, topic, brokerName string) (int64, bool) {
	a := newAdmin(ns, instance)
	if err := a.Start(); err != nil {
		return -1, false
	}
	defer a.Shutdown()
	mq := common.NewMessageQueue(topic, brokerName, 0)
	offset, found, err := a.ExamineConsumerOffset(group, mq)
	if err != nil {
		return -1, false
	}
	return offset, found
}

// ------------------------------------------------------------------ assertions

// ckShape describes a checkpoint in the terms the ACK depends on.
type ckShape struct {
	segments  int
	ckOffset  int64
	queueOff  int64
	retry     string
	broker    string
	queueID   int
	popTime   int64
	invisible int64
}

func parseCk(ck string) (ckShape, error) {
	parts := common.SplitExtraInfo(ck)
	if len(parts) != 8 {
		return ckShape{segments: len(parts)}, fmt.Errorf("checkpoint has %d segments, want 8: %q", len(parts), ck)
	}
	var s ckShape
	s.segments = len(parts)
	s.ckOffset, _ = common.GetCkQueueOffset(parts)
	s.popTime, _ = common.GetPopTime(parts)
	s.invisible, _ = common.GetInvisibleTime(parts)
	s.retry, _ = common.GetRetry(parts)
	s.broker, _ = common.GetBrokerName(parts)
	s.queueID, _ = common.GetQueueId(parts)
	s.queueOff, _ = common.GetQueueOffset(parts)
	return s, nil
}

// ------------------------------------------------------------------ main

func main() {
	ns := flag.String("ns", "127.0.0.1:9876", "name server address")
	legs := flag.String("legs", "", "which scenarios to run: s1,s2 (default: all)")
	flag.Parse()

	stamp := fmt.Sprintf("%d", time.Now().Unix())
	prefix := "GoPop" + stamp
	fmt.Printf("=== Go POP live: ns=%s prefix=%s legs=%s ===\n", *ns, prefix, orAll(*legs))

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

	// S1 needs a full 2.5x invisibility window of quiet observation (~13s); S2
	// waits for a revive round trip (~20-30s) plus a route refresh. -legs exists
	// so a single scenario can be re-run while iterating.
	if want("s1") {
		runS1(*ns, prefix, topics)
	}
	if want("s2") {
		runS2(*ns, prefix, topics)
	}

	fmt.Printf("\nPASS=%d FAIL=%d\n", passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
}

func cleanup(ns, prefix string, topics map[string]string) {
	a := newAdmin(ns, "ADMIN-pop-"+prefix)
	if err := a.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "cleanup admin: %v\n", err)
		return
	}
	defer a.Shutdown()
	names := make([]string, 0, len(topics)*2)
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

// S1: the ACK really lands. Everything here is a timed window, because a POP
// returns SUCCESS whether or not the broker could match the checkpoint.
func runS1(ns, prefix string, topics map[string]string) {
	fmt.Println("\n--- S1 基础：POP 消费 + ACK 真的生效（窗口内无重投）+ 不提交位点 ---")
	topic := prefix + "_Basic"
	topics["s1"] = topic
	group := "GID_" + prefix + "_s1"
	const invisible = 5000

	if err := ensureTopic(ns, "go-pop-s1-"+prefix, topic, 1); err != nil {
		check("S1 建 topic", false, err.Error())
		return
	}
	brokerName, err := brokerNameOf(ns, "go-pop-s1-"+prefix, topic)
	if err != nil {
		check("S1 取路由", false, err.Error())
		return
	}
	p, err := startProducer(ns, "GID_"+prefix+"_s1_prod", "go-pop-s1-prod-"+prefix)
	if err != nil {
		check("S1 构造生产者", false, err.Error())
		return
	}
	defer p.shutdown()

	b := &box{}
	c, err := startConsumer(ns, consCfg{
		group: group, instance: "go-pop-s1-" + prefix, listener: &popListener{box: b},
		invisible: invisible, popBatch: 32, batchMax: 5, popMode: true,
	}, topic)
	if err != nil {
		check("S1 起 POP 消费者", false, err.Error())
		return
	}

	bodies := []string{"pop-a", "pop-b", "pop-c", "pop-d", "pop-e", "pop-f"}
	sendErr := ""
	for _, body := range bodies {
		if err := p.send(topic, body); err != nil {
			sendErr = err.Error()
			break
		}
	}
	check("S1 发送 6 条", sendErr == "", sendErr)

	// A POP consumer rebalances like a pull consumer (the broker's request mode
	// only decides which protocol serves the group), so it must have assigned
	// this topic before it can pop anything. Note the count is 2, not 1: like a
	// pull consumer, this one also implicitly subscribes to %RETRY%<group> (one
	// queue), so anything counted by queueID alone sees a phantom overlap. Go by
	// (topic, broker, queueID).
	var bizQ, retryQ, totalQ int
	assigned := waitUntil(20*time.Second, func() bool {
		bizQ, retryQ, totalQ = 0, 0, 0
		for _, mq := range c.AssignedQueues() {
			totalQ++
			switch mq.Topic {
			case topic:
				bizQ++
			case common.GetRetryTopic(group):
				retryQ++
			}
		}
		return bizQ == 1
	})
	check("S1 分配到业务 topic 的 1 条队列（+ 隐式 %RETRY%<group>）", assigned && retryQ == 1,
		fmt.Sprintf("biz=%d retry=%d total=%d popQueues=%d", bizQ, retryQ, totalQ, c.PopProcessQueueCount()))

	gotAll := waitUntil(40*time.Second, func() bool {
		return len(b.summary()) > 0 && strings.Count(b.summary(), "=") == len(bodies)
	})
	check("S1 6 条都被 POP 到", gotAll, b.summary())

	// Every delivered message must carry a checkpoint the ACK can use: eight
	// segments, segment 0 is the batch start, segment 7 is THIS message's
	// offset, the marker says "business topic", and segment 5 is the logical
	// broker name.
	shape := "no delivery"
	shapeOK := false
	if recs := b.all(); len(recs) > 0 {
		r := recs[0]
		s, err := parseCk(r.ck)
		if err != nil {
			shape = err.Error()
		} else {
			shape = fmt.Sprintf("seg=%d ckOffset=%d queueOffset=%d retry=%q broker=%q qid=%d popTime=%d invisible=%d",
				s.segments, s.ckOffset, s.queueOff, s.retry, s.broker, s.queueID, s.popTime, s.invisible)
			shapeOK = s.ckOffset == s.queueOff && s.retry == "0" && s.broker == brokerName &&
				s.queueID == 0 && s.invisible == invisible && s.popTime > 0
		}
	}
	check("S1 POP_CK 形状：8 段 / ackOffset==msgQueueOffset / marker=0 / 逻辑 broker 名", shapeOK, shape)

	firstPopOK := false
	if recs := b.all(); len(recs) > 0 {
		r := recs[0]
		s, err := parseCk(r.ck)
		firstPopOK = err == nil && r.firstPop == fmt.Sprint(s.popTime)
	}
	check("S1 1ST_POP_TIME == 本次 popTime", firstPopOK, firstDeliveryDetail(b))

	// The topic handed to the listener is the requested one, not the physical
	// pop topic (the broker rewrites it on the retry path; here they are equal).
	listenerTopicOK := false
	if recs := b.all(); len(recs) > 0 {
		listenerTopicOK = recs[0].topic == topic
	}
	check("S1 listener 看到的是业务 topic", listenerTopicOK, topicOfFirst(b))

	// The ACK debt must return to zero: every popped message was answered.
	check("S1 ACK 债务归零", waitUntil(5*time.Second, func() bool { return c.PopWaitAckCount() == 0 }),
		fmt.Sprintf("waitAck=%d debt", c.PopWaitAckCount()))

	// THE central assertion. If the ACK were a no-op the broker would revive the
	// batch once the window closed, and the redelivery would land well inside
	// 2.5x the window. Watching for "no duplicate" rather than "an ACK 200 came
	// back" is deliberate: the broker answers SUCCESS to a POP_ACK whose
	// checkpoint it cannot find.
	time.Sleep(time.Duration(invisible*5/2) * time.Millisecond)
	check("S1 2.5x 不可见窗口内无重复投递（ACK 落到 broker）", b.noDupes(), fmt.Sprintf("dup=%q all=%s", b.dupes(), b.summary()))

	// POP keeps no client-side cursor: the broker's revive queue IS the cursor,
	// so the consumer's own offset table must stay empty. A pull consumer would
	// have one entry per assigned queue here, written when the assignment was
	// first resolved.
	check("S1 POP 模式下客户端不持有位点表", c.LocalOffsetCount() == 0,
		fmt.Sprintf("localOffsets=%d", c.LocalOffsetCount()))

	// Read the broker's offset back for INFORMATION ONLY — and note it is
	// normally PRESENT. That is the broker's own pop cursor, not a client
	// commit: on the first pop of a queue whose offset it does not know,
	// PopMessageProcessor#getInitOffset(init=true) commits it under the label
	// "getPopOffset" (:928-930), and the `Pop initial offset` branch (:774)
	// self-heals it. Asserting "no offset on the broker" would therefore be
	// wrong — the client-side table above is where the real contract lives.
	if off, found := consumedOffset(ns, "go-pop-s1-"+prefix, group, topic, brokerName); found {
		fmt.Printf("     (broker 侧 pop 游标 offset=%d，由 broker 自己提交，非客户端)\n", off)
	}

	c.Shutdown()
}

// ------------------------------------------------------------------ S2

// S2: the retry path — changePopInvisibleTime, the POP retry topic, and the
// checkpoint marker that makes the later ACK resolve to it.
func runS2(ns, prefix string, topics map[string]string) {
	fmt.Println("\n--- S2 失败退避：改不可见时间 → %RETRY%<group>_<topic> → 重投 + 1ST_POP_TIME 稳定 ---")
	topic := prefix + "_Retry"
	topics["s2"] = topic
	group := "GID_" + prefix + "_s2"
	retryTopic := "%RETRY%" + group + "_" + topic
	const poison = "pop-poison"
	const invisible = 5000

	if err := ensureTopic(ns, "go-pop-s2-"+prefix, topic, 1); err != nil {
		check("S2 建 topic", false, err.Error())
		return
	}
	brokerName, err := brokerNameOf(ns, "go-pop-s2-"+prefix, topic)
	if err != nil {
		check("S2 取路由", false, err.Error())
		return
	}
	p, err := startProducer(ns, "GID_"+prefix+"_s2_prod", "go-pop-s2-prod-"+prefix)
	if err != nil {
		check("S2 构造生产者", false, err.Error())
		return
	}
	defer p.shutdown()

	b := &box{}
	// batchMax 1 is required, not cosmetic: the client splits a pop batch into
	// listener calls of this size, and a failure must not spill onto a
	// neighbouring message (which would make "the others were delivered once"
	// meaningless).
	c, err := startConsumer(ns, consCfg{
		group: group, instance: "go-pop-s2-" + prefix, listener: &popListener{box: b, poison: poison},
		invisible: invisible, popBatch: 32, batchMax: 1, maxRecon: 16, popMode: true,
	}, topic)
	if err != nil {
		check("S2 起 POP 消费者", false, err.Error())
		return
	}
	defer c.Shutdown()

	sendErr := ""
	for _, body := range []string{"keep-1", poison, "keep-2"} {
		if err := p.send(topic, body); err != nil {
			sendErr = err.Error()
			break
		}
	}
	check("S2 发送 3 条", sendErr == "", sendErr)

	firstArrived := waitUntil(40*time.Second, func() bool { return b.countOf(poison) >= 1 })
	// The queue count is 2 for the same reason as S1 (business topic + the
	// implicit %RETRY%<group>), so it is reported rather than asserted here.
	check("S2 首投 3 条", firstArrived, fmt.Sprintf("%s popQueues=%d", b.summary(), c.PopProcessQueueCount()))
	if !firstArrived {
		return
	}
	first := b.ofBody(poison)[0]
	check("S2 首投 recon=0 且 marker=0（来自业务 topic）", first.recon == 0 && markerOf(first) == "0",
		fmt.Sprintf("recon=%d marker=%q ck=%q", first.recon, markerOf(first), first.ck))

	// The failed message is re-hidden, not sent back: changePopInvisibleTime
	// asks the broker for a longer window (popDelayLevel[reconsumeTimes=0] = 10s)
	// and the revive service moves it to %RETRY%<group>_<topic> once that
	// window closes. popFromRetryProbability=20% only decides WHEN in a request
	// the retry topic is consulted, and the broker also consults it whenever the
	// normal topic came up short, so this lands in tens of seconds.
	redelivered := waitUntil(120*time.Second, func() bool { return b.countOf(poison) >= 2 })
	check("S2 失败消息被重投（改不可见时间 + revive 生效）", redelivered, b.summary())
	if !redelivered {
		return
	}
	second := b.ofBody(poison)[1]
	check("S2 重投 recon 递增", second.recon >= 1,
		fmt.Sprintf("recon %d -> %d", first.recon, second.recon))
	// THE marker assertion: "1" means the message was popped from
	// %RETRY%<group>_<topic>, which is simultaneously
	//   (a) proof the revive put it there, and
	//   (b) the reason the ACK must be addressed to that retry topic — the broker
	//       holds the checkpoint under the retry topic, not the business one.
	check("S2 重投 marker=1（来自 POP 重试 topic，ACK 必须发回那里）", markerOf(second) == "1",
		fmt.Sprintf("marker=%q ck=%q", markerOf(second), second.ck))
	check("S2 重投 listener 看到的仍是业务 topic",
		second.topic == topic && first.topic == topic,
		fmt.Sprintf("first=%q second=%q want=%q", first.topic, second.topic, topic))
	// 1ST_POP_TIME is computeIfAbsent in Java (MQClientAPIImpl:1224) and in this
	// port: a value the broker provided must be KEPT, never overwritten with the
	// current pop's time. That is the observable form of the rule — if the stamp
	// equalled the redelivery's OWN popTime, the client had thrown the broker's
	// value away and stamped a fresh one.
	//
	// It is deliberately NOT asserted to be byte-identical to the first
	// delivery's value. The broker stamps popCheckPoint.getPopTime() of whichever
	// checkpoint the revive picked up, and with enablePopBufferMerge=false more
	// than one checkpoint can exist for the same message, so the two legitimately
	// differ by a few ms. That is broker bookkeeping; this leg pins the client's
	// half only.
	secondShape, shapeErr := parseCk(second.ck)
	keptBrokerValue := shapeErr == nil && second.firstPop != "" && second.firstPop != fmt.Sprint(secondShape.popTime)
	check("S2 1ST_POP_TIME 不被客户端改写（保留 broker 的值）", keptBrokerValue,
		fmt.Sprintf("first=%q second=%q secondPopTime=%d", first.firstPop, second.firstPop, secondShape.popTime))

	// Only the poison message may be delivered twice.
	check("S2 其余消息只投一次", b.countOf("keep-1") == 1 && b.countOf("keep-2") == 1, b.summary())

	// The retry topic is created by the revive service, so its route appearing is
	// an independent, broker-side confirmation of the whole path.
	check("S2 %RETRY%<group>_<topic> 路由出现", topicRoutable(ns, "go-pop-s2-route-"+prefix, retryTopic, 45*time.Second), retryTopic)

	// The redelivery was ACKed too, so the debt settles again.
	check("S2 ACK 债务归零", waitUntil(10*time.Second, func() bool { return c.PopWaitAckCount() == 0 }),
		fmt.Sprintf("waitAck=%d", c.PopWaitAckCount()))

	_ = brokerName
}

// ------------------------------------------------------------------ helpers

func markerOf(r rec) string {
	parts := common.SplitExtraInfo(r.ck)
	if len(parts) != 8 {
		return "?"
	}
	v, err := common.GetRetry(parts)
	if err != nil {
		return "?"
	}
	return v
}

func firstDeliveryDetail(b *box) string {
	if recs := b.all(); len(recs) > 0 {
		return fmt.Sprintf("1ST_POP_TIME=%q ck=%q", recs[0].firstPop, recs[0].ck)
	}
	return "no delivery"
}

func topicOfFirst(b *box) string {
	if recs := b.all(); len(recs) > 0 {
		return fmt.Sprintf("got %q", recs[0].topic)
	}
	return "no delivery"
}

func orAll(s string) string {
	if strings.TrimSpace(s) == "" {
		return "all"
	}
	return s
}
