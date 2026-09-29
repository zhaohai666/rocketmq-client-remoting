// live_consumer is the Go push-consumer end-to-end smoke test against a real
// RocketMQ 5.x cluster.
//
//	go run ./examples/live_consumer -ns 127.0.0.1:9876 \
//	    -topic T -group G -expect 12 -orderly-topic T_ord -orderly-expect 6
//
// The messages are produced by the PYTHON client (go_consumer_feed_check.py),
// so the check is genuinely cross-language: Go's own producer already proves
// the wire format by being read back by Python, and this direction proves the
// Go consumer can decode and ack what another implementation wrote.
//
// Scenarios:
//
//	S1  CONSUME_FROM_FIRST_OFFSET drains everything the Python client produced;
//	    every body arrives exactly once and in per-queue offset order.
//	S2  the committed offset is on the broker: a SECOND consumer in the same
//	    group receives nothing (the group already consumed it).
//	S3  tag filtering: subscribing to TagA drops the TagB messages.
//	S4  ORDERLY: a one-queue topic is consumed strictly in order.
//	S5  two consumers in one group split the queues and neither pulls outside
//	    its slice.
//	S6  graceful shutdown unregisters the client from the broker.
//
// TWO TRAPS, both of which produced bogus "FAIL"s before this file was fixed:
//
//  1. The consumer IMPLICITLY subscribes to %RETRY%<group> (Java
//     copySubscription, and this port matches it). So "the queues I was
//     assigned" = the main topic's slice PLUS the retry topic's slice, and the
//     retry topic has ONE queue. Any bookkeeping keyed on queueID alone will
//     see main#0 and retry#0 as a bogus "two consumers own the same queue",
//     and the per-consumer totals come out as e.g. 3/2 rather than 2/2. Every
//     count below is therefore per (topic, queueID).
//  2. The queue count of the topic is NOT 4 by assumption — the feeder creates
//     it explicitly, and these checks read the real count back through
//     SubscribeQueuesOf rather than hardcoding a number.
//
// Every check prints PASS/FAIL and the process exits non-zero if any failed.
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

func check(name string, ok bool, detail ...string) {
	d := ""
	if len(detail) > 0 {
		d = detail[0]
	}
	mu.Lock()
	defer mu.Unlock()
	if ok {
		passCount++
		fmt.Printf("PASS  %s%s\n", name, suffix(d))
		return
	}
	failCount++
	fmt.Printf("FAIL  %s%s\n", name, suffix(d))
}

func suffix(detail string) string {
	if detail == "" {
		return ""
	}
	return "  [" + detail + "]"
}

func main() {
	ns := flag.String("ns", "127.0.0.1:9876", "name server address")
	topic := flag.String("topic", "", "topic the Python feeder filled")
	group := flag.String("group", "", "consumer group")
	expect := flag.Int("expect", 12, "how many messages the feeder produced")
	orderlyTopic := flag.String("orderly-topic", "", "single-queue topic for the orderly scenario")
	orderlyExpect := flag.Int("orderly-expect", 0, "how many messages the feeder produced there")
	flag.Parse()
	if *topic == "" || *group == "" {
		fmt.Fprintln(os.Stderr, "usage: live_consumer -topic T -group G [-ns addr] [-expect N] [-orderly-topic T2 -orderly-expect N]")
		os.Exit(2)
	}

	fmt.Printf("=== Go push consumer live: topic=%s group=%s expect=%d ===\n", *topic, *group, *expect)

	runS1(*ns, *topic, *group, *expect)
	runS2(*ns, *topic, *group, *expect)
	runS3(*ns, *topic, *group+"_tag", *expect)
	runS4(*ns, *orderlyTopic, *group+"_orderly", *orderlyExpect)
	runS5(*ns, *topic, *group+"_split")
	runS6(*ns, *topic, *group+"_unreg")

	fmt.Printf("\nPASS=%d FAIL=%d\n", passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
}

// ---------------------------------------------------------------- collector

// record is one delivered message, keyed by (topic, queueID) on the way in so
// that the implicit %RETRY% topic can never be confused with the real one.
type record struct {
	topic   string
	queueID int32
	offset  int64
	body    string
}

// collector records every delivery and can script a status.
type collector struct {
	mu      sync.Mutex
	recs    []record
	status  client.ConsumeConcurrentlyStatus
	onMsg   func(batch []*common.MessageExt)
	byTopic map[string]int
}

func newCollector() *collector {
	return &collector{
		byTopic: map[string]int{},
		status:  client.ConsumeSuccess,
	}
}

func (c *collector) ConsumeMessage(msgs []*common.MessageExt, ctx *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	c.mu.Lock()
	for _, m := range msgs {
		c.recs = append(c.recs, record{
			topic:   m.Topic,
			queueID: m.QueueID,
			offset:  m.QueueOffset,
			body:    string(m.Body),
		})
		c.byTopic[m.Topic]++
	}
	status := c.status
	hook := c.onMsg
	c.mu.Unlock()
	if hook != nil {
		hook(msgs)
	}
	return status
}

// count is how many messages were delivered for one topic.
func (c *collector) count(topic string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byTopic[topic]
}

func (c *collector) snapshot(topic string) []record {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]record, 0, len(c.recs))
	for _, r := range c.recs {
		if topic == "" || r.topic == topic {
			out = append(out, r)
		}
	}
	return out
}

func (c *collector) setStatus(s client.ConsumeConcurrentlyStatus) {
	c.mu.Lock()
	c.status = s
	c.mu.Unlock()
}

// ---------------------------------------------------------------- helpers

// waitFor polls until cond is true or the deadline passes.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return cond()
}

// newConsumer builds a consumer with a fresh instance name so repeated runs in
// one group do not collide on a shared clientId.
func newConsumer(ns, group string, listener any, opts ...func(*client.DefaultMQPushConsumer)) (*client.DefaultMQPushConsumer, error) {
	c, err := client.NewDefaultMQPushConsumer(group)
	if err != nil {
		return nil, err
	}
	inst := fmt.Sprintf("go-live-%s-%d", strings.ReplaceAll(group, "/", "_"), time.Now().UnixNano())
	c.SetNameServerAddresses([]string{ns})
	c.SetInstanceName(inst)
	c.SetConsumeThreadMin(2)
	c.SetConsumeThreadMax(8)
	if err := c.SetMessageListener(listener); err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// startSubscribed constructs, subscribes and starts in one go.
func startSubscribed(ns, group, topic, expr string, listener any,
	opts ...func(*client.DefaultMQPushConsumer)) (*client.DefaultMQPushConsumer, error) {
	c, err := newConsumer(ns, group, listener, opts...)
	if err != nil {
		return nil, err
	}
	if err := c.Subscribe(topic, expr); err != nil {
		return nil, err
	}
	if err := c.Start(); err != nil {
		return nil, err
	}
	return c, nil
}

// mqID renders a queue uniquely: 'brokerName:queueID'.
func mqID(mq common.MessageQueue) string { return fmt.Sprintf("%s:%d", mq.BrokerName, mq.QueueID) }

// ownedOf filters an assignment down to one topic, sorted by queue id.
func ownedOf(c *client.DefaultMQPushConsumer, topic string) []common.MessageQueue {
	var out []common.MessageQueue
	for _, mq := range c.AssignedQueues() {
		if mq.Topic == topic {
			out = append(out, mq)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].QueueID < out[j].QueueID })
	return out
}

func queueIDs(mqs []common.MessageQueue) string {
	ids := make([]int, 0, len(mqs))
	for _, mq := range mqs {
		ids = append(ids, int(mq.QueueID))
	}
	return fmt.Sprint(ids)
}

// allTopics lists the topics this consumer subscribed to — useful when a count
// looks wrong, because the %RETRY% topic is in there too.
func allTopics(c *client.DefaultMQPushConsumer) string {
	subs := c.Subscriptions()
	names := make([]string, 0, len(subs))
	for _, s := range subs {
		names = append(names, s.Topic)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// ---------------------------------------------------------------- scenarios

// S1: CONSUME_FROM_FIRST_OFFSET drains everything the Python client produced;
// every body arrives exactly once and in per-queue offset order.
func runS1(ns, topic, group string, expect int) {
	coll := newCollector()
	c, err := startSubscribed(ns, group, topic, "*", coll, func(c *client.DefaultMQPushConsumer) {
		c.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	})
	if err != nil {
		check("S1 构造/订阅/启动", false, err.Error())
		return
	}
	defer c.Shutdown()
	fmt.Printf("      S1 subscribed topics: %s\n", allTopics(c))

	ok := waitFor(90*time.Second, func() bool { return coll.count(topic) >= expect })
	check(fmt.Sprintf("S1 收全 %d 条（Python 生产）", expect), ok,
		fmt.Sprintf("got=%d route=%s assigned=%s", coll.count(topic),
			queueIDs(c.SubscribeQueuesOf(topic)), queueIDs(ownedOf(c, topic))))

	// Per-queue offset bookkeeping, keyed by (topic,queueId): the implicit
	// %RETRY% topic restarts at offset 0 and would break a flat per-queue list.
	//
	// The assertion is "each queue's offsets form exactly 0..n-1", NOT "they
	// arrived in ascending order". A concurrent listener runs on several threads
	// (consumeThreadMin..Max) and two batches of the SAME queue can be dispatched
	// to two of them at once, so arrival order is not monotonic — exactly as in
	// Java's ConsumeMessageConcurrentlyService. What must hold is that no offset
	// is skipped and none is delivered twice.
	byQueue := map[string][]int64{}
	for _, r := range coll.snapshot(topic) {
		byQueue[fmt.Sprintf("q%d", r.queueID)] = append(byQueue[fmt.Sprintf("q%d", r.queueID)], r.offset)
	}
	gapless := len(byQueue) > 0
	detail := ""
	for key, offs := range byQueue {
		sort.Slice(offs, func(i, j int) bool { return offs[i] < offs[j] })
		for i, off := range offs {
			if off != int64(i) {
				gapless = false
				detail = fmt.Sprintf("%s has %v (want 0..%d)", key, offs, len(offs)-1)
				break
			}
		}
		if !gapless {
			break
		}
	}
	check("S1 每个队列内 queueOffset 连续无缺无重（0..n-1）", gapless, detail)

	// Exactly once: no duplicates by body.
	seen := map[string]int{}
	for _, r := range coll.snapshot(topic) {
		seen[r.body]++
	}
	dupes := 0
	for _, n := range seen {
		if n > 1 {
			dupes++
		}
	}
	check("S1 无重复投递", dupes == 0, fmt.Sprintf("distinct=%d duplicate bodies=%d", len(seen), dupes))

	// Nothing may arrive on the retry topic.
	check("S1 重投 topic 上无消息", coll.count(common.GetRetryTopic(group)) == 0,
		fmt.Sprintf("retry=%d", coll.count(common.GetRetryTopic(group))))

	if err := c.PersistConsumerOffset(); err != nil {
		check("S1 位点落盘 (UPDATE_CONSUMER_OFFSET)", false, err.Error())
	} else {
		check("S1 位点落盘 (UPDATE_CONSUMER_OFFSET)", true)
	}
}

// S2: the committed offset lives on the broker — a second consumer in the same
// group must receive nothing.
func runS2(ns, topic, group string, expect int) {
	coll := newCollector()
	c, err := startSubscribed(ns, group, topic, "*", coll, func(c *client.DefaultMQPushConsumer) {
		c.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	})
	if err != nil {
		check("S2 构造/订阅/启动", false, err.Error())
		return
	}
	defer c.Shutdown()

	// Nothing to receive: the group already consumed all `expect` messages.
	time.Sleep(8 * time.Second)
	check("S2 同组第二个消费者不再收到已消费的消息", coll.count(topic) == 0,
		fmt.Sprintf("got=%d (组已消费 %d 条)", coll.count(topic), expect))
}

// S3: tag filtering. The broker does not filter (postSubscriptionWhenPull is off
// by default), so the CLIENT's second-stage filter is what has to drop TagB.
//
// This needs its OWN group: reusing S1's group would start from the already
// committed offset and legitimately receive nothing.
func runS3(ns, topic, group string, expect int) {
	coll := newCollector()
	c, err := startSubscribed(ns, group, topic, "TagA", coll, func(c *client.DefaultMQPushConsumer) {
		c.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	})
	if err != nil {
		check("S3 构造/订阅/启动", false, err.Error())
		return
	}
	defer c.Shutdown()

	want := expect / 2
	ok := waitFor(60*time.Second, func() bool { return coll.count(topic) >= want })
	got := coll.snapshot(topic)
	bad := 0
	for _, r := range got {
		if !strings.HasPrefix(r.body, "TagA") {
			bad++
		}
	}
	// Every scanned offset (TagA and TagB) advances the cursor, so the client
	// must have skipped exactly the TagB half rather than stalling on it.
	check("S3 客户端二次 tag 过滤：只收到 TagA", ok && bad == 0,
		fmt.Sprintf("got=%d want=%d non-TagA=%d", len(got), want, bad))
}

// S4: orderly consumption reads a single queue strictly in order. The topic
// must have exactly ONE queue — a flat offset list across 4 queues restarts at
// 0 per queue and would look "unordered".
func runS4(ns, topic, group string, expect int) {
	if topic == "" || expect <= 0 {
		check("S4 顺序消费（需要 -orderly-topic/-orderly-expect）", true, "skipped")
		return
	}
	coll := &orderlyCollector{}
	c, err := startSubscribed(ns, group, topic, "*", coll, func(c *client.DefaultMQPushConsumer) {
		c.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	})
	if err != nil {
		check("S4 构造/订阅/启动", false, err.Error())
		return
	}
	defer c.Shutdown()

	waitFor(90*time.Second, func() bool { return coll.count(topic) >= expect })
	off := coll.offsets(topic)
	fmt.Printf("      S4 offsets: %v\n", off)
	ascending := len(off) == expect
	for i := 1; i < len(off); i++ {
		if off[i] <= off[i-1] {
			ascending = false
		}
	}
	check(fmt.Sprintf("S4 顺序消费：%d 条 queueOffset 严格递增", expect),
		ascending && len(off) > 0, fmt.Sprintf("got=%d", len(off)))
}

// orderlyCollector records queueOffsets for the orderly listener SPI.
type orderlyCollector struct {
	mu   sync.Mutex
	recs []record
}

func (c *orderlyCollector) ConsumeMessage(msgs []*common.MessageExt, ctx *client.ConsumeOrderlyContext) client.ConsumeOrderlyStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range msgs {
		c.recs = append(c.recs, record{topic: m.Topic, queueID: m.QueueID, offset: m.QueueOffset})
	}
	return client.OrderlySuccess
}

func (c *orderlyCollector) offsets(topic string) []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []int64
	for _, r := range c.recs {
		if r.topic == topic {
			out = append(out, r.offset)
		}
	}
	return out
}

func (c *orderlyCollector) count(topic string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, r := range c.recs {
		if r.topic == topic {
			n++
		}
	}
	return n
}

// S5: two consumers in one group must split the queues and stay inside their
// slice. The expected split is read back from the route, not hardcoded.
func runS5(ns, topic, group string) {
	firstColl, secondColl := newCollector(), newCollector()
	first, err1 := newConsumer(ns, group, firstColl, func(c *client.DefaultMQPushConsumer) {
		c.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	})
	second, err2 := newConsumer(ns, group, secondColl, func(c *client.DefaultMQPushConsumer) {
		c.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	})
	if err1 != nil || err2 != nil {
		check("S5 构造两个消费者", false, fmt.Sprintf("%v %v", err1, err2))
		return
	}
	for _, c := range []*client.DefaultMQPushConsumer{first, second} {
		if err := c.Subscribe(topic, "*"); err != nil {
			check("S5 订阅", false, err.Error())
			return
		}
	}
	if err := first.Start(); err != nil {
		check("S5 first 启动", false, err.Error())
		return
	}
	defer first.Shutdown()
	if err := second.Start(); err != nil {
		check("S5 second 启动", false, err.Error())
		return
	}
	defer second.Shutdown()

	// The route is what BOTH sides must agree on; a mismatch here (rather than
	// in the strategy) is the classic cause of a half-split assignment.
	route := first.SubscribeQueuesOf(topic)
	want := len(route) / 2
	fmt.Printf("      S5 route=%d queues, each should own %d\n", len(route), want)

	// The second instance's heartbeat triggers NOTIFY_CONSUMER_IDS_CHANGED, so
	// both rebalance within a few seconds.
	split := func() bool {
		a, b := ownedOf(first, topic), ownedOf(second, topic)
		if len(a) != want || len(b) != want {
			return false
		}
		seen := map[string]bool{}
		for _, mq := range a {
			seen[mqID(mq)] = true
		}
		for _, mq := range b {
			if seen[mqID(mq)] {
				return false
			}
		}
		return len(seen)+len(b) == len(route)
	}
	settled := waitFor(60*time.Second, split)
	// A single sample can catch a rebalance in flight; require it to hold.
	if settled {
		time.Sleep(time.Second)
		settled = split()
	}
	check(fmt.Sprintf("S5 两个实例把 %d 条队列各分 %d 条", len(route), want), settled,
		fmt.Sprintf("first=%s second=%s", queueIDs(ownedOf(first, topic)), queueIDs(ownedOf(second, topic))))

	// Same key space as the split check: (broker, queueId), NOT queueId alone.
	owned := map[string]int{}
	for _, mq := range ownedOf(first, topic) {
		owned[mqID(mq)]++
	}
	for _, mq := range ownedOf(second, topic) {
		owned[mqID(mq)]++
	}
	clashes := 0
	for _, n := range owned {
		if n > 1 {
			clashes++
		}
	}
	check("S5 分配不重叠", clashes == 0, fmt.Sprintf("clashing queues=%d", clashes))
	check(fmt.Sprintf("S5 %d 条队列全覆盖", len(route)), len(owned) == len(route),
		fmt.Sprintf("covered=%d", len(owned)))
}

// S6: shutdown must unregister the client at the broker.
//
// The Go side can only prove the group WAS registered (a non-empty assignment
// means the broker answered GET_CONSUMER_LIST_BY_GROUP for this clientId) and
// that the consumer then reports itself stopped. The third leg — that the
// broker-side connection really disappeared — is read back by the Python
// verifier, which is why this scenario's group is passed to it.
func runS6(ns, topic, group string) {
	coll := newCollector()
	c, err := startSubscribed(ns, group, topic, "*", coll, func(c *client.DefaultMQPushConsumer) {
		c.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	})
	if err != nil {
		check("S6 构造/订阅/启动", false, err.Error())
		return
	}
	// Registered at the broker = the group list came back with our clientId,
	// which is the precondition for an assignment at all.
	registered := waitFor(45*time.Second, func() bool { return c.AssignedQueueCount() > 0 })
	check("S6 已向 broker 注册（拿到队列分配）", registered,
		fmt.Sprintf("assigned=%d", c.AssignedQueueCount()))

	started := c.IsStarted()
	c.Shutdown()
	check("S6 优雅停机后 IsStarted=false", started && !c.IsStarted(),
		fmt.Sprintf("started=%v after=%v", started, c.IsStarted()))
}
