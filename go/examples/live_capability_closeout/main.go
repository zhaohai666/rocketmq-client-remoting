// live_capability_closeout is the live counterpart of the offline
// capability_closeout suite: the three capabilities added in the 2026-11-11
// round are asserted against a real RocketMQ 5.x broker, because each of them
// either is wire-visible or changes which messages a client sees.
//
//	go run ./examples/live_capability_closeout -ns 127.0.0.1:9876
//
// L1 ConsumeFromWhere's three "deprecated" values. Java folds
// CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST / MIN_OFFSET / MAX_OFFSET
// into the CONSUME_FROM_LAST_OFFSET branch of
// RebalancePushImpl#computePullFromWhereWithException (lines 171-173). One backlog,
// five fresh groups:
//
//	FIRST_OFFSET (positive control) -> the whole backlog (the only equality assertion)
//	the four LAST-family values     -> accepted, started, sane counts (printed)
//
// Delivery counts are NOT used to prove the folding. On a **fresh topic** the broker
// answers QUERY_CONSUMER_OFFSET for a never-committed group with SUCCESS offset=0 on its
// V3_0_6 path (minOffset<=0 && offset 0 in memory) and with QUERY_NOT_FOUND otherwise, and
// that answer flips from leg to leg -- measured 2026-11-11 with the JAVA reference client on
// one backlog in one process: LAST_OFFSET=0 while MIN_OFFSET=8 and MAX_OFFSET=8. The folding
// claim belongs to the offline suite, which reads the resolved offset with a fake store.
//
// L2 LitePull pull-from-which-node pin. Go cannot observe the id that travels
// from outside the client package (the pull API is unexported; the offline suite
// asserts recalculatePullFromWhichNode directly), so the live leg asserts the
// load-bearing wiring instead: after Start the pin must be readable **through the
// pull API**, and all three modes must still deliver the backlog. A pin lost
// between consumer and pull API would show up here as the accessor disagreeing.
//
// L3 adminExtGroup: pure identity, no wire effect, so the live leg only proves a
// custom-group admin still boots and serves an RPC.
package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/client"
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

const nMsg = 8

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
		fmt.Printf("PASS  %s  %s\n", name, detail)
		return
	}
	failCount++
	fmt.Printf("FAIL  %s  %s\n", name, detail)
}

func fatal(label string, err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "%s: %v\n", label, err)
	os.Exit(1)
}

// collector records every body a push consumer delivers.
type collector struct {
	mu     sync.Mutex
	bodies []string
}

func (c *collector) ConsumeMessage(msgs []*common.MessageExt,
	_ *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	c.mu.Lock()
	for _, m := range msgs {
		c.bodies = append(c.bodies, string(m.Body))
	}
	c.mu.Unlock()
	return client.ConsumeSuccess
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.bodies)
}

// pushLeg starts one push consumer at `where` and records arrivals for `window`.
// Every leg uses a fresh group: a group that already committed offsets reads them
// back whatever consumeFromWhere says, which is the false green to avoid.
func pushLeg(ns, topic, group, where string, window time.Duration) int {
	c, err := client.NewDefaultMQPushConsumer(group)
	fatal("push consumer", err)
	c.SetNameServerAddresses([]string{ns})
	c.SetInstanceName(group)
	c.SetConsumeFromWhere(where)
	coll := &collector{}
	fatal("set listener", c.SetMessageListener(coll))
	fatal("subscribe", c.Subscribe(topic, "*"))
	fatal("push start", c.Start())
	time.Sleep(window)
	c.Shutdown()
	return coll.count()
}

// liteLeg runs one LitePull subscribe-mode leg with the given pin and returns how
// many messages it polled.
func liteLeg(ns, topic, group string, pin bool, brokerID int64, want int,
	window time.Duration) (int, bool, int64) {
	c, err := client.NewDefaultLitePullConsumer(group)
	fatal("lite consumer", err)
	c.SetNameServerAddresses([]string{ns})
	c.SetInstanceName(group)
	c.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	c.SetConnectBrokerByUser(pin)
	c.SetDefaultBrokerID(brokerID)
	fatal("subscribe", c.Subscribe(topic, "*"))
	fatal("lite start", c.Start())
	// Read the pin back through the pull API: after Start the consumer's own
	// fields are no longer what the pull rounds use.
	pinSeen := c.IsConnectBrokerByUser()
	idSeen := c.DefaultBrokerID()
	got := 0
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) && got < want {
		msgs, perr := c.PollWithTimeout(1000)
		if perr != nil {
			fmt.Printf("      poll error: %v\n", perr)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		got += len(msgs)
	}
	c.Shutdown()
	return got, pinSeen, idSeen
}

func main() {
	ns := flag.String("ns", "127.0.0.1:9876", "nameserver address")
	stamp := time.Now().UnixNano() / int64(time.Millisecond)
	prefix := fmt.Sprintf("CapLive_%d", stamp)
	flag.Parse()

	p, err := client.NewDefaultMQProducer(prefix + "_pg")
	fatal("producer", err)
	p.SetNameServerAddresses([]string{*ns})
	fatal("producer start", p.Start())
	defer p.Shutdown()

	createTopic := func(topic string) {
		a, aerr := client.NewDefaultMQPullConsumer(prefix + "_admin")
		fatal("topic admin", aerr)
		a.SetNameServerAddresses([]string{*ns})
		fatal("topic admin start", a.Start())
		fatal("create topic", a.CreateTopic(common.DefaultTopic, topic, 1, 0))
		a.Shutdown()
		// The route only shows up once the broker's 30s registration round (or the
		// admin's own topic creation) has landed: poll instead of a fixed sleep.
		wait := time.Now().Add(15 * time.Second)
		for time.Now().Before(wait) {
			b, berr := client.NewDefaultMQPullConsumer(prefix + "_wait")
			if berr == nil {
				b.SetNameServerAddresses([]string{*ns})
				if b.Start() == nil {
					queues, qerr := b.FetchSubscribeMessageQueues(topic)
					b.Shutdown()
					if qerr == nil && len(queues) == 1 {
						return
					}
				}
			}
			time.Sleep(300 * time.Millisecond)
		}
		fatal("route for "+topic, fmt.Errorf("not visible within 15s"))
	}
	send := func(topic string, i int) {
		m := common.NewMessage(topic, []byte(fmt.Sprintf("cap-%02d", i)))
		fatal("send", func() error { _, serr := p.Send(m); return serr }())
	}

	// ---------------------------------------------------------------- L1
	fmt.Println("\nL1 ConsumeFromWhere: the three deprecated values fold into LAST_OFFSET")
	t1 := prefix + "_FromWhere"
	createTopic(t1)
	for i := 0; i < nMsg; i++ {
		send(t1, i)
	}
	fmt.Printf("  sent %d messages, no consumer has ever joined this topic\n", nMsg)

	window := 12 * time.Second
	n := pushLeg(*ns, t1, prefix+"_gFirst", client.ConsumeFromWhereFirstOffset, window)
	check("L1-positive control: FIRST_OFFSET delivers the whole backlog", n == nMsg,
		fmt.Sprintf("received=%d expect=%d", n, nMsg))

	// The four LAST-family values are measured, never compared to a constant, and **not**
	// required to equal each other. Delivery counts cannot discriminate the branch: the
	// broker answers QUERY_CONSUMER_OFFSET for a never-committed group with SUCCESS
	// offset=0 on its V3_0_6 path (minOffset<=0 and offset 0 still in memory) and with
	// QUERY_NOT_FOUND otherwise, and that answer flips between legs. Measured 2026-11-11
	// with the JAVA reference client on one backlog in one process: LAST_OFFSET=0 while
	// MIN_OFFSET=8 and MAX_OFFSET=8. So this leg asserts what it can honestly assert --
	// every value is accepted, the consumer starts and nothing exceeds the backlog -- and
	// prints the counts as the parity record. The "folds into the LAST_OFFSET branch"
	// claim is pinned by the offline suite, which reads the resolved offset directly.
	counts := ""
	for _, leg := range []struct{ label, where string }{
		{"LAST_OFFSET", client.ConsumeFromWhereLastOffset},
		{"MIN_OFFSET", client.ConsumeFromWhereMinOffset},
		{"MAX_OFFSET", client.ConsumeFromWhereMaxOffset},
		{"LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST", client.ConsumeFromWhereLastOffsetAndFromMinWhenBootFirst},
	} {
		n = pushLeg(*ns, t1, prefix+"_g"+leg.label, leg.where, window)
		check("L1-"+leg.label+" accepted: the consumer started and received a sane count",
			0 <= n && n <= nMsg, fmt.Sprintf("received=%d of %d", n, nMsg))
		counts += fmt.Sprintf(" %s=%d", leg.label, n)
	}
	fmt.Printf("  INFO LAST-family delivery counts on this end:%s "+
		"(not a parity oracle: the broker V3_0_6 offset answer is timing-dependent)\n", counts)

	// ---------------------------------------------------------------- L2
	fmt.Println("\nL2 LitePull pin (connectBrokerByUser / defaultBrokerId)")
	t2 := prefix + "_Pin"
	createTopic(t2)
	for i := 0; i < nMsg; i++ {
		send(t2, i)
	}
	fmt.Printf("  sent %d messages to %s\n", nMsg, t2)

	got, pinSeen, idSeen := liteLeg(*ns, t2, prefix+"_pinOff", false, 0, nMsg, 20*time.Second)
	check("L2-pin off: the pull API reports the switch off", !pinSeen, fmt.Sprintf("pin=%v", pinSeen))
	check("L2-pin off: delivers the backlog", got == nMsg, fmt.Sprintf("got=%d expect=%d", got, nMsg))

	got, pinSeen, idSeen = liteLeg(*ns, t2, prefix+"_pinMaster", true, 0, nMsg, 20*time.Second)
	check("L2-pin to the master (0): the pull API reports pin on and defaultBrokerId=0",
		pinSeen && idSeen == 0, fmt.Sprintf("pin=%v id=%d", pinSeen, idSeen))
	check("L2-pin to the master (0): delivers the backlog", got == nMsg,
		fmt.Sprintf("got=%d expect=%d", got, nMsg))

	got, pinSeen, idSeen = liteLeg(*ns, t2, prefix+"_pinSlave", true, 1, nMsg, 12*time.Second)
	check("L2-pin to a non-existent slave (1): the pull API reports defaultBrokerId=1",
		pinSeen && idSeen == 1, fmt.Sprintf("pin=%v id=%d", pinSeen, idSeen))
	check("L2-pin to a non-existent slave (1): FindBrokerAddressInSubscribe falls back to "+
		"the table's first broker (Java parity), so delivery still happens",
		got == nMsg, fmt.Sprintf("got=%d expect=%d", got, nMsg))

	// ---------------------------------------------------------------- L3
	fmt.Println("\nL3 adminExtGroup (identity only, no wire effect)")
	admin := client.NewDefaultMQAdminExt(nil)
	admin.SetNamesrvAddr(*ns)
	check("L3-default adminExtGroup is 'admin_ext_group'",
		admin.AdminExtGroup() == "admin_ext_group", fmt.Sprintf("value=%q", admin.AdminExtGroup()))
	check("L3-DefaultAdminExtGroup matches the Java constant",
		client.DefaultAdminExtGroup == "admin_ext_group",
		fmt.Sprintf("value=%q", client.DefaultAdminExtGroup))
	admin.SetAdminExtGroup("cap_closeout_admin_" + prefix)
	check("L3-override takes", admin.AdminExtGroup() == "cap_closeout_admin_"+prefix,
		fmt.Sprintf("value=%q", admin.AdminExtGroup()))
	fatal("admin start", admin.Start())
	topics, terr := admin.FetchAllTopicList()
	admin.Shutdown()
	found := false
	if terr == nil {
		for _, name := range topics.TopicList {
			if name == t2 {
				found = true
				break
			}
		}
	}
	check("L3-a custom-group admin still boots and serves an RPC on the live cluster",
		found, fmt.Sprintf("err=%v topics=%d", terr, len(topics.TopicList)))

	fmt.Printf("\n%s: %d passed, %d failed\n", prefix, passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
}
