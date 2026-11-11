// Live verification of the pull-side rebalance (Java DefaultMQPullConsumer.enableRebalance).
//
// Java's pull consumer rebalances: DefaultMQPullConsumer.java:97 defaults enableRebalance
// to true, and the shared RebalanceService drives tryRebalance() every 20s, calling
// MessageQueueListener.messageQueueChanged(topic, mqAll, mqDivided) when the allocation
// changed. This probe proves against a real broker:
//
//	L1  two instances of one group split a multi-queue topic -- disjoint shares whose
//	    union is the whole topic (only possible if both read the same broker-side
//	    consumer list and run the same allocation formula);
//	L2  the listener really fired with mqAll = the topic's full queue set;
//	L3  enableRebalance=false stops the loop (no callbacks, empty table) while
//	    FetchMessageQueuesInBalance keeps computing on the spot.
//
// Single broker, one topic with 8 queues -- enough for all three: the split is across
// queues within one broker, not across brokers.
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/client"
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

var pass, fail int

func check(name string, ok bool, detail string) {
	if ok {
		pass++
		fmt.Printf("  [PASS] %s  %s\n", name, detail)
		return
	}
	fail++
	fmt.Printf("  [FAIL] %s  %s\n", name, detail)
}

func fatal(what string, err error) {
	if err != nil {
		fmt.Printf("FATAL %s: %v\n", what, err)
		os.Exit(2)
	}
}

type recorder struct{ calls int }

func (r *recorder) MessageQueueChanged(topic string, mqAll, mqDivided []common.MessageQueue) {
	r.calls++
}

func ids(queues []common.MessageQueue) []int32 {
	out := make([]int32, 0, len(queues))
	for _, mq := range queues {
		out = append(out, mq.QueueID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func main() {
	ns := flag.String("ns", "127.0.0.1:9876", "nameserver address")
	stamp := time.Now().UnixNano() / int64(time.Millisecond)
	prefix := fmt.Sprintf("PullRebLive_%d", stamp)
	topic := prefix + "_Topic"
	const nQueue = 8
	// Shorter than Java's 20s so the probe finishes quickly; the loop shape is the point.
	const waitMillis, minMillis = 3000, 500
	flag.Parse()

	fmt.Printf("%s: pull-side rebalance live (ns=%s, topic=%s, %d queues)\n", prefix, *ns, topic, nQueue)

	// Create the topic up front: a consumer gets no route for an uncreated topic
	// (TBW102 is a producer-side fallback only).
	admin, err := client.NewDefaultMQPullConsumer(prefix + "_admin")
	fatal("topic admin", err)
	admin.SetNameServerAddresses([]string{*ns})
	fatal("topic admin start", admin.Start())
	fatal("create topic", admin.CreateTopic(common.DefaultTopic, topic, nQueue, 0))
	admin.Shutdown()
	wait := time.Now().Add(20 * time.Second)
	for {
		probe, perr := client.NewDefaultMQPullConsumer(prefix + "_wait")
		if perr == nil {
			probe.SetNameServerAddresses([]string{*ns})
			if probe.Start() == nil {
				queues, qerr := probe.FetchSubscribeMessageQueues(topic)
				probe.Shutdown()
				if qerr == nil && len(queues) == nQueue {
					break
				}
			}
		}
		if time.Now().After(wait) {
			fatal("route for "+topic, fmt.Errorf("not visible within 20s"))
		}
		time.Sleep(300 * time.Millisecond)
	}
	fmt.Printf("  created %s with %d queues\n", topic, nQueue)

	newConsumer := func(inst, group string, rec *recorder, enable bool) *client.DefaultMQPullConsumer {
		c, cerr := client.NewDefaultMQPullConsumer(group)
		fatal("consumer", cerr)
		// A distinct instanceName gives a distinct clientId; the clientId is what the
		// broker's GET_CONSUMER_LIST_BY_GROUP returns, so without this both facades
		// would collapse into one group member.
		c.SetInstanceName(inst)
		c.SetNameServerAddresses([]string{*ns})
		c.SetRebalanceWaitMillis(waitMillis)
		c.SetRebalanceMinIntervalMillis(minMillis)
		if rec != nil {
			c.RegisterMessageQueueListener(topic, rec)
		} else {
			c.RegisterMessageQueueListener(topic, nil)
		}
		c.SetEnableRebalance(enable)
		fatal("consumer start", c.Start())
		return c
	}

	recA, recB := &recorder{}, &recorder{}
	a := newConsumer(prefix+"InstA", prefix+"_group", recA, true)
	b := newConsumer(prefix+"InstB", prefix+"_group", recB, true)
	var c *client.DefaultMQPullConsumer
	defer func() {
		if c != nil {
			c.Shutdown()
		}
		b.Shutdown()
		a.Shutdown()
	}()

	fmt.Println("  started two instances, waiting for the first rebalance rounds...")
	table := func(cc *client.DefaultMQPullConsumer) []common.MessageQueue {
		return cc.GetRebalanceTable()[topic]
	}
	waitFor := func(cond func() bool, timeout time.Duration) bool {
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			if cond() {
				return true
			}
			time.Sleep(200 * time.Millisecond)
		}
		return cond()
	}

	// L1: both must end up with a non-empty share of the topic.
	got := waitFor(func() bool {
		return len(table(a)) > 0 && len(table(b)) > 0
	}, 30*time.Second)
	shareA, shareB := table(a), table(b)
	fmt.Printf("  INFO share A=%d queues, share B=%d queues\n", len(shareA), len(shareB))
	check("L1-both instances ended up with a share of the topic", got,
		fmt.Sprintf("A=%d B=%d", len(shareA), len(shareB)))

	inA := map[int32]bool{}
	for _, id := range ids(shareA) {
		inA[id] = true
	}
	overlap := 0
	union := map[int32]bool{}
	for _, id := range ids(shareB) {
		if inA[id] {
			overlap++
		}
		union[id] = true
	}
	for _, id := range ids(shareA) {
		union[id] = true
	}
	check("L1-the two shares are disjoint (one queue is never owned twice)", overlap == 0,
		fmt.Sprintf("overlap=%d", overlap))
	full := true
	for i := int32(0); i < nQueue; i++ {
		if !union[i] {
			full = false
		}
	}
	check("L1-the two shares cover every queue of the topic exactly once", full && len(union) == nQueue,
		fmt.Sprintf("union=%d of %d", len(union), nQueue))

	// L2: the listener really ran, and mqAll was the topic's full queue set.
	check("L2-the allocation change was reported to the listener",
		recA.calls >= 1 && recB.calls >= 1,
		fmt.Sprintf("A calls=%d B calls=%d", recA.calls, recB.calls))
	inBalance, ierr := a.FetchMessageQueuesInBalance(topic)
	fatal("fetch in balance", ierr)
	check("L1-fetch_message_queues_in_balance answers from the rebalance table",
		fmt.Sprint(ids(inBalance)) == fmt.Sprint(ids(shareA)),
		fmt.Sprintf("got=%d expect=%d", len(inBalance), len(shareA)))

	// L3: the switch really gates the loop.
	recC := &recorder{}
	c = newConsumer(prefix+"InstC", prefix+"_group", recC, false)
	fmt.Println("  started a third instance with enableRebalance=false, waiting 2 rounds...")
	time.Sleep(time.Duration(waitMillis*2+4000) * time.Millisecond)
	check("L3-enableRebalance=false stops the loop: no callback at all", recC.calls == 0,
		fmt.Sprintf("calls=%d", recC.calls))
	check("L3-enableRebalance=false leaves the rebalance table empty",
		len(c.GetRebalanceTable()) == 0, fmt.Sprintf("table size=%d", len(c.GetRebalanceTable())))
	onTheSpot, oerr := c.FetchMessageQueuesInBalance(topic)
	fatal("fetch on the spot", oerr)
	check("L3-with the loop off, fetch_message_queues_in_balance still computes on the spot",
		len(onTheSpot) > 0, fmt.Sprintf("got=%d queues", len(onTheSpot)))

	fmt.Printf("\n%s: %d passed, %d failed\n", prefix, pass, fail)
	if fail > 0 {
		os.Exit(1)
	}
}
