// live_pull is the Go pull-consumer (DefaultMQPullConsumer) smoke test against a
// real RocketMQ 5.x cluster: topic creation, queue discovery, min/max offset,
// the short poll, the short poll at an IDLE TAIL (the suspend-bit guard), the
// long poll both waking early and expiring, the caller-owned cursor, and
// sendMessageBack into %RETRY%group.
//
//	go run ./examples/live_pull -ns 127.0.0.1:9876
//
// Every check prints PASS/FAIL and the process exits non-zero if any failed. Use
// a fresh topic per run (the default embeds a timestamp) so leftover state from
// an earlier run cannot make a check pass.
//
// The last line is `PASS=<n> FAIL=<n> COMMITTED=<offset> TOTAL=<n>`:
// COMMITTED is the deliberately PARTIAL offset this consumer left in the broker
// and TOTAL is how many messages the topic holds. The external read-back
// checker (driven by scripts/run_go_pull_live.sh) consumes both,
// so adding a check here can never silently make the read-back expectation
// wrong.
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
	// total counts messages the broker ACCEPTED, so the read-back can assert on
	// an exact number.
	total int
	mu    sync.Mutex
)

// committedOffset is intentionally NOT the tail: committing 3 of the 6 messages
// proves the broker stored what this client sent (right group, right topic,
// right queue) rather than something that happens to equal "everything".
const committedOffset = 3

const (
	seedBodies = 5 // go-pull-seed-1..5
	wakeBody   = "go-pull-wake"
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

func main() {
	nsAddr := flag.String("ns", "127.0.0.1:9876", "nameserver address")
	topic := flag.String("topic", "", "topic name (default: GoPullLive_<unix>)")
	group := flag.String("group", "", "pull consumer group (default: GID_GoPullLive_<unix>)")
	suspend := flag.Duration("suspend", 3*time.Second,
		"broker suspend budget for the long poll; normally 20s, lowered here so the idle-tail check stays quick")
	flag.Parse()

	stamp := time.Now().Unix()
	topicName := *topic
	if topicName == "" {
		topicName = fmt.Sprintf("GoPullLive_%d", stamp)
	}
	groupName := *group
	if groupName == "" {
		groupName = fmt.Sprintf("GID_GoPullLive_%d", stamp)
	}
	sendGroup := fmt.Sprintf("GID_GoPullSend_%d", stamp)
	if !strings.Contains(*nsAddr, ":") {
		fmt.Fprintln(os.Stderr, "nameserver address must be host:port")
		os.Exit(2)
	}
	suspendMillis := suspend.Milliseconds()
	fmt.Printf("nameserver=%s topic=%s group=%s suspend=%v\n\n", *nsAddr, topicName, groupName, *suspend)

	// ---- 0. producer: only used to seed the topic and to wake the long poll --------
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

	c := client.MustNewDefaultMQPullConsumer(groupName)
	c.SetNameServerAddresses([]string{*nsAddr})
	// Long-poll invariants: the client must out-wait the broker's hold, or every
	// request on an idle queue is a guaranteed timeout (checkConfig :811). The
	// margin is 7s rather than 4s because the broker's PullRequestHoldService
	// ticks every 5s when longPollingEnable is on, so an idle long poll can take
	// ~5s to notice that its own (shorter) budget expired.
	c.SetBrokerSuspendMaxTimeMillis(suspendMillis)
	c.SetConsumerTimeoutMillisWhenSuspend(suspendMillis + 7000)
	if err := c.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "pull consumer start: %v\n", err)
		os.Exit(1)
	}
	defer c.Shutdown()
	check("consumer clientId carries @STREAM", strings.Contains(c.ClientID(), "@STREAM"), "clientId="+c.ClientID())

	if err := c.CreateTopic(common.DefaultTopic, topicName, 1, 0); err != nil {
		check("create Topic", false, err.Error())
	} else {
		check("create Topic succeeded (1 read/write queue)", true, "")
	}
	var queues []common.MessageQueue
	deadline := time.Now().Add(15 * time.Second)
	for {
		queues, err = c.FetchSubscribeMessageQueues(topicName)
		if err == nil && len(queues) == 1 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if len(queues) != 1 {
		check("FetchSubscribeMessageQueues returns 1 queue", false, fmt.Sprintf("got=%d err=%v", len(queues), err))
		fmt.Printf("\nPASS=%d FAIL=%d COMMITTED=%d TOTAL=%d\n", passCount, failCount, committedOffset, totalCount())
		os.Exit(1)
	}
	check("FetchSubscribeMessageQueues returns 1 queue", queues[0].QueueID == 0,
		fmt.Sprintf("queue=%v", queues[0]))
	q0 := queues[0]

	published, err := c.FetchPublishMessageQueues(topicName)
	if err != nil || len(published) != 1 {
		check("FetchPublishMessageQueues returns 1 write queue", false, fmt.Sprintf("got=%d err=%v", len(published), err))
	} else {
		check("FetchPublishMessageQueues returns 1 write queue", published[0].QueueID == 0,
			fmt.Sprintf("queue=%v", published[0]))
	}

	var balanced []common.MessageQueue
	deadline = time.Now().Add(20 * time.Second)
	for {
		balanced, err = c.FetchMessageQueuesInBalance(topicName)
		if err == nil && len(balanced) == 1 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		check("FetchMessageQueuesInBalance usable", false, err.Error())
	} else {
		check("FetchMessageQueuesInBalance returns this 1 queue in an exclusive group",
			len(balanced) == 1 && balanced[0].QueueID == 0,
			fmt.Sprintf("got=%d queues=%v (no queue was ever pulled; a non-empty result can only come from route+38)", len(balanced), balanced))
	}
	emptyView, err := c.FetchMessageQueuesInBalance(topicName + "_NotCreated")
	check("a never-created topic does not throw: an empty share is returned instead of swallowing all queues",
		err == nil && len(emptyView) == 0, fmt.Sprintf("err=%v got=%d", err, len(emptyView)))

	// ---- 2. seed the topic --------------------------------------------------------
	seeded := 0
	for i := 1; i <= seedBodies; i++ {
		m := common.NewMessage(topicName, []byte(fmt.Sprintf("go-pull-seed-%d", i)))
		m.SetKeys(fmt.Sprintf("go-pull-key-%d", i))
		if _, err := p.Send(m); err != nil {
			fmt.Printf("      seed %d send error: %v\n", i, err)
			continue
		}
		seeded++
		bump(1)
	}
	check(fmt.Sprintf("all %d seed messages written", seedBodies), seeded == seedBodies,
		fmt.Sprintf("seeded=%d", seeded))

	// ---- 3. min / max offset ------------------------------------------------------
	// MaxOffset is the ConsumeQueue's dispatch progress, and the broker dispatches
	// the commitlog asynchronously — right after a burst of sends it can still be
	// one behind. Poll instead of reading once: a one-shot read is flaky for
	// exactly that reason.
	maxOffset := int64(-1)
	seedDeadline := time.Now().Add(15 * time.Second)
	for {
		maxOffset, err = c.MaxOffset(q0)
		if err == nil && maxOffset == int64(seedBodies) {
			break
		}
		if time.Now().After(seedDeadline) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if err != nil {
		check("MaxOffset usable", false, err.Error())
	} else {
		check("MaxOffset == seed count", maxOffset == int64(seedBodies),
			fmt.Sprintf("max=%d want=%d", maxOffset, seedBodies))
	}
	minOffset, err := c.MinOffset(q0)
	if err != nil {
		check("MinOffset usable", false, err.Error())
	} else {
		check("MinOffset == 0", minOffset == 0, fmt.Sprintf("min=%d", minOffset))
	}
	if _, err := c.SearchOffset(q0, common.CurrentTimeMillis()); err != nil {
		check("SearchOffset usable", false, err.Error())
	} else {
		check("SearchOffset usable", true, "")
	}
	if _, err := c.EarliestMsgStoreTime(q0); err != nil {
		check("EarliestMsgStoreTime usable", false, err.Error())
	} else {
		check("EarliestMsgStoreTime usable", true, "")
	}

	// ---- 4. the short poll at the head: everything, quickly -----------------------
	begin := time.Now()
	head, err := c.Pull(q0, "*", 0, 32)
	shortElapsed := time.Since(begin)
	if err != nil {
		check("Pull short poll at the queue head returns", false, err.Error())
	} else {
		check("Pull short poll at the queue head returns FOUND", head.PullStatus == client.PullFound,
			fmt.Sprintf("status=%v msgs=%d", head.PullStatus, len(head.MsgFoundList)))
		check(fmt.Sprintf("pulled %d seed messages", seedBodies), len(head.MsgFoundList) == seedBodies,
			fmt.Sprintf("got=%d", len(head.MsgFoundList)))
		check("NextBeginOffset == seed count", head.NextBeginOffset == int64(seedBodies),
			fmt.Sprintf("next=%d", head.NextBeginOffset))
		check("short poll takes far less than the suspend budget", shortElapsed < *suspend/2,
			fmt.Sprintf("elapsed=%v suspend=%v", shortElapsed, *suspend))
		okBodies, okKeys := true, true
		for i, msg := range head.MsgFoundList {
			want := fmt.Sprintf("go-pull-seed-%d", i+1)
			if string(msg.Body) != want {
				okBodies = false
				fmt.Printf("      msg[%d] body=%q want=%q\n", i, msg.Body, want)
			}
			if keys, _ := msg.GetProperty("KEYS"); keys != fmt.Sprintf("go-pull-key-%d", i+1) {
				okKeys = false
			}
		}
		check("bodies complete in queue order", okBodies, "")
		check("KEYS property preserved as-is", okKeys, "")
	}

	// ---- 5. THE guard: a short poll at an IDLE tail must not suspend --------------
	// If the SUSPEND bit leaked into Pull(), the broker would hold the request for
	// the whole suspend budget while the client gives up at
	// consumerPullTimeoutMillis — on an empty queue that is a guaranteed timeout,
	// and it is the single most expensive trap on this path.
	idleBegin := time.Now()
	tail, err := c.Pull(q0, "*", int64(seedBodies), 32)
	idleElapsed := time.Since(idleBegin)
	if err != nil {
		check("short poll at an idle tail returns (the SUSPEND bit must be 0)", false, err.Error())
	} else {
		check("short poll at an idle tail = NO_NEW_MSG", tail.PullStatus == client.PullNoNewMsg, "status="+tail.PullStatus.String())
		check("short poll at an idle tail did not suspend", idleElapsed < *suspend/2,
			fmt.Sprintf("elapsed=%v suspend=%v", idleElapsed, *suspend))
	}

	// ---- 6. the long poll: (a) wakes as soon as a message arrives -----------------
	wakeErr := make(chan error, 1)
	go func() {
		time.Sleep(700 * time.Millisecond)
		m := common.NewMessage(topicName, []byte(wakeBody))
		_, err := p.Send(m)
		wakeErr <- err
	}()
	wakeBegin := time.Now()
	woken, err := c.PullBlockIfNotFound(q0, "*", int64(seedBodies), 32)
	wakeElapsed := time.Since(wakeBegin)
	if sendErr := <-wakeErr; sendErr != nil {
		check("wake-up message written successfully", false, sendErr.Error())
	} else {
		bump(1)
		check("wake-up message written successfully", true, "")
	}
	if err != nil {
		check("long poll woken by the new message", false, err.Error())
	} else {
		check("long poll woken by the new message", woken.PullStatus == client.PullFound && len(woken.MsgFoundList) == 1,
			fmt.Sprintf("status=%v msgs=%d", woken.PullStatus, len(woken.MsgFoundList)))
		if len(woken.MsgFoundList) == 1 {
			check("wake-up message body correct", string(woken.MsgFoundList[0].Body) == wakeBody,
				fmt.Sprintf("body=%q", woken.MsgFoundList[0].Body))
		}
		check("the long poll really waited (did not return immediately)", wakeElapsed >= 500*time.Millisecond,
			fmt.Sprintf("elapsed=%v", wakeElapsed))
		check("the long poll returned early, within the suspend budget", wakeElapsed < *suspend,
			fmt.Sprintf("elapsed=%v suspend=%v", wakeElapsed, *suspend))
	}

	// ---- 7. the long poll: (b) an idle tail really does suspend -------------------
	// Positive proof the frame carries suspend=true: the broker can only hold it
	// for its own budget, so the reply cannot arrive before that. The measured
	// wall time is the broker's hold tick (~5s with the default
	// longPollingEnable), not our requested budget, so only a lower bound is
	// asserted here.
	expireBegin := time.Now()
	expired, err := c.PullBlockIfNotFound(q0, "*", int64(seedBodies+1), 32)
	expireElapsed := time.Since(expireBegin)
	if err != nil {
		check("long poll at an idle tail returns", false, err.Error())
	} else {
		check("long poll at an idle tail = NO_NEW_MSG (suspend expired)", expired.PullStatus == client.PullNoNewMsg,
			"status="+expired.PullStatus.String())
	}
	check("the long poll at an idle tail really suspended", expireElapsed >= (*suspend)*7/10,
		fmt.Sprintf("elapsed=%v suspend=%v", expireElapsed, *suspend))

	// ---- 8. the caller owns the cursor -------------------------------------------
	// Pull() must NOT commit anything: the caller decides when the work is done.
	if _, err := c.Pull(q0, "*", 0, 1); err != nil {
		check("re-pull the queue head for the offset check", false, err.Error())
	} else {
		check("re-pull the queue head for the offset check", true, "")
	}
	// A fresh group has no record yet: the broker answers QUERY_NOT_FOUND, which
	// the store reports as -1. Either way it must not be the value we are about to
	// commit — that is the observable half of "Pull() does not commit".
	before, beforeErr := c.FetchConsumeOffset(q0, true)
	check("Pull committed no offset (the caller owns the cursor)", beforeErr == nil && before != int64(committedOffset),
		fmt.Sprintf("broker offset=%d err=%v", before, beforeErr))

	if err := c.UpdateConsumeOffset(q0, committedOffset); err != nil {
		check("UpdateConsumeOffset enters the local offset table", false, err.Error())
	} else {
		check("UpdateConsumeOffset enters the local offset table", true, "")
	}
	if got, err := c.FetchConsumeOffset(q0, false); err != nil || got != committedOffset {
		check("just-written offset read back locally", false, fmt.Sprintf("got=%d err=%v", got, err))
	} else {
		check("just-written offset read back locally", true, fmt.Sprintf("offset=%d", got))
	}
	if err := c.PersistConsumerOffset(); err != nil {
		check("PersistConsumerOffset", false, err.Error())
	} else {
		check("PersistConsumerOffset", true, "")
	}
	// READ_FROM_STORE forces a broker round trip: this asserts the offset really
	// left the process (the external read-back then confirms the same value
	// from its own connection).
	if got, err := c.FetchConsumeOffset(q0, true); err != nil || got != committedOffset {
		check("offset read back from the broker", false, fmt.Sprintf("got=%d err=%v", got, err))
	} else {
		check("offset read back from the broker", true, fmt.Sprintf("offset=%d", got))
	}
	if err := c.UpdateConsumeOffsetToBroker(q0, committedOffset); err != nil {
		check("UpdateConsumeOffsetToBroker", false, err.Error())
	} else {
		check("UpdateConsumeOffsetToBroker", true, "")
	}

	// ---- 9. sendMessageBack: the message must land in %RETRY%group ----------------
	// maxReconsumeTimes is 16 for a pull consumer (not the push consumer's -1), so
	// the broker re-queues instead of DLQ-ing on the first bounce. The external
	// read-back checks %RETRY%<group> and asserts the body is there — and that
	// %DLQ%<group> is not.
	if bounced, err := c.Pull(q0, "*", 0, 1); err != nil {
		check("take one message for the send-back", false, err.Error())
	} else if len(bounced.MsgFoundList) == 0 {
		check("take one message for the send-back", false, "no message")
	} else {
		check("take one message for the send-back", true, "")
		msg := bounced.MsgFoundList[0]
		// delayLevel 1 == 1s in the default messageDelayLevel table: the retry topic
		// only sees it after the schedule service releases it, and 3 (=10s) would
		// just add wall time to the run.
		if err := c.SendMessageBack(msg, 1, q0.BrokerName); err != nil {
			check("sendMessageBack accepted by the broker", false, err.Error())
		} else {
			check("sendMessageBack accepted by the broker", true, fmt.Sprintf("body=%q delay=1", msg.Body))
		}
	}

	fmt.Printf("\nPASS=%d FAIL=%d COMMITTED=%d TOTAL=%d\n", passCount, failCount, committedOffset, totalCount())
	if failCount > 0 {
		os.Exit(1)
	}
}
