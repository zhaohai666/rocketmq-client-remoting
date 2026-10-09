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
// and TOTAL is how many messages the topic holds. The Python read-back
// (scripts/run_go_pull_live.sh → python/go_pull_consume_check.py) consumes both,
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
		"broker suspend budget for the long poll; Java's default is 20s, lowered here so the idle-tail check stays quick")
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
	check("消费者 clientId 带 @STREAM", strings.Contains(c.ClientID(), "@STREAM"), "clientId="+c.ClientID())

	// ---- 1. create the topic with ONE queue so the offsets are exact ---------------
	// The key is the default topic (TBW102) whose route names the brokers to push
	// the new topic to — Java's MQAdminImpl.createTopic(key, ...) contract. It is
	// NOT an attributes field: sending a topic name there makes the broker answer
	// "kv string format wrong".
	if err := c.CreateTopic(common.DefaultTopic, topicName, 1, 0); err != nil {
		check("建 Topic", false, err.Error())
	} else {
		check("建 Topic 成功（1 个读写队列）", true, "")
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
		check("FetchSubscribeMessageQueues 返回 1 个队列", false, fmt.Sprintf("got=%d err=%v", len(queues), err))
		fmt.Printf("\nPASS=%d FAIL=%d COMMITTED=%d TOTAL=%d\n", passCount, failCount, committedOffset, totalCount())
		os.Exit(1)
	}
	check("FetchSubscribeMessageQueues 返回 1 个队列", queues[0].QueueID == 0,
		fmt.Sprintf("queue=%v", queues[0]))
	q0 := queues[0]

	published, err := c.FetchPublishMessageQueues(topicName)
	if err != nil || len(published) != 1 {
		check("FetchPublishMessageQueues 返回 1 个写队列", false, fmt.Sprintf("got=%d err=%v", len(published), err))
	} else {
		check("FetchPublishMessageQueues 返回 1 个写队列", published[0].QueueID == 0,
			fmt.Sprintf("queue=%v", published[0]))
	}

	// ---- 1b. the balanced view, BEFORE pulling anything -----------------------------
	// FetchMessageQueuesInBalance is Java DefaultMQPullConsumerImpl:120-135. Asking
	// for it while the instance has pulled no queue is the whole point: when the
	// route or GET_CONSUMER_LIST_BY_GROUP(38) cannot be had, this port keeps the
	// CURRENT assignment — which here is the empty set. So a non-empty answer can
	// only come from the real route + the real broker-side member list, i.e. the
	// group actually landed in the broker's consumerTable via the heartbeat.
	//
	// The beat rides the instance's scheduled task, whose first tick is
	// initialDelay=1s (Java MQClientInstance:260 — same anchor), so retry until the
	// broker answers with this clientId instead of failing on a 0-cid first round.
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
		check("FetchMessageQueuesInBalance 可用", false, err.Error())
	} else {
		check("FetchMessageQueuesInBalance 独占分组时拿到这 1 个队列",
			len(balanced) == 1 && balanced[0].QueueID == 0,
			fmt.Sprintf("got=%d queues=%v（没拉取过任何队列，非空只可能来自路由+38）", len(balanced), balanced))
	}
	emptyView, err := c.FetchMessageQueuesInBalance(topicName + "_NotCreated")
	check("未建过的 topic 不抛、返回空份额而不是吞下全部队列",
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
	check(fmt.Sprintf("%d 条种子消息全部写入", seedBodies), seeded == seedBodies,
		fmt.Sprintf("seeded=%d", seeded))

	// ---- 3. min / max offset ------------------------------------------------------
	// MaxOffset is the ConsumeQueue's dispatch progress, and the broker dispatches
	// the commitlog asynchronously — right after a burst of sends it can still be
	// one behind. Poll instead of reading once: the same reason python's
	// verify_pull_live.py has wait_offsets() and this script did not.
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
		check("MaxOffset 可用", false, err.Error())
	} else {
		check("MaxOffset == 种子条数", maxOffset == int64(seedBodies),
			fmt.Sprintf("max=%d want=%d", maxOffset, seedBodies))
	}
	minOffset, err := c.MinOffset(q0)
	if err != nil {
		check("MinOffset 可用", false, err.Error())
	} else {
		check("MinOffset == 0", minOffset == 0, fmt.Sprintf("min=%d", minOffset))
	}
	if _, err := c.SearchOffset(q0, common.CurrentTimeMillis()); err != nil {
		check("SearchOffset 可用", false, err.Error())
	} else {
		check("SearchOffset 可用", true, "")
	}
	if _, err := c.EarliestMsgStoreTime(q0); err != nil {
		check("EarliestMsgStoreTime 可用", false, err.Error())
	} else {
		check("EarliestMsgStoreTime 可用", true, "")
	}

	// ---- 4. the short poll at the head: everything, quickly -----------------------
	begin := time.Now()
	head, err := c.Pull(q0, "*", 0, 32)
	shortElapsed := time.Since(begin)
	if err != nil {
		check("Pull 队头短轮询返回", false, err.Error())
	} else {
		check("Pull 队头短轮询返回 FOUND", head.PullStatus == client.PullFound,
			fmt.Sprintf("status=%v msgs=%d", head.PullStatus, len(head.MsgFoundList)))
		check(fmt.Sprintf("拉到 %d 条种子消息", seedBodies), len(head.MsgFoundList) == seedBodies,
			fmt.Sprintf("got=%d", len(head.MsgFoundList)))
		check("NextBeginOffset == 种子条数", head.NextBeginOffset == int64(seedBodies),
			fmt.Sprintf("next=%d", head.NextBeginOffset))
		check("短轮询耗时远小于挂起预算", shortElapsed < *suspend/2,
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
		check("正文按队列顺序完整", okBodies, "")
		check("KEYS 属性原样保留", okKeys, "")
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
		check("空队尾短轮询返回（SUSPEND 位必须为 0）", false, err.Error())
	} else {
		check("空队尾短轮询 = NO_NEW_MSG", tail.PullStatus == client.PullNoNewMsg, "status="+tail.PullStatus.String())
		check("空队尾短轮询没有挂起", idleElapsed < *suspend/2,
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
		check("唤醒用的消息写入成功", false, sendErr.Error())
	} else {
		bump(1)
		check("唤醒用的消息写入成功", true, "")
	}
	if err != nil {
		check("长轮询被新消息唤醒", false, err.Error())
	} else {
		check("长轮询被新消息唤醒", woken.PullStatus == client.PullFound && len(woken.MsgFoundList) == 1,
			fmt.Sprintf("status=%v msgs=%d", woken.PullStatus, len(woken.MsgFoundList)))
		if len(woken.MsgFoundList) == 1 {
			check("唤醒消息正文正确", string(woken.MsgFoundList[0].Body) == wakeBody,
				fmt.Sprintf("body=%q", woken.MsgFoundList[0].Body))
		}
		check("长轮询确实等待过（不是立刻返回）", wakeElapsed >= 500*time.Millisecond,
			fmt.Sprintf("elapsed=%v", wakeElapsed))
		check("长轮询在挂起预算内提前返回", wakeElapsed < *suspend,
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
		check("空闲队尾长轮询返回", false, err.Error())
	} else {
		check("空闲队尾长轮询 = NO_NEW_MSG（挂起到期）", expired.PullStatus == client.PullNoNewMsg,
			"status="+expired.PullStatus.String())
	}
	check("空闲队尾长轮询真的挂起了", expireElapsed >= (*suspend)*7/10,
		fmt.Sprintf("elapsed=%v suspend=%v", expireElapsed, *suspend))

	// ---- 8. the caller owns the cursor -------------------------------------------
	// Pull() must NOT commit anything: the caller decides when the work is done.
	if _, err := c.Pull(q0, "*", 0, 1); err != nil {
		check("重拉队头用于位点检查", false, err.Error())
	} else {
		check("重拉队头用于位点检查", true, "")
	}
	// A fresh group has no record yet: the broker answers QUERY_NOT_FOUND, which
	// the store reports as -1. Either way it must not be the value we are about to
	// commit — that is the observable half of "Pull() does not commit".
	before, beforeErr := c.FetchConsumeOffset(q0, true)
	check("Pull 未提交任何位点（调用方持有游标）", beforeErr == nil && before != int64(committedOffset),
		fmt.Sprintf("broker offset=%d err=%v", before, beforeErr))

	if err := c.UpdateConsumeOffset(q0, committedOffset); err != nil {
		check("UpdateConsumeOffset 入本地位点表", false, err.Error())
	} else {
		check("UpdateConsumeOffset 入本地位点表", true, "")
	}
	if got, err := c.FetchConsumeOffset(q0, false); err != nil || got != committedOffset {
		check("本地读回刚才写入的位点", false, fmt.Sprintf("got=%d err=%v", got, err))
	} else {
		check("本地读回刚才写入的位点", true, fmt.Sprintf("offset=%d", got))
	}
	if err := c.PersistConsumerOffset(); err != nil {
		check("PersistConsumerOffset", false, err.Error())
	} else {
		check("PersistConsumerOffset", true, "")
	}
	// READ_FROM_STORE forces a broker round trip: this asserts the offset really
	// left the process (the Python side then confirms the same value from its own
	// connection).
	if got, err := c.FetchConsumeOffset(q0, true); err != nil || got != committedOffset {
		check("从 broker 读回位点", false, fmt.Sprintf("got=%d err=%v", got, err))
	} else {
		check("从 broker 读回位点", true, fmt.Sprintf("offset=%d", got))
	}
	if err := c.UpdateConsumeOffsetToBroker(q0, committedOffset); err != nil {
		check("UpdateConsumeOffsetToBroker", false, err.Error())
	} else {
		check("UpdateConsumeOffsetToBroker", true, "")
	}

	// ---- 9. sendMessageBack: the message must land in %RETRY%group ----------------
	// maxReconsumeTimes is 16 for a pull consumer (not the push consumer's -1), so
	// the broker re-queues instead of DLQ-ing on the first bounce. Python reads
	// %RETRY%<group> and asserts the body is there — and that %DLQ%<group> is not.
	if bounced, err := c.Pull(q0, "*", 0, 1); err != nil {
		check("取一条消息用于回投", false, err.Error())
	} else if len(bounced.MsgFoundList) == 0 {
		check("取一条消息用于回投", false, "no message")
	} else {
		check("取一条消息用于回投", true, "")
		msg := bounced.MsgFoundList[0]
		// delayLevel 1 == 1s in the default messageDelayLevel table: the retry topic
		// only sees it after the schedule service releases it, and 3 (=10s) would
		// just add wall time to the run.
		if err := c.SendMessageBack(msg, 1, q0.BrokerName); err != nil {
			check("sendMessageBack 被 broker 接受", false, err.Error())
		} else {
			check("sendMessageBack 被 broker 接受", true, fmt.Sprintf("body=%q delay=1", msg.Body))
		}
	}

	fmt.Printf("\nPASS=%d FAIL=%d COMMITTED=%d TOTAL=%d\n", passCount, failCount, committedOffset, totalCount())
	if failCount > 0 {
		os.Exit(1)
	}
}
