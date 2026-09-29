// live_producer is the Go end-to-end smoke test against a real RocketMQ 5.x
// cluster: sync / batch / oneway sends, the async chain (burst, back pressure,
// pinned queue, selector), the two-phase transaction flow, and the broker's
// transaction check-back.
//
//	go run ./examples/live_producer -ns 127.0.0.1:9876
//
// Every check prints PASS/FAIL and the process exits non-zero if any failed.
// Use a fresh topic per run (the default embeds a timestamp) so leftover state
// from an earlier run cannot make a check pass.
//
// The last line is `SENT=<n>`: how many messages the broker accepted. The
// read-back check in the other language needs that number, and deriving it here
// means adding a check here can never silently make the read-back expectation
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
	// sent counts messages the broker ACCEPTED (a 3-message batch counts 3), so
	// the read-back can assert on an exact number.
	sent int
	mu   sync.Mutex
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

// txListener records what happened so the check-back can be observed.
type txListener struct {
	mu         sync.Mutex
	executeRun int
	checkRun   int
	executeRet client.LocalTransactionState
	checkRet   client.LocalTransactionState
}

func (l *txListener) ExecuteLocalTransaction(*common.Message, any) client.LocalTransactionState {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.executeRun++
	return l.executeRet
}

func (l *txListener) CheckLocalTransaction(msg *common.MessageExt) client.LocalTransactionState {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.checkRun++
	return l.checkRet
}

func (l *txListener) counts() (int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.executeRun, l.checkRun
}

func main() {
	nsAddr := flag.String("ns", "127.0.0.1:9876", "nameserver address")
	topic := flag.String("topic", "", "topic name (default: GoLive_<unix>)")
	group := flag.String("group", "", "producer group (default: GID_GoLive_<unix>)")
	checkBackBudget := flag.Duration("check-back-budget", 90*time.Second,
		"how long to wait for the broker's transaction check-back")
	flag.Parse()

	stamp := time.Now().Unix()
	topicName := *topic
	if topicName == "" {
		topicName = fmt.Sprintf("GoLive_%d", stamp)
	}
	groupName := *group
	if groupName == "" {
		groupName = fmt.Sprintf("GID_GoLive_%d", stamp)
	}
	if !strings.Contains(*nsAddr, ":") {
		fmt.Fprintln(os.Stderr, "nameserver address must be host:port")
		os.Exit(2)
	}
	fmt.Printf("nameserver=%s topic=%s group=%s\n\n", *nsAddr, topicName, groupName)

	p, err := client.NewDefaultMQProducer(groupName)
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

	// ---- 1. sync send, and the shape of the result ----------------------------------
	msg := common.NewMessage(topicName, []byte("go-live-sync"))
	msg.SetTags("goLive")
	msg.SetKeys("go-live-key-1")
	res, err := p.Send(msg)
	if err != nil {
		check("同步发送 SEND_OK", false, " err="+err.Error())
	} else {
		uniq, _ := common.GetUniqID(msg)
		bump(1)
		check("同步发送 SEND_OK", res.SendStatus == client.SendOK,
			fmt.Sprintf(" status=%s msgId=%s", res.SendStatus, res.MsgID))
		// msgId must be the client UNIQ_KEY and offsetMsgId the broker's id —
		// swapping them breaks trace/console correlation silently.
		check("SendResult.msgId == UNIQ_KEY", res.MsgID == uniq,
			fmt.Sprintf(" msgId=%s uniq=%s", res.MsgID, uniq))
		check("SendResult.offsetMsgId 非空", res.OffsetMsgID != "",
			" offsetMsgId="+res.OffsetMsgID)
		check("queueOffset >= 0", res.QueueOffset >= 0, fmt.Sprintf(" offset=%d", res.QueueOffset))
		check("regionId 有值", res.RegionID != "", " region="+res.RegionID)
	}

	// ---- 2. batch send (SEND_BATCH_MESSAGE = 320) -----------------------------------
	batch := make([]*common.Message, 0, 3)
	for i := 1; i <= 3; i++ {
		batch = append(batch, common.NewMessage(topicName, []byte(fmt.Sprintf("go-live-batch-%d", i))))
	}
	if bres, err := p.SendBatch(batch); err != nil {
		check("批量发送 3 条", false, " err="+err.Error())
	} else {
		bump(len(batch))
		check("批量发送 3 条", bres.SendStatus == client.SendOK, " status="+bres.SendStatus.String())
	}

	// ---- 3. compression (body >= 4096 -> zlib) ---------------------------------------
	big := []byte(strings.Repeat("compress-me-", 500))
	if cres, err := p.Send(common.NewMessage(topicName, big)); err != nil {
		check("大消息压缩发送", false, " err="+err.Error())
	} else {
		bump(1)
		check("大消息压缩发送", cres.SendStatus == client.SendOK, " status="+cres.SendStatus.String())
	}

	// ---- 4. oneway ------------------------------------------------------------------
	onewayErr := p.SendOneway(common.NewMessage(topicName, []byte("go-live-oneway")), nil)
	check("单向发送不报错", onewayErr == nil, errDetail(onewayErr))
	if onewayErr == nil {
		bump(1)
	}

	// ---- 5. transaction, COMMIT -----------------------------------------------------
	commitListener := &txListener{executeRet: client.CommitMessage, checkRet: client.CommitMessage}
	txMsg := common.NewMessage(topicName, []byte("go-live-tx-commit"))
	txRes, err := p.SendMessageInTransaction(txMsg, commitListener, nil)
	if err != nil {
		check("事务提交(COMMIT) 返回", false, " err="+err.Error())
	} else {
		bump(1)
		check("事务提交(COMMIT) 返回", txRes.LocalTransactionState == client.CommitMessage,
			" state="+txRes.LocalTransactionState.String())
		check("本地事务被执行", mustExecute(commitListener) == 1,
			fmt.Sprintf(" execute=%d", mustExecute(commitListener)))
	}

	// ---- 6. transaction, UNKNOW -> broker check-back --------------------------------
	// The broker only checks a half message back over the channel this producer
	// registered via heartbeat, so this is the single most valuable check in the
	// file: it proves the ProducerData registration actually reached the broker.
	unknownListener := &txListener{executeRet: client.Unknow, checkRet: client.CommitMessage}
	unknownMsg := common.NewMessage(topicName, []byte("go-live-tx-unknown"))
	if _, err := p.SendMessageInTransaction(unknownMsg, unknownListener, nil); err != nil {
		check("事务 UNKNOW 发送", false, " err="+err.Error())
	} else {
		bump(1)
		check("事务 UNKNOW 发送", true, " (半消息已写入，等待 broker 回查)")
	}
	fmt.Printf("      ... 等待 broker 回查（最多 %s，transactionCheckInterval 默认 30s）\n", *checkBackBudget)
	deadline := time.Now().Add(*checkBackBudget)
	for {
		if _, checks := unknownListener.counts(); checks > 0 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	_, checks := unknownListener.counts()
	check("UNKNOW 触发 broker 回查", checks > 0, fmt.Sprintf(" checkLocalTransaction 调用=%d", checks))

	runAsyncChecks(p, topicName)

	fmt.Printf("\nPASS=%d FAIL=%d SENT=%d\n", passCount, failCount, sentCount())
	if failCount > 0 {
		os.Exit(1)
	}
}

// runAsyncChecks exercises the DEFAULT-ASYNC chain against the real broker.
//
// What only a real broker can answer, and why each of these is worth the round
// trips:
//
//   - the request is built ONCE and reused by every attempt, so the response
//     header's queueId is the queue that was asked for on the FIRST attempt —
//     observed here for the pinned and selector sends;
//   - the back-pressure permits must come back. The count budget is 10 and the
//     burst is 16, so 6 requests have to WAIT for a permit; if a single release
//     were missed they would time out instead of succeeding;
//   - every callback must fire exactly once. A duplicate delivery is invisible
//     against a mock but shows up here as more outcomes than sends.
func runAsyncChecks(p *client.DefaultMQProducer, topic string) {
	// 7a. one async send, with the property segment the async request build
	//     writes (TAGS / KEYS / UNIQ_KEY) intact.
	msg := common.NewMessage(topic, []byte("go-live-async"))
	msg.SetTags("goAsync")
	msg.SetKeys("go-live-async-key")
	out := make(chan asyncOutcome, 4)
	err := p.SendAsync(msg, collect(out))
	if err != nil {
		check("异步发送提交", false, errDetail(err))
	} else {
		results, fails, extra, complete := drainAsync(out, 1, 15*time.Second)
		uniq, _ := common.GetUniqID(msg)
		switch {
		case !complete:
			check("异步发送提交", false, " 回调 15s 内没回来")
		case extra > 0:
			check("异步发送回调恰好一次", false, fmt.Sprintf(" 触发了 %d 次", extra+1))
		case len(fails) > 0:
			check("异步发送提交", false, " err="+fails[0].Error())
		default:
			bump(1)
			r := results[0]
			check("异步发送提交", r.SendStatus == client.SendOK,
				fmt.Sprintf(" status=%s msgId=%s", r.SendStatus, r.MsgID))
			check("异步回调恰好一次", true, " callbacks=1")
			check("异步结果 offsetMsgId 非空", r.OffsetMsgID != "", " offsetMsgId="+r.OffsetMsgID)
			// Same rule as the sync path: msgId is the client UNIQ_KEY, not the
			// broker's id. The async request build must write UNIQ_KEY too.
			check("异步结果 msgId == UNIQ_KEY", r.MsgID == uniq,
				fmt.Sprintf(" msgId=%s uniq=%s", r.MsgID, uniq))
		}
	}

	// 7b. a concurrent burst: 16 independent chains, 16 distinct messages.
	const burst = 16
	burstOut := make(chan asyncOutcome, burst*2)
	accepted := 0
	for i := 0; i < burst; i++ {
		m := common.NewMessage(topic, []byte(fmt.Sprintf("go-live-async-burst-%02d", i)))
		if err := p.SendAsync(m, collect(burstOut)); err != nil {
			continue // full sender queue without back pressure: a legal refusal
		}
		accepted++
	}
	results, fails, extra, complete := drainAsync(burstOut, accepted, 30*time.Second)
	switch {
	case !complete:
		check(fmt.Sprintf("异步并发 %d 条全部回调", accepted), false,
			fmt.Sprintf(" 只回来 %d 条", len(results)+len(fails)))
	default:
		bump(saved(results, fails))
		check(fmt.Sprintf("异步并发 %d 条全部回调", accepted), true,
			fmt.Sprintf(" callbacks=%d", accepted))
		check("异步并发无失败", len(fails) == 0, firstErr(fails))
		check("异步并发回调不重复", extra == 0, fmt.Sprintf(" extra=%d", extra))
		statuses := 0
		uniqueID := map[string]struct{}{}
		for _, r := range results {
			if r.SendStatus == client.SendOK {
				statuses++
			}
			uniqueID[r.OffsetMsgID] = struct{}{}
		}
		check("异步并发全部 SEND_OK", statuses == len(results),
			fmt.Sprintf(" ok=%d/%d", statuses, len(results)))
		// 16 distinct broker ids means 16 distinct commits, i.e. no chain lost
		// its own request to a neighbour.
		check("异步并发 16 条互不相同", len(uniqueID) == len(results),
			fmt.Sprintf(" distinct=%d", len(uniqueID)))
	}

	// 7c. back pressure. The budget is deliberately smaller than the burst so
	//     the gate actually engages, and the LAST check is the leak detector:
	//     after everything settles every permit must be back.
	const bpBurst = 16
	p.SetEnableBackpressureForAsyncMode(true)
	p.SetBackPressureForAsyncSendNum(bpBurst - 6) // 10, i.e. 6 requests must wait
	p.SetBackPressureForAsyncSendSize(1024 * 1024)
	bpOut := make(chan asyncOutcome, bpBurst*2)
	for i := 0; i < bpBurst; i++ {
		m := common.NewMessage(topic, []byte(fmt.Sprintf("go-live-async-bp-%02d", i)))
		// Under back pressure a full queue runs inline rather than failing, so a
		// non-nil error here would be a real bug rather than a legal refusal.
		if err := p.SendAsync(m, collect(bpOut)); err != nil {
			check("背压下发异步发送", false, errDetail(err))
			return
		}
	}
	results, fails, _, complete = drainAsync(bpOut, bpBurst, 30*time.Second)
	bump(saved(results, fails))
	check("背压下 16 条全部回调", complete, fmt.Sprintf(" callbacks=%d", len(results)+len(fails)))
	check("背压下全部 SEND_OK", len(fails) == 0 && len(results) == bpBurst,
		fmt.Sprintf(" ok=%d fails=%d%s", len(results), len(fails), firstErr(fails)))
	waitNum := p.SemaphoreAsyncSendNumAvailablePermits()
	waitSize := p.SemaphoreAsyncSendSizeAvailablePermits()
	check("背压消息数许可全部归还", waitNum == p.BackPressureForAsyncSendNum(),
		fmt.Sprintf(" available=%d total=%d", waitNum, p.BackPressureForAsyncSendNum()))
	check("背压字节许可全部归还", waitSize == p.BackPressureForAsyncSendSize(),
		fmt.Sprintf(" available=%d total=%d", waitSize, p.BackPressureForAsyncSendSize()))
	p.SetEnableBackpressureForAsyncMode(false)
	p.SetBackPressureForAsyncSendSize(100 * 1024 * 1024)

	// 7d. pinned queue: the response header must report the queue that was asked
	//     for, so a mis-built header (or a queue silently re-selected) shows up.
	pinned := common.NewMessageQueue(topic, routeBrokerName(p, topic), 1)
	pinOut := make(chan asyncOutcome, 4)
	if err := p.SendAsyncToQueue(common.NewMessage(topic, []byte("go-live-async-pinned")),
		pinned, collect(pinOut)); err != nil {
		check("异步指定队列发送", false, errDetail(err))
	} else {
		results, fails, _, complete = drainAsync(pinOut, 1, 15*time.Second)
		switch {
		case !complete || len(fails) > 0:
			check("异步指定队列发送", false, fmt.Sprintf(" complete=%v%s", complete, firstErr(fails)))
		default:
			bump(1)
			check("异步指定队列发送", results[0].SendStatus == client.SendOK,
				fmt.Sprintf(" status=%s", results[0].SendStatus))
			check("异步指定队列落在 queueId=1", results[0].MessageQueue.QueueID == 1,
				fmt.Sprintf(" queueId=%d", results[0].MessageQueue.QueueID))
		}
	}

	// 7e. selector: sendSelectImpl enters the kernel with a NIL publish info, so
	//     the queue name comes from the selector and nothing else.
	selOut := make(chan asyncOutcome, 4)
	if err := p.SendAsyncBySelector(common.NewMessage(topic, []byte("go-live-async-selected")),
		pickQueue{queueID: 2}, nil, collect(selOut)); err != nil {
		check("异步选择器发送", false, errDetail(err))
	} else {
		results, fails, _, complete = drainAsync(selOut, 1, 15*time.Second)
		switch {
		case !complete || len(fails) > 0:
			check("异步选择器发送", false, fmt.Sprintf(" complete=%v%s", complete, firstErr(fails)))
		default:
			bump(1)
			check("异步选择器发送", results[0].SendStatus == client.SendOK,
				fmt.Sprintf(" status=%s", results[0].SendStatus))
			check("异步选择器落在 queueId=2", results[0].MessageQueue.QueueID == 2,
				fmt.Sprintf(" queueId=%d", results[0].MessageQueue.QueueID))
		}
	}

	// 7f. batch: this port runs the SYNC batch kernel on the sender goroutine and
	//     delivers only the result through the callback, so what is worth
	//     checking live is that the callback path carries the batch's own uniq
	//     id (BATCH_UNIQ_ID) rather than a per-message one.
	batch := make([]*common.Message, 0, 3)
	for i := 1; i <= 3; i++ {
		batch = append(batch, common.NewMessage(topic, []byte(fmt.Sprintf("go-live-async-batch-%d", i))))
	}
	batchOut := make(chan asyncOutcome, 4)
	if err := p.SendAsyncBatch(batch, collect(batchOut)); err != nil {
		check("异步批量发送", false, errDetail(err))
	} else {
		results, fails, _, complete = drainAsync(batchOut, 1, 15*time.Second)
		switch {
		case !complete || len(fails) > 0:
			check("异步批量发送", false, fmt.Sprintf(" complete=%v%s", complete, firstErr(fails)))
		default:
			bump(len(batch))
			check("异步批量发送", results[0].SendStatus == client.SendOK,
				fmt.Sprintf(" status=%s msgId=%s", results[0].SendStatus, results[0].MsgID))
			check("异步批量结果 msgId 非空", results[0].MsgID != "",
				" msgId="+results[0].MsgID)
		}
	}
}

// asyncOutcome is what one async callback delivered.
type asyncOutcome struct {
	res *client.SendResult
	err error
}

// collect returns a callback that reports every outcome on ch. The channel must
// be buffered for at least the number of sends it will serve, or a slow reader
// would block a callback worker.
func collect(ch chan<- asyncOutcome) client.SendCallback {
	return client.SendCallbackFunc{
		SuccessFn: func(r *client.SendResult) { ch <- asyncOutcome{res: r} },
		ExceptFn:  func(e error) { ch <- asyncOutcome{err: e} },
	}
}

// drainAsync waits for `n` outcomes, then gives a duplicate delivery a window to
// show up: `extra` counts callbacks beyond the expected number, which is the
// only way a double-fire is observable.
func drainAsync(ch <-chan asyncOutcome, n int, budget time.Duration) (
	results []*client.SendResult, fails []error, extra int, complete bool) {

	if n == 0 {
		return nil, nil, 0, true
	}
	deadline := time.NewTimer(budget)
	defer deadline.Stop()
	for len(results)+len(fails) < n {
		select {
		case o := <-ch:
			if o.err != nil {
				fails = append(fails, o.err)
			} else {
				results = append(results, o.res)
			}
		case <-deadline.C:
			return results, fails, 0, false
		}
	}
	settle := time.NewTimer(300 * time.Millisecond)
	defer settle.Stop()
	for {
		select {
		case o := <-ch:
			extra++
			if o.err != nil {
				fails = append(fails, o.err)
			} else {
				results = append(results, o.res)
			}
		case <-settle.C:
			return results, fails, extra, true
		}
	}
}

// pickQueue always returns the queue with the requested id — the selector sees
// the queues WITHOUT the namespace (Java invokeMessageQueueSelector).
type pickQueue struct{ queueID int32 }

func (s pickQueue) Select(mqs []common.MessageQueue, _ *common.Message, _ any) (common.MessageQueue, error) {
	for _, mq := range mqs {
		if mq.QueueID == s.queueID {
			return mq, nil
		}
	}
	return common.MessageQueue{}, fmt.Errorf("no queue with id %d", s.queueID)
}

// routeBrokerName asks the producer's route table for a writable broker of the
// topic, so the pinned-queue check does not hardcode a broker name.
func routeBrokerName(p *client.DefaultMQProducer, topic string) string {
	mqs, err := p.FetchPublishMessageQueues(topic)
	if err != nil {
		return ""
	}
	for _, mq := range mqs {
		return mq.BrokerName
	}
	return ""
}

func saved(results []*client.SendResult, fails []error) int {
	ok := 0
	for _, r := range results {
		if r.SendStatus == client.SendOK {
			ok++
		}
	}
	return ok
}

func firstErr(fails []error) string {
	if len(fails) == 0 {
		return ""
	}
	return " err=" + fails[0].Error()
}

func bump(n int) {
	mu.Lock()
	defer mu.Unlock()
	sent += n
}

func sentCount() int {
	mu.Lock()
	defer mu.Unlock()
	return sent
}

func mustExecute(l *txListener) int {
	execute, _ := l.counts()
	return execute
}

func errDetail(err error) string {
	if err == nil {
		return ""
	}
	return " err=" + err.Error()
}
