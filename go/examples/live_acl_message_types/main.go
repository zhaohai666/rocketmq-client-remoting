// live_acl_message_types is the Go ACL end-to-end proof against a real
// RocketMQ cluster with authenticationEnabled=true: every message type the
// port supports must be SENT and RECEIVED with broker signatures enabled.
//
//	go run ./examples/live_acl_message_types -ns 127.0.0.1:9876 \
//	    -ak <accessKey> -sk <secretKey>
//
// Coverage (all against the live broker, one run):
//
//	A0  negative probe — a send WITHOUT credentials must be rejected with the
//	    broker's own response code NO_PERMISSION(16), asserted on the code,
//	    not merely on "an error came back";
//	B   credentialed sends: sync, async+callback, oneway, batch
//	    (SEND_BATCH_MESSAGE), delay level, timer (TIMER_DELIVER_MS),
//	    transaction (half -> local commit -> END_TRANSACTION), tag-producing,
//	    orderly send through a queue selector, keys + user property;
//	C  credentialed consumption: every message type read back, the delay and
//	    timer gaps proven by store_timestamp - born_timestamp, the committed
//	    transaction message visible as a normal consumable message, tag
//	    filtering, and per-queue order preserved by the orderly listener;
//	D  server-side KEYS index query through the admin client.
//
// ORDER MATTERS: the ACL hook belongs to the shared client instance and is
// installed only when that instance is first created (Instance.EnsureRPCHooks
// is once-per-instance). The uncredentialed probe therefore runs FIRST, on its
// own clientId, is shut down, and the run sleeps 1s before any credentialed
// client boots — so no credentialed client can ever land on the probe's
// hook-less instance.
//
// Every check prints PASS/FAIL and the process exits non-zero if any failed.
// Topic and group names embed a timestamp, so leftover state from an earlier
// run can never make a check pass, and the entry is otherwise re-runnable.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/client"
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
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
		fmt.Printf("PASS  %s%s\n", name, detail)
		return
	}
	failCount++
	fmt.Printf("FAIL  %s%s\n", name, detail)
}

// skip marks a capability the entry could not exercise, so a missing feature
// is never mistaken for a passing check.
func skip(name, why string) {
	mu.Lock()
	defer mu.Unlock()
	fmt.Printf("SKIP  %s%s\n", name, why)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "live_acl_message_types: "+format+"\n", args...)
	os.Exit(2)
}

// ------------------------------------------------------------ listeners

// concurrentCollector records every delivered message.
type concurrentCollector struct {
	mu   sync.Mutex
	recs []*common.MessageExt
}

func (c *concurrentCollector) ConsumeMessage(msgs []*common.MessageExt,
	_ *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recs = append(c.recs, msgs...)
	return client.ConsumeSuccess
}

func (c *concurrentCollector) snapshot() []*common.MessageExt {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*common.MessageExt(nil), c.recs...)
}

func (c *concurrentCollector) count() int { return len(c.snapshot()) }

// find returns the first record whose body equals want.
func (c *concurrentCollector) find(want string) (*common.MessageExt, bool) {
	for _, r := range c.snapshot() {
		if string(r.Body) == want {
			return r, true
		}
	}
	return nil, false
}

// bodySet returns the set of bodies collected.
func (c *concurrentCollector) bodySet() map[string]struct{} {
	set := map[string]struct{}{}
	for _, r := range c.snapshot() {
		set[string(r.Body)] = struct{}{}
	}
	return set
}

// orderlyCollector records deliveries in callback order, per queue.
type orderlyCollector struct {
	mu   sync.Mutex
	recs []*common.MessageExt
}

func (o *orderlyCollector) ConsumeMessage(msgs []*common.MessageExt,
	_ *client.ConsumeOrderlyContext) client.ConsumeOrderlyStatus {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.recs = append(o.recs, msgs...)
	return client.OrderlySuccess
}

func (o *orderlyCollector) snapshot() []*common.MessageExt {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]*common.MessageExt(nil), o.recs...)
}

// ------------------------------------------------------------ helpers

// brokerErrorParts digs out the broker response code and remark from an error.
// A nil code (no *common.Error in the chain) reports 0 so the caller's
// "code == 16" assertion fails loudly instead of passing on vague text.
func brokerErrorParts(err error) (int32, string) {
	var e *common.Error
	if errors.As(err, &e) {
		code, _ := e.ResponseCode()
		return code, e.Remark
	}
	return 0, ""
}

// waitFor polls cond until it is true or the budget runs out.
func waitFor(budget time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(budget)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// fixedHashSelector pins a message to ONE queue for a given key
// (key -> index over the sorted queue list) — the classic orderly-shard
// selector. The same arg must always resolve to the same queue while the
// route is stable, which the caller asserts from the send results.
type fixedHashSelector struct{ key int }

func (s fixedHashSelector) Select(mqs []common.MessageQueue, _ *common.Message, arg any) (common.MessageQueue, error) {
	if len(mqs) == 0 {
		return common.MessageQueue{}, fmt.Errorf("no queues to select from")
	}
	k, ok := arg.(int)
	if !ok {
		return common.MessageQueue{}, fmt.Errorf("selector arg must be int, got %T", arg)
	}
	sorted := append([]common.MessageQueue(nil), mqs...)
	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		if a.BrokerName != b.BrokerName {
			return a.BrokerName < b.BrokerName
		}
		return a.QueueID < b.QueueID
	})
	return sorted[k%len(sorted)], nil
}

// commitListener always commits the local transaction — END_TRANSACTION then
// carries COMMIT to the broker and the half message becomes consumable.
type commitListener struct {
	mu       sync.Mutex
	executed int
}

func (l *commitListener) ExecuteLocalTransaction(*common.Message, any) client.LocalTransactionState {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.executed++
	return client.CommitMessage
}

func (l *commitListener) CheckLocalTransaction(*common.MessageExt) client.LocalTransactionState {
	return client.CommitMessage
}

func (l *commitListener) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.executed
}

// ------------------------------------------------------------ main

const (
	// delayLevel 3 = 10s on the broker's default messageDelayLevel table
	// ("1s 5s 10s 30s 1m ..."). The consumer-side assertion tolerates clock
	// rounding, hence >= 8s rather than >= 10s.
	delayLevel            = 3
	delayLevelMinMillis   = 8000
	timerDeliverAheadMs   = 5000
	timerDeliverMinMillis = 3000
)

func main() {
	nsAddr := flag.String("ns", "127.0.0.1:9876", "nameserver address")
	accessKey := flag.String("ak", "", "ACL accessKey (required)")
	secretKey := flag.String("sk", "", "ACL secretKey (required)")
	flag.Parse()

	if *accessKey == "" || *secretKey == "" {
		fatal("-ak and -sk are required: the cluster has authenticationEnabled=true and " +
			"every broker RPC without a valid signature is rejected with NO_PERMISSION(16)")
	}

	stamp := time.Now().Unix()
	mainTopic := fmt.Sprintf("GoAclMain_%d", stamp)
	tagTopic := fmt.Sprintf("GoAclTag_%d", stamp)
	orderTopic := fmt.Sprintf("GoAclOrd_%d", stamp)
	prodGroup := fmt.Sprintf("GID_GoAclProd_%d", stamp)
	mainGroup := fmt.Sprintf("GID_GoAclMain_%d", stamp)
	tagGroup := fmt.Sprintf("GID_GoAclTag_%d", stamp)
	orderGroup := fmt.Sprintf("GID_GoAclOrd_%d", stamp)
	fmt.Printf("nameserver=%s ak=%s topics=%s/%s/%s\n\n", *nsAddr, *accessKey, mainTopic, tagTopic, orderTopic)

	// ---- A0. negative probe: no credentials must be rejected with code 16 ----
	runUncredentialedProbe(*nsAddr, mainTopic, prodGroup)
	// The probe's instance (and its hook-less RPC chain) is torn down here;
	// give the registry a moment so the credentialed clients below cannot
	// possibly attach to it.
	time.Sleep(1 * time.Second)

	// ---- credentialed clients below: each gets its own instance, and the
	//      ACL hook is installed when that instance is first created ----
	hook, err := remoting.NewAclClientRPCHook(remoting.NewSessionCredentials(*accessKey, *secretKey))
	if err != nil {
		fatal("acl hook: %v", err)
	}

	msgKey := fmt.Sprintf("go-acl-key-%d", stamp)
	sendPhase(*nsAddr, hook, prodGroup, mainTopic, tagTopic, orderTopic, msgKey)
	consumePhase(*nsAddr, hook, mainTopic, tagTopic, orderTopic, mainGroup, tagGroup, orderGroup, msgKey)
	adminPhase(*nsAddr, hook, mainTopic, msgKey)

	fmt.Printf("\nPASS=%d FAIL=%d\n", passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
}

// runUncredentialedProbe asserts the cluster really enforces ACL: a send with
// no session credentials must come back as broker response code 16
// (NO_PERMISSION), not merely "some error".
func runUncredentialedProbe(ns, topic, group string) {
	fmt.Println("---- A0 uncredentialed negative probe (must run first: hook belongs to instance, loaded only when instance is first created) ----")
	p, err := client.NewDefaultMQProducer(group + "_NoAuth")
	if err != nil {
		check("uncredentialed send rejected (NO_PERMISSION=16)", false, " producer build: "+err.Error())
		return
	}
	p.SetNameServerAddresses([]string{ns})
	p.SetInstanceName("GoAclNoAuthProbe")
	p.SetSendMsgTimeout(3000)
	p.SetRetryTimesWhenSendFailed(0)
	if err := p.Start(); err != nil {
		check("uncredentialed send rejected (NO_PERMISSION=16)", false, " start: "+err.Error())
		return
	}
	defer p.Shutdown()

	res, err := p.Send(common.NewMessage(topic, []byte("go-acl-must-be-rejected")))
	if err == nil {
		check("uncredentialed send rejected (NO_PERMISSION=16)", false,
			fmt.Sprintf(" unexpectedly accepted msgId=%s (cluster did not enforce ACL?)", res.MsgID))
		return
	}
	code, remark := brokerErrorParts(err)
	check("uncredentialed send rejected (NO_PERMISSION=16)", code == remoting.RespNoPermission,
		fmt.Sprintf(" code=%d remark=%q", code, remark))
}

// sendPhase boots one credentialed producer and exercises every send path.
func sendPhase(ns string, hook remoting.RPCHook, group, mainTopic, tagTopic, orderTopic, msgKey string) {
	fmt.Println("\n---- B credentialed sends: all message types ----")
	p, err := client.NewDefaultMQProducer(group)
	if err != nil {
		fatal("producer: %v", err)
	}
	p.SetRpcHook(hook)
	p.SetInstanceName("GoAclProducer")
	p.SetNameServerAddresses([]string{ns})
	p.SetSendMsgTimeout(5000)
	if err := p.Start(); err != nil {
		fatal("producer start: %v", err)
	}
	defer p.Shutdown()
	check("ACL producer started (signed heartbeat/route)", true, "")

	// B1 sync normal + keys + user property
	syncMsg := common.NewMessage(mainTopic, []byte("acl-sync"))
	syncMsg.SetTags("aclTag")
	syncMsg.SetKeys(msgKey)
	syncMsg.PutProperty("x-go-acl-prop", "passed-through")
	res, err := p.Send(syncMsg)
	check("sync send (SEND_MESSAGE)", err == nil && res != nil && res.SendStatus == client.SendOK,
		detail(res, err))

	// B2 async with callback
	asyncMsg := common.NewMessage(mainTopic, []byte("acl-async"))
	out := make(chan *client.SendResult, 4)
	aerr := make(chan error, 4)
	cb := client.SendCallbackFunc{
		SuccessFn: func(r *client.SendResult) { out <- r },
		ExceptFn:  func(e error) { aerr <- e },
	}
	if err := p.SendAsync(asyncMsg, cb); err != nil {
		check("async send (callback commit)", false, " err="+err.Error())
	} else {
		select {
		case r := <-out:
			check("async send (callback returns SEND_OK)", r.SendStatus == client.SendOK,
				fmt.Sprintf(" status=%s msgId=%s", r.SendStatus, r.MsgID))
		case e := <-aerr:
			check("async send (callback returns SEND_OK)", false, " err="+e.Error())
		case <-time.After(15 * time.Second):
			check("async send (callback returns SEND_OK)", false, " callback did not return within 15s")
		}
	}

	// B3 oneway
	owErr := p.SendOneway(common.NewMessage(mainTopic, []byte("acl-oneway")), nil)
	check("oneway send (ONEWAY) no local error", owErr == nil, errText(owErr))

	// B4 batch (SEND_BATCH_MESSAGE)
	batch := make([]*common.Message, 0, 3)
	for i := 1; i <= 3; i++ {
		batch = append(batch, common.NewMessage(mainTopic,
			[]byte(fmt.Sprintf("acl-batch-%d", i))))
	}
	bres, err := p.SendBatch(batch)
	check("batch send (SEND_BATCH_MESSAGE) 3 messages", err == nil && bres != nil && bres.SendStatus == client.SendOK,
		detail(bres, err))

	// B5 delay level — the level itself only needs to be accepted here;
	//     store_timestamp - born_timestamp >= the level is asserted on the
	//     consumed copy in the consume phase.
	delayMsg := common.NewMessage(mainTopic, []byte("acl-delay"))
	delayMsg.SetDelayTimeLevel(delayLevel)
	dres, err := p.Send(delayMsg)
	check("delay message (LEVEL "+strconv.Itoa(delayLevel)+") sent", err == nil && dres != nil && dres.SendStatus == client.SendOK,
		detail(dres, err))

	// B6 scheduled/timer message — TIMER_DELIVER_MS 5s ahead.
	timerMsg := common.NewMessage(mainTopic, []byte("acl-timer"))
	timerMsg.PutProperty(common.PropertyTimerDeliverMs,
		strconv.FormatInt(time.Now().UnixMilli()+timerDeliverAheadMs, 10))
	tres, err := p.Send(timerMsg)
	check("scheduled message (TIMER_DELIVER_MS+5s) sent", err == nil && tres != nil && tres.SendStatus == client.SendOK,
		detail(tres, err))

	// B7 transaction: half message -> ExecuteLocalTransaction(COMMIT) ->
	//     END_TRANSACTION. Becoming CONSUMABLE is proven by the read-back.
	txListener := &commitListener{}
	txMsg := common.NewMessage(mainTopic, []byte("acl-tx"))
	txRes, err := p.SendMessageInTransaction(txMsg, txListener, nil)
	check("transaction message half->commit (END_TRANSACTION=COMMIT)",
		err == nil && txRes != nil && txRes.LocalTransactionState == client.CommitMessage && txListener.count() == 1,
		txDetail(txRes, err, txListener))

	// B8 tag-producing side: 2x TagA + 2x TagB, filtered on consumption.
	tagOK := 0
	for _, spec := range []struct{ tag, body string }{
		{"TagA", "acl-tag-hit-1"}, {"TagB", "acl-tag-miss-1"},
		{"TagA", "acl-tag-hit-2"}, {"TagB", "acl-tag-miss-2"},
	} {
		m := common.NewMessage(tagTopic, []byte(spec.body))
		m.SetTags(spec.tag)
		if r, err := p.Send(m); err == nil && r.SendStatus == client.SendOK {
			tagOK++
		}
	}
	check("tag messages 4 (2xTagA 2xTagB) sent", tagOK == 4, fmt.Sprintf(" ok=%d", tagOK))

	// B9 orderly send through the queue selector: 5 messages, same arg, must
	//     land on one queue in send order.
	first := ""
	allSame, ordOK := true, 0
	ordFail := ""
	for i := 0; i < 5; i++ {
		r, err := p.SendBySelector(common.NewMessage(orderTopic,
			[]byte(fmt.Sprintf("acl-order-%d", i))), fixedHashSelector{key: 7}, 7)
		if err != nil || r == nil || r.SendStatus != client.SendOK {
			ordFail = fmt.Sprintf(" i=%d err=%v res=%s", i, err, sendResultText(r))
			break
		}
		loc := fmt.Sprintf("%s@%d", r.MessageQueue.BrokerName, r.MessageQueue.QueueID)
		if first == "" {
			first = loc
		} else if loc != first {
			allSame = false
		}
		ordOK++
	}
	check("orderly messages 5 (selector) sent", ordOK == 5,
		fmt.Sprintf(" ok=%d%s", ordOK, ordFail))
	check("orderly messages same arg lands same queue", ordOK == 5 && allSame, " queue="+first)
}

// consumePhase starts three credentialed push consumers and asserts the
// read-back of everything the send phase wrote.
func consumePhase(ns string, hook remoting.RPCHook, mainTopic, tagTopic, orderTopic,
	mainGroup, tagGroup, orderGroup, msgKey string) {
	fmt.Println("\n---- C credentialed consumption: read back and assert all message types ----")

	// C1 main topic: all 9 messages, incl. delay/timer/tx/oneway read-back.
	mainCol := &concurrentCollector{}
	mainCons := newPushConsumer(ns, hook, mainGroup, "GoAclMainConsumer")
	mainCons.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	if err := mainCons.Subscribe(mainTopic, "*"); err != nil {
		fatal("subscribe main: %v", err)
	}
	mainCons.SetConcurrentlyListener(mainCol)
	if err := mainCons.Start(); err != nil {
		fatal("main consumer start: %v", err)
	}
	defer mainCons.Shutdown()

	want := []string{"acl-sync", "acl-async", "acl-oneway",
		"acl-batch-1", "acl-batch-2", "acl-batch-3",
		"acl-delay", "acl-timer", "acl-tx"}
	// 75s covers the 10s delay level plus the timer wheel and normal pull
	// latency; the wait ends the moment the count is in.
	gotAll := waitFor(75*time.Second, func() bool { return mainCol.count() >= len(want) })
	seen := mainCol.bodySet()
	missing := []string{}
	for _, w := range want {
		if _, ok := seen[w]; !ok {
			missing = append(missing, w)
		}
	}
	check("all 9 messages consumed and read back", gotAll && len(missing) == 0,
		fmt.Sprintf(" got=%d missing=%v", mainCol.count(), missing))

	// oneway has no send-side receipt: the read-back IS its proof of arrival.
	if m, ok := mainCol.find("acl-oneway"); ok {
		check("oneway message truly persisted (read back)", true, fmt.Sprintf(" offsetMsgId=%s", m.OffsetMsgID))
	} else {
		check("oneway message truly persisted (read back)", false, " acl-oneway not read")
	}

	// user property + keys + tag pass-through on the sync message.
	if m, ok := mainCol.find("acl-sync"); ok {
		prop, _ := m.GetProperty("x-go-acl-prop")
		check("user-defined property passthrough", prop == "passed-through", fmt.Sprintf(" x-go-acl-prop=%q", prop))
		keys, _ := m.GetProperty(common.PropertyKeys)
		check("message KEYS passthrough", keys == msgKey, fmt.Sprintf(" KEYS=%q want=%q", keys, msgKey))
		tags, _ := m.GetProperty(common.PropertyTags)
		check("message TAGS passthrough", tags == "aclTag", fmt.Sprintf(" TAGS=%q", tags))
	} else {
		skip("user-defined property passthrough", " (acl-sync not consumed, cannot check property)")
	}

	// the committed transaction message must be consumable like any normal
	// message — that is what proves half -> COMMIT -> END_TRANSACTION worked.
	if m, ok := mainCol.find("acl-tx"); ok {
		check("transaction message consumable after commit (read back)", true, fmt.Sprintf(" msgId=%s", m.MsgID))
	} else {
		check("transaction message consumable after commit (read back)", false, " acl-tx not read")
	}

	// delay level: the broker only releases the message after the level has
	// elapsed; store_timestamp is the release append, born_timestamp the send.
	assertDelay(mainCol, "acl-delay", delayLevelMinMillis, "delay message store-born >= 8s (LEVEL 3=10s)")
	// timer: TIMER_DELIVER_MS 5s ahead -> release ~5s after the send.
	assertDelay(mainCol, "acl-timer", timerDeliverMinMillis, "scheduled message store-born >= 3s (deliver-ahead 5s)")

	// C2 tag filtering: exactly the two TagA messages, never a TagB.
	tagCol := &concurrentCollector{}
	tagCons := newPushConsumer(ns, hook, tagGroup, "GoAclTagConsumer")
	tagCons.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	if err := tagCons.Subscribe(tagTopic, "TagA"); err != nil {
		fatal("subscribe tag: %v", err)
	}
	tagCons.SetConcurrentlyListener(tagCol)
	if err := tagCons.Start(); err != nil {
		fatal("tag consumer start: %v", err)
	}
	defer tagCons.Shutdown()
	waitFor(30*time.Second, func() bool { return tagCol.count() >= 2 })
	time.Sleep(3 * time.Second) // window for a wrongly-delivered TagB to show up
	tset := tagCol.bodySet()
	_, hit1 := tset["acl-tag-hit-1"]
	_, hit2 := tset["acl-tag-hit-2"]
	_, miss1 := tset["acl-tag-miss-1"]
	_, miss2 := tset["acl-tag-miss-2"]
	check("tag filtering consumption: only TagA 2 received, TagB 0",
		hit1 && hit2 && !miss1 && !miss2 && tagCol.count() == 2,
		fmt.Sprintf(" got=%d bodies=%v", tagCol.count(), sortedKeys(tset)))

	// C3 orderly consumption: the 5 selector messages must arrive on one queue
	//     with strictly increasing offsets AND in send order.
	ordCol := &orderlyCollector{}
	ordCons := newPushConsumer(ns, hook, orderGroup, "GoAclOrderConsumer")
	ordCons.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	if err := ordCons.Subscribe(orderTopic, "*"); err != nil {
		fatal("subscribe order: %v", err)
	}
	ordCons.SetOrderlyListener(ordConsListener{col: ordCol})
	if err := ordCons.Start(); err != nil {
		fatal("order consumer start: %v", err)
	}
	defer ordCons.Shutdown()
	gotOrder := waitFor(30*time.Second, func() bool { return len(ordCol.snapshot()) >= 5 })
	recs := ordCol.snapshot()
	oneQueue, offsetsAsc, bodiesAsc := true, true, true
	for i, r := range recs {
		if r.QueueID != recs[0].QueueID || r.BrokerName != recs[0].BrokerName {
			oneQueue = false
		}
		if i > 0 {
			if r.QueueOffset <= recs[i-1].QueueOffset {
				offsetsAsc = false
			}
			if string(r.Body) != fmt.Sprintf("acl-order-%d", i) {
				bodiesAsc = false
			}
		}
	}
	check("orderly consumption received 5 messages", gotOrder && len(recs) == 5, fmt.Sprintf(" got=%d", len(recs)))
	check("orderly consumption all on same queue with increasing offset", oneQueue && offsetsAsc,
		fmt.Sprintf(" queue=%s same=%v offsetsAsc=%v", recs0Name(recs), oneQueue, offsetsAsc))
	check("orderly consumption preserves send order acl-order-0..4", bodiesAsc, "")
}

// adminPhase proves the server-side KEYS index query over the ACL hook.
func adminPhase(ns string, hook remoting.RPCHook, mainTopic, msgKey string) {
	fmt.Println("\n---- D admin: query by KEYS ----")
	admin := client.NewDefaultMQAdminExt(hook)
	admin.SetInstanceName("GoAclAdmin")
	admin.SetNameServerAddresses([]string{ns})
	if err := admin.Start(); err != nil {
		check("ACL admin started", false, err.Error())
		return
	}
	defer admin.Shutdown()
	check("ACL admin started", true, "")

	// The broker's index build service flushes on its own cadence, so poll.
	var found []*common.MessageExt
	var qerr error
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		found, qerr = admin.QueryMessageByKey(mainTopic, msgKey, 32)
		if qerr == nil && len(found) > 0 {
			break
		}
		time.Sleep(2 * time.Second)
	}
	check("server-side KEYS query hit", qerr == nil && len(found) >= 1,
		fmt.Sprintf(" n=%d err=%v", len(found), qerr))
	if len(found) >= 1 {
		body := string(found[0].Body)
		check("KEYS query returned correct message", body == "acl-sync", fmt.Sprintf(" body=%q", body))
	}
}

// ------------------------------------------------------------ plumbing

func newPushConsumer(ns string, hook remoting.RPCHook, group, instance string) *client.DefaultMQPushConsumer {
	c, err := client.NewDefaultMQPushConsumer(group)
	if err != nil {
		fatal("consumer %s: %v", group, err)
	}
	c.SetRpcHook(hook)
	c.SetInstanceName(instance)
	c.SetNameServerAddresses([]string{ns})
	return c
}

// ordConsListener adapts the collector to the orderly SPI (the collector type
// and the listener type are distinct method sets, so this wraps rather than
// doubles as both).
type ordConsListener struct{ col *orderlyCollector }

func (l ordConsListener) ConsumeMessage(msgs []*common.MessageExt,
	_ *client.ConsumeOrderlyContext) client.ConsumeOrderlyStatus {
	return l.col.ConsumeMessage(msgs, nil)
}

// assertDelay compares store_timestamp - born_timestamp on the consumed copy.
func assertDelay(col *concurrentCollector, body string, minMillis int64, name string) {
	m, ok := col.find(body)
	if !ok {
		check(name, false, " not read: "+body)
		return
	}
	delta := m.StoreTimestamp - m.BornTimestamp
	check(name, delta >= minMillis,
		fmt.Sprintf(" store-born=%dms born=%d store=%d", delta, m.BornTimestamp, m.StoreTimestamp))
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func recs0Name(recs []*common.MessageExt) string {
	if len(recs) == 0 {
		return "<none>"
	}
	return fmt.Sprintf("%s@%d", recs[0].BrokerName, recs[0].QueueID)
}

func detail(res *client.SendResult, err error) string {
	if err != nil {
		code, remark := brokerErrorParts(err)
		return fmt.Sprintf(" err=%v code=%d remark=%q", err, code, remark)
	}
	if res == nil {
		return " nil result"
	}
	return fmt.Sprintf(" status=%s msgId=%s", res.SendStatus, res.MsgID)
}

func txDetail(res *client.TransactionSendResult, err error, l *commitListener) string {
	if err != nil {
		return " err=" + err.Error()
	}
	if res == nil {
		return " nil result"
	}
	return fmt.Sprintf(" state=%s localExecuted=%d", res.LocalTransactionState, l.count())
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return " err=" + err.Error()
}

func sendResultText(r *client.SendResult) string {
	if r == nil {
		return " <nil result>"
	}
	return fmt.Sprintf(" <status=%s msgId=%s>", r.SendStatus, r.MsgID)
}
