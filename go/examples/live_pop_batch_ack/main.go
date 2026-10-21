// Command live_pop_batch_ack verifies BATCH_ACK_MESSAGE(200151) against a real
// RocketMQ 5.x broker.
//
//	go run ./examples/live_pop_batch_ack -ns 127.0.0.1:9876 [-legs l1,l2,l3]
//
// Why this needs a live leg of its own: the POP push consumer answers every
// message with one ACK_MESSAGE(200051) and never builds a batch, so nothing in
// the normal runtime exercises the batch wire path. The whole capability — the
// grouping key, the bit index, the base64 BitSet, the "no custom header" request
// shape — is only reachable through the exported primitive, and a primitive
// nobody has ever pointed at a broker is a primitive that may not work.
//
// The batch body carries no per-message offsets, only a start offset plus a bit
// set, so the one question a return code cannot answer is "which messages did the
// broker actually clear". As with every POP leg, the criterion is a timed window
// plus broker-side observable state:
//
//	L1 ONE REQUEST, WHOLE BATCH. Pop a full batch, ack it with a single 200151,
//	   then keep popping both the business topic and the group's POP retry topic
//	   for 2.5x the invisibility window. If the offsets or the bits are wrong, the
//	   checkpoints survive and the revive service puts the messages back inside
//	   that window.
//	L2 PAIRED CONTROL. Send the same batch with exactly one checkpoint left out.
//	   That message must come back and the acked ones must not — the assertion
//	   that fails if the broker (or the builder) treats a bit set as "the whole
//	   batch". Its follow-up also pins the retry direction: the revived
//	   message's checkpoint is addressed with marker "1" (the retry topic's route
//	   reaching the name server is the broker-side proof), and the batch ack that
//	   clears it carries the BUSINESS topic, because the broker re-derives the
//	   retry topic from marker + group. The window that waits for the revival
//	   stops at the first sighting: a sighted checkpoint that is left to expire is
//	   revived before the ack lands, and no ack can take a revival back, so a
//	   longer window would fail on timing instead of on the wire path.
//	L3 NO OVER-MERGING. Two pop rounds of the same queue acked by one request.
//	   Checkpoints of different rounds must land in different entries
//	   (retry@queueId@ckOffset@popTime is the merge key); collapsing them would
//	   acknowledge offsets nobody popped.
//
// The broker needs no special switch for this: 200151 is registered
// unconditionally and the revive side handles the batch ack tag the same way, so
// the standard test cluster config already serves it. Every topic is created up
// front, and each leg waits for the broker to report the expected queue depth
// before popping — a partially dispatched batch would make "one request cleared
// the batch" unprovable.
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
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

const (
	// Ten seconds of invisibility keeps the windows short enough for the whole
	// run to stay in a few minutes while still being far above the broker's
	// 1s revive scan interval, so a missed ack is always observed.
	invisibleMillis int64 = 10000

	// A checkpoint that no ack covered is revived at popTime + invisibleTime, so
	// 2.5x the window is a generous bound on "it should have come back by now".
	observeWindow = time.Duration(invisibleMillis*5/2) * time.Millisecond

	// L2's first window is a probe rather than a fixed window: it stops shortly
	// after the dropped message is sighted. Every sighting leaves a fresh
	// checkpoint that is invisible for invisibleMillis, so a long fixed window
	// watches the same message revive, get popped and revive again, and the batch
	// ack then carries a checkpoint whose invisibility already ran out — the
	// broker moved it on, the ack cannot take it back, and the final silence
	// check fails on timing instead of on anything the client did.
	reviveWaitWindow = 40 * time.Second
	reviveTail       = 3 * time.Second

	popTimeoutOverhead int64 = 5000
	rpcTimeoutMillis   int64 = 5000
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

// ------------------------------------------------------------------ wire driver

// verifier drives the POP wire commands directly. It borrows the admin's client
// instance on purpose: the admin already knows how to reach the broker master,
// and a POP consumer would auto-ack everything, which is exactly what must not
// happen here.
type verifier struct {
	admin      *client.DefaultMQAdminExt
	inst       *client.Instance
	brokerName string
	addr       string
}

func newAdmin(ns, instance string) *client.DefaultMQAdminExt {
	a := client.NewDefaultMQAdminExt(nil)
	a.SetNameServerAddresses([]string{ns})
	a.SetInstanceName(instance)
	a.SetTimeoutMillis(10_000)
	return a
}

// ensureTopic creates the topic with a single queue so one POP can see the whole
// batch: multi-queue topics would spread it and the batch semantics under test
// are per-queue.
func ensureTopic(ns, instance, topic string) error {
	a := newAdmin(ns, instance)
	if err := a.Start(); err != nil {
		return err
	}
	defer a.Shutdown()
	return a.CreateTopic(common.DefaultTopic, topic, 1, 0)
}

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

// routeVisible waits for the topic route to reach the name server. The POP retry
// topic is created by the revive service when it moves a checkpoint there, so its
// route appearing is broker-side evidence of the revival itself.
func routeVisible(ns, instance, topic string, window time.Duration) bool {
	a := newAdmin(ns, instance)
	if err := a.Start(); err != nil {
		return false
	}
	defer a.Shutdown()
	deadline := time.Now().Add(window)
	for {
		route, err := a.ExamineTopicRoute(topic)
		if err == nil && route != nil && len(route.BrokerDatas) > 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func newVerifier(ns, instance, topic, brokerName string) (*verifier, error) {
	a := newAdmin(ns, instance)
	if err := a.Start(); err != nil {
		return nil, err
	}
	inst, err := a.Instance()
	if err != nil {
		a.Shutdown()
		return nil, err
	}
	addr, _, found := inst.FindBrokerAddressInSubscribe(brokerName, int64(common.MasterID), true)
	if !found {
		if _, err := inst.UpdateTopicRouteInfoFromNameServer(topic, 0, false); err != nil {
			a.Shutdown()
			return nil, err
		}
		addr, _, found = inst.FindBrokerAddressInSubscribe(brokerName, int64(common.MasterID), true)
	}
	if !found {
		a.Shutdown()
		return nil, fmt.Errorf("broker %s master address not resolvable", brokerName)
	}
	return &verifier{admin: a, inst: inst, brokerName: brokerName, addr: addr}, nil
}

func (v *verifier) shutdown() { v.admin.Shutdown() }

// pop sends POP_MESSAGE(200050). queueID -1 asks the broker for every queue of
// the topic, which is what the observation loop wants; the batch pops use the
// real queue id so the batch is deterministic.
func (v *verifier) pop(topic, group string, queueID int32, maxNums int32, pollMillis int64) (*client.PopResult, error) {
	header := &remoting.PopMessageRequestHeader{
		Bname:         remoting.StrPtr(v.brokerName),
		ConsumerGroup: remoting.StrPtr(group),
		Topic:         remoting.StrPtr(topic),
		QueueID:       remoting.I32Ptr(queueID),
		MaxMsgNums:    remoting.I32Ptr(maxNums),
		InvisibleTime: remoting.I64Ptr(invisibleMillis),
		PollTime:      remoting.I64Ptr(pollMillis),
		BornTime:      remoting.I64Ptr(common.CurrentTimeMillis()),
		// MIN: a brand-new group resolves its initial offset to the earliest
		// message. The broker only reads this knob when it has to pick one.
		InitMode: remoting.I32Ptr(0),
		Order:    remoting.BoolPtr(false),
	}
	return v.inst.PopMessage(v.brokerName, v.addr, header, "", pollMillis+popTimeoutOverhead)
}

// buildBatchAck runs the builder without sending, so the body can be asserted on
// before the broker is asked to act on it.
func buildBatchAck(topic, group string, checkpoints []string) (*remoting.BatchAckMessageRequestBody, error) {
	return remoting.BuildBatchAckMessageRequestBody(topic, group, checkpoints)
}

func (v *verifier) batchAck(body *remoting.BatchAckMessageRequestBody) error {
	return v.inst.BatchAckMessage(v.addr, body, rpcTimeoutMillis)
}

// ------------------------------------------------------------------ producers

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

func (h *producerHandle) send(topic string, bodies ...string) error {
	for _, body := range bodies {
		var lastErr error
		for attempt := 0; attempt < 30; attempt++ {
			res, err := h.p.Send(common.NewMessage(topic, []byte(body)))
			if err == nil && res.SendStatus == client.SendOK {
				lastErr = nil
				break
			}
			if err != nil {
				lastErr = err
			} else {
				lastErr = fmt.Errorf("send status %s", res.SendStatus)
			}
			time.Sleep(2 * time.Second)
		}
		if lastErr != nil {
			return fmt.Errorf("send %q never succeeded: %w", body, lastErr)
		}
	}
	return nil
}

func (h *producerHandle) shutdown() { h.p.Shutdown() }

// awaitQueueDepth blocks until the broker reports `want` messages on the topic.
// The ConsumeQueue is dispatched asynchronously, so a POP fired straight after
// the last send can answer with a partial batch — and a partial batch makes
// "one request cleared the batch" unprovable.
func awaitQueueDepth(ns, instance, topic string, want int, window time.Duration) (int64, bool) {
	a := newAdmin(ns, instance)
	if err := a.Start(); err != nil {
		return -1, false
	}
	defer a.Shutdown()
	deadline := time.Now().Add(window)
	var total int64
	for {
		total = 0
		stats, err := a.ExamineTopicStats(topic)
		if err == nil && stats != nil {
			for _, offset := range stats.OffsetTable {
				if offset != nil {
					total += offset.MaxOffset - offset.MinOffset
				}
			}
		}
		if total >= int64(want) {
			return total, true
		}
		if time.Now().After(deadline) {
			return total, false
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// ------------------------------------------------------------------ checkpoints

// ck is one message's view of its checkpoint, in the terms the batch body is
// built from.
type ck struct {
	raw       string
	body      string
	queueOff  int64
	ckOff     int64
	popTime   int64
	invisible int64
	retry     string
	broker    string
	queueID   int
	reviveQID int
	segments  int
}

// parseCks reads every popped message's checkpoint back out. It never infers a
// value: a message whose checkpoint cannot be parsed is a FAIL, not a skip.
func parseCks(msgs []*common.MessageExt) ([]ck, []string) {
	out := make([]ck, 0, len(msgs))
	problems := make([]string, 0)
	for _, m := range msgs {
		raw, _ := m.GetProperty(common.PropertyPopCk)
		entry, err := parseCkString(raw, string(m.Body))
		if err != nil {
			problems = append(problems, err.Error())
			out = append(out, entry)
			continue
		}
		// The bit index the batch body computes is (segment 7 - segment 0), and
		// the single-message ACK sends segment 7 as the offset. If the decoded
		// segment 7 were not this message's own queue offset, the body would be
		// addressing somebody else's message.
		if entry.queueOff != m.QueueOffset {
			problems = append(problems, fmt.Sprintf("%s: checkpoint segment 7 = %d but the message sits at %d",
				entry.body, entry.queueOff, m.QueueOffset))
		}
		out = append(out, entry)
	}
	return out, problems
}

// parseCkString reads every segment a batch body needs out of one checkpoint. It
// works on the captured string, so the observation loop's sightings can be
// re-parsed after the messages themselves have gone back out of sight.
func parseCkString(raw, body string) (ck, error) {
	parts := common.SplitExtraInfo(raw)
	entry := ck{raw: raw, body: body, segments: len(parts)}
	if len(parts) != common.ExtraInfoSegmentCount {
		return entry, fmt.Errorf("%s: checkpoint has %d segments, want %d: %q",
			body, len(parts), common.ExtraInfoSegmentCount, raw)
	}
	var err error
	if entry.ckOff, err = common.GetCkQueueOffset(parts); err != nil {
		return entry, fmt.Errorf("%s: %v", body, err)
	}
	if entry.popTime, err = common.GetPopTime(parts); err == nil {
		entry.invisible, err = common.GetInvisibleTime(parts)
	}
	if err == nil {
		entry.reviveQID, err = common.GetReviveQid(parts)
	}
	if err == nil {
		entry.retry, err = common.GetRetry(parts)
	}
	if err == nil {
		entry.broker, err = common.GetBrokerName(parts)
	}
	if err == nil {
		entry.queueID, err = common.GetQueueId(parts)
	}
	if err == nil {
		// Segment 7, not the message's decoded field: this is the value the
		// batch body turns into a bit index.
		entry.queueOff, err = common.GetQueueOffset(parts)
	}
	if err != nil {
		return entry, fmt.Errorf("%s: %v", body, err)
	}
	return entry, nil
}

func parseCkStrings(raws []string, body string) ([]ck, []string) {
	out := make([]ck, 0, len(raws))
	problems := make([]string, 0)
	for _, raw := range raws {
		entry, err := parseCkString(raw, body)
		if err != nil {
			problems = append(problems, err.Error())
		}
		out = append(out, entry)
	}
	return out, problems
}

// mergeKey is the grouping key the batch body is built on. It is recomputed here,
// independently of the builder, so "the builder grouped it" is not the same
// statement as "the key says it should group".
func mergeKey(c ck) string {
	return fmt.Sprintf("%s@%d@%d@%d", c.retry, c.queueID, c.ckOff, c.popTime)
}

// mergeKeyOf is the same key formula, read from an entry the builder produced.
// Applying it to both sides is what makes "the entries match the checkpoints"
// a statement about the builder rather than a tautology.
func mergeKeyOf(a *remoting.BatchAck) string {
	return fmt.Sprintf("%s@%d@%d@%d", a.Retry, a.QueueID, a.StartOffset, a.PopTime)
}

func distinctKeys(cks []ck) []string {
	set := map[string]bool{}
	for _, c := range cks {
		set[mergeKey(c)] = true
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ackedOffsets expands the body back into the queue offsets it claims to
// acknowledge: entry start offset + bit index.
func ackedOffsets(body *remoting.BatchAckMessageRequestBody) map[int64]int {
	out := map[int64]int{}
	for _, ack := range body.Acks {
		bits := 64 * len(ack.BitSet.Bytes())
		for i := 0; i < bits; i++ {
			if ack.BitSet.Get(i) {
				out[ack.StartOffset+int64(i)]++
			}
		}
	}
	return out
}

func offsetsOf(cks []ck) []int64 {
	out := make([]int64, 0, len(cks))
	for _, c := range cks {
		out = append(out, c.queueOff)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sameIntSet(a map[int64]int, b []int64) bool {
	seen := map[int64]bool{}
	for _, v := range b {
		seen[v] = true
	}
	if len(a) != len(seen) {
		return false
	}
	for k, n := range a {
		if !seen[k] || n != 1 {
			return false
		}
	}
	return true
}

func bodiesOf(cks []ck) string {
	names := make([]string, 0, len(cks))
	for _, c := range cks {
		names = append(names, c.body)
	}
	sort.Strings(names)
	return strings.Join(names, " ")
}

func detailOf(cks []ck) string {
	parts := make([]string, 0, len(cks))
	for _, c := range cks {
		parts = append(parts, fmt.Sprintf("%s{ck=%d off=%d pt=%d r=%q q=%d broker=%q}",
			c.body, c.ckOff, c.queueOff, c.popTime, c.retry, c.queueID, c.broker))
	}
	return strings.Join(parts, " ")
}

// ------------------------------------------------------------------ observation

// sighting is one answer the observation loop got, kept together with the
// checkpoint that came with it. The checkpoint has to be captured at sighting
// time: every POP that returns a message hides it again for the whole
// invisibility window, so a later re-pop of the same body would answer
// POLLING_NOT_FOUND and the follow-up ack would have nothing to work from.
type sighting struct {
	from     string
	ck       string
	marker   string
	recon    int32
	firstPop string
}

// eyes collects what a POP answered during an observation window.
type eyes struct {
	mu       sync.Mutex
	sighted  map[string][]sighting // body -> every answer for it
	problems []string
}

func newEyes() *eyes { return &eyes{sighted: map[string][]sighting{}} }

func (e *eyes) record(msgs []*common.MessageExt, from string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, m := range msgs {
		body := string(m.Body)
		raw, _ := m.GetProperty(common.PropertyPopCk)
		marker := ""
		if parts := common.SplitExtraInfo(raw); len(parts) == common.ExtraInfoSegmentCount {
			marker, _ = common.GetRetry(parts)
		}
		firstPop, _ := m.GetProperty(common.PropertyFirstPopTime)
		e.sighted[body] = append(e.sighted[body], sighting{
			from:     from,
			ck:       raw,
			marker:   marker,
			recon:    m.ReconsumeTimes,
			firstPop: firstPop,
		})
	}
}

func (e *eyes) note(problem string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.problems = append(e.problems, problem)
}

func (e *eyes) sightings(body string) []sighting {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sighting(nil), e.sighted[body]...)
}

func (e *eyes) topicsSeen(body string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, s := range e.sightings(body) {
		if !seen[s.from] {
			seen[s.from] = true
			out = append(out, s.from)
		}
	}
	return out
}

// checkpointsFor returns every checkpoint the window collected for a body. A
// message sighted more than once has one live checkpoint per sighting, and an
// ack that clears only one of them leaves the others to revive it again.
func (e *eyes) checkpointsFor(body string) []string {
	out := []string{}
	for _, s := range e.sightings(body) {
		if s.ck != "" {
			out = append(out, s.ck)
		}
	}
	return out
}

func (e *eyes) markersFor(body string) []string {
	out := []string{}
	for _, s := range e.sightings(body) {
		out = append(out, s.marker)
	}
	return out
}

func (e *eyes) cameBack(body string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.sighted[body]) > 0
}

func (e *eyes) allBodies() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, 0, len(e.sighted))
	for b := range e.sighted {
		out = append(out, b)
	}
	sort.Strings(out)
	return out
}

func (e *eyes) summary() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, 0, len(e.sighted))
	for b := range e.sighted {
		out = append(out, b)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return "nothing revived"
	}
	return "revived: " + strings.Join(out, " ")
}

// round pops the business topic AND the group's POP retry topic once. Both,
// because a checkpoint nobody acked is revived into the retry topic, and a POP of
// the business topic only consults that topic by probability — watching one of
// them would make a silence ambiguous.
func (v *verifier) round(group, topic string, e *eyes, firstError *string) {
	retryTopic := common.BuildPopRetryTopicV1(topic, group)
	for _, t := range []string{topic, retryTopic} {
		res, err := v.pop(t, group, -1, 32, 500)
		if err != nil {
			// A POP retry topic only exists once the revive service has put
			// something in it, so "topic not exist" is the expected answer
			// while nothing has been revived; anything else is worth seeing.
			if *firstError == "" && !strings.Contains(err.Error(), "not exist") {
				*firstError = fmt.Sprintf("%s: %v", t, err)
			}
			continue
		}
		if res == nil || res.PopStatus != client.PopFound {
			continue
		}
		e.record(res.MsgFoundList, t)
	}
}

// watch keeps popping both topics until the window expires.
func (v *verifier) watch(group, topic string, window time.Duration, e *eyes) {
	deadline := time.Now().Add(window)
	firstError := ""
	for time.Now().Before(deadline) {
		v.round(group, topic, e, &firstError)
	}
	if firstError != "" {
		e.note(firstError)
	}
}

// probe pops both topics until waitFor turns up, then keeps going for tail —
// enough for the same rounds to show that the acked messages did NOT come back,
// not enough to leave the collected checkpoint to expire — and returns whether it
// came back.
func (v *verifier) probe(group, topic, waitFor string, maxWindow, tail time.Duration, e *eyes) bool {
	deadline := time.Now().Add(maxWindow)
	stoppedAt := time.Time{}
	firstError := ""
	for time.Now().Before(deadline) {
		v.round(group, topic, e, &firstError)
		if stoppedAt.IsZero() && e.cameBack(waitFor) {
			stoppedAt = time.Now().Add(tail)
		}
		if !stoppedAt.IsZero() && !time.Now().Before(stoppedAt) {
			break
		}
	}
	if firstError != "" {
		e.note(firstError)
	}
	return e.cameBack(waitFor)
}

// ------------------------------------------------------------------ L1

func runL1(ns, prefix string) {
	fmt.Println("\n--- L1 one BATCH_ACK_MESSAGE(200151) clears a whole POP batch ---")
	const batch = 8
	topic := prefix + "_BaAll"
	group := "GID_" + prefix + "_l1"
	bodies := make([]string, 0, batch)
	for i := 0; i < batch; i++ {
		bodies = append(bodies, fmt.Sprintf("ba-all-%d", i))
	}

	if err := ensureTopic(ns, "go-ba-l1-topic-"+prefix, topic); err != nil {
		check("L1 create topic", false, err.Error())
		return
	}
	brokerName, err := brokerNameOf(ns, "go-ba-l1-route-"+prefix, topic)
	if err != nil {
		check("L1 fetch route", false, err.Error())
		return
	}
	p, err := startProducer(ns, "GID_"+prefix+"_l1_prod", "go-ba-l1-prod-"+prefix)
	if err != nil {
		check("L1 build producer", false, err.Error())
		return
	}
	defer p.shutdown()
	if err := p.send(topic, bodies...); err != nil {
		check("L1 send 8 messages", false, err.Error())
		return
	}
	check("L1 send 8 messages", true, topic)

	depth, ready := awaitQueueDepth(ns, "go-ba-l1-depth-"+prefix, topic, batch, 30*time.Second)
	check("L1 all 8 messages are on the queue before popping", ready, fmt.Sprintf("depth=%d want=%d", depth, batch))
	if !ready {
		return
	}

	v, err := newVerifier(ns, "go-ba-l1-"+prefix, topic, brokerName)
	if err != nil {
		check("L1 build wire verifier", false, err.Error())
		return
	}
	defer v.shutdown()

	res, err := v.pop(topic, group, 0, batch, 3000)
	if err != nil {
		check("L1 POP_MESSAGE answered the batch", false, err.Error())
		return
	}
	check("L1 POP_MESSAGE answered the whole batch", res.PopStatus == client.PopFound && len(res.MsgFoundList) == batch,
		fmt.Sprintf("status=%s got=%d", res.PopStatus, len(res.MsgFoundList)))

	cks, problems := parseCks(res.MsgFoundList)
	check("L1 every message carries a parsable 8-segment checkpoint", len(problems) == 0, strings.Join(problems, "; "))
	if len(cks) != batch {
		check("L1 checkpoint count", false, fmt.Sprintf("%d", len(cks)))
		return
	}

	// The batch body can only address a message through segment 7, and it can
	// only be routed through segments 4-6, so pin them per message before the
	// builder is trusted with them.
	shapeOK := true
	shapeDetail := ""
	for _, c := range cks {
		if c.retry != common.ExtraNormalTopic || c.broker != brokerName || c.queueID != 0 ||
			c.invisible != invisibleMillis || c.popTime <= 0 || c.ckOff > c.queueOff {
			shapeOK = false
			shapeDetail = fmt.Sprintf("%s{ck=%d off=%d r=%q q=%d broker=%q it=%d}",
				c.body, c.ckOff, c.queueOff, c.retry, c.queueID, c.broker, c.invisible)
			break
		}
	}
	check("L1 checkpoint fields the batch body depends on (marker 0 / logical broker / queueId / invisible / ck<=msg offset)",
		shapeOK, shapeDetail)
	if !shapeOK {
		fmt.Printf("     %s\n", detailOf(cks))
		return
	}

	body, err := buildBatchAck(topic, group, rawStrings(cks))
	if err != nil {
		check("L1 build batch ack body", false, err.Error())
		return
	}

	// Three body invariants, each one a way the builder could be wrong while
	// still producing a request the broker answers SUCCESS to.
	check("L1 body acknowledges exactly the popped offsets, once each",
		sameIntSet(ackedOffsets(body), offsetsOf(cks)),
		fmt.Sprintf("acked=%v want=%v", sortedKeys(ackedOffsets(body)), offsetsOf(cks)))
	check("L1 body has exactly one entry per merge key",
		len(body.Acks) == len(distinctKeys(cks)),
		fmt.Sprintf("entries=%d keys=%v", len(body.Acks), distinctKeys(cks)))

	// Every field of an entry has to be the value the checkpoint carried: the
	// broker looks up the checkpoint by (group, real topic, queueId, popTime,
	// invisibleTime, revive queue) and then walks the bits from startOffset, so
	// one invented field makes the whole entry a no-op that still answers
	// SUCCESS.
	byKey := map[string]ck{}
	for _, c := range cks {
		byKey[mergeKey(c)] = c
	}
	fieldsOK := true
	fieldDetail := ""
	for _, ack := range body.Acks {
		if body.BrokerName != brokerName || ack.ConsumerGroup != group || ack.Topic != topic {
			fieldsOK = false
			fieldDetail = fmt.Sprintf("entry c=%q t=%q bodyBroker=%q want c=%q t=%q broker=%q",
				ack.ConsumerGroup, ack.Topic, body.BrokerName, group, topic, brokerName)
			break
		}
		source, known := byKey[mergeKeyOf(ack)]
		if !known {
			fieldsOK = false
			fieldDetail = fmt.Sprintf("entry key %s matches no popped checkpoint", mergeKeyOf(ack))
			break
		}
		if ack.StartOffset != source.ckOff || ack.PopTime != source.popTime ||
			ack.InvisibleTime != source.invisible || int(ack.ReviveQueueID) != source.reviveQID ||
			int(ack.QueueID) != source.queueID || ack.Retry != source.retry || ack.BitSet.PopCount() == 0 {
			fieldsOK = false
			fieldDetail = fmt.Sprintf("entry %s diverges from checkpoint %s: so=%d/%d pt=%d/%d it=%d/%d rq=%d/%d q=%d/%d r=%q/%q bits=%d",
				mergeKeyOf(ack), bodiesOf([]ck{source}),
				ack.StartOffset, source.ckOff, ack.PopTime, source.popTime,
				ack.InvisibleTime, source.invisible, ack.ReviveQueueID, source.reviveQID,
				ack.QueueID, source.queueID, ack.Retry, source.retry, ack.BitSet.PopCount())
			break
		}
	}
	check("L1 every entry echoes its checkpoint fields, names the group, the business topic and the checkpoint broker",
		fieldsOK, fieldDetail)

	// The wire form is where a base64 BitSet goes wrong silently: the broker
	// reads bit 0 of the LAST byte first, so a non-minimal or byte-reversed
	// encoding still decodes as *some* set of offsets.
	encoded := body.Encode()
	decoded, err := remoting.DecodeBatchAckMessageRequestBody(encoded)
	if err != nil {
		check("L1 body round-trips through the wire codec", false, err.Error())
	} else {
		check("L1 body round-trips through the wire codec",
			len(decoded.Acks) == len(body.Acks) && sameIntSet(ackedOffsets(decoded), offsetsOf(cks)),
			fmt.Sprintf("entries %d -> %d, bytes=%d, body=%s", len(body.Acks), len(decoded.Acks), len(encoded), clip(string(encoded))))
	}

	if err := v.batchAck(body); err != nil {
		check("L1 one BATCH_ACK_MESSAGE answered SUCCESS", false, err.Error())
		return
	}
	check("L1 one BATCH_ACK_MESSAGE answered SUCCESS", true,
		fmt.Sprintf("%d checkpoints, %d entries", len(cks), len(body.Acks)))

	e := newEyes()
	v.watch(group, topic, observeWindow, e)
	check("L1 nothing revived inside 2.5x the invisibility window (the batch really landed)",
		len(e.allBodies()) == 0, e.summary())
	if len(e.problems) > 0 {
		fmt.Printf("     watch notes: %s\n", strings.Join(e.problems, "; "))
	}
}

func rawStrings(cks []ck) []string {
	out := make([]string, 0, len(cks))
	for _, c := range cks {
		out = append(out, c.raw)
	}
	return out
}

func sortedKeys(m map[int64]int) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func clip(s string) string {
	if len(s) > 220 {
		return s[:220] + "..."
	}
	return s
}

// ------------------------------------------------------------------ L2

// runL2 is the paired control: a batch ack of the same size with one checkpoint
// left out must revive exactly that message. It then finishes the retry direction,
// which the batch body carries through the marker rather than the topic.
func runL2(ns, prefix string) {
	fmt.Println("\n--- L2 paired control: an unacked bit comes back, the acked ones do not ---")
	const batch = 6
	topic := prefix + "_BaPart"
	group := "GID_" + prefix + "_l2"
	bodies := make([]string, 0, batch)
	for i := 0; i < batch; i++ {
		bodies = append(bodies, fmt.Sprintf("ba-part-%d", i))
	}

	if err := ensureTopic(ns, "go-ba-l2-topic-"+prefix, topic); err != nil {
		check("L2 create topic", false, err.Error())
		return
	}
	brokerName, err := brokerNameOf(ns, "go-ba-l2-route-"+prefix, topic)
	if err != nil {
		check("L2 fetch route", false, err.Error())
		return
	}
	p, err := startProducer(ns, "GID_"+prefix+"_l2_prod", "go-ba-l2-prod-"+prefix)
	if err != nil {
		check("L2 build producer", false, err.Error())
		return
	}
	defer p.shutdown()
	if err := p.send(topic, bodies...); err != nil {
		check("L2 send 6 messages", false, err.Error())
		return
	}
	check("L2 send 6 messages", true, topic)
	depth, ready := awaitQueueDepth(ns, "go-ba-l2-depth-"+prefix, topic, batch, 30*time.Second)
	check("L2 all 6 messages are on the queue before popping", ready, fmt.Sprintf("depth=%d", depth))
	if !ready {
		return
	}

	v, err := newVerifier(ns, "go-ba-l2-"+prefix, topic, brokerName)
	if err != nil {
		check("L2 build wire verifier", false, err.Error())
		return
	}
	defer v.shutdown()

	res, err := v.pop(topic, group, 0, batch, 3000)
	if err != nil {
		check("L2 POP_MESSAGE answered the batch", false, err.Error())
		return
	}
	check("L2 POP_MESSAGE answered the batch", res.PopStatus == client.PopFound && len(res.MsgFoundList) == batch,
		fmt.Sprintf("status=%s got=%d", res.PopStatus, len(res.MsgFoundList)))
	if res.PopStatus != client.PopFound || len(res.MsgFoundList) != batch {
		return
	}
	cks, problems := parseCks(res.MsgFoundList)
	if len(problems) > 0 {
		check("L2 checkpoints parsable", false, strings.Join(problems, "; "))
		return
	}

	// Drop exactly one: the message with the HIGHEST queue offset, whose bit is
	// the last one the bitmap can carry. Every other message keeps its ack.
	sorted := append([]ck(nil), cks...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].queueOff < sorted[j].queueOff })
	dropped := sorted[len(sorted)-1]
	acked := make([]ck, 0, len(sorted)-1)
	for _, c := range sorted[:len(sorted)-1] {
		acked = append(acked, c)
	}

	body, err := buildBatchAck(topic, group, rawStrings(acked))
	if err != nil {
		check("L2 build partial batch ack body", false, err.Error())
		return
	}
	check("L2 body acknowledges 5 of 6 offsets and not the dropped one",
		sameIntSet(ackedOffsets(body), offsetsOf(acked)) && ackedOffsets(body)[dropped.queueOff] == 0,
		fmt.Sprintf("acked=%v dropped=%d", sortedKeys(ackedOffsets(body)), dropped.queueOff))

	if err := v.batchAck(body); err != nil {
		check("L2 partial BATCH_ACK_MESSAGE answered SUCCESS", false, err.Error())
		return
	}
	check("L2 partial BATCH_ACK_MESSAGE answered SUCCESS", true,
		fmt.Sprintf("%d of %d checkpoints", len(acked), len(cks)))

	e := newEyes()
	// Stop at the first sighting plus a short tail: the tail is the fair comparison
	// for the acked messages (they would have revived at the same instant if the
	// bitmap had been decorative), and stopping there keeps the checkpoint we are
	// about to ack inside its own invisibility.
	broughtBack := v.probe(group, topic, dropped.body, reviveWaitWindow, reviveTail, e)

	// THE control. If the bitmap were decorative — on the client or the broker —
	// this leg looks exactly like L1.
	check("L2 the unacked message really came back", broughtBack,
		fmt.Sprintf("bodies=%q topics=%v", dropped.body, e.topicsSeen(dropped.body)))
	restOK := true
	for _, c := range acked {
		if e.cameBack(c.body) {
			restOK = false
			break
		}
	}
	check("L2 the five acked messages stayed gone while the dropped one came back", restOK, e.summary())
	if !broughtBack {
		fmt.Printf("     watch notes: %s\n", strings.Join(e.problems, "; "))
		return
	}

	// The revived message's checkpoint is addressed with marker "1", and it is that
	// marker — not the topic name in the reply — that tells the broker where the
	// checkpoint lives. So the follow-up batch ack must carry the BUSINESS topic.
	retryTopic := common.BuildPopRetryTopicV1(topic, group)
	seenFrom := e.topicsSeen(dropped.body)

	// Clear the checkpoint right away: its invisibility started the moment the reply
	// handed it over, and everything in between has to be a local check.
	raws := e.checkpointsFor(dropped.body)
	cks2, problems2 := parseCkStrings(raws, dropped.body)
	if len(problems2) > 0 {
		check("L2 the revived checkpoints are parsable", false, strings.Join(problems2, "; "))
		return
	}
	check("L2 the revived checkpoints carry the retry marker (they live in the retry queue)",
		len(cks2) > 0 && allMarkers(cks2, common.ExtraRetryTopic),
		fmt.Sprintf("sightings=%d markers=%v", len(raws), e.markersFor(dropped.body)))

	second, err := buildBatchAck(topic, group, rawStrings(cks2))
	if err != nil {
		check("L2 build the retry-topic batch ack body", false, err.Error())
		return
	}
	// The body names the BUSINESS topic even though the checkpoints came from the
	// retry topic: the broker re-derives the physical topic from marker + group,
	// so a client that "helpfully" passed the retry topic here would double-wrap
	// the name and ack nothing.
	retryBodyOK := len(second.Acks) >= 1
	for _, ack := range second.Acks {
		if ack.Topic != topic || ack.Retry != common.ExtraRetryTopic || ack.ConsumerGroup != group {
			retryBodyOK = false
		}
	}
	check("L2 the retry body keeps the business topic and carries the marker on every entry",
		retryBodyOK, fmt.Sprintf("entries=%d t=%q r=%v", len(second.Acks), topic, markersOfBody(second)))
	if err := v.batchAck(second); err != nil {
		check("L2 retry-topic BATCH_ACK_MESSAGE answered SUCCESS", false, err.Error())
		return
	}
	check("L2 retry-topic BATCH_ACK_MESSAGE answered SUCCESS", true,
		fmt.Sprintf("%d revived checkpoints -> %d entries", len(cks2), len(second.Acks)))

	// Which RPC answered the message is deliberately NOT the criterion: a POP of
	// the business topic also consults the retry queue (and the client rewrites the
	// reply topic to the requested one), so "seen from" can name either queue's
	// topic for the same physical revival. What is broker state: the retry topic
	// exists, and the checkpoint the reply carried points into it. Probed after the
	// ack, because a route reaching the name server can take a broker heartbeat and
	// that wait must not push the ack past the checkpoint's invisibility.
	check("L2 the POP retry topic exists on the broker (the revive service really moved it there)",
		routeVisible(ns, "go-ba-l2-retryroute-"+prefix, retryTopic, 30*time.Second),
		fmt.Sprintf("%s answered from %q", retryTopic, strings.Join(seenFrom, ",")))

	settled := newEyes()
	v.watch(group, topic, observeWindow, settled)
	check("L2 the marker-driven retry ack silenced it (no further revival)",
		!settled.cameBack(dropped.body), settled.summary())
}

func allMarkers(cks []ck, want string) bool {
	for _, c := range cks {
		if c.retry != want {
			return false
		}
	}
	return true
}

func markersOfBody(body *remoting.BatchAckMessageRequestBody) []string {
	out := make([]string, 0, len(body.Acks))
	for _, a := range body.Acks {
		out = append(out, a.Retry)
	}
	return out
}

func statusOf(res *client.PopResult) string {
	if res == nil {
		return "<nil>"
	}
	return res.PopStatus.String()
}

func lenOf(res *client.PopResult) int {
	if res == nil {
		return 0
	}
	return len(res.MsgFoundList)
}

// ------------------------------------------------------------------ L3

// runL3 pins the grouping key: two pop rounds of one queue are acked by one
// request, and the entries must not merge across rounds.
func runL3(ns, prefix string) {
	fmt.Println("\n--- L3 two pop rounds, one request: entries group by retry@queueId@ckOffset@popTime ---")
	const total = 4
	const perRound = 2
	topic := prefix + "_BaRounds"
	group := "GID_" + prefix + "_l3"
	bodies := make([]string, 0, total)
	for i := 0; i < total; i++ {
		bodies = append(bodies, fmt.Sprintf("ba-round-%d", i))
	}

	if err := ensureTopic(ns, "go-ba-l3-topic-"+prefix, topic); err != nil {
		check("L3 create topic", false, err.Error())
		return
	}
	brokerName, err := brokerNameOf(ns, "go-ba-l3-route-"+prefix, topic)
	if err != nil {
		check("L3 fetch route", false, err.Error())
		return
	}
	p, err := startProducer(ns, "GID_"+prefix+"_l3_prod", "go-ba-l3-prod-"+prefix)
	if err != nil {
		check("L3 build producer", false, err.Error())
		return
	}
	defer p.shutdown()
	if err := p.send(topic, bodies...); err != nil {
		check("L3 send 4 messages", false, err.Error())
		return
	}
	check("L3 send 4 messages", true, topic)
	depth, ready := awaitQueueDepth(ns, "go-ba-l3-depth-"+prefix, topic, total, 30*time.Second)
	check("L3 all 4 messages are on the queue before popping", ready, fmt.Sprintf("depth=%d", depth))
	if !ready {
		return
	}

	v, err := newVerifier(ns, "go-ba-l3-"+prefix, topic, brokerName)
	if err != nil {
		check("L3 build wire verifier", false, err.Error())
		return
	}
	defer v.shutdown()

	first, err := v.pop(topic, group, 0, perRound, 3000)
	if err != nil || first.PopStatus != client.PopFound || len(first.MsgFoundList) != perRound {
		check("L3 first POP answers 2 messages", false,
			fmt.Sprintf("err=%v status=%v got=%d", err, statusOf(first), lenOf(first)))
		return
	}
	// The second round asks for 2 more. The first round's messages are invisible
	// for the whole window, so the broker has no choice but to hand out the other
	// two — which is what makes the two rounds distinguishable by checkpoint.
	second, err := v.pop(topic, group, 0, perRound, 1000)
	if err != nil || second.PopStatus != client.PopFound || len(second.MsgFoundList) != perRound {
		check("L3 second POP answers the other 2 messages", false,
			fmt.Sprintf("err=%v status=%v got=%d", err, statusOf(second), lenOf(second)))
		return
	}
	cksA, problemsA := parseCks(first.MsgFoundList)
	cksB, problemsB := parseCks(second.MsgFoundList)
	if len(problemsA)+len(problemsB) > 0 {
		check("L3 both rounds carry parsable checkpoints", false,
			strings.Join(append(problemsA, problemsB...), "; "))
		return
	}
	overlap := map[string]bool{}
	for _, c := range cksA {
		overlap[c.body] = true
	}
	disjoint := true
	for _, c := range cksB {
		if overlap[c.body] {
			disjoint = false
		}
	}
	check("L3 the two rounds are disjoint batches of one queue", disjoint,
		fmt.Sprintf("A=%s B=%s", bodiesOf(cksA), bodiesOf(cksB)))

	all := append(append([]ck(nil), cksA...), cksB...)
	body, err := buildBatchAck(topic, group, rawStrings(all))
	if err != nil {
		check("L3 build the two-round batch ack body", false, err.Error())
		return
	}
	keys := distinctKeys(all)
	roundA := map[string]bool{}
	for _, c := range cksA {
		roundA[mergeKey(c)] = true
	}
	roundB := map[string]bool{}
	for _, c := range cksB {
		roundB[mergeKey(c)] = true
	}
	shared := 0
	for k := range roundA {
		if roundB[k] {
			shared++
		}
	}
	check("L3 the two rounds produce different merge keys, so the body cannot collapse them",
		shared == 0 && len(keys) >= 2,
		fmt.Sprintf("keys=%d shared=%d %v", len(keys), shared, keys))
	check("L3 one body covers all 4 offsets exactly once",
		sameIntSet(ackedOffsets(body), offsetsOf(all)) && len(body.Acks) == len(keys),
		fmt.Sprintf("entries=%d acked=%v want=%v", len(body.Acks), sortedKeys(ackedOffsets(body)), offsetsOf(all)))

	if err := v.batchAck(body); err != nil {
		check("L3 one BATCH_ACK_MESSAGE for two rounds answered SUCCESS", false, err.Error())
		return
	}
	check("L3 one BATCH_ACK_MESSAGE for two rounds answered SUCCESS", true,
		fmt.Sprintf("%d checkpoints -> %d entries", len(all), len(body.Acks)))

	e := newEyes()
	v.watch(group, topic, observeWindow, e)
	check("L3 no message of either round revived inside 2.5x the window",
		len(e.allBodies()) == 0, e.summary())
}

// ------------------------------------------------------------------ main

func main() {
	nsFlag := flag.String("ns", "127.0.0.1:9876", "name server address")
	legs := flag.String("legs", "", "which legs to run: l1,l2,l3 (default: all)")
	flag.Parse()
	ns := *nsFlag

	stamp := time.Now().Unix()
	prefix := fmt.Sprintf("GoBatchAck%d", stamp)
	want := *legs
	selected := func(name string) bool {
		if strings.TrimSpace(want) == "" {
			return true
		}
		for _, l := range strings.Split(want, ",") {
			if strings.TrimSpace(l) == name {
				return true
			}
		}
		return false
	}

	fmt.Printf("=== Go POP batch-ack live: ns=%s prefix=%s legs=%s ===\n", ns, prefix, orAll(want))

	if selected("l1") {
		runL1(ns, prefix)
	}
	if selected("l2") {
		runL2(ns, prefix)
	}
	if selected("l3") {
		runL3(ns, prefix)
	}

	fmt.Printf("\nPASS=%d FAIL=%d\n", passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
}

func orAll(s string) string {
	if strings.TrimSpace(s) == "" {
		return "all"
	}
	return s
}
