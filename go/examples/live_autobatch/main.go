// live_autobatch exercises the ProduceAccumulator (auto-batch send) against a
// real RocketMQ 5.x cluster: same-key accumulation flushed as ONE broker batch
// by the byte threshold and by the guard timer, per-message SendResult split
// from the comma-separated batch reply, and the async accumulation chain.
//
//	go run ./examples/live_autobatch -ns 127.0.0.1:9876
//
// Every check prints PASS/FAIL and the process exits non-zero if any failed.
// The last line is `SENT=<n>` (messages the broker accepted) for the
// cross-language read-back check.
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
	sent      int
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

func countSent(n int) {
	mu.Lock()
	sent += n
	mu.Unlock()
}

func main() {
	nsAddr := flag.String("ns", "127.0.0.1:9876", "nameserver address")
	topic := flag.String("topic", "", "topic name (default: GoAutoBatch_<unix>)")
	group := flag.String("group", "", "producer group (default: GID_GoAutoBatch_<unix>)")
	flag.Parse()

	stamp := time.Now().Unix()
	topicName := *topic
	if topicName == "" {
		topicName = fmt.Sprintf("GoAutoBatch_%d", stamp)
	}
	groupName := *group
	if groupName == "" {
		groupName = fmt.Sprintf("GID_GoAutoBatch_%d", stamp)
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
	// Accumulate by bytes: 256B threshold flushes the batch inside the loop
	// without relying on the timer; the timer (1s) is the backstop for the
	// tail messages.
	if err := p.SetBatchMaxBytes(256); err != nil {
		fmt.Fprintf(os.Stderr, "batchMaxBytes: %v\n", err)
		os.Exit(1)
	}
	if err := p.SetBatchMaxDelayMs(1000); err != nil {
		fmt.Fprintf(os.Stderr, "batchMaxDelayMs: %v\n", err)
		os.Exit(1)
	}
	p.SetAutoBatch(true)
	if err := p.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "producer start: %v\n", err)
		os.Exit(1)
	}
	defer p.Shutdown()

	// ---- 1. sync auto-batch: CONCURRENT senders coalesce into batches ----
	// Java semantics: a sync Add blocks until its batch is flushed, so a
	// single-threaded sequential sender legitimately gets one batch per
	// message (flushed at the hold deadline). Coalescing shows up with
	// concurrent senders: 6 goroutines racing inside the 1s hold window must
	// share batches, and every caller still gets its OWN per-message SendResult
	// (split from the batch reply).
	ids := map[string]bool{}
	distinct := true
	var idsMu sync.Mutex
	var sendWg sync.WaitGroup
	for i := 0; i < 6; i++ {
		sendWg.Add(1)
		go func(i int) {
			defer sendWg.Done()
			msg := common.NewMessage(topicName, []byte(fmt.Sprintf("autobatch-sync-%02d-0123456789abcdef", i)))
			msg.SetTags("autoBatch")
			msg.SetKeys(fmt.Sprintf("auto-batch-key-%d", i))
			res, err := p.Send(msg)
			if err != nil {
				check(fmt.Sprintf("sync accumulated send %d", i), false, err.Error())
				return
			}
			check(fmt.Sprintf("sync accumulated send %d", i), res.SendStatus == client.SendOK,
				fmt.Sprintf("msgId=%s", res.MsgID))
			idsMu.Lock()
			if ids[res.MsgID] {
				distinct = false
			}
			ids[res.MsgID] = true
			idsMu.Unlock()
			countSent(1)
		}(i)
	}
	sendWg.Wait()
	check("coalesced batch split yields per-message msgIds", distinct && len(ids) == 6,
		fmt.Sprintf("distinct=%d", len(ids)))

	// ---- 2. sync auto-batch: timer flush of the tail (below the byte gate) --
	// 2 small messages stay under 256B: the guard goroutine (max(1, 500ms))
	// must flush them via BatchMaxDelayMs.
	tailIDs := map[string]bool{}
	for i := 0; i < 2; i++ {
		msg := common.NewMessage(topicName, []byte(fmt.Sprintf("autobatch-tail-%d", i)))
		msg.SetTags("autoBatch")
		res, err := p.Send(msg)
		if err != nil {
			check(fmt.Sprintf("tail send %d", i), false, err.Error())
			continue
		}
		tailIDs[res.MsgID] = true
		countSent(1)
	}
	time.Sleep(1500 * time.Millisecond) // guard tick (500ms) + margin
	check("tail messages flushed by guard timer", len(tailIDs) == 2, fmt.Sprintf("ids=%d", len(tailIDs)))

	// ---- 3. async auto-batch: accumulation + callback results ---------------
	var wg sync.WaitGroup
	var cbMu sync.Mutex
	cbOK, cbFail := 0, 0
	cbIDs := map[string]bool{}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		msg := common.NewMessage(topicName, []byte(fmt.Sprintf("autobatch-async-%02d-0123456789abcdef", i)))
		msg.SetTags("autoBatch")
		err := p.SendAsync(msg, client.SendCallbackFunc{
			SuccessFn: func(r *client.SendResult) {
				defer wg.Done()
				cbMu.Lock()
				cbOK++
				cbIDs[r.MsgID] = true
				cbMu.Unlock()
				countSent(1)
			},
			ExceptFn: func(e error) {
				defer wg.Done()
				cbMu.Lock()
				cbFail++
				cbMu.Unlock()
			},
		})
		if err != nil {
			wg.Done()
			check(fmt.Sprintf("async accumulated send %d", i), false, err.Error())
		}
	}
	wg.Wait()
	time.Sleep(1200 * time.Millisecond) // let the async guard flush leftovers
	check("async callbacks all succeed", cbOK == 4 && cbFail == 0,
		fmt.Sprintf("ok=%d fail=%d", cbOK, cbFail))
	cbMu.Lock()
	idList := make([]string, 0, len(cbIDs))
	for id := range cbIDs {
		idList = append(idList, id)
	}
	cbMu.Unlock()
	check("async split yields per-message msgIds", len(cbIDs) == 4,
		fmt.Sprintf("distinct=%d ids=%v", len(cbIDs), idList))

	fmt.Printf("\nPASS=%d FAIL=%d SENT=%d\n", passCount, failCount, sent)
	if failCount > 0 {
		os.Exit(1)
	}
}
