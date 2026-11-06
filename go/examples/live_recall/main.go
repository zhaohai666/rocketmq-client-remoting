// live_recall is the Go recall (RECALL_MESSAGE(370)) live smoke: a timer
// message carries a recallHandle, %RETRY% topics and garbage handles are
// rejected locally, the recall returns the withdrawn message's uniqKey, and
// the recalled message is never delivered while its control timers arrive.
//
//	go run ./examples/live_recall -ns 127.0.0.1:9876
//
// The broker's recallMessageEnable is flipped on for the run and restored to
// its original value at the end. The consume-observation window is longer than
// the timer delay: the control message must arrive on time, the recalled one
// must stay absent for the whole window.
//
// The last line is `PASS=<n> FAIL=<n>`; exits non-zero on any failure.
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

const (
	delaySec    = 12
	windowSec   = delaySec + 20
	recallKey   = "recallMessageEnable"
	defaultAddr = "127.0.0.1:10911"
)

var (
	mu        sync.Mutex
	passCount int
	failCount int
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

type record struct {
	topic string
	body  string
}

type collector struct {
	mu   sync.Mutex
	recs []record
}

func (c *collector) ConsumeMessage(msgs []*common.MessageExt, _ *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	c.mu.Lock()
	for _, m := range msgs {
		c.recs = append(c.recs, record{topic: m.Topic, body: string(m.GetBody())})
	}
	c.mu.Unlock()
	return client.ConsumeSuccess
}

func (c *collector) snapshot() []record {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]record(nil), c.recs...)
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return cond()
}

func main() {
	nsAddr := flag.String("ns", "127.0.0.1:9876", "nameserver address")
	flag.Parse()
	if !strings.Contains(*nsAddr, ":") {
		fmt.Fprintln(os.Stderr, "nameserver address must be host:port")
		os.Exit(2)
	}
	stamp := time.Now().Unix()
	topic := fmt.Sprintf("GoRecallLive_%d", stamp)
	group := fmt.Sprintf("GID_GoRecallLive_%d", stamp)

	admin := client.NewDefaultMQAdminExt(nil)
	admin.SetNamesrvAddr(*nsAddr)
	if err := admin.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "admin start: %v\n", err)
		os.Exit(1)
	}
	producer, err := client.NewDefaultMQProducer("GID_GoRecallSend_" + fmt.Sprint(stamp))
	if err != nil {
		fmt.Fprintf(os.Stderr, "producer: %v\n", err)
		os.Exit(1)
	}
	producer.SetNameServerAddr(*nsAddr)
	producer.SetSendMsgTimeout(5000)
	if err := producer.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "producer start: %v\n", err)
		os.Exit(1)
	}

	// ---- R0: recallMessageEnable readable + flipped on ----
	cluster, err := admin.FetchBrokerClusterInfo()
	if err != nil || len(cluster.BrokerAddrs()) == 0 {
		check("R0 cluster probe", false, "no broker registered with the name server")
		os.Exit(1)
	}
	brokerAddr := cluster.BrokerAddrs()[0]
	if brokerAddr == "" {
		brokerAddr = defaultAddr
	}
	cfg, err := admin.GetBrokerConfig(brokerAddr, 5000)
	original, hasOriginal := cfg[recallKey]
	check("R0 the broker's recallMessageEnable is readable", err == nil && hasOriginal,
		fmt.Sprintf("value=%s", original))
	if !hasOriginal || original != "true" {
		if err := admin.UpdateBrokerConfig(brokerAddr, map[string]string{recallKey: "true"}, 5000); err != nil {
			check("R0 recallMessageEnable flipped on", false, err.Error())
			os.Exit(1)
		}
		time.Sleep(500 * time.Millisecond)
		check("R0 recallMessageEnable flipped on", true, "was="+original)
	}

	// ---- R1: only a timer message carries a recallHandle ----
	timerMsg := common.NewMessage(topic, []byte("to-recall"))
	timerMsg.PutProperty(common.PropertyTimerDelaySec, fmt.Sprint(delaySec))
	controlMsg := common.NewMessage(topic, []byte("control"))
	controlMsg.PutProperty(common.PropertyTimerDelaySec, fmt.Sprint(delaySec))
	plainMsg := common.NewMessage(topic, []byte("plain"))

	rRecall, err := producer.Send(timerMsg)
	if err != nil {
		check("R1 send timer message", false, err.Error())
		os.Exit(1)
	}
	rControl, err := producer.Send(controlMsg)
	if err != nil {
		check("R1 send control timer", false, err.Error())
		os.Exit(1)
	}
	rPlain, err := producer.Send(plainMsg)
	if err != nil {
		check("R1 send plain message", false, err.Error())
		os.Exit(1)
	}
	handle := rRecall.RecallHandle
	check("R1 the timer message came back with a recallHandle", handle != "", "handle="+handle)
	check("R1 the ordinary message carries no recallHandle", rPlain.RecallHandle == "", "")
	check("R1 the two timer messages have different handles", handle != "" && rControl.RecallHandle != handle, "")

	// ---- R4/R5: local validation before touching the network ----
	_, err = producer.RecallMessage("%RETRY%"+group, handle)
	check("R4 a %RETRY% topic is rejected", err != nil && strings.Contains(err.Error(), "topic is not supported"),
		fmt.Sprintf("%v", err))
	began := time.Now()
	_, err = producer.RecallMessage(topic, "not-a-handle")
	check("R5 an illegal handle is rejected locally",
		err != nil && strings.Contains(err.Error(), common.RecallHandleInvalid) && time.Since(began) < 200*time.Millisecond,
		fmt.Sprintf("%v (%s)", err, time.Since(began).Round(time.Millisecond)))

	// ---- R3: the recall returns the withdrawn message's uniqKey ----
	recalledID, err := producer.RecallMessage(topic, handle)
	check("R3 RecallMessage returns the uniqKey of the recalled message",
		err == nil && recalledID == rRecall.MsgID,
		fmt.Sprintf("resp=%s send=%s err=%v", recalledID, rRecall.MsgID, err))

	// ---- R6: the control timers arrive, the recalled one never does ----
	listener := &collector{}
	consumer, err := client.NewDefaultMQPushConsumer(group)
	if err != nil {
		fmt.Fprintf(os.Stderr, "consumer: %v\n", err)
		os.Exit(1)
	}
	consumer.SetNameServerAddresses([]string{*nsAddr})
	consumer.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)
	if err := consumer.SetMessageListener(listener); err != nil {
		fmt.Fprintf(os.Stderr, "listener: %v\n", err)
		os.Exit(1)
	}
	if err := consumer.Subscribe(topic, "*"); err != nil {
		fmt.Fprintf(os.Stderr, "subscribe: %v\n", err)
		os.Exit(1)
	}
	if err := consumer.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "consumer start: %v\n", err)
		os.Exit(1)
	}
	time.Sleep(windowSec * time.Second)
	consumer.Shutdown()

	bodies := make(map[string]bool)
	for _, r := range listener.snapshot() {
		bodies[r.body] = true
	}
	check("R6 the control timer message was delivered on time", bodies["control"], "")
	check("R6 the ordinary message was delivered", bodies["plain"], "")
	check("R6 the recalled timer message was never delivered", !bodies["to-recall"], "")

	// ---- R7: restore the flag ----
	restored := false
	if hasOriginal {
		if err := admin.UpdateBrokerConfig(brokerAddr, map[string]string{recallKey: original}, 5000); err == nil {
			time.Sleep(500 * time.Millisecond)
			cfg2, err2 := admin.GetBrokerConfig(brokerAddr, 5000)
			restored = err2 == nil && cfg2[recallKey] == original
		}
	}
	check("R7 the broker's recallMessageEnable was restored to "+original, restored, "")

	admin.Shutdown()
	producer.Shutdown()

	fmt.Printf("\nPASS=%d FAIL=%d\n", passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
}
