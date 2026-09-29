// live_producer is the Go end-to-end smoke test against a real RocketMQ 5.x
// cluster: sync / batch / oneway sends, the two-phase transaction flow, and the
// broker's transaction check-back.
//
//	go run ./examples/live_producer -ns 127.0.0.1:9876
//
// Every check prints PASS/FAIL and the process exits non-zero if any failed.
// Use a fresh topic per run (the default embeds a timestamp) so leftover state
// from an earlier run cannot make a check pass.
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
		check("批量发送 3 条", bres.SendStatus == client.SendOK, " status="+bres.SendStatus.String())
	}

	// ---- 3. compression (body >= 4096 -> zlib) ---------------------------------------
	big := []byte(strings.Repeat("compress-me-", 500))
	if cres, err := p.Send(common.NewMessage(topicName, big)); err != nil {
		check("大消息压缩发送", false, " err="+err.Error())
	} else {
		check("大消息压缩发送", cres.SendStatus == client.SendOK, " status="+cres.SendStatus.String())
	}

	// ---- 4. oneway ------------------------------------------------------------------
	onewayErr := p.SendOneway(common.NewMessage(topicName, []byte("go-live-oneway")), nil)
	check("单向发送不报错", onewayErr == nil, errDetail(onewayErr))

	// ---- 5. transaction, COMMIT -----------------------------------------------------
	commitListener := &txListener{executeRet: client.CommitMessage, checkRet: client.CommitMessage}
	txMsg := common.NewMessage(topicName, []byte("go-live-tx-commit"))
	txRes, err := p.SendMessageInTransaction(txMsg, commitListener, nil)
	if err != nil {
		check("事务提交(COMMIT) 返回", false, " err="+err.Error())
	} else {
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

	fmt.Printf("\nPASS=%d FAIL=%d\n", passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
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
