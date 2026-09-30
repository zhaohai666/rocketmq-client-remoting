// Pull path (Java org.apache.rocketmq.client.impl.consumer.PullAPIWrapper +
// DefaultMQPushConsumerImpl.pullMessage).
//
// Three rules here are easy to get wrong and expensive to debug — they are
// called out again at each site:
//
//  1. consumeMessageDirectly/pull() being a SHORT poll has nothing to do with
//     this file; the push consumer is a LONG poll (suspend=true). What matters
//     here is that the COMMIT_OFFSET bit is cleared whenever the request lands
//     on a slave — committing an offset to a slave is pointless.
//  2. The `subscription` extField is written ONLY when the SUBSCRIPTION sysFlag
//     bit is on. The broker branches on the bit, so an expression that travels
//     without the bit is ignored, and a bit that travels without the value
//     makes the broker reject the request with SUBSCRIPTION_NOT_EXIST.
//  3. Every response rewrites pullFromWhichNodeTable from suggestWhichBrokerId,
//     and a MISSING field means master(0) — not "keep the previous value".
//     The next round uses that id to pick master-or-slave.
package client

import (
	"fmt"
	"sync"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- PullResult

// PullStatus mirrors Java
// org.apache.rocketmq.client.consumer.PullResult.PullStatus.
type PullStatus int

const (
	// PullFound: the broker returned messages.
	PullFound PullStatus = iota
	// PullNoNewMsg: PULL_NOT_FOUND(19) — nothing at that offset.
	PullNoNewMsg
	// PullNoMatchedMsg: PULL_RETRY_IMMEDIATELY(20) — messages exist but none
	// matched the subscription expression.
	PullNoMatchedMsg
	// PullOffsetIllegal: PULL_OFFSET_MOVED(21) — the requested offset is out of
	// range and the broker put the corrected value in nextBeginOffset.
	PullOffsetIllegal
)

func (s PullStatus) String() string {
	switch s {
	case PullFound:
		return "FOUND"
	case PullNoNewMsg:
		return "NO_NEW_MSG"
	case PullNoMatchedMsg:
		return "NO_MATCHED_MSG"
	case PullOffsetIllegal:
		return "OFFSET_ILLEGAL"
	default:
		return fmt.Sprintf("PullStatus(%d)", int(s))
	}
}

// PullResult is one pull response (Java PullResult/PullResultExt merged).
type PullResult struct {
	PullStatus PullStatus
	// NextBeginOffset is the cursor for the next round. It is ALSO the
	// corrected offset when PullStatus is PullOffsetIllegal.
	NextBeginOffset int64
	MinOffset       int64
	MaxOffset       int64
	MsgFoundList    []*common.MessageExt
	// SuggestWhichBrokerID is nil when the broker sent no field (and only
	// then — see rule 3 at the top of this file).
	SuggestWhichBrokerID *int32
}

// ------------------------------------------------- address / node selection

// FindBrokerAddressInSubscribe mirrors Java MQClientInstance
// #findBrokerAddressInSubscribe. Returns (addr, isSlave, found).
//
// The isSlave flag comes from the id that was ACTUALLY matched, which is why
// the fallback branch recomputes it: a request for slave id 3 that falls back
// to the master must NOT be reported as a slave request — otherwise the
// COMMIT_OFFSET bit gets cleared for nothing.
func (i *Instance) FindBrokerAddressInSubscribe(brokerName string, brokerID int64, onlyThisBroker bool) (string, bool, bool) {
	i.mu.Lock()
	addrs := i.brokerAddrTable[brokerName]
	snapshot := make(map[int64]string, len(addrs))
	for id, addr := range addrs {
		snapshot[id] = addr
	}
	i.mu.Unlock()

	if len(snapshot) == 0 {
		return "", false, false
	}
	if addr, ok := snapshot[brokerID]; ok {
		return addr, brokerID != int64(common.MasterID), true
	}
	if brokerID != int64(common.MasterID) {
		// Java's convention: a slave registers as <slaveId>, so +1 is the
		// "primary slave" of the same group.
		if addr, ok := snapshot[brokerID+1]; ok {
			return addr, true, true
		}
	}
	if !onlyThisBroker {
		// Java takes `entrySet().iterator().next()` — map order. The four ports
		// agree on the deterministic form instead: the smallest id (the master
		// when it exists).
		minID := int64(0)
		first := true
		for id := range snapshot {
			if first || id < minID {
				minID, first = id, false
			}
		}
		return snapshot[minID], minID != int64(common.MasterID), true
	}
	return "", false, false
}

// ---------------------------------------------------------------- pullAPIWrapper

// pullAPI is Java PullAPIWrapper: the per-consumer pull bookkeeping.
type pullAPI struct {
	instance *Instance
	group    string
	unitMode bool
	hooks    []FilterMessageHook

	// pullFromWhichNode maps MessageQueue -> the brokerId to ask next round.
	// Keyed by MessageQueue so it is comparable, per Java.
	//
	// Java backs this with a ConcurrentHashMap and it is genuinely contended:
	// ONE pull goroutine per queue reads it (pullKernel) while the same
	// goroutines write it after every response (processPullResult). A plain map
	// here is a `fatal error: concurrent map writes`.
	mu                sync.Mutex
	pullFromWhichNode map[common.MessageQueue]int64
}

func newPullAPI(instance *Instance, group string) *pullAPI {
	return &pullAPI{
		instance:          instance,
		group:             group,
		pullFromWhichNode: map[common.MessageQueue]int64{},
	}
}

// recalculatePullFromWhichNode is Java PullAPIWrapper#recalculatePullFromWhichNode:
// the table value, or MASTER_ID when the queue has no entry.
func (p *pullAPI) recalculatePullFromWhichNode(mq common.MessageQueue) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if id, ok := p.pullFromWhichNode[mq]; ok {
		return id
	}
	return int64(common.MasterID)
}

// PullFromWhichNode reads the table (diagnostics/tests).
func (p *pullAPI) PullFromWhichNode(mq common.MessageQueue) (int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	id, ok := p.pullFromWhichNode[mq]
	return id, ok
}

// updatePullFromWhichNode is Java PullAPIWrapper#updatePullFromWhichNode.
func (p *pullAPI) updatePullFromWhichNode(mq common.MessageQueue, brokerID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pullFromWhichNode[mq] = brokerID
}

// forgetPullFromWhichNode drops one queue's entry (Java's removeProcessQueue
// path rebuilds the entry from scratch).
func (p *pullAPI) forgetPullFromWhichNode(mq common.MessageQueue) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.pullFromWhichNode, mq)
}

// pullKernel mirrors Java PullAPIWrapper#pullKernelImpl. It resolves the
// broker address by the pullFromWhichNode id, clears COMMIT_OFFSET when a slave
// was hit, and issues PULL_MESSAGE(11).
func (p *pullAPI) pullKernel(group string, mq common.MessageQueue, offset int64,
	sub *remoting.SubscriptionData, sysFlag int32, commitOffset int64,
	maxMsgNums int32, maxMsgBytes int32, suspendTimeoutMillis int64,
	timeoutMillis int64) (*PullResult, error) {
	return p.pullKernelWithCode(remoting.ReqPullMessage, group, mq, offset, sub,
		sysFlag, commitOffset, maxMsgNums, maxMsgBytes, suspendTimeoutMillis, timeoutMillis)
}

// pullKernelLite is the LITE_PULL_MESSAGE(361) flavour the lite pull consumer
// uses: identical header and response shape, and the caller has already set the
// FLAG_LITE_PULL_MESSAGE bit in sysFlag. The pull is always a SHORT poll with
// no inline offset commit (the lite consumer owns its own commit path), so
// commitOffset is fixed at 0 and the broker hold budget at 15s.
func (p *pullAPI) pullKernelLite(group string, mq common.MessageQueue, offset int64,
	sub *remoting.SubscriptionData, sysFlag int32, maxMsgNums int32,
	timeoutMillis int64) (*PullResult, error) {
	return p.pullKernelWithCode(remoting.ReqLitePullMessage, group, mq, offset, sub,
		sysFlag, 0, maxMsgNums, -1, 15000, timeoutMillis)
}

func (p *pullAPI) pullKernelWithCode(code int32, group string, mq common.MessageQueue,
	offset int64, sub *remoting.SubscriptionData, sysFlag int32, commitOffset int64,
	maxMsgNums int32, maxMsgBytes int32, suspendTimeoutMillis int64,
	timeoutMillis int64) (*PullResult, error) {

	addr, slave, found := p.instance.FindBrokerAddressInSubscribe(mq.BrokerName, p.recalculatePullFromWhichNode(mq), false)
	if !found {
		if _, err := p.instance.UpdateTopicRouteInfoFromNameServer(mq.Topic, 5000, false); err != nil {
			return nil, err
		}
		addr, slave, found = p.instance.FindBrokerAddressInSubscribe(mq.BrokerName, p.recalculatePullFromWhichNode(mq), false)
		if !found {
			return nil, common.ClientError(fmt.Sprintf("The broker[%s] not exist", mq.BrokerName))
		}
	}
	if slave {
		// Java pullKernelImpl:219-221 — a slave does not accept offset commits.
		sysFlag = common.ClearCommitOffsetFlag(sysFlag)
	}

	header := &remoting.PullMessageRequestHeader{
		ConsumerGroup:        remoting.StrPtr(group),
		Topic:                remoting.StrPtr(mq.Topic),
		QueueID:              remoting.I32Ptr(mq.QueueID),
		QueueOffset:          remoting.I64Ptr(offset),
		MaxMsgNums:           remoting.I32Ptr(maxMsgNums),
		SysFlag:              remoting.I32Ptr(sysFlag),
		CommitOffset:         remoting.I64Ptr(commitOffset),
		SuspendTimeoutMillis: remoting.I64Ptr(suspendTimeoutMillis),
		SubVersion:           remoting.I64Ptr(sub.SubVersion),
		ExpressionType:       remoting.StrPtr(sub.ExpressionType),
		MaxMsgBytes:          remoting.I32Ptr(maxMsgBytes),
		RequestSource:        remoting.I32Ptr(0),
	}
	// Rule 2: the expression rides the wire only when the SUBSCRIPTION bit is
	// on. Java's makeCustomHeaderToNet drops the null field entirely, so the
	// key must not be present at all when the bit is off.
	if common.HasSubscriptionFlag(sysFlag) {
		header.Subscription = remoting.StrPtr(sub.SubString)
	}

	request := remoting.CreateRequestCommand(code, header)
	response, err := p.instance.invokeSync(addr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}

	var status PullStatus
	switch response.Code {
	case remoting.RespSuccess:
		status = PullFound
	case remoting.RespPullNotFound:
		status = PullNoNewMsg
	case remoting.RespPullOffsetMoved:
		status = PullOffsetIllegal
	case remoting.RespPullRetryImmediately:
		status = PullNoMatchedMsg
	default:
		return nil, common.BrokerError(response.Code, response.Remark)
	}

	var respHeader remoting.PullMessageResponseHeader
	respHeader.FromExtFields(response.ExtFields())

	var found2 []*common.MessageExt
	if len(response.Body) > 0 {
		found2 = common.DecodeMessages(response.Body)
		for _, msg := range found2 {
			msg.BrokerName = mq.BrokerName
			msg.QueueID = mq.QueueID
		}
	}
	return &PullResult{
		PullStatus:           status,
		NextBeginOffset:      i64Or(respHeader.NextBeginOffset, 0),
		MinOffset:            i64Or(respHeader.MinOffset, 0),
		MaxOffset:            i64Or(respHeader.MaxOffset, 0),
		MsgFoundList:         found2,
		SuggestWhichBrokerID: respHeader.SuggestWhichBrokerID,
	}, nil
}

// processPullResult mirrors Java PullAPIWrapper#processPullResult. It MUST run
// for every response, not only FOUND ones — rule 3: the node table is fed by
// every reply, and absent means master.
func (p *pullAPI) processPullResult(mq common.MessageQueue, result *PullResult,
	sub *remoting.SubscriptionData) *PullResult {

	brokerID := int64(common.MasterID)
	if result.SuggestWhichBrokerID != nil {
		brokerID = int64(*result.SuggestWhichBrokerID)
	}
	p.updatePullFromWhichNode(mq, brokerID)

	if result.PullStatus != PullFound || len(result.MsgFoundList) == 0 {
		return result
	}
	msgs := clientSideTagFilter(sub, result.MsgFoundList)
	if len(p.hooks) > 0 {
		ctx := &FilterMessageContext{
			ConsumerGroup: p.group,
			MsgList:       msgs,
			MQ:            mq,
			UnitMode:      p.unitMode,
			AccessChannel: "LOCAL",
		}
		executeFilterHooks(p.hooks, ctx)
		msgs = ctx.MsgList
	}
	for _, msg := range msgs {
		// A half message carries its transaction id in UNIQ_KEY; Java lifts it
		// so the listener can see it as MessageExt.getTransactionId().
		if raw, ok := msg.GetProperty(common.PropertyTransactionPrepared); ok && raw == "true" {
			if uniq, ok := msg.GetProperty(common.PropertyUniqKey); ok {
				msg.TransactionID = uniq
			}
		}
		msg.PutProperty(common.PropertyMinOffset, i64Text(result.MinOffset))
		msg.PutProperty(common.PropertyMaxOffset, i64Text(result.MaxOffset))
		msg.BrokerName = mq.BrokerName
	}
	result.MsgFoundList = msgs
	return result
}

// ---------------------------------------------------------------- small helpers

func i64Or(v *int64, def int64) int64 {
	if v == nil {
		return def
	}
	return *v
}

func i32Or(v *int32, def int32) int32 {
	if v == nil {
		return def
	}
	return *v
}

func i64Text(v int64) string { return fmt.Sprintf("%d", v) }
