// Client-side ConsumerRunningInfo — the answer to
// GET_CONSUMER_RUNNING_INFO(307).
//
//	MQClientInstance.consumerRunningInfo(group)   <- instance-level props
//	  └── MQConsumerInner.consumerRunningInfo()   <- per-consumer-type shape
//
// The instance layer adds exactly three properties and they are the ones the
// admin console reads first: PROP_NAMESERVER_ADDR (every nameserver address with
// a trailing ';', so a single address becomes "127.0.0.1:9876;"),
// PROP_CONSUME_TYPE (the ConsumeType enum NAME) and PROP_CLIENT_VERSION
// (MQVersion.getVersionDesc(CURRENT_VERSION)).
//
// The per-type layer differs by consumer kind, which is why this file has one
// builder per type rather than one shared body:
//
//	push  : props + subscriptionSet + mqTable + mqPopTable + statusTable
//	pull  : props + subscriptionSet
//	lite  : props + subscriptionSet + mqTable
//
// `mqTable` and `mqPopTable` are DISJOINT, always. A classic consumer's
// rebalance processQueueTable is empty in POP mode and vice versa, and putting
// the same queue in both makes the console count one consumption path as two.
package client

import (
	"strconv"
	"strings"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

func (i *Instance) consumerRunningInfo(group string) *remoting.ConsumerRunningInfo {
	consumer, ok := i.FindConsumer(group)
	if !ok {
		return nil
	}
	info := consumer.ConsumerRunningInfo()
	if info == nil {
		return nil
	}
	if info.Properties == nil {
		info.Properties = map[string]string{}
	}
	var sb strings.Builder
	for _, addr := range i.NameServerAddrs() {
		sb.WriteString(addr)
		sb.WriteString(";")
	}
	info.Properties[remoting.PropNameServerAddr] = sb.String()
	info.Properties[remoting.PropConsumeType] = consumer.ConsumeType()
	info.Properties[remoting.PropClientVersion] = remoting.CurrentVersionDesc
	return info
}

func consumerStartTimestampText(start time.Time) string {
	if start.IsZero() {
		return "0"
	}
	return strconv.FormatInt(start.UnixMilli(), 10)
}

// ---------------- push consumer ----------------

func (c *DefaultMQPushConsumer) ConsumerRunningInfo() *remoting.ConsumerRunningInfo {
	info := remoting.NewConsumerRunningInfo()

	c.mu.Lock()
	orderly := c.orderly
	corePoolSize := c.corePoolSize
	if corePoolSize <= 0 {
		corePoolSize = c.consumeThreadMin
	}
	startTime := c.startTime
	popMode := c.popMode
	processQueues := make(map[common.MessageQueue]*processQueue, len(c.processQueueTable))
	for mq, pq := range c.processQueueTable {
		processQueues[mq] = pq
	}
	popQueues := make(map[common.MessageQueue]*popProcessQueue, len(c.popQueueTable))
	for mq, pq := range c.popQueueTable {
		popQueues[mq] = pq
	}
	offsetStore := c.offsetStore
	group := c.consumerGroup
	c.mu.Unlock()

	info.Properties[remoting.PropConsumeOrderly] = strconv.FormatBool(orderly)
	info.Properties[remoting.PropThreadPoolCoreSize] = strconv.Itoa(corePoolSize)
	info.Properties[remoting.PropConsumerStartTimestamp] = consumerStartTimestampText(startTime)

	subscriptions := c.Subscriptions()
	for _, sub := range subscriptions {
		info.SubscriptionSet = append(info.SubscriptionSet, subscriptionDataJSON(sub))
	}

	// The two tables are mutually exclusive by construction: processQueueTable
	// is only populated on the pull path and popQueueTable only on the pop path.
	// Go's POP mode keeps the queues in popQueueTable, so popMode is not even
	// needed for the split — but it is asserted below so a future refactor that
	// starts populating both cannot do so silently.
	if popMode && len(processQueues) > 0 {
		common.LogWarnf("consumer %s is in POP mode but has %d classic process queues; "+
			"the 307 answer would list the same queue in both tables",
			group, len(processQueues))
	}
	for mq, pq := range processQueues {
		info.MQTable[mq] = fillProcessQueueInfo(pq, offsetStore, mq)
	}
	for mq, pq := range popQueues {
		pqi := &remoting.PopProcessQueueInfo{}
		pq.FillPopProcessQueueInfo(pqi)
		info.MQPopTable[mq] = pqi.ToJSONValue()
	}

	// statusTable is per SUBSCRIBED TOPIC (not per queue) and comes from the
	// shared stats manager; a missing manager yields the zero snapshot rather
	// than dropping the key, because the console shows "no data" for an absent
	// topic and "zero" is the truthful answer.
	for _, sub := range subscriptions {
		if c.instance != nil && c.instance.consumerStats != nil {
			info.StatusTable[sub.Topic] = c.instance.consumerStats.consumeStatus(group, sub.Topic).ToJSONValue()
			continue
		}
		info.StatusTable[sub.Topic] = (&remoting.ConsumeStatus{}).ToJSONValue()
	}
	return info
}

func fillProcessQueueInfo(pq *processQueue, store OffsetStore, mq common.MessageQueue) map[string]any {
	info := &remoting.ProcessQueueInfo{CommitOffset: -1}
	if store != nil {
		if v, err := store.ReadOffset(mq, ReadMemoryFirst); err == nil {
			info.CommitOffset = v
		}
	}
	pq.FillProcessQueueInfo(info)
	return info.ToJSONValue()
}

func subscriptionDataJSON(sub *remoting.SubscriptionData) map[string]any {
	if sub == nil {
		return map[string]any{}
	}
	return sub.ToJSONValue()
}

// ---------------- pull consumer ----------------

func (c *DefaultMQPullConsumer) ConsumerRunningInfo() *remoting.ConsumerRunningInfo {
	info := remoting.NewConsumerRunningInfo()

	c.mu.Lock()
	startTime := c.startTime
	c.mu.Unlock()

	info.Properties[remoting.PropConsumerStartTimestamp] = consumerStartTimestampText(startTime)

	for _, sub := range c.Subscriptions() {
		info.SubscriptionSet = append(info.SubscriptionSet, subscriptionDataJSON(sub))
	}
	return info
}
