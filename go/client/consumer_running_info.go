// Client-side ConsumerRunningInfo — the answer to
// GET_CONSUMER_RUNNING_INFO(307).
//
// Java builds it in two layers, and the split matters:
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
//
// Oracle status (bodies captured from a live consumer and fed to the 5.5.1
// jars): fastjson2 parses every shape below with zero rejections. The two 309
// verdicts re-encode BYTE-IDENTICALLY. The 307 bodies are deep-equal —
// same keys, same values — with one cosmetic difference: the ORDER of the keys
// inside `properties`. Java's `MixAll.object2Properties` returns a
// java.util.Properties (a Hashtable), so its iteration order comes from the
// bucket layout and is not alphabetical (measured for the three-key case:
// PROP_CONSUMER_START_TIMESTAMP, then PROP_CONSUMEORDERLY, then
// PROP_THREADPOOL_CORE_SIZE). Go sorts its map keys like the other four ports.
// JSON object order is not semantically meaningful and Hashtable's layout is an
// unspecified implementation detail, so this is deliberately NOT replicated.
package client

import (
	"strconv"
	"strings"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// Instance.consumerRunningInfo is Java MQClientInstance#consumerRunningInfo
// (:1546-1562): the consumer's own body plus the three instance-level
// properties. Returns nil for an unknown group, which the caller turns into
// ResponseCode.SYSTEM_ERROR.
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
	// Java appends ";" to EVERY address, so N addresses end with one too.
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

// consumerStartTimestampText renders Java's `consumerStartTimestamp`.
//
// Java declares it as a plain `long` that stays 0 until start() stamps it
// (DefaultMQPushConsumer.consumerStartTimestamp / the pull consumer's twin), so
// an unstarted consumer answers "0" and the console reads that as "not started".
// Go's time.Time zero value is NOT 0 epoch — UnixMilli() on it is
// -6795364578871, i.e. the year 1754 — so the zero case has to be translated
// explicitly or an unstarted consumer reports a nonsense start date.
func consumerStartTimestampText(start time.Time) string {
	if start.IsZero() {
		return "0"
	}
	return strconv.FormatInt(start.UnixMilli(), 10)
}

// ---------------- push consumer ----------------

// ConsumerRunningInfo is Java DefaultMQPushConsumerImpl#consumerRunningInfo
// (:1446-1489).
//
// Java starts from MixAll.object2Properties(this.defaultMQPushConsumer) — a
// reflective dump of every non-null field of the consumer object, including
// inner objects' toString(). That is not reproducible outside Java (and the
// values would be Java toString() text), so all five ports emit the reduced set
// of properties that actually carry information, in the same keys: the three
// per-consumer ones below plus the three the instance adds. Java OVERWRITES the
// same three keys after the dump, so this is a subset, not a redefinition.
func (c *DefaultMQPushConsumer) ConsumerRunningInfo() *remoting.ConsumerRunningInfo {
	info := remoting.NewConsumerRunningInfo()

	c.mu.Lock()
	orderly := c.orderly
	// Java reads this from consumeMessageService.getCorePoolSize(), i.e. the
	// executor's core size — which start() installs as consumeThreadMin. Go's
	// corePoolSize field is only stamped at start, so the effective value has to
	// be derived; reporting the raw field would say "0 consume threads".
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

// fillProcessQueueInfo builds one mqTable entry: the committed offset Java reads
// with MEMORY_FIRST_THEN_STORE, plus the queue's own snapshot.
//
// Java stores the raw return of readOffset, which is -1 when the offset is
// unknown; the field is a primitive long so -1 travels as -1 rather than being
// clamped to 0. Reading it as 0 would make an unknown-offset queue look like a
// queue that has consumed nothing.
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

// subscriptionDataJSON is the wire shape Java writes for SubscriptionData
// (classFilterMode / topic / subString / tagsSet / codeSet / subVersion /
// expressionType — note `filterClassSource` is NOT serialised).
func subscriptionDataJSON(sub *remoting.SubscriptionData) map[string]any {
	if sub == nil {
		return map[string]any{}
	}
	return sub.ToJSONValue()
}

// ---------------- pull consumer ----------------

// ConsumerRunningInfo is Java DefaultMQPullConsumerImpl#consumerRunningInfo
// (:450-459): properties + subscriptionSet only. There is deliberately no
// mqTable — a pull consumer has no rebalance-owned process queue table, and
// fabricating one from the caller's offsets would claim a state the consumer
// does not track. (Java's LITE pull consumer is the one that adds mqTable; it is
// not registered with the instance in this port, see the note in
// lite_pull_consumer.go.)
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
