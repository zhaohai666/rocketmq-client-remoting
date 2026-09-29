package common

import (
	"fmt"
	"math/bits"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MixAll constants and helpers (Java org.apache.rocketmq.common.MixAll), cut
// down to what the client layer uses.
const (
	NamesrvAddrProperty  = "rocketmq.namesrv.addr"
	NamesrvAddrEnv       = "NAMESRV_ADDR"
	MessageCompressLevel = "rocketmq.message.compressLevel"

	DefaultTopic             = "TBW102"
	BenchmarkTopic           = "BenchmarkTest"
	DefaultProducerGroup     = "DEFAULT_PRODUCER"
	DefaultConsumerGroup     = "DEFAULT_CONSUMER"
	ClientInnerProducerGroup = "CLIENT_INNER_PRODUCER"

	RetryGroupTopicPrefix = "%RETRY%"
	DLQGroupTopicPrefix   = "%DLQ%"
	ReplyTopicPrefix      = "%REPLY%"
	// Request-Reply: reply topic = <cluster>_REPLY_TOPIC.
	ReplyTopicPostfix = "REPLY_TOPIC"
	// Request-Reply: MSG_TYPE property value of a reply message.
	ReplyMessageFlag  = "reply"
	SystemTopicPrefix = "rmq_sys_"

	UniqueMsgQueryFlag = "_UNIQUE_KEY_QUERY"
	TraceTopic         = "RMQ_SYS_TRACE_TOPIC"
	// Region placeholder when the SEND response header carries no MSG_REGION.
	DefaultTraceRegionID = "DefaultRegion"

	// ReqT: the extFields key StreamTypeRPCHook writes.
	ReqT              = "ReqT"
	StreamRequestType = "STREAM"

	DefaultTopicQueueNums         int32 = 4
	MaxTopicLength                      = 127
	MaxGroupLength                      = 255
	MasterID                      int32 = 0
	DefaultInstanceName                 = "DEFAULT"
	DefaultTraceRegionPlaceholder       = "DefaultRegion"
)

// GetRetryTopic mirrors Java MixAll.getRetryTopic.
func GetRetryTopic(consumerGroup string) string { return RetryGroupTopicPrefix + consumerGroup }

func IsRetryTopic(topic string) bool { return strings.HasPrefix(topic, RetryGroupTopicPrefix) }

// GetDLQTopic mirrors Java MixAll.getDLQTopic.
func GetDLQTopic(consumerGroup string) string { return DLQGroupTopicPrefix + consumerGroup }

func IsDLQTopic(topic string) bool { return strings.HasPrefix(topic, DLQGroupTopicPrefix) }

// GetReplyTopic mirrors Java MixAll.getReplyTopic(clusterName). NOT the
// %REPLY%<topic> prefix (that is a different mechanism).
func GetReplyTopic(clusterName string) string { return clusterName + "_" + ReplyTopicPostfix }

// ResetRetryAndDLQTopic strips the %RETRY% / %DLQ% prefix.
func ResetRetryAndDLQTopic(topic string) string {
	if IsRetryTopic(topic) {
		return topic[len(RetryGroupTopicPrefix):]
	}
	if IsDLQTopic(topic) {
		return topic[len(DLQGroupTopicPrefix):]
	}
	return topic
}

// CompareAndIncreaseNamespace mirrors Java MixAll.compareAndIncreaseNamespace;
// an empty namespace is a no-op.
func CompareAndIncreaseNamespace(instanceName, namespace string) string {
	if namespace == "" {
		return instanceName
	}
	if strings.HasPrefix(instanceName, namespace) {
		return instanceName
	}
	if strings.HasPrefix(instanceName, "%"+namespace) {
		return instanceName
	}
	return fmt.Sprintf("%%%s%%%%%s", namespace, instanceName)
}

var uniqNameCounter uint64

// CreateUniqName mirrors Java MixAll.createUniqName: prefix + 32 hex chars
// built from pid/nanos/per-process counter.
func CreateUniqName(prefix string) string {
	seq := atomic.AddUint64(&uniqNameCounter, 1) - 1
	nanos := uint64(time.Now().UnixNano())
	return fmt.Sprintf("%s%08x%08x%08x%08x", prefix,
		uint32(Pid()), uint32(nanos>>32), uint32(nanos),
		uint32(seq>>32)^bits.RotateLeft32(uint32(seq), 16))
}

var (
	cachedIPOnce  sync.Once
	cachedIP      string
	cachedPIDOnce sync.Once
	cachedPIDVal  int
)

// CachedIPStr probes the outbound IP once per process (msgId/clientId reuse it).
func CachedIPStr() string {
	cachedIPOnce.Do(func() { cachedIP = LocalIP() })
	return cachedIP
}

func CachedPID() int {
	cachedPIDOnce.Do(func() { cachedPIDVal = Pid() })
	return cachedPIDVal
}

// BuildMQClientID mirrors Java ClientConfig#buildMQClientId:
// clientIP + "@" + instanceName + ("@" + unitName if not blank) + ("@STREAM" if enabled).
func BuildMQClientID(clientIP, instanceName, unitName string, enableStreamRequestType bool) string {
	var sb strings.Builder
	sb.WriteString(clientIP)
	sb.WriteByte('@')
	sb.WriteString(instanceName)
	if IsNotBlank(unitName) {
		sb.WriteByte('@')
		sb.WriteString(unitName)
	}
	if enableStreamRequestType {
		sb.WriteByte('@')
		sb.WriteString(StreamRequestType)
	}
	return sb.String()
}

// ChangeInstanceNameToPID mirrors Java ClientConfig#changeInstanceNameToPID:
// only the default name becomes "<pid>#<nanoTime>" (idempotent afterwards).
func ChangeInstanceNameToPID(instanceName string) string {
	if instanceName == DefaultInstanceName {
		return fmt.Sprintf("%d#%d", CachedPID(), NanoTime())
	}
	return instanceName
}

// InstanceNameForModel: clustering rewrites DEFAULT to pid#nanoTime;
// broadcasting deliberately keeps the name so same-process broadcast consumers
// share the instance.
func InstanceNameForModel(instanceName string, clustering bool) string {
	if clustering {
		return ChangeInstanceNameToPID(instanceName)
	}
	return instanceName
}

// ClientIDFor is the default clientId: <local IP>@<pid>#<nanoTime>[@unit][@STREAM].
func ClientIDFor(instanceName, unitName string, enableStreamRequestType bool) string {
	return BuildMQClientID(CachedIPStr(), ChangeInstanceNameToPID(instanceName), unitName, enableStreamRequestType)
}

// BrokerVIPChannel mirrors Java MixAll.brokerVIPChannel: VIP port = port - 2;
// unparseable ports come back unchanged.
func BrokerVIPChannel(isChange bool, brokerAddr string) string {
	if !isChange {
		return brokerAddr
	}
	i := strings.LastIndexByte(brokerAddr, ':')
	if i < 0 {
		return brokerAddr
	}
	port, err := strconv.ParseInt(brokerAddr[i+1:], 10, 64)
	if err != nil {
		return brokerAddr
	}
	return fmt.Sprintf("%s:%d", brokerAddr[:i], port-2)
}

// MessageQueueToString renders "topic brokerName queueId".
func MessageQueueToString(q MessageQueue) string { return q.String() }

// StringToMessageQueue splits on separator into topic/broker/queueId.
func StringToMessageQueue(queue, separator string) (MessageQueue, error) {
	parts := strings.Split(queue, separator)
	if len(parts) < 3 {
		return MessageQueue{}, DecodeError(fmt.Sprintf("message queue string %q has %d parts, need 3", queue, len(parts)))
	}
	queueID, err := strconv.ParseInt(strings.TrimSpace(parts[2]), 10, 32)
	if err != nil {
		return MessageQueue{}, DecodeError(fmt.Sprintf("bad queueId in %q: %v", queue, err))
	}
	return NewMessageQueue(parts[0], parts[1], int32(queueID)), nil
}

// StringToMessageQueues parses newline-separated queues, skipping blank lines
// and (like Java) silently skipping lines that fail to parse.
func StringToMessageQueues(queues string) []MessageQueue {
	var out []MessageQueue
	for _, line := range strings.Split(queues, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if mq, err := StringToMessageQueue(line, " "); err == nil {
			out = append(out, mq)
		}
	}
	return out
}

// PropertiesToString renders each entry as `key=value\n`; isSort sorts by key.
func PropertiesToString(properties *StringMap, isSort bool) string {
	type kv struct{ k, v string }
	items := make([]kv, 0, properties.Len())
	properties.Range(func(k, v string) { items = append(items, kv{k, v}) })
	if isSort {
		sort.Slice(items, func(i, j int) bool { return items[i].k < items[j].k })
	}
	var buf strings.Builder
	for _, it := range items {
		buf.WriteString(it.k)
		buf.WriteByte('=')
		buf.WriteString(it.v)
		buf.WriteByte('\n')
	}
	return buf.String()
}
