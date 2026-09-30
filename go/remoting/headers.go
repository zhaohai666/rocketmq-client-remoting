// Command headers (Java org.apache.rocketmq.remoting.protocol.header.*).
//
// Field names must match the Java reflection keys byte for byte — the wire
// carries them as extFields keys. All fields are pointers: nil means the key
// is not written (Java skips null fields). Bools serialize lowercase
// ("true"/"false", Java Boolean.toString).
package remoting

import (
	"strconv"
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// StrPtr / I32Ptr / I64Ptr / U32Ptr / BoolPtr build pointer fields for header
// literals.
func StrPtr(v string) *string { return &v }
func I32Ptr(v int32) *int32   { return &v }
func I64Ptr(v int64) *int64   { return &v }
func U32Ptr(v uint32) *uint32 { return &v }
func BoolPtr(v bool) *bool    { return &v }

func putStr(ext *common.StringMap, key string, v *string) {
	if v != nil {
		ext.Put(key, *v)
	}
}

func putI32(ext *common.StringMap, key string, v *int32) {
	if v != nil {
		ext.Put(key, strconv.FormatInt(int64(*v), 10))
	}
}

func putI64(ext *common.StringMap, key string, v *int64) {
	if v != nil {
		ext.Put(key, strconv.FormatInt(*v, 10))
	}
}

func putU32(ext *common.StringMap, key string, v *uint32) {
	if v != nil {
		ext.Put(key, strconv.FormatUint(uint64(*v), 10))
	}
}

func putBool(ext *common.StringMap, key string, v *bool) {
	if v != nil {
		if *v {
			ext.Put(key, "true")
		} else {
			ext.Put(key, "false")
		}
	}
}

func getStr(ext *common.StringMap, key string) *string {
	if v, ok := ext.Get(key); ok {
		return &v
	}
	return nil
}

func getI32(ext *common.StringMap, key string) *int32 {
	v, ok := ext.Get(key)
	if !ok {
		return nil
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return nil
	}
	out := int32(n)
	return &out
}

func getI64(ext *common.StringMap, key string) *int64 {
	v, ok := ext.Get(key)
	if !ok {
		return nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return nil
	}
	return &n
}

func getU32(ext *common.StringMap, key string) *uint32 {
	v, ok := ext.Get(key)
	if !ok {
		return nil
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return nil
	}
	out := uint32(n)
	return &out
}

func getBool(ext *common.StringMap, key string) *bool {
	v, ok := ext.Get(key)
	if !ok {
		return nil
	}
	out := strings.EqualFold(v, "true") || v == "1"
	return &out
}

// BoundaryType mirrors Java BoundaryType; the wire text is the uppercase enum
// name.
type BoundaryType string

const (
	BoundaryLower BoundaryType = "LOWER"
	BoundaryUpper BoundaryType = "UPPER"
)

// ---------------- producer ----------------

// SendMessageRequestHeader (SEND_MESSAGE / SEND_MESSAGE_V2 short keys a..m).
type SendMessageRequestHeader struct {
	ProducerGroup         *string
	Topic                 *string
	DefaultTopic          *string
	DefaultTopicQueueNums *int32
	QueueID               *int32
	SysFlag               *int32
	BornTimestamp         *int64
	Flag                  *int32
	Properties            *string
	ReconsumeTimes        *int32
	UnitMode              *bool
	MaxReconsumeTimes     *int32
	Batch                 *bool
}

func (h *SendMessageRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "producerGroup", h.ProducerGroup)
	putStr(out, "topic", h.Topic)
	putStr(out, "defaultTopic", h.DefaultTopic)
	putI32(out, "defaultTopicQueueNums", h.DefaultTopicQueueNums)
	putI32(out, "queueId", h.QueueID)
	putI32(out, "sysFlag", h.SysFlag)
	putI64(out, "bornTimestamp", h.BornTimestamp)
	putI32(out, "flag", h.Flag)
	putStr(out, "properties", h.Properties)
	putI32(out, "reconsumeTimes", h.ReconsumeTimes)
	putBool(out, "unitMode", h.UnitMode)
	putI32(out, "maxReconsumeTimes", h.MaxReconsumeTimes)
	putBool(out, "batch", h.Batch)
}

func (h *SendMessageRequestHeader) FromExtFields(ext *common.StringMap) {
	h.ProducerGroup = getStr(ext, "producerGroup")
	h.Topic = getStr(ext, "topic")
	h.DefaultTopic = getStr(ext, "defaultTopic")
	h.DefaultTopicQueueNums = getI32(ext, "defaultTopicQueueNums")
	h.QueueID = getI32(ext, "queueId")
	h.SysFlag = getI32(ext, "sysFlag")
	h.BornTimestamp = getI64(ext, "bornTimestamp")
	h.Flag = getI32(ext, "flag")
	h.Properties = getStr(ext, "properties")
	h.ReconsumeTimes = getI32(ext, "reconsumeTimes")
	h.UnitMode = getBool(ext, "unitMode")
	h.MaxReconsumeTimes = getI32(ext, "maxReconsumeTimes")
	h.Batch = getBool(ext, "batch")
}

// CreateV2 converts to the V2 (SEND_MESSAGE_V2) header. The brokerName field
// (key "n") is set by the producer, not here — Java's converter does the same.
func (h *SendMessageRequestHeader) CreateV2() *SendMessageRequestHeaderV2 {
	return &SendMessageRequestHeaderV2{
		ProducerGroup:         h.ProducerGroup,
		Topic:                 h.Topic,
		DefaultTopic:          h.DefaultTopic,
		DefaultTopicQueueNums: h.DefaultTopicQueueNums,
		QueueID:               h.QueueID,
		SysFlag:               h.SysFlag,
		BornTimestamp:         h.BornTimestamp,
		Flag:                  h.Flag,
		Properties:            h.Properties,
		ReconsumeTimes:        h.ReconsumeTimes,
		UnitMode:              h.UnitMode,
		MaxReconsumeTimes:     h.MaxReconsumeTimes,
		Batch:                 h.Batch,
	}
}

// SendMessageRequestHeaderV2 uses the single-letter extFields keys a..n.
type SendMessageRequestHeaderV2 struct {
	ProducerGroup         *string // a
	Topic                 *string // b
	DefaultTopic          *string // c
	DefaultTopicQueueNums *int32  // d
	QueueID               *int32  // e
	SysFlag               *int32  // f
	BornTimestamp         *int64  // g
	Flag                  *int32  // h
	Properties            *string // i
	ReconsumeTimes        *int32  // j
	UnitMode              *bool   // k
	MaxReconsumeTimes     *int32  // l
	Batch                 *bool   // m
	BrokerName            *string // n
}

func (h *SendMessageRequestHeaderV2) ToExtFields(out *common.StringMap) {
	putStr(out, "a", h.ProducerGroup)
	putStr(out, "b", h.Topic)
	putStr(out, "c", h.DefaultTopic)
	putI32(out, "d", h.DefaultTopicQueueNums)
	putI32(out, "e", h.QueueID)
	putI32(out, "f", h.SysFlag)
	putI64(out, "g", h.BornTimestamp)
	putI32(out, "h", h.Flag)
	putStr(out, "i", h.Properties)
	putI32(out, "j", h.ReconsumeTimes)
	putBool(out, "k", h.UnitMode)
	putI32(out, "l", h.MaxReconsumeTimes)
	putBool(out, "m", h.Batch)
	putStr(out, "n", h.BrokerName)
}

func (h *SendMessageRequestHeaderV2) FromExtFields(ext *common.StringMap) {
	h.ProducerGroup = getStr(ext, "a")
	h.Topic = getStr(ext, "b")
	h.DefaultTopic = getStr(ext, "c")
	h.DefaultTopicQueueNums = getI32(ext, "d")
	h.QueueID = getI32(ext, "e")
	h.SysFlag = getI32(ext, "f")
	h.BornTimestamp = getI64(ext, "g")
	h.Flag = getI32(ext, "h")
	h.Properties = getStr(ext, "i")
	h.ReconsumeTimes = getI32(ext, "j")
	h.UnitMode = getBool(ext, "k")
	h.MaxReconsumeTimes = getI32(ext, "l")
	h.Batch = getBool(ext, "m")
	h.BrokerName = getStr(ext, "n")
}

// CreateV1 converts back to the V1 header (brokerName has no V1 slot).
func (h *SendMessageRequestHeaderV2) CreateV1() *SendMessageRequestHeader {
	return &SendMessageRequestHeader{
		ProducerGroup:         h.ProducerGroup,
		Topic:                 h.Topic,
		DefaultTopic:          h.DefaultTopic,
		DefaultTopicQueueNums: h.DefaultTopicQueueNums,
		QueueID:               h.QueueID,
		SysFlag:               h.SysFlag,
		BornTimestamp:         h.BornTimestamp,
		Flag:                  h.Flag,
		Properties:            h.Properties,
		ReconsumeTimes:        h.ReconsumeTimes,
		UnitMode:              h.UnitMode,
		MaxReconsumeTimes:     h.MaxReconsumeTimes,
		Batch:                 h.Batch,
	}
}

// ReplyMessageRequestHeader (SEND_REPLY_MESSAGE).
type ReplyMessageRequestHeader struct {
	ProducerGroup         *string
	Topic                 *string
	DefaultTopic          *string
	DefaultTopicQueueNums *int32
	QueueID               *int32
	SysFlag               *int32
	BornTimestamp         *int64
	Flag                  *int32
	Properties            *string
	ReconsumeTimes        *int32
	UnitMode              *bool
	BornHost              *string
	StoreHost             *string
	StoreTimestamp        *int64
}

func (h *ReplyMessageRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "producerGroup", h.ProducerGroup)
	putStr(out, "topic", h.Topic)
	putStr(out, "defaultTopic", h.DefaultTopic)
	putI32(out, "defaultTopicQueueNums", h.DefaultTopicQueueNums)
	putI32(out, "queueId", h.QueueID)
	putI32(out, "sysFlag", h.SysFlag)
	putI64(out, "bornTimestamp", h.BornTimestamp)
	putI32(out, "flag", h.Flag)
	putStr(out, "properties", h.Properties)
	putI32(out, "reconsumeTimes", h.ReconsumeTimes)
	putBool(out, "unitMode", h.UnitMode)
	putStr(out, "bornHost", h.BornHost)
	putStr(out, "storeHost", h.StoreHost)
	putI64(out, "storeTimestamp", h.StoreTimestamp)
}

func (h *ReplyMessageRequestHeader) FromExtFields(ext *common.StringMap) {
	h.ProducerGroup = getStr(ext, "producerGroup")
	h.Topic = getStr(ext, "topic")
	h.DefaultTopic = getStr(ext, "defaultTopic")
	h.DefaultTopicQueueNums = getI32(ext, "defaultTopicQueueNums")
	h.QueueID = getI32(ext, "queueId")
	h.SysFlag = getI32(ext, "sysFlag")
	h.BornTimestamp = getI64(ext, "bornTimestamp")
	h.Flag = getI32(ext, "flag")
	h.Properties = getStr(ext, "properties")
	h.ReconsumeTimes = getI32(ext, "reconsumeTimes")
	h.UnitMode = getBool(ext, "unitMode")
	h.BornHost = getStr(ext, "bornHost")
	h.StoreHost = getStr(ext, "storeHost")
	h.StoreTimestamp = getI64(ext, "storeTimestamp")
}

// SendMessageResponseHeader.
type SendMessageResponseHeader struct {
	MsgID         *string
	QueueID       *int32
	QueueOffset   *int64
	TransactionID *string
	BatchUniqID   *string
	RecallHandle  *string
}

func (h *SendMessageResponseHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "msgId", h.MsgID)
	putI32(out, "queueId", h.QueueID)
	putI64(out, "queueOffset", h.QueueOffset)
	putStr(out, "transactionId", h.TransactionID)
	putStr(out, "batchUniqId", h.BatchUniqID)
	putStr(out, "recallHandle", h.RecallHandle)
}

func (h *SendMessageResponseHeader) FromExtFields(ext *common.StringMap) {
	h.MsgID = getStr(ext, "msgId")
	h.QueueID = getI32(ext, "queueId")
	h.QueueOffset = getI64(ext, "queueOffset")
	h.TransactionID = getStr(ext, "transactionId")
	h.BatchUniqID = getStr(ext, "batchUniqId")
	h.RecallHandle = getStr(ext, "recallHandle")
}

// RecallMessageRequestHeader (RECALL_MESSAGE = 370). The inherited
// RpcRequestHeader field reflects as "bname", NOT "brokerName" — writing the
// wrong key silently drops the field on the broker side.
type RecallMessageRequestHeader struct {
	ProducerGroup *string
	Topic         *string
	RecallHandle  *string
	Bname         *string
}

func (h *RecallMessageRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "producerGroup", h.ProducerGroup)
	putStr(out, "topic", h.Topic)
	putStr(out, "recallHandle", h.RecallHandle)
	putStr(out, "bname", h.Bname)
}

func (h *RecallMessageRequestHeader) FromExtFields(ext *common.StringMap) {
	h.ProducerGroup = getStr(ext, "producerGroup")
	h.Topic = getStr(ext, "topic")
	h.RecallHandle = getStr(ext, "recallHandle")
	h.Bname = getStr(ext, "bname")
}

// RecallMessageResponseHeader.
type RecallMessageResponseHeader struct {
	MsgID *string
}

func (h *RecallMessageResponseHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "msgId", h.MsgID)
}

func (h *RecallMessageResponseHeader) FromExtFields(ext *common.StringMap) {
	h.MsgID = getStr(ext, "msgId")
}

// EndTransactionRequestHeader (END_TRANSACTION). Same RpcRequestHeader rule:
// the inherited brokerName reflects as "bname".
type EndTransactionRequestHeader struct {
	Topic                *string
	ProducerGroup        *string
	TranStateTableOffset *int64
	CommitLogOffset      *int64
	CommitOrRollback     *int32
	FromTransactionCheck *bool
	MsgID                *string
	TransactionID        *string
	Bname                *string
}

func (h *EndTransactionRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "topic", h.Topic)
	putStr(out, "producerGroup", h.ProducerGroup)
	putI64(out, "tranStateTableOffset", h.TranStateTableOffset)
	putI64(out, "commitLogOffset", h.CommitLogOffset)
	putI32(out, "commitOrRollback", h.CommitOrRollback)
	putBool(out, "fromTransactionCheck", h.FromTransactionCheck)
	putStr(out, "msgId", h.MsgID)
	putStr(out, "transactionId", h.TransactionID)
	putStr(out, "bname", h.Bname)
}

func (h *EndTransactionRequestHeader) FromExtFields(ext *common.StringMap) {
	h.Topic = getStr(ext, "topic")
	h.ProducerGroup = getStr(ext, "producerGroup")
	h.TranStateTableOffset = getI64(ext, "tranStateTableOffset")
	h.CommitLogOffset = getI64(ext, "commitLogOffset")
	h.CommitOrRollback = getI32(ext, "commitOrRollback")
	h.FromTransactionCheck = getBool(ext, "fromTransactionCheck")
	h.MsgID = getStr(ext, "msgId")
	h.TransactionID = getStr(ext, "transactionId")
	h.Bname = getStr(ext, "bname")
}

// EndTransactionResponseHeader.
type EndTransactionResponseHeader struct {
	MsgID         *string
	TransactionID *string
}

func (h *EndTransactionResponseHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "msgId", h.MsgID)
	putStr(out, "transactionId", h.TransactionID)
}

func (h *EndTransactionResponseHeader) FromExtFields(ext *common.StringMap) {
	h.MsgID = getStr(ext, "msgId")
	h.TransactionID = getStr(ext, "transactionId")
}

// ---------------- pull / offset ----------------

// PullMessageRequestHeader (PULL_MESSAGE / LITE_PULL_MESSAGE).
type PullMessageRequestHeader struct {
	ConsumerGroup        *string
	Topic                *string
	LiteTopic            *string
	QueueID              *int32
	QueueOffset          *int64
	MaxMsgNums           *int32
	SysFlag              *int32
	CommitOffset         *int64
	SuspendTimeoutMillis *int64
	Subscription         *string
	SubVersion           *int64
	ExpressionType       *string
	MaxMsgBytes          *int32
	RequestSource        *int32
	ProxyFrowardClientID *string
}

func (h *PullMessageRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "consumerGroup", h.ConsumerGroup)
	putStr(out, "topic", h.Topic)
	putStr(out, "liteTopic", h.LiteTopic)
	putI32(out, "queueId", h.QueueID)
	putI64(out, "queueOffset", h.QueueOffset)
	putI32(out, "maxMsgNums", h.MaxMsgNums)
	putI32(out, "sysFlag", h.SysFlag)
	putI64(out, "commitOffset", h.CommitOffset)
	putI64(out, "suspendTimeoutMillis", h.SuspendTimeoutMillis)
	putStr(out, "subscription", h.Subscription)
	putI64(out, "subVersion", h.SubVersion)
	putStr(out, "expressionType", h.ExpressionType)
	putI32(out, "maxMsgBytes", h.MaxMsgBytes)
	putI32(out, "requestSource", h.RequestSource)
	putStr(out, "proxyFrowardClientId", h.ProxyFrowardClientID)
}

func (h *PullMessageRequestHeader) FromExtFields(ext *common.StringMap) {
	h.ConsumerGroup = getStr(ext, "consumerGroup")
	h.Topic = getStr(ext, "topic")
	h.LiteTopic = getStr(ext, "liteTopic")
	h.QueueID = getI32(ext, "queueId")
	h.QueueOffset = getI64(ext, "queueOffset")
	h.MaxMsgNums = getI32(ext, "maxMsgNums")
	h.SysFlag = getI32(ext, "sysFlag")
	h.CommitOffset = getI64(ext, "commitOffset")
	h.SuspendTimeoutMillis = getI64(ext, "suspendTimeoutMillis")
	h.Subscription = getStr(ext, "subscription")
	h.SubVersion = getI64(ext, "subVersion")
	h.ExpressionType = getStr(ext, "expressionType")
	h.MaxMsgBytes = getI32(ext, "maxMsgBytes")
	h.RequestSource = getI32(ext, "requestSource")
	h.ProxyFrowardClientID = getStr(ext, "proxyFrowardClientId")
}

// PullMessageResponseHeader.
type PullMessageResponseHeader struct {
	NextBeginOffset      *int64
	MinOffset            *int64
	MaxOffset            *int64
	SuggestWhichBrokerID *int32
	TopicSysFlag         *int32
	GroupSysFlag         *int32
	ForbiddenType        *int32
	OffsetDelta          *int64
}

func (h *PullMessageResponseHeader) ToExtFields(out *common.StringMap) {
	putI64(out, "nextBeginOffset", h.NextBeginOffset)
	putI64(out, "minOffset", h.MinOffset)
	putI64(out, "maxOffset", h.MaxOffset)
	putI32(out, "suggestWhichBrokerId", h.SuggestWhichBrokerID)
	putI32(out, "topicSysFlag", h.TopicSysFlag)
	putI32(out, "groupSysFlag", h.GroupSysFlag)
	putI32(out, "forbiddenType", h.ForbiddenType)
	putI64(out, "offsetDelta", h.OffsetDelta)
}

func (h *PullMessageResponseHeader) FromExtFields(ext *common.StringMap) {
	h.NextBeginOffset = getI64(ext, "nextBeginOffset")
	h.MinOffset = getI64(ext, "minOffset")
	h.MaxOffset = getI64(ext, "maxOffset")
	h.SuggestWhichBrokerID = getI32(ext, "suggestWhichBrokerId")
	h.TopicSysFlag = getI32(ext, "topicSysFlag")
	h.GroupSysFlag = getI32(ext, "groupSysFlag")
	h.ForbiddenType = getI32(ext, "forbiddenType")
	h.OffsetDelta = getI64(ext, "offsetDelta")
}

// QueryConsumerOffsetRequestHeader.
type QueryConsumerOffsetRequestHeader struct {
	ConsumerGroup     *string
	Topic             *string
	QueueID           *int32
	SetZeroIfNotFound *bool
}

func (h *QueryConsumerOffsetRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "consumerGroup", h.ConsumerGroup)
	putStr(out, "topic", h.Topic)
	putI32(out, "queueId", h.QueueID)
	putBool(out, "setZeroIfNotFound", h.SetZeroIfNotFound)
}

func (h *QueryConsumerOffsetRequestHeader) FromExtFields(ext *common.StringMap) {
	h.ConsumerGroup = getStr(ext, "consumerGroup")
	h.Topic = getStr(ext, "topic")
	h.QueueID = getI32(ext, "queueId")
	h.SetZeroIfNotFound = getBool(ext, "setZeroIfNotFound")
}

// QueryConsumerOffsetResponseHeader.
type QueryConsumerOffsetResponseHeader struct {
	Offset *int64
}

func (h *QueryConsumerOffsetResponseHeader) ToExtFields(out *common.StringMap) {
	putI64(out, "offset", h.Offset)
}

func (h *QueryConsumerOffsetResponseHeader) FromExtFields(ext *common.StringMap) {
	h.Offset = getI64(ext, "offset")
}

// UpdateConsumerOffsetRequestHeader.
type UpdateConsumerOffsetRequestHeader struct {
	ConsumerGroup *string
	Topic         *string
	QueueID       *int32
	CommitOffset  *int64
}

func (h *UpdateConsumerOffsetRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "consumerGroup", h.ConsumerGroup)
	putStr(out, "topic", h.Topic)
	putI32(out, "queueId", h.QueueID)
	putI64(out, "commitOffset", h.CommitOffset)
}

func (h *UpdateConsumerOffsetRequestHeader) FromExtFields(ext *common.StringMap) {
	h.ConsumerGroup = getStr(ext, "consumerGroup")
	h.Topic = getStr(ext, "topic")
	h.QueueID = getI32(ext, "queueId")
	h.CommitOffset = getI64(ext, "commitOffset")
}

// UpdateConsumerOffsetResponseHeader.
type UpdateConsumerOffsetResponseHeader struct {
	Offset *int64
}

func (h *UpdateConsumerOffsetResponseHeader) ToExtFields(out *common.StringMap) {
	putI64(out, "offset", h.Offset)
}

func (h *UpdateConsumerOffsetResponseHeader) FromExtFields(ext *common.StringMap) {
	h.Offset = getI64(ext, "offset")
}

// GetMaxOffsetRequestHeader.
type GetMaxOffsetRequestHeader struct {
	Topic   *string
	QueueID *int32
}

func (h *GetMaxOffsetRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "topic", h.Topic)
	putI32(out, "queueId", h.QueueID)
}

func (h *GetMaxOffsetRequestHeader) FromExtFields(ext *common.StringMap) {
	h.Topic = getStr(ext, "topic")
	h.QueueID = getI32(ext, "queueId")
}

// GetMaxOffsetResponseHeader.
type GetMaxOffsetResponseHeader struct {
	Offset *int64
}

func (h *GetMaxOffsetResponseHeader) ToExtFields(out *common.StringMap) {
	putI64(out, "offset", h.Offset)
}

func (h *GetMaxOffsetResponseHeader) FromExtFields(ext *common.StringMap) {
	h.Offset = getI64(ext, "offset")
}

// GetMinOffsetRequestHeader.
type GetMinOffsetRequestHeader struct {
	Topic   *string
	QueueID *int32
}

func (h *GetMinOffsetRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "topic", h.Topic)
	putI32(out, "queueId", h.QueueID)
}

func (h *GetMinOffsetRequestHeader) FromExtFields(ext *common.StringMap) {
	h.Topic = getStr(ext, "topic")
	h.QueueID = getI32(ext, "queueId")
}

// GetMinOffsetResponseHeader.
type GetMinOffsetResponseHeader struct {
	Offset *int64
}

func (h *GetMinOffsetResponseHeader) ToExtFields(out *common.StringMap) {
	putI64(out, "offset", h.Offset)
}

func (h *GetMinOffsetResponseHeader) FromExtFields(ext *common.StringMap) {
	h.Offset = getI64(ext, "offset")
}

// SearchOffsetRequestHeader. boundaryType carries the uppercase enum name;
// when absent the whole key is skipped (@CFNullable).
type SearchOffsetRequestHeader struct {
	Topic        *string
	QueueID      *int32
	Timestamp    *int64
	BoundaryType *BoundaryType
}

func (h *SearchOffsetRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "topic", h.Topic)
	putI32(out, "queueId", h.QueueID)
	putI64(out, "timestamp", h.Timestamp)
	if h.BoundaryType != nil {
		out.Put("boundaryType", string(*h.BoundaryType))
	}
}

func (h *SearchOffsetRequestHeader) FromExtFields(ext *common.StringMap) {
	h.Topic = getStr(ext, "topic")
	h.QueueID = getI32(ext, "queueId")
	h.Timestamp = getI64(ext, "timestamp")
	if v, ok := ext.Get("boundaryType"); ok {
		// Java BoundaryType.getType is lenient: anything but UPPER is LOWER.
		if strings.EqualFold(v, string(BoundaryUpper)) {
			upper := BoundaryUpper
			h.BoundaryType = &upper
		} else {
			lower := BoundaryLower
			h.BoundaryType = &lower
		}
	}
}

// SearchOffsetResponseHeader.
type SearchOffsetResponseHeader struct {
	Offset *int64
}

func (h *SearchOffsetResponseHeader) ToExtFields(out *common.StringMap) {
	putI64(out, "offset", h.Offset)
}

func (h *SearchOffsetResponseHeader) FromExtFields(ext *common.StringMap) {
	h.Offset = getI64(ext, "offset")
}

// GetEarliestMsgStoretimeRequestHeader.
type GetEarliestMsgStoretimeRequestHeader struct {
	Topic   *string
	QueueID *int32
}

func (h *GetEarliestMsgStoretimeRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "topic", h.Topic)
	putI32(out, "queueId", h.QueueID)
}

func (h *GetEarliestMsgStoretimeRequestHeader) FromExtFields(ext *common.StringMap) {
	h.Topic = getStr(ext, "topic")
	h.QueueID = getI32(ext, "queueId")
}

// GetEarliestMsgStoretimeResponseHeader.
type GetEarliestMsgStoretimeResponseHeader struct {
	Timestamp *int64
}

func (h *GetEarliestMsgStoretimeResponseHeader) ToExtFields(out *common.StringMap) {
	putI64(out, "timestamp", h.Timestamp)
}

func (h *GetEarliestMsgStoretimeResponseHeader) FromExtFields(ext *common.StringMap) {
	h.Timestamp = getI64(ext, "timestamp")
}

// ---------------- client lifecycle / heartbeat ----------------

// HeartbeatRequestHeader. The Java field name is "clientID" (capital ID).
type HeartbeatRequestHeader struct {
	ClientID *string
}

func (h *HeartbeatRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "clientID", h.ClientID)
}

func (h *HeartbeatRequestHeader) FromExtFields(ext *common.StringMap) {
	h.ClientID = getStr(ext, "clientID")
}

// UnregisterClientRequestHeader.
type UnregisterClientRequestHeader struct {
	ClientID      *string
	ProducerGroup *string
	ConsumerGroup *string
}

func (h *UnregisterClientRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "clientID", h.ClientID)
	putStr(out, "producerGroup", h.ProducerGroup)
	putStr(out, "consumerGroup", h.ConsumerGroup)
}

func (h *UnregisterClientRequestHeader) FromExtFields(ext *common.StringMap) {
	h.ClientID = getStr(ext, "clientID")
	h.ProducerGroup = getStr(ext, "producerGroup")
	h.ConsumerGroup = getStr(ext, "consumerGroup")
}

// NotifyConsumerIdsChangedRequestHeader (broker -> client push).
type NotifyConsumerIdsChangedRequestHeader struct {
	ConsumerGroup *string
}

func (h *NotifyConsumerIdsChangedRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "consumerGroup", h.ConsumerGroup)
}

func (h *NotifyConsumerIdsChangedRequestHeader) FromExtFields(ext *common.StringMap) {
	h.ConsumerGroup = getStr(ext, "consumerGroup")
}

// GetConsumerListByGroupRequestHeader.
type GetConsumerListByGroupRequestHeader struct {
	ConsumerGroup *string
}

func (h *GetConsumerListByGroupRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "consumerGroup", h.ConsumerGroup)
}

func (h *GetConsumerListByGroupRequestHeader) FromExtFields(ext *common.StringMap) {
	h.ConsumerGroup = getStr(ext, "consumerGroup")
}

// GetConsumerListByGroupResponseHeader (fieldless).
type GetConsumerListByGroupResponseHeader struct{}

func (h *GetConsumerListByGroupResponseHeader) ToExtFields(out *common.StringMap) {}

func (h *GetConsumerListByGroupResponseHeader) FromExtFields(ext *common.StringMap) {}

// ConsumerSendMsgBackRequestHeader (CONSUMER_SEND_MSG_BACK = 36).
type ConsumerSendMsgBackRequestHeader struct {
	Offset            *int64
	Group             *string
	DelayLevel        *int32
	OriginMsgID       *string
	OriginTopic       *string
	UnitMode          *bool
	MaxReconsumeTimes *int32
}

func (h *ConsumerSendMsgBackRequestHeader) ToExtFields(out *common.StringMap) {
	putI64(out, "offset", h.Offset)
	putStr(out, "group", h.Group)
	putI32(out, "delayLevel", h.DelayLevel)
	putStr(out, "originMsgId", h.OriginMsgID)
	putStr(out, "originTopic", h.OriginTopic)
	putBool(out, "unitMode", h.UnitMode)
	putI32(out, "maxReconsumeTimes", h.MaxReconsumeTimes)
}

func (h *ConsumerSendMsgBackRequestHeader) FromExtFields(ext *common.StringMap) {
	h.Offset = getI64(ext, "offset")
	h.Group = getStr(ext, "group")
	h.DelayLevel = getI32(ext, "delayLevel")
	h.OriginMsgID = getStr(ext, "originMsgId")
	h.OriginTopic = getStr(ext, "originTopic")
	h.UnitMode = getBool(ext, "unitMode")
	h.MaxReconsumeTimes = getI32(ext, "maxReconsumeTimes")
}

// LockBatchMqRequestHeader.
type LockBatchMqRequestHeader struct {
	ConsumerGroup *string
	ClientID      *string
}

func (h *LockBatchMqRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "consumerGroup", h.ConsumerGroup)
	putStr(out, "clientId", h.ClientID)
}

func (h *LockBatchMqRequestHeader) FromExtFields(ext *common.StringMap) {
	h.ConsumerGroup = getStr(ext, "consumerGroup")
	h.ClientID = getStr(ext, "clientId")
}

// UnlockBatchMqRequestHeader.
type UnlockBatchMqRequestHeader struct {
	ConsumerGroup *string
	ClientID      *string
}

func (h *UnlockBatchMqRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "consumerGroup", h.ConsumerGroup)
	putStr(out, "clientId", h.ClientID)
}

func (h *UnlockBatchMqRequestHeader) FromExtFields(ext *common.StringMap) {
	h.ConsumerGroup = getStr(ext, "consumerGroup")
	h.ClientID = getStr(ext, "clientId")
}

// ---------------- broker -> client handlers ----------------

// ResetOffsetRequestHeader (RESET_CONSUMER_CLIENT_OFFSET = 220 push).
type ResetOffsetRequestHeader struct {
	Topic     *string
	Group     *string
	Timestamp *int64
	IsForce   *bool
}

func (h *ResetOffsetRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "topic", h.Topic)
	putStr(out, "group", h.Group)
	putI64(out, "timestamp", h.Timestamp)
	putBool(out, "isForce", h.IsForce)
}

func (h *ResetOffsetRequestHeader) FromExtFields(ext *common.StringMap) {
	h.Topic = getStr(ext, "topic")
	h.Group = getStr(ext, "group")
	h.Timestamp = getI64(ext, "timestamp")
	h.IsForce = getBool(ext, "isForce")
}

// GetConsumerStatusRequestHeader (GET_CONSUMER_STATUS_FROM_CLIENT = 221 push).
type GetConsumerStatusRequestHeader struct {
	Topic      *string
	Group      *string
	ClientAddr *string
}

func (h *GetConsumerStatusRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "topic", h.Topic)
	putStr(out, "group", h.Group)
	putStr(out, "clientAddr", h.ClientAddr)
}

func (h *GetConsumerStatusRequestHeader) FromExtFields(ext *common.StringMap) {
	h.Topic = getStr(ext, "topic")
	h.Group = getStr(ext, "group")
	h.ClientAddr = getStr(ext, "clientAddr")
}

// GetConsumerRunningInfoRequestHeader (GET_CONSUMER_RUNNING_INFO = 307 push).
type GetConsumerRunningInfoRequestHeader struct {
	ConsumerGroup *string
	ClientID      *string
	JstackEnable  *bool
}

func (h *GetConsumerRunningInfoRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "consumerGroup", h.ConsumerGroup)
	putStr(out, "clientId", h.ClientID)
	putBool(out, "jstackEnable", h.JstackEnable)
}

func (h *GetConsumerRunningInfoRequestHeader) FromExtFields(ext *common.StringMap) {
	h.ConsumerGroup = getStr(ext, "consumerGroup")
	h.ClientID = getStr(ext, "clientId")
	h.JstackEnable = getBool(ext, "jstackEnable")
}

// ConsumeMessageDirectlyResultRequestHeader (CONSUME_MESSAGE_DIRECTLY = 309 push).
type ConsumeMessageDirectlyResultRequestHeader struct {
	ConsumerGroup *string
	ClientID      *string
	MsgID         *string
	BrokerName    *string
}

func (h *ConsumeMessageDirectlyResultRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "consumerGroup", h.ConsumerGroup)
	putStr(out, "clientId", h.ClientID)
	putStr(out, "msgId", h.MsgID)
	putStr(out, "brokerName", h.BrokerName)
}

func (h *ConsumeMessageDirectlyResultRequestHeader) FromExtFields(ext *common.StringMap) {
	h.ConsumerGroup = getStr(ext, "consumerGroup")
	h.ClientID = getStr(ext, "clientId")
	h.MsgID = getStr(ext, "msgId")
	h.BrokerName = getStr(ext, "brokerName")
}

// CheckTransactionStateRequestHeader (CHECK_TRANSACTION_STATE = 39 push).
// Inherited brokerName reflects as "bname".
type CheckTransactionStateRequestHeader struct {
	Topic                *string
	TranStateTableOffset *int64
	CommitLogOffset      *int64
	MsgID                *string
	TransactionID        *string
	OffsetMsgID          *string
	Bname                *string
}

func (h *CheckTransactionStateRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "topic", h.Topic)
	putI64(out, "tranStateTableOffset", h.TranStateTableOffset)
	putI64(out, "commitLogOffset", h.CommitLogOffset)
	putStr(out, "msgId", h.MsgID)
	putStr(out, "transactionId", h.TransactionID)
	putStr(out, "offsetMsgId", h.OffsetMsgID)
	putStr(out, "bname", h.Bname)
}

func (h *CheckTransactionStateRequestHeader) FromExtFields(ext *common.StringMap) {
	h.Topic = getStr(ext, "topic")
	h.TranStateTableOffset = getI64(ext, "tranStateTableOffset")
	h.CommitLogOffset = getI64(ext, "commitLogOffset")
	h.MsgID = getStr(ext, "msgId")
	h.TransactionID = getStr(ext, "transactionId")
	h.OffsetMsgID = getStr(ext, "offsetMsgId")
	h.Bname = getStr(ext, "bname")
}

// CheckTransactionStateResponseHeader (client's reply to 39).
type CheckTransactionStateResponseHeader struct {
	GroupName        *string
	TransactionState *int32
	Offset           *int64
}

func (h *CheckTransactionStateResponseHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "groupName", h.GroupName)
	putI32(out, "transactionState", h.TransactionState)
	putI64(out, "offset", h.Offset)
}

func (h *CheckTransactionStateResponseHeader) FromExtFields(ext *common.StringMap) {
	h.GroupName = getStr(ext, "groupName")
	h.TransactionState = getI32(ext, "transactionState")
	h.Offset = getI64(ext, "offset")
}

// ---------------- pop ----------------

// PopMessageRequestHeader (POP_MESSAGE = 200050).
//
// Note which fields are primitives in Java: queueId/maxMsgNums/invisibleTime/
// pollTime/bornTime/initMode are `int`/`long`, so RemotingCommand's
// reflection-based extFields writer emits them even when they are 0, and
// `order` is a `Boolean` initialised to FALSE so it is always emitted as
// "false". Treating those as optional here would change the wire bytes.
//
// Bname is the inherited RpcRequestHeader.bname; Java's popAsync sets it from
// the message queue's broker name. The broker ignores it for POP, but it is
// part of what Java sends.
type PopMessageRequestHeader struct {
	Bname         *string
	ConsumerGroup *string
	Topic         *string
	QueueID       *int32
	MaxMsgNums    *int32
	InvisibleTime *int64
	PollTime      *int64
	BornTime      *int64
	InitMode      *int32
	ExpType       *string
	Exp           *string
	Order         *bool
	AttemptID     *string
}

func (h *PopMessageRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "bname", h.Bname)
	putStr(out, "consumerGroup", h.ConsumerGroup)
	putStr(out, "topic", h.Topic)
	putI32(out, "queueId", h.QueueID)
	putI32(out, "maxMsgNums", h.MaxMsgNums)
	putI64(out, "invisibleTime", h.InvisibleTime)
	putI64(out, "pollTime", h.PollTime)
	putI64(out, "bornTime", h.BornTime)
	putI32(out, "initMode", h.InitMode)
	putStr(out, "expType", h.ExpType)
	putStr(out, "exp", h.Exp)
	putBool(out, "order", h.Order)
	putStr(out, "attemptId", h.AttemptID)
}

func (h *PopMessageRequestHeader) FromExtFields(ext *common.StringMap) {
	h.Bname = getStr(ext, "bname")
	h.ConsumerGroup = getStr(ext, "consumerGroup")
	h.Topic = getStr(ext, "topic")
	h.QueueID = getI32(ext, "queueId")
	h.MaxMsgNums = getI32(ext, "maxMsgNums")
	h.InvisibleTime = getI64(ext, "invisibleTime")
	h.PollTime = getI64(ext, "pollTime")
	h.BornTime = getI64(ext, "bornTime")
	h.InitMode = getI32(ext, "initMode")
	h.ExpType = getStr(ext, "expType")
	h.Exp = getStr(ext, "exp")
	h.Order = getBool(ext, "order")
	h.AttemptID = getStr(ext, "attemptId")
}

// IsOrder mirrors Java PopMessageRequestHeader#isOrder: a nil `order` is false,
// never true-by-default.
func (h *PopMessageRequestHeader) IsOrder() bool {
	return h.Order != nil && *h.Order
}

// IsTimeoutTooMuch mirrors Java PopMessageRequestHeader#isTimeoutTooMuch.
func (h *PopMessageRequestHeader) IsTimeoutTooMuch() bool {
	poll, born := int64(0), int64(0)
	if h.PollTime != nil {
		poll = *h.PollTime
	}
	if h.BornTime != nil {
		born = *h.BornTime
	}
	return common.CurrentTimeMillis()-born-poll > 500
}

// PopMessageResponseHeader.
//
// startOffsetInfo/msgOffsetInfo/orderCountInfo are the broker's per-queue
// offset tables. They are what makes an ACK addressable at all: without them
// the client falls back to building a checkpoint from the message's own queue
// offset (see MQClientAPIImpl.processPopResponse).
type PopMessageResponseHeader struct {
	PopTime         *int64
	InvisibleTime   *int64
	ReviveQid       *int32
	RestNum         *int64
	StartOffsetInfo *string
	MsgOffsetInfo   *string
	OrderCountInfo  *string
}

func (h *PopMessageResponseHeader) ToExtFields(out *common.StringMap) {
	putI64(out, "popTime", h.PopTime)
	putI64(out, "invisibleTime", h.InvisibleTime)
	putI32(out, "reviveQid", h.ReviveQid)
	putI64(out, "restNum", h.RestNum)
	putStr(out, "startOffsetInfo", h.StartOffsetInfo)
	putStr(out, "msgOffsetInfo", h.MsgOffsetInfo)
	putStr(out, "orderCountInfo", h.OrderCountInfo)
}

func (h *PopMessageResponseHeader) FromExtFields(ext *common.StringMap) {
	h.PopTime = getI64(ext, "popTime")
	h.InvisibleTime = getI64(ext, "invisibleTime")
	h.ReviveQid = getI32(ext, "reviveQid")
	h.RestNum = getI64(ext, "restNum")
	h.StartOffsetInfo = getStr(ext, "startOffsetInfo")
	h.MsgOffsetInfo = getStr(ext, "msgOffsetInfo")
	h.OrderCountInfo = getStr(ext, "orderCountInfo")
}

// AckMessageRequestHeader (ACK_MESSAGE = 200051).
//
// `offset` here is the message's own queue offset (checkpoint segment 7), NOT
// the batch's start offset — the broker looks the checkpoint up by it.
type AckMessageRequestHeader struct {
	Bname         *string
	ConsumerGroup *string
	Topic         *string
	QueueID       *int32
	ExtraInfo     *string
	Offset        *int64
	LiteTopic     *string
}

func (h *AckMessageRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "bname", h.Bname)
	putStr(out, "consumerGroup", h.ConsumerGroup)
	putStr(out, "topic", h.Topic)
	putI32(out, "queueId", h.QueueID)
	putStr(out, "extraInfo", h.ExtraInfo)
	putI64(out, "offset", h.Offset)
	putStr(out, "liteTopic", h.LiteTopic)
}

func (h *AckMessageRequestHeader) FromExtFields(ext *common.StringMap) {
	h.Bname = getStr(ext, "bname")
	h.ConsumerGroup = getStr(ext, "consumerGroup")
	h.Topic = getStr(ext, "topic")
	h.QueueID = getI32(ext, "queueId")
	h.ExtraInfo = getStr(ext, "extraInfo")
	h.Offset = getI64(ext, "offset")
	h.LiteTopic = getStr(ext, "liteTopic")
}

// ChangeInvisibleTimeRequestHeader (CHANGE_MESSAGE_INVISIBLETIME = 200053).
//
// `suspend` is a Java primitive `boolean` defaulting to false, so it is always
// on the wire.
type ChangeInvisibleTimeRequestHeader struct {
	Bname         *string
	ConsumerGroup *string
	Topic         *string
	QueueID       *int32
	ExtraInfo     *string
	Offset        *int64
	InvisibleTime *int64
	LiteTopic     *string
	Suspend       *bool
}

func (h *ChangeInvisibleTimeRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "bname", h.Bname)
	putStr(out, "consumerGroup", h.ConsumerGroup)
	putStr(out, "topic", h.Topic)
	putI32(out, "queueId", h.QueueID)
	putStr(out, "extraInfo", h.ExtraInfo)
	putI64(out, "offset", h.Offset)
	putI64(out, "invisibleTime", h.InvisibleTime)
	putStr(out, "liteTopic", h.LiteTopic)
	putBool(out, "suspend", h.Suspend)
}

func (h *ChangeInvisibleTimeRequestHeader) FromExtFields(ext *common.StringMap) {
	h.Bname = getStr(ext, "bname")
	h.ConsumerGroup = getStr(ext, "consumerGroup")
	h.Topic = getStr(ext, "topic")
	h.QueueID = getI32(ext, "queueId")
	h.ExtraInfo = getStr(ext, "extraInfo")
	h.Offset = getI64(ext, "offset")
	h.InvisibleTime = getI64(ext, "invisibleTime")
	h.LiteTopic = getStr(ext, "liteTopic")
	h.Suspend = getBool(ext, "suspend")
}

// ChangeInvisibleTimeResponseHeader. The reply carries the NEW popTime and
// invisibleTime; the client rebuilds its checkpoint from them (MQClientAPIImpl
// .changeInvisibleTimeAsync).
type ChangeInvisibleTimeResponseHeader struct {
	PopTime       *int64
	InvisibleTime *int64
	ReviveQid     *int32
}

func (h *ChangeInvisibleTimeResponseHeader) ToExtFields(out *common.StringMap) {
	putI64(out, "popTime", h.PopTime)
	putI64(out, "invisibleTime", h.InvisibleTime)
	putI32(out, "reviveQid", h.ReviveQid)
}

func (h *ChangeInvisibleTimeResponseHeader) FromExtFields(ext *common.StringMap) {
	h.PopTime = getI64(ext, "popTime")
	h.InvisibleTime = getI64(ext, "invisibleTime")
	h.ReviveQid = getI32(ext, "reviveQid")
}

// ---------------- name server ----------------

// GetRouteInfoRequestHeader (GET_ROUTEINFO_BY_TOPIC).
type GetRouteInfoRequestHeader struct {
	Topic                  *string
	AcceptStandardJSONOnly *bool
}

func (h *GetRouteInfoRequestHeader) ToExtFields(out *common.StringMap) {
	putStr(out, "topic", h.Topic)
	putBool(out, "acceptStandardJsonOnly", h.AcceptStandardJSONOnly)
}

func (h *GetRouteInfoRequestHeader) FromExtFields(ext *common.StringMap) {
	h.Topic = getStr(ext, "topic")
	h.AcceptStandardJSONOnly = getBool(ext, "acceptStandardJsonOnly")
}
