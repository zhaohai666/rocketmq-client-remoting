package common

// Message property key constants (Java MessageConst). These strings are wire
// literals brokered between client and broker (the 17th segment of the message
// format, broker-side filtering/trace/transaction logic) — changing one
// character breaks compatibility.
const (
	PropertyKeys                       = "KEYS"
	PropertyTags                       = "TAGS"
	PropertyWaitStoreMsgOK             = "WAIT"
	PropertyDelayTimeLevel             = "DELAY"
	PropertyRetryTopic                 = "RETRY_TOPIC"
	PropertyRealTopic                  = "REAL_TOPIC"
	PropertyRealQueueID                = "REAL_QID"
	PropertyTransactionPrepared        = "TRAN_MSG"
	PropertyProducerGroup              = "PGROUP"
	PropertyMinOffset                  = "MIN_OFFSET"
	PropertyMaxOffset                  = "MAX_OFFSET"
	PropertyBuyerID                    = "BUYER_ID"
	PropertyOriginMessageID            = "ORIGIN_MESSAGE_ID"
	PropertyTransferFlag               = "TRANSFER_FLAG"
	PropertyCheckImmunityTimeInSeconds = "CHECK_IMMUNITY_TIME_IN_SECONDS"
	PropertyReconsumeTime              = "RECONSUME_TIME"
	PropertyMsgRegion                  = "MSG_REGION"
	// PropertyTraceSwitch: broker echoes it in the SEND response header.
	PropertyTraceSwitch = "TRACE_ON"
	// PropertyUniqClientMessageIDKeyIDX (MessageClientIDSetter).
	PropertyUniqKey                        = "UNIQ_KEY"
	PropertyMaxReconsumeTimes              = "MAX_RECONSUME_TIMES"
	PropertyConsumeStartTime               = "CONSUME_START_TIME"
	PropertyTransactionPreparedQueueOffset = "TRAN_PREPARED_QUEUE_OFFSET"
	PropertyTransactionCheckTimes          = "TRANSACTION_CHECK_TIMES"
	PropertyCheckedTopic                   = "CHECKED_TOPIC"
	PropertyBornHost                       = "BORN_HOST"
	PropertyBornTimestamp                  = "BORN_TIMESTAMP"
	PropertyStoreHost                      = "STORE_HOST"
	PropertyStoreTimestamp                 = "STORE_TIMESTAMP"
	PropertyMsgID                          = "MSG_ID"
	PropertyWaitStoreMsgOKProp             = "WAIT_STORE_MSG_OK"
	PropertyInstanceID                     = "INSTANCE_ID"
	PropertyCluster                        = "CLUSTER"
	PropertyMessageType                    = "MSG_TYPE"
	// Request-Reply: CORRELATION_ID / REPLY_TO_CLIENT / TTL.
	PropertyCorrelationID          = "CORRELATION_ID"
	PropertyMessageReplyToClient   = "REPLY_TO_CLIENT"
	PropertyMessageTTL             = "TTL"
	PropertyReplyMessageArriveTime = "REPLY_MESSAGE_ARRIVE_TIME"
	PropertyPushReplyTime          = "PUSH_REPLY_TIME"
	PropertyInnerMultiDispatch     = "INNER_MULTI_DISPATCH"
	PropertyInnerMultiQueueOffset  = "INNER_MULTI_QUEUE_OFFSET"
	PropertyPopCk                  = "POP_CK"
	PropertyPopCkOffset            = "POP_CK_OFFSET"
	PropertyPopTime                = "POP_TIME"
	// Java writes "1ST_POP_TIME" (PROPERTY_FIRST_POP_TIME); the client only
	// backfills it when missing.
	PropertyFirstPopTime    = "1ST_POP_TIME"
	PropertyInvisibleTime   = "INVISIBLE_TIME"
	PropertyDelayTime       = "DELAY_TIME"
	PropertyStartTime       = "START_TIME"
	PropertyEndTime         = "END_TIME"
	PropertyExpireTime      = "EXPIRE_TIME"
	PropertyLastConsumeTime = "LAST_CONSUME_TIME"
	PropertySelfConsumeOK   = "SELF_CONSUME"
	PropertyReconsumeGroup  = "RECONSUME_GROUP"
	PropertyReconsumeTopic  = "RECONSUME_TOPIC"
	PropertyKeysConst       = "KEYS"
	PropertyOriginQueueID   = "ORIGIN_QID"
	PropertyOriginTopic     = "ORIGIN_TOPIC"

	// PropertyStartDeliverTime (Java MessageConst.PROPERTY_STARTDELIVERTIME,
	// "_" + "_STARTDELIVERTIME"). Distinct from PropertyStartTime
	// ("START_TIME"), which is a trace-only field — substituting one for the
	// other makes the delay-message classification silently miss every
	// timer-scheduled message.
	PropertyStartDeliverTime = "__STARTDELIVERTIME"
	// Timer (5.x) delivery properties, the three aliases of DELAY.
	PropertyTimerDeliverMs = "TIMER_DELIVER_MS"
	PropertyTimerDelaySec  = "TIMER_DELAY_SEC"
	PropertyTimerDelayMs   = "TIMER_DELAY_MS"

	KeySeparator       = " "
	CharacterMaxLength = 255
	MessageIDPrefix    = "MSGID-"

	// Index query types (broker QueryMessageRequestHeader.indexType; missing
	// means "K").
	IndexKeyType    = "K"
	IndexUniqueType = "U"
	IndexTagType    = "T"
)

// StringAllProperty lists every system property key (Java 4.x
// MessageConst.STRING_ALL_PROPERTY), used to separate system properties from
// user properties (trace, dashboards).
var StringAllProperty = []string{
	PropertyKeys,
	PropertyTags,
	PropertyWaitStoreMsgOK,
	PropertyDelayTimeLevel,
	PropertyRetryTopic,
	PropertyRealTopic,
	PropertyRealQueueID,
	PropertyTransactionPrepared,
	PropertyProducerGroup,
	PropertyMinOffset,
	PropertyMaxOffset,
	PropertyBuyerID,
	PropertyOriginMessageID,
	PropertyTransferFlag,
	PropertyCheckImmunityTimeInSeconds,
	PropertyReconsumeTime,
	PropertyMsgRegion,
	PropertyTraceSwitch,
	PropertyUniqKey,
	PropertyMaxReconsumeTimes,
	PropertyConsumeStartTime,
	PropertyTransactionPreparedQueueOffset,
	PropertyTransactionCheckTimes,
	PropertyCheckedTopic,
	PropertyBornHost,
	PropertyBornTimestamp,
	PropertyStoreHost,
	PropertyStoreTimestamp,
	PropertyMsgID,
	PropertyInstanceID,
	PropertyCluster,
	PropertyMessageType,
	PropertyCorrelationID,
	PropertyMessageReplyToClient,
	PropertyMessageTTL,
	PropertyInnerMultiDispatch,
	PropertyInnerMultiQueueOffset,
	PropertyPopCk,
	PropertyPopCkOffset,
	PropertyPopTime,
	PropertyFirstPopTime,
	PropertyInvisibleTime,
}

// IsSystemProperty reports whether name belongs to STRING_ALL_PROPERTY.
func IsSystemProperty(name string) bool {
	for _, k := range StringAllProperty {
		if k == name {
			return true
		}
	}
	return false
}
