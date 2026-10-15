// WARNING: These key names are part of the protocol: broker/client match them literally; changing even one letter silently drops the property.
namespace RocketMQ.Common;

public static class MessageConst
{
    public const string PropertyKeys = "KEYS";
    public const string PropertyTags = "TAGS";
    public const string PropertyWaitStoreMsgOk = "WAIT";
    public const string PropertyDelayTimeLevel = "DELAY";
    public const string PropertyRetryTopic = "RETRY_TOPIC";
    public const string PropertyRealTopic = "REAL_TOPIC";
    public const string PropertyRealQueueId = "REAL_QID";
    public const string PropertyTransactionPrepared = "TRAN_MSG";
    public const string PropertyProducerGroup = "PGROUP";
    public const string PropertyMinOffset = "MIN_OFFSET";
    public const string PropertyMaxOffset = "MAX_OFFSET";
    public const string PropertyBuyerId = "BUYER_ID";
    public const string PropertyOriginMessageId = "ORIGIN_MESSAGE_ID";
    public const string PropertyTransferFlag = "TRANSFER_FLAG";
    public const string PropertyCheckImmunityTimeInSeconds = "CHECK_IMMUNITY_TIME_IN_SECONDS";
    public const string PropertyReconsumeTime = "RECONSUME_TIME";
    public const string PropertyMsgRegion = "MSG_REGION";
    /// <summary>The broker uses TRACE_ON in the SEND response to decide whether to record the trace ("false" means off; anything else means on).</summary>
    public const string PropertyTraceSwitch = "TRACE_ON";
    public const string PropertyUniqClientMessageIdKeyidx = "UNIQ_KEY";
    public const string PropertyMaxReconsumeTimes = "MAX_RECONSUME_TIMES";
    public const string PropertyConsumeStartTimestamp = "CONSUME_START_TIME";

    public const string PropertyTransactionPreparedQueueOffset = "TRAN_PREPARED_QUEUE_OFFSET";
    public const string PropertyTransactionCheckTimes = "TRANSACTION_CHECK_TIMES";
    public const string PropertyCheckedTopic = "CHECKED_TOPIC";
    public const string PropertyBornHost = "BORN_HOST";
    public const string PropertyBornTimestamp = "BORN_TIMESTAMP";
    public const string PropertyStoreHost = "STORE_HOST";
    public const string PropertyStoreTimestamp = "STORE_TIMESTAMP";
    public const string PropertyMsgId = "MSG_ID";
    public const string PropertyWaitStoreMsgOkProp = "WAIT_STORE_MSG_OK";
    public const string PropertyInstanceId = "INSTANCE_ID";
    public const string PropertyCluster = "CLUSTER";
    public const string PropertyMessageType = "MSG_TYPE";

    public const string PropertyCorrelationId = "CORRELATION_ID";
    public const string PropertyReplyToClient = "REPLY_TO_CLIENT";
    public const string PropertyMessageTTL = "TTL";
    public const string PropertyReplyMessageArriveTime = "REPLY_MESSAGE_ARRIVE_TIME";
    public const string PropertyInnerMultiDispatch = "INNER_MULTI_DISPATCH";
    public const string PropertyInnerMultiQueueOffset = "INNER_MULTI_QUEUE_OFFSET";
    public const string PropertyPopCk = "POP_CK";
    public const string PropertyPopCkOffset = "POP_CK_OFFSET";
    public const string PropertyPopTime = "POP_TIME";

    public const string PropertyFirstPopTime = "1ST_POP_TIME";
    public const string PropertyInvisibleTime = "INVISIBLE_TIME";
    public const string PropertyDelayTime = "DELAY_TIME";
    public const string PropertyStartTime = "START_TIME";
    public const string PropertyEndTime = "END_TIME";
    public const string PropertyExpireTime = "EXPIRE_TIME";
    public const string PropertyLastConsumeTimestamp = "LAST_CONSUME_TIME";
    public const string PropertySelfConsumeEnable = "SELF_CONSUME";
    public const string PropertyReconsumeGroup = "RECONSUME_GROUP";
    public const string PropertyReconsumeTopic = "RECONSUME_TOPIC";
    public const string PropertyKeysConst = "KEYS";
    public const string PropertyOriginQueueId = "ORIGIN_QID";
    public const string PropertyOriginTopic = "ORIGIN_TOPIC";

    public const string PropertyTimerDelaySec = "TIMER_DELAY_SEC";
    public const string PropertyTimerDelayMs = "TIMER_DELAY_MS";
    public const string PropertyTimerDeliverMs = "TIMER_DELIVER_MS";

    public const int StringHashSet = 1;

    public const string KeySeparator = " ";
    public const int CharacterMaxLength = 255;
    public const string MessageIdPrefix = "MSGID-";

    // Index query types (corresponding to MessageConst.INDEX_KEY_TYPE / INDEX_UNIQUE_TYPE / INDEX_TAG_TYPE).
    // The broker's QueryMessageRequestHeader.indexType takes these values; when empty the broker treats it as "K".
    public const string IndexKeyType = "K";
    public const string IndexUniqueType = "U";
    public const string IndexTagType = "T";

    public static string MessageIdPrefixValue() => MessageIdPrefix;
}
