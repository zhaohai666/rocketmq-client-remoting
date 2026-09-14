// org.apache.rocketmq.common.message.MessageConst 的 C# 对应：消息属性键常量。
//
// ⚠ 这些键名是**协议的一部分**：broker/客户端按字面匹配，改一个字母就静默丢属性。
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
    public const string PropertyProducerGroup = "PRODUCER_GROUP";
    public const string PropertyMinOffset = "MIN_OFFSET";
    public const string PropertyMaxOffset = "MAX_OFFSET";
    public const string PropertyBuyerId = "BUYER_ID";
    public const string PropertyOriginMessageId = "ORIGIN_MESSAGE_ID";
    public const string PropertyTransferFlag = "TRANSFER_FLAG";
    public const string PropertyCheckImmunityTimeInSeconds = "CHECK_IMMUNITY_TIME_IN_SECONDS";
    public const string PropertyReconsumeTime = "RECONSUME_TIME";
    public const string PropertyMsgRegion = "MSG_REGION";
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
    public const string PropertyInnerMultiDispatch = "INNER_MULTI_DISPATCH";
    public const string PropertyInnerMultiQueueOffset = "INNER_MULTI_QUEUE_OFFSET";
    public const string PropertyPopCk = "POP_CK";
    public const string PropertyPopCkOffset = "POP_CK_OFFSET";
    public const string PropertyPopTime = "POP_TIME";
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

    public const int StringHashSet = 1;

    public const string KeySeparator = " ";
    public const int CharacterMaxLength = 255;
    public const string MessageIdPrefix = "MSGID-";

    // 索引查询类型（对应 MessageConst.INDEX_KEY_TYPE / INDEX_UNIQUE_TYPE / INDEX_TAG_TYPE）。
    // broker 的 QueryMessageRequestHeader.indexType 取这些值；为空时 broker 按 "K" 处理。
    public const string IndexKeyType = "K";
    public const string IndexUniqueType = "U";
    public const string IndexTagType = "T";

    public static string MessageIdPrefixValue() => MessageIdPrefix;
}
