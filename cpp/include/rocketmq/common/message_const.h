#ifndef ROCKETMQ_COMMON_MESSAGE_CONST_H
#define ROCKETMQ_COMMON_MESSAGE_CONST_H

#include <cstdint>
#include <string>

namespace rocketmq {

struct MessageConst {
    static constexpr const char* PROPERTY_KEYS = "KEYS";
    static constexpr const char* PROPERTY_TAGS = "TAGS";
    static constexpr const char* PROPERTY_WAIT_STORE_MSG_OK = "WAIT";
    static constexpr const char* PROPERTY_DELAY_TIME_LEVEL = "DELAY";
    // The three 5.x timed-message properties: setDelayTimeSec writes TIMER_DELAY_SEC,
    // setDelayTimeMs writes TIMER_DELAY_MS, setDeliverTimeMs writes TIMER_DELIVER_MS.
    // The canBatch accumulation check uses the maximum of these four (DELAY included).
    static constexpr const char* PROPERTY_TIMER_DELAY_SEC = "TIMER_DELAY_SEC";
    static constexpr const char* PROPERTY_TIMER_DELAY_MS = "TIMER_DELAY_MS";
    static constexpr const char* PROPERTY_TIMER_DELIVER_MS = "TIMER_DELIVER_MS";
    static constexpr const char* PROPERTY_RETRY_TOPIC = "RETRY_TOPIC";
    static constexpr const char* PROPERTY_REAL_TOPIC = "REAL_TOPIC";
    static constexpr const char* PROPERTY_REAL_QUEUE_ID = "REAL_QID";
    static constexpr const char* PROPERTY_TRANSACTION_PREPARED = "TRAN_MSG";
    static constexpr const char* PROPERTY_PRODUCER_GROUP = "PGROUP";
    static constexpr const char* PROPERTY_MIN_OFFSET = "MIN_OFFSET";
    static constexpr const char* PROPERTY_MAX_OFFSET = "MAX_OFFSET";
    static constexpr const char* PROPERTY_BUYER_ID = "BUYER_ID";
    static constexpr const char* PROPERTY_ORIGIN_MESSAGE_ID = "ORIGIN_MESSAGE_ID";
    static constexpr const char* PROPERTY_TRANSFER_FLAG = "TRANSFER_FLAG";
    static constexpr const char* PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS = "CHECK_IMMUNITY_TIME_IN_SECONDS";
    static constexpr const char* PROPERTY_RECONSUME_TIME = "RECONSUME_TIME";
    static constexpr const char* PROPERTY_MSG_REGION = "MSG_REGION";
    static constexpr const char* PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX = "UNIQ_KEY";
    static constexpr const char* PROPERTY_MAX_RECONSUME_TIMES = "MAX_RECONSUME_TIMES";
    static constexpr const char* PROPERTY_CONSUME_START_TIMESTAMP = "CONSUME_START_TIME";

    static constexpr const char* PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET = "TRAN_PREPARED_QUEUE_OFFSET";
    static constexpr const char* PROPERTY_TRANSACTION_CHECK_TIMES = "TRANSACTION_CHECK_TIMES";
    static constexpr const char* PROPERTY_CHECKED_TOPIC = "CHECKED_TOPIC";
    static constexpr const char* PROPERTY_BORN_HOST = "BORN_HOST";
    static constexpr const char* PROPERTY_BORN_TIMESTAMP = "BORN_TIMESTAMP";
    static constexpr const char* PROPERTY_STORE_HOST = "STORE_HOST";
    static constexpr const char* PROPERTY_STORE_TIMESTAMP = "STORE_TIMESTAMP";
    static constexpr const char* PROPERTY_MSG_ID = "MSG_ID";
    static constexpr const char* PROPERTY_WAIT_STORE_MSG_OK_PROP = "WAIT_STORE_MSG_OK";
    static constexpr const char* PROPERTY_INSTANCE_ID = "INSTANCE_ID";
    static constexpr const char* PROPERTY_CLUSTER = "CLUSTER";
    static constexpr const char* PROPERTY_MESSAGE_TYPE = "MSG_TYPE";
    // Request-Reply (5.x): a request message carries CORRELATION_ID / REPLY_TO_CLIENT / TTL,
    // the reply message has MSG_TYPE == "reply" (which is what maps the request code to SEND_REPLY_MESSAGE_V2),
    // and the broker looks up the requester channel by REPLY_TO_CLIENT and pushes PUSH_REPLY_MESSAGE_TO_CLIENT.
    static constexpr const char* PROPERTY_CORRELATION_ID = "CORRELATION_ID";
    static constexpr const char* PROPERTY_MESSAGE_REPLY_TO_CLIENT = "REPLY_TO_CLIENT";
    static constexpr const char* PROPERTY_MESSAGE_TTL = "TTL";
    static constexpr const char* PROPERTY_REPLY_MESSAGE_ARRIVE_TIME = "REPLY_MESSAGE_ARRIVE_TIME";
    static constexpr const char* PROPERTY_PUSH_REPLY_TIME = "PUSH_REPLY_TIME";
    static constexpr const char* PROPERTY_INNER_MULTI_DISPATCH = "INNER_MULTI_DISPATCH";
    static constexpr const char* PROPERTY_INNER_MULTI_QUEUE_OFFSET = "INNER_MULTI_QUEUE_OFFSET";
    static constexpr const char* PROPERTY_POP_CK = "POP_CK";
    static constexpr const char* PROPERTY_POP_CK_OFFSET = "POP_CK_OFFSET";
    static constexpr const char* PROPERTY_FIRST_POP_TIME = "1ST_POP_TIME";
    static constexpr const char* PROPERTY_POP_TIME = "POP_TIME";
    static constexpr const char* PROPERTY_INVISIBLE_TIME = "INVISIBLE_TIME";
    static constexpr const char* PROPERTY_DELAY_TIME = "DELAY_TIME";
    static constexpr const char* PROPERTY_START_TIME = "START_TIME";
    static constexpr const char* PROPERTY_END_TIME = "END_TIME";
    static constexpr const char* PROPERTY_EXPIRE_TIME = "EXPIRE_TIME";
    static constexpr const char* PROPERTY_LAST_CONSUME_TIMESTAMP = "LAST_CONSUME_TIME";
    static constexpr const char* PROPERTY_SELF_CONSUME_ENABLE = "SELF_CONSUME";
    static constexpr const char* PROPERTY_RECONSUME_GROUP = "RECONSUME_GROUP";
    static constexpr const char* PROPERTY_RECONSUME_TOPIC = "RECONSUME_TOPIC";
    static constexpr const char* PROPERTY_KEYS_CONST = "KEYS";
    static constexpr const char* PROPERTY_ORIGIN_QUEUE_ID = "ORIGIN_QID";
    static constexpr const char* PROPERTY_ORIGIN_TOPIC = "ORIGIN_TOPIC";

    static constexpr int32_t STRING_HASH_SET = 1;

    static constexpr const char* KEY_SEPARATOR = " ";
    static constexpr int32_t CHARACTER_MAX_LENGTH = 255;
    static constexpr const char* MESSAGE_ID_PREFIX = "MSGID-";

    // Index query types: MessageConst.INDEX_KEY_TYPE / INDEX_UNIQUE_TYPE / INDEX_TAG_TYPE.
    // The broker's QueryMessageRequestHeader.indexType takes these values; when empty the broker treats it as "K".
    static constexpr const char* INDEX_KEY_TYPE = "K";
    static constexpr const char* INDEX_UNIQUE_TYPE = "U";
    static constexpr const char* INDEX_TAG_TYPE = "T";

    static std::string messageIdPrefix() { return MESSAGE_ID_PREFIX; }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_MESSAGE_CONST_H
