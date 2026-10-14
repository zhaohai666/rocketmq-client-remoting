#ifndef ROCKETMQ_CLIENT_TRACE_H
#define ROCKETMQ_CLIENT_TRACE_H

#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "rocketmq/common/util_all.h"

namespace rocketmq {

// ---------------------------------------------------------------- Constants
struct TraceConstants {
    static constexpr const char* GROUP_NAME_PREFIX = "_INNER_TRACE_PRODUCER";
    static constexpr const char* CONTENT_SPLITOR = "\x01";  // SOH
    static constexpr const char* FIELD_SPLITOR = "\x02";    // STX
    static constexpr const char* TRACE_INSTANCE_NAME = "PID_CLIENT_INNER_TRACE_PRODUCER";
    static constexpr const char* TRACE_TOPIC_PREFIX = "rmq_sys_TRACE_DATA_";
    static constexpr const char* TO_PREFIX = "To_";
    static constexpr const char* FROM_PREFIX = "From_";
    static constexpr const char* END_TRANSACTION = "EndTransaction";

    static constexpr const char* ROCKETMQ_SERVICE = "rocketmq";
    static constexpr const char* ROCKETMQ_SUCCESS = "rocketmq.success";
    static constexpr const char* ROCKETMQ_TAGS = "rocketmq.tags";
    static constexpr const char* ROCKETMQ_KEYS = "rocketmq.keys";
    static constexpr const char* ROCKETMQ_STORE_HOST = "rocketmq.store_host";
    static constexpr const char* ROCKETMQ_BODY_LENGTH = "rocketmq.body_length";
    static constexpr const char* ROCKETMQ_MSG_ID = "rocketmq.mgs_id";
    static constexpr const char* ROCKETMQ_MSG_TYPE = "rocketmq.mgs_type";
    static constexpr const char* ROCKETMQ_REGION_ID = "rocketmq.region_id";
    static constexpr const char* ROCKETMQ_TRANSACTION_ID = "rocketmq.transaction_id";
    static constexpr const char* ROCKETMQ_TRANSACTION_STATE = "rocketmq.transaction_state";
    static constexpr const char* ROCKETMQ_IS_FROM_TRANSACTION_CHECK = "rocketmq.is_from_transaction_check";
    static constexpr const char* ROCKETMQ_RETRY_TIMERS = "rocketmq.retry_times";

    static constexpr const char* DEFAULT_TRACE_REGION_ID = "DefaultRegion";
    static constexpr const char* KEY_SEPARATOR = " ";
    static constexpr const char* PROPERTY_MSG_REGION = "MSG_REGION";
    static constexpr const char* PROPERTY_TRACE_SWITCH = "TRACE_ON";
    static constexpr const char* PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX = "UNIQ_KEY";
};

// ---------------------------------------------------------------- TraceType
// The enum name is the first segment of the on-wire field (Pub / Recall / SubBefore / SubAfter / EndTransaction).
enum class TraceType {
    PUB = 0,
    RECALL = 1,
    SUB_BEFORE = 2,
    SUB_AFTER = 3,
    END_TRANSACTION = 4,
};

inline const char* traceTypeName(TraceType t) {
    switch (t) {
        case TraceType::PUB: return "Pub";
        case TraceType::RECALL: return "Recall";
        case TraceType::SUB_BEFORE: return "SubBefore";
        case TraceType::SUB_AFTER: return "SubAfter";
        case TraceType::END_TRANSACTION: return "EndTransaction";
    }
    return "";
}

inline TraceType traceTypeFromName(const std::string& name) {
    if (name == "Pub") return TraceType::PUB;
    if (name == "Recall") return TraceType::RECALL;
    if (name == "SubBefore") return TraceType::SUB_BEFORE;
    if (name == "SubAfter") return TraceType::SUB_AFTER;
    if (name == "EndTransaction") return TraceType::END_TRANSACTION;
    return TraceType::PUB;  // unknown types are left for the caller to "skip this entry"
}

// ---------------------------------------------------------------- AccessChannel
// Only takes effect in SubAfter encoding: only non-CLOUD appends the timestamp + groupName two segments.
enum class AccessChannel {
    LOCAL = 0,
    CLOUD = 1,
};

inline const char* accessChannelName(AccessChannel c) {
    return c == AccessChannel::CLOUD ? "CLOUD" : "LOCAL";
}

enum class TraceMessageType {
    NORMAL = 0,
    TRANS = 1,
    TRANS_COMMIT = 2,
    DELAY = 3,
    ORDER = 4,
};

// Local egress address (default for storeHost/clientHost).
std::string localAddressForTrace();

// ---------------------------------------------------------------- TraceBean
struct TraceBean {
    std::string topic;
    std::string msgId;
    std::string offsetMsgId;
    std::string tags;
    std::string keys;
    std::string storeHost = localAddressForTrace();
    std::string clientHost = localAddressForTrace();
    int64_t storeTime = 0;
    int32_t retryTimes = 0;
    int32_t bodyLength = 0;
    int32_t msgType = static_cast<int32_t>(TraceMessageType::NORMAL);  // ordinal
    std::string transactionState;   // name of the LocalTransactionState ("COMMIT_MESSAGE" etc.)
    std::string transactionId;
    bool fromTransactionCheck = false;
};

// ---------------------------------------------------------------- TraceContext
struct TraceContext {
    std::optional<TraceType> traceType;
    int64_t timeStamp = UtilAll::currentTimeMillis();
    std::string regionId;
    std::string regionName;
    std::string groupName;
    int64_t costTime = 0;
    bool isSuccess = true;
    // Default request id (SubBefore/SubAfter share one; it is the key that lets the console stitch one consumption's before/after
    // together); non-empty business scenarios get explicitly overridden.
    std::string requestId = InnerIdGenerator::createUniqId();
    int32_t contextCode = 0;
    std::optional<AccessChannel> accessChannel;
    std::vector<TraceBean> traceBeans;

    AccessChannel accessChannelOrLocal() const { return accessChannel.value_or(AccessChannel::LOCAL); }
};

// ---------------------------------------------------------------- TraceTransferBean
struct TraceTransferBean {
    std::string transData;
    std::set<std::string> transKey;
};

// ---------------------------------------------------------------- Encode / decode
class TraceDataEncoder {
public:
    static std::vector<std::string> javaSplit(const std::string& value, const std::string& sep);

    static std::optional<TraceTransferBean> encoderFromContextBean(const TraceContext* ctx);

    static std::vector<TraceContext> decoderFromTraceDataString(const std::string& traceData);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_TRACE_H
