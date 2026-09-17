// 消息轨迹（对应 org.apache.rocketmq.client.trace 包 + client.AccessChannel）。
//
// 包含：
//   * ``TraceConstants``     —— 常量（org.apache.rocketmq.client.trace.TraceConstants）
//   * ``TraceType``          —— Pub / Recall / SubBefore / SubAfter / EndTransaction
//   * ``AccessChannel``      —— LOCAL / CLOUD（org.apache.rocketmq.client.AccessChannel）
//   * ``TraceBean``          —— 轨迹里被追踪的那条消息
//   * ``TraceContext``       —— 一次追踪上下文（Pub/SubBefore/... 各一份）
//   * ``TraceTransferBean``  —— 编码结果：trans_data + trans_key
//   * ``TraceDataEncoder``   —— 文本编解码（与 Java 逐字节一致）
//
// ⚠ 两个必须保留的 Java 语义差异（否则编解码对不上）：
//   1. CONTENT_SPLITOR = \x01、FIELD_SPLITOR = \x02，
//      编码时**每条记录末尾补 FIELD_SPLITOR**；解码用 Java 的 String.split
//      语义（**丢弃末尾空串**）切分 —— 见 ``TraceDataEncoder::javaSplit``。
//   2. C++ 的 split/istream 实现很容易漏掉"丢弃末尾空串"，一旦漏掉就会多出一个空段、
//      把字段整体错位。本模块一律走 ``javaSplit``，且对缺段做了安全取值（避免越界
//      导致整条 trace data 解码失败）。
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

// ---------------------------------------------------------------- 常量
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

    // 与 JVM/Python 侧共享的全局常量
    static constexpr const char* DEFAULT_TRACE_REGION_ID = "DefaultRegion";
    static constexpr const char* KEY_SEPARATOR = " ";
    static constexpr const char* PROPERTY_MSG_REGION = "MSG_REGION";
    static constexpr const char* PROPERTY_TRACE_SWITCH = "TRACE_ON";
    static constexpr const char* PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX = "UNIQ_KEY";
};

// ---------------------------------------------------------------- TraceType
// 枚举名即线上字段第 1 段（Pub / Recall / SubBefore / SubAfter / EndTransaction）。
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

// 由第 1 段字符串解析（对应 Java 的 "Pub".equals(...) 判定）。
inline TraceType traceTypeFromName(const std::string& name) {
    if (name == "Pub") return TraceType::PUB;
    if (name == "Recall") return TraceType::RECALL;
    if (name == "SubBefore") return TraceType::SUB_BEFORE;
    if (name == "SubAfter") return TraceType::SUB_AFTER;
    if (name == "EndTransaction") return TraceType::END_TRANSACTION;
    return TraceType::PUB;  // 未知类型交由调用方按"跳过该条"处理
}

// ---------------------------------------------------------------- AccessChannel
// 只在 SubAfter 编码时起作用：非 CLOUD 才追加 timestamp + groupName 两段。
enum class AccessChannel {
    LOCAL = 0,
    CLOUD = 1,
};

inline const char* accessChannelName(AccessChannel c) {
    return c == AccessChannel::CLOUD ? "CLOUD" : "LOCAL";
}

// 消息类型 ordinal（与 Java MessageType 严格一致：Normal=0, Trans=1, TransCommit=2,
// Delay=3, Order=4）。TraceBean 直接存 int，编码时 std::to_string。
enum class TraceMessageType {
    NORMAL = 0,
    TRANS = 1,
    TRANS_COMMIT = 2,
    DELAY = 3,
    ORDER = 4,
};

// 本机出口地址（storeHost/clientHost 默认），对齐 Python LOCAL_ADDRESS。
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
    std::string transactionState;   // LocalTransactionState 的名字（"COMMIT_MESSAGE" 等）
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
    // 默认请求 id（SubBefore/SubAfter 共用一个，是控制台把一次消费前后串起来的关键）；
    // 非空业务场景会被显式覆盖。
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

// ---------------------------------------------------------------- 编解码
class TraceDataEncoder {
public:
    // 复刻 Java String.split：**丢弃末尾的空串**（C++ 原生 split 不会丢弃，必须专门实现）。
    static std::vector<std::string> javaSplit(const std::string& value, const std::string& sep);

    // 把 TraceContext 编成可发送的文本（对应 Java encoderFromContextBean）。
    // 返回 nullopt 表示 ctx 为空（对应 Java 的 null 短路）。
    static std::optional<TraceTransferBean> encoderFromContextBean(const TraceContext* ctx);

    // 把线上轨迹文本解回 TraceContext 列表（对应 Java decoderFromTraceDataString）。
    // 单条记录解码失败时只跳过该条并记 warning，不会让整条 trace data 解码失败。
    static std::vector<TraceContext> decoderFromTraceDataString(const std::string& traceData);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_TRACE_H
