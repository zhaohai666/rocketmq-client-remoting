// 请求/响应自定义头（对应 common/protocol/header/*.java）。
//
// 为避免 C++ 反射缺失带来的样板代码，这里用 std::optional 表达"字段未设置"：
// toExtFields() 只写入已设置的字段，与 Java RemotingCommand.makeCustomHeaderToNet
// 的"非空才写"语义等价。
#ifndef ROCKETMQ_REMOTING_PROTOCOL_HEADERS_H
#define ROCKETMQ_REMOTING_PROTOCOL_HEADERS_H

#include <cstdint>
#include <optional>
#include <string>

#include "rocketmq/common/boundary_type.h"
#include "rocketmq/common/types.h"

namespace rocketmq {

// 对应 CommandCustomHeader 接口
struct CommandCustomHeader {
    virtual ~CommandCustomHeader() = default;
    virtual PropertyMap toExtFields() const = 0;
    virtual void fromExtFields(const PropertyMap& ext) = 0;
};

// ------------------------------------------------ 帮助函数：optional <-> 字符串

inline void putOptStr(PropertyMap& out, const std::string& key,
                      const std::optional<std::string>& v) {
    if (v.has_value()) out[key] = *v;
}

inline void putOptInt(PropertyMap& out, const std::string& key,
                      const std::optional<int64_t>& v) {
    if (v.has_value()) out[key] = std::to_string(*v);
}

inline void putOptInt32(PropertyMap& out, const std::string& key,
                        const std::optional<int32_t>& v) {
    if (v.has_value()) out[key] = std::to_string(*v);
}

// Java Boolean.toString(true) == "true"，与 broker 端 Boolean.parseBoolean 的大小写不敏感
// 解析互为逆操作；解析侧同样大小写不敏感，因此 "True" / "TRUE" 也能读回。
inline void putOptBool(PropertyMap& out, const std::string& key,
                       const std::optional<bool>& v) {
    if (v.has_value()) out[key] = *v ? "true" : "false";
}

inline std::optional<std::string> getOptStr(const PropertyMap& ext, const std::string& key) {
    auto it = ext.find(key);
    if (it == ext.end()) return std::nullopt;
    return it->second;
}

// Java makeCustomHeaderToNet 写 Enum.toString()：枚举字段的入网文本是枚举名大写。
inline void putOptBoundaryType(PropertyMap& out, const std::string& key,
                               const std::optional<BoundaryType>& v) {
    if (v.has_value()) out[key] = boundaryTypeName(*v);
}

// 缺键回 nullopt（读取端自行回落 LOWER）；有键则走 Java getType 的宽松解析。
inline std::optional<BoundaryType> getOptBoundaryType(const PropertyMap& ext,
                                                      const std::string& key) {
    auto it = ext.find(key);
    if (it == ext.end()) return std::nullopt;
    return boundaryTypeFromString(it->second);
}

inline std::optional<int64_t> getOptLong(const PropertyMap& ext, const std::string& key) {
    auto it = ext.find(key);
    if (it == ext.end() || it->second.empty()) return std::nullopt;
    try {
        return std::stoll(it->second);
    } catch (...) {
        return std::nullopt;
    }
}

inline std::optional<int32_t> getOptInt(const PropertyMap& ext, const std::string& key) {
    auto v = getOptLong(ext, key);
    if (!v.has_value()) return std::nullopt;
    return static_cast<int32_t>(*v);
}

inline std::optional<bool> getOptBool(const PropertyMap& ext, const std::string& key) {
    auto it = ext.find(key);
    if (it == ext.end()) return std::nullopt;
    const std::string& s = it->second;
    if (s.size() == 4 && (s[0] == 't' || s[0] == 'T') &&
        (s[1] == 'r' || s[1] == 'R') && (s[2] == 'u' || s[2] == 'U') &&
        (s[3] == 'e' || s[3] == 'E')) {
        return true;
    }
    return s == "1";
}

// ------------------------------------------------ 发送消息

// SendMessageRequestHeader：长字段名（V1）
struct SendMessageRequestHeader : public CommandCustomHeader {
    std::optional<std::string> producerGroup;
    std::optional<std::string> topic;
    std::optional<std::string> defaultTopic;
    std::optional<int32_t> defaultTopicQueueNums;
    std::optional<int32_t> queueId;
    std::optional<int32_t> sysFlag;
    std::optional<int64_t> bornTimestamp;
    std::optional<int32_t> flag;
    std::optional<std::string> properties;
    std::optional<int32_t> reconsumeTimes;
    std::optional<bool> unitMode;
    std::optional<int32_t> maxReconsumeTimes;
    std::optional<bool> batch;
    std::optional<std::string> brokerName;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// SendMessageRequestHeaderV2：短字段名（a..n），减少每条消息的头部体积
struct SendMessageRequestHeaderV2 : public CommandCustomHeader {
    std::optional<std::string> producerGroup;   // a
    std::optional<std::string> topic;           // b
    std::optional<std::string> defaultTopic;    // c
    std::optional<int32_t> defaultTopicQueueNums;  // d
    std::optional<int32_t> queueId;             // e
    std::optional<int32_t> sysFlag;             // f
    std::optional<int64_t> bornTimestamp;       // g
    std::optional<int32_t> flag;                // h
    std::optional<std::string> properties;      // i
    std::optional<int32_t> reconsumeTimes;      // j
    std::optional<bool> unitMode;               // k
    std::optional<int32_t> maxReconsumeTimes;   // l
    std::optional<bool> batch;                  // m
    std::optional<std::string> brokerName;      // n

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;

    // V1 <-> V2 互转（对应 DefaultMQProducerImpl.sendKernelImpl 里的 implicitly 逻辑）
    static SendMessageRequestHeaderV2 fromV1(const SendMessageRequestHeader& v1);
    SendMessageRequestHeader toV1() const;
};

struct SendMessageResponseHeader : public CommandCustomHeader {
    std::optional<std::string> msgId;
    std::optional<int32_t> queueId;
    std::optional<int64_t> queueOffset;
    std::optional<std::string> transactionId;
    // 批量消息（inner-batch）时 broker 回的是批量消息自身的 UNIQ_KEY
    // （SendMessageProcessor:630），普通 topic 的客户端批量不会回。
    std::optional<std::string> batchUniqId;
    std::optional<int64_t> msgRegion;
    // 只给定时/延迟消息：broker 的 SendMessageProcessor#attachRecallHandle 看到
    // TIMER_OUT_MS + REAL_TOPIC 才挂上，普通消息恒为 nullopt。
    std::optional<std::string> recallHandle;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// 对应 org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader。
// ⚠ Java 侧继承 TopicRequestHeader → RpcRequestHeader，父类字段 bname 的**反射名就是
// bname**（不是 brokerName）：写成 brokerName 会被 broker 静默丢掉。
struct RecallMessageRequestHeader : public CommandCustomHeader {
    std::optional<std::string> producerGroup;
    std::optional<std::string> topic;
    std::optional<std::string> recallHandle;
    std::optional<std::string> bname;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// 对应 RecallMessageResponseHeader：Java 只有一个字段 msgId（被撤回消息的 uniqKey）。
struct RecallMessageResponseHeader : public CommandCustomHeader {
    std::optional<std::string> msgId;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// Request-Reply（5.x）：broker 用 PUSH_REPLY_MESSAGE_TO_CLIENT(326) 把应答推回请求方，
// 该 header 就是随 326 下发的（对应 Java ReplyMessageRequestHeader）。
// ⚠ 与 Java 字段名逐字一致（broker 侧用 fastjson2 按属性名反序列化/序列化）。
struct ReplyMessageRequestHeader : public CommandCustomHeader {
    std::optional<std::string> producerGroup;
    std::optional<std::string> topic;
    std::optional<std::string> defaultTopic;
    std::optional<int32_t> defaultTopicQueueNums;
    std::optional<int32_t> queueId;
    std::optional<int32_t> sysFlag;
    std::optional<int64_t> bornTimestamp;
    std::optional<int32_t> flag;
    std::optional<std::string> properties;
    std::optional<int32_t> reconsumeTimes;
    std::optional<bool> unitMode;
    std::optional<std::string> bornHost;
    std::optional<std::string> storeHost;
    std::optional<int64_t> storeTimestamp;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// ------------------------------------------------ 拉取消息

struct PullMessageRequestHeader : public CommandCustomHeader {
    std::optional<std::string> consumerGroup;
    std::optional<std::string> topic;
    std::optional<std::string> liteTopic;
    std::optional<int32_t> queueId;
    std::optional<int64_t> queueOffset;
    std::optional<int32_t> maxMsgNums;
    std::optional<int32_t> sysFlag;
    std::optional<int64_t> commitOffset;
    std::optional<int64_t> suspendTimeoutMillis;
    std::optional<std::string> subscription;
    std::optional<int64_t> subVersion;
    std::optional<std::string> expressionType;
    std::optional<int32_t> maxMsgBytes;
    std::optional<int32_t> requestSource;
    std::optional<std::string> proxyFrowardClientId;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct PullMessageResponseHeader : public CommandCustomHeader {
    std::optional<int64_t> suggestWhichBrokerId;
    std::optional<int64_t> nextBeginOffset;
    std::optional<int64_t> minOffset;
    std::optional<int64_t> maxOffset;
    std::optional<bool> forbidCommitOffset;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// ------------------------------------------------ 消费位点

struct QueryConsumerOffsetRequestHeader : public CommandCustomHeader {
    std::optional<std::string> consumerGroup;
    std::optional<std::string> topic;
    std::optional<int32_t> queueId;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct QueryConsumerOffsetResponseHeader : public CommandCustomHeader {
    std::optional<int64_t> offset;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct UpdateConsumerOffsetRequestHeader : public CommandCustomHeader {
    std::optional<std::string> consumerGroup;
    std::optional<std::string> topic;
    std::optional<int32_t> queueId;
    std::optional<int64_t> commitOffset;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct UpdateConsumerOffsetResponseHeader : public CommandCustomHeader {
    PropertyMap toExtFields() const override { return {}; }
    void fromExtFields(const PropertyMap&) override {}
};

// ------------------------------------------------ offset 查询

struct GetMaxOffsetRequestHeader : public CommandCustomHeader {
    std::optional<std::string> topic;
    std::optional<int32_t> queueId;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct GetMaxOffsetResponseHeader : public CommandCustomHeader {
    std::optional<int64_t> offset;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct GetMinOffsetRequestHeader : public CommandCustomHeader {
    std::optional<std::string> topic;
    std::optional<int32_t> queueId;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct GetMinOffsetResponseHeader : public CommandCustomHeader {
    std::optional<int64_t> offset;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct SearchOffsetRequestHeader : public CommandCustomHeader {
    std::optional<std::string> topic;
    std::optional<int32_t> queueId;
    std::optional<int64_t> timestamp;
    // Java 该字段是 @CFNullable：未设置时**不写键**（只有已废弃的 5 参
    // MQClientAPIImpl#searchOffset 会这样发）。
    std::optional<BoundaryType> boundaryType;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct SearchOffsetResponseHeader : public CommandCustomHeader {
    std::optional<int64_t> offset;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// ------------------------------------------------ 其它常用头部

struct ViewMessageRequestHeader : public CommandCustomHeader {
    std::optional<int64_t> offset;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct QueryMessageRequestHeader : public CommandCustomHeader {
    std::optional<std::string> topic;
    std::optional<std::string> key;
    std::optional<int32_t> maxNum;
    std::optional<int64_t> beginTimestamp;
    std::optional<int64_t> endTimestamp;
    // 索引类型："K"=普通 KEYS 索引，"U"=uniqKey（需 RocksDB 索引），"T"=tag。
    // 见 MessageConst::INDEX_*_TYPE。空值时 broker 按 "K" 处理。
    std::optional<std::string> indexType;
    // 分页游标：broker 侧每页最多返回 maxNum 条，继续翻页时带上上一页最后一条的 key。
    std::optional<std::string> lastKey;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct EndTransactionRequestHeader : public CommandCustomHeader {
    std::optional<std::string> topic;
    std::optional<std::string> producerGroup;
    std::optional<int64_t> tranStateTableOffset;
    std::optional<int64_t> commitLogOffset;
    std::optional<int32_t> commitOrRollback;  // MessageSysFlag.TRANSACTION_*_TYPE
    std::optional<bool> fromTransactionCheck;
    std::optional<std::string> msgId;
    std::optional<std::string> transactionId;
    // ⚠ 键名必须是 bname：该字段继承自 Java RpcRequestHeader 的 `protected String bname`
    // （setter 叫 setBrokerName，但 RemotingCommand.makeCustomHeaderToNet 用反射取的是
    // **字段声明名**），写成 brokerName 会让 broker 静默丢字段。
    std::optional<std::string> bname;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct ConsumerSendMsgBackRequestHeader : public CommandCustomHeader {
    std::optional<std::string> group;
    std::optional<int64_t> offset;
    std::optional<int32_t> delayLevel;
    std::optional<std::string> originMsgId;
    std::optional<std::string> originTopic;
    std::optional<bool> unitMode;
    std::optional<int32_t> maxReconsumeTimes;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct HeartbeatRequestHeader : public CommandCustomHeader {
    std::optional<std::string> clientId;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct UnregisterClientRequestHeader : public CommandCustomHeader {
    std::optional<std::string> clientId;
    std::optional<std::string> producerGroup;
    std::optional<std::string> consumerGroup;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct GetConsumerListByGroupRequestHeader : public CommandCustomHeader {
    std::optional<std::string> consumerGroup;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct GetConsumerListByGroupResponseHeader : public CommandCustomHeader {
    PropertyMap toExtFields() const override { return {}; }
    void fromExtFields(const PropertyMap&) override {}
};

struct NotifyConsumerIdsChangedRequestHeader : public CommandCustomHeader {
    std::optional<std::string> consumerGroup;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct GetRouteInfoRequestHeader : public CommandCustomHeader {
    std::optional<std::string> topic;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// broker -> client 的事务回查请求（RequestCode.CHECK_TRANSACTION_STATE = 39）。
// ⚠ Java 的 offsetMsgId 是 **String**（不是 long），别按整型序列化。
struct CheckTransactionStateRequestHeader : public CommandCustomHeader {
    std::optional<std::string> topic;
    std::optional<int64_t> tranStateTableOffset;
    std::optional<int64_t> commitLogOffset;
    std::optional<std::string> msgId;
    std::optional<std::string> transactionId;
    std::optional<std::string> offsetMsgId;
    std::optional<std::string> bname;  // 同 EndTransactionRequestHeader：键名是 bname

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// 对应 org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader
// 用于 UPDATE_AND_CREATE_TOPIC：在 broker 上按指定队列数/权限建 topic。
struct CreateTopicRequestHeader : public CommandCustomHeader {
    std::optional<std::string> topic;
    std::optional<std::string> defaultTopic;
    std::optional<int32_t> readQueueNums;
    std::optional<int32_t> writeQueueNums;
    std::optional<int32_t> perm;
    std::optional<std::string> topicFilterType;  // 默认 SINGLE_TAG
    std::optional<int32_t> topicSysFlag;
    std::optional<bool> order;   // 默认 false
    std::optional<std::string> attributes;
    std::optional<bool> force;   // 默认 false

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// ------------------------------------------------ POP 模式（5.x 轻量消费）
//
// ext key 必须逐字等于 Java 侧字段名：broker 用 fastjson2 按 Java 属性名反序列化，
// 错一个字母就**静默丢字段**（不报错、行为静默退化）。
//
// 注意 order / suspend：Java 用的是非空 Boolean/boolean，encodeHeader 只跳过 null，
// 所以它们**总是**出现在报文里 —— 这里用非 optional 的 bool，保证同样总是写出。

// Java PopMessageRequestHeader（RequestCode::POP_MESSAGE = 200050）
struct PopMessageRequestHeader : public CommandCustomHeader {
    std::optional<std::string> consumerGroup;
    std::optional<std::string> topic;
    std::optional<int32_t> queueId;
    std::optional<int32_t> maxMsgNums;
    std::optional<int64_t> invisibleTime;
    std::optional<int64_t> pollTime;
    // bornTime 必须是**当前毫秒时间戳**：broker 校验
    // now - bornTime - pollTime > 500 就直接回 POLLING_TIMEOUT(210)。
    std::optional<int64_t> bornTime;
    // 0 = MIN（从最小位点开始），非 0 = MAX（只拿新消息）
    std::optional<int32_t> initMode;
    std::optional<std::string> expType;
    std::optional<std::string> exp;
    bool order = false;
    std::optional<std::string> attemptId;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// Java PopMessageResponseHeader
struct PopMessageResponseHeader : public CommandCustomHeader {
    std::optional<int64_t> popTime;
    std::optional<int64_t> invisibleTime;
    std::optional<int32_t> reviveQid;
    std::optional<int64_t> restNum;
    std::optional<std::string> startOffsetInfo;
    std::optional<std::string> msgOffsetInfo;
    std::optional<std::string> orderCountInfo;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// Java AckMessageRequestHeader（RequestCode::ACK_MESSAGE = 200051）
// offset 是 **consumeQueue offset**（CK 串第 8 段），不是 commitlog offset。
struct AckMessageRequestHeader : public CommandCustomHeader {
    std::optional<std::string> consumerGroup;
    std::optional<std::string> topic;
    std::optional<int32_t> queueId;
    std::optional<std::string> extraInfo;
    std::optional<int64_t> offset;
    std::optional<std::string> liteTopic;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// Java ChangeInvisibleTimeRequestHeader（CHANGE_MESSAGE_INVISIBLETIME = 200053）
struct ChangeInvisibleTimeRequestHeader : public CommandCustomHeader {
    std::optional<std::string> consumerGroup;
    std::optional<std::string> topic;
    std::optional<int32_t> queueId;
    std::optional<std::string> extraInfo;
    std::optional<int64_t> offset;
    std::optional<int64_t> invisibleTime;
    std::optional<std::string> liteTopic;
    bool suspend = false;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// Java ChangeInvisibleTimeResponseHeader：返回的是**新的** popTime/invisibleTime/reviveQid
struct ChangeInvisibleTimeResponseHeader : public CommandCustomHeader {
    std::optional<int64_t> popTime;
    std::optional<int64_t> invisibleTime;
    std::optional<int32_t> reviveQid;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_HEADERS_H
