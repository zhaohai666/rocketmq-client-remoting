// Request/response custom headers (the broker-side protocol definitions under remoting.protocol.header).
//
// To avoid the boilerplate a missing reflection layer would force, std::optional expresses "field not set":
// toExtFields() writes only the set fields (equivalent to the protocol's "write only non-null" semantics).
#ifndef ROCKETMQ_REMOTING_PROTOCOL_HEADERS_H
#define ROCKETMQ_REMOTING_PROTOCOL_HEADERS_H

#include <cstdint>
#include <optional>
#include <string>

#include "rocketmq/common/boundary_type.h"
#include "rocketmq/common/types.h"

namespace rocketmq {

// The CommandCustomHeader interface
struct CommandCustomHeader {
    virtual ~CommandCustomHeader() = default;
    virtual PropertyMap toExtFields() const = 0;
    virtual void fromExtFields(const PropertyMap& ext) = 0;
};

// ------------------------------------------------ helpers: optional <-> string

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

// A bool goes on the wire as "true"/"false"; the broker parses booleans case-insensitively and so does
// this side, hence "True" / "TRUE" also read back.
inline void putOptBool(PropertyMap& out, const std::string& key,
                       const std::optional<bool>& v) {
    if (v.has_value()) out[key] = *v ? "true" : "false";
}

inline std::optional<std::string> getOptStr(const PropertyMap& ext, const std::string& key) {
    auto it = ext.find(key);
    if (it == ext.end()) return std::nullopt;
    return it->second;
}

// The protocol serializes by Enum name: the on-wire text of an enum field is the upper-case enum name.
inline void putOptBoundaryType(PropertyMap& out, const std::string& key,
                               const std::optional<BoundaryType>& v) {
    if (v.has_value()) out[key] = boundaryTypeName(*v);
}

// A missing key yields nullopt (the reader falls back to LOWER itself); a present key goes through the protocol's lenient getType parsing.
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

// ------------------------------------------------ sending messages

// SendMessageRequestHeader: long field names (V1)
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

// SendMessageRequestHeaderV2: short field names (a..n), shrinking the header size of every message
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

    // V1 <-> V2 conversion (the same field set; V2 uses short names to compress the header size)
    static SendMessageRequestHeaderV2 fromV1(const SendMessageRequestHeader& v1);
    SendMessageRequestHeader toV1() const;
};

struct SendMessageResponseHeader : public CommandCustomHeader {
    std::optional<std::string> msgId;
    std::optional<int32_t> queueId;
    std::optional<int64_t> queueOffset;
    std::optional<std::string> transactionId;
    // For an inner-batch message the broker returns the UNIQ_KEY of the batch message itself;
    // a client-side batch on a normal topic does not return it.
    std::optional<std::string> batchUniqId;
    std::optional<int64_t> msgRegion;
    // Only for timed/delay messages: the broker's SendMessageProcessor#attachRecallHandle attaches it
    // when it sees TIMER_OUT_MS + REAL_TOPIC, so a normal message is always nullopt.
    std::optional<std::string> recallHandle;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct RecallMessageRequestHeader : public CommandCustomHeader {
    std::optional<std::string> producerGroup;
    std::optional<std::string> topic;
    std::optional<std::string> recallHandle;
    std::optional<std::string> bname;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// RecallMessageResponseHeader: the protocol has a single field msgId (the uniqKey of the recalled message).
struct RecallMessageResponseHeader : public CommandCustomHeader {
    std::optional<std::string> msgId;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// Request-Reply (5.x): the broker pushes the reply back to the requester with PUSH_REPLY_MESSAGE_TO_CLIENT(326),
// and this header is the one carried with 326 (the protocol ReplyMessageRequestHeader).
// ⚠ Verbatim identical to the protocol field names (the broker de/serializes by property name with fastjson).
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

// ------------------------------------------------ pulling messages

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

// ------------------------------------------------ consumer offset

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

// ------------------------------------------------ offset queries

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
    // The protocol marks this field @CFNullable: when unset, the **key is not written**.
    std::optional<BoundaryType> boundaryType;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct SearchOffsetResponseHeader : public CommandCustomHeader {
    std::optional<int64_t> offset;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// ------------------------------------------------ other common headers

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
    // Index type: "K"=ordinary KEYS index, "U"=uniqKey (needs a RocksDB index), "T"=tag.
    // See MessageConst::INDEX_*_TYPE. An empty value is treated as "K" by the broker.
    std::optional<std::string> indexType;
    // Paging cursor: the broker returns at most maxNum entries per page; the next page carries the key of the last entry of the previous page.
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
    // ⚠ The key name must be bname: this field is inherited from the protocol RpcRequestHeader, and the broker reads it by
    // the **declared field name** (not the setter name brokerName); using brokerName makes the broker drop the field silently.
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

// broker -> client transaction check-back request (RequestCode.CHECK_TRANSACTION_STATE = 39).
// ⚠ In the protocol offsetMsgId is a **String** (not a long); do not serialize it as an integer.
struct CheckTransactionStateRequestHeader : public CommandCustomHeader {
    std::optional<std::string> topic;
    std::optional<int64_t> tranStateTableOffset;
    std::optional<int64_t> commitLogOffset;
    std::optional<std::string> msgId;
    std::optional<std::string> transactionId;
    std::optional<std::string> offsetMsgId;
    std::optional<std::string> bname;  // Same as EndTransactionRequestHeader: the key name is bname

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

struct CreateTopicRequestHeader : public CommandCustomHeader {
    std::optional<std::string> topic;
    std::optional<std::string> defaultTopic;
    std::optional<int32_t> readQueueNums;
    std::optional<int32_t> writeQueueNums;
    std::optional<int32_t> perm;
    std::optional<std::string> topicFilterType;  // Defaults to SINGLE_TAG
    std::optional<int32_t> topicSysFlag;
    std::optional<bool> order;   // Defaults to false
    std::optional<std::string> attributes;
    std::optional<bool> force;   // Defaults to false

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// ------------------------------------------------ POP mode (5.x lightweight consumption)
//
// The ext key must equal the protocol field name verbatim: the broker deserializes with fastjson by
// protocol property name, so one wrong letter **drops the field silently** (no error, behaviour degrades silently).
//
// Note order / suspend: in the protocol these two fields are non-null and encodeHeader skips only null,
// so they **always** appear on the wire -- hence the non-optional bool here, guaranteeing they are always written too.

// Protocol PopMessageRequestHeader (RequestCode::POP_MESSAGE = 200050)
struct PopMessageRequestHeader : public CommandCustomHeader {
    std::optional<std::string> consumerGroup;
    std::optional<std::string> topic;
    std::optional<int32_t> queueId;
    std::optional<int32_t> maxMsgNums;
    std::optional<int64_t> invisibleTime;
    std::optional<int64_t> pollTime;
    // bornTime must be the **current millisecond timestamp**: the broker answers
    // POLLING_TIMEOUT(210) straight away when now - bornTime - pollTime > 500.
    std::optional<int64_t> bornTime;
    // 0 = MIN (start from the smallest offset), non-0 = MAX (only take new messages)
    std::optional<int32_t> initMode;
    std::optional<std::string> expType;
    std::optional<std::string> exp;
    bool order = false;
    std::optional<std::string> attemptId;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

// Protocol PopMessageResponseHeader
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

// Protocol AckMessageRequestHeader (RequestCode::ACK_MESSAGE = 200051)
// offset is the **consumeQueue offset** (the 8th segment of the CK string), not a commitlog offset.
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

// Protocol ChangeInvisibleTimeRequestHeader (CHANGE_MESSAGE_INVISIBLETIME = 200053)
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

// Protocol ChangeInvisibleTimeResponseHeader: returns the **new** popTime/invisibleTime/reviveQid
struct ChangeInvisibleTimeResponseHeader : public CommandCustomHeader {
    std::optional<int64_t> popTime;
    std::optional<int64_t> invisibleTime;
    std::optional<int32_t> reviveQid;

    PropertyMap toExtFields() const override;
    void fromExtFields(const PropertyMap& ext) override;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_HEADERS_H
