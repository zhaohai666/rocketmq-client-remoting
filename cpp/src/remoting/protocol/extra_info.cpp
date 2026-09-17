#include "rocketmq/remoting/protocol/extra_info.h"

#include <stdexcept>

#include "rocketmq/common/mix_all.h"

namespace rocketmq {
namespace extra_info {

namespace {

constexpr char kRetrySepV1 = '_';
constexpr char kRetrySepV2 = '+';

// 复刻 Java String.split(sep) 的"丢弃末尾空串"行为。
std::vector<std::string> splitAndDropTrailing(const std::string& value, char sep) {
    std::vector<std::string> parts;
    std::size_t start = 0;
    while (true) {
        std::size_t pos = value.find(sep, start);
        if (pos == std::string::npos) {
            parts.push_back(value.substr(start));
            break;
        }
        parts.push_back(value.substr(start, pos - start));
        start = pos + 1;
    }
    while (!parts.empty() && parts.back().empty()) {
        parts.pop_back();
    }
    return parts;
}

void requireSize(const std::vector<std::string>& segments, std::size_t need, const char* what) {
    if (segments.size() < need) {
        throw std::invalid_argument(std::string(what) + " fail, extraInfoStrs length "
                                    + std::to_string(segments.size()));
    }
}

int64_t toInt64(const std::string& s) {
    return static_cast<int64_t>(std::stoll(s));
}

int32_t toInt32(const std::string& s) {
    return static_cast<int32_t>(std::stol(s));
}

template <typename Value, typename Convert>
std::optional<std::map<std::string, Value>> parseThreeFieldInfo(
    const std::string& raw, const char* what, Convert convert) {
    if (raw.empty()) {
        return std::nullopt;
    }
    std::map<std::string, Value> out;
    std::vector<std::string> segments;
    if (raw.find(kQueueSeparator) == std::string::npos) {
        segments.push_back(raw);
    } else {
        segments = splitAndDropTrailing(raw, kQueueSeparator);
    }
    for (const std::string& one : segments) {
        std::vector<std::string> parts = splitAndDropTrailing(one, ' ');
        if (parts.size() != 3) {
            throw std::invalid_argument(std::string("parse ") + what + " error, " + raw);
        }
        std::string key = parts[0] + "@" + parts[1];
        if (out.find(key) != out.end()) {
            throw std::invalid_argument(std::string("parse ") + what + " error, duplicate, " + raw);
        }
        out[key] = convert(parts[2]);
    }
    return out;
}

std::string join(const std::vector<std::string>& parts, const char* sep) {
    std::string out;
    for (std::size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) {
            out += sep;
        }
        out += parts[i];
    }
    return out;
}

}  // namespace

std::string buildPopRetryTopicV1(const std::string& topic, const std::string& cid) {
    return std::string(MixAll::RETRY_GROUP_TOPIC_PREFIX) + cid + kRetrySepV1 + topic;
}

std::string buildPopRetryTopicV2(const std::string& topic, const std::string& cid) {
    return std::string(MixAll::RETRY_GROUP_TOPIC_PREFIX) + cid + kRetrySepV2 + topic;
}

std::string buildPopRetryTopic(const std::string& topic, const std::string& cid,
                               bool enableRetryV2) {
    if (enableRetryV2) {
        return buildPopRetryTopicV2(topic, cid);
    }
    return buildPopRetryTopicV1(topic, cid);
}

bool isPopRetryTopicV2(const std::string& retryTopic) {
    if (retryTopic.empty()) {
        return false;
    }
    return retryTopic.rfind(MixAll::RETRY_GROUP_TOPIC_PREFIX, 0) == 0
           && retryTopic.find(kRetrySepV2) != std::string::npos;
}

std::vector<std::string> split(const std::string& extraInfo) {
    return splitAndDropTrailing(extraInfo, ' ');
}

int64_t getCkQueueOffset(const std::vector<std::string>& segments) {
    requireSize(segments, 1, "getCkQueueOffset");
    return toInt64(segments[0]);
}

int64_t getPopTime(const std::vector<std::string>& segments) {
    requireSize(segments, 2, "getPopTime");
    return toInt64(segments[1]);
}

int64_t getInvisibleTime(const std::vector<std::string>& segments) {
    requireSize(segments, 3, "getInvisibleTime");
    return toInt64(segments[2]);
}

int32_t getReviveQid(const std::vector<std::string>& segments) {
    requireSize(segments, 4, "getReviveQid");
    return toInt32(segments[3]);
}

std::string getRetry(const std::vector<std::string>& segments) {
    requireSize(segments, 5, "getRetry");
    return segments[4];
}

std::string getBrokerName(const std::vector<std::string>& segments) {
    requireSize(segments, 6, "getBrokerName");
    return segments[5];
}

int32_t getQueueId(const std::vector<std::string>& segments) {
    requireSize(segments, 7, "getQueueId");
    return toInt32(segments[6]);
}

int64_t getQueueOffset(const std::vector<std::string>& segments) {
    requireSize(segments, 8, "getQueueOffset");
    return toInt64(segments[7]);
}

std::string retryOfTopic(const std::string& topic) {
    if (isPopRetryTopicV2(topic)) {
        return kRetryTopicV2;
    }
    if (topic.rfind(MixAll::RETRY_GROUP_TOPIC_PREFIX, 0) == 0) {
        return kRetryTopic;
    }
    return kNormalTopic;
}

std::string buildExtraInfo(int64_t ckQueueOffset, int64_t popTime, int64_t invisibleTime,
                           int32_t reviveQid, const std::string& topic,
                           const std::string& brokerName, int32_t queueId) {
    std::vector<std::string> parts = {
        std::to_string(ckQueueOffset),
        std::to_string(popTime),
        std::to_string(invisibleTime),
        std::to_string(reviveQid),
        retryOfTopic(topic),
        brokerName,
        std::to_string(queueId),
    };
    return join(parts, kKeySeparator);
}

std::string buildExtraInfo(int64_t ckQueueOffset, int64_t popTime, int64_t invisibleTime,
                           int32_t reviveQid, const std::string& topic,
                           const std::string& brokerName, int32_t queueId,
                           int64_t msgQueueOffset) {
    return buildExtraInfo(ckQueueOffset, popTime, invisibleTime, reviveQid, topic, brokerName,
                          queueId)
           + kKeySeparator + std::to_string(msgQueueOffset);
}

std::optional<std::map<std::string, int64_t>> parseStartOffsetInfo(const std::string& s) {
    return parseThreeFieldInfo<int64_t>(s, "startOffsetInfo",
                                        [](const std::string& v) { return toInt64(v); });
}

std::optional<std::map<std::string, std::vector<int64_t>>> parseMsgOffsetInfo(
    const std::string& s) {
    return parseThreeFieldInfo<std::vector<int64_t>>(
        s, "msgOffsetInfo", [](const std::string& v) {
            std::vector<int64_t> offsets;
            for (const std::string& one : splitAndDropTrailing(v, kOffsetSeparator)) {
                offsets.push_back(toInt64(one));
            }
            return offsets;
        });
}

std::optional<std::map<std::string, int32_t>> parseOrderCountInfo(const std::string& s) {
    return parseThreeFieldInfo<int32_t>(s, "orderCountInfo",
                                        [](const std::string& v) { return toInt32(v); });
}

std::string getStartOffsetInfoMapKey(const std::string& topic, int64_t key) {
    return retryOfTopic(topic) + "@" + std::to_string(key);
}

std::string getQueueOffsetKeyValueKey(int64_t queueId, int64_t queueOffset) {
    return std::string("qo") + std::to_string(queueId) + "%" + std::to_string(queueOffset);
}

std::string getQueueOffsetMapKey(const std::string& topic, int64_t queueId,
                                 int64_t queueOffset) {
    return retryOfTopic(topic) + "@" + getQueueOffsetKeyValueKey(queueId, queueOffset);
}

bool isOrder(const std::vector<std::string>& segments) {
    return getReviveQid(segments) == kPopOrderReviveQueue;
}

std::string getRealTopic(const std::string& topic, const std::string& cid,
                         const std::string& retry) {
    if (retry == kNormalTopic) {
        return topic;
    }
    if (retry == kRetryTopic) {
        return buildPopRetryTopicV1(topic, cid);
    }
    if (retry == kRetryTopicV2) {
        return buildPopRetryTopicV2(topic, cid);
    }
    throw std::invalid_argument("getRetry fail, format is wrong");
}

}  // namespace extra_info
}  // namespace rocketmq
