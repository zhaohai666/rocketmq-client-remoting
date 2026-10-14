// The CK string is the core credential of POP: on a normal topic the broker does **not** write the `POP_CK`
// property in the POP response (it only writes it on the retry topic re-encode path), so the client must
// rebuild it itself from the response headers `startOffsetInfo` / `msgOffsetInfo` and send it back as the
// `extraInfo` of ACK / CHANGE_MESSAGE_INVISIBLETIME. The format is 8 segments, separated by **spaces**:
//
//   ckQueueOffset popTime invisibleTime reviveQid retryFlag brokerName queueId [msgQueueOffset]
//
// A wrong separator (a comma, for instance) makes the broker fail to parse silently, so the protocol constants are fixed here.
//
// Error semantics: invalid arguments (too few segments, a segment count that is not 3, duplicate key, illegal retry
// value) throw `std::invalid_argument`, matching how the broker rejects the same inputs.
#ifndef ROCKETMQ_REMOTING_PROTOCOL_EXTRA_INFO_H
#define ROCKETMQ_REMOTING_PROTOCOL_EXTRA_INFO_H

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace rocketmq {
namespace extra_info {

// Separator inside a segment; it **must** be the space, i.e. the protocol constant MessageConst.KEY_SEPARATOR
inline constexpr const char* kKeySeparator = " ";
// Separator between queues
inline constexpr char kQueueSeparator = ';';
// Separator between several offsets of the same queue inside msgOffsetInfo
inline constexpr char kOffsetSeparator = ',';

inline constexpr const char* kNormalTopic = "0";
inline constexpr const char* kRetryTopic = "1";
inline constexpr const char* kRetryTopicV2 = "2";

// Fixed revive queue number used by ordered consumption, i.e. the protocol constant KeyBuilder.POP_ORDER_REVIVE_QUEUE
inline constexpr int32_t kPopOrderReviveQueue = 999;

// ---------------------------------------------------------------- retry topic

// Protocol KeyBuilder.buildPopRetryTopicV1 -> "%RETRY%<cid>_<topic>"
std::string buildPopRetryTopicV1(const std::string& topic, const std::string& cid);
// Protocol KeyBuilder.buildPopRetryTopicV2 -> "%RETRY%<cid>+<topic>"
std::string buildPopRetryTopicV2(const std::string& topic, const std::string& cid);
// buildPopRetryTopic: when enableRetryTopicV2 is off (the broker default), use V1
std::string buildPopRetryTopic(const std::string& topic, const std::string& cid,
                               bool enableRetryV2 = false);
// Protocol KeyBuilder.isPopRetryTopicV2: %RETRY% prefix and containing '+'
bool isPopRetryTopicV2(const std::string& retryTopic);

// ---------------------------------------------------------------- splitting and field access

// Split on spaces, **dropping trailing empty strings** (matching the "tail trimming" semantics of the broker-side
// split; keeping an empty segment would make the segment-count checks below disagree with the broker).
std::vector<std::string> split(const std::string& extraInfo);

int64_t getCkQueueOffset(const std::vector<std::string>& segments);
int64_t getPopTime(const std::vector<std::string>& segments);
int64_t getInvisibleTime(const std::vector<std::string>& segments);
int32_t getReviveQid(const std::vector<std::string>& segments);
std::string getRetry(const std::vector<std::string>& segments);
std::string getBrokerName(const std::vector<std::string>& segments);
int32_t getQueueId(const std::vector<std::string>& segments);
int64_t getQueueOffset(const std::vector<std::string>& segments);

// Derive retryFlag from the shape of the topic. Order matters: test V2 first (contains '+'), then the %RETRY% prefix.
std::string retryOfTopic(const std::string& topic);

// ---------------------------------------------------------------- assembly

// 7-segment version (without msgQueueOffset)
std::string buildExtraInfo(int64_t ckQueueOffset, int64_t popTime, int64_t invisibleTime,
                           int32_t reviveQid, const std::string& topic,
                           const std::string& brokerName, int32_t queueId);
// 8-segment version; the ACK path uses this one
std::string buildExtraInfo(int64_t ckQueueOffset, int64_t popTime, int64_t invisibleTime,
                           int32_t reviveQid, const std::string& topic,
                           const std::string& brokerName, int32_t queueId,
                           int64_t msgQueueOffset);

// ---------------------------------------------------------------- parsing of response-header encodings

// Empty string -> nullopt (an empty protocol value means null); every segment must hold exactly 3 fields, a duplicate key throws
std::optional<std::map<std::string, int64_t>> parseStartOffsetInfo(const std::string& s);
std::optional<std::map<std::string, std::vector<int64_t>>> parseMsgOffsetInfo(const std::string& s);
std::optional<std::map<std::string, int32_t>> parseOrderCountInfo(const std::string& s);

// Key shape: <retryFlag>@<queueId> / <retryFlag>@qo<queueId>%<queueOffset>
std::string getStartOffsetInfoMapKey(const std::string& topic, int64_t key);
std::string getQueueOffsetKeyValueKey(int64_t queueId, int64_t queueOffset);
std::string getQueueOffsetMapKey(const std::string& topic, int64_t queueId, int64_t queueOffset);

// reviveQid == 999 means ordered consumption
bool isOrder(const std::vector<std::string>& segments);

// Recover the real topic from retryFlag (protocol ExtraInfoUtil.getRealTopic)
std::string getRealTopic(const std::string& topic, const std::string& cid,
                         const std::string& retry);

}  // namespace extra_info
}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_EXTRA_INFO_H
