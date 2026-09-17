// POP 模式的 extraInfo（CK 串）编解码 —— 逐条移植 Java
// `org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil`。
//
// CK 串是 POP 的核心凭据：broker 在普通 topic 的 POP 响应里**不写** `POP_CK`
// 属性（只在 retry topic 重编码路径才写），客户端必须自己用响应头的
// `startOffsetInfo` / `msgOffsetInfo` 反构出来，再作为 ACK /
// CHANGE_MESSAGE_INVISIBLETIME 的 `extraInfo` 回传。格式是 8 段、**空格**分隔：
//
//   ckQueueOffset popTime invisibleTime reviveQid retryFlag brokerName queueId [msgQueueOffset]
//
// 分隔符用错（比如逗号）会让 broker 静默解析失败，所以这里照抄 Java 的常量。
//
// 报错语义与 Java 对齐：参数非法（段数不足、段数不为 3、重复 key、非法 retry 值）
// 抛 `std::invalid_argument`，对应 Java 的 IllegalArgumentException。
#ifndef ROCKETMQ_REMOTING_PROTOCOL_EXTRA_INFO_H
#define ROCKETMQ_REMOTING_PROTOCOL_EXTRA_INFO_H

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace rocketmq {
namespace extra_info {

// 段内分隔符；**必须**是空格，对应 Java MessageConst.KEY_SEPARATOR
inline constexpr const char* kKeySeparator = " ";
// 队列之间的分隔符
inline constexpr char kQueueSeparator = ';';
// msgOffsetInfo 里同一队列多条 offset 的分隔符
inline constexpr char kOffsetSeparator = ',';

inline constexpr const char* kNormalTopic = "0";
inline constexpr const char* kRetryTopic = "1";
inline constexpr const char* kRetryTopicV2 = "2";

// 顺序消费用的固定 revive 队列号，对应 Java KeyBuilder.POP_ORDER_REVIVE_QUEUE
inline constexpr int32_t kPopOrderReviveQueue = 999;

// ---------------------------------------------------------------- 重试 topic

// Java KeyBuilder.buildPopRetryTopicV1 -> "%RETRY%<cid>_<topic>"
std::string buildPopRetryTopicV1(const std::string& topic, const std::string& cid);
// Java KeyBuilder.buildPopRetryTopicV2 -> "%RETRY%<cid>+<topic>"
std::string buildPopRetryTopicV2(const std::string& topic, const std::string& cid);
// Java buildPopRetryTopic：enableRetryTopicV2 关（broker 默认）走 V1
std::string buildPopRetryTopic(const std::string& topic, const std::string& cid,
                               bool enableRetryV2 = false);
// Java KeyBuilder.isPopRetryTopicV2：%RETRY% 前缀且含 '+'
bool isPopRetryTopicV2(const std::string& retryTopic);

// ---------------------------------------------------------------- 切分与取值

// 按空格切分，并**丢弃末尾空串**（复刻 Java String.split 语义；C++ 手写实现
// 若保留空串会让下面的段数校验产生与 Java 不一致的结果）。
std::vector<std::string> split(const std::string& extraInfo);

int64_t getCkQueueOffset(const std::vector<std::string>& segments);
int64_t getPopTime(const std::vector<std::string>& segments);
int64_t getInvisibleTime(const std::vector<std::string>& segments);
int32_t getReviveQid(const std::vector<std::string>& segments);
std::string getRetry(const std::vector<std::string>& segments);
std::string getBrokerName(const std::vector<std::string>& segments);
int32_t getQueueId(const std::vector<std::string>& segments);
int64_t getQueueOffset(const std::vector<std::string>& segments);

// 由 topic 形状判定 retryFlag。顺序很重要：先判 V2（含 '+'），再判 %RETRY% 前缀。
std::string retryOfTopic(const std::string& topic);

// ---------------------------------------------------------------- 拼装

// 7 段版本（不带 msgQueueOffset）
std::string buildExtraInfo(int64_t ckQueueOffset, int64_t popTime, int64_t invisibleTime,
                           int32_t reviveQid, const std::string& topic,
                           const std::string& brokerName, int32_t queueId);
// 8 段版本；ACK 场景用这个
std::string buildExtraInfo(int64_t ckQueueOffset, int64_t popTime, int64_t invisibleTime,
                           int32_t reviveQid, const std::string& topic,
                           const std::string& brokerName, int32_t queueId,
                           int64_t msgQueueOffset);

// ---------------------------------------------------------------- 解析响应头编码

// 空串 -> nullopt（Java 返回 null）；每段必须正好 3 个字段，key 重复抛异常
std::optional<std::map<std::string, int64_t>> parseStartOffsetInfo(const std::string& s);
std::optional<std::map<std::string, std::vector<int64_t>>> parseMsgOffsetInfo(const std::string& s);
std::optional<std::map<std::string, int32_t>> parseOrderCountInfo(const std::string& s);

// key 形态：<retryFlag>@<queueId> / <retryFlag>@qo<queueId>%<queueOffset>
std::string getStartOffsetInfoMapKey(const std::string& topic, int64_t key);
std::string getQueueOffsetKeyValueKey(int64_t queueId, int64_t queueOffset);
std::string getQueueOffsetMapKey(const std::string& topic, int64_t queueId, int64_t queueOffset);

// reviveQid == 999 表示顺序消费
bool isOrder(const std::vector<std::string>& segments);

// 由 retryFlag 还原真实 topic（Java ExtraInfoUtil.getRealTopic）
std::string getRealTopic(const std::string& topic, const std::string& cid,
                         const std::string& retry);

}  // namespace extra_info
}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_EXTRA_INFO_H
