// 发送/订阅入口上的名字校验（org.apache.rocketmq.client.Validators 的 C++ 对应，
// 行为口径以 Python 参考实现 python/rocketmq/client/validators.py 为准）。
//
// **为什么要在客户端就拦下来**：topic/group 名字非法时 broker 也会拒，但要等到请求
// 真的打出去才拿到 TOPIC_NOT_EXIST / ILLEGAL_TOPIC，而 TOPIC_NOT_EXIST 在发送重试的
// 可重试码集合里——于是每条必然失败的消息都会把重试次数和超时预算空转一遍，最后报的
// 还是同一个原因。本地校验让这类输入在 send() 的第一行就失败。
//
// 全部失败都抛 MQClientException，**文案与 Python 逐字一致**：
//   * checkTopic / checkGroup / isSystemTopic / isNotAllowedSendTopic：三步判定顺序为
//     blank → 长度（127 / 120）→ 字符表，都不带 broker 响应码（Java 走
//     MQClientException(String, null)，responseCode = -1 表示"纯客户端错误"；
//     本工程的默认码是 UNKNOWN=1，语义同族：都不是 broker 回来的码，别按 13 分支）；
//   * checkMessage：只有 body 三档（null body / 零长 / 超 maxMessageSize）与
//     INNER_MULTI_DISPATCH 分隔符带 MESSAGE_ILLEGAL(13)，且顺序要对——先 topic、
//     再禁发 topic、最后 body。
#ifndef ROCKETMQ_CLIENT_VALIDATORS_H
#define ROCKETMQ_CLIENT_VALIDATORS_H

#include <cstdint>
#include <string>

#include "rocketmq/common/message.h"

namespace rocketmq {

// 对应 Java File.separator / Python os.sep：Windows 上是 '\'，POSIX 上是 '/'。
// 暴露出来是为了单测能构造"带分隔符的 INNER_MULTI_DISPATCH"而不必各平台写死。
#ifdef _WIN32
inline constexpr const char* kFileSeparator = "\\";
#else
inline constexpr const char* kFileSeparator = "/";
#endif

struct Validators {
    // 对应 Java Validators.CHARACTER_MAX_LENGTH（本文件未用到，保留常量口径）
    static constexpr int32_t CHARACTER_MAX_LENGTH = 255;
    static constexpr int32_t TOPIC_MAX_LENGTH = 127;
    static constexpr int32_t GROUP_MAX_LENGTH = 120;

    // 对应 Validators.checkGroup：blank → 长度 → 字符表。
    static void checkGroup(const std::string& group);

    // 对应 Validators.checkTopic：blank → 长度 → 字符表。
    static void checkTopic(const std::string& topic);

    // 对应 Validators.isSystemTopic / isNotAllowedSendTopic：命中即抛，正常返回。
    static void isSystemTopic(const std::string& topic);
    static void isNotAllowedSendTopic(const std::string& topic);

    // 对应 Validators.checkMessage(msg, producer)。maxMessageSize 由调用方
    // （生产者）传入，等价于 Java 从 DefaultMQProducer.getMaxMessageSize() 取值。
    static void checkMessage(const Message& msg, int32_t maxMessageSize);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_VALIDATORS_H
