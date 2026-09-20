// 定时消息的撤回句柄（对应 org.apache.rocketmq.common.producer.RecallMessageHandle）。
//
// 句柄不是客户端自己造的：发送带 TIMER_DELIVER_MS / TIMER_DELAY_MS / TIMER_DELAY_SEC
// 的定时消息时，broker 在 SendMessageProcessor#attachRecallHandle 里把这个句柄挂到
// SEND 响应头的 recallHandle 字段上返回，客户端只负责原样带回去调用 recallMessage。
// 普通消息的响应里没有这个字段。
//
// 编码格式与 Java 完全一致：base64url("v1 <topic> <brokerName> <timestampStr> <messageId>")，
// 5 段、空格分隔。
//
// 与 Java 的两处显式差异：
//   * Java buildHandle 用 Base64.getUrlEncoder()（**带** '=' 填充），decodeHandle 用
//     getUrlDecoder()（严格，无填充串会抛错）。这里编码同样带填充，解码两种都吃：
//     另外三个客户端移植版用无填充解码器，只发无填充句柄的客户端写下的消息也要能撤回。
//   * Java 解码失败抛 DecoderException，DefaultMQProducerImpl#recallMessage 再包成
//     MQClientException(e.getMessage())。C++ 没有 checked exception，这里直接抛
//     MQClientException，文案仍是 Java 的 "recall handle is invalid"。
#ifndef ROCKETMQ_COMMON_RECALL_MESSAGE_HANDLE_H
#define ROCKETMQ_COMMON_RECALL_MESSAGE_HANDLE_H

#include <string>

namespace rocketmq {

// 对应 RecallMessageHandle.HandleV1。
// timestampStr 保留字符串而不是转成 int64：Java 就存 String，撤回时要原样回填，
// 非法时间戳由 broker 判 ILLEGAL_OPERATION。
struct HandleV1 {
    std::string topic;
    std::string brokerName;
    std::string timestampStr;
    std::string messageId;

    bool operator==(const HandleV1& other) const {
        return topic == other.topic && brokerName == other.brokerName &&
               timestampStr == other.timestampStr && messageId == other.messageId;
    }
};

// 对应 RecallMessageHandle.buildHandle（输出带 '=' 填充，与 Java 一致）。
std::string buildRecallHandle(const std::string& topic, const std::string& brokerName,
                              const std::string& timestampStr, const std::string& messageId);

// 对应 RecallMessageHandle.decodeHandle：空串 / 非法 base64 / 非 utf-8 /
// 首段不是 "v1" / 段数 < 5 都抛 MQClientException("recall handle is invalid")。
// 超过 5 段时忽略尾段（Java 的 split 取 items[1..4] 同样忽略）。
HandleV1 decodeRecallHandle(const std::string& handle);

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_RECALL_MESSAGE_HANDLE_H
