// 定时消息撤回（RECALL_MESSAGE 370）离线单测 —— 不需要集群。
//
// 对齐基准是 Java 5.5.1 的 RecallMessageHandle / RecallMessageRequestHeader /
// DefaultMQProducerImpl#recallMessage(:1570-1601)。盯的是三类错得很安静的东西：
//   * 句柄编解码：分隔符是**空格**、版本段是 "v1"、编码是 base64url **带 '=' 填充**；
//     Java 解码器不吃无填充串，本端口两种都吃（见头文件差异说明），所以两种都要测。
//   * 报文键名：RecallMessageRequestHeader 在 Java 里继承 RpcRequestHeader，
//     brokerName 的**反射名是 bname**，写成 brokerName 会被 broker 静默丢掉。
//   * 客户端校验顺序：状态 → checkTopic → 禁 retry/DLQ → 解句柄 → 定位 broker，
//     前三步都不该打网络（一个手滑的句柄不该耗掉一次 RPC 超时）。
//
// 语义（撤回后消息到底还不投递）只有真机能证，见 examples/recall_live.cpp。
#include <chrono>
#include <cstdio>
#include <functional>
#include <string>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/recall_message_handle.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/headers.h"

using namespace rocketmq;

namespace {

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

const char* kTopic = "TopicRecallUnit";
const char* kBroker = "broker-a";
const char* kUniqKey = "0123456789ABCDEF0123456789abcdef";
// Java 的 RecallMessageHandle 在所有解码失败分支上给的都是这一句文案。
const char* kInvalid = "recall handle is invalid";

std::string catchMessage(const std::function<void()>& fn) {
    try {
        fn();
    } catch (const MQClientException& e) {
        return e.what();
    } catch (const std::exception& e) {
        return std::string("other:") + e.what();
    }
    return "<no exception>";
}

// ---------------------------------------------------------------- 句柄编解码
void testCodec() {
    // 向量是用 Java 的 Base64.getUrlEncoder() 实算出来的，改动即失配。
    const std::string handle = buildRecallHandle(kTopic, kBroker, "1700000000000", kUniqKey);
    expect(handle == "djEgVG9waWNSZWNhbGxVbml0IGJyb2tlci1hIDE3MDAwMDAwMDAwMDAg"
                     "MDEyMzQ1Njc4OUFCQ0RFRjAxMjM0NTY3ODlhYmNkZWY=",
           "buildHandle 与 Java 向量一致", handle);
    expect(handle.size() % 4 == 0, "buildHandle 保留 Java 的 '=' 填充");

    const HandleV1 parsed = decodeRecallHandle(handle);
    expect(parsed.topic == kTopic && parsed.brokerName == kBroker &&
               parsed.timestampStr == "1700000000000" && parsed.messageId == kUniqKey,
           "句柄往返一致", parsed.topic + "/" + parsed.brokerName);
    // Java 的 getUrlDecoder 会拒无填充串；本端口必须两种都吃（另外三个移植用无填充编码器）。
    std::string unpadded = handle;
    while (!unpadded.empty() && unpadded.back() == '=') unpadded.pop_back();
    expect(decodeRecallHandle(unpadded) == parsed, "无填充句柄同样能解");

    // Java split(" ") 后只取 items[1..4]，多余分段直接忽略。
    // 下面是 base64url("v1 TopicA broker-a 1700000000000 abc junk")。
    const HandleV1 truncated =
        decodeRecallHandle("djEgVG9waWNBIGJyb2tlci1hIDE3MDAwMDAwMDAwMDAgYWJjIGp1bms=");
    expect(truncated.topic == "TopicA" && truncated.messageId == "abc",
           "多余分段被忽略（与 Java 一致）", truncated.messageId);

    expect(catchMessage([] { decodeRecallHandle(""); }) == kInvalid, "空句柄被拒");
    expect(catchMessage([] { decodeRecallHandle("not-a-handle"); }) == kInvalid,
           "非 v1 五段的句柄被拒");
    expect(catchMessage([] { decodeRecallHandle("!!!!"); }) == kInvalid,
           "非法 base64 字符被拒");
    // base64url("v2 TopicA b 1 id") / ("v1 TopicA b 1")
    expect(catchMessage([] { decodeRecallHandle("djIgVG9waWNBIGIgMSBpZA=="); }) == kInvalid,
           "版本号不对被拒");
    expect(catchMessage([] { decodeRecallHandle("djEgVG9waWNBIGIgMQ=="); }) == kInvalid,
           "段数不足被拒");
    // utf-8 编码的 0xFF 0xFE 开头 —— 解出来根本不是文本
    expect(catchMessage([] { decodeRecallHandle("__4gYmFkIHV0Zjg="); }) == kInvalid,
           "解出来不是合法 utf-8 时拒绝");
}

// ---------------------------------------------------------------- 报文头
void testHeaders() {
    RecallMessageRequestHeader header;
    header.producerGroup = std::string("PG");
    header.topic = std::string(kTopic);
    header.recallHandle = std::string("djEg");
    header.bname = std::string(kBroker);
    const PropertyMap ext = header.toExtFields();
    expect(ext.size() == 4 && ext.count("producerGroup") == 1 && ext.count("topic") == 1 &&
               ext.count("recallHandle") == 1 && ext.count("bname") == 1,
           "撤回请求头用 Java 的键名（brokerName 反射名是 bname）");
    expect(ext.at("bname") == kBroker, "bname 落盘", ext.count("bname") ? ext.at("bname") : "");

    RecallMessageRequestHeader decoded;
    decoded.fromExtFields(ext);
    expect(decoded.bname == kBroker && decoded.recallHandle == "djEg" && decoded.topic == kTopic &&
               decoded.producerGroup == "PG",
           "撤回请求头往返一致");

    RecallMessageResponseHeader resp;
    PropertyMap respExt;
    respExt["msgId"] = std::string(kUniqKey);
    resp.fromExtFields(respExt);
    expect(resp.msgId == kUniqKey, "撤回响应头读 msgId");
    expect(resp.toExtFields().at("msgId") == kUniqKey, "撤回响应头写 msgId");

    SendMessageResponseHeader send;
    PropertyMap sendExt;
    sendExt["msgId"] = std::string("0000000000000000");
    sendExt["recallHandle"] = std::string("djEg");
    send.fromExtFields(sendExt);
    expect(send.recallHandle == "djEg", "SEND 响应头解析 recallHandle");
    expect(send.toExtFields().at("recallHandle") == "djEg", "recallHandle 能编回线上格式");
    SendMessageResponseHeader plain;
    plain.fromExtFields(PropertyMap{{"msgId", std::string("x")}});
    expect(!plain.recallHandle.has_value(), "普通消息没有 recallHandle");
    expect(plain.toExtFields().count("recallHandle") == 0, "缺失时不会被编进报文");

    expect(RequestCode::RECALL_MESSAGE == 370, "请求码 370");
}

// ---------------------------------------------------------------- 生产者本地校验
void testProducerValidation() {
    DefaultMQProducer notStarted("PG_not_started");
    expect(catchMessage([&] { (void)notStarted.recallMessage(kTopic, "djEg"); }) ==
               "producer not started, call start() first",
           "未 start 就撤回会被拒");

    // 起了实例但地址服务器不可达：本地校验必须在打网络之前跑完。
    DefaultMQProducer p("PG_recall_unit");
    p.setNamesrvAddr("127.0.0.1:1");
    p.start();

    expect(catchMessage([&] { (void)p.recallMessage("%RETRY%PG_recall_unit", "djEg"); }) ==
               "topic is not supported",
           "%RETRY% topic 在本地就被拒（Java 文案）");
    expect(catchMessage([&] { (void)p.recallMessage("%DLQ%PG_recall_unit", "djEg"); }) ==
               "topic is not supported",
           "%DLQ% topic 在本地就被拒（Java 文案）");
    expect(catchMessage([&] { (void)p.recallMessage("bad topic!", "djEg"); }) !=
               "<no exception>",
           "非法 topic 名被拒");

    const auto began = std::chrono::steady_clock::now();
    const std::string corrupt = catchMessage([&] { (void)p.recallMessage(kTopic, "not-a-handle"); });
    const auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - began)
                          .count();
    expect(corrupt == kInvalid, "非法句柄本地即拒", corrupt);
    expect(cost < 200, "非法句柄是秒回的（没打网络）", std::to_string(cost) + "ms");

    // 句柄合法但拿不到路由：Java 的 tryToFindTopicPublishInfo **异常照抛**
    // （DefaultMQProducerImpl:1586），所以这里 surfacing 的是路由错误而不是
    // "The broker service address not found"（后者要有路由、只是没有可用 broker 才会走到）。
    const std::string handle = buildRecallHandle(kTopic, kBroker, "1700000000000", kUniqKey);
    const std::string noRoute = catchMessage([&] { (void)p.recallMessage(kTopic, handle); });
    expect(noRoute == "Can not find Message Queue for topic: " + std::string(kTopic),
           "路由缺失时预热带异常照抛（Java 语义）", noRoute);

    p.shutdown();
}

}  // namespace

int main() {
    testCodec();
    testHeaders();
    testProducerValidation();
    std::printf("%s recall_message: %d checks, %d failures\n", fails == 0 ? "OK" : "FAILED",
                checks, fails);
    return fails == 0 ? 0 : 1;
}
