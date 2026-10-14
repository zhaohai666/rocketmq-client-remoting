// Offline unit test for scheduled-message recall (RECALL_MESSAGE 370) -- no cluster required.
//
//
// Only a live cluster can prove the semantics (whether the message is really not delivered after a recall); see examples/recall_live.cpp.
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

// ---------------------------------------------------------------- handle codec
void testCodec() {
    const std::string handle = buildRecallHandle(kTopic, kBroker, "1700000000000", kUniqKey);
    expect(handle == "djEgVG9waWNSZWNhbGxVbml0IGJyb2tlci1hIDE3MDAwMDAwMDAwMDAg"
                     "MDEyMzQ1Njc4OUFCQ0RFRjAxMjM0NTY3ODlhYmNkZWY=",
           "buildHandle matches the known vector", handle);
    expect(handle.size() % 4 == 0, "buildHandle keeps the '=' padding");

    const HandleV1 parsed = decodeRecallHandle(handle);
    expect(parsed.topic == kTopic && parsed.brokerName == kBroker &&
               parsed.timestampStr == "1700000000000" && parsed.messageId == kUniqKey,
           "the handle round trip is consistent", parsed.topic + "/" + parsed.brokerName);
    std::string unpadded = handle;
    while (!unpadded.empty() && unpadded.back() == '=') unpadded.pop_back();
    expect(decodeRecallHandle(unpadded) == parsed, "an unpadded handle decodes just as well");

    const HandleV1 truncated =
        decodeRecallHandle("djEgVG9waWNBIGJyb2tlci1hIDE3MDAwMDAwMDAwMDAgYWJjIGp1bms=");
    expect(truncated.topic == "TopicA" && truncated.messageId == "abc",
           "extra segments are ignored", truncated.messageId);

    expect(catchMessage([] { decodeRecallHandle(""); }) == kInvalid, "an empty handle is rejected");
    expect(catchMessage([] { decodeRecallHandle("not-a-handle"); }) == kInvalid,
           "a handle that is not five v1 segments is rejected");
    expect(catchMessage([] { decodeRecallHandle("!!!!"); }) == kInvalid,
           "illegal base64 characters are rejected");
    // base64url("v2 TopicA b 1 id") / ("v1 TopicA b 1")
    expect(catchMessage([] { decodeRecallHandle("djIgVG9waWNBIGIgMSBpZA=="); }) == kInvalid,
           "a wrong version number is rejected");
    expect(catchMessage([] { decodeRecallHandle("djEgVG9waWNBIGIgMQ=="); }) == kInvalid,
           "too few segments are rejected");
    // Starting with the utf-8 encoding of 0xFF 0xFE -- the decoded result is not text at all
    expect(catchMessage([] { decodeRecallHandle("__4gYmFkIHV0Zjg="); }) == kInvalid,
           "rejected when the decoded bytes are not valid utf-8");
}

// ---------------------------------------------------------------- message header
void testHeaders() {
    RecallMessageRequestHeader header;
    header.producerGroup = std::string("PG");
    header.topic = std::string(kTopic);
    header.recallHandle = std::string("djEg");
    header.bname = std::string(kBroker);
    const PropertyMap ext = header.toExtFields();
    expect(ext.size() == 4 && ext.count("producerGroup") == 1 && ext.count("topic") == 1 &&
               ext.count("recallHandle") == 1 && ext.count("bname") == 1,
           "the recall request header uses protocol key names (the reflected name of brokerName is bname)");
    expect(ext.at("bname") == kBroker, "bname is written out", ext.count("bname") ? ext.at("bname") : "");

    RecallMessageRequestHeader decoded;
    decoded.fromExtFields(ext);
    expect(decoded.bname == kBroker && decoded.recallHandle == "djEg" && decoded.topic == kTopic &&
               decoded.producerGroup == "PG",
           "the recall request header round-trips consistently");

    RecallMessageResponseHeader resp;
    PropertyMap respExt;
    respExt["msgId"] = std::string(kUniqKey);
    resp.fromExtFields(respExt);
    expect(resp.msgId == kUniqKey, "the recall response header reads msgId");
    expect(resp.toExtFields().at("msgId") == kUniqKey, "the recall response header writes msgId");

    SendMessageResponseHeader send;
    PropertyMap sendExt;
    sendExt["msgId"] = std::string("0000000000000000");
    sendExt["recallHandle"] = std::string("djEg");
    send.fromExtFields(sendExt);
    expect(send.recallHandle == "djEg", "the SEND response header parses recallHandle");
    expect(send.toExtFields().at("recallHandle") == "djEg", "recallHandle encodes back into the wire format");
    SendMessageResponseHeader plain;
    plain.fromExtFields(PropertyMap{{"msgId", std::string("x")}});
    expect(!plain.recallHandle.has_value(), "an ordinary message has no recallHandle");
    expect(plain.toExtFields().count("recallHandle") == 0, "a missing value is not encoded into the payload");

    expect(RequestCode::RECALL_MESSAGE == 370, "request code 370");
}

// ---------------------------------------------------------------- producer-side local validation
void testProducerValidation() {
    DefaultMQProducer notStarted("PG_not_started");
    expect(catchMessage([&] { (void)notStarted.recallMessage(kTopic, "djEg"); }) ==
               "producer not started, call start() first",
           "recalling without start is rejected");

    // The instance is started but the address server is unreachable: local validation must finish before any network call.
    DefaultMQProducer p("PG_recall_unit");
    p.setNamesrvAddr("127.0.0.1:1");
    p.start();

    expect(catchMessage([&] { (void)p.recallMessage("%RETRY%PG_recall_unit", "djEg"); }) ==
               "topic is not supported",
           "%RETRY% topic is rejected locally (message checked verbatim)");
    expect(catchMessage([&] { (void)p.recallMessage("%DLQ%PG_recall_unit", "djEg"); }) ==
               "topic is not supported",
           "%DLQ% topic is rejected locally (message checked verbatim)");
    expect(catchMessage([&] { (void)p.recallMessage("bad topic!", "djEg"); }) !=
               "<no exception>",
           "an illegal topic name is rejected");

    const auto began = std::chrono::steady_clock::now();
    const std::string corrupt = catchMessage([&] { (void)p.recallMessage(kTopic, "not-a-handle"); });
    const auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - began)
                          .count();
    expect(corrupt == kInvalid, "an illegal handle is rejected locally", corrupt);
    expect(cost < 200, "an illegal handle returns immediately (no network was touched)", std::to_string(cost) + "ms");

    const std::string handle = buildRecallHandle(kTopic, kBroker, "1700000000000", kUniqKey);
    const std::string noRoute = catchMessage([&] { (void)p.recallMessage(kTopic, handle); });
    // Only the prefix is asserted: when the route fetch really fails, a "(route fetch failed: ...)" detail follows.
    expect(noRoute.rfind("Can not find Message Queue for topic: " + std::string(kTopic), 0) == 0,
           "with no route the prefixed exception is still thrown", noRoute);

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
