// 编解码往返单测：JSON / RocketMQ 二进制 / RemotingCommand / 消息格式 / 工具函数。
//
// 目标：在不依赖真实集群的前提下，证明 C++ 协议层与 Python 参考实现行为一致（字节级往返正确）。
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "rocketmq/common/message.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/serialize.h"

using namespace rocketmq;

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg)                                                       \
    do {                                                                       \
        if (cond) {                                                            \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::cout << "[FAIL] " << (msg) << "\n";                           \
        }                                                                      \
    } while (0)

static void testJson() {
    JsonValue v;
    CHECK(jsonParse("{\"a\":1,\"b\":\"x\",\"c\":[1,2,3],\"d\":true,\"e\":null}", v),
          "json parse basic");
    CHECK(v.get("a").intValue() == 1, "json int value");
    CHECK(v.get("b").stringValue() == "x", "json string value");
    CHECK(v.get("c").size() == 3 && v.get("c").at(2).intValue() == 3, "json array");
    CHECK(v.get("d").boolValue() == true, "json bool");
    CHECK(v.get("e").isNull(), "json null");

    // FastJSON 无引号数字键兼容
    JsonValue v2;
    CHECK(jsonParse("{\"brokerAddrs\":{0:\"127.0.0.1:10911\"}}", v2),
          "json tolerant numeric key parse");
    const JsonValue* ba = v2.find("brokerAddrs");
    CHECK(ba && ba->contains("0") && ba->get("0").stringValue() == "127.0.0.1:10911",
          "json tolerant numeric key value");

    // 转义与 UTF-8
    JsonValue v3;
    CHECK(jsonParse("{\"s\":\"a\\nb\\u4e2d\"}", v3), "json escape parse");
    CHECK(v3.get("s").stringValue() == std::string("a\nb\u4e2d"), "json escape round value");

    // dump 后再解析应等价
    JsonValue v4;
    CHECK(jsonParse(v.dump(), v4), "json dump->parse");
    CHECK(v4.get("a").intValue() == 1 && v4.get("c").size() == 3, "json dump->parse equiv");
}

static void testRocketMQSerialize() {
    // writeDecimalLong: [4B len][ascii]
    Bytes buf;
    RocketMQSerializable::writeDecimalLong(buf, -12345);
    int32_t len = ByteReader::getInt32At(buf, 0);
    CHECK(len == 6, "writeDecimalLong length field");
    CHECK(buf.substr(4) == std::string("-12345"), "writeDecimalLong digits");

    // writeStr 短/长长度
    Bytes s;
    RocketMQSerializable::writeStr(s, true, "abc");
    CHECK(s.size() == 2 + 3 && static_cast<unsigned char>(s[1]) == 3, "writeStr short length");
    std::string out;
    size_t no = 0;
    CHECK(RocketMQSerializable::readStr(s, 0, true, out, no) && out == "abc" && no == s.size(),
          "readStr short round");
}

static void testRemotingCommandJson() {
    auto hdr = std::make_shared<SendMessageRequestHeader>();
    hdr->producerGroup = "pg";
    hdr->topic = "T_TEST";
    hdr->queueId = 2;
    hdr->bornTimestamp = 1699999999000LL;

    RemotingCommand cmd = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE, hdr);
    cmd.remark = "hello";
    cmd.hasRemark = true;
    cmd.body = "payload";

    Bytes wire = cmd.encode();
    // totalLength = 4(headerLen 字段) + header + body
    int32_t total = ByteReader::getInt32At(wire, 0);
    CHECK(static_cast<size_t>(total) + 4 == wire.size(), "encode totalLength framing");

    RemotingCommand dec = RemotingCommand::decode(wire);
    CHECK(dec.code == RequestCode::SEND_MESSAGE, "decode code");
    CHECK(dec.remark == "hello", "decode remark");
    CHECK(dec.body == std::string("payload"), "decode body");
    CHECK(dec.getExtField("topic") == "T_TEST", "decode extField topic");
    CHECK(dec.getExtField("queueId") == "2", "decode extField queueId");
    CHECK(dec.getExtField("bornTimestamp") == "1699999999000", "decode extField bornTimestamp");
    CHECK(dec.serializeTypeCurrentRPC == SerializeType::JSON, "decode serializeType JSON");

    // 从 extFields 还原自定义头
    SendMessageRequestHeader back;
    dec.decodeCommandCustomHeader(back);
    CHECK(back.topic.has_value() && *back.topic == "T_TEST", "decodeCommandCustomHeader topic");
    CHECK(back.queueId.has_value() && *back.queueId == 2, "decodeCommandCustomHeader queueId");
}

static void testRemotingCommandRocketMq() {
    auto hdr = std::make_shared<PullMessageRequestHeader>();
    hdr->consumerGroup = "cg";
    hdr->topic = "T_PULL";
    hdr->queueId = 1;
    hdr->queueOffset = 1234567890123LL;

    RemotingCommand cmd = RemotingCommand::createRequestCommand(RequestCode::PULL_MESSAGE, hdr);
    cmd.serializeTypeCurrentRPC = SerializeType::ROCKETMQ;
    cmd.remark = "rk";
    cmd.hasRemark = true;
    cmd.body = "b1";

    Bytes wire = cmd.encode();
    RemotingCommand dec = RemotingCommand::decode(wire);
    CHECK(dec.serializeTypeCurrentRPC == SerializeType::ROCKETMQ, "rocketmq header serializeType");
    CHECK(dec.code == RequestCode::PULL_MESSAGE, "rocketmq decode code");
    CHECK(dec.remark == "rk", "rocketmq decode remark");
    CHECK(dec.body == std::string("b1"), "rocketmq decode body");
    CHECK(dec.getExtField("queueOffset") == "1234567890123", "rocketmq decode queueOffset");
    CHECK(dec.getExtField("consumerGroup") == "cg", "rocketmq decode consumerGroup");
}

static void testHeaderV1V2() {
    SendMessageRequestHeader v1;
    v1.producerGroup = "pg";
    v1.topic = "T";
    v1.queueId = 3;
    v1.bornTimestamp = 42;
    v1.batch = false;

    SendMessageRequestHeaderV2 v2 = SendMessageRequestHeaderV2::fromV1(v1);
    PropertyMap ext = v2.toExtFields();
    CHECK(ext["a"] == "pg" && ext["b"] == "T", "V2 short names a/b");
    CHECK(ext["e"] == "3" && ext["g"] == "42", "V2 short names e/g");
    CHECK(ext["m"] == "false", "V2 short name m bool");

    SendMessageRequestHeader back = v2.toV1();
    CHECK(back.producerGroup.has_value() && *back.producerGroup == "pg", "V2->V1 producerGroup");
    CHECK(back.queueId.has_value() && *back.queueId == 3, "V2->V1 queueId");
    CHECK(back.batch.has_value() && *back.batch == false, "V2->V1 batch");
}

static void testMessageCodec() {
    MessageExt m;
    m.topic = "T_MSG";
    m.body = "hello-body";
    m.flag = 7;
    m.queueId = 2;
    m.queueOffset = 100;
    m.sysFlag = 0;
    m.bornTimestamp = 1700000000000LL;
    m.storeTimestamp = 1700000000001LL;
    m.bornHost = "127.0.0.1";
    m.bornHostPort = 10911;
    m.storeHost = "127.0.0.1";
    m.storeHostPort = 10911;
    m.commitLogOffset = 4096;
    m.reconsumeTimes = 0;
    m.setTags("TagA");
    m.setKeys("K1");
    m.setUserProperty("city", "Hangzhou");

    Bytes raw = encodeMessageExt(m, false);
    CHECK(raw.size() >= 4 && ByteReader::getInt32At(raw, 0) == static_cast<int32_t>(raw.size()),
          "encodeMessageExt totalSize self-consistent");
    CHECK(ByteReader::getInt32At(raw, MESSAGE_MAGIC_CODE_POSITION) == MESSAGE_MAGIC_CODE,
          "encodeMessageExt magic v1");

    MessageExt d;
    CHECK(decodeMessage(raw, d, true), "decodeMessage ok");
    CHECK(d.topic == "T_MSG", "decode topic");
    CHECK(d.body == std::string("hello-body"), "decode body");
    CHECK(d.queueId == 2 && d.queueOffset == 100, "decode queueId/offset");
    CHECK(d.flag == 7, "decode flag");
    CHECK(d.getTags() == "TagA", "decode tags");
    CHECK(d.getKeys() == "K1", "decode keys");
    CHECK(d.getUserProperty("city") == "Hangzhou", "decode user property");
    CHECK(!d.msgId.empty(), "decode msgId non-empty");

    // 多消息流
    Bytes stream = raw + encodeMessageExt(m, false);
    std::vector<MessageExt> list = decodeMessages(stream, true);
    CHECK(list.size() == 2, "decodeMessages count");
}

static void testBatchCodec() {
    Message a("T_B", "aaa");
    Message b("T_B", "bbb");
    b.setTags("TagB");

    Bytes body = encodeMessages({a, b});
    CHECK(countInnerMsgNum(body) == 2, "countInnerMsgNum");
    std::vector<Message> back = decodeBatchMessages(body);
    CHECK(back.size() == 2, "decodeBatchMessages count");
    CHECK(back[0].body == std::string("aaa"), "batch msg0 body");
    CHECK(back[1].getTags() == "TagB", "batch msg1 tags");

    MessageBatch batch = MessageBatch::generateFromList({a, b});
    CHECK(batch.size() == 2 && batch.topic == "T_B", "MessageBatch generateFromList");
    CHECK(batch.body == body, "MessageBatch encode equals encodeMessages");
}

static void testUtilAndHash() {
    // MessageQueue.hashCode 与 Java 语义一致（topic=A, broker=B, qid=0 -> 93282）
    MessageQueue mq("A", "B", 0);
    CHECK(mq.hashCode() == 93282, "MessageQueue hashCode java-semantics");

    // 十六进制（大写）
    Bytes bs;
    bs.push_back(static_cast<char>(0x00));
    bs.push_back(static_cast<char>(0x1F));
    bs.push_back(static_cast<char>(0xFF));
    CHECK(UtilAll::bytes2String(bs) == "001FFF", "bytes2String uppercase");
    CHECK(UtilAll::string2Bytes("001FFF") == bs, "string2Bytes round");
    CHECK(UtilAll::string2Bytes("ZZ").empty(), "string2Bytes invalid -> empty");

    // 标准 CRC32("123456789") = 0xCBF43926
    CHECK(UtilAll::crc32("123456789") == 0xCBF43926u, "crc32 known vector");
    CHECK(crc32("123456789") == 0xCBF43926u, "message_decoder::crc32 delegates");

    // msgId 往返
    Bytes addr = ipAndPortToBytes("127.0.0.1", 10911, false);
    CHECK(addr.size() == 8, "ipAndPortToBytes v4 length");
    std::string msgId = createMessageId(addr, 123456789LL);
    std::string ip;
    int32_t port = 0;
    int64_t off = 0;
    CHECK(decodeMessageId(msgId, ip, port, off), "decodeMessageId ok");
    CHECK(ip == "127.0.0.1" && port == 10911 && off == 123456789LL, "decodeMessageId values");
}

int main() {
    testJson();
    testRocketMQSerialize();
    testRemotingCommandJson();
    testRemotingCommandRocketMq();
    testHeaderV1V2();
    testMessageCodec();
    testBatchCodec();
    testUtilAndHash();

    std::cout << "\n===== codec test summary =====\n";
    std::cout << "  PASS=" << g_pass << " FAIL=" << g_fail << "\n";
    if (g_fail == 0) {
        std::cout << "  result: all " << g_pass << " checks passed\n";
        return 0;
    }
    std::cout << "  result: " << g_fail << " checks failed\n";
    return 1;
}
