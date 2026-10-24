// Codec round-trip unit tests: JSON / RocketMQ binary / RemotingCommand / message format / utilities.
//
// Goal: prove this port's protocol layer round-trips byte-for-byte without depending on a real cluster.
#include <cstdint>
#include <exception>
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

    // FastJSON tolerance for unquoted numeric keys
    JsonValue v2;
    CHECK(jsonParse("{\"brokerAddrs\":{0:\"127.0.0.1:10911\"}}", v2),
          "json tolerant numeric key parse");
    const JsonValue* ba = v2.find("brokerAddrs");
    CHECK(ba && ba->contains("0") && ba->get("0").stringValue() == "127.0.0.1:10911",
          "json tolerant numeric key value");

    // Escapes and UTF-8
    JsonValue v3;
    CHECK(jsonParse("{\"s\":\"a\\nb\\u4e2d\"}", v3), "json escape parse");
    CHECK(v3.get("s").stringValue() == std::string("a\nb\u4e2d"), "json escape round value");

    // Re-parsing the dumped text must be equivalent
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

    // writeStr short/long length prefix
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
    // totalLength = 4 (headerLen field) + header + body
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

    // Restore the custom header from extFields
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

    // A stream carrying several messages
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

static void testBatchDefaultsWaitStoreMsgOkToTrue() {
    Message a("T_W", "a");
    Message b("T_W", "b");
    CHECK(a.properties.count(MessageConst::PROPERTY_WAIT_STORE_MSG_OK) == 0, "WAIT not preset");
    CHECK(a.isWaitStoreMsgOk(), "default waitStoreMsgOK is true");

    MessageBatch batch = MessageBatch::generateFromList({a, b});
    CHECK(batch.getProperty(MessageConst::PROPERTY_WAIT_STORE_MSG_OK) == "true",
          "batch defaults WAIT=true");

    Message c("T_W", "c");
    Message d("T_W", "d");
    c.setWaitStoreMsgOk(false);
    d.setWaitStoreMsgOk(false);
    MessageBatch off = MessageBatch::generateFromList({c, d});
    CHECK(off.getProperty(MessageConst::PROPERTY_WAIT_STORE_MSG_OK) == "false",
          "batch keeps explicit WAIT=false");
}

static std::string batchRejection(const std::vector<Message>& msgs) {
    try {
        MessageBatch::generateFromList(msgs);
        return "";
    } catch (const std::exception& e) {
        return e.what();
    }
}

// The schedule guard covers the delay level AND the three timer spellings, on any member: a batch
// leaves as one message with one property set, so a member's schedule would land on the envelope
// carrying the others. Each rejection is paired with a 0 under the same name, which means "no
// schedule" and must still pack — without it a presence-only check would pass the same asserts.
static void testBatchRejectsEveryScheduleSpelling() {
    const char* names[] = {MessageConst::PROPERTY_DELAY_TIME_LEVEL,
                           MessageConst::PROPERTY_TIMER_DELAY_SEC,
                           MessageConst::PROPERTY_TIMER_DELAY_MS,
                           MessageConst::PROPERTY_TIMER_DELIVER_MS};
    for (const char* name : names) {
        Message scheduled("T_S", "a");
        scheduled.properties[name] = "3";
        CHECK(batchRejection({scheduled}).find("Delayed messages are not supported for batching") !=
                  std::string::npos,
              std::string(name) + "=3 rejects the batch");

        Message tail("T_S", "b");
        tail.properties[name] = "3";
        CHECK(batchRejection({Message("T_S", "a"), tail}).find(
                  "Delayed messages are not supported for batching") != std::string::npos,
              std::string(name) + "=3 on a non-first member rejects the batch");

        Message zero("T_S", "a");
        zero.properties[name] = "0";
        CHECK(batchRejection({zero}).empty(), std::string(name) + "=0 leaves the batch alone");
    }

    // The setters the public API exposes reach the same four properties.
    Message viaLevel("T_S", "a");
    viaLevel.setDelayTimeLevel(2);
    CHECK(!batchRejection({viaLevel}).empty(), "setDelayTimeLevel rejects");
    Message viaSec("T_S", "a");
    viaSec.setDelayTimeSec(30);
    CHECK(!batchRejection({viaSec}).empty(), "setDelayTimeSec rejects");
    Message viaMs("T_S", "a");
    viaMs.setDelayTimeMs(30000);
    CHECK(!batchRejection({viaMs}).empty(), "setDelayTimeMs rejects");
    Message viaDeliver("T_S", "a");
    viaDeliver.setDeliverTimeMs(1700000000000LL);
    CHECK(!batchRejection({viaDeliver}).empty(), "setDeliverTimeMs rejects");

    // A plain batch is the control the guard must never touch.
    CHECK(batchRejection({Message("T_S", "a"), Message("T_S", "b")}).empty(),
          "a plain batch is still accepted");
}

static void testWaitStoreMsgOkParseBooleanContract() {
    Message msg("T_W", "x");
    CHECK(msg.isWaitStoreMsgOk(), "absent -> true");
    msg.setWaitStoreMsgOk(true);
    CHECK(msg.isWaitStoreMsgOk(), "explicit true");
    msg.setWaitStoreMsgOk(false);
    CHECK(!msg.isWaitStoreMsgOk(), "explicit false");
    msg.properties[MessageConst::PROPERTY_WAIT_STORE_MSG_OK] = "TRUE";
    CHECK(msg.isWaitStoreMsgOk(), "case-insensitive true");
    // Any other value is false (not "anything except false means true")
    for (const char* bad : {"1", "", "yes", "tru"}) {
        msg.properties[MessageConst::PROPERTY_WAIT_STORE_MSG_OK] = bad;
        CHECK(!msg.isWaitStoreMsgOk(), std::string("non-canonical value is false: ") + bad);
    }
}

static void testUtilAndHash() {
    MessageQueue mq("A", "B", 0);
    CHECK(mq.hashCode() == 93282, "MessageQueue hashCode protocol semantics");

    // Hexadecimal, uppercase
    Bytes bs;
    bs.push_back(static_cast<char>(0x00));
    bs.push_back(static_cast<char>(0x1F));
    bs.push_back(static_cast<char>(0xFF));
    CHECK(UtilAll::bytes2String(bs) == "001FFF", "bytes2String uppercase");
    CHECK(UtilAll::string2Bytes("001FFF") == bs, "string2Bytes round");
    CHECK(UtilAll::string2Bytes("ZZ").empty(), "string2Bytes invalid -> empty");

    // The stored BODYCRC shape: CRC-32("123456789") = 0xCBF43926, sign bit dropped -> 0x4BF43926
    CHECK(UtilAll::crc32("123456789") == 0x4BF43926u, "crc32 returns the stored BODYCRC field value");
    CHECK(crc32("123456789") == 0x4BF43926u, "message_decoder::crc32 delegates");

    // msgId round trip
    Bytes addr = ipAndPortToBytes("127.0.0.1", 10911, false);
    CHECK(addr.size() == 8, "ipAndPortToBytes v4 length");
    std::string msgId = createMessageId(addr, 123456789LL);
    std::string ip;
    int32_t port = 0;
    int64_t off = 0;
    CHECK(decodeMessageId(msgId, ip, port, off), "decodeMessageId ok");
    CHECK(ip == "127.0.0.1" && port == 10911 && off == 123456789LL, "decodeMessageId values");
}

// Local unmasked CRC-32, computed from scratch here so the raw value stays distinguishable from the
// value the store puts into BODYCRC. Bit-by-bit form of the same polynomial, no lookup table.
static uint32_t unmaskedCrc32(const Bytes& data) {
    uint32_t crc = 0xFFFFFFFFu;
    for (unsigned char b : data) {
        crc ^= static_cast<uint32_t>(b);
        for (int i = 0; i < 8; ++i) {
            crc = (crc & 1u) != 0 ? (crc >> 1) ^ 0xEDB88320u : crc >> 1;
        }
    }
    return crc ^ 0xFFFFFFFFu;
}

static void testBodyCrcFieldShape() {
    // Two numbers pinned side by side so they cannot be confused: the helper returns the masked one.
    CHECK(unmaskedCrc32("a") == 0xE8B7BE43u, "unmasked crc32 of a");
    CHECK((unmaskedCrc32("a") & 0x80000000u) != 0, "unmasked crc32 of a has the sign bit set");
    CHECK(UtilAll::crc32("a") == 0x68B7BE43u, "stored BODYCRC of a drops the sign bit");

    // Below 2^31 the mask changes nothing, so that value is also a valid stored BODYCRC.
    CHECK(unmaskedCrc32("abc") == 0x352441C2u, "unmasked crc32 of abc");
    CHECK(UtilAll::crc32("abc") == 0x352441C2u, "stored BODYCRC of abc is unchanged");

    // The helper never returns a value with the sign bit set -- it is the field, not the raw CRC.
    for (const char* s : {"", "a", "abc", "123456789", "hello-body", "T_MSG"}) {
        CHECK((UtilAll::crc32(s) & 0x80000000u) == 0,
              std::string("returned BODYCRC always fits in 31 bits: ") + s);
    }
    CHECK(crc32("a") == UtilAll::crc32("a"), "message_decoder::crc32 returns the same field value");
}

static void testDecodeMessageCheckCrcContract() {
    // A body whose unmasked CRC has the sign bit set: only the masked value is the stored BODYCRC.
    Bytes body;
    uint32_t raw = 0;
    for (int i = 0; i < 200; ++i) {
        const Bytes candidate = "crc-highbit-" + std::to_string(i);
        const uint32_t c = unmaskedCrc32(candidate);
        if ((c & 0x80000000u) != 0) {
            body = candidate;
            raw = c;
            break;
        }
    }
    CHECK(!body.empty() && (raw & 0x80000000u) != 0, "found a body whose raw crc has the sign bit");

    const uint32_t stored = raw & 0x7FFFFFFFu;
    CHECK(stored != raw, "masked field differs from the raw crc for that body");

    MessageExt m;
    m.topic = "T_CRCCHECK";
    m.body = body;
    m.bodyCrc = stored;
    m.queueId = 0;
    m.flag = 0;
    m.sysFlag = 0;
    m.bornHost = "127.0.0.1";
    m.bornHostPort = 10911;
    m.storeHost = "127.0.0.1";
    m.storeHostPort = 10911;
    m.commitLogOffset = 8192;
    const Bytes frame = encodeMessageExt(m, false);

    MessageExt accepted;
    CHECK(decodeMessage(frame, accepted, /*readBody=*/true, /*decompressBody=*/false,
                        /*isClient=*/true, /*checkCrc=*/true),
          "checkCrc accepts the frame whose BODYCRC is the masked crc");
    CHECK(accepted.body == body, "the accepted frame keeps the body bytes");
    CHECK(accepted.bodyCrc == stored, "decoded BODYCRC equals the masked field");

    // Without the opt-in check the same frame decodes, so the assertion above is about the comparison.
    MessageExt unchecked;
    CHECK(decodeMessage(frame, unchecked, true, false, true, false), "checkCrc off still decodes");

    // A deliberately wrong BODYCRC must be rejected: flip one bit of the field.
    MessageExt badField = m;
    badField.bodyCrc = stored ^ 1u;
    const Bytes badFieldFrame = encodeMessageExt(badField, false);
    MessageExt rejectedField;
    CHECK(!decodeMessage(badFieldFrame, rejectedField, true, false, true, true),
          "checkCrc rejects a frame with a wrong BODYCRC field");

    // Same field, tampered body: also rejected.
    MessageExt badBody = m;
    badBody.body = body + "x";
    const Bytes badBodyFrame = encodeMessageExt(badBody, false);
    MessageExt rejectedBody;
    CHECK(!decodeMessage(badBodyFrame, rejectedBody, true, false, true, true),
          "checkCrc rejects a body that does not match the BODYCRC field");
}

int main() {
    testJson();
    testRocketMQSerialize();
    testRemotingCommandJson();
    testRemotingCommandRocketMq();
    testHeaderV1V2();
    testMessageCodec();
    testBatchCodec();
    testBatchDefaultsWaitStoreMsgOkToTrue();
    testBatchRejectsEveryScheduleSpelling();
    testWaitStoreMsgOkParseBooleanContract();
    testUtilAndHash();
    testBodyCrcFieldShape();
    testDecodeMessageCheckCrcContract();

    std::cout << "\n===== codec test summary =====\n";
    std::cout << "  PASS=" << g_pass << " FAIL=" << g_fail << "\n";
    if (g_fail == 0) {
        std::cout << "  result: all " << g_pass << " checks passed\n";
        return 0;
    }
    std::cout << "  result: " << g_fail << " checks failed\n";
    return 1;
}
