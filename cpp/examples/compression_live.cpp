// **Real-cluster + cross-client** verification tool for auto compression.
//
//
//
//
// Usage:
//   rmq_compression_live selftest <namesrv>
//   rmq_compression_live send     <namesrv> <topic> <group> <size> [codec]
//   rmq_compression_live recv     <namesrv> <topic> <group> <size>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/compression.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"

using namespace rocketmq;

namespace {

const char* kLine = "rocketmq-compress-interop-payload-line-0123456789\n";

std::string buildPayload(int size) {
    std::string s;
    s.reserve(static_cast<size_t>(size) + 64);
    while (static_cast<int>(s.size()) < size) s += kLine;
    s.resize(static_cast<size_t>(size));
    return s;
}

Bytes str2bytes(const std::string& s) { return Bytes(s.begin(), s.end()); }

// Algorithm-name parsing for the matrix: a misspelled codec exits right away, so it cannot look like an "interop failure".
int32_t parseCodec(const std::string& name, bool *ok) {
    *ok = true;
    if (name.empty() || name == "zlib") return CompressionType::ZLIB;
    if (name == "lz4") return CompressionType::LZ4;
    if (name == "zstd") return CompressionType::ZSTD;
    *ok = false;
    return CompressionType::ZLIB;
}

std::string codecName(int32_t codec) {
    if (codec == CompressionType::LZ4) return "lz4";
    if (codec == CompressionType::ZSTD) return "zstd";
    return "zlib";
}

// Consumes 1 message (FIRST_OFFSET), returns false on timeout
class FirstMsgListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                            ConsumeConcurrentlyContext&) override {
        for (const MessageExt& m : msgs) {
            std::lock_guard<std::mutex> lk(m_);
            if (!have_) {
                first_ = m;
                have_ = true;
            }
            ++count_;
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
    bool have() {
        std::lock_guard<std::mutex> lk(m_);
        return have_;
    }
    MessageExt first() {
        std::lock_guard<std::mutex> lk(m_);
        return first_;
    }
    int64_t count() {
        std::lock_guard<std::mutex> lk(m_);
        return count_;
    }

private:
    std::mutex m_;
    MessageExt first_;
    bool have_ = false;
    int64_t count_ = 0;
};

// Receives one message back; returns false on failure
bool recvOne(const std::string& namesrv, const std::string& topic, const std::string& group,
             int32_t timeoutSec, MessageExt& out) {
    auto listener = std::make_shared<FirstMsgListener>();
    DefaultMQPushConsumer cons(group);
    cons.setNamesrvAddr(namesrv);
    cons.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    cons.subscribe(topic, "*");
    cons.setMessageListener(listener);
    cons.setPullTimeoutMillis(3000);
    cons.setPullSuspendTimeoutMillis(1000);
    cons.start();
    for (int i = 0; i < timeoutSec * 4 && !listener->have(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    bool ok = listener->have();
    if (ok) out = listener->first();
    cons.shutdown();
    return ok;
}

int usage() {
    std::cerr << "usage: rmq_compression_live selftest <namesrv>\n"
                 "       rmq_compression_live send <namesrv> <topic> <group> <size> [codec]\n"
                 "       rmq_compression_live recv <namesrv> <topic> <group> <size>\n"
                 "  codec = zlib (default) | lz4 | zstd, affects only the sender; the receiver decompresses automatically from the sysFlag type bits\n";
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) return usage();
    const std::string mode = argv[1];
    const std::string namesrv = argv[2];

    // send/recv are endpoints for the cross-language matrix: exceptions must land as one greppable line
    // (`SEND_FAIL` / `RECV_FAIL`), otherwise failures like TLS/ACL rejection end in SIGABRT and
    // the matrix only sees exit code 134 with no readable cause.
    try {
        if (mode == "send") {
            if (argc < 6) return usage();
            const std::string topic = argv[3];
            const std::string group = argv[4];
            const int size = std::stoi(argv[5]);
            bool codecOk = false;
            const int32_t codec = parseCodec(argc >= 7 ? argv[6] : "", &codecOk);
            if (!codecOk) {
                std::cout << "SEND_FAIL unknown codec=" << argv[6] << std::endl;
                return usage();
            }
            const std::string payload = buildPayload(size);
            const Bytes body = str2bytes(payload);
            DefaultMQProducer prod(group);
            prod.setNamesrvAddr(namesrv);
            prod.setSendMsgTimeout(10000);
            prod.setCompressType(codec);
            prod.start();
            SendResult sr = prod.send(Message(topic, body));
            prod.shutdown();
            if (sr.sendStatus != SendStatus::SEND_OK) {
                std::cout << "SEND_FAIL status=" << sendStatusName(sr.sendStatus) << std::endl;
                return 1;
            }
            std::cout << "SEND_OK codec=" << codecName(codec)
                      << " len=" << body.size()
                      << " crc32=" << crc32(body)
                      << " msgId=" << sr.msgId << std::endl;
            return 0;
        }

        if (mode == "recv") {
            if (argc < 6) return usage();
            const std::string topic = argv[3];
            const std::string group = argv[4];
            const int size = std::stoi(argv[5]);
            const std::string payload = buildPayload(size);
            MessageExt m;
            if (!recvOne(namesrv, topic, group, 30, m)) {
                std::cout << "RECV_TIMEOUT" << std::endl;
                return 3;
            }
            const bool lenOk = static_cast<int>(m.body.size()) == size;
            const bool crcOk = !m.body.empty() && crc32(m.body) == crc32(str2bytes(payload));
            std::cout << "RECV_OK len=" << m.body.size()
                      << " crc32=" << (m.body.empty() ? 0u : crc32(m.body))
                      << " storeSize=" << m.storeSize
                      << " match=" << ((lenOk && crcOk) ? 1 : 0) << std::endl;
            return (lenOk && crcOk) ? 0 : 1;
        }
    } catch (const std::exception& e) {
        std::cout << (mode == "recv" ? "RECV_FAIL " : "SEND_FAIL ") << e.what() << std::endl;
        return 1;
    }

    if (mode != "selftest") return usage();

    // ---------------- selftest: self produce-and-consume plus compression reality checks ----------------
    int fail = 0;
    auto check = [&](const std::string& name, bool ok, const std::string& detail) {
        if (!ok) ++fail;
        std::cout << "[" << (ok ? "PASS" : "FAIL") << "] " << name << "  " << detail << std::endl;
    };

    const std::string stamp = std::to_string(UtilAll::currentTimeMillis());
    const std::string topic = "CompressLiveCpp_" + stamp;
    const std::string group = "CompressLiveCppGroup_" + stamp;
    const int size = 8192;  // far above compressMsgBodyOverHowmuch(4096)
    const std::string payload = buildPayload(size);
    const Bytes body = str2bytes(payload);
    const uint32_t payloadCrc = crc32(body);
    std::cout << "payload len=" << size << " crc32=" << payloadCrc
              << " (deterministic payload)" << std::endl;

    // 1) send it out
    std::string msgId;
    {
        DefaultMQProducer prod("CompressLiveCppProducer_" + stamp);
        prod.setNamesrvAddr(namesrv);
        prod.setSendMsgTimeout(10000);
        prod.start();
        SendResult sr = prod.send(Message(topic, body));
        msgId = sr.msgId;
        check("send a " + std::to_string(size) + "B message (triggers auto compression)",
              sr.sendStatus == SendStatus::SEND_OK, "msgId=" + sr.msgId);
        prod.shutdown();
    }
    if (fail) return 1;

    // 2) receive it back; the body must match the original byte for byte (proves the compress-store-decompress loop)
    MessageExt m;
    if (!recvOne(namesrv, topic, group, 30, m)) {
        check("consume the compressed message back", false, "30 s timeout");
        return 1;
    }
    check("consume the compressed message back", true,
          "len=" + std::to_string(m.body.size()) + " storeSize=" + std::to_string(m.storeSize));
    check("decompressed body matches the original (len + CRC32)",
          m.body.size() == body.size() && crc32(m.body) == payloadCrc,
          "expect len=" + std::to_string(body.size()) + " crc=" + std::to_string(payloadCrc)
          + " actual len=" + std::to_string(m.body.size()) + " crc=" + std::to_string(crc32(m.body)));

    // 3) prove what the broker stores **really is the compressed body**: storeSize should be far below the original length.
    //    Only assert "clearly smaller", not a concrete ratio -- the compression ratio depends on the zlib version.
    check("broker-side storage is the compressed body (storeSize far below the original)",
          m.storeSize > 0 && m.storeSize < size / 2,
          "storeSize=" + std::to_string(m.storeSize) + " original=" + std::to_string(size)
          + " ratio=" + std::to_string(size / (m.storeSize > 0 ? m.storeSize : 1)) + ":1");

    check("COMPRESSED_FLAG cleared after decompression",
          !MessageSysFlag::isCompressed(m.sysFlag),
          "sysFlag=" + std::to_string(m.sysFlag));

    // 5) a small message **must not** be compressed (threshold semantics)
    {
        const std::string smallTopic = "CompressLiveSmall_" + stamp;
        const std::string smallGroup = "CompressLiveSmallGroup_" + stamp;
        const std::string small = "tiny-payload-under-threshold";
        DefaultMQProducer prod("CompressLiveSmallProducer_" + stamp);
        prod.setNamesrvAddr(namesrv);
        prod.start();
        prod.send(Message(smallTopic, str2bytes(small)));
        prod.shutdown();
        MessageExt sm;
        if (recvOne(namesrv, smallTopic, smallGroup, 20, sm)) {
            check("a message below the threshold (4096) is not compressed",
                  sm.body.size() == small.size() && !MessageSysFlag::isCompressed(sm.sysFlag),
                  "len=" + std::to_string(sm.body.size())
                  + " storeSize=" + std::to_string(sm.storeSize));
        } else {
            check("a message below the threshold (4096) is not compressed", false, "not consumed within 20 s");
        }
    }

    // 6) replay the encode/decode path of a wire message carrying the compression flag and confirm the flag semantics close the loop:
    //    a MessageExt with COMPRESSED_FLAG|ZLIB, encoded with needCompress=true -> the stored body shrinks;
    //    decoded with decompression -> the original is restored; decoded without decompression -> the bytes are the compressed ones.
    //    This is exactly the same branch the broker write path and the consumer read path take.
    {
        MessageExt probe;
        probe.topic = topic;
        probe.body = body;
        probe.sysFlag = MessageSysFlag::COMPRESSED_FLAG
            | CompressionType::getCompressionFlag(CompressionType::ZLIB);

        Bytes storedCompressed = encodeMessageExt(probe, /*needCompress=*/true);
        MessageExt raw;
        bool rawOk = decodeMessage(storedCompressed, raw, /*readBody=*/true,
                                   /*decompressBody=*/false);
        MessageExt restored;
        bool restoredOk = decodeMessage(storedCompressed, restored, /*readBody=*/true,
                                        /*decompressBody=*/true);

        check("encoding with the compression flag yields a smaller stored body",
              storedCompressed.size() < body.size() / 2,
              "stored=" + std::to_string(storedCompressed.size())
              + " original=" + std::to_string(body.size()));
        check("decoding without decompression yields the compressed bytes, decoding with decompression restores the original",
              rawOk && restoredOk && raw.body.size() < body.size()
              && restored.body.size() == body.size() && crc32(restored.body) == payloadCrc
              && !MessageSysFlag::isCompressed(restored.sysFlag),
              "rawLen=" + std::to_string(raw.body.size()) + " restoredLen="
              + std::to_string(restored.body.size()));
    }

    struct Backend {
        const char* name;
        int32_t type;
        bool available;
    };
    const Backend backends[] = {
        {"lz4", CompressionType::LZ4, hasLz4Support()},
        {"zstd", CompressionType::ZSTD, hasZstdSupport()},
    };
    for (const Backend& b : backends) {
        const std::string tag = std::string("backend ") + b.name + " live round trip";
        if (!b.available) {
            std::cout << "[SKIP] " << tag << "  not compiled into this build: lib" << b.name << std::endl;
            continue;
        }
        const std::string suffix = std::string(b.name) + "_" + stamp;
        const std::string t = "CompressLiveCpp_" + suffix;
        const std::string g = "CompressLiveCppGroup_" + suffix;
        DefaultMQProducer prod("CompressLiveCppProducer_" + suffix);
        prod.setNamesrvAddr(namesrv);
        prod.setSendMsgTimeout(10000);
        prod.setCompressType(b.type);
        prod.start();
        SendResult sr = prod.send(Message(t, body));
        prod.shutdown();
        if (sr.sendStatus != SendStatus::SEND_OK) {
            check(tag, false, "send status=" + std::string(sendStatusName(sr.sendStatus)));
            continue;
        }
        MessageExt cme;
        if (!recvOne(namesrv, t, g, 30, cme)) {
            check(tag, false, "not consumed within 30 s");
            continue;
        }
        check(tag,
              cme.body.size() == body.size() && crc32(cme.body) == payloadCrc
                  && cme.storeSize > 0 && cme.storeSize < size / 2
                  && !MessageSysFlag::isCompressed(cme.sysFlag),
              "len=" + std::to_string(cme.body.size())
                  + " storeSize=" + std::to_string(cme.storeSize)
                  + " sysFlag=" + std::to_string(cme.sysFlag));
    }

    std::cout << "\n==== compression live summary ====" << std::endl;
    std::cout << (fail == 0 ? "ALL PASS" : "FAILED") << " (fail=" << fail << ")" << std::endl;
    return fail == 0 ? 0 : 1;}
