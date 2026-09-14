// 压缩/解压单测：压缩类型解析、zlib 与外部实现的互通、17 段报文里
// "解压后清 COMPRESSED_FLAG" 的语义、以及老版本 **类型位为 0** 的兼容路径。
//
// 关键点：本文件里有一条用**硬编码 zlib 字节**（由 Python zlib.compress 生成）的用例，
// 用来证明我们解析的是真正的 zlib 流格式（RFC1950），而不是"自己压自己解"的自嗨。
#include <cstdint>
#include <iostream>
#include <string>

#include "rocketmq/common/compression.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"

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

namespace {

// 与 Python 侧生成 zlib 真值时使用的同一段明文
std::string repeatedPayload() {
    std::string s;
    for (int i = 0; i < 40; ++i) {
        s += "rocketmq-compressed-payload-";
    }
    return s;
}

// Python: zlib.compress(b"rocketmq-compressed-payload-"*40, 5)  —— 1120B -> 47B
const char* kZlibHex =
    "785e2bca4fce4e2dc92dd44dcecf2d284a2d2e4e4dd12d48acccc94f4cd12d1a951b951b951b95a3400e"
    "0058e0b9f0";

MessageExt makeMessageExt(const std::string& body, int32_t sysFlag) {
    MessageExt m;
    m.topic = "T_COMPRESS";
    m.body = body;
    m.hasBody = true;
    m.sysFlag = sysFlag;
    m.queueId = 3;
    m.flag = 0;
    m.bornTimestamp = 1700000000000LL;
    m.bornHost = "127.0.0.1";
    m.bornHostPort = 10911;
    m.storeTimestamp = 1700000000001LL;
    m.storeHost = "127.0.0.1";
    m.storeHostPort = 10911;
    m.reconsumeTimes = 0;
    m.preparedTransactionOffset = 0;
    m.queueOffset = 7;
    return m;
}

}  // namespace

// ---------------------------------------------------------------- 类型解析
static void testTypeResolution() {
    CHECK(hasZlibSupport(), "zlib support compiled in");

    // Java CompressionType.findByValue 的向后兼容映射：0 与 3 都是 ZLIB
    CHECK(CompressionType::findByValue(0) == CompressionType::ZLIB,
          "type bits 0 -> ZLIB (legacy compatibility)");
    CHECK(CompressionType::findByValue(3) == CompressionType::ZLIB, "type bits 3 -> ZLIB");
    CHECK(CompressionType::findByValue(1) == CompressionType::LZ4, "type bits 1 -> LZ4");
    CHECK(CompressionType::findByValue(2) == CompressionType::ZSTD, "type bits 2 -> ZSTD");

    bool threw = false;
    try {
        CompressionType::findByValue(7);
    } catch (const std::exception&) {
        threw = true;
    }
    CHECK(threw, "unknown type value throws (no silent passthrough)");

    CHECK(CompressionType::getCompressionFlag(CompressionType::ZLIB) == (0x3 << 8),
          "zlib compression flag == 0x300");
    CHECK(CompressionType::getCompressionFlag(CompressionType::LZ4) == (0x1 << 8),
          "lz4 compression flag == 0x100");
    CHECK(CompressionType::getCompressionFlag(CompressionType::ZSTD) == (0x2 << 8),
          "zstd compression flag == 0x200");
}

// ---------------------------------------------------------------- zlib 往返 + 外部互通
static void testZlibRoundTrip() {
    const std::string payload = repeatedPayload();

    Bytes compressed = CompressorFactory::compress(payload, CompressionType::ZLIB, 5);
    CHECK(!compressed.empty() && compressed.size() < payload.size(),
          "zlib compress shrinks a repetitive payload");
    // zlib 流格式头：0x78（CMF，deflate + 32K window）
    CHECK(static_cast<unsigned char>(compressed[0]) == 0x78, "zlib stream has 0x78 CMF byte");
    CHECK(CompressorFactory::decompress(compressed, CompressionType::ZLIB) == payload,
          "zlib round-trip");

    // 类型位 0 也必须按 ZLIB 解（这是老版本客户端产的压缩消息）
    CHECK(CompressorFactory::decompress(compressed, 0) == payload,
          "decompress with type 0 falls back to ZLIB");

    // 外部真值：Python 生成的 zlib 字节必须能被解开
    Bytes external = UtilAll::string2Bytes(kZlibHex);
    CHECK(!external.empty(), "external zlib fixture decoded from hex");
    CHECK(external.size() == 47, "external zlib fixture length == 47");
    Bytes restored = CompressorFactory::decompress(external, CompressionType::ZLIB);
    CHECK(restored == payload, "decompress external (Python-produced) zlib bytes");
}

// ---------------------------------------------------------------- 17 段报文往返
static void testEncodeDecodeCompressed() {
    const std::string payload = repeatedPayload();
    const int32_t compressedFlag = MessageSysFlag::COMPRESSED_FLAG | (0x3 << 8);

    MessageExt src = makeMessageExt(payload, compressedFlag);
    Bytes raw = encodeMessageExt(src, /*needCompress=*/true);

    // 报文里存的应是压缩后的 body：总长明显小于原始 body 长度
    CHECK(raw.size() < payload.size(), "encoded frame smaller than raw payload");

    // 解压路径：body 还原 + 清 COMPRESSED_FLAG（保留类型位）
    MessageExt out;
    CHECK(decodeMessage(raw, out, true, true), "decode compressed message");
    CHECK(out.body == payload, "compressed body restored");
    CHECK((out.sysFlag & MessageSysFlag::COMPRESSED_FLAG) == 0,
          "COMPRESSED_FLAG cleared after decompress");
    CHECK(MessageSysFlag::getCompressionType(out.sysFlag) == 3,
          "compression type bits preserved after clear");

    // 不解压路径：body 仍是压缩字节，标志位保留
    MessageExt kept;
    CHECK(decodeMessage(raw, kept, true, false), "decode without decompress");
    CHECK(kept.body != payload, "body stays compressed when decompressBody=false");
    CHECK((kept.sysFlag & MessageSysFlag::COMPRESSED_FLAG) != 0,
          "COMPRESSED_FLAG kept when decompressBody=false");
    CHECK(CompressorFactory::decompress(kept.body, 3) == payload,
          "manually decompressing the kept body works");

    // 类型位为 0（老版本）也要能解：先伪造一个类型位为 0 的 sysFlag 再编码
    MessageExt legacy = makeMessageExt(payload, MessageSysFlag::COMPRESSED_FLAG);
    // 直接给出压缩后的 body（不经 maybeCompress，模拟"就是老客户端产的"）
    legacy.body = CompressorFactory::compress(payload, CompressionType::ZLIB, 5);
    Bytes legacyRaw = encodeMessageExt(legacy, /*needCompress=*/false);
    MessageExt legacyOut;
    CHECK(decodeMessage(legacyRaw, legacyOut, true, true), "decode legacy type-0 message");
    CHECK(legacyOut.body == payload, "legacy type-0 body decompressed as ZLIB");
    CHECK((legacyOut.sysFlag & MessageSysFlag::COMPRESSED_FLAG) == 0,
          "legacy COMPRESSED_FLAG cleared");

    // 未支持的压缩类型：**绝不能原样透传**。Java 是 CompressorFactory 抛异常 ->
    // MessageDecoder.decode 的 catch 吞掉 -> 返回 null（消息被丢弃）。
    // C++ 对齐为 decodeMessage 返回 false。若哪天退化成"交出压缩字节流"，
    // 就是静默数据损坏（标志位已被清，事后无法识别）。
    {
        MessageExt bogus = makeMessageExt(payload, MessageSysFlag::COMPRESSED_FLAG | (0x4 << 8));
        bogus.body = CompressorFactory::compress(payload, CompressionType::ZLIB, 5);
        Bytes bogusRaw = encodeMessageExt(bogus, /*needCompress=*/false);
        MessageExt bogusOut;
        CHECK(!decodeMessage(bogusRaw, bogusOut, true, true),
              "unsupported compression type -> decodeMessage returns false (not garbage)");
        bool threw = false;
        try {
            CompressorFactory::decompress(bogus.body, 4);
        } catch (const std::exception&) {
            threw = true;
        }
        CHECK(threw, "unsupported compression type -> decompress throws (Java-aligned)");
    }
}

// ---------------------------------------------------------------- 失败必须响亮
static void testFailuresAreLoud() {
    // 截断的 zlib 流必须抛异常，而不是静默返回半截数据
    const std::string payload = repeatedPayload();
    Bytes compressed = CompressorFactory::compress(payload, CompressionType::ZLIB, 5);
    Bytes truncated = compressed.substr(0, compressed.size() / 2);

    bool threw = false;
    try {
        CompressorFactory::decompress(truncated, CompressionType::ZLIB);
    } catch (const std::exception&) {
        threw = true;
    }
    CHECK(threw, "truncated zlib stream throws");

    // 未支持的算法（LZ4/ZSTD 未编入）也必须抛，而不是返回压缩字节
    bool threw2 = false;
    try {
        CompressorFactory::decompress(compressed, CompressionType::LZ4);
    } catch (const std::exception&) {
        threw2 = true;
    }
    CHECK(threw2, "unsupported codec (LZ4) throws instead of silent passthrough");

    // 非压缩的垃圾数据同样抛错
    bool threw3 = false;
    try {
        CompressorFactory::decompress(std::string("not-a-zlib-stream"), CompressionType::ZLIB);
    } catch (const std::exception&) {
        threw3 = true;
    }
    CHECK(threw3, "garbage input throws");
}

int main() {
    testTypeResolution();
    testZlibRoundTrip();
    testEncodeDecodeCompressed();
    testFailuresAreLoud();

    std::cout << "compression: PASS=" << g_pass << " FAIL=" << g_fail << "\n";
    return g_fail == 0 ? 0 : 1;
}
