// 压缩/解压单测：压缩类型解析、zlib/lz4/zstd 与外部实现的互通、17 段报文里
// "解压后清 COMPRESSED_FLAG" 的语义、以及老版本 **类型位为 0** 的兼容路径。
//
// 关键点：本文件里有多条用**硬编码外部字节**做的用例——zlib 段由 Python
// zlib.compress 生成，lz4 / zstd 段由**真 RocketMQ Java 生产代码**用的同一套库
// （lz4-java 1.10.3 的 LZ4FrameOutputStream、zstd-jni 1.5.2-2 的 ZstdOutputStream）
// 生成。它们用来证明我们读写的是真正的**线上格式**，而不是"自己压自己解"的自嗨。
#include <cstdint>
#include <functional>
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

// 同一段 1120B 明文，由 **Java lz4-java 1.10.3 的 LZ4FrameOutputStream**（即
// Lz4Compressor 用的那个类）产出，58B。帧头 04 22 4d 18 = LZ4 Frame magic。
const char* kLz4JavaHex =
    "04224d186070732b000000ff0d726f636b65746d712d636f6d707265737365642d7061796c6f61642d1c"
    "00ffffffff30506c6f61642d00000000";

// 同一段明文，由 **Java zstd-jni 1.5.2-2 的 ZstdOutputStream(out, 5)**（即
// ZstdCompressor 用的那个类）产出，48B。帧头 28 b5 2f fd = zstd magic。
const char* kZstdJavaHex =
    "28b52ffd0058240100e0726f636b65746d712d636f6d707265737365642d7061796c6f61642d010004f1"
    "ff7402010000";

// 确定性可压缩载荷：重复一行固定文本到 size 字节
// （与 examples/compression_live.cpp、rust live_compression_matrix.rs 同配方）
std::string bigPayload(size_t size) {
    const std::string line = "rocketmq-compress-interop-payload-line-0123456789\n";
    std::string s;
    s.reserve(size + line.size());
    while (s.size() < size) s += line;
    s.resize(size);
    return s;
}

// 确定性**不可压缩**载荷：线性同余伪随机字节，与 Java 探针逐位一致。
// 用于验证"膨胀也要原样往返"——压缩后端绝不能因为压不动就静默透传。
std::string incompressiblePayload(size_t size) {
    std::string s;
    s.resize(size);
    uint64_t state = 0x123456789abcdefULL;
    for (size_t i = 0; i < size; ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        s[i] = static_cast<char>((state >> 33) & 0xFF);
    }
    return s;
}

// 后端可用性由编译期决定（CMake 找不到库就会关掉对应宏），所以每个用例都要能
// 走"没编入 -> 必须抛错"这条分支——**不能**假装通过。
bool codecAvailable(int32_t type) {
    switch (type) {
        case CompressionType::ZLIB:
            return hasZlibSupport();
        case CompressionType::ZSTD:
            return hasZstdSupport();
        case CompressionType::LZ4:
            return hasLz4Support();
        default:
            return false;
    }
}

// 捕获式断言：返回抛出的原因（未抛则返回空串）
std::string throwsWhat(const std::function<void()>& fn) {
    try {
        fn();
    } catch (const std::exception& e) {
        return e.what();
    }
    return std::string();
}

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

// ---------------------------------------------------------------- LZ4 Frame
//
// Java Lz4Compressor 用 lz4-java 的 LZ4FrameOutputStream，即 **LZ4 Frame 互操作格式**。
// 这里要证明两件事：(1) 我们产的帧带标准帧头（不是 LZ4_compress_default 的裸 block）；
// (2) 我们能把**真 Java 产的**帧解回原文——这只有硬编码外部字节才能证明。
static void testLz4RoundTrip() {
    const std::string payload = repeatedPayload();
    if (!hasLz4Support()) {
        // 后端被编掉：必须抛错，绝不返回压缩字节（同 zlib 的约定）
        CHECK(!throwsWhat([&] { CompressorFactory::compress(payload, CompressionType::LZ4); }).empty(),
              "lz4 backend compiled out -> compress throws (no silent passthrough)");
        return;
    }
    Bytes compressed = CompressorFactory::compress(payload, CompressionType::LZ4);
    CHECK(!compressed.empty() && compressed.size() < payload.size(),
          "lz4 compress shrinks a repetitive payload");
    // LZ4 Frame magic 0x184D2204，小端落盘 = 04 22 4d 18
    CHECK(static_cast<unsigned char>(compressed[0]) == 0x04 &&
              static_cast<unsigned char>(compressed[1]) == 0x22 &&
              static_cast<unsigned char>(compressed[2]) == 0x4d &&
              static_cast<unsigned char>(compressed[3]) == 0x18,
          "lz4 output carries LZ4 Frame magic (not a raw block)");
    // FLG 的 bit5 = Block Independence：lz4-java 的 LZ4FrameInputStream **拒绝**
    // 依赖块，多块载荷（>64KB）时这一位没置就直接解不开。
    CHECK((static_cast<unsigned char>(compressed[4]) & 0x20) != 0,
          "lz4 frame sets Block-Independence flag (lz4-java requires it)");
    CHECK(CompressorFactory::decompress(compressed, CompressionType::LZ4) == payload,
          "lz4 round-trip");
    CHECK(CompressorFactory::decompress(compressed, 1) == payload,
          "lz4 round-trip via raw type bit 1");

    // 外部真值：Java lz4-java 产的 58B 帧必须能解成同一段明文
    Bytes external = UtilAll::string2Bytes(kLz4JavaHex);
    CHECK(external.size() == 58, "external (Java lz4-java) lz4 frame fixture length == 58");
    CHECK(CompressorFactory::decompress(external, CompressionType::LZ4) == payload,
          "decompress external (Java-produced) lz4 frame");
}

// ---------------------------------------------------------------- ZSTD
//
// Java ZstdCompressor 用 zstd-jni 的 ZstdOutputStream/ZstdInputStream，产出标准
// zstd 帧。同样需要"Java 真值"来证明格式对齐。
static void testZstdRoundTrip() {
    const std::string payload = repeatedPayload();
    if (!hasZstdSupport()) {
        CHECK(!throwsWhat([&] { CompressorFactory::compress(payload, CompressionType::ZSTD); }).empty(),
              "zstd backend compiled out -> compress throws (no silent passthrough)");
        return;
    }
    Bytes compressed = CompressorFactory::compress(payload, CompressionType::ZSTD, 5);
    CHECK(!compressed.empty() && compressed.size() < payload.size(),
          "zstd compress shrinks a repetitive payload");
    // zstd magic 0xFD2FB528，小端落盘 = 28 b5 2f fd
    CHECK(static_cast<unsigned char>(compressed[0]) == 0x28 &&
              static_cast<unsigned char>(compressed[1]) == 0xb5 &&
              static_cast<unsigned char>(compressed[2]) == 0x2f &&
              static_cast<unsigned char>(compressed[3]) == 0xfd,
          "zstd output carries zstd frame magic");
    CHECK(CompressorFactory::decompress(compressed, CompressionType::ZSTD) == payload,
          "zstd round-trip");
    CHECK(CompressorFactory::decompress(compressed, 2) == payload,
          "zstd round-trip via raw type bit 2");

    // 外部真值 1：Java zstd-jni（流式写，帧头**不带** content size）产的 48B 帧。
    // 这条覆盖的是"未知长度"分支——解码必须靠指数扩容而不是预先按声明长度开缓冲。
    Bytes external = UtilAll::string2Bytes(kZstdJavaHex);
    CHECK(external.size() == 48, "external (Java zstd-jni) zstd frame fixture length == 48");
    CHECK(CompressorFactory::decompress(external, CompressionType::ZSTD) == payload,
          "decompress external (Java-produced) zstd frame with unknown content size");

    // 外部真值 2：`zstd -3` CLI 产的**带** content size 的帧（50B），覆盖已知长度分支
    Bytes withSize = UtilAll::string2Bytes(
        "28b52ffd646003250100e0726f636b65746d712d636f6d707265737365642d7061796c6f61642d010004f1"
        "ff740278aa9792");
    CHECK(withSize.size() == 50, "external (zstd CLI) frame-with-content-size fixture length == 50");
    CHECK(CompressorFactory::decompress(withSize, CompressionType::ZSTD) == payload,
          "decompress zstd frame that declares its content size");
}

// ---------------------------------------------------------------- 尺寸矩阵
//
// 覆盖：空 / 1B / 阈值上下（4095/4096/4097，producer 的自动压缩阈值）/
// 跨多个 lz4 block（>64KB）/ **不可压缩**载荷（压不动也必须原样往返）。
static void testSizeMatrix() {
    const size_t sizes[] = {0, 1, 100, 4095, 4096, 4097, 8192, 1120, 65536, 65537, 200000};
    const int32_t codecs[] = {CompressionType::ZLIB, CompressionType::LZ4, CompressionType::ZSTD};
    for (int32_t codec : codecs) {
        if (!codecAvailable(codec)) continue;
        for (size_t n : sizes) {
            const std::string plain = bigPayload(n);
            const Bytes c = CompressorFactory::compress(plain, codec, 5);
            CHECK(CompressorFactory::decompress(c, codec) == plain,
                  "round-trip codec=" + std::to_string(codec) + " size=" + std::to_string(n));
            // 非空输入压完还得带帧头（说明走的是真编码器，不是透传）
            CHECK(n == 0 ? true : c.size() >= 4,
                  "compressed output is a real frame, size=" + std::to_string(n));
        }
        // 不可压缩载荷：必然膨胀，但**必须**逐字节还原（膨胀 != 透传）
        const std::string noisy = incompressiblePayload(20000);
        const Bytes cn = CompressorFactory::compress(noisy, codec, 5);
        CHECK(cn != noisy, "incompressible payload is not passed through unchanged");
        CHECK(CompressorFactory::decompress(cn, codec) == noisy,
              "incompressible payload round-trips (codec=" + std::to_string(codec) + ")");
    }
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

// ---------------------------------------------------------------- 17 段报文 x LZ4/ZSTD
//
// 上面几段只证明了 codec 本身；这条走的是**线上路径**：sysFlag 的类型位
// (COMPRESSED_FLAG | 0x1<<8 / 0x2<<8) 要能把消息正确路由到 lz4 / zstd 后端，
// 解压后清 COMPRESSED_FLAG 但**保留类型位**（对齐 Java MessageDecoder）。
static void testEncodeDecodeCompressedLz4Zstd() {
    const std::string payload = bigPayload(8192);  // 远超 4096 阈值，形状同线上大消息
    const int32_t codecs[] = {CompressionType::LZ4, CompressionType::ZSTD};
    for (int32_t codec : codecs) {
        if (!codecAvailable(codec)) continue;
        const std::string tag = "codec=" + std::to_string(codec);
        const int32_t compressedFlag =
            MessageSysFlag::COMPRESSED_FLAG | CompressionType::getCompressionFlag(codec);

        MessageExt src = makeMessageExt(payload, compressedFlag);
        Bytes raw = encodeMessageExt(src, /*needCompress=*/true);
        CHECK(raw.size() < payload.size(), tag + ": encoded frame smaller than raw payload");

        MessageExt out;
        CHECK(decodeMessage(raw, out, true, true), tag + ": decode compressed message");
        CHECK(out.body == payload, tag + ": compressed body restored byte-for-byte");
        CHECK((out.sysFlag & MessageSysFlag::COMPRESSED_FLAG) == 0,
              tag + ": COMPRESSED_FLAG cleared after decompress");
        CHECK(MessageSysFlag::getCompressionType(out.sysFlag) == codec,
              tag + ": compression type bits preserved after clear");

        // 不解压路径：body 保持压缩字节，标志位保留，手工解压仍正确
        MessageExt kept;
        CHECK(decodeMessage(raw, kept, true, false), tag + ": decode without decompress");
        CHECK(kept.body != payload, tag + ": body stays compressed");
        CHECK((kept.sysFlag & MessageSysFlag::COMPRESSED_FLAG) != 0,
              tag + ": COMPRESSED_FLAG kept");
        CHECK(CompressorFactory::decompress(kept.body, codec) == payload,
              tag + ": manually decompressing the kept body works");
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

    // **未知**压缩类型（SNAPPY=4 及 5/6/7）：Java 的 findByValue 直接抛
    // RuntimeException，这里同样必须抛——这是"静默透传"的最后一条退路。
    for (int32_t bogus : {4, 5, 6, 7}) {
        CHECK(!throwsWhat([&] { CompressorFactory::compress(payload, bogus, 5); }).empty(),
              "unknown compression type " + std::to_string(bogus) + " -> compress throws");
        CHECK(!throwsWhat([&] { CompressorFactory::decompress(compressed, bogus); }).empty(),
              "unknown compression type " + std::to_string(bogus) + " -> decompress throws");
    }

    // 非压缩的垃圾数据同样抛错
    CHECK(!throwsWhat([&] {
              CompressorFactory::decompress(std::string("not-a-zlib-stream"), CompressionType::ZLIB);
          }).empty(),
          "garbage input throws (zlib)");

    // 编入的后端：截帧 / 错喂别的codec / 垃圾输入都必须抛（每一端都不许"尽力而为"）
    if (hasLz4Support()) {
        const Bytes lz = CompressorFactory::compress(payload, CompressionType::LZ4);
        CHECK(!throwsWhat([&] {
                  CompressorFactory::decompress(lz.substr(0, lz.size() / 2), CompressionType::LZ4);
              }).empty(),
              "truncated lz4 frame throws");
        CHECK(!throwsWhat([&] {
                  CompressorFactory::decompress(std::string("not-an-lz4-frame"),
                                               CompressionType::LZ4);
              }).empty(),
              "garbage input throws (lz4)");
        // 把 zlib 流喂给 lz4 解码器：帧头 magic 不符必须报错，而不是"试试看"
        CHECK(!throwsWhat([&] { CompressorFactory::decompress(compressed, CompressionType::LZ4); }).empty(),
              "zlib bytes fed to the lz4 decoder throws");
    }
    if (hasZstdSupport()) {
        const Bytes zs = CompressorFactory::compress(payload, CompressionType::ZSTD, 5);
        CHECK(!throwsWhat([&] {
                  CompressorFactory::decompress(zs.substr(0, zs.size() / 2), CompressionType::ZSTD);
              }).empty(),
              "truncated zstd frame throws");
        CHECK(!throwsWhat([&] {
                  CompressorFactory::decompress(std::string("not-a-zstd-frame"),
                                               CompressionType::ZSTD);
              }).empty(),
              "garbage input throws (zstd)");
        CHECK(!throwsWhat([&] { CompressorFactory::decompress(compressed, CompressionType::ZSTD); }).empty(),
              "zlib bytes fed to the zstd decoder throws");
        // 帧头声明一个荒谬的 content size：必须被上限拦住并抛错，而不是照着开内存
        Bytes hostile = UtilAll::string2Bytes("28b52ffd");
        hostile += std::string(8, '\xff');  // content size = 0xFFFFFFFFFFFFFFFF
        CHECK(!throwsWhat([&] { CompressorFactory::decompress(hostile, CompressionType::ZSTD); }).empty(),
              "absurd declared zstd content size throws (no huge allocation)");
    }
}

int main() {
    testTypeResolution();
    testZlibRoundTrip();
    testLz4RoundTrip();
    testZstdRoundTrip();
    testSizeMatrix();
    testEncodeDecodeCompressed();
    testEncodeDecodeCompressedLz4Zstd();
    testFailuresAreLoud();

    std::cout << "compression: PASS=" << g_pass << " FAIL=" << g_fail
              << "  (zlib=" << hasZlibSupport() << " lz4=" << hasLz4Support()
              << " zstd=" << hasZstdSupport() << ")\n";
    return g_fail == 0 ? 0 : 1;
}
