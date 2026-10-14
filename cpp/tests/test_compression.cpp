// Compression/decompression unit tests: compression type resolution, interop with external implementations for zlib/lz4/zstd,
// the "clear COMPRESSED_FLAG after decompression" semantics in the 17-section frame, and the legacy path where **the type bits are 0**.
#include <cstdint>
#include <exception>
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

// The zlib reference and the plaintext shared by the cases below
std::string repeatedPayload() {
    std::string s;
    for (int i = 0; i < 40; ++i) {
        s += "rocketmq-compressed-payload-";
    }
    return s;
}

// zlib level 5 compressing "rocketmq-compressed-payload-"*40 (1120B) yields this 47B reference
const char* kZlibHex =
    "785e2bca4fce4e2dc92dd44dcecf2d284a2d2e4e4dd12d48acccc94f4cd12d1a951b951b951b95a3400e"
    "0058e0b9f0";

const char* kLz4ExternalHex =
    "04224d186070732b000000ff0d726f636b65746d712d636f6d707265737365642d7061796c6f61642d1c"
    "00ffffffff30506c6f61642d00000000";

const char* kZstdExternalHex =
    "28b52ffd0058240100e0726f636b65746d712d636f6d707265737365642d7061796c6f61642d010004f1"
    "ff7402010000";

// Deterministic compressible payload: one fixed line of text repeated until it reaches size bytes
// (same recipe as examples/compression_live.cpp)
std::string bigPayload(size_t size) {
    const std::string line = "rocketmq-compress-interop-payload-line-0123456789\n";
    std::string s;
    s.reserve(size + line.size());
    while (s.size() < size) s += line;
    s.resize(size);
    return s;
}

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

// Backend availability is decided at compile time (CMake switches the matching macro off when the library is missing), so every
// case must be able to take the "not compiled in -> must throw" branch -- it must **not** pretend to pass.
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

// Catching assertion: returns the reason thrown (an empty string when nothing was thrown)
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

// ---------------------------------------------------------------- type resolution
static void testTypeResolution() {
    CHECK(hasZlibSupport(), "zlib support compiled in");

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

// ---------------------------------------------------------------- zlib round trip + external interop
static void testZlibRoundTrip() {
    const std::string payload = repeatedPayload();

    Bytes compressed = CompressorFactory::compress(payload, CompressionType::ZLIB, 5);
    CHECK(!compressed.empty() && compressed.size() < payload.size(),
          "zlib compress shrinks a repetitive payload");
    // zlib stream format header: 0x78 (CMF, deflate + 32K window)
    CHECK(static_cast<unsigned char>(compressed[0]) == 0x78, "zlib stream has 0x78 CMF byte");
    CHECK(CompressorFactory::decompress(compressed, CompressionType::ZLIB) == payload,
          "zlib round-trip");

    // Type bits 0 must also decompress as ZLIB (these are compressed messages produced by legacy clients)
    CHECK(CompressorFactory::decompress(compressed, 0) == payload,
          "decompress with type 0 falls back to ZLIB");

    // External reference: bytes produced by another zlib implementation must decompress
    Bytes external = UtilAll::string2Bytes(kZlibHex);
    CHECK(!external.empty(), "external zlib fixture decoded from hex");
    CHECK(external.size() == 47, "external zlib fixture length == 47");
    Bytes restored = CompressorFactory::decompress(external, CompressionType::ZLIB);
    CHECK(restored == payload, "decompress external zlib bytes");
}

// ---------------------------------------------------------------- LZ4 Frame
static void testLz4RoundTrip() {
    const std::string payload = repeatedPayload();
    if (!hasLz4Support()) {
        // Backend compiled out: must throw and never return compressed bytes (same contract as zlib)
        CHECK(!throwsWhat([&] { CompressorFactory::compress(payload, CompressionType::LZ4); }).empty(),
              "lz4 backend compiled out -> compress throws (no silent passthrough)");
        return;
    }
    Bytes compressed = CompressorFactory::compress(payload, CompressionType::LZ4);
    CHECK(!compressed.empty() && compressed.size() < payload.size(),
          "lz4 compress shrinks a repetitive payload");
    // LZ4 Frame magic 0x184D2204, written little-endian as 04 22 4d 18
    CHECK(static_cast<unsigned char>(compressed[0]) == 0x04 &&
              static_cast<unsigned char>(compressed[1]) == 0x22 &&
              static_cast<unsigned char>(compressed[2]) == 0x4d &&
              static_cast<unsigned char>(compressed[3]) == 0x18,
          "lz4 output carries LZ4 Frame magic (not a raw block)");
    // FLG bit5 = Block Independence: the LZ4FrameInputStream of lz4-java **rejects** dependent blocks,
    // so with a multi-block payload (>64KB) this bit unset makes the frame undecodable.
    CHECK((static_cast<unsigned char>(compressed[4]) & 0x20) != 0,
          "lz4 frame sets Block-Independence flag (lz4-java requires it)");
    CHECK(CompressorFactory::decompress(compressed, CompressionType::LZ4) == payload,
          "lz4 round-trip");
    CHECK(CompressorFactory::decompress(compressed, 1) == payload,
          "lz4 round-trip via raw type bit 1");

    Bytes external = UtilAll::string2Bytes(kLz4ExternalHex);
    CHECK(external.size() == 58, "external (lz4-java produced) lz4 frame fixture length == 58");
    CHECK(CompressorFactory::decompress(external, CompressionType::LZ4) == payload,
          "decompress external (reference-produced) lz4 frame");
}

// ---------------------------------------------------------------- ZSTD
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
    // zstd magic 0xFD2FB528, written little-endian as 28 b5 2f fd
    CHECK(static_cast<unsigned char>(compressed[0]) == 0x28 &&
              static_cast<unsigned char>(compressed[1]) == 0xb5 &&
              static_cast<unsigned char>(compressed[2]) == 0x2f &&
              static_cast<unsigned char>(compressed[3]) == 0xfd,
          "zstd output carries zstd frame magic");
    CHECK(CompressorFactory::decompress(compressed, CompressionType::ZSTD) == payload,
          "zstd round-trip");
    CHECK(CompressorFactory::decompress(compressed, 2) == payload,
          "zstd round-trip via raw type bit 2");

    Bytes external = UtilAll::string2Bytes(kZstdExternalHex);
    CHECK(external.size() == 48, "external (zstd-jni produced) zstd frame fixture length == 48");
    CHECK(CompressorFactory::decompress(external, CompressionType::ZSTD) == payload,
          "decompress external (reference-produced) zstd frame with unknown content size");

    // External reference 2: a frame **with** content size produced by the `zstd -3` CLI (50B), covering the known-length branch
    Bytes withSize = UtilAll::string2Bytes(
        "28b52ffd646003250100e0726f636b65746d712d636f6d707265737365642d7061796c6f61642d010004f1"
        "ff740278aa9792");
    CHECK(withSize.size() == 50, "external (zstd CLI) frame-with-content-size fixture length == 50");
    CHECK(CompressorFactory::decompress(withSize, CompressionType::ZSTD) == payload,
          "decompress zstd frame that declares its content size");
}

// ---------------------------------------------------------------- size matrix
//
// Coverage: empty / 1B / around the threshold (4095/4096/4097, the producer auto-compression threshold) /
// spanning several lz4 blocks (>64KB) / an **incompressible** payload (even when nothing shrinks it must round-trip verbatim).
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
            // A non-empty input must still carry a frame header afterwards (proof a real encoder ran, not a passthrough)
            CHECK(n == 0 ? true : c.size() >= 4,
                  "compressed output is a real frame, size=" + std::to_string(n));
        }
        // Incompressible payload: it necessarily inflates, yet must restore byte-for-byte (inflation != passthrough)
        const std::string noisy = incompressiblePayload(20000);
        const Bytes cn = CompressorFactory::compress(noisy, codec, 5);
        CHECK(cn != noisy, "incompressible payload is not passed through unchanged");
        CHECK(CompressorFactory::decompress(cn, codec) == noisy,
              "incompressible payload round-trips (codec=" + std::to_string(codec) + ")");
    }
}

// ---------------------------------------------------------------- 17-section frame round trip
static void testEncodeDecodeCompressed() {
    const std::string payload = repeatedPayload();
    const int32_t compressedFlag = MessageSysFlag::COMPRESSED_FLAG | (0x3 << 8);

    MessageExt src = makeMessageExt(payload, compressedFlag);
    Bytes raw = encodeMessageExt(src, /*needCompress=*/true);

    // The body stored in the frame should be the compressed one: the total length is clearly below the raw body length
    CHECK(raw.size() < payload.size(), "encoded frame smaller than raw payload");

    // Decompress path: body restored + COMPRESSED_FLAG cleared (type bits preserved)
    MessageExt out;
    CHECK(decodeMessage(raw, out, true, true), "decode compressed message");
    CHECK(out.body == payload, "compressed body restored");
    CHECK((out.sysFlag & MessageSysFlag::COMPRESSED_FLAG) == 0,
          "COMPRESSED_FLAG cleared after decompress");
    CHECK(MessageSysFlag::getCompressionType(out.sysFlag) == 3,
          "compression type bits preserved after clear");

    // Non-decompress path: the body stays compressed bytes and the flag bits are kept
    MessageExt kept;
    CHECK(decodeMessage(raw, kept, true, false), "decode without decompress");
    CHECK(kept.body != payload, "body stays compressed when decompressBody=false");
    CHECK((kept.sysFlag & MessageSysFlag::COMPRESSED_FLAG) != 0,
          "COMPRESSED_FLAG kept when decompressBody=false");
    CHECK(CompressorFactory::decompress(kept.body, 3) == payload,
          "manually decompressing the kept body works");

    // Type bits 0 (legacy) must also decode: first fake a sysFlag whose type bits are 0, then encode
    MessageExt legacy = makeMessageExt(payload, MessageSysFlag::COMPRESSED_FLAG);
    // Feed the compressed body directly (bypassing maybeCompress, simulating "exactly what a legacy client produced")
    legacy.body = CompressorFactory::compress(payload, CompressionType::ZLIB, 5);
    Bytes legacyRaw = encodeMessageExt(legacy, /*needCompress=*/false);
    MessageExt legacyOut;
    CHECK(decodeMessage(legacyRaw, legacyOut, true, true), "decode legacy type-0 message");
    CHECK(legacyOut.body == payload, "legacy type-0 body decompressed as ZLIB");
    CHECK((legacyOut.sysFlag & MessageSysFlag::COMPRESSED_FLAG) == 0,
          "legacy COMPRESSED_FLAG cleared");

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
        CHECK(threw, "unsupported compression type -> decompress throws (protocol-aligned)");
    }
}

// ---------------------------------------------------------------- 17-section frame x LZ4/ZSTD
static void testEncodeDecodeCompressedLz4Zstd() {
    const std::string payload = bigPayload(8192);  // far above the 4096 threshold, same shape as a large live message
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

        // Non-decompress path: the body keeps the compressed bytes, flag bits kept, manual decompression still correct
        MessageExt kept;
        CHECK(decodeMessage(raw, kept, true, false), tag + ": decode without decompress");
        CHECK(kept.body != payload, tag + ": body stays compressed");
        CHECK((kept.sysFlag & MessageSysFlag::COMPRESSED_FLAG) != 0,
              tag + ": COMPRESSED_FLAG kept");
        CHECK(CompressorFactory::decompress(kept.body, codec) == payload,
              tag + ": manually decompressing the kept body works");
    }
}

// ---------------------------------------------------------------- failures must be loud
static void testFailuresAreLoud() {
    // A truncated zlib stream must throw instead of silently returning half the data
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

    for (int32_t bogus : {4, 5, 6, 7}) {
        CHECK(!throwsWhat([&] { CompressorFactory::compress(payload, bogus, 5); }).empty(),
              "unknown compression type " + std::to_string(bogus) + " -> compress throws");
        CHECK(!throwsWhat([&] { CompressorFactory::decompress(compressed, bogus); }).empty(),
              "unknown compression type " + std::to_string(bogus) + " -> decompress throws");
    }

    // Non-compressed garbage throws as well
    CHECK(!throwsWhat([&] {
              CompressorFactory::decompress(std::string("not-a-zlib-stream"), CompressionType::ZLIB);
          }).empty(),
          "garbage input throws (zlib)");

    // For a compiled-in backend: a truncated frame / bytes of another codec / garbage input must all throw (no backend may best-effort)
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
        // Feeding a zlib stream to the lz4 decoder: a frame magic mismatch must error instead of "give it a try"
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
        // A frame header declaring an absurd content size: it must be capped and throw instead of allocating to match
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
