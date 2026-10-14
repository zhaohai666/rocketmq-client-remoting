// The compile-time switches ROCKETMQ_HAS_ZLIB / ROCKETMQ_HAS_ZSTD / ROCKETMQ_HAS_LZ4 come from CMake's
// find_package(ZLIB) and find_path/find_library(zstd|lz4). If a backend was not compiled in, a message
// compressed with that algorithm **throws** instead of passing through: silent passthrough is data corruption.
#include "rocketmq/common/compression.h"

#include <cstring>
#include <stdexcept>
#include <vector>

#if defined(ROCKETMQ_HAS_ZLIB)
#include <zlib.h>
#endif

#if defined(ROCKETMQ_HAS_ZSTD)
#include <zstd.h>
#endif

#if defined(ROCKETMQ_HAS_LZ4)
#include <lz4frame.h>
#endif

namespace rocketmq {

// ---------------------------------------------------------------- CompressionType

int32_t CompressionType::findByValue(int32_t value) {
    switch (value) {
        case 1:
            return LZ4;
        case 2:
            return ZSTD;
        case 0:  // Backwards compatible: a compressed message without a type byte is treated as ZLIB
        case 3:
            return ZLIB;
        default:
            throw std::runtime_error("unknown compress type value: " + std::to_string(value));
    }
}

int32_t CompressionType::getCompressionFlag(int32_t value) {
    switch (value) {
        case 1:
            return 0x1 << 8;  // COMPRESSION_LZ4_TYPE
        case 2:
            return 0x2 << 8;  // COMPRESSION_ZSTD_TYPE
        case 3:
            return 0x3 << 8;  // COMPRESSION_ZLIB_TYPE
        default:
            throw std::runtime_error("unsupported compress type flag: " + std::to_string(value));
    }
}

// ---------------------------------------------------------------- zlib details

#if defined(ROCKETMQ_HAS_ZLIB)

namespace {

Bytes zlibDeflate(const Bytes& src, int level) {
    if (level < 0 || level > 9) {
        level = 5;
    }
    if (src.empty()) {
        return Bytes();
    }
    z_stream zs;
    std::memset(&zs, 0, sizeof(zs));
    if (::deflateInit(&zs, level) != Z_OK) {
        throw std::runtime_error("zlib deflateInit failed");
    }
    zs.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(src.data()));
    zs.avail_in = static_cast<uInt>(src.size());

    Bytes out;
    char buf[32 * 1024];
    int ret = Z_OK;
    while (ret != Z_STREAM_END) {
        zs.next_out = reinterpret_cast<Bytef*>(buf);
        zs.avail_out = static_cast<uInt>(sizeof(buf));
        // Feed all input at once and use Z_FINISH so deflate emits a complete stream
        ret = ::deflate(&zs, Z_FINISH);
        if (ret != Z_OK && ret != Z_STREAM_END && ret != Z_BUF_ERROR) {
            ::deflateEnd(&zs);
            throw std::runtime_error("zlib deflate failed: code=" + std::to_string(ret));
        }
        out.append(buf, sizeof(buf) - zs.avail_out);
        if (ret == Z_BUF_ERROR && zs.avail_out != 0) {
            // No progress and no output space left -- cannot continue; avoid an infinite loop
            break;
        }
    }
    ::deflateEnd(&zs);
    return out;
}

Bytes zlibInflate(const Bytes& src) {
    if (src.empty()) {
        return Bytes();
    }
    z_stream zs;
    std::memset(&zs, 0, sizeof(zs));
    if (::inflateInit(&zs) != Z_OK) {
        throw std::runtime_error("zlib inflateInit failed");
    }
    zs.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(src.data()));
    zs.avail_in = static_cast<uInt>(src.size());

    Bytes out;
    char buf[32 * 1024];
    int ret = Z_OK;
    while (ret != Z_STREAM_END) {
        zs.next_out = reinterpret_cast<Bytef*>(buf);
        zs.avail_out = static_cast<uInt>(sizeof(buf));
        ret = ::inflate(&zs, Z_NO_FLUSH);
        if (ret != Z_OK && ret != Z_STREAM_END && ret != Z_BUF_ERROR) {
            const char* msg = zs.msg != nullptr ? zs.msg : "unknown";
            ::inflateEnd(&zs);
            throw std::runtime_error(std::string("zlib inflate failed: ") + msg);
        }
        out.append(buf, sizeof(buf) - zs.avail_out);
        if (ret != Z_STREAM_END && zs.avail_in == 0 && zs.avail_out != 0) {
            // Input exhausted and the output buffer not full, yet the stream end was not reached -> the data is truncated
            ::inflateEnd(&zs);
            throw std::runtime_error("zlib inflate failed: truncated stream");
        }
    }
    ::inflateEnd(&zs);
    return out;
}

}  // namespace

#endif  // ROCKETMQ_HAS_ZLIB

// ---------------------------------------------------------------- zstd details
#if defined(ROCKETMQ_HAS_ZSTD)

namespace {

// libzstd clamps ZSTD_compress's level to [1,19] anyway, but clamping explicitly avoids version-dependent
// handling of out-of-range values (0 means "use the default level" in zstd semantics).
int clampZstdLevel(int level) {
    if (level <= 0) {
        return ZSTD_CLEVEL_DEFAULT;  // 3
    }
    const int maxLevel = ZSTD_maxCLevel();
    return level > maxLevel ? maxLevel : level;
}

Bytes zstdCompressBytes(const Bytes& src, int level) {
    // compressBound covers the worst case: even incompressible data is allocated in one shot, no retry loop
    const size_t bound = ::ZSTD_compressBound(src.size());
    Bytes out;
    out.resize(bound);
    // src.data() is a valid pointer even for an empty string (it points at the terminating '\0'), so no special case for empty
    const size_t produced =
        ::ZSTD_compress(&out[0], bound, src.data(), src.size(), clampZstdLevel(level));
    if (::ZSTD_isError(produced)) {
        throw std::runtime_error(std::string("zstd compress failed: ") +
                                 ::ZSTD_getErrorName(produced));
    }
    out.resize(produced);
    return out;
}

// Decompression ceiling: prevents allocating memory from an absurd content size claimed by a corrupt frame header (a DoS surface).
// 2GB is far above the RocketMQ single message body limit (4MB), so it is generous enough.
constexpr size_t kMaxZstdOutput = 2048ULL * 1024ULL * 1024ULL;

Bytes zstdDecompressBytes(const Bytes& src) {
    if (src.empty()) {
        return Bytes();
    }
    unsigned long long declared = ::ZSTD_getFrameContentSize(src.data(), src.size());
    if (declared == ZSTD_CONTENTSIZE_ERROR) {
        // Even the frame header is invalid (wrong magic/window/contentSize reserved bits) -- it must never be handed out as payload
        throw std::runtime_error("zstd decompress failed: not a valid zstd frame");
    }
    const bool sizeKnown = (declared != ZSTD_CONTENTSIZE_UNKNOWN);
    // zstd-jni's ZstdOutputStream writes incrementally and puts 0 (unknown) in the frame header's content size field,
    // so the unknown branch is the **normal case**, not an error: it is covered by exponential growth plus dstSize_tooSmall retries.
    size_t capacity = sizeKnown ? static_cast<size_t>(declared)
                                : (src.size() < (16ULL * 1024 * 1024)
                                       ? (src.size() < (64ULL * 1024) ? (64ULL * 1024) : src.size() * 4)
                                       : src.size() * 2);
    for (int attempt = 0;; ++attempt) {
        if (capacity > kMaxZstdOutput) {
            throw std::runtime_error("zstd decompress failed: output too large (> " +
                                     std::to_string(kMaxZstdOutput) + " bytes)");
        }
        Bytes out;
        out.resize(capacity);
        const size_t produced =
            ::ZSTD_decompress(&out[0], capacity, src.data(), src.size());
        if (!::ZSTD_isError(produced)) {
            out.resize(produced);
            return out;
        }
        // Only "output buffer too small" is worth growing the buffer and retrying; every other error (bad frame/truncation) throws,
        // and retrying when the buffer already matches the size declared in the frame header is pointless -- the data itself is corrupt.
        // Note: ZSTD_getErrorCode() / ZSTD_error_dstSize_tooSmall are not exported by every
        // zstd distribution (the CI version lacks both symbols), so the decision uses the stable, readable error-name string -- the target
        // string is "Dst buffer is too small", matched on the substring "too small" to tolerate wording changes.
        const std::string zstd_err = ::ZSTD_getErrorName(produced);
        if (sizeKnown || std::string(zstd_err).find("too small") == std::string::npos ||
            attempt >= 32) {
            throw std::runtime_error(std::string("zstd decompress failed: ") + zstd_err);
        }
        capacity *= 2;
    }
}

}  // namespace

#endif  // ROCKETMQ_HAS_ZSTD

// ---------------------------------------------------------------- LZ4 Frame details
#if defined(ROCKETMQ_HAS_LZ4)

namespace {

// Output block size during decompression; the LZ4F streaming API does not need the total decompressed length up front.
constexpr size_t kLz4OutputChunk = 64ULL * 1024ULL;
// Same as zstd: refuse to allocate memory without bound based on a corrupt frame header.
constexpr size_t kMaxLz4Output = 2048ULL * 1024ULL * 1024ULL;

Bytes lz4CompressFrame(const Bytes& src) {
    LZ4F_preferences_t prefs;
    std::memset(&prefs, 0, sizeof(prefs));
    prefs.frameInfo.blockSizeID = LZ4F_max64KB;
    prefs.frameInfo.blockMode = LZ4F_blockIndependent;
    prefs.frameInfo.contentChecksumFlag = LZ4F_noContentChecksum;
    prefs.frameInfo.contentSize = static_cast<unsigned long long>(src.size());
    prefs.compressionLevel = 0;  // 0 = liblz4 default level

    const size_t bound = ::LZ4F_compressFrameBound(src.size(), &prefs);
    Bytes out;
    out.resize(bound);
    const size_t produced =
        ::LZ4F_compressFrame(&out[0], bound, src.data(), src.size(), &prefs);
    if (::LZ4F_isError(produced)) {
        throw std::runtime_error(std::string("lz4 compress failed: ") +
                                 ::LZ4F_getErrorName(produced));
    }
    out.resize(produced);
    return out;
}

// LZ4F_decompress is a streaming API: one call may consume only part of the input or fill only part of the output,
// returning 0 at the end of the frame and >0 as a hint of "how many more input bytes until the next block boundary".
Bytes lz4DecompressFrame(const Bytes& src) {
    if (src.empty()) {
        return Bytes();
    }
    LZ4F_decompressionContext_t ctx = nullptr;
    if (::LZ4F_isError(::LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION))) {
        throw std::runtime_error("lz4 decompress failed: cannot create decompression context");
    }
    // RAII: every throwing path below (and the bad_alloc out.append may raise) must release this handle;
    // hand-written paired frees would leak eventually.
    struct CtxGuard {
        LZ4F_decompressionContext_t* p;
        ~CtxGuard() { if (*p != nullptr) ::LZ4F_freeDecompressionContext(*p); }
    } guard{&ctx};

    Bytes out;
    std::vector<char> chunk(kLz4OutputChunk);
    size_t consumed = 0;
    for (;;) {
        size_t srcAvail = src.size() - consumed;
        size_t dstWritten = chunk.size();
        const size_t hint = ::LZ4F_decompress(
            ctx, &chunk[0], &dstWritten,
            reinterpret_cast<const unsigned char*>(src.data()) + consumed, &srcAvail, nullptr);
        if (::LZ4F_isError(hint)) {
            const char* name = ::LZ4F_getErrorName(hint);
            throw std::runtime_error(std::string("lz4 decompress failed: ") +
                                     (name != nullptr ? name : "unknown error"));
        }
        consumed += srcAvail;
        out.append(chunk.data(), dstWritten);
        if (out.size() > kMaxLz4Output) {
            throw std::runtime_error("lz4 decompress failed: output too large (> " +
                                     std::to_string(kMaxLz4Output) + " bytes)");
        }
        if (hint == 0) {
            return out;  // frame boundary reached; leftover bytes (if any) do not belong to this frame
        }
        if (srcAvail == 0 && dstWritten == 0) {
            // No input consumed and no output produced -- looping again would spin forever; the frame is incomplete
            throw std::runtime_error("lz4 decompress failed: truncated frame");
        }
    }
}

}  // namespace

#endif  // ROCKETMQ_HAS_LZ4

bool hasZlibSupport() {
#if defined(ROCKETMQ_HAS_ZLIB)
    return true;
#else
    return false;
#endif
}

bool hasZstdSupport() {
#if defined(ROCKETMQ_HAS_ZSTD)
    return true;
#else
    return false;
#endif
}

bool hasLz4Support() {
#if defined(ROCKETMQ_HAS_LZ4)
    return true;
#else
    return false;
#endif
}

// ---------------------------------------------------------------- CompressorFactory

Bytes CompressorFactory::compress(const Bytes& src, int32_t compressionType, int level) {
    const int32_t type = CompressionType::findByValue(compressionType);
    if (type == CompressionType::ZLIB) {
#if defined(ROCKETMQ_HAS_ZLIB)
        return zlibDeflate(src, level);
#else
        throw std::runtime_error(
            "zlib compression is not available in this build (rebuild with -DRMQ_WITH_ZLIB=ON)");
#endif
    }
    if (type == CompressionType::ZSTD) {
#if defined(ROCKETMQ_HAS_ZSTD)
        return zstdCompressBytes(src, level);
#else
        throw std::runtime_error(
            "zstd compression is not available in this build (rebuild with -DRMQ_WITH_ZSTD=ON)");
#endif
    }
    if (type == CompressionType::LZ4) {
#if defined(ROCKETMQ_HAS_LZ4)
        (void)level;
        return lz4CompressFrame(src);
#else
        throw std::runtime_error(
            "lz4 compression is not available in this build (rebuild with -DRMQ_WITH_LZ4=ON)");
#endif
    }
    // findByValue can only return one of LZ4/ZSTD/ZLIB; reaching here means a backend was compiled out above.
    throw std::runtime_error("unsupported compression type for compress: " + std::to_string(type));
}

Bytes CompressorFactory::decompress(const Bytes& src, int32_t compressionType) {
    const int32_t type = CompressionType::findByValue(compressionType);
    if (type == CompressionType::ZLIB) {
#if defined(ROCKETMQ_HAS_ZLIB)
        return zlibInflate(src);
#else
        // Key point: never return the bytes unchanged. Handing back compressed bytes would be treated as payload by the caller -- silent data corruption.
        throw std::runtime_error(
            "message is zlib-compressed but this build has no zlib support "
            "(rebuild with -DRMQ_WITH_ZLIB=ON)");
#endif
    }
    if (type == CompressionType::ZSTD) {
#if defined(ROCKETMQ_HAS_ZSTD)
        return zstdDecompressBytes(src);
#else
        // As above: throw when the backend is compiled out and never hand a zstd frame to the caller as payload.
        throw std::runtime_error(
            "message is zstd-compressed but this build has no zstd support "
            "(rebuild with -DRMQ_WITH_ZSTD=ON)");
#endif
    }
    if (type == CompressionType::LZ4) {
#if defined(ROCKETMQ_HAS_LZ4)
        return lz4DecompressFrame(src);
#else
        throw std::runtime_error(
            "message is lz4-compressed but this build has no lz4 support "
            "(rebuild with -DRMQ_WITH_LZ4=ON)");
#endif
    }
    // Unknown type bytes already throw in CompressionType::findByValue, so this point is unreachable.
    throw std::runtime_error("unsupported compression type for decompress: " + std::to_string(type));
}

}  // namespace rocketmq
