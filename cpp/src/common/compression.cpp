// 消息体压缩/解压实现（对应 org.apache.rocketmq.common.compression.*）。
//
// 编译期开关 ROCKETMQ_HAS_ZLIB / ROCKETMQ_HAS_ZSTD / ROCKETMQ_HAS_LZ4 由 CMake 的
// find_package(ZLIB) 与 find_path/find_library(zstd|lz4) 决定。若某个后端未编入，
// 遇到该算法压缩的消息会**抛异常**而不是原样返回——静默透传等于数据损坏。
//
// 三种格式的"线上格式"以 Java 参考实现为准（本文件与之逐字节互通，见 examples/
// compression_live.cpp 与 /tmp 下的一次性 Java 探针）：
//   ZLIB -> java.util.zip.Deflater/Inflater，RFC1950 zlib 流
//   ZSTD -> zstd-jni 的 ZstdOutputStream/ZstdInputStream，标准 zstd 帧（magic 0xFD2FB528）
//   LZ4  -> lz4-java 的 LZ4FrameOutputStream/LZ4FrameInputStream，
//           **LZ4 Frame 互操作格式**（magic 0x184D2204），不是 raw block
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
        case 0:  // 兼容老版本：没有类型位的压缩消息按 ZLIB 处理
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

// ---------------------------------------------------------------- zlib 细节

#if defined(ROCKETMQ_HAS_ZLIB)

namespace {

Bytes zlibDeflate(const Bytes& src, int level) {
    if (level < 0 || level > 9) {
        level = 5;  // Java/Python 的默认级别
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
        // 输入一次性给足，用 Z_FINISH 让 deflate 产出完整流
        ret = ::deflate(&zs, Z_FINISH);
        if (ret != Z_OK && ret != Z_STREAM_END && ret != Z_BUF_ERROR) {
            ::deflateEnd(&zs);
            throw std::runtime_error("zlib deflate failed: code=" + std::to_string(ret));
        }
        out.append(buf, sizeof(buf) - zs.avail_out);
        if (ret == Z_BUF_ERROR && zs.avail_out != 0) {
            // 既没有进展也没有输出空间可用 —— 无法继续，避免死循环
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
    // inflateInit 默认按 zlib 流格式（RFC1950）解析，与 Java InflaterInputStream 一致
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
            // 输入已耗尽、输出缓冲未填满，却仍未到流结尾 → 数据被截断
            ::inflateEnd(&zs);
            throw std::runtime_error("zlib inflate failed: truncated stream");
        }
    }
    ::inflateEnd(&zs);
    return out;
}

}  // namespace

#endif  // ROCKETMQ_HAS_ZLIB

// ---------------------------------------------------------------- zstd 细节
//
// 对应 org.apache.rocketmq.common.compression.ZstdCompressor：Java 侧用 zstd-jni 的
// ZstdOutputStream(stream, level) / ZstdInputStream，产出/消费的是**标准 zstd 帧**
// （magic 0xFD2FB528，即任务书里说的 zstd-jni simple API 写法）。这里用 libzstd 的
// ZSTD_compress / ZSTD_decompress 处理同样的帧。帧头自带窗口大小与 content size，
// 解码不依赖对端参数，所以**帧格式**与 Java / Python(zstandard) / Rust(zstd) 互通
// （压缩级别只影响产出字节，不影响互解；已用 Java 真值双向验证）。
#if defined(ROCKETMQ_HAS_ZSTD)

namespace {

// ZSTD_compress 的 level 会被 libzstd 夹紧到 [1,19]，但显式夹一次可以避免不同版本
// 对越界值的处理差异（0 在 zstd 语义里是"用默认级别"）。
int clampZstdLevel(int level) {
    if (level <= 0) {
        return ZSTD_CLEVEL_DEFAULT;  // 3
    }
    const int maxLevel = ZSTD_maxCLevel();
    return level > maxLevel ? maxLevel : level;
}

Bytes zstdCompressBytes(const Bytes& src, int level) {
    // 用 compressBound 兜住最坏情况：不可压缩数据也保证一次分配到位，无需重试循环
    const size_t bound = ::ZSTD_compressBound(src.size());
    Bytes out;
    out.resize(bound);
    // src.data() 对空串也是有效指针（指向结尾 '\0'），不必特判 empty
    const size_t produced =
        ::ZSTD_compress(&out[0], bound, src.data(), src.size(), clampZstdLevel(level));
    if (::ZSTD_isError(produced)) {
        throw std::runtime_error(std::string("zstd compress failed: ") +
                                 ::ZSTD_getErrorName(produced));
    }
    out.resize(produced);
    return out;
}

// 解压上限：防止坏帧头声明天文数字 content size 时按它去申请内存（DoS 面）。
// 2GB 远超 RocketMQ 单条消息体上限（4MB），足够宽松。
constexpr size_t kMaxZstdOutput = 2048ULL * 1024ULL * 1024ULL;

Bytes zstdDecompressBytes(const Bytes& src) {
    if (src.empty()) {
        // Java ZstdCompressor#decompress 对空输入是 read() 立即 -1 → 返回空正文，
        // 这里保持同样语义（而不是把"空"当成坏帧抛错）。
        return Bytes();
    }
    unsigned long long declared = ::ZSTD_getFrameContentSize(src.data(), src.size());
    if (declared == ZSTD_CONTENTSIZE_ERROR) {
        // 帧头都不合法（magic/window/contentSize 保留位错）——绝不能当正文交出
        throw std::runtime_error("zstd decompress failed: not a valid zstd frame");
    }
    const bool sizeKnown = (declared != ZSTD_CONTENTSIZE_UNKNOWN);
    // zstd-jni 的 ZstdOutputStream 是流式写，帧头里 content size 位置写的是 0（未知），
    // 所以未知分支是**常态**而不是异常：用指数扩容 + dstSize_tooSmall 重试覆盖。
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
        // 只有"输出缓冲不够"这一种错误值得扩容重试；其余（坏帧/截断）一律抛错，
        // 且缓冲大小已按帧头声明值给足时也不该再重试——那说明数据本身是坏的。
        if (sizeKnown || ::ZSTD_getErrorCode(produced) != ZSTD_error_dstSize_tooSmall ||
            attempt >= 32) {
            throw std::runtime_error(std::string("zstd decompress failed: ") +
                                     ::ZSTD_getErrorName(produced));
        }
        capacity *= 2;
    }
}

}  // namespace

#endif  // ROCKETMQ_HAS_ZSTD

// ---------------------------------------------------------------- LZ4 Frame 细节
//
// 对应 org.apache.rocketmq.common.compression.Lz4Compressor：Java 用 lz4-java 的
// LZ4FrameOutputStream / LZ4FrameInputStream，即 **LZ4 Frame 互操作格式**
// （magic 0x184D2204，自描述：帧头写明 block 大小/是否独立/有无校验和）。
// 因此这里必须走 liblz4 的 LZ4F（frame）API；LZ4_compress_default 产出的是**裸
// block**，没有帧头和块边界标记，Java/Python/Rust 三端都解不开，绝不能拿来对齐。
#if defined(ROCKETMQ_HAS_LZ4)

namespace {

// 解压时的输出块大小；LZ4F 是流式接口，不需要预知解压后总长。
constexpr size_t kLz4OutputChunk = 64ULL * 1024ULL;
// 同 zstd：拒绝按坏帧头的指示无限申请内存。
constexpr size_t kMaxLz4Output = 2048ULL * 1024ULL * 1024ULL;

Bytes lz4CompressFrame(const Bytes& src) {
    // ⚠ 必须显式给出 prefs，不能用 liblz4 的默认帧参数：默认是 **blockLinked**
    //（后一块可引用前一块），而 lz4-java 的 LZ4FrameInputStream **拒绝依赖块**
    // ——单块（<=64KB）时看不出来，一旦载荷跨块就抛
    // "Dependent block stream is unsupported (BLOCK_INDEPENDENCE must be set)"。
    // 所以这里对齐 lz4-java / Python lz4.frame 的默认写法：块独立 + 64KB 块 + 无校验和。
    // 解码侧不受此约束（LZ4F_decompress 两种模式都能读），故 Java 产的 linked 块我们能解。
    LZ4F_preferences_t prefs;
    std::memset(&prefs, 0, sizeof(prefs));
    prefs.frameInfo.blockSizeID = LZ4F_max64KB;
    prefs.frameInfo.blockMode = LZ4F_blockIndependent;
    prefs.frameInfo.contentChecksumFlag = LZ4F_noContentChecksum;
    prefs.frameInfo.contentSize = static_cast<unsigned long long>(src.size());
    prefs.compressionLevel = 0;  // 0 = liblz4 默认级别

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

// LZ4F_decompress 是流式接口：一次调用可能只吃掉部分输入或只填一部分输出，
// 返回 0 表示帧结束，>0 是"到下一个块边界还需多少输入字节"的提示。
Bytes lz4DecompressFrame(const Bytes& src) {
    if (src.empty()) {
        // 与 Java LZ4FrameInputStream 对空流的行为一致：read() 直接 EOF → 空正文
        return Bytes();
    }
    LZ4F_decompressionContext_t ctx = nullptr;
    if (::LZ4F_isError(::LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION))) {
        throw std::runtime_error("lz4 decompress failed: cannot create decompression context");
    }
    // RAII：下面每条抛错路径（以及 out.append 可能抛的 bad_alloc）都要还掉这个句柄，
    // 手写成对 free 迟早漏一次。
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
            return out;  // 帧边界已到，剩余字节（若有）不属于本帧
        }
        if (srcAvail == 0 && dstWritten == 0) {
            // 输入没吃掉、输出没写出——再循环就是死循环，说明帧不完整
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
        // 与 Java 一致把 level 透传给 zstd（Java: new ZstdOutputStream(out, level)）。
        // ⚠ 因此本端 ZSTD 产出的字节可能与 Python/Rust 不同（它们固定用默认级别 3，
        // 而消息编码路径传的是 zlib 用的 level=5）。帧格式仍是标准 zstd frame，
        // 互解不受影响——级别只影响压缩率，不影响可解码性。
        return zstdCompressBytes(src, level);
#else
        throw std::runtime_error(
            "zstd compression is not available in this build (rebuild with -DRMQ_WITH_ZSTD=ON)");
#endif
    }
    if (type == CompressionType::LZ4) {
#if defined(ROCKETMQ_HAS_LZ4)
        // level 对 LZ4 无意义：Java 的 LZ4FrameOutputStream 构造时就不接受 level。
        (void)level;
        return lz4CompressFrame(src);
#else
        throw std::runtime_error(
            "lz4 compression is not available in this build (rebuild with -DRMQ_WITH_LZ4=ON)");
#endif
    }
    // findByValue 只可能返回 LZ4/ZSTD/ZLIB 三者之一，走到这里说明上面有后端被编掉了。
    throw std::runtime_error("unsupported compression type for compress: " + std::to_string(type));
}

Bytes CompressorFactory::decompress(const Bytes& src, int32_t compressionType) {
    const int32_t type = CompressionType::findByValue(compressionType);
    if (type == CompressionType::ZLIB) {
#if defined(ROCKETMQ_HAS_ZLIB)
        return zlibInflate(src);
#else
        // 关键：不能原样返回。返回压缩字节会被上层当作正文，属静默数据损坏。
        throw std::runtime_error(
            "message is zlib-compressed but this build has no zlib support "
            "(rebuild with -DRMQ_WITH_ZLIB=ON)");
#endif
    }
    if (type == CompressionType::ZSTD) {
#if defined(ROCKETMQ_HAS_ZSTD)
        return zstdDecompressBytes(src);
#else
        // 同上：后端被编掉时抛错，绝不把 zstd 帧当正文交给调用方。
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
    // 未知类型位已在 CompressionType::findByValue 抛出，这里不可达。
    throw std::runtime_error("unsupported compression type for decompress: " + std::to_string(type));
}

}  // namespace rocketmq
