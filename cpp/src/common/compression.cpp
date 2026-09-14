// 消息体压缩/解压实现（对应 org.apache.rocketmq.common.compression.*）。
//
// 编译期开关 ROCKETMQ_HAS_ZLIB 由 CMake 的 find_package(ZLIB) 决定。若未编入
// zlib，遇到被压缩的消息会**抛异常**而不是原样返回——静默透传等于数据损坏。
#include "rocketmq/common/compression.h"

#include <cstring>
#include <stdexcept>

#if defined(ROCKETMQ_HAS_ZLIB)
#include <zlib.h>
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

bool hasZlibSupport() {
#if defined(ROCKETMQ_HAS_ZLIB)
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
    // LZ4 / ZSTD 当前未编入（Java 侧由 lz4-java / zstd-jni 提供）。
    // 同样选择抛错而非静默透传，让问题立刻暴露。
    throw std::runtime_error("unsupported compression type for decompress: " + std::to_string(type));
}

}  // namespace rocketmq
